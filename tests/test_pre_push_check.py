from __future__ import annotations

import ast
import importlib.util
import io
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import ModuleType


def load_pre_push_check_module() -> ModuleType:
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "pre_push_check.py"
    spec = importlib.util.spec_from_file_location("pre_push_check", module_path)
    if spec is None or spec.loader is None:
        raise AssertionError("failed to load pre_push_check module")
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault(spec.name, module)
    spec.loader.exec_module(module)
    return module


pre_push_check = load_pre_push_check_module()


def read_repo_text(relative_path: str) -> str:
    return (Path(__file__).resolve().parents[1] / relative_path).read_text(encoding="utf-8")


def extract_makefile_lane_markers() -> set[str]:
    makefile_text = read_repo_text("Makefile")
    return set(re.findall(r"integration and db_[a-z_]+ and not compose_smoke", makefile_text))


def extract_conftest_db_lane_mapping() -> dict[str, str]:
    module = ast.parse(read_repo_text("tests/conftest.py"))
    for statement in module.body:
        value: ast.expr | None
        if isinstance(statement, ast.Assign):
            targets = statement.targets
            value = statement.value
        elif isinstance(statement, ast.AnnAssign):
            targets = [statement.target]
            value = statement.value
        else:
            continue
        if value is None:
            continue
        for target in targets:
            if isinstance(target, ast.Name) and target.id == "_DB_LANE_BY_TEST_FILE_KEY":
                parsed = ast.literal_eval(value)
                if not isinstance(parsed, dict):
                    raise AssertionError("_DB_LANE_BY_TEST_FILE_KEY must stay a dict literal")
                return {str(key): str(value) for key, value in parsed.items()}
    raise AssertionError("failed to find _DB_LANE_BY_TEST_FILE_KEY in tests/conftest.py")


class QueryStub:
    def __init__(
        self,
        responses: Mapping[tuple[str, ...], str],
        failures: Sequence[tuple[str, ...]] = (),
    ) -> None:
        self._responses = dict(responses)
        self._failures = set(failures)
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, args: Sequence[str]) -> str:
        key = tuple(args)
        self.calls.append(key)
        if key in self._failures:
            raise pre_push_check.CommandError(f"failed: {' '.join(args)}")
        try:
            return self._responses[key]
        except KeyError as error:
            raise AssertionError(f"unexpected query: {key!r}") from error


class ExecStub:
    def __init__(self, exit_codes: Mapping[tuple[str, ...], int] | None = None) -> None:
        self._exit_codes = {} if exit_codes is None else dict(exit_codes)
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, args: Sequence[str]) -> int:
        key = tuple(args)
        self.calls.append(key)
        return self._exit_codes.get(key, 0)


def test_is_sensitive_path_detects_expected_paths() -> None:
    assert pre_push_check.is_sensitive_path("app/api/v1/revisions.py")
    assert pre_push_check.is_sensitive_path("app/schemas/file.py")
    assert pre_push_check.is_sensitive_path("app/jobs/worker.py")
    assert pre_push_check.is_sensitive_path("app/exports/csv.py")
    assert pre_push_check.is_sensitive_path("app/estimating/money.py")
    assert pre_push_check.is_sensitive_path("app/models/job.py")
    assert pre_push_check.is_sensitive_path("app/db/session.py")
    assert pre_push_check.is_sensitive_path("alembic/versions/0001_init.py")
    assert pre_push_check.is_sensitive_path("Makefile")
    assert pre_push_check.is_sensitive_path("pyproject.toml")
    assert pre_push_check.is_sensitive_path("scripts/pre_push_check.py")
    assert pre_push_check.is_sensitive_path("tests/api/test_revisions.py")
    assert pre_push_check.is_sensitive_path("tests/test_files.py")
    assert pre_push_check.is_sensitive_path("tests/test_revision_api.py")
    assert pre_push_check.is_sensitive_path("tests/persistence/test_catalog.py")
    assert not pre_push_check.is_sensitive_path("tests/test_storage.py")
    assert not pre_push_check.is_sensitive_path("docs/runbook.md")


def test_determine_changed_files_uses_merge_base_for_new_branch_push() -> None:
    query = QueryStub(
        {
            (
                "git",
                "for-each-ref",
                "--format=%(upstream:short)",
                "refs/heads/feature/pre-push",
            ): "",
            (
                "git",
                "symbolic-ref",
                "--quiet",
                "--short",
                "refs/remotes/origin/HEAD",
            ): "origin/main",
            ("git", "merge-base", "abc123", "origin/main"): "base123",
            (
                "git",
                "diff",
                "--name-status",
                "--find-renames",
                "base123..abc123",
            ): ("M\tapp/api/v1/revisions.py\nM\ttests/test_revision_api.py\n"),
        },
    )
    updates = [
        pre_push_check.RefUpdate(
            local_ref="refs/heads/feature/pre-push",
            local_sha="abc123",
            remote_ref="refs/heads/feature/pre-push",
            remote_sha=pre_push_check.ZERO_OID,
        )
    ]

    changed_files = pre_push_check.determine_changed_files(updates, "origin", query)

    assert changed_files == [
        "app/api/v1/revisions.py",
        "tests/test_revision_api.py",
    ]


def test_makefile_lane_markers_match_script_constants() -> None:
    assert extract_makefile_lane_markers() == {
        pre_push_check.DB_API_PYTEST_ARGS[4],
        pre_push_check.DB_WORKER_PYTEST_ARGS[4],
        pre_push_check.DB_ESTIMATION_EXPORT_PYTEST_ARGS[4],
        pre_push_check.DB_LINEAGE_PYTEST_ARGS[4],
        pre_push_check.DB_MIGRATION_PYTEST_ARGS[4],
    }


def test_conftest_db_test_file_lane_keys_match_script_mapping() -> None:
    lane_name_by_args = {
        tuple(pre_push_check.DB_API_PYTEST_ARGS): "db_api",
        tuple(pre_push_check.DB_WORKER_PYTEST_ARGS): "db_worker",
        tuple(pre_push_check.DB_ESTIMATION_EXPORT_PYTEST_ARGS): "db_estimation_export",
        tuple(pre_push_check.DB_LINEAGE_PYTEST_ARGS): "db_lineage",
        tuple(pre_push_check.DB_MIGRATION_PYTEST_ARGS): "db_migration",
    }

    assert {
        file_key: lane_name_by_args[tuple(pytest_args)]
        for file_key, pytest_args in pre_push_check.DB_TEST_FILE_KEY_LANE_ARGS.items()
    } == extract_conftest_db_lane_mapping()


def test_main_runs_pytest_only_for_non_sensitive_changes() -> None:
    query = QueryStub(
        {
            (
                "git",
                "diff",
                "--name-status",
                "--find-renames",
                "def456..abc123",
            ): ("M\tdocs/runbook.md\n")
        }
    )
    exec_runner = ExecStub()
    stderr = io.StringIO()

    exit_code = pre_push_check.main(
        ["origin", "git@github.com:momoshell/draupnir.git"],
        stdin=io.StringIO(
            "refs/heads/feature/pre-push abc123 refs/heads/feature/pre-push def456\n"
        ),
        env={},
        query_runner=query,
        exec_runner=exec_runner,
        stderr=stderr,
    )

    assert exit_code == 0
    assert exec_runner.calls == [("uv", "run", "pytest")]
    assert "no DB-sensitive changes" in stderr.getvalue()


def test_main_requires_database_url_for_sensitive_changes() -> None:
    query = QueryStub(
        {
            (
                "git",
                "diff",
                "--name-status",
                "--find-renames",
                "def456..abc123",
            ): ("M\tapp/db/session.py\nM\ttests/test_storage.py\n")
        }
    )
    exec_runner = ExecStub()
    stderr = io.StringIO()

    exit_code = pre_push_check.main(
        ["origin", "git@github.com:momoshell/draupnir.git"],
        stdin=io.StringIO(
            "refs/heads/feature/pre-push abc123 refs/heads/feature/pre-push def456\n"
        ),
        env={},
        query_runner=query,
        exec_runner=exec_runner,
        stderr=stderr,
    )

    assert exit_code == 1
    assert exec_runner.calls == []
    error_output = stderr.getvalue()
    assert "DATABASE_URL" in error_output
    assert "uv run alembic upgrade head" in error_output
    assert 'uv run pytest -m "integration and not compose_smoke"' in error_output
    assert "DB integration lane" in error_output
    assert "app/db/session.py" in error_output


def test_main_runs_migration_before_single_source_db_lane() -> None:
    query = QueryStub(
        {
            (
                "git",
                "diff",
                "--name-status",
                "--find-renames",
                "def456..abc123",
            ): ("M\tapp/api/v1/revisions.py\n")
        }
    )
    exec_runner = ExecStub()
    stderr = io.StringIO()

    exit_code = pre_push_check.main(
        ["origin", "git@github.com:momoshell/draupnir.git"],
        stdin=io.StringIO(
            "refs/heads/feature/pre-push abc123 refs/heads/feature/pre-push def456\n"
        ),
        env={"DATABASE_URL": pre_push_check.EXAMPLE_DATABASE_URL},
        query_runner=query,
        exec_runner=exec_runner,
        stderr=stderr,
    )

    assert exit_code == 0
    assert exec_runner.calls == [
        ("uv", "run", "alembic", "upgrade", "head"),
        ("uv", "run", "pytest", "-m", "integration and db_api and not compose_smoke"),
    ]
    error_output = stderr.getvalue()
    assert "single-source DB lane" in error_output
    assert 'pytest -m "integration and db_api and not compose_smoke"' in error_output


def test_main_runs_migration_before_same_lane_mixed_source_and_test_changes() -> None:
    query = QueryStub(
        {
            (
                "git",
                "diff",
                "--name-status",
                "--find-renames",
                "def456..abc123",
            ): ("M\tapp/api/v1/revisions.py\nM\ttests/test_files.py\n")
        }
    )
    exec_runner = ExecStub()
    stderr = io.StringIO()

    exit_code = pre_push_check.main(
        ["origin", "git@github.com:momoshell/draupnir.git"],
        stdin=io.StringIO(
            "refs/heads/feature/pre-push abc123 refs/heads/feature/pre-push def456\n"
        ),
        env={"DATABASE_URL": pre_push_check.EXAMPLE_DATABASE_URL},
        query_runner=query,
        exec_runner=exec_runner,
        stderr=stderr,
    )

    assert exit_code == 0
    assert exec_runner.calls == [
        ("uv", "run", "alembic", "upgrade", "head"),
        ("uv", "run", "pytest", "-m", "integration and db_api and not compose_smoke"),
    ]
    error_output = stderr.getvalue()
    assert "single-source DB lane" in error_output
    assert 'pytest -m "integration and db_api and not compose_smoke"' in error_output


def test_main_runs_migration_before_focused_db_test_files() -> None:
    query = QueryStub(
        {
            (
                "git",
                "diff",
                "--name-status",
                "--find-renames",
                "def456..abc123",
            ): ("M\ttests/test_jobs_worker_ingest.py\nM\ttests/test_jobs_worker_quantity.py\n")
        }
    )
    exec_runner = ExecStub()
    stderr = io.StringIO()

    exit_code = pre_push_check.main(
        ["origin", "git@github.com:momoshell/draupnir.git"],
        stdin=io.StringIO(
            "refs/heads/feature/pre-push abc123 refs/heads/feature/pre-push def456\n"
        ),
        env={"DATABASE_URL": pre_push_check.EXAMPLE_DATABASE_URL},
        query_runner=query,
        exec_runner=exec_runner,
        stderr=stderr,
    )

    assert exit_code == 0
    assert exec_runner.calls == [
        ("uv", "run", "alembic", "upgrade", "head"),
        (
            "uv",
            "run",
            "pytest",
            "tests/test_jobs_worker_ingest.py",
            "tests/test_jobs_worker_quantity.py",
        ),
    ]
    error_output = stderr.getvalue()
    assert "focused DB test files" in error_output
    assert (
        "uv run pytest tests/test_jobs_worker_ingest.py "
        "tests/test_jobs_worker_quantity.py" in error_output
    )


def test_main_runs_migration_before_focused_db_api_test_file() -> None:
    query = QueryStub(
        {
            (
                "git",
                "diff",
                "--name-status",
                "--find-renames",
                "def456..abc123",
            ): ("M\ttests/test_files.py\n")
        }
    )
    exec_runner = ExecStub()
    stderr = io.StringIO()

    exit_code = pre_push_check.main(
        ["origin", "git@github.com:momoshell/draupnir.git"],
        stdin=io.StringIO(
            "refs/heads/feature/pre-push abc123 refs/heads/feature/pre-push def456\n"
        ),
        env={"DATABASE_URL": pre_push_check.EXAMPLE_DATABASE_URL},
        query_runner=query,
        exec_runner=exec_runner,
        stderr=stderr,
    )

    assert exit_code == 0
    assert exec_runner.calls == [
        ("uv", "run", "alembic", "upgrade", "head"),
        ("uv", "run", "pytest", "tests/test_files.py"),
    ]
    error_output = stderr.getvalue()
    assert "focused DB test files" in error_output
    assert "uv run pytest tests/test_files.py" in error_output


def test_main_falls_back_to_full_integration_for_mixed_sensitive_lanes() -> None:
    query = QueryStub(
        {
            (
                "git",
                "diff",
                "--name-status",
                "--find-renames",
                "def456..abc123",
            ): ("M\tapp/api/v1/revisions.py\nM\tapp/jobs/worker.py\n")
        }
    )
    exec_runner = ExecStub()
    stderr = io.StringIO()

    exit_code = pre_push_check.main(
        ["origin", "git@github.com:momoshell/draupnir.git"],
        stdin=io.StringIO(
            "refs/heads/feature/pre-push abc123 refs/heads/feature/pre-push def456\n"
        ),
        env={"DATABASE_URL": pre_push_check.EXAMPLE_DATABASE_URL},
        query_runner=query,
        exec_runner=exec_runner,
        stderr=stderr,
    )

    assert exit_code == 0
    assert exec_runner.calls == [
        ("uv", "run", "alembic", "upgrade", "head"),
        ("uv", "run", "pytest", "-m", "integration and not compose_smoke"),
    ]
    assert "DB integration lane" in stderr.getvalue()


def test_main_falls_back_to_full_integration_for_selector_changes() -> None:
    query = QueryStub(
        {
            (
                "git",
                "diff",
                "--name-status",
                "--find-renames",
                "def456..abc123",
            ): ("M\tscripts/pre_push_check.py\n")
        }
    )
    exec_runner = ExecStub()
    stderr = io.StringIO()

    exit_code = pre_push_check.main(
        ["origin", "git@github.com:momoshell/draupnir.git"],
        stdin=io.StringIO(
            "refs/heads/feature/pre-push abc123 refs/heads/feature/pre-push def456\n"
        ),
        env={"DATABASE_URL": pre_push_check.EXAMPLE_DATABASE_URL},
        query_runner=query,
        exec_runner=exec_runner,
        stderr=stderr,
    )

    assert exit_code == 0
    assert exec_runner.calls == [
        ("uv", "run", "alembic", "upgrade", "head"),
        ("uv", "run", "pytest", "-m", "integration and not compose_smoke"),
    ]
    assert "DB integration lane" in stderr.getvalue()


def test_main_falls_back_to_full_integration_for_makefile_and_pyproject_changes() -> None:
    query = QueryStub(
        {
            (
                "git",
                "diff",
                "--name-status",
                "--find-renames",
                "def456..abc123",
            ): ("M\tMakefile\nM\tpyproject.toml\n")
        }
    )
    exec_runner = ExecStub()
    stderr = io.StringIO()

    exit_code = pre_push_check.main(
        ["origin", "git@github.com:momoshell/draupnir.git"],
        stdin=io.StringIO(
            "refs/heads/feature/pre-push abc123 refs/heads/feature/pre-push def456\n"
        ),
        env={"DATABASE_URL": pre_push_check.EXAMPLE_DATABASE_URL},
        query_runner=query,
        exec_runner=exec_runner,
        stderr=stderr,
    )

    assert exit_code == 0
    assert exec_runner.calls == [
        ("uv", "run", "alembic", "upgrade", "head"),
        ("uv", "run", "pytest", "-m", "integration and not compose_smoke"),
    ]
    assert "DB integration lane" in stderr.getvalue()


def test_main_falls_back_to_full_integration_for_added_db_test_files() -> None:
    query = QueryStub(
        {
            (
                "git",
                "diff",
                "--name-status",
                "--find-renames",
                "def456..abc123",
            ): ("A\ttests/test_jobs_worker_ingest.py\n")
        }
    )
    exec_runner = ExecStub()
    stderr = io.StringIO()

    exit_code = pre_push_check.main(
        ["origin", "git@github.com:momoshell/draupnir.git"],
        stdin=io.StringIO(
            "refs/heads/feature/pre-push abc123 refs/heads/feature/pre-push def456\n"
        ),
        env={"DATABASE_URL": pre_push_check.EXAMPLE_DATABASE_URL},
        query_runner=query,
        exec_runner=exec_runner,
        stderr=stderr,
    )

    assert exit_code == 0
    assert exec_runner.calls == [
        ("uv", "run", "alembic", "upgrade", "head"),
        ("uv", "run", "pytest", "-m", "integration and not compose_smoke"),
    ]
    assert "DB integration lane" in stderr.getvalue()


def test_main_falls_back_to_full_integration_for_schema_changes() -> None:
    query = QueryStub(
        {
            (
                "git",
                "diff",
                "--name-status",
                "--find-renames",
                "def456..abc123",
            ): ("M\tapp/schemas/files.py\n")
        }
    )
    exec_runner = ExecStub()
    stderr = io.StringIO()

    exit_code = pre_push_check.main(
        ["origin", "git@github.com:momoshell/draupnir.git"],
        stdin=io.StringIO(
            "refs/heads/feature/pre-push abc123 refs/heads/feature/pre-push def456\n"
        ),
        env={"DATABASE_URL": pre_push_check.EXAMPLE_DATABASE_URL},
        query_runner=query,
        exec_runner=exec_runner,
        stderr=stderr,
    )

    assert exit_code == 0
    assert exec_runner.calls == [
        ("uv", "run", "alembic", "upgrade", "head"),
        ("uv", "run", "pytest", "-m", "integration and not compose_smoke"),
    ]
    assert "DB integration lane" in stderr.getvalue()


def test_main_falls_back_to_full_integration_for_deleted_mapped_db_test_files() -> None:
    query = QueryStub(
        {
            (
                "git",
                "diff",
                "--name-status",
                "--find-renames",
                "def456..abc123",
            ): ("D\ttests/test_files.py\n")
        }
    )
    exec_runner = ExecStub()
    stderr = io.StringIO()

    exit_code = pre_push_check.main(
        ["origin", "git@github.com:momoshell/draupnir.git"],
        stdin=io.StringIO(
            "refs/heads/feature/pre-push abc123 refs/heads/feature/pre-push def456\n"
        ),
        env={"DATABASE_URL": pre_push_check.EXAMPLE_DATABASE_URL},
        query_runner=query,
        exec_runner=exec_runner,
        stderr=stderr,
    )

    assert exit_code == 0
    assert exec_runner.calls == [
        ("uv", "run", "alembic", "upgrade", "head"),
        ("uv", "run", "pytest", "-m", "integration and not compose_smoke"),
    ]
    assert "DB integration lane" in stderr.getvalue()


def test_main_rejects_non_local_or_non_test_database_url() -> None:
    query = QueryStub(
        {
            (
                "git",
                "diff",
                "--name-status",
                "--find-renames",
                "def456..abc123",
            ): ("M\ttests/test_files.py\n")
        }
    )
    exec_runner = ExecStub()
    stderr = io.StringIO()

    exit_code = pre_push_check.main(
        ["origin", "git@github.com:momoshell/draupnir.git"],
        stdin=io.StringIO(
            "refs/heads/feature/pre-push abc123 refs/heads/feature/pre-push def456\n"
        ),
        env={"DATABASE_URL": "postgresql+asyncpg://postgres:postgres@db.internal:5432/draupnir"},
        query_runner=query,
        exec_runner=exec_runner,
        stderr=stderr,
    )

    assert exit_code == 1
    assert exec_runner.calls == []
    error_output = stderr.getvalue()
    assert "dedicated local test DATABASE_URL" in error_output
    assert "localhost, 127.0.0.1, or ::1" in error_output
    assert pre_push_check.EXAMPLE_DATABASE_URL in error_output
