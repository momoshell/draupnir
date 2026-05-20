from __future__ import annotations

import importlib.util
import io
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
    assert pre_push_check.is_sensitive_path("app/models/job.py")
    assert pre_push_check.is_sensitive_path("app/db/session.py")
    assert pre_push_check.is_sensitive_path("alembic/versions/0001_init.py")
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
            ("git", "diff", "--name-only", "base123..abc123"): (
                "app/api/v1/revisions.py\ntests/test_revision_api.py\n"
            ),
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


def test_main_runs_pytest_only_for_non_sensitive_changes() -> None:
    query = QueryStub(
        {
            ("git", "diff", "--name-only", "def456..abc123"): (
                "docs/runbook.md\nscripts/pre_push_check.py\n"
            )
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
            ("git", "diff", "--name-only", "def456..abc123"): (
                "app/db/session.py\ntests/test_storage.py\n"
            )
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
    assert "app/db/session.py" in error_output


def test_main_runs_migration_before_pytest_for_sensitive_changes() -> None:
    query = QueryStub(
        {
            ("git", "diff", "--name-only", "def456..abc123"): (
                "app/models/job.py\ntests/persistence/test_catalog.py\n"
            )
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
        ("uv", "run", "pytest"),
    ]
    assert "running alembic upgrade head" in stderr.getvalue()


def test_main_rejects_non_local_or_non_test_database_url() -> None:
    query = QueryStub({("git", "diff", "--name-only", "def456..abc123"): ("tests/test_files.py\n")})
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
