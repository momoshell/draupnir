"""Adaptive pre-push CI parity gate."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TextIO
from urllib.parse import urlparse

ZERO_OID = "0" * 40
EXAMPLE_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/draupnir_test"
DB_INTEGRATION_PYTEST_ARGS = [
    "uv",
    "run",
    "pytest",
    "-m",
    "integration and not compose_smoke",
]
DB_API_PYTEST_ARGS = [
    "uv",
    "run",
    "pytest",
    "-m",
    "integration and db_api and not compose_smoke",
]
DB_WORKER_PYTEST_ARGS = [
    "uv",
    "run",
    "pytest",
    "-m",
    "integration and db_worker and not compose_smoke",
]
DB_ESTIMATION_EXPORT_PYTEST_ARGS = [
    "uv",
    "run",
    "pytest",
    "-m",
    "integration and db_estimation_export and not compose_smoke",
]
DB_LINEAGE_PYTEST_ARGS = [
    "uv",
    "run",
    "pytest",
    "-m",
    "integration and db_lineage and not compose_smoke",
]
DB_MIGRATION_PYTEST_ARGS = [
    "uv",
    "run",
    "pytest",
    "-m",
    "integration and db_migration and not compose_smoke",
]
SENSITIVE_PREFIXES = (
    "app/api/",
    "app/schemas/",
    "app/jobs/",
    "app/estimating/",
    "app/exports/",
    "app/models/",
    "app/db/",
    "alembic/",
)
SENSITIVE_TEST_TOKENS = frozenset({"api", "db", "persistence"})
SENSITIVE_TEST_CONTENT_MARKERS = frozenset(
    {
        "requires_database",
        "from app.db",
        "import app.db",
        "from sqlalchemy",
        "import sqlalchemy",
        "asyncsession",
        "async_sessionmaker",
    }
)
UNSAFE_FALLBACK_PATHS = frozenset(
    {
        "Makefile",
        "pyproject.toml",
        "scripts/pre_push_check.py",
        "tests/conftest.py",
        "tests/jobs_test_helpers.py",
    }
)
SAFE_SOURCE_LANE_ARGS = {
    "app/api/": DB_API_PYTEST_ARGS,
    "app/jobs/": DB_WORKER_PYTEST_ARGS,
    "app/estimating/": DB_ESTIMATION_EXPORT_PYTEST_ARGS,
    "app/exports/": DB_ESTIMATION_EXPORT_PYTEST_ARGS,
}
DB_TEST_FILE_KEY_LANE_ARGS = {
    "projects": DB_API_PYTEST_ARGS,
    "files": DB_API_PYTEST_ARGS,
    "idempotency": DB_API_PYTEST_ARGS,
    "extraction_profiles": DB_API_PYTEST_ARGS,
    "quantity_takeoff_api": DB_API_PYTEST_ARGS,
    "estimate_read_api": DB_API_PYTEST_ARGS,
    "changeset_api": DB_API_PYTEST_ARGS,
    "changeset_to_dxf_smoke": DB_API_PYTEST_ARGS,
    "export_create_api": DB_API_PYTEST_ARGS,
    "generated_artifact_download_api": DB_API_PYTEST_ARGS,
    "revision_materialization_api": DB_API_PYTEST_ARGS,
    "revision_scale_api": DB_API_PYTEST_ARGS,
    "revision_summary_api": DB_API_PYTEST_ARGS,
    "revision_room_entities_api": DB_API_PYTEST_ARGS,
    "revision_diff_api": DB_API_PYTEST_ARGS,
    "revision_entity_source_api": DB_API_PYTEST_ARGS,
    "validation_report_api": DB_API_PYTEST_ARGS,
    "revision_quantity_estimate_flow": DB_API_PYTEST_ARGS,
    "smoke": DB_API_PYTEST_ARGS,
    "jobs": DB_WORKER_PYTEST_ARGS,
    "jobs_api": DB_WORKER_PYTEST_ARGS,
    "jobs_worker_ingest": DB_WORKER_PYTEST_ARGS,
    "jobs_worker_lifecycle": DB_WORKER_PYTEST_ARGS,
    "jobs_worker_quantity": DB_WORKER_PYTEST_ARGS,
    "jobs_worker_estimate": DB_WORKER_PYTEST_ARGS,
    "jobs_worker_exports": DB_WORKER_PYTEST_ARGS,
    "ingest_output_persistence": DB_WORKER_PYTEST_ARGS,
    "interpretation_service_takeoff_loaders": DB_WORKER_PYTEST_ARGS,
    "csv_exports": DB_ESTIMATION_EXPORT_PYTEST_ARGS,
    "revised_dxf_export": DB_ESTIMATION_EXPORT_PYTEST_ARGS,
    "revision_json_export": DB_ESTIMATION_EXPORT_PYTEST_ARGS,
    "estimate_pdf_export": DB_ESTIMATION_EXPORT_PYTEST_ARGS,
    "estimate_engine_persistence_payloads": DB_ESTIMATION_EXPORT_PYTEST_ARGS,
    "estimate_job_input_contract": DB_ESTIMATION_EXPORT_PYTEST_ARGS,
    "estimate_snapshot_item_persistence": DB_ESTIMATION_EXPORT_PYTEST_ARGS,
    "estimate_version_persistence": DB_ESTIMATION_EXPORT_PYTEST_ARGS,
    "estimation_catalog_api": DB_ESTIMATION_EXPORT_PYTEST_ARGS,
    "estimation_catalog_persistence": DB_ESTIMATION_EXPORT_PYTEST_ARGS,
    "estimation_catalog_resolver": DB_ESTIMATION_EXPORT_PYTEST_ARGS,
    "export_job_input_contract": DB_ESTIMATION_EXPORT_PYTEST_ARGS,
    "quantity_takeoff_persistence": DB_ESTIMATION_EXPORT_PYTEST_ARGS,
    "append_only_lineage_tables": DB_LINEAGE_PYTEST_ARGS,
    "changeset_apply": DB_LINEAGE_PYTEST_ARGS,
    "changeset_persistence": DB_LINEAGE_PYTEST_ARGS,
    "changeset_validation": DB_LINEAGE_PYTEST_ARGS,
    "lineage_delete_restrictions": DB_LINEAGE_PYTEST_ARGS,
    "revision_routed_length_model": DB_LINEAGE_PYTEST_ARGS,
    "db": DB_MIGRATION_PYTEST_ARGS,
    "jobs_base_revision_migration": DB_MIGRATION_PYTEST_ARGS,
    "jobs_revision_scoped_contract_migration": DB_MIGRATION_PYTEST_ARGS,
}
SAFE_DB_TEST_FILE_LANE_ARGS = {
    f"tests/test_{file_key}.py": lane_args
    for file_key, lane_args in DB_TEST_FILE_KEY_LANE_ARGS.items()
}
DB_LANE_PYTEST_ARGS = (
    tuple(DB_API_PYTEST_ARGS),
    tuple(DB_WORKER_PYTEST_ARGS),
    tuple(DB_ESTIMATION_EXPORT_PYTEST_ARGS),
    tuple(DB_LINEAGE_PYTEST_ARGS),
    tuple(DB_MIGRATION_PYTEST_ARGS),
    tuple(DB_INTEGRATION_PYTEST_ARGS),
)
LOCAL_DATABASE_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


class CommandError(RuntimeError):
    """Raised when a git discovery command fails."""


@dataclass(frozen=True)
class RefUpdate:
    local_ref: str
    local_sha: str
    remote_ref: str
    remote_sha: str


@dataclass(frozen=True)
class ChangedPath:
    path: str
    status: str


@dataclass(frozen=True)
class DbPytestSelection:
    pytest_args: tuple[str, ...]
    description: str


QueryRunner = Callable[[Sequence[str]], str]
ExecRunner = Callable[[Sequence[str]], int]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("remote_name", nargs="?", default="origin")
    parser.add_argument("remote_url", nargs="?", default="")
    return parser.parse_args(argv)


def parse_ref_updates(lines: Iterable[str]) -> list[RefUpdate]:
    updates: list[RefUpdate] = []
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 4:
            raise ValueError(f"invalid pre-push update line: {raw_line!r}")
        updates.append(RefUpdate(*parts[:4]))
    return updates


def default_query_runner(args: Sequence[str]) -> str:
    result = subprocess.run(
        list(args),
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip() or "command failed"
        raise CommandError(f"{' '.join(args)}: {message}")
    return result.stdout.strip()


def default_exec_runner(args: Sequence[str]) -> int:
    result = subprocess.run(list(args), check=False)
    return result.returncode


def try_query(args: Sequence[str], query_runner: QueryRunner) -> str | None:
    try:
        output = query_runner(args)
    except CommandError:
        return None
    return output or None


def unique_paths(paths: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for path in paths:
        normalized = path.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def normalize_path(path: str) -> str:
    return path.replace("\\", "/").lstrip("./")


def unique_changed_paths(paths: Iterable[ChangedPath]) -> list[ChangedPath]:
    seen: set[str] = set()
    ordered: list[ChangedPath] = []
    for changed_path in paths:
        normalized = normalize_path(changed_path.path)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(ChangedPath(path=normalized, status=changed_path.status))
    return ordered


def parse_name_status_output(lines: Iterable[str]) -> list[ChangedPath]:
    parsed: list[ChangedPath] = []
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        parts = raw_line.rstrip("\n").split("\t")
        if len(parts) < 2:
            raise ValueError(f"invalid name-status line: {raw_line!r}")
        status = parts[0].strip().upper()
        if status.startswith(("R", "C")):
            if len(parts) < 3:
                raise ValueError(f"invalid rename/copy line: {raw_line!r}")
            changed_file_paths = parts[1:3]
        else:
            changed_file_paths = parts[1:2]
        for path in changed_file_paths:
            normalized = normalize_path(path)
            if normalized:
                parsed.append(ChangedPath(path=normalized, status=status))
    return unique_changed_paths(parsed)


def list_diff_changed_paths(
    left: str,
    right: str,
    query_runner: QueryRunner,
) -> list[ChangedPath]:
    output = query_runner(["git", "diff", "--name-status", "--find-renames", f"{left}..{right}"])
    return parse_name_status_output(output.splitlines())


def current_head_changed_paths(query_runner: QueryRunner) -> list[ChangedPath]:
    output = query_runner(["git", "diff", "--name-status", "--find-renames", "HEAD"])
    return parse_name_status_output(output.splitlines())


def resolve_merge_base(
    local_ref: str,
    local_sha: str,
    remote_name: str,
    query_runner: QueryRunner,
) -> str | None:
    candidates: list[str] = []
    if local_ref.startswith("refs/heads/"):
        upstream = try_query(
            ["git", "for-each-ref", "--format=%(upstream:short)", local_ref],
            query_runner,
        )
        if upstream:
            candidates.append(upstream)
    remote_head = try_query(
        ["git", "symbolic-ref", "--quiet", "--short", f"refs/remotes/{remote_name}/HEAD"],
        query_runner,
    )
    if remote_head:
        candidates.append(remote_head)
    candidates.extend((f"{remote_name}/main", f"{remote_name}/master"))
    for candidate in unique_paths(candidates):
        merge_base = try_query(["git", "merge-base", local_sha, candidate], query_runner)
        if merge_base:
            return merge_base
    return None


def changed_paths_for_update(
    update: RefUpdate,
    remote_name: str,
    query_runner: QueryRunner,
) -> list[ChangedPath]:
    if update.local_sha == ZERO_OID:
        return []
    if update.remote_sha != ZERO_OID:
        return list_diff_changed_paths(update.remote_sha, update.local_sha, query_runner)
    merge_base = resolve_merge_base(
        local_ref=update.local_ref,
        local_sha=update.local_sha,
        remote_name=remote_name,
        query_runner=query_runner,
    )
    if merge_base is not None:
        return list_diff_changed_paths(merge_base, update.local_sha, query_runner)
    output = query_runner(
        [
            "git",
            "diff-tree",
            "--no-commit-id",
            "--name-status",
            "--find-renames",
            "-r",
            update.local_sha,
        ]
    )
    return parse_name_status_output(output.splitlines())


def determine_changed_paths(
    updates: Sequence[RefUpdate],
    remote_name: str,
    query_runner: QueryRunner,
) -> list[ChangedPath]:
    if not updates:
        return current_head_changed_paths(query_runner)
    paths: list[ChangedPath] = []
    for update in updates:
        paths.extend(changed_paths_for_update(update, remote_name, query_runner))
    return unique_changed_paths(paths)


def determine_changed_files(
    updates: Sequence[RefUpdate],
    remote_name: str,
    query_runner: QueryRunner,
) -> list[str]:
    changed_paths = determine_changed_paths(updates, remote_name, query_runner)
    return [changed_path.path for changed_path in changed_paths]


def test_file_uses_database(path: str) -> bool:
    file_path = Path(path)
    if not file_path.is_file():
        return False
    try:
        contents = file_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return False
    lowered = contents.lower()
    return any(marker in lowered for marker in SENSITIVE_TEST_CONTENT_MARKERS)


def is_sensitive_path(path: str) -> bool:
    normalized = normalize_path(path)
    if normalized in UNSAFE_FALLBACK_PATHS:
        return True
    if normalized in SAFE_DB_TEST_FILE_LANE_ARGS:
        return True
    if any(normalized.startswith(prefix) for prefix in SENSITIVE_PREFIXES):
        return True
    pure_path = PurePosixPath(normalized)
    if not pure_path.parts or pure_path.parts[0] != "tests":
        return False
    test_tokens = set(re.split(r"[^a-z0-9]+", pure_path.stem.lower()))
    path_tokens = {part.lower() for part in pure_path.parts[1:-1]}
    if SENSITIVE_TEST_TOKENS & (test_tokens | path_tokens):
        return True
    return test_file_uses_database(normalized)


def format_pytest_command(args: Sequence[str]) -> str:
    if len(args) >= 5 and list(args[:4]) == ["uv", "run", "pytest", "-m"]:
        return f'uv run pytest -m "{args[4]}"'
    return " ".join(args)


def format_database_url_message(
    sensitive_paths: Sequence[str],
    pytest_args: Sequence[str],
) -> str:
    changed_lines = "\n".join(f"  - {path}" for path in sensitive_paths)
    return (
        "pre-push check failed: DB-sensitive changes require DATABASE_URL.\n"
        "Detected paths:\n"
        f"{changed_lines}\n"
        "Set DATABASE_URL and rerun push. Example:\n"
        f"  export DATABASE_URL={EXAMPLE_DATABASE_URL}\n"
        "This gate runs the selected DB pytest command "
        "(DB integration lane fallback when needed) with:\n"
        "  uv run alembic upgrade head\n"
        f"  {format_pytest_command(pytest_args)}"
    )


def validate_database_url(database_url: str) -> str | None:
    parsed = urlparse(database_url)
    if not parsed.scheme.startswith("postgresql"):
        return "DATABASE_URL must use a PostgreSQL URL."
    if parsed.hostname not in LOCAL_DATABASE_HOSTS:
        return (
            "DATABASE_URL must point to a dedicated local test database on "
            "localhost, 127.0.0.1, or ::1."
        )
    database_name = parsed.path.lstrip("/")
    if not database_name.endswith("_test"):
        return "DATABASE_URL must use a dedicated test database name ending in '_test'."
    return None


def format_invalid_database_url_message(
    sensitive_paths: Sequence[str],
    validation_error: str,
) -> str:
    changed_lines = "\n".join(f"  - {path}" for path in sensitive_paths)
    return (
        "pre-push check failed: DB-sensitive changes require a dedicated local "
        "test DATABASE_URL.\n"
        "Detected paths:\n"
        f"{changed_lines}\n"
        f"{validation_error}\n"
        "Set DATABASE_URL to a local test database and rerun push. Example:\n"
        f"  export DATABASE_URL={EXAMPLE_DATABASE_URL}"
    )


def classify_db_change(changed_path: ChangedPath) -> DbPytestSelection | None:
    if changed_path.status[:1] != "M":
        return DbPytestSelection(tuple(DB_INTEGRATION_PYTEST_ARGS), "DB integration lane")

    path = normalize_path(changed_path.path)
    if path in UNSAFE_FALLBACK_PATHS:
        return DbPytestSelection(tuple(DB_INTEGRATION_PYTEST_ARGS), "DB integration lane")

    for prefix, pytest_args in SAFE_SOURCE_LANE_ARGS.items():
        if path.startswith(prefix):
            return DbPytestSelection(tuple(pytest_args), "single-source DB lane")

    if any(path.startswith(prefix) for prefix in ("app/models/", "app/db/", "alembic/")):
        return DbPytestSelection(tuple(DB_INTEGRATION_PYTEST_ARGS), "DB integration lane")

    if not path.startswith("tests/"):
        return DbPytestSelection(tuple(DB_INTEGRATION_PYTEST_ARGS), "DB integration lane")

    mapped_pytest_args = SAFE_DB_TEST_FILE_LANE_ARGS.get(path)
    if mapped_pytest_args is None or not Path(path).is_file():
        return DbPytestSelection(tuple(DB_INTEGRATION_PYTEST_ARGS), "DB integration lane")
    return DbPytestSelection(tuple(mapped_pytest_args), "focused DB test files")


def select_db_pytest_selection(changed_paths: Sequence[ChangedPath]) -> DbPytestSelection | None:
    sensitive_changes = [
        changed_path for changed_path in changed_paths if is_sensitive_path(changed_path.path)
    ]
    if not sensitive_changes:
        return None

    lane_args: tuple[str, ...] | None = None
    focused_test_paths: list[str] = []
    saw_source_change = False
    saw_test_change = False
    fallback_selection = DbPytestSelection(tuple(DB_INTEGRATION_PYTEST_ARGS), "DB integration lane")

    for changed_path in sensitive_changes:
        selection = classify_db_change(changed_path)
        if selection is None:
            continue
        if selection.pytest_args == tuple(DB_INTEGRATION_PYTEST_ARGS):
            return fallback_selection

        current_path = normalize_path(changed_path.path)
        if current_path.startswith("tests/"):
            saw_test_change = True
            focused_test_paths.append(current_path)
        else:
            saw_source_change = True

        if lane_args is None:
            lane_args = selection.pytest_args
            continue
        if selection.pytest_args != lane_args:
            return fallback_selection

    if lane_args is None:
        return fallback_selection
    if saw_test_change:
        if saw_source_change:
            return DbPytestSelection(pytest_args=lane_args, description="single-source DB lane")
        return DbPytestSelection(
            pytest_args=("uv", "run", "pytest", *focused_test_paths),
            description="focused DB test files",
        )
    return DbPytestSelection(pytest_args=lane_args, description="single-source DB lane")


def run_gate(
    changed_paths: Sequence[ChangedPath],
    env: Mapping[str, str],
    exec_runner: ExecRunner,
    stderr: TextIO,
) -> int:
    selection = select_db_pytest_selection(changed_paths)
    if selection is not None:
        sensitive_paths = [
            changed_path.path
            for changed_path in changed_paths
            if is_sensitive_path(changed_path.path)
        ]
        database_url = env.get("DATABASE_URL")
        if not database_url:
            print(format_database_url_message(sensitive_paths, selection.pytest_args), file=stderr)
            return 1
        validation_error = validate_database_url(database_url)
        if validation_error is not None:
            print(
                format_invalid_database_url_message(sensitive_paths, validation_error),
                file=stderr,
            )
            return 1
        print(
            "pre-push check: DB-sensitive changes detected; running "
            f"{selection.description} via alembic upgrade head then "
            f"{format_pytest_command(selection.pytest_args)}.",
            file=stderr,
        )
        migration_code = exec_runner(["uv", "run", "alembic", "upgrade", "head"])
        if migration_code != 0:
            return migration_code
    else:
        print("pre-push check: no DB-sensitive changes detected.", file=stderr)
        return exec_runner(["uv", "run", "pytest"])
    return exec_runner(selection.pytest_args)


def main(
    argv: Sequence[str] | None = None,
    *,
    stdin: TextIO | None = None,
    env: Mapping[str, str] | None = None,
    query_runner: QueryRunner = default_query_runner,
    exec_runner: ExecRunner = default_exec_runner,
    stderr: TextIO | None = None,
) -> int:
    args = parse_args(argv)
    input_stream = sys.stdin if stdin is None else stdin
    output_stream = sys.stderr if stderr is None else stderr
    active_env = os.environ if env is None else env
    try:
        updates = parse_ref_updates(input_stream)
        changed_paths = determine_changed_paths(updates, args.remote_name, query_runner)
    except (CommandError, ValueError) as error:
        print(f"pre-push check failed: {error}", file=output_stream)
        return 1
    return run_gate(changed_paths, active_env, exec_runner, output_stream)


if __name__ == "__main__":
    raise SystemExit(main())
