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
SENSITIVE_PREFIXES = (
    "app/api/",
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
LOCAL_DATABASE_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


class CommandError(RuntimeError):
    """Raised when a git discovery command fails."""


@dataclass(frozen=True)
class RefUpdate:
    local_ref: str
    local_sha: str
    remote_ref: str
    remote_sha: str


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


def list_diff_files(left: str, right: str, query_runner: QueryRunner) -> list[str]:
    output = query_runner(["git", "diff", "--name-only", f"{left}..{right}"])
    return unique_paths(output.splitlines())


def current_head_files(query_runner: QueryRunner) -> list[str]:
    output = query_runner(["git", "diff", "--name-only", "HEAD"])
    return unique_paths(output.splitlines())


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


def changed_files_for_update(
    update: RefUpdate,
    remote_name: str,
    query_runner: QueryRunner,
) -> list[str]:
    if update.local_sha == ZERO_OID:
        return []
    if update.remote_sha != ZERO_OID:
        return list_diff_files(update.remote_sha, update.local_sha, query_runner)
    merge_base = resolve_merge_base(
        local_ref=update.local_ref,
        local_sha=update.local_sha,
        remote_name=remote_name,
        query_runner=query_runner,
    )
    if merge_base is not None:
        return list_diff_files(merge_base, update.local_sha, query_runner)
    output = query_runner(
        ["git", "diff-tree", "--no-commit-id", "--name-only", "-r", update.local_sha]
    )
    return unique_paths(output.splitlines())


def determine_changed_files(
    updates: Sequence[RefUpdate],
    remote_name: str,
    query_runner: QueryRunner,
) -> list[str]:
    if not updates:
        return current_head_files(query_runner)
    paths: list[str] = []
    for update in updates:
        paths.extend(changed_files_for_update(update, remote_name, query_runner))
    return unique_paths(paths)


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
    normalized = path.replace("\\", "/").lstrip("./")
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


def format_database_url_message(sensitive_paths: Sequence[str]) -> str:
    changed_lines = "\n".join(f"  - {path}" for path in sensitive_paths)
    return (
        "pre-push check failed: DB-sensitive changes require DATABASE_URL.\n"
        "Detected paths:\n"
        f"{changed_lines}\n"
        "Set DATABASE_URL and rerun push. Example:\n"
        f"  export DATABASE_URL={EXAMPLE_DATABASE_URL}\n"
        "This gate runs the DB integration lane with:\n"
        "  uv run alembic upgrade head\n"
        '  uv run pytest -m "integration and not compose_smoke"'
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


def run_gate(
    changed_files: Sequence[str],
    env: Mapping[str, str],
    exec_runner: ExecRunner,
    stderr: TextIO,
) -> int:
    sensitive_paths = [path for path in changed_files if is_sensitive_path(path)]
    if sensitive_paths:
        database_url = env.get("DATABASE_URL")
        if not database_url:
            print(format_database_url_message(sensitive_paths), file=stderr)
            return 1
        validation_error = validate_database_url(database_url)
        if validation_error is not None:
            print(
                format_invalid_database_url_message(sensitive_paths, validation_error),
                file=stderr,
            )
            return 1
        db_integration_lane = 'pytest -m "integration and not compose_smoke"'
        print(
            "pre-push check: DB-sensitive changes detected; running DB integration lane "
            f"via alembic upgrade head then {db_integration_lane}.",
            file=stderr,
        )
        migration_code = exec_runner(["uv", "run", "alembic", "upgrade", "head"])
        if migration_code != 0:
            return migration_code
    else:
        print("pre-push check: no DB-sensitive changes detected.", file=stderr)
        return exec_runner(["uv", "run", "pytest"])
    return exec_runner(DB_INTEGRATION_PYTEST_ARGS)


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
        changed_files = determine_changed_files(updates, args.remote_name, query_runner)
    except (CommandError, ValueError) as error:
        print(f"pre-push check failed: {error}", file=output_stream)
        return 1
    return run_gate(changed_files, active_env, exec_runner, output_stream)


if __name__ == "__main__":
    raise SystemExit(main())
