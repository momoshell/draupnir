"""Profile DB integration lanes across migration, collect-only, and pytest run."""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TextIO

DEFAULT_DURATIONS = 50
DEFAULT_DURATIONS_MIN = 0.2
BASE_MIGRATION_COMMAND = ("uv", "run", "alembic", "upgrade", "head")
BASE_PYTEST_COMMAND = ("uv", "run", "pytest")


@dataclass(frozen=True)
class ProfileConfig:
    marker: str
    durations: int
    durations_min: float
    pytest_args: tuple[str, ...]


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


CommandRunner = Callable[[Sequence[str], bool], CommandResult]
Clock = Callable[[], float]


def parse_args(argv: Sequence[str] | None = None) -> ProfileConfig:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--marker", required=True)
    parser.add_argument("--durations", type=int, default=DEFAULT_DURATIONS)
    parser.add_argument("--durations-min", type=float, default=DEFAULT_DURATIONS_MIN)
    namespace, pytest_args = parser.parse_known_args(argv)
    return ProfileConfig(
        marker=namespace.marker,
        durations=namespace.durations,
        durations_min=namespace.durations_min,
        pytest_args=tuple(normalize_pytest_args(pytest_args)),
    )


def normalize_pytest_args(args: Sequence[str]) -> list[str]:
    if args and args[0] == "--":
        return list(args[1:])
    return list(args)


def build_collection_command(config: ProfileConfig) -> list[str]:
    return [
        *BASE_PYTEST_COMMAND,
        "-m",
        config.marker,
        "--collect-only",
        "-q",
        *config.pytest_args,
    ]


def build_migration_command() -> list[str]:
    return [*BASE_MIGRATION_COMMAND]


def build_execution_command(config: ProfileConfig) -> list[str]:
    return [
        *BASE_PYTEST_COMMAND,
        "-m",
        config.marker,
        f"--durations={config.durations}",
        f"--durations-min={config.durations_min:g}",
        *config.pytest_args,
    ]


def default_runner(command: Sequence[str], capture_output: bool) -> CommandResult:
    result = subprocess.run(
        list(command),
        check=False,
        capture_output=capture_output,
        text=capture_output,
    )
    return CommandResult(
        returncode=result.returncode,
        stdout=result.stdout or "",
        stderr=result.stderr or "",
    )


def format_command(command: Sequence[str]) -> str:
    return shlex.join(command)


def print_command_start(label: str, command: Sequence[str], stream: TextIO) -> None:
    print(f"[profile] {label} command: {format_command(command)}", file=stream)


def print_command_finish(
    label: str,
    elapsed_seconds: float,
    returncode: int,
    stream: TextIO,
) -> None:
    print(
        f"[profile] {label} elapsed: {elapsed_seconds:.3f}s (exit {returncode})",
        file=stream,
    )


def relay_captured_output(result: CommandResult, stdout: TextIO, stderr: TextIO) -> None:
    if result.stdout:
        print(result.stdout, end="", file=stdout)
    if result.stderr:
        print(result.stderr, end="", file=stderr)


def run_profile(
    config: ProfileConfig,
    *,
    runner: CommandRunner = default_runner,
    clock: Clock = time.perf_counter,
    stdout: TextIO,
    stderr: TextIO,
) -> int:
    migration_command = build_migration_command()
    collection_command = build_collection_command(config)
    execution_command = build_execution_command(config)

    print_command_start("migration", migration_command, stderr)
    migration_started = clock()
    migration_result = runner(migration_command, True)
    migration_elapsed = clock() - migration_started
    print_command_finish("migration", migration_elapsed, migration_result.returncode, stderr)
    if migration_result.returncode != 0:
        relay_captured_output(migration_result, stdout, stderr)
        print("[profile] collect-only skipped after migration failure.", file=stderr)
        print("[profile] full pytest run skipped after migration failure.", file=stderr)
        return migration_result.returncode

    print_command_start("collect-only", collection_command, stderr)
    collection_started = clock()
    collection_result = runner(collection_command, True)
    collection_elapsed = clock() - collection_started
    print_command_finish("collect-only", collection_elapsed, collection_result.returncode, stderr)
    if collection_result.returncode != 0:
        relay_captured_output(collection_result, stdout, stderr)
        print("[profile] full pytest run skipped after collect-only failure.", file=stderr)
        return collection_result.returncode

    print_command_start("full pytest run", execution_command, stderr)
    execution_started = clock()
    execution_result = runner(execution_command, False)
    execution_elapsed = clock() - execution_started
    print_command_finish("full pytest run", execution_elapsed, execution_result.returncode, stderr)
    print(
        "[profile] summary: "
        f"migration={migration_elapsed:.3f}s "
        f"collect-only={collection_elapsed:.3f}s "
        f"full-pytest-run={execution_elapsed:.3f}s "
        f"combined-wall={migration_elapsed + collection_elapsed + execution_elapsed:.3f}s",
        file=stderr,
    )
    return execution_result.returncode


def main(
    argv: Sequence[str] | None = None,
    *,
    runner: CommandRunner = default_runner,
    clock: Clock = time.perf_counter,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    config = parse_args(argv)
    output_stream = sys.stdout if stdout is None else stdout
    error_stream = sys.stderr if stderr is None else stderr
    return run_profile(
        config,
        runner=runner,
        clock=clock,
        stdout=output_stream,
        stderr=error_stream,
    )


if __name__ == "__main__":
    raise SystemExit(main())
