from __future__ import annotations

import importlib.util
import io
import sys
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType


def load_profile_integration_lane_module() -> ModuleType:
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "profile_integration_lane.py"
    spec = importlib.util.spec_from_file_location("profile_integration_lane", module_path)
    if spec is None or spec.loader is None:
        raise AssertionError("failed to load profile_integration_lane module")
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault(spec.name, module)
    spec.loader.exec_module(module)
    return module


profile_integration_lane = load_profile_integration_lane_module()


class RunnerStub:
    def __init__(self, results: Sequence[object]) -> None:
        self._results = list(results)
        self.calls: list[tuple[tuple[str, ...], bool]] = []

    def __call__(self, command: Sequence[str], capture_output: bool) -> object:
        self.calls.append((tuple(command), capture_output))
        if not self._results:
            raise AssertionError("unexpected runner call")
        return self._results.pop(0)


class ClockStub:
    def __init__(self, values: Sequence[float]) -> None:
        self._values = list(values)

    def __call__(self) -> float:
        if not self._values:
            raise AssertionError("unexpected clock call")
        return self._values.pop(0)


def test_parse_args_supports_marker_durations_and_passthrough() -> None:
    config = profile_integration_lane.parse_args(
        [
            "--marker",
            "integration and db_api and not compose_smoke",
            "--durations",
            "25",
            "--durations-min",
            "0.5",
            "--",
            "--maxfail=1",
            "tests/test_jobs_api.py",
        ]
    )

    assert config.marker == "integration and db_api and not compose_smoke"
    assert config.durations == 25
    assert config.durations_min == 0.5
    assert config.pytest_args == ("--maxfail=1", "tests/test_jobs_api.py")


def test_main_profiles_collect_only_and_full_pytest_run_with_summary_output() -> None:
    runner = RunnerStub(
        [
            profile_integration_lane.CommandResult(returncode=0),
            profile_integration_lane.CommandResult(returncode=0),
            profile_integration_lane.CommandResult(returncode=0),
        ]
    )
    clock = ClockStub([1.0, 1.5, 10.0, 10.25, 20.0, 21.75])
    stdout = io.StringIO()
    stderr = io.StringIO()

    exit_code = profile_integration_lane.main(
        [
            "--marker",
            "integration and db_worker and not compose_smoke",
            "--durations",
            "50",
            "--durations-min",
            "0.2",
            "--maxfail=1",
        ],
        runner=runner,
        clock=clock,
        stdout=stdout,
        stderr=stderr,
    )

    assert exit_code == 0
    assert runner.calls == [
        (
            (
                "uv",
                "run",
                "alembic",
                "upgrade",
                "head",
            ),
            True,
        ),
        (
            (
                "uv",
                "run",
                "pytest",
                "-m",
                "integration and db_worker and not compose_smoke",
                "--collect-only",
                "-q",
                "--maxfail=1",
            ),
            True,
        ),
        (
            (
                "uv",
                "run",
                "pytest",
                "-m",
                "integration and db_worker and not compose_smoke",
                "--durations=50",
                "--durations-min=0.2",
                "--maxfail=1",
            ),
            False,
        ),
    ]
    assert stdout.getvalue() == ""
    error_output = stderr.getvalue()
    assert "[profile] migration command:" in error_output
    assert "[profile] collect-only command:" in error_output
    assert "[profile] full pytest run command:" in error_output
    assert "[profile] migration elapsed: 0.500s (exit 0)" in error_output
    assert "[profile] collect-only elapsed: 0.250s (exit 0)" in error_output
    assert "[profile] full pytest run elapsed: 1.750s (exit 0)" in error_output
    assert (
        "[profile] summary: migration=0.500s collect-only=0.250s "
        "full-pytest-run=1.750s combined-wall=2.500s" in error_output
    )


def test_main_returns_migration_failure_and_skips_collect_only_and_execution() -> None:
    runner = RunnerStub(
        [
            profile_integration_lane.CommandResult(
                returncode=1,
                stdout="migration stdout\n",
                stderr="migration stderr\n",
            )
        ]
    )
    clock = ClockStub([1.0, 1.2])
    stdout = io.StringIO()
    stderr = io.StringIO()

    exit_code = profile_integration_lane.main(
        ["--marker", "integration and db_api and not compose_smoke"],
        runner=runner,
        clock=clock,
        stdout=stdout,
        stderr=stderr,
    )

    assert exit_code == 1
    assert runner.calls == [
        (
            (
                "uv",
                "run",
                "alembic",
                "upgrade",
                "head",
            ),
            True,
        )
    ]
    assert stdout.getvalue() == "migration stdout\n"
    error_output = stderr.getvalue()
    assert "migration stderr\n" in error_output
    assert "[profile] collect-only skipped after migration failure." in error_output
    assert "[profile] full pytest run skipped after migration failure." in error_output


def test_main_returns_collection_failure_and_skips_execution() -> None:
    runner = RunnerStub(
        [
            profile_integration_lane.CommandResult(returncode=0),
            profile_integration_lane.CommandResult(
                returncode=2,
                stdout="collection stdout\n",
                stderr="collection stderr\n",
            ),
        ]
    )
    clock = ClockStub([1.0, 1.2, 2.0, 2.1])
    stdout = io.StringIO()
    stderr = io.StringIO()

    exit_code = profile_integration_lane.main(
        ["--marker", "integration and db_api and not compose_smoke"],
        runner=runner,
        clock=clock,
        stdout=stdout,
        stderr=stderr,
    )

    assert exit_code == 2
    assert runner.calls == [
        (
            (
                "uv",
                "run",
                "alembic",
                "upgrade",
                "head",
            ),
            True,
        ),
        (
            (
                "uv",
                "run",
                "pytest",
                "-m",
                "integration and db_api and not compose_smoke",
                "--collect-only",
                "-q",
            ),
            True,
        ),
    ]
    assert stdout.getvalue() == "collection stdout\n"
    error_output = stderr.getvalue()
    assert "[profile] migration elapsed: 0.200s (exit 0)" in error_output
    assert "collection stderr\n" in error_output
    assert "[profile] full pytest run skipped after collect-only failure." in error_output
