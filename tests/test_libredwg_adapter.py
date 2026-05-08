"""Contract tests for the thin LibreDWG DWG adapter."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, cast

import pytest

from app.ingestion.adapters import libredwg as adapter_module
from app.ingestion.contracts import (
    AdapterExecutionOptions,
    AdapterSource,
    AdapterStatus,
    AdapterTimeout,
    AdapterUnavailableError,
    AvailabilityReason,
    InputFamily,
    UploadFormat,
)
from tests.ingestion_contract_harness import (
    ContractFinalizationExpectation,
    build_contract_source,
    exercise_adapter_contract,
)

_FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "dwg" / "libredwg-wrapper-smoke.txt"
)


class _FakeProcess:
    def __init__(
        self,
        *,
        complete_with: int | None = None,
        complete_when: asyncio.Event | None = None,
    ) -> None:
        self.returncode: int | None = None
        self.terminate_calls = 0
        self.kill_calls = 0
        self._complete_with = complete_with
        self._complete_when = complete_when
        self._killed = asyncio.Event()

    async def wait(self) -> int:
        if self.returncode is not None:
            return self.returncode
        if self._complete_with is not None and self._complete_when is None:
            self.returncode = self._complete_with
            return self.returncode

        wait_tasks = [asyncio.create_task(self._killed.wait())]
        complete_task: asyncio.Task[bool] | None = None
        if self._complete_when is not None:
            complete_task = asyncio.create_task(self._complete_when.wait())
            wait_tasks.append(complete_task)

        done, pending = await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)
        for pending_task in pending:
            pending_task.cancel()

        if (
            complete_task is not None
            and complete_task in done
            and self.returncode is None
            and self._complete_with is not None
        ):
            self.returncode = self._complete_with

        return cast(int, self.returncode)

    def terminate(self) -> None:
        self.terminate_calls += 1

    def kill(self) -> None:
        self.kill_calls += 1
        self.returncode = -9
        self._killed.set()


class _DeferredCancellation:
    def __init__(self, *, cancel_after_calls: int) -> None:
        self.calls = 0
        self._cancel_after_calls = cancel_after_calls

    def is_cancelled(self) -> bool:
        self.calls += 1
        return self.calls >= self._cancel_after_calls


def _build_source() -> AdapterSource:
    return build_contract_source(
        file_path=_FIXTURE_PATH,
        upload_format=UploadFormat.DWG,
        input_family=InputFamily.DWG,
        media_type="image/vnd.dwg",
        original_name="fixtures/libredwg-wrapper-smoke.dwg",
    )


def _install_fake_subprocess(
    monkeypatch: pytest.MonkeyPatch,
    *,
    process: _FakeProcess,
    output_text: str = '{"drawing": {"version": "R2018"}}',
    stdout_text: str = "",
    stderr_text: str = "SUCCESS\n",
    write_output: bool = True,
    format_output: bool = False,
    on_spawn: Callable[[Path, Path, Path], None] | None = None,
) -> dict[str, tuple[str, ...]]:
    seen: dict[str, tuple[str, ...]] = {}

    async def _fake_create_subprocess_exec(*command: str, **kwargs: Any) -> _FakeProcess:
        output_flag_index = command.index("-o") + 1
        output_path = Path(command[output_flag_index])
        stdout_path = Path(cast(str, kwargs["stdout"].name))
        stderr_path = Path(cast(str, kwargs["stderr"].name))
        tempdir = str(output_path.parent)
        source_path = command[output_flag_index + 1]
        if write_output:
            rendered_output = (
                output_text.format(source=source_path, tempdir=tempdir, output=output_path)
                if format_output
                else output_text
            )
            output_path.write_text(rendered_output, encoding="utf-8")

        stdout_handle = kwargs["stdout"]
        stderr_handle = kwargs["stderr"]
        stdout_payload = stdout_text.format(
            source=source_path,
            tempdir=tempdir,
            output=output_path,
        ).encode("utf-8")
        stderr_payload = stderr_text.format(
            source=source_path,
            tempdir=tempdir,
            output=output_path,
        ).encode("utf-8")
        stdout_handle.write(
            stdout_payload
        )
        stdout_handle.flush()
        stderr_handle.write(stderr_payload)
        stderr_handle.flush()

        if on_spawn is not None:
            on_spawn(output_path, stdout_path, stderr_path)

        seen["command"] = command
        return process

    monkeypatch.setattr(
        "app.ingestion.adapters.libredwg.asyncio.create_subprocess_exec",
        _fake_create_subprocess_exec,
    )
    return seen


def _schedule_live_overflow_write(
    *,
    overflow_path_kind: str,
    payload_text: str,
) -> Callable[[Path, Path, Path], None]:
    background_tasks: list[asyncio.Task[None]] = []

    def _on_spawn(output_path: Path, stdout_path: Path, stderr_path: Path) -> None:
        target_path = {
            "output": output_path,
            "stdout": stdout_path,
            "stderr": stderr_path,
        }[overflow_path_kind]

        async def _writer() -> None:
            await asyncio.sleep(adapter_module._PROCESS_POLL_INTERVAL_SECONDS)
            target_path.write_text(payload_text, encoding="utf-8")

        background_tasks.append(asyncio.create_task(_writer()))

    return _on_spawn


def test_probe_reports_missing_binary_with_license_review_posture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: None)

    availability = adapter_module.create_adapter().probe()

    assert availability.status is AdapterStatus.UNAVAILABLE
    assert availability.availability_reason is AvailabilityReason.MISSING_BINARY
    assert [(item.kind.value, item.name, item.status.value) for item in availability.observed] == [
        ("binary", "dwgread", "missing"),
        ("license", "libredwg-distribution-review", "available"),
    ]


@pytest.mark.asyncio
async def test_ingest_preflights_missing_binary_as_shared_unavailable_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: None)

    with pytest.raises(AdapterUnavailableError) as exc_info:
        await adapter_module.create_adapter().ingest(
            _build_source(),
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.1)),
        )

    assert exc_info.value.availability_reason is AvailabilityReason.MISSING_BINARY


@pytest.mark.asyncio
async def test_libredwg_adapter_emits_review_gated_placeholder_canonical_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _build_source()
    process = _FakeProcess(complete_with=0)
    seen = _install_fake_subprocess(
        monkeypatch,
        process=process,
        stdout_text="reading {source}\n",
        stderr_text="wrote {source} to {output} from {tempdir}\n",
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    adapter = adapter_module.create_adapter()
    payload = await exercise_adapter_contract(
        adapter,
        source=source,
        input_family=InputFamily.DWG,
        adapter_key="libredwg",
        expectation=ContractFinalizationExpectation(
            validation_status="needs_review",
            review_state="review_required",
            quantity_gate="review_gated",
            warning_codes=("libredwg.placeholder_canonical",),
            diagnostic_codes=("libredwg.extract",),
        ),
    )

    command = seen["command"]
    output_path = Path(command[4])
    assert command[:4] == ("/opt/homebrew/bin/dwgread", "-O", "JSON", "-o")
    assert output_path.parent != _FIXTURE_PATH.parent
    assert payload.canonical_json["entities"] == []
    assert payload.canonical_json["metadata"]["adapter_mode"] == "placeholder"

    result = await adapter.ingest(
        source,
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )
    second_output_path = Path(seen["command"][4])
    diagnostic_details = cast(Mapping[str, object], result.diagnostics[0].details)
    stdout_excerpt = cast(str, diagnostic_details["stdout_excerpt"])
    stderr_excerpt = cast(str, diagnostic_details["stderr_excerpt"])
    assert str(source.file_path) not in stdout_excerpt
    assert str(source.file_path) not in stderr_excerpt
    assert str(second_output_path.parent) not in stderr_excerpt
    assert "<source>" in stdout_excerpt
    assert "<tempdir>" in stderr_excerpt


@pytest.mark.asyncio
async def test_libredwg_adapter_rejects_oversized_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oversized_output = json.dumps({"x": "y" * adapter_module._MAX_OUTPUT_BYTES})
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(monkeypatch, process=process, output_text=oversized_output)
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    with pytest.raises(RuntimeError, match="output limit"):
        await adapter_module.create_adapter().ingest(
            _build_source(),
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overflow_path_kind", "payload_text", "expected_message"),
    [
        ("stdout", "A" * (adapter_module._MAX_STDOUT_BYTES + 1), "stdout exceeded"),
        ("stderr", "B" * (adapter_module._MAX_STDERR_BYTES + 1), "stderr exceeded"),
        (
            "output",
            json.dumps({"x": "y" * adapter_module._MAX_OUTPUT_BYTES}),
            "JSON output exceeded",
        ),
    ],
)
async def test_libredwg_adapter_kills_process_on_live_output_overflow(
    monkeypatch: pytest.MonkeyPatch,
    overflow_path_kind: str,
    payload_text: str,
    expected_message: str,
) -> None:
    process = _FakeProcess(complete_with=0, complete_when=asyncio.Event())
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        write_output=overflow_path_kind != "output",
        on_spawn=_schedule_live_overflow_write(
            overflow_path_kind=overflow_path_kind,
            payload_text=payload_text,
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")
    monkeypatch.setattr(adapter_module, "_PROCESS_POLL_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(adapter_module, "_PROCESS_TERMINATE_GRACE_SECONDS", 0.01)
    monkeypatch.setattr(adapter_module, "_PROCESS_KILL_GRACE_SECONDS", 0.01)

    with pytest.raises(RuntimeError, match=expected_message):
        await adapter_module.create_adapter().ingest(
            _build_source(),
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
        )

    assert process.terminate_calls == 1
    assert process.kill_calls == 1


@pytest.mark.asyncio
async def test_libredwg_adapter_terminates_and_kills_process_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess()
    _install_fake_subprocess(monkeypatch, process=process)
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")
    monkeypatch.setattr(adapter_module, "_PROCESS_POLL_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(adapter_module, "_PROCESS_TERMINATE_GRACE_SECONDS", 0.01)
    monkeypatch.setattr(adapter_module, "_PROCESS_KILL_GRACE_SECONDS", 0.01)

    with pytest.raises(TimeoutError):
        await adapter_module.create_adapter().ingest(
            _build_source(),
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.02)),
        )

    assert process.terminate_calls == 1
    assert process.kill_calls == 1


@pytest.mark.asyncio
async def test_libredwg_adapter_handles_task_cancellation_while_process_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess()
    seen = _install_fake_subprocess(monkeypatch, process=process)
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")
    monkeypatch.setattr(adapter_module, "_PROCESS_POLL_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(adapter_module, "_PROCESS_TERMINATE_GRACE_SECONDS", 0.01)
    monkeypatch.setattr(adapter_module, "_PROCESS_KILL_GRACE_SECONDS", 0.01)

    task = asyncio.create_task(
        adapter_module.create_adapter().ingest(
            _build_source(),
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
        )
    )
    while "command" not in seen:
        await asyncio.sleep(0)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert process.terminate_calls == 1
    assert process.kill_calls == 1


@pytest.mark.asyncio
async def test_libredwg_adapter_reports_sanitized_stdio_excerpts_within_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    stdout_template = "{source} {tempdir}\n" + ("A" * 1024)
    stderr_template = "{source} {tempdir}\n" + ("B" * 1024)
    seen = _install_fake_subprocess(
        monkeypatch,
        process=process,
        stdout_text=stdout_template,
        stderr_text=stderr_template,
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    source = _build_source()
    result = await adapter_module.create_adapter().ingest(
        source,
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    command = seen["command"]
    output_path = Path(command[4])
    source_path = str(source.file_path)
    tempdir = str(output_path.parent)
    expected_stdout_bytes = len(
        stdout_template.format(source=source_path, tempdir=tempdir, output=output_path).encode(
            "utf-8"
        )
    )
    expected_stderr_bytes = len(
        stderr_template.format(source=source_path, tempdir=tempdir, output=output_path).encode(
            "utf-8"
        )
    )

    diagnostic_details = cast(Mapping[str, object], result.diagnostics[0].details)
    stdout_excerpt = cast(str, diagnostic_details["stdout_excerpt"])
    stderr_excerpt = cast(str, diagnostic_details["stderr_excerpt"])

    assert diagnostic_details["stdout_bytes"] == expected_stdout_bytes
    assert diagnostic_details["stderr_bytes"] == expected_stderr_bytes
    assert diagnostic_details["stdout_truncated"] is False
    assert diagnostic_details["stderr_truncated"] is False
    assert "<source>" in stdout_excerpt
    assert "<tempdir>" in stdout_excerpt
    assert "<source>" in stderr_excerpt
    assert "<tempdir>" in stderr_excerpt
    assert source_path not in stdout_excerpt
    assert source_path not in stderr_excerpt
    assert tempdir not in stdout_excerpt
    assert tempdir not in stderr_excerpt


@pytest.mark.asyncio
async def test_libredwg_adapter_nonzero_exit_failure_is_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _build_source()
    process = _FakeProcess(complete_with=3)
    seen = _install_fake_subprocess(
        monkeypatch,
        process=process,
        stdout_text="failed for {source} in {tempdir}\n",
        stderr_text="error while writing {output}\n",
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    with pytest.raises(RuntimeError, match="execution failed") as exc_info:
        await adapter_module.create_adapter().ingest(
            source,
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
        )

    output_path = Path(seen["command"][4])
    tempdir = str(output_path.parent)
    message = str(exc_info.value)
    assert str(source.file_path) not in message
    assert tempdir not in message


@pytest.mark.asyncio
async def test_libredwg_adapter_missing_output_failure_is_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _build_source()
    process = _FakeProcess(complete_with=0)
    seen = _install_fake_subprocess(
        monkeypatch,
        process=process,
        write_output=False,
        stdout_text="source={source} tempdir={tempdir}\n",
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    with pytest.raises(RuntimeError, match="did not produce JSON output") as exc_info:
        await adapter_module.create_adapter().ingest(
            source,
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
        )

    tempdir = str(Path(seen["command"][4]).parent)
    message = str(exc_info.value)
    assert str(source.file_path) not in message
    assert tempdir not in message


@pytest.mark.asyncio
async def test_libredwg_adapter_invalid_json_failure_sanitizes_decode_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _build_source()
    process = _FakeProcess(complete_with=0)
    seen = _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text='{{"source":"{source}","tempdir":"{tempdir}"',
        format_output=True,
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    with pytest.raises(RuntimeError, match="output was invalid") as exc_info:
        await adapter_module.create_adapter().ingest(
            source,
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
        )

    tempdir = str(Path(seen["command"][4]).parent)
    message = str(exc_info.value)
    assert str(source.file_path) not in message
    assert tempdir not in message

    cause = exc_info.value.__cause__
    assert isinstance(cause, json.JSONDecodeError)
    assert str(source.file_path) not in cause.doc
    assert tempdir not in cause.doc
    assert "<source>" in cause.doc
    assert "<tempdir>" in cause.doc


@pytest.mark.asyncio
async def test_libredwg_adapter_terminates_and_kills_process_on_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess()
    cancellation = _DeferredCancellation(cancel_after_calls=4)
    _install_fake_subprocess(monkeypatch, process=process)
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")
    monkeypatch.setattr(adapter_module, "_PROCESS_POLL_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(adapter_module, "_PROCESS_TERMINATE_GRACE_SECONDS", 0.01)
    monkeypatch.setattr(adapter_module, "_PROCESS_KILL_GRACE_SECONDS", 0.01)

    with pytest.raises(asyncio.CancelledError):
        await adapter_module.create_adapter().ingest(
            _build_source(),
            AdapterExecutionOptions(
                timeout=AdapterTimeout(seconds=0.5),
                cancellation=cancellation,
            ),
        )

    assert process.terminate_calls == 1
    assert process.kill_calls == 1
