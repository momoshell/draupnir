"""Thin LibreDWG DWG adapter backed by the ``dwgread`` CLI."""

from __future__ import annotations

import asyncio
import json
import resource
import shutil
import tempfile
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any, cast

from app.ingestion.contracts import (
    AdapterAvailability,
    AdapterDiagnostic,
    AdapterExecutionOptions,
    AdapterResult,
    AdapterSource,
    AdapterStatus,
    AdapterUnavailableError,
    AdapterWarning,
    AvailabilityReason,
    ConfidenceSummary,
    IngestionAdapter,
    InputFamily,
    JSONValue,
    ProbeKind,
    ProbeObservation,
    ProbeStatus,
    ProvenanceRecord,
)
from app.ingestion.registry import evaluate_availability, get_descriptor

_DESCRIPTOR = get_descriptor(InputFamily.DWG)
_BINARY_NAME = "dwgread"
_LICENSE_PROBE_NAME = "libredwg-distribution-review"
_SCHEMA_VERSION = "0.1"
_PROCESS_POLL_INTERVAL_SECONDS = 0.05
_PROCESS_TERMINATE_GRACE_SECONDS = 0.2
_PROCESS_KILL_GRACE_SECONDS = 0.2
_MAX_STDOUT_BYTES = 8 * 1024
_MAX_STDERR_BYTES = 16 * 1024
_MAX_OUTPUT_BYTES = 1 * 1024 * 1024
_PLACEHOLDER_CONFIDENCE_SCORE = 0.4


@dataclass(frozen=True, slots=True)
class _CapturedText:
    text: str
    byte_count: int
    truncated: bool


@dataclass(frozen=True, slots=True)
class _DwgreadRunResult:
    stdout: _CapturedText
    stderr: _CapturedText
    output_size_bytes: int
    output_kind: str
    output_key_count: int | None


@dataclass(frozen=True, slots=True)
class _LiveFileLimit:
    path: Path
    limit_bytes: int
    overflow_message: str


def create_adapter() -> IngestionAdapter:
    """Create the LibreDWG adapter without touching the runtime binary."""

    return LibreDWGAdapter()


class LibreDWGAdapter(IngestionAdapter):
    """DWG adapter that shells out to ``dwgread`` and emits placeholder canonical JSON."""

    descriptor = _DESCRIPTOR

    def probe(self) -> AdapterAvailability:
        """Report binary/license posture without invoking ``dwgread``."""

        started_at = perf_counter()
        binary_path = _binary_path()
        observations = (
            _binary_probe_observation(binary_path),
            _license_probe_observation(),
        )
        details: dict[str, JSONValue] = {
            "binary": _BINARY_NAME,
            "distribution_review_required": True,
            "placeholder_contract_phase": 2,
        }
        if binary_path is not None:
            details["binary_name"] = Path(binary_path).name

        return evaluate_availability(
            self.descriptor,
            observations=observations,
            details=details,
            probe_elapsed_ms=(perf_counter() - started_at) * 1000.0,
        )

    async def ingest(
        self,
        source: AdapterSource,
        options: AdapterExecutionOptions,
    ) -> AdapterResult:
        """Run ``dwgread`` and emit a review-gated placeholder canonical payload."""

        started_at = perf_counter()
        _raise_if_cancelled(options)
        binary_path = _binary_path_for_ingest(self.probe())
        _raise_if_cancelled(options)

        run_result = await _run_dwgread(
            binary_path=binary_path,
            source=source,
            options=options,
        )
        elapsed_ms = (perf_counter() - started_at) * 1000.0

        warning = AdapterWarning(
            code="libredwg.placeholder_canonical",
            message=(
                "LibreDWG Phase 2 emits placeholder canonical output; "
                "DWG entities remain review-gated until native mapping lands."
            ),
            details={
                "output_kind": run_result.output_kind,
                "output_size_bytes": run_result.output_size_bytes,
            },
        )
        diagnostic_details: dict[str, JSONValue] = {
            "command": (_BINARY_NAME, "-O", "JSON", "-o", "<tempdir>/dwgread.json", "<source>"),
            "stdout_bytes": run_result.stdout.byte_count,
            "stdout_truncated": run_result.stdout.truncated,
            "stdout_excerpt": run_result.stdout.text,
            "stderr_bytes": run_result.stderr.byte_count,
            "stderr_truncated": run_result.stderr.truncated,
            "stderr_excerpt": run_result.stderr.text,
            "output_size_bytes": run_result.output_size_bytes,
            "output_kind": run_result.output_kind,
        }
        if run_result.output_key_count is not None:
            diagnostic_details["output_key_count"] = run_result.output_key_count

        return AdapterResult(
            canonical=_build_placeholder_canonical(source=source, run_result=run_result),
            provenance=(
                ProvenanceRecord(
                    stage="extract",
                    adapter_key=self.descriptor.key,
                    source_ref=_source_ref(source),
                    details={
                        "upload_format": source.upload_format.value,
                        "input_family": source.input_family.value,
                        "output_kind": run_result.output_kind,
                    },
                ),
            ),
            confidence=ConfidenceSummary(
                score=_PLACEHOLDER_CONFIDENCE_SCORE,
                review_required=True,
                basis="libredwg_placeholder_wrapper",
            ),
            warnings=(warning,),
            diagnostics=(
                AdapterDiagnostic(
                    code="libredwg.extract",
                    message="Executed dwgread and produced placeholder canonical DWG output.",
                    details=diagnostic_details,
                    elapsed_ms=elapsed_ms,
                ),
            ),
        )


def _binary_probe_observation(binary_path: str | None) -> ProbeObservation:
    if binary_path is None:
        return ProbeObservation(
            kind=ProbeKind.BINARY,
            name=_BINARY_NAME,
            status=ProbeStatus.MISSING,
            detail="LibreDWG dwgread binary is not installed.",
        )

    return ProbeObservation(
        kind=ProbeKind.BINARY,
        name=_BINARY_NAME,
        status=ProbeStatus.AVAILABLE,
        detail="LibreDWG dwgread binary is available for local execution.",
    )


def _license_probe_observation() -> ProbeObservation:
    return ProbeObservation(
        kind=ProbeKind.LICENSE,
        name=_LICENSE_PROBE_NAME,
        status=ProbeStatus.AVAILABLE,
        detail="GPL distribution/on-prem bundling still requires external review.",
    )


def _binary_path() -> str | None:
    return shutil.which(_BINARY_NAME)


def _binary_path_for_ingest(availability: AdapterAvailability) -> str:
    if availability.status is not AdapterStatus.AVAILABLE:
        issue_detail = availability.issues[0].detail if availability.issues else None
        raise AdapterUnavailableError(
            availability.availability_reason or AvailabilityReason.PROBE_FAILED,
            detail=issue_detail,
        )

    binary_path = _binary_path()
    if binary_path is None:
        raise AdapterUnavailableError(
            AvailabilityReason.MISSING_BINARY,
            detail="LibreDWG dwgread binary is not installed.",
        )
    return binary_path


async def _run_dwgread(
    *,
    binary_path: str,
    source: AdapterSource,
    options: AdapterExecutionOptions,
) -> _DwgreadRunResult:
    with tempfile.TemporaryDirectory(prefix="libredwg-") as tempdir:
        temp_path = Path(tempdir)
        output_path = temp_path / "dwgread.json"
        stdout_path = temp_path / "dwgread.stdout.log"
        stderr_path = temp_path / "dwgread.stderr.log"

        process = await _spawn_dwgread_process(
            binary_path=binary_path,
            source=source,
            output_path=output_path,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )
        live_limits = (
            _LiveFileLimit(
                path=stdout_path,
                limit_bytes=_MAX_STDOUT_BYTES,
                overflow_message="LibreDWG stdout exceeded the adapter output limit.",
            ),
            _LiveFileLimit(
                path=stderr_path,
                limit_bytes=_MAX_STDERR_BYTES,
                overflow_message="LibreDWG stderr exceeded the adapter output limit.",
            ),
            _LiveFileLimit(
                path=output_path,
                limit_bytes=_MAX_OUTPUT_BYTES,
                overflow_message="LibreDWG JSON output exceeded the adapter output limit.",
            ),
        )

        try:
            returncode = await _wait_for_process(process, options, live_limits=live_limits)
        except BaseException:
            await asyncio.shield(_terminate_process(process))
            raise

        stdout = _read_text_capture(
            stdout_path,
            limit_bytes=_MAX_STDOUT_BYTES,
            source_path=source.file_path,
            temp_path=temp_path,
        )
        stderr = _read_text_capture(
            stderr_path,
            limit_bytes=_MAX_STDERR_BYTES,
            source_path=source.file_path,
            temp_path=temp_path,
        )

        if returncode != 0:
            raise RuntimeError("LibreDWG dwgread execution failed.")

        if not output_path.exists():
            raise RuntimeError("LibreDWG dwgread did not produce JSON output.")

        output_size_bytes = output_path.stat().st_size
        if output_size_bytes > _MAX_OUTPUT_BYTES:
            raise RuntimeError("LibreDWG JSON output exceeded the adapter output limit.")

        output_payload = _load_output_payload(
            output_path,
            source_path=source.file_path,
            temp_path=temp_path,
        )
        output_kind, output_key_count = _summarize_output_payload(output_payload)

        return _DwgreadRunResult(
            stdout=stdout,
            stderr=stderr,
            output_size_bytes=output_size_bytes,
            output_kind=output_kind,
            output_key_count=output_key_count,
        )


async def _spawn_dwgread_process(
    *,
    binary_path: str,
    source: AdapterSource,
    output_path: Path,
    stdout_path: Path,
    stderr_path: Path,
) -> asyncio.subprocess.Process:
    try:
        with stdout_path.open("wb") as stdout_handle, stderr_path.open("wb") as stderr_handle:
            return await asyncio.create_subprocess_exec(
                binary_path,
                "-O",
                "JSON",
                "-o",
                str(output_path),
                str(source.file_path),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=stdout_handle,
                stderr=stderr_handle,
                preexec_fn=_configure_child_file_size_limit,
            )
    except FileNotFoundError as exc:
        raise AdapterUnavailableError(
            AvailabilityReason.MISSING_BINARY,
            detail="LibreDWG dwgread binary is not installed.",
        ) from exc


async def _wait_for_process(
    process: asyncio.subprocess.Process,
    options: AdapterExecutionOptions,
    *,
    live_limits: tuple[_LiveFileLimit, ...],
) -> int:
    started_at = perf_counter()
    timeout_seconds = options.timeout.seconds if options.timeout is not None else None

    while True:
        _raise_if_cancelled(options)
        _enforce_live_file_limits(live_limits)

        if process.returncode is not None:
            return process.returncode

        poll_timeout = _PROCESS_POLL_INTERVAL_SECONDS
        if timeout_seconds is not None:
            remaining_seconds = timeout_seconds - (perf_counter() - started_at)
            if remaining_seconds <= 0:
                raise TimeoutError("LibreDWG extraction timed out.")
            poll_timeout = min(poll_timeout, remaining_seconds)

        try:
            returncode = await asyncio.wait_for(process.wait(), timeout=poll_timeout)
        except TimeoutError:
            continue

        _enforce_live_file_limits(live_limits)
        return returncode


def _configure_child_file_size_limit() -> None:
    max_limit = max(_MAX_STDOUT_BYTES, _MAX_STDERR_BYTES, _MAX_OUTPUT_BYTES)

    try:
        _, hard_limit = resource.getrlimit(resource.RLIMIT_FSIZE)
    except (OSError, ValueError):
        return

    target_limit = max_limit if hard_limit == resource.RLIM_INFINITY else min(max_limit, hard_limit)

    try:
        resource.setrlimit(resource.RLIMIT_FSIZE, (target_limit, target_limit))
    except (OSError, ValueError):
        return


def _enforce_live_file_limits(live_limits: tuple[_LiveFileLimit, ...]) -> None:
    for live_limit in live_limits:
        try:
            size_bytes = live_limit.path.stat().st_size
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise RuntimeError("LibreDWG process output could not be monitored.") from exc

        if size_bytes > live_limit.limit_bytes:
            raise RuntimeError(live_limit.overflow_message)


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        with suppress(ProcessLookupError):
            await process.wait()
        return

    with suppress(ProcessLookupError):
        process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=_PROCESS_TERMINATE_GRACE_SECONDS)
        return
    except TimeoutError:
        pass

    with suppress(ProcessLookupError):
        process.kill()
    with suppress(ProcessLookupError, asyncio.TimeoutError):
        await asyncio.wait_for(process.wait(), timeout=_PROCESS_KILL_GRACE_SECONDS)


def _load_output_payload(
    output_path: Path,
    *,
    source_path: Path,
    temp_path: Path,
) -> Any:
    try:
        output_bytes = output_path.read_bytes()
        output_text = _sanitize_text(
            output_bytes.decode("utf-8", errors="replace"),
            source_path=source_path,
            temp_path=temp_path,
        )
        return json.loads(output_text)
    except OSError as exc:
        raise RuntimeError("LibreDWG JSON output could not be read.") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError("LibreDWG JSON output was invalid.") from exc


def _summarize_output_payload(payload: Any) -> tuple[str, int | None]:
    if isinstance(payload, Mapping):
        return "object", len(payload)
    if isinstance(payload, list):
        return "array", len(payload)
    if payload is None:
        return "null", None
    if isinstance(payload, bool):
        return "boolean", None
    if isinstance(payload, (int, float)):
        return "number", None
    if isinstance(payload, str):
        return "string", None
    return type(payload).__name__, None


def _read_text_capture(
    capture_path: Path,
    *,
    limit_bytes: int,
    source_path: Path,
    temp_path: Path,
) -> _CapturedText:
    if not capture_path.exists():
        return _CapturedText(text="", byte_count=0, truncated=False)

    byte_count = capture_path.stat().st_size
    with capture_path.open("rb") as capture_handle:
        raw = capture_handle.read(limit_bytes)

    return _CapturedText(
        text=_sanitize_text(
            raw.decode("utf-8", errors="replace"),
            source_path=source_path,
            temp_path=temp_path,
        ),
        byte_count=byte_count,
        truncated=byte_count > limit_bytes,
    )


def _sanitize_text(text: str, *, source_path: Path, temp_path: Path) -> str:
    sanitized = text.replace(str(source_path), "<source>")
    sanitized = sanitized.replace(str(temp_path), "<tempdir>")
    return "".join(
        character if character.isprintable() or character in {"\n", "\t"} else "?"
        for character in sanitized
    ).strip()


def _build_placeholder_canonical(
    *,
    source: AdapterSource,
    run_result: _DwgreadRunResult,
) -> dict[str, JSONValue]:
    metadata: dict[str, JSONValue] = {
        "source_format": source.upload_format.value,
        "adapter_mode": "placeholder",
        "empty_entities_reason": "placeholder_canonical_no_entity_mapping",
        "dwgread": {
            "output_kind": run_result.output_kind,
            "output_size_bytes": run_result.output_size_bytes,
            "stdout_bytes": run_result.stdout.byte_count,
            "stderr_bytes": run_result.stderr.byte_count,
        },
    }
    if run_result.output_key_count is not None:
        dwgread_metadata = cast(dict[str, JSONValue], metadata["dwgread"])
        dwgread_metadata["output_key_count"] = run_result.output_key_count

    return {
        "schema_version": _SCHEMA_VERSION,
        "canonical_entity_schema_version": _SCHEMA_VERSION,
        "units": {"normalized": "unknown"},
        "coordinate_system": {
            "name": "local",
            "type": "cartesian",
            "source": "libredwg_placeholder_wrapper",
        },
        "layouts": ({"name": "Model"},),
        "layers": (),
        "blocks": (),
        "entities": (),
        "xrefs": (),
        "metadata": metadata,
    }


def _source_ref(source: AdapterSource) -> str:
    candidate = (
        source.original_name.replace("\\", "/").split("/")[-1].strip()
        if source.original_name
        else ""
    )
    return f"originals/{candidate or source.file_path.name}"


def _raise_if_cancelled(options: AdapterExecutionOptions) -> None:
    if options.cancellation is not None and options.cancellation.is_cancelled():
        raise asyncio.CancelledError()


__all__ = [
    "LibreDWGAdapter",
    "create_adapter",
]
