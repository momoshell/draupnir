"""Thin LibreDWG DWG adapter backed by the ``dwgread`` CLI."""

from __future__ import annotations

import asyncio
import json
import math
import resource
import shutil
import tempfile
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from time import perf_counter
from typing import Any, cast

from app.core.config import settings
from app.ingestion.canonical.entity_provenance import build_entity_provenance
from app.ingestion.canonical.hashing import (
    sha256_canonical_json_hex,
    sha256_canonical_json_prefixed,
    sha256_text_hex,
)
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
_MAX_OUTPUT_BYTES = 32 * 1024 * 1024
_MAX_HATCH_BOUNDARY_COMPONENTS = 512
_PLACEHOLDER_CONFIDENCE_SCORE = 0.4
_LINE_ENTITY_CONFIDENCE_SCORE = 0.72
_TEXT_ENTITY_CONFIDENCE_SCORE = 0.72
_MIXED_ENTITY_CONFIDENCE_SCORE = 0.5
_BLOCK_REFERENCE_CONFIDENCE_SCORE = 0.45
_UNKNOWN_ENTITY_CONFIDENCE_SCORE = 0.2
# INSERT scale components are treated as uniform when they agree within this relative
# tolerance; anything outside it is a non-uniform transform we keep review-gated.
_INSERT_SCALE_UNIFORM_TOLERANCE = 1e-9
_DEFAULT_LAYOUT_NAME = "Model"
_WRAPPER_KEYS = ("object", "entity", "record", "data", "payload", "value")
_SUPPORTED_LWPOLYLINE_FLAG_MASK = 0b1
_SUPPORTED_ARC_ANGLE_UNIT_TOKENS = frozenset({"deg", "degree", "degrees"})
_SUPPORTED_ARC_DIRECTION_TOKENS = frozenset({"ccw", "counterclockwise"})
_UNSUPPORTED_ARC_DIRECTION_TOKENS = frozenset({"cw", "clockwise"})
_NON_DRAWABLE_OBJECT_TYPES = frozenset(
    {
        "APPID",
        "BLOCK",
        "BLOCK_CONTROL",
        "BLOCK_HEADER",
        "ENDBLK",
        "CLASS",
        "DBPLACEHOLDER",
        "DICTIONARY",
        "DICTIONARYVAR",
        "DIMSTYLE",
        "GROUP",
        "LAYOUT",
        "LAYER",
        "LAYER_INDEX",
        "LTYPE",
        "MATERIAL",
        "MLINESTYLE",
        "OBJECT_PTR",
        "PLOTSETTINGS",
        "STYLE",
        "TABLESTYLE",
        "UCS",
        "VIEW",
        "VISUALSTYLE",
        "VPORT",
        "XDICTIONARY",
        "XRECORD",
    }
)

_JSONDict = dict[str, JSONValue]

# Curated, confidence-gated subset of AutoCAD INSUNITS codes that map to an unambiguous,
# well-defined length scale to meters. Codes outside this table (0/unitless, exotic or rare
# codes, missing or non-integral values) stay review-gated as ``unknown``. Easy to extend
# later (e.g. US survey foot, code 21) once those conversions are deliberately reviewed.
_INSUNITS_METER_SCALE: dict[int, float] = {
    1: 0.0254,  # inch
    2: 0.3048,  # foot
    3: 1609.344,  # mile
    4: 0.001,  # millimeter
    5: 0.01,  # centimeter
    6: 1.0,  # meter
    7: 1000.0,  # kilometer
    10: 0.9144,  # yard
    14: 0.1,  # decimeter
    15: 10.0,  # decameter
    16: 100.0,  # hectometer
}


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
    output_payload: Any


class _OutputLimitExceededError(RuntimeError):
    """Raised when dwgread output grows beyond the configured limit."""

    def __init__(
        self,
        *,
        message: str,
        output_kind: str,
        max_output_bytes: int,
        output_size_bytes: int,
        stage: str = "execute",
    ) -> None:
        super().__init__(message)
        self.output_kind = output_kind
        self.max_output_bytes = max_output_bytes
        self.output_size_bytes = output_size_bytes
        self.reason = "output_cap_exceeded"
        self.stage = stage
        self.adapter_key = "libredwg"


@dataclass(frozen=True, slots=True)
class _CanonicalBuildResult:
    canonical: _JSONDict
    warnings: tuple[AdapterWarning, ...]
    confidence: ConfidenceSummary


@dataclass(frozen=True, slots=True)
class _UnitsResolution:
    """Resolved drawing units for a single ingest, threaded through every builder."""

    payload: _JSONDict
    scale: float
    confirmed: bool


@dataclass(frozen=True, slots=True)
class _LiveFileLimit:
    path: Path
    limit_bytes: int
    overflow_message: str
    output_kind: str | None = None


@dataclass(frozen=True, slots=True)
class _HatchBuildResult:
    entity: _JSONDict | None
    reason: str | None = None
    malformed: bool = False


@dataclass(frozen=True, slots=True)
class _HatchVerticesResult:
    vertices: tuple[tuple[float, float, float], ...] | None
    reason: str | None = None
    malformed: bool = False


@dataclass(frozen=True, slots=True)
class _InsertBuildResult:
    """Outcome of mapping a single INSERT (block-reference) record.

    ``entity`` is ``None`` only when the record is malformed (no usable insertion point) and
    should fall back to the generic unknown path. A captured-but-review-gated block reference —
    including one whose transform is unsupported — still returns an entity, with
    ``transform_supported`` reflecting whether the captured transform is within the safe subset.
    """

    entity: _JSONDict | None
    transform_supported: bool = False
    malformed: bool = False
    reason: str | None = None


def _configured_max_output_bytes() -> int:
    return settings.libredwg_max_output_mb * 1024 * 1024


def create_adapter() -> IngestionAdapter:
    """Create the LibreDWG adapter without touching the runtime binary."""

    return LibreDWGAdapter()


class LibreDWGAdapter(IngestionAdapter):
    """DWG adapter that shells out to ``dwgread`` and maps minimal canonical JSON."""

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
        """Run ``dwgread`` and emit a review-gated canonical payload."""

        started_at = perf_counter()
        _raise_if_cancelled(options)
        binary_path = _binary_path_for_ingest(self.probe())
        _raise_if_cancelled(options)

        run_result = await _run_dwgread(
            binary_path=binary_path,
            source=source,
            options=options,
        )
        canonical_result = _build_canonical_output(source=source, run_result=run_result)
        elapsed_ms = (perf_counter() - started_at) * 1000.0

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
        entity_counts = _diagnostic_entity_counts(canonical_result.canonical)
        if entity_counts is not None:
            diagnostic_details["entity_counts"] = entity_counts
        diagnostic_details["mapping_confidence"] = {
            "score": canonical_result.confidence.score,
            "review_required": canonical_result.confidence.review_required,
            "basis": canonical_result.confidence.basis,
        }
        canonical_metadata = canonical_result.canonical.get("metadata")
        if isinstance(canonical_metadata, Mapping):
            units_summary = canonical_metadata.get("units")
            if isinstance(units_summary, Mapping):
                diagnostic_details["units"] = dict(units_summary)

        return AdapterResult(
            canonical=canonical_result.canonical,
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
            confidence=canonical_result.confidence,
            warnings=canonical_result.warnings,
            diagnostics=(
                AdapterDiagnostic(
                    code="libredwg.extract",
                    message="Executed dwgread and mapped LibreDWG JSON into canonical DWG output.",
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
    max_output_bytes = _configured_max_output_bytes()

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
                limit_bytes=max_output_bytes,
                overflow_message="LibreDWG JSON output exceeded the adapter output limit.",
                output_kind="json",
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

        output_size_bytes = output_path.stat().st_size if output_path.exists() else None
        if output_size_bytes is not None and output_size_bytes >= max_output_bytes:
            raise _OutputLimitExceededError(
                message="LibreDWG JSON output exceeded the adapter output limit.",
                output_kind="json",
                max_output_bytes=max_output_bytes,
                output_size_bytes=output_size_bytes,
            )

        if output_size_bytes is None:
            raise RuntimeError("LibreDWG dwgread did not produce JSON output.")

        if returncode != 0:
            raise RuntimeError("LibreDWG dwgread execution failed.")

        if output_size_bytes >= max_output_bytes:
            raise _OutputLimitExceededError(
                message="LibreDWG JSON output exceeded the adapter output limit.",
                output_kind="json",
                max_output_bytes=max_output_bytes,
                output_size_bytes=output_size_bytes,
            )

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
            output_payload=output_payload,
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
    max_limit = max(_MAX_STDOUT_BYTES, _MAX_STDERR_BYTES, _configured_max_output_bytes())

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
            if live_limit.output_kind is None:
                raise RuntimeError(live_limit.overflow_message)
            raise _OutputLimitExceededError(
                message=live_limit.overflow_message,
                output_kind=live_limit.output_kind,
                max_output_bytes=live_limit.limit_bytes,
                output_size_bytes=size_bytes,
            )


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


def _build_canonical_output(
    *,
    source: AdapterSource,
    run_result: _DwgreadRunResult,
) -> _CanonicalBuildResult:
    entities: list[JSONValue] = []
    layouts_seen: set[str] = {_DEFAULT_LAYOUT_NAME}
    layers_seen: set[str] = set()
    blocks_seen: set[str] = set()
    supported_geometry_count = 0
    supported_line_count = 0
    supported_text_count = 0
    unsupported_drawable_count = 0
    malformed_drawable_count = 0
    unsupported_hatch_count = 0
    malformed_hatch_count = 0
    block_reference_count = 0
    unsupported_transform_count = 0
    malformed_insert_count = 0
    unsupported_types: set[str] = set()
    malformed_handles: set[str] = set()
    unsupported_hatch_handles: set[str] = set()
    malformed_hatch_handles: set[str] = set()
    block_reference_handles: set[str] = set()
    unsupported_transform_handles: set[str] = set()
    unsupported_transform_reasons: set[str] = set()
    malformed_insert_handles: set[str] = set()
    drawable_candidate_count = 0
    units = _resolve_units(run_result.output_payload)
    records = _iter_object_records(run_result.output_payload)
    block_header_names = _build_block_header_name_map(records)

    def _record_entity(entity: _JSONDict) -> None:
        entities.append(entity)
        _collect_observed_collection_refs(
            entity,
            layouts_seen=layouts_seen,
            layers_seen=layers_seen,
            blocks_seen=blocks_seen,
        )

    def _record_unknown(record: Mapping[str, Any], *, reason: str) -> str | None:
        unknown_entity = _build_unknown_entity(record, reason=reason, units=units)
        _record_entity(unknown_entity)
        return cast(str | None, unknown_entity.get("source_entity_handle"))

    for record in records:
        record_type = _extract_record_type(record)
        if record_type is None:
            continue
        if record_type in _NON_DRAWABLE_OBJECT_TYPES:
            continue

        drawable_candidate_count += 1
        if record_type == "LINE":
            line_entity = _build_line_entity(record, units=units)
            if line_entity is not None:
                _record_entity(line_entity)
                supported_geometry_count += 1
                supported_line_count += 1
                continue

            malformed_drawable_count += 1
            handle = _record_unknown(record, reason="malformed_line_geometry")
            if handle is not None:
                malformed_handles.add(handle)
            continue

        if record_type == "CIRCLE":
            circle_entity = _build_circle_entity(record, units=units)
            if circle_entity is not None:
                _record_entity(circle_entity)
                supported_geometry_count += 1
                continue

            malformed_drawable_count += 1
            handle = _record_unknown(record, reason="malformed_circle_geometry")
            if handle is not None:
                malformed_handles.add(handle)
            continue

        if record_type == "ARC":
            arc_entity = _build_arc_entity(record, units=units)
            if arc_entity is not None:
                _record_entity(arc_entity)
                supported_geometry_count += 1
                continue

            malformed_drawable_count += 1
            handle = _record_unknown(record, reason="malformed_arc_geometry")
            if handle is not None:
                malformed_handles.add(handle)
            continue

        if record_type == "LWPOLYLINE":
            polyline_entity = _build_lwpolyline_entity(record, units=units)
            if polyline_entity is not None:
                _record_entity(polyline_entity)
                supported_geometry_count += 1
                continue

            malformed_drawable_count += 1
            handle = _record_unknown(record, reason="malformed_lwpolyline_geometry")
            if handle is not None:
                malformed_handles.add(handle)
            continue

        if record_type == "HATCH":
            hatch_result = _build_hatch_entity(record, units=units)
            if hatch_result.entity is not None:
                _record_entity(hatch_result.entity)
                supported_geometry_count += 1
                continue

            handle = _record_unknown(
                record,
                reason=hatch_result.reason or "unsupported_hatch_geometry",
            )
            if hatch_result.malformed:
                malformed_hatch_count += 1
                if handle is not None:
                    malformed_hatch_handles.add(handle)
            else:
                unsupported_hatch_count += 1
                if handle is not None:
                    unsupported_hatch_handles.add(handle)
            continue

        if record_type == "MTEXT":
            text_entity = _build_text_entity(record, units=units)
            _record_entity(text_entity)
            supported_text_count += 1
            continue

        if record_type == "INSERT":
            insert_result = _build_insert_entity(
                record, units=units, block_header_names=block_header_names
            )
            if insert_result.entity is not None:
                _record_entity(insert_result.entity)
                block_reference_count += 1
                handle = cast(str | None, insert_result.entity.get("source_entity_handle"))
                if handle is not None:
                    block_reference_handles.add(handle)
                if not insert_result.transform_supported:
                    unsupported_transform_count += 1
                    if handle is not None:
                        unsupported_transform_handles.add(handle)
                    if insert_result.reason is not None:
                        unsupported_transform_reasons.add(insert_result.reason)
                continue

            malformed_insert_count += 1
            handle = _record_unknown(
                record, reason=insert_result.reason or "malformed_insert_record"
            )
            if handle is not None:
                malformed_insert_handles.add(handle)
            continue

        unsupported_drawable_count += 1
        unsupported_types.add(record_type)
        _record_unknown(record, reason="unsupported_drawable_record")

    metadata: dict[str, JSONValue] = {
        "source_format": source.upload_format.value,
        "adapter_mode": "dwgread_json_v0_1",
        "dwgread": {
            "output_kind": run_result.output_kind,
            "output_size_bytes": run_result.output_size_bytes,
            "stdout_bytes": run_result.stdout.byte_count,
            "stderr_bytes": run_result.stderr.byte_count,
        },
        "entity_counts": {
            "drawable_candidates": drawable_candidate_count,
            "supported_geometry": supported_geometry_count,
            "supported_lines": supported_line_count,
            "supported_text": supported_text_count,
            "unsupported_drawables": unsupported_drawable_count,
            "malformed_drawables": malformed_drawable_count,
            "unsupported_hatches": unsupported_hatch_count,
            "malformed_hatches": malformed_hatch_count,
            "block_references": block_reference_count,
            "unsupported_block_transforms": unsupported_transform_count,
            "malformed_inserts": malformed_insert_count,
        },
        "units": _units_summary(units),
    }
    # Confirm block-transform validity for the downstream validation check only when every
    # captured block reference is within the supported transform subset; otherwise leave the
    # hint unset so the reference stays review-gated.
    if (
        block_reference_count > 0
        and unsupported_transform_count == 0
        and malformed_insert_count == 0
    ):
        metadata["block_transform_validity"] = True
    if run_result.output_key_count is not None:
        dwgread_metadata = cast(dict[str, JSONValue], metadata["dwgread"])
        dwgread_metadata["output_key_count"] = run_result.output_key_count
    if not entities:
        metadata["empty_entities_reason"] = "no_drawable_candidates_detected"

    warnings: list[AdapterWarning] = []
    if not units.confirmed:
        warnings.append(_units_unconfirmed_warning())
    if unsupported_drawable_count > 0:
        warnings.append(
            AdapterWarning(
                code="libredwg.unsupported_drawable_record",
                message="LibreDWG emitted drawable records that are not mapped yet.",
                details={
                    "count": unsupported_drawable_count,
                    "types": tuple(sorted(unsupported_types))[:10],
                },
            )
        )
    if malformed_drawable_count > 0:
        warnings.append(
            AdapterWarning(
                code="libredwg.malformed_drawable_record",
                message="LibreDWG emitted drawable records with malformed geometry.",
                details={
                    "count": malformed_drawable_count,
                    "handles": tuple(sorted(malformed_handles))[:10],
                },
            )
        )
    if unsupported_hatch_count > 0:
        warnings.append(
            AdapterWarning(
                code="libredwg.unsupported_hatch_geometry",
                message=(
                    "LibreDWG emitted HATCH records outside the supported straight "
                    "single-loop subset."
                ),
                details={
                    "count": unsupported_hatch_count,
                    "handles": tuple(sorted(unsupported_hatch_handles))[:10],
                },
            )
        )
    if malformed_hatch_count > 0:
        warnings.append(
            AdapterWarning(
                code="libredwg.malformed_hatch_geometry",
                message="LibreDWG emitted HATCH records with malformed boundary geometry.",
                details={
                    "count": malformed_hatch_count,
                    "handles": tuple(sorted(malformed_hatch_handles))[:10],
                },
            )
        )
    if block_reference_count > 0:
        warnings.append(
            AdapterWarning(
                code="libredwg.block_reference_captured",
                message=(
                    "LibreDWG INSERT records were captured as review-gated block references "
                    "without materialized geometry."
                ),
                details={
                    "count": block_reference_count,
                    "unsupported_transform_count": unsupported_transform_count,
                    "handles": tuple(sorted(block_reference_handles))[:10],
                },
            )
        )
    if unsupported_transform_count > 0:
        warnings.append(
            AdapterWarning(
                code="libredwg.unsupported_block_transform",
                message=(
                    "LibreDWG INSERT records use block transforms outside the supported subset."
                ),
                details={
                    "count": unsupported_transform_count,
                    "handles": tuple(sorted(unsupported_transform_handles))[:10],
                    "reasons": tuple(sorted(unsupported_transform_reasons)),
                },
            )
        )
    if malformed_insert_count > 0:
        warnings.append(
            AdapterWarning(
                code="libredwg.malformed_insert_record",
                message="LibreDWG INSERT records were missing a usable insertion point.",
                details={
                    "count": malformed_insert_count,
                    "handles": tuple(sorted(malformed_insert_handles))[:10],
                },
            )
        )

    has_supported_drawable_count = supported_geometry_count + supported_text_count
    has_mixed_entity_outcomes = has_supported_drawable_count > 0 and (
        unsupported_drawable_count > 0
        or malformed_drawable_count > 0
        or unsupported_hatch_count > 0
        or malformed_hatch_count > 0
        or block_reference_count > 0
        or malformed_insert_count > 0
    )
    if has_mixed_entity_outcomes:
        confidence_score = _MIXED_ENTITY_CONFIDENCE_SCORE
        confidence_basis = "libredwg_dwgread_json_mixed_entity_mapping"
    elif supported_geometry_count > 0 and supported_text_count > 0:
        confidence_score = _LINE_ENTITY_CONFIDENCE_SCORE
        confidence_basis = "libredwg_dwgread_json_geometry_text_mapping"
    elif supported_line_count > 0 and supported_line_count == supported_geometry_count:
        confidence_score = _LINE_ENTITY_CONFIDENCE_SCORE
        confidence_basis = "libredwg_dwgread_json_line_mapping"
    elif supported_geometry_count > 0:
        confidence_score = _LINE_ENTITY_CONFIDENCE_SCORE
        confidence_basis = "libredwg_dwgread_json_geometry_mapping"
    elif supported_text_count > 0:
        confidence_score = _TEXT_ENTITY_CONFIDENCE_SCORE
        confidence_basis = "libredwg_dwgread_json_text_mapping"
    elif block_reference_count > 0:
        confidence_score = _BLOCK_REFERENCE_CONFIDENCE_SCORE
        confidence_basis = "libredwg_dwgread_json_block_reference_mapping"
    elif entities:
        confidence_score = _UNKNOWN_ENTITY_CONFIDENCE_SCORE
        confidence_basis = "libredwg_dwgread_json_unknown_entity_mapping"
    else:
        confidence_score = _PLACEHOLDER_CONFIDENCE_SCORE
        confidence_basis = "libredwg_dwgread_json_placeholder"

    canonical: _JSONDict = {
        "schema_version": _SCHEMA_VERSION,
        "canonical_entity_schema_version": _SCHEMA_VERSION,
        "units": dict(units.payload),
        "coordinate_system": {
            "name": "local",
            "type": "cartesian",
            "source": "libredwg_dwgread_json",
        },
        "layouts": tuple({"name": layout_name} for layout_name in sorted(layouts_seen)),
        "layers": tuple({"name": layer_name} for layer_name in sorted(layers_seen)),
        "blocks": tuple({"name": block_name} for block_name in sorted(blocks_seen)),
        "entities": tuple(entities),
        "xrefs": (),
        "metadata": metadata,
    }

    return _CanonicalBuildResult(
        canonical=canonical,
        warnings=tuple(warnings),
        confidence=ConfidenceSummary(
            score=confidence_score,
            review_required=True,
            basis=confidence_basis,
        ),
    )


def _collect_observed_collection_refs(
    entity: Mapping[str, Any],
    *,
    layouts_seen: set[str],
    layers_seen: set[str],
    blocks_seen: set[str],
) -> None:
    layout_name = cast(str | None, entity.get("layout_name"))
    if layout_name:
        layouts_seen.add(layout_name)

    layer_name = cast(str | None, entity.get("layer_name"))
    if layer_name:
        layers_seen.add(layer_name)

    block_name = cast(str | None, entity.get("block_name"))
    if block_name:
        blocks_seen.add(block_name)


def _diagnostic_entity_counts(canonical: Mapping[str, Any]) -> _JSONDict | None:
    metadata = _mapping_get(canonical, "metadata")
    if not isinstance(metadata, Mapping):
        return None
    counts = _mapping_get(metadata, "entity_counts")
    if not isinstance(counts, Mapping):
        return None

    sanitized_counts: _JSONDict = {}
    for key in (
        "drawable_candidates",
        "supported_geometry",
        "supported_lines",
        "supported_text",
        "unsupported_drawables",
        "malformed_drawables",
        "unsupported_hatches",
        "malformed_hatches",
        "block_references",
        "unsupported_block_transforms",
        "malformed_inserts",
    ):
        value = _mapping_get(counts, key)
        if isinstance(value, int) and not isinstance(value, bool):
            sanitized_counts[key] = value
    return sanitized_counts or None


def _units_unconfirmed_warning() -> AdapterWarning:
    return AdapterWarning(
        code="libredwg.units_unconfirmed",
        message="LibreDWG JSON output does not confirm drawing units for canonical quantities.",
        details={"normalized_units": "unknown"},
    )


def _resolve_units(payload: Any) -> _UnitsResolution:
    """Resolve drawing units from the dwgread ``HEADER.INSUNITS`` code.

    Confirms (and scales to meters) only the curated ``_INSUNITS_METER_SCALE`` subset; every
    other case (unitless code 0, unsupported/exotic codes, missing or non-integral values, or a
    list payload without a header) degrades to the review-gated ``{"normalized": "unknown"}``
    shape that the adapter has always emitted. Mirrors ``ezdxf._units_payload`` (the DXF adapter)
    except libredwg's header key is ``INSUNITS`` (no ``$``).
    """

    code = _extract_insunits_code(payload)
    if code is not None and code in _INSUNITS_METER_SCALE:
        factor = _INSUNITS_METER_SCALE[code]
        return _UnitsResolution(
            payload={
                "normalized": "meter",
                "source": "INSUNITS",
                "source_value": code,
                "conversion_target": "meter",
                "conversion_factor": factor,
            },
            scale=factor,
            confirmed=True,
        )

    return _UnitsResolution(payload={"normalized": "unknown"}, scale=1.0, confirmed=False)


def _extract_insunits_code(payload: Any) -> int | None:
    if not isinstance(payload, Mapping):
        return None
    header = _mapping_get(payload, "HEADER")
    if not isinstance(header, Mapping):
        return None
    coerced = _coerce_float(_mapping_get(header, "INSUNITS"))
    if coerced is None or not math.isfinite(coerced) or not coerced.is_integer():
        return None
    return int(coerced)


def _units_summary(units: _UnitsResolution) -> _JSONDict:
    """Diagnostic-only units summary for metadata and the extract diagnostic details."""

    return {
        "normalized": units.payload["normalized"],
        "source": "INSUNITS",
        "source_value": units.payload.get("source_value"),
        "confirmed": units.confirmed,
    }


def _units_label(units: _UnitsResolution) -> _JSONDict:
    # Per-entity geometry intentionally carries only the slim normalized label (not the full
    # canonical units payload that ezdxf stamps on each entity); the full payload lives on the
    # top-level canonical ``units``. Unconfirmed stays ``"unknown"`` (not ezdxf's ``"unitless"``)
    # so the validation units check keeps libredwg review-gated. Both divergences are deliberate.
    return {"normalized": "meter" if units.confirmed else "unknown"}


def _units_notes(units: _UnitsResolution) -> tuple[str, ...]:
    return () if units.confirmed else ("units_unconfirmed",)


def _units_basis_suffix(units: _UnitsResolution) -> str:
    return "units_meter" if units.confirmed else "units_unconfirmed"


def _scale_value(value: float, scale: float) -> float:
    if scale == 1.0:
        return value
    return value * scale


def _scale_point(point: tuple[float, float, float], scale: float) -> tuple[float, float, float]:
    if scale == 1.0:
        return point
    return (point[0] * scale, point[1] * scale, point[2] * scale)


def _iter_object_records(payload: Any) -> tuple[Mapping[str, Any], ...]:
    if isinstance(payload, Mapping):
        objects = _mapping_get(payload, "OBJECTS")
        if objects is None:
            return ()
        return tuple(_iter_mapping_candidates(objects))
    if isinstance(payload, list):
        return tuple(item for item in payload if isinstance(item, Mapping))
    return ()


def _iter_mapping_candidates(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, list):
        list_candidates: list[Mapping[str, Any]] = []
        for item in value:
            if isinstance(item, Mapping):
                list_candidates.extend(_iter_mapping_candidates(item))
        return list_candidates
    if not isinstance(value, Mapping):
        return []
    if _extract_record_type_from_current_mapping(value) is not None:
        return [cast(Mapping[str, Any], value)]

    candidates: list[Mapping[str, Any]] = []
    for nested_value in value.values():
        if isinstance(nested_value, Mapping):
            candidates.extend(_iter_mapping_candidates(nested_value))
        elif isinstance(nested_value, list):
            for item in nested_value:
                if isinstance(item, Mapping):
                    candidates.extend(_iter_mapping_candidates(item))

    if len(candidates) == 1 and _extract_record_type_from_wrapper_views(value) is not None:
        return [cast(Mapping[str, Any], value)]
    return candidates


def _build_line_entity(record: Mapping[str, Any], *, units: _UnitsResolution) -> _JSONDict | None:
    start_point = _extract_point(
        record,
        prefixes=("start", "start_point", "first_endpoint", "point1", "p1", "from"),
    )
    end_point = _extract_point(
        record,
        prefixes=("end", "end_point", "second_endpoint", "point2", "p2", "to"),
    )
    if start_point is None or end_point is None:
        return None

    start_point = _scale_point(start_point, units.scale)
    end_point = _scale_point(end_point, units.scale)
    if not _point_is_finite(start_point) or not _point_is_finite(end_point):
        return None

    entity_id = _entity_id(record, record_type="line")
    layout_name = _extract_layout_name(record)
    layer_name = _extract_layer_name(record)
    block_name = _extract_block_name(record)
    source_handle = _extract_handle(record)
    quantity_length = math.dist(start_point, end_point)
    start = _point_json(start_point)
    end = _point_json(end_point)
    bbox = {
        "min": {
            "x": min(start_point[0], end_point[0]),
            "y": min(start_point[1], end_point[1]),
            "z": min(start_point[2], end_point[2]),
        },
        "max": {
            "x": max(start_point[0], end_point[0]),
            "y": max(start_point[1], end_point[1]),
            "z": max(start_point[2], end_point[2]),
        },
    }
    safe_projection = _safe_record_projection(
        record,
        record_type="LINE",
        geometry={
            "start": start,
            "end": end,
        },
    )
    provenance = _entity_provenance(
        record,
        record_type="LINE",
        source_handle=source_handle,
        safe_projection=safe_projection,
        notes=_units_notes(units),
    )

    return {
        "entity_id": entity_id,
        "entity_type": "line",
        "entity_schema_version": _SCHEMA_VERSION,
        **provenance,
        "source_entity_handle": source_handle,
        "layout_name": layout_name,
        "layer_name": layer_name,
        "block_name": block_name,
        "parent_entity_id": None,
        "drawing_revision_id": None,
        "source_file_id": None,
        "layout_ref": layout_name,
        "layer_ref": layer_name,
        "block_ref": block_name,
        "parent_entity_ref": None,
        "bbox": bbox,
        "geometry": {
            "start": start,
            "end": end,
            "bbox": bbox,
            "units": _units_label(units),
            "geometry_summary": {
                "kind": "line_segment",
                "length": quantity_length,
                "vertex_count": 2,
            },
        },
        "properties": {
            "source_type": "LINE",
            "source_handle": source_handle,
            "quantity_hints": {
                "length": quantity_length,
                "count": 1.0,
            },
            "adapter_native": {
                "libredwg": {
                    "section": "OBJECTS",
                    "record_type": "LINE",
                    "handle": source_handle,
                }
            },
        },
        "provenance": provenance,
        "confidence": {
            "score": _LINE_ENTITY_CONFIDENCE_SCORE,
            "review_required": True,
            "basis": f"libredwg_line_mapping_{_units_basis_suffix(units)}",
        },
        "kind": "line",
        "start": start,
        "end": end,
        "length": quantity_length,
    }


def _build_text_entity(record: Mapping[str, Any], *, units: _UnitsResolution) -> _JSONDict:
    insertion_point = _extract_point(
        record,
        prefixes=("insertion", "insertion_point", "ins_pt", "text_position"),
    )
    if insertion_point is not None:
        insertion_point = _scale_point(insertion_point, units.scale)
    layer_name = _extract_layer_name(record)
    layout_name = _extract_layout_name(record)
    block_name = _extract_block_name(record)
    source_handle = _extract_handle(record)
    text = _extract_text_value(record)
    text_length = len(text)

    if insertion_point is None:
        geometry_reason = "missing_text_placement"
        point_json: _JSONDict | None = None
        bbox = None
    else:
        if not _point_is_finite(insertion_point):
            geometry_reason = "malformed_text_placement"
            point_json = None
            bbox = None
        else:
            geometry_reason = None
            point_json = _point_json(insertion_point)
            bbox = {
                "min": {
                    "x": insertion_point[0],
                    "y": insertion_point[1],
                    "z": insertion_point[2],
                },
                "max": {
                    "x": insertion_point[0],
                    "y": insertion_point[1],
                    "z": insertion_point[2],
                },
            }

    text_summary: _JSONDict = {
        "kind": "text",
        "source_type": "MTEXT",
        "text_length": text_length,
    }
    if geometry_reason is not None:
        text_summary["reason"] = geometry_reason

    geometry_projection: _JSONDict = {
        "text": text,
        "units": _units_label(units),
        "geometry_summary": text_summary,
    }
    if point_json is not None:
        geometry_projection["insertion"] = point_json
    else:
        geometry_projection.update({"bbox": None, "status": "absent", "reason": geometry_reason})

    safe_projection = _safe_record_projection(
        record,
        record_type="MTEXT",
        geometry={
            "text": text,
            "insertion": point_json,
        },
    )
    provenance = _entity_provenance(
        record,
        record_type="MTEXT",
        source_handle=source_handle,
        safe_projection=safe_projection,
        notes=_units_notes(units),
    )
    safe_bbox = bbox
    entity_bbox = None if geometry_reason is not None else safe_bbox

    return {
        "entity_id": _entity_id(record, record_type="text"),
        "entity_type": "text",
        "entity_schema_version": _SCHEMA_VERSION,
        **provenance,
        "source_entity_handle": source_handle,
        "layout_name": layout_name,
        "layer_name": layer_name,
        "block_name": block_name,
        "parent_entity_id": None,
        "drawing_revision_id": None,
        "source_file_id": None,
        "layout_ref": layout_name,
        "layer_ref": layer_name,
        "block_ref": block_name,
        "parent_entity_ref": None,
        "bbox": entity_bbox,
        "geometry": geometry_projection,
        "properties": {
            "source_type": "MTEXT",
            "source_handle": source_handle,
            "text": text,
            "text_length": text_length,
            "adapter_native": {
                "libredwg": {
                    "section": "OBJECTS",
                    "record_type": "MTEXT",
                    "handle": source_handle,
                }
            },
        },
        "provenance": provenance,
        "confidence": {
            "score": _TEXT_ENTITY_CONFIDENCE_SCORE,
            "review_required": True,
            "basis": f"libredwg_mtext_mapping_{_units_basis_suffix(units)}",
        },
        "kind": "text",
        "text": text,
        "text_length": text_length,
    }


def _build_circle_entity(record: Mapping[str, Any], *, units: _UnitsResolution) -> _JSONDict | None:
    center_point = _extract_point(record, prefixes=("center", "center_point", "centerpoint"))
    radius = _extract_positive_float(record, "radius")
    if center_point is None or radius is None:
        return None
    center_point = _scale_point(center_point, units.scale)
    radius = _scale_value(radius, units.scale)
    if not _point_is_finite(center_point):
        return None

    center = _point_json(center_point)
    bbox = _bbox_from_points(
        (
            (center_point[0] - radius, center_point[1] - radius, center_point[2]),
            (center_point[0] + radius, center_point[1] + radius, center_point[2]),
        )
    )
    circumference = 2.0 * math.pi * radius
    diameter = 2.0 * radius
    area = math.pi * radius * radius
    if bbox is None or not _floats_are_finite(circumference, diameter, area):
        return None
    geometry_summary: _JSONDict = {
        "kind": "circle",
        "radius": radius,
        "diameter": diameter,
        "circumference": circumference,
    }

    return _build_supported_geometry_entity(
        record,
        record_type="CIRCLE",
        entity_type="circle",
        bbox=bbox,
        geometry_projection={
            "center": center,
            "radius": radius,
        },
        geometry={
            "center": center,
            "radius": radius,
            "bbox": bbox,
            "units": _units_label(units),
            "geometry_summary": geometry_summary,
        },
        properties={
            "source_type": "CIRCLE",
            "quantity_hints": {
                "length": circumference,
                "area": area,
                "count": 1.0,
            },
        },
        units=units,
        confidence_basis_prefix="libredwg_circle_mapping",
        extra_fields={
            "kind": "circle",
            "center": center,
            "radius": radius,
            "diameter": diameter,
        },
    )


def _build_arc_entity(record: Mapping[str, Any], *, units: _UnitsResolution) -> _JSONDict | None:
    center_point = _extract_point(record, prefixes=("center", "center_point", "centerpoint"))
    radius = _extract_positive_float(record, "radius")
    if not _record_uses_supported_arc_angles(record):
        return None
    start_angle = _extract_angle_degrees(record, "start_angle", "startangle")
    end_angle = _extract_angle_degrees(record, "end_angle", "endangle")
    if center_point is None or radius is None or start_angle is None or end_angle is None:
        return None
    center_point = _scale_point(center_point, units.scale)
    radius = _scale_value(radius, units.scale)
    if not _point_is_finite(center_point):
        return None

    sweep_degrees = _arc_sweep_degrees(start_angle, end_angle)
    if sweep_degrees is None:
        return None

    center = _point_json(center_point)
    start_point = _point_on_circle(center_point, radius, start_angle)
    end_point = _point_on_circle(center_point, radius, end_angle)
    if start_point is None or end_point is None:
        return None
    bbox = _arc_bbox(
        center_point=center_point,
        radius=radius,
        start_angle_degrees=start_angle,
        sweep_degrees=sweep_degrees,
        start_point=start_point,
        end_point=end_point,
    )
    if bbox is None:
        return None
    start = _point_json(start_point)
    end = _point_json(end_point)
    length = radius * math.radians(sweep_degrees)
    if not _floats_are_finite(length):
        return None
    geometry_summary: _JSONDict = {
        "kind": "arc",
        "radius": radius,
        "start_angle_degrees": start_angle,
        "end_angle_degrees": end_angle,
        "sweep_degrees": sweep_degrees,
        "length": length,
    }

    return _build_supported_geometry_entity(
        record,
        record_type="ARC",
        entity_type="arc",
        bbox=bbox,
        geometry_projection={
            "center": center,
            "radius": radius,
            "start_angle_degrees": start_angle,
            "end_angle_degrees": end_angle,
        },
        geometry={
            "center": center,
            "radius": radius,
            "start_angle_degrees": start_angle,
            "end_angle_degrees": end_angle,
            "start": start,
            "end": end,
            "bbox": bbox,
            "units": _units_label(units),
            "geometry_summary": geometry_summary,
        },
        properties={
            "source_type": "ARC",
            "quantity_hints": {
                "length": length,
                "count": 1.0,
            },
        },
        units=units,
        confidence_basis_prefix="libredwg_arc_mapping",
        extra_fields={
            "kind": "arc",
            "center": center,
            "radius": radius,
            "start_angle_degrees": start_angle,
            "end_angle_degrees": end_angle,
            "start": start,
            "end": end,
            "length": length,
        },
    )


def _build_lwpolyline_entity(
    record: Mapping[str, Any], *, units: _UnitsResolution
) -> _JSONDict | None:
    raw_vertices = _first_value(record, "vertices", "points", "vertexes")
    vertices = _extract_vertices(raw_vertices)
    if len(vertices) < 2:
        return None
    if units.scale != 1.0:
        vertices = tuple(_scale_point(vertex, units.scale) for vertex in vertices)
    if any(not _point_is_finite(vertex) for vertex in vertices):
        return None
    if _mapping_has_nondefault_zero_value(
        record,
        "const_width",
        "constant_width",
        "width",
        "start_width",
        "end_width",
    ):
        return None
    if _mapping_has_nondefault_zero_value(record, "bulge"):
        return None
    if _vertices_have_nondefault_zero_value(
        raw_vertices,
        "bulge",
        "width",
        "start_width",
        "end_width",
        "const_width",
        "constant_width",
    ):
        return None

    closed = _extract_closed(record)
    if closed is None:
        return None
    bbox = _bbox_from_points(vertices)
    if bbox is None:
        return None
    vertex_json = tuple(_point_json(vertex) for vertex in vertices)
    length = _polyline_length(vertices, closed=closed)
    if length is None:
        return None
    geometry_summary: _JSONDict = {
        "kind": "polyline",
        "length": length,
        "vertex_count": len(vertices),
        "closed": closed,
    }

    return _build_supported_geometry_entity(
        record,
        record_type="LWPOLYLINE",
        entity_type="polyline",
        bbox=bbox,
        geometry_projection={
            "vertices": vertex_json,
            "closed": closed,
        },
        geometry={
            "vertices": vertex_json,
            "closed": closed,
            "bbox": bbox,
            "units": _units_label(units),
            "geometry_summary": geometry_summary,
        },
        properties={
            "source_type": "LWPOLYLINE",
            "quantity_hints": {
                "length": length,
                "count": 1.0,
            },
        },
        units=units,
        confidence_basis_prefix="libredwg_lwpolyline_mapping",
        extra_fields={
            "kind": "polyline",
            "vertices": vertex_json,
            "closed": closed,
            "length": length,
        },
    )


def _build_hatch_entity(record: Mapping[str, Any], *, units: _UnitsResolution) -> _HatchBuildResult:
    vertices_result = _extract_hatch_vertices(record)
    if vertices_result.reason is not None:
        return _HatchBuildResult(
            None,
            reason=vertices_result.reason,
            malformed=vertices_result.malformed,
        )

    vertices = cast(tuple[tuple[float, float, float], ...], vertices_result.vertices)
    if units.scale != 1.0:
        vertices = tuple(_scale_point(vertex, units.scale) for vertex in vertices)
    bbox = _bbox_from_points(vertices)
    if bbox is None:
        return _HatchBuildResult(None, reason="malformed_hatch_geometry", malformed=True)

    perimeter = _polyline_length(vertices, closed=True)
    area = _polygon_area(vertices)
    if perimeter is None or area is None or not _floats_are_finite(perimeter, area) or area <= 0.0:
        return _HatchBuildResult(None, reason="malformed_hatch_geometry", malformed=True)

    vertex_json = tuple(_point_json(vertex) for vertex in vertices)
    kind = "hatch"
    closed = True
    geometry_summary: _JSONDict = {
        "kind": kind,
        "vertex_count": len(vertices),
        "closed": closed,
        "area": area,
        "perimeter": perimeter,
    }
    return _HatchBuildResult(
        _build_supported_geometry_entity(
            record,
            record_type="HATCH",
            entity_type=kind,
            bbox=bbox,
            geometry_projection={
                "vertices": vertex_json,
                "closed": closed,
            },
            geometry={
                "vertices": vertex_json,
                "closed": closed,
                "bbox": bbox,
                "units": _units_label(units),
                "geometry_summary": geometry_summary,
            },
            properties={
                "source_type": "HATCH",
                "quantity_hints": {
                    "length": perimeter,
                    "area": area,
                    "count": 1.0,
                },
            },
            units=units,
            confidence_basis_prefix="libredwg_hatch_mapping",
            extra_fields={
                "kind": kind,
                "vertices": vertex_json,
                "closed": closed,
                "area": area,
                "perimeter": perimeter,
            },
        )
    )


def _build_block_header_name_map(records: tuple[Mapping[str, Any], ...]) -> dict[str, str]:
    """Map BLOCK_HEADER handles to block names so INSERT references resolve to a name.

    Keyed by lowercased handle to tolerate case differences between the BLOCK_HEADER handle and
    the handle reference an INSERT carries in its ``block_header`` field.
    """

    names: dict[str, str] = {}
    for record in records:
        if _extract_record_type(record) != "BLOCK_HEADER":
            continue
        handle = _extract_handle(record)
        name = _first_string(record, "name", "block_name")
        if handle is not None and name is not None:
            names[handle.lower()] = name
    return names


def _coerce_handle_ref(value: Any) -> str | None:
    if isinstance(value, str):
        return _sanitize_string_value(value)
    if isinstance(value, Mapping):
        handle = _mapping_get(value, "handle", "absolute_ref", "ref", "value", "code")
        return _coerce_handle_ref(handle) if handle is not None else None
    if isinstance(value, (list, tuple)) and value:
        return _coerce_handle_ref(value[0])
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _sanitize_string_value(str(int(value)))
    return None


def _resolve_referenced_block_name(
    record: Mapping[str, Any], *, block_header_names: Mapping[str, str]
) -> str | None:
    """Resolve the block an INSERT references, by direct name or BLOCK_HEADER handle lookup.

    The ``block``/``block_name`` owner fields are intentionally not consulted here: for an INSERT
    those identify the owning space (e.g. model space), not the block being placed.
    """

    direct = _first_string(record, "block_header_name", "ref_block", "name")
    if direct is not None:
        return direct
    ref = _coerce_handle_ref(
        _first_value(record, "block_header", "block_record", "block_record_ref")
    )
    if ref is None:
        return None
    return block_header_names.get(ref.lower())


def _extract_insert_scale(record: Mapping[str, Any]) -> tuple[float, float, float]:
    scale_point = _coerce_point(_first_value(record, "scale", "scale_factor"))
    if scale_point is not None:
        return scale_point
    x_scale = _coerce_float(_first_value(record, "xscale", "scale_x", "x_scale"))
    y_scale = _coerce_float(_first_value(record, "yscale", "scale_y", "y_scale"))
    z_scale = _coerce_float(_first_value(record, "zscale", "scale_z", "z_scale"))
    return (
        x_scale if x_scale is not None else 1.0,
        y_scale if y_scale is not None else 1.0,
        z_scale if z_scale is not None else 1.0,
    )


def _extract_insert_array(record: Mapping[str, Any], *, units: _UnitsResolution) -> _JSONDict:
    columns = _coerce_float(_first_value(record, "num_cols", "numcols", "column_count", "ncols"))
    rows = _coerce_float(_first_value(record, "num_rows", "numrows", "row_count", "nrows"))
    column_spacing = _coerce_float(
        _first_value(record, "col_spacing", "column_spacing", "colspacing")
    )
    row_spacing = _coerce_float(_first_value(record, "row_spacing", "rowspacing"))
    return {
        "columns": int(columns) if columns is not None and columns >= 1 else 1,
        "rows": int(rows) if rows is not None and rows >= 1 else 1,
        "column_spacing": _scale_value(column_spacing, units.scale)
        if column_spacing is not None
        else 0.0,
        "row_spacing": _scale_value(row_spacing, units.scale) if row_spacing is not None else 0.0,
    }


def _scales_are_uniform(scale: tuple[float, float, float]) -> bool:
    reference, *rest = scale
    return all(
        math.isclose(
            component,
            reference,
            rel_tol=_INSERT_SCALE_UNIFORM_TOLERANCE,
            abs_tol=_INSERT_SCALE_UNIFORM_TOLERANCE,
        )
        for component in rest
    )


def _classify_insert_transform(
    *,
    scale: tuple[float, float, float],
    rotation_radians: float,
    array: Mapping[str, Any],
    block_name: str | None,
) -> tuple[str, ...]:
    """Return the ordered reasons an INSERT transform is outside the safe subset (empty == safe)."""

    reasons: list[str] = []
    if block_name is None:
        reasons.append("unresolved_block_reference")
    if not _floats_are_finite(*scale) or any(component == 0.0 for component in scale):
        reasons.append("unsupported_degenerate_scale")
    elif any(component < 0.0 for component in scale):
        reasons.append("unsupported_mirrored_scale")
    elif not _scales_are_uniform(scale):
        reasons.append("unsupported_nonuniform_scale")
    if int(array.get("columns", 1)) > 1 or int(array.get("rows", 1)) > 1:
        reasons.append("unsupported_block_array")
    if not math.isfinite(rotation_radians):
        reasons.append("unsupported_rotation")
    return tuple(reasons)


def _build_insert_entity(
    record: Mapping[str, Any],
    *,
    units: _UnitsResolution,
    block_header_names: Mapping[str, str],
) -> _InsertBuildResult:
    """Capture an INSERT as a review-gated block reference with safe transform metadata.

    Geometry is not materialized (that is deliberately out of scope); the placement is recorded
    as an ``insert`` entity carrying the insertion point (scaled to meters), scale, rotation, and
    array metadata, plus a supported/unsupported classification that drives validation review.
    """

    insertion_point = _extract_point(
        record,
        prefixes=("ins_pt", "insertion_point", "insertion", "ins", "position", "point", "location"),
    )
    if insertion_point is None or not _point_is_finite(insertion_point):
        return _InsertBuildResult(None, malformed=True, reason="malformed_insert_record")

    scaled_insertion = _scale_point(insertion_point, units.scale)
    scale = _extract_insert_scale(record)
    raw_rotation = _coerce_float(_first_value(record, "rotation", "rotation_radians", "angle"))
    rotation_radians = raw_rotation if raw_rotation is not None else 0.0
    array = _extract_insert_array(record, units=units)
    block_name = _resolve_referenced_block_name(record, block_header_names=block_header_names)

    reasons = _classify_insert_transform(
        scale=scale,
        rotation_radians=rotation_radians,
        array=array,
        block_name=block_name,
    )
    transform_supported = not reasons
    primary_reason = reasons[0] if reasons else None
    rotation_finite = math.isfinite(rotation_radians)

    transform: _JSONDict = {
        "insertion_point": _point_json(scaled_insertion),
        "scale": {"x": scale[0], "y": scale[1], "z": scale[2]},
        "rotation_degrees": math.degrees(rotation_radians) if rotation_finite else None,
        "rotation_radians": rotation_radians if rotation_finite else None,
        "array": array,
        "supported": transform_supported,
        "unsupported_reasons": reasons,
    }

    source_handle = _extract_handle(record)
    layout_name = _extract_layout_name(record)
    layer_name = _extract_layer_name(record)
    safe_projection = _safe_record_projection(record, record_type="INSERT", geometry=None)
    extra_notes = (primary_reason,) if primary_reason is not None else ()
    provenance = _entity_provenance(
        record,
        record_type="INSERT",
        source_handle=source_handle,
        safe_projection=safe_projection,
        notes=(*_units_notes(units), "block_reference_unmaterialized", *extra_notes),
    )
    geometry_summary: _JSONDict = {
        "kind": "insert",
        "block_name": block_name,
        "transform_supported": transform_supported,
        "reason": primary_reason,
    }
    block_reference: _JSONDict = {
        "block_name": block_name,
        "transform_supported": transform_supported,
        "reason": primary_reason,
    }

    entity: _JSONDict = {
        "entity_id": _entity_id(record, record_type="insert"),
        "entity_type": "insert",
        "entity_schema_version": _SCHEMA_VERSION,
        **provenance,
        "source_entity_handle": source_handle,
        "layout_name": layout_name,
        "layer_name": layer_name,
        "block_name": block_name,
        "parent_entity_id": None,
        "drawing_revision_id": None,
        "source_file_id": None,
        "layout_ref": layout_name,
        "layer_ref": layer_name,
        "block_ref": block_name,
        "parent_entity_ref": None,
        "bbox": None,
        "geometry": {
            "bbox": None,
            "units": _units_label(units),
            "status": "reference",
            "geometry_summary": geometry_summary,
            "transform": transform,
        },
        # The top-level ``insert`` coordinate lets the geometry-validity check recognise the
        # placement; ``transform``/``kind`` drive the block-transform-validity check.
        "insert": _point_json(scaled_insertion),
        "transform": transform,
        "quantity_hints": {},
        "properties": {
            "source_type": "INSERT",
            "source_handle": source_handle,
            "quantity_hints": {},
            "block_reference": block_reference,
            "adapter_native": {
                "libredwg": {
                    "section": "OBJECTS",
                    "record_type": "INSERT",
                    "handle": source_handle,
                }
            },
        },
        "provenance": provenance,
        "confidence": {
            "score": _BLOCK_REFERENCE_CONFIDENCE_SCORE,
            "review_required": True,
            "basis": f"libredwg_dwgread_json_block_reference_{_units_basis_suffix(units)}",
        },
        "kind": "insert",
        "block_reference": block_reference,
        "transform_supported": transform_supported,
    }
    return _InsertBuildResult(
        entity,
        transform_supported=transform_supported,
        malformed=False,
        reason=primary_reason,
    )


def _build_supported_geometry_entity(
    record: Mapping[str, Any],
    *,
    record_type: str,
    entity_type: str,
    bbox: _JSONDict,
    geometry_projection: _JSONDict,
    geometry: _JSONDict,
    properties: _JSONDict,
    units: _UnitsResolution,
    confidence_basis_prefix: str,
    extra_fields: _JSONDict,
) -> _JSONDict:
    layout_name = _extract_layout_name(record)
    layer_name = _extract_layer_name(record)
    block_name = _extract_block_name(record)
    source_handle = _extract_handle(record)
    safe_projection = _safe_record_projection(
        record,
        record_type=record_type,
        geometry=geometry_projection,
    )
    provenance = _entity_provenance(
        record,
        record_type=record_type,
        source_handle=source_handle,
        safe_projection=safe_projection,
        notes=_units_notes(units),
    )
    entity_properties = {
        "source_type": record_type,
        "source_handle": source_handle,
        **properties,
        "adapter_native": {
            "libredwg": {
                "section": "OBJECTS",
                "record_type": record_type,
                "handle": source_handle,
            }
        },
    }

    return {
        "entity_id": _entity_id(record, record_type=entity_type),
        "entity_type": entity_type,
        "entity_schema_version": _SCHEMA_VERSION,
        **provenance,
        "source_entity_handle": source_handle,
        "layout_name": layout_name,
        "layer_name": layer_name,
        "block_name": block_name,
        "parent_entity_id": None,
        "drawing_revision_id": None,
        "source_file_id": None,
        "layout_ref": layout_name,
        "layer_ref": layer_name,
        "block_ref": block_name,
        "parent_entity_ref": None,
        "bbox": bbox,
        "geometry": geometry,
        "properties": entity_properties,
        "provenance": provenance,
        "confidence": {
            "score": _LINE_ENTITY_CONFIDENCE_SCORE,
            "review_required": True,
            "basis": f"{confidence_basis_prefix}_{_units_basis_suffix(units)}",
        },
        **extra_fields,
    }


def _extract_text_value(record: Mapping[str, Any]) -> str:
    raw_text = _first_raw_string(
        record,
        "text",
        "text_value",
        "textstring",
        "string",
        "content",
        "label",
    )
    if raw_text is not None:
        return _sanitize_text_content(raw_text)

    fallback = _first_raw_string(record, "value", "title", "description", "notes")
    if fallback is None:
        return ""
    return _sanitize_text_content(fallback)


def _build_unknown_entity(
    record: Mapping[str, Any], *, reason: str, units: _UnitsResolution
) -> _JSONDict:
    record_type = _extract_record_type(record) or "UNKNOWN"
    layout_name = _extract_layout_name(record)
    layer_name = _extract_layer_name(record)
    block_name = _extract_block_name(record)
    source_handle = _extract_handle(record)
    safe_projection = _safe_record_projection(record, record_type=record_type, geometry=None)
    provenance = _entity_provenance(
        record,
        record_type=record_type,
        source_handle=source_handle,
        safe_projection=safe_projection,
        notes=(*_units_notes(units), reason),
    )

    return {
        "entity_id": _entity_id(record, record_type="unknown"),
        "entity_type": "unknown",
        "entity_schema_version": _SCHEMA_VERSION,
        **provenance,
        "source_entity_handle": source_handle,
        "layout_name": layout_name,
        "layer_name": layer_name,
        "block_name": block_name,
        "parent_entity_id": None,
        "drawing_revision_id": None,
        "source_file_id": None,
        "layout_ref": layout_name,
        "layer_ref": layer_name,
        "block_ref": block_name,
        "parent_entity_ref": None,
        "bbox": None,
        "geometry": {
            "bbox": None,
            "units": _units_label(units),
            "status": "absent",
            "reason": reason,
            "geometry_summary": {
                "kind": "unknown",
                "source_type": record_type,
                "reason": reason,
            },
        },
        "quantity_hints": {},
        "adapter_native": {
            "section": "OBJECTS",
            "record_type": record_type,
            "handle": source_handle,
        },
        "provenance": provenance,
        "confidence": {
            "score": _UNKNOWN_ENTITY_CONFIDENCE_SCORE,
            "review_required": True,
            "basis": reason,
        },
        "kind": "unknown",
        "geometry_reason": reason,
        "unknown_reason": reason,
    }


def _normalize_record_type_token(raw_type: str) -> str | None:
    token = raw_type.strip()
    if not token:
        return None
    for separator in ("::", "/", "."):
        if separator in token:
            token = token.split(separator)[-1]
    token = token.replace("-", "_").replace(" ", "_")
    token_upper = token.upper()
    if token_upper.startswith("DWG_TYPE_"):
        token_upper = token_upper.removeprefix("DWG_TYPE_")
    if token_upper.startswith("ACDB"):
        token_upper = token_upper.removeprefix("ACDB")
    return token_upper or None


def _extract_record_type(record: Mapping[str, Any]) -> str | None:
    current_record_type = _extract_record_type_from_current_mapping(record)
    if current_record_type is not None:
        return current_record_type
    return _extract_record_type_from_wrapper_views(record)


def _extract_record_type_from_current_mapping(record: Mapping[str, Any]) -> str | None:
    raw_type = _first_string_from_mapping_by_priority(
        record,
        "entity",
        "object",
        "dxfname",
        "dxf_name",
    )
    if raw_type is not None:
        return _normalize_record_type_token(raw_type)

    legacy_type = _first_string_from_mapping_by_priority(record, "type")
    legacy_type_upper = _normalize_record_type_token(legacy_type) if legacy_type else None
    if legacy_type_upper is None:
        fallback_type = _first_string_from_mapping_by_priority(
            record,
            "object_type",
            "entity_type",
            "fixedtype",
            "fixed_type",
            "name",
            "dxf_name",
            "class",
        )
        if fallback_type is None:
            return None
        return _normalize_record_type_token(fallback_type)

    if legacy_type_upper in _NON_DRAWABLE_OBJECT_TYPES:
        fallback_type = _first_string_from_mapping_by_priority(
            record,
            "object_type",
            "entity_type",
            "fixedtype",
            "fixed_type",
            "class",
        )
        if fallback_type is not None:
            fallback_type_upper = _normalize_record_type_token(fallback_type)
            if (
                fallback_type_upper is not None
                and fallback_type_upper not in _NON_DRAWABLE_OBJECT_TYPES
            ):
                return fallback_type_upper

    return legacy_type_upper


def _extract_record_type_from_wrapper_views(record: Mapping[str, Any]) -> str | None:
    return _extract_record_type_from_nested_wrappers(record, seen=set())


def _extract_record_type_from_nested_wrappers(
    record: Mapping[str, Any], *, seen: set[int]
) -> str | None:
    record_id = id(record)
    if record_id in seen:
        return None
    seen.add(record_id)

    for wrapper_key in _WRAPPER_KEYS:
        nested_value = _mapping_get(record, wrapper_key)
        if not isinstance(nested_value, Mapping):
            continue
        nested_record = cast(Mapping[str, Any], nested_value)
        nested_record_type = _extract_record_type_from_current_mapping(nested_record)
        if nested_record_type is not None:
            if nested_record_type in _NON_DRAWABLE_OBJECT_TYPES:
                continue
            return nested_record_type

        deeper_record_type = _extract_record_type_from_nested_wrappers(
            nested_record,
            seen=seen,
        )
        if deeper_record_type is not None:
            return deeper_record_type

    return None


def _first_string_from_mapping_by_priority(
    mapping: Mapping[str, Any], *candidates: str
) -> str | None:
    for candidate in candidates:
        value = _mapping_get(mapping, candidate)
        if isinstance(value, str):
            sanitized = _sanitize_string_value(value)
            if sanitized is not None:
                return sanitized
    return None


def _extract_handle(record: Mapping[str, Any]) -> str | None:
    raw_handle = _first_value(
        record,
        "handle",
        "entity_handle",
        "object_handle",
        "id",
        "entity_id",
        "object_id",
    )
    if raw_handle is None:
        return None
    return _sanitize_string_value(str(raw_handle))


def _extract_layer_name(record: Mapping[str, Any]) -> str | None:
    return _first_string(record, "layer", "layer_name", "owner_layer")


def _extract_layout_name(record: Mapping[str, Any]) -> str | None:
    return _first_string(record, "layout", "layout_name", "owner_layout") or _DEFAULT_LAYOUT_NAME


def _extract_block_name(record: Mapping[str, Any]) -> str | None:
    return _first_string(record, "block", "block_name", "owner_block")


def _extract_point(
    record: Mapping[str, Any], *, prefixes: tuple[str, ...]
) -> tuple[float, float, float] | None:
    for prefix in prefixes:
        point_value = _first_value(record, prefix)
        point = _coerce_point(point_value)
        if point is not None:
            return point

        x_value = _first_value(record, f"{prefix}_x", f"{prefix}.x", f"{prefix}x")
        y_value = _first_value(record, f"{prefix}_y", f"{prefix}.y", f"{prefix}y")
        z_value = _first_value(record, f"{prefix}_z", f"{prefix}.z", f"{prefix}z")
        if x_value is None or y_value is None:
            continue

        point = _coerce_point((x_value, y_value, z_value if z_value is not None else 0.0))
        if point is not None:
            return point
    return None


def _coerce_point(value: Any) -> tuple[float, float, float] | None:
    if isinstance(value, Mapping):
        x_value = _mapping_get(value, "x", "10", "0")
        y_value = _mapping_get(value, "y", "20", "1")
        z_value = _mapping_get(value, "z", "30", "2")
        if x_value is None or y_value is None:
            return None
        return _coerce_point((x_value, y_value, z_value if z_value is not None else 0.0))
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        x_value = _coerce_float(value[0])
        y_value = _coerce_float(value[1])
        z_value = _coerce_float(value[2]) if len(value) >= 3 else 0.0
        if x_value is None or y_value is None or z_value is None:
            return None
        return (x_value, y_value, z_value)
    return None


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        try:
            return float(candidate)
        except ValueError:
            return None
    return None


def _point_is_finite(point: tuple[float, float, float]) -> bool:
    return all(math.isfinite(component) for component in point)


def _point_json(point: tuple[float, float, float]) -> _JSONDict:
    return {"x": point[0], "y": point[1], "z": point[2]}


def _bbox_from_points(points: tuple[tuple[float, float, float], ...]) -> _JSONDict | None:
    if not points or any(not _point_is_finite(point) for point in points):
        return None
    bbox: _JSONDict = {
        "min": {
            "x": min(point[0] for point in points),
            "y": min(point[1] for point in points),
            "z": min(point[2] for point in points),
        },
        "max": {
            "x": max(point[0] for point in points),
            "y": max(point[1] for point in points),
            "z": max(point[2] for point in points),
        },
    }
    return bbox if _bbox_is_finite(bbox) else None


def _extract_positive_float(record: Mapping[str, Any], *candidates: str) -> float | None:
    value = _first_value(record, *candidates)
    number = _coerce_float(value)
    if number is None or not math.isfinite(number) or number <= 0.0:
        return None
    return number


def _extract_angle_degrees(record: Mapping[str, Any], *candidates: str) -> float | None:
    value = _first_value(record, *candidates)
    angle = _coerce_float(value)
    if angle is None or not math.isfinite(angle):
        return None
    return _normalize_angle_degrees(angle)


def _normalize_angle_degrees(angle: float) -> float:
    normalized = math.fmod(angle, 360.0)
    if normalized < 0.0:
        normalized += 360.0
    if normalized == -0.0:
        return 0.0
    return normalized


def _arc_sweep_degrees(start_angle_degrees: float, end_angle_degrees: float) -> float | None:
    sweep = end_angle_degrees - start_angle_degrees
    if sweep < 0.0:
        sweep += 360.0
    if sweep <= 0.0:
        return None
    return sweep


def _point_on_circle(
    center_point: tuple[float, float, float],
    radius: float,
    angle_degrees: float,
) -> tuple[float, float, float] | None:
    radians = math.radians(angle_degrees)
    point = (
        center_point[0] + (radius * math.cos(radians)),
        center_point[1] + (radius * math.sin(radians)),
        center_point[2],
    )
    return point if _point_is_finite(point) else None


def _arc_bbox(
    *,
    center_point: tuple[float, float, float],
    radius: float,
    start_angle_degrees: float,
    sweep_degrees: float,
    start_point: tuple[float, float, float],
    end_point: tuple[float, float, float],
) -> _JSONDict | None:
    bbox_points = [start_point, end_point]
    for cardinal_angle in (0.0, 90.0, 180.0, 270.0):
        if _angle_in_ccw_sweep(cardinal_angle, start_angle_degrees, sweep_degrees):
            point = _point_on_circle(center_point, radius, cardinal_angle)
            if point is None:
                return None
            bbox_points.append(point)
    return _bbox_from_points(tuple(bbox_points))


def _angle_in_ccw_sweep(candidate_angle: float, start_angle: float, sweep_degrees: float) -> bool:
    adjusted_candidate = candidate_angle
    while adjusted_candidate < start_angle:
        adjusted_candidate += 360.0
    return adjusted_candidate <= start_angle + sweep_degrees + 1e-9


def _extract_vertices(value: Any) -> tuple[tuple[float, float, float], ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    vertices: list[tuple[float, float, float]] = []
    for item in value:
        point = _coerce_lwpolyline_vertex(item)
        if point is None:
            return ()
        vertices.append(point)
    return tuple(vertices)


def _mapping_has_nondefault_zero_value(record: Mapping[str, Any], *candidates: str) -> bool:
    normalized_candidates = {_normalize_lookup_key(candidate) for candidate in candidates}
    for view in _record_views(record):
        for key, value in view.items():
            if not isinstance(key, str):
                continue
            if _normalize_lookup_key(key) in normalized_candidates and not _is_default_zero_value(
                value
            ):
                return True
    return False


def _vertices_have_nondefault_zero_value(value: Any, *candidates: str) -> bool:
    if not isinstance(value, (list, tuple)):
        return False
    normalized_candidates = {_normalize_lookup_key(candidate) for candidate in candidates}
    for item in value:
        if not isinstance(item, Mapping):
            continue
        for key, field_value in item.items():
            if not isinstance(key, str):
                continue
            if _normalize_lookup_key(key) in normalized_candidates and not _is_default_zero_value(
                field_value
            ):
                return True
    return False


def _is_default_zero_value(value: Any) -> bool:
    if value is None:
        return True
    number = _coerce_float(value)
    return number is not None and math.isfinite(number) and number == 0.0


def _extract_closed(record: Mapping[str, Any]) -> bool | None:
    closed_value = _first_value(record, "closed", "is_closed", "closed_flag")
    coerced_closed = _coerce_closed_boolish(closed_value)
    if (
        _record_has_any_key(record, "closed", "is_closed", "closed_flag")
        and closed_value is not None
        and coerced_closed is None
    ):
        return None

    raw_flags_value = _first_value(record, "flags", "flag")
    if raw_flags_value is None:
        return coerced_closed if coerced_closed is not None else False

    flags_value = _coerce_float(raw_flags_value)
    if flags_value is None or not math.isfinite(flags_value) or not flags_value.is_integer():
        return None
    flags = int(flags_value)
    if flags < 0 or flags & ~_SUPPORTED_LWPOLYLINE_FLAG_MASK:
        return None

    flags_closed = bool(flags & _SUPPORTED_LWPOLYLINE_FLAG_MASK)
    if coerced_closed is not None and coerced_closed != flags_closed:
        return None
    return coerced_closed if coerced_closed is not None else flags_closed


def _coerce_closed_boolish(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        if value in (0, 1):
            return bool(value)
        return None
    if isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            return None
        integer = int(value)
        if integer in (0, 1):
            return bool(integer)
        return None
    if isinstance(value, str):
        candidate = value.strip()
        if candidate == "0":
            return False
        if candidate == "1":
            return True
    return None


def _coerce_boolish(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return bool(int(value))
    if isinstance(value, str):
        candidate = value.strip().lower()
        if candidate in {"true", "t", "yes", "y", "1", "closed"}:
            return True
        if candidate in {"false", "f", "no", "n", "0", "open"}:
            return False
    return None


def _polyline_length(
    vertices: tuple[tuple[float, float, float], ...], *, closed: bool
) -> float | None:
    length = 0.0
    for start_vertex, end_vertex in pairwise(vertices):
        segment_length = math.dist(start_vertex, end_vertex)
        if not math.isfinite(segment_length):
            return None
        length += segment_length
        if not math.isfinite(length):
            return None
    if closed:
        closing_length = math.dist(vertices[-1], vertices[0])
        if not math.isfinite(closing_length):
            return None
        length += closing_length
        if not math.isfinite(length):
            return None
    return length


def _polygon_area(vertices: tuple[tuple[float, float, float], ...]) -> float | None:
    if len(vertices) < 3:
        return None
    area = 0.0
    closing_vertices = (*vertices, vertices[0])
    for start_vertex, end_vertex in pairwise(closing_vertices):
        area += (start_vertex[0] * end_vertex[1]) - (end_vertex[0] * start_vertex[1])
        if not math.isfinite(area):
            return None
    normalized_area = abs(area) * 0.5
    return normalized_area if math.isfinite(normalized_area) else None


def _coerce_lwpolyline_vertex(value: Any) -> tuple[float, float, float] | None:
    if isinstance(value, (list, tuple)) and len(value) != 2:
        return None
    return _coerce_point(value)


def _extract_hatch_vertices(record: Mapping[str, Any]) -> _HatchVerticesResult:
    if _hatch_is_non_solid_fill(record):
        return _HatchVerticesResult(None, reason="unsupported_hatch_geometry")

    if _record_has_any_key(record, "boundary_loops", "boundaryloops", "loops"):
        return _extract_hatch_vertices_from_loops(
            _first_value(record, "boundary_loops", "boundaryloops", "loops"),
            owner=record,
        )

    if _record_has_any_key(record, "vertices", "points"):
        return _extract_hatch_vertices_from_points(
            _first_value(record, "vertices", "points"),
            owner=record,
        )

    if _record_has_any_key(record, "edges", "boundary_edges", "boundaryedges"):
        return _extract_hatch_vertices_from_edges(
            _first_value(record, "edges", "boundary_edges", "boundaryedges")
        )

    return _HatchVerticesResult(None, reason="unsupported_hatch_geometry")


def _extract_hatch_vertices_from_loops(
    raw_loops: Any,
    *,
    owner: Mapping[str, Any] | None = None,
) -> _HatchVerticesResult:
    if not isinstance(raw_loops, (list, tuple)):
        return _HatchVerticesResult(None, reason="malformed_hatch_geometry", malformed=True)
    if len(raw_loops) != 1:
        return _HatchVerticesResult(None, reason="unsupported_hatch_geometry")

    loop = raw_loops[0]
    if not isinstance(loop, Mapping):
        return _HatchVerticesResult(None, reason="malformed_hatch_geometry", malformed=True)
    if _hatch_loop_flags_are_ambiguous(loop):
        return _HatchVerticesResult(None, reason="unsupported_hatch_geometry")
    if _record_has_any_key(loop, "vertices", "points"):
        raw_vertices = _first_value(loop, "vertices", "points")
        if owner is not None:
            boundary_validation = _validate_hatch_point_boundary(owner, raw_vertices)
            if boundary_validation is not None:
                return boundary_validation
        return _extract_hatch_vertices_from_points(
            raw_vertices,
            owner=loop,
        )
    if _record_has_any_key(loop, "edges", "boundary_edges", "boundaryedges"):
        return _extract_hatch_vertices_from_edges(
            _first_value(loop, "edges", "boundary_edges", "boundaryedges")
        )
    return _HatchVerticesResult(None, reason="unsupported_hatch_geometry")


def _extract_hatch_vertices_from_points(
    raw_vertices: Any,
    *,
    owner: Mapping[str, Any] | None = None,
) -> _HatchVerticesResult:
    if not isinstance(raw_vertices, (list, tuple)):
        return _HatchVerticesResult(None, reason="malformed_hatch_geometry", malformed=True)
    if len(raw_vertices) > _MAX_HATCH_BOUNDARY_COMPONENTS:
        return _HatchVerticesResult(None, reason="unsupported_hatch_geometry")
    if owner is not None:
        boundary_validation = _validate_hatch_point_boundary(owner, raw_vertices)
        if boundary_validation is not None:
            return boundary_validation

    vertices: list[tuple[float, float, float]] = []
    for item in raw_vertices:
        point = _coerce_point(item)
        if point is None or not _point_is_finite(point) or point[2] != 0.0:
            return _HatchVerticesResult(None, reason="malformed_hatch_geometry", malformed=True)
        vertices.append((point[0], point[1], 0.0))
    normalized_vertices = _normalize_hatch_vertices(tuple(vertices))
    if normalized_vertices is None:
        return _HatchVerticesResult(None, reason="malformed_hatch_geometry", malformed=True)
    return _HatchVerticesResult(normalized_vertices)


def _extract_hatch_vertices_from_edges(raw_edges: Any) -> _HatchVerticesResult:
    if not isinstance(raw_edges, (list, tuple)):
        return _HatchVerticesResult(None, reason="malformed_hatch_geometry", malformed=True)
    if len(raw_edges) > _MAX_HATCH_BOUNDARY_COMPONENTS:
        return _HatchVerticesResult(None, reason="unsupported_hatch_geometry")

    parsed_edges: list[tuple[tuple[float, float, float], tuple[float, float, float]]] = []
    for edge in raw_edges:
        parsed_edge = _extract_hatch_edge(edge)
        if parsed_edge is None:
            return _HatchVerticesResult(None, reason="malformed_hatch_geometry", malformed=True)
        if isinstance(parsed_edge, str):
            return _HatchVerticesResult(None, reason="unsupported_hatch_geometry")
        parsed_edges.append(parsed_edge)
    ordered_vertices = _order_hatch_edges(tuple(parsed_edges))
    if ordered_vertices is None:
        return _HatchVerticesResult(None, reason="unsupported_hatch_geometry")
    normalized_vertices = _normalize_hatch_vertices(ordered_vertices)
    if normalized_vertices is None:
        return _HatchVerticesResult(None, reason="malformed_hatch_geometry", malformed=True)
    return _HatchVerticesResult(normalized_vertices)


def _validate_hatch_point_boundary(
    owner: Mapping[str, Any],
    raw_vertices: Any,
) -> _HatchVerticesResult | None:
    if not isinstance(raw_vertices, (list, tuple)):
        return _HatchVerticesResult(None, reason="malformed_hatch_geometry", malformed=True)
    mixed_boundary_result = _validate_mixed_hatch_boundary_geometry(owner)
    if mixed_boundary_result is not None:
        return mixed_boundary_result
    if _hatch_edge_is_curved(owner):
        return _HatchVerticesResult(None, reason="unsupported_hatch_geometry")
    if _hatch_mapping_is_explicitly_open(owner):
        return _HatchVerticesResult(None, reason="unsupported_hatch_geometry")
    for item in raw_vertices:
        if not isinstance(item, Mapping):
            continue
        if _record_has_any_key(item, "bulge"):
            return _HatchVerticesResult(None, reason="unsupported_hatch_geometry")
        if _hatch_edge_is_curved(item) or _hatch_mapping_is_explicitly_open(item):
            return _HatchVerticesResult(None, reason="unsupported_hatch_geometry")
    return None


def _validate_mixed_hatch_boundary_geometry(
    owner: Mapping[str, Any],
) -> _HatchVerticesResult | None:
    if not _record_has_any_key(owner, "edges", "boundary_edges", "boundaryedges"):
        return None

    raw_edges = _first_value(owner, "edges", "boundary_edges", "boundaryedges")
    if not isinstance(raw_edges, (list, tuple)):
        return _HatchVerticesResult(None, reason="malformed_hatch_geometry", malformed=True)
    if raw_edges:
        return _HatchVerticesResult(None, reason="unsupported_hatch_geometry")
    return None


def _hatch_loop_flags_are_ambiguous(loop: Mapping[str, Any]) -> bool:
    raw_flags = _first_value(loop, "flags", "flag", "loop_flags", "loopflag")
    if raw_flags is None:
        return False
    if isinstance(raw_flags, bool):
        return False
    if isinstance(raw_flags, (int, float)):
        if not math.isfinite(float(raw_flags)) or not float(raw_flags).is_integer():
            return True
        return int(raw_flags) not in {0, 1}
    if isinstance(raw_flags, str):
        normalized = _normalize_lookup_key(raw_flags)
        return normalized not in {"0", "1", "default", "external", "polyline"}
    return True


def _extract_hatch_edge(
    edge: Any,
) -> tuple[tuple[float, float, float], tuple[float, float, float]] | str | None:
    if not isinstance(edge, Mapping):
        return None
    if _hatch_edge_is_curved(edge):
        return "unsupported"

    edge_type = _first_string(edge, "type", "edge_type", "kind")
    if edge_type is not None and _normalize_lookup_key(edge_type) not in {
        "line",
        "straight",
        "segment",
    }:
        return "unsupported"

    start_point = _extract_point(
        edge,
        prefixes=("start", "start_point", "first_endpoint", "point1", "from"),
    )
    end_point = _extract_point(
        edge,
        prefixes=("end", "end_point", "second_endpoint", "point2", "to"),
    )
    if start_point is None or end_point is None:
        raw_vertices = _first_value(edge, "vertices", "points")
        if isinstance(raw_vertices, (list, tuple)) and len(raw_vertices) == 2:
            start_point = _coerce_point(raw_vertices[0])
            end_point = _coerce_point(raw_vertices[1])
    if start_point is None or end_point is None:
        return None
    if (
        not _point_is_finite(start_point)
        or not _point_is_finite(end_point)
        or start_point[2] != 0.0
        or end_point[2] != 0.0
    ):
        return None
    return ((start_point[0], start_point[1], 0.0), (end_point[0], end_point[1], 0.0))


def _hatch_edge_is_curved(edge: Mapping[str, Any]) -> bool:
    edge_type = _first_string(edge, "type", "edge_type", "kind")
    if edge_type is not None and _normalize_lookup_key(edge_type) in {
        "arc",
        "circle",
        "bulge",
        "ellipse",
        "ellipticarc",
        "spline",
        "bezier",
        "curve",
    }:
        return True
    for key in (
        "bulge",
        "radius",
        "center",
        "start_angle",
        "end_angle",
        "major_axis",
        "minor_axis",
        "control_points",
        "fit_points",
        "knots",
        "degree",
    ):
        if _record_has_any_key(edge, key):
            return True
    return False


def _hatch_is_non_solid_fill(record: Mapping[str, Any]) -> bool:
    solid_value = _first_value(record, "solid", "is_solid", "solid_fill")
    return solid_value is not None and _coerce_boolish(solid_value) is False


def _hatch_mapping_is_explicitly_open(record: Mapping[str, Any]) -> bool:
    closed_value = _first_value(record, "closed", "is_closed", "closed_flag")
    if closed_value is not None and _coerce_closed_boolish(closed_value) is False:
        return True

    open_value = _first_value(record, "open", "is_open", "open_flag")
    return open_value is not None and _coerce_boolish(open_value) is True


def _order_hatch_edges(
    edges: tuple[tuple[tuple[float, float, float], tuple[float, float, float]], ...],
) -> tuple[tuple[float, float, float], ...] | None:
    if len(edges) < 3:
        return None

    ordered_vertices: list[tuple[float, float, float]] = [edges[0][0], edges[0][1]]
    current_point = edges[0][1]
    remaining_edges = list(edges[1:])
    while remaining_edges:
        matches: list[tuple[int, tuple[float, float, float]]] = []
        for index, (start_point, end_point) in enumerate(remaining_edges):
            if _points_match(start_point, current_point):
                matches.append((index, end_point))
            elif _points_match(end_point, current_point):
                matches.append((index, start_point))
        if len(matches) != 1:
            return None
        match_index, next_point = matches[0]
        ordered_vertices.append(next_point)
        current_point = next_point
        remaining_edges.pop(match_index)

    if not _points_match(ordered_vertices[-1], ordered_vertices[0]):
        return None
    return tuple(ordered_vertices[:-1])


def _normalize_hatch_vertices(
    vertices: tuple[tuple[float, float, float], ...],
) -> tuple[tuple[float, float, float], ...] | None:
    if len(vertices) < 3:
        return None
    normalized_vertices: list[tuple[float, float, float]] = []
    for vertex in vertices:
        if normalized_vertices and _points_match(normalized_vertices[-1], vertex):
            continue
        normalized_vertices.append(vertex)
    if len(normalized_vertices) > 1 and _points_match(
        normalized_vertices[0], normalized_vertices[-1]
    ):
        normalized_vertices.pop()
    if len(normalized_vertices) < 3:
        return None
    return tuple(normalized_vertices)


def _points_match(left: tuple[float, float, float], right: tuple[float, float, float]) -> bool:
    return all(
        math.isclose(left_component, right_component, rel_tol=1e-9, abs_tol=1e-9)
        for left_component, right_component in zip(left, right, strict=True)
    )


def _bbox_is_finite(bbox: Mapping[str, Any]) -> bool:
    min_value = _mapping_get(bbox, "min")
    max_value = _mapping_get(bbox, "max")
    if not isinstance(min_value, Mapping) or not isinstance(max_value, Mapping):
        return False

    min_point = _coerce_point(min_value)
    max_point = _coerce_point(max_value)
    return (
        min_point is not None
        and max_point is not None
        and _point_is_finite(min_point)
        and _point_is_finite(max_point)
    )


def _floats_are_finite(*values: float) -> bool:
    return all(math.isfinite(value) for value in values)


def _record_uses_supported_arc_angles(record: Mapping[str, Any]) -> bool:
    angle_unit = _first_string(record, "angle_unit", "angle_units", "angleunit")
    if (
        angle_unit is not None
        and _normalize_lookup_key(angle_unit) not in _SUPPORTED_ARC_ANGLE_UNIT_TOKENS
    ):
        return False

    clockwise_value = _first_value(record, "clockwise", "is_clockwise")
    if clockwise_value is not None:
        clockwise = _coerce_boolish(clockwise_value)
        return clockwise is False

    direction = _first_string(record, "direction", "arc_direction", "orientation")
    if direction is None:
        return True
    normalized_direction = _normalize_lookup_key(direction)
    if normalized_direction in _SUPPORTED_ARC_DIRECTION_TOKENS:
        return True
    if normalized_direction in _UNSUPPORTED_ARC_DIRECTION_TOKENS:
        return False
    return False


def _entity_id(record: Mapping[str, Any], *, record_type: str) -> str:
    handle = _extract_handle(record)
    if handle is not None:
        return f"libredwg-{record_type}-{handle.lower()}"
    record_hash = _hash_json_value(
        _safe_record_projection(record, record_type=record_type.upper(), geometry=None)
    )
    return f"libredwg-{record_type}-{record_hash[:18]}"


def _entity_provenance(
    record: Mapping[str, Any],
    *,
    record_type: str,
    source_handle: str | None,
    safe_projection: _JSONDict,
    notes: tuple[str, ...],
) -> _JSONDict:
    extraction_path = ("OBJECTS", record_type)
    source_locator = _source_locator(record, record_type=record_type)
    source_hash = _canonical_hash_json_value(safe_projection)
    source_identity = source_handle or _entity_id(record, record_type=record_type.lower())
    record_hash = _hash_json_value(safe_projection)
    provenance = cast(
        _JSONDict,
        build_entity_provenance(
            origin="adapter_normalized",
            adapter={"key": _DESCRIPTOR.key},
            source_ref=source_locator,
            source_identity=source_identity,
            source_hash=source_hash,
            extraction_path=list(extraction_path),
            notes=list(notes),
            extra={
                "native": {
                    _DESCRIPTOR.key: {
                        "section": "OBJECTS",
                        "record_type": record_type,
                        "handle": source_handle,
                    }
                },
                "legacy_aliases": {
                    "adapter_key": _DESCRIPTOR.key,
                    "source": source_locator,
                    "source_section": "OBJECTS",
                    "source_entity_ref": source_locator,
                    "source_locator": source_locator,
                    "entity_ref": source_locator,
                    "normalized_source_hash": source_hash,
                    "native_handle": source_handle,
                    "source_entity_handle": source_handle,
                    "record_hash": record_hash,
                },
            },
        ),
    )
    provenance.update(
        {
            "adapter_key": _DESCRIPTOR.key,
            "source": source_locator,
            "source_section": "OBJECTS",
            "source_entity_ref": source_locator,
            "source_locator": source_locator,
            "entity_ref": source_locator,
            "normalized_source_hash": source_hash,
            "native_handle": source_handle,
            "source_entity_handle": source_handle,
            "record_hash": record_hash,
        }
    )
    provenance["extraction_path"] = extraction_path
    provenance["notes"] = notes
    return provenance


def _source_locator(record: Mapping[str, Any], *, record_type: str) -> str:
    handle = _extract_handle(record)
    if handle is not None:
        return f"OBJECTS/{record_type}/{handle}"
    return f"OBJECTS/{record_type}/{_entity_id(record, record_type='record')}"


def _record_views(record: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    views: list[Mapping[str, Any]] = []
    _append_record_views(record, views=views, seen=set())
    return tuple(views)


def _append_record_views(
    record: Mapping[str, Any], *, views: list[Mapping[str, Any]], seen: set[int]
) -> None:
    record_id = id(record)
    if record_id in seen:
        return
    seen.add(record_id)
    views.append(record)

    for wrapper_key in _WRAPPER_KEYS:
        nested_value = _mapping_get(record, wrapper_key)
        if isinstance(nested_value, Mapping):
            _append_record_views(cast(Mapping[str, Any], nested_value), views=views, seen=seen)


def _record_has_any_key(record: Mapping[str, Any], *candidates: str) -> bool:
    normalized_candidates = {_normalize_lookup_key(candidate) for candidate in candidates}
    for view in _record_views(record):
        for key in view:
            if isinstance(key, str) and _normalize_lookup_key(key) in normalized_candidates:
                return True
    return False


def _first_value(record: Mapping[str, Any], *candidates: str) -> Any:
    for view in _record_views(record):
        value = _mapping_get(view, *candidates)
        if value is not None:
            return value
    return None


def _first_raw_string(record: Mapping[str, Any], *candidates: str) -> str | None:
    value = _first_value(record, *candidates)
    if isinstance(value, str):
        return value
    return None


def _first_string(record: Mapping[str, Any], *candidates: str) -> str | None:
    value = _first_value(record, *candidates)
    if not isinstance(value, str):
        return None
    return _sanitize_string_value(value)


def _sanitize_text_content(value: str) -> str:
    return "".join(
        character if (character.isprintable() or character == "\n") else "?" for character in value
    )


def _mapping_get(mapping: Mapping[str, Any], *candidates: str) -> Any:
    normalized_candidates = {_normalize_lookup_key(candidate) for candidate in candidates}
    for key, value in mapping.items():
        if not isinstance(key, str):
            continue
        if _normalize_lookup_key(key) in normalized_candidates:
            return value
    return None


def _normalize_lookup_key(value: str) -> str:
    return "".join(character for character in value.lower() if character.isalnum())


def _sanitize_string_value(value: str) -> str | None:
    sanitized = "".join(
        character
        for character in value.strip()
        if character.isprintable() or character in {" ", "_", "-"}
    )
    if not sanitized:
        return None
    if _looks_like_path(sanitized):
        return f"<redacted:{_hash_text(sanitized)[:12]}>"
    return sanitized


def _looks_like_path(value: str) -> bool:
    return (
        "/" in value
        or "\\" in value
        or value.startswith("~")
        or (len(value) >= 2 and value[1] == ":")
    )


def _safe_record_projection(
    record: Mapping[str, Any],
    *,
    record_type: str,
    geometry: _JSONDict | None,
) -> _JSONDict:
    projection: _JSONDict = {
        "record_type": record_type,
        "handle": _extract_handle(record),
        "layer_name": _extract_layer_name(record),
        "layout_name": _extract_layout_name(record),
        "block_name": _extract_block_name(record),
    }
    if geometry is not None:
        projection["geometry"] = geometry
    return projection


def _hash_json_value(value: JSONValue) -> str:
    return sha256_canonical_json_prefixed(value)


def _canonical_hash_json_value(value: JSONValue) -> str:
    return sha256_canonical_json_hex(value)


def _hash_text(value: str) -> str:
    return sha256_text_hex(value)


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
