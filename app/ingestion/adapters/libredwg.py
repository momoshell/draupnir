"""Thin LibreDWG DWG adapter backed by the ``dwgread`` CLI."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
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
_LINE_ENTITY_CONFIDENCE_SCORE = 0.72
_MIXED_ENTITY_CONFIDENCE_SCORE = 0.5
_UNKNOWN_ENTITY_CONFIDENCE_SCORE = 0.2
_DEFAULT_LAYOUT_NAME = "Model"
_WRAPPER_KEYS = ("object", "entity", "record", "data", "payload", "value")
_NON_DRAWABLE_OBJECT_TYPES = frozenset(
    {
        "APPID",
        "BLOCK_CONTROL",
        "BLOCK_HEADER",
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


@dataclass(frozen=True, slots=True)
class _CanonicalBuildResult:
    canonical: _JSONDict
    warnings: tuple[AdapterWarning, ...]
    confidence: ConfidenceSummary


@dataclass(frozen=True, slots=True)
class _LiveFileLimit:
    path: Path
    limit_bytes: int
    overflow_message: str


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


def _build_canonical_output(
    *,
    source: AdapterSource,
    run_result: _DwgreadRunResult,
) -> _CanonicalBuildResult:
    entities: list[JSONValue] = []
    layers_seen: set[str] = set()
    supported_line_count = 0
    unsupported_drawable_count = 0
    malformed_drawable_count = 0
    unsupported_types: set[str] = set()
    malformed_handles: set[str] = set()
    drawable_candidate_count = 0

    for record in _iter_object_records(run_result.output_payload):
        record_type = _extract_record_type(record)
        if record_type is None:
            continue
        if record_type in _NON_DRAWABLE_OBJECT_TYPES:
            continue

        drawable_candidate_count += 1
        if record_type == "LINE":
            line_entity = _build_line_entity(record)
            if line_entity is not None:
                entities.append(line_entity)
                supported_line_count += 1
                layer_name = cast(str | None, line_entity.get("layer_name"))
                if layer_name:
                    layers_seen.add(layer_name)
                continue

            malformed_drawable_count += 1
            unknown_entity = _build_unknown_entity(record, reason="malformed_line_geometry")
            entities.append(unknown_entity)
            layer_name = cast(str | None, unknown_entity.get("layer_name"))
            if layer_name:
                layers_seen.add(layer_name)
            handle = cast(str | None, unknown_entity.get("source_entity_handle"))
            if handle is not None:
                malformed_handles.add(handle)
            continue

        unsupported_drawable_count += 1
        unsupported_types.add(record_type)
        unknown_entity = _build_unknown_entity(record, reason="unsupported_drawable_record")
        entities.append(unknown_entity)
        layer_name = cast(str | None, unknown_entity.get("layer_name"))
        if layer_name:
            layers_seen.add(layer_name)

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
            "supported_lines": supported_line_count,
            "unsupported_drawables": unsupported_drawable_count,
            "malformed_drawables": malformed_drawable_count,
        },
    }
    if run_result.output_key_count is not None:
        dwgread_metadata = cast(dict[str, JSONValue], metadata["dwgread"])
        dwgread_metadata["output_key_count"] = run_result.output_key_count
    if not entities:
        metadata["empty_entities_reason"] = "no_drawable_candidates_detected"

    warnings = [_units_unconfirmed_warning()]
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

    has_mixed_entity_outcomes = supported_line_count > 0 and (
        unsupported_drawable_count > 0 or malformed_drawable_count > 0
    )
    if has_mixed_entity_outcomes:
        confidence_score = _MIXED_ENTITY_CONFIDENCE_SCORE
        confidence_basis = "libredwg_dwgread_json_mixed_entity_mapping"
    elif supported_line_count > 0:
        confidence_score = _LINE_ENTITY_CONFIDENCE_SCORE
        confidence_basis = "libredwg_dwgread_json_line_mapping"
    elif entities:
        confidence_score = _UNKNOWN_ENTITY_CONFIDENCE_SCORE
        confidence_basis = "libredwg_dwgread_json_unknown_entity_mapping"
    else:
        confidence_score = _PLACEHOLDER_CONFIDENCE_SCORE
        confidence_basis = "libredwg_dwgread_json_placeholder"

    canonical: _JSONDict = {
        "schema_version": _SCHEMA_VERSION,
        "canonical_entity_schema_version": _SCHEMA_VERSION,
        "units": {"normalized": "unknown"},
        "coordinate_system": {
            "name": "local",
            "type": "cartesian",
            "source": "libredwg_dwgread_json",
        },
        "layouts": ({"name": _DEFAULT_LAYOUT_NAME},),
        "layers": tuple({"name": layer_name} for layer_name in sorted(layers_seen)),
        "blocks": (),
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


def _units_unconfirmed_warning() -> AdapterWarning:
    return AdapterWarning(
        code="libredwg.units_unconfirmed",
        message="LibreDWG JSON output does not confirm drawing units for canonical quantities.",
        details={"normalized_units": "unknown"},
    )


def _iter_object_records(payload: Any) -> tuple[Mapping[str, Any], ...]:
    if isinstance(payload, Mapping):
        objects = _mapping_get(payload, "OBJECTS")
        if objects is None:
            return ()
        return tuple(_iter_mapping_candidates(objects))
    if isinstance(payload, list):
        return tuple(
            item for item in payload if isinstance(item, Mapping)
        )
    return ()


def _iter_mapping_candidates(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, Mapping)]
    if not isinstance(value, Mapping):
        return []
    if _extract_record_type(value) is not None:
        return [cast(Mapping[str, Any], value)]

    candidates: list[Mapping[str, Any]] = []
    for nested_value in value.values():
        if isinstance(nested_value, Mapping):
            candidates.extend(_iter_mapping_candidates(nested_value))
        elif isinstance(nested_value, list):
            candidates.extend(item for item in nested_value if isinstance(item, Mapping))
    return candidates


def _build_line_entity(record: Mapping[str, Any]) -> _JSONDict | None:
    start_point = _extract_point(record, prefixes=("start", "start_point", "point1", "p1", "from"))
    end_point = _extract_point(record, prefixes=("end", "end_point", "point2", "p2", "to"))
    if start_point is None or end_point is None:
        return None

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

    return {
        "entity_id": entity_id,
        "entity_type": "line",
        "entity_schema_version": _SCHEMA_VERSION,
        "source_entity_handle": source_handle,
        "layout_name": layout_name,
        "layer_name": layer_name,
        "block_name": block_name,
        "parent_entity_id": None,
        "drawing_revision_id": None,
        "source_file_id": None,
        "layout_ref": None,
        "layer_ref": None,
        "block_ref": None,
        "parent_entity_ref": None,
        "bbox": bbox,
        "geometry": {
            "start": start,
            "end": end,
            "bbox": bbox,
            "units": {"normalized": "unknown"},
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
        "provenance": _entity_provenance(
            record,
            record_type="LINE",
            source_handle=source_handle,
            safe_projection=safe_projection,
            notes=("units_unconfirmed",),
        ),
        "confidence": {
            "score": _LINE_ENTITY_CONFIDENCE_SCORE,
            "review_required": True,
            "basis": "libredwg_line_mapping_units_unconfirmed",
        },
        "kind": "line",
        "start": start,
        "end": end,
        "length": quantity_length,
    }


def _build_unknown_entity(record: Mapping[str, Any], *, reason: str) -> _JSONDict:
    record_type = _extract_record_type(record) or "UNKNOWN"
    layout_name = _extract_layout_name(record)
    layer_name = _extract_layer_name(record)
    block_name = _extract_block_name(record)
    source_handle = _extract_handle(record)
    safe_projection = _safe_record_projection(record, record_type=record_type, geometry=None)

    return {
        "entity_id": _entity_id(record, record_type="unknown"),
        "entity_type": "unknown",
        "entity_schema_version": _SCHEMA_VERSION,
        "source_entity_handle": source_handle,
        "layout_name": layout_name,
        "layer_name": layer_name,
        "block_name": block_name,
        "parent_entity_id": None,
        "drawing_revision_id": None,
        "source_file_id": None,
        "layout_ref": None,
        "layer_ref": None,
        "block_ref": None,
        "parent_entity_ref": None,
        "bbox": None,
        "geometry": {
            "bbox": None,
            "units": {"normalized": "unknown"},
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
        "provenance": _entity_provenance(
            record,
            record_type=record_type,
            source_handle=source_handle,
            safe_projection=safe_projection,
            notes=("units_unconfirmed", reason),
        ),
        "confidence": {
            "score": _UNKNOWN_ENTITY_CONFIDENCE_SCORE,
            "review_required": True,
            "basis": reason,
        },
        "kind": "unknown",
        "geometry_reason": reason,
        "unknown_reason": reason,
    }


def _extract_record_type(record: Mapping[str, Any]) -> str | None:
    raw_type = _first_string(
        record,
        "type",
        "object_type",
        "entity_type",
        "fixedtype",
        "fixed_type",
        "name",
        "dxf_name",
        "class",
    )
    if raw_type is None:
        return None

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
    source_locator = _source_locator(record, record_type=record_type)
    source_hash = _canonical_hash_json_value(safe_projection)
    return {
        "origin": "adapter_normalized",
        "adapter": {"key": _DESCRIPTOR.key},
        "adapter_key": _DESCRIPTOR.key,
        "source": source_locator,
        "source_section": "OBJECTS",
        "source_ref": source_locator,
        "source_entity_ref": source_locator,
        "source_locator": source_locator,
        "entity_ref": source_locator,
        "source_identity": source_handle or _entity_id(record, record_type=record_type.lower()),
        "source_hash": source_hash,
        "normalized_source_hash": source_hash,
        "native_handle": source_handle,
        "source_entity_handle": source_handle,
        "record_hash": _hash_json_value(safe_projection),
        "extraction_path": ("OBJECTS", record_type),
        "notes": notes,
    }


def _source_locator(record: Mapping[str, Any], *, record_type: str) -> str:
    handle = _extract_handle(record)
    if handle is not None:
        return f"OBJECTS/{record_type}/{handle}"
    return f"OBJECTS/{record_type}/{_entity_id(record, record_type='record')}"


def _record_views(record: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    views: list[Mapping[str, Any]] = [record]
    for wrapper_key in _WRAPPER_KEYS:
        nested_value = _mapping_get(record, wrapper_key)
        if isinstance(nested_value, Mapping):
            views.append(cast(Mapping[str, Any], nested_value))
    return tuple(views)


def _first_value(record: Mapping[str, Any], *candidates: str) -> Any:
    for view in _record_views(record):
        value = _mapping_get(view, *candidates)
        if value is not None:
            return value
    return None


def _first_string(record: Mapping[str, Any], *candidates: str) -> str | None:
    value = _first_value(record, *candidates)
    if not isinstance(value, str):
        return None
    return _sanitize_string_value(value)


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
    return f"sha256:{_canonical_hash_json_value(value)}"


def _canonical_hash_json_value(value: JSONValue) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return _hash_text(payload)


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


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
