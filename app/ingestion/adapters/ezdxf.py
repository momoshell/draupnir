"""Concrete DXF ingestion adapter backed by ezdxf."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import dataclass
from itertools import pairwise
from math import isfinite, sqrt
from pathlib import Path
from typing import Any

from app.ingestion.contracts import (
    AdapterAvailability,
    AdapterDescriptor,
    AdapterDiagnostic,
    AdapterExecutionOptions,
    AdapterResult,
    AdapterSource,
    AdapterTimeout,
    AdapterWarning,
    CancellationHandle,
    ConfidenceSummary,
    InputFamily,
    JSONValue,
    ProbeKind,
    ProbeObservation,
    ProbeStatus,
    ProgressCallback,
    ProgressUpdate,
    ProvenanceRecord,
)
from app.ingestion.registry import evaluate_availability, get_descriptor

_DESCRIPTOR = get_descriptor(InputFamily.DXF)
_CANONICAL_SCHEMA_VERSION = "0.1"
_METER_UNITS = 6
_MODEL_LAYOUT_NAME = "Model"
_MAX_POLYLINE_VERTICES = 10_000
_UNIT_NAMES: dict[int, str] = {
    0: "unitless",
    1: "inch",
    2: "foot",
    3: "mile",
    4: "millimeter",
    5: "centimeter",
    6: "meter",
    7: "kilometer",
    8: "microinch",
    9: "mil",
    10: "yard",
    11: "angstrom",
    12: "nanometer",
    13: "micron",
    14: "decimeter",
    15: "decameter",
    16: "hectometer",
    17: "gigameter",
    18: "astronomical_unit",
    19: "light_year",
    20: "parsec",
    21: "us_survey_foot",
    22: "us_survey_inch",
    23: "us_survey_yard",
    24: "us_survey_mile",
}


@dataclass(frozen=True, slots=True)
class _RuntimeBindings:
    ezdxf: Any
    units: Any
    dxf_structure_error: type[Exception]


@dataclass(frozen=True, slots=True)
class _CheckpointBudget:
    timeout: AdapterTimeout | None
    cancellation: CancellationHandle | None
    started_at: float

    @classmethod
    def start(cls, options: AdapterExecutionOptions) -> _CheckpointBudget:
        return cls(
            timeout=options.timeout,
            cancellation=options.cancellation,
            started_at=time.monotonic(),
        )

    def check(self) -> None:
        if self.cancellation is not None and self.cancellation.is_cancelled():
            raise asyncio.CancelledError()

        if self.timeout is None:
            return

        elapsed_seconds = time.monotonic() - self.started_at
        if elapsed_seconds >= self.timeout.seconds:
            raise TimeoutError("DXF ingestion exceeded the adapter timeout budget.")


class _UnsupportedPolylineError(ValueError):
    """Raised when a DXF polyline uses unsupported simple-geometry semantics."""


class _PolylineLimitError(ValueError):
    """Raised when a DXF polyline exceeds supported adapter limits."""


class EzdxfAdapter:
    """DXF adapter implementation that emits canonical v0.1 payloads."""

    descriptor: AdapterDescriptor = _DESCRIPTOR

    def __init__(self, runtime: _RuntimeBindings) -> None:
        self._runtime = runtime
        self.version = str(runtime.ezdxf.__version__)

    def probe(self) -> AdapterAvailability:
        started_at = time.monotonic()
        observation = ProbeObservation(
            kind=ProbeKind.PYTHON_PACKAGE,
            name="ezdxf",
            status=ProbeStatus.AVAILABLE,
            detail=f"ezdxf {self.version} is installed.",
        )
        return evaluate_availability(
            self.descriptor,
            observations=(observation,),
            details={
                "package": "ezdxf",
                "package_version": self.version,
            },
            probe_elapsed_ms=(time.monotonic() - started_at) * 1000,
        )

    async def ingest(
        self,
        source: AdapterSource,
        options: AdapterExecutionOptions,
    ) -> AdapterResult:
        budget = _CheckpointBudget.start(options)
        budget.check()
        _emit_progress(options.on_progress, stage="load", message="Opening DXF document.")

        load_started_at = time.monotonic()
        document = self._read_document(source.file_path)
        load_elapsed_ms = (time.monotonic() - load_started_at) * 1000
        budget.check()
        units_payload = _units_payload(document, self._runtime.units)

        modelspace_entities = list(document.modelspace())
        total_entities = len(modelspace_entities)
        _emit_progress(
            options.on_progress,
            stage="extract",
            message="Extracting DXF entities.",
            completed=0,
            total=total_entities,
            percent=0.0 if total_entities else 1.0,
        )

        warnings: list[AdapterWarning] = []
        canonical_entities: list[dict[str, JSONValue]] = []
        used_layers: set[str] = set()
        supported_entities = 0
        unsupported_entities = 0
        invalid_geometry_entities = 0
        units_review_required = not _units_are_normalized_to_meter(units_payload)
        if units_review_required:
            warnings.append(
                AdapterWarning(
                    code="units_unconfirmed",
                    message=(
                        "DXF units could not be normalized to meters; "
                        "geometry-derived quantities require review."
                    ),
                    details={
                        "source_value": units_payload["source_value"],
                        "normalized": units_payload["normalized"],
                    },
                )
            )

        extract_started_at = time.monotonic()
        for index, entity in enumerate(modelspace_entities, start=1):
            budget.check()
            layout_name = _layout_name(entity)
            entity_type = str(entity.dxftype()).upper()
            if entity_type == "LINE":
                try:
                    canonical_entity = _line_entity_payload(
                        entity,
                        layout_name=layout_name,
                        units=units_payload,
                    )
                except ValueError as exc:
                    canonical_entity = _unknown_entity_payload(
                        entity,
                        layout_name=layout_name,
                        units=units_payload,
                    )
                    warnings.append(
                        AdapterWarning(
                            code="malformed_coordinates",
                            message=(
                                "DXF entity coordinates were invalid and the entity "
                                "was retained as unknown."
                            ),
                            details={
                                "entity_type": entity_type,
                                "handle": _entity_handle(entity),
                                "layer": _entity_layer(entity),
                                "layout": layout_name,
                                "reason": str(exc),
                            },
                        )
                    )
                    invalid_geometry_entities += 1
                else:
                    supported_entities += 1
            elif entity_type in {"LWPOLYLINE", "POLYLINE"}:
                try:
                    canonical_entity = _polyline_entity_payload(
                        entity,
                        layout_name=layout_name,
                        units=units_payload,
                        budget=budget,
                    )
                except _PolylineLimitError as exc:
                    canonical_entity = _unknown_entity_payload(
                        entity,
                        layout_name=layout_name,
                        units=units_payload,
                    )
                    warnings.append(
                        AdapterWarning(
                            code="polyline_vertex_limit_exceeded",
                            message=(
                                "DXF polyline exceeded the supported vertex limit and "
                                "was retained as unknown."
                            ),
                            details={
                                "entity_type": entity_type,
                                "handle": _entity_handle(entity),
                                "layer": _entity_layer(entity),
                                "layout": layout_name,
                                "reason": str(exc),
                            },
                        )
                    )
                    unsupported_entities += 1
                except _UnsupportedPolylineError as exc:
                    canonical_entity = _unknown_entity_payload(
                        entity,
                        layout_name=layout_name,
                        units=units_payload,
                    )
                    warnings.append(
                        AdapterWarning(
                            code="unsupported_entity",
                            message=(
                                f"DXF polyline entity type '{entity_type}' uses unsupported "
                                "semantics and was retained as unknown."
                            ),
                            details={
                                "entity_type": entity_type,
                                "handle": _entity_handle(entity),
                                "layer": _entity_layer(entity),
                                "layout": layout_name,
                                "reason": str(exc),
                            },
                        )
                    )
                    unsupported_entities += 1
                except ValueError as exc:
                    canonical_entity = _unknown_entity_payload(
                        entity,
                        layout_name=layout_name,
                        units=units_payload,
                    )
                    warnings.append(
                        AdapterWarning(
                            code="malformed_coordinates",
                            message=(
                                "DXF entity coordinates were invalid and the entity "
                                "was retained as unknown."
                            ),
                            details={
                                "entity_type": entity_type,
                                "handle": _entity_handle(entity),
                                "layer": _entity_layer(entity),
                                "layout": layout_name,
                                "reason": str(exc),
                            },
                        )
                    )
                    invalid_geometry_entities += 1
                else:
                    supported_entities += 1
            else:
                canonical_entity = _unknown_entity_payload(
                    entity,
                    layout_name=layout_name,
                    units=units_payload,
                )
                warnings.append(
                    AdapterWarning(
                        code="unsupported_entity",
                        message=(
                            f"Unsupported DXF entity type '{entity_type}' was retained as unknown."
                        ),
                        details={
                            "entity_type": entity_type,
                            "handle": _entity_handle(entity),
                            "layer": _entity_layer(entity),
                            "layout": layout_name,
                        },
                    )
                )
                unsupported_entities += 1

            layer_name = canonical_entity.get("layer")
            if isinstance(layer_name, str) and layer_name:
                used_layers.add(layer_name)

            canonical_entities.append(canonical_entity)
            _emit_progress(
                options.on_progress,
                stage="extract",
                message="Extracting DXF entities.",
                completed=index,
                total=total_entities,
                percent=(index / total_entities) if total_entities else 1.0,
            )

        extract_elapsed_ms = (time.monotonic() - extract_started_at) * 1000
        budget.check()
        xrefs = _xrefs_payload(document)
        unresolved_xref_count = sum(
            1
            for xref in xrefs
            if isinstance(xref.get("status"), str) and xref["status"] == "review_required"
        )
        if unresolved_xref_count > 0:
            warnings.append(
                AdapterWarning(
                    code="xref_unresolved",
                    message="DXF external references require review.",
                    details={"xref_count": unresolved_xref_count},
                )
            )
        _emit_progress(
            options.on_progress,
            stage="finalize",
            message="Preparing canonical DXF payload.",
            completed=total_entities,
            total=total_entities,
            percent=1.0,
        )

        canonical: dict[str, JSONValue] = {
            "canonical_entity_schema_version": _CANONICAL_SCHEMA_VERSION,
            "schema_version": _CANONICAL_SCHEMA_VERSION,
            "units": units_payload,
            "coordinate_system": {
                "name": "local",
                "type": "cartesian",
                "axis_order": "x,y,z",
                "source": "dxf_modelspace_coordinates",
            },
            "layouts": _layouts_payload(document),
            "layers": _layers_payload(document, used_layers=used_layers),
            "blocks": _blocks_payload(document, units=units_payload),
            "entities": tuple(canonical_entities),
            "xrefs": xrefs,
        }
        if not canonical_entities:
            canonical["metadata"] = {
                "empty_entities_reason": "dxf_modelspace_empty",
            }

        review_required = (
            unsupported_entities > 0
            or invalid_geometry_entities > 0
            or units_review_required
            or unresolved_xref_count > 0
        )

        diagnostics = (
            AdapterDiagnostic(
                code="dxf_document_loaded",
                message="DXF document loaded successfully.",
                details={
                    "layout_count": len(tuple(document.layouts)),
                    "modelspace_entity_count": total_entities,
                    "version": str(document.dxfversion),
                },
                elapsed_ms=load_elapsed_ms,
            ),
            AdapterDiagnostic(
                code="dxf_entities_extracted",
                message="DXF entities extracted into canonical output.",
                details={
                    "entity_count": total_entities,
                    "supported_entity_count": supported_entities,
                    "unsupported_entity_count": unsupported_entities,
                    "invalid_geometry_entity_count": invalid_geometry_entities,
                    "unresolved_xref_count": unresolved_xref_count,
                },
                elapsed_ms=extract_elapsed_ms,
            ),
        )

        provenance = (
            ProvenanceRecord(
                stage="extract",
                adapter_key=self.descriptor.key,
                source_ref=_source_ref(source),
                details={
                    "upload_format": source.upload_format.value,
                    "input_family": source.input_family.value,
                    "original_name": _safe_original_name(source.original_name),
                    "modelspace_entity_count": total_entities,
                },
            ),
        )

        return AdapterResult(
            canonical=canonical,
            provenance=provenance,
            confidence=ConfidenceSummary(
                score=0.99 if not review_required else 0.95,
                review_required=review_required,
                basis="native_dxf_geometry",
            ),
            warnings=tuple(warnings),
            diagnostics=diagnostics,
        )

    def _read_document(self, file_path: Path) -> Any:
        try:
            return self._runtime.ezdxf.readfile(file_path)
        except OSError as exc:
            raise RuntimeError("DXF source could not be opened.") from exc
        except UnicodeDecodeError as exc:
            raise RuntimeError("DXF source encoding could not be decoded.") from exc
        except Exception as exc:
            if isinstance(exc, self._runtime.dxf_structure_error):
                raise RuntimeError("DXF source structure is invalid.") from exc
            raise


def create_adapter() -> EzdxfAdapter:
    """Create the concrete DXF adapter instance for loader consumers."""
    try:
        return EzdxfAdapter(_load_runtime())
    except Exception as exc:
        raise RuntimeError("DXF adapter runtime could not be loaded.") from exc


def _load_runtime() -> _RuntimeBindings:
    import ezdxf
    from ezdxf import units
    from ezdxf.lldxf.const import DXFStructureError

    return _RuntimeBindings(
        ezdxf=ezdxf,
        units=units,
        dxf_structure_error=DXFStructureError,
    )


def _emit_progress(
    callback: ProgressCallback | None,
    *,
    stage: str,
    message: str,
    completed: int | None = None,
    total: int | None = None,
    percent: float | None = None,
) -> None:
    if callback is None:
        return

    callback(
        ProgressUpdate(
            stage=stage,
            message=message,
            completed=completed,
            total=total,
            percent=percent,
        )
    )


def _units_payload(document: Any, units_module: Any) -> dict[str, JSONValue]:
    source_value = int(document.header.get("$INSUNITS", document.units))
    conversion_factor_to_meter: float | None = None
    if source_value != 0:
        try:
            candidate = float(units_module.conversion_factor(source_value, _METER_UNITS))
        except Exception:
            pass
        else:
            if isfinite(candidate) and candidate > 0:
                conversion_factor_to_meter = candidate

    normalized = (
        "meter"
        if conversion_factor_to_meter is not None
        else _normalized_unit_name(source_value)
    )

    payload: dict[str, JSONValue] = {
        "normalized": normalized,
        "source": "$INSUNITS",
        "source_value": source_value,
        "conversion_target": "meter",
        "conversion_factor": conversion_factor_to_meter,
    }
    return payload


def _normalized_unit_name(source_value: int) -> str:
    return _UNIT_NAMES.get(source_value, "unitless")


def _units_are_normalized_to_meter(units: dict[str, JSONValue]) -> bool:
    return _unit_scale_factor(units) is not None


def _layouts_payload(document: Any) -> tuple[dict[str, JSONValue], ...]:
    layouts: list[dict[str, JSONValue]] = []
    for layout in document.layouts:
        layouts.append(
            {
                "name": str(layout.name),
                "kind": "modelspace" if str(layout.name) == _MODEL_LAYOUT_NAME else "paperspace",
                "tab_order": int(getattr(layout.dxf, "taborder", 0)),
            }
        )
    return tuple(layouts)


def _layers_payload(document: Any, *, used_layers: set[str]) -> tuple[dict[str, JSONValue], ...]:
    layers: list[dict[str, JSONValue]] = []
    for layer in document.layers:
        layer_name = str(layer.dxf.name)
        if used_layers and layer_name not in used_layers:
            continue
        layers.append(
            {
                "name": layer_name,
                "color": int(layer.color),
                "linetype": str(layer.dxf.linetype),
            }
        )
    return tuple(layers)


def _blocks_payload(
    document: Any,
    *,
    units: dict[str, JSONValue],
) -> tuple[dict[str, JSONValue], ...]:
    blocks: list[dict[str, JSONValue]] = []
    for block in document.blocks:
        if bool(getattr(block.block, "is_xref", False)):
            continue
        block_name = str(block.name)
        if block_name.startswith("*"):
            continue
        try:
            base_point: dict[str, JSONValue] | None = _scaled_point_payload(
                block.block.dxf.base_point,
                units=units,
            )
        except ValueError:
            base_point = None
        blocks.append(
            {
                "name": block_name,
                "base_point": base_point,
                "entity_count": len(list(block)),
            }
        )
    return tuple(blocks)


def _xrefs_payload(document: Any) -> tuple[dict[str, JSONValue], ...]:
    xrefs: list[dict[str, JSONValue]] = []
    for block in document.blocks:
        if not bool(getattr(block.block, "is_xref", False)):
            continue
        xref_path = getattr(block.block.dxf, "xref_path", "")
        reference = _safe_reference_name(xref_path)
        path_hash = _redacted_path_hash(xref_path)
        xrefs.append(
            {
                "name": str(block.name),
                "reference": reference,
                "path_sha256": path_hash,
                "status": "review_required",
            }
        )
    return tuple(xrefs)


def _line_entity_payload(
    entity: Any,
    *,
    layout_name: str,
    units: dict[str, JSONValue],
) -> dict[str, JSONValue]:
    native_type = str(entity.dxftype()).upper()
    handle = _entity_handle(entity)
    layer_name = _entity_layer(entity)
    start_native = _point_payload(entity.dxf.start)
    end_native = _point_payload(entity.dxf.end)
    start = _scaled_point_payload(entity.dxf.start, units=units, point_payload=start_native)
    end = _scaled_point_payload(entity.dxf.end, units=units, point_payload=end_native)
    native_length = _line_length(start_native, end_native)
    length = _line_length(start, end)
    adapter_native: dict[str, JSONValue] = {
        "layer": layer_name,
        "linetype": _optional_string(getattr(entity.dxf, "linetype", None)),
    }
    if _native_geometry_should_be_preserved(units):
        adapter_native["geometry"] = {
            "start": start_native,
            "end": end_native,
            "length": native_length,
            "units": {
                "normalized": _normalized_unit_name(_source_unit_value(units)),
                "source_value": _source_unit_value(units),
            },
        }
    entity_id = _entity_id(
        native_type=native_type,
        handle=handle,
        layout_name=layout_name,
        layer_name=layer_name,
        geometry={"start": start, "end": end},
    )
    return {
        "entity_id": entity_id,
        "entity_type": "line",
        "entity_schema_version": _CANONICAL_SCHEMA_VERSION,
        "geometry": {
            "start": start,
            "end": end,
            "bbox": _bbox_payload(start, end),
            "units": dict(units),
            "geometry_summary": {
                "kind": "line_segment",
                "length": length,
                "vertex_count": 2,
            },
        },
        "properties": {
            "source_type": native_type,
            "source_handle": handle or None,
            "quantity_hints": {
                "length": length,
                "count": 1.0,
            },
            "adapter_native": {
                "ezdxf": adapter_native
            },
        },
        "provenance": _entity_provenance(
            native_type=native_type,
            handle=handle,
            layout_name=layout_name,
            layer_name=layer_name,
            entity_id=entity_id,
            geometry={"start": start, "end": end},
        ),
        "confidence": 0.99,
        "drawing_revision_id": None,
        "source_file_id": None,
        "layout_ref": layout_name,
        "layer_ref": layer_name,
        "block_ref": None,
        "parent_entity_ref": None,
        "kind": "line",
        "handle": handle,
        "layer": layer_name,
        "layout": layout_name,
        "start": start,
        "end": end,
        "length": length,
    }


def _unknown_entity_payload(
    entity: Any,
    *,
    layout_name: str,
    units: dict[str, JSONValue],
) -> dict[str, JSONValue]:
    native_type = str(entity.dxftype()).upper()
    handle = _entity_handle(entity)
    layer_name = _entity_layer(entity)
    entity_id = _entity_id(
        native_type=native_type,
        handle=handle,
        layout_name=layout_name,
        layer_name=layer_name,
    )
    return {
        "entity_id": entity_id,
        "entity_type": "unknown",
        "entity_schema_version": _CANONICAL_SCHEMA_VERSION,
        "geometry": {
            "bbox": None,
            "units": dict(units),
            "status": "absent",
            "reason": "unsupported_or_invalid_geometry",
            "geometry_summary": {
                "kind": "unknown",
                "source_type": native_type,
                "reason": "unsupported_or_invalid_geometry",
            },
        },
        "properties": {
            "source_type": native_type,
            "source_handle": handle or None,
            "quantity_hints": None,
            "adapter_native": {
                "ezdxf": {
                    "layer": layer_name,
                }
            },
        },
        "provenance": _entity_provenance(
            native_type=native_type,
            handle=handle,
            layout_name=layout_name,
            layer_name=layer_name,
            entity_id=entity_id,
            geometry=None,
        ),
        "confidence": 0.0,
        "drawing_revision_id": None,
        "source_file_id": None,
        "layout_ref": layout_name,
        "layer_ref": layer_name,
        "block_ref": None,
        "parent_entity_ref": None,
        "kind": "unknown",
        "handle": handle,
        "layer": layer_name,
        "layout": layout_name,
    }


def _polyline_entity_payload(
    entity: Any,
    *,
    layout_name: str,
    units: dict[str, JSONValue],
    budget: _CheckpointBudget,
) -> dict[str, JSONValue]:
    native_type = str(entity.dxftype()).upper()
    handle = _entity_handle(entity)
    layer_name = _entity_layer(entity)
    native_points, closed, adapter_native = _polyline_native_geometry(entity, budget=budget)
    points = _scaled_points_payload(native_points, units=units)
    metric_name = "perimeter" if closed else "length"
    native_measure = _polyline_measure(native_points, closed=closed, budget=budget)
    measure = _polyline_measure(points, closed=closed, budget=budget)
    geometry_payload: dict[str, JSONValue] = {
        "points": points,
        "closed": closed,
    }
    entity_id = _entity_id(
        native_type=native_type,
        handle=handle,
        layout_name=layout_name,
        layer_name=layer_name,
        geometry=geometry_payload,
    )

    if _native_geometry_should_be_preserved(units):
        adapter_native["geometry"] = {
            "points": native_points,
            "closed": closed,
            metric_name: native_measure,
            "units": {
                "normalized": _normalized_unit_name(_source_unit_value(units)),
                "source_value": _source_unit_value(units),
            },
        }

    return {
        "entity_id": entity_id,
        "entity_type": "polyline",
        "entity_schema_version": _CANONICAL_SCHEMA_VERSION,
        "geometry": {
            "vertices": points,
            "points": points,
            "closed": closed,
            "bbox": _points_bbox_payload(points),
            "units": dict(units),
            "geometry_summary": {
                "kind": "polyline",
                "vertex_count": len(points),
                "closed": closed,
                "dimensionality": _polyline_dimensionality(points, budget=budget),
                metric_name: measure,
            },
        },
        "properties": {
            "source_type": native_type,
            "source_handle": handle or None,
            "quantity_hints": {
                metric_name: measure,
                "count": 1.0,
            },
            "adapter_native": {
                "ezdxf": adapter_native,
            },
        },
        "provenance": _entity_provenance(
            native_type=native_type,
            handle=handle,
            layout_name=layout_name,
            layer_name=layer_name,
            entity_id=entity_id,
            geometry=geometry_payload,
        ),
        "confidence": 0.99,
        "drawing_revision_id": None,
        "source_file_id": None,
        "layout_ref": layout_name,
        "layer_ref": layer_name,
        "block_ref": None,
        "parent_entity_ref": None,
        "kind": "polyline",
        "handle": handle,
        "layer": layer_name,
        "layout": layout_name,
        "vertices": points,
        "points": points,
        "closed": closed,
        metric_name: measure,
    }


def _polyline_native_geometry(
    entity: Any,
    *,
    budget: _CheckpointBudget,
) -> tuple[tuple[dict[str, JSONValue], ...], bool, dict[str, JSONValue]]:
    native_type = str(entity.dxftype()).upper()
    layer_name = _entity_layer(entity)
    linetype = _optional_string(getattr(entity.dxf, "linetype", None))

    if native_type == "LWPOLYLINE":
        if bool(getattr(entity, "has_arc", False)):
            raise _UnsupportedPolylineError("Polyline bulge arcs are not supported.")
        if _polyline_width_is_nonzero(getattr(entity.dxf, "const_width", 0.0)):
            raise _UnsupportedPolylineError("Polyline widths are not supported.")
        if bool(getattr(entity, "has_width", False)):
            for vertex_count, vertex in enumerate(entity, start=1):
                budget.check()
                _enforce_polyline_vertex_limit(vertex_count)
                if _lwpolyline_vertex_has_width(vertex):
                    raise _UnsupportedPolylineError("Polyline widths are not supported.")
        closed = bool(getattr(entity, "is_closed", False))
        points = _points_payload(entity.vertices_in_wcs(), budget=budget)
        if len(points) < 2:
            raise _UnsupportedPolylineError("Polyline must contain at least 2 vertices.")
        return (
            points,
            closed,
            {
                "layer": layer_name,
                "linetype": linetype,
                "flags": int(getattr(entity.dxf, "flags", 0)),
                "closed": closed,
            },
        )

    if native_type != "POLYLINE":
        raise _UnsupportedPolylineError(f"Unsupported polyline entity type '{native_type}'.")

    flags = int(getattr(entity.dxf, "flags", 0))
    if flags & 2:
        raise _UnsupportedPolylineError("Polyline curve-fit vertices are not supported.")
    if flags & 4:
        raise _UnsupportedPolylineError("Polyline spline-fit vertices are not supported.")
    if bool(getattr(entity, "is_polygon_mesh", False)):
        raise _UnsupportedPolylineError("Polyline mesh entities are not supported.")
    if bool(getattr(entity, "is_poly_face_mesh", False)):
        raise _UnsupportedPolylineError("Polyline polyface entities are not supported.")
    if not bool(getattr(entity, "is_2d_polyline", False)) and not bool(
        getattr(entity, "is_3d_polyline", False)
    ):
        raise _UnsupportedPolylineError("Polyline semantics are not supported.")
    if bool(getattr(entity, "has_arc", False)):
        raise _UnsupportedPolylineError("Polyline bulge arcs are not supported.")

    unsupported_vertex_mask = 1 | 2 | 8 | 16 | 64 | 128
    point_payloads: list[dict[str, JSONValue]] = []
    for vertex_count, vertex in enumerate(entity.vertices, start=1):
        budget.check()
        _enforce_polyline_vertex_limit(vertex_count)
        vertex_flags = int(getattr(vertex.dxf, "flags", 0))
        if vertex_flags & unsupported_vertex_mask:
            raise _UnsupportedPolylineError("Polyline vertex semantics are not supported.")
        if _polyline_width_is_nonzero(getattr(vertex.dxf, "start_width", 0.0)) or (
            _polyline_width_is_nonzero(getattr(vertex.dxf, "end_width", 0.0))
        ):
            raise _UnsupportedPolylineError("Polyline widths are not supported.")
        point_payloads.append(_point_payload(vertex.dxf.location))

    native_points = tuple(point_payloads)
    if len(native_points) < 2:
        raise _UnsupportedPolylineError("Polyline must contain at least 2 vertices.")

    closed = bool(getattr(entity, "is_closed", False))
    mode = "3d" if bool(getattr(entity, "is_3d_polyline", False)) else "2d"
    return (
        native_points,
        closed,
        {
            "layer": layer_name,
            "linetype": linetype,
            "flags": flags,
            "closed": closed,
            "mode": mode,
        },
    )


def _points_payload(
    points: Any,
    *,
    budget: _CheckpointBudget,
) -> tuple[dict[str, JSONValue], ...]:
    payloads: list[dict[str, JSONValue]] = []
    for vertex_count, point in enumerate(points, start=1):
        budget.check()
        _enforce_polyline_vertex_limit(vertex_count)
        payloads.append(_point_payload(point))
    if len(payloads) < 2:
        raise _UnsupportedPolylineError("Polyline must contain at least 2 vertices.")
    return tuple(payloads)


def _enforce_polyline_vertex_limit(vertex_count: int) -> None:
    if vertex_count > _MAX_POLYLINE_VERTICES:
        raise _PolylineLimitError(
            f"Polyline vertex count exceeds supported limit of {_MAX_POLYLINE_VERTICES}."
        )


def _polyline_width_is_nonzero(value: Any) -> bool:
    try:
        width = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Polyline width must be finite.") from exc
    if not isfinite(width):
        raise ValueError("Polyline width must be finite.")
    return width != 0.0


def _lwpolyline_vertex_has_width(vertex: Any) -> bool:
    if not hasattr(vertex, "__len__") or not hasattr(vertex, "__getitem__") or len(vertex) < 4:
        return False
    return _polyline_width_is_nonzero(vertex[2]) or _polyline_width_is_nonzero(vertex[3])


def _scaled_points_payload(
    points: tuple[dict[str, JSONValue], ...],
    *,
    units: dict[str, JSONValue],
) -> tuple[dict[str, JSONValue], ...]:
    return tuple(_scaled_point_payload(point, units=units, point_payload=point) for point in points)


def _points_bbox_payload(points: tuple[dict[str, JSONValue], ...]) -> dict[str, JSONValue]:
    x_values = tuple(_point_component(point, "x") for point in points)
    y_values = tuple(_point_component(point, "y") for point in points)
    z_values = tuple(_point_component(point, "z") for point in points)
    return {
        "min": {
            "x": min(x_values),
            "y": min(y_values),
            "z": min(z_values),
        },
        "max": {
            "x": max(x_values),
            "y": max(y_values),
            "z": max(z_values),
        },
    }


def _polyline_measure(
    points: tuple[dict[str, JSONValue], ...],
    *,
    closed: bool,
    budget: _CheckpointBudget,
) -> float:
    metric_name = "perimeter" if closed else "length"
    total = 0.0
    for start, end in pairwise(points):
        budget.check()
        total += _line_length(start, end)
        if not isfinite(total):
            raise ValueError(f"Polyline {metric_name} must be finite.")
    if closed:
        budget.check()
        total += _line_length(points[-1], points[0])
        if not isfinite(total):
            raise ValueError("Polyline perimeter must be finite.")
    return total


def _polyline_dimensionality(
    points: tuple[dict[str, JSONValue], ...],
    *,
    budget: _CheckpointBudget,
) -> int:
    first_z = _point_component(points[0], "z")
    for point in points[1:]:
        budget.check()
        if _point_component(point, "z") != first_z:
            return 3
    return 2


def _entity_provenance(
    *,
    native_type: str,
    handle: str,
    layout_name: str,
    layer_name: str,
    entity_id: str,
    geometry: dict[str, JSONValue] | None,
) -> dict[str, JSONValue]:
    return {
        "source_entity_ref": f"entities.{native_type}:{handle or entity_id}",
        "dxf_handle": handle or None,
        "native_entity_type": native_type,
        "layout_name": layout_name,
        "layer_name": layer_name,
        "normalized_source_hash": _entity_fingerprint(
            native_type=native_type,
            handle=handle,
            layout_name=layout_name,
            layer_name=layer_name,
            geometry=geometry,
        ),
    }


def _entity_id(
    *,
    native_type: str,
    handle: str,
    layout_name: str,
    layer_name: str,
    geometry: dict[str, JSONValue] | None = None,
) -> str:
    if handle:
        return f"dxf:{handle.lower()}"
    fingerprint = _entity_fingerprint(
        native_type=native_type,
        handle=handle,
        layout_name=layout_name,
        layer_name=layer_name,
        geometry=geometry,
    )
    return f"dxf:{fingerprint[:16]}"


def _entity_fingerprint(
    *,
    native_type: str,
    handle: str,
    layout_name: str,
    layer_name: str,
    geometry: dict[str, JSONValue] | None,
) -> str:
    payload = {
        "native_type": native_type,
        "handle": handle,
        "layout_name": layout_name,
        "layer_name": layer_name,
        "geometry": geometry,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _bbox_payload(
    start: dict[str, JSONValue],
    end: dict[str, JSONValue],
) -> dict[str, JSONValue]:
    return {
        "min": {
            "x": min(_point_component(start, "x"), _point_component(end, "x")),
            "y": min(_point_component(start, "y"), _point_component(end, "y")),
            "z": min(_point_component(start, "z"), _point_component(end, "z")),
        },
        "max": {
            "x": max(_point_component(start, "x"), _point_component(end, "x")),
            "y": max(_point_component(start, "y"), _point_component(end, "y")),
            "z": max(_point_component(start, "z"), _point_component(end, "z")),
        },
    }


def _line_length(start: dict[str, JSONValue], end: dict[str, JSONValue]) -> float:
    try:
        length = sqrt(
            (_point_component(end, "x") - _point_component(start, "x")) ** 2
            + (_point_component(end, "y") - _point_component(start, "y")) ** 2
            + (_point_component(end, "z") - _point_component(start, "z")) ** 2
        )
    except OverflowError as exc:
        raise ValueError("Line length must be finite.") from exc
    if not isfinite(length):
        raise ValueError("Line length must be finite.")
    return length


def _point_component(point: dict[str, JSONValue], axis: str) -> float:
    value = point[axis]
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise TypeError(f"Point component '{axis}' must be numeric.")
    return float(value)


def _point_payload(point: Any) -> dict[str, JSONValue]:
    x = _finite_float(point.x, axis="x")
    y = _finite_float(point.y, axis="y")
    z = _finite_float(point.z, axis="z")
    return {
        "x": x,
        "y": y,
        "z": z,
    }


def _scaled_point_payload(
    point: Any,
    *,
    units: dict[str, JSONValue],
    point_payload: dict[str, JSONValue] | None = None,
) -> dict[str, JSONValue]:
    payload = point_payload if point_payload is not None else _point_payload(point)
    scale_factor = _unit_scale_factor(units)
    if scale_factor is None or scale_factor == 1.0:
        return payload

    return {
        "x": _scaled_point_component(payload, axis="x", scale_factor=scale_factor),
        "y": _scaled_point_component(payload, axis="y", scale_factor=scale_factor),
        "z": _scaled_point_component(payload, axis="z", scale_factor=scale_factor),
    }


def _scaled_point_component(
    point: dict[str, JSONValue],
    *,
    axis: str,
    scale_factor: float,
) -> float:
    return _finite_float(_point_component(point, axis) * scale_factor, axis=axis)


def _unit_scale_factor(units: dict[str, JSONValue]) -> float | None:
    value = units.get("conversion_factor")
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    factor = float(value)
    if not isfinite(factor) or factor <= 0:
        return None
    return factor


def _native_geometry_should_be_preserved(units: dict[str, JSONValue]) -> bool:
    scale_factor = _unit_scale_factor(units)
    return scale_factor is not None and scale_factor != 1.0


def _source_unit_value(units: dict[str, JSONValue]) -> int:
    value = units.get("source_value")
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError("DXF source units must be represented as an integer.")
    return value


def _finite_float(value: Any, *, axis: str) -> float:
    numeric = float(value)
    if not isfinite(numeric):
        raise ValueError(f"Point component '{axis}' must be finite.")
    return numeric


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _entity_handle(entity: Any) -> str:
    return str(getattr(entity.dxf, "handle", ""))


def _entity_layer(entity: Any) -> str:
    return str(getattr(entity.dxf, "layer", "0"))


def _layout_name(entity: Any) -> str:
    layout = entity.get_layout()
    return str(layout.name) if layout is not None else _MODEL_LAYOUT_NAME


def _source_ref(source: AdapterSource) -> str:
    original_name = _safe_original_name(source.original_name)
    return f"originals/{original_name or source.file_path.name}"


def _safe_original_name(original_name: str | None) -> str | None:
    if original_name is None:
        return None
    normalized = original_name.replace("\\", "/").split("/")[-1].strip()
    return normalized or None


def _safe_reference_name(reference: Any) -> str | None:
    if reference is None:
        return None
    normalized = str(reference).replace("\\", "/").split("/")[-1].strip()
    return normalized or None


def _redacted_path_hash(reference: Any) -> str | None:
    if reference is None:
        return None
    raw_reference = str(reference).strip()
    if not raw_reference:
        return None
    return hashlib.sha256(raw_reference.encode("utf-8")).hexdigest()


__all__ = ["EzdxfAdapter", "create_adapter"]
