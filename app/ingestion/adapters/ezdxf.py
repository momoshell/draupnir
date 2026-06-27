"""Concrete DXF ingestion adapter backed by ezdxf."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import time
from dataclasses import dataclass
from itertools import pairwise
from math import hypot, isfinite, sqrt
from pathlib import Path
from typing import Any, cast

from app.ingestion.adapters._units import METER_SCALE, resolve_units, unit_name
from app.ingestion.canonical import build_entity_provenance
from app.ingestion.canonical.geometry import canonical_bbox_from_points
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
from app.ingestion.units_inference import DimensionObservation, extent_from_bboxes, infer_units

_DESCRIPTOR = get_descriptor(InputFamily.DXF)
_CANONICAL_SCHEMA_VERSION = "0.1"
_MODEL_LAYOUT_NAME = "Model"
_MAX_POLYLINE_VERTICES = 10_000


@dataclass(frozen=True, slots=True)
class _RuntimeBindings:
    ezdxf: Any
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
        units_payload = _units_payload(document)

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

        # Presentation-style context for per-entity color/linetype/lineweight with ByLayer
        # inheritance + dashed detection — same `style` shape as the DWG adapter (#574).
        linetype_table, linetype_dashed = _linetype_table(document)
        layer_styles = _layer_style_table(document)

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
            elif entity_type == "DIMENSION":
                try:
                    canonical_entity = _dimension_entity_payload(
                        entity,
                        layout_name=layout_name,
                        units=units_payload,
                    )
                except Exception as exc:
                    canonical_entity = _unknown_entity_payload(
                        entity,
                        layout_name=layout_name,
                        units=units_payload,
                    )
                    warnings.append(
                        AdapterWarning(
                            code="malformed_dimension",
                            message=(
                                "DXF DIMENSION entity was malformed and was retained as unknown."
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
                else:
                    supported_entities += 1
            elif entity_type == "SPLINE":
                try:
                    canonical_entity = _spline_entity_payload(
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
                                "DXF SPLINE exceeded the supported vertex limit and "
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
                                "DXF SPLINE entity coordinates were invalid and the entity "
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
                else:
                    supported_entities += 1
            elif entity_type == "HATCH":
                try:
                    canonical_entity = _hatch_entity_payload(
                        entity,
                        layout_name=layout_name,
                        units=units_payload,
                    )
                except Exception as exc:
                    canonical_entity = _unknown_entity_payload(
                        entity,
                        layout_name=layout_name,
                        units=units_payload,
                    )
                    warnings.append(
                        AdapterWarning(
                            code="malformed_hatch",
                            message=(
                                "DXF HATCH entity was malformed or used unsupported boundary "
                                "geometry and was retained as unknown."
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

            canonical_entity["style"] = _resolve_entity_style(
                entity, linetypes=linetype_dashed, layers=layer_styles
            )
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

        # --- Unit inference (UNCONFIRMED path only) ---
        # When $INSUNITS is missing/0/exotic (confirmed=False), attempt to infer
        # the native unit from the aggregate entity extent.  The confirmed path
        # (conversion_factor already set) is left byte-for-byte unchanged.
        if units_review_required:
            _sv = units_payload.get("source_value")
            _source_int = (
                int(_sv) if isinstance(_sv, (int, float)) and not isinstance(_sv, bool) else 0
            )
            _apply_unit_inference(
                units_payload=units_payload,
                entities=canonical_entities,
                source_value=_source_int,
                warnings=warnings,
                warning_code_prefix="ezdxf",
                dimension_observations=_dimension_observations_from_entities(canonical_entities),
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
            "linetypes": linetype_table,
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
    from ezdxf.lldxf.const import DXFStructureError

    return _RuntimeBindings(
        ezdxf=ezdxf,
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


def _units_payload(document: Any) -> dict[str, JSONValue]:
    source_value = int(document.header.get("$INSUNITS", document.units))
    resolution = resolve_units(
        source_value,
        allowed_codes=METER_SCALE,
        fallback_label=unit_name,
    )

    payload: dict[str, JSONValue] = {
        "normalized": resolution.normalized,
        "source": "$INSUNITS",
        "source_value": source_value,
        "conversion_target": "meter",
        "conversion_factor": resolution.conversion_factor,
    }
    return payload


def _normalized_unit_name(source_value: int) -> str:
    return unit_name(source_value)


def _apply_unit_inference(
    *,
    units_payload: dict[str, JSONValue],
    entities: list[dict[str, JSONValue]],
    source_value: int,
    warnings: list[AdapterWarning],
    warning_code_prefix: str,
    dimension_observations: tuple[DimensionObservation, ...] = (),
) -> None:
    """Attempt unit inference and mutate *units_payload* in-place.

    Only called on the UNCONFIRMED path (confirmed=False).  The confirmed path
    is never mutated so its exact byte output is preserved.

    Tier-1 (dimension-text) runs first via dimension_observations; Tier-2
    (extent magnitude) is the fallback.
    """
    # In the ezdxf canonical schema the bbox lives under entity["geometry"]["bbox"].
    bboxes: list[dict[str, object]] = []
    for entity in entities:
        geometry = entity.get("geometry")
        if not isinstance(geometry, dict):
            continue
        bbox = geometry.get("bbox")
        if isinstance(bbox, dict):
            bboxes.append(bbox)

    extent = extent_from_bboxes(bboxes)
    result = infer_units(
        declared_code=source_value if source_value else None,
        extent=extent,
        dimension_observations=dimension_observations,
    )

    if result.confidence == "inferred":
        units_payload["confidence"] = "inferred"
        units_payload["conversion_factor"] = result.conversion_factor
        units_payload["normalized"] = result.normalized
        units_payload["source"] = "$INSUNITS"
        units_payload["basis"] = result.basis
        if extent is not None or dimension_observations:
            inference_block: dict[str, JSONValue] = {
                "candidates": tuple(
                    {
                        "normalized": c.normalized,
                        "conversion_factor": c.conversion_factor,
                        "band_label": c.band_label,
                    }
                    for c in result.candidates
                ),
                "basis": result.basis,
                "dimension_observation_count": len(dimension_observations),
            }
            if extent is not None:
                inference_block["extent"] = {"width": extent.width, "height": extent.height}
            units_payload["inference"] = inference_block
        if result.contradicts_declared:
            units_payload["contradiction"] = {
                "declared_normalized": result.declared_normalized,
                "inferred_normalized": result.normalized,
                "basis": result.basis,
            }
            warnings.append(
                AdapterWarning(
                    code=f"{warning_code_prefix}.units_contradiction",
                    message=(
                        "Inferred unit contradicts the declared $INSUNITS code; "
                        "geometry quantities require review."
                    ),
                    details={
                        "declared_normalized": result.declared_normalized,
                        "inferred_normalized": result.normalized,
                        "basis": result.basis,
                    },
                )
            )


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


def _linetype_table(document: Any) -> tuple[tuple[dict[str, JSONValue], ...], dict[str, bool]]:
    """Build the canonical LTYPE table + a name→dashed map (#574).

    A linetype is dashed when its pattern length > 0 (Continuous/ByLayer/ByBlock are 0),
    mirroring the libredwg adapter's ``pattern_len > 0`` rule (#573).
    """
    table: list[dict[str, JSONValue]] = []
    dashed_by_name: dict[str, bool] = {}
    for ltype in getattr(document, "linetypes", ()):
        name = _optional_string(getattr(ltype.dxf, "name", None))
        if name is None:
            continue
        pattern_len = _linetype_pattern_len(ltype)
        dashed = pattern_len > 0
        table.append({"name": name, "pattern_len": pattern_len, "dashed": dashed})
        dashed_by_name[name.lower()] = dashed
    return tuple(table), dashed_by_name


def _linetype_pattern_len(ltype: Any) -> float:
    """Total dash-pattern length (group code 40); 0.0 for solid lines, > 0 for dashed.

    Mirrors the libredwg adapter's ``pattern_len`` (#573). ezdxf does not surface this on
    ``dxf.length``, so it is read from the linetype's pattern tags.
    """
    pattern = getattr(ltype, "pattern_tags", None)
    for tag in getattr(pattern, "tags", ()):
        if getattr(tag, "code", None) == 40:
            try:
                return float(tag.value)
            except (TypeError, ValueError):
                return 0.0
    return 0.0


def _layer_style_table(document: Any) -> dict[str, dict[str, JSONValue]]:
    """Map layer name → its color/linetype/lineweight for ByLayer style resolution."""
    table: dict[str, dict[str, JSONValue]] = {}
    for layer in getattr(document, "layers", ()):
        name = _optional_string(getattr(layer.dxf, "name", None))
        if name is None:
            continue
        table[name] = {
            "color": int(getattr(layer, "color", 256)),
            "rgb": _rgb_hex(getattr(layer, "rgb", None)),
            "linetype": _optional_string(getattr(layer.dxf, "linetype", None)),
            "lineweight": int(getattr(layer.dxf, "lineweight", -3)),
        }
    return table


def _rgb_hex(rgb: Any) -> str | None:
    """Format an ezdxf ``(r, g, b)`` tuple as ``RRGGBB`` hex, or None."""
    if isinstance(rgb, (tuple, list)) and len(rgb) == 3:
        try:
            return "".join(f"{int(c):02x}" for c in rgb)
        except (TypeError, ValueError):
            return None
    return None


def _resolve_entity_style(
    entity: Any,
    *,
    linetypes: dict[str, bool],
    layers: dict[str, dict[str, JSONValue]],
) -> dict[str, JSONValue]:
    """Resolve an entity's effective style (color/linetype/lineweight) with ByLayer inheritance.

    Emits the same ``style`` shape as the libredwg adapter (#573) so the API/MCP surface is
    adapter-agnostic. ACI color 256 = ByLayer, 0 = ByBlock; lineweight < 0 = ByLayer/ByBlock/
    Default; linetype name ``BYLAYER`` inherits the owning layer's linetype.
    """
    dxf = entity.dxf
    layer_name = _optional_string(getattr(dxf, "layer", None))
    layer = layers.get(layer_name) if layer_name is not None else None

    color_index = int(getattr(dxf, "color", 256))
    color_by_layer = color_index == 256
    color_by_block = color_index == 0
    rgb = _rgb_hex(getattr(entity, "rgb", None))
    if color_by_layer and layer is not None:
        color_index = cast(int, layer.get("color", color_index))
        rgb = rgb or cast("str | None", layer.get("rgb"))

    raw_lt = _optional_string(getattr(dxf, "linetype", None))
    lt_by_layer = raw_lt is None or raw_lt.upper() == "BYLAYER"
    effective_lt = (
        cast("str | None", layer.get("linetype")) if lt_by_layer and layer is not None else raw_lt
    )
    dashed = linetypes.get(effective_lt.lower()) if effective_lt is not None else None

    raw_lw = int(getattr(dxf, "lineweight", -1))
    lw_by_layer = raw_lw < 0
    if lw_by_layer and layer is not None:
        layer_lw = cast(int, layer.get("lineweight", -3))
        raw_lw = layer_lw if layer_lw >= 0 else -1
    lw_mm = raw_lw / 100.0 if raw_lw >= 0 else None

    return {
        "color": {
            "index": color_index,
            "rgb": rgb,
            "by_layer": color_by_layer,
            "by_block": color_by_block,
        },
        "linetype": {
            "name": effective_lt,
            "by_layer": lt_by_layer,
            "dashed": dashed,
            "scale": float(getattr(dxf, "ltscale", 1.0) or 1.0),
        },
        "lineweight": {"raw": raw_lw if raw_lw >= 0 else None, "mm": lw_mm},
    }


def _dimension_dimlfac(entity: Any) -> float:
    """Resolve the DIMLFAC display scale factor for a DIMENSION entity.

    Uses ezdxf's DimStyleOverride.get(), which merges entity-level override
    attributes -> named dimstyle -> header defaults in one call.  The
    entity.dxf.get("dimlfac") branch is intentionally absent: dimlfac is not a
    direct DIMENSION dxf attribute and always raises DXFAttributeError there.
    Never raises.
    """
    try:
        val = entity.override().get("dimlfac")
        if val is not None and isinstance(val, (int, float)) and not isinstance(val, bool):
            f = float(val)
            if isfinite(f) and f > 0:
                return f
    except Exception:
        pass

    return 1.0


def _dimension_stated_override(text: str | None) -> float | None:
    """Parse the drafter text override to a float, or return None if unusable.

    '<>' or '' means measured-only (no drafter override).  Non-numeric strings
    are also unusable.
    """
    if text is None or text.strip() in ("<>", ""):
        return None
    try:
        return float(text.strip())
    except (ValueError, TypeError):
        return None


def _dimension_entity_payload(
    entity: Any,
    *,
    layout_name: str,
    units: dict[str, JSONValue],
) -> dict[str, JSONValue]:
    """Build a canonical entity payload for a DXF DIMENSION entity.

    Mirrors the structure of _line_entity_payload; bbox spans defpoint2/defpoint3
    (the linear extension-line definition points).
    """
    native_type = "DIMENSION"
    handle = _entity_handle(entity)
    layer_name = _entity_layer(entity)

    # Raw dimension type; base type = dimtype & 7.
    dimtype_raw = int(getattr(entity.dxf, "dimtype", 0))
    dimtype_base = dimtype_raw & 7
    # base 0 = rotated/horizontal/vertical linear, 1 = aligned linear => is_linear
    is_linear = dimtype_base in (0, 1)

    # Extension-line definition points (defpoint2/defpoint3 for linear dims).
    dp2_raw = getattr(entity.dxf, "defpoint2", None)
    dp3_raw = getattr(entity.dxf, "defpoint3", None)

    def _safe_pt(pt: Any) -> tuple[float, float, float]:
        x = float(getattr(pt, "x", 0.0))
        y = float(getattr(pt, "y", 0.0))
        z = float(getattr(pt, "z", 0.0))
        return x, y, z

    p2x, p2y, p2z = _safe_pt(dp2_raw) if dp2_raw is not None else (0.0, 0.0, 0.0)
    p3x, p3y, p3z = _safe_pt(dp3_raw) if dp3_raw is not None else (0.0, 0.0, 0.0)
    raw_span = hypot(p3x - p2x, p3y - p2y)

    defpoint2_payload: dict[str, JSONValue] = {"x": p2x, "y": p2y, "z": p2z}
    defpoint3_payload: dict[str, JSONValue] = {"x": p3x, "y": p3y, "z": p3z}

    text_override_raw = _optional_string(getattr(entity.dxf, "text", None))
    dimlfac = _dimension_dimlfac(entity)

    # Scale defpoints to meters when a confirmed conversion factor is available.
    dp2_scaled = _scaled_point_payload(None, units=units, point_payload=defpoint2_payload)
    dp3_scaled = _scaled_point_payload(None, units=units, point_payload=defpoint3_payload)

    entity_id = _entity_id(
        native_type=native_type,
        handle=handle,
        layout_name=layout_name,
        layer_name=layer_name,
        geometry={"defpoint2": dp2_scaled, "defpoint3": dp3_scaled},
    )

    bbox = _bbox_payload(dp2_scaled, dp3_scaled)

    adapter_native: dict[str, JSONValue] = {
        "layer": layer_name,
        "linetype": _optional_string(getattr(entity.dxf, "linetype", None)),
        "dimension": {
            "dimtype_base": dimtype_base,
            "is_linear": is_linear,
            "text_override": text_override_raw,
            "raw_span": raw_span,
            "dimlfac": dimlfac,
            "defpoint2": {"x": p2x, "y": p2y, "z": p2z},
            "defpoint3": {"x": p3x, "y": p3y, "z": p3z},
        },
    }

    return {
        "entity_id": entity_id,
        "entity_type": "dimension",
        "entity_schema_version": _CANONICAL_SCHEMA_VERSION,
        "geometry": {
            "defpoint2": dp2_scaled,
            "defpoint3": dp3_scaled,
            "bbox": bbox,
            "units": dict(units),
            "geometry_summary": {
                "kind": "dimension",
                "raw_span": raw_span,
                "is_linear": is_linear,
            },
        },
        "properties": {
            "source_type": native_type,
            "source_handle": handle or None,
            "quantity_hints": {
                "span": raw_span,
                "count": 1.0,
            },
            "adapter_native": {"ezdxf": adapter_native},
        },
        "provenance": _entity_provenance(
            native_type=native_type,
            handle=handle,
            layout_name=layout_name,
            layer_name=layer_name,
            entity_id=entity_id,
            geometry={"defpoint2": dp2_scaled, "defpoint3": dp3_scaled},
        ),
        "confidence": 0.99,
        "drawing_revision_id": None,
        "source_file_id": None,
        "layout_ref": layout_name,
        "layer_ref": layer_name,
        "block_ref": None,
        "parent_entity_ref": None,
        "kind": "dimension",
        "handle": handle,
        "layer": layer_name,
        "layout": layout_name,
    }


def _dimension_observations_from_entities(
    entities: list[dict[str, JSONValue]],
) -> tuple[DimensionObservation, ...]:
    """Walk canonical entities and build DimensionObservation tuples for Tier-1 inference.

    Only processes entities with entity_type=="dimension" that carry the
    adapter_native.ezdxf.dimension sub-block populated by _dimension_entity_payload.
    """
    observations: list[DimensionObservation] = []
    for entity in entities:
        if entity.get("entity_type") != "dimension":
            continue
        try:
            props = entity.get("properties")
            if not isinstance(props, dict):
                continue
            adapter_native = props.get("adapter_native")
            if not isinstance(adapter_native, dict):
                continue
            ezdxf_block = adapter_native.get("ezdxf")
            if not isinstance(ezdxf_block, dict):
                continue
            dim_block = ezdxf_block.get("dimension")
            if not isinstance(dim_block, dict):
                continue

            raw_span_val = dim_block.get("raw_span")
            dimlfac_val = dim_block.get("dimlfac")
            is_linear_val = dim_block.get("is_linear")
            text_override_val = dim_block.get("text_override")

            if not isinstance(raw_span_val, (int, float)) or isinstance(raw_span_val, bool):
                continue
            if not isinstance(dimlfac_val, (int, float)) or isinstance(dimlfac_val, bool):
                continue
            if not isinstance(is_linear_val, bool):
                continue

            stated_override = _dimension_stated_override(
                text_override_val if isinstance(text_override_val, str) else None
            )
            observations.append(
                DimensionObservation(
                    raw_span=float(raw_span_val),
                    stated_override=stated_override,
                    dimlfac=float(dimlfac_val),
                    is_linear=bool(is_linear_val),
                )
            )
        except Exception:
            continue

    return tuple(observations)


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
            "adapter_native": {"ezdxf": adapter_native},
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


def _spline_entity_payload(
    entity: Any,
    *,
    layout_name: str,
    units: dict[str, JSONValue],
    budget: _CheckpointBudget,
) -> dict[str, JSONValue]:
    """Build a canonical 'spline' entity from an ezdxf SPLINE.

    Uses the control polygon (control_points) as the approximation — symmetric with the
    libredwg _build_spline_entity which also uses ctrl_pts (#369 symmetry).  Flattening
    is intentionally avoided so vertex-for-vertex endpoints are byte-comparable across
    adapters and the entity remains purely topological (no length takeoff).
    """
    native_type = "SPLINE"
    handle = _entity_handle(entity)
    layer_name = _entity_layer(entity)
    linetype = _optional_string(getattr(entity.dxf, "linetype", None))

    # ezdxf Spline.control_points is a VertexArray that yields numpy arrays ([x, y, z]).
    # Fall back to fit_points if control_points is empty (fit_points preserve endpoints).
    # Neither → malformed.  (#369: control_polygon approximation, symmetric with libredwg)
    native_points = _spline_control_points_payload(entity, budget=budget)

    closed = bool(getattr(entity, "closed", False))
    flags = int(getattr(entity.dxf, "flags", 0))
    periodic = bool(flags & getattr(entity, "PERIODIC", 2))
    # clamped (non-periodic) splines have exact endpoints on the curve; periodic do not
    endpoints_on_curve = not periodic

    degree_raw = getattr(entity.dxf, "degree", None)
    source_degree = int(degree_raw) if degree_raw is not None else 3

    points = _scaled_points_payload(native_points, units=units)

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
        adapter_native_geometry: dict[str, JSONValue] = {
            "points": native_points,
            "closed": closed,
            "units": {
                "normalized": _normalized_unit_name(_source_unit_value(units)),
                "source_value": _source_unit_value(units),
            },
        }
    else:
        adapter_native_geometry = {}

    adapter_native: dict[str, JSONValue] = {
        "layer": layer_name,
        "linetype": linetype,
        "flags": flags,
        "closed": closed,
        "degree": source_degree,
    }
    if adapter_native_geometry:
        adapter_native["geometry"] = adapter_native_geometry

    geometry_summary: dict[str, JSONValue] = {
        "kind": "spline",
        "vertex_count": len(points),
        "closed": closed,
        "approximation": "control_polygon",
        "source_degree": source_degree,
        "endpoints_on_curve": endpoints_on_curve,
    }

    return {
        "entity_id": entity_id,
        "entity_type": "spline",
        "entity_schema_version": _CANONICAL_SCHEMA_VERSION,
        "geometry": {
            "vertices": points,
            "points": points,
            "closed": closed,
            "bbox": _points_bbox_payload(points),
            "units": dict(units),
            "geometry_summary": geometry_summary,
        },
        "properties": {
            "source_type": native_type,
            "source_handle": handle or None,
            "quantity_hints": {
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
        "kind": "spline",
        "handle": handle,
        "layer": layer_name,
        "layout": layout_name,
        "vertices": points,
        "points": points,
        "closed": closed,
    }


def _spline_control_points_payload(
    entity: Any,
    *,
    budget: _CheckpointBudget,
) -> tuple[dict[str, JSONValue], ...]:
    """Extract control points from an ezdxf Spline entity as {x,y,z} dicts.

    ezdxf Spline.control_points yields numpy arrays indexed positionally ([0]=x,[1]=y,[2]=z).
    Falls back to fit_points when control_points is empty (fit_points preserve endpoints).
    Raises ValueError when fewer than 2 usable points exist.
    """
    raw_ctrl = list(getattr(entity, "control_points", []))
    if raw_ctrl:
        source = raw_ctrl
    else:
        raw_fit = list(getattr(entity, "fit_points", []))
        if not raw_fit:
            raise ValueError("SPLINE has neither control_points nor fit_points.")
        source = raw_fit

    payloads: list[dict[str, JSONValue]] = []
    for vertex_count, pt in enumerate(source, start=1):
        budget.check()
        _enforce_polyline_vertex_limit(vertex_count)
        x = _finite_float(float(pt[0]), axis="x")
        y = _finite_float(float(pt[1]), axis="y")
        z = _finite_float(float(pt[2]), axis="z")
        payloads.append({"x": x, "y": y, "z": z})

    if len(payloads) < 2:
        raise ValueError("SPLINE must have at least 2 control points.")
    return tuple(payloads)


def _hatch_polygon_area(points: list[tuple[float, float]]) -> float | None:
    """Shoelace polygon area for a flat (x, y) loop — matches libredwg's _polygon_area."""
    n = len(points)
    if n < 3:
        return None
    area = 0.0
    for i in range(n):
        x0, y0 = points[i]
        x1, y1 = points[(i + 1) % n]
        area += (x0 * y1) - (x1 * y0)
        if not math.isfinite(area):
            return None
    normalized = abs(area) * 0.5
    return normalized if math.isfinite(normalized) else None


def _hatch_loop_perimeter(points: list[tuple[float, float]]) -> float | None:
    """Closed-loop perimeter for a flat (x, y) point list — matches libredwg's _polyline_length."""
    if len(points) < 2:
        return None
    total = 0.0
    for i in range(len(points)):
        x0, y0 = points[i]
        x1, y1 = points[(i + 1) % len(points)]
        seg = math.hypot(x1 - x0, y1 - y0)
        if not math.isfinite(seg):
            return None
        total += seg
        if not math.isfinite(total):
            return None
    return total


_HATCH_ARC_STEP_DEG = 10.0  # degrees per tessellation chord — matches libredwg's ~10° step


def _tessellate_arc_edge(
    center: tuple[float, float],
    radius: float,
    start_angle_deg: float,
    end_angle_deg: float,
    ccw: bool,
) -> list[tuple[float, float]]:
    """Tessellate a circular arc edge into chord points at ~10° intervals.

    Mirrors libredwg's _tessellate_hatch_arc_seg density (math.pi/18 radians per step).
    """
    two_pi = 2.0 * math.pi
    start_rad = math.radians(start_angle_deg)
    end_rad = math.radians(end_angle_deg)
    if ccw:
        sweep = (end_rad - start_rad) % two_pi or two_pi
    else:
        sweep = -((start_rad - end_rad) % two_pi) or -two_pi
    steps = max(2, math.ceil(abs(sweep) / math.radians(_HATCH_ARC_STEP_DEG)))
    cx, cy = center
    pts: list[tuple[float, float]] = []
    for i in range(steps + 1):
        angle = start_rad + sweep * (i / steps)
        pts.append((cx + radius * math.cos(angle), cy + radius * math.sin(angle)))
    return pts


def _tessellate_ellipse_edge(
    center: tuple[float, float],
    major_axis: tuple[float, float],
    ratio: float,
    start_angle_deg: float,
    end_angle_deg: float,
    ccw: bool,
) -> list[tuple[float, float]]:
    """Tessellate an ellipse edge into chord points at ~10° intervals.

    Mirrors the arc step density.  The ellipse is parametric: the ezdxf
    start_angle/end_angle are the parametric angles measured from the major axis.
    """
    two_pi = 2.0 * math.pi
    start_rad = math.radians(start_angle_deg)
    end_rad = math.radians(end_angle_deg)
    if ccw:
        sweep = (end_rad - start_rad) % two_pi or two_pi
    else:
        sweep = -((start_rad - end_rad) % two_pi) or -two_pi
    steps = max(2, math.ceil(abs(sweep) / math.radians(_HATCH_ARC_STEP_DEG)))
    cx, cy = center
    mx, my = major_axis
    major_len = math.hypot(mx, my)
    if major_len == 0.0:
        return []
    rot = math.atan2(my, mx)
    a = major_len
    b = major_len * ratio
    pts: list[tuple[float, float]] = []
    for i in range(steps + 1):
        t = start_rad + sweep * (i / steps)
        lx = a * math.cos(t)
        ly = b * math.sin(t)
        # rotate by the major-axis orientation
        rx = lx * math.cos(rot) - ly * math.sin(rot)
        ry = lx * math.sin(rot) + ly * math.cos(rot)
        pts.append((cx + rx, cy + ry))
    return pts


def _extract_hatch_loops(
    entity: Any,
    *,
    units: dict[str, JSONValue],
) -> tuple[list[list[tuple[float, float]]], bool]:
    """Extract boundary loops from an ezdxf Hatch entity, returning (loops, all_supported).

    Each loop is a flat list of (x, y) points in scaled (canonical) units.
    all_supported is False when a SplineEdge is encountered — caller degrades to unknown.
    """
    scale = _unit_scale_factor(units) or 1.0
    loops: list[list[tuple[float, float]]] = []
    all_supported = True

    for path in entity.paths:
        path_type = type(path).__name__
        pts: list[tuple[float, float]] = []

        if path_type == "PolylinePath":
            for v in path.vertices:
                # ezdxf vertex is a 3-tuple (x, y, bulge) or (x, y)
                x = float(v[0]) * scale
                y = float(v[1]) * scale
                pts.append((x, y))

        elif path_type == "EdgePath":
            for edge in path.edges:
                edge_type = type(edge).__name__
                if edge_type == "LineEdge":
                    if not pts:
                        sx = float(edge.start[0]) * scale
                        sy = float(edge.start[1]) * scale
                        pts.append((sx, sy))
                    ex = float(edge.end[0]) * scale
                    ey = float(edge.end[1]) * scale
                    pts.append((ex, ey))
                elif edge_type == "ArcEdge":
                    cx = float(edge.center[0]) * scale
                    cy = float(edge.center[1]) * scale
                    r = float(edge.radius) * scale
                    arc_pts = _tessellate_arc_edge(
                        (cx, cy), r, float(edge.start_angle), float(edge.end_angle), bool(edge.ccw)
                    )
                    if not pts and arc_pts:
                        pts.extend(arc_pts)
                    elif arc_pts:
                        # skip duplicate junction point
                        pts.extend(arc_pts[1:])
                elif edge_type == "EllipseEdge":
                    cx = float(edge.center[0]) * scale
                    cy = float(edge.center[1]) * scale
                    mx = float(edge.major_axis[0]) * scale
                    my = float(edge.major_axis[1]) * scale
                    ell_pts = _tessellate_ellipse_edge(
                        (cx, cy),
                        (mx, my),
                        float(edge.ratio),
                        float(edge.start_angle),
                        float(edge.end_angle),
                        bool(edge.ccw),
                    )
                    if not pts and ell_pts:
                        pts.extend(ell_pts)
                    elif ell_pts:
                        pts.extend(ell_pts[1:])
                else:
                    # SplineEdge — not tessellated; mirror libredwg's "unsupported" gate
                    all_supported = False

        if len(pts) >= 3:
            loops.append(pts)

    return loops, all_supported


def _hatch_entity_payload(
    entity: Any,
    *,
    layout_name: str,
    units: dict[str, JSONValue],
) -> dict[str, JSONValue]:
    """Build a canonical 'hatch' entity from an ezdxf HATCH.

    The output shape exactly mirrors libredwg's _build_hatch_entity so downstream
    loaders (containment / service fill bands / takeoff) are adapter-agnostic.
    """
    native_type = "HATCH"
    handle = _entity_handle(entity)
    layer_name = _entity_layer(entity)

    loops, all_supported = _extract_hatch_loops(entity, units=units)
    if not all_supported:
        raise ValueError("HATCH contains unsupported SplineEdge boundary.")
    if not loops:
        raise ValueError("HATCH has no usable boundary loops.")

    # Compute per-loop area/perimeter; treat largest loop as outer boundary.
    loop_areas: list[float] = []
    loop_perimeters: list[float] = []
    for loop in loops:
        area_val = _hatch_polygon_area(loop)
        perim_val = _hatch_loop_perimeter(loop)
        if (
            area_val is None
            or perim_val is None
            or not isfinite(area_val)
            or not isfinite(perim_val)
        ):
            raise ValueError("HATCH loop has non-finite area or perimeter.")
        if area_val <= 0.0:
            raise ValueError("HATCH loop has zero or negative area.")
        loop_areas.append(area_val)
        loop_perimeters.append(perim_val)

    outer_index = max(range(len(loop_areas)), key=loop_areas.__getitem__)
    outer_area = loop_areas[outer_index]
    holes_area = sum(loop_areas) - outer_area
    area = outer_area - holes_area
    if area <= 0.0:
        area = outer_area
    perimeter = loop_perimeters[outer_index]

    if not (isfinite(area) and isfinite(perimeter)):
        raise ValueError("HATCH area/perimeter must be finite.")

    # Convert loops to canonical {x,y,z} dicts for JSON serialisation.
    def _loop_to_dicts(loop: list[tuple[float, float]]) -> tuple[dict[str, JSONValue], ...]:
        return tuple({"x": x, "y": y, "z": 0.0} for x, y in loop)

    outer_loop = loops[outer_index]
    vertex_json = _loop_to_dicts(outer_loop)
    boundary_loops_json: tuple[tuple[dict[str, JSONValue], ...], ...] = tuple(
        _loop_to_dicts(lp) for lp in loops
    )
    all_points_flat = [pt for loop in loops for pt in loop]
    bbox = cast(
        dict[str, JSONValue],
        canonical_bbox_from_points({"x": x, "y": y, "z": 0.0} for x, y in all_points_flat),
    )

    fill_type = "solid" if entity.has_solid_fill else "pattern"
    kind = "hatch"
    closed = True
    pattern_name: str = ""
    try:
        raw_pn = entity.dxf.pattern_name
        pattern_name = str(raw_pn) if raw_pn is not None else ""
    except Exception:
        pattern_name = ""

    geometry_summary: dict[str, JSONValue] = {
        "kind": kind,
        "vertex_count": len(all_points_flat),
        "loop_count": len(loops),
        "fill_type": fill_type,
        "closed": closed,
        "area": area,
        "perimeter": perimeter,
        "pattern_name": pattern_name,
    }

    geometry_for_id: dict[str, JSONValue] = {
        "vertices": vertex_json,
        "closed": closed,
    }
    entity_id = _entity_id(
        native_type=native_type,
        handle=handle,
        layout_name=layout_name,
        layer_name=layer_name,
        geometry=geometry_for_id,
    )

    return {
        "entity_id": entity_id,
        "entity_type": kind,
        "entity_schema_version": _CANONICAL_SCHEMA_VERSION,
        "geometry": {
            "vertices": vertex_json,
            "boundary_loops": boundary_loops_json,
            "closed": closed,
            "bbox": bbox,
            "units": dict(units),
            "geometry_summary": geometry_summary,
        },
        "properties": {
            "source_type": native_type,
            "source_handle": handle or None,
            "fill_type": fill_type,
            "quantity_hints": {
                "length": perimeter,
                "area": area,
                "count": 1.0,
            },
            "adapter_native": {
                "ezdxf": {
                    "layer": layer_name,
                    "pattern_name": pattern_name,
                },
            },
        },
        "provenance": _entity_provenance(
            native_type=native_type,
            handle=handle,
            layout_name=layout_name,
            layer_name=layer_name,
            entity_id=entity_id,
            geometry=geometry_for_id,
        ),
        "confidence": 0.99,
        "drawing_revision_id": None,
        "source_file_id": None,
        "layout_ref": layout_name,
        "layer_ref": layer_name,
        "block_ref": None,
        "parent_entity_ref": None,
        "kind": kind,
        "handle": handle,
        "layer": layer_name,
        "layout": layout_name,
        "vertices": vertex_json,
        "boundary_loops": boundary_loops_json,
        "closed": closed,
        "area": area,
        "perimeter": perimeter,
        "fill_type": fill_type,
        "pattern_name": pattern_name,
    }


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
    return cast(
        dict[str, JSONValue],
        canonical_bbox_from_points(
            {
                "x": _point_component(point, "x"),
                "y": _point_component(point, "y"),
                "z": _point_component(point, "z"),
            }
            for point in points
        ),
    )


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
    source_entity_ref = f"entities.{native_type}:{handle or entity_id}"
    source_hash = _entity_fingerprint(
        native_type=native_type,
        handle=handle,
        layout_name=layout_name,
        layer_name=layer_name,
        geometry=geometry,
    )
    notes: tuple[str, ...] = ()
    if geometry is None:
        notes = ("unsupported_or_invalid_geometry",)

    legacy_aliases: dict[str, JSONValue] = {
        "adapter_key": _DESCRIPTOR.key,
        "source": source_entity_ref,
        "source_entity_ref": source_entity_ref,
        "normalized_source_hash": source_hash,
    }
    native_details: dict[str, JSONValue] = {
        "dxf_handle": handle or None,
        "native_entity_type": native_type,
        "layout_name": layout_name,
        "layer_name": layer_name,
    }
    extraction_path = ("modelspace", native_type)
    provenance = cast(
        dict[str, JSONValue],
        build_entity_provenance(
            origin="adapter_normalized",
            adapter={"key": _DESCRIPTOR.key},
            source_ref=source_entity_ref,
            source_identity=handle or entity_id,
            source_hash=source_hash,
            extraction_path=list(extraction_path),
            notes=list(notes),
            extra={
                "native": {"ezdxf": native_details},
                "legacy_aliases": legacy_aliases,
            },
        ),
    )
    provenance["extraction_path"] = extraction_path
    provenance["notes"] = notes
    provenance.update(legacy_aliases)
    provenance.update(native_details)
    return provenance


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
    return cast(
        dict[str, JSONValue],
        canonical_bbox_from_points(
            (
                {
                    "x": _point_component(start, "x"),
                    "y": _point_component(start, "y"),
                    "z": _point_component(start, "z"),
                },
                {
                    "x": _point_component(end, "x"),
                    "y": _point_component(end, "y"),
                    "z": _point_component(end, "z"),
                },
            )
        ),
    )


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
