"""Deterministic revised DXF export service."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.exports._base import (
    ExportArtifactWithOptions,
    JSONValue,
    build_artifact,
    normalize_datetime,
    normalize_json_value_tree,
    normalize_options,
)
from app.ingestion.contracts import (
    AdapterAvailability,
    AdapterDescriptor,
    AdapterExecutionOptions,
    AdapterExportRequest,
    AdapterStatus,
    ExportAdapter,
    ProbeIssue,
    ProbeObservation,
)
from app.ingestion.contracts import (
    JSONValue as AdapterJSONValue,
)
from app.ingestion.loader import load_export_adapter
from app.ingestion.selection import select_export_descriptor
from app.models.drawing_revision import DrawingRevision
from app.models.revision_materialization import (
    RevisionBlock,
    RevisionEntity,
    RevisionEntityManifest,
    RevisionLayer,
    RevisionLayout,
)

REVISED_DXF_EXPORT_FORMAT = "revised_dxf"
REVISED_DXF_EXPORT_MEDIA_TYPE = "application/dxf"
REVISED_DXF_EXPORT_GENERATOR_NAME = "revised_dxf_export"
REVISED_DXF_EXPORT_GENERATOR_VERSION = "1"

_PASSTHROUGH_INPUT_ERROR_CODES = frozenset(
    {
        "INPUT_INVALID",
        "MANIFEST_NOT_FOUND",
        "MATERIALIZATION_MISSING",
        "MISSING_LAYER",
        "MISSING_LAYOUT",
        "NONFINITE_COORDINATE",
        "NONZERO_Z_COORDINATE",
        "REVISION_NOT_FOUND",
    }
)


class RevisedDxfExportError(Exception):
    """Raised when a revised DXF export cannot be rendered."""

    code: str
    message: str
    details: Mapping[str, JSONValue]

    def __init__(
        self,
        *,
        code: str,
        message: str,
        details: Mapping[str, JSONValue] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


@dataclass(frozen=True, slots=True)
class RevisedDxfExportResult(ExportArtifactWithOptions):
    """Pure rendered revised DXF export artifact metadata."""


async def render_revised_dxf_export(
    db: AsyncSession,
    revision_id: UUID,
    *,
    options: Mapping[str, object] | None = None,
) -> RevisedDxfExportResult:
    """Render a materialized drawing revision into deterministic DXF bytes."""

    options_snapshot = _normalize_options(options)

    revision = await db.get(DrawingRevision, revision_id)
    if revision is None:
        raise RevisedDxfExportError(
            code="REVISION_NOT_FOUND",
            message=f"Drawing revision {revision_id} was not found.",
            details={"revision_id": str(revision_id)},
        )
    if revision.revision_kind != "changeset" or revision.changeset_id is None:
        raise RevisedDxfExportError(
            code="INPUT_INVALID",
            message="Revised DXF export requires a changeset-origin drawing revision.",
            details={
                "revision_id": str(revision_id),
                "revision_kind": revision.revision_kind,
            },
        )

    manifest = await _load_manifest(db, revision_id)
    if manifest is None:
        raise RevisedDxfExportError(
            code="MANIFEST_NOT_FOUND",
            message=f"Revision entity manifest for drawing revision {revision_id} was not found.",
            details={"revision_id": str(revision_id)},
        )

    layouts = await _load_layouts(db, revision_id)
    layers = await _load_layers(db, revision_id)
    blocks = await _load_blocks(db, revision_id)
    entities = await _load_entities(db, revision_id)
    _validate_manifest_counts(
        manifest=manifest,
        layouts=layouts,
        layers=layers,
        blocks=blocks,
        entities=entities,
    )

    payload = _build_canonical_payload(
        layouts=layouts,
        layers=layers,
        blocks=blocks,
        entities=entities,
    )

    descriptor = _select_export_descriptor()
    adapter = _load_adapter(descriptor)
    availability = adapter.probe()
    _require_available(availability=availability, descriptor_key=descriptor.key)

    try:
        export_result = await adapter.export(
            AdapterExportRequest(
                canonical=cast(Mapping[str, AdapterJSONValue], payload),
                output_format=REVISED_DXF_EXPORT_FORMAT,
            ),
            AdapterExecutionOptions(),
        )
    except RevisedDxfExportError:
        raise
    except Exception as exc:
        raise _map_export_exception(exc, descriptor_key=descriptor.key) from exc

    if export_result.output_format != REVISED_DXF_EXPORT_FORMAT:
        raise RevisedDxfExportError(
            code="ADAPTER_FAILED",
            message="Revised DXF export adapter returned an unexpected output format.",
            details={
                "adapter_key": descriptor.key,
                "expected_output_format": REVISED_DXF_EXPORT_FORMAT,
                "output_format": export_result.output_format,
            },
        )

    if export_result.media_type != REVISED_DXF_EXPORT_MEDIA_TYPE:
        raise RevisedDxfExportError(
            code="ADAPTER_FAILED",
            message="Revised DXF export adapter returned an unexpected media type.",
            details={
                "adapter_key": descriptor.key,
                "expected_media_type": REVISED_DXF_EXPORT_MEDIA_TYPE,
                "media_type": export_result.media_type,
            },
        )

    return build_artifact(
        RevisedDxfExportResult,
        content_bytes=export_result.content,
        media_type=REVISED_DXF_EXPORT_MEDIA_TYPE,
        generator_name=REVISED_DXF_EXPORT_GENERATOR_NAME,
        generator_version=REVISED_DXF_EXPORT_GENERATOR_VERSION,
        options=options_snapshot,
    )


async def _load_manifest(
    db: AsyncSession,
    revision_id: UUID,
) -> RevisionEntityManifest | None:
    result = await db.execute(
        select(RevisionEntityManifest).where(
            RevisionEntityManifest.drawing_revision_id == revision_id
        )
    )
    return result.scalar_one_or_none()


async def _load_layouts(db: AsyncSession, revision_id: UUID) -> list[RevisionLayout]:
    result = await db.execute(
        select(RevisionLayout)
        .where(RevisionLayout.drawing_revision_id == revision_id)
        .order_by(RevisionLayout.sequence_index.asc(), RevisionLayout.id.asc())
    )
    return list(result.scalars().all())


async def _load_layers(db: AsyncSession, revision_id: UUID) -> list[RevisionLayer]:
    result = await db.execute(
        select(RevisionLayer)
        .where(RevisionLayer.drawing_revision_id == revision_id)
        .order_by(RevisionLayer.sequence_index.asc(), RevisionLayer.id.asc())
    )
    return list(result.scalars().all())


async def _load_blocks(db: AsyncSession, revision_id: UUID) -> list[RevisionBlock]:
    result = await db.execute(
        select(RevisionBlock)
        .where(RevisionBlock.drawing_revision_id == revision_id)
        .order_by(RevisionBlock.sequence_index.asc(), RevisionBlock.id.asc())
    )
    return list(result.scalars().all())


async def _load_entities(db: AsyncSession, revision_id: UUID) -> list[RevisionEntity]:
    result = await db.execute(
        select(RevisionEntity)
        .where(RevisionEntity.drawing_revision_id == revision_id)
        .order_by(RevisionEntity.sequence_index.asc(), RevisionEntity.id.asc())
    )
    return list(result.scalars().all())


def _validate_manifest_counts(
    *,
    manifest: RevisionEntityManifest,
    layouts: list[RevisionLayout],
    layers: list[RevisionLayer],
    blocks: list[RevisionBlock],
    entities: list[RevisionEntity],
) -> None:
    counts = _manifest_counts(manifest)
    actual_counts = {
        "layouts": len(layouts),
        "layers": len(layers),
        "blocks": len(blocks),
        "entities": len(entities),
    }
    for component, expected_count in counts.items():
        actual_count = actual_counts[component]
        if actual_count != expected_count:
            raise RevisedDxfExportError(
                code="MATERIALIZATION_MISSING",
                message="Revision materialization rows do not match the manifest counts.",
                details={
                    "revision_id": str(manifest.drawing_revision_id),
                    "component": component,
                    "expected_count": expected_count,
                    "actual_count": actual_count,
                },
            )


def _manifest_counts(manifest: RevisionEntityManifest) -> dict[str, int]:
    raw_counts = manifest.counts_json
    return {
        "layouts": int(raw_counts.get("layouts", 0)),
        "layers": int(raw_counts.get("layers", 0)),
        "blocks": int(raw_counts.get("blocks", 0)),
        "entities": int(raw_counts.get("entities", 0)),
    }


def _build_canonical_payload(
    *,
    layouts: list[RevisionLayout],
    layers: list[RevisionLayer],
    blocks: list[RevisionBlock],
    entities: list[RevisionEntity],
) -> dict[str, JSONValue]:
    payload: dict[str, JSONValue] = {
        "layouts": [_build_layout_payload(layout) for layout in layouts],
        "layers": [_build_layer_payload(layer) for layer in layers],
        "blocks": [_build_block_payload(block) for block in blocks],
        "entities": [_build_entity_payload(entity) for entity in entities],
    }
    units = _infer_top_level_units(entities)
    if units is not None:
        payload["units"] = units
    return payload


def _build_layout_payload(layout: RevisionLayout) -> dict[str, JSONValue]:
    payload = dict(layout.payload_json)
    payload.setdefault("id", layout.layout_ref)
    payload.setdefault("layout_ref", layout.layout_ref)
    payload.setdefault("name", layout.layout_ref)
    return payload


def _build_layer_payload(layer: RevisionLayer) -> dict[str, JSONValue]:
    payload = dict(layer.payload_json)
    payload.setdefault("id", layer.layer_ref)
    payload.setdefault("layer_ref", layer.layer_ref)
    payload.setdefault("name", layer.layer_ref)
    return payload


def _build_block_payload(block: RevisionBlock) -> dict[str, JSONValue]:
    payload = dict(block.payload_json)
    payload.setdefault("id", block.block_ref)
    payload.setdefault("block_ref", block.block_ref)
    payload.setdefault("name", block.block_ref)
    return payload


def _build_entity_payload(entity: RevisionEntity) -> dict[str, JSONValue]:
    payload: dict[str, JSONValue] = {
        "entity_id": entity.entity_id,
        "entity_type": entity.entity_type,
        "entity_schema_version": entity.entity_schema_version,
        "sequence_index": entity.sequence_index,
        "layout_ref": entity.layout_ref,
        "layer_ref": entity.layer_ref,
        "block_ref": entity.block_ref,
        "geometry_json": dict(entity.geometry_json),
        "properties_json": dict(entity.properties_json),
    }
    if entity.canonical_entity_json is not None:
        payload["canonical_entity_json"] = dict(entity.canonical_entity_json)
    return payload


def _infer_top_level_units(entities: list[RevisionEntity]) -> dict[str, JSONValue] | None:
    normalized_unit: str | None = None
    for entity in entities:
        geometry = entity.geometry_json
        units = geometry.get("units")
        if not isinstance(units, Mapping):
            continue
        candidate = units.get("normalized")
        if not isinstance(candidate, str) or not candidate:
            continue
        if normalized_unit is None:
            normalized_unit = candidate
            continue
        if candidate != normalized_unit:
            return None

    if normalized_unit is None:
        return None
    return {"normalized": normalized_unit}


def _select_export_descriptor() -> AdapterDescriptor:
    try:
        return select_export_descriptor(REVISED_DXF_EXPORT_FORMAT)
    except Exception as exc:
        raise RevisedDxfExportError(
            code="ADAPTER_UNAVAILABLE",
            message="Revised DXF export adapter is not configured.",
            details={"output_format": REVISED_DXF_EXPORT_FORMAT},
        ) from exc


def _load_adapter(descriptor: AdapterDescriptor) -> ExportAdapter:
    try:
        return load_export_adapter(descriptor)
    except Exception as exc:
        raise RevisedDxfExportError(
            code="ADAPTER_LOAD_FAILED",
            message="Revised DXF export adapter could not be loaded.",
            details={
                "adapter_key": descriptor.key,
                "output_format": REVISED_DXF_EXPORT_FORMAT,
                "error_type": type(exc).__name__,
            },
        ) from exc


def _require_available(*, availability: AdapterAvailability, descriptor_key: str) -> None:
    if availability.status == AdapterStatus.AVAILABLE:
        return

    raise RevisedDxfExportError(
        code="ADAPTER_UNAVAILABLE",
        message="Revised DXF export adapter is unavailable.",
        details={
            "adapter_key": descriptor_key,
            "output_format": REVISED_DXF_EXPORT_FORMAT,
            "status": availability.status.value,
            "availability_reason": (
                None
                if availability.availability_reason is None
                else availability.availability_reason.value
            ),
            "license_state": availability.license_state.value,
            "issues": [_serialize_issue(issue) for issue in availability.issues],
            "observed": [
                _serialize_observation(observation) for observation in availability.observed
            ],
            "details": (
                None
                if availability.details is None
                else _normalize_json_mapping(availability.details)
            ),
        },
    )


def _map_export_exception(exc: Exception, *, descriptor_key: str) -> RevisedDxfExportError:
    error_code = getattr(exc, "code", None)
    if isinstance(error_code, str) and error_code:
        details = getattr(exc, "details", None)
        normalized_details: dict[str, JSONValue] = {
            "adapter_key": descriptor_key,
            "output_format": REVISED_DXF_EXPORT_FORMAT,
        }
        if isinstance(details, Mapping):
            normalized_details.update(_normalize_json_mapping(details))
        if error_code in {
            "ADAPTER_UNAVAILABLE",
            "ADAPTER_LOAD_FAILED",
        } or _is_passthrough_input_error_code(error_code):
            return RevisedDxfExportError(
                code=error_code,
                message=str(exc),
                details=normalized_details,
            )
        normalized_details["original_error_code"] = error_code
        return RevisedDxfExportError(
            code="ADAPTER_FAILED",
            message=str(exc),
            details=normalized_details,
        )

    return RevisedDxfExportError(
        code="ADAPTER_FAILED",
        message="Revised DXF export rendering failed.",
        details={
            "adapter_key": descriptor_key,
            "output_format": REVISED_DXF_EXPORT_FORMAT,
            "error_type": type(exc).__name__,
        },
    )


def _is_passthrough_input_error_code(code: str) -> bool:
    return code in _PASSTHROUGH_INPUT_ERROR_CODES or code.startswith(("INVALID_", "UNSUPPORTED_"))


def _serialize_issue(issue: ProbeIssue) -> dict[str, JSONValue]:
    return {
        "kind": issue.kind.value,
        "name": issue.name,
        "observed_status": issue.observed_status.value,
        "adapter_status": issue.adapter_status.value,
        "detail": issue.detail,
    }


def _serialize_observation(observation: ProbeObservation) -> dict[str, JSONValue]:
    payload: dict[str, JSONValue] = {
        "kind": observation.kind.value,
        "name": observation.name,
        "status": observation.status.value,
    }
    if observation.detail is not None:
        payload["detail"] = observation.detail
    return payload


def _normalize_options(options: Mapping[str, object] | None) -> dict[str, JSONValue]:
    return normalize_options(
        options,
        normalize_value=_normalize_json_value,
        normalize_mapping_key=_normalize_mapping_key,
    )


def _normalize_json_mapping(mapping: Mapping[Any, Any]) -> dict[str, JSONValue]:
    return {
        _normalize_mapping_key(key): _normalize_json_value(value) for key, value in mapping.items()
    }


def _normalize_json_value(value: object) -> JSONValue:
    return normalize_json_value_tree(
        value,
        normalize_scalar=_normalize_json_scalar,
        normalize_mapping_key=_normalize_mapping_key,
        unsupported_value=_unsupported_json_value,
    )


def _normalize_json_scalar(value: object) -> tuple[bool, JSONValue]:
    if isinstance(value, UUID):
        return True, str(value)
    if isinstance(value, datetime):
        return True, _normalize_datetime(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return True, value
    return False, None


def _unsupported_json_value(value: object) -> Exception:
    return TypeError(f"Unsupported revised DXF export value type: {type(value)!r}")


def _normalize_mapping_key(key: object) -> str:
    if isinstance(key, str):
        return key
    if isinstance(key, UUID):
        return str(key)
    if isinstance(key, datetime):
        return _normalize_datetime(key)
    if isinstance(key, (int, float, bool)):
        return str(key)

    raise TypeError(f"Unsupported revised DXF export mapping key type: {type(key)!r}")


def _normalize_datetime(value: datetime) -> str:
    return normalize_datetime(value)
