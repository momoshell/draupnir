"""Deterministic revision JSON export service."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.exports._base import (
    ExportArtifactWithOptions,
    JSONValue,
    build_artifact,
    canonical_json_bytes,
    normalize_datetime,
    normalize_json_value_tree,
    normalize_options,
)
from app.models.drawing_revision import DrawingRevision
from app.models.revision_materialization import (
    RevisionBlock,
    RevisionEntity,
    RevisionEntityManifest,
    RevisionLayer,
    RevisionLayout,
)

REVISION_JSON_EXPORT_SCHEMA_VERSION = "revision-json-export-v1"
REVISION_JSON_EXPORT_MEDIA_TYPE = "application/json"
REVISION_JSON_EXPORT_GENERATOR_NAME = "revision_json_export"
REVISION_JSON_EXPORT_GENERATOR_VERSION = "1"


class RevisionJsonExportError(Exception):
    """Raised when a revision JSON export cannot be rendered."""


@dataclass(frozen=True, slots=True)
class RevisionJsonExportResult(ExportArtifactWithOptions):
    """Pure rendered revision JSON export artifact metadata."""


async def render_revision_json_export(
    db: AsyncSession,
    revision_id: UUID,
    *,
    options: Mapping[str, object] | None = None,
) -> RevisionJsonExportResult:
    """Render a materialized drawing revision into canonical JSON bytes."""

    options_snapshot = _normalize_options(options)

    revision = await db.get(DrawingRevision, revision_id)
    if revision is None:
        raise RevisionJsonExportError(f"Drawing revision {revision_id} was not found.")

    manifest = await _load_manifest(db, revision_id)
    if manifest is None:
        raise RevisionJsonExportError(
            f"Revision entity manifest for drawing revision {revision_id} was not found."
        )

    layouts = await _load_layouts(db, revision_id)
    layers = await _load_layers(db, revision_id)
    blocks = await _load_blocks(db, revision_id)
    entities = await _load_entities(db, revision_id)

    payload: dict[str, object] = {
        "schema_version": REVISION_JSON_EXPORT_SCHEMA_VERSION,
        "generator": {
            "name": REVISION_JSON_EXPORT_GENERATOR_NAME,
            "version": REVISION_JSON_EXPORT_GENERATOR_VERSION,
            "options": options_snapshot,
        },
        "revision": {
            "id": revision.id,
            "project_id": revision.project_id,
            "source_file_id": revision.source_file_id,
            "extraction_profile_id": revision.extraction_profile_id,
            "source_job_id": revision.source_job_id,
            "adapter_run_output_id": revision.adapter_run_output_id,
            "predecessor_revision_id": revision.predecessor_revision_id,
            "revision_sequence": revision.revision_sequence,
            "revision_kind": revision.revision_kind,
            "canonical_entity_schema_version": revision.canonical_entity_schema_version,
            "created_at": revision.created_at,
        },
        "manifest": {
            "id": manifest.id,
            "counts": _manifest_counts(manifest),
            "canonical_entity_schema_version": manifest.canonical_entity_schema_version,
            "created_at": manifest.created_at,
        },
        "layouts": [
            {
                "layout_ref": layout.layout_ref,
                "sequence_index": layout.sequence_index,
                "payload": layout.payload_json,
            }
            for layout in layouts
        ],
        "layers": [
            {
                "layer_ref": layer.layer_ref,
                "sequence_index": layer.sequence_index,
                "payload": layer.payload_json,
            }
            for layer in layers
        ],
        "blocks": [
            {
                "block_ref": block.block_ref,
                "sequence_index": block.sequence_index,
                "payload": block.payload_json,
            }
            for block in blocks
        ],
        "entities": [
            {
                "entity_id": entity.entity_id,
                "entity_type": entity.entity_type,
                "entity_schema_version": entity.entity_schema_version,
                "sequence_index": entity.sequence_index,
                "parent_entity_ref": entity.parent_entity_ref,
                "layout_ref": entity.layout_ref,
                "layer_ref": entity.layer_ref,
                "block_ref": entity.block_ref,
                "source_identity": entity.source_identity,
                "source_hash": entity.source_hash,
                "confidence": entity.confidence_json,
                "geometry": entity.geometry_json,
                "properties": entity.properties_json,
                "provenance": entity.provenance_json,
                "canonical_entity": entity.canonical_entity_json,
            }
            for entity in entities
        ],
    }

    content_bytes = _canonical_json_bytes(payload)
    return build_artifact(
        RevisionJsonExportResult,
        content_bytes=content_bytes,
        media_type=REVISION_JSON_EXPORT_MEDIA_TYPE,
        generator_name=REVISION_JSON_EXPORT_GENERATOR_NAME,
        generator_version=REVISION_JSON_EXPORT_GENERATOR_VERSION,
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


def _manifest_counts(manifest: RevisionEntityManifest) -> dict[str, int]:
    raw_counts = manifest.counts_json
    return {
        "layouts": int(raw_counts.get("layouts", 0)),
        "layers": int(raw_counts.get("layers", 0)),
        "blocks": int(raw_counts.get("blocks", 0)),
        "entities": int(raw_counts.get("entities", 0)),
    }


def _normalize_options(options: Mapping[str, object] | None) -> dict[str, JSONValue]:
    return normalize_options(
        options,
        normalize_value=_normalize_json_value,
        normalize_mapping_key=_normalize_mapping_key,
    )


def _canonical_json_bytes(payload: Mapping[str, object]) -> bytes:
    return canonical_json_bytes(
        payload,
        normalize_value=_normalize_json_value,
    )


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
    return TypeError(f"Unsupported revision JSON export value type: {type(value)!r}")


def _normalize_mapping_key(key: object) -> str:
    if isinstance(key, str):
        return key
    if isinstance(key, UUID):
        return str(key)
    if isinstance(key, datetime):
        return _normalize_datetime(key)
    if isinstance(key, (int, float, bool)):
        return str(key)

    raise TypeError(f"Unsupported revision JSON export mapping key type: {type(key)!r}")


def _normalize_datetime(value: datetime) -> str:
    return normalize_datetime(value)
