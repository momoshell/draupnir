"""Pure builders that prepare normalized revision-materialization rows.

Extracted from ``worker.py``: these turn ingest/changeset canonical payloads into
the DB-insertable row tuples (layouts/layers/blocks/entities) plus entity provenance,
with deterministic ref/id resolution. All pure transformations over their arguments
(no DB access, no worker-module state), so they carry no monkeypatch seams.
"""

from __future__ import annotations

import heapq
import uuid
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, cast
from uuid import UUID

from app.cad.changeset import (
    ChangeSetApplySuccess,
)
from app.ingestion.canonical import EntityProvenanceError, canonicalize_entity_provenance
from app.ingestion.entity_geometry import compute_entity_bbox
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.ref_resolution import (
    BLOCK_IDENTITY_KEYS,
    COLLECTION_REF_FALLBACK_KEYS,
    LAYER_IDENTITY_KEYS,
    collection_identity,
    string_ref,
)
from app.models.revision_materialization import (
    RevisionBlock,
    RevisionEntityManifest,
    RevisionLayer,
    RevisionLayout,
)


@dataclass(frozen=True, slots=True)
class _RevisionMaterializationRows:
    """Prepared normalized revision payload rows for DB insertion."""

    counts_json: dict[str, int]
    layouts: list[dict[str, Any]]
    layers: list[dict[str, Any]]
    blocks: list[dict[str, Any]]
    entities: list[dict[str, Any]]


# Layouts have no name-alias variants; their identity is the typed ref + the generic fallbacks.
_LAYOUT_IDENTITY_KEYS = ("layout_ref", *COLLECTION_REF_FALLBACK_KEYS)


def _materialized_payload_json(value: Any) -> dict[str, Any]:
    """Coerce a canonical collection item into a persisted JSON object payload."""
    if isinstance(value, dict):
        return deepcopy(value)

    return {"value": deepcopy(value)}


def _canonical_payload_list(payload: IngestFinalizationPayload, key: str) -> list[Any]:
    """Return a canonical collection list or an empty list when absent."""
    raw_value = payload.canonical_json.get(key)
    return list(raw_value) if isinstance(raw_value, list) else []


# Shared with reconciliation so the two never drift on what identifies a ref.
_string_ref = string_ref


def _hash_ref(value: Any) -> str | None:
    """Normalize persisted hash refs to lowercase SHA-256 strings when valid."""
    normalized = _string_ref(value)
    if normalized is None:
        return None

    lowered = normalized.lower()
    return lowered if len(lowered) == 64 else None


def _first_string_ref(*values: Any) -> str | None:
    """Return the first non-empty normalized string from candidate values."""
    for value in values:
        normalized = _string_ref(value)
        if normalized is not None:
            return normalized

    return None


def _first_hash_ref(*values: Any) -> str | None:
    """Return the first valid normalized hash from candidate values."""
    for value in values:
        normalized = _hash_ref(value)
        if normalized is not None:
            return normalized

    return None


def _json_object(value: Any) -> dict[str, Any]:
    """Return a deep-copied JSON object or an empty object."""
    if isinstance(value, dict):
        return deepcopy(value)

    return {}


def _json_array(value: Any) -> list[Any]:
    """Return a deep-copied JSON array-like value or an empty list."""
    if isinstance(value, list):
        return deepcopy(value)
    if isinstance(value, tuple):
        return deepcopy(list(value))

    return []


def _float_value(value: Any) -> float | None:
    """Normalize persisted numeric fields to floats when possible."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)

    return None


def _allocate_unique_ref(
    *,
    candidates: list[Any],
    prefix: str,
    sequence_index: int,
    used_values: set[str],
) -> str:
    """Allocate a deterministic unique ref from preferred candidates or a sequence fallback."""
    for candidate in candidates:
        normalized = _string_ref(candidate)
        if normalized is not None and normalized not in used_values:
            used_values.add(normalized)
            return normalized

    fallback_base = f"{prefix}-{sequence_index:06d}"
    fallback = fallback_base
    suffix = 1
    while fallback in used_values:
        fallback = f"{fallback_base}-{suffix}"
        suffix += 1

    used_values.add(fallback)
    return fallback


def _resolve_collection_ref(
    payload_json: dict[str, Any],
    *,
    explicit_key: str,
    fallback_keys: tuple[str, ...],
    prefix: str,
    sequence_index: int,
    used_values: set[str],
) -> str:
    """Resolve a stable non-null unique collection ref for a materialized row."""
    return _allocate_unique_ref(
        candidates=[
            payload_json.get(explicit_key),
            *[payload_json.get(key) for key in fallback_keys],
        ],
        prefix=prefix,
        sequence_index=sequence_index,
        used_values=used_values,
    )


def _entity_provenance_json(entity_payload_json: dict[str, Any]) -> dict[str, Any]:
    """Return canonical entity provenance JSON from contract or legacy payloads."""
    provenance_json = entity_payload_json.get("provenance_json")
    provenance = provenance_json if isinstance(provenance_json, dict) else None
    if provenance is None:
        legacy_provenance = entity_payload_json.get("provenance")
        provenance = legacy_provenance if isinstance(legacy_provenance, dict) else {}

    canonical_input = deepcopy(provenance)
    for key in (
        "origin",
        "adapter",
        "source_ref",
        "source_entity_ref",
        "source_identity",
        "source_handle",
        "dxf_handle",
        "source_entity_handle",
        "native_handle",
        "source_id",
        "source_hash",
        "normalized_source_hash",
        "record_hash",
        "extraction_path",
        "notes",
    ):
        if key not in canonical_input and key in entity_payload_json:
            canonical_input[key] = deepcopy(entity_payload_json[key])

    try:
        canonical_provenance = canonicalize_entity_provenance(canonical_input)
    except EntityProvenanceError:
        origin = _string_ref(canonical_input.get("origin"))
        if origin is None and any(
            key in canonical_input
            for key in (
                "adapter",
                "source_ref",
                "source_entity_ref",
                "source_handle",
                "dxf_handle",
                "source_entity_handle",
                "native_handle",
                "source_hash",
                "normalized_source_hash",
                "record_hash",
            )
        ):
            origin = "adapter_normalized"

        adapter_json = canonical_input.get("adapter")
        if isinstance(adapter_json, dict):
            adapter = deepcopy(adapter_json)
        elif adapter_json is None:
            adapter = {}
        else:
            adapter = {"value": deepcopy(adapter_json)}

        return {
            "origin": origin,
            "adapter": adapter,
            "source_ref": _first_string_ref(
                canonical_input.get("source_ref"),
                canonical_input.get("source_entity_ref"),
            ),
            "source_identity": _first_string_ref(
                canonical_input.get("source_identity"),
                canonical_input.get("source_handle"),
                canonical_input.get("dxf_handle"),
                canonical_input.get("source_entity_handle"),
                canonical_input.get("native_handle"),
                canonical_input.get("source_id"),
            ),
            "source_hash": _first_hash_ref(
                canonical_input.get("source_hash"),
                canonical_input.get("normalized_source_hash"),
                canonical_input.get("record_hash"),
            ),
            "extraction_path": _json_array(canonical_input.get("extraction_path")),
            "notes": _json_array(canonical_input.get("notes")),
        }

    adapter_json = canonical_provenance["adapter"]
    if isinstance(adapter_json, dict):
        adapter = deepcopy(adapter_json)
    elif adapter_json is None:
        adapter = {}
    else:
        adapter = {"value": deepcopy(adapter_json)}

    return {
        "origin": canonical_provenance["origin"],
        "adapter": adapter,
        "source_ref": _string_ref(canonical_provenance["source_ref"]),
        "source_identity": _string_ref(canonical_provenance["source_identity"]),
        "source_hash": _hash_ref(canonical_provenance["source_hash"]),
        "extraction_path": _json_array(canonical_provenance["extraction_path"]),
        "notes": _json_array(canonical_provenance["notes"]),
    }


def _resolve_entity_source_identity(entity_payload_json: dict[str, Any]) -> str | None:
    """Resolve the best-effort stable source identity for a materialized entity row."""
    provenance = _entity_provenance_json(entity_payload_json)
    return _string_ref(provenance.get("source_identity"))


def _resolve_entity_source_hash(entity_payload_json: dict[str, Any]) -> str | None:
    """Resolve the best-effort stable source hash for a materialized entity row."""
    provenance = _entity_provenance_json(entity_payload_json)
    return _hash_ref(provenance.get("source_hash"))


def _resolve_entity_on_sheet(entity_payload_json: dict[str, Any]) -> bool | None:
    """Project the printed-sheet membership tag (#568) into the queryable column (#569).

    ``properties.sheet_membership.on_sheet`` is True/False/None; None (undetermined or no
    viewports) stays NULL so the column never fabricates a claim.
    """
    properties = (
        entity_payload_json.get("properties_json")
        if "properties_json" in entity_payload_json
        else entity_payload_json.get("properties")
    )
    if not isinstance(properties, dict):
        return None
    membership = properties.get("sheet_membership")
    if not isinstance(membership, dict):
        return None
    value = membership.get("on_sheet")
    return value if isinstance(value, bool) else None


def _resolve_entity_ref(
    entity_payload_json: dict[str, Any],
    *,
    explicit_key: str,
    legacy_key: str,
) -> str | None:
    """Resolve a raw entity relationship ref from contract or legacy payloads.

    Asymmetry note (#539): this resolves over (explicit_key, legacy_key, provenance) — it does NOT
    include the ``*_name`` alias that validation's ``ENTITY_LAYER_KEYS`` does. This is safe only
    because every adapter that sets an entity ``layer_name``/``block_name`` also co-emits the
    matching ``layer_ref``/``block_ref`` (the name appears solely as descriptive provenance, never
    as the sole top-level reference). A future adapter emitting ``layer_name`` alone would link in
    validation but get ``layer_id=NULL`` here; extend this resolver (and the stored column) to the
    name alias if that invariant ever breaks.
    """
    explicit_ref = _string_ref(entity_payload_json.get(explicit_key))
    if explicit_ref is not None:
        return explicit_ref

    legacy_ref = _string_ref(entity_payload_json.get(legacy_key))
    if legacy_ref is not None:
        return legacy_ref

    provenance = _entity_provenance_json(entity_payload_json)
    if not provenance:
        return None

    return _string_ref(provenance.get(explicit_key))


def _resolve_entity_parent_ref(entity_payload_json: dict[str, Any]) -> str | None:
    """Resolve a raw parent entity reference from contract or legacy payloads."""
    parent_entity_ref = _string_ref(entity_payload_json.get("parent_entity_ref"))
    if parent_entity_ref is not None:
        return parent_entity_ref

    parent_id = _string_ref(entity_payload_json.get("parent_id"))
    if parent_id is not None:
        return parent_id

    provenance = _entity_provenance_json(entity_payload_json)
    if not provenance:
        return None

    return _string_ref(provenance.get("parent_entity_ref")) or _string_ref(
        provenance.get("parent_source_id")
    )


def _resolve_entity_id(
    entity_payload_json: dict[str, Any],
    *,
    sequence_index: int,
    used_values: set[str],
) -> str:
    """Resolve a stable non-null unique entity id for a materialized row."""
    provenance = _entity_provenance_json(entity_payload_json)
    return _allocate_unique_ref(
        candidates=[
            entity_payload_json.get("entity_id"),
            entity_payload_json.get("id"),
            entity_payload_json.get("source_identity"),
            provenance.get("source_identity"),
            provenance.get("source_ref"),
        ],
        prefix="entity",
        sequence_index=sequence_index,
        used_values=used_values,
    )


def _resolve_entity_type(entity_payload_json: dict[str, Any]) -> str:
    """Resolve a stable non-null entity type from contract or legacy payloads."""
    return (
        _string_ref(entity_payload_json.get("entity_type"))
        or _string_ref(entity_payload_json.get("kind"))
        or "unknown"
    )


def _resolve_entity_schema_version(
    entity_payload_json: dict[str, Any],
    *,
    default_schema_version: str,
) -> str:
    """Resolve the entity schema version from the payload or manifest default."""
    return _string_ref(entity_payload_json.get("entity_schema_version")) or default_schema_version


def _resolve_entity_confidence_json(
    entity_payload_json: dict[str, Any],
) -> dict[str, Any]:
    """Resolve the entity confidence payload from contract or legacy payloads."""
    for key in ("confidence_json", "confidence"):
        confidence_payload = entity_payload_json.get(key)
        confidence_json = _json_object(confidence_payload)
        if confidence_json:
            return confidence_json

        numeric_confidence = _float_value(confidence_payload)
        if numeric_confidence is not None:
            return {"score": numeric_confidence}

    return {}


def _collection_id_index(
    rows: list[dict[str, Any]], ref_key: str, identity_keys: tuple[str, ...]
) -> dict[str, uuid.UUID]:
    """Map every identifier a collection row can be matched by to its row id (#539).

    Single authoritative set-membership linkage rule (shared with reconciliation + layer-mapping):
    a row is reachable by ANY of its declared identifiers (typed ref + name/alias/ref/id), not just
    the single allocated ref — so an entity that references a layer by any of its identifiers links.
    ``setdefault`` keeps sequence precedence when two rows share an identifier (rare; duplicate
    names are deduplicated to distinct allocated refs upstream).
    """
    index: dict[str, uuid.UUID] = {}
    for row in rows:
        for ref in collection_identity(row["payload_json"], identity_keys):
            index.setdefault(ref, row["id"])
        allocated = row.get(ref_key)
        if isinstance(allocated, str) and allocated:
            index.setdefault(allocated, row["id"])
    return index


def _build_revision_materialization_rows(
    payload: IngestFinalizationPayload,
) -> _RevisionMaterializationRows:
    """Build revision-scoped normalized payload rows from canonical JSON."""
    layouts: list[dict[str, Any]] = []
    used_layout_refs: set[str] = set()
    for index, layout in enumerate(_canonical_payload_list(payload, "layouts")):
        payload_json = _materialized_payload_json(layout)
        layouts.append(
            {
                "id": uuid.uuid4(),
                "sequence_index": index,
                "payload_json": payload_json,
                "layout_ref": _resolve_collection_ref(
                    payload_json,
                    explicit_key="layout_ref",
                    fallback_keys=COLLECTION_REF_FALLBACK_KEYS,
                    prefix="layout",
                    sequence_index=index,
                    used_values=used_layout_refs,
                ),
            }
        )

    layers: list[dict[str, Any]] = []
    used_layer_refs: set[str] = set()
    for index, layer in enumerate(_canonical_payload_list(payload, "layers")):
        payload_json = _materialized_payload_json(layer)
        layers.append(
            {
                "id": uuid.uuid4(),
                "sequence_index": index,
                "payload_json": payload_json,
                "layer_ref": _resolve_collection_ref(
                    payload_json,
                    explicit_key="layer_ref",
                    fallback_keys=COLLECTION_REF_FALLBACK_KEYS,
                    prefix="layer",
                    sequence_index=index,
                    used_values=used_layer_refs,
                ),
            }
        )

    blocks: list[dict[str, Any]] = []
    used_block_refs: set[str] = set()
    for index, block in enumerate(_canonical_payload_list(payload, "blocks")):
        payload_json = _materialized_payload_json(block)
        blocks.append(
            {
                "id": uuid.uuid4(),
                "sequence_index": index,
                "payload_json": payload_json,
                "block_ref": _resolve_collection_ref(
                    payload_json,
                    explicit_key="block_ref",
                    fallback_keys=COLLECTION_REF_FALLBACK_KEYS,
                    prefix="block",
                    sequence_index=index,
                    used_values=used_block_refs,
                ),
            }
        )

    layout_ids_by_ref = _collection_id_index(layouts, "layout_ref", _LAYOUT_IDENTITY_KEYS)
    layer_ids_by_ref = _collection_id_index(layers, "layer_ref", LAYER_IDENTITY_KEYS)
    block_ids_by_ref = _collection_id_index(blocks, "block_ref", BLOCK_IDENTITY_KEYS)

    entities: list[dict[str, Any]] = []
    used_entity_ids: set[str] = set()
    for index, entity in enumerate(_canonical_payload_list(payload, "entities")):
        payload_json = _materialized_payload_json(entity)
        entity_id = _resolve_entity_id(
            payload_json,
            sequence_index=index,
            used_values=used_entity_ids,
        )
        entity_type = _resolve_entity_type(payload_json)
        entity_schema_version = _resolve_entity_schema_version(
            payload_json,
            default_schema_version=payload.canonical_entity_schema_version,
        )
        parent_entity_ref = _resolve_entity_parent_ref(payload_json)
        confidence_json = _resolve_entity_confidence_json(payload_json)
        provenance_json = _entity_provenance_json(payload_json)
        layout_ref = _resolve_entity_ref(
            payload_json,
            explicit_key="layout_ref",
            legacy_key="layout",
        )
        layer_ref = _resolve_entity_ref(
            payload_json,
            explicit_key="layer_ref",
            legacy_key="layer",
        )
        block_ref = _resolve_entity_ref(
            payload_json,
            explicit_key="block_ref",
            legacy_key="block",
        )
        geometry_json = _json_object(
            payload_json.get("geometry_json")
            if "geometry_json" in payload_json
            else payload_json.get("geometry")
        )
        bbox = compute_entity_bbox(geometry_json)
        entities.append(
            {
                "id": uuid.uuid4(),
                "sequence_index": index,
                "entity_id": entity_id,
                "entity_type": entity_type,
                "entity_schema_version": entity_schema_version,
                "parent_entity_ref": parent_entity_ref,
                "confidence_json": confidence_json,
                "geometry_json": geometry_json,
                "bbox_min_x": bbox[0] if bbox is not None else None,
                "bbox_min_y": bbox[1] if bbox is not None else None,
                "bbox_max_x": bbox[2] if bbox is not None else None,
                "bbox_max_y": bbox[3] if bbox is not None else None,
                "on_sheet": _resolve_entity_on_sheet(payload_json),
                "properties_json": _json_object(
                    payload_json.get("properties_json")
                    if "properties_json" in payload_json
                    else payload_json.get("properties")
                ),
                "provenance_json": provenance_json,
                "canonical_entity_json": payload_json,
                "layout_ref": layout_ref,
                "layer_ref": layer_ref,
                "block_ref": block_ref,
                "source_identity": _resolve_entity_source_identity(payload_json),
                "source_hash": _resolve_entity_source_hash(payload_json),
                "layout_id": layout_ids_by_ref.get(layout_ref) if layout_ref is not None else None,
                "layer_id": layer_ids_by_ref.get(layer_ref) if layer_ref is not None else None,
                "block_id": block_ids_by_ref.get(block_ref) if block_ref is not None else None,
            }
        )

    entity_row_ids_by_entity_id = {row["entity_id"]: row["id"] for row in entities}
    for row in entities:
        parent_entity_ref = row["parent_entity_ref"]
        row["parent_entity_row_id"] = (
            entity_row_ids_by_entity_id.get(parent_entity_ref)
            if parent_entity_ref is not None
            else None
        )

    counts_json = {
        "layouts": len(layouts),
        "layers": len(layers),
        "blocks": len(blocks),
        "entities": len(entities),
    }
    return _RevisionMaterializationRows(
        counts_json=counts_json,
        layouts=layouts,
        layers=layers,
        blocks=blocks,
        entities=entities,
    )


def _copy_revision_collection_rows(
    rows: Sequence[RevisionLayout | RevisionLayer | RevisionBlock],
    *,
    ref_key: str,
) -> list[dict[str, Any]]:
    """Copy persisted collection rows into a new in-memory materialization plan."""
    copied_rows: list[dict[str, Any]] = []
    for row in sorted(
        rows,
        key=lambda item: (int(item.sequence_index), str(getattr(item, ref_key)), str(item.id)),
    ):
        copied_rows.append(
            {
                "id": uuid.uuid4(),
                "sequence_index": row.sequence_index,
                "payload_json": _materialized_payload_json(row.payload_json),
                ref_key: cast(str, getattr(row, ref_key)),
            }
        )
    return copied_rows


def _copy_json_mapping(value: Mapping[str, Any] | None) -> dict[str, Any]:
    """Deep-copy mapping-backed JSON contract fields into plain dicts."""
    if value is None:
        return {}
    return deepcopy(dict(value))


def _build_changeset_revision_materialization_rows(
    apply_result: ChangeSetApplySuccess,
    *,
    base_manifest: RevisionEntityManifest,
    base_layouts: Sequence[RevisionLayout],
    base_layers: Sequence[RevisionLayer],
    base_blocks: Sequence[RevisionBlock],
) -> _RevisionMaterializationRows:
    """Map applied changeset entities plus base collections into insert-ready rows."""
    layouts = _copy_revision_collection_rows(base_layouts, ref_key="layout_ref")
    layers = _copy_revision_collection_rows(base_layers, ref_key="layer_ref")
    blocks = _copy_revision_collection_rows(base_blocks, ref_key="block_ref")

    layout_ids_by_ref = _collection_id_index(layouts, "layout_ref", _LAYOUT_IDENTITY_KEYS)
    layer_ids_by_ref = _collection_id_index(layers, "layer_ref", LAYER_IDENTITY_KEYS)
    block_ids_by_ref = _collection_id_index(blocks, "block_ref", BLOCK_IDENTITY_KEYS)

    entities: list[dict[str, Any]] = []
    for entity in sorted(
        apply_result.entities,
        key=lambda item: (item.sequence_index, item.entity_id, str(item.id)),
    ):
        layout_ref = _string_ref(entity.layout_ref)
        layer_ref = _string_ref(entity.layer_ref)
        block_ref = _string_ref(entity.block_ref)
        parent_entity_ref = _string_ref(entity.parent_entity_ref)
        geometry_json = _copy_json_mapping(entity.geometry_json)
        bbox = compute_entity_bbox(geometry_json)
        entities.append(
            {
                "id": uuid.uuid4(),
                "sequence_index": entity.sequence_index,
                "entity_id": entity.entity_id,
                "entity_type": entity.entity_type,
                "entity_schema_version": entity.entity_schema_version,
                "parent_entity_ref": parent_entity_ref,
                "confidence_json": _copy_json_mapping(entity.confidence_json),
                "geometry_json": geometry_json,
                "bbox_min_x": bbox[0] if bbox is not None else None,
                "bbox_min_y": bbox[1] if bbox is not None else None,
                "bbox_max_x": bbox[2] if bbox is not None else None,
                "bbox_max_y": bbox[3] if bbox is not None else None,
                "properties_json": _copy_json_mapping(entity.properties_json),
                "provenance_json": _copy_json_mapping(entity.provenance_json),
                "canonical_entity_json": (
                    _copy_json_mapping(entity.canonical_entity_json)
                    if entity.canonical_entity_json is not None
                    else None
                ),
                "layout_ref": layout_ref,
                "layer_ref": layer_ref,
                "block_ref": block_ref,
                "source_identity": _string_ref(entity.source_identity),
                "source_hash": _hash_ref(entity.source_hash),
                "layout_id": layout_ids_by_ref.get(layout_ref) if layout_ref is not None else None,
                "layer_id": layer_ids_by_ref.get(layer_ref) if layer_ref is not None else None,
                "block_id": block_ids_by_ref.get(block_ref) if block_ref is not None else None,
            }
        )

    entity_row_ids_by_entity_id = {row["entity_id"]: row["id"] for row in entities}
    for row in entities:
        parent_entity_ref = row["parent_entity_ref"]
        row["parent_entity_row_id"] = (
            entity_row_ids_by_entity_id.get(parent_entity_ref)
            if parent_entity_ref is not None
            else None
        )

    counts_json = _json_object(base_manifest.counts_json)
    counts_json.update(
        {
            "layouts": len(layouts),
            "layers": len(layers),
            "blocks": len(blocks),
            "entities": len(entities),
        }
    )
    return _RevisionMaterializationRows(
        counts_json=counts_json,
        layouts=layouts,
        layers=layers,
        blocks=blocks,
        entities=entities,
    )


def _order_revision_entity_insert_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Order entity rows so parent self-FKs insert before children when possible."""
    if len(rows) < 2:
        return rows

    row_by_id = {row["id"]: row for row in rows}
    pending_parent_counts = {row["id"]: 0 for row in rows}
    child_ids_by_parent: dict[UUID, list[UUID]] = {}
    original_order_by_id = {row["id"]: index for index, row in enumerate(rows)}

    for row in rows:
        row_id = row["id"]
        parent_row_id = row.get("parent_entity_row_id")
        if isinstance(parent_row_id, UUID) and parent_row_id in row_by_id:
            pending_parent_counts[row_id] += 1
            child_ids_by_parent.setdefault(parent_row_id, []).append(row_id)

    ready: list[tuple[int, int, UUID]] = []
    for row in rows:
        row_id = row["id"]
        if pending_parent_counts[row_id] == 0:
            heapq.heappush(
                ready,
                (int(row["sequence_index"]), original_order_by_id[row_id], row_id),
            )

    ordered_rows: list[dict[str, Any]] = []
    while ready:
        _, _, row_id = heapq.heappop(ready)
        ordered_rows.append(row_by_id[row_id])
        for child_row_id in child_ids_by_parent.get(row_id, []):
            pending_parent_counts[child_row_id] -= 1
            if pending_parent_counts[child_row_id] == 0:
                child_row = row_by_id[child_row_id]
                heapq.heappush(
                    ready,
                    (
                        int(child_row["sequence_index"]),
                        original_order_by_id[child_row_id],
                        child_row_id,
                    ),
                )

    if len(ordered_rows) == len(rows):
        return ordered_rows

    # Any rows left unprocessed form a parent_entity_row_id cycle (every acyclic row is
    # reachable from a parentless root via Kahn's algorithm). Inserting them in arbitrary
    # order would violate the self-referential, non-deferrable FK with an opaque
    # IntegrityError; surface the cycle members explicitly instead.
    ordered_row_ids = {row["id"] for row in ordered_rows}
    cyclic_ids = [str(row["id"]) for row in rows if row["id"] not in ordered_row_ids]
    raise ValueError(
        "Revision entities have a cyclic parent_entity_row_id dependency and cannot be "
        f"ordered for insert ({len(cyclic_ids)} involved): {cyclic_ids[:10]}"
    )
