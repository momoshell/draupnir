"""Round-trip reconciliation: structural fidelity of the canonical model (#523).

Coverage says how *much* of the source we mapped; reconciliation asks whether the
canonical model is internally coherent — the adapter's declared counts match the
materialized arrays, units are present, and the drawing has a finite extent — plus
a descriptive structural fingerprint (layout/layer/block/entity counts, block
instances, nesting). It is deterministic and pure over ``canonical_json`` (no
re-parsing): the adapter's declared ``entity_counts`` is the source-side reference
and the arrays are the canonical side.

Gating (a ``warning`` → ``valid_with_warnings``, never ``invalid``): a declared-vs-actual
count mismatch, and — since #539 — reference integrity (layer/block/parent resolution).
Reference matching uses the single authoritative set-membership rule in ``ref_resolution``
(shared with layer-mapping and materialization linkage), so a reference that resolves to any
declared identifier is not a false orphan; only genuinely unresolved references drift.
"""

from collections.abc import Mapping, Sequence
from typing import Any

from app.ingestion.entity_geometry import compute_entity_bbox
from app.ingestion.ref_resolution import (
    BLOCK_IDENTITY_KEYS,
    ENTITY_BLOCK_KEYS,
    ENTITY_ID_KEYS,
    ENTITY_LAYER_KEYS,
    ENTITY_PARENT_KEYS,
    LAYER_IDENTITY_KEYS,
    collection_identity_index,
    first_ref,
    ref_set,
)

from .checks._common import _check, _pass_check

RECONCILIATION_SCHEMA_VERSION = "0.1"
_CHECK_KEY = "reconciliation"

_LAYER_KEYS = ("layer_ref", "layer", "layer_name")
_GEOMETRY_KEYS = ("geometry", "geometry_json")
_COLLECTIONS = ("layouts", "layers", "blocks", "entities")


def build_reconciliation(canonical_json: Mapping[str, Any]) -> dict[str, Any]:
    """Build the reconciliation report: per-invariant match/drift over the canonical model."""

    entities = _as_list(canonical_json.get("entities"))
    layers = _as_list(canonical_json.get("layers"))
    blocks = _as_list(canonical_json.get("blocks"))

    invariants = [
        _declared_counts(canonical_json),
        _structure(canonical_json, entities),
        _references(entities, layers, blocks),
        _units(canonical_json),
        _census(canonical_json),
        _extents(entities),
    ]

    drifted = [inv["key"] for inv in invariants if inv["gating"] and inv["status"] == "drift"]
    return {
        "schema_version": RECONCILIATION_SCHEMA_VERSION,
        "status": "drift" if drifted else "match",
        "drifted_invariants": drifted,
        "invariants": invariants,
    }


def build_reconciliation_check(reconciliation: Mapping[str, Any]) -> dict[str, Any]:
    """Build the ``reconciliation`` validation check from a reconciliation report."""

    drifted = list(reconciliation.get("drifted_invariants") or [])
    if drifted:
        return _check(
            check_key=_CHECK_KEY,
            status="warning",
            summary_message=f"Canonical model drifts from the source on: {', '.join(drifted)}.",
            details={"drifted_invariants": drifted},
        )
    return _pass_check(_CHECK_KEY, "Canonical model is structurally consistent with the source.")


def _declared_counts(canonical_json: Mapping[str, Any]) -> dict[str, Any]:
    """Compare the adapter's declared entity_counts to the materialized array lengths."""

    actual = {name: _len(canonical_json.get(name)) for name in _COLLECTIONS}
    declared = canonical_json.get("entity_counts")
    if not isinstance(declared, Mapping):
        return {
            "key": "declared_counts",
            "status": "not_applicable",
            "gating": True,
            "reason": "Adapter did not declare entity_counts.",
            "actual": actual,
        }

    deltas = {
        name: {"declared": int(declared.get(name, 0)), "actual": value}
        for name, value in actual.items()
        if int(declared.get(name, 0)) != value
    }
    return {
        "key": "declared_counts",
        "status": "drift" if deltas else "match",
        "gating": True,
        "deltas": deltas,
        "actual": actual,
    }


def _structure(
    canonical_json: Mapping[str, Any], entities: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    """Descriptive structural fingerprint (informational): the shape of the canonical model."""

    block_instances = sum(1 for e in entities if _str(e.get("block_ref")) is not None)
    nested = sum(1 for e in entities if _str(e.get("parent_entity_ref")) is not None)
    distinct_layers = {ref for e in entities if (ref := _first_str(e, _LAYER_KEYS)) is not None}
    return {
        "key": "structure",
        "status": "reported",
        "gating": False,
        "layouts": _len(canonical_json.get("layouts")),
        "layers": _len(canonical_json.get("layers")),
        "block_definitions": _len(canonical_json.get("blocks")),
        "entities": len(entities),
        "block_instances": block_instances,
        "nested_entities": nested,
        "distinct_layer_refs": len(distinct_layers),
    }


def _references(
    entities: Sequence[Mapping[str, Any]],
    layers: Sequence[Mapping[str, Any]],
    blocks: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Gate on entity references that resolve to no declared definition (#539).

    Uses the single authoritative matching rule (:func:`collection_identity_index`): a declared
    layer/block is identified by the SET of all its candidate refs, and an entity links iff its
    resolved ref is in that set — the same rule layer-mapping and materialization linkage use, so
    "is this an orphan?" has one answer everywhere. Gating: a genuine orphan is ``drift`` →
    ``warning`` (``valid_with_warnings``, never ``invalid``). The set-membership rule means an
    entry matched by any of its identifiers (e.g. a layer ``{"ref":"L","name":"A-WALL"}`` referenced
    as either) is not a false orphan; only references with no declared target at all drift.
    """

    layer_ref_index = collection_identity_index(layers, LAYER_IDENTITY_KEYS)
    block_ref_index = collection_identity_index(blocks, BLOCK_IDENTITY_KEYS)
    entity_id_index: set[str] = set()
    for entity in entities:
        entity_id_index |= ref_set(entity, ENTITY_ID_KEYS)

    orphan_layers = sorted(
        {
            ref
            for entity in entities
            if (ref := first_ref(entity, ENTITY_LAYER_KEYS)) and ref not in layer_ref_index
        }
    )
    orphan_blocks = sorted(
        {
            ref
            for entity in entities
            if (ref := first_ref(entity, ENTITY_BLOCK_KEYS)) and ref not in block_ref_index
        }
    )
    dangling_parents = sorted(
        {
            ref
            for entity in entities
            if (ref := first_ref(entity, ENTITY_PARENT_KEYS)) and ref not in entity_id_index
        }
    )

    has_orphans = bool(orphan_layers or orphan_blocks or dangling_parents)
    return {
        "key": "references",
        "status": "drift" if has_orphans else "match",
        "gating": True,
        "orphan_layer_refs": orphan_layers,
        "orphan_block_refs": orphan_blocks,
        "dangling_parent_refs": dangling_parents,
    }


def _units(canonical_json: Mapping[str, Any]) -> dict[str, Any]:
    """Report whether units resolved (informational — 'unknown' is legitimate for PDFs)."""

    units = canonical_json.get("units")
    normalized = units.get("normalized") if isinstance(units, Mapping) else None
    resolved = bool(normalized) and normalized != "unknown"
    return {
        "key": "units",
        "status": "match" if resolved else "unresolved",
        "gating": False,
        "normalized": normalized,
    }


def _census(canonical_json: Mapping[str, Any]) -> dict[str, Any]:
    """Report the source census: what the reader surfaced vs what we materialized (#563).

    Descriptive (non-gating): turns silent extraction loss into a number. ``incomplete``
    means the adapter dropped drawable records or LibreDWG could not resolve some classes
    (proxy/zombie reader blind spots); neither flips ``validation_status`` in v1.
    """

    census = canonical_json.get("census")
    if not isinstance(census, Mapping):
        return {
            "key": "census",
            "status": "not_applicable",
            "gating": False,
            "reason": "Adapter did not emit a source census.",
        }

    dropped = census.get("dropped")
    dropped_total = int(dropped.get("total") or 0) if isinstance(dropped, Mapping) else 0
    unsupported_classes = census.get("unsupported_classes")
    classes_count = _len(unsupported_classes)
    has_loss = dropped_total > 0 or classes_count > 0
    return {
        "key": "census",
        "status": "incomplete" if has_loss else "complete",
        "gating": False,
        "raw_object_total": int(census.get("raw_object_total") or 0),
        "drawable_candidates": int(census.get("drawable_candidates") or 0),
        "materialized": int(census.get("materialized") or 0),
        "dropped": dropped_total,
        "unsupported_classes": classes_count,
    }


def _extents(entities: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Aggregate the drawing extent from entity geometry (informational)."""

    xs_min, ys_min, xs_max, ys_max = [], [], [], []
    for entity in entities:
        box = _entity_bbox(entity)
        if box is None:
            continue
        xs_min.append(box[0])
        ys_min.append(box[1])
        xs_max.append(box[2])
        ys_max.append(box[3])

    if not xs_min:
        return {"key": "extents", "status": "missing", "gating": False, "bbox": None}

    bbox = [min(xs_min), min(ys_min), max(xs_max), max(ys_max)]
    degenerate = bbox[0] == bbox[2] or bbox[1] == bbox[3]
    return {
        "key": "extents",
        "status": "degenerate" if degenerate else "match",
        "gating": False,
        "bbox": bbox,
        "entities_with_extent": len(xs_min),
    }


def _entity_bbox(entity: Mapping[str, Any]) -> tuple[float, float, float, float] | None:
    for geometry_key in _GEOMETRY_KEYS:
        bbox = compute_entity_bbox(entity.get(geometry_key))
        if bbox is not None:
            return bbox
    return None


def _as_list(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, (list, tuple)):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _len(value: Any) -> int:
    return len(value) if isinstance(value, (list, tuple)) else 0


def _str(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _first_str(entry: Mapping[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = _str(entry.get(key))
        if value is not None:
            return value
    return None
