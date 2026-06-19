"""Round-trip reconciliation: structural fidelity of the canonical model (#523).

Coverage says how *much* of the source we mapped; reconciliation asks whether the
canonical model is internally coherent — the adapter's declared counts match the
materialized arrays, units are present, and the drawing has a finite extent — plus
a descriptive structural fingerprint (layout/layer/block/entity counts, block
instances, nesting). It is deterministic and pure over ``canonical_json`` (no
re-parsing): the adapter's declared ``entity_counts`` is the source-side reference
and the arrays are the canonical side.

Gating is intentionally narrow in v1: only a declared-vs-actual count mismatch is
drift (a ``warning`` → ``valid_with_warnings``, never ``invalid``). Reference
integrity (layer/block/parent resolution) is descriptive-only here — faithful
resolution needs the same semantic resolver as layer-mapping/materialization and
is deferred to a follow-up rather than re-implemented (and risking false drift).
"""

from collections.abc import Mapping, Sequence
from typing import Any

from app.ingestion.entity_geometry import compute_entity_bbox

from .checks._common import _check, _pass_check

RECONCILIATION_SCHEMA_VERSION = "0.1"
_CHECK_KEY = "reconciliation"

_LAYER_KEYS = ("layer_ref", "layer", "layer_name")
_GEOMETRY_KEYS = ("geometry", "geometry_json")
_COLLECTIONS = ("layouts", "layers", "blocks", "entities")


def build_reconciliation(canonical_json: Mapping[str, Any]) -> dict[str, Any]:
    """Build the reconciliation report: per-invariant match/drift over the canonical model."""

    entities = _as_list(canonical_json.get("entities"))

    invariants = [
        _declared_counts(canonical_json),
        _structure(canonical_json, entities),
        _units(canonical_json),
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
