"""Shared validation utility helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from typing import Any
from uuid import UUID

from app.ingestion.contracts import AdapterResult

from ._constants import _FAIL_STATUS_VALUES, _PLACEHOLDER_STATUS_VALUES, _REVIEW_STATUS_VALUES


def _mapping_value(value: Any, key: str) -> Any | None:
    if isinstance(value, Mapping):
        return value.get(key)

    return None


def _normalized_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None

    normalized = value.strip().lower()
    return normalized or None


def _extract_placeholder_semantics(canonical_json: Mapping[str, Any]) -> Mapping[str, Any] | None:
    candidates: tuple[Any | None, ...] = (
        canonical_json.get("placeholder_semantics"),
        _mapping_value(canonical_json.get("metadata"), "placeholder_semantics"),
    )
    for candidate in candidates:
        if isinstance(candidate, Mapping):
            return candidate

    return None


def _placeholder_semantics_requires_review(placeholder_semantics: Mapping[str, Any]) -> bool:
    if placeholder_semantics.get("review_required") is True:
        return True

    if _normalized_string(placeholder_semantics.get("quantity_gate")) == "review_gated":
        return True

    return _normalized_string(placeholder_semantics.get("status")) in _PLACEHOLDER_STATUS_VALUES


def _metadata_candidate(canonical_json: Mapping[str, Any], *keys: str) -> Any | None:
    metadata = canonical_json.get("metadata")
    for key in keys:
        value = canonical_json.get(key)
        if value is not None:
            return value
        mapped_value = _mapping_value(metadata, key)
        if mapped_value is not None:
            return mapped_value

    return None


def _extract_meaningful_metadata(canonical_json: Mapping[str, Any], *keys: str) -> Any | None:
    for key in keys:
        candidate = _metadata_candidate(canonical_json, key)
        if _is_meaningful_value(candidate):
            return _json_compatible(candidate)

    return None


def _is_meaningful_value(value: Any) -> bool:
    if value is None or value is False:
        return False
    if isinstance(value, str):
        normalized = value.strip().lower()
        return (
            bool(normalized)
            and normalized not in _REVIEW_STATUS_VALUES
            and normalized not in _FAIL_STATUS_VALUES
        )
    if isinstance(value, (int, float)):
        return value > 0
    if isinstance(value, Mapping):
        return bool(value) and any(_is_meaningful_value(item) for item in value.values())
    if isinstance(value, (list, tuple, set, frozenset)):
        return any(_is_meaningful_value(item) for item in value)

    return True


def _adapter_warning_message(warning: Mapping[str, Any], *, index: int) -> str:
    for key in ("message", "summary", "warning"):
        value = warning.get(key)
        if value is not None:
            return str(value)

    return f"Adapter warning {index} was reported."


def _adapter_warning_target_ref(warning: Mapping[str, Any], *, index: int) -> str:
    for key in ("target_ref", "source_ref", "entity_ref", "ref", "code"):
        value = warning.get(key)
        if value is not None:
            return str(value)

    return f"adapter-warning-{index}"


def _build_summary(
    *,
    canonical_json: Mapping[str, Any],
    checks: list[dict[str, Any]],
    findings: list[dict[str, Any]],
) -> dict[str, Any]:
    check_status_totals = {
        "pass": 0,
        "warning": 0,
        "review_required": 0,
        "fail": 0,
    }
    for check in checks:
        status = str(check["status"])
        check_status_totals[status] = check_status_totals.get(status, 0) + 1

    severity_totals = {"info": 0, "warning": 0, "error": 0, "critical": 0}
    for finding in findings:
        severity = str(finding["severity"])
        severity_totals[severity] = severity_totals.get(severity, 0) + 1

    return {
        "checks_total": len(checks),
        "findings_total": len(findings),
        "warnings_total": severity_totals["warning"],
        "errors_total": severity_totals["error"],
        "critical_total": severity_totals["critical"],
        "check_status_totals": check_status_totals,
        "entity_counts": _entity_counts(canonical_json),
    }


def _entity_counts(canonical_json: Mapping[str, Any]) -> dict[str, int]:
    return {
        "layouts": _sequence_length(canonical_json.get("layouts")),
        "layers": _sequence_length(canonical_json.get("layers")),
        "blocks": _sequence_length(canonical_json.get("blocks")),
        "entities": _sequence_length(canonical_json.get("entities")),
    }


def _sequence_length(value: Any) -> int:
    if isinstance(value, (list, tuple)):
        return len(value)

    return 0


_BLOCK_REF_KEYS = ("block_ref", "name", "block_handle")


def _entity_kind(entity: Mapping[str, Any]) -> str | None:
    """Entity type token, matching block_expansion's precedence (entity_type|kind|type)."""
    for key in ("entity_type", "kind", "type"):
        value = entity.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def geometry_placement(canonical_json: Mapping[str, Any]) -> dict[str, Any]:
    """Spatial-completeness signal: block geometry placed into the world vs left unexpanded (#542).

    Post-#541 the block-expansion pass appends world-placed copies of block-instance geometry to
    ``entities`` (tagged ``provenance_json.origin == "block_expansion"`` with the ``source_block``
    it came from). This measures whether block-definition geometry actually reached the spatial
    model: a block definition that carries leaf (non-INSERT) geometry but whose ref never produced
    a placement is **unexpanded** — its geometry is absent from the model (the #542 blind spot,
    distinct from entity-mapping ``mapped_ratio``). ``ratio`` is 1.0 when every block with geometry
    placed at least once; it drops as blocks are stranded. Descriptive + honest, never gating.

    Accounting note: ``placed_from_blocks`` counts placed entity *instances* (a block referenced N
    times contributes N), whereas ``in_block_unexpanded`` counts a stranded block's leaf entities
    once. The ratio is therefore instance-weighted, not type-weighted — it can understate a small
    stranded block against many placed instances, but is monotonic (==1.0 iff nothing is stranded)
    and never exceeds 1.0. ``unexpanded_blocks`` is the unweighted count for the blunt view.
    """

    raw_entities = canonical_json.get("entities")
    entities = list(raw_entities) if isinstance(raw_entities, (list, tuple)) else []
    raw_blocks = canonical_json.get("blocks")
    blocks = list(raw_blocks) if isinstance(raw_blocks, (list, tuple)) else []

    placed_from_blocks = 0
    placed_source_refs: set[str] = set()
    for entity in entities:
        if not isinstance(entity, Mapping):
            continue
        provenance = entity.get("provenance_json")
        if not isinstance(provenance, Mapping) or provenance.get("origin") != "block_expansion":
            continue
        placed_from_blocks += 1
        detail = provenance.get("block_expansion")
        if isinstance(detail, Mapping):
            ref = detail.get("source_block")
            if isinstance(ref, str) and ref:
                placed_source_refs.add(ref)

    in_block_unexpanded = 0
    unexpanded_blocks = 0
    blocks_with_geometry = 0
    for block in blocks:
        if not isinstance(block, Mapping):
            continue
        children = block.get("entities")
        if not isinstance(children, (list, tuple)):
            continue
        leaf_geometry = [
            child
            for child in children
            if isinstance(child, Mapping) and _entity_kind(child) != "insert"
        ]
        if not leaf_geometry:
            continue
        blocks_with_geometry += 1
        # A block is "placed" when any of its identifying refs produced a placement. The expander
        # records ``source_block`` as the INSERT's ref, which matches one of the definition keys.
        refs = {str(block.get(key)) for key in _BLOCK_REF_KEYS if isinstance(block.get(key), str)}
        if not (refs & placed_source_refs):
            in_block_unexpanded += len(leaf_geometry)
            unexpanded_blocks += 1

    denominator = placed_from_blocks + in_block_unexpanded
    ratio = round(placed_from_blocks / denominator, 4) if denominator else 1.0
    return {
        "placed_from_blocks": placed_from_blocks,
        "in_block_unexpanded": in_block_unexpanded,
        "unexpanded_blocks": unexpanded_blocks,
        "blocks_with_geometry": blocks_with_geometry,
        "ratio": ratio,
    }


def _build_coverage(canonical_json: Mapping[str, Any]) -> dict[str, Any]:
    """Honest extraction-coverage metrics for the validation report.

    Reports what was actually extracted (mapped vs. unmapped geometry, by type and by reason;
    layer/block resolution; review-flagged entities) so progress is measurable and downstream
    consumers (and AI agents) get an explicit, non-gating provenance signal — distinct from the
    coarse confidence score. Computed from the canonical payload; no ground truth required.
    """

    raw_entities = canonical_json.get("entities")
    entities = list(raw_entities) if isinstance(raw_entities, (list, tuple)) else []

    by_type: dict[str, int] = {}
    unmapped_by_reason: dict[str, int] = {}
    entities_with_layer_ref = 0
    review_flagged = 0
    for entity in entities:
        if not isinstance(entity, Mapping):
            continue
        entity_type = str(
            entity.get("entity_type") or entity.get("type") or entity.get("kind") or "unknown"
        )
        by_type[entity_type] = by_type.get(entity_type, 0) + 1
        if entity.get("layer_ref"):
            entities_with_layer_ref += 1
        confidence = entity.get("confidence")
        if isinstance(confidence, Mapping) and confidence.get("review_required"):
            review_flagged += 1
        if entity_type == "unknown":
            geometry = entity.get("geometry")
            summary = geometry.get("geometry_summary") if isinstance(geometry, Mapping) else None
            reason = None
            if isinstance(summary, Mapping):
                reason = summary.get("reason") or summary.get("source_type")
            reason_key = str(reason or "unspecified")
            unmapped_by_reason[reason_key] = unmapped_by_reason.get(reason_key, 0) + 1

    total = len(entities)
    unmapped = by_type.get("unknown", 0)
    mapped = total - unmapped

    raw_blocks = canonical_json.get("blocks")
    blocks = list(raw_blocks) if isinstance(raw_blocks, (list, tuple)) else []
    block_child_geometry = 0
    for block in blocks:
        if not isinstance(block, Mapping):
            continue
        children = block.get("entities")
        if isinstance(children, (list, tuple)):
            block_child_geometry += len(children)

    coverage: dict[str, Any] = {
        "schema_version": "0.1",
        "entities": {
            "total": total,
            "mapped": mapped,
            "unmapped": unmapped,
            "mapped_ratio": round(mapped / total, 4) if total else 0.0,
            "by_type": dict(sorted(by_type.items())),
        },
        "unmapped_by_reason": dict(sorted(unmapped_by_reason.items())),
        "layers": {
            "count": _sequence_length(canonical_json.get("layers")),
            "entities_with_layer_ref": entities_with_layer_ref,
            # Provenance of the layer set: native ("ocg") vs derived ("pen_signature") vs the
            # source adapter's own table; present when the adapter records it.
            "source": canonical_json.get("layer_source"),
        },
        "blocks": {
            "count": len(blocks),
            "child_geometry_count": block_child_geometry,
        },
        "geometry_placement": geometry_placement(canonical_json),
        "review_flagged_entities": review_flagged,
    }

    metadata = canonical_json.get("metadata")
    adapter_counts = metadata.get("entity_counts") if isinstance(metadata, Mapping) else None
    if isinstance(adapter_counts, Mapping):
        coverage["adapter_counts"] = {
            str(key): int(value)
            for key, value in adapter_counts.items()
            if isinstance(value, int) and not isinstance(value, bool)
        }

    return coverage


def _confidence_score(result: AdapterResult) -> float:
    if result.confidence is None or result.confidence.score is None:
        return 0.0

    return float(result.confidence.score)


def _json_compatible(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _json_compatible(asdict(value))

    if isinstance(value, Mapping):
        return {str(key): _json_compatible(item) for key, item in value.items()}

    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_compatible(item) for item in value]

    if isinstance(value, UUID):
        return str(value)

    return value
