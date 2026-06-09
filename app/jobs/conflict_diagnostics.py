"""Pure builders for persisted job-failure diagnostic payloads.

These helpers shape deterministic, size-bounded JSON metadata for job failures
(quantity gate failures, quantity conflict summaries, and changeset-apply stale
conflicts). They are pure transformations over their arguments — no DB access, no
worker module state — extracted from ``worker.py`` to keep that module focused on
orchestration.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from app.cad.changeset import ChangeSetApplyConflict

_QUANTITY_CONFLICT_SUMMARY_LIMIT = 5
_QUANTITY_CONFLICT_ENTITY_ID_LIMIT = 10
_QUANTITY_CONFLICT_DETAIL_ITEM_LIMIT = 10
_QUANTITY_CONFLICT_DETAIL_DEPTH_LIMIT = 3
_QUANTITY_CONFLICT_TEXT_LIMIT = 200


def _quantity_gate_details(
    *,
    drawing_revision_id: UUID,
    review_state: str,
    validation_status: str,
    quantity_gate: str,
) -> dict[str, Any]:
    """Build stable structured metadata for quantity gate failures."""
    return {
        "drawing_revision_id": str(drawing_revision_id),
        "review_state": review_state,
        "validation_status": validation_status,
        "quantity_gate": quantity_gate,
    }


def _bounded_conflict_text(value: Any) -> str | None:
    """Return a bounded string payload for persisted conflict metadata."""
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    return normalized[:_QUANTITY_CONFLICT_TEXT_LIMIT]


def _bounded_conflict_json(value: Any, *, depth: int = 0) -> Any:
    """Bound persisted conflict details to stable JSON-safe payloads."""
    if value is None or isinstance(value, bool | int | float):
        return value
    if isinstance(value, str):
        return value[:_QUANTITY_CONFLICT_TEXT_LIMIT]
    if depth >= _QUANTITY_CONFLICT_DETAIL_DEPTH_LIMIT:
        return _bounded_conflict_text(value)
    if isinstance(value, dict):
        bounded: dict[str, Any] = {}
        for key, nested_value in list(value.items())[:_QUANTITY_CONFLICT_DETAIL_ITEM_LIMIT]:
            normalized_key = _bounded_conflict_text(key)
            if normalized_key is None:
                continue
            bounded[normalized_key] = _bounded_conflict_json(nested_value, depth=depth + 1)
        return bounded
    if isinstance(value, list | tuple):
        return [
            _bounded_conflict_json(item, depth=depth + 1)
            for item in list(value)[:_QUANTITY_CONFLICT_DETAIL_ITEM_LIMIT]
        ]
    return _bounded_conflict_text(value)


def _bounded_conflict_entity_ids(entity_ids: Any) -> list[str]:
    """Copy contributor conflict entity ids into a bounded JSON-safe list."""
    if not isinstance(entity_ids, tuple | list):
        return []

    bounded_ids: list[str] = []
    for entity_id in entity_ids[:_QUANTITY_CONFLICT_ENTITY_ID_LIMIT]:
        normalized = _bounded_conflict_text(entity_id)
        if normalized is not None:
            bounded_ids.append(normalized)
    return bounded_ids


def _build_quantity_conflict_summaries(conflicts: Sequence[Any]) -> list[dict[str, Any]]:
    """Build bounded persisted summaries for deterministic quantity conflicts."""
    summaries: list[dict[str, Any]] = []
    for conflict in conflicts[:_QUANTITY_CONFLICT_SUMMARY_LIMIT]:
        summaries.append(
            {
                "dedup_key": _bounded_conflict_text(getattr(conflict, "dedup_key", None)),
                "entity_ids": _bounded_conflict_entity_ids(getattr(conflict, "entity_ids", ())),
                "reason": _bounded_conflict_text(getattr(conflict, "reason", None)),
                "details": _bounded_conflict_json(getattr(conflict, "details", None)),
            }
        )
    return summaries


def _build_changeset_apply_conflict_details(
    result: ChangeSetApplyConflict,
) -> dict[str, Any]:
    """Convert a stale apply conflict into the shared job failure detail shape."""
    return {
        "change_set_id": str(result.details.change_set_id),
        "base_revision_id": str(result.details.base_revision_id),
        "base_revision_sequence": result.details.base_revision_sequence,
        "current_revision_id": str(result.details.current_revision_id),
        "current_revision_sequence": result.details.current_revision_sequence,
        "conflicting_targets": [
            {
                "operation_id": str(target.operation_id),
                "sequence_index": target.sequence_index,
                "operation_type": target.operation_type,
                "target_revision_entity_id": (
                    str(target.target_revision_entity_id)
                    if target.target_revision_entity_id is not None
                    else None
                ),
                "entity_id": target.entity_id,
                "expected_source_identity": target.expected_source_identity,
                "expected_source_hash": target.expected_source_hash,
            }
            for target in result.details.conflicting_targets
        ],
    }
