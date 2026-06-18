"""Pure row-building for job finalization (issue D5a).

These builders turn in-memory engine results / execution inputs into the ORM
row objects that the finalizers persist. They perform no database access, so the
row-construction logic is unit-testable from payloads and engine results alone.
Finalizers call the builders and own the session/commit lifecycle.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from app.estimating.engine.contracts import EstimateEngineOutput
from app.estimating.quantities.contracts import QuantityEngineResult
from app.jobs.execution_inputs import _QuantityTakeoffExecutionInput
from app.models.estimate_version import EstimateItem, EstimateSnapshotEntry, EstimateVersion
from app.models.job import JobType
from app.models.quantity_takeoff import QuantityItem, QuantityItemKind, QuantityTakeoff


@dataclass(frozen=True, slots=True)
class QuantityTakeoffRows:
    """Quantity takeoff header plus its quantity item rows, ready to persist."""

    takeoff: QuantityTakeoff
    items: list[QuantityItem]


@dataclass(frozen=True, slots=True)
class EstimateRows:
    """Estimate version plus its snapshot entries and line items, ready to persist."""

    version: EstimateVersion
    snapshot_entries: list[EstimateSnapshotEntry]
    line_items: list[EstimateItem]


def _nonempty_quantity_type(value: str | None) -> str:
    """Normalize persisted quantity type labels to non-empty strings."""
    if value is None:
        return "unknown"
    normalized = value.strip()
    return normalized or "unknown"


def _nonempty_quantity_unit(value: str | None) -> str:
    """Normalize persisted quantity units to non-empty strings."""
    if value is None:
        return "unknown"
    normalized = value.strip()
    return normalized or "unknown"


def _duplicate_entity_ids_json(values: tuple[str, ...]) -> list[str]:
    """Copy duplicate contributor lineage ids into a JSON-safe list."""
    return [value for value in values if value]


def _serialize_quantity_context(context: Any) -> str | None:
    """Render quantity context as a deterministic persisted label suffix."""
    if context is None:
        return None
    if isinstance(context, str):
        normalized = context.strip()
        return normalized or None

    try:
        serialized = json.dumps(context, sort_keys=True, separators=(",", ":"))
    except TypeError:
        serialized = str(context).strip()

    return serialized or None


def _quantity_item_type_label(quantity_type: str | None, context: Any) -> str:
    """Persist the quantity type with stable quantity-context disambiguation."""
    normalized_quantity_type = _nonempty_quantity_type(quantity_type)
    serialized_context = _serialize_quantity_context(context)
    if serialized_context is None:
        return normalized_quantity_type

    return f"{normalized_quantity_type}:{serialized_context}"


def _build_quantity_items(
    *,
    quantity_takeoff_id: UUID,
    project_id: UUID,
    drawing_revision_id: UUID,
    validation_status: str,
    result: QuantityEngineResult,
) -> list[QuantityItem]:
    """Build immutable quantity item rows for a takeoff result."""
    items: list[QuantityItem] = []

    for contributor in result.contributors:
        items.append(
            QuantityItem(
                id=uuid.uuid4(),
                quantity_takeoff_id=quantity_takeoff_id,
                project_id=project_id,
                drawing_revision_id=drawing_revision_id,
                item_kind=QuantityItemKind.CONTRIBUTOR.value,
                quantity_type=_quantity_item_type_label(
                    contributor.quantity_type,
                    getattr(contributor, "context", None),
                ),
                value=contributor.value,
                unit=_nonempty_quantity_unit(contributor.unit),
                validation_status=validation_status,
                source_entity_id=contributor.entity_id,
                excluded_source_entity_ids_json=_duplicate_entity_ids_json(
                    contributor.duplicate_entity_ids
                ),
            )
        )

    for aggregate in result.aggregates:
        items.append(
            QuantityItem(
                id=uuid.uuid4(),
                quantity_takeoff_id=quantity_takeoff_id,
                project_id=project_id,
                drawing_revision_id=drawing_revision_id,
                item_kind=QuantityItemKind.AGGREGATE.value,
                quantity_type=_quantity_item_type_label(
                    aggregate.quantity_type,
                    getattr(aggregate, "context", None),
                ),
                value=aggregate.total,
                unit=_nonempty_quantity_unit(aggregate.unit),
                validation_status=validation_status,
                source_entity_id=None,
                excluded_source_entity_ids_json=[],
            )
        )

    for exclusion in result.exclusions:
        items.append(
            QuantityItem(
                id=uuid.uuid4(),
                quantity_takeoff_id=quantity_takeoff_id,
                project_id=project_id,
                drawing_revision_id=drawing_revision_id,
                item_kind=QuantityItemKind.EXCLUSION.value,
                quantity_type=_quantity_item_type_label(
                    exclusion.quantity_type,
                    getattr(exclusion, "context", None),
                ),
                value=None,
                unit="unknown",
                validation_status=validation_status,
                source_entity_id=exclusion.entity_id,
                excluded_source_entity_ids_json=[],
            )
        )

    return items


def build_quantity_takeoff_rows(
    *,
    project_id: UUID,
    source_file_id: UUID,
    source_job_id: UUID,
    execution: _QuantityTakeoffExecutionInput,
    result: QuantityEngineResult,
) -> QuantityTakeoffRows:
    """Build the quantity takeoff header and its item rows from an engine result.

    Pure: allocates ids and constructs ORM instances; performs no DB access.
    """
    quantity_takeoff_id = uuid.uuid4()
    takeoff = QuantityTakeoff(
        id=quantity_takeoff_id,
        project_id=project_id,
        source_file_id=source_file_id,
        drawing_revision_id=execution.drawing_revision_id,
        source_job_id=source_job_id,
        source_job_type=JobType.QUANTITY_TAKEOFF.value,
        validation_status=execution.validation_status,
    )
    items = _build_quantity_items(
        quantity_takeoff_id=quantity_takeoff_id,
        project_id=project_id,
        drawing_revision_id=execution.drawing_revision_id,
        validation_status=execution.validation_status,
        result=result,
    )
    return QuantityTakeoffRows(takeoff=takeoff, items=items)


def build_estimate_rows(output: EstimateEngineOutput) -> EstimateRows:
    """Materialize estimate version, snapshot entry, and line item rows.

    Pure: reads the engine output's model kwargs and constructs ORM instances;
    performs no DB access.
    """
    version = EstimateVersion(**output.estimate_version_model_kwargs())
    snapshot_entries = [
        EstimateSnapshotEntry(**kwargs) for kwargs in output.snapshot_entry_model_kwargs()
    ]
    line_items = [EstimateItem(**kwargs) for kwargs in output.line_item_model_kwargs()]
    return EstimateRows(
        version=version,
        snapshot_entries=snapshot_entries,
        line_items=line_items,
    )
