"""Quantity takeoff API schemas."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class QuantityTakeoffRead(BaseModel):
    """Committed quantity takeoff payload."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    project_id: UUID
    source_file_id: UUID
    drawing_revision_id: UUID
    source_job_id: UUID
    source_job_type: str
    review_state: str
    validation_status: str
    quantity_gate: str
    trusted_totals: bool
    created_at: datetime


class QuantityTakeoffListResponse(BaseModel):
    """Cursor-paginated committed takeoff page."""

    items: list[QuantityTakeoffRead]
    next_cursor: str | None = None


class QuantityItemRead(BaseModel):
    """Committed quantity item payload."""

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: UUID
    quantity_takeoff_id: UUID
    project_id: UUID
    drawing_revision_id: UUID
    item_kind: str
    quantity_type: str
    value: float | None
    unit: str
    review_state: str
    validation_status: str
    quantity_gate: str
    source_entity_id: str | None
    excluded_source_entity_ids: list[str] = Field(
        validation_alias="excluded_source_entity_ids_json"
    )
    created_at: datetime


class QuantityItemListResponse(BaseModel):
    """Cursor-paginated committed quantity item page."""

    items: list[QuantityItemRead]
    next_cursor: str | None = None
