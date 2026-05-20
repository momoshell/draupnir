"""Estimate API schemas."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class EstimateVersionRead(BaseModel):
    """Serialized estimate version."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    project_id: UUID
    source_file_id: UUID
    drawing_revision_id: UUID
    quantity_takeoff_id: UUID
    source_job_id: UUID
    quantity_gate: str
    trusted_totals: bool
    currency: str
    subtotal_amount: Decimal
    tax_amount: Decimal
    total_amount: Decimal
    created_at: datetime


class EstimateVersionListResponse(BaseModel):
    """Paginated estimate version list response."""

    items: list[EstimateVersionRead]
    next_cursor: str | None = None


class EstimateItemRead(BaseModel):
    """Serialized estimate line item."""

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: UUID
    estimate_version_id: UUID
    project_id: UUID
    drawing_revision_id: UUID
    line_type: str
    line_number: int
    line_key: str
    description: str
    currency: str
    quantity_value: Decimal | None
    quantity_unit: str | None
    unit_rate_amount: Decimal | None
    effective_date: date | None
    subtotal_amount: Decimal
    tax_amount: Decimal
    total_amount: Decimal
    rounding: dict[str, Any] | None = Field(default=None, validation_alias="rounding_json")
    quantity_snapshot_entry_id: UUID | None
    quantity_snapshot_entry_type: str
    rate_snapshot_entry_id: UUID | None
    rate_snapshot_entry_type: str
    material_snapshot_entry_id: UUID | None
    material_snapshot_entry_type: str
    formula_snapshot_entry_id: UUID | None
    formula_snapshot_entry_type: str
    assumption_snapshot_entry_id: UUID | None
    assumption_snapshot_entry_type: str
    created_at: datetime


class EstimateItemListResponse(BaseModel):
    """Paginated estimate line item list response."""

    items: list[EstimateItemRead]
    next_cursor: str | None = None


class EstimateSnapshotEntryRead(BaseModel):
    """Serialized estimate snapshot entry."""

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: UUID
    estimate_version_id: UUID
    project_id: UUID
    drawing_revision_id: UUID
    entry_type: str
    entry_key: str
    entry_label: str
    sort_order: int
    currency: str | None
    quantity_value: Decimal | None
    unit: str | None
    effective_date: date | None
    unit_amount: Decimal | None
    source_payload: dict[str, Any] = Field(validation_alias="source_payload_json")
    rounding: dict[str, Any] | None = Field(default=None, validation_alias="rounding_json")
    source_rate_id: UUID | None
    source_material_id: UUID | None
    source_formula_id: UUID | None
    source_quantity_takeoff_id: UUID | None
    source_quantity_item_id: UUID | None
    source_checksum_sha256: str | None
    created_at: datetime


class EstimateSnapshotEntryListResponse(BaseModel):
    """Paginated estimate snapshot entry list response."""

    items: list[EstimateSnapshotEntryRead]
    next_cursor: str | None = None
