"""Estimate API schemas."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator


class EstimateVersionCreatePricing(BaseModel):
    """Pricing selection for a queued estimate job."""

    currency: str = "GBP"
    effective_date: date | None = Field(
        default=None,
        validation_alias=AliasChoices("effective_date", "pricing_date"),
    )

    @field_validator("currency")
    @classmethod
    def _normalize_currency(cls, value: str) -> str:
        return value.strip().upper()


class EstimateVersionCreateCatalogRef(BaseModel):
    """Catalog/formula selection used to build estimate worker input."""

    ref_type: str = Field(validation_alias=AliasChoices("ref_type", "line_type"))
    selection_id: UUID = Field(
        validation_alias=AliasChoices("selection_id", "catalog_entry_id", "ref_id")
    )
    selection_key: str = Field(
        validation_alias=AliasChoices("selection_key", "entry_key", "ref_key")
    )
    selection_checksum_sha256: str = Field(
        validation_alias=AliasChoices(
            "selection_checksum_sha256",
            "checksum_sha256",
            "checksum",
        )
    )
    description: str
    line_key: str | None = None
    quantity_item_id: UUID | None = None
    formula_inputs: dict[str, Any] | None = None

    @field_validator("ref_type")
    @classmethod
    def _normalize_ref_type(cls, value: str) -> str:
        return value.strip().lower()


class EstimateVersionCreateRequest(BaseModel):
    """Create-request payload for estimate job enqueue."""

    pricing: EstimateVersionCreatePricing = Field(default_factory=EstimateVersionCreatePricing)
    assumptions: dict[str, Any] = Field(default_factory=dict)
    catalog_refs: list[EstimateVersionCreateCatalogRef] = Field(
        default_factory=list,
        validation_alias=AliasChoices("catalog_refs", "refs", "line_refs"),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_flat_pricing(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value

        payload = dict(value)
        if "pricing" not in payload:
            pricing: dict[str, Any] = {}
            if "currency" in payload:
                pricing["currency"] = payload.pop("currency")
            if "pricing_date" in payload:
                pricing["pricing_date"] = payload.pop("pricing_date")
            if "effective_date" in payload:
                pricing["effective_date"] = payload.pop("effective_date")
            if pricing:
                payload["pricing"] = pricing
        return payload


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
