"""Pydantic schemas for versioned estimation catalog endpoints."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.estimating.catalog.api_checksums import formula_checksum_sha256
from app.estimating.money import validate_catalog_money

CatalogScopeType = Literal["global", "project"]
CatalogCurrencyCode = Literal["GBP"]


def _require_json_object(value: object, *, field_name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object.")
    return value


def _reject_json_floats(value: object, *, field_name: str) -> None:
    if isinstance(value, float):
        raise ValueError(f"{field_name} must not contain float values.")
    if isinstance(value, Mapping):
        for key, item in value.items():
            nested_field_name = f"{field_name}.{key}" if isinstance(key, str) else field_name
            _reject_json_floats(item, field_name=nested_field_name)
        return
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        for index, item in enumerate(value):
            _reject_json_floats(item, field_name=f"{field_name}[{index}]")


def _require_json_object_array(value: object, *, field_name: str) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be an array.")
    result: list[dict[str, object]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValueError(f"{field_name}[{index}] must be an object.")
        result.append(item)
    return result


def _validate_positive_money(value: Decimal, *, field_name: str) -> Decimal:
    return validate_catalog_money(value, field_name=field_name)


def _validate_scope(scope_type: CatalogScopeType, project_id: UUID | None) -> None:
    if scope_type == "global" and project_id is not None:
        raise ValueError("project_id must be null when scope_type is 'global'.")
    if scope_type == "project" and project_id is None:
        raise ValueError("project_id is required when scope_type is 'project'.")


class EstimationRateCreate(BaseModel):
    """Request payload for creating an immutable rate catalog entry."""

    model_config = ConfigDict(extra="forbid")

    scope_type: CatalogScopeType = Field(..., description="Catalog entry scope type")
    project_id: UUID | None = Field(None, description="Owning project for project-scoped rates")
    rate_key: str = Field(..., min_length=1, max_length=255, description="Stable rate key")
    source: str = Field(..., min_length=1, max_length=64, description="Catalog source label")
    metadata_json: dict[str, object] = Field(
        default_factory=dict,
        description="Arbitrary source metadata JSON object",
    )
    name: str = Field(..., min_length=1, max_length=255, description="Human-readable rate name")
    item_type: str = Field(..., min_length=1, max_length=64, description="Rate item type")
    per_unit: str = Field(..., min_length=1, max_length=64, description="Rate unit")
    currency: CatalogCurrencyCode = Field(..., description="Three-letter uppercase currency code")
    amount: Decimal = Field(
        ...,
        gt=Decimal("0"),
        max_digits=18,
        decimal_places=6,
        description="Positive rate amount in GBP",
    )
    effective_from: date = Field(..., description="Inclusive effective start date")
    effective_to: date | None = Field(None, description="Exclusive effective end date")
    supersedes_rate_id: UUID | None = Field(
        None,
        description="Optional predecessor rate entry to supersede atomically",
    )

    @field_validator("metadata_json", mode="before")
    @classmethod
    def validate_metadata_json(cls, value: object) -> dict[str, object]:
        metadata_json = _require_json_object(value, field_name="metadata_json")
        _reject_json_floats(metadata_json, field_name="metadata_json")
        return metadata_json

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, value: Decimal) -> Decimal:
        return _validate_positive_money(value, field_name="amount")

    @model_validator(mode="after")
    def validate_scope_and_window(self) -> EstimationRateCreate:
        _validate_scope(self.scope_type, self.project_id)
        if self.effective_to is not None and self.effective_to <= self.effective_from:
            raise ValueError("effective_to must be greater than effective_from.")
        return self


class EstimationRateRead(BaseModel):
    """Committed immutable rate catalog entry payload."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    scope_type: CatalogScopeType
    project_id: UUID | None
    rate_key: str
    source: str
    metadata_json: dict[str, object]
    name: str
    item_type: str
    per_unit: str
    currency: CatalogCurrencyCode
    amount: Decimal
    effective_from: date
    effective_to: date | None
    checksum_sha256: str = Field(..., min_length=64, max_length=64)
    superseded_by_id: UUID | None = None
    supersedes_rate_id: UUID | None = None
    created_at: datetime


class EstimationRateListResponse(BaseModel):
    """Cursor-paginated rate catalog page."""

    items: list[EstimationRateRead]
    next_cursor: str | None = None


class EstimationMaterialCreate(BaseModel):
    """Request payload for creating an immutable material catalog entry."""

    model_config = ConfigDict(extra="forbid")

    scope_type: CatalogScopeType = Field(..., description="Catalog entry scope type")
    project_id: UUID | None = Field(
        None,
        description="Owning project for project-scoped materials",
    )
    material_key: str = Field(..., min_length=1, max_length=255, description="Stable material key")
    source: str = Field(..., min_length=1, max_length=64, description="Catalog source label")
    metadata_json: dict[str, object] = Field(
        default_factory=dict,
        description="Arbitrary source metadata JSON object",
    )
    name: str = Field(..., min_length=1, max_length=255, description="Human-readable material name")
    unit: str = Field(..., min_length=1, max_length=64, description="Material unit")
    currency: CatalogCurrencyCode = Field(..., description="Three-letter uppercase currency code")
    unit_cost: Decimal = Field(
        ...,
        gt=Decimal("0"),
        max_digits=18,
        decimal_places=6,
        description="Positive unit cost in GBP",
    )
    effective_from: date = Field(..., description="Inclusive effective start date")
    effective_to: date | None = Field(None, description="Exclusive effective end date")
    supersedes_material_id: UUID | None = Field(
        None,
        description="Optional predecessor material entry to supersede atomically",
    )

    @field_validator("metadata_json", mode="before")
    @classmethod
    def validate_metadata_json(cls, value: object) -> dict[str, object]:
        metadata_json = _require_json_object(value, field_name="metadata_json")
        _reject_json_floats(metadata_json, field_name="metadata_json")
        return metadata_json

    @field_validator("unit_cost")
    @classmethod
    def validate_unit_cost(cls, value: Decimal) -> Decimal:
        return _validate_positive_money(value, field_name="unit_cost")

    @model_validator(mode="after")
    def validate_scope_and_window(self) -> EstimationMaterialCreate:
        _validate_scope(self.scope_type, self.project_id)
        if self.effective_to is not None and self.effective_to <= self.effective_from:
            raise ValueError("effective_to must be greater than effective_from.")
        return self


class EstimationMaterialRead(BaseModel):
    """Committed immutable material catalog entry payload."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    scope_type: CatalogScopeType
    project_id: UUID | None
    material_key: str
    source: str
    metadata_json: dict[str, object]
    name: str
    unit: str
    currency: CatalogCurrencyCode
    unit_cost: Decimal
    effective_from: date
    effective_to: date | None
    checksum_sha256: str = Field(..., min_length=64, max_length=64)
    superseded_by_id: UUID | None = None
    supersedes_material_id: UUID | None = None
    created_at: datetime


class EstimationMaterialListResponse(BaseModel):
    """Cursor-paginated material catalog page."""

    items: list[EstimationMaterialRead]
    next_cursor: str | None = None


class EstimationFormulaCreate(BaseModel):
    """Request payload for creating an immutable formula definition."""

    model_config = ConfigDict(extra="forbid")

    scope_type: CatalogScopeType = Field(..., description="Formula scope type")
    project_id: UUID | None = Field(None, description="Owning project for project-scoped formulas")
    formula_id: str = Field(..., min_length=1, max_length=255, description="Stable formula id")
    version: int = Field(..., ge=1, description="Positive formula version")
    name: str = Field(..., min_length=1, max_length=255, description="Human-readable formula name")
    dsl_version: str = Field(..., min_length=1, max_length=32, description="Formula DSL version")
    output_key: str = Field(..., min_length=1, max_length=255, description="Output field key")
    output_contract_json: dict[str, object] = Field(..., description="Formula output contract JSON")
    declared_inputs_json: list[dict[str, object]] = Field(
        ...,
        description="Formula declared input JSON array",
    )
    expression_json: dict[str, object] = Field(..., description="Formula expression JSON object")
    rounding_json: dict[str, object] | None = Field(
        None,
        description="Optional formula-level rounding JSON object",
    )
    supersedes_formula_id: UUID | None = Field(
        None,
        description="Optional predecessor formula definition to supersede atomically",
    )

    @field_validator("output_contract_json", mode="before")
    @classmethod
    def validate_output_contract_json(cls, value: object) -> dict[str, object]:
        return _require_json_object(value, field_name="output_contract_json")

    @field_validator("declared_inputs_json", mode="before")
    @classmethod
    def validate_declared_inputs_json(cls, value: object) -> list[dict[str, object]]:
        return _require_json_object_array(value, field_name="declared_inputs_json")

    @field_validator("expression_json", mode="before")
    @classmethod
    def validate_expression_json(cls, value: object) -> dict[str, object]:
        return _require_json_object(value, field_name="expression_json")

    @field_validator("rounding_json", mode="before")
    @classmethod
    def validate_rounding_json(cls, value: object) -> dict[str, object] | None:
        if value is None:
            return None
        return _require_json_object(value, field_name="rounding_json")

    @model_validator(mode="after")
    def validate_scope_and_definition(self) -> EstimationFormulaCreate:
        _validate_scope(self.scope_type, self.project_id)
        formula_checksum_sha256(
            scope_type=self.scope_type,
            project_id=self.project_id,
            formula_id=self.formula_id,
            version=self.version,
            name=self.name,
            dsl_version=self.dsl_version,
            output_key=self.output_key,
            output_contract_json=self.output_contract_json,
            declared_inputs_json=self.declared_inputs_json,
            expression_json=self.expression_json,
            rounding_json=self.rounding_json,
        )
        return self


class EstimationFormulaRead(BaseModel):
    """Committed immutable formula definition payload."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    scope_type: CatalogScopeType
    project_id: UUID | None
    formula_id: str
    version: int
    name: str
    dsl_version: str
    output_key: str
    output_contract_json: dict[str, Any]
    declared_inputs_json: list[dict[str, Any]]
    expression_json: dict[str, Any]
    rounding_json: dict[str, Any] | None
    checksum_sha256: str = Field(..., min_length=64, max_length=64)
    superseded_by_id: UUID | None = None
    supersedes_formula_id: UUID | None = None
    created_at: datetime


class EstimationFormulaListResponse(BaseModel):
    """Cursor-paginated formula catalog page."""

    items: list[EstimationFormulaRead]
    next_cursor: str | None = None
