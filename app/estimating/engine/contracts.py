from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from app.estimating.formulas import (
    FormulaDefinition,
    FormulaInputDefinition,
    FormulaNode,
    RoundingSpec,
    ValueContract,
)

type JSONScalar = None | bool | int | float | str
type JSONValue = JSONScalar | list[JSONValue] | dict[str, JSONValue]
type EstimateCurrency = Literal["GBP"]
type EstimateQuantityGate = Literal["allowed", "provisional", "review_gated", "blocked"]
type EstimateSnapshotEntryType = Literal[
    "quantity_input",
    "rate",
    "material",
    "formula",
    "assumption",
]
type EstimateLineType = Literal["rate", "material", "formula", "assumption", "adjustment"]


def _validate_non_empty(name: str, value: str) -> None:
    if not value:
        raise ValueError(f"{name} must be a non-empty string")


def deterministic_estimate_version_id(estimate_job_id: UUID) -> UUID:
    return uuid5(NAMESPACE_URL, f"draupnir:estimate:{estimate_job_id}:version")


def deterministic_snapshot_entry_id(estimate_job_id: UUID, entry_key: str) -> UUID:
    _validate_non_empty("entry_key", entry_key)
    return uuid5(NAMESPACE_URL, f"draupnir:estimate:{estimate_job_id}:snapshot:{entry_key}")


def deterministic_item_id(estimate_job_id: UUID, line_key: str) -> UUID:
    _validate_non_empty("line_key", line_key)
    return uuid5(NAMESPACE_URL, f"draupnir:estimate:{estimate_job_id}:item:{line_key}")


@dataclass(frozen=True, slots=True)
class EstimateSnapshotEntrySpec:
    id: UUID
    entry_type: EstimateSnapshotEntryType
    entry_key: str
    entry_label: str
    sort_order: int
    source_payload: dict[str, JSONValue] = field(default_factory=dict)
    source_rate_id: UUID | None = None
    source_material_id: UUID | None = None
    source_formula_id: UUID | None = None
    source_quantity_takeoff_id: UUID | None = None
    source_quantity_item_id: UUID | None = None
    source_checksum_sha256: str | None = None
    quantity_value: Decimal | None = None
    unit: str | None = None
    currency: EstimateCurrency | None = None
    effective_date: date | None = None
    unit_amount: Decimal | None = None
    formula_definition: FormulaDefinition | None = None
    rounding: RoundingSpec | None = None
    money_amount: Decimal | None = None

    def as_model_kwargs(
        self,
        *,
        estimate_version_id: UUID,
        project_id: UUID,
        drawing_revision_id: UUID,
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "id": self.id,
            "estimate_version_id": estimate_version_id,
            "project_id": project_id,
            "drawing_revision_id": drawing_revision_id,
            "entry_type": self.entry_type,
            "entry_key": self.entry_key,
            "entry_label": self.entry_label,
            "sort_order": self.sort_order,
            "currency": self.currency,
            "quantity_value": self.quantity_value,
            "unit": self.unit,
            "effective_date": self.effective_date,
            "unit_amount": self.unit_amount,
            "source_payload_json": _snapshot_source_payload_json(self),
            "source_rate_id": self.source_rate_id,
            "source_material_id": self.source_material_id,
            "source_formula_id": self.source_formula_id,
            "source_quantity_takeoff_id": self.source_quantity_takeoff_id,
            "source_quantity_item_id": self.source_quantity_item_id,
            "source_checksum_sha256": self.source_checksum_sha256,
        }
        rounding_json = _rounding_json(self.rounding)
        if rounding_json is not None:
            payload["rounding_json"] = rounding_json
        return payload


@dataclass(frozen=True, slots=True)
class EstimateLineSpec:
    id: UUID
    line_number: int
    line_key: str
    line_type: EstimateLineType
    description: str
    currency: EstimateCurrency
    subtotal_amount: Decimal
    tax_amount: Decimal
    total_amount: Decimal
    quantity_snapshot_entry_id: UUID | None = None
    rate_snapshot_entry_id: UUID | None = None
    material_snapshot_entry_id: UUID | None = None
    formula_snapshot_entry_id: UUID | None = None
    assumption_snapshot_entry_id: UUID | None = None
    quantity_value: Decimal | None = None
    quantity_unit: str | None = None
    unit_rate_amount: Decimal | None = None
    effective_date: date | None = None
    rounding: RoundingSpec | None = None
    details: dict[str, JSONValue] = field(default_factory=dict)

    def as_model_kwargs(
        self,
        *,
        estimate_version_id: UUID,
        project_id: UUID,
        drawing_revision_id: UUID,
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "id": self.id,
            "estimate_version_id": estimate_version_id,
            "project_id": project_id,
            "drawing_revision_id": drawing_revision_id,
            "line_type": self.line_type,
            "line_number": self.line_number,
            "line_key": self.line_key,
            "description": self.description,
            "currency": self.currency,
            "quantity_value": self.quantity_value,
            "quantity_unit": self.quantity_unit,
            "unit_rate_amount": self.unit_rate_amount,
            "effective_date": self.effective_date,
            "subtotal_amount": self.subtotal_amount,
            "tax_amount": self.tax_amount,
            "total_amount": self.total_amount,
            "quantity_snapshot_entry_id": self.quantity_snapshot_entry_id,
            "rate_snapshot_entry_id": self.rate_snapshot_entry_id,
            "material_snapshot_entry_id": self.material_snapshot_entry_id,
            "formula_snapshot_entry_id": self.formula_snapshot_entry_id,
            "assumption_snapshot_entry_id": self.assumption_snapshot_entry_id,
        }
        rounding_json = _rounding_json(self.rounding)
        if rounding_json is not None:
            payload["rounding_json"] = rounding_json
        return payload


@dataclass(frozen=True, slots=True)
class EstimateVersionHeader:
    estimate_version_id: UUID
    estimate_job_id: UUID
    project_id: UUID
    file_id: UUID
    drawing_revision_id: UUID
    quantity_takeoff_id: UUID
    source_job_id: UUID
    currency: EstimateCurrency
    subtotal_amount: Decimal
    tax_amount: Decimal
    total_amount: Decimal
    quantity_gate: EstimateQuantityGate = "allowed"
    trusted_totals: bool = True

    def as_model_kwargs(self) -> dict[str, object]:
        return {
            "id": self.estimate_version_id,
            "project_id": self.project_id,
            "source_file_id": self.file_id,
            "drawing_revision_id": self.drawing_revision_id,
            "quantity_takeoff_id": self.quantity_takeoff_id,
            "source_job_id": self.source_job_id,
            "quantity_gate": self.quantity_gate,
            "trusted_totals": self.trusted_totals,
            "currency": self.currency,
            "subtotal_amount": self.subtotal_amount,
            "tax_amount": self.tax_amount,
            "total_amount": self.total_amount,
        }


@dataclass(frozen=True, slots=True)
class EstimateQuantityEntryInput:
    entry_key: str
    entry_label: str
    sort_order: int
    source_quantity_item_id: UUID
    source_checksum_sha256: str
    quantity_value: Decimal
    unit: str
    source_quantity_takeoff_id: UUID | None = None
    source_payload: dict[str, JSONValue] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EstimateRateEntryInput:
    entry_key: str
    entry_label: str
    sort_order: int
    source_rate_id: UUID
    source_checksum_sha256: str
    unit: str
    effective_date: date
    unit_amount: Decimal
    source_payload: dict[str, JSONValue] = field(default_factory=dict)
    currency: EstimateCurrency = "GBP"


@dataclass(frozen=True, slots=True)
class EstimateMaterialEntryInput:
    entry_key: str
    entry_label: str
    sort_order: int
    source_material_id: UUID
    source_checksum_sha256: str
    unit: str
    effective_date: date
    unit_amount: Decimal
    source_payload: dict[str, JSONValue] = field(default_factory=dict)
    currency: EstimateCurrency = "GBP"


@dataclass(frozen=True, slots=True)
class EstimateFormulaEntryInput:
    entry_key: str
    entry_label: str
    sort_order: int
    source_formula_id: UUID
    source_checksum_sha256: str
    definition: FormulaDefinition
    source_payload: dict[str, JSONValue] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EstimateAssumptionEntryInput:
    entry_key: str
    entry_label: str
    sort_order: int
    source_checksum_sha256: str
    amount: Decimal
    source_payload: dict[str, JSONValue] = field(default_factory=dict)
    currency: EstimateCurrency = "GBP"


@dataclass(frozen=True, slots=True)
class EstimateLineInputSpec:
    line_key: str
    line_type: EstimateLineType
    description: str
    quantity_entry_key: str | None = None
    rate_entry_key: str | None = None
    material_entry_key: str | None = None
    formula_entry_key: str | None = None
    assumption_entry_key: str | None = None
    formula_inputs: dict[str, str] = field(default_factory=dict)
    adjustment_amount: Decimal | None = None


@dataclass(frozen=True, slots=True)
class EstimateEngineInput:
    estimate_job_id: UUID
    project_id: UUID
    drawing_revision_id: UUID
    quantity_takeoff_id: UUID
    file_id: UUID | None = None
    source_job_id: UUID | None = None
    currency: EstimateCurrency = "GBP"
    quantity_gate: EstimateQuantityGate | None = None
    trusted_totals: bool = False
    tax_rate: Decimal = Decimal("0")
    quantity_entries: tuple[EstimateQuantityEntryInput, ...] = ()
    rate_entries: tuple[EstimateRateEntryInput, ...] = ()
    material_entries: tuple[EstimateMaterialEntryInput, ...] = ()
    formula_entries: tuple[EstimateFormulaEntryInput, ...] = ()
    assumption_entries: tuple[EstimateAssumptionEntryInput, ...] = ()
    line_inputs: tuple[EstimateLineInputSpec, ...] = ()
    snapshot_entries: tuple[EstimateSnapshotEntrySpec, ...] = ()
    line_specs: tuple[EstimateLineSpec, ...] = ()


@dataclass(frozen=True, slots=True)
class EstimateEngineOutput:
    estimate_version_id: UUID
    currency: EstimateCurrency
    snapshot_entries: tuple[EstimateSnapshotEntrySpec, ...]
    line_items: tuple[EstimateLineSpec, ...]
    header: EstimateVersionHeader | None = None
    project_id: UUID | None = None
    file_id: UUID | None = None
    drawing_revision_id: UUID | None = None
    quantity_takeoff_id: UUID | None = None
    source_job_id: UUID | None = None
    subtotal_amount: Decimal = Decimal("0")
    tax_amount: Decimal = Decimal("0")
    total_amount: Decimal = Decimal("0")

    def estimate_version_model_kwargs(self) -> dict[str, object]:
        header = _required_header(self.header)
        return header.as_model_kwargs()

    def snapshot_entry_model_kwargs(self) -> tuple[dict[str, object], ...]:
        header = _required_header(self.header)
        return tuple(
            entry.as_model_kwargs(
                estimate_version_id=header.estimate_version_id,
                project_id=header.project_id,
                drawing_revision_id=header.drawing_revision_id,
            )
            for entry in self.snapshot_entries
        )

    def line_item_model_kwargs(self) -> tuple[dict[str, object], ...]:
        header = _required_header(self.header)
        return tuple(
            line.as_model_kwargs(
                estimate_version_id=header.estimate_version_id,
                project_id=header.project_id,
                drawing_revision_id=header.drawing_revision_id,
            )
            for line in self.line_items
        )


def _required_header(header: EstimateVersionHeader | None) -> EstimateVersionHeader:
    if header is None:
        raise ValueError("estimate output header is required for ORM payload generation")
    return header


def _snapshot_source_payload_json(snapshot: EstimateSnapshotEntrySpec) -> dict[str, JSONValue]:
    payload = dict(snapshot.source_payload)
    if snapshot.formula_definition is not None:
        payload["formula_definition"] = _formula_definition_json(snapshot.formula_definition)
    if snapshot.money_amount is not None:
        payload["money_amount"] = _decimal_text(snapshot.money_amount)
    return payload


def _formula_definition_json(definition: FormulaDefinition) -> dict[str, JSONValue]:
    return {
        "formula_id": definition.formula_id,
        "name": definition.name,
        "version": definition.version,
        "checksum": definition.checksum,
        "output_key": definition.output_key,
        "output_contract": _value_contract_json(definition.output_contract),
        "declared_inputs": [
            _formula_input_definition_json(item) for item in definition.declared_inputs
        ],
        "expression": _formula_node_json(definition.expression),
        "rounding": _rounding_json(definition.rounding),
    }


def _formula_input_definition_json(definition: FormulaInputDefinition) -> dict[str, JSONValue]:
    return {
        "name": definition.name,
        "contract": _value_contract_json(definition.contract),
    }


def _formula_node_json(node: FormulaNode) -> dict[str, JSONValue]:
    return {
        "kind": node.kind,
        "value": node.value,
        "name": node.name,
        "args": [_formula_node_json(arg) for arg in node.args],
        "rounding": _rounding_json(node.rounding),
    }


def _value_contract_json(contract: ValueContract) -> dict[str, JSONValue]:
    return {
        "kind": contract.kind,
        "currency": contract.currency,
        "unit": contract.unit,
        "per_unit": contract.per_unit,
    }


def _rounding_json(rounding: RoundingSpec | None) -> dict[str, JSONValue] | None:
    if rounding is None:
        return None
    return {"scale": rounding.scale, "mode": rounding.mode}


def _decimal_text(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"
