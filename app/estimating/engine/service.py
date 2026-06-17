from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Literal
from uuid import UUID

from app.estimating.decimal_text import display_text
from app.estimating.engine.contracts import (
    EstimateAssumptionEntryInput,
    EstimateEngineInput,
    EstimateEngineOutput,
    EstimateFormulaEntryInput,
    EstimateLineInputSpec,
    EstimateLineSpec,
    EstimateMaterialEntryInput,
    EstimateQuantityEntryInput,
    EstimateRateEntryInput,
    EstimateSnapshotEntrySpec,
    EstimateVersionHeader,
    JSONValue,
    deterministic_estimate_version_id,
    deterministic_item_id,
    deterministic_snapshot_entry_id,
)
from app.estimating.engine.errors import raise_input_invalid
from app.estimating.formulas import (
    FormulaValue,
    RoundingSpec,
    ValueContract,
    evaluate_formula,
)
from app.estimating.money import (
    CATALOG_QUANTUM,
    MONEY_SCALE,
    round_catalog_decimal,
    round_money,
)

type _SnapshotValueType = Literal["quantity_input", "rate", "material", "assumption"]

_CATALOG_QUANTUM = CATALOG_QUANTUM
_CHECKSUM_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_MAX_TAX_RATE = Decimal("1")


@dataclass(frozen=True, slots=True)
class _SnapshotValue:
    snapshot: EstimateSnapshotEntrySpec
    amount: Decimal
    contract: ValueContract
    value_type: _SnapshotValueType


def compose_estimate(engine_input: EstimateEngineInput) -> EstimateEngineOutput:
    _validate_engine_input(engine_input)

    snapshot_entries: list[EstimateSnapshotEntrySpec] = []
    snapshot_keys: set[str] = set()
    snapshot_values: dict[str, _SnapshotValue] = {}
    formula_entries: dict[str, EstimateFormulaEntryInput] = {}

    for quantity_entry in engine_input.quantity_entries:
        snapshot = _build_quantity_snapshot(engine_input, quantity_entry)
        snapshot_entries.append(snapshot)
        _register_snapshot(snapshot_keys, snapshot)
        snapshot_values[quantity_entry.entry_key] = _SnapshotValue(
            snapshot=snapshot,
            amount=snapshot.quantity_value or Decimal("0"),
            contract=ValueContract(kind="quantity", unit=snapshot.unit),
            value_type="quantity_input",
        )

    for rate_entry in engine_input.rate_entries:
        snapshot = _build_rate_snapshot(engine_input, rate_entry)
        snapshot_entries.append(snapshot)
        _register_snapshot(snapshot_keys, snapshot)
        snapshot_values[rate_entry.entry_key] = _SnapshotValue(
            snapshot=snapshot,
            amount=snapshot.unit_amount or Decimal("0"),
            contract=ValueContract(kind="rate", currency=snapshot.currency, per_unit=snapshot.unit),
            value_type="rate",
        )

    for material_entry in engine_input.material_entries:
        snapshot = _build_material_snapshot(engine_input, material_entry)
        snapshot_entries.append(snapshot)
        _register_snapshot(snapshot_keys, snapshot)
        snapshot_values[material_entry.entry_key] = _SnapshotValue(
            snapshot=snapshot,
            amount=snapshot.unit_amount or Decimal("0"),
            contract=ValueContract(kind="rate", currency=snapshot.currency, per_unit=snapshot.unit),
            value_type="material",
        )

    for formula_entry in engine_input.formula_entries:
        snapshot = _build_formula_snapshot(engine_input, formula_entry)
        snapshot_entries.append(snapshot)
        _register_snapshot(snapshot_keys, snapshot)
        formula_entries[formula_entry.entry_key] = formula_entry

    for assumption_entry in engine_input.assumption_entries:
        snapshot = _build_assumption_snapshot(engine_input, assumption_entry)
        snapshot_entries.append(snapshot)
        _register_snapshot(snapshot_keys, snapshot)
        # Contract is driven by the input's kind, not the persisted entry: assumption
        # rows store currency NULL, but a money formula input requires currency="GBP",
        # and a scalar input requires a unitless scalar contract.
        assumption_contract = (
            ValueContract(kind="scalar")
            if assumption_entry.kind == "scalar"
            else ValueContract(kind="money", currency=assumption_entry.currency)
        )
        snapshot_values[assumption_entry.entry_key] = _SnapshotValue(
            snapshot=snapshot,
            amount=snapshot.money_amount or Decimal("0"),
            contract=assumption_contract,
            value_type="assumption",
        )

    ordered_snapshot_entries = tuple(
        sorted(snapshot_entries, key=lambda entry: (entry.sort_order, entry.entry_key))
    )

    line_keys: set[str] = set()
    line_items: list[EstimateLineSpec] = []
    subtotal_amount = Decimal("0")
    tax_amount = Decimal("0")
    total_amount = Decimal("0")

    for index, line_input in enumerate(engine_input.line_inputs, start=1):
        _register_line_key(line_keys, line_input.line_key)
        line_item = _build_line_item(
            engine_input=engine_input,
            line_input=line_input,
            line_number=index,
            snapshot_values=snapshot_values,
            formula_entries=formula_entries,
        )
        line_items.append(line_item)
        subtotal_amount += line_item.subtotal_amount
        tax_amount += line_item.tax_amount
        total_amount += line_item.total_amount

    version_id = deterministic_estimate_version_id(engine_input.estimate_job_id)
    header = EstimateVersionHeader(
        estimate_version_id=version_id,
        estimate_job_id=engine_input.estimate_job_id,
        project_id=engine_input.project_id,
        file_id=_required_uuid("file_id", engine_input.file_id),
        drawing_revision_id=engine_input.drawing_revision_id,
        quantity_takeoff_id=engine_input.quantity_takeoff_id,
        source_job_id=_required_uuid("source_job_id", engine_input.source_job_id),
        currency=engine_input.currency,
        quantity_gate=_required_quantity_gate(engine_input.quantity_gate),
        trusted_totals=engine_input.trusted_totals,
        subtotal_amount=subtotal_amount,
        tax_amount=tax_amount,
        total_amount=total_amount,
    )

    return EstimateEngineOutput(
        estimate_version_id=version_id,
        currency=engine_input.currency,
        snapshot_entries=ordered_snapshot_entries,
        line_items=tuple(line_items),
        header=header,
        project_id=engine_input.project_id,
        file_id=header.file_id,
        drawing_revision_id=engine_input.drawing_revision_id,
        quantity_takeoff_id=engine_input.quantity_takeoff_id,
        source_job_id=header.source_job_id,
        subtotal_amount=subtotal_amount,
        tax_amount=tax_amount,
        total_amount=total_amount,
    )


def _validate_engine_input(engine_input: EstimateEngineInput) -> None:
    if engine_input.file_id is None or engine_input.source_job_id is None:
        raise_input_invalid(
            reason="missing_lineage",
            message="estimate input must include file_id and source_job_id lineage fields",
        )
    if engine_input.currency != "GBP":
        raise_input_invalid(
            reason="unsupported_currency",
            message="estimate engine supports GBP only",
        )
    if engine_input.tax_rate < 0:
        raise_input_invalid(reason="negative_tax_rate", message="tax_rate must be nonnegative")
    if engine_input.tax_rate > _MAX_TAX_RATE:
        # A tax rate is a fraction (0.20 == 20%). A value above 1 is almost always a
        # fat-fingered percentage (e.g. 20 instead of 0.20) that would 100x the estimate.
        raise_input_invalid(
            reason="tax_rate_out_of_range",
            message="tax_rate must be between 0 and 1 (a fraction, not a percentage)",
        )
    if not engine_input.line_inputs:
        raise_input_invalid(
            reason="missing_line_items",
            message="estimate input requires line_inputs",
        )


def _build_quantity_snapshot(
    engine_input: EstimateEngineInput,
    quantity_entry: EstimateQuantityEntryInput,
) -> EstimateSnapshotEntrySpec:
    _validate_entry_key(quantity_entry.entry_key)
    _validate_non_empty_text("entry_label", quantity_entry.entry_label)
    _validate_non_empty_text("unit", quantity_entry.unit)
    _validate_checksum(quantity_entry.source_checksum_sha256)
    if (
        quantity_entry.source_quantity_takeoff_id is not None
        and quantity_entry.source_quantity_takeoff_id != engine_input.quantity_takeoff_id
    ):
        raise_input_invalid(
            reason="quantity_takeoff_mismatch",
            message="quantity entry lineage must match estimate input quantity_takeoff_id",
        )
    quantity_value = _quantity_decimal(quantity_entry.quantity_value)
    if quantity_value < 0:
        raise_input_invalid(
            reason="negative_quantity",
            message="quantity values must be nonnegative",
        )
    return EstimateSnapshotEntrySpec(
        id=deterministic_snapshot_entry_id(engine_input.estimate_job_id, quantity_entry.entry_key),
        entry_type="quantity_input",
        entry_key=quantity_entry.entry_key,
        entry_label=quantity_entry.entry_label,
        sort_order=quantity_entry.sort_order,
        source_payload=quantity_entry.source_payload,
        source_quantity_takeoff_id=engine_input.quantity_takeoff_id,
        source_quantity_item_id=quantity_entry.source_quantity_item_id,
        # The source checksum is validated above for input integrity but intentionally
        # not persisted: the ck_estimate_snapshot_entries_source_fields_by_type DB
        # constraint requires source_checksum_sha256 IS NULL for quantity_input entries.
        source_checksum_sha256=None,
        quantity_value=quantity_value,
        unit=quantity_entry.unit,
    )


def _build_rate_snapshot(
    engine_input: EstimateEngineInput,
    rate_entry: EstimateRateEntryInput,
) -> EstimateSnapshotEntrySpec:
    _validate_money_rate_entry(
        rate_entry.entry_key,
        rate_entry.entry_label,
        rate_entry.source_checksum_sha256,
        rate_entry.unit,
        rate_entry.currency,
        rate_entry.unit_amount,
    )
    return EstimateSnapshotEntrySpec(
        id=deterministic_snapshot_entry_id(engine_input.estimate_job_id, rate_entry.entry_key),
        entry_type="rate",
        entry_key=rate_entry.entry_key,
        entry_label=rate_entry.entry_label,
        sort_order=rate_entry.sort_order,
        source_payload=rate_entry.source_payload,
        source_rate_id=rate_entry.source_rate_id,
        source_checksum_sha256=rate_entry.source_checksum_sha256,
        unit=rate_entry.unit,
        currency=rate_entry.currency,
        effective_date=rate_entry.effective_date,
        unit_amount=rate_entry.unit_amount,
    )


def _build_material_snapshot(
    engine_input: EstimateEngineInput,
    material_entry: EstimateMaterialEntryInput,
) -> EstimateSnapshotEntrySpec:
    _validate_money_rate_entry(
        material_entry.entry_key,
        material_entry.entry_label,
        material_entry.source_checksum_sha256,
        material_entry.unit,
        material_entry.currency,
        material_entry.unit_amount,
    )
    return EstimateSnapshotEntrySpec(
        id=deterministic_snapshot_entry_id(engine_input.estimate_job_id, material_entry.entry_key),
        entry_type="material",
        entry_key=material_entry.entry_key,
        entry_label=material_entry.entry_label,
        sort_order=material_entry.sort_order,
        source_payload=material_entry.source_payload,
        source_material_id=material_entry.source_material_id,
        source_checksum_sha256=material_entry.source_checksum_sha256,
        unit=material_entry.unit,
        currency=material_entry.currency,
        effective_date=material_entry.effective_date,
        unit_amount=material_entry.unit_amount,
    )


def _build_formula_snapshot(
    engine_input: EstimateEngineInput,
    formula_entry: EstimateFormulaEntryInput,
) -> EstimateSnapshotEntrySpec:
    _validate_entry_key(formula_entry.entry_key)
    _validate_non_empty_text("entry_label", formula_entry.entry_label)
    _validate_checksum(formula_entry.source_checksum_sha256)
    if (
        formula_entry.definition.output_contract.kind != "money"
        or formula_entry.definition.output_contract.currency != "GBP"
    ):
        raise_input_invalid(
            reason="unsupported_formula_output",
            message="formula entries must resolve to GBP money outputs",
        )
    if formula_entry.source_checksum_sha256 != formula_entry.definition.checksum:
        raise_input_invalid(
            reason="formula_checksum_mismatch",
            message="formula entry checksum must match definition checksum",
        )
    definition_rounding = formula_entry.definition.rounding
    if definition_rounding is not None and definition_rounding.scale > MONEY_SCALE:
        # A money formula that rounds to more decimal places than money supports would
        # be rounded a second time by the money quantize step, and chained rounding is
        # not equal to a single rounding. Constrain it so the formula rounding stays
        # authoritative and the money step only normalizes representation.
        raise_input_invalid(
            reason="unsupported_formula_rounding",
            message=(
                f"money formula rounding scale must not exceed the money scale ({MONEY_SCALE})"
            ),
        )
    return EstimateSnapshotEntrySpec(
        id=deterministic_snapshot_entry_id(engine_input.estimate_job_id, formula_entry.entry_key),
        entry_type="formula",
        entry_key=formula_entry.entry_key,
        entry_label=formula_entry.entry_label,
        sort_order=formula_entry.sort_order,
        source_payload=formula_entry.source_payload,
        source_formula_id=formula_entry.source_formula_id,
        source_checksum_sha256=formula_entry.source_checksum_sha256,
        formula_definition=formula_entry.definition,
        rounding=formula_entry.definition.rounding,
    )


def _build_assumption_snapshot(
    engine_input: EstimateEngineInput,
    assumption_entry: EstimateAssumptionEntryInput,
) -> EstimateSnapshotEntrySpec:
    _validate_entry_key(assumption_entry.entry_key)
    _validate_non_empty_text("entry_label", assumption_entry.entry_label)
    _validate_checksum(assumption_entry.source_checksum_sha256)
    # Scalar assumptions are unitless (no currency); only money assumptions are
    # currency-constrained.
    if assumption_entry.kind == "money" and assumption_entry.currency != "GBP":
        raise_input_invalid(
            reason="unsupported_currency",
            message="estimate engine supports GBP only",
        )
    if assumption_entry.amount < 0:
        raise_input_invalid(
            reason="negative_assumption_amount",
            message="assumption amounts must be nonnegative",
        )
    return EstimateSnapshotEntrySpec(
        id=deterministic_snapshot_entry_id(
            engine_input.estimate_job_id,
            assumption_entry.entry_key,
        ),
        entry_type="assumption",
        entry_key=assumption_entry.entry_key,
        entry_label=assumption_entry.entry_label,
        sort_order=assumption_entry.sort_order,
        source_payload=assumption_entry.source_payload,
        # Validated above but not persisted: the source-fields-by-type DB constraint
        # requires source_checksum_sha256 IS NULL for assumption entries.
        source_checksum_sha256=None,
        money_amount=assumption_entry.amount,
    )


def _build_line_item(
    *,
    engine_input: EstimateEngineInput,
    line_input: EstimateLineInputSpec,
    line_number: int,
    snapshot_values: dict[str, _SnapshotValue],
    formula_entries: dict[str, EstimateFormulaEntryInput],
) -> EstimateLineSpec:
    _validate_entry_key(line_input.line_key)
    _validate_non_empty_text("description", line_input.description)

    if line_input.line_type == "rate":
        quantity_value = _resolve_snapshot_value(
            snapshot_values,
            line_input.quantity_entry_key,
            expected_type="quantity_input",
        )
        rate_value = _resolve_snapshot_value(
            snapshot_values,
            line_input.rate_entry_key,
            expected_type="rate",
        )
        _validate_rate_unit_match(quantity_value, rate_value)
        subtotal_amount = _money(rate_value.amount * quantity_value.amount)
        return _finalize_line_item(
            engine_input=engine_input,
            line_input=line_input,
            line_number=line_number,
            subtotal_amount=subtotal_amount,
            quantity_snapshot_entry_id=quantity_value.snapshot.id,
            rate_snapshot_entry_id=rate_value.snapshot.id,
            quantity_value=quantity_value.amount,
            quantity_unit=quantity_value.snapshot.unit,
            unit_rate_amount=rate_value.amount,
            effective_date=rate_value.snapshot.effective_date,
        )

    if line_input.line_type == "material":
        quantity_value = _resolve_snapshot_value(
            snapshot_values,
            line_input.quantity_entry_key,
            expected_type="quantity_input",
        )
        material_value = _resolve_snapshot_value(
            snapshot_values,
            line_input.material_entry_key,
            expected_type="material",
        )
        _validate_rate_unit_match(quantity_value, material_value)
        subtotal_amount = _money(material_value.amount * quantity_value.amount)
        return _finalize_line_item(
            engine_input=engine_input,
            line_input=line_input,
            line_number=line_number,
            subtotal_amount=subtotal_amount,
            quantity_snapshot_entry_id=quantity_value.snapshot.id,
            material_snapshot_entry_id=material_value.snapshot.id,
            quantity_value=quantity_value.amount,
            quantity_unit=quantity_value.snapshot.unit,
            unit_rate_amount=material_value.amount,
            effective_date=material_value.snapshot.effective_date,
        )

    if line_input.line_type == "assumption":
        assumption_value = _resolve_snapshot_value(
            snapshot_values,
            line_input.assumption_entry_key,
            expected_type="assumption",
        )
        subtotal_amount = _money(assumption_value.amount)
        return _finalize_line_item(
            engine_input=engine_input,
            line_input=line_input,
            line_number=line_number,
            subtotal_amount=subtotal_amount,
            assumption_snapshot_entry_id=assumption_value.snapshot.id,
            details={"amount": display_text(assumption_value.amount)},
        )

    if line_input.line_type == "adjustment":
        adjustment_amount = line_input.adjustment_amount
        if adjustment_amount is None:
            raise_input_invalid(
                reason="missing_adjustment_amount",
                message="adjustment lines require adjustment_amount",
            )
        if adjustment_amount < 0:
            raise_input_invalid(
                reason="negative_adjustment",
                message="adjustment lines must be nonnegative explicit amounts",
            )
        formula_snapshot_entry_id, assumption_snapshot_entry_id = _resolve_adjustment_anchor(
            engine_input=engine_input,
            line_input=line_input,
            snapshot_values=snapshot_values,
            formula_entries=formula_entries,
        )
        subtotal_amount = _money(adjustment_amount)
        return _finalize_line_item(
            engine_input=engine_input,
            line_input=line_input,
            line_number=line_number,
            subtotal_amount=subtotal_amount,
            formula_snapshot_entry_id=formula_snapshot_entry_id,
            assumption_snapshot_entry_id=assumption_snapshot_entry_id,
            details={"adjustment_amount": display_text(adjustment_amount)},
        )

    if line_input.line_type == "formula":
        return _build_formula_line_item(
            engine_input=engine_input,
            line_input=line_input,
            line_number=line_number,
            snapshot_values=snapshot_values,
            formula_entries=formula_entries,
        )

    raise_input_invalid(
        reason="unsupported_line_type",
        message=f"unsupported line type: {line_input.line_type}",
    )
    raise AssertionError("unreachable")


def _build_formula_line_item(
    *,
    engine_input: EstimateEngineInput,
    line_input: EstimateLineInputSpec,
    line_number: int,
    snapshot_values: dict[str, _SnapshotValue],
    formula_entries: dict[str, EstimateFormulaEntryInput],
) -> EstimateLineSpec:
    formula_entry_key = _required_key("formula_entry_key", line_input.formula_entry_key)
    formula_entry = formula_entries.get(formula_entry_key)
    if formula_entry is None:
        raise_input_invalid(
            reason="line_reference_missing",
            message="formula line references unknown formula entry",
        )
    resolved_formula_entry = formula_entry

    formula_inputs: dict[str, FormulaValue] = {}
    bound_keys: list[str] = []
    quantity_snapshot: _SnapshotValue | None = None

    for declared_input in resolved_formula_entry.definition.declared_inputs:
        snapshot_key = line_input.formula_inputs.get(declared_input.name)
        if snapshot_key is None:
            raise_input_invalid(
                reason="missing_formula_input",
                message=f"formula line missing binding for input '{declared_input.name}'",
            )
        snapshot_value = snapshot_values.get(snapshot_key)
        if snapshot_value is None:
            raise_input_invalid(
                reason="line_reference_missing",
                message="formula line references unknown snapshot entry",
            )
        resolved_snapshot_value = snapshot_value
        if not _contracts_match(resolved_snapshot_value.contract, declared_input.contract):
            raise_input_invalid(
                reason="unit_mismatch",
                message="formula input contract does not match bound snapshot",
            )
        formula_inputs[declared_input.name] = FormulaValue(
            amount=resolved_snapshot_value.amount,
            contract=resolved_snapshot_value.contract,
        )
        bound_keys.append(snapshot_key)
        if resolved_snapshot_value.value_type == "quantity_input" and quantity_snapshot is None:
            quantity_snapshot = resolved_snapshot_value

    formula_result = evaluate_formula(resolved_formula_entry.definition, formula_inputs)
    if (
        formula_result.value.contract.kind != "money"
        or formula_result.value.contract.currency != "GBP"
    ):
        raise_input_invalid(
            reason="unsupported_formula_output",
            message="formula lines must resolve to GBP money outputs",
        )

    pre_round_amount = formula_result.value.amount
    evaluated_amount = _apply_formula_rounding(formula_result.value.amount, formula_result.rounding)
    if evaluated_amount < 0:
        raise_input_invalid(
            reason="negative_money",
            message="formula lines must resolve to nonnegative money",
        )

    subtotal_amount = _money(evaluated_amount)
    bound_input_keys_json: list[JSONValue] = list(bound_keys)
    details: dict[str, JSONValue] = {
        "formula_id": resolved_formula_entry.definition.formula_id,
        "formula_version": resolved_formula_entry.definition.version,
        "formula_checksum": resolved_formula_entry.definition.checksum,
        "bound_input_keys": bound_input_keys_json,
        "pre_round_amount": display_text(pre_round_amount),
        "evaluated_amount": display_text(evaluated_amount),
    }
    if formula_result.rounding is not None:
        details["applied_rounding"] = {
            "scale": formula_result.rounding.scale,
            "mode": formula_result.rounding.mode,
        }

    return _finalize_line_item(
        engine_input=engine_input,
        line_input=line_input,
        line_number=line_number,
        subtotal_amount=subtotal_amount,
        quantity_snapshot_entry_id=(
            quantity_snapshot.snapshot.id if quantity_snapshot is not None else None
        ),
        formula_snapshot_entry_id=deterministic_snapshot_entry_id(
            engine_input.estimate_job_id,
            resolved_formula_entry.entry_key,
        ),
        quantity_value=quantity_snapshot.amount if quantity_snapshot is not None else None,
        quantity_unit=quantity_snapshot.snapshot.unit if quantity_snapshot is not None else None,
        rounding=formula_result.rounding,
        details=details,
    )


def _finalize_line_item(
    *,
    engine_input: EstimateEngineInput,
    line_input: EstimateLineInputSpec,
    line_number: int,
    subtotal_amount: Decimal,
    quantity_snapshot_entry_id: UUID | None = None,
    rate_snapshot_entry_id: UUID | None = None,
    material_snapshot_entry_id: UUID | None = None,
    formula_snapshot_entry_id: UUID | None = None,
    assumption_snapshot_entry_id: UUID | None = None,
    quantity_value: Decimal | None = None,
    quantity_unit: str | None = None,
    unit_rate_amount: Decimal | None = None,
    effective_date: date | None = None,
    rounding: RoundingSpec | None = None,
    details: dict[str, JSONValue] | None = None,
) -> EstimateLineSpec:
    tax_amount = _money(subtotal_amount * engine_input.tax_rate)
    total_amount = subtotal_amount + tax_amount
    normalized_details: dict[str, JSONValue] = details or {}
    return EstimateLineSpec(
        id=deterministic_item_id(engine_input.estimate_job_id, line_input.line_key),
        line_number=line_number,
        line_key=line_input.line_key,
        line_type=line_input.line_type,
        description=line_input.description,
        currency=engine_input.currency,
        subtotal_amount=subtotal_amount,
        tax_amount=tax_amount,
        total_amount=total_amount,
        quantity_snapshot_entry_id=quantity_snapshot_entry_id,
        rate_snapshot_entry_id=rate_snapshot_entry_id,
        material_snapshot_entry_id=material_snapshot_entry_id,
        formula_snapshot_entry_id=formula_snapshot_entry_id,
        assumption_snapshot_entry_id=assumption_snapshot_entry_id,
        quantity_value=quantity_value,
        quantity_unit=quantity_unit,
        unit_rate_amount=unit_rate_amount,
        effective_date=effective_date,
        rounding=rounding,
        details=normalized_details,
    )


def _register_snapshot(snapshot_keys: set[str], snapshot: EstimateSnapshotEntrySpec) -> None:
    if snapshot.entry_key in snapshot_keys:
        raise_input_invalid(
            reason="duplicate_snapshot_entry_key",
            message="snapshot entry keys must be unique",
        )
    snapshot_keys.add(snapshot.entry_key)


def _register_line_key(line_keys: set[str], line_key: str) -> None:
    if line_key in line_keys:
        raise_input_invalid(reason="duplicate_line_key", message="line keys must be unique")
    line_keys.add(line_key)


def _resolve_snapshot_value(
    snapshot_values: dict[str, _SnapshotValue],
    entry_key: str | None,
    *,
    expected_type: _SnapshotValueType,
) -> _SnapshotValue:
    key = _required_key("entry_key", entry_key)
    snapshot_value = snapshot_values.get(key)
    if snapshot_value is None:
        raise_input_invalid(
            reason="line_reference_missing",
            message="line references unknown snapshot entry",
        )
    resolved_snapshot_value = snapshot_value
    if resolved_snapshot_value.value_type != expected_type:
        raise_input_invalid(
            reason="line_reference_type_mismatch",
            message="line references incompatible snapshot entry type",
        )
    return resolved_snapshot_value


def _validate_rate_unit_match(quantity_value: _SnapshotValue, rate_value: _SnapshotValue) -> None:
    if rate_value.contract.per_unit != quantity_value.contract.unit:
        raise_input_invalid(
            reason="unit_mismatch",
            message="estimate engine does not support unit conversion",
        )


def _apply_formula_rounding(amount: Decimal, rounding_spec: RoundingSpec | None) -> Decimal:
    if rounding_spec is None:
        return amount
    quantum = Decimal(1).scaleb(-rounding_spec.scale)
    return amount.quantize(quantum, rounding=rounding_spec.mode)


def _money(value: Decimal) -> Decimal:
    return round_money(value)


def _quantity_decimal(value: Decimal) -> Decimal:
    # Use the shared HALF_UP catalog rounding so quantities, rates, and money all
    # round the same way and the result never depends on the ambient Decimal context.
    return round_catalog_decimal(value)


def _validate_money_rate_entry(
    entry_key: str,
    entry_label: str,
    source_checksum_sha256: str,
    unit: str,
    currency: str,
    unit_amount: Decimal,
) -> None:
    _validate_entry_key(entry_key)
    _validate_non_empty_text("entry_label", entry_label)
    _validate_checksum(source_checksum_sha256)
    _validate_non_empty_text("unit", unit)
    if currency != "GBP":
        raise_input_invalid(
            reason="unsupported_currency",
            message="estimate engine supports GBP only",
        )
    if unit_amount < 0:
        raise_input_invalid(reason="negative_money", message="unit amounts must be nonnegative")
    if unit_amount.quantize(_CATALOG_QUANTUM) != unit_amount:
        raise_input_invalid(
            reason="invalid_rate_scale",
            message="catalog unit amounts must use scale 6 or less",
        )


def _validate_checksum(checksum: str) -> None:
    if not _CHECKSUM_PATTERN.fullmatch(checksum):
        raise_input_invalid(
            reason="invalid_checksum",
            message="source checksums must be lowercase sha256 hex",
        )


def _validate_entry_key(entry_key: str) -> None:
    _validate_non_empty_text("entry_key", entry_key)


def _validate_non_empty_text(name: str, value: str) -> None:
    if not value:
        raise_input_invalid(reason="input_invalid", message=f"{name} must be a non-empty string")


def _contracts_match(actual: ValueContract, expected: ValueContract) -> bool:
    return (
        actual.kind == expected.kind
        and actual.currency == expected.currency
        and actual.unit == expected.unit
        and actual.per_unit == expected.per_unit
    )


def _required_key(name: str, value: str | None) -> str:
    if value is None or not value:
        raise_input_invalid(reason="missing_reference", message=f"{name} is required")
    return value


def _required_uuid(name: str, value: UUID | None) -> UUID:
    if value is None:
        raise_input_invalid(reason="missing_lineage", message=f"{name} is required")
    return value


def _required_quantity_gate(
    value: str | None,
) -> Literal["allowed", "provisional", "review_gated", "blocked"]:
    if value is None:
        raise_input_invalid(
            reason="quantity_gate_not_allowed",
            message="quantity_gate is required",
        )
    return value  # type: ignore[return-value]


def _resolve_adjustment_anchor(
    *,
    engine_input: EstimateEngineInput,
    line_input: EstimateLineInputSpec,
    snapshot_values: dict[str, _SnapshotValue],
    formula_entries: dict[str, EstimateFormulaEntryInput],
) -> tuple[UUID | None, UUID | None]:
    has_formula_anchor = bool(line_input.formula_entry_key)
    has_assumption_anchor = bool(line_input.assumption_entry_key)
    if has_formula_anchor == has_assumption_anchor:
        raise_input_invalid(
            reason="invalid_adjustment_anchor",
            message="adjustment lines require exactly one formula or assumption anchor",
        )

    if line_input.formula_entry_key is not None:
        formula_entry = formula_entries.get(line_input.formula_entry_key)
        if formula_entry is None:
            raise_input_invalid(
                reason="line_reference_missing",
                message="adjustment line references unknown formula entry",
            )
        return (
            deterministic_snapshot_entry_id(
                engine_input.estimate_job_id,
                formula_entry.entry_key,
            ),
            None,
        )

    assumption_value = _resolve_snapshot_value(
        snapshot_values,
        line_input.assumption_entry_key,
        expected_type="assumption",
    )
    return None, assumption_value.snapshot.id
