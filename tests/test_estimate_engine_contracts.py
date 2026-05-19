from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import date
from decimal import Decimal
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest

from app.estimating.catalog.selection import SelectedFormula
from app.estimating.engine import (
    EstimateEngineError,
    EstimateEngineInput,
    EstimateEngineOutput,
    EstimateLineSpec,
    EstimateSnapshotEntrySpec,
    deterministic_estimate_version_id,
    deterministic_item_id,
    deterministic_snapshot_entry_id,
    formula_definition_from_json,
    formula_definition_from_selected_formula,
)
from app.estimating.formulas import (
    FormulaDefinition,
    FormulaInputDefinition,
    FormulaNode,
    RoundingSpec,
    ValueContract,
)

SCALAR = ValueContract(kind="scalar")
MONEY_GBP = ValueContract(kind="money", currency="GBP")
QUANTITY_M = ValueContract(kind="quantity", unit="m")
RATE_GBP_PER_M = ValueContract(kind="rate", currency="GBP", per_unit="m")


def test_engine_contracts_expose_deterministic_ids_and_frozen_specs() -> None:
    estimate_job_id = UUID("11111111-1111-1111-1111-111111111111")
    snapshot_id = deterministic_snapshot_entry_id(estimate_job_id, "rate:installer")
    item_id = deterministic_item_id(estimate_job_id, "line:1:rate")
    version_id = deterministic_estimate_version_id(estimate_job_id)

    assert version_id == uuid5(
        NAMESPACE_URL,
        "draupnir:estimate:11111111-1111-1111-1111-111111111111:version",
    )
    assert snapshot_id == uuid5(
        NAMESPACE_URL,
        "draupnir:estimate:11111111-1111-1111-1111-111111111111:snapshot:rate:installer",
    )
    assert item_id == uuid5(
        NAMESPACE_URL,
        "draupnir:estimate:11111111-1111-1111-1111-111111111111:item:line:1:rate",
    )

    snapshot = EstimateSnapshotEntrySpec(
        id=snapshot_id,
        entry_type="rate",
        entry_key="rate:installer",
        entry_label="Installer labour",
        sort_order=1,
        source_payload={"rate_key": "labour:installer"},
        source_rate_id=uuid4(),
        source_checksum_sha256="a" * 64,
        unit="m",
        currency="GBP",
        effective_date=date(2026, 1, 1),
        unit_amount=Decimal("12.345678"),
        rounding=RoundingSpec(scale=2, mode="ROUND_HALF_UP"),
    )
    line = EstimateLineSpec(
        id=item_id,
        line_number=1,
        line_key="line:1:rate",
        line_type="rate",
        description="Installer labour",
        currency="GBP",
        subtotal_amount=Decimal("10.00"),
        tax_amount=Decimal("2.00"),
        total_amount=Decimal("12.00"),
        quantity_snapshot_entry_id=uuid4(),
        rate_snapshot_entry_id=snapshot.id,
        quantity_value=Decimal("5.0"),
        quantity_unit="m",
        unit_rate_amount=Decimal("12.345678"),
        effective_date=date(2026, 1, 1),
    )
    engine_input = EstimateEngineInput(
        estimate_job_id=estimate_job_id,
        project_id=uuid4(),
        drawing_revision_id=uuid4(),
        quantity_takeoff_id=uuid4(),
        snapshot_entries=(snapshot,),
        line_specs=(line,),
    )
    engine_output = EstimateEngineOutput(
        estimate_version_id=version_id,
        currency="GBP",
        snapshot_entries=(snapshot,),
        line_items=(line,),
    )

    assert engine_input.currency == "GBP"
    assert engine_output.line_items[0].id == item_id
    field_name = "description"
    with pytest.raises(FrozenInstanceError):
        setattr(line, field_name, "mutated")


def test_formula_definition_from_json_maps_dataclass_equivalent_json() -> None:
    definition = formula_definition_from_json(
        formula_id="formula.paint_total",
        name="Paint total",
        version=1,
        checksum="c" * 64,
        output_key="estimate.paint_total",
        output_contract_json={"kind": "money", "currency": "GBP"},
        declared_inputs_json=[
            {"name": "rate", "contract": {"kind": "rate", "currency": "GBP", "per_unit": "m"}},
            {"name": "quantity", "contract": {"kind": "quantity", "unit": "m"}},
        ],
        expression_json={
            "kind": "round",
            "args": [
                {
                    "kind": "multiply",
                    "args": [
                        {"kind": "input", "name": "rate"},
                        {"kind": "input", "name": "quantity"},
                    ],
                }
            ],
            "rounding": {"scale": 2, "mode": "ROUND_HALF_UP"},
        },
        rounding_json={"scale": 2, "mode": "ROUND_HALF_UP"},
    )

    assert definition == FormulaDefinition(
        formula_id="formula.paint_total",
        name="Paint total",
        version=1,
        checksum="c" * 64,
        output_key="estimate.paint_total",
        output_contract=MONEY_GBP,
        declared_inputs=(
            FormulaInputDefinition(name="rate", contract=RATE_GBP_PER_M),
            FormulaInputDefinition(name="quantity", contract=QUANTITY_M),
        ),
        expression=FormulaNode(
            kind="round",
            args=(
                FormulaNode(
                    kind="multiply",
                    args=(
                        FormulaNode(kind="input", name="rate"),
                        FormulaNode(kind="input", name="quantity"),
                    ),
                ),
            ),
            rounding=RoundingSpec(scale=2, mode="ROUND_HALF_UP"),
        ),
        rounding=RoundingSpec(scale=2, mode="ROUND_HALF_UP"),
    )


def test_formula_definition_from_selected_formula_uses_selected_formula_payloads() -> None:
    selected = SelectedFormula(
        definition_id=uuid4(),
        scope_type="project",
        project_id=uuid4(),
        formula_id="formula.selected",
        version=2,
        name="Selected formula",
        dsl_version="1.0",
        output_key="estimate.selected",
        output_contract={"kind": "scalar"},
        declared_inputs=[{"name": "base", "contract": {"kind": "scalar"}}],
        checksum_sha256="d" * 64,
        expression={"kind": "input", "name": "base"},
        rounding=None,
    )

    definition = formula_definition_from_selected_formula(selected)

    assert definition == FormulaDefinition(
        formula_id="formula.selected",
        name="Selected formula",
        version=2,
        checksum="d" * 64,
        output_key="estimate.selected",
        output_contract=SCALAR,
        declared_inputs=(FormulaInputDefinition(name="base", contract=SCALAR),),
        expression=FormulaNode(kind="input", name="base"),
        rounding=None,
    )


@pytest.mark.parametrize(
    ("declared_inputs_json", "expression_json", "output_contract_json", "reason"),
    [
        (
            [{"key": "quantity", "type": "decimal"}],
            {"kind": "literal", "value": "1"},
            {"kind": "money", "currency": "GBP"},
            "legacy_input_shape",
        ),
        (
            [{"name": "base", "contract": {"kind": "scalar"}}],
            {"op": "literal", "value": "1"},
            {"kind": "money", "currency": "GBP"},
            "legacy_node_shape",
        ),
        (
            [{"name": "base", "contract": {"kind": "scalar"}}],
            {"kind": "literal", "value": "1"},
            {"kind": "money", "currency": "USD"},
            "unsupported_currency",
        ),
    ],
)
def test_formula_definition_from_json_rejects_legacy_and_unsupported_shapes(
    declared_inputs_json: list[dict[str, str | dict[str, str]]],
    expression_json: dict[str, object],
    output_contract_json: dict[str, str],
    reason: str,
) -> None:
    with pytest.raises(EstimateEngineError) as exc_info:
        formula_definition_from_json(
            formula_id="formula.invalid",
            name="Invalid formula",
            version=1,
            checksum="e" * 64,
            output_key="estimate.invalid",
            output_contract_json=output_contract_json,
            declared_inputs_json=declared_inputs_json,
            expression_json=expression_json,
        )

    assert exc_info.value.code == "UNSUPPORTED_FORMULA_JSON"
    assert exc_info.value.reason == reason
