from __future__ import annotations

import dataclasses
from datetime import date
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from app.estimating.engine.contracts import (
    EstimateAssumptionEntryInput,
    EstimateEngineInput,
    EstimateFormulaEntryInput,
    EstimateLineInputSpec,
    EstimateMaterialEntryInput,
    EstimateQuantityEntryInput,
    EstimateRateEntryInput,
    deterministic_estimate_version_id,
    deterministic_item_id,
    deterministic_snapshot_entry_id,
)
from app.estimating.engine.errors import EstimateEngineError
from app.estimating.engine.service import compose_estimate
from app.estimating.formulas import (
    FormulaDefinition,
    FormulaInputDefinition,
    FormulaNode,
    RoundingSpec,
    ValueContract,
)
from app.estimating.money import round_money

QUANTITY_M = ValueContract(kind="quantity", unit="m")
RATE_GBP_PER_M = ValueContract(kind="rate", currency="GBP", per_unit="m")
MONEY_GBP = ValueContract(kind="money", currency="GBP")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("0"), Decimal("0.00")),
        (Decimal("2.225"), Decimal("2.23")),
        (Decimal("5.555"), Decimal("5.56")),
        (Decimal("30.864195"), Decimal("30.86")),
    ],
)
def test_round_money_uses_gbp_cent_half_up(value: Decimal, expected: Decimal) -> None:
    assert round_money(value) == expected


def _formula_definition() -> FormulaDefinition:
    return FormulaDefinition(
        formula_id="formula.rounded_markup",
        name="Rounded markup",
        version=1,
        checksum="f" * 64,
        output_key="estimate.rounded_markup",
        output_contract=MONEY_GBP,
        declared_inputs=(
            FormulaInputDefinition(name="rate", contract=RATE_GBP_PER_M),
            FormulaInputDefinition(name="quantity", contract=QUANTITY_M),
        ),
        expression=FormulaNode(
            kind="multiply",
            args=(
                FormulaNode(kind="input", name="rate"),
                FormulaNode(kind="input", name="quantity"),
            ),
        ),
        rounding=RoundingSpec(scale=1, mode="ROUND_HALF_UP"),
    )


def _valid_engine_input(*, quantity_gate: str = "allowed") -> EstimateEngineInput:
    estimate_job_id = UUID("11111111-1111-1111-1111-111111111111")
    project_id = UUID("22222222-2222-2222-2222-222222222222")
    file_id = UUID("33333333-3333-3333-3333-333333333333")
    drawing_revision_id = UUID("44444444-4444-4444-4444-444444444444")
    quantity_takeoff_id = UUID("55555555-5555-5555-5555-555555555555")
    source_job_id = UUID("66666666-6666-6666-6666-666666666666")
    formula_definition = _formula_definition()
    return EstimateEngineInput(
        estimate_job_id=estimate_job_id,
        project_id=project_id,
        file_id=file_id,
        drawing_revision_id=drawing_revision_id,
        quantity_takeoff_id=quantity_takeoff_id,
        source_job_id=source_job_id,
        quantity_gate=quantity_gate,  # type: ignore[arg-type]
        trusted_totals=True,
        tax_rate=Decimal("0.20"),
        quantity_entries=(
            EstimateQuantityEntryInput(
                entry_key="quantity:labour",
                entry_label="Labour quantity",
                sort_order=1,
                source_quantity_item_id=uuid4(),
                source_checksum_sha256="a" * 64,
                quantity_value=Decimal("2.5"),
                unit="m",
                source_quantity_takeoff_id=quantity_takeoff_id,
                source_payload={"source": "takeoff"},
            ),
            EstimateQuantityEntryInput(
                entry_key="quantity:material",
                entry_label="Material quantity",
                sort_order=2,
                source_quantity_item_id=uuid4(),
                source_checksum_sha256="b" * 64,
                quantity_value=Decimal("1.25"),
                unit="m",
                source_quantity_takeoff_id=quantity_takeoff_id,
            ),
        ),
        rate_entries=(
            EstimateRateEntryInput(
                entry_key="rate:installer",
                entry_label="Installer labour",
                sort_order=3,
                source_rate_id=uuid4(),
                source_checksum_sha256="c" * 64,
                unit="m",
                effective_date=date(2026, 1, 1),
                unit_amount=Decimal("12.345678"),
            ),
            EstimateRateEntryInput(
                entry_key="rate:formula",
                entry_label="Formula rate",
                sort_order=4,
                source_rate_id=uuid4(),
                source_checksum_sha256="d" * 64,
                unit="m",
                effective_date=date(2026, 1, 1),
                unit_amount=Decimal("0.496000"),
            ),
        ),
        material_entries=(
            EstimateMaterialEntryInput(
                entry_key="material:paint",
                entry_label="Paint",
                sort_order=5,
                source_material_id=uuid4(),
                source_checksum_sha256="1" * 64,
                unit="m",
                effective_date=date(2026, 1, 1),
                unit_amount=Decimal("3.210000"),
            ),
        ),
        formula_entries=(
            EstimateFormulaEntryInput(
                entry_key="formula:rounded_markup",
                entry_label="Rounded markup",
                sort_order=6,
                source_formula_id=uuid4(),
                source_checksum_sha256=formula_definition.checksum,
                definition=formula_definition,
            ),
        ),
        assumption_entries=(
            EstimateAssumptionEntryInput(
                entry_key="assumption:mobilisation",
                entry_label="Mobilisation",
                sort_order=7,
                source_checksum_sha256="2" * 64,
                amount=Decimal("5.555"),
            ),
        ),
        line_inputs=(
            EstimateLineInputSpec(
                line_key="line:labour",
                line_type="rate",
                description="Installer labour",
                quantity_entry_key="quantity:labour",
                rate_entry_key="rate:installer",
            ),
            EstimateLineInputSpec(
                line_key="line:material",
                line_type="material",
                description="Paint",
                quantity_entry_key="quantity:material",
                material_entry_key="material:paint",
            ),
            EstimateLineInputSpec(
                line_key="line:formula",
                line_type="formula",
                description="Rounded markup",
                formula_entry_key="formula:rounded_markup",
                formula_inputs={
                    "rate": "rate:formula",
                    "quantity": "quantity:labour",
                },
            ),
            EstimateLineInputSpec(
                line_key="line:assumption",
                line_type="assumption",
                description="Mobilisation",
                assumption_entry_key="assumption:mobilisation",
            ),
            EstimateLineInputSpec(
                line_key="line:adjustment",
                line_type="adjustment",
                description="Access uplift",
                assumption_entry_key="assumption:mobilisation",
                adjustment_amount=Decimal("2.225"),
            ),
        ),
    )


def test_compose_estimate_builds_deterministic_payloads_and_totals() -> None:
    engine_input = _valid_engine_input()

    result = compose_estimate(engine_input)

    assert result.estimate_version_id == deterministic_estimate_version_id(
        engine_input.estimate_job_id
    )
    assert result.header is not None
    assert result.header.file_id == engine_input.file_id
    assert result.header.source_job_id == engine_input.source_job_id
    assert result.project_id == engine_input.project_id
    assert result.quantity_takeoff_id == engine_input.quantity_takeoff_id
    assert [entry.entry_key for entry in result.snapshot_entries] == [
        "quantity:labour",
        "quantity:material",
        "rate:installer",
        "rate:formula",
        "material:paint",
        "formula:rounded_markup",
        "assumption:mobilisation",
    ]
    assert result.snapshot_entries[0].id == deterministic_snapshot_entry_id(
        engine_input.estimate_job_id,
        "quantity:labour",
    )
    # quantity_input and assumption snapshots do not persist the source checksum: the
    # source-fields-by-type DB constraint requires it to be NULL for those entry types.
    assert result.snapshot_entries[0].source_checksum_sha256 is None
    assert result.snapshot_entries[-1].source_checksum_sha256 is None
    assert result.snapshot_entries[-1].currency is None

    labour_line, material_line, formula_line, assumption_line, adjustment_line = result.line_items
    assert labour_line.id == deterministic_item_id(engine_input.estimate_job_id, "line:labour")
    assert labour_line.subtotal_amount == Decimal("30.86")
    assert labour_line.tax_amount == Decimal("6.17")
    assert labour_line.total_amount == Decimal("37.03")

    assert material_line.subtotal_amount == Decimal("4.01")
    assert material_line.tax_amount == Decimal("0.80")
    assert material_line.total_amount == Decimal("4.81")

    assert formula_line.subtotal_amount == Decimal("1.20")
    assert formula_line.tax_amount == Decimal("0.24")
    assert formula_line.total_amount == Decimal("1.44")
    assert formula_line.formula_snapshot_entry_id == deterministic_snapshot_entry_id(
        engine_input.estimate_job_id,
        "formula:rounded_markup",
    )
    assert formula_line.quantity_snapshot_entry_id == deterministic_snapshot_entry_id(
        engine_input.estimate_job_id,
        "quantity:labour",
    )
    assert formula_line.quantity_value == Decimal("2.5")
    assert formula_line.quantity_unit == "m"
    assert formula_line.rate_snapshot_entry_id is None
    assert formula_line.material_snapshot_entry_id is None
    assert formula_line.assumption_snapshot_entry_id is None
    assert formula_line.details["pre_round_amount"] == "1.24"
    assert formula_line.details["evaluated_amount"] == "1.2"
    assert formula_line.details["applied_rounding"] == {"scale": 1, "mode": "ROUND_HALF_UP"}

    assert assumption_line.subtotal_amount == Decimal("5.56")
    assert assumption_line.total_amount == Decimal("6.67")
    assert adjustment_line.subtotal_amount == Decimal("2.23")
    assert adjustment_line.total_amount == Decimal("2.68")
    assert adjustment_line.formula_snapshot_entry_id is None
    assert adjustment_line.assumption_snapshot_entry_id == deterministic_snapshot_entry_id(
        engine_input.estimate_job_id,
        "assumption:mobilisation",
    )

    assert result.subtotal_amount == Decimal("43.86")
    assert result.tax_amount == Decimal("8.77")
    assert result.total_amount == Decimal("52.63")
    assert result.header.subtotal_amount == result.subtotal_amount
    assert result.header.tax_amount == result.tax_amount
    assert result.header.total_amount == result.total_amount
    assert result.header.quantity_gate == "allowed"
    assert result.header.trusted_totals is True

    snapshot_payloads = result.snapshot_entry_model_kwargs()
    formula_snapshot_payload = next(
        payload for payload in snapshot_payloads if payload["entry_key"] == "formula:rounded_markup"
    )
    assumption_snapshot_payload = next(
        payload
        for payload in snapshot_payloads
        if payload["entry_key"] == "assumption:mobilisation"
    )
    formula_line_payload = next(
        payload
        for payload in result.line_item_model_kwargs()
        if payload["line_key"] == "line:formula"
    )

    assert result.estimate_version_model_kwargs()["quantity_gate"] == "allowed"
    assert formula_snapshot_payload["source_payload_json"] == {
        "formula_definition": {
            "formula_id": "formula.rounded_markup",
            "name": "Rounded markup",
            "version": 1,
            "checksum": "f" * 64,
            "output_key": "estimate.rounded_markup",
            "output_contract": {"kind": "money", "currency": "GBP", "unit": None, "per_unit": None},
            "declared_inputs": [
                {
                    "name": "rate",
                    "contract": {"kind": "rate", "currency": "GBP", "unit": None, "per_unit": "m"},
                },
                {
                    "name": "quantity",
                    "contract": {
                        "kind": "quantity",
                        "currency": None,
                        "unit": "m",
                        "per_unit": None,
                    },
                },
            ],
            "expression": {
                "kind": "multiply",
                "value": None,
                "name": None,
                "args": [
                    {"kind": "input", "value": None, "name": "rate", "args": [], "rounding": None},
                    {
                        "kind": "input",
                        "value": None,
                        "name": "quantity",
                        "args": [],
                        "rounding": None,
                    },
                ],
                "rounding": None,
            },
            "rounding": {"scale": 1, "mode": "ROUND_HALF_UP"},
        }
    }
    assert assumption_snapshot_payload["currency"] is None
    assert assumption_snapshot_payload["source_payload_json"] == {"money_amount": "5.555"}
    assert formula_line_payload["quantity_snapshot_entry_id"] == deterministic_snapshot_entry_id(
        engine_input.estimate_job_id,
        "quantity:labour",
    )
    assert formula_line_payload["quantity_value"] == Decimal("2.5")
    assert formula_line_payload["quantity_unit"] == "m"
    assert formula_line_payload["rate_snapshot_entry_id"] is None
    assert formula_line_payload["material_snapshot_entry_id"] is None
    assert formula_line_payload["assumption_snapshot_entry_id"] is None


def test_compose_estimate_replays_rate_lines_from_persisted_quantity_scale() -> None:
    estimate_job_id = UUID("77777777-7777-7777-7777-777777777777")
    project_id = UUID("88888888-8888-8888-8888-888888888888")
    file_id = UUID("99999999-9999-9999-9999-999999999999")
    drawing_revision_id = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    quantity_takeoff_id = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    source_job_id = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
    quantity_item_id = UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
    rate_id = UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")

    def _engine_input(quantity_value: Decimal) -> EstimateEngineInput:
        return EstimateEngineInput(
            estimate_job_id=estimate_job_id,
            project_id=project_id,
            file_id=file_id,
            drawing_revision_id=drawing_revision_id,
            quantity_takeoff_id=quantity_takeoff_id,
            source_job_id=source_job_id,
            quantity_gate="allowed",
            trusted_totals=True,
            tax_rate=Decimal("0"),
            quantity_entries=(
                EstimateQuantityEntryInput(
                    entry_key="quantity:labour",
                    entry_label="Labour quantity",
                    sort_order=1,
                    source_quantity_item_id=quantity_item_id,
                    source_checksum_sha256="a" * 64,
                    quantity_value=quantity_value,
                    unit="m",
                    source_quantity_takeoff_id=quantity_takeoff_id,
                ),
            ),
            rate_entries=(
                EstimateRateEntryInput(
                    entry_key="rate:installer",
                    entry_label="Installer labour",
                    sort_order=2,
                    source_rate_id=rate_id,
                    source_checksum_sha256="c" * 64,
                    unit="m",
                    effective_date=date(2026, 1, 1),
                    unit_amount=Decimal("0.500000"),
                ),
            ),
            line_inputs=(
                EstimateLineInputSpec(
                    line_key="line:labour",
                    line_type="rate",
                    description="Installer labour",
                    quantity_entry_key="quantity:labour",
                    rate_entry_key="rate:installer",
                ),
            ),
        )

    result = compose_estimate(_engine_input(Decimal("0.29")))
    replay = compose_estimate(_engine_input(Decimal("0.290000")))

    assert result.snapshot_entries[0].quantity_value == Decimal("0.290000")
    assert result.line_items[0].subtotal_amount == Decimal("0.15")
    assert replay.line_items[0].subtotal_amount == Decimal("0.15")
    assert result.snapshot_entry_model_kwargs() == replay.snapshot_entry_model_kwargs()
    assert result.line_item_model_kwargs() == replay.line_item_model_kwargs()


@pytest.mark.parametrize("quantity_gate", ["provisional", "review_gated", "blocked"])
def test_compose_estimate_rejects_non_allowed_takeoffs(quantity_gate: str) -> None:
    engine_input = _valid_engine_input(quantity_gate=quantity_gate)

    with pytest.raises(EstimateEngineError) as exc_info:
        compose_estimate(engine_input)

    assert exc_info.value.code == "INPUT_INVALID"
    assert exc_info.value.reason == "quantity_gate_not_allowed"


def test_compose_estimate_rejects_missing_lineage_and_untrusted_totals() -> None:
    engine_input = _valid_engine_input()
    broken_input = EstimateEngineInput(
        estimate_job_id=engine_input.estimate_job_id,
        project_id=engine_input.project_id,
        file_id=None,
        drawing_revision_id=engine_input.drawing_revision_id,
        quantity_takeoff_id=engine_input.quantity_takeoff_id,
        source_job_id=engine_input.source_job_id,
        quantity_gate=engine_input.quantity_gate,
        trusted_totals=False,
        tax_rate=engine_input.tax_rate,
        quantity_entries=engine_input.quantity_entries,
        rate_entries=engine_input.rate_entries,
        material_entries=engine_input.material_entries,
        formula_entries=engine_input.formula_entries,
        assumption_entries=engine_input.assumption_entries,
        line_inputs=engine_input.line_inputs,
    )

    with pytest.raises(EstimateEngineError) as exc_info:
        compose_estimate(broken_input)

    assert exc_info.value.reason == "missing_lineage"


def test_compose_estimate_rejects_tax_rate_above_one() -> None:
    """A tax_rate above 1 is a fat-fingered percentage and must be rejected."""
    broken_input = dataclasses.replace(_valid_engine_input(), tax_rate=Decimal("20"))

    with pytest.raises(EstimateEngineError) as exc_info:
        compose_estimate(broken_input)

    assert exc_info.value.reason == "tax_rate_out_of_range"


def test_compose_estimate_rejects_money_formula_rounding_finer_than_money_scale() -> None:
    """A money formula that rounds to >2dp would be double-rounded by the money step."""
    engine_input = _valid_engine_input()
    sub_penny_definition = dataclasses.replace(
        _formula_definition(),
        rounding=RoundingSpec(scale=3, mode="ROUND_HALF_UP"),
    )
    broken_input = dataclasses.replace(
        engine_input,
        formula_entries=(
            dataclasses.replace(
                engine_input.formula_entries[0],
                definition=sub_penny_definition,
                source_checksum_sha256=sub_penny_definition.checksum,
            ),
        ),
    )

    with pytest.raises(EstimateEngineError) as exc_info:
        compose_estimate(broken_input)

    assert exc_info.value.reason == "unsupported_formula_rounding"


def test_compose_estimate_rejects_invalid_checksum() -> None:
    engine_input = _valid_engine_input()
    broken_input = EstimateEngineInput(
        estimate_job_id=engine_input.estimate_job_id,
        project_id=engine_input.project_id,
        file_id=engine_input.file_id,
        drawing_revision_id=engine_input.drawing_revision_id,
        quantity_takeoff_id=engine_input.quantity_takeoff_id,
        source_job_id=engine_input.source_job_id,
        quantity_gate=engine_input.quantity_gate,
        trusted_totals=engine_input.trusted_totals,
        tax_rate=engine_input.tax_rate,
        quantity_entries=engine_input.quantity_entries,
        rate_entries=(
            EstimateRateEntryInput(
                entry_key="rate:installer",
                entry_label="Installer labour",
                sort_order=3,
                source_rate_id=uuid4(),
                source_checksum_sha256="NOT-A-CHECKSUM",
                unit="ft",
                effective_date=date(2026, 1, 1),
                unit_amount=Decimal("12.345678"),
            ),
        ),
        material_entries=(),
        formula_entries=(),
        assumption_entries=(),
        line_inputs=(
            EstimateLineInputSpec(
                line_key="line:labour",
                line_type="rate",
                description="Installer labour",
                quantity_entry_key="quantity:labour",
                rate_entry_key="rate:installer",
            ),
        ),
    )

    with pytest.raises(EstimateEngineError) as exc_info:
        compose_estimate(broken_input)

    assert exc_info.value.reason == "invalid_checksum"


def test_compose_estimate_rejects_unit_mismatch_without_conversion() -> None:
    engine_input = _valid_engine_input()
    broken_input = EstimateEngineInput(
        estimate_job_id=engine_input.estimate_job_id,
        project_id=engine_input.project_id,
        file_id=engine_input.file_id,
        drawing_revision_id=engine_input.drawing_revision_id,
        quantity_takeoff_id=engine_input.quantity_takeoff_id,
        source_job_id=engine_input.source_job_id,
        quantity_gate=engine_input.quantity_gate,
        trusted_totals=engine_input.trusted_totals,
        tax_rate=engine_input.tax_rate,
        quantity_entries=engine_input.quantity_entries,
        rate_entries=(
            EstimateRateEntryInput(
                entry_key="rate:installer",
                entry_label="Installer labour",
                sort_order=3,
                source_rate_id=uuid4(),
                source_checksum_sha256="c" * 64,
                unit="ft",
                effective_date=date(2026, 1, 1),
                unit_amount=Decimal("12.345678"),
            ),
        ),
        material_entries=(),
        formula_entries=(),
        assumption_entries=(),
        line_inputs=(
            EstimateLineInputSpec(
                line_key="line:labour",
                line_type="rate",
                description="Installer labour",
                quantity_entry_key="quantity:labour",
                rate_entry_key="rate:installer",
            ),
        ),
    )

    with pytest.raises(EstimateEngineError) as exc_info:
        compose_estimate(broken_input)

    assert exc_info.value.reason == "unit_mismatch"


def test_compose_estimate_rejects_negative_adjustment_lines() -> None:
    engine_input = _valid_engine_input()
    broken_input = EstimateEngineInput(
        estimate_job_id=engine_input.estimate_job_id,
        project_id=engine_input.project_id,
        file_id=engine_input.file_id,
        drawing_revision_id=engine_input.drawing_revision_id,
        quantity_takeoff_id=engine_input.quantity_takeoff_id,
        source_job_id=engine_input.source_job_id,
        quantity_gate=engine_input.quantity_gate,
        trusted_totals=engine_input.trusted_totals,
        tax_rate=engine_input.tax_rate,
        quantity_entries=engine_input.quantity_entries,
        rate_entries=engine_input.rate_entries,
        material_entries=engine_input.material_entries,
        formula_entries=engine_input.formula_entries,
        assumption_entries=engine_input.assumption_entries,
        line_inputs=(
            EstimateLineInputSpec(
                line_key="line:adjustment",
                line_type="adjustment",
                description="Discount",
                adjustment_amount=Decimal("-0.01"),
            ),
        ),
    )

    with pytest.raises(EstimateEngineError) as exc_info:
        compose_estimate(broken_input)

    assert exc_info.value.reason == "negative_adjustment"


def test_compose_estimate_rejects_invalid_rate_scale() -> None:
    engine_input = _valid_engine_input()
    broken_input = EstimateEngineInput(
        estimate_job_id=engine_input.estimate_job_id,
        project_id=engine_input.project_id,
        file_id=engine_input.file_id,
        drawing_revision_id=engine_input.drawing_revision_id,
        quantity_takeoff_id=engine_input.quantity_takeoff_id,
        source_job_id=engine_input.source_job_id,
        quantity_gate=engine_input.quantity_gate,
        trusted_totals=engine_input.trusted_totals,
        tax_rate=engine_input.tax_rate,
        quantity_entries=engine_input.quantity_entries,
        rate_entries=(
            EstimateRateEntryInput(
                entry_key="rate:installer",
                entry_label="Installer labour",
                sort_order=3,
                source_rate_id=uuid4(),
                source_checksum_sha256="c" * 64,
                unit="m",
                effective_date=date(2026, 1, 1),
                unit_amount=Decimal("12.3456789"),
            ),
        ),
        material_entries=(),
        formula_entries=(),
        assumption_entries=(),
        line_inputs=(
            EstimateLineInputSpec(
                line_key="line:labour",
                line_type="rate",
                description="Installer labour",
                quantity_entry_key="quantity:labour",
                rate_entry_key="rate:installer",
            ),
        ),
    )

    with pytest.raises(EstimateEngineError) as exc_info:
        compose_estimate(broken_input)

    assert exc_info.value.reason == "invalid_rate_scale"


@pytest.mark.parametrize(
    "line_input",
    [
        EstimateLineInputSpec(
            line_key="line:adjustment",
            line_type="adjustment",
            description="Access uplift",
            adjustment_amount=Decimal("2.225"),
        ),
        EstimateLineInputSpec(
            line_key="line:adjustment",
            line_type="adjustment",
            description="Access uplift",
            formula_entry_key="formula:rounded_markup",
            assumption_entry_key="assumption:mobilisation",
            adjustment_amount=Decimal("2.225"),
        ),
    ],
)
def test_compose_estimate_rejects_adjustments_without_exactly_one_anchor(
    line_input: EstimateLineInputSpec,
) -> None:
    engine_input = _valid_engine_input()
    broken_input = EstimateEngineInput(
        estimate_job_id=engine_input.estimate_job_id,
        project_id=engine_input.project_id,
        file_id=engine_input.file_id,
        drawing_revision_id=engine_input.drawing_revision_id,
        quantity_takeoff_id=engine_input.quantity_takeoff_id,
        source_job_id=engine_input.source_job_id,
        quantity_gate=engine_input.quantity_gate,
        trusted_totals=engine_input.trusted_totals,
        tax_rate=engine_input.tax_rate,
        quantity_entries=engine_input.quantity_entries,
        rate_entries=engine_input.rate_entries,
        material_entries=engine_input.material_entries,
        formula_entries=engine_input.formula_entries,
        assumption_entries=engine_input.assumption_entries,
        line_inputs=(*engine_input.line_inputs[:-1], line_input),
    )

    with pytest.raises(EstimateEngineError) as exc_info:
        compose_estimate(broken_input)

    assert exc_info.value.reason == "invalid_adjustment_anchor"


def test_compose_estimate_resolves_scalar_and_money_formula_inputs() -> None:
    """Formula inputs of kind money + scalar resolve from named assumption entries."""
    scalar_contract = ValueContract(kind="scalar")
    formula = FormulaDefinition(
        formula_id="formula.scaled_cost",
        name="Scaled cost",
        version=1,
        checksum="e" * 64,
        output_key="estimate.scaled_cost",
        output_contract=MONEY_GBP,
        declared_inputs=(
            FormulaInputDefinition(name="base", contract=MONEY_GBP),
            FormulaInputDefinition(name="divisor", contract=scalar_contract),
        ),
        expression=FormulaNode(
            kind="divide",
            args=(
                FormulaNode(kind="input", name="base"),
                FormulaNode(kind="input", name="divisor"),
            ),
        ),
        rounding=RoundingSpec(scale=2, mode="ROUND_HALF_UP"),
    )
    engine_input = EstimateEngineInput(
        estimate_job_id=UUID("11111111-1111-1111-1111-111111111111"),
        project_id=UUID("22222222-2222-2222-2222-222222222222"),
        file_id=UUID("33333333-3333-3333-3333-333333333333"),
        drawing_revision_id=UUID("44444444-4444-4444-4444-444444444444"),
        quantity_takeoff_id=UUID("55555555-5555-5555-5555-555555555555"),
        source_job_id=UUID("66666666-6666-6666-6666-666666666666"),
        quantity_gate="allowed",
        trusted_totals=True,
        tax_rate=Decimal("0"),
        assumption_entries=(
            EstimateAssumptionEntryInput(
                entry_key="assumption:base_cost",
                entry_label="base_cost",
                sort_order=1,
                source_checksum_sha256="2" * 64,
                amount=Decimal("10.00"),
                kind="money",
            ),
            EstimateAssumptionEntryInput(
                entry_key="assumption:divisor",
                entry_label="divisor",
                sort_order=2,
                source_checksum_sha256="3" * 64,
                amount=Decimal("4"),
                kind="scalar",
            ),
        ),
        formula_entries=(
            EstimateFormulaEntryInput(
                entry_key="formula:scaled_cost",
                entry_label="Scaled cost",
                sort_order=3,
                source_formula_id=uuid4(),
                source_checksum_sha256=formula.checksum,
                definition=formula,
            ),
        ),
        line_inputs=(
            EstimateLineInputSpec(
                line_key="line:formula",
                line_type="formula",
                description="Scaled cost",
                formula_entry_key="formula:scaled_cost",
                formula_inputs={
                    "base": "assumption:base_cost",
                    "divisor": "assumption:divisor",
                },
            ),
        ),
    )

    output = compose_estimate(engine_input)

    formula_line = next(line for line in output.line_items if line.line_key == "line:formula")
    # money 10.00 / scalar 4 = 2.50 money
    assert formula_line.subtotal_amount == Decimal("2.50")
    snapshot_keys = {entry.entry_key for entry in output.snapshot_entries}
    assert {"assumption:base_cost", "assumption:divisor"} <= snapshot_keys
