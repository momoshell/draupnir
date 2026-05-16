from decimal import Decimal, localcontext
from typing import Any, cast

import pytest

from app.estimating.formulas import (
    FormulaDefinition,
    FormulaEvaluationError,
    FormulaInputDefinition,
    FormulaNode,
    FormulaValue,
    RoundingSpec,
    ValueContract,
    evaluate_formula,
)

SCALAR = ValueContract(kind="scalar")
MONEY_GBP = ValueContract(kind="money", currency="GBP")
QUANTITY_M2 = ValueContract(kind="quantity", unit="m2")
RATE_GBP_PER_M2 = ValueContract(kind="rate", currency="GBP", per_unit="m2")
RATE_GBP_PER_M = ValueContract(kind="rate", currency="GBP", per_unit="m")


def test_evaluate_formula_supports_literals_and_inputs() -> None:
    definition = FormulaDefinition(
        formula_id="formula.base_plus_offset",
        name="Base plus offset",
        version=1,
        checksum="sha256:base-plus-offset",
        output_key="adjusted_base",
        output_contract=SCALAR,
        declared_inputs=(FormulaInputDefinition(name="base", contract=SCALAR),),
        expression=FormulaNode(
            kind="add",
            args=(
                FormulaNode(kind="input", name="base"),
                FormulaNode(kind="literal", value="2.5"),
            ),
        ),
    )

    result = evaluate_formula(
        definition,
        {"base": FormulaValue(amount=Decimal("3.25"), contract=SCALAR)},
    )

    assert result.formula_id == "formula.base_plus_offset"
    assert result.version == 1
    assert result.checksum == "sha256:base-plus-offset"
    assert result.output_key == "adjusted_base"
    assert result.value == FormulaValue(amount=Decimal("5.75"), contract=SCALAR)
    assert result.rounding is None


def test_evaluate_formula_supports_allowed_ops_and_functions() -> None:
    definition = FormulaDefinition(
        formula_id="formula.allowed_ops",
        name="Allowed ops",
        version=2,
        checksum="sha256:allowed-ops",
        output_key="result",
        output_contract=SCALAR,
        declared_inputs=(
            FormulaInputDefinition(name="a", contract=SCALAR),
            FormulaInputDefinition(name="b", contract=SCALAR),
            FormulaInputDefinition(name="c", contract=SCALAR),
        ),
        expression=FormulaNode(
            kind="max",
            args=(
                FormulaNode(
                    kind="divide",
                    args=(
                        FormulaNode(
                            kind="sum",
                            args=(
                                FormulaNode(kind="input", name="a"),
                                FormulaNode(
                                    kind="subtract",
                                    args=(
                                        FormulaNode(kind="input", name="b"),
                                        FormulaNode(kind="literal", value="1"),
                                    ),
                                ),
                                FormulaNode(kind="literal", value="1.5"),
                            ),
                        ),
                        FormulaNode(kind="literal", value="2"),
                    ),
                ),
                FormulaNode(
                    kind="min",
                    args=(
                        FormulaNode(kind="input", name="c"),
                        FormulaNode(
                            kind="negate",
                            args=(FormulaNode(kind="literal", value="-4"),),
                        ),
                    ),
                ),
            ),
        ),
    )

    result = evaluate_formula(
        definition,
        {
            "a": FormulaValue(amount=Decimal("5"), contract=SCALAR),
            "b": FormulaValue(amount=Decimal("2"), contract=SCALAR),
            "c": FormulaValue(amount=Decimal("3"), contract=SCALAR),
        },
    )

    assert result.value == FormulaValue(amount=Decimal("3.75"), contract=SCALAR)


def test_evaluate_formula_carries_formula_level_rounding_without_auto_applying() -> None:
    definition = FormulaDefinition(
        formula_id="formula.metadata_rounding",
        name="Metadata rounding",
        version=3,
        checksum="sha256:metadata-rounding",
        output_key="extended_price",
        output_contract=MONEY_GBP,
        declared_inputs=(
            FormulaInputDefinition(name="rate", contract=RATE_GBP_PER_M2),
            FormulaInputDefinition(name="quantity", contract=QUANTITY_M2),
        ),
        expression=FormulaNode(
            kind="multiply",
            args=(
                FormulaNode(kind="input", name="rate"),
                FormulaNode(kind="input", name="quantity"),
            ),
        ),
        rounding=RoundingSpec(scale=2, mode="ROUND_HALF_UP"),
    )

    result = evaluate_formula(
        definition,
        {
            "rate": FormulaValue(amount=Decimal("12.345"), contract=RATE_GBP_PER_M2),
            "quantity": FormulaValue(amount=Decimal("3"), contract=QUANTITY_M2),
        },
    )

    assert result.value == FormulaValue(amount=Decimal("37.035"), contract=MONEY_GBP)
    assert result.rounding == RoundingSpec(scale=2, mode="ROUND_HALF_UP")


def test_evaluate_formula_supports_explicit_round_nodes() -> None:
    definition = FormulaDefinition(
        formula_id="formula.explicit_round",
        name="Explicit round",
        version=4,
        checksum="sha256:explicit-round",
        output_key="rounded_total",
        output_contract=MONEY_GBP,
        declared_inputs=(
            FormulaInputDefinition(name="rate", contract=RATE_GBP_PER_M2),
            FormulaInputDefinition(name="quantity", contract=QUANTITY_M2),
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
    )

    result = evaluate_formula(
        definition,
        {
            "rate": FormulaValue(amount=Decimal("12.345"), contract=RATE_GBP_PER_M2),
            "quantity": FormulaValue(amount=Decimal("3"), contract=QUANTITY_M2),
        },
    )

    assert result.value == FormulaValue(amount=Decimal("37.04"), contract=MONEY_GBP)


def test_evaluate_formula_supports_rate_times_quantity_to_money() -> None:
    definition = FormulaDefinition(
        formula_id="formula.rate_times_quantity",
        name="Rate times quantity",
        version=5,
        checksum="sha256:rate-times-quantity",
        output_key="extended_price",
        output_contract=MONEY_GBP,
        declared_inputs=(
            FormulaInputDefinition(name="rate", contract=RATE_GBP_PER_M2),
            FormulaInputDefinition(name="quantity", contract=QUANTITY_M2),
        ),
        expression=FormulaNode(
            kind="multiply",
            args=(
                FormulaNode(kind="input", name="rate"),
                FormulaNode(kind="input", name="quantity"),
            ),
        ),
    )

    result = evaluate_formula(
        definition,
        {
            "rate": FormulaValue(amount=Decimal("19.99"), contract=RATE_GBP_PER_M2),
            "quantity": FormulaValue(amount=Decimal("2.5"), contract=QUANTITY_M2),
        },
    )

    assert result.value == FormulaValue(amount=Decimal("49.975"), contract=MONEY_GBP)


def test_evaluate_formula_raises_input_invalid_for_contract_mismatch() -> None:
    definition = FormulaDefinition(
        formula_id="formula.invalid_contract",
        name="Invalid contract",
        version=6,
        checksum="sha256:invalid-contract",
        output_key="extended_price",
        output_contract=MONEY_GBP,
        declared_inputs=(
            FormulaInputDefinition(name="rate", contract=RATE_GBP_PER_M2),
            FormulaInputDefinition(name="quantity", contract=QUANTITY_M2),
        ),
        expression=FormulaNode(
            kind="multiply",
            args=(
                FormulaNode(kind="input", name="rate"),
                FormulaNode(kind="input", name="quantity"),
            ),
        ),
    )

    with pytest.raises(FormulaEvaluationError) as exc_info:
        evaluate_formula(
            definition,
            {
                "rate": FormulaValue(amount=Decimal("10"), contract=RATE_GBP_PER_M),
                "quantity": FormulaValue(amount=Decimal("3"), contract=QUANTITY_M2),
            },
        )

    assert exc_info.value.code == "INPUT_INVALID"
    assert exc_info.value.reason == "input_contract_mismatch"


def test_evaluate_formula_raises_input_invalid_when_declared_input_is_missing() -> None:
    definition = FormulaDefinition(
        formula_id="formula.missing_input",
        name="Missing input",
        version=7,
        checksum="sha256:missing-input",
        output_key="result",
        output_contract=SCALAR,
        declared_inputs=(
            FormulaInputDefinition(name="a", contract=SCALAR),
            FormulaInputDefinition(name="b", contract=SCALAR),
        ),
        expression=FormulaNode(
            kind="add",
            args=(FormulaNode(kind="input", name="a"), FormulaNode(kind="input", name="b")),
        ),
    )

    with pytest.raises(FormulaEvaluationError) as exc_info:
        evaluate_formula(
            definition,
            {
                "a": FormulaValue(amount=Decimal("2"), contract=SCALAR),
            },
        )

    assert exc_info.value.code == "INPUT_INVALID"


def test_evaluate_formula_raises_input_invalid_for_invalid_decimal_literal() -> None:
    definition = FormulaDefinition(
        formula_id="formula.invalid_decimal_literal",
        name="Invalid decimal literal",
        version=8,
        checksum="sha256:invalid-decimal-literal",
        output_key="result",
        output_contract=SCALAR,
        declared_inputs=(),
        expression=FormulaNode(kind="literal", value="not-a-decimal"),
    )

    with pytest.raises(FormulaEvaluationError) as exc_info:
        evaluate_formula(definition, {})

    assert exc_info.value.code == "INPUT_INVALID"


def test_formula_node_rejects_unknown_extra_ast_keys() -> None:
    payload: Any = {"kind": "literal", "value": "1", "extra_key": "unexpected"}

    with pytest.raises(TypeError):
        FormulaNode(**payload)


def test_evaluate_formula_raises_input_invalid_for_divide_by_zero() -> None:
    definition = FormulaDefinition(
        formula_id="formula.divide_by_zero",
        name="Divide by zero",
        version=9,
        checksum="sha256:divide-by-zero",
        output_key="result",
        output_contract=SCALAR,
        declared_inputs=(FormulaInputDefinition(name="a", contract=SCALAR),),
        expression=FormulaNode(
            kind="divide",
            args=(FormulaNode(kind="input", name="a"), FormulaNode(kind="literal", value="0")),
        ),
    )

    with pytest.raises(FormulaEvaluationError) as exc_info:
        evaluate_formula(
            definition,
            {
                "a": FormulaValue(amount=Decimal("10"), contract=SCALAR),
            },
        )

    assert exc_info.value.code == "INPUT_INVALID"


def test_evaluate_formula_raises_input_invalid_for_currency_mismatch_in_addition() -> None:
    eur_money = ValueContract(kind="money", currency="EUR")
    definition = FormulaDefinition(
        formula_id="formula.currency_mismatch",
        name="Currency mismatch",
        version=10,
        checksum="sha256:currency-mismatch",
        output_key="result",
        output_contract=MONEY_GBP,
        declared_inputs=(
            FormulaInputDefinition(name="left", contract=MONEY_GBP),
            FormulaInputDefinition(name="right", contract=eur_money),
        ),
        expression=FormulaNode(
            kind="add",
            args=(FormulaNode(kind="input", name="left"), FormulaNode(kind="input", name="right")),
        ),
    )

    with pytest.raises(FormulaEvaluationError) as exc_info:
        evaluate_formula(
            definition,
            {
                "left": FormulaValue(amount=Decimal("10"), contract=MONEY_GBP),
                "right": FormulaValue(amount=Decimal("2"), contract=eur_money),
            },
        )

    assert exc_info.value.code == "INPUT_INVALID"


def test_evaluate_formula_raises_input_invalid_for_output_contract_mismatch() -> None:
    definition = FormulaDefinition(
        formula_id="formula.output_contract_mismatch",
        name="Output contract mismatch",
        version=11,
        checksum="sha256:output-contract-mismatch",
        output_key="extended_price",
        output_contract=QUANTITY_M2,
        declared_inputs=(
            FormulaInputDefinition(name="rate", contract=RATE_GBP_PER_M2),
            FormulaInputDefinition(name="quantity", contract=QUANTITY_M2),
        ),
        expression=FormulaNode(
            kind="multiply",
            args=(
                FormulaNode(kind="input", name="rate"),
                FormulaNode(kind="input", name="quantity"),
            ),
        ),
    )

    with pytest.raises(FormulaEvaluationError) as exc_info:
        evaluate_formula(
            definition,
            {
                "rate": FormulaValue(amount=Decimal("5"), contract=RATE_GBP_PER_M2),
                "quantity": FormulaValue(amount=Decimal("3"), contract=QUANTITY_M2),
            },
        )

    assert exc_info.value.code == "INPUT_INVALID"


def test_evaluate_formula_rejects_unsupported_execution_style_node_kinds() -> None:
    definition = FormulaDefinition(
        formula_id="formula.unsupported_node_kind",
        name="Unsupported node kind",
        version=12,
        checksum="sha256:unsupported-node-kind",
        output_key="result",
        output_contract=SCALAR,
        declared_inputs=(),
        expression=FormulaNode(kind=cast(Any, "import"), args=()),
    )

    with pytest.raises(FormulaEvaluationError) as exc_info:
        evaluate_formula(definition, {})

    assert exc_info.value.code == "INPUT_INVALID"


def test_evaluate_formula_ignores_ambient_decimal_context() -> None:
    definition = FormulaDefinition(
        formula_id="formula.ambient_context",
        name="Ambient context",
        version=13,
        checksum="sha256:ambient-context",
        output_key="rounded_total",
        output_contract=MONEY_GBP,
        declared_inputs=(
            FormulaInputDefinition(name="rate", contract=RATE_GBP_PER_M2),
            FormulaInputDefinition(name="quantity", contract=QUANTITY_M2),
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
    )

    with localcontext() as context:
        context.prec = 2
        context.rounding = "ROUND_DOWN"
        result = evaluate_formula(
            definition,
            {
                "rate": FormulaValue(amount=Decimal("12.345"), contract=RATE_GBP_PER_M2),
                "quantity": FormulaValue(amount=Decimal("3"), contract=QUANTITY_M2),
            },
        )

    assert result.value == FormulaValue(amount=Decimal("37.04"), contract=MONEY_GBP)


def test_evaluate_formula_raises_input_invalid_for_non_formula_node_children() -> None:
    definition = FormulaDefinition(
        formula_id="formula.bad_child",
        name="Bad child",
        version=14,
        checksum="sha256:bad-child",
        output_key="result",
        output_contract=SCALAR,
        declared_inputs=(),
        expression=FormulaNode(kind="sum", args=(cast(Any, "not-a-node"),)),
    )

    with pytest.raises(FormulaEvaluationError) as exc_info:
        evaluate_formula(definition, {})

    assert exc_info.value.code == "INPUT_INVALID"


def test_evaluate_formula_raises_input_invalid_for_non_rounding_spec() -> None:
    definition = FormulaDefinition(
        formula_id="formula.bad_rounding",
        name="Bad rounding",
        version=15,
        checksum="sha256:bad-rounding",
        output_key="result",
        output_contract=SCALAR,
        declared_inputs=(),
        expression=FormulaNode(
            kind="round",
            args=(FormulaNode(kind="literal", value="1"),),
            rounding=cast(Any, {"scale": 2, "mode": "ROUND_HALF_UP"}),
        ),
    )

    with pytest.raises(FormulaEvaluationError) as exc_info:
        evaluate_formula(definition, {})

    assert exc_info.value.code == "INPUT_INVALID"


def test_evaluate_formula_raises_input_invalid_for_non_string_fields() -> None:
    definition = FormulaDefinition(
        formula_id="formula.bad_fields",
        name=cast(Any, 123),
        version=16,
        checksum="sha256:bad-fields",
        output_key="result",
        output_contract=cast(Any, ValueContract(kind=cast(Any, 1))),
        declared_inputs=(FormulaInputDefinition(name=cast(Any, 1), contract=SCALAR),),
        expression=FormulaNode(kind="input", name=cast(Any, 2)),
    )

    with pytest.raises(FormulaEvaluationError) as exc_info:
        evaluate_formula(definition, {"2": FormulaValue(amount=Decimal("1"), contract=SCALAR)})

    assert exc_info.value.code == "INPUT_INVALID"


def test_evaluate_formula_raises_input_invalid_for_excessive_literal_length() -> None:
    definition = FormulaDefinition(
        formula_id="formula.long_literal",
        name="Long literal",
        version=17,
        checksum="sha256:long-literal",
        output_key="result",
        output_contract=SCALAR,
        declared_inputs=(),
        expression=FormulaNode(kind="literal", value="1" * 257),
    )

    with pytest.raises(FormulaEvaluationError) as exc_info:
        evaluate_formula(definition, {})

    assert exc_info.value.code == "INPUT_INVALID"


def test_evaluate_formula_raises_input_invalid_for_excessive_rounding_scale() -> None:
    definition = FormulaDefinition(
        formula_id="formula.rounding_scale",
        name="Rounding scale",
        version=18,
        checksum="sha256:rounding-scale",
        output_key="result",
        output_contract=SCALAR,
        declared_inputs=(),
        expression=FormulaNode(
            kind="round",
            args=(FormulaNode(kind="literal", value="1"),),
            rounding=RoundingSpec(scale=29, mode="ROUND_HALF_UP"),
        ),
    )

    with pytest.raises(FormulaEvaluationError) as exc_info:
        evaluate_formula(definition, {})

    assert exc_info.value.code == "INPUT_INVALID"


def test_evaluate_formula_raises_input_invalid_for_excessive_arg_count() -> None:
    definition = FormulaDefinition(
        formula_id="formula.arg_limit",
        name="Arg limit",
        version=19,
        checksum="sha256:arg-limit",
        output_key="result",
        output_contract=SCALAR,
        declared_inputs=(),
        expression=FormulaNode(
            kind="sum",
            args=tuple(FormulaNode(kind="literal", value="1") for _ in range(65)),
        ),
    )

    with pytest.raises(FormulaEvaluationError) as exc_info:
        evaluate_formula(definition, {})

    assert exc_info.value.code == "INPUT_INVALID"


def test_evaluate_formula_raises_input_invalid_for_excessive_node_count() -> None:
    nodes = [FormulaNode(kind="literal", value="1") for _ in range(257)]
    while len(nodes) > 1:
        next_level: list[FormulaNode] = []
        for index in range(0, len(nodes), 2):
            if index + 1 == len(nodes):
                next_level.append(nodes[index])
                continue
            next_level.append(
                FormulaNode(kind="add", args=(nodes[index], nodes[index + 1]))
            )
        nodes = next_level

    definition = FormulaDefinition(
        formula_id="formula.node_limit",
        name="Node limit",
        version=20,
        checksum="sha256:node-limit",
        output_key="result",
        output_contract=SCALAR,
        declared_inputs=(),
        expression=nodes[0],
    )

    with pytest.raises(FormulaEvaluationError) as exc_info:
        evaluate_formula(definition, {})

    assert exc_info.value.code == "INPUT_INVALID"


def test_evaluate_formula_raises_input_invalid_for_excessive_depth() -> None:
    expression = FormulaNode(kind="literal", value="1")
    for _ in range(64):
        expression = FormulaNode(kind="negate", args=(expression,))

    definition = FormulaDefinition(
        formula_id="formula.depth_limit",
        name="Depth limit",
        version=21,
        checksum="sha256:depth-limit",
        output_key="result",
        output_contract=SCALAR,
        declared_inputs=(),
        expression=expression,
    )

    with pytest.raises(FormulaEvaluationError) as exc_info:
        evaluate_formula(definition, {})

    assert exc_info.value.code == "INPUT_INVALID"


def test_formula_definition_requires_name() -> None:
    definition = FormulaDefinition(
        formula_id="formula.missing_name",
        name="",
        version=22,
        checksum="sha256:missing-name",
        output_key="result",
        output_contract=SCALAR,
        declared_inputs=(),
        expression=FormulaNode(kind="literal", value="1"),
    )

    with pytest.raises(FormulaEvaluationError) as exc_info:
        evaluate_formula(definition, {})

    assert exc_info.value.code == "INPUT_INVALID"
