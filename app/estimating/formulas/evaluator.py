from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import (
    ROUND_HALF_EVEN,
    ROUND_HALF_UP,
    Context,
    Decimal,
    DecimalException,
    InvalidOperation,
    localcontext,
)
from typing import Final, NoReturn

from .contracts import (
    FormulaDefinition,
    FormulaEvaluationError,
    FormulaEvaluationResult,
    FormulaInputDefinition,
    FormulaNode,
    FormulaValue,
    RoundingSpec,
    ValueContract,
)

_INPUT_INVALID: Final = "INPUT_INVALID"
_ROUNDING_MODES: Final = {"ROUND_HALF_UP": ROUND_HALF_UP}
_SCALAR_CONTRACT: Final = ValueContract(kind="scalar")
_MAX_FORMULA_DEPTH: Final = 64
_MAX_NODE_COUNT: Final = 512
_MAX_NODE_ARGS: Final = 64
_MAX_LITERAL_LENGTH: Final = 256
_MAX_ROUNDING_SCALE: Final = 28
_EVALUATION_CONTEXT: Final = Context(
    prec=16_384,
    rounding=ROUND_HALF_EVEN,
    Emin=-999_999,
    Emax=999_999,
)


@dataclass(slots=True)
class _ValidationState:
    node_count: int = 0


def evaluate_formula(
    definition: FormulaDefinition,
    inputs: Mapping[str, FormulaValue],
) -> FormulaEvaluationResult:
    if not isinstance(inputs, Mapping):
        _raise_input_invalid(
            "inputs_invalid",
            "Formula inputs must be provided as a mapping.",
        )
    try:
        with localcontext(_EVALUATION_CONTEXT):
            _validate_definition(definition)
            declared_inputs = {
                declared_input.name: declared_input.contract
                for declared_input in definition.declared_inputs
            }
            for provided_name, provided_value in inputs.items():
                if not isinstance(provided_name, str) or not provided_name:
                    _raise_input_invalid(
                        "input_name_invalid",
                        "Input names must be non-empty strings.",
                    )
                if provided_name not in declared_inputs:
                    _raise_input_invalid(
                        "input_not_declared",
                        f"Input '{provided_name}' is not declared.",
                    )
                _validate_formula_value(provided_name, provided_value)
                expected_contract = declared_inputs[provided_name]
                if provided_value.contract != expected_contract:
                    _raise_input_invalid(
                        "input_contract_mismatch",
                        f"Input '{provided_name}' contract does not match the declared contract.",
                    )
            value = _evaluate_node(definition.expression, inputs, declared_inputs)
            if value.contract != definition.output_contract:
                _raise_input_invalid(
                    "output_contract_mismatch",
                    "Formula output contract does not match the declared output contract.",
                )
            return FormulaEvaluationResult(
                formula_id=definition.formula_id,
                version=definition.version,
                checksum=definition.checksum,
                output_key=definition.output_key,
                value=value,
                rounding=definition.rounding,
            )
    except FormulaEvaluationError:
        raise
    except DecimalException as exc:
        raise FormulaEvaluationError(
            code=_INPUT_INVALID,
            reason="decimal_invalid",
            message="Invalid decimal operation during formula evaluation.",
        ) from exc


def _validate_definition(definition: FormulaDefinition) -> None:
    if not isinstance(definition, FormulaDefinition):
        _raise_input_invalid(
            "formula_definition_invalid",
            "Formula definition must be a FormulaDefinition.",
        )
    _require_non_empty_string(
        definition.formula_id,
        reason="formula_id_required",
        label="Formula id",
    )
    _require_non_empty_string(definition.name, reason="formula_name_required", label="Formula name")
    if type(definition.version) is not int or definition.version < 1:
        _raise_input_invalid(
            "formula_version_invalid",
            "Formula version must be greater than or equal to one.",
        )
    _require_non_empty_string(
        definition.checksum,
        reason="formula_checksum_required",
        label="Formula checksum",
    )
    _require_non_empty_string(
        definition.output_key,
        reason="formula_output_key_required",
        label="Formula output key",
    )
    _validate_contract(definition.output_contract)
    declared_inputs = _validate_declared_inputs(definition.declared_inputs)
    if definition.rounding is not None:
        _validate_rounding(definition.rounding)
    state = _ValidationState()
    _validate_node(definition.expression, declared_inputs, depth=1, state=state)


def _validate_declared_inputs(
    declared_inputs: tuple[FormulaInputDefinition, ...],
) -> set[str]:
    if not isinstance(declared_inputs, (tuple, list)):
        _raise_input_invalid(
            "declared_inputs_invalid",
            "Declared inputs must be provided as a sequence of FormulaInputDefinition values.",
        )
    seen_inputs: set[str] = set()
    for declared_input in declared_inputs:
        _validate_declared_input(declared_input)
        if declared_input.name in seen_inputs:
            _raise_input_invalid(
                "declared_inputs_not_unique",
                "Declared inputs must be unique.",
            )
        seen_inputs.add(declared_input.name)
    return seen_inputs


def _validate_declared_input(declared_input: FormulaInputDefinition) -> None:
    if not isinstance(declared_input, FormulaInputDefinition):
        _raise_input_invalid(
            "declared_input_invalid",
            "Declared inputs must be FormulaInputDefinition values.",
        )
    _require_non_empty_string(
        declared_input.name,
        reason="declared_input_name_required",
        label="Declared input name",
    )
    _validate_contract(declared_input.contract)


def _validate_contract(contract: ValueContract) -> None:
    if not isinstance(contract, ValueContract):
        _raise_input_invalid("contract_invalid", "Formula contracts must be ValueContract values.")
    if not isinstance(contract.kind, str):
        _raise_input_invalid("contract_kind_invalid", "Contract kind must be a string.")
    _validate_optional_string(contract.currency, label="Contract currency")
    _validate_optional_string(contract.unit, label="Contract unit")
    _validate_optional_string(contract.per_unit, label="Contract per-unit")
    if contract.kind == "scalar":
        if (
            contract.currency is not None
            or contract.unit is not None
            or contract.per_unit is not None
        ):
            _raise_input_invalid(
                "contract_shape_invalid",
                "Scalar contracts cannot declare currency, unit, or per-unit metadata.",
            )
        return
    if contract.kind == "money":
        if not contract.currency or contract.unit is not None or contract.per_unit is not None:
            _raise_input_invalid(
                "contract_shape_invalid",
                "Money contracts must declare only a currency.",
            )
        return
    if contract.kind == "quantity":
        if contract.currency is not None or not contract.unit or contract.per_unit is not None:
            _raise_input_invalid(
                "contract_shape_invalid",
                "Quantity contracts must declare only a unit.",
            )
        return
    if contract.kind == "rate":
        if not contract.currency or contract.unit is not None or not contract.per_unit:
            _raise_input_invalid(
                "contract_shape_invalid",
                "Rate contracts must declare a currency and per-unit.",
            )
        return
    _raise_input_invalid("contract_kind_invalid", f"Unsupported contract kind '{contract.kind}'.")


def _validate_formula_value(name: str, value: FormulaValue) -> None:
    if not isinstance(value, FormulaValue):
        _raise_input_invalid("input_value_invalid", f"Input '{name}' must be a FormulaValue.")
    if not isinstance(value.amount, Decimal):
        _raise_input_invalid("input_amount_invalid", f"Input '{name}' amount must be a Decimal.")
    if not value.amount.is_finite():
        _raise_input_invalid("input_amount_invalid", f"Input '{name}' amount must be finite.")
    _validate_contract(value.contract)


def _validate_node(
    node: FormulaNode,
    declared_inputs: set[str],
    *,
    depth: int,
    state: _ValidationState,
) -> None:
    if not isinstance(node, FormulaNode):
        _raise_input_invalid("node_invalid", "Formula expression nodes must be FormulaNode values.")
    if depth > _MAX_FORMULA_DEPTH:
        _raise_input_invalid(
            "formula_depth_invalid",
            f"Formula expressions cannot be deeper than {_MAX_FORMULA_DEPTH} nodes.",
        )
    state.node_count += 1
    if state.node_count > _MAX_NODE_COUNT:
        _raise_input_invalid(
            "formula_node_count_invalid",
            f"Formula expressions cannot contain more than {_MAX_NODE_COUNT} nodes.",
        )
    if not isinstance(node.kind, str):
        _raise_input_invalid("node_kind_invalid", "Formula node kind must be a string.")
    args = _validate_node_args(node)
    if node.kind == "literal":
        if (
            not isinstance(node.value, str)
            or node.name is not None
            or args
            or node.rounding is not None
        ):
            _raise_input_invalid(
                "literal_node_invalid",
                "Literal nodes must define only a canonical decimal string value.",
            )
        _parse_canonical_decimal(node.value)
        return
    if node.kind == "input":
        if (
            not isinstance(node.name, str)
            or not node.name
            or node.value is not None
            or args
            or node.rounding is not None
        ):
            _raise_input_invalid(
                "input_node_invalid",
                "Input nodes must define only a declared input name.",
            )
        if node.name not in declared_inputs:
            _raise_input_invalid("input_not_declared", f"Input '{node.name}' is not declared.")
        return
    if node.kind == "negate":
        _validate_args(node, args, declared_inputs, depth=depth, state=state, expected=1)
        return
    if node.kind in {"add", "subtract", "multiply", "divide"}:
        _validate_args(node, args, declared_inputs, depth=depth, state=state, expected=2)
        return
    if node.kind in {"min", "max"}:
        _validate_args(node, args, declared_inputs, depth=depth, state=state, minimum=1)
        return
    if node.kind == "sum":
        _validate_args(node, args, declared_inputs, depth=depth, state=state, minimum=1)
        return
    if node.kind == "round":
        if node.value is not None or node.name is not None:
            _raise_input_invalid(
                "round_node_invalid",
                "Round nodes cannot define value or name fields.",
            )
        if len(args) != 1 or node.rounding is None:
            _raise_input_invalid(
                "round_node_invalid",
                "Round nodes require exactly one argument and a rounding spec.",
            )
        _validate_rounding(node.rounding)
        _validate_node(_require_node_child(args[0]), declared_inputs, depth=depth + 1, state=state)
        return
    _raise_input_invalid("node_kind_invalid", f"Unsupported formula node kind '{node.kind}'.")


def _validate_node_args(node: FormulaNode) -> tuple[object, ...]:
    if not isinstance(node.args, (tuple, list)):
        _raise_input_invalid(
            "node_args_invalid",
            "Formula node args must be provided as a sequence of child nodes.",
        )
    if len(node.args) > _MAX_NODE_ARGS:
        _raise_input_invalid(
            "node_args_invalid",
            f"Formula nodes cannot define more than {_MAX_NODE_ARGS} arguments.",
        )
    return tuple(node.args)


def _validate_args(
    node: FormulaNode,
    args: tuple[object, ...],
    declared_inputs: set[str],
    *,
    depth: int,
    state: _ValidationState,
    expected: int | None = None,
    minimum: int | None = None,
) -> None:
    if node.value is not None or node.name is not None or node.rounding is not None:
        _raise_input_invalid(
            f"{node.kind}_node_invalid",
            f"{node.kind} nodes cannot define value, name, or rounding fields.",
        )
    if expected is not None and len(args) != expected:
        _raise_input_invalid(
            f"{node.kind}_args_invalid",
            f"{node.kind} nodes require exactly {expected} arguments.",
        )
    if minimum is not None and len(args) < minimum:
        _raise_input_invalid(
            f"{node.kind}_args_invalid",
            f"{node.kind} nodes require at least {minimum} arguments.",
        )
    for child in args:
        _validate_node(_require_node_child(child), declared_inputs, depth=depth + 1, state=state)


def _validate_rounding(rounding: RoundingSpec) -> None:
    if not isinstance(rounding, RoundingSpec):
        _raise_input_invalid("rounding_invalid", "Rounding specs must be RoundingSpec values.")
    if type(rounding.scale) is not int:
        _raise_input_invalid("rounding_scale_invalid", "Rounding scale must be an integer.")
    if rounding.scale < 0:
        _raise_input_invalid(
            "rounding_scale_invalid",
            "Rounding scale must be greater than or equal to zero.",
        )
    if rounding.scale > _MAX_ROUNDING_SCALE:
        _raise_input_invalid(
            "rounding_scale_invalid",
            f"Rounding scale must be less than or equal to {_MAX_ROUNDING_SCALE}.",
        )
    if not isinstance(rounding.mode, str):
        _raise_input_invalid("rounding_mode_invalid", "Rounding mode must be a string.")
    if rounding.mode not in _ROUNDING_MODES:
        _raise_input_invalid(
            "rounding_mode_invalid",
            f"Unsupported rounding mode '{rounding.mode}'.",
        )


def _evaluate_node(
    node: FormulaNode,
    inputs: Mapping[str, FormulaValue],
    declared_inputs: Mapping[str, ValueContract],
) -> FormulaValue:
    if node.kind == "literal":
        return FormulaValue(
            amount=_parse_canonical_decimal(node.value or ""),
            contract=_SCALAR_CONTRACT,
        )
    if node.kind == "input":
        if node.name is None or node.name not in declared_inputs or node.name not in inputs:
            missing_name = node.name or "<unknown>"
            _raise_input_invalid("input_missing", f"Input '{missing_name}' is missing.")
        return inputs[node.name]
    if node.kind == "add":
        return _add_values(
            _evaluate_node(node.args[0], inputs, declared_inputs),
            _evaluate_node(node.args[1], inputs, declared_inputs),
        )
    if node.kind == "subtract":
        return _subtract_values(
            _evaluate_node(node.args[0], inputs, declared_inputs),
            _evaluate_node(node.args[1], inputs, declared_inputs),
        )
    if node.kind == "multiply":
        return _multiply_values(
            _evaluate_node(node.args[0], inputs, declared_inputs),
            _evaluate_node(node.args[1], inputs, declared_inputs),
        )
    if node.kind == "divide":
        divisor = _evaluate_node(node.args[1], inputs, declared_inputs)
        return _divide_values(_evaluate_node(node.args[0], inputs, declared_inputs), divisor)
    if node.kind == "negate":
        value = _evaluate_node(node.args[0], inputs, declared_inputs)
        return FormulaValue(amount=-value.amount, contract=value.contract)
    if node.kind == "min":
        return _min_or_max_values(node.kind, node.args, inputs, declared_inputs)
    if node.kind == "max":
        return _min_or_max_values(node.kind, node.args, inputs, declared_inputs)
    if node.kind == "sum":
        return _sum_values(node.args, inputs, declared_inputs)
    if node.kind == "round":
        value = _evaluate_node(node.args[0], inputs, declared_inputs)
        return FormulaValue(
            amount=_round_decimal(value.amount, node.rounding),
            contract=value.contract,
        )
    _raise_input_invalid("node_kind_invalid", f"Unsupported formula node kind '{node.kind}'.")
    raise AssertionError("unreachable")


def _add_values(left: FormulaValue, right: FormulaValue) -> FormulaValue:
    _require_identical_contracts(left.contract, right.contract, operation="add")
    return FormulaValue(amount=left.amount + right.amount, contract=left.contract)


def _subtract_values(left: FormulaValue, right: FormulaValue) -> FormulaValue:
    _require_identical_contracts(left.contract, right.contract, operation="subtract")
    return FormulaValue(amount=left.amount - right.amount, contract=left.contract)


def _multiply_values(left: FormulaValue, right: FormulaValue) -> FormulaValue:
    if left.contract == _SCALAR_CONTRACT:
        return FormulaValue(amount=left.amount * right.amount, contract=right.contract)
    if right.contract == _SCALAR_CONTRACT:
        return FormulaValue(amount=left.amount * right.amount, contract=left.contract)
    if left.contract.kind == "rate" and right.contract.kind == "quantity":
        return _multiply_rate_by_quantity(left, right)
    if left.contract.kind == "quantity" and right.contract.kind == "rate":
        return _multiply_rate_by_quantity(right, left)
    _raise_input_invalid(
        "contract_operation_unsupported",
        "Unsupported contract combination for multiply: "
        f"{left.contract.kind} and {right.contract.kind}.",
    )
    raise AssertionError("unreachable")


def _multiply_rate_by_quantity(rate: FormulaValue, quantity: FormulaValue) -> FormulaValue:
    if rate.contract.per_unit != quantity.contract.unit:
        _raise_input_invalid(
            "contract_mismatch",
            "Rate and quantity contracts must use the same unit for multiplication.",
        )
    return FormulaValue(
        amount=rate.amount * quantity.amount,
        contract=ValueContract(kind="money", currency=rate.contract.currency),
    )


def _divide_values(left: FormulaValue, right: FormulaValue) -> FormulaValue:
    if right.amount == Decimal("0"):
        _raise_input_invalid("division_by_zero", "Division by zero is not allowed.")
    if right.contract != _SCALAR_CONTRACT:
        _raise_input_invalid(
            "contract_operation_unsupported",
            "Unsupported divisor contract "
            f"'{right.contract.kind}'. Division requires a scalar divisor.",
        )
    result_contract = _SCALAR_CONTRACT if left.contract == _SCALAR_CONTRACT else left.contract
    return FormulaValue(amount=left.amount / right.amount, contract=result_contract)


def _min_or_max_values(
    operation: str,
    args: tuple[FormulaNode, ...],
    inputs: Mapping[str, FormulaValue],
    declared_inputs: Mapping[str, ValueContract],
) -> FormulaValue:
    values = [_evaluate_node(child, inputs, declared_inputs) for child in args]
    contract = values[0].contract
    for value in values[1:]:
        _require_identical_contracts(contract, value.contract, operation=operation)
    selector = min if operation == "min" else max
    selected = selector(values, key=lambda value: value.amount)
    return FormulaValue(amount=selected.amount, contract=contract)


def _sum_values(
    args: tuple[FormulaNode, ...],
    inputs: Mapping[str, FormulaValue],
    declared_inputs: Mapping[str, ValueContract],
) -> FormulaValue:
    values = [_evaluate_node(child, inputs, declared_inputs) for child in args]
    contract = values[0].contract
    total = values[0].amount
    for value in values[1:]:
        _require_identical_contracts(contract, value.contract, operation="sum")
        total += value.amount
    return FormulaValue(amount=total, contract=contract)


def _require_identical_contracts(
    left: ValueContract,
    right: ValueContract,
    *,
    operation: str,
) -> None:
    if left != right:
        _raise_input_invalid(
            "contract_mismatch",
            f"{operation.capitalize()} requires identical contracts.",
        )


def _parse_canonical_decimal(value: str) -> Decimal:
    if not isinstance(value, str):
        _raise_input_invalid(
            "literal_invalid",
            "Decimal literals must be canonical decimal strings.",
        )
    if len(value) > _MAX_LITERAL_LENGTH:
        _raise_input_invalid(
            "literal_invalid",
            f"Decimal literals cannot exceed {_MAX_LITERAL_LENGTH} characters.",
        )
    try:
        decimal_value = Decimal(value)
    except InvalidOperation as exc:
        raise FormulaEvaluationError(
            code=_INPUT_INVALID,
            reason="literal_invalid",
            message=f"Invalid decimal literal '{value}'.",
        ) from exc
    if not decimal_value.is_finite():
        _raise_input_invalid("literal_invalid", f"Invalid decimal literal '{value}'.")
    if value != _canonical_decimal_string(decimal_value):
        _raise_input_invalid(
            "literal_not_canonical",
            f"Decimal literal '{value}' is not canonical.",
        )
    return decimal_value


def _canonical_decimal_string(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        text = format(normalized.quantize(Decimal("1")), "f")
    else:
        text = format(normalized, "f")
    if text == "-0":
        return "0"
    return text


def _round_decimal(value: Decimal, rounding: RoundingSpec | None) -> Decimal:
    if rounding is None:
        _raise_input_invalid("round_node_invalid", "Round nodes require a rounding spec.")
    quantum = Decimal(1).scaleb(-rounding.scale)
    return value.quantize(quantum, rounding=_ROUNDING_MODES[rounding.mode])


def _require_non_empty_string(value: object, *, reason: str, label: str) -> None:
    if not isinstance(value, str) or not value:
        _raise_input_invalid(reason, f"{label} must be a non-empty string.")


def _require_node_child(value: object) -> FormulaNode:
    if not isinstance(value, FormulaNode):
        _raise_input_invalid("node_invalid", "Formula expression nodes must be FormulaNode values.")
    return value


def _validate_optional_string(value: object, *, label: str) -> None:
    if value is not None and not isinstance(value, str):
        _raise_input_invalid("contract_shape_invalid", f"{label} must be a string when provided.")


def _raise_input_invalid(reason: str, message: str) -> NoReturn:
    raise FormulaEvaluationError(code=_INPUT_INVALID, reason=reason, message=message)
