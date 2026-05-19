from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

from app.estimating.catalog.selection import SelectedFormula
from app.estimating.formulas import (
    FormulaDefinition,
    FormulaInputDefinition,
    FormulaNode,
    RoundingSpec,
    ValueContract,
)

from .errors import raise_unsupported_formula_json

_CONTRACT_KEYS = frozenset({"kind", "currency", "unit", "per_unit"})
_INPUT_KEYS = frozenset({"name", "contract"})
_NODE_KEYS = frozenset({"kind", "value", "name", "args", "rounding"})
_ROUNDING_KEYS = frozenset({"scale", "mode"})
_GBP_ONLY_KINDS = frozenset({"money", "rate"})


def formula_definition_from_selected_formula(selected: SelectedFormula) -> FormulaDefinition:
    return formula_definition_from_json(
        formula_id=selected.formula_id,
        name=selected.name,
        version=selected.version,
        checksum=selected.checksum_sha256,
        output_key=selected.output_key,
        output_contract_json=selected.output_contract,
        declared_inputs_json=selected.declared_inputs,
        expression_json=selected.expression,
        rounding_json=selected.rounding,
    )


def formula_definition_from_json(
    *,
    formula_id: str,
    name: str,
    version: int,
    checksum: str,
    output_key: str,
    output_contract_json: Mapping[str, object],
    declared_inputs_json: Sequence[Mapping[str, object]],
    expression_json: Mapping[str, object],
    rounding_json: Mapping[str, object] | None = None,
) -> FormulaDefinition:
    return FormulaDefinition(
        formula_id=formula_id,
        name=name,
        version=version,
        checksum=checksum,
        output_key=output_key,
        output_contract=_value_contract_from_json(output_contract_json, path="output_contract"),
        declared_inputs=tuple(
            _input_definition_from_json(item, path=f"declared_inputs[{index}]")
            for index, item in enumerate(declared_inputs_json)
        ),
        expression=_formula_node_from_json(expression_json, path="expression"),
        rounding=_rounding_from_json(rounding_json, path="rounding"),
    )


def _value_contract_from_json(payload: Mapping[str, object], *, path: str) -> ValueContract:
    _reject_legacy_shape(payload, path=path, legacy_key="type", reason="legacy_contract_shape")
    _reject_unknown_keys(payload, allowed_keys=_CONTRACT_KEYS, path=path, reason="contract_unknown_keys")

    kind = _require_str(payload.get("kind"), path=f"{path}.kind", reason="contract_kind_invalid")
    currency = _optional_str(payload.get("currency"), path=f"{path}.currency", reason="contract_currency_invalid")
    unit = _optional_str(payload.get("unit"), path=f"{path}.unit", reason="contract_unit_invalid")
    per_unit = _optional_str(payload.get("per_unit"), path=f"{path}.per_unit", reason="contract_per_unit_invalid")

    if kind in _GBP_ONLY_KINDS and currency != "GBP":
        raise_unsupported_formula_json(
            "unsupported_currency",
            f"{path}.currency must be 'GBP' for {kind} contracts.",
        )

    return ValueContract(kind=cast(Any, kind), currency=currency, unit=unit, per_unit=per_unit)


def _input_definition_from_json(payload: Mapping[str, object], *, path: str) -> FormulaInputDefinition:
    _reject_legacy_shape(payload, path=path, legacy_key="key", reason="legacy_input_shape")
    _reject_unknown_keys(payload, allowed_keys=_INPUT_KEYS, path=path, reason="input_unknown_keys")

    name = _require_str(payload.get("name"), path=f"{path}.name", reason="input_name_invalid")
    contract_payload = _require_mapping(
        payload.get("contract"),
        path=f"{path}.contract",
        reason="input_contract_invalid",
    )
    return FormulaInputDefinition(
        name=name,
        contract=_value_contract_from_json(contract_payload, path=f"{path}.contract"),
    )


def _formula_node_from_json(payload: Mapping[str, object], *, path: str) -> FormulaNode:
    _reject_legacy_shape(payload, path=path, legacy_key="op", reason="legacy_node_shape")
    _reject_unknown_keys(payload, allowed_keys=_NODE_KEYS, path=path, reason="node_unknown_keys")

    kind = _require_str(payload.get("kind"), path=f"{path}.kind", reason="node_kind_invalid")
    value = _optional_str(payload.get("value"), path=f"{path}.value", reason="node_value_invalid")
    name = _optional_str(payload.get("name"), path=f"{path}.name", reason="node_name_invalid")
    args_payload = payload.get("args", ())
    args: tuple[FormulaNode, ...]
    if args_payload is None:
        args = ()
    else:
        items = _require_sequence(args_payload, path=f"{path}.args", reason="node_args_invalid")
        args = tuple(
            _formula_node_from_json(
                _require_mapping(item, path=f"{path}.args[{index}]", reason="node_arg_invalid"),
                path=f"{path}.args[{index}]",
            )
            for index, item in enumerate(items)
        )

    return FormulaNode(
        kind=cast(Any, kind),
        value=value,
        name=name,
        args=args,
        rounding=_rounding_from_json(payload.get("rounding"), path=f"{path}.rounding"),
    )


def _rounding_from_json(
    payload: Mapping[str, object] | None | object,
    *,
    path: str,
) -> RoundingSpec | None:
    if payload is None:
        return None
    mapping = _require_mapping(payload, path=path, reason="rounding_invalid")
    _reject_unknown_keys(mapping, allowed_keys=_ROUNDING_KEYS, path=path, reason="rounding_unknown_keys")

    scale = mapping.get("scale")
    if not isinstance(scale, int) or isinstance(scale, bool):
        raise_unsupported_formula_json("rounding_scale_invalid", f"{path}.scale must be an integer.")

    mode = _require_str(mapping.get("mode"), path=f"{path}.mode", reason="rounding_mode_invalid")
    return RoundingSpec(scale=scale, mode=cast(Any, mode))


def _require_mapping(value: object, *, path: str, reason: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise_unsupported_formula_json(reason, f"{path} must be an object.")
    return cast(Mapping[str, object], value)


def _require_sequence(value: object, *, path: str, reason: str) -> Sequence[object]:
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise_unsupported_formula_json(reason, f"{path} must be an array.")
    return cast(Sequence[object], value)


def _require_str(value: object, *, path: str, reason: str) -> str:
    if not isinstance(value, str) or not value:
        raise_unsupported_formula_json(reason, f"{path} must be a non-empty string.")
    return value


def _optional_str(value: object, *, path: str, reason: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise_unsupported_formula_json(reason, f"{path} must be a non-empty string when provided.")
    return value


def _reject_legacy_shape(
    payload: Mapping[str, object],
    *,
    path: str,
    legacy_key: str,
    reason: str,
) -> None:
    if legacy_key in payload and "kind" not in payload:
        raise_unsupported_formula_json(reason, f"{path} uses unsupported legacy '{legacy_key}' JSON.")


def _reject_unknown_keys(
    payload: Mapping[str, object],
    *,
    allowed_keys: frozenset[str],
    path: str,
    reason: str,
) -> None:
    unknown_keys = sorted(key for key in payload if key not in allowed_keys)
    if unknown_keys:
        joined = ", ".join(unknown_keys)
        raise_unsupported_formula_json(reason, f"{path} contains unsupported keys: {joined}.")
