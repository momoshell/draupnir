"""Server-derived canonical checksum helpers for estimation catalog APIs."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from uuid import UUID

from app.estimating.formulas import (
    FormulaInputDefinition,
    FormulaNode,
    RoundingSpec,
    ValueContract,
    validate_formula_definition_json,
)

_SIX_DECIMAL_PLACES = Decimal("0.000001")
_ZERO = Decimal("0.000000")


def rate_checksum_sha256(
    *,
    scope_type: str,
    project_id: UUID | None,
    rate_key: str,
    source: str,
    metadata_json: Mapping[str, object],
    name: str,
    item_type: str,
    per_unit: str,
    currency: str,
    amount: Decimal,
    effective_from: date,
    effective_to: date | None,
) -> str:
    _validate_scope(scope_type, project_id)
    _require_non_empty_string(rate_key, field_name="rate_key")
    _require_non_empty_string(source, field_name="source")
    _require_mapping(metadata_json, field_name="metadata_json")
    _require_non_empty_string(name, field_name="name")
    _require_non_empty_string(item_type, field_name="item_type")
    _require_non_empty_string(per_unit, field_name="per_unit")
    _validate_gbp_currency(currency)
    _validate_positive_money(amount, field_name="amount")
    _validate_effective_window(effective_from, effective_to)
    return _checksum(
        {
            "scope_type": scope_type,
            "project_id": project_id,
            "rate_key": rate_key,
            "source": source,
            "metadata_json": dict(metadata_json),
            "name": name,
            "item_type": item_type,
            "per_unit": per_unit,
            "currency": currency,
            "amount": amount,
            "effective_from": effective_from,
            "effective_to": effective_to,
        }
    )


def material_checksum_sha256(
    *,
    scope_type: str,
    project_id: UUID | None,
    material_key: str,
    source: str,
    metadata_json: Mapping[str, object],
    name: str,
    unit: str,
    currency: str,
    unit_cost: Decimal,
    effective_from: date,
    effective_to: date | None,
) -> str:
    _validate_scope(scope_type, project_id)
    _require_non_empty_string(material_key, field_name="material_key")
    _require_non_empty_string(source, field_name="source")
    _require_mapping(metadata_json, field_name="metadata_json")
    _require_non_empty_string(name, field_name="name")
    _require_non_empty_string(unit, field_name="unit")
    _validate_gbp_currency(currency)
    _validate_positive_money(unit_cost, field_name="unit_cost")
    _validate_effective_window(effective_from, effective_to)
    return _checksum(
        {
            "scope_type": scope_type,
            "project_id": project_id,
            "material_key": material_key,
            "source": source,
            "metadata_json": dict(metadata_json),
            "name": name,
            "unit": unit,
            "currency": currency,
            "unit_cost": unit_cost,
            "effective_from": effective_from,
            "effective_to": effective_to,
        }
    )


def formula_checksum_sha256(
    *,
    scope_type: str,
    project_id: UUID | None,
    formula_id: str,
    version: int,
    name: str,
    dsl_version: str,
    output_key: str,
    output_contract_json: Mapping[str, object],
    declared_inputs_json: Sequence[Mapping[str, object]],
    expression_json: Mapping[str, object],
    rounding_json: Mapping[str, object] | None = None,
) -> str:
    _validate_scope(scope_type, project_id)
    _require_non_empty_string(formula_id, field_name="formula_id")
    if type(version) is not int or version < 1:
        raise ValueError("version must be greater than or equal to one.")
    _require_non_empty_string(name, field_name="name")
    _require_non_empty_string(dsl_version, field_name="dsl_version")
    _require_non_empty_string(output_key, field_name="output_key")
    _require_mapping(output_contract_json, field_name="output_contract_json")
    _require_mapping_sequence(declared_inputs_json, field_name="declared_inputs_json")
    _require_mapping(expression_json, field_name="expression_json")
    if rounding_json is not None:
        _require_mapping(rounding_json, field_name="rounding_json")

    definition = validate_formula_definition_json(
        formula_id=formula_id,
        name=name,
        version=version,
        checksum="pending",
        output_key=output_key,
        output_contract_json=output_contract_json,
        declared_inputs_json=declared_inputs_json,
        expression_json=expression_json,
        rounding_json=rounding_json,
    )
    return _checksum(
        {
            "scope_type": scope_type,
            "project_id": project_id,
            "formula_id": definition.formula_id,
            "version": definition.version,
            "name": definition.name,
            "dsl_version": dsl_version,
            "output_key": definition.output_key,
            "output_contract_json": _value_contract_json(definition.output_contract),
            "declared_inputs_json": [
                _formula_input_definition_json(item) for item in definition.declared_inputs
            ],
            "expression_json": _formula_node_json(definition.expression),
            "rounding_json": _rounding_json(definition.rounding),
        }
    )


def _checksum(payload: Mapping[str, object]) -> str:
    canonical = _canonicalize(payload)
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"), allow_nan=False).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def _canonicalize(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, date | datetime):
        return value.isoformat()
    if isinstance(value, float):
        raise ValueError("canonical checksum payloads do not accept float values.")
    if isinstance(value, Mapping):
        canonical: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("canonical checksum payload object keys must be strings.")
            canonical[key] = _canonicalize(item)
        return canonical
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_canonicalize(item) for item in value]
    raise ValueError(f"Unsupported canonical checksum payload value type: {type(value).__name__}.")


def _decimal_text(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("canonical checksum decimal values must be finite.")
    try:
        quantized = value.quantize(_SIX_DECIMAL_PLACES)
    except InvalidOperation as exc:
        raise ValueError("canonical checksum decimal values must fit six decimal places.") from exc
    if quantized == _ZERO:
        quantized = _ZERO
    return format(quantized, ".6f")


def _validate_scope(scope_type: str, project_id: UUID | None) -> None:
    if scope_type not in {"global", "project"}:
        raise ValueError("scope_type must be 'global' or 'project'.")
    if scope_type == "global" and project_id is not None:
        raise ValueError("project_id must be null when scope_type is 'global'.")
    if scope_type == "project" and project_id is None:
        raise ValueError("project_id is required when scope_type is 'project'.")


def _validate_gbp_currency(currency: str) -> None:
    if currency != "GBP":
        raise ValueError("currency must be 'GBP'.")


def _validate_positive_money(value: Decimal, *, field_name: str) -> None:
    if not isinstance(value, Decimal):
        raise ValueError(f"{field_name} must be a Decimal.")
    if not value.is_finite():
        raise ValueError(f"{field_name} must be finite.")
    if value <= Decimal("0"):
        raise ValueError(f"{field_name} must be greater than zero.")
    exponent = value.as_tuple().exponent
    if not isinstance(exponent, int) or exponent < -6:
        raise ValueError(f"{field_name} must use at most six decimal places.")


def _validate_effective_window(effective_from: date, effective_to: date | None) -> None:
    if not isinstance(effective_from, date):
        raise ValueError("effective_from must be a date.")
    if effective_to is not None and not isinstance(effective_to, date):
        raise ValueError("effective_to must be a date when provided.")
    if effective_to is not None and effective_to <= effective_from:
        raise ValueError("effective_to must be greater than effective_from.")


def _require_non_empty_string(value: str, *, field_name: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string.")


def _require_mapping(value: Mapping[str, object], *, field_name: str) -> None:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object.")


def _require_mapping_sequence(value: Sequence[Mapping[str, object]], *, field_name: str) -> None:
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise ValueError(f"{field_name} must be an array.")
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            raise ValueError(f"{field_name}[{index}] must be an object.")


def _formula_input_definition_json(definition: FormulaInputDefinition) -> dict[str, object]:
    return {
        "name": definition.name,
        "contract": _value_contract_json(definition.contract),
    }


def _formula_node_json(node: FormulaNode) -> dict[str, object]:
    return {
        "kind": node.kind,
        "value": node.value,
        "name": node.name,
        "args": [_formula_node_json(arg) for arg in node.args],
        "rounding": _rounding_json(node.rounding),
    }


def _value_contract_json(contract: ValueContract) -> dict[str, object]:
    return {
        "kind": contract.kind,
        "currency": contract.currency,
        "unit": contract.unit,
        "per_unit": contract.per_unit,
    }


def _rounding_json(rounding: RoundingSpec | None) -> dict[str, object] | None:
    if rounding is None:
        return None
    return {"scale": rounding.scale, "mode": rounding.mode}
