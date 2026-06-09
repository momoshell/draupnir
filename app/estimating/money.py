from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

GBP_MONEY_QUANTUM = Decimal("0.01")
CATALOG_QUANTUM = Decimal("0.000001")
MONEY_QUANTUM = GBP_MONEY_QUANTUM
# Number of decimal places money is expressed in (derived from the quantum so the
# two never drift). Used to keep formula-output rounding from re-rounding money.
MONEY_SCALE = -int(GBP_MONEY_QUANTUM.as_tuple().exponent)
_CATALOG_SCALE_EXPONENT = -6
_ZERO = Decimal("0")


def _quantize(value: Decimal, *, quantum: Decimal) -> Decimal:
    return value.quantize(quantum, rounding=ROUND_HALF_UP)


def round_money(value: Decimal) -> Decimal:
    return _quantize(value, quantum=GBP_MONEY_QUANTUM)


def round_catalog_decimal(value: Decimal) -> Decimal:
    return _quantize(value, quantum=CATALOG_QUANTUM)


def format_money(value: Decimal, *, normalize_zero: bool = False) -> str:
    return _format_decimal(round_money(value), normalize_zero=normalize_zero)


def format_catalog_decimal(value: Decimal, *, normalize_zero: bool = False) -> str:
    return _format_decimal(round_catalog_decimal(value), normalize_zero=normalize_zero)


def validate_catalog_money(
    value: Decimal,
    *,
    field_name: str,
    require_decimal: bool = False,
) -> Decimal:
    if require_decimal and not isinstance(value, Decimal):
        raise ValueError(f"{field_name} must be a Decimal.")
    if not value.is_finite():
        raise ValueError(f"{field_name} must be finite.")
    if value <= _ZERO:
        raise ValueError(f"{field_name} must be greater than zero.")
    exponent = value.as_tuple().exponent
    if not isinstance(exponent, int) or exponent < _CATALOG_SCALE_EXPONENT:
        raise ValueError(f"{field_name} must use at most six decimal places.")
    return value


def _format_decimal(value: Decimal, *, normalize_zero: bool) -> str:
    normalized = abs(value) if normalize_zero and value.is_zero() else value
    return format(normalized, "f")
