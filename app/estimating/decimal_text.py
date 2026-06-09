"""Canonical decimal-to-text helpers for the estimating subsystem.

Three distinct string representations are needed, and they intentionally differ —
keeping them in one module makes the distinction explicit so callers pick the right
one and the forms don't silently drift apart:

- :func:`display_text` — normalized, trailing zeros stripped. Human-facing
  details/snapshot JSON where the exact scale is not significant.
- :func:`checksum_text` — fixed six decimal places. Canonical input to catalog
  checksums, where the scale is part of the hashed contract.
- :func:`canonical_text` — normalized, integral values rendered without a
  fractional part. Used by the formula-checksum canonicalizer.

All three are checksum/serialization sensitive; change behavior only deliberately.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

from app.estimating.money import CATALOG_QUANTUM

_CHECKSUM_QUANTUM = CATALOG_QUANTUM
_CHECKSUM_ZERO = Decimal("0.000000")


def display_text(value: Decimal) -> str:
    """Return a normalized decimal with trailing zeros (and a bare ``.``) stripped."""

    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def checksum_text(value: Decimal) -> str:
    """Return a fixed six-decimal-place rendering for canonical catalog checksums."""

    if not value.is_finite():
        raise ValueError("canonical checksum decimal values must be finite.")
    try:
        quantized = value.quantize(_CHECKSUM_QUANTUM)
    except InvalidOperation as exc:
        raise ValueError("canonical checksum decimal values must fit six decimal places.") from exc
    if quantized == _CHECKSUM_ZERO:
        quantized = _CHECKSUM_ZERO
    return format(quantized, ".6f")


def canonical_text(value: Decimal) -> str:
    """Return a normalized decimal, rendering integral values without a fraction."""

    normalized = value.normalize()
    if normalized == normalized.to_integral():
        text = format(normalized.quantize(Decimal("1")), "f")
    else:
        text = format(normalized, "f")
    if text == "-0":
        return "0"
    return text
