"""DXF/INSERT units helpers for revision-to-DXF conversion."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class UnitSpec:
    """Normalized unit description for canonical to DXF conversion."""

    canonical: str
    insunits: int
    meter_scale: float


_UNIT_ALIASES: dict[str, UnitSpec] = {
    "m": UnitSpec(canonical="meter", insunits=6, meter_scale=1.0),
    "meter": UnitSpec(canonical="meter", insunits=6, meter_scale=1.0),
    "meters": UnitSpec(canonical="meter", insunits=6, meter_scale=1.0),
    "metre": UnitSpec(canonical="meter", insunits=6, meter_scale=1.0),
    "metres": UnitSpec(canonical="meter", insunits=6, meter_scale=1.0),
}


def resolve_unit(unit: str | None) -> UnitSpec:
    """Return a normalized unit spec for supported units."""

    if unit is None:
        return _UNIT_ALIASES["meter"]

    if not isinstance(unit, str):
        raise ValueError(f"Unsupported unit {unit!r}; expected string")

    normalized = unit.strip().lower()
    spec = _UNIT_ALIASES.get(normalized)
    if spec is not None:
        return spec

    raise ValueError(f"Unsupported unit {unit!r}")


__all__ = ["UnitSpec", "resolve_unit"]
