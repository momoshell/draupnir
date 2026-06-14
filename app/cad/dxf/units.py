"""DXF/INSERT units helpers for revision-to-DXF conversion."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class UnitSpec:
    """Normalized unit description for canonical to DXF conversion."""

    canonical: str
    insunits: int
    meter_scale: float


#: Spec for geometry whose real-world unit is unknown. INSUNITS 0 == "unitless" in DXF;
#: meter_scale 1.0 passes coordinates through unchanged (faithful native/paper coordinates).
UNITLESS = UnitSpec(canonical="unitless", insunits=0, meter_scale=1.0)

_UNIT_ALIASES: dict[str, UnitSpec] = {
    "m": UnitSpec(canonical="meter", insunits=6, meter_scale=1.0),
    "meter": UnitSpec(canonical="meter", insunits=6, meter_scale=1.0),
    "meters": UnitSpec(canonical="meter", insunits=6, meter_scale=1.0),
    "metre": UnitSpec(canonical="meter", insunits=6, meter_scale=1.0),
    "metres": UnitSpec(canonical="meter", insunits=6, meter_scale=1.0),
    # INSUNITS 4 == millimetres. meter_scale 1.0: coordinates are already in the declared unit
    # ($INSUNITS just labels them); the PDF real-world export pre-scales points to mm via the
    # canonical "coordinate_scale" field.
    "mm": UnitSpec(canonical="millimeter", insunits=4, meter_scale=1.0),
    "millimeter": UnitSpec(canonical="millimeter", insunits=4, meter_scale=1.0),
    "millimeters": UnitSpec(canonical="millimeter", insunits=4, meter_scale=1.0),
    "millimetre": UnitSpec(canonical="millimeter", insunits=4, meter_scale=1.0),
    "millimetres": UnitSpec(canonical="millimeter", insunits=4, meter_scale=1.0),
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


__all__ = ["UNITLESS", "UnitSpec", "resolve_unit"]
