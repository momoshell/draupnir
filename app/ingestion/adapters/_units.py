"""Shared AutoCAD ``INSUNITS`` → meters knowledge for the DXF and DWG adapters.

This is the single source of truth for the ``INSUNITS`` code → unit-name and
code → meter-scale mappings, plus the resolution helper both adapters consume.
The DXF (``ezdxf``) and DWG (``libredwg``) adapters used to encode this domain
knowledge independently — ``ezdxf`` delegated to ``ezdxf.units.conversion_factor``
while ``libredwg`` hardcoded a curated table — which let them silently drift and
produce different scaled geometry for the same drawing (see issue #369).

The domain knowledge now lives only here. Each adapter keeps its own *policy* by
passing it into :func:`resolve_units`:

* which codes it is willing to confirm (``allowed_codes``), and
* what label it emits when a code is not confirmed (``fallback_label``).

``ezdxf`` confirms every convertible code and falls back to the unit name (e.g.
``"inch"``, ``"unitless"``); ``libredwg`` confirms only its review-gated curated
subset and falls back to ``"unknown"``. Those divergences are deliberate and stay
explicit here rather than being flattened.
"""

from __future__ import annotations

from collections.abc import Callable, Collection
from dataclasses import dataclass

# Full AutoCAD ``INSUNITS`` code → unit name (codes 0-24). Used by ``ezdxf`` for the
# normalized-name fallback when a code is not scaled to meters.
UNIT_NAMES: dict[int, str] = {
    0: "unitless",
    1: "inch",
    2: "foot",
    3: "mile",
    4: "millimeter",
    5: "centimeter",
    6: "meter",
    7: "kilometer",
    8: "microinch",
    9: "mil",
    10: "yard",
    11: "angstrom",
    12: "nanometer",
    13: "micron",
    14: "decimeter",
    15: "decameter",
    16: "hectometer",
    17: "gigameter",
    18: "astronomical_unit",
    19: "light_year",
    20: "parsec",
    21: "us_survey_foot",
    22: "us_survey_inch",
    23: "us_survey_yard",
    24: "us_survey_mile",
}

# Authoritative ``INSUNITS`` code → exact meters factor. Only codes with a
# well-defined conversion that ``ezdxf.units`` also normalizes appear here. Codes
# ``ezdxf`` cannot convert (0 unitless, 8 microinch, 9 mil, 21-24 US survey units --
# its library raises ``TypeError`` on these) are deliberately absent and stay
# name/``unknown`` fallbacks.
#
# Common construction codes use exact canonical definitions, which also match the
# DWG adapter's long-standing curated values, so the DWG output is unchanged. They
# sit within ~4e-9 (relative) of ``ezdxf``'s lossy intermediate-unit factors; the
# DXF adapter now emits these exact values instead of the lossy ones (a deliberate
# correction of the divergence in #369). The astronomical codes (18-20) match
# ``ezdxf``'s incumbent rounded values. ``tests/test_ingestion_units.py`` pins the
# whole table against ``ezdxf.units`` so it cannot drift, and so that enabling a
# code ``ezdxf`` cannot convert (e.g. US survey foot, code 21) forces a deliberate
# review.
METER_SCALE: dict[int, float] = {
    1: 0.0254,  # inch
    2: 0.3048,  # foot
    3: 1609.344,  # mile
    4: 0.001,  # millimeter
    5: 0.01,  # centimeter
    6: 1.0,  # meter
    7: 1000.0,  # kilometer
    10: 0.9144,  # yard
    11: 1e-10,  # angstrom
    12: 1e-9,  # nanometer
    13: 1e-6,  # micron
    14: 0.1,  # decimeter
    15: 10.0,  # decameter
    16: 100.0,  # hectometer
    17: 1e9,  # gigameter
    18: 149_597_870_700.0,  # astronomical unit
    19: 9.46e15,  # light year (matches ezdxf's rounded value)
    20: 3.09e16,  # parsec (matches ezdxf's rounded value)
}

_UNKNOWN_UNIT_NAME = "unitless"


def unit_name(code: int | None) -> str:
    """Return the AutoCAD unit name for ``code``, defaulting to ``"unitless"``."""

    if code is None:
        return _UNKNOWN_UNIT_NAME
    return UNIT_NAMES.get(code, _UNKNOWN_UNIT_NAME)


@dataclass(frozen=True, slots=True)
class UnitsResolution:
    """Outcome of resolving an ``INSUNITS`` code against an adapter's policy.

    ``normalized`` is ``"meter"`` when the code was confirmed and scaled, otherwise
    the adapter's fallback label. ``scale`` is the meters factor (``1.0`` when not
    confirmed). ``conversion_factor`` is the meters factor when confirmed, else
    ``None`` — adapters that surface the full units payload echo it directly.
    """

    normalized: str
    scale: float
    confirmed: bool
    conversion_factor: float | None


def resolve_units(
    code: int | None,
    *,
    allowed_codes: Collection[int],
    fallback_label: Callable[[int | None], str],
) -> UnitsResolution:
    """Resolve an ``INSUNITS`` ``code`` to meters under an adapter's policy.

    The code is confirmed (and scaled to meters) only when it is both within the
    shared :data:`METER_SCALE` table *and* in the adapter's ``allowed_codes``.
    Every other case (``None``, code 0, unsupported/exotic codes, or codes the
    adapter has chosen not to confirm yet) degrades to an unconfirmed resolution
    labelled by ``fallback_label``.
    """

    if code is not None and code in allowed_codes and code in METER_SCALE:
        factor = METER_SCALE[code]
        return UnitsResolution(
            normalized="meter",
            scale=factor,
            confirmed=True,
            conversion_factor=factor,
        )

    return UnitsResolution(
        normalized=fallback_label(code),
        scale=1.0,
        confirmed=False,
        conversion_factor=None,
    )
