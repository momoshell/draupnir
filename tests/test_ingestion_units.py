"""Tests for the shared INSUNITS → meters units module (issue #369).

These lock the single source of truth for both the DXF (``ezdxf``) and DWG
(``libredwg``) adapters: a parity check proves :data:`METER_SCALE` reproduces
``ezdxf.units`` (so the DXF adapter's behavior is unchanged and the table cannot
silently drift), and policy checks cover each adapter's deliberate divergence.
"""

from __future__ import annotations

import math
from typing import Any, cast

import pytest

from app.ingestion.adapters._units import (
    METER_SCALE,
    UNIT_NAMES,
    resolve_units,
    unit_name,
)

# INSUNITS codes ``ezdxf.units.conversion_factor`` cannot convert to meters — it raises
# ``TypeError`` on these (0 unitless, microinch, mil, the US survey units). They must stay
# out of the shared table so the DXF adapter keeps falling back to the unit name.
_EZDXF_NON_CONVERTIBLE_CODES = (0, 8, 9, 21, 22, 23, 24)

# The DWG adapter's curated allowlist (mirrors ``libredwg._CONFIRMED_INSUNITS_CODES``).
_LIBREDWG_CONFIRMED_CODES = frozenset({1, 2, 3, 4, 5, 6, 7, 10, 14, 15, 16})


def _libredwg_fallback(_code: int | None) -> str:
    return "unknown"


def test_meter_scale_matches_ezdxf_units_within_tolerance() -> None:
    import ezdxf.units as ezdxf_units

    for code, factor in METER_SCALE.items():
        reference = float(ezdxf_units.conversion_factor(cast(Any, code), cast(Any, 6)))
        assert math.isclose(factor, reference, rel_tol=1e-6), (code, factor, reference)


def test_meter_scale_excludes_codes_ezdxf_cannot_convert() -> None:
    import ezdxf.units as ezdxf_units

    for code in _EZDXF_NON_CONVERTIBLE_CODES:
        assert code not in METER_SCALE
        with pytest.raises(TypeError):
            ezdxf_units.conversion_factor(cast(Any, code), cast(Any, 6))


def test_unit_names_cover_full_insunits_range() -> None:
    assert set(UNIT_NAMES) == set(range(25))
    assert unit_name(6) == "meter"
    assert unit_name(0) == "unitless"
    assert unit_name(None) == "unitless"
    assert unit_name(999) == "unitless"


def test_meter_six_is_identity_factor() -> None:
    assert METER_SCALE[6] == 1.0


# --- ezdxf policy: confirm every convertible code, fall back to the unit name --------------


def test_ezdxf_policy_confirms_inch_to_exact_meter_factor() -> None:
    resolution = resolve_units(1, allowed_codes=METER_SCALE, fallback_label=unit_name)

    assert resolution.confirmed is True
    assert resolution.normalized == "meter"
    assert resolution.scale == 0.0254
    assert resolution.conversion_factor == 0.0254


def test_ezdxf_policy_names_codes_it_cannot_convert() -> None:
    resolution = resolve_units(8, allowed_codes=METER_SCALE, fallback_label=unit_name)

    assert resolution.confirmed is False
    assert resolution.normalized == "microinch"
    assert resolution.scale == 1.0
    assert resolution.conversion_factor is None


@pytest.mark.parametrize("code", [0, 99, None])
def test_ezdxf_policy_falls_back_to_unitless(code: int | None) -> None:
    resolution = resolve_units(code, allowed_codes=METER_SCALE, fallback_label=unit_name)

    assert resolution.confirmed is False
    assert resolution.normalized == "unitless"
    assert resolution.scale == 1.0


# --- libredwg policy: confirm only the curated subset, fall back to "unknown" --------------


def test_libredwg_policy_confirms_curated_code() -> None:
    resolution = resolve_units(
        2, allowed_codes=_LIBREDWG_CONFIRMED_CODES, fallback_label=_libredwg_fallback
    )

    assert resolution.confirmed is True
    assert resolution.normalized == "meter"
    assert resolution.scale == 0.3048
    assert resolution.conversion_factor == 0.3048


@pytest.mark.parametrize("code", [11, 17, 8, 0, None])
def test_libredwg_policy_keeps_uncurated_codes_unknown(code: int | None) -> None:
    resolution = resolve_units(
        code, allowed_codes=_LIBREDWG_CONFIRMED_CODES, fallback_label=_libredwg_fallback
    )

    assert resolution.confirmed is False
    assert resolution.normalized == "unknown"
    assert resolution.scale == 1.0
    assert resolution.conversion_factor is None


def test_libredwg_confirmed_codes_are_a_subset_of_shared_table() -> None:
    assert set(METER_SCALE) >= _LIBREDWG_CONFIRMED_CODES
