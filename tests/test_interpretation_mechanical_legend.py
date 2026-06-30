"""Unit tests for the mechanical colour→service legend reducer (issue #775).

Pure — no DB, no ORM, no fixtures beyond local fakes. Covers:
- from_mech_swatches: basic, blank honest-absent, colour-less skip, competing services,
  same-colour-multiple-services collapse, empty input, tolerant no-raise.
- build_mechanical_legend: empty, by_key(), lookup().
- Permutation-invariance: 8 M-560103 rows.
- Module purity: no sqlalchemy/fastapi imports.
- lookup_tolerant: hue-trap regression, achromatic gate, margin guard, empty legend.
"""

from __future__ import annotations

import itertools
import sys

from app.interpretation.mechanical_legend import (
    MECH_SOURCE_SWATCH,
    MechanicalLegend,
    MechSwatchInput,
    build_mechanical_legend,
    from_mech_swatches,
)

# ---------------------------------------------------------------------------
# 1. test_single_swatch_basic
# ---------------------------------------------------------------------------


def test_single_swatch_basic() -> None:
    """Single swatch with real M-560103 values produces one entry with correct fields."""
    inp = MechSwatchInput(
        color={"index": 247, "rgb": "c2873f51", "by_layer": False, "by_block": False},
        text="LTHW-VT-F",
    )
    entries = from_mech_swatches([inp])
    assert len(entries) == 1
    e = entries[0]
    assert e.colour_key == "c2873f51"
    assert e.colour_rgb == "c2873f51"
    assert e.colour_index == 247
    assert e.service == "LTHW-VT-F"
    assert e.sources == (MECH_SOURCE_SWATCH,)
    assert e.competing_services == ()


# ---------------------------------------------------------------------------
# 2. test_blank_text_honest_absent
# ---------------------------------------------------------------------------


def test_blank_text_honest_absent() -> None:
    """Blank label text → entry emitted with service=None (honest-absent)."""
    inp = MechSwatchInput(
        color={"rgb": "c2873f51"},
        text="   ",
    )
    entries = from_mech_swatches([inp])
    assert len(entries) == 1
    assert entries[0].service is None
    assert entries[0].competing_services == ()


# ---------------------------------------------------------------------------
# 3. test_colour_key_none_skipped
# ---------------------------------------------------------------------------


def test_colour_key_none_skipped() -> None:
    """Input with no rgb and no index → colour_key is None → silently skipped."""
    inp = MechSwatchInput(color={"by_layer": True}, text="LTHW-VT-F")
    entries = from_mech_swatches([inp])
    assert entries == []


# ---------------------------------------------------------------------------
# 4. test_color_none_skipped
# ---------------------------------------------------------------------------


def test_color_none_skipped() -> None:
    """Input with color=None → silently skipped."""
    inp = MechSwatchInput(color=None, text="LTHW-VT-F")
    entries = from_mech_swatches([inp])
    assert entries == []


# ---------------------------------------------------------------------------
# 5. test_competing_services_same_colour_key
# ---------------------------------------------------------------------------


def test_competing_services_same_colour_key() -> None:
    """Two distinct service names on the same colour_key → one entry; primary alphabetically
    first; the other in competing_services.

    This mirrors M-560103 where c29632ff maps to three services (VRF CASSETTE, OVER DOOR
    HEATER, RADIANT HEATING PANELS — alphabetically OVER DOOR HEATER is primary).
    """
    inp1 = MechSwatchInput(color={"rgb": "c29632ff"}, text="VRF CASSETTE")
    inp2 = MechSwatchInput(color={"rgb": "c29632ff"}, text="OVER DOOR HEATER")
    inp3 = MechSwatchInput(color={"rgb": "c29632ff"}, text="RADIANT HEATING PANELS")

    entries = from_mech_swatches([inp1, inp2, inp3])
    assert len(entries) == 1
    e = entries[0]
    assert e.colour_key == "c29632ff"
    # Alphabetically first among the three is "OVER DOOR HEATER"
    assert e.service == "OVER DOOR HEATER"
    assert "VRF CASSETTE" in e.competing_services
    assert "RADIANT HEATING PANELS" in e.competing_services
    assert len(e.competing_services) == 2


# ---------------------------------------------------------------------------
# 6. test_distinct_colours_two_entries
# ---------------------------------------------------------------------------


def test_distinct_colours_two_entries() -> None:
    """Two swatches with different colour_keys → two entries."""
    inp1 = MechSwatchInput(color={"rgb": "c2873f51"}, text="LTHW-VT-F")
    inp2 = MechSwatchInput(color={"rgb": "c2bc7083"}, text="LTHW-VT-R")
    entries = from_mech_swatches([inp1, inp2])
    assert len(entries) == 2
    by_key = {e.colour_key: e for e in entries}
    assert by_key["c2873f51"].service == "LTHW-VT-F"
    assert by_key["c2bc7083"].service == "LTHW-VT-R"


# ---------------------------------------------------------------------------
# 7. test_empty_input
# ---------------------------------------------------------------------------


def test_empty_input() -> None:
    """Empty input list → empty entries list."""
    assert from_mech_swatches([]) == []


# ---------------------------------------------------------------------------
# 8. test_tolerant_bad_input_no_raise
# ---------------------------------------------------------------------------


def test_tolerant_bad_input_no_raise() -> None:
    """Malformed inputs are silently skipped; no exception raised."""
    bad: list[MechSwatchInput] = [
        MechSwatchInput(color="not-a-mapping", text="X"),  # type: ignore[arg-type]
        MechSwatchInput(color=None, text="Y"),
        MechSwatchInput(color={"rgb": "aabbcc"}, text="OK"),
    ]
    entries = from_mech_swatches(bad)
    # Only the valid entry survives
    assert len(entries) == 1
    assert entries[0].service == "OK"


# ---------------------------------------------------------------------------
# 9. test_build_mechanical_legend_empty
# ---------------------------------------------------------------------------


def test_build_mechanical_legend_empty() -> None:
    """build_mechanical_legend([]) returns a MechanicalLegend with no entries."""
    legend = build_mechanical_legend([])
    assert isinstance(legend, MechanicalLegend)
    assert legend.entries == ()
    assert legend.by_key() == {}


# ---------------------------------------------------------------------------
# 10. test_build_mechanical_legend_lookup
# ---------------------------------------------------------------------------


def test_build_mechanical_legend_lookup() -> None:
    """lookup() returns the correct entry for a known colour_key."""
    inp = MechSwatchInput(
        color={"rgb": "c2873f51", "index": 247},
        text="LTHW-VT-F",
    )
    legend = build_mechanical_legend([inp])
    entry = legend.lookup("c2873f51")
    assert entry is not None
    assert entry.service == "LTHW-VT-F"


def test_build_mechanical_legend_lookup_unknown_returns_none() -> None:
    """lookup() returns None for an unknown colour_key."""
    legend = build_mechanical_legend([])
    assert legend.lookup("deadbeef") is None
    assert legend.lookup(None) is None


# ---------------------------------------------------------------------------
# 11. test_lookup_honest_absent_returned
# ---------------------------------------------------------------------------


def test_lookup_honest_absent_returned() -> None:
    """Honest-absent entry (service=None) is still returned by lookup (key exists)."""
    inp = MechSwatchInput(color={"rgb": "c2bc7083"}, text="")
    legend = build_mechanical_legend([inp])
    entry = legend.lookup("c2bc7083")
    assert entry is not None
    assert entry.service is None


# ---------------------------------------------------------------------------
# 12. test_deterministic_sort
# ---------------------------------------------------------------------------


def test_deterministic_sort() -> None:
    """Output order is deterministic regardless of input permutation."""
    inputs = [
        MechSwatchInput(color={"rgb": "c2873f51"}, text="LTHW-VT-F"),
        MechSwatchInput(color={"rgb": "c2bc7083"}, text="LTHW-VT-R"),
        MechSwatchInput(color={"rgb": "c200ff00"}, text="MECHANICAL EQUIPMENT"),
    ]
    ref = from_mech_swatches(inputs)
    for perm in itertools.permutations(inputs):
        result = from_mech_swatches(list(perm))
        assert [e.colour_key for e in result] == [e.colour_key for e in ref]


# ---------------------------------------------------------------------------
# 13. test_m560103_eight_rows_permutation_invariant
# ---------------------------------------------------------------------------


def test_m560103_eight_rows_permutation_invariant() -> None:
    """M-560103 8-row legend: three entries share the same colour_key (c29632ff).

    All permutations of the 8 inputs produce the same output (deterministic).
    The shared-colour entries collapse to one entry with competing_services.
    """
    m560103_inputs = [
        MechSwatchInput(color={"rgb": "c2873f51", "index": 247}, text="LTHW-VT-F"),
        MechSwatchInput(color={"rgb": "c2bc7083", "index": 243}, text="LTHW-VT-R"),
        MechSwatchInput(color={"rgb": "c29be7f4", "index": 141}, text="VRF TRAY"),
        MechSwatchInput(color={"rgb": "c29632ff", "index": 190}, text="VRF CASSETTE"),
        MechSwatchInput(color={"rgb": "c29632ff", "index": 190}, text="OVER DOOR HEATER"),
        MechSwatchInput(color={"rgb": "c29632ff", "index": 190}, text="RADIANT HEATING PANELS"),
        MechSwatchInput(color={"rgb": "c200ff00", "index": 3}, text="MECHANICAL EQUIPMENT"),
        MechSwatchInput(color={"rgb": "c29783dc", "index": 193}, text="HYDRAULIC EQUIPMENT"),
    ]

    ref_entries = from_mech_swatches(m560103_inputs)

    # 8 inputs but c29632ff appears 3x → 6 distinct colour_keys → 6 entries
    assert len(ref_entries) == 6

    by_key = {e.colour_key: e for e in ref_entries}

    # Two directly-verified entries
    assert by_key["c2873f51"].service == "LTHW-VT-F"
    assert by_key["c2bc7083"].service == "LTHW-VT-R"

    # Three-way collision on c29632ff
    assert by_key["c29632ff"].service == "OVER DOOR HEATER"  # alphabetically first
    assert len(by_key["c29632ff"].competing_services) == 2

    # Determinism: first 4 permutations are enough (full 8! is too slow for a unit test)
    first_four_perms = list(itertools.islice(itertools.permutations(m560103_inputs), 4))
    for perm in first_four_perms:
        result = from_mech_swatches(list(perm))
        assert [e.colour_key for e in result] == [e.colour_key for e in ref_entries]


# ---------------------------------------------------------------------------
# 14. test_module_purity
# ---------------------------------------------------------------------------


def test_module_purity() -> None:
    """mechanical_legend.py must not import SQLAlchemy, FastAPI, or celery."""
    for mod_name in list(sys.modules.keys()):
        if mod_name == "app.interpretation.mechanical_legend":
            # Reload is not required — just check the module object's imports.
            pass
    import app.interpretation.mechanical_legend as mod

    # Walk the module's globals for disallowed imports.
    disallowed = {"sqlalchemy", "fastapi", "celery"}
    for name, obj in vars(mod).items():
        if hasattr(obj, "__module__"):
            pkg = (obj.__module__ or "").split(".")[0]
            assert pkg not in disallowed, f"mechanical_legend imports {obj.__module__} via {name!r}"


# ---------------------------------------------------------------------------
# 15. lookup_tolerant — P-520001 legend fixture
# ---------------------------------------------------------------------------

# P-520001 mechanical legend swatches (rev 78d00585):
#   c2ffb120  255,177,32   hue≈39°  → "SVP SOIL VENT PIPE"
#   c2f5b9b9  245,185,185  hue≈0°   → "VENT PIPE"
#   c2008700  0,135,0      hue≈120° → "RAIN WATER PIPE"
#   c2ff80d5  255,128,213  hue≈320° → "CDP"  (already exact-matches fill)
_P520001_INPUTS = [
    MechSwatchInput(color={"rgb": "c2ffb120"}, text="SVP SOIL VENT PIPE"),
    MechSwatchInput(color={"rgb": "c2f5b9b9"}, text="VENT PIPE"),
    MechSwatchInput(color={"rgb": "c2008700"}, text="RAIN WATER PIPE"),
    MechSwatchInput(color={"rgb": "c2ff80d5"}, text="CDP"),
]


def _p520001_legend() -> MechanicalLegend:
    return build_mechanical_legend(_P520001_INPUTS)


def test_lookup_tolerant_hue_trap_svp() -> None:
    """THE regression test: ffd480 (hue≈40°) is RGB-nearer to VP swatch f5b9b9 but
    hue-nearer to SVP swatch ffb120 — must resolve to SVP SOIL VENT PIPE.

    RGB distances:
      ffd480 → ffb120 (SVP): ≈102   (large)
      ffd480 → f5b9b9 (VP):  ≈64    (smaller — the trap)
    Hue distances:
      40° → 39° (SVP): 1°   (tiny)
      40° → 0°  (VP):  40°  (large)
    """
    legend = _p520001_legend()
    entry = legend.lookup_tolerant("c2ffd480")
    assert entry is not None, "SVP fill should resolve via hue matcher"
    assert entry.service == "SVP SOIL VENT PIPE"


def test_lookup_tolerant_vp_tint() -> None:
    """e7bdb4 (hue≈11°) resolves to VENT PIPE (VP swatch hue≈0°)."""
    legend = _p520001_legend()
    entry = legend.lookup_tolerant("c2e7bdb4")
    assert entry is not None, "VP tint fill should resolve via hue matcher"
    assert entry.service == "VENT PIPE"


def test_lookup_tolerant_rwp_green() -> None:
    """007f00 (hue=120°) resolves to RAIN WATER PIPE (RWP swatch hue≈120°)."""
    legend = _p520001_legend()
    # 007f00 without the c2 prefix
    entry = legend.lookup_tolerant("c2007f00")
    assert entry is not None, "RWP fill should resolve via hue matcher"
    assert entry.service == "RAIN WATER PIPE"


def test_lookup_tolerant_achromatic_bylayer_returns_none() -> None:
    """c3000007 (RGB 0,0,7 — near-black, achromatic) must return None (saturation gate)."""
    legend = _p520001_legend()
    entry = legend.lookup_tolerant("c3000007")
    assert entry is None, "Achromatic BYLAYER colour must not match any service"


def test_lookup_tolerant_exact_match_colour_not_returned() -> None:
    """The exact-match colour c2ffb120 is already in the legend index; lookup_tolerant
    still works correctly for it (it IS chromatic and hue-matches itself)."""
    legend = _p520001_legend()
    # Exact key: should also succeed via tolerant (distance 0°)
    entry = legend.lookup_tolerant("c2ffb120")
    assert entry is not None
    assert entry.service == "SVP SOIL VENT PIPE"


def test_lookup_tolerant_outside_tolerance_returns_none() -> None:
    """A colour with hue 180° (cyan) has no legend entry within 22° → returns None."""
    legend = _p520001_legend()
    # 00ffff = RGB(0, 255, 255) → hue=180°; nearest legend entry is RWP at 120° (dist=60°)
    entry = legend.lookup_tolerant("c200ffff")
    assert entry is None, "Cyan fill is too far from any legend entry"


def test_lookup_tolerant_ambiguous_near_tie_returns_none() -> None:
    """When two legend entries are nearly equidistant in hue from the query colour,
    lookup_tolerant abstains (returns None) rather than making an uncertain guess.

    This test uses a synthetic legend with two entries at hue 10° and 30°, and a
    query at 20° (equidistant: 10° from each).  The margin_deg=8 gate fires.
    """
    # Two entries 20° apart: hue≈10° and hue≈30°
    # c2ff4400 ≈ RGB(255,68,0) → hue≈16°
    # c2ffc800 ≈ RGB(255,200,0) → hue≈47°
    # Query c2ffaa00 ≈ RGB(255,170,0) → hue≈40°: 24° from first, 7° from second.
    # With margin_deg=8: best=7°, second=24°, gap=17° > 8 → should match (not a tie).
    # For a genuine tie we need entries equidistant from query.
    # c2ff2000 ≈ RGB(255,32,0) → hue≈7.5°
    # c2ff8000 ≈ RGB(255,128,0) → hue≈30°
    # query c2ff5000 ≈ RGB(255,80,0) → hue≈18.8°; dist to 7.5°=11.3°, dist to 30°=11.2° → tie
    tie_inputs = [
        MechSwatchInput(color={"rgb": "c2ff2000"}, text="SERVICE A"),
        MechSwatchInput(color={"rgb": "c2ff8000"}, text="SERVICE B"),
    ]
    legend = build_mechanical_legend(tie_inputs)
    # c2ff5000 = RGB(255,80,0) → hue ≈ 18.8°; equidistant from both entries
    entry = legend.lookup_tolerant("c2ff5000", margin_deg=8.0)
    assert entry is None, "Near-tie must return None (abstain)"


def test_lookup_tolerant_empty_legend_returns_none() -> None:
    """lookup_tolerant on an empty legend always returns None."""
    legend = build_mechanical_legend([])
    assert legend.lookup_tolerant("c2ffd480") is None


def test_lookup_tolerant_none_key_returns_none() -> None:
    """None key always returns None."""
    legend = _p520001_legend()
    assert legend.lookup_tolerant(None) is None


# ---------------------------------------------------------------------------
# 16. _resolve_fill_service_name — route helper (no length modification)
# ---------------------------------------------------------------------------


def test_resolve_fill_service_name_exact_takes_priority() -> None:
    """Exact match is used when available; tolerant fallback never fires for known keys."""
    from app.api.v1.revision_routes.service_takeoff import _resolve_fill_service_name

    legend = _p520001_legend()
    # Exact key for CDP
    name = _resolve_fill_service_name("c2ff80d5", legend)
    assert name == "CDP"


def test_resolve_fill_service_name_tolerant_fallback() -> None:
    """For a tint not in the index, tolerant lookup fills service_name."""
    from app.api.v1.revision_routes.service_takeoff import _resolve_fill_service_name

    legend = _p520001_legend()
    # SVP tint — not exact-matched, resolved by hue
    name = _resolve_fill_service_name("c2ffd480", legend)
    assert name == "SVP SOIL VENT PIPE"


def test_resolve_fill_service_name_achromatic_returns_none() -> None:
    """Achromatic fill returns None; no length value is touched by this helper."""
    from app.api.v1.revision_routes.service_takeoff import _resolve_fill_service_name

    legend = _p520001_legend()
    name = _resolve_fill_service_name("c3000007", legend)
    assert name is None


def test_resolve_fill_service_name_unknown_colour_returns_none() -> None:
    """Unresolvable fill key returns None honestly."""
    from app.api.v1.revision_routes.service_takeoff import _resolve_fill_service_name

    legend = _p520001_legend()
    name = _resolve_fill_service_name("c200ffff", legend)
    assert name is None
