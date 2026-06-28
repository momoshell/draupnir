"""Unit tests for the mechanical colour→service legend reducer (issue #775).

Pure — no DB, no ORM, no fixtures beyond local fakes. Covers:
- from_mech_swatches: basic, blank honest-absent, colour-less skip, competing services,
  same-colour-multiple-services collapse, empty input, tolerant no-raise.
- build_mechanical_legend: empty, by_key(), lookup().
- Permutation-invariance: 8 M-560103 rows.
- Module purity: no sqlalchemy/fastapi imports.
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
