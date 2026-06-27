"""Unit tests for the containment legend reducer (issue #754 / Phase 752a).

Pure — no DB, no ORM, no fixtures beyond local fakes. Covers:
- from_containment_swatches: basic, blank honest-absent, colour-less skip, conflict path,
  same-colour-different-pattern split, empty-string pattern key, tolerant no-raise.
- build_containment_legend: empty, by_key(), lookup().
- Permutation-invariance: 12 E-610003 rows.
- Module purity: no sqlalchemy/fastapi imports.
"""

from __future__ import annotations

import itertools
import sys

from app.interpretation.containment_legend import (
    CONTAINMENT_SOURCE_SWATCH,
    ContainmentLegend,
    ContainmentLegendEntry,
    ContainmentSwatchInput,
    build_containment_legend,
    from_containment_swatches,
)
from app.interpretation.service_legend import colour_key

# ---------------------------------------------------------------------------
# 1. test_single_swatch_basic
# ---------------------------------------------------------------------------


def test_single_swatch_basic() -> None:
    """Single swatch with real E-610003 values produces one entry with correct fields."""
    inp = ContainmentSwatchInput(
        color={"rgb": "c20000ff"},
        pattern_name="SOLID",
        text="LV/UPS/ESSENTIAL/LIFE SAFETY SUB-MAIN LADDER / HEAVY DUTY TRAY",
    )
    entries = from_containment_swatches([inp])
    assert len(entries) == 1
    e = entries[0]
    assert e.colour_key == "c20000ff"
    assert e.pattern_name == "SOLID"
    assert e.containment_type == "LV/UPS/ESSENTIAL/LIFE SAFETY SUB-MAIN LADDER / HEAVY DUTY TRAY"
    assert e.sources == (CONTAINMENT_SOURCE_SWATCH,)
    assert e.competing_types == ()


# ---------------------------------------------------------------------------
# 2. test_same_colour_different_pattern_split
# ---------------------------------------------------------------------------


def test_same_colour_different_pattern_split() -> None:
    """Two inputs with the same colour but different pattern_name → TWO distinct entries.

    This is the load-bearing #752 behaviour: the key axis is (colour_key, pattern_name),
    not colour alone.
    """
    inp1 = ContainmentSwatchInput(
        color={"index": 256, "rgb": "c3000007"},
        pattern_name="FP_1",
        text="CONTAINMENT TYPE ALPHA",
    )
    inp2 = ContainmentSwatchInput(
        color={"index": 256, "rgb": "c3000007"},
        pattern_name="FP_35",
        text="CONTAINMENT TYPE BETA",
    )
    entries = from_containment_swatches([inp1, inp2])
    assert len(entries) == 2

    keys = {(e.colour_key, e.pattern_name) for e in entries}
    assert ("c3000007", "FP_1") in keys
    assert ("c3000007", "FP_35") in keys

    by_key = {(e.colour_key, e.pattern_name): e for e in entries}
    assert by_key[("c3000007", "FP_1")].containment_type == "CONTAINMENT TYPE ALPHA"
    assert by_key[("c3000007", "FP_35")].containment_type == "CONTAINMENT TYPE BETA"


# ---------------------------------------------------------------------------
# 3. test_same_key_competing_types
# ---------------------------------------------------------------------------


def test_same_key_competing_types() -> None:
    """Two inputs with identical (colour, pattern) but different labels → ONE entry.

    Primary = alphabetically first type name; remainder in competing_types.
    """
    inp1 = ContainmentSwatchInput(
        color={"rgb": "aabbcc"},
        pattern_name="FP_10",
        text="DADO TRUNKING",
    )
    inp2 = ContainmentSwatchInput(
        color={"rgb": "aabbcc"},
        pattern_name="FP_10",
        text="SMALL POWER AND LIGHTING TRUNKING",
    )
    entries = from_containment_swatches([inp1, inp2])
    assert len(entries) == 1
    e = entries[0]
    assert e.containment_type == "DADO TRUNKING"
    assert e.competing_types == ("SMALL POWER AND LIGHTING TRUNKING",)


# ---------------------------------------------------------------------------
# 4. test_colourless_input_skipped
# ---------------------------------------------------------------------------


def test_colourless_input_skipped() -> None:
    """Inputs with color=None or color={} (no resolvable colour) are skipped."""
    inp_none = ContainmentSwatchInput(color=None, pattern_name="SOLID", text="SOME TRAY")
    inp_empty = ContainmentSwatchInput(color={}, pattern_name="SOLID", text="OTHER TRAY")
    entries = from_containment_swatches([inp_none, inp_empty])
    assert entries == []

    legend = build_containment_legend([inp_none, inp_empty])
    assert legend.by_key() == {}


# ---------------------------------------------------------------------------
# 5. test_blank_label_honest_absent
# ---------------------------------------------------------------------------


def test_blank_label_honest_absent() -> None:
    """Valid colour + pattern but blank text → entry present with containment_type=None."""
    inp = ContainmentSwatchInput(
        color={"rgb": "c20000ff"},
        pattern_name="SOLID",
        text="   ",
    )
    entries = from_containment_swatches([inp])
    assert len(entries) == 1
    e = entries[0]
    assert e.containment_type is None
    assert e.colour_key == "c20000ff"

    # Also present in by_key()
    legend = build_containment_legend([inp])
    bk = legend.by_key()
    assert ("c20000ff", "SOLID") in bk
    assert bk[("c20000ff", "SOLID")].containment_type is None

    # Contract-load-bearing for 752b: lookup distinguishes "swatch present, type unknown"
    # (returns the honest-absent entry) from "no such swatch" (returns None).
    hit = legend.lookup("c20000ff", "SOLID")
    assert hit is not None
    assert hit.containment_type is None
    assert legend.lookup("c20000ff", "FP_99") is None


# ---------------------------------------------------------------------------
# 6. test_lookup_unmapped_returns_none
# ---------------------------------------------------------------------------


def test_lookup_unmapped_returns_none() -> None:
    """lookup() on a key not in the legend returns None — never guesses."""
    legend = build_containment_legend([])
    assert legend.lookup("deadbeef", "FP_99") is None

    # Also test with a populated legend that doesn't contain the key.
    inp = ContainmentSwatchInput(color={"rgb": "c20000ff"}, pattern_name="SOLID", text="TRAY A")
    legend2 = build_containment_legend([inp])
    assert legend2.lookup("deadbeef", "FP_99") is None
    assert legend2.lookup("c20000ff", "FP_99") is None  # colour present but pattern missing
    assert legend2.lookup(None, "SOLID") is None  # None colour_key always None


# ---------------------------------------------------------------------------
# 7. test_lookup_hit
# ---------------------------------------------------------------------------


def test_lookup_hit() -> None:
    """lookup() returns the correct entry for a mapped (colour_key, pattern_name)."""
    ck_val = colour_key({"rgb": "c20000ff"})
    assert ck_val is not None

    inp = ContainmentSwatchInput(
        color={"rgb": "c20000ff"},
        pattern_name="SOLID",
        text="LV/UPS/ESSENTIAL/LIFE SAFETY SUB-MAIN LADDER / HEAVY DUTY TRAY",
    )
    legend = build_containment_legend([inp])
    result = legend.lookup(ck_val, "SOLID")
    assert result is not None
    expected_type = "LV/UPS/ESSENTIAL/LIFE SAFETY SUB-MAIN LADDER / HEAVY DUTY TRAY"
    assert result.containment_type == expected_type
    assert result.colour_key == ck_val


# ---------------------------------------------------------------------------
# 8. test_empty_input
# ---------------------------------------------------------------------------


def test_empty_input() -> None:
    """build_containment_legend([]) returns a ContainmentLegend with empty entries."""
    legend = build_containment_legend([])
    assert isinstance(legend, ContainmentLegend)
    assert legend.entries == ()
    assert legend.by_key() == {}


# ---------------------------------------------------------------------------
# 9. test_permutation_invariant (headline — 12 E-610003 rows)
# ---------------------------------------------------------------------------

# E-610003 evidence: 12 distinct (colour, pattern) combinations.
# ci=5/c20000ff SOLID (1)
# ci=13/c2b16363 SOLID (1)
# ci=11/c2ff8040 SOLID (1)
# ci=96/c2007f00 FP_1  (1)
# ci=256/c3000007 FP_1, FP_31, FP_32, FP_33, FP_35, FP_41, FP_42, SOLID (8)
_E610003_ROWS: list[tuple[dict[str, object], str, str]] = [
    ({"index": 5, "rgb": "c20000ff"}, "SOLID", "LV/UPS SUB-MAIN LADDER"),
    ({"index": 13, "rgb": "c2b16363"}, "SOLID", "DATA AND COMMS WIRE BASKET"),
    ({"index": 11, "rgb": "c2ff8040"}, "SOLID", "ELV CONTAINMENT TRAY"),
    ({"index": 96, "rgb": "c2007f00"}, "FP_1", "FIRE ALARM CONTAINMENT"),
    ({"index": 256, "rgb": "c3000007"}, "FP_1", "DADO TRUNKING"),
    ({"index": 256, "rgb": "c3000007"}, "FP_31", "SMALL POWER TRUNKING"),
    ({"index": 256, "rgb": "c3000007"}, "FP_32", "LIGHTING TRUNKING"),
    ({"index": 256, "rgb": "c3000007"}, "FP_33", "SECURITY TRUNKING"),
    ({"index": 256, "rgb": "c3000007"}, "FP_35", "HVAC CONTROL TRUNKING"),
    ({"index": 256, "rgb": "c3000007"}, "FP_41", "BMS TRUNKING"),
    ({"index": 256, "rgb": "c3000007"}, "FP_42", "NURSE CALL TRUNKING"),
    ({"index": 256, "rgb": "c3000007"}, "SOLID", "POWER TRUNKING"),
]


def _make_e610003_inputs() -> list[ContainmentSwatchInput]:
    return [
        ContainmentSwatchInput(color=color, pattern_name=pattern, text=label)
        for color, pattern, label in _E610003_ROWS
    ]


def test_permutation_invariant() -> None:
    """build_containment_legend produces identical entries for all tested permutations.

    Uses the 12 E-610003 (colour, pattern, label) rows; asserts identical entries tuple
    and exactly 12 entries in by_key() for each permutation tested.
    """
    base_inputs = _make_e610003_inputs()
    reference: tuple[ContainmentLegendEntry, ...] | None = None

    # 12! is too large; test a representative sample of permutations.
    # Use first 120 permutations (sufficient for invariance confidence).
    perm_iter = itertools.islice(itertools.permutations(base_inputs), 120)

    for perm in perm_iter:
        legend = build_containment_legend(list(perm))
        assert len(legend.by_key()) == 12, (
            f"Expected 12 entries in by_key(), got {len(legend.by_key())}"
        )
        if reference is None:
            reference = legend.entries
        else:
            assert legend.entries == reference, (
                "Order-dependent result detected across permutations"
            )

    # Pin the load-bearing split directly: the ci=256 / c3000007 colour-collision bucket
    # must remain 8 distinct (colour,pattern) families — not collapsed, not over-split.
    assert reference is not None
    c3000007_families = [e for e in reference if e.colour_key == "c3000007"]
    assert len(c3000007_families) == 8


# ---------------------------------------------------------------------------
# 10. test_tolerant_no_raise
# ---------------------------------------------------------------------------


class _BadMapping:
    """Fake that raises on .get() to simulate a malformed color mapping."""

    def get(self, key: object, default: object = None) -> object:  # noqa: ARG002
        raise RuntimeError("simulated .get() failure")

    def __contains__(self, item: object) -> bool:
        raise RuntimeError("simulated __contains__ failure")


def test_tolerant_no_raise() -> None:
    """Malformed inputs must not propagate exceptions — they are silently skipped."""
    malformed = [
        # color is a non-mapping type entirely
        ContainmentSwatchInput(color=None, pattern_name="SOLID", text="TRAY"),
        # A valid-looking input where pattern_name would cause issues if not handled
        ContainmentSwatchInput(color={"rgb": "c20000ff"}, pattern_name="SOLID", text="VALID"),
    ]
    # Should not raise; valid entry survives
    entries = from_containment_swatches(malformed)
    # The None-colour one is skipped; the valid one survives
    assert len(entries) == 1
    assert entries[0].containment_type == "VALID"

    # Truly malformed mapping (raises on .get)
    bad_inputs: list[ContainmentSwatchInput] = []
    try:
        bad_obj = _BadMapping()
        # We construct with type: ignore since the type doesn't match Mapping
        bad_inputs.append(
            ContainmentSwatchInput(
                color=bad_obj,  # type: ignore[arg-type]
                pattern_name="SOLID",
                text="TRAY",
            )
        )
    except Exception:
        pass  # construction itself might fail — that's fine

    if bad_inputs:
        result = from_containment_swatches(bad_inputs)
        # Either skipped (no entries) or an exception was caught internally
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# 11. test_module_is_pure
# ---------------------------------------------------------------------------


def test_module_is_pure() -> None:
    """containment_legend.py must not contain any DB/ORM/FastAPI import.

    Uses AST inspection of the module source — the same pattern as the cable_estimate
    purity test — so that conftest-loaded modules don't produce false positives.
    """
    import ast
    import pathlib

    mod = sys.modules.get("app.interpretation.containment_legend")
    assert mod is not None, "module not loaded — import it above"

    forbidden = {"sqlalchemy", "fastapi", "asyncpg"}
    src = pathlib.Path(mod.__file__).read_text()  # type: ignore[arg-type]
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                for f in forbidden:
                    assert not alias.name.startswith(f), (
                        f"forbidden import in containment_legend: {alias.name}"
                    )
        elif isinstance(node, ast.ImportFrom) and node.module:
            for f in forbidden:
                assert not node.module.startswith(f), (
                    f"forbidden from-import in containment_legend: {node.module}"
                )


# ---------------------------------------------------------------------------
# 12. test_pattern_name_empty_string_is_a_real_key
# ---------------------------------------------------------------------------


def test_pattern_name_empty_string_is_a_real_key() -> None:
    """pattern_name='' is a valid, distinct key component — keyed as (colour_key, '').

    An entry with pattern_name="" must appear in by_key() under the key ("c20000ff", "").
    """
    inp = ContainmentSwatchInput(
        color={"rgb": "c20000ff"},
        pattern_name="",
        text="SOME TRAY WITHOUT PATTERN",
    )
    legend = build_containment_legend([inp])
    bk = legend.by_key()
    assert ("c20000ff", "") in bk, "Empty pattern_name '' must be a real key in by_key()"
    e = bk[("c20000ff", "")]
    assert e.pattern_name == ""
    assert e.containment_type == "SOME TRAY WITHOUT PATTERN"

    # Verify it is distinct from a SOLID entry on the same colour
    inp2 = ContainmentSwatchInput(
        color={"rgb": "c20000ff"},
        pattern_name="SOLID",
        text="SOLID TRAY",
    )
    legend2 = build_containment_legend([inp, inp2])
    bk2 = legend2.by_key()
    assert ("c20000ff", "") in bk2
    assert ("c20000ff", "SOLID") in bk2
    assert len(bk2) == 2
