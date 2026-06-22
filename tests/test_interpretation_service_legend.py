"""Unit tests for the per-revision service legend reducer (be-01 / issue #606 P1).

Pure -- no DB, no ORM, no fixtures beyond local fakes. Covers:
- colour_key helper (rgb preference, index fallback, None when both absent).
- from_colour_swatches extractor (basic, blank skip, colour-less skip, conflict path).
- from_abbreviation_prose extractor (DENOTES, equals, garbage tolerance).
- fuse() (empty, colour groups, abbreviation groups, permutation-invariance).
- by_colour() / by_abbreviation() accessors.
- M-540003 fixture shapes.
- Permutation-invariance: contested colour + discipline inputs.
"""

from __future__ import annotations

import itertools

import pytest

from app.interpretation.service_legend import (
    SERVICE_SOURCE_PROSE,
    SERVICE_SOURCE_SWATCH,
    ProseInput,
    ServiceEntry,
    ServiceLegend,
    SwatchInput,
    colour_key,
    from_abbreviation_prose,
    from_colour_swatches,
    fuse,
)

# ---------------------------------------------------------------------------
# colour_key helper
# ---------------------------------------------------------------------------


def test_colour_key_rgb_preferred() -> None:
    """RGB hex string is returned lowercased when present."""
    assert colour_key({"index": 150, "rgb": "C22D71", "by_layer": False}) == "c22d71"


def test_colour_key_rgb_already_lowercase() -> None:
    assert colour_key({"rgb": "c22d71"}) == "c22d71"


def test_colour_key_index_fallback() -> None:
    """When rgb is absent, index is used as 'idx:<n>'."""
    assert colour_key({"index": 150, "by_layer": False}) == "idx:150"


def test_colour_key_none_input() -> None:
    assert colour_key(None) is None


def test_colour_key_both_absent() -> None:
    assert colour_key({}) is None
    assert colour_key({"by_layer": True}) is None


def test_colour_key_explicit_idx150_and_bylayer_resolved_to_150_same_key() -> None:
    """Explicit index=150 and a BYLAYER style the adapter resolved to index=150 collapse to
    the same key -- the adapter pre-resolves; we read final resolved fields."""
    explicit = colour_key({"index": 150, "rgb": None, "by_layer": False})
    bylayer_resolved = colour_key({"index": 150, "rgb": None, "by_layer": True})
    assert explicit == bylayer_resolved == "idx:150"


def test_colour_key_none_when_index_and_rgb_none() -> None:
    assert colour_key({"index": None, "rgb": None}) is None


# ---------------------------------------------------------------------------
# from_colour_swatches
# ---------------------------------------------------------------------------


def test_swatch_basic() -> None:
    """Single swatch entry produces one ServiceEntry with correct fields."""
    inp = SwatchInput(
        color={"index": 150, "rgb": "c22d71", "by_layer": False},
        text="HYDRAULIC EQUIPMENT",
    )
    entries = from_colour_swatches([inp])
    assert len(entries) == 1
    e = entries[0]
    assert e.colour_key == "c22d71"
    assert e.colour_index == 150
    assert e.colour_rgb == "c22d71"
    assert e.discipline == "HYDRAULIC EQUIPMENT"
    assert e.abbreviation is None
    assert e.sources == (SERVICE_SOURCE_SWATCH,)
    assert e.confidence is None
    assert e.competing_disciplines == ()


def test_swatch_m540003_fixture() -> None:
    """M-540003 fixture: by_colour()[key].discipline == 'HYDRAULIC EQUIPMENT'."""
    inp = SwatchInput(
        color={"index": 150, "rgb": "c22d71", "by_layer": False},
        text="HYDRAULIC EQUIPMENT",
    )
    legend = fuse(from_colour_swatches([inp]))
    by_col = legend.by_colour()
    assert "c22d71" in by_col
    e = by_col["c22d71"]
    assert e.discipline == "HYDRAULIC EQUIPMENT"
    assert e.sources == (SERVICE_SOURCE_SWATCH,)


def test_swatch_blank_text_skipped() -> None:
    """Blank or whitespace-only text is skipped."""
    entries = from_colour_swatches(
        [
            SwatchInput(color={"index": 5, "rgb": "aabbcc"}, text=""),
            SwatchInput(color={"index": 6, "rgb": "ddeeff"}, text="   "),
        ]
    )
    assert entries == []


def test_swatch_colour_less_skipped() -> None:
    """Entries with no resolvable colour are skipped."""
    entries = from_colour_swatches(
        [
            SwatchInput(color={}, text="SOME SERVICE"),
            SwatchInput(color={"by_layer": True}, text="OTHER"),
        ]
    )
    assert entries == []


def test_swatch_conflict_same_colour_two_disciplines() -> None:
    """Two distinct disciplines for the same colour -> ONE entry, competing_disciplines populated."""  # noqa: E501
    inp1 = SwatchInput(color={"index": 10, "rgb": "ff0000"}, text="FIRE SUPPRESSION")
    inp2 = SwatchInput(color={"index": 10, "rgb": "ff0000"}, text="FIRE ALARM")
    entries = from_colour_swatches([inp1, inp2])
    assert len(entries) == 1
    e = entries[0]
    assert e.colour_key == "ff0000"
    assert e.confidence is None
    all_discs = {e.discipline} | set(e.competing_disciplines)
    assert all_discs == {"FIRE SUPPRESSION", "FIRE ALARM"}
    # Primary is sorted-first alphabetically.
    assert e.discipline == "FIRE ALARM"
    assert e.competing_disciplines == ("FIRE SUPPRESSION",)


def test_swatch_empty_input() -> None:
    assert from_colour_swatches([]) == []


def test_swatch_underline_directive_stripped() -> None:
    r"""A leading \L underline directive is stripped from text."""
    inp = SwatchInput(color={"rgb": "aabbcc"}, text=r"\LGAS SUPPLY")
    entries = from_colour_swatches([inp])
    assert len(entries) == 1
    assert entries[0].discipline == "GAS SUPPLY"


def test_swatch_whitespace_normalized() -> None:
    inp = SwatchInput(color={"rgb": "123456"}, text="  HOT  WATER   HEATING  ")
    entries = from_colour_swatches([inp])
    assert entries[0].discipline == "HOT WATER HEATING"


def test_swatch_never_raises_on_garbage() -> None:
    """Garbage color mappings must not raise."""
    entries = from_colour_swatches(
        [
            SwatchInput(color={"rgb": None, "index": None}, text="X"),
            SwatchInput(color={}, text="Y"),
        ]
    )
    # Neither has a resolvable colour -- skipped.
    assert entries == []


def test_swatch_distinct_colours() -> None:
    """Three distinct colours produce three entries."""
    inputs = [
        SwatchInput(color={"rgb": "ff0000"}, text="FIRE"),
        SwatchInput(color={"rgb": "00ff00"}, text="MECHANICAL"),
        SwatchInput(color={"rgb": "0000ff"}, text="ELECTRICAL"),
    ]
    entries = from_colour_swatches(inputs)
    assert len(entries) == 3
    keys = {e.colour_key for e in entries}
    assert keys == {"ff0000", "00ff00", "0000ff"}


# ---------------------------------------------------------------------------
# from_abbreviation_prose
# ---------------------------------------------------------------------------


def test_prose_denotes_basic() -> None:
    """GFR DENOTES GAS FIRED RADIANT HEATERS -> abbreviation + description."""
    entries = from_abbreviation_prose([ProseInput("GFR DENOTES GAS FIRED RADIANT HEATERS")])
    assert len(entries) == 1
    e = entries[0]
    assert e.abbreviation == "GFR"
    assert e.description == "GAS FIRED RADIANT HEATERS"
    assert e.colour_key is None
    assert e.discipline is None
    assert e.sources == (SERVICE_SOURCE_PROSE,)
    assert e.confidence is None
    assert e.competing_disciplines == ()


def test_prose_m540003_fixture() -> None:
    """M-540003 fixture: prose 'GFR DENOTES GAS FIRED RADIANT HEATERS'."""
    entries = from_abbreviation_prose([ProseInput("GFR DENOTES GAS FIRED RADIANT HEATERS")])
    assert len(entries) == 1
    e = entries[0]
    assert e.abbreviation == "GFR"
    assert e.description == "GAS FIRED RADIANT HEATERS"


def test_prose_equals_basic() -> None:
    entries = from_abbreviation_prose([ProseInput("GP = GAS PROVING PANEL")])
    assert len(entries) == 1
    e = entries[0]
    assert e.abbreviation == "GP"
    assert e.description == "GAS PROVING PANEL"
    assert e.colour_key is None


def test_prose_empty_string() -> None:
    assert from_abbreviation_prose([ProseInput("")]) == []
    assert from_abbreviation_prose([ProseInput("   ")]) == []


def test_prose_bare_abbreviation_skipped() -> None:
    """A bare all-caps abbreviation token with no meaning is skipped."""
    assert from_abbreviation_prose([ProseInput("GFR")]) == []


def test_prose_garbage_never_raises() -> None:
    garbage = [
        ProseInput("!!!@@@###"),
        ProseInput("\x00\xff"),
        ProseInput("123 = ???"),
        ProseInput("= nothing"),
        ProseInput("DENOTES"),
        ProseInput("こんにちは"),
        ProseInput("🔥🔥🔥"),
    ]
    result = from_abbreviation_prose(garbage)
    # None should have discipline or colour invented.
    assert all(e.discipline is None for e in result)
    assert all(e.colour_key is None for e in result)


def test_prose_case_insensitive_denotes() -> None:
    entries = from_abbreviation_prose([ProseInput("GFR denotes GAS FIRED RADIANT HEATERS")])
    assert len(entries) == 1
    assert entries[0].abbreviation == "GFR"


def test_prose_underline_directive_stripped() -> None:
    r"""Leading \L is stripped before parsing."""
    entries = from_abbreviation_prose([ProseInput(r"\LGP = GAS PROVING PANEL")])
    assert len(entries) == 1
    assert entries[0].abbreviation == "GP"
    assert entries[0].description == "GAS PROVING PANEL"


def test_prose_never_invents_discipline_or_colour() -> None:
    """Prose extractor must never produce discipline or colour_key."""
    entries = from_abbreviation_prose(
        [
            ProseInput("GFR DENOTES GAS FIRED RADIANT HEATERS"),
            ProseInput("GP = GAS PROVING PANEL"),
        ]
    )
    assert all(e.discipline is None for e in entries)
    assert all(e.colour_key is None for e in entries)


# ---------------------------------------------------------------------------
# fuse
# ---------------------------------------------------------------------------


def test_fuse_empty() -> None:
    legend = fuse([])
    assert isinstance(legend, ServiceLegend)
    assert legend.entries == ()
    assert legend.by_colour() == {}
    assert legend.by_abbreviation() == {}


def test_fuse_all_empty_extractors() -> None:
    a = from_colour_swatches([])
    b = from_abbreviation_prose([])
    legend = fuse(a + b)
    assert legend.entries == ()


def test_fuse_swatch_only() -> None:
    inp = SwatchInput(color={"rgb": "c22d71"}, text="HYDRAULIC EQUIPMENT")
    legend = fuse(from_colour_swatches([inp]))
    assert len(legend.entries) == 1
    assert "c22d71" in legend.by_colour()
    assert legend.by_abbreviation() == {}


def test_fuse_prose_only() -> None:
    legend = fuse(from_abbreviation_prose([ProseInput("GFR DENOTES GAS FIRED RADIANT HEATERS")]))
    assert len(legend.entries) == 1
    assert "GFR" in legend.by_abbreviation()
    assert legend.by_colour() == {}


def test_fuse_no_colour_abbr_bridge() -> None:
    """Colour entries and abbreviation entries must NOT be bridged (equality-only contract)."""
    swatch = from_colour_swatches([SwatchInput(color={"rgb": "ff0000"}, text="FIRE")])
    prose = from_abbreviation_prose([ProseInput("GFR DENOTES GAS FIRED RADIANT HEATERS")])
    legend = fuse(swatch + prose)
    # Both must survive as separate entries.
    assert len(legend.entries) == 2
    assert "ff0000" in legend.by_colour()
    assert "GFR" in legend.by_abbreviation()


def test_fuse_deterministic_order() -> None:
    """Entries are sorted by (colour_key, discipline, abbreviation)."""
    inputs = [
        *from_colour_swatches(
            [
                SwatchInput(color={"rgb": "zzzzzz"}, text="ZONE"),
                SwatchInput(color={"rgb": "aaaaaa"}, text="AIR"),
                SwatchInput(color={"rgb": "mmmmmm"}, text="MECH"),
            ]
        )
    ]
    d1 = fuse(inputs)
    d2 = fuse(list(reversed(inputs)))
    assert [e.colour_key for e in d1.entries] == [e.colour_key for e in d2.entries]
    assert [e.colour_key for e in d1.entries] == ["aaaaaa", "mmmmmm", "zzzzzz"]


def test_fuse_sources_sorted_deduped() -> None:
    """sources on each entry are sorted and deduped."""
    inp = SwatchInput(color={"rgb": "c22d71"}, text="HYDRAULIC")
    legend = fuse(from_colour_swatches([inp]))
    e = legend.entries[0]
    assert e.sources == tuple(sorted(set(e.sources)))


def test_fuse_conflict_same_colour_competing_disciplines() -> None:
    """Two swatches with same colour and different disciplines fuse with conflict markers."""
    inp1 = SwatchInput(color={"rgb": "ff0000"}, text="FIRE ALARM")
    inp2 = SwatchInput(color={"rgb": "ff0000"}, text="FIRE SUPPRESSION")
    legend = fuse(from_colour_swatches([inp1, inp2]))
    assert len(legend.entries) == 1
    e = legend.by_colour()["ff0000"]
    assert e.confidence is None
    assert e.competing_disciplines != ()
    all_discs = {e.discipline} | set(e.competing_disciplines)
    assert all_discs == {"FIRE ALARM", "FIRE SUPPRESSION"}


def test_fuse_duplicate_prose_entries_collapse() -> None:
    """Duplicate prose entries passed to fuse() collapse to a single entry."""
    entry = ServiceEntry(
        colour_key=None,
        colour_index=None,
        colour_rgb=None,
        discipline=None,
        abbreviation="GFR",
        description="GAS FIRED RADIANT HEATERS",
        sources=(SERVICE_SOURCE_PROSE,),
        confidence=None,
        competing_disciplines=(),
    )
    legend = fuse([entry, entry, entry])
    assert len(legend.entries) == 1
    assert legend.entries[0].abbreviation == "GFR"


def test_fuse_duplicate_colour_entries_collapse() -> None:
    """Duplicate colour entries collapse."""
    entry = ServiceEntry(
        colour_key="c22d71",
        colour_index=150,
        colour_rgb="c22d71",
        discipline="HYDRAULIC EQUIPMENT",
        abbreviation=None,
        description=None,
        sources=(SERVICE_SOURCE_SWATCH,),
        confidence=None,
        competing_disciplines=(),
    )
    legend = fuse([entry, entry])
    assert len(legend.entries) == 1


# ---------------------------------------------------------------------------
# Accessors
# ---------------------------------------------------------------------------


def test_by_colour_omits_colour_less() -> None:
    legend = fuse(from_abbreviation_prose([ProseInput("GFR DENOTES GAS FIRED RADIANT HEATERS")]))
    assert legend.by_colour() == {}
    assert "GFR" in legend.by_abbreviation()


def test_by_abbreviation_omits_swatch_only() -> None:
    inp = SwatchInput(color={"rgb": "c22d71"}, text="HYDRAULIC EQUIPMENT")
    legend = fuse(from_colour_swatches([inp]))
    assert legend.by_abbreviation() == {}
    assert "c22d71" in legend.by_colour()


def test_accessors_consistent_with_entries() -> None:
    inputs = [
        *from_colour_swatches([SwatchInput(color={"rgb": "ff0000"}, text="FIRE")]),
        *from_abbreviation_prose([ProseInput("GFR DENOTES GAS FIRED RADIANT HEATERS")]),
    ]
    legend = fuse(inputs)
    all_entry_ids = {id(e) for e in legend.entries}
    for e in legend.by_colour().values():
        assert id(e) in all_entry_ids
    for e in legend.by_abbreviation().values():
        assert id(e) in all_entry_ids


# ---------------------------------------------------------------------------
# Permutation-invariance
# ---------------------------------------------------------------------------


def test_fuse_permutation_invariance_contested_colour() -> None:
    """fuse() must produce identical ServiceLegend regardless of input permutation.

    Contested set: three swatches where two share the same colour but have distinct
    disciplines. All permutations must yield the same frozen result.
    """
    inp_a = SwatchInput(color={"rgb": "ff0000"}, text="FIRE ALARM")
    inp_b = SwatchInput(color={"rgb": "ff0000"}, text="FIRE SUPPRESSION")
    inp_c = SwatchInput(color={"rgb": "00ff00"}, text="MECHANICAL")

    base_entries = from_colour_swatches([inp_a, inp_b, inp_c])
    reference: ServiceLegend | None = None

    for perm in itertools.permutations(base_entries):
        result = fuse(list(perm))
        if reference is None:
            reference = result
        else:
            assert result.entries == reference.entries, (
                f"Order-dependent result for permutation "
                f"{[e.colour_key for e in perm]}: "
                f"got {result.entries} vs reference {reference.entries}"
            )


def test_fuse_permutation_invariance_mixed_sources() -> None:
    """Permutation-invariance with both swatch and prose entries mixed."""
    swatch1 = ServiceEntry(
        colour_key="ff0000",
        colour_index=None,
        colour_rgb="ff0000",
        discipline="FIRE ALARM",
        abbreviation=None,
        description=None,
        sources=(SERVICE_SOURCE_SWATCH,),
        confidence=None,
        competing_disciplines=(),
    )
    swatch2 = ServiceEntry(
        colour_key="00ff00",
        colour_index=None,
        colour_rgb="00ff00",
        discipline="MECHANICAL",
        abbreviation=None,
        description=None,
        sources=(SERVICE_SOURCE_SWATCH,),
        confidence=None,
        competing_disciplines=(),
    )
    prose1 = ServiceEntry(
        colour_key=None,
        colour_index=None,
        colour_rgb=None,
        discipline=None,
        abbreviation="GFR",
        description="GAS FIRED RADIANT HEATERS",
        sources=(SERVICE_SOURCE_PROSE,),
        confidence=None,
        competing_disciplines=(),
    )
    prose2 = ServiceEntry(
        colour_key=None,
        colour_index=None,
        colour_rgb=None,
        discipline=None,
        abbreviation="GP",
        description="GAS PROVING PANEL",
        sources=(SERVICE_SOURCE_PROSE,),
        confidence=None,
        competing_disciplines=(),
    )

    base_inputs = [swatch1, swatch2, prose1, prose2]
    reference: ServiceLegend | None = None

    for perm in itertools.permutations(base_inputs):
        result = fuse(list(perm))
        if reference is None:
            reference = result
        else:
            assert result.entries == reference.entries, (
                "Order-dependent result detected across permutations"
            )


def test_fuse_permutation_same_colour_many_disciplines() -> None:
    """Three distinct disciplines on one colour: primary is sorted-first; competitors sorted."""
    inp_c = SwatchInput(color={"rgb": "abcdef"}, text="CHILLED WATER")
    inp_h = SwatchInput(color={"rgb": "abcdef"}, text="HOT WATER")
    inp_g = SwatchInput(color={"rgb": "abcdef"}, text="GAS")

    base_entries = from_colour_swatches([inp_c, inp_h, inp_g])
    reference: ServiceLegend | None = None

    for perm in itertools.permutations(base_entries):
        result = fuse(list(perm))
        assert len(result.entries) == 1
        e = result.entries[0]
        # Sorted-first alphabetically: CHILLED WATER < GAS < HOT WATER.
        assert e.discipline == "CHILLED WATER"
        assert e.competing_disciplines == ("GAS", "HOT WATER")
        if reference is None:
            reference = result
        else:
            assert result.entries == reference.entries


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_fuse_entries_are_in_entries_tuple() -> None:
    """All entries reachable via accessors are present in .entries."""
    inputs = [
        *from_colour_swatches([SwatchInput(color={"rgb": "ff0000"}, text="FIRE")]),
        *from_abbreviation_prose([ProseInput("GFR DENOTES GAS FIRED RADIANT HEATERS")]),
    ]
    legend = fuse(inputs)
    entry_ids = {id(e) for e in legend.entries}
    for e in legend.by_colour().values():
        assert id(e) in entry_ids
    for e in legend.by_abbreviation().values():
        assert id(e) in entry_ids


def test_colour_key_rgb_uppercased_normalized() -> None:
    """RGB in any case is lowercased in the key."""
    assert colour_key({"rgb": "FF0000"}) == "ff0000"
    assert colour_key({"rgb": "Ff0000"}) == "ff0000"


@pytest.mark.parametrize(
    ("text", "expected_abbr", "expected_desc"),
    [
        ("CV DENOTES CONTROL VALVE", "CV", "CONTROL VALVE"),
        ("NC-H DENOTES NORMALLY CLOSED HEAT", "NC-H", "NORMALLY CLOSED HEAT"),
        ("GP = GAS PROVING PANEL", "GP", "GAS PROVING PANEL"),
        ("GFR DENOTES GAS FIRED RADIANT HEATERS", "GFR", "GAS FIRED RADIANT HEATERS"),
    ],
)
def test_prose_parametrize(text: str, expected_abbr: str, expected_desc: str) -> None:
    entries = from_abbreviation_prose([ProseInput(text)])
    assert len(entries) == 1
    e = entries[0]
    assert e.abbreviation == expected_abbr
    assert e.description == expected_desc
    assert e.colour_key is None
    assert e.discipline is None
