"""Unit tests for the per-revision legend dictionary builder (#550, Phase 1).

Pure — no DB, no ORM, no fixtures beyond local fakes. Covers:
- Each extractor in isolation (A/B/C).
- Authority rules (A/C > B for type_name; B enriches description).
- Conflict path (A vs C disagree → competing_type_names, confidence=None).
- Never-fail prose path (garbage input must not raise).
- Empty / degraded path (fuse([]) → empty LegendDictionary).
- Deterministic ordering.
- by_abbreviation() / by_symbol_family() accessors.
- Permutation-invariance: contested cases (a) same-abbr/distinct-families,
  (b) same-family/distinct-abbrs, (c) same-tier/distinct-type_names.
"""

from __future__ import annotations

import itertools

import pytest

from app.interpretation.legend_dictionary import (
    LEGEND_SOURCE_FAMILY,
    LEGEND_SOURCE_PROSE,
    LEGEND_SOURCE_TAG,
    FamilyInput,
    LegendDictionary,
    LegendEntry,
    ProseInput,
    TagInput,
    from_block_families,
    from_prose_schedule,
    from_tag_layers,
    fuse,
)

# ---------------------------------------------------------------------------
# Source A — from_block_families
# ---------------------------------------------------------------------------


def test_family_basic() -> None:
    entries = from_block_families([FamilyInput("Smoke Detector with Beacon")])
    assert len(entries) == 1
    e = entries[0]
    assert e.symbol_family == "Smoke Detector with Beacon"
    assert e.type_name == "Smoke Detector with Beacon"
    assert e.abbreviation is None
    assert e.sources == (LEGEND_SOURCE_FAMILY,)
    assert e.competing_type_names == ()


def test_family_symbol_family_keeps_raw_name() -> None:
    """symbol_family MUST stay the raw block name — it is the lookup key matched against a
    placed instance's block_ref. Only type_name is humanized (#590)."""
    raw = (
        "Smoke Detector with Beaconrfa - Smoke Detector with Beaconrfa"
        "-2364563-03_ Ss_75_50_28_29-FIRE DETN _ ALM SYS_Legend Stage 4"
    )
    e = from_block_families([FamilyInput(raw)])[0]
    assert e.symbol_family == raw  # raw key preserved
    assert e.type_name == "Smoke Detector with Beaconrfa"  # cleaned for display


@pytest.mark.parametrize(
    ("raw", "expected_type"),
    [
        # Doubled "Family - Type" + GUID + Uniclass + stage marker → collapsed human name.
        (
            "Fire Alarm Sounder - Fire Alarm Sounder-2362494-03_ Ss_75_50_28_29"
            "-FIRE DETN _ ALM SYS_Legend Stage 4",
            "Fire Alarm Sounder",
        ),
        (
            "Fire Bicon annotation - Fire Bicon annotation-8466721-03_ Ss_..._Legend Stage 4",
            "Fire Bicon annotation",
        ),
        # Already-clean names are unchanged.
        ("Manual Call Point", "Manual Call Point"),
        # GUID tail with no doubling still strips.
        ("Heat Detector-998877-03_ Ss", "Heat Detector"),
    ],
)
def test_family_type_name_cleaned(raw: str, expected_type: str) -> None:
    e = from_block_families([FamilyInput(raw)])[0]
    assert e.symbol_family == raw
    assert e.type_name == expected_type


@pytest.mark.parametrize(
    "raw",
    [
        "Family - ___-8465483-03_ Ss_75_50_28_29-FIRE DETN _ ALM SYS_Legend Stage 4",
        "Family - ___",
        "Family",
        "____-12345",
    ],
)
def test_family_placeholder_type_name_is_none(raw: str) -> None:
    """Generic Revit placeholder families have NO human type → type_name None (kept as a
    symbol_family entry so the instance still classifies device, but stays UNRESOLVED)."""
    e = from_block_families([FamilyInput(raw)])[0]
    assert e.symbol_family == raw  # still a lookup key (instance still classifies as device)
    assert e.type_name is None  # honest unknown — never guessed


def test_family_deduplication() -> None:
    inputs = [FamilyInput("FAP"), FamilyInput("FAP"), FamilyInput("FAPR")]
    entries = from_block_families(inputs)
    assert len(entries) == 2
    families = {e.symbol_family for e in entries}
    assert families == {"FAP", "FAPR"}


def test_family_empty_input() -> None:
    assert from_block_families([]) == []


def test_family_blank_name_skipped() -> None:
    entries = from_block_families([FamilyInput("   ")])
    assert entries == []


# ---------------------------------------------------------------------------
# Source B — from_prose_schedule
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected_abbr", "expected_desc"),
    [
        ("CV DENOTES CONTROL VALVE", "CV", "CONTROL VALVE"),
        ("'CV' DENOTES CONTROL VALVE", None, None),  # quoted token — not a bare abbr match
        ("XX = SOME MEANING", "XX", "SOME MEANING"),
        ("FAP = Fire Alarm Panel", "FAP", "Fire Alarm Panel"),
        ("NC-H DENOTES NORMALLY CLOSED HEAT", "NC-H", "NORMALLY CLOSED HEAT"),
    ],
)
def test_prose_denotes_and_equals(
    text: str, expected_abbr: str | None, expected_desc: str | None
) -> None:
    entries = from_prose_schedule([ProseInput(text)])
    if expected_abbr is None:
        # Quoted token is not a valid abbreviation match — may produce a description-only entry.
        # We only assert it doesn't crash and produces no typed entry.
        assert all(e.type_name is None for e in entries)
        return
    assert len(entries) == 1
    e = entries[0]
    assert e.abbreviation == expected_abbr
    assert e.description == expected_desc
    assert e.type_name is None  # B never invents a type
    assert e.sources == (LEGEND_SOURCE_PROSE,)


def test_prose_bare_description() -> None:
    """A multi-word phrase with no abbreviation → description-only, no type invented."""
    entries = from_prose_schedule([ProseInput("SMOKE DETECTOR WITH BEACON")])
    assert len(entries) == 1
    e = entries[0]
    assert e.abbreviation is None
    assert e.type_name is None
    assert e.description == "SMOKE DETECTOR WITH BEACON"


def test_prose_bare_abbreviation_token_skipped() -> None:
    """A bare abbreviation token (no meaning) → skipped, not emitted as noise."""
    entries = from_prose_schedule([ProseInput("FAP")])
    assert entries == []


def test_prose_empty_string() -> None:
    assert from_prose_schedule([ProseInput("")]) == []
    assert from_prose_schedule([ProseInput("   ")]) == []


def test_prose_never_raises_on_garbage() -> None:
    """Garbage inputs must not raise under any circumstances."""
    garbage_inputs = [
        ProseInput("!!!@@@###"),
        ProseInput("\x00\xff"),
        ProseInput("123 = ???"),
        ProseInput("= nothing"),
        ProseInput("DENOTES"),
    ]
    # Must not raise.
    result = from_prose_schedule(garbage_inputs)
    # Entries that do come back must have type_name=None (B never invents types).
    assert all(e.type_name is None for e in result)


def test_prose_multiple_lines() -> None:
    inputs = [
        ProseInput("CV DENOTES CONTROL VALVE"),
        ProseInput("FAP = FIRE ALARM PANEL"),
        ProseInput("SMOKE DETECTOR WITH BEACON"),
    ]
    entries = from_prose_schedule(inputs)
    assert len(entries) == 3
    by_abbr = {e.abbreviation: e for e in entries if e.abbreviation}
    assert "CV" in by_abbr
    assert "FAP" in by_abbr
    assert by_abbr["CV"].description == "CONTROL VALVE"


# ---------------------------------------------------------------------------
# Source C — from_tag_layers
# ---------------------------------------------------------------------------


def test_tag_basic() -> None:
    entries = from_tag_layers([TagInput("H"), TagInput("IF"), TagInput("ACS")])
    tokens = {e.abbreviation for e in entries}
    assert tokens == {"H", "IF", "ACS"}
    assert all(e.sources == (LEGEND_SOURCE_TAG,) for e in entries)
    assert all(e.type_name == e.abbreviation for e in entries)


def test_tag_deduplication() -> None:
    entries = from_tag_layers([TagInput("FAP"), TagInput("FAPR"), TagInput("FAP")])
    assert len(entries) == 2
    tokens = {e.abbreviation for e in entries}
    assert tokens == {"FAP", "FAPR"}


def test_tag_non_abbreviation_skipped() -> None:
    """Tokens not matching the abbreviation pattern are skipped."""
    entries = from_tag_layers(
        [TagInput("lowercase"), TagInput("TOOLONG123"), TagInput("123"), TagInput("")]
    )
    assert entries == []


def test_tag_empty_input() -> None:
    assert from_tag_layers([]) == []


def test_tag_five_tokens() -> None:
    tokens = ["H", "IF", "ACS", "FAP", "FAPR"]
    entries = from_tag_layers([TagInput(t) for t in tokens])
    assert len(entries) == 5
    assert {e.abbreviation for e in entries} == set(tokens)


# ---------------------------------------------------------------------------
# fuse — authority + conflict
# ---------------------------------------------------------------------------


def test_fuse_empty() -> None:
    d = fuse([])
    assert isinstance(d, LegendDictionary)
    assert d.entries == ()
    assert d.by_abbreviation() == {}
    assert d.by_symbol_family() == {}


def test_fuse_all_empty_extractors() -> None:
    a = from_block_families([])
    b = from_prose_schedule([])
    c = from_tag_layers([])
    d = fuse(a + b + c)
    assert d.entries == ()


def test_fuse_family_only() -> None:
    a = from_block_families([FamilyInput("Smoke Detector")])
    d = fuse(a)
    assert len(d.entries) == 1
    e = d.entries[0]
    assert e.type_name == "Smoke Detector"
    assert e.symbol_family == "Smoke Detector"


def test_fuse_tag_only() -> None:
    c = from_tag_layers([TagInput("FAP")])
    d = fuse(c)
    assert len(d.entries) == 1
    e = d.entries[0]
    assert e.abbreviation == "FAP"
    assert e.type_name == "FAP"
    assert LEGEND_SOURCE_TAG in e.sources


def test_fuse_prose_enriches_description_from_tag() -> None:
    """Tag (C) provides type; prose (B) enriches description; type_name comes from C."""
    c = from_tag_layers([TagInput("FAP")])
    b = from_prose_schedule([ProseInput("FAP = FIRE ALARM PANEL")])
    d = fuse(c + b)
    by_abbr = d.by_abbreviation()
    assert "FAP" in by_abbr
    e = by_abbr["FAP"]
    assert e.type_name == "FAP"  # C authority
    assert e.description == "FIRE ALARM PANEL"  # B enrichment
    assert LEGEND_SOURCE_TAG in e.sources
    assert LEGEND_SOURCE_PROSE in e.sources
    assert e.competing_type_names == ()


def test_fuse_family_type_wins_over_tag_on_conflict() -> None:
    """When A and C disagree on type_name, A wins; C goes into competing_type_names."""
    # A says family_name is "Smoke Alarm"; C placed token is "SA" with type_name="SA".
    # If they share no abbreviation/family they don't naturally merge — we need a scenario
    # where the same entity is described by both. Use an abbreviation that matches the family.
    # Simplest: provide two entries that share the SAME abbreviation.
    entry_a = LegendEntry(
        abbreviation="SA",
        symbol_family="Smoke Alarm Family",
        type_name="Smoke Alarm Family",
        description=None,
        sources=(LEGEND_SOURCE_FAMILY,),
        confidence=None,
        competing_type_names=(),
    )
    entry_c = LegendEntry(
        abbreviation="SA",
        symbol_family=None,
        type_name="SA",
        description=None,
        sources=(LEGEND_SOURCE_TAG,),
        confidence=None,
        competing_type_names=(),
    )
    d = fuse([entry_a, entry_c])
    by_abbr = d.by_abbreviation()
    assert "SA" in by_abbr
    e = by_abbr["SA"]
    # A wins primary type_name (A before C in authority chain).
    assert e.type_name == "Smoke Alarm Family"
    # C's type appears in competitors.
    assert "SA" in e.competing_type_names
    # Both sources present.
    assert LEGEND_SOURCE_FAMILY in e.sources
    assert LEGEND_SOURCE_TAG in e.sources


def test_fuse_prose_does_not_produce_type_name() -> None:
    """B-only entries always have type_name=None even after fuse."""
    b = from_prose_schedule([ProseInput("CV DENOTES CONTROL VALVE")])
    d = fuse(b)
    by_abbr = d.by_abbreviation()
    assert "CV" in by_abbr
    assert by_abbr["CV"].type_name is None
    assert by_abbr["CV"].description == "CONTROL VALVE"


def test_fuse_deterministic_order() -> None:
    """Entries are sorted by (abbreviation, symbol_family) — deterministic across calls."""
    inputs = [
        *from_tag_layers([TagInput("ZZ"), TagInput("AA"), TagInput("MM")]),
    ]
    d1 = fuse(inputs)
    d2 = fuse(list(reversed(inputs)))
    assert [e.abbreviation for e in d1.entries] == [e.abbreviation for e in d2.entries]
    assert [e.abbreviation for e in d1.entries] == ["AA", "MM", "ZZ"]


def test_fuse_sources_sorted_deduped() -> None:
    c = from_tag_layers([TagInput("FAP")])
    b = from_prose_schedule([ProseInput("FAP = FIRE ALARM PANEL")])
    d = fuse(c + b)
    e = d.by_abbreviation()["FAP"]
    # Sources must be sorted and deduped.
    assert e.sources == tuple(sorted(set(e.sources)))
    assert LEGEND_SOURCE_PROSE in e.sources
    assert LEGEND_SOURCE_TAG in e.sources


def test_fuse_merges_family_and_prose_via_abbreviation() -> None:
    """Family entry + prose entry for same abbreviation → merged, prose enriches description."""
    # Craft a family entry that also has an abbreviation (as Phase 2 might produce).
    entry_a = LegendEntry(
        abbreviation="SD",
        symbol_family="Smoke Detector",
        type_name="Smoke Detector",
        description=None,
        sources=(LEGEND_SOURCE_FAMILY,),
        confidence=None,
        competing_type_names=(),
    )
    b = from_prose_schedule([ProseInput("SD DENOTES SMOKE DETECTOR")])
    d = fuse([entry_a, *b])
    by_abbr = d.by_abbreviation()
    assert "SD" in by_abbr
    e = by_abbr["SD"]
    assert e.type_name == "Smoke Detector"
    assert e.description == "SMOKE DETECTOR"
    assert e.symbol_family == "Smoke Detector"


# ---------------------------------------------------------------------------
# Accessors
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Source B — additional prose edge cases
# ---------------------------------------------------------------------------


def test_prose_mixed_case_denotes_accepted() -> None:
    """DENOTES match is case-insensitive; uppercase-coerced abbreviation must still be valid."""
    # "CV denotes CONTROL VALVE" — regex has re.IGNORECASE, group(1) is uppercased in code.
    entries = from_prose_schedule([ProseInput("CV denotes CONTROL VALVE")])
    assert len(entries) == 1
    e = entries[0]
    assert e.abbreviation == "CV"
    assert e.description == "CONTROL VALVE"
    assert e.type_name is None


def test_prose_lowercase_abbreviation_with_denotes_skipped() -> None:
    """A fully lowercase token ('cv denotes ...') — the uppercased 'CV' passes the abbr regex,
    so this is accepted. Confirm it doesn't crash and produces no type_name."""
    entries = from_prose_schedule([ProseInput("cv denotes CONTROL VALVE")])
    # Either matched (abbreviation='CV') or skipped; never raises; never has type_name.
    assert all(e.type_name is None for e in entries)


def test_prose_never_raises_on_unicode() -> None:
    """Unicode and emoji strings must not raise — B tolerates all input."""
    unicode_inputs = [
        ProseInput("こんにちは"),
        ProseInput("CV DENOTES café au lait"),
        ProseInput("🔥🔥🔥"),
        ProseInput("NC-H DENOTES Ñoño"),
        ProseInput("​​"),  # zero-width spaces
        ProseInput("AB\x00CD"),  # embedded null
    ]
    result = from_prose_schedule(unicode_inputs)
    assert all(e.type_name is None for e in result)


def test_prose_whitespace_only_variants_skipped() -> None:
    """All whitespace-only variants produce no entries."""
    for ws in ["\t", "\n", "\r\n", "  \t  "]:
        assert from_prose_schedule([ProseInput(ws)]) == [], f"Expected empty for {ws!r}"


def test_prose_quoted_abbreviation_produces_no_typed_entry() -> None:
    """'XX' DENOTES … — quoted form is not a valid bare abbreviation.

    Whatever the parser emits (description-only or nothing), type_name is always None.
    """
    entries = from_prose_schedule([ProseInput("'FAP' DENOTES FIRE ALARM PANEL")])
    assert all(e.type_name is None for e in entries)
    assert all(e.abbreviation != "FAP" for e in entries)


def test_prose_bare_description_only_phrase() -> None:
    """Multi-word mixed-case phrase yields description-only entry: no abbreviation, no type."""
    entries = from_prose_schedule([ProseInput("Heat Detector with Rate-of-Rise")])
    assert len(entries) == 1
    e = entries[0]
    assert e.abbreviation is None
    assert e.type_name is None
    assert e.description == "Heat Detector with Rate-of-Rise"
    assert e.sources == (LEGEND_SOURCE_PROSE,)


# ---------------------------------------------------------------------------
# fuse — authority rules (explicit)
# ---------------------------------------------------------------------------


def test_fuse_family_and_prose_a_wins_type_b_enriches_description() -> None:
    """A+B fused: A contributes type_name; B contributes description; no conflict."""
    # Prose uses a separate abbreviation — won't naturally merge with family-only A entry.
    # Craft entries with shared abbreviation so they merge.
    entry_a = LegendEntry(
        abbreviation="HD",
        symbol_family="Heat Detector",
        type_name="Heat Detector",
        description=None,
        sources=(LEGEND_SOURCE_FAMILY,),
        confidence=None,
        competing_type_names=(),
    )
    b = from_prose_schedule([ProseInput("HD DENOTES HEAT DETECTOR")])
    d = fuse([entry_a, *b])
    e = d.by_abbreviation()["HD"]
    assert e.type_name == "Heat Detector"  # A's type preserved
    assert e.description == "HEAT DETECTOR"  # B's description added
    assert e.symbol_family == "Heat Detector"
    assert LEGEND_SOURCE_FAMILY in e.sources
    assert LEGEND_SOURCE_PROSE in e.sources
    assert e.competing_type_names == ()  # no conflict


def test_fuse_b_only_abbreviation_type_name_stays_none() -> None:
    """Abbreviation present only in B (no A or C) → type_name must remain None; never fabricated."""
    b = from_prose_schedule([ProseInput("ZZ = ZONE DAMPER")])
    d = fuse(b)
    by_abbr = d.by_abbreviation()
    assert "ZZ" in by_abbr
    e = by_abbr["ZZ"]
    assert e.type_name is None  # B never invents a type, even when it is the sole source
    assert e.description == "ZONE DAMPER"
    assert e.sources == (LEGEND_SOURCE_PROSE,)


def test_fuse_conflict_confidence_is_none() -> None:
    """A vs C conflict: confidence must be None (the conflict marker)."""
    entry_a = LegendEntry(
        abbreviation="SD",
        symbol_family=None,
        type_name="Smoke Detector Family",
        description=None,
        sources=(LEGEND_SOURCE_FAMILY,),
        confidence=None,
        competing_type_names=(),
    )
    entry_c = LegendEntry(
        abbreviation="SD",
        symbol_family=None,
        type_name="SD",
        description=None,
        sources=(LEGEND_SOURCE_TAG,),
        confidence=None,
        competing_type_names=(),
    )
    d = fuse([entry_a, entry_c])
    e = d.by_abbreviation()["SD"]
    assert e.type_name == "Smoke Detector Family"  # A wins
    assert "SD" in e.competing_type_names  # C in competitors
    assert e.confidence is None  # explicit conflict marker
    assert e.competing_type_names != ()


def test_fuse_conflict_deterministic_primary() -> None:
    """Running fuse twice with reversed input order gives the same primary type_name."""
    entry_a = LegendEntry(
        abbreviation="CV",
        symbol_family=None,
        type_name="Control Valve Family",
        description=None,
        sources=(LEGEND_SOURCE_FAMILY,),
        confidence=None,
        competing_type_names=(),
    )
    entry_c = LegendEntry(
        abbreviation="CV",
        symbol_family=None,
        type_name="CV",
        description=None,
        sources=(LEGEND_SOURCE_TAG,),
        confidence=None,
        competing_type_names=(),
    )
    d1 = fuse([entry_a, entry_c])
    d2 = fuse([entry_c, entry_a])
    e1 = d1.by_abbreviation()["CV"]
    e2 = d2.by_abbreviation()["CV"]
    # A always wins regardless of input order — block_family source is authoritative.
    assert e1.type_name == e2.type_name == "Control Valve Family"
    assert set(e1.competing_type_names) == set(e2.competing_type_names)


# ---------------------------------------------------------------------------
# fuse — keying by abbreviation AND symbol_family
# ---------------------------------------------------------------------------


def test_fuse_entry_linkable_by_both_keys() -> None:
    """An entry with both abbreviation and symbol_family appears in both accessor dicts,
    and both return the identical object."""
    entry = LegendEntry(
        abbreviation="FAP",
        symbol_family="Fire Alarm Panel Family",
        type_name="Fire Alarm Panel Family",
        description=None,
        sources=(LEGEND_SOURCE_FAMILY,),
        confidence=None,
        competing_type_names=(),
    )
    d = fuse([entry])
    assert "FAP" in d.by_abbreviation()
    assert "Fire Alarm Panel Family" in d.by_symbol_family()
    # Both keys must resolve to the same entry object.
    assert d.by_abbreviation()["FAP"] is d.by_symbol_family()["Fire Alarm Panel Family"]


def test_fuse_dedup_repeated_tag_entries_through_fuse() -> None:
    """Duplicate entries passed directly to fuse() (after extractor-level dedup bypass)
    must still collapse to a single entry."""
    entry = LegendEntry(
        abbreviation="FAP",
        symbol_family=None,
        type_name="FAP",
        description=None,
        sources=(LEGEND_SOURCE_TAG,),
        confidence=None,
        competing_type_names=(),
    )
    d = fuse([entry, entry, entry])
    assert len(d.entries) == 1
    assert d.entries[0].abbreviation == "FAP"


def test_fuse_dedup_repeated_family_entries_through_fuse() -> None:
    """Duplicate family entries passed directly to fuse() collapse to one."""
    entry = LegendEntry(
        abbreviation=None,
        symbol_family="Smoke Detector",
        type_name="Smoke Detector",
        description=None,
        sources=(LEGEND_SOURCE_FAMILY,),
        confidence=None,
        competing_type_names=(),
    )
    d = fuse([entry, entry])
    assert len(d.entries) == 1
    assert d.entries[0].symbol_family == "Smoke Detector"


def test_fuse_merging_via_shared_symbol_family() -> None:
    """Two entries sharing symbol_family (but different abbreviation / none) merge into one."""
    entry_a = LegendEntry(
        abbreviation=None,
        symbol_family="Smoke Detector",
        type_name="Smoke Detector",
        description=None,
        sources=(LEGEND_SOURCE_FAMILY,),
        confidence=None,
        competing_type_names=(),
    )
    # A second entry that has the same symbol_family and an abbreviation — should merge.
    entry_b = LegendEntry(
        abbreviation="SD",
        symbol_family="Smoke Detector",
        type_name=None,
        description="SMOKE DETECTOR",
        sources=(LEGEND_SOURCE_PROSE,),
        confidence=None,
        competing_type_names=(),
    )
    d = fuse([entry_a, entry_b])
    assert len(d.entries) == 1
    e = d.entries[0]
    assert e.symbol_family == "Smoke Detector"
    assert e.abbreviation == "SD"
    assert e.type_name == "Smoke Detector"  # A wins
    assert e.description == "SMOKE DETECTOR"


def test_fuse_by_abbreviation_and_by_symbol_family_consistent_with_entries() -> None:
    """Every entry reachable via by_abbreviation() / by_symbol_family() is present in entries."""
    inputs = [
        *from_block_families([FamilyInput("Smoke Detector")]),
        *from_tag_layers([TagInput("SD"), TagInput("FAP")]),
        *from_prose_schedule([ProseInput("FAP = FIRE ALARM PANEL")]),
    ]
    d = fuse(inputs)
    all_entry_ids = {id(e) for e in d.entries}
    for e in d.by_abbreviation().values():
        assert id(e) in all_entry_ids
    for e in d.by_symbol_family().values():
        assert id(e) in all_entry_ids


def test_fuse_tag_whitespace_padded_token_stripped() -> None:
    """TagInput with surrounding whitespace is stripped before matching — valid token accepted."""
    entries = from_tag_layers([TagInput("  FAP  ")])
    assert len(entries) == 1
    assert entries[0].abbreviation == "FAP"


def test_by_abbreviation_omits_family_only() -> None:
    a = from_block_families([FamilyInput("Smoke Detector")])
    d = fuse(a)
    # Family-only entries have no abbreviation → omitted from by_abbreviation().
    assert d.by_abbreviation() == {}
    assert "Smoke Detector" in d.by_symbol_family()


def test_by_symbol_family_omits_tag_only() -> None:
    c = from_tag_layers([TagInput("FAP")])
    d = fuse(c)
    assert d.by_symbol_family() == {}
    assert "FAP" in d.by_abbreviation()


def test_accessors_are_consistent_with_entries() -> None:
    inputs = [
        *from_block_families([FamilyInput("Heat Detector")]),
        *from_tag_layers([TagInput("HD")]),
        *from_prose_schedule([ProseInput("HD DENOTES HEAT DETECTOR")]),
    ]
    d = fuse(inputs)
    for e in d.entries:
        if e.abbreviation is not None:
            assert d.by_abbreviation()[e.abbreviation] is e
        if e.symbol_family is not None:
            assert d.by_symbol_family()[e.symbol_family] is e


# ---------------------------------------------------------------------------
# Regression guards: permutation-invariance and transitive merge
# ---------------------------------------------------------------------------


def test_fuse_permutation_invariance() -> None:
    """fuse() must produce an identical dictionary regardless of input order.

    This is the regression guard for the order-dependent merge bug.  We build a
    contested multi-key set:
      - entry1: abbr=AA, family=FAM1, source A  (bridges AA ↔ FAM1)
      - entry2: family=FAM1, type=FAM1, source A (same family, no abbr)
      - entry3: abbr=AA, type=AA, source C       (tag for AA — conflicts with A on type)

    All three share either abbr=AA or family=FAM1 and must fuse into a SINGLE entry
    regardless of which permutation they arrive in.
    """
    entry1 = LegendEntry(
        abbreviation="AA",
        symbol_family="FAM1",
        type_name="FAM1",
        description=None,
        sources=(LEGEND_SOURCE_FAMILY,),
        confidence=None,
        competing_type_names=(),
    )
    entry2 = LegendEntry(
        abbreviation=None,
        symbol_family="FAM1",
        type_name="FAM1",
        description="Family entry",
        sources=(LEGEND_SOURCE_FAMILY,),
        confidence=None,
        competing_type_names=(),
    )
    entry3 = LegendEntry(
        abbreviation="AA",
        symbol_family=None,
        type_name="AA",
        description=None,
        sources=(LEGEND_SOURCE_TAG,),
        confidence=None,
        competing_type_names=(),
    )

    base_inputs = [entry1, entry2, entry3]
    reference: LegendDictionary | None = None

    for perm in itertools.permutations(base_inputs):
        result = fuse(list(perm))
        # All permutations must yield exactly ONE entry (all three share a key).
        assert len(result.entries) == 1, (
            f"Expected 1 entry, got {len(result.entries)} for permutation "
            f"{[e.abbreviation or e.symbol_family for e in perm]}"
        )
        if reference is None:
            reference = result
        else:
            assert result.entries == reference.entries, (
                f"Order-dependent result for permutation "
                f"{[e.abbreviation or e.symbol_family for e in perm]}: "
                f"got {result.entries} vs reference {reference.entries}"
            )


def test_fuse_transitive_merge() -> None:
    """Entries that share keys transitively (A↔B, B↔C) must all collapse into one entry.

    entry1: abbr=CV, family=FAM-CV   — bridges abbr CV to family FAM-CV
    entry2: family=FAM-CV, type=FAM-CV  — knows about FAM-CV (no abbr)
    entry3: abbr=CV, description="CONTROL VALVE"  — prose for abbr CV

    None of {entry2, entry3} shares both keys with the other directly; they only
    connect through entry1.  The fused result must be ONE entry with abbr=CV,
    symbol_family=FAM-CV, and description="CONTROL VALVE".
    """
    entry1 = LegendEntry(
        abbreviation="CV",
        symbol_family="FAM-CV",
        type_name="FAM-CV",
        description=None,
        sources=(LEGEND_SOURCE_FAMILY,),
        confidence=None,
        competing_type_names=(),
    )
    entry2 = LegendEntry(
        abbreviation=None,
        symbol_family="FAM-CV",
        type_name="FAM-CV",
        description=None,
        sources=(LEGEND_SOURCE_FAMILY,),
        confidence=None,
        competing_type_names=(),
    )
    entry3 = LegendEntry(
        abbreviation="CV",
        symbol_family=None,
        type_name=None,
        description="CONTROL VALVE",
        sources=(LEGEND_SOURCE_PROSE,),
        confidence=None,
        competing_type_names=(),
    )

    d = fuse([entry1, entry2, entry3])

    assert len(d.entries) == 1, f"Expected 1 entry after transitive merge, got {len(d.entries)}"
    e = d.entries[0]
    assert e.abbreviation == "CV"
    assert e.symbol_family == "FAM-CV"
    assert e.type_name == "FAM-CV"  # A wins
    assert e.description == "CONTROL VALVE"  # B enriches
    assert LEGEND_SOURCE_FAMILY in e.sources
    assert LEGEND_SOURCE_PROSE in e.sources


# ---------------------------------------------------------------------------
# Contested permutation tests (a/b/c) — the reviewer-named cases
# ---------------------------------------------------------------------------


def test_fuse_permutation_same_abbr_distinct_families() -> None:
    """(a) Two entries share the SAME abbreviation but have DISTINCT symbol_family values.

    These MUST NOT be merged — both families survive as separate entries.
    The result must be identical across all input permutations.

    This is the "SD → Smoke Detector Ceiling / Smoke Detector Duct" scenario.
    Old insertion-order logic would drop one family; the new conflict-aware logic keeps both.
    """
    entry_ceil = LegendEntry(
        abbreviation="SD",
        symbol_family="Smoke Detector Ceiling",
        type_name="Smoke Detector Ceiling",
        description=None,
        sources=(LEGEND_SOURCE_FAMILY,),
        confidence=None,
        competing_type_names=(),
    )
    entry_duct = LegendEntry(
        abbreviation="SD",
        symbol_family="Smoke Detector Duct",
        type_name="Smoke Detector Duct",
        description=None,
        sources=(LEGEND_SOURCE_FAMILY,),
        confidence=None,
        competing_type_names=(),
    )

    reference: LegendDictionary | None = None
    for perm in itertools.permutations([entry_ceil, entry_duct]):
        result = fuse(list(perm))

        # Both families must survive — neither dropped.
        families = {e.symbol_family for e in result.entries}
        assert "Smoke Detector Ceiling" in families, (
            f"Smoke Detector Ceiling dropped in permutation {[e.symbol_family for e in perm]}"
        )
        assert "Smoke Detector Duct" in families, (
            f"Smoke Detector Duct dropped in permutation {[e.symbol_family for e in perm]}"
        )

        # Order-independent.
        if reference is None:
            reference = result
        else:
            assert result.entries == reference.entries, (
                f"Order-dependent result: {result.entries} vs {reference.entries}"
            )


def test_fuse_permutation_same_family_distinct_abbrs() -> None:
    """(b) Two entries share the SAME symbol_family but have DISTINCT abbreviation values.

    Both abbreviations must survive — the first one encountered (by insertion order) must NOT
    silently win; the result must be identical across all input permutations.

    In practice a single family can only carry one abbreviation slot, so the second distinct
    abbreviation forms its own entry (no silent drop).
    """
    entry_long = LegendEntry(
        abbreviation="FAPR",
        symbol_family="Fire Alarm Panel",
        type_name="Fire Alarm Panel",
        description=None,
        sources=(LEGEND_SOURCE_FAMILY,),
        confidence=None,
        competing_type_names=(),
    )
    entry_short = LegendEntry(
        abbreviation="FAP",
        symbol_family="Fire Alarm Panel",
        type_name="Fire Alarm Panel",
        description=None,
        sources=(LEGEND_SOURCE_FAMILY,),
        confidence=None,
        competing_type_names=(),
    )

    reference: LegendDictionary | None = None
    for perm in itertools.permutations([entry_long, entry_short]):
        result = fuse(list(perm))

        # Both abbreviations must appear in the dictionary — neither silently dropped.
        abbrs = {e.abbreviation for e in result.entries if e.abbreviation is not None}
        assert "FAP" in abbrs, f"FAP dropped in permutation {[e.abbreviation for e in perm]}"
        assert "FAPR" in abbrs, f"FAPR dropped in permutation {[e.abbreviation for e in perm]}"

        # Order-independent.
        if reference is None:
            reference = result
        else:
            assert result.entries == reference.entries, (
                f"Order-dependent result: {result.entries} vs {reference.entries}"
            )


def test_fuse_family_only_plus_unrelated_tags_not_bridged() -> None:
    """Regression: family-only entry (abbr=None) must NOT be bridged onto unrelated tag entries.

    The extractor-realistic shape is [family-only F (abbr=None), tag X, tag Y] where X and Y
    have distinct abbreviations and no symbol_family.  The old open-acc heuristic would
    fabricate a family↔abbreviation link by picking whichever tag happened to be processed
    first when there was exactly one open (abbr=None) family accumulator — order-dependent
    and a false bridge against "legend is ground truth, never guess".

    After the fix (equality-only bridging):
      (i)  F keeps abbreviation=None and competing_type_names==() — no fabricated bridge.
      (ii) X and Y are SEPARATE entries keyed by their own abbreviations, not attached to F.
      (iii) the LegendDictionary is IDENTICAL across all input permutations.
    """
    family_f = LegendEntry(
        abbreviation=None,
        symbol_family="Smoke Detector Family",
        type_name="Smoke Detector Family",
        description=None,
        sources=(LEGEND_SOURCE_FAMILY,),
        confidence=None,
        competing_type_names=(),
    )
    tag_x = LegendEntry(
        abbreviation="SD",
        symbol_family=None,
        type_name="SD",
        description=None,
        sources=(LEGEND_SOURCE_TAG,),
        confidence=None,
        competing_type_names=(),
    )
    tag_y = LegendEntry(
        abbreviation="FAP",
        symbol_family=None,
        type_name="FAP",
        description=None,
        sources=(LEGEND_SOURCE_TAG,),
        confidence=None,
        competing_type_names=(),
    )

    base_inputs = [family_f, tag_x, tag_y]
    reference: LegendDictionary | None = None

    for perm in itertools.permutations(base_inputs):
        result = fuse(list(perm))

        # Must produce exactly three entries — no bridging collapses F with X or Y.
        assert len(result.entries) == 3, (
            f"Expected 3 entries, got {len(result.entries)} for permutation "
            f"{[e.abbreviation or e.symbol_family for e in perm]}"
        )

        by_abbr = result.by_abbreviation()
        by_fam = result.by_symbol_family()

        # (i) F keeps abbreviation=None and no competing_type_names.
        assert "Smoke Detector Family" in by_fam, "Family entry missing from by_symbol_family"
        f_entry = by_fam["Smoke Detector Family"]
        assert f_entry.abbreviation is None, (
            f"Family entry must keep abbreviation=None, got {f_entry.abbreviation!r} "
            f"in permutation {[e.abbreviation or e.symbol_family for e in perm]}"
        )
        assert f_entry.competing_type_names == (), (
            f"Family entry must have no competing_type_names, got "
            f"{f_entry.competing_type_names} in permutation "
            f"{[e.abbreviation or e.symbol_family for e in perm]}"
        )

        # (ii) X and Y are separate entries, not attached to F.
        assert "SD" in by_abbr, "Tag X (SD) must be a separate entry"
        assert "FAP" in by_abbr, "Tag Y (FAP) must be a separate entry"
        assert by_abbr["SD"].symbol_family is None, "SD must not be attached to any family"
        assert by_abbr["FAP"].symbol_family is None, "FAP must not be attached to any family"

        # (iii) Identical across all permutations.
        if reference is None:
            reference = result
        else:
            assert result.entries == reference.entries, (
                f"Order-dependent result for permutation "
                f"{[e.abbreviation or e.symbol_family for e in perm]}: "
                f"got {result.entries} vs reference {reference.entries}"
            )


def test_fuse_permutation_same_tier_distinct_type_names() -> None:
    """(c) Two same-tier (both family/A) distinct type_name candidates sharing a key.

    The primary type_name must be chosen by sorted order (deterministic), not insertion order.
    competing_type_names must contain the sorted remainder.
    Result must be identical across all input permutations.
    """
    # Two family entries sharing symbol_family "Multi-Family" but with different type_names
    # (e.g. from two blocks that map to the same family but disagree on canonical name).
    # We craft them with the same family key so they end up in the same group.
    entry_beta = LegendEntry(
        abbreviation=None,
        symbol_family="Multi-Family",
        type_name="Beta Type",
        description=None,
        sources=(LEGEND_SOURCE_FAMILY,),
        confidence=None,
        competing_type_names=(),
    )
    entry_alpha = LegendEntry(
        abbreviation=None,
        symbol_family="Multi-Family",
        type_name="Alpha Type",
        description=None,
        sources=(LEGEND_SOURCE_FAMILY,),
        confidence=None,
        competing_type_names=(),
    )

    reference: LegendDictionary | None = None
    for perm in itertools.permutations([entry_beta, entry_alpha]):
        result = fuse(list(perm))

        assert len(result.entries) == 1, (
            f"Expected 1 entry, got {len(result.entries)} for permutation "
            f"{[e.type_name for e in perm]}"
        )
        e = result.entries[0]

        # Sorted-order determinism: "Alpha Type" < "Beta Type" alphabetically → Alpha is primary.
        assert e.type_name == "Alpha Type", (
            f"Expected sorted primary 'Alpha Type', got '{e.type_name}' for permutation "
            f"{[entry.type_name for entry in perm]}"
        )
        assert e.competing_type_names == ("Beta Type",), (
            f"Expected ('Beta Type',) in competitors, got {e.competing_type_names}"
        )

        # Order-independent.
        if reference is None:
            reference = result
        else:
            assert result.entries == reference.entries, (
                f"Order-dependent result: {result.entries} vs {reference.entries}"
            )


def test_fuse_bare_description_permutation_invariance() -> None:
    """Bare-description prose entries (abbreviation=None, symbol_family=None) must come out
    in the same order regardless of input order.

    Without the full four-field sort key these entries all share sort key ("", "") and fall
    back to insertion order — violating the determinism contract.
    """
    entry_smoke = LegendEntry(
        abbreviation=None,
        symbol_family=None,
        type_name=None,
        description="SMOKE DETECTOR WITH BEACON",
        sources=(LEGEND_SOURCE_PROSE,),
        confidence=None,
        competing_type_names=(),
    )
    entry_mcp = LegendEntry(
        abbreviation=None,
        symbol_family=None,
        type_name=None,
        description="MANUAL CALL POINT",
        sources=(LEGEND_SOURCE_PROSE,),
        confidence=None,
        competing_type_names=(),
    )

    result_ab = fuse([entry_smoke, entry_mcp])
    result_ba = fuse([entry_mcp, entry_smoke])

    assert result_ab.entries == result_ba.entries, (
        f"Order-dependent result for bare-description entries: "
        f"{result_ab.entries} vs {result_ba.entries}"
    )
    # Sanity: both entries are present and ordered by description text ("MANUAL..." < "SMOKE...").
    assert len(result_ab.entries) == 2
    assert result_ab.entries[0].description == "MANUAL CALL POINT"
    assert result_ab.entries[1].description == "SMOKE DETECTOR WITH BEACON"
