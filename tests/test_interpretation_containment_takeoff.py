"""Synthetic unit tests for app.interpretation.containment_takeoff (issue #755, Phase 752b-2).

No DB, no ORM, no FastAPI.  All geometry in metres.
"""

from __future__ import annotations

import pytest

from app.interpretation.containment_legend import (
    ContainmentLegend,
    ContainmentLegendEntry,
)
from app.interpretation.containment_takeoff import (
    BASIS_LEGEND,
    BASIS_RUN_LABEL,
    BASIS_UNRESOLVED,
    ContainmentBand,
    _token_to_type,
    _type_to_token,
    compute_containment_attributed_lengths,
)
from app.interpretation.segment_label_takeoff import SegmentLabel

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TRAY_TYPE = "Cable Tray"
_LADDER_TYPE = "Cable Ladder"
_CONDUIT_TYPE = "Conduit"


def _square_ring(cx: float, cy: float, half: float) -> tuple[tuple[float, float], ...]:
    return (
        (cx - half, cy - half),
        (cx + half, cy - half),
        (cx + half, cy + half),
        (cx - half, cy + half),
    )


def _band(
    colour_key: str,
    cx: float,
    cy: float,
    half: float = 0.5,
    pattern_name: str = "SOLID",
    colour_index: int | None = None,
) -> ContainmentBand:
    return ContainmentBand(
        colour_key=colour_key,
        colour_index=colour_index,
        colour_rgb=None,
        ring=_square_ring(cx, cy, half),
        pattern_name=pattern_name,
    )


def _legend_from_pairs(
    pairs: list[tuple[str, str, str | None]],
) -> ContainmentLegend:
    """Build a ContainmentLegend from (colour_key, pattern_name, containment_type) triples.

    Uses ContainmentLegendEntry directly to bypass the swatch colour parsing.
    """
    entries = [
        ContainmentLegendEntry(
            colour_key=ck,
            colour_index=None,
            colour_rgb=None,
            pattern_name=pn,
            containment_type=ct,
            sources=("test",),
            competing_types=(),
        )
        for ck, pn, ct in pairs
    ]
    entries.sort(key=lambda e: (e.colour_key or "", e.pattern_name, e.containment_type or ""))
    return ContainmentLegend(entries=tuple(entries))


def _seg(
    x0: float, y0: float, x1: float, y1: float
) -> tuple[tuple[float, float], tuple[float, float]]:
    return ((x0, y0), (x1, y1))


# ---------------------------------------------------------------------------
# Test: two bands with SAME resolved type fold into ONE per_type entry
# ---------------------------------------------------------------------------


def test_two_bands_same_type_fold() -> None:
    """Two distinct (colour,pattern) bands resolving to the same type fold into one entry."""
    legend = _legend_from_pairs(
        [
            ("idx:1", "SOLID", _TRAY_TYPE),
            ("idx:2", "FP_1", _TRAY_TYPE),
        ]
    )

    bands = [
        _band("idx:1", 0.0, 0.0, pattern_name="SOLID"),  # segment at (0,0) is in this
        _band("idx:2", 5.0, 0.0, pattern_name="FP_1"),  # segment at (5,0) is in this
    ]

    segments = [_seg(-0.1, 0.0, 0.1, 0.0), _seg(4.9, 0.0, 5.1, 0.0)]

    result = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
    )

    # Both bands resolve to _TRAY_TYPE → must fold into ONE entry
    assert len(result.per_type) == 1
    entry = result.per_type[0]
    assert entry.containment_type == _TRAY_TYPE
    assert entry.length_m > 0.0
    # Both colour_keys and pattern_names appear
    assert "idx:1" in entry.member_colour_keys
    assert "idx:2" in entry.member_colour_keys
    assert "SOLID" in entry.member_pattern_names
    assert "FP_1" in entry.member_pattern_names


# ---------------------------------------------------------------------------
# Test: band with no legend entry → None bucket
# ---------------------------------------------------------------------------


def test_unmapped_key_goes_to_none_bucket() -> None:
    """A band whose (colour_key, pattern_name) is not in the legend → None per_type entry."""
    legend = _legend_from_pairs([])  # empty legend

    bands = [_band("idx:99", 0.0, 0.0, pattern_name="SOLID")]
    segments = [_seg(-0.1, 0.0, 0.1, 0.0)]

    result = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
    )

    types = [e.containment_type for e in result.per_type]
    assert None in types


# ---------------------------------------------------------------------------
# Test: band with entry whose containment_type is None (blank label) → None bucket
# ---------------------------------------------------------------------------


def test_blank_mapped_entry_goes_to_none_bucket() -> None:
    """A band with a legend entry that has containment_type=None → None per_type entry."""
    legend = _legend_from_pairs([("idx:1", "SOLID", None)])  # entry exists, type is absent

    bands = [_band("idx:1", 0.0, 0.0, pattern_name="SOLID")]
    segments = [_seg(-0.1, 0.0, 0.1, 0.0)]

    result = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
    )

    types = [e.containment_type for e in result.per_type]
    assert None in types


# ---------------------------------------------------------------------------
# Test: distinct types → separate per_type entries
# ---------------------------------------------------------------------------


def test_distinct_types_separate() -> None:
    """Two bands with different types produce two separate per_type entries."""
    legend = _legend_from_pairs(
        [
            ("idx:1", "SOLID", _TRAY_TYPE),
            ("idx:2", "SOLID", _CONDUIT_TYPE),
        ]
    )

    bands = [
        _band("idx:1", 0.0, 0.0, pattern_name="SOLID"),
        _band("idx:2", 5.0, 0.0, pattern_name="SOLID"),
    ]
    segments = [_seg(-0.1, 0.0, 0.1, 0.0), _seg(4.9, 0.0, 5.1, 0.0)]

    result = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
    )

    types = {e.containment_type for e in result.per_type}
    assert _TRAY_TYPE in types
    assert _CONDUIT_TYPE in types
    assert len(result.per_type) == 2


# ---------------------------------------------------------------------------
# Test: invariant Σ(per_type) + shared == total ±0.1
# ---------------------------------------------------------------------------


def test_invariant_sum_plus_shared_equals_total() -> None:
    """Mixed fixture: INVARIANT Σ(per_type lengths) + shared == total ±0.1 m."""
    legend = _legend_from_pairs(
        [
            ("idx:1", "SOLID", _TRAY_TYPE),
            ("idx:2", "SOLID", _CONDUIT_TYPE),
            ("idx:3", "FP_1", None),  # honest-absent
        ]
    )

    # Two labelled bands + one unlabelled band, spatially spread out.
    bands = [
        _band("idx:1", 0.0, 0.0, 0.5, "SOLID"),
        _band("idx:2", 5.0, 0.0, 0.5, "SOLID"),
        _band("idx:3", 10.0, 0.0, 0.5, "FP_1"),
    ]
    # One segment per band + one far-from-all-bands (→ shared)
    segments = [
        _seg(-0.1, 0.0, 0.1, 0.0),  # tray
        _seg(4.9, 0.0, 5.1, 0.0),  # conduit
        _seg(9.9, 0.0, 10.1, 0.0),  # honest-absent
        _seg(50.0, 0.0, 50.1, 0.0),  # far from all → shared
    ]

    result = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
    )

    total_from_types = sum(e.length_m for e in result.per_type)
    assert abs(total_from_types + result.shared_length_m - result.total_length_m) < 0.1
    # The far-from-all-bands segment must actually land in shared (else the invariant
    # would hold vacuously without exercising the shared bucket).
    assert result.shared_length_m > 0.0


# ---------------------------------------------------------------------------
# Test: None sorts LAST in per_type
# ---------------------------------------------------------------------------


def test_none_type_sorts_last() -> None:
    """ContainmentTypeLength with containment_type=None is the LAST entry in per_type."""
    legend = _legend_from_pairs(
        [
            ("idx:1", "SOLID", _TRAY_TYPE),
            ("idx:2", "SOLID", None),  # honest-absent
        ]
    )

    bands = [
        _band("idx:1", 0.0, 0.0, pattern_name="SOLID"),
        _band("idx:2", 5.0, 0.0, pattern_name="SOLID"),
    ]
    segments = [_seg(-0.1, 0.0, 0.1, 0.0), _seg(4.9, 0.0, 5.1, 0.0)]

    result = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
    )

    # There should be at least two entries; None must be the very last.
    assert len(result.per_type) >= 1
    assert result.per_type[-1].containment_type is None


# ---------------------------------------------------------------------------
# Test: empty segments → empty result, no raise
# ---------------------------------------------------------------------------


def test_empty_segments_no_raise() -> None:
    """Empty segment input returns empty ContainmentAttributionResult without raising."""
    legend = _legend_from_pairs([("idx:1", "SOLID", _TRAY_TYPE)])
    bands = [_band("idx:1", 0.0, 0.0)]

    result = compute_containment_attributed_lengths(
        centerline_segments=[],
        containment_bands=bands,
        legend=legend,
    )

    assert result.per_type == ()
    assert result.shared_length_m == 0.0
    assert result.total_length_m == 0.0
    assert result.centerline_segment_count == 0


# ---------------------------------------------------------------------------
# Test: token round-trip is lossless, incl. reserved-token & prefix-collision shapes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "containment_type",
    [
        "Cable Tray",  # ordinary
        "__shared__",  # equals an engine/reserved sentinel
        "__unmapped__",  # equals our honest-absent sentinel
        "__ct__weird",  # real type that merely STARTS with the disambig prefix
    ],
)
def test_token_round_trip_is_lossless(containment_type: str) -> None:
    """_type_to_token → _token_to_type recovers the original type for every shape.

    Guards the one-sided-strip hole: a real type starting with the disambig prefix but not
    equal to (prefix + reserved) must pass through verbatim, never silently de-prefixed.
    """
    assert _token_to_type(_type_to_token(containment_type)) == containment_type


def test_none_token_round_trip() -> None:
    """None (honest-absent) → unmapped sentinel → None."""
    assert _token_to_type(_type_to_token(None)) is None


# ---------------------------------------------------------------------------
# Test: shared vs unmapped are distinct buckets
# ---------------------------------------------------------------------------


def test_shared_vs_unmapped_are_distinct_buckets() -> None:
    """Segment far from all bands → shared_length_m; on-band-but-unmapped → None bucket."""
    # Legend maps idx:1/SOLID → None (honest-absent); idx:2 not in legend.
    legend = _legend_from_pairs([("idx:1", "SOLID", None)])

    # Band at (0,0) — unmapped (honest-absent); no band near (50,0).
    bands = [_band("idx:1", 0.0, 0.0, 0.5, "SOLID")]

    seg_on_unmapped = _seg(-0.1, 0.0, 0.1, 0.0)  # on the unmapped band → None bucket
    seg_far = _seg(50.0, 0.0, 50.1, 0.0)  # far from all bands → shared bucket

    result = compute_containment_attributed_lengths(
        centerline_segments=[seg_on_unmapped, seg_far],
        containment_bands=bands,
        legend=legend,
    )

    # Unmapped segment must appear in per_type as None
    none_entries = [e for e in result.per_type if e.containment_type is None]
    assert none_entries, "Expected a None-type entry for the on-band-but-unmapped segment"
    assert none_entries[0].length_m > 0.0

    # Far segment goes to shared
    assert result.shared_length_m > 0.0

    # Invariant
    total_from_types = sum(e.length_m for e in result.per_type)
    assert abs(total_from_types + result.shared_length_m - result.total_length_m) < 0.1


# ---------------------------------------------------------------------------
# 752b-3 assembler: AsyncMock-patch the three loaders
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_assemble_containment_takeoff_folding_and_invariant() -> None:
    """Assembler integration: patch loaders; assert type folding, None bucket, invariant."""
    from unittest.mock import AsyncMock, MagicMock, patch
    from uuid import UUID

    from app.interpretation.service_takeoff_loaders import assemble_containment_takeoff

    rev_id = UUID("00000000-0000-0000-0000-000000000001")
    db = MagicMock()  # not used — all three loaders are patched

    # Legend: 13 (colour,pattern) families covering ≥12 entries + one BYLAYER under two patterns.
    # "BYLAYER" simulates a colour_key from a BYLAYER hatch; still valid as a string key.
    legend_pairs: list[tuple[str, str, str | None]] = [
        ("idx:1", "SOLID", "Cable Tray"),
        ("idx:2", "FP_1", "Cable Tray"),  # same type as idx:1 → fold
        ("idx:3", "SOLID", "Cable Ladder"),
        ("idx:4", "SOLID", "Conduit"),
        ("idx:5", "SOLID", "Duct"),
        ("idx:6", "SOLID", "Trunking"),
        ("idx:7", "SOLID", "Basket"),
        ("idx:8", "SOLID", "Wireway"),
        ("idx:9", "FP_2", "Cable Tray"),  # 3rd band resolving to Cable Tray
        ("idx:10", "SOLID", "Power Conduit"),
        ("idx:11", "SOLID", "Fire Conduit"),
        ("idx:12", "SOLID", None),  # honest-absent
        ("BYLAYER", "SOLID", "HVAC Duct"),  # BYLAYER colour_key, one pattern
        ("BYLAYER", "FP_3", "HVAC Duct"),  # same type, different pattern → fold
    ]
    legend = _legend_from_pairs(legend_pairs)

    # Build a band per entry (centred on a grid 5 m apart); ring half=0.5 m.
    spacing = 5.0
    bands: list[ContainmentBand] = [
        ContainmentBand(
            colour_key=ck,
            colour_index=None,
            colour_rgb=None,
            ring=_square_ring(i * spacing, 0.0, 0.5),
            pattern_name=pn,
        )
        for i, (ck, pn, _ct) in enumerate(legend_pairs)
    ]

    # One segment per band (centred on each band's centre), plus one far segment → shared.
    segments: list[tuple[tuple[float, float], tuple[float, float]]] = [
        _seg(i * spacing - 0.1, 0.0, i * spacing + 0.1, 0.0) for i in range(len(legend_pairs))
    ]
    segments.append(_seg(999.0, 999.0, 999.1, 999.0))  # far → shared

    with (
        patch(
            "app.interpretation.service_takeoff_loaders.build_containment_legend_db",
            new=AsyncMock(return_value=legend),
        ),
        patch(
            "app.interpretation.service_takeoff_loaders.load_containment_bands",
            new=AsyncMock(return_value=bands),
        ),
        patch(
            "app.interpretation.service_takeoff_loaders.load_containment_centerline_segments",
            new=AsyncMock(return_value=segments),
        ),
        patch(
            "app.interpretation.service_takeoff_loaders.load_tag_placements",
            new=AsyncMock(return_value=[]),
        ),
    ):
        result = await assemble_containment_takeoff(db=db, revision_id=rev_id)

    # Cable Tray has 3 contributing bands (idx:1/SOLID, idx:2/FP_1, idx:9/FP_2) → one entry
    tray_entries = [e for e in result.per_type if e.containment_type == "Cable Tray"]
    assert len(tray_entries) == 1, "Cable Tray bands must fold into one entry"
    tray = tray_entries[0]
    assert "idx:1" in tray.member_colour_keys
    assert "idx:2" in tray.member_colour_keys
    assert "idx:9" in tray.member_colour_keys

    # HVAC Duct has 2 bands (BYLAYER/SOLID and BYLAYER/FP_3) → one entry
    hvac_entries = [e for e in result.per_type if e.containment_type == "HVAC Duct"]
    assert len(hvac_entries) == 1
    hvac = hvac_entries[0]
    assert "SOLID" in hvac.member_pattern_names
    assert "FP_3" in hvac.member_pattern_names

    # Honest-absent band (idx:12) → None bucket present
    none_entries = [e for e in result.per_type if e.containment_type is None]
    assert none_entries, "Expected a None-type entry for the honest-absent band"

    # None sorts last
    assert result.per_type[-1].containment_type is None

    # INVARIANT
    total_from_types = sum(e.length_m for e in result.per_type)
    assert abs(total_from_types + result.shared_length_m - result.total_length_m) < 0.1


# ---------------------------------------------------------------------------
# Tests: label pass — run-label recovery over unmapped segments (issue #761)
# ---------------------------------------------------------------------------


def _label(x: float, y: float, service: str) -> SegmentLabel:
    return SegmentLabel(point=(x, y), service=service, size_raw="100", size_kind="rect")


def test_label_precedence_legend_type_kept() -> None:
    """A segment resolving to a real legend type keeps it; label only fills unmapped segments."""
    legend = _legend_from_pairs([("idx:1", "SOLID", _TRAY_TYPE)])
    bands = [_band("idx:1", 0.0, 0.0, pattern_name="SOLID")]
    # Segment inside the band → attributed to Cable Tray.
    segments = [_seg(-0.1, 0.0, 0.1, 0.0)]
    # Place a label near the segment that would assign a DIFFERENT service.
    labels = [_label(0.0, 0.0, "SOME OTHER SERVICE")]

    result = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
        labels=labels,
    )
    # The legend attribution wins; no run_label entries in per_type (nothing was unmapped).
    tray_entries = [e for e in result.per_type if e.containment_type == _TRAY_TYPE]
    assert tray_entries, "Legend attribution must be kept for mapped segments"
    assert tray_entries[0].basis == BASIS_LEGEND
    run_label_entries = [e for e in result.per_type if e.basis == BASIS_RUN_LABEL]
    assert run_label_entries == []


def test_label_exact_partition_conservation() -> None:
    """Exact partition: Σ(real per_type) + Σ(label_attributed) + label_unknown + shared == total."""
    # One mapped band (tray), one unmapped band, one far segment (→ shared).
    legend = _legend_from_pairs([("idx:1", "SOLID", _TRAY_TYPE)])
    bands = [
        _band("idx:1", 0.0, 0.0, pattern_name="SOLID"),  # mapped
        _band("idx:2", 5.0, 0.0, pattern_name="FP_N"),  # unmapped (not in legend)
    ]
    segments = [
        _seg(-0.1, 0.0, 0.1, 0.0),  # → Cable Tray (legend)
        _seg(4.9, 0.0, 5.1, 0.0),  # → unmapped (no legend entry for idx:2)
        _seg(50.0, 0.0, 51.0, 0.0),  # → shared
    ]
    # Label near the unmapped segment.
    labels = [_label(5.0, 0.0, "FA DECTN ALM")]

    result = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
        labels=labels,
    )
    per_type_total = sum(e.length_m for e in result.per_type)
    total_check = per_type_total + result.label_unknown_length_m + result.shared_length_m
    assert abs(total_check - result.total_length_m) < 0.1, (
        f"Extended invariant violated: {total_check} != {result.total_length_m}"
    )
    # Label pass recovered the unmapped segment — should appear in per_type with basis=run_label.
    label_entries = [e for e in result.per_type if e.basis == BASIS_RUN_LABEL]
    assert label_entries, "Expected run_label entries in per_type after label pass"
    assert any(e.containment_type == "FA DECTN ALM" for e in label_entries)


def test_label_unknown_preserved_when_beyond_cap() -> None:
    """Unmapped segment whose nearest label is beyond the 5 m cap stays in label_unknown."""
    legend = _legend_from_pairs([])  # all bands unmapped
    bands = [_band("idx:1", 0.0, 0.0, pattern_name="FP_N")]
    segments = [_seg(-0.1, 0.0, 0.1, 0.0)]  # unmapped
    # Label placed 10 m away — beyond the 5 m cap.
    labels = [_label(10.0, 0.0, "FAR SERVICE")]

    result = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
        labels=labels,
        label_nearest_max_m=5.0,
    )
    # Segment is beyond cap → honest-UNKNOWN; no run_label entries emitted.
    run_label_entries = [e for e in result.per_type if e.basis == BASIS_RUN_LABEL]
    assert run_label_entries == []
    assert result.label_unknown_length_m > 0.0
    # The None per_type entry (basis=unresolved) length must equal label_unknown_length_m.
    none_entries = [e for e in result.per_type if e.containment_type is None]
    assert none_entries
    assert none_entries[0].basis == BASIS_UNRESOLVED
    assert abs(none_entries[0].length_m - result.label_unknown_length_m) < 1e-6


def test_no_double_count_segment_counted_under_one_basis() -> None:
    """A segment is counted under exactly one basis: legend OR label, never both."""
    legend = _legend_from_pairs([("idx:1", "SOLID", _TRAY_TYPE)])
    bands = [
        _band("idx:1", 0.0, 0.0, pattern_name="SOLID"),
        _band("idx:2", 5.0, 0.0, pattern_name="FP_N"),
    ]
    segments = [
        _seg(-0.1, 0.0, 0.1, 0.0),  # → legend
        _seg(4.9, 0.0, 5.1, 0.0),  # → label
    ]
    labels = [_label(5.0, 0.0, "LABEL SVC")]

    result = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
        labels=labels,
    )
    # All bases are folded into per_type; total must equal total_length_m (shared=0 here).
    per_type_total = sum(e.length_m for e in result.per_type)
    no_double_count = per_type_total + result.label_unknown_length_m
    assert abs(no_double_count - result.total_length_m) < 0.1
    # Legend segment appears under basis=legend; label segment under basis=run_label.
    legend_entries = [e for e in result.per_type if e.basis == BASIS_LEGEND]
    run_label_entries = [e for e in result.per_type if e.basis == BASIS_RUN_LABEL]
    assert legend_entries, "Expected legend entry for mapped segment"
    assert run_label_entries, "Expected run_label entry for label-recovered segment"


def test_basis_is_run_label_constant() -> None:
    """Every label-recovered entry in per_type has basis == BASIS_RUN_LABEL."""
    legend = _legend_from_pairs([])  # all unmapped
    bands = [_band("idx:1", 0.0, 0.0, pattern_name="FP_N")]
    segments = [_seg(-0.1, 0.0, 0.1, 0.0)]
    labels = [_label(0.0, 0.0, "MY SERVICE")]

    result = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
        labels=labels,
    )
    run_label_entries = [e for e in result.per_type if e.basis == BASIS_RUN_LABEL]
    assert run_label_entries, "Expected at least one run_label entry in per_type"
    for entry in run_label_entries:
        assert entry.basis == BASIS_RUN_LABEL


def test_empty_labels_is_byte_identical_to_pre_label() -> None:
    """With labels=() (default), result is byte-identical to pre-label behaviour."""
    legend = _legend_from_pairs(
        [
            ("idx:1", "SOLID", _TRAY_TYPE),
            ("idx:2", "FP_N", None),
        ]
    )
    bands = [
        _band("idx:1", 0.0, 0.0, pattern_name="SOLID"),
        _band("idx:2", 5.0, 0.0, pattern_name="FP_N"),
    ]
    segments = [
        _seg(-0.1, 0.0, 0.1, 0.0),
        _seg(4.9, 0.0, 5.1, 0.0),
    ]

    result_no_labels = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
        # labels omitted — default ()
    )
    result_empty_labels = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
        labels=(),
    )
    # Both must be equal (labels=() is the default).
    assert result_no_labels == result_empty_labels
    # No run_label entries; unknown is zero.
    run_label_entries = [e for e in result_no_labels.per_type if e.basis == BASIS_RUN_LABEL]
    assert run_label_entries == []
    assert result_no_labels.label_unknown_length_m == 0.0


def test_label_partition_invariant_unmapped_subset() -> None:
    """Σ label_attributed + label_unknown_length_m == original unmapped length U ±0.1."""
    legend = _legend_from_pairs([("idx:1", "SOLID", _TRAY_TYPE)])
    bands = [
        _band("idx:1", 0.0, 0.0, pattern_name="SOLID"),
        _band("idx:2", 5.0, 0.0, pattern_name="FP_N"),  # unmapped
        _band("idx:3", 10.0, 0.0, pattern_name="FP_N"),  # unmapped, no near label
    ]
    segments = [
        _seg(-0.1, 0.0, 0.1, 0.0),  # → tray (legend)
        _seg(4.9, 0.0, 5.1, 0.0),  # → unmapped; label nearby
        _seg(9.9, 0.0, 10.1, 0.0),  # → unmapped; no label within 5 m
    ]
    # Label only near second unmapped segment.
    labels = [_label(5.0, 0.0, "RECOVERED SVC")]

    result = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
        labels=labels,
        label_nearest_max_m=5.0,
    )
    label_total = sum(e.length_m for e in result.per_type if e.basis == BASIS_RUN_LABEL)
    # U = label_total + label_unknown_length_m (within tolerance).
    u_check = label_total + result.label_unknown_length_m
    # Both unmapped segments contribute ~0.2 m each → U ≈ 0.4 m.
    none_entries_no_labels = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
    )
    original_u = sum(
        e.length_m for e in none_entries_no_labels.per_type if e.containment_type is None
    )
    assert abs(u_check - original_u) < 0.1, (
        f"Label partition: {label_total:.4f}"
        f" + {result.label_unknown_length_m:.4f} != U={original_u:.4f}"
    )


@pytest.mark.asyncio
async def test_assembler_with_tag_placement_loader_mock() -> None:
    """Assembler integration: mock tag-placement loader; assert invariant holds with labels."""
    from unittest.mock import AsyncMock, MagicMock, patch
    from uuid import UUID

    from app.interpretation.service_takeoff_loaders import assemble_containment_takeoff

    rev_id = UUID("00000000-0000-0000-0000-000000000002")
    db = MagicMock()

    # Simple legend: one mapped band, one unmapped.
    legend = _legend_from_pairs([("idx:1", "SOLID", _TRAY_TYPE)])
    bands = [
        ContainmentBand(
            colour_key="idx:1",
            colour_index=None,
            colour_rgb=None,
            ring=_square_ring(0.0, 0.0, 0.5),
            pattern_name="SOLID",
        ),
        ContainmentBand(
            colour_key="idx:2",
            colour_index=None,
            colour_rgb=None,
            ring=_square_ring(5.0, 0.0, 0.5),
            pattern_name="FP_N",
        ),
    ]
    segments = [
        _seg(-0.1, 0.0, 0.1, 0.0),  # → tray
        _seg(4.9, 0.0, 5.1, 0.0),  # → unmapped
    ]

    # Simulate a tag placement that parse_tag will accept as a valid label.
    from app.interpretation.run_service_identity import TagPlacement

    tag_placements = [TagPlacement(text="100x200 FA DECTN ALM", point=(5.0, 0.0), layer_ref=None)]

    with (
        patch(
            "app.interpretation.service_takeoff_loaders.build_containment_legend_db",
            new=AsyncMock(return_value=legend),
        ),
        patch(
            "app.interpretation.service_takeoff_loaders.load_containment_bands",
            new=AsyncMock(return_value=bands),
        ),
        patch(
            "app.interpretation.service_takeoff_loaders.load_containment_centerline_segments",
            new=AsyncMock(return_value=segments),
        ),
        patch(
            "app.interpretation.service_takeoff_loaders.load_tag_placements",
            new=AsyncMock(return_value=tag_placements),
        ),
    ):
        result = await assemble_containment_takeoff(db=db, revision_id=rev_id)

    # Label should have recovered the unmapped segment — entry in per_type with basis=run_label.
    label_entries = [e for e in result.per_type if e.basis == BASIS_RUN_LABEL]
    assert label_entries, "Expected run_label entry in per_type after tag-placement label pass"
    per_type_total = sum(e.length_m for e in result.per_type)
    total_check = per_type_total + result.label_unknown_length_m + result.shared_length_m
    assert abs(total_check - result.total_length_m) < 0.1, (
        f"Assembler extended invariant violated: {total_check} != {result.total_length_m}"
    )


# ---------------------------------------------------------------------------
# Tests: basis field on per_type entries (issue #770)
# ---------------------------------------------------------------------------


def test_legend_entries_carry_basis_legend() -> None:
    """Every legend-resolved entry in per_type carries basis='legend'."""
    legend = _legend_from_pairs([("idx:1", "SOLID", _TRAY_TYPE)])
    bands = [_band("idx:1", 0.0, 0.0, pattern_name="SOLID")]
    segments = [_seg(-0.1, 0.0, 0.1, 0.0)]

    result = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
    )
    assert all(e.basis == BASIS_LEGEND for e in result.per_type if e.containment_type is not None)


def test_honest_absent_entry_carries_basis_unresolved() -> None:
    """The honest-absent (containment_type=None) entry carries basis='unresolved'."""
    legend = _legend_from_pairs([])  # empty legend — all bands unmapped
    bands = [_band("idx:1", 0.0, 0.0, pattern_name="SOLID")]
    segments = [_seg(-0.1, 0.0, 0.1, 0.0)]

    result = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
    )
    none_entries = [e for e in result.per_type if e.containment_type is None]
    assert none_entries, "Expected honest-absent (None) entry"
    assert none_entries[0].basis == BASIS_UNRESOLVED


def test_per_type_sorted_deterministically() -> None:
    """per_type is sorted: legend entries first (alphabetically), then run_label, None last."""
    legend = _legend_from_pairs([("idx:1", "SOLID", _TRAY_TYPE)])
    bands = [
        _band("idx:1", 0.0, 0.0, pattern_name="SOLID"),
        _band("idx:2", 5.0, 0.0, pattern_name="FP_N"),  # unmapped
    ]
    segments = [
        _seg(-0.1, 0.0, 0.1, 0.0),  # → legend (Cable Tray)
        _seg(4.9, 0.0, 5.1, 0.0),  # → unmapped → label
    ]
    labels = [_label(5.0, 0.0, "SOME SERVICE")]

    result = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
        labels=labels,
    )
    bases = [e.basis for e in result.per_type]
    # legend entries come before run_label entries; None (unresolved) sorts last within its group.
    legend_indices = [i for i, b in enumerate(bases) if b == BASIS_LEGEND]
    run_label_indices = [i for i, b in enumerate(bases) if b == BASIS_RUN_LABEL]
    unresolved_indices = [i for i, b in enumerate(bases) if b == BASIS_UNRESOLVED]
    if legend_indices and run_label_indices:
        assert max(legend_indices) < min(run_label_indices), (
            "legend entries must precede run_label entries"
        )
    if run_label_indices and unresolved_indices:
        assert max(run_label_indices) < min(unresolved_indices), (
            "run_label entries must precede unresolved entries"
        )


def test_extended_invariant_all_bases() -> None:
    """Extended invariant: Σ(per_type all bases) + label_unknown + shared == total ±0.1."""
    legend = _legend_from_pairs([("idx:1", "SOLID", _TRAY_TYPE)])
    bands = [
        _band("idx:1", 0.0, 0.0, pattern_name="SOLID"),
        _band("idx:2", 5.0, 0.0, pattern_name="FP_N"),  # unmapped
        _band("idx:3", 10.0, 0.0, pattern_name="FP_N"),  # unmapped, no nearby label
    ]
    segments = [
        _seg(-0.1, 0.0, 0.1, 0.0),  # legend
        _seg(4.9, 0.0, 5.1, 0.0),  # → label recovery
        _seg(9.9, 0.0, 10.1, 0.0),  # unmapped, no label → unknown
        _seg(50.0, 0.0, 51.0, 0.0),  # shared
    ]
    labels = [_label(5.0, 0.0, "RECOVERED")]

    result = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
        labels=labels,
        label_nearest_max_m=5.0,
    )
    total_check = (
        sum(e.length_m for e in result.per_type)
        + result.label_unknown_length_m
        + result.shared_length_m
    )
    assert abs(total_check - result.total_length_m) < 0.1, (
        f"Extended invariant (all bases) violated: {total_check} != {result.total_length_m}"
    )


# ---------------------------------------------------------------------------
# Tests: F2 label-family merge (issue #770)
# ---------------------------------------------------------------------------


def test_f2_merge_life_safety_fb_into_life_safety() -> None:
    """'LIFE SAFETY FB' merges into 'LIFE SAFETY' when both appear as label families."""
    legend = _legend_from_pairs([])  # all unmapped — two segments go through label pass
    bands = [
        _band("idx:1", 0.0, 0.0, pattern_name="FP_N"),  # unmapped
        _band("idx:2", 5.0, 0.0, pattern_name="FP_N"),  # unmapped
    ]
    segments = [
        _seg(-0.1, 0.0, 0.1, 0.0),
        _seg(4.9, 0.0, 5.1, 0.0),
    ]
    # Two labels: "LIFE SAFETY" near seg 1, "LIFE SAFETY FB" near seg 2.
    labels = [
        _label(0.0, 0.0, "LIFE SAFETY"),
        _label(5.0, 0.0, "LIFE SAFETY FB"),
    ]

    result = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
        labels=labels,
    )
    run_label_entries = [e for e in result.per_type if e.basis == BASIS_RUN_LABEL]
    # "LIFE SAFETY FB" must be merged into "LIFE SAFETY".
    types = {e.containment_type for e in run_label_entries}
    assert "LIFE SAFETY FB" not in types, "'LIFE SAFETY FB' must be merged into 'LIFE SAFETY'"
    assert "LIFE SAFETY" in types, "Merged 'LIFE SAFETY' entry must be present"
    life_safety_entry = next(e for e in run_label_entries if e.containment_type == "LIFE SAFETY")
    # Combined length must equal sum of both individual segments.
    assert life_safety_entry.length_m == pytest.approx(
        sum(e.length_m for e in run_label_entries), abs=1e-6
    )


def test_f2_no_merge_when_prefix_absent() -> None:
    """'LIFE SAFETY FB' is NOT merged when 'LIFE SAFETY' is NOT present as a label family."""
    legend = _legend_from_pairs([])
    bands = [_band("idx:1", 0.0, 0.0, pattern_name="FP_N")]
    segments = [_seg(-0.1, 0.0, 0.1, 0.0)]
    # Only "LIFE SAFETY FB" — prefix not present, so no merge.
    labels = [_label(0.0, 0.0, "LIFE SAFETY FB")]

    result = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
        labels=labels,
    )
    run_label_entries = [e for e in result.per_type if e.basis == BASIS_RUN_LABEL]
    types = {e.containment_type for e in run_label_entries}
    # No merge — "LIFE SAFETY FB" stays as-is.
    assert "LIFE SAFETY FB" in types
    assert "LIFE SAFETY" not in types


def test_f2_no_cross_basis_merge() -> None:
    """Legend entry 'ESSENTIAL TRUNKING' and label entry 'ESSENTIAL' stay distinct."""
    legend = _legend_from_pairs(
        [
            ("idx:1", "SOLID", "ESSENTIAL TRUNKING"),
            ("idx:2", "FP_N", None),  # unmapped
        ]
    )
    bands = [
        _band("idx:1", 0.0, 0.0, pattern_name="SOLID"),
        _band("idx:2", 5.0, 0.0, pattern_name="FP_N"),
    ]
    segments = [
        _seg(-0.1, 0.0, 0.1, 0.0),  # → legend (ESSENTIAL TRUNKING)
        _seg(4.9, 0.0, 5.1, 0.0),  # → unmapped → label
    ]
    # Label that matches a prefix of the legend type — must NOT cross-merge.
    labels = [_label(5.0, 0.0, "ESSENTIAL")]

    result = compute_containment_attributed_lengths(
        centerline_segments=segments,
        containment_bands=bands,
        legend=legend,
        labels=labels,
    )
    legend_entries = [e for e in result.per_type if e.basis == BASIS_LEGEND]
    run_label_entries = [e for e in result.per_type if e.basis == BASIS_RUN_LABEL]
    legend_types = {e.containment_type for e in legend_entries}
    run_label_types = {e.containment_type for e in run_label_entries}
    # Both exist as distinct entries.
    assert "ESSENTIAL TRUNKING" in legend_types, "Legend entry must be kept"
    assert "ESSENTIAL" in run_label_types, "Label entry must be kept separate"
