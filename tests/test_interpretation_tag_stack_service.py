"""Tests for app/interpretation/tag_stack_service.py (pure, no DB).

All band fixtures use overlapping RINGS that are crossed by a vertical scan line,
so they exercise the scan-line code path rather than centroid logic.

Coordinate convention: metre-scale geometry, pipes run horizontally (left-right),
bands are tall-and-narrow rectangles (taller than wide), stacked in y.  A vertical
scan line at a fixed x will cross all bands in the stack and read their y-order.
"""

from __future__ import annotations

import random

from app.interpretation.tag_stack_service import (
    BundleColourBand,
    StackHeader,
    TagStackServiceResult,
    TagStackText,
    _is_degenerate_fingerprint,
    _normalize_header,
    _rle,
    _scan_line_colour_rle,
    assign_services_by_tag_stack,
)


def test_normalize_header_substring_match() -> None:
    """Header phrase embedded in surrounding words is recognised (real drawings wrap it)."""
    assert _normalize_header("FROM TOP TO BOTTOM TA :") == "TOP TO BOTTOM"
    assert _normalize_header("from left to right to the building") == "LEFT TO RIGHT"
    assert _normalize_header("NOTES") == ""  # no orientation phrase
    # ambiguous: contains two distinct phrases -> abstain
    assert _normalize_header("TOP TO BOTTOM then LEFT TO RIGHT") == ""


# ---------------------------------------------------------------------------
# Ring construction helpers
# ---------------------------------------------------------------------------


def _vband(
    colour_key: str,
    x0: float,
    x1: float,
    y0: float,
    y1: float,
) -> BundleColourBand:
    """Tall-and-narrow band that a vertical scan line at x in [x0,x1] will cross."""
    ring = ((x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0))
    return BundleColourBand(colour_key=colour_key, ring=ring)


# ---------------------------------------------------------------------------
# Unit tests for _rle
# ---------------------------------------------------------------------------


def test_rle_basic() -> None:
    assert _rle(["MA", "VAC", "VAC", "AGSS"]) == (("MA", 1), ("VAC", 2), ("AGSS", 1))


def test_rle_single() -> None:
    assert _rle(["A"]) == (("A", 1),)


def test_rle_empty() -> None:
    assert _rle([]) == ()


def test_rle_all_same() -> None:
    assert _rle(["X", "X", "X"]) == (("X", 3),)


# ---------------------------------------------------------------------------
# Unit tests for _is_degenerate_fingerprint
# ---------------------------------------------------------------------------


def test_degenerate_single_run() -> None:
    assert _is_degenerate_fingerprint((("MA", 1),)) is True


def test_degenerate_all_same_service() -> None:
    assert _is_degenerate_fingerprint((("VAC", 2), ("VAC", 3))) is True


def test_not_degenerate() -> None:
    assert _is_degenerate_fingerprint((("MA", 1), ("VAC", 4))) is False


# ---------------------------------------------------------------------------
# Unit test for _scan_line_colour_rle
# ---------------------------------------------------------------------------


def test_scan_line_rle_vertical() -> None:
    """Vertical scan line at x=5 crosses three bands in y order, one bundle segment."""
    bands = [
        _vband("c1", 4.0, 6.0, 2.0, 3.0),  # y centroid 2.5
        _vband("c2", 4.0, 6.0, 1.0, 2.0),  # y centroid 1.5
        _vband("c2", 4.0, 6.0, 0.0, 1.0),  # y centroid 0.5
    ]
    # perp_descending=True -> sort descending y -> c1 first, then c2, c2
    segments = _scan_line_colour_rle(
        bands, 5.0, scan_axis=0, perp_descending=True, level_merge_tol=0.03, bundle_gap_tol=0.5
    )
    assert len(segments) == 1
    assert segments[0] == (("c1", 1), ("c2", 2))


def test_scan_line_rle_misses_band() -> None:
    """Band not spanning scan position is excluded."""
    bands = [
        _vband("c1", 4.0, 6.0, 0.0, 1.0),
        _vband("c2", 8.0, 10.0, 0.0, 1.0),  # x-range does not span x=5
    ]
    segments = _scan_line_colour_rle(
        bands, 5.0, scan_axis=0, perp_descending=True, level_merge_tol=0.03, bundle_gap_tol=0.5
    )
    assert len(segments) == 1
    assert segments[0] == (("c1", 1),)


def test_scan_line_rle_merge_gap() -> None:
    """Two same-colour bands with centroids within level_merge_tol are merged."""
    bands = [
        _vband("c1", 4.0, 6.0, 1.000, 1.010),  # y centroid 1.005
        _vband("c1", 4.0, 6.0, 1.015, 1.025),  # y centroid 1.020; gap ~0.015 < 0.03
        _vband("c2", 4.0, 6.0, 0.0, 0.5),
    ]
    segments = _scan_line_colour_rle(
        bands, 5.0, scan_axis=0, perp_descending=True, level_merge_tol=0.03, bundle_gap_tol=0.5
    )
    # c1 appears at top (descending y -> ~1.0 first), merged to 1; c2 below
    assert len(segments) == 1
    assert segments[0] == (("c1", 1), ("c2", 1))


def test_scan_line_rle_gap_segmentation() -> None:
    """A perpendicular gap > bundle_gap_tol splits the crossing into two segments."""
    bands = [
        # Bundle A: y=22..24 (tight cluster)
        _vband("orange", 4.0, 6.0, 23.5, 24.0),  # y centroid 23.75
        _vband("brown", 4.0, 6.0, 23.0, 23.5),  # y centroid 23.25
        _vband("brown", 4.0, 6.0, 22.5, 23.0),  # y centroid 22.75
        # Gap of 7+ m
        # Bundle B: y=14..16 (different service cluster)
        _vband("cyan", 4.0, 6.0, 15.0, 16.0),  # y centroid 15.5
        _vband("cyan", 4.0, 6.0, 14.0, 15.0),  # y centroid 14.5
    ]
    segments = _scan_line_colour_rle(
        bands, 5.0, scan_axis=0, perp_descending=True, level_merge_tol=0.03, bundle_gap_tol=0.5
    )
    # Segments are in ascending-y order (gap-detection walks low-y to high-y).
    # Segment 0 = lower cluster (cyan, y=14..16); segment 1 = upper cluster (orange+brown).
    assert len(segments) == 2
    # Bundle B (lower, y=14..16): descending y -> cyan at 15.5 first, then 14.5 -> merged (2).
    assert segments[0] == (("cyan", 2),)
    # Bundle A (upper, y=22..24): descending y -> orange(23.75) first, then 2x brown.
    assert segments[1] == (("orange", 1), ("brown", 2))


# ---------------------------------------------------------------------------
# Happy-path test 1: stack [A, B, B] + bundle [c1:1, c2:2]
# ---------------------------------------------------------------------------


def _simple_tags() -> list[TagStackText]:
    """Vertical stack at x=0: AA at top (y=3), BB x2 below (y=2, y=1)."""
    return [
        TagStackText(text="50 mm AA", point=(0.0, 3.0)),
        TagStackText(text="76 mm BB", point=(0.0, 2.0)),
        TagStackText(text="42 mm BB", point=(0.0, 1.0)),
    ]


def _simple_header() -> list[StackHeader]:
    return [StackHeader(text="TOP TO BOTTOM", point=(0.0, 3.5))]


def _simple_bands() -> dict[str, list[BundleColourBand]]:
    """Bands at x=10..11, stacked in y.

    c1 is at y=2..3 (top), c2 appears twice at y=1..2 and y=0..1.
    Multiple copies along x so MIN_STABLE_MATCH_COUNT=2 is satisfied.
    """
    return {
        "c1": [_vband("c1", 10.0 + i * 1.0, 10.5 + i * 1.0, 2.0, 3.0) for i in range(3)],
        "c2": [_vband("c2", 10.0 + i * 1.0, 10.5 + i * 1.0, 1.0, 2.0) for i in range(3)]
        + [_vband("c2", 10.0 + i * 1.0, 10.5 + i * 1.0, 0.0, 1.0) for i in range(3)],
    }


def test_simple_happy_path() -> None:
    result = assign_services_by_tag_stack(
        tags=_simple_tags(),
        headers=_simple_header(),
        bundle_bands_by_colour=_simple_bands(),
    )
    assert isinstance(result, TagStackServiceResult)
    assert result.matched_stack_count == 1
    by_colour = {a.colour_key: a for a in result.assignments}
    assert by_colour["c1"].service == "AA"
    assert by_colour["c2"].service == "BB"
    assert len(by_colour["c2"].sizes) == 2
    assert result.unmatched_colour_keys == ()


# ---------------------------------------------------------------------------
# Happy-path test 2: M-540003 regression (MA x1, VAC x4, AGSS x4)
# ---------------------------------------------------------------------------


def _m540003_tags() -> list[TagStackText]:
    """Vertical stack at x=0. TOP TO BOTTOM: MA first, then 4 VAC, 4 AGSS."""
    tags = [TagStackText(text="Ø25 mm MA", point=(0.0, 9.0))]
    for i, y in enumerate([8.0, 7.0, 6.0, 5.0]):
        tags.append(TagStackText(text=f"O{54 + i} mm VAC", point=(0.0, y)))
    for i, y in enumerate([4.0, 3.0, 2.0, 1.0]):
        tags.append(TagStackText(text=f"O{42 + i} mm AGSS", point=(0.0, y)))
    return tags


def _m540003_header() -> list[StackHeader]:
    return [StackHeader(text="TOP TO BOTTOM", point=(0.0, 9.5))]


def _m540003_bands() -> dict[str, list[BundleColourBand]]:
    """Three colour groups forming a bundle cross-section, plus a distant cluster.

    Main bundle (y=0..9): orange at top (1 band y=8..9), brown x4 (y=4..8),
    green x4 (y=0..4). Bands extend over multiple x-positions so >=2 scan
    positions cross them (stability requirement satisfied).

    Distant cluster (y=15..17, 7+ m gap): cyan x2 bands. This cluster is
    separated from the main bundle by >BUNDLE_GAP_TOL_M, so the gap-segmentation
    should keep it as a separate segment. Its count sequence (2,) != (1,4,4),
    so it will NOT match the stack fingerprint and stays unmatched/opaque.

    A turning-corner section at x=50..51 has only brown+green bands (count seq
    (4,4) != (1,4,4)), exercising robustness.
    """
    bands: dict[str, list[BundleColourBand]] = {}

    # Straight section: all 9 bands present at x=44..46 (4 x-positions).
    bands["orange"] = [_vband("orange", 44.0 + i * 0.5, 44.4 + i * 0.5, 8.0, 9.0) for i in range(4)]
    bands["brown"] = []
    for y in [4.0, 5.0, 6.0, 7.0]:
        bands["brown"] += [
            _vband("brown", 44.0 + i * 0.5, 44.4 + i * 0.5, y, y + 1.0) for i in range(4)
        ]
    bands["green"] = []
    for y in [0.0, 1.0, 2.0, 3.0]:
        bands["green"] += [
            _vband("green", 44.0 + i * 0.5, 44.4 + i * 0.5, y, y + 1.0) for i in range(4)
        ]

    # Turning corner: only brown and green (count seq (4,4) != (1,4,4)).
    for y in [4.0, 5.0, 6.0, 7.0]:
        bands["brown"] += [_vband("brown", 50.0, 51.0, y, y + 1.0)]
    for y in [0.0, 1.0, 2.0, 3.0]:
        bands["green"] += [_vband("green", 50.0, 51.0, y, y + 1.0)]

    # Distant cluster at y=15..17 (7+ m above the main bundle).
    # Multiple x-positions so it's stable, but count seq (2,) != (1,4,4).
    bands["cyan"] = [
        _vband("cyan", 44.0 + i * 0.5, 44.4 + i * 0.5, 15.0, 16.0) for i in range(4)
    ] + [_vband("cyan", 44.0 + i * 0.5, 44.4 + i * 0.5, 16.0, 17.0) for i in range(4)]

    return bands


def test_m540003_shape() -> None:
    """Main bundle matches [MA,VAC x4,AGSS x4]; distant cyan cluster stays unmatched."""
    result = assign_services_by_tag_stack(
        tags=_m540003_tags(),
        headers=_m540003_header(),
        bundle_bands_by_colour=_m540003_bands(),
    )
    assert result.matched_stack_count == 1, f"Got: {result}"
    by_colour = {a.colour_key: a for a in result.assignments}
    assert by_colour["orange"].service == "MA", f"orange: {by_colour.get('orange')}"
    assert by_colour["brown"].service == "VAC"
    assert by_colour["green"].service == "AGSS"
    assert len(by_colour["orange"].sizes) == 1
    assert len(by_colour["brown"].sizes) == 4
    assert len(by_colour["green"].sizes) == 4
    # cyan is a separate, non-matching bundle segment -> stays unmatched (honest).
    assert "cyan" in result.unmatched_colour_keys


# ---------------------------------------------------------------------------
# Abstain case 1: no header in range
# ---------------------------------------------------------------------------


def test_abstain_no_header_in_range() -> None:
    tags = [
        TagStackText(text="50 mm AA", point=(0.0, 3.0)),
        TagStackText(text="76 mm BB", point=(0.0, 2.0)),
    ]
    headers = [StackHeader(text="TOP TO BOTTOM", point=(999.0, 999.0))]
    bands = {
        "c1": [_vband("c1", 4.0 + i * 0.5, 4.4 + i * 0.5, 2.0, 3.0) for i in range(4)],
        "c2": [_vband("c2", 4.0 + i * 0.5, 4.4 + i * 0.5, 1.0, 2.0) for i in range(4)],
    }
    result = assign_services_by_tag_stack(tags=tags, headers=headers, bundle_bands_by_colour=bands)
    assert result.assignments == ()
    assert result.ambiguous is True
    assert "c1" in result.unmatched_colour_keys
    assert "c2" in result.unmatched_colour_keys


# ---------------------------------------------------------------------------
# Abstain case 2: count mismatch
# ---------------------------------------------------------------------------


def test_abstain_count_mismatch() -> None:
    """Stack fingerprint (1,4,4) but bundle scan yields (1,3,5) at all positions."""
    tags = _m540003_tags()
    headers = _m540003_header()
    # orange=1, brown=3, green=5 stacked bands.
    bands: dict[str, list[BundleColourBand]] = {
        "orange": [_vband("orange", 44.0 + i * 0.5, 44.4 + i * 0.5, 8.0, 9.0) for i in range(4)],
        "brown": [],
        "green": [],
    }
    for y in [5.0, 6.0, 7.0]:
        bands["brown"] += [
            _vband("brown", 44.0 + i * 0.5, 44.4 + i * 0.5, y, y + 1.0) for i in range(4)
        ]
    for y in [0.0, 1.0, 2.0, 3.0, 4.0]:
        bands["green"] += [
            _vband("green", 44.0 + i * 0.5, 44.4 + i * 0.5, y, y + 1.0) for i in range(4)
        ]
    result = assign_services_by_tag_stack(tags=tags, headers=headers, bundle_bands_by_colour=bands)
    assert result.assignments == ()
    assert result.ambiguous is True


# ---------------------------------------------------------------------------
# Abstain case 4: two stacks with conflicting mappings
# ---------------------------------------------------------------------------


def test_abstain_two_stacks_conflicting_mappings() -> None:
    """Two stacks whose scan-line matches disagree on colour->service -> abstain."""
    # Stack1 (TOP_TO_BOTTOM at x=0): AA(1), BB(2) -> c1=AA, c2=BB
    # Stack2 (BOTTOM_TO_TOP at x=5): DD(1), CC(2) -> c2=CC, c1=DD  => c1 conflict.
    tags = [
        TagStackText(text="50 mm AA", point=(0.0, 3.0)),
        TagStackText(text="76 mm BB", point=(0.0, 2.0)),
        TagStackText(text="42 mm BB", point=(0.0, 1.0)),
        TagStackText(text="60 mm DD", point=(5.0, 3.0)),
        TagStackText(text="76 mm CC", point=(5.0, 2.0)),
        TagStackText(text="42 mm CC", point=(5.0, 1.0)),
    ]
    headers = [
        StackHeader(text="TOP TO BOTTOM", point=(0.0, 3.5)),
        StackHeader(text="BOTTOM TO TOP", point=(5.0, 0.5)),
    ]
    # Bundle: c1:1 at top (y=2..3), c2:2 below (y=0..2).
    # TOP_TO_BOTTOM sees (c1:1, c2:2) -> c1=AA, c2=BB.
    # BOTTOM_TO_TOP reads ascending y: c2 first (y=0..2), c1 second (y=2..3)
    #   -> c2:2, c1:1 count seq (2,1); stack2 fingerprint (DD:1, CC:2) counts (1,2).
    # Count mismatch for stack2 -> it abstains, stack1 alone succeeds.
    # To force a conflict: use a separate bundle at a different x where the
    # BOTTOM_TO_TOP stack's count seq DOES match.
    extra_bands_c1 = [_vband("c1", 20.0 + i * 0.5, 20.4 + i * 0.5, 0.0, 1.0) for i in range(4)]
    extra_bands_c2 = [_vband("c2", 20.0 + i * 0.5, 20.4 + i * 0.5, 1.0, 2.0) for i in range(4)] + [
        _vband("c2", 20.0 + i * 0.5, 20.4 + i * 0.5, 2.0, 3.0) for i in range(4)
    ]
    bands = {
        "c1": [_vband("c1", 10.0 + i * 0.5, 10.4 + i * 0.5, 2.0, 3.0) for i in range(4)]
        + extra_bands_c1,
        "c2": [_vband("c2", 10.0 + i * 0.5, 10.4 + i * 0.5, 1.0, 2.0) for i in range(4)]
        + [_vband("c2", 10.0 + i * 0.5, 10.4 + i * 0.5, 0.0, 1.0) for i in range(4)]
        + extra_bands_c2,
    }
    result = assign_services_by_tag_stack(tags=tags, headers=headers, bundle_bands_by_colour=bands)
    # Stack1: TOP_TO_BOTTOM reads c1:1(top), c2:2(bot); count (1,2) matches AA(1),BB(2).
    # Stack2: BOTTOM_TO_TOP reads ascending y -> from x=20 bundle:
    #   c1 at y=0..1 (bottom), c2 at y=1..3 -> c1:1, c2:2 ascending = (1,2); DD(1),CC(2).
    # Both stacks find a stable match; stack1 says c1->AA,c2->BB; stack2 says c1->DD,c2->CC
    # -> conflict on c1 (AA vs DD) and c2 (BB vs CC) -> abstain.
    assert result.assignments == ()
    assert result.ambiguous is True


# ---------------------------------------------------------------------------
# Abstain case 5: degenerate fingerprint
# ---------------------------------------------------------------------------


def test_abstain_single_run_fingerprint() -> None:
    """Stack with all-same service -> degenerate, abstain."""
    tags = [
        TagStackText(text="50 mm VAC", point=(0.0, 3.0)),
        TagStackText(text="76 mm VAC", point=(0.0, 2.0)),
        TagStackText(text="42 mm VAC", point=(0.0, 1.0)),
    ]
    headers = [StackHeader(text="TOP TO BOTTOM", point=(0.0, 3.5))]
    bands = {
        "c1": [_vband("c1", 4.0 + i * 0.5, 4.4 + i * 0.5, 1.0, 3.0) for i in range(4)],
        "c2": [_vband("c2", 4.0 + i * 0.5, 4.4 + i * 0.5, 0.0, 1.0) for i in range(4)],
    }
    result = assign_services_by_tag_stack(tags=tags, headers=headers, bundle_bands_by_colour=bands)
    assert result.assignments == ()
    assert result.ambiguous is True


# ---------------------------------------------------------------------------
# Abstain case 7: unstable match (only one scan position matches)
# ---------------------------------------------------------------------------


def test_abstain_unstable_single_position() -> None:
    """Only one scan position yields the matching RLE -> unstable, abstain."""
    tags = [
        TagStackText(text="50 mm AA", point=(0.0, 2.0)),
        TagStackText(text="76 mm BB", point=(0.0, 1.0)),
    ]
    headers = [StackHeader(text="TOP TO BOTTOM", point=(0.0, 2.5))]
    # Only a single pair of bands (one x-midpoint each) -> only ONE candidate
    # scan position spans both -> fewer than MIN_STABLE_MATCH_COUNT=2.
    bands = {
        "c1": [_vband("c1", 4.0, 5.0, 1.0, 2.0)],
        "c2": [_vband("c2", 4.0, 5.0, 0.0, 1.0)],
    }
    result = assign_services_by_tag_stack(tags=tags, headers=headers, bundle_bands_by_colour=bands)
    assert result.assignments == ()
    assert result.ambiguous is True


# ---------------------------------------------------------------------------
# Determinism: shuffled input -> identical result
# ---------------------------------------------------------------------------


def test_determinism_shuffled_inputs() -> None:
    tags = _m540003_tags()
    headers = _m540003_header()
    bands = _m540003_bands()

    result_a = assign_services_by_tag_stack(
        tags=tags, headers=headers, bundle_bands_by_colour=bands
    )

    rng = random.Random(42)
    shuffled_tags = list(tags)
    rng.shuffle(shuffled_tags)

    shuffled_bands: dict[str, list[BundleColourBand]] = {}
    for ck in ["green", "brown", "orange", "cyan"]:
        shuffled_bands[ck] = list(bands[ck])
        rng.shuffle(shuffled_bands[ck])

    result_b = assign_services_by_tag_stack(
        tags=shuffled_tags, headers=list(headers), bundle_bands_by_colour=shuffled_bands
    )

    assert result_a == result_b


# ---------------------------------------------------------------------------
# Empty inputs -> empty result, no raise
# ---------------------------------------------------------------------------


def test_empty_tags() -> None:
    result = assign_services_by_tag_stack(
        tags=[],
        headers=[StackHeader(text="TOP TO BOTTOM", point=(0.0, 4.0))],
        bundle_bands_by_colour=_simple_bands(),
    )
    assert result.assignments == ()
    assert result.matched_stack_count == 0
    assert result.ambiguous is False


def test_empty_bands() -> None:
    result = assign_services_by_tag_stack(
        tags=_simple_tags(),
        headers=_simple_header(),
        bundle_bands_by_colour={},
    )
    assert result.assignments == ()
    assert result.matched_stack_count == 0


def test_empty_both() -> None:
    result = assign_services_by_tag_stack(
        tags=[],
        headers=[],
        bundle_bands_by_colour={},
    )
    assert result.assignments == ()
    assert result.matched_stack_count == 0
    assert result.ambiguous is False


# ---------------------------------------------------------------------------
# size_kind consistency
# ---------------------------------------------------------------------------


def test_size_kind_consistent_round() -> None:
    """All entries round -> size_kind 'round'."""
    result = assign_services_by_tag_stack(
        tags=_simple_tags(),
        headers=_simple_header(),
        bundle_bands_by_colour=_simple_bands(),
    )
    by_colour = {a.colour_key: a for a in result.assignments}
    assert by_colour["c1"].size_kind == "round"
    assert by_colour["c2"].size_kind == "round"


def test_size_kind_none_on_mixed() -> None:
    """Mixed round+rect within a service run -> size_kind None."""
    tags = [
        TagStackText(text="50 mm AA", point=(0.0, 3.0)),
        TagStackText(text="700x300 CC", point=(0.0, 2.0)),
        TagStackText(text="Ø42 mm CC", point=(0.0, 1.0)),
    ]
    headers = [StackHeader(text="TOP TO BOTTOM", point=(0.0, 3.5))]
    bands = {
        "c1": [_vband("c1", 10.0 + i * 1.0, 10.5 + i * 1.0, 2.0, 3.0) for i in range(3)],
        "c2": [_vband("c2", 10.0 + i * 1.0, 10.5 + i * 1.0, 1.0, 2.0) for i in range(3)]
        + [_vband("c2", 10.0 + i * 1.0, 10.5 + i * 1.0, 0.0, 1.0) for i in range(3)],
    }
    result = assign_services_by_tag_stack(tags=tags, headers=headers, bundle_bands_by_colour=bands)
    by_colour = {a.colour_key: a for a in result.assignments}
    assert by_colour["c2"].size_kind is None
    assert len(by_colour["c2"].sizes) == 2


# ---------------------------------------------------------------------------
# sizes-vary-within-service preserved
# ---------------------------------------------------------------------------


def test_sizes_vary_within_service_preserved() -> None:
    result = assign_services_by_tag_stack(
        tags=_m540003_tags(),
        headers=_m540003_header(),
        bundle_bands_by_colour=_m540003_bands(),
    )
    by_colour = {a.colour_key: a for a in result.assignments}
    vac_sizes = by_colour["brown"].sizes
    assert len(vac_sizes) == 4


# ---------------------------------------------------------------------------
# Assignments sorted by colour_key
# ---------------------------------------------------------------------------


def test_assignments_sorted_by_colour_key() -> None:
    result = assign_services_by_tag_stack(
        tags=_m540003_tags(),
        headers=_m540003_header(),
        bundle_bands_by_colour=_m540003_bands(),
    )
    keys = [a.colour_key for a in result.assignments]
    assert keys == sorted(keys)
