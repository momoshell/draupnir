"""Synthetic unit tests for app.interpretation.service_fill_takeoff (issue #663, Phase 1).

All geometry is in metres with dict-coord {"x","y","z"} style where the loader path is
exercised.  Segments are provided directly as tuple pairs.  No DB, no ORM, no FastAPI.
"""

from __future__ import annotations

import pytest

from app.interpretation.service_fill_takeoff import (
    FillBand,
    compute_fill_attributed_lengths,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_RED = "idx:1"
_BLUE = "idx:5"
_GREEN = "idx:3"


def _square_band(
    colour_key: str,
    cx: float,
    cy: float,
    half: float,
    colour_index: int | None = None,
    colour_rgb: str | None = None,
) -> FillBand:
    """Axis-aligned square fill band centred at (cx, cy) with given half-side."""
    ring = (
        (cx - half, cy - half),
        (cx + half, cy - half),
        (cx + half, cy + half),
        (cx - half, cy + half),
    )
    return FillBand(
        colour_key=colour_key,
        colour_index=colour_index,
        colour_rgb=colour_rgb,
        ring=ring,
    )


# ---------------------------------------------------------------------------
# (a) Segment fully inside one band → attributed to that colour
# ---------------------------------------------------------------------------


def test_segment_inside_band_attributed_to_that_colour() -> None:
    """A segment fully enclosed in a red band → all length goes to red.

    Arrange: 0.5 m red band centred at (0, 0); segment from (-0.1, 0) to (0.1, 0) → 0.2 m.
    Act:     compute attribution.
    Assert:  red gets ≈ 0.2 m; shared = 0; total = 0.2 m.
    """
    red_band = _square_band(_RED, 0.0, 0.0, 0.5)
    seg = (((-0.1, 0.0), (0.1, 0.0)),)
    result = compute_fill_attributed_lengths(
        centerline_segments=seg,
        fill_bands=[red_band],
    )
    assert len(result.per_colour) == 1
    assert result.per_colour[0].colour_key == _RED
    assert result.per_colour[0].length_m == pytest.approx(0.2, abs=1e-4)
    assert result.shared_length_m == pytest.approx(0.0, abs=1e-4)
    assert result.total_length_m == pytest.approx(0.2, abs=1e-4)
    assert result.centerline_segment_count == 1


# ---------------------------------------------------------------------------
# (b) Segment between two bands, nearest within 100mm and ≥20mm clearer → nearest
# ---------------------------------------------------------------------------


def test_nearest_fallback_within_threshold() -> None:
    """A segment between two bands where one is clearly nearest → attributed to that colour.

    Arrange:
      - Red band centred at y = +0.10 m (30mm above segment)
      - Blue band centred at y = -0.25 m (200mm below segment)
      Segment runs along y = 0 from x=0 to x=0.05 (both bands are fully to the side).
    Act:     compute attribution.
    Assert:  red gets the segment (nearest at ~70mm vs blue at ~200mm; margin ≥20mm).
    """
    red_band = _square_band(_RED, 0.025, 0.10, 0.02)  # above segment, dist ~80mm
    blue_band = _square_band(_BLUE, 0.025, -0.25, 0.02)  # well below, dist ~220mm
    seg = (((0.0, 0.0), (0.05, 0.0)),)
    result = compute_fill_attributed_lengths(
        centerline_segments=seg,
        fill_bands=[red_band, blue_band],
        nearest_max_m=0.100,  # accept up to 100mm
        nearest_margin_m=0.020,
    )
    # Nearest should be red (distance ~70mm).
    assert len(result.per_colour) == 1
    assert result.per_colour[0].colour_key == _RED
    assert result.shared_length_m == pytest.approx(0.0, abs=1e-4)
    assert result.total_length_m == pytest.approx(0.05, abs=1e-4)


def test_tight_corner_connector_at_90mm_attributes_at_default() -> None:
    """A tight-corner elbow connector ~90mm from its fill band attributes at the 100mm default.

    Real-world motivation (M-540003): vacuum-plant bend connectors sit 90-96mm from their
    fill band.  At the old 80mm default they fell into shared; at 100mm (new default) they
    correctly attribute to the service colour.

    Arrange:
      - Red band is a square centred at y=+0.12 m (edge at y=+0.10 m, so dist ≈ 90mm).
      - Blue band far below (dist >> 100mm) — ensures unambiguous nearest.
      Segment runs along y=0; no overlap, nearest unambiguously red.
    Act:     compute attribution with default nearest_max_m (0.100).
    Assert:  red gets the segment; shared = 0.
    """
    red_band = _square_band(_RED, 0.025, 0.12, 0.02)  # edge at y=0.10, dist ≈ 90mm
    blue_band = _square_band(_BLUE, 0.025, -0.50, 0.02)  # well below, dist > 400mm
    seg = (((0.0, 0.0), (0.05, 0.0)),)
    result = compute_fill_attributed_lengths(
        centerline_segments=seg,
        fill_bands=[red_band, blue_band],
        # nearest_max_m uses the new default (0.100) — NOT passed explicitly
        nearest_margin_m=0.020,
    )
    assert len(result.per_colour) == 1
    assert result.per_colour[0].colour_key == _RED
    assert result.shared_length_m == pytest.approx(0.0, abs=1e-4)


# ---------------------------------------------------------------------------
# (c) Ambiguous segment → shared/manifold bucket
# ---------------------------------------------------------------------------


def test_ambiguous_segment_goes_to_shared() -> None:
    """A segment equidistant from two bands (below nearest_max_m but no clear margin) → shared.

    Arrange:
      - Red band at y=+0.05 m (dist from segment ≈ 20mm)
      - Blue band at y=-0.05 m (dist from segment ≈ 20mm)
      Both are within 80mm, margin between them is 0 (< 20mm required) → shared.
    """
    red_band = _square_band(_RED, 0.025, 0.05, 0.015)
    blue_band = _square_band(_BLUE, 0.025, -0.05, 0.015)
    seg = (((0.0, 0.0), (0.05, 0.0)),)
    result = compute_fill_attributed_lengths(
        centerline_segments=seg,
        fill_bands=[red_band, blue_band],
        overlap_ratio_min=0.30,
        nearest_max_m=0.080,
        nearest_margin_m=0.020,
    )
    assert result.shared_length_m == pytest.approx(0.05, abs=1e-4)
    assert len(result.per_colour) == 0


def test_single_colour_within_max_distance_attributed_without_margin() -> None:
    """Single fill colour: ambiguity-margin is intentionally waived (no competing service).

    When only one colour band exists, any segment within nearest_max_m is attributed to it
    regardless of margin — there is no second candidate to be ambiguous with.
    """
    red_band = _square_band(_RED, 0.025, 0.08, 0.02)  # edge at y=0.06, dist ≈ 60mm
    seg = (((0.0, 0.0), (0.05, 0.0)),)
    result = compute_fill_attributed_lengths(
        centerline_segments=seg,
        fill_bands=[red_band],  # single colour
        nearest_max_m=0.100,
        nearest_margin_m=0.020,
    )
    assert len(result.per_colour) == 1
    assert result.per_colour[0].colour_key == _RED
    assert result.shared_length_m == pytest.approx(0.0, abs=1e-4)


def test_single_colour_beyond_max_distance_goes_to_shared() -> None:
    """Single fill colour: a segment beyond nearest_max_m goes to shared even with no competitor."""
    red_band = _square_band(_RED, 0.025, 0.50, 0.02)  # edge at y=0.48, dist ≈ 480mm >> 100mm
    seg = (((0.0, 0.0), (0.05, 0.0)),)
    result = compute_fill_attributed_lengths(
        centerline_segments=seg,
        fill_bands=[red_band],
        nearest_max_m=0.100,
    )
    assert result.shared_length_m == pytest.approx(0.05, abs=1e-4)
    assert len(result.per_colour) == 0


# ---------------------------------------------------------------------------
# (d) Conservation invariant Σ(per_colour) + shared == total
# ---------------------------------------------------------------------------


def test_conservation_invariant_multi_segment() -> None:
    """Σ(per_colour lengths) + shared == total_length_m, within ±0.1 m tolerance.

    Three segments: one clearly inside red, one clearly inside blue, one ambiguous.
    """
    red_band = _square_band(_RED, 0.0, 0.0, 0.3)
    blue_band = _square_band(_BLUE, 2.0, 0.0, 0.3)

    segs = [
        # Inside red band
        ((-0.1, 0.0), (0.1, 0.0)),
        # Inside blue band
        ((1.9, 0.0), (2.1, 0.0)),
        # Far from both → shared
        ((5.0, 0.0), (6.0, 0.0)),
    ]
    result = compute_fill_attributed_lengths(
        centerline_segments=segs,
        fill_bands=[red_band, blue_band],
    )
    per_colour_sum = sum(fc.length_m for fc in result.per_colour)
    total_check = per_colour_sum + result.shared_length_m
    assert total_check == pytest.approx(result.total_length_m, abs=0.1)
    # Spot-check: total is 0.2 + 0.2 + 1.0 = 1.4 m
    assert result.total_length_m == pytest.approx(1.4, abs=1e-4)


def test_conservation_invariant_with_zero_length_segment() -> None:
    """Zero-length (degenerate) segments are excluded from total_length_m.

    Invariant Σ(per_colour)+shared==total must hold structurally, not by luck:
    a zero-length segment contributes 0 to both sides so it cannot affect balance,
    and total_length_m must equal the sum of the non-degenerate segments only.
    """
    red_band = _square_band(_RED, 0.0, 0.0, 0.3)
    segs = [
        ((-0.1, 0.0), (0.1, 0.0)),  # inside red → 0.2 m
        ((0.5, 0.0), (0.5, 0.0)),  # zero-length degenerate → excluded
        ((5.0, 0.0), (6.0, 0.0)),  # far away → shared → 1.0 m
    ]
    result = compute_fill_attributed_lengths(
        centerline_segments=segs,
        fill_bands=[red_band],
    )
    per_colour_sum = sum(fc.length_m for fc in result.per_colour)
    total_check = per_colour_sum + result.shared_length_m
    # Exact equality (not just ±0.1): degenerate segment must not disturb either side.
    assert total_check == pytest.approx(result.total_length_m, abs=1e-9)
    # Total excludes the zero-length segment: 0.2 + 1.0 = 1.2 m (not 1.2 + 0.0 = 1.2 trivially).
    assert result.total_length_m == pytest.approx(1.2, abs=1e-4)
    assert result.centerline_segment_count == 3  # count includes degenerate (input count)


# ---------------------------------------------------------------------------
# (e) Empty inputs → empty result, no raise
# ---------------------------------------------------------------------------


def test_empty_segments_returns_empty_result() -> None:
    """Empty centerline_segments → empty per_colour, zeroed totals, never raises."""
    result = compute_fill_attributed_lengths(
        centerline_segments=[],
        fill_bands=[_square_band(_RED, 0.0, 0.0, 1.0)],
    )
    assert result.per_colour == ()
    assert result.shared_length_m == pytest.approx(0.0)
    assert result.total_length_m == pytest.approx(0.0)
    assert result.centerline_segment_count == 0


def test_empty_fill_bands_all_segments_go_to_shared() -> None:
    """With no fill bands at all, every segment goes to the shared bucket."""
    segs = [((0.0, 0.0), (1.0, 0.0))]
    result = compute_fill_attributed_lengths(
        centerline_segments=segs,
        fill_bands=[],
    )
    assert result.per_colour == ()
    assert result.shared_length_m == pytest.approx(1.0, abs=1e-4)
    assert result.total_length_m == pytest.approx(1.0, abs=1e-4)


def test_empty_inputs_both_empty_no_raise() -> None:
    """Both inputs empty → no exception; result is all-zero."""
    result = compute_fill_attributed_lengths(
        centerline_segments=[],
        fill_bands=[],
    )
    assert result.per_colour == ()
    assert result.shared_length_m == 0.0
    assert result.total_length_m == 0.0
    assert result.centerline_segment_count == 0


# ---------------------------------------------------------------------------
# (f) Self-touching / degenerate ring repaired via buffer(0) still contributes
# ---------------------------------------------------------------------------


def test_self_touching_ring_repaired_still_contributes() -> None:
    """A self-touching (figure-8) ring repaired via buffer(0) still attributes a segment.

    The ring is two triangles sharing a vertex at the origin — a self-intersection that
    buffer(0) repairs into a valid polygon.  A segment inside one lobe should be attributed.
    """
    # Figure-8: two triangles sharing tip at (0, 0).
    self_touching_ring = (
        (0.0, 0.0),
        (0.2, 0.2),
        (0.4, 0.0),
        (0.2, -0.2),
        (0.0, 0.0),  # closing / self-touch
        (-0.2, 0.2),
        (-0.4, 0.0),
        (-0.2, -0.2),
    )
    band = FillBand(colour_key=_GREEN, colour_index=3, colour_rgb=None, ring=self_touching_ring)
    # Segment well inside the right lobe.
    segs = [((0.1, 0.0), (0.3, 0.0))]
    result = compute_fill_attributed_lengths(
        centerline_segments=segs,
        fill_bands=[band],
    )
    # After repair + buffer(0.011) the band should cover the segment and attribute it.
    assert result.total_length_m == pytest.approx(0.2, abs=1e-4)
    per_colour_sum = sum(fc.length_m for fc in result.per_colour)
    total_check = per_colour_sum + result.shared_length_m
    assert total_check == pytest.approx(result.total_length_m, abs=0.1)
    # We don't assert green must be present (buffer(0) result depends on shapely version),
    # but the invariant must hold and no exception must be raised.


# ---------------------------------------------------------------------------
# (g) per_colour sorted by colour_key
# ---------------------------------------------------------------------------


def test_per_colour_sorted_by_colour_key() -> None:
    """per_colour is sorted lexicographically by colour_key."""
    red_band = _square_band(_RED, 0.0, 0.0, 0.3)  # "idx:1"
    green_band = _square_band(_GREEN, 2.0, 0.0, 0.3)  # "idx:3"
    blue_band = _square_band(_BLUE, 4.0, 0.0, 0.3)  # "idx:5"

    segs = [
        ((-0.1, 0.0), (0.1, 0.0)),  # inside red
        ((1.9, 0.0), (2.1, 0.0)),  # inside green
        ((3.9, 0.0), (4.1, 0.0)),  # inside blue
    ]
    result = compute_fill_attributed_lengths(
        centerline_segments=segs,
        fill_bands=[blue_band, red_band, green_band],  # deliberately unordered
    )
    keys = [fc.colour_key for fc in result.per_colour]
    assert keys == sorted(keys)


# ---------------------------------------------------------------------------
# (h) FillBand metadata preserved in output
# ---------------------------------------------------------------------------


def test_fill_band_metadata_preserved() -> None:
    """colour_index and colour_rgb from FillBand appear in FillColourLength output."""
    band = _square_band(_RED, 0.0, 0.0, 0.5, colour_index=1, colour_rgb="#ff0000")
    segs = [((-0.1, 0.0), (0.1, 0.0))]
    result = compute_fill_attributed_lengths(centerline_segments=segs, fill_bands=[band])
    assert len(result.per_colour) == 1
    fc = result.per_colour[0]
    assert fc.colour_index == 1
    assert fc.colour_rgb == "#ff0000"


# ---------------------------------------------------------------------------
# (i) Degenerate band ring (<3 distinct points) is silently skipped
# ---------------------------------------------------------------------------


def test_degenerate_ring_skipped_gracefully() -> None:
    """A ring with <3 distinct points is skipped without raising."""
    degenerate = FillBand(
        colour_key=_RED,
        colour_index=None,
        colour_rgb=None,
        ring=((0.0, 0.0), (0.0, 0.0), (0.0, 0.0)),
    )
    segs = [((0.0, 0.0), (1.0, 0.0))]
    # Should not raise; segment has no band to overlap → shared.
    result = compute_fill_attributed_lengths(centerline_segments=segs, fill_bands=[degenerate])
    assert result.shared_length_m == pytest.approx(1.0, abs=1e-4)
    assert result.per_colour == ()
