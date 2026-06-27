"""Synthetic unit tests for app.interpretation.routed_connectivity (issue #667, PR-1).

All geometry in metres; dict-coord style not used (segments are tuple pairs directly).
No DB, ORM, FastAPI.
"""

from __future__ import annotations

import pytest

from app.interpretation.routed_connectivity import refine_shared_by_connectivity
from app.interpretation.service_fill_takeoff import (
    FillAttributionResult,
    FillBand,
    compute_fill_attributed_lengths,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_GREEN = "idx:3"
_RED = "idx:1"
_BLUE = "idx:5"


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


def _assert_invariant(result: FillAttributionResult, base: FillAttributionResult) -> None:
    """Shared invariant checks: conservation, total and count match base fn."""
    per_colour_sum = sum(fc.length_m for fc in result.per_colour)
    assert per_colour_sum + result.shared_length_m == pytest.approx(
        result.total_length_m, abs=0.1
    ), "Conservation invariant violated"
    assert result.total_length_m == pytest.approx(base.total_length_m, abs=0.1)
    assert result.centerline_segment_count == base.centerline_segment_count


# ---------------------------------------------------------------------------
# Case 1: Short connector touching only one colour's chain → inherits it
# ---------------------------------------------------------------------------


def test_short_connector_inherits_sole_neighbour_colour() -> None:
    """Short connector (<= connector_len_m) adjacent only to green segments inherits green.

    Arrange:
      - Green band is a narrow square covering only x in [-0.05, 0.25] (half=0.15 at cx=0.1).
      - seg0 [(0.0, 0.0), (0.2, 0.0)] — inside green (attributed by base fn).
      - seg2 [(0.2, 0.0), (0.3, 0.0)] — short connector (0.1 m); distance to band edge
        (at x=0.25+buffer=0.261) is ~0.039 m which is larger than nearest_max_m=0.030.
        So base fn puts seg2 in shared.
      - seg2 shares its left endpoint (0.2, 0.0) with seg0 (right endpoint).
    Act:     refine_shared_by_connectivity with connector_len_m=0.15, nearest_max_m=0.030.
    Assert:  seg2 inherits green; shared drops by 0.1 m.
    """
    # Band centred at cx=0.025, half=0.03 → right edge at x=0.055, buffered to x=0.066.
    # seg0: [0.0, 0.1] — overlaps band by ~66% (> 30%) → attributed by base fn.
    # seg2: [0.1, 0.2] — distance from band ~0.034 m > nearest_max_m=0.030 → shared in base fn.
    # seg2 shares its left endpoint (0.1, 0.0) with seg0's right endpoint.
    green_band = _square_band(_GREEN, 0.025, 0.0, 0.03, colour_index=3)

    segs = [
        ((0.0, 0.0), (0.1, 0.0)),  # seg0 — overlaps green band → attributed by base fn
        ((0.1, 0.0), (0.2, 0.0)),  # seg2 — connector (0.1 m); distance ~0.034 m > nearest_max_m
    ]
    fill_bands = [green_band]

    base = compute_fill_attributed_lengths(
        centerline_segments=segs,
        fill_bands=fill_bands,
        nearest_max_m=0.030,  # tight — seg2 at ~0.034 m stays shared
    )
    # Confirm base fn puts seg2 in shared.
    assert base.shared_length_m == pytest.approx(0.1, abs=0.01)

    refined = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=fill_bands,
        connector_len_m=0.15,
        nearest_max_m=0.030,  # same tight threshold
    )

    _assert_invariant(refined, base)

    # Connector should now be attributed to green.
    green_lengths = {fc.colour_key: fc.length_m for fc in refined.per_colour}
    assert _GREEN in green_lengths
    # Shared should be 0 (connector was the only shared segment).
    assert refined.shared_length_m == pytest.approx(0.0, abs=0.01)
    # Green should have gained connector length.
    base_green = {fc.colour_key: fc.length_m for fc in base.per_colour}
    assert green_lengths[_GREEN] == pytest.approx(base_green.get(_GREEN, 0.0) + 0.1, abs=0.01)


# ---------------------------------------------------------------------------
# Case 2: Near-collinear shared segment with only-green neighbours → green via collinearity
# ---------------------------------------------------------------------------


def test_collinear_shared_segment_inherits_sole_neighbour_colour() -> None:
    """A shared segment length > connector_len_m qualifies via the collinearity gate.

    Arrange:
      - Two green segments on the x-axis: [0.0, 0.5] and [0.7, 1.2].
      - A shared segment [0.5, 0.7] (0.2 m — longer than connector_len_m=0.15) bridging them.
        All three are collinear (dot product = 1.0 ≥ 0.98).
      - Green band does NOT cover [0.5, 0.7] and distance > nearest_max_m so base fn
        puts the bridge in shared.
    Act:     refine_shared_by_connectivity.
    Assert:  bridge is attributed to green via collinearity; shared drops by 0.2 m.
    """
    # left band: cx=0.2, half=0.25 → right edge at 0.45, buffered to 0.461.
    # right band: cx=1.0, half=0.25 → left edge at 0.75, buffered to 0.739.
    # Bridge [0.5, 0.7]: distance to left band ~0.039 m, to right band ~0.039 m.
    # With nearest_max_m=0.030, bridge is beyond threshold → shared in base fn.
    left_band = _square_band(_GREEN, 0.2, 0.0, 0.25, colour_index=3)
    right_band = _square_band(_GREEN, 1.0, 0.0, 0.25, colour_index=3)

    segs = [
        ((0.0, 0.0), (0.5, 0.0)),  # seg0 — inside left green band
        ((0.5, 0.0), (0.7, 0.0)),  # seg1 — bridge (shared, 0.2 m)
        ((0.7, 0.0), (1.2, 0.0)),  # seg2 — inside right green band
    ]
    fill_bands = [left_band, right_band]

    base = compute_fill_attributed_lengths(
        centerline_segments=segs,
        fill_bands=fill_bands,
        nearest_max_m=0.030,  # bridge at ~0.039 m stays shared
    )
    assert base.shared_length_m == pytest.approx(0.2, abs=0.02)

    refined = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=fill_bands,
        connector_len_m=0.10,  # 0.2 m bridge EXCEEDS this → collinearity gate triggers
        collinear_dot_min=0.98,
        nearest_max_m=0.030,
    )

    _assert_invariant(refined, base)
    assert refined.shared_length_m == pytest.approx(0.0, abs=0.02)
    green_lengths = {fc.colour_key: fc.length_m for fc in refined.per_colour}
    assert _GREEN in green_lengths


# ---------------------------------------------------------------------------
# Case 3: Shared segment touching TWO distinct colours → stays shared
# ---------------------------------------------------------------------------


def test_two_distinct_neighbour_colours_stays_shared() -> None:
    """A shared segment adjacent to both red and green neighbours stays shared.

    Arrange:
      - green band: cx=0.2, half=0.25 → right edge buffered to ~0.461; seg0 [0.0, 0.5] overlaps.
      - red band: cx=1.0, half=0.25 → left edge buffered to ~0.739; seg2 [0.6, 1.1] overlaps.
      - seg1 [0.5, 0.6] — junction segment; nearest band (green) at ~0.039 m > nearest_max_m=0.030
        AND nearest band (red) at ~0.139 m, both beyond threshold → shared in base fn.
      seg1 shares its left node with seg0 (green) and its right node with seg2 (red) → two distinct
      colours in the run graph → connectivity cannot resolve → stays shared.
    Act:     refine_shared_by_connectivity.
    Assert:  seg1 stays shared; no metres moved.
    """
    green_band = _square_band(_GREEN, 0.2, 0.0, 0.25, colour_index=3)
    red_band = _square_band(_RED, 1.0, 0.0, 0.25, colour_index=1)

    segs = [
        ((0.0, 0.0), (0.5, 0.0)),  # inside green
        ((0.5, 0.0), (0.6, 0.0)),  # shared junction segment
        ((0.6, 0.0), (1.1, 0.0)),  # inside red
    ]
    fill_bands = [green_band, red_band]

    base = compute_fill_attributed_lengths(
        centerline_segments=segs,
        fill_bands=fill_bands,
        nearest_max_m=0.030,
    )
    assert base.shared_length_m == pytest.approx(0.1, abs=0.02)

    refined = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=fill_bands,
        connector_len_m=0.15,
        nearest_max_m=0.030,
    )

    _assert_invariant(refined, base)
    # seg1 must remain shared — two competing colours.
    assert refined.shared_length_m == pytest.approx(base.shared_length_m, abs=0.01)


# ---------------------------------------------------------------------------
# Case 4: Single neighbour colour but gate fails (perpendicular T, dot < 0.98, len > 0.15)
# ---------------------------------------------------------------------------


def test_perpendicular_long_segment_stays_shared() -> None:
    """A perpendicular stub that fails both gates stays shared.

    Arrange:
      - green band at cx=0.025, half=0.03; buffered right edge ~0.066 m.
      - seg0 [(0.0, 0.0), (0.1, 0.0)] overlaps the band (attributed to green by base fn).
      - seg1 [(0.1, 0.0), (0.1, 0.5)] perpendicular stub (0.5 m > connector_len_m=0.15).
        Nearest distance from stub to band: 0.1 - 0.066 = 0.034 m > nearest_max_m=0.030 -> shared
        in base fn.  Shares its left endpoint (0.1, 0.0) with seg0 -> green is the sole neighbour
        colour in the run graph.  Dot product (horizontal x vertical) = 0 << 0.98 -> collinearity
        gate fails; length 0.5 m > 0.15 m -> connector gate also fails -> stays shared.
    Act:     refine_shared_by_connectivity.
    Assert:  seg1 stays shared; no metres moved.
    """
    # Band: cx=0.025, half=0.03 → buffered right edge ~0.066 m.
    green_band = _square_band(_GREEN, 0.025, 0.0, 0.03, colour_index=3)

    segs = [
        ((0.0, 0.0), (0.1, 0.0)),  # seg0 — overlaps green band → attributed
        ((0.1, 0.0), (0.1, 0.5)),  # seg1 — perpendicular stub, 0.5 m, ~0.034 m from band
    ]
    fill_bands = [green_band]

    base = compute_fill_attributed_lengths(
        centerline_segments=segs,
        fill_bands=fill_bands,
        nearest_max_m=0.030,  # stub at 0.034 m → shared
    )
    assert base.shared_length_m == pytest.approx(0.5, abs=0.01)

    refined = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=fill_bands,
        connector_len_m=0.15,  # stub (0.5 m) > threshold
        collinear_dot_min=0.98,  # dot=0 fails
        nearest_max_m=0.030,
    )

    _assert_invariant(refined, base)
    assert refined.shared_length_m == pytest.approx(base.shared_length_m, abs=0.01)


# ---------------------------------------------------------------------------
# Case 5: Invariant in every above case (already checked via _assert_invariant)
# ---------------------------------------------------------------------------
# Invariant checks are embedded in _assert_invariant called by each case.
# This dedicated test validates it explicitly for a multi-colour scenario.


def test_invariant_multi_colour_scenario() -> None:
    """Σ(per_colour)+shared==total AND total/count unchanged vs base fn for multi-colour."""
    green_band = _square_band(_GREEN, 0.2, 0.0, 0.25, colour_index=3)
    red_band = _square_band(_RED, 1.0, 0.0, 0.25, colour_index=1)

    segs = [
        ((0.0, 0.0), (0.5, 0.0)),
        ((0.5, 0.0), (0.6, 0.0)),
        ((0.6, 0.0), (1.1, 0.0)),
        ((2.0, 0.0), (3.0, 0.0)),  # completely isolated shared
    ]
    fill_bands = [green_band, red_band]

    base = compute_fill_attributed_lengths(
        centerline_segments=segs, fill_bands=fill_bands, nearest_max_m=0.030
    )
    refined = refine_shared_by_connectivity(
        centerline_segments=segs, fill_bands=fill_bands, nearest_max_m=0.030
    )

    _assert_invariant(refined, base)


# ---------------------------------------------------------------------------
# Case 6: Determinism — shuffled segments+bands give identical result
# ---------------------------------------------------------------------------


def test_determinism_shuffled_inputs() -> None:
    """Shuffling segments and bands does not change the refined result.

    Arrange: same fixture as Case 1 (green band at cx=0.025, half=0.03) with permuted segments.
    Assert:  per_colour and shared_length_m are identical.
    """
    # Same band as Case 1: seg0 [0.0,0.1] attributed, seg1 [0.1,0.2] connector (shared in base fn).
    green_band = _square_band(_GREEN, 0.025, 0.0, 0.03, colour_index=3)

    segs_ordered = [
        ((0.0, 0.0), (0.1, 0.0)),
        ((0.1, 0.0), (0.2, 0.0)),
    ]
    segs_shuffled = [
        ((0.1, 0.0), (0.2, 0.0)),
        ((0.0, 0.0), (0.1, 0.0)),
    ]
    fill_bands = [green_band]

    kwargs = {
        "fill_bands": fill_bands,
        "connector_len_m": 0.15,
        "nearest_max_m": 0.030,
    }

    r1 = refine_shared_by_connectivity(centerline_segments=segs_ordered, **kwargs)  # type: ignore[arg-type]
    r2 = refine_shared_by_connectivity(centerline_segments=segs_shuffled, **kwargs)  # type: ignore[arg-type]

    # Results must agree on totals (segment count is input-order dependent so we compare metres).
    assert r1.shared_length_m == pytest.approx(r2.shared_length_m, abs=0.01)
    assert r1.total_length_m == pytest.approx(r2.total_length_m, abs=0.001)
    r1_colours = {fc.colour_key: fc.length_m for fc in r1.per_colour}
    r2_colours = {fc.colour_key: fc.length_m for fc in r2.per_colour}
    assert r1_colours.keys() == r2_colours.keys()
    for ck in r1_colours:
        assert r1_colours[ck] == pytest.approx(r2_colours[ck], abs=0.01)


# ---------------------------------------------------------------------------
# Case 7: Empty inputs → no-op, no raise, equals base fn result
# ---------------------------------------------------------------------------


def test_empty_segments_no_raise_equals_base() -> None:
    """No segments → empty result matching base fn; no exception."""
    fill_bands = [_square_band(_GREEN, 0.0, 0.0, 1.0, colour_index=3)]
    base = compute_fill_attributed_lengths(centerline_segments=[], fill_bands=fill_bands)
    refined = refine_shared_by_connectivity(centerline_segments=[], fill_bands=fill_bands)
    assert refined.per_colour == base.per_colour
    assert refined.shared_length_m == pytest.approx(base.shared_length_m)
    assert refined.total_length_m == pytest.approx(base.total_length_m)
    assert refined.centerline_segment_count == base.centerline_segment_count


def test_segments_no_bands_no_raise_equals_base() -> None:
    """Segments but no fill bands → all shared; result matches base fn; no exception."""
    segs = [((0.0, 0.0), (1.0, 0.0))]
    base = compute_fill_attributed_lengths(centerline_segments=segs, fill_bands=[])
    refined = refine_shared_by_connectivity(centerline_segments=segs, fill_bands=[])
    assert refined.per_colour == base.per_colour
    assert refined.shared_length_m == pytest.approx(base.shared_length_m, abs=0.001)
    assert refined.total_length_m == pytest.approx(base.total_length_m, abs=0.001)
    assert refined.centerline_segment_count == base.centerline_segment_count


# ---------------------------------------------------------------------------
# Case 8: Lone shared segment with no coloured neighbour → stays shared
# ---------------------------------------------------------------------------


def test_lone_shared_segment_no_coloured_neighbour_stays_shared() -> None:
    """A single segment with no connections and no neighbouring attributed segment stays shared.

    Arrange: one isolated segment, no fill bands → shared in base fn.
    Act:     refine_shared_by_connectivity.
    Assert:  stays shared.
    """
    segs = [((5.0, 0.0), (6.0, 0.0))]
    fill_bands: list[FillBand] = []

    base = compute_fill_attributed_lengths(centerline_segments=segs, fill_bands=fill_bands)
    refined = refine_shared_by_connectivity(centerline_segments=segs, fill_bands=fill_bands)

    _assert_invariant(refined, base)
    assert refined.shared_length_m == pytest.approx(1.0, abs=0.001)
    assert refined.per_colour == ()


# ---------------------------------------------------------------------------
# #668 fitting-bridge pass tests
# ---------------------------------------------------------------------------

_ORANGE = "c2ff8000"
_CYAN = "idx:4"


def _fitting_band(
    colour_key: str,
    cx: float,
    cy: float,
    half: float,
    colour_index: int | None = None,
    colour_rgb: str | None = None,
) -> FillBand:
    """Axis-aligned square fitting band (same shape helper as _square_band)."""
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


def test_fitting_bridge_single_colour_inherits() -> None:
    """Shared segment with an endpoint inside a single-colour fitting hatch inherits that colour.

    Arrange:
      - Orange fill band at cx=0.025, half=0.03 → covers only [0.0, 0.1] roughly.
      - seg0 [(0.0, 0.0) → (0.1, 0.0)] — inside fill band → orange in base fn.
      - seg1 [(0.1, 0.0) → (0.1, 0.4)] — perpendicular stub (0.4 m); far from fill band
        (distance ~0.034 m > nearest_max_m=0.030) → shared in base fn.
      - #667 pass: seg1 fails collinear gate (perpendicular, dot=0) and connector gate
        (0.4 m > 0.15 m) → stays shared after #667.
      - Orange fitting band centred at (0.1, 0.4), half=0.06 — covers seg1's far endpoint.
    Act:     refine_shared_by_connectivity with fitting_bands=[orange_fitting].
    Assert:  seg1 inherits orange via fitting bridge; shared drops to 0; invariant holds.
    """
    # Narrow fill band: barely covers seg0.
    orange_fill = _square_band(_ORANGE, 0.025, 0.0, 0.03, colour_rgb="c2ff8000")
    # Fitting band at the far end of the perpendicular segment.
    orange_fitting = _fitting_band(_ORANGE, 0.1, 0.4, 0.06, colour_rgb="c2ff8000")

    segs = [
        ((0.0, 0.0), (0.1, 0.0)),  # seg0 — horizontal, overlaps orange fill → orange
        ((0.1, 0.0), (0.1, 0.4)),  # seg1 — vertical (perpendicular), 0.4 m, shared
    ]

    base = compute_fill_attributed_lengths(
        centerline_segments=segs,
        fill_bands=[orange_fill],
        nearest_max_m=0.030,
    )
    # seg1 far from fill band → shared in base fn.
    assert base.shared_length_m == pytest.approx(0.4, abs=0.05)

    # Confirm #667-only leaves seg1 shared (fails both gates).
    result_667_only = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=[orange_fill],
        fitting_bands=(),
        connector_len_m=0.15,
        collinear_dot_min=0.98,
        nearest_max_m=0.030,
    )
    assert result_667_only.shared_length_m == pytest.approx(0.4, abs=0.05)

    # Now add the fitting band → seg1 inherits orange.
    refined = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=[orange_fill],
        fitting_bands=[orange_fitting],
        connector_len_m=0.15,
        collinear_dot_min=0.98,
        nearest_max_m=0.030,
    )

    _assert_invariant(refined, base)
    assert refined.shared_length_m == pytest.approx(0.0, abs=0.05)
    orange_lengths = {fc.colour_key: fc.length_m for fc in refined.per_colour}
    assert _ORANGE in orange_lengths
    # Per-colour metadata must be populated (the all_bands = fill + fitting merge feeds
    # colour_rgb/index even for a colour that a bridge introduced) — guards the metadata path.
    orange_fc = next(fc for fc in refined.per_colour if fc.colour_key == _ORANGE)
    assert orange_fc.colour_rgb == "c2ff8000"


def test_fitting_bridge_two_colour_stays_shared() -> None:
    """Shared segment with endpoints near TWO different-colour fittings stays shared.

    Arrange:
      - Orange fill: covers seg0 [(0.0,0.0)→(0.1,0.0)].
      - seg1 [(0.1,0.0)→(0.1,0.4)]: perpendicular, shared in base fn AND after #667.
      - Orange fitting band at (0.1, 0.0) → covers seg1's start.
      - Cyan fitting band at (0.1, 0.4) → covers seg1's end.
      - candidate_colours = {orange, cyan} → ≥2 → stays shared.
    """
    orange_fill = _square_band(_ORANGE, 0.025, 0.0, 0.03, colour_rgb="c2ff8000")
    orange_fitting = _fitting_band(_ORANGE, 0.1, 0.0, 0.06, colour_rgb="c2ff8000")
    cyan_fitting = _fitting_band(_CYAN, 0.1, 0.4, 0.06, colour_index=4)

    segs = [
        ((0.0, 0.0), (0.1, 0.0)),  # seg0 — horizontal, orange
        ((0.1, 0.0), (0.1, 0.4)),  # seg1 — perpendicular, 0.4 m, shared
    ]

    base = compute_fill_attributed_lengths(
        centerline_segments=segs,
        fill_bands=[orange_fill],
        nearest_max_m=0.030,
    )
    assert base.shared_length_m == pytest.approx(0.4, abs=0.05)

    refined = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=[orange_fill],
        fitting_bands=[orange_fitting, cyan_fitting],
        connector_len_m=0.15,
        collinear_dot_min=0.98,
        nearest_max_m=0.030,
    )

    _assert_invariant(refined, base)
    # Two different fitting colours → must stay shared.
    assert refined.shared_length_m == pytest.approx(base.shared_length_m, abs=0.01)


def test_fitting_bridge_empty_bands_identical_to_667() -> None:
    """Empty fitting_bands produces output byte-identical to #667-only (regression guard).

    Ensures that the fitting-bridge pass is a strict no-op when fitting_bands=().
    """
    green_band = _square_band(_GREEN, 0.025, 0.0, 0.03, colour_index=3)
    segs = [
        ((0.0, 0.0), (0.1, 0.0)),
        ((0.1, 0.0), (0.2, 0.0)),
    ]
    fill_bands = [green_band]

    result_no_fitting = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=fill_bands,
        connector_len_m=0.15,
        nearest_max_m=0.030,
    )
    result_empty_fitting = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=fill_bands,
        fitting_bands=[],
        connector_len_m=0.15,
        nearest_max_m=0.030,
    )
    result_empty_tuple = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=fill_bands,
        fitting_bands=(),
        connector_len_m=0.15,
        nearest_max_m=0.030,
    )

    # All three must be byte-identical in all fields.
    assert result_no_fitting.shared_length_m == result_empty_fitting.shared_length_m
    assert result_no_fitting.total_length_m == result_empty_fitting.total_length_m
    assert result_no_fitting.per_colour == result_empty_fitting.per_colour
    assert (
        result_no_fitting.centerline_segment_count == result_empty_fitting.centerline_segment_count
    )

    assert result_no_fitting.shared_length_m == result_empty_tuple.shared_length_m
    assert result_no_fitting.total_length_m == result_empty_tuple.total_length_m
    assert result_no_fitting.per_colour == result_empty_tuple.per_colour
    assert result_no_fitting.centerline_segment_count == result_empty_tuple.centerline_segment_count


def test_fitting_bridge_overlapping_unions_stays_shared() -> None:
    """A point inside two overlapping grown unions of different colours stays shared.

    Arrange:
      - Orange fill band (narrow): covers seg0 [(0.0,0.0)→(0.1,0.0)].
      - seg1 [(0.1,0.0)→(0.1,0.4)]: perpendicular stub, shared in base fn AND after #667.
      - Orange fitting band centred at (0.06, 0.0): grown union (half=0.06+buffer=0.011~0.071)
        reaches x=[-0.011, 0.131] -- contains (0.1, 0.0).
      - Cyan fitting band centred at (0.14, 0.0): grown union reaches x=[0.069, 0.211]
        — also contains (0.1, 0.0).
      - candidate_colours at seg1's start = {orange, cyan} → ≥2 → stays shared.
    """
    orange_fill = _square_band(_ORANGE, 0.025, 0.0, 0.03, colour_rgb="c2ff8000")
    # Both fitting bands' grown unions contain (0.1, 0.0).
    orange_fitting = _fitting_band(_ORANGE, 0.06, 0.0, 0.06, colour_rgb="c2ff8000")
    cyan_fitting = _fitting_band(_CYAN, 0.14, 0.0, 0.06, colour_index=4)

    segs = [
        ((0.0, 0.0), (0.1, 0.0)),  # seg0 — orange (covered by fill band)
        ((0.1, 0.0), (0.1, 0.4)),  # seg1 — perpendicular, shared
    ]

    base = compute_fill_attributed_lengths(
        centerline_segments=segs,
        fill_bands=[orange_fill],
        nearest_max_m=0.030,
    )
    assert base.shared_length_m == pytest.approx(0.4, abs=0.05)

    refined = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=[orange_fill],
        fitting_bands=[orange_fitting, cyan_fitting],
        connector_len_m=0.15,
        collinear_dot_min=0.98,
        nearest_max_m=0.030,
    )

    _assert_invariant(refined, base)
    # Overlapping unions at the start endpoint → ≥2 colours → must stay shared.
    assert refined.shared_length_m == pytest.approx(base.shared_length_m, abs=0.01)
