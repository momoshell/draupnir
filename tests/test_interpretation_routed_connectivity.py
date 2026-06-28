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

    # Results must agree on the metre tallies regardless of input order.
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


# ---------------------------------------------------------------------------
# #669 mid-span tee split tests
# ---------------------------------------------------------------------------


def test_midspan_tee_split_connects_t_junction() -> None:
    """T-junction: horizontal trunk split at the perpendicular stub's start node.

    Arrange:
      - Green fill band: covers left portion of trunk (x ∈ [0, 0.45], y ∈ [-0.5, 0.5]).
        Trunk [0,1] has 45% inside band → 45/100 > 30% → trunk is green in base fn.
        Stub start at (0.5, 0): distance from band right edge (x=0.45+buffer=0.461) is
        ~0.039 m > nearest_max_m=0.030 → stub is shared in base fn.
      - seg0 [(0,0) → (1,0)] — horizontal trunk, 1.0 m, green in base fn.
      - seg1 [(0.5,0) → (0.5,0.1)] — perpendicular stub, 0.1 m (≤ connector_len_m=0.15).
        Its START (0.5,0) is interior to seg0 (t=0.5, perp=0.0 ≤ tee_tol_m).

    Without split (#667-only): stub's start node ≠ seg0's original endpoint nodes →
    run-graph sees no shared node between stub and trunk → stub stays shared.

    After #669 split: seg0 splits into [0,0.5] and [0.5,1]; split vertex has SAME node id
    as stub's start → connector gate fires (0.1 m ≤ 0.15 m) → stub inherits green.

    Assert:
      - stub inherits green (shared drops by 0.1 m vs base fn).
      - _assert_invariant holds.
      - green length == base_green + 0.1.
    """
    # Band covers left portion: x ∈ [-0.05, 0.45], y ∈ [-0.5, 0.5] (half ~0.25 at cx=0.2).
    green_band = _square_band(_GREEN, 0.2, 0.0, 0.25, colour_index=3)
    # Trunk [0,1] overlaps band from x=0 to x=0.45 → 45% overlap > 30% → green.
    # Stub start (0.5, 0): nearest to band right edge at x=0.261 (buffered) → dist ≈ 0.239 m
    # Wait: half=0.25 → right edge x=0.2+0.25=0.45; buffered = 0.45+0.011=0.461.
    # dist from (0.5,0) to band = 0.5 - 0.461 = 0.039 m > nearest_max_m=0.030 → shared ✓.

    segs = [
        ((0.0, 0.0), (1.0, 0.0)),  # seg0 — trunk (green in base fn: 45% overlap)
        ((0.5, 0.0), (0.5, 0.1)),  # seg1 — perpendicular stub, 0.1 m; shared in base fn
    ]
    fill_bands = [green_band]

    base = compute_fill_attributed_lengths(
        centerline_segments=segs,
        fill_bands=fill_bands,
        nearest_max_m=0.030,
    )
    # Confirm: trunk is green, stub is shared in base fn.
    base_green = {fc.colour_key: fc.length_m for fc in base.per_colour}
    assert base.shared_length_m == pytest.approx(0.1, abs=0.01)
    assert _GREEN in base_green

    refined = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=fill_bands,
        tee_tol_m=0.030,
        connector_len_m=0.15,
        collinear_dot_min=0.98,
        nearest_max_m=0.030,
    )

    _assert_invariant(refined, base)
    # Stub must now be attributed to green via tee-split + connector gate.
    green_lengths = {fc.colour_key: fc.length_m for fc in refined.per_colour}
    assert _GREEN in green_lengths
    assert refined.shared_length_m == pytest.approx(0.0, abs=0.01)
    assert green_lengths[_GREEN] == pytest.approx(base_green[_GREEN] + 0.1, abs=0.01)


def test_midspan_split_length_conservation() -> None:
    """Mid-span tee split is length-conserving: Σ sub-lengths == parent length.

    Arrange:
      - Green band covering only the left half of trunk [0, 0.5].
      - seg0 [(0,0) → (1,0)] — trunk, 1.0 m; left half green, right half shared.
      - seg1 [(0.75,0) → (0.75,0.05)] — short stub, 0.05 m; start is interior to seg0
        (t=0.75, perp=0.0). Stub's start is NOT inside the green band → stub stays shared.

    After split: seg0 splits into [0,0.75] and [0.75,1]; Σ = 0.75 + 0.25 = 1.0 (exact).

    Assert:
      - refined.total_length_m == pytest.approx(base.total_length_m, abs=1e-6).
      - Σ(per_colour) + shared == total within 1e-6.
    """
    # Green band covers only [0, 0.5] of the trunk.
    green_band = _square_band(_GREEN, 0.25, 0.0, 0.26, colour_index=3)

    segs = [
        ((0.0, 0.0), (1.0, 0.0)),  # seg0 — trunk, 1.0 m
        ((0.75, 0.0), (0.75, 0.05)),  # seg1 — short stub, start interior to seg0
    ]
    fill_bands = [green_band]

    base = compute_fill_attributed_lengths(
        centerline_segments=segs,
        fill_bands=fill_bands,
        nearest_max_m=0.030,
    )

    refined = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=fill_bands,
        tee_tol_m=0.030,
        connector_len_m=0.15,
        nearest_max_m=0.030,
    )

    _assert_invariant(refined, base)
    assert refined.total_length_m == pytest.approx(base.total_length_m, abs=1e-6)
    per_colour_sum = sum(fc.length_m for fc in refined.per_colour)
    assert per_colour_sum + refined.shared_length_m == pytest.approx(
        refined.total_length_m, abs=1e-6
    )


def test_split_on_endpoint_guard_no_split() -> None:
    """A node coincident with an edge ENDPOINT is not an interior split.

    Arrange:
      - seg0 [(0,0) → (0.5,0)]; seg1 [(0.5,0) → (0.5,0.3)].
      - seg1's start (0.5,0) = seg0's END — not interior; t=1.0 / arc-length guard fires.
      - No split should occur.

    Assert: all fields equal a run where no split applies (per_colour, shared, total, count).
    """
    green_band = _square_band(_GREEN, 0.25, 0.0, 0.26, colour_index=3)

    segs = [
        ((0.0, 0.0), (0.5, 0.0)),  # seg0 — horizontal
        ((0.5, 0.0), (0.5, 0.3)),  # seg1 — vertical; start == seg0's end (endpoint, not interior)
    ]
    fill_bands = [green_band]

    # Reference: a geometry where seg1's start is clearly not interior to any edge.
    result_no_split = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=fill_bands,
        tee_tol_m=0.030,
        connector_len_m=0.15,
        nearest_max_m=0.030,
    )

    # With a far-away geometry (no tee possible): same segs, tee_tol_m=0 (never triggers).
    result_zero_tee = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=fill_bands,
        tee_tol_m=0.0,
        connector_len_m=0.15,
        nearest_max_m=0.030,
    )

    # Both runs must yield the same result (the endpoint guard prevents any split).
    assert result_no_split.shared_length_m == pytest.approx(
        result_zero_tee.shared_length_m, abs=1e-9
    )
    assert result_no_split.total_length_m == pytest.approx(result_zero_tee.total_length_m, abs=1e-9)
    assert result_no_split.centerline_segment_count == result_zero_tee.centerline_segment_count
    r1_colours = {fc.colour_key: fc.length_m for fc in result_no_split.per_colour}
    r2_colours = {fc.colour_key: fc.length_m for fc in result_zero_tee.per_colour}
    assert r1_colours == pytest.approx(r2_colours, abs=1e-9)


def test_no_tees_byte_identical_to_667_668() -> None:
    """Geometry with NO interior-projecting nodes: output equals the #667/#668 documented values.

    Reuses the Case-2 collinear fixture (two green segments + bridge) — all nodes are
    endpoints of their own segments, so no node can project interior to another edge.

    Expected values are the documented #667-only result: bridge attributed to green, shared=0.
    """
    # Same fixture as test_collinear_shared_segment_inherits_sole_neighbour_colour.
    left_band = _square_band(_GREEN, 0.2, 0.0, 0.25, colour_index=3)
    right_band = _square_band(_GREEN, 1.0, 0.0, 0.25, colour_index=3)

    segs = [
        ((0.0, 0.0), (0.5, 0.0)),  # seg0 — inside left green band
        ((0.5, 0.0), (0.7, 0.0)),  # seg1 — bridge (shared, 0.2 m)
        ((0.7, 0.0), (1.2, 0.0)),  # seg2 — inside right green band
    ]
    fill_bands = [left_band, right_band]

    # #667-only reference (no tee split possible — no interior projections).
    ref = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=fill_bands,
        tee_tol_m=0.0,  # disable tee gate entirely
        connector_len_m=0.10,
        collinear_dot_min=0.98,
        nearest_max_m=0.030,
    )

    # With tee_tol_m=0.030 — same geometry, no interior projections exist.
    result = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=fill_bands,
        tee_tol_m=0.030,
        connector_len_m=0.10,
        collinear_dot_min=0.98,
        nearest_max_m=0.030,
    )

    # Fields must be identical (no tee split occurred).
    assert result.shared_length_m == ref.shared_length_m
    assert result.total_length_m == ref.total_length_m
    assert result.per_colour == ref.per_colour
    assert result.centerline_segment_count == ref.centerline_segment_count

    # Documented #667-only expected values: bridge inherits green, shared = 0.
    assert result.shared_length_m == pytest.approx(0.0, abs=0.02)
    base = compute_fill_attributed_lengths(
        centerline_segments=segs,
        fill_bands=fill_bands,
        nearest_max_m=0.030,
    )
    _assert_invariant(result, base)


def test_midspan_split_permutation_stable() -> None:
    """T-junction result is stable under input permutation.

    Mirror of test_determinism_shuffled_inputs for the #669 T-junction fixture.
    """
    # Green band covering the entire trunk.
    green_band = _square_band(_GREEN, 0.5, 0.0, 0.6, colour_index=3)
    # Extra isolated segment to add variety.
    isolated = ((5.0, 0.0), (5.5, 0.0))

    trunk = ((0.0, 0.0), (1.0, 0.0))
    stub = ((0.5, 0.0), (0.5, 0.1))

    segs_ordered = [trunk, stub, isolated]
    segs_shuffled = [isolated, stub, trunk]

    fill_bands = [green_band]
    r1 = refine_shared_by_connectivity(
        centerline_segments=segs_ordered,
        fill_bands=fill_bands,
        tee_tol_m=0.030,
        connector_len_m=0.15,
        collinear_dot_min=0.98,
        nearest_max_m=0.030,
    )
    r2 = refine_shared_by_connectivity(
        centerline_segments=segs_shuffled,
        fill_bands=fill_bands,
        tee_tol_m=0.030,
        connector_len_m=0.15,
        collinear_dot_min=0.98,
        nearest_max_m=0.030,
    )

    assert r1.shared_length_m == pytest.approx(r2.shared_length_m, abs=1e-9)
    assert r1.total_length_m == pytest.approx(r2.total_length_m, abs=1e-9)
    r1_colours = {fc.colour_key: fc.length_m for fc in r1.per_colour}
    r2_colours = {fc.colour_key: fc.length_m for fc in r2.per_colour}
    assert r1_colours.keys() == r2_colours.keys()
    for ck in r1_colours:
        assert r1_colours[ck] == pytest.approx(r2_colours[ck], abs=1e-9)


def test_edge_case_one_node_onto_two_edges_single_shared_node() -> None:
    """Keystone: one node projects onto the interior of TWO edges; both split at the same node.

    GENUINE regression guard for Approach A (carry the SOURCE node's grid id), constructed so
    that naive ``_snap(P)`` on the projection foot DIVERGES from the source node's id — i.e.
    this test FAILS if the split vertex used ``_snap((px, py))`` instead of the carried ``nid``.

    The divergence is engineered by offsetting each trunk in its perpendicular direction so the
    projection foot lands in a DIFFERENT grid row than the source node's cell:

      - Source node n = stub start (0.510, 0.0).  snap_tol_m=0.030 → nid = (17, 0); the
        cell-center used for projection is c = (0.510, 0.0).
      - Trunk A horizontal at y=+0.020: foot of c = (0.510, 0.020) → naive _snap = (17, 1).
      - Trunk B horizontal at y=-0.020: foot of c = (0.510, -0.020) → naive _snap = (17, -1).
      - Approach A carries nid=(17,0) at BOTH split vertices; the stub's start node is also
        (17,0) → genuine three-way junction → stub inherits green.
      - Under naive _snap(P): A-vertex=(17,1), B-vertex=(17,-1), stub=(17,0) — all distinct →
        the stub has no green neighbour → stays shared → this test's green assertion FAILS.

    Both trunks are banded only on their LEFT portion (x∈[0,0.45]) so each is green in the base
    fn (45% overlap) while the stub at x=0.510 stays clear of every band (>0.030 m) → shared.
    The stub descends to y=-0.10 (crosses trunk B's *line* at y=-0.020, but that crossing is
    not a node, so it triggers no split; the stub's far end is >0.030 m from both trunks).
    """
    # Thin left-portion bands so trunks are green but the stub (x≈0.510) is clear of bands.
    band_a = FillBand(
        colour_key=_GREEN,
        colour_index=3,
        colour_rgb=None,
        ring=((0.0, 0.010), (0.45, 0.010), (0.45, 0.030), (0.0, 0.030)),
    )
    band_b = FillBand(
        colour_key=_GREEN,
        colour_index=3,
        colour_rgb=None,
        ring=((0.0, -0.030), (0.45, -0.030), (0.45, -0.010), (0.0, -0.010)),
    )

    segs = [
        ((0.0, 0.020), (1.0, 0.020)),  # A — horizontal trunk at y=+0.020 (green)
        ((0.0, -0.020), (1.0, -0.020)),  # B — horizontal trunk at y=-0.020 (green)
        ((0.510, 0.0), (0.510, -0.10)),  # stub — start n=(0.510,0); 0.10 m ≤ connector_len_m
    ]
    fill_bands = [band_a, band_b]

    base = compute_fill_attributed_lengths(
        centerline_segments=segs,
        fill_bands=fill_bands,
        nearest_max_m=0.030,
    )
    # Stub shared in base fn (>0.030 m from every band).
    assert base.shared_length_m == pytest.approx(0.10, abs=0.01)

    refined = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=fill_bands,
        tee_tol_m=0.030,
        connector_len_m=0.15,
        collinear_dot_min=0.98,
        nearest_max_m=0.030,
    )

    _assert_invariant(refined, base)
    # Stub must be attributed to green — only possible if BOTH split vertices carry nid=(17,0)
    # (Approach A).  Under naive _snap(P) the stub would stay shared and this would fail.
    assert refined.shared_length_m == pytest.approx(0.0, abs=0.01)
    green_lengths = {fc.colour_key: fc.length_m for fc in refined.per_colour}
    assert _GREEN in green_lengths


def test_edge_case_split_coincident_with_third_node_merges() -> None:
    """Two tee feet within snap_tol_m of each other dedupe to a SINGLE split node (no slivers).

    Edge case 2: two distinct source nodes project onto the SAME trunk at feet only 0.008 m
    apart (< snap_tol_m=0.030).  The per-edge dedupe must collapse them to one split point so
    no zero/sliver sub-segment is created and length is conserved.  Both source nodes also fall
    in grid cell (17,0), so both stubs share that node with the trunk sub-segment and inherit
    green.  The trunk is banded only on its left portion so both stubs are shared in the base
    fn; only the split + connector gate can attribute them.

    Geometry:
      - seg0 [(0,0)→(1,0)] green trunk; band covers x∈[0,0.45] (thin) → green; x≈0.50 clear.
      - seg1 [(0.500, 0.004)→(0.500, 0.12)] — start projects onto seg0 at (0.500, 0), nid=(17,0).
      - seg2 [(0.508,-0.004)→(0.508,-0.12)] — start projects onto seg0 at (0.508, 0), nid=(17,0).
      - feet (0.500,0) & (0.508,0): 0.008 m apart < snap_tol_m → dedupe to ONE split at (17,0).

    Assert: both stubs inherit green; shared→0; _assert_invariant; total length conserved
    (the dedupe prevented a 0.008 m sliver sub-segment from corrupting the tally).
    """
    band = FillBand(
        colour_key=_GREEN,
        colour_index=3,
        colour_rgb=None,
        ring=((0.0, -0.020), (0.45, -0.020), (0.45, 0.020), (0.0, 0.020)),
    )

    segs = [
        ((0.0, 0.0), (1.0, 0.0)),  # seg0 — horizontal trunk (green)
        ((0.500, 0.004), (0.500, 0.12)),  # seg1 — stub up; start projects at (0.500, 0)
        ((0.508, -0.004), (0.508, -0.12)),  # seg2 — stub down; start projects at (0.508, 0)
    ]
    fill_bands = [band]

    base = compute_fill_attributed_lengths(
        centerline_segments=segs,
        fill_bands=fill_bands,
        nearest_max_m=0.030,
    )
    # Both stubs shared in base fn (>0.030 m from the band).
    assert base.shared_length_m == pytest.approx(0.116 + 0.116, abs=0.02)

    refined = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=fill_bands,
        tee_tol_m=0.030,
        connector_len_m=0.15,
        nearest_max_m=0.030,
    )

    _assert_invariant(refined, base)
    assert refined.total_length_m == pytest.approx(base.total_length_m, abs=1e-6)
    # Dedupe → single split → both connectors attach to green; nothing left shared.
    assert refined.shared_length_m == pytest.approx(0.0, abs=0.01)
    green_lengths = {fc.colour_key: fc.length_m for fc in refined.per_colour}
    assert _GREEN in green_lengths


def test_long_perpendicular_tee_stays_shared() -> None:
    """Tee split occurs but a LONG perpendicular stub still fails both gates → stays shared.

    The honesty guard: splitting the trunk creates a shared green neighbour for the stub,
    but the stub is 0.5 m (> connector_len_m=0.15) AND perpendicular (dot=0 < collinear_dot_min).
    Both gates fail → stub stays shared despite the tee split.

    Arrange:
      - Green band: left portion of trunk only; cx=0.2, half=0.25 -> covers x=[-0.05,0.45],
        buffered to x=[-0.061, 0.461].
        Trunk [0,1] overlap ≈ 46% > 30% → green in base fn.
        Stub start at (0.5, 0): distance to buffered band right edge = 0.039 m > nearest_max_m=0.030
        → stub is shared in base fn.
      - seg0 [(0,0) → (1,0)] — trunk (green).
      - seg1 [(0.5,0) → (0.5,0.5)] — LONG perpendicular stub (0.5 m > connector_len_m=0.15).
        Start (0.5,0) is interior to seg0 (t=0.5, perp=0 ≤ tee_tol_m=0.030) → split OCCURS.

    After split: seg0 splits into [0,0.5] and [0.5,1], both green. Stub has sole green neighbour.
      - Connector gate: 0.5 m > 0.15 m → fails.
      - Collinear gate: dot(stub, trunk) = 0 < 0.98 → fails.
    → stub STAYS shared (honesty: ambiguous geometry must not be guessed).

    Assert:
      - stub stays shared (shared ≈ 0.5 m after refinement).
      - _assert_invariant holds.
    """
    # Band covers left portion of trunk; stub at x=0.5 is outside band x range.
    green_band = _square_band(_GREEN, 0.2, 0.0, 0.25, colour_index=3)

    segs = [
        ((0.0, 0.0), (1.0, 0.0)),  # seg0 — trunk (green: 46% overlap with band)
        ((0.5, 0.0), (0.5, 0.5)),  # seg1 — long perp stub (0.5 m); shared in base fn
    ]
    fill_bands = [green_band]

    base = compute_fill_attributed_lengths(
        centerline_segments=segs,
        fill_bands=fill_bands,
        nearest_max_m=0.030,
    )
    # Confirm stub is shared in base fn (distance 0.039 m > 0.030).
    assert base.shared_length_m == pytest.approx(0.5, abs=0.01)

    refined = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=fill_bands,
        tee_tol_m=0.030,  # split OCCURS (perp=0 ≤ 0.030) but gates fail
        connector_len_m=0.15,  # stub (0.5 m) > threshold → connector gate fails
        collinear_dot_min=0.98,  # dot=0 → collinear gate fails
        nearest_max_m=0.030,
    )

    _assert_invariant(refined, base)
    # Stub must remain shared — both gates fail regardless of the tee split.
    assert refined.shared_length_m == pytest.approx(0.5, abs=0.01)


# ---------------------------------------------------------------------------
# Regression: zero-length segment interleaved before a real segment must NOT
# shift verdict alignment (issue #761 / _segment_verdicts contract).
# ---------------------------------------------------------------------------


def test_zero_length_segment_does_not_shift_verdict_alignment() -> None:
    """Zero-length segment interleaved BEFORE a real segment: the real segment's
    colour attribution must be correct.

    Regression guard for the contract that _segment_verdicts skips zero-length
    segments entirely (so routed_connectivity's nondegen_segs + verdicts stay
    aligned).  A previous change emitted (0.0, None) for zero-length inputs,
    which shifted verdicts[i] for all subsequent real segments.

    Arrange:
      - seg0: zero-length (degenerate) at (0, 0)
      - seg1: inside the red band (0.2 m long)
    Red band centred at (0, 0), half-side 0.5 m → seg1 overlaps heavily.
    """
    red_band = _square_band(_RED, 0.0, 0.0, 0.5, colour_index=1)
    segs = [
        ((0.0, 0.0), (0.0, 0.0)),  # zero-length — must not consume a verdict slot
        ((-0.1, 0.0), (0.1, 0.0)),  # inside red band — must still get _RED attribution
    ]

    base = compute_fill_attributed_lengths(
        centerline_segments=segs,
        fill_bands=[red_band],
    )
    # The real segment must be attributed to red, not lost.
    assert len(base.per_colour) == 1
    assert base.per_colour[0].colour_key == _RED
    assert base.per_colour[0].length_m == pytest.approx(0.2, abs=1e-4)
    assert base.shared_length_m == pytest.approx(0.0, abs=1e-4)

    # segment_attribution must be 2 entries (one per input), with None for the
    # zero-length slot and _RED for the real segment — NOT shifted.
    assert len(base.segment_attribution) == 2
    assert base.segment_attribution[0] is None  # zero-length → neutral None
    assert base.segment_attribution[1] == _RED  # real segment → correct colour

    # refine_shared_by_connectivity must also handle the interleaved zero correctly.
    refined = refine_shared_by_connectivity(
        centerline_segments=segs,
        fill_bands=[red_band],
    )
    _assert_invariant(refined, base)
    # The real segment's length must still appear in some colour bucket (red or a
    # refined attribution); nothing should be lost.
    refined_colour_total = sum(fc.length_m for fc in refined.per_colour)
    assert refined_colour_total == pytest.approx(0.2, abs=0.05)
