"""Tests for the bounded Voronoi room-partition primitive (issue #715, Phase R-B).

All tests use coordinate fixtures only — no DB, no FastAPI, no heavy deps.
"""

from __future__ import annotations

import random

import pytest

from app.interpretation.room_voronoi import (
    DEFAULT_BOUND_MARGIN_M,
    RoomTag,
    assign_point_to_room,
    floor_bound,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

M = DEFAULT_BOUND_MARGIN_M  # alias for readability

# Single tag centred at origin, single anchor.
_TAG_A = RoomTag(number="A.01", anchors=((0.0, 0.0),))

# Two tags side by side (bisector at x=5).
_TAG_LEFT = RoomTag(number="L.01", anchors=((0.0, 0.0),))
_TAG_RIGHT = RoomTag(number="R.01", anchors=((10.0, 0.0),))

# Multi-anchor U-shaped room (#581): two arms, one competitor in the middle.
_TAG_U = RoomTag(number="U.01", anchors=((-10.0, 0.0), (10.0, 0.0)))
_TAG_MID = RoomTag(number="M.02", anchors=((0.0, 0.0),))


# ---------------------------------------------------------------------------
# floor_bound
# ---------------------------------------------------------------------------


def test_floor_bound_empty_tags_returns_none() -> None:
    assert floor_bound([]) is None


def test_floor_bound_extent_simple() -> None:
    """Bbox = tag extent ± margin on every side."""
    tags = [
        RoomTag(number="1.01", anchors=((2.0, 3.0),)),
        RoomTag(number="1.02", anchors=((8.0, 7.0),)),
    ]
    result = floor_bound(tags, margin=1.0)
    assert result == (1.0, 2.0, 9.0, 8.0)


def test_floor_bound_default_margin() -> None:
    tags = [_TAG_A]
    result = floor_bound(tags)
    assert result == (-M, -M, M, M)


def test_floor_bound_multi_anchor_covers_all_placements() -> None:
    """All anchors of all tags feed the bbox, not just the first."""
    tag = RoomTag(number="X.01", anchors=((0.0, 0.0), (20.0, 10.0)))
    result = floor_bound([tag], margin=0.0)
    assert result == (0.0, 0.0, 20.0, 10.0)


# ---------------------------------------------------------------------------
# assign_point_to_room — basic cases
# ---------------------------------------------------------------------------


def test_empty_tags_returns_none() -> None:
    assert assign_point_to_room((0.0, 0.0), []) is None


def test_single_tag_in_bound() -> None:
    result = assign_point_to_room((0.0, 0.0), [_TAG_A])
    assert result == "A.01"


def test_single_tag_out_of_bound() -> None:
    """A point far outside the tag extent + margin returns None."""
    far = (1000.0, 1000.0)
    assert assign_point_to_room(far, [_TAG_A]) is None


# ---------------------------------------------------------------------------
# Boundedness — margin edge
# ---------------------------------------------------------------------------


def test_boundedness_rejects_far_points() -> None:
    """Points strictly outside extent+margin are None; points just inside are assigned."""
    tags = [_TAG_A]  # anchor at (0,0); bound = (-M, -M, M, M)

    just_inside_x = (M * 0.99, 0.0)
    just_outside_x = (M * 1.01, 0.0)

    assert assign_point_to_room(just_inside_x, tags) == "A.01"
    assert assign_point_to_room(just_outside_x, tags) is None


def test_boundedness_margin_edge_y() -> None:
    tags = [_TAG_A]

    just_inside_y = (0.0, -M * 0.99)
    just_outside_y = (0.0, -M * 1.01)

    assert assign_point_to_room(just_inside_y, tags) == "A.01"
    assert assign_point_to_room(just_outside_y, tags) is None


# ---------------------------------------------------------------------------
# Total coverage — all strictly-interior points must be assigned
# ---------------------------------------------------------------------------


def test_total_coverage_within_bound() -> None:
    """Every point strictly inside the extent of a non-empty tag set is assigned."""
    tags = [_TAG_LEFT, _TAG_RIGHT]
    # Points well within the extent (-M..10+M in x, -M..M in y)
    interior_points = [
        (0.0, 0.0),
        (5.0, 0.0),
        (10.0, 0.0),
        (3.0, 0.0),
        (7.0, 0.0),
    ]
    for pt in interior_points:
        result = assign_point_to_room(pt, tags)
        assert result is not None, f"Expected assignment for {pt}"


# ---------------------------------------------------------------------------
# Nearest-anchor assignment (bisector behavior)
# ---------------------------------------------------------------------------


def test_nearest_anchor_assignment_left_wins() -> None:
    tags = [_TAG_LEFT, _TAG_RIGHT]
    # At x=4 the point is closer to L.01 (distance 4) than R.01 (distance 6).
    assert assign_point_to_room((4.0, 0.0), tags) == "L.01"


def test_nearest_anchor_assignment_right_wins() -> None:
    tags = [_TAG_LEFT, _TAG_RIGHT]
    # At x=6 the point is closer to R.01 (distance 4) than L.01 (distance 6).
    assert assign_point_to_room((6.0, 0.0), tags) == "R.01"


# ---------------------------------------------------------------------------
# Multi-anchor room (#581): nearest-anchor not centroid
# ---------------------------------------------------------------------------


def test_multi_anchor_room_near_right_arm() -> None:
    """A point near the right arm of U.01 should prefer U.01 over M.02.

    U.01 anchors: (-10, 0) and (10, 0) — centroid is (0, 0).
    M.02 anchor: (0, 0).
    Point at (9, 0): distance to U.01's nearest anchor = 1.0; distance to M.02 = 9.0.
    Nearest-anchor wins: U.01.
    """
    tags = [_TAG_U, _TAG_MID]
    assert assign_point_to_room((9.0, 0.0), tags) == "U.01"


def test_multi_anchor_room_near_left_arm() -> None:
    """Symmetric: point near the left arm of U.01 still assigns U.01."""
    tags = [_TAG_U, _TAG_MID]
    assert assign_point_to_room((-9.0, 0.0), tags) == "U.01"


def test_multi_anchor_centroid_competitor_wins_mid() -> None:
    """At the bisector between U.01-right and M.02 (x=5), M.02 is 5 away, U.01-nearest is 5.

    Both distances are equal at x=5.  Tie-break: lexicographic by number → M.02 < U.01.
    """
    tags = [_TAG_U, _TAG_MID]
    # distance to U.01 nearest anchor (10,0) = 5; distance to M.02 (0,0) = 5 — tie.
    result = assign_point_to_room((5.0, 0.0), tags)
    assert result == "M.02"


# ---------------------------------------------------------------------------
# Tie-break by number (deterministic)
# ---------------------------------------------------------------------------


def test_tie_break_by_number_smaller_wins() -> None:
    """On the bisector between two equidistant rooms, the lexicographically smaller number wins."""
    # Tags at (0,0) and (10,0); bisector is x=5.
    tag_alpha = RoomTag(number="A.01", anchors=((0.0, 0.0),))
    tag_beta = RoomTag(number="B.02", anchors=((10.0, 0.0),))

    result = assign_point_to_room((5.0, 0.0), [tag_alpha, tag_beta])
    assert result == "A.01"


def test_tie_break_by_number_reverse_input_order() -> None:
    """Same bisector scenario with reversed input order still gives the same result."""
    tag_alpha = RoomTag(number="A.01", anchors=((0.0, 0.0),))
    tag_beta = RoomTag(number="B.02", anchors=((10.0, 0.0),))

    result = assign_point_to_room((5.0, 0.0), [tag_beta, tag_alpha])
    assert result == "A.01"


# ---------------------------------------------------------------------------
# Determinism under shuffle
# ---------------------------------------------------------------------------


def test_determinism_under_shuffle() -> None:
    """Identical results across shuffled tag + anchor permutations."""
    base_tags = [
        RoomTag(number="1.01", anchors=((0.0, 0.0), (0.0, 10.0))),
        RoomTag(number="1.02", anchors=((20.0, 0.0),)),
        RoomTag(number="1.03", anchors=((10.0, 5.0),)),
    ]
    test_points = [(5.0, 5.0), (15.0, 5.0), (10.0, 0.0)]

    rng = random.Random(42)
    reference: dict[tuple[float, float], str | None] = {}
    for pt in test_points:
        reference[pt] = assign_point_to_room(pt, base_tags)

    for _ in range(20):
        shuffled = list(base_tags)
        rng.shuffle(shuffled)
        for pt in test_points:
            assert assign_point_to_room(pt, shuffled) == reference[pt], (
                f"Non-deterministic result for {pt} after shuffle"
            )


# ---------------------------------------------------------------------------
# Margin parameter override
# ---------------------------------------------------------------------------


def test_margin_parameter_small_rejects() -> None:
    """With a tiny margin a distant point is rejected."""
    tags = [_TAG_A]  # anchor at origin
    far = (2.0, 0.0)
    # With margin=1.0 the bound is (-1..1); point at x=2 is outside.
    assert assign_point_to_room(far, tags, margin=1.0) is None


def test_margin_parameter_large_accepts() -> None:
    """With a generous margin the same distant point is accepted."""
    tags = [_TAG_A]  # anchor at origin
    far = (2.0, 0.0)
    # With margin=3.0 the bound is (-3..3); point at x=2 is inside.
    assert assign_point_to_room(far, tags, margin=3.0) == "A.01"


# ---------------------------------------------------------------------------
# Exact boundary-edge inclusiveness (locks the >= / <= decision)
# ---------------------------------------------------------------------------


def test_boundary_edge_max_x_is_inclusive() -> None:
    """A point at exactly the max-x envelope edge is assigned, not rejected.

    floor_bound([tag at (0,0)], margin=2.0) → (-2, -2, 2, 2).
    Point (2.0, 0.0) sits on the max-x edge and must return the room, not None.
    A future < → <= refactor would flip this to None; this test locks the intent.
    """
    tag = RoomTag(number="E.01", anchors=((0.0, 0.0),))
    bound = floor_bound([tag], margin=2.0)
    assert bound is not None
    _, _, max_x, _ = bound
    result = assign_point_to_room((max_x, 0.0), [tag], margin=2.0)
    assert result == "E.01"


def test_boundary_edge_min_x_is_inclusive() -> None:
    """A point at exactly the min-x envelope edge is assigned, not rejected.

    floor_bound([tag at (0,0)], margin=2.0) → (-2, -2, 2, 2).
    Point (-2.0, 0.0) sits on the min-x edge and must return the room, not None.
    """
    tag = RoomTag(number="E.01", anchors=((0.0, 0.0),))
    bound = floor_bound([tag], margin=2.0)
    assert bound is not None
    min_x, _, _, _ = bound
    result = assign_point_to_room((min_x, 0.0), [tag], margin=2.0)
    assert result == "E.01"


# ---------------------------------------------------------------------------
# In-bound-but-distant: bound is the SOLE rejection criterion
# ---------------------------------------------------------------------------


def test_in_bound_but_far_from_every_anchor_is_still_assigned() -> None:
    """A point well inside the envelope but far from every anchor is still assigned.

    The bound is the only gate; distance only determines WHICH room wins.
    R-C depends on this — it uses None solely to detect out-of-floor-plan geometry.
    """
    # Two tags in the corners; a huge margin makes the bound very large.
    tags = [
        RoomTag(number="C.01", anchors=((0.0, 0.0),)),
        RoomTag(number="C.02", anchors=((10.0, 0.0),)),
    ]
    # Point at (5, 100): inside the bound (margin=200 → y goes to ±200), very
    # far from both anchors.
    result = assign_point_to_room((5.0, 100.0), tags, margin=200.0)
    assert result is not None


# ---------------------------------------------------------------------------
# RoomTag validation
# ---------------------------------------------------------------------------


def test_room_tag_rejects_empty_anchors() -> None:
    with pytest.raises(ValueError, match="anchors must be non-empty"):
        RoomTag(number="X.01", anchors=())
