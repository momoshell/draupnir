"""Tests for the room_fusion D1 hybrid registry (issue #717, Phase R-C).

All tests use coordinate fixtures only — no DB, no FastAPI, no heavy deps.
"""

from __future__ import annotations

import random

import pytest
from shapely.geometry import Polygon

from app.interpretation.room_fusion import (
    DEFAULT_POLYGON_VORONOI_CONFIDENCE,
    DEFAULT_VORONOI_CONFIDENCE,
    RoomAssignment,
    RoomRegistry,
    build_room_registry,
)
from app.interpretation.room_voronoi import RoomTag
from app.interpretation.rooms import Room, room_from_label, room_from_polygon

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

_POLY_A = Polygon([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)])
_POLY_INNER = Polygon([(2.0, 2.0), (5.0, 2.0), (5.0, 5.0), (2.0, 5.0)])


def _polygon_room(room_id: str, poly: Polygon, *, number: str | None = None) -> Room:
    return room_from_polygon(
        room_id,
        poly,
        source="explicit_layer",
        name=f"Room {room_id}",
        number=number,
    )


def _label_room(
    room_id: str,
    x: float,
    y: float,
    *,
    number: str | None = None,
    needs_review: bool = False,
    extra_anchors: tuple[tuple[float, float], ...] = (),
) -> Room:
    anchors = ((x, y), *extra_anchors)
    return room_from_label(
        room_id,
        source="label_cluster",
        location=(x, y),
        name=f"Label {room_id}",
        number=number,
        anchors=anchors,
        needs_review=needs_review,
    )


# ---------------------------------------------------------------------------
# Voronoi on/off byte-identity (KEYSTONE)
# ---------------------------------------------------------------------------


def test_voronoi_on_off_polygon_hit_byte_identical() -> None:
    """Polygon-room points yield IDENTICAL RoomAssignment with voronoi_fallback=True vs False.

    This is the D1 keystone invariant: the polygon-first branch must return before
    touching any Voronoi state so enabling/disabling Voronoi has zero effect on
    polygon-room assignments.

    Arrange: registry with one polygon room + one label-only room at a different location.
    Act:     classify a point inside the polygon twice (voronoi on, voronoi off).
    Assert:  assignments are equal (byte-identical frozen dataclasses).
    """
    poly_room = _polygon_room("P1", _POLY_A, number="1.01")
    label_room = _label_room("L1", 20.0, 20.0, number="2.01")

    registry_on = build_room_registry([poly_room, label_room], voronoi_fallback=True)
    registry_off = build_room_registry([poly_room, label_room], voronoi_fallback=False)

    pt_inside = (5.0, 5.0)
    result_on = registry_on.classify(pt_inside)
    result_off = registry_off.classify(pt_inside)

    assert result_on == result_off, (
        f"Voronoi on/off diverged for polygon point:\non={result_on}\noff={result_off}"
    )
    assert result_on.boundary_basis == "polygon"
    assert result_on.room_number == "1.01"


def test_voronoi_on_off_multiple_polygon_rooms_byte_identical() -> None:
    """All polygon-room points are byte-identical across voronoi on/off for a richer fixture."""
    outer = _polygon_room("outer", _POLY_A, number="0.01")
    inner = _polygon_room("inner", _POLY_INNER, number="0.02")
    label_room = _label_room("L1", 50.0, 50.0, number="0.99")

    registry_on = build_room_registry([outer, inner, label_room], voronoi_fallback=True)
    registry_off = build_room_registry([outer, inner, label_room], voronoi_fallback=False)

    test_points = [
        (5.0, 5.0),  # inside outer only
        (3.0, 3.0),  # inside inner (and outer, but inner is smaller)
        (0.5, 0.5),  # inside outer only (corner region)
    ]
    for pt in test_points:
        on = registry_on.classify(pt)
        off = registry_off.classify(pt)
        assert on == off, f"Byte-identity failed at {pt}:\non={on}\noff={off}"
        assert on.boundary_basis == "polygon"


# ---------------------------------------------------------------------------
# Polygon-first / smallest-containing
# ---------------------------------------------------------------------------


def test_polygon_first_takes_precedence_over_voronoi() -> None:
    """A point inside a polygon room is classified as polygon even if a label room is closer."""
    poly_room = _polygon_room("P1", _POLY_A, number="1.01")
    # Label room anchor right at the test point — much "closer" than polygon centroid.
    label_room = _label_room("L1", 5.0, 5.0, number="2.99")

    registry = build_room_registry([poly_room, label_room], voronoi_fallback=True)
    result = registry.classify((5.0, 5.0))

    assert result.boundary_basis == "polygon"
    assert result.room_number == "1.01"


def test_smallest_containing_polygon_wins_for_nested_point() -> None:
    """Point inside both outer and inner polygon → classified to inner (smallest area)."""
    outer = _polygon_room("outer", _POLY_A, number="0.01")
    inner = _polygon_room("inner", _POLY_INNER, number="0.02")

    registry = build_room_registry([outer, inner], voronoi_fallback=False)
    result = registry.classify((3.0, 3.0))

    assert result.boundary_basis == "polygon"
    assert result.room_number == "0.02"
    assert result.room_id == "inner"


# ---------------------------------------------------------------------------
# Voronoi fallback path
# ---------------------------------------------------------------------------


def test_voronoi_fallback_assigns_outside_polygon() -> None:
    """Point outside every polygon but inside the Voronoi envelope is assigned via Voronoi."""
    poly_room = _polygon_room("P1", _POLY_A, number="1.01")
    label_room = _label_room("L1", 30.0, 30.0, number="9.01")

    registry = build_room_registry([poly_room, label_room], voronoi_fallback=True)
    result = registry.classify((30.0, 30.0))

    assert result.boundary_basis == "voronoi"
    assert result.room_number == "9.01"
    assert result.confidence == DEFAULT_VORONOI_CONFIDENCE


def test_voronoi_fallback_disabled_returns_unassigned() -> None:
    """Same point with voronoi_fallback=False returns unassigned (boundary_basis=None)."""
    poly_room = _polygon_room("P1", _POLY_A, number="1.01")
    label_room = _label_room("L1", 30.0, 30.0, number="9.01")

    registry = build_room_registry([poly_room, label_room], voronoi_fallback=False)
    result = registry.classify((30.0, 30.0))

    assert result.boundary_basis is None
    assert result.room_number is None


# ---------------------------------------------------------------------------
# Unassigned (out-of-bound)
# ---------------------------------------------------------------------------


def test_out_of_bound_point_returns_unassigned() -> None:
    """A point far outside all rooms and the Voronoi envelope returns all-None."""
    label_room = _label_room("L1", 0.0, 0.0, number="1.01")
    registry = build_room_registry([label_room], voronoi_fallback=True)

    far_point = (10000.0, 10000.0)
    result = registry.classify(far_point)

    assert result.boundary_basis is None
    assert result.room_number is None
    assert result.room_id is None
    assert result.confidence is None
    assert result.needs_review is False


# ---------------------------------------------------------------------------
# Dedupe by number (two label-only rooms share a number)
# ---------------------------------------------------------------------------


def test_dedupe_by_number_merges_anchors() -> None:
    """Two label-only rooms with the same number → one merged RoomTag with both anchors."""
    room_a = _label_room("L1a", 0.0, 0.0, number="1.01")
    room_b = _label_room("L1b", 100.0, 0.0, number="1.01")  # same number, different location

    registry = build_room_registry([room_a, room_b], voronoi_fallback=True)

    # Both anchor locations should classify to room 1.01 (merged).
    result_near_a = registry.classify((0.0, 0.0))
    result_near_b = registry.classify((100.0, 0.0))

    assert result_near_a.room_number == "1.01"
    assert result_near_b.room_number == "1.01"


def test_dedupe_voronoi_tag_count() -> None:
    """Two rooms sharing a number produce exactly one Voronoi tag (not two)."""
    room_a = _label_room("La", 0.0, 0.0, number="X.01")
    room_b = _label_room("Lb", 20.0, 0.0, number="X.01")
    room_c = _label_room("Lc", 50.0, 0.0, number="X.02")

    registry = build_room_registry([room_a, room_b, room_c], voronoi_fallback=True)

    # X.01 tag should have 2 anchors; X.02 should have 1.
    assert len(registry._voronoi_tags) == 2
    tag_map = {tag.number: tag for tag in registry._voronoi_tags}
    assert len(tag_map["X.01"].anchors) == 2
    assert len(tag_map["X.02"].anchors) == 1


# ---------------------------------------------------------------------------
# Determinism under shuffled input order
# ---------------------------------------------------------------------------


def test_determinism_under_shuffled_room_order() -> None:
    """Classification is deterministic regardless of the order rooms are supplied."""
    rooms_base = [
        _label_room("L1", 0.0, 0.0, number="1.01"),
        _label_room("L2", 20.0, 0.0, number="1.02"),
        _label_room("L3", 40.0, 0.0, number="1.03"),
    ]
    test_points = [(5.0, 0.0), (22.0, 0.0), (38.0, 0.0)]

    reference: dict[tuple[float, float], RoomAssignment] = {}
    registry = build_room_registry(rooms_base, voronoi_fallback=True)
    for pt in test_points:
        reference[pt] = registry.classify(pt)

    rng = random.Random(7)
    for _ in range(20):
        shuffled = list(rooms_base)
        rng.shuffle(shuffled)
        reg = build_room_registry(shuffled, voronoi_fallback=True)
        for pt in test_points:
            assert reg.classify(pt) == reference[pt], (
                f"Non-deterministic result at {pt} after shuffle"
            )


def test_determinism_under_shuffled_room_order_with_dedupe() -> None:
    """Dedupe-by-number anchor merging is deterministic under shuffled input room order.

    This exercises the exact scenario flagged in the R-B contract: when two label-only
    rooms share a number, the resulting merged RoomTag's anchors and classify() results
    must be identical regardless of the order the rooms are supplied.

    Fixture:
    - Two rooms sharing number "D.01" at anchors (0,0) and (30,0).
    - One room with distinct number "D.02" at (60,0).
    - Test points near each anchor and the bisectors between them.
    """
    room_d01_a = _label_room("Da", 0.0, 0.0, number="D.01")
    room_d01_b = _label_room("Db", 30.0, 0.0, number="D.01")  # same number, different anchor
    room_d02 = _label_room("Dc", 60.0, 0.0, number="D.02")

    rooms_base = [room_d01_a, room_d01_b, room_d02]
    test_points = [(2.0, 0.0), (28.0, 0.0), (45.0, 0.0), (62.0, 0.0)]

    # Build reference with canonical order.
    reference_registry = build_room_registry(rooms_base, voronoi_fallback=True)
    reference_results: dict[tuple[float, float], RoomAssignment] = {
        pt: reference_registry.classify(pt) for pt in test_points
    }
    # Both anchors of D.01 should be present in the merged tag.
    ref_tags = {tag.number: tag for tag in reference_registry._voronoi_tags}
    assert len(ref_tags["D.01"].anchors) == 2, "Merged tag must carry both anchors"

    rng = random.Random(13)
    for _ in range(30):
        shuffled = list(rooms_base)
        rng.shuffle(shuffled)
        reg = build_room_registry(shuffled, voronoi_fallback=True)
        # Merged anchor count must be stable regardless of order.
        tags = {tag.number: tag for tag in reg._voronoi_tags}
        assert len(tags["D.01"].anchors) == 2, "Dedupe anchor count non-deterministic after shuffle"
        for pt in test_points:
            assert reg.classify(pt) == reference_results[pt], (
                f"Dedupe determinism failed at {pt} after shuffle"
            )


# ---------------------------------------------------------------------------
# Nested label-only room → needs_review=True on Voronoi hit
# ---------------------------------------------------------------------------


def test_nested_label_room_voronoi_hit_sets_needs_review() -> None:
    """A label-only room whose location falls inside a polygon room is nested.

    When a Voronoi hit returns that room's number, ``needs_review`` is True.

    Fixture design (unconditional assertions):
    - Small polygon at x∈[0,2], y∈[0,2].
    - Nested label anchor at (1, 1) — inside the polygon; marked nested.
    - No other label rooms → the nested label is the sole Voronoi tag.
    - Test point at (1, 4): outside the polygon (y>2), inside the default 5m margin
      of the Voronoi envelope (tag anchor at (1,1); bound expands by 5m → y≤6).
    - Distance from (1,4) to anchor (1,1) = 3.0 < margin → in-bound → assigned.
    - Assertions are UNCONDITIONAL: boundary_basis MUST be "voronoi" and
      needs_review MUST be True.
    """
    small_poly = Polygon([(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0)])
    poly_room = _polygon_room("P_small", small_poly, number="outer.01")
    # Label anchor at (1,1) — inside the polygon → flagged as nested.
    nested_label = _label_room("L_nested", 1.0, 1.0, number="inner.01")

    registry = build_room_registry([poly_room, nested_label], voronoi_fallback=True)

    # Precondition: nested detection must have fired.
    assert "inner.01" in registry._nested_numbers

    # Test point: outside the polygon (y=4 > 2), in-bound for Voronoi (margin=5).
    # Only one Voronoi tag exists (inner.01), so assignment is guaranteed in-bound.
    result = registry.classify((1.0, 4.0))

    assert result.boundary_basis == "voronoi", (
        f"Expected voronoi-basis for point outside polygon but in envelope; got {result}"
    )
    assert result.room_number == "inner.01"
    assert result.needs_review is True


def test_nested_numbers_populated() -> None:
    """Nested label rooms are registered in _nested_numbers."""
    poly_room = _polygon_room("P1", _POLY_A, number="0.01")
    inside_label = _label_room("L_in", 5.0, 5.0, number="nested.01")
    outside_label = _label_room("L_out", 50.0, 50.0, number="outside.01")

    registry = build_room_registry([poly_room, inside_label, outside_label])

    assert "nested.01" in registry._nested_numbers
    assert "outside.01" not in registry._nested_numbers


# ---------------------------------------------------------------------------
# Registry with no rooms / no label rooms
# ---------------------------------------------------------------------------


def test_empty_registry_returns_unassigned() -> None:
    """An empty room list yields unassigned for every point."""
    registry = build_room_registry([])
    result = registry.classify((0.0, 0.0))
    assert result.boundary_basis is None
    assert result.room_number is None


def test_polygon_only_registry_out_of_bound() -> None:
    """A registry with only polygon rooms returns unassigned for out-of-polygon points."""
    poly_room = _polygon_room("P1", _POLY_A, number="1.01")
    registry = build_room_registry([poly_room], voronoi_fallback=True)
    result = registry.classify((50.0, 50.0))
    assert result.boundary_basis is None


# ---------------------------------------------------------------------------
# Confidence propagated from polygon rooms
# ---------------------------------------------------------------------------


def test_polygon_room_confidence_propagated() -> None:
    """The polygon room's confidence value is forwarded in the assignment."""
    poly_room = room_from_polygon(
        "P1", _POLY_A, source="wall_polygonize", number="1.01", confidence=0.75
    )
    registry = build_room_registry([poly_room])
    result = registry.classify((5.0, 5.0))
    assert result.confidence == pytest.approx(0.75)
    assert result.boundary_basis == "polygon"


# ---------------------------------------------------------------------------
# Multi-tag polygon subdivision (issue #733)
# ---------------------------------------------------------------------------

# A wide polygon that swallows two labelled rooms A and B.
_WIDE_POLY = Polygon([(0.0, 0.0), (20.0, 0.0), (20.0, 10.0), (0.0, 10.0)])
# Tag A anchor near left side; tag B anchor near right side.
_TAG_A = RoomTag(number="0.9.04", anchors=((3.0, 5.0),))
_TAG_B = RoomTag(number="0.9.05", anchors=((17.0, 5.0),))


def _multi_tag_registry(voronoi_fallback: bool = True) -> RoomRegistry:
    """Build a registry with one big polygon containing two tags."""
    poly_room = _polygon_room("big_poly", _WIDE_POLY, number="0.9.04")
    return build_room_registry(
        [poly_room],
        voronoi_fallback=voronoi_fallback,
        polygon_tags_by_room_id={"big_poly": (_TAG_A, _TAG_B)},
    )


def test_multi_tag_point_near_tag_a_returns_a() -> None:
    """Point near tag A inside a multi-tag polygon → assigned to A."""
    registry = _multi_tag_registry()
    result = registry.classify((2.0, 5.0))

    assert result.room_number == "0.9.04"
    assert result.room_id == "big_poly"
    assert result.boundary_basis == "polygon_voronoi"
    assert result.confidence == pytest.approx(DEFAULT_POLYGON_VORONOI_CONFIDENCE)
    assert result.needs_review is True


def test_multi_tag_point_near_tag_b_returns_b() -> None:
    """Point near tag B inside a multi-tag polygon → assigned to B."""
    registry = _multi_tag_registry()
    result = registry.classify((18.0, 5.0))

    assert result.room_number == "0.9.05"
    assert result.room_id == "big_poly"
    assert result.boundary_basis == "polygon_voronoi"
    assert result.confidence == pytest.approx(DEFAULT_POLYGON_VORONOI_CONFIDENCE)
    assert result.needs_review is True


def test_multi_tag_subdivision_independent_of_voronoi_fallback() -> None:
    """Multi-tag subdivision fires regardless of voronoi_fallback flag."""
    registry_on = _multi_tag_registry(voronoi_fallback=True)
    registry_off = _multi_tag_registry(voronoi_fallback=False)

    pt_near_a = (2.0, 5.0)
    pt_near_b = (18.0, 5.0)

    # Both registries should sub-partition the same way.
    assert registry_on.classify(pt_near_a) == registry_off.classify(pt_near_a)
    assert registry_on.classify(pt_near_b) == registry_off.classify(pt_near_b)
    assert registry_on.classify(pt_near_a).boundary_basis == "polygon_voronoi"
    assert registry_on.classify(pt_near_b).boundary_basis == "polygon_voronoi"


def test_single_tag_polygon_byte_identical_with_and_without_map_voronoi_on() -> None:
    """Single-tag polygon: byte-identical with/without map (voronoi=True).

    KEYSTONE invariant: providing polygon_tags_by_room_id=None or an empty map
    (or a map that has no entry for this polygon) must not change the RoomAssignment
    for a polygon with only one tag.
    """
    poly_room = _polygon_room("P1", _POLY_A, number="1.01")

    registry_no_map = build_room_registry([poly_room], voronoi_fallback=True)
    registry_with_map = build_room_registry(
        [poly_room],
        voronoi_fallback=True,
        polygon_tags_by_room_id={},
    )

    pt = (5.0, 5.0)
    result_no_map = registry_no_map.classify(pt)
    result_with_map = registry_with_map.classify(pt)

    assert result_no_map == result_with_map, (
        f"Single-tag polygon byte-identity failed:\n"
        f"no_map={result_no_map}\nwith_map={result_with_map}"
    )
    assert result_no_map.boundary_basis == "polygon"


def test_single_tag_polygon_byte_identical_with_and_without_map_voronoi_off() -> None:
    """Same byte-identity invariant holds with voronoi_fallback=False."""
    poly_room = _polygon_room("P1", _POLY_A, number="1.01")

    registry_no_map = build_room_registry([poly_room], voronoi_fallback=False)
    registry_with_map = build_room_registry(
        [poly_room],
        voronoi_fallback=False,
        polygon_tags_by_room_id={},
    )

    pt = (5.0, 5.0)
    result_no_map = registry_no_map.classify(pt)
    result_with_map = registry_with_map.classify(pt)

    assert result_no_map == result_with_map, (
        f"Single-tag byte-identity failed (voronoi=False):\n"
        f"no_map={result_no_map}\nwith_map={result_with_map}"
    )
    assert result_no_map.boundary_basis == "polygon"


def test_multi_tag_determinism_over_20_tag_order_shuffles() -> None:
    """Classification results are identical across 20 shuffles of contained-tag order."""
    poly_room = _polygon_room("big_poly", _WIDE_POLY, number="0.9.04")
    tag_a = RoomTag(number="0.9.04", anchors=((3.0, 5.0),))
    tag_b = RoomTag(number="0.9.05", anchors=((17.0, 5.0),))
    tag_c = RoomTag(number="0.9.06", anchors=((10.0, 5.0),))

    tags_base = [tag_a, tag_b, tag_c]
    test_points = [(2.0, 5.0), (18.0, 5.0), (10.0, 5.0)]

    base_registry = build_room_registry(
        [poly_room],
        polygon_tags_by_room_id={"big_poly": tuple(tags_base)},
    )
    reference = {pt: base_registry.classify(pt) for pt in test_points}

    rng = random.Random(42)
    for _ in range(20):
        shuffled = list(tags_base)
        rng.shuffle(shuffled)
        reg = build_room_registry(
            [poly_room],
            polygon_tags_by_room_id={"big_poly": tuple(shuffled)},
        )
        for pt in test_points:
            got = reg.classify(pt)
            assert got == reference[pt], (
                f"Multi-tag determinism failed at {pt} after shuffle: "
                f"got={got} want={reference[pt]}"
            )


def test_multi_tag_point_far_from_tags_still_subdivides() -> None:
    """A point far from all tags (beyond default margin) inside a large polygon still subdivides.

    Root cause guarded: before fix, assign_point_to_room used margin=self._margin so a point
    inside the polygon but outside tags-bbox+5m returned None and fell back to the aggregate
    polygon number.  Fix uses margin=inf so any in-polygon point reaches the nearest tag.

    Fixture: very wide polygon x∈[0,200], tag A at x=3, tag B at x=17.  Test point at x=100
    is 83m from the nearest tag — well beyond the default 5m margin.  With margin=inf the
    subdivision must still fire (not fall back to the plain polygon number).
    """
    very_wide_poly = Polygon([(0.0, 0.0), (200.0, 0.0), (200.0, 10.0), (0.0, 10.0)])
    poly_room = _polygon_room("wide", very_wide_poly, number="0.9.04")
    tag_a = RoomTag(number="0.9.04", anchors=((3.0, 5.0),))
    tag_b = RoomTag(number="0.9.05", anchors=((17.0, 5.0),))

    registry = build_room_registry(
        [poly_room],
        polygon_tags_by_room_id={"wide": (tag_a, tag_b)},
    )

    # x=100 is 83m from the nearest tag — well beyond default 5m margin.
    result = registry.classify((100.0, 5.0))

    assert result.boundary_basis == "polygon_voronoi", (
        f"Expected polygon_voronoi but got {result.boundary_basis!r} — "
        "margin check must not reject in-polygon points during subdivision"
    )
    assert result.room_number in ("0.9.04", "0.9.05")
    assert result.needs_review is True


def test_default_polygon_tags_none_existing_tests_unaffected() -> None:
    """Default polygon_tags_by_room_id=None → _polygon_tags_by_room_id is empty dict."""
    poly_room = _polygon_room("P1", _POLY_A, number="1.01")
    label_room = _label_room("L1", 30.0, 30.0, number="9.01")

    registry = build_room_registry([poly_room, label_room])
    assert registry._polygon_tags_by_room_id == {}

    # All polygon-first behaviour unchanged.
    result = registry.classify((5.0, 5.0))
    assert result.boundary_basis == "polygon"
    assert result.room_number == "1.01"
