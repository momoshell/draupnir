"""Tests for app/interpretation/room_partition.py (pure, no DB).

Exercises partition_polylines_by_room directly to pin LP2 clip algorithm behavior:
- clip-splits across rooms (length distributed correctly)
- outside any room -> (None, length)
- label-radius fallback attributes remainder to nearest label room
- empty / degenerate geometry -> []
- smallest-room-first nesting (inner room wins over outer)
"""

from __future__ import annotations

from shapely.geometry import Polygon

from app.interpretation.room_partition import (
    _CLIP_EPS,
    _LABEL_ROOM_RADIUS_M,
    partition_polylines_by_room,
)
from app.interpretation.rooms import Room, room_from_label, room_from_polygon

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _rect_room(
    room_id: str,
    x0: float,
    y0: float,
    x1: float,
    y1: float,
    *,
    name: str | None = None,
) -> Room:
    """Build a polygon room from axis-aligned rectangle corners."""
    poly = Polygon([(x0, y0), (x1, y0), (x1, y1), (x0, y1)])
    return room_from_polygon(room_id, poly, source="test", name=name)


def _label_room(
    room_id: str,
    x: float,
    y: float,
    *,
    name: str | None = None,
) -> Room:
    return room_from_label(room_id, source="test", location=(x, y), name=name)


# ---------------------------------------------------------------------------
# Empty / degenerate geometry
# ---------------------------------------------------------------------------


def test_empty_polylines_returns_empty() -> None:
    """partition_polylines_by_room with no polylines returns [].

    Arrange: empty tuple of polylines.
    Act:     call with one room.
    Assert:  returns [].
    """
    room = _rect_room("r1", 0, 0, 10, 10)
    result = partition_polylines_by_room((), [room])
    assert result == []


def test_single_vertex_polyline_returns_empty() -> None:
    """A polyline with fewer than 2 vertices contributes no geometry.

    Arrange: one single-point polyline.
    Act:     call.
    Assert:  returns [] (degenerate; no segments).
    """
    room = _rect_room("r1", 0, 0, 10, 10)
    result = partition_polylines_by_room((((5.0, 5.0),),), [room])
    assert result == []


def test_no_rooms_returns_unassigned() -> None:
    """When no rooms are supplied the full length is unassigned (None).

    Arrange: a straight horizontal polyline, empty rooms list.
    Act:     call with label_radius=None.
    Assert:  one (None, length) entry.
    """
    polyline = ((0.0, 0.0), (10.0, 0.0))
    result = partition_polylines_by_room((polyline,), [])
    assert len(result) == 1
    room, length = result[0]
    assert room is None
    assert abs(length - 10.0) < 1e-9


# ---------------------------------------------------------------------------
# Whole line inside one room
# ---------------------------------------------------------------------------


def test_line_fully_inside_room_attributed_to_that_room() -> None:
    """A polyline entirely inside a room is fully attributed to it.

    Arrange: 10-unit horizontal line inside a 20x20 room.
    Act:     call partition_polylines_by_room.
    Assert:  one partition for that room with length ~10.
    """
    room = _rect_room("r1", 0, 0, 20, 20)
    polyline = ((1.0, 10.0), (11.0, 10.0))
    result = partition_polylines_by_room((polyline,), [room])
    assert len(result) == 1
    r, length = result[0]
    assert r is not None
    assert r.id == "r1"
    assert abs(length - 10.0) < 1e-9


# ---------------------------------------------------------------------------
# Line outside all rooms
# ---------------------------------------------------------------------------


def test_line_outside_all_rooms_is_unassigned() -> None:
    """A polyline outside all room polygons produces (None, length).

    Arrange: room covers x=[0,5]; line runs x=[10,20].
    Act:     call with no label_radius.
    Assert:  one (None, 10.0) entry.
    """
    room = _rect_room("r1", 0, 0, 5, 5)
    polyline = ((10.0, 2.0), (20.0, 2.0))
    result = partition_polylines_by_room((polyline,), [room])
    assert len(result) == 1
    r, length = result[0]
    assert r is None
    assert abs(length - 10.0) < 1e-6


# ---------------------------------------------------------------------------
# Clip split across two rooms
# ---------------------------------------------------------------------------


def test_line_split_across_two_rooms() -> None:
    """A horizontal line crossing two adjacent rooms is split proportionally.

    Arrange: room A covers x=[0,5], room B covers x=[5,10]; line runs x=[0,10].
    Act:     call partition_polylines_by_room.
    Assert:  two partitions summing to 10, one per room.
    """
    room_a = _rect_room("a", 0, 0, 5, 1)
    room_b = _rect_room("b", 5, 0, 10, 1)
    polyline = ((0.0, 0.5), (10.0, 0.5))
    result = partition_polylines_by_room((polyline,), [room_a, room_b])

    by_id = {r.id: length for r, length in result if r is not None}
    assert set(by_id.keys()) == {"a", "b"}
    assert abs(by_id["a"] - 5.0) < 1e-6
    assert abs(by_id["b"] - 5.0) < 1e-6
    # No unassigned portion
    assert all(r is not None for r, _ in result)


def test_line_partly_outside_rooms_with_unassigned() -> None:
    """A line crossing one room and extending beyond produces a room + unassigned partition.

    Arrange: room covers x=[0,5]; line runs x=[0,10].
    Act:     call with label_radius=None.
    Assert:  one room partition (length ~5) + one (None, ~5).
    """
    room = _rect_room("r1", 0, 0, 5, 1)
    polyline = ((0.0, 0.5), (10.0, 0.5))
    result = partition_polylines_by_room((polyline,), [room])

    room_parts = [(r, ln) for r, ln in result if r is not None]
    none_parts = [(r, ln) for r, ln in result if r is None]
    assert len(room_parts) == 1
    assert len(none_parts) == 1
    assert abs(room_parts[0][1] - 5.0) < 1e-6
    assert abs(none_parts[0][1] - 5.0) < 1e-6


# ---------------------------------------------------------------------------
# Smallest-room-first nesting (nested rooms)
# ---------------------------------------------------------------------------


def test_nested_rooms_inner_wins() -> None:
    """When an inner room nests inside an outer room, the inner room claims intersecting length.

    Arrange: outer room 0-10x0-10; inner room 3-7x0-10; line runs x=[0,10] at y=5.
    Act:     call partition_polylines_by_room.
    Assert:  inner room gets ~4 units (x=3..7), outer gets ~6 (x=0..3 + x=7..10), no unassigned.
    """
    outer = _rect_room("outer", 0, 0, 10, 10)
    inner = _rect_room("inner", 3, 0, 7, 10)
    polyline = ((0.0, 5.0), (10.0, 5.0))
    result = partition_polylines_by_room((polyline,), [outer, inner])

    by_id = {r.id: length for r, length in result if r is not None}
    assert "inner" in by_id
    assert "outer" in by_id
    assert abs(by_id["inner"] - 4.0) < 1e-6
    assert abs(by_id["outer"] - 6.0) < 1e-6
    total = sum(ln for _, ln in result)
    assert abs(total - 10.0) < 1e-6


# ---------------------------------------------------------------------------
# Label-radius fallback
# ---------------------------------------------------------------------------


def test_label_radius_attributes_remainder_to_label_room() -> None:
    """Remainder outside polygon rooms is attributed to nearest label room when in range.

    Arrange: polygon room covers x=[0,5]; line runs x=[0,10]; label room at x=8.
    Act:     call with label_radius=5 (covers midpoint of x=5..10 segment).
    Assert:  polygon room partition + label room partition; no unassigned (None).
    """
    poly_room = _rect_room("poly", 0, 0, 5, 1)
    label_room = _label_room("label", 8.0, 0.5)
    polyline = ((0.0, 0.5), (10.0, 0.5))
    result = partition_polylines_by_room((polyline,), [poly_room, label_room], label_radius=5.0)

    by_id = {r.id: length for r, length in result if r is not None}
    assert "poly" in by_id
    assert "label" in by_id
    # No unassigned
    assert all(r is not None for r, _ in result)
    assert abs(by_id["poly"] - 5.0) < 1e-6
    assert abs(by_id["label"] - 5.0) < 1e-6


def test_label_radius_too_small_leaves_unassigned() -> None:
    """When label room is outside the radius, remainder stays unassigned.

    Arrange: polygon room covers x=[0,5]; label room at x=20; line runs x=[0,10].
    Act:     call with label_radius=1 (too small to reach the label room).
    Assert:  polygon partition + (None, ~5).
    """
    poly_room = _rect_room("poly", 0, 0, 5, 1)
    label_room = _label_room("label", 20.0, 0.5)
    polyline = ((0.0, 0.5), (10.0, 0.5))
    result = partition_polylines_by_room((polyline,), [poly_room, label_room], label_radius=1.0)

    none_parts = [ln for r, ln in result if r is None]
    assert len(none_parts) == 1
    assert abs(none_parts[0] - 5.0) < 1e-6


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------


def test_clip_eps_value() -> None:
    """_CLIP_EPS is 1e-9 (architectural scale degenerate-segment guard)."""
    assert _CLIP_EPS == 1e-9


def test_label_room_radius_m_value() -> None:
    """_LABEL_ROOM_RADIUS_M is 8.0 (mirrors device-label-room precedent, #662)."""
    assert _LABEL_ROOM_RADIUS_M == 8.0


# ---------------------------------------------------------------------------
# Determinism / permutation invariance
# ---------------------------------------------------------------------------


def test_result_is_permutation_invariant_rooms() -> None:
    """Swapping room list order produces the same result.

    Arrange: two equal-area rooms side by side (different ids); line spans both.
    Act:     call with rooms in both orderings.
    Assert:  same (room_id, length) sets.
    """
    room_a = _rect_room("a", 0, 0, 5, 1)
    room_b = _rect_room("b", 5, 0, 10, 1)
    polyline = ((0.0, 0.5), (10.0, 0.5))
    result_ab = partition_polylines_by_room((polyline,), [room_a, room_b])
    result_ba = partition_polylines_by_room((polyline,), [room_b, room_a])

    def _key_set(
        result: list[tuple[Room | None, float]],
    ) -> set[tuple[str | None, float]]:
        return {(r.id if r is not None else None, round(ln, 9)) for r, ln in result}

    assert _key_set(result_ab) == _key_set(result_ba)
