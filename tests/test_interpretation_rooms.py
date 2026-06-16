"""Unit tests for pure room model + label/device assignment (issue #461).

Fixture-driven; no DB, no mocks.
"""

from __future__ import annotations

from app.interpretation.geometry import build_polygon
from app.interpretation.rooms import (
    DevicePlacement,
    Room,
    RoomLabel,
    assign_devices_to_rooms,
    assign_labels_to_rooms,
    room_from_polygon,
)

SQUARE = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]
INNER = [(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0)]


def _room(room_id: str, vertices: list[tuple[float, float]], *, name: str | None = None) -> Room:
    polygon = build_polygon(vertices)
    assert polygon is not None
    return room_from_polygon(room_id, polygon, source="test", name=name)


def test_room_from_polygon_derives_area_and_bounds() -> None:
    room = _room("r1", SQUARE)
    assert room.area == 100.0
    assert room.bounds == (0.0, 0.0, 10.0, 10.0)
    assert room.name is None
    assert room.source == "test"


# --- assign_labels_to_rooms ---


def test_label_inside_room_sets_name() -> None:
    rooms = [_room("r1", SQUARE)]
    labelled = assign_labels_to_rooms(rooms, [RoomLabel("Kitchen", (5.0, 5.0))])
    assert labelled[0].name == "Kitchen"


def test_label_outside_all_rooms_leaves_name_unassigned() -> None:
    rooms = [_room("r1", SQUARE)]
    labelled = assign_labels_to_rooms(rooms, [RoomLabel("Nowhere", (99.0, 99.0))])
    assert labelled[0].name is None


def test_label_in_nested_rooms_names_smallest() -> None:
    rooms = [_room("outer", SQUARE), _room("inner", INNER)]
    labelled = assign_labels_to_rooms(rooms, [RoomLabel("Closet", (5.0, 5.0))])
    by_id = {room.id: room for room in labelled}
    assert by_id["inner"].name == "Closet"
    assert by_id["outer"].name is None


def test_label_in_outer_only_names_outer() -> None:
    rooms = [_room("outer", SQUARE), _room("inner", INNER)]
    labelled = assign_labels_to_rooms(rooms, [RoomLabel("Hall", (1.0, 1.0))])
    by_id = {room.id: room for room in labelled}
    assert by_id["outer"].name == "Hall"
    assert by_id["inner"].name is None


def test_first_label_wins_for_a_room() -> None:
    rooms = [_room("r1", SQUARE)]
    labelled = assign_labels_to_rooms(
        rooms,
        [RoomLabel("First", (3.0, 3.0)), RoomLabel("Second", (7.0, 7.0))],
    )
    assert labelled[0].name == "First"


def test_assign_labels_does_not_mutate_inputs() -> None:
    rooms = [_room("r1", SQUARE)]
    assign_labels_to_rooms(rooms, [RoomLabel("Kitchen", (5.0, 5.0))])
    assert rooms[0].name is None  # original untouched


# --- assign_devices_to_rooms ---


def test_device_inside_room_is_assigned() -> None:
    rooms = [_room("r1", SQUARE)]
    assignments = assign_devices_to_rooms([DevicePlacement("d1", (5.0, 5.0))], rooms)
    assert len(assignments) == 1
    assert assignments[0].device_id == "d1"
    assert assignments[0].room_id == "r1"


def test_device_outside_all_rooms_is_unassigned() -> None:
    rooms = [_room("r1", SQUARE)]
    assignments = assign_devices_to_rooms([DevicePlacement("d1", (99.0, 99.0))], rooms)
    assert assignments == []


def test_device_in_nested_rooms_assigned_to_smallest() -> None:
    rooms = [_room("outer", SQUARE), _room("inner", INNER)]
    assignments = assign_devices_to_rooms([DevicePlacement("d1", (5.0, 5.0))], rooms)
    assert assignments[0].room_id == "inner"


def test_devices_assigned_independently_and_in_order() -> None:
    rooms = [_room("outer", SQUARE), _room("inner", INNER)]
    devices = [
        DevicePlacement("d_inner", (5.0, 5.0)),
        DevicePlacement("d_outer", (1.0, 1.0)),
        DevicePlacement("d_out", (50.0, 50.0)),
    ]
    assignments = assign_devices_to_rooms(devices, rooms)
    assert [(a.device_id, a.room_id) for a in assignments] == [
        ("d_inner", "inner"),
        ("d_outer", "outer"),
    ]


def test_no_rooms_means_no_assignments() -> None:
    assignments = assign_devices_to_rooms([DevicePlacement("d1", (5.0, 5.0))], [])
    assert assignments == []
