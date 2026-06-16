"""Unit tests for wall-derived room interpretation (issue #463, #428 core).

Fixture-driven over the EntityRow protocol; no DB, no mocks.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from app.interpretation.rooms import DevicePlacement, RoomLabel
from app.interpretation.wall_rooms import (
    ROOM_SOURCE_WALL_POLYGONIZE,
    build_rooms_from_walls,
    detect_wall_layers,
    interpret_wall_rooms,
    is_wall_layer,
    wall_segments,
)


@dataclass(frozen=True)
class _FakeEntity:
    entity_id: str
    layer_ref: str | None
    geometry_json: Mapping[str, Any] | None
    sequence_index: int = 0
    block_ref: str | None = None


def _line(
    start: tuple[float, float], end: tuple[float, float], *, layer: str = "A-WALL"
) -> _FakeEntity:
    geometry = {
        "kind": "line",
        "start": {"x": start[0], "y": start[1], "z": 0.0},
        "end": {"x": end[0], "y": end[1], "z": 0.0},
    }
    return _FakeEntity(f"line-{start}-{end}", layer, geometry)


def _polyline(
    vertices: list[tuple[float, float]], *, closed: bool, layer: str = "A-WALL"
) -> _FakeEntity:
    geometry = {
        "kind": "polyline",
        "closed": closed,
        "vertices": [{"x": x, "y": y, "z": 0.0} for x, y in vertices],
    }
    return _FakeEntity(f"poly-{vertices[0]}", layer, geometry)


def _square_walls(
    corners: list[tuple[float, float]], *, layer: str = "A-WALL"
) -> list[_FakeEntity]:
    closed = [*corners, corners[0]]
    return [_line(closed[i], closed[i + 1], layer=layer) for i in range(len(corners))]


SQUARE = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]


# --- layer detection ---


def test_is_wall_layer() -> None:
    assert is_wall_layer("A-WALL")
    assert is_wall_layer("walls")
    assert not is_wall_layer("A-ROOM")
    assert not is_wall_layer(None)


def test_detect_wall_layers() -> None:
    entities = [*_square_walls(SQUARE), _line((0.0, 0.0), (1.0, 1.0), layer="A-DOOR")]
    assert detect_wall_layers(entities) == {"A-WALL"}


# --- segment extraction ---


def test_wall_segments_from_lines() -> None:
    segments = wall_segments(_square_walls(SQUARE), layer_refs={"A-WALL"})
    assert len(segments) == 4
    assert ((0.0, 0.0), (10.0, 0.0)) in segments


def test_wall_segments_from_open_polyline() -> None:
    entity = _polyline([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0)], closed=False)
    segments = wall_segments([entity], layer_refs={"A-WALL"})
    assert segments == [((0.0, 0.0), (10.0, 0.0)), ((10.0, 0.0), (10.0, 10.0))]


def test_wall_segments_closes_closed_polyline() -> None:
    entity = _polyline(SQUARE, closed=True)
    segments = wall_segments([entity], layer_refs={"A-WALL"})
    assert len(segments) == 4
    assert ((0.0, 10.0), (0.0, 0.0)) in segments  # closing segment


# --- build / polygonize ---


def test_build_rooms_from_closed_wall_loop() -> None:
    rooms = build_rooms_from_walls(_square_walls(SQUARE), layer_refs={"A-WALL"})
    assert len(rooms) == 1
    assert rooms[0].area == 100.0
    assert rooms[0].source == ROOM_SOURCE_WALL_POLYGONIZE
    assert rooms[0].id == "wall-room-0"


def test_open_wall_run_yields_no_room() -> None:
    walls = _square_walls(SQUARE)[:-1]  # three sides -> not closed
    assert build_rooms_from_walls(walls, layer_refs={"A-WALL"}) == []


def test_two_adjacent_rooms_share_a_wall() -> None:
    # Two unit squares sharing the x=10 edge -> two faces.
    left = _square_walls([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)])
    right = _square_walls([(10.0, 0.0), (20.0, 0.0), (20.0, 10.0), (10.0, 10.0)])
    rooms = build_rooms_from_walls([*left, *right], layer_refs={"A-WALL"})
    assert len(rooms) == 2
    # Deterministic ordering by (bounds, area): left room first.
    assert [room.id for room in rooms] == ["wall-room-0", "wall-room-1"]
    assert rooms[0].bounds == (0.0, 0.0, 10.0, 10.0)
    assert rooms[1].bounds == (10.0, 0.0, 20.0, 10.0)


# --- full pipeline ---


def test_interpret_wall_rooms_assigns_devices_and_names() -> None:
    entities = [*_square_walls(SQUARE), _line((0.0, 0.0), (1.0, 1.0), layer="A-DOOR")]
    result = interpret_wall_rooms(
        entities,
        devices=[DevicePlacement("d1", (5.0, 5.0)), DevicePlacement("d_out", (99.0, 99.0))],
        labels=[RoomLabel("Office", (5.0, 5.0))],
    )
    assert result.wall_layers == ("A-WALL",)
    assert len(result.rooms) == 1
    assert result.rooms[0].name == "Office"
    assignments = {a.device_id: a.room_id for a in result.device_assignments}
    assert assignments == {"d1": "wall-room-0"}


def test_interpret_wall_rooms_no_wall_layer() -> None:
    entities = [_line((0.0, 0.0), (1.0, 1.0), layer="A-ROOM")]
    result = interpret_wall_rooms(entities, devices=[], labels=[])
    assert result.rooms == []
    assert result.wall_layers == ()
