"""Unit tests for wall-derived room interpretation (issue #463, #428 core).

Fixture-driven over the EntityRow protocol; no DB, no mocks.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from app.interpretation.rooms import DevicePlacement, RoomLabel
from app.interpretation.wall_rooms import (
    GAP_CLOSED_ROOM_CONFIDENCE,
    ROOM_SOURCE_WALL_POLYGONIZE,
    SELECTION_POLYGONIZE_YIELD,
    WALL_ROOM_CONFIDENCE,
    build_rooms_from_walls,
    detect_wall_layers,
    interpret_wall_rooms,
    is_wall_layer,
    select_wall_layers,
    wall_segments,
)


@dataclass(frozen=True)
class _FakeEntity:
    entity_id: str
    layer_ref: str | None
    geometry_json: Mapping[str, Any] | None
    sequence_index: int = 0
    block_ref: str | None = None
    style: Mapping[str, Any] | None = None


def _line(
    start: tuple[float, float],
    end: tuple[float, float],
    *,
    layer: str = "A-WALL",
    dashed: bool = False,
) -> _FakeEntity:
    geometry = {
        "kind": "line",
        "start": {"x": start[0], "y": start[1], "z": 0.0},
        "end": {"x": end[0], "y": end[1], "z": 0.0},
    }
    style = {"linetype": {"name": "DASHED" if dashed else "Continuous", "dashed": dashed}}
    return _FakeEntity(f"line-{start}-{end}-{layer}", layer, geometry, style=style)


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
    corners: list[tuple[float, float]], *, layer: str = "A-WALL", dashed: bool = False
) -> list[_FakeEntity]:
    closed = [*corners, corners[0]]
    return [
        _line(closed[i], closed[i + 1], layer=layer, dashed=dashed) for i in range(len(corners))
    ]


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


# --- wall-layer selection (#554): named-first, else by polygonization yield ---


def test_select_wall_layers_picks_unnamed_architecture_layer_by_yield() -> None:
    # No "wall"-named layer; the architecture layer "A210" forms a closed face, the
    # annotation layer "A-ANNO" is a stray non-enclosing line → only A210 is selected.
    entities = [
        *_square_walls(SQUARE, layer="A210"),
        _line((100.0, 100.0), (105.0, 100.0), layer="A-ANNO"),
    ]
    layers, method = select_wall_layers(entities)
    assert layers == {"A210"}
    assert method == SELECTION_POLYGONIZE_YIELD


def test_select_wall_layers_excludes_dashed_grid() -> None:
    # A dashed structural grid spans a larger square (would polygonize into a big face), but
    # being dashed it is excluded so it doesn't compete with the solid architecture layer.
    grid = _square_walls(
        [(-5.0, -5.0), (15.0, -5.0), (15.0, 15.0), (-5.0, 15.0)], layer="Z030G", dashed=True
    )
    entities = [*_square_walls(SQUARE, layer="A210"), *grid]
    layers, method = select_wall_layers(entities)
    assert layers == {"A210"}
    assert method == SELECTION_POLYGONIZE_YIELD


def test_interpret_wall_rooms_auto_selects_architecture_layer() -> None:
    # End-to-end: a clean closed room on a non-"wall" layer + a dashed grid → one room from
    # the architecture layer, recorded via the polygonize-yield selection method.
    entities = [
        *_square_walls(SQUARE, layer="A210"),
        *_square_walls(
            [(-5.0, -5.0), (15.0, -5.0), (15.0, 15.0), (-5.0, 15.0)], layer="GRID", dashed=True
        ),
    ]
    result = interpret_wall_rooms(entities, devices=[], labels=[])
    assert result.wall_layers == ("A210",)
    assert result.selection_method == SELECTION_POLYGONIZE_YIELD
    assert len(result.rooms) == 1


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


# --- robustness (#428b): gap-close, slivers, confidence ---


def _walls_with_door_gap(gap: float) -> list[_FakeEntity]:
    # A square whose bottom edge has a `gap`-wide door opening at the middle.
    return [
        _line((0.0, 0.0), (5.0 - gap / 2, 0.0)),  # bottom-left up to the opening
        _line((5.0 + gap / 2, 0.0), (10.0, 0.0)),  # bottom-right after the opening
        _line((10.0, 0.0), (10.0, 10.0)),
        _line((10.0, 10.0), (0.0, 10.0)),
        _line((0.0, 10.0), (0.0, 0.0)),
    ]


def test_door_gap_breaks_loop_without_snapping() -> None:
    rooms = build_rooms_from_walls(_walls_with_door_gap(0.9), layer_refs={"A-WALL"})
    assert rooms == []  # the opening leaves the loop unclosed


def test_snap_tolerance_closes_door_gap_and_lowers_confidence() -> None:
    rooms = build_rooms_from_walls(
        _walls_with_door_gap(0.9), layer_refs={"A-WALL"}, snap_tolerance=1.0
    )
    assert len(rooms) == 1
    assert rooms[0].confidence == GAP_CLOSED_ROOM_CONFIDENCE


def test_clean_loop_keeps_base_confidence_even_with_snap_enabled() -> None:
    # Snapping is enabled but nothing needs bridging -> base confidence.
    rooms = build_rooms_from_walls(_square_walls(SQUARE), layer_refs={"A-WALL"}, snap_tolerance=1.0)
    assert len(rooms) == 1
    assert rooms[0].confidence == WALL_ROOM_CONFIDENCE


def test_min_area_drops_sliver_faces() -> None:
    # A big room plus a tiny 1x1 sliver room; min_area filters the sliver.
    big = _square_walls(SQUARE)
    sliver = _square_walls([(20.0, 20.0), (21.0, 20.0), (21.0, 21.0), (20.0, 21.0)])
    rooms = build_rooms_from_walls([*big, *sliver], layer_refs={"A-WALL"}, min_area=10.0)
    assert [room.area for room in rooms] == [100.0]


def test_snap_tolerance_does_not_over_merge_distinct_corners() -> None:
    # A clean square with tolerance smaller than the shortest edge stays one room.
    rooms = build_rooms_from_walls(_square_walls(SQUARE), layer_refs={"A-WALL"}, snap_tolerance=0.5)
    assert len(rooms) == 1
    assert rooms[0].area == 100.0
