"""Unit tests for explicit-layer room interpretation (issue #427).

Fixture-driven over the EntityRow protocol; no DB, no mocks.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from app.interpretation.explicit_rooms import (
    ROOM_SOURCE_EXPLICIT_LAYER,
    build_rooms_from_entities,
    detect_room_boundary_layers,
    interpret_explicit_rooms,
    is_room_boundary_layer,
)
from app.interpretation.rooms import DevicePlacement, RoomLabel


@dataclass(frozen=True)
class _FakeEntity:
    """Minimal EntityRow stand-in for the interpretation helpers."""

    entity_id: str
    layer_ref: str | None
    geometry_json: Mapping[str, Any] | None
    sequence_index: int = 0
    block_ref: str | None = None


def _polyline(vertices: list[tuple[float, float]], *, closed: bool = True) -> dict[str, Any]:
    return {
        "kind": "polyline",
        "closed": closed,
        "vertices": [{"x": x, "y": y, "z": 0.0} for x, y in vertices],
    }


SQUARE = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]
INNER = [(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0)]


# --- layer detection ---


def test_is_room_boundary_layer_matches_conventions() -> None:
    assert is_room_boundary_layer("A-ROOM")
    assert is_room_boundary_layer("rooms")
    assert is_room_boundary_layer("I-SPACE-BNDY")
    assert is_room_boundary_layer("A-ROOM-RMSEP")
    assert not is_room_boundary_layer("A-WALL")
    assert not is_room_boundary_layer(None)


def test_detect_room_boundary_layers_returns_distinct_matches() -> None:
    entities = [
        _FakeEntity("e1", "A-ROOM", _polyline(SQUARE)),
        _FakeEntity("e2", "A-ROOM", _polyline(INNER)),
        _FakeEntity("e3", "A-WALL", _polyline(SQUARE)),
    ]
    assert detect_room_boundary_layers(entities) == {"A-ROOM"}


# --- build_rooms_from_entities ---


def test_build_rooms_skips_non_boundary_layers() -> None:
    entities = [
        _FakeEntity("room1", "A-ROOM", _polyline(SQUARE)),
        _FakeEntity("wall1", "A-WALL", _polyline(SQUARE)),
    ]
    rooms = build_rooms_from_entities(entities, layer_refs={"A-ROOM"})
    assert [room.id for room in rooms] == ["room1"]
    assert rooms[0].source == ROOM_SOURCE_EXPLICIT_LAYER
    assert rooms[0].area == 100.0


def test_build_rooms_skips_open_polylines() -> None:
    entities = [_FakeEntity("open1", "A-ROOM", _polyline(SQUARE, closed=False))]
    assert build_rooms_from_entities(entities, layer_refs={"A-ROOM"}) == []


def test_build_rooms_skips_degenerate_rings() -> None:
    entities = [_FakeEntity("bad", "A-ROOM", _polyline([(0.0, 0.0), (1.0, 0.0)]))]
    assert build_rooms_from_entities(entities, layer_refs={"A-ROOM"}) == []


def test_build_rooms_accepts_xy_pair_vertices() -> None:
    geometry = {"kind": "polyline", "closed": True, "vertices": [list(v) for v in SQUARE]}
    rooms = build_rooms_from_entities(
        [_FakeEntity("room1", "A-ROOM", geometry)], layer_refs={"A-ROOM"}
    )
    assert len(rooms) == 1
    assert rooms[0].area == 100.0


# --- full pipeline ---


def test_interpret_explicit_rooms_assigns_devices_and_names() -> None:
    entities = [
        _FakeEntity("outer", "A-ROOM", _polyline(SQUARE)),
        _FakeEntity("inner", "A-ROOM", _polyline(INNER)),
        _FakeEntity("wall", "A-WALL", _polyline(SQUARE)),
    ]
    devices = [
        DevicePlacement("dev_inner", (5.0, 5.0)),
        DevicePlacement("dev_outer", (1.0, 1.0)),
        DevicePlacement("dev_out", (99.0, 99.0)),
    ]
    labels = [
        RoomLabel("Closet", (5.0, 5.0)),
        RoomLabel("Hall", (1.0, 1.0)),
    ]

    result = interpret_explicit_rooms(entities, devices=devices, labels=labels)

    assert result.boundary_layers == ("A-ROOM",)
    rooms_by_id = {room.id: room for room in result.rooms}
    assert set(rooms_by_id) == {"outer", "inner"}  # wall layer excluded
    assert rooms_by_id["inner"].name == "Closet"
    assert rooms_by_id["outer"].name == "Hall"

    # Innermost room wins for the nested point; the outside device is unassigned.
    assignments = {a.device_id: a.room_id for a in result.device_assignments}
    assert assignments == {"dev_inner": "inner", "dev_outer": "outer"}


def test_interpret_explicit_rooms_honors_explicit_layer_refs() -> None:
    # A non-standard layer name is used directly via layer_refs (no auto-detection).
    entities = [_FakeEntity("room1", "ZONE-7", _polyline(SQUARE))]
    result = interpret_explicit_rooms(
        entities,
        devices=[DevicePlacement("d1", (5.0, 5.0))],
        labels=[],
        layer_refs={"ZONE-7"},
    )
    assert result.boundary_layers == ("ZONE-7",)
    assert [r.id for r in result.rooms] == ["room1"]
    assert result.device_assignments[0].room_id == "room1"


def test_interpret_explicit_rooms_no_boundary_layer_yields_nothing() -> None:
    entities = [_FakeEntity("wall", "A-WALL", _polyline(SQUARE))]
    result = interpret_explicit_rooms(
        entities,
        devices=[DevicePlacement("d1", (5.0, 5.0))],
        labels=[RoomLabel("X", (5.0, 5.0))],
    )
    assert result.rooms == []
    assert result.device_assignments == []
    assert result.boundary_layers == ()
