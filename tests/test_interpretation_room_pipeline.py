"""Unit tests for room strategy selection + route adapters (issue #479).

Pure / fixture-driven; no DB. The endpoint's DB-backed behavior is covered by the
API integration lane (test_rooms_api.py).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from app.api.v1.revision_routes.rooms import _device_placements, _room_labels
from app.interpretation.devices import Device, _TagCandidate
from app.interpretation.room_pipeline import (
    ROOM_STRATEGY_EXPLICIT,
    ROOM_STRATEGY_IFC,
    ROOM_STRATEGY_WALLS,
    interpret_rooms,
)
from app.interpretation.rooms import DevicePlacement, RoomLabel


@dataclass(frozen=True)
class _FakeEntity:
    entity_id: str
    layer_ref: str | None
    geometry_json: Mapping[str, Any] | None
    sequence_index: int = 0
    block_ref: str | None = None


def _closed_polyline(vertices: list[tuple[float, float]], *, layer: str) -> _FakeEntity:
    return _FakeEntity(
        f"poly-{layer}-{vertices[0]}",
        layer,
        {
            "kind": "polyline",
            "closed": True,
            "vertices": [{"x": x, "y": y, "z": 0.0} for x, y in vertices],
        },
    )


def _wall_line(start: tuple[float, float], end: tuple[float, float]) -> _FakeEntity:
    return _FakeEntity(
        f"line-{start}-{end}",
        "A-WALL",
        {
            "kind": "line",
            "start": {"x": start[0], "y": start[1]},
            "end": {"x": end[0], "y": end[1]},
        },
    )


def _ifc_space(vertices: list[tuple[float, float]], *, name: str | None = None) -> _FakeEntity:
    return _FakeEntity(
        f"space-{vertices[0]}",
        None,
        {
            "kind": "polygon",
            "closed": True,
            "reason": "ifc_space_footprint",
            "name": name,
            "vertices": [{"x": x, "y": y, "z": 0.0} for x, y in vertices],
        },
    )


SQUARE = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]


def _square_wall_lines() -> list[_FakeEntity]:
    closed = [*SQUARE, SQUARE[0]]
    return [_wall_line(closed[i], closed[i + 1]) for i in range(len(SQUARE))]


# --- strategy selection ---


def test_auto_prefers_explicit_layer_when_present() -> None:
    entities = [_closed_polyline(SQUARE, layer="A-ROOM"), *_square_wall_lines()]
    result = interpret_rooms(entities, devices=[], labels=[])
    assert result.strategy == ROOM_STRATEGY_EXPLICIT
    assert result.source_layers == ("A-ROOM",)
    assert len(result.rooms) == 1


def test_auto_prefers_ifc_spaces_over_explicit_and_walls() -> None:
    entities = [
        _ifc_space(SQUARE, name="Atrium"),
        _closed_polyline(SQUARE, layer="A-ROOM"),
        *_square_wall_lines(),
    ]
    result = interpret_rooms(entities, devices=[], labels=[])
    assert result.strategy == ROOM_STRATEGY_IFC
    assert result.source_layers == ()
    assert len(result.rooms) == 1
    assert result.rooms[0].name == "Atrium"


def test_ifc_strategy_forced_even_when_empty() -> None:
    result = interpret_rooms(
        _square_wall_lines(), devices=[], labels=[], strategy=ROOM_STRATEGY_IFC
    )
    assert result.strategy == ROOM_STRATEGY_IFC
    assert result.rooms == []


def test_auto_skips_ifc_when_no_spaces_present() -> None:
    entities = [_closed_polyline(SQUARE, layer="A-ROOM")]
    result = interpret_rooms(entities, devices=[], labels=[])
    assert result.strategy == ROOM_STRATEGY_EXPLICIT


def test_auto_falls_back_to_walls_without_explicit_layer() -> None:
    result = interpret_rooms(_square_wall_lines(), devices=[], labels=[])
    assert result.strategy == ROOM_STRATEGY_WALLS
    assert result.source_layers == ("A-WALL",)
    assert len(result.rooms) == 1


def test_explicit_strategy_forced_even_when_empty() -> None:
    # Forcing explicit returns the explicit (empty) result, never the wall fallback.
    result = interpret_rooms(
        _square_wall_lines(), devices=[], labels=[], strategy=ROOM_STRATEGY_EXPLICIT
    )
    assert result.strategy == ROOM_STRATEGY_EXPLICIT
    assert result.rooms == []


def test_walls_strategy_forced_ignores_explicit_layer() -> None:
    entities = [_closed_polyline(SQUARE, layer="A-ROOM"), *_square_wall_lines()]
    result = interpret_rooms(entities, devices=[], labels=[], strategy=ROOM_STRATEGY_WALLS)
    assert result.strategy == ROOM_STRATEGY_WALLS
    assert result.source_layers == ("A-WALL",)


def test_auto_assigns_devices_and_names_via_chosen_source() -> None:
    entities = [_closed_polyline(SQUARE, layer="A-ROOM")]
    result = interpret_rooms(
        entities,
        devices=[DevicePlacement("d1", (5.0, 5.0))],
        labels=[RoomLabel("Lab", (5.0, 5.0))],
    )
    assert result.rooms[0].name == "Lab"
    assert result.device_assignments[0].device_id == "d1"


def test_snap_tolerance_applies_only_to_wall_strategy() -> None:
    # Walls with a door gap: auto falls through to walls and snapping closes it.
    walls = [
        _wall_line((0.0, 0.0), (4.5, 0.0)),
        _wall_line((5.5, 0.0), (10.0, 0.0)),
        _wall_line((10.0, 0.0), (10.0, 10.0)),
        _wall_line((10.0, 10.0), (0.0, 10.0)),
        _wall_line((0.0, 10.0), (0.0, 0.0)),
    ]
    assert interpret_rooms(walls, devices=[], labels=[]).rooms == []
    closed = interpret_rooms(walls, devices=[], labels=[], snap_tolerance=1.0)
    assert len(closed.rooms) == 1
    assert closed.strategy == ROOM_STRATEGY_WALLS


# --- route adapters ---


def _device(entity_id: str, position: dict[str, float] | None) -> Device:
    return Device(
        entity_id=entity_id,
        sequence_index=0,
        depth=0,
        block_ref=None,
        layer_ref=None,
        position=position,
        tag=None,
    )


def test_device_placements_skips_devices_without_position() -> None:
    devices = [
        _device("d1", {"x": 1.0, "y": 2.0}),
        _device("d2", None),
        _device("d3", {"x": 3.0, "y": 4.0}),
    ]
    placements = _device_placements(devices)
    assert [(p.device_id, p.point) for p in placements] == [
        ("d1", (1.0, 2.0)),
        ("d3", (3.0, 4.0)),
    ]


def test_room_labels_from_tag_candidates() -> None:
    candidates = [
        _TagCandidate(entity_id="t1", text="Kitchen", layer_ref="A-ANNO", x=5.0, y=6.0),
    ]
    labels = _room_labels(candidates)
    assert labels == [RoomLabel(text="Kitchen", point=(5.0, 6.0))]
