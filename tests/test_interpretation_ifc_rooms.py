"""Unit tests for IFC-space room interpretation (issue #429).

Fixture-driven over the EntityRow protocol; no DB, no real IFC.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from app.interpretation.ifc_rooms import (
    ROOM_SOURCE_IFC_SPACE,
    build_rooms_from_ifc_spaces,
    has_ifc_space_footprints,
    interpret_ifc_rooms,
)
from app.interpretation.rooms import DevicePlacement, RoomLabel

SQUARE = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]


@dataclass(frozen=True)
class _FakeEntity:
    entity_id: str
    layer_ref: str | None
    geometry_json: Mapping[str, Any] | None
    sequence_index: int = 0
    block_ref: str | None = None


def _space_footprint(
    vertices: list[tuple[float, float]], *, name: str | None = None
) -> dict[str, Any]:
    return {
        "kind": "polygon",
        "closed": True,
        "reason": "ifc_space_footprint",
        "name": name,
        "vertices": [{"x": x, "y": y, "z": 0.0} for x, y in vertices],
    }


def _semantic_stub() -> dict[str, Any]:
    return {"status": "absent", "reason": "semantic_metadata_only"}


def test_has_ifc_space_footprints() -> None:
    assert has_ifc_space_footprints([_FakeEntity("s1", None, _space_footprint(SQUARE))])
    assert not has_ifc_space_footprints([_FakeEntity("w1", "A-WALL", _semantic_stub())])


def test_build_rooms_from_ifc_spaces_uses_space_name() -> None:
    entities = [
        _FakeEntity("space-1", None, _space_footprint(SQUARE, name="Office 101")),
        _FakeEntity("wall-1", "A-WALL", _semantic_stub()),
    ]
    rooms = build_rooms_from_ifc_spaces(entities)
    assert len(rooms) == 1
    assert rooms[0].id == "space-1"
    assert rooms[0].name == "Office 101"
    assert rooms[0].source == ROOM_SOURCE_IFC_SPACE
    assert rooms[0].area == 100.0


def test_build_rooms_skips_degenerate_footprint() -> None:
    entities = [_FakeEntity("bad", None, _space_footprint([(0.0, 0.0), (1.0, 1.0)]))]
    assert build_rooms_from_ifc_spaces(entities) == []


def test_interpret_ifc_rooms_assigns_devices() -> None:
    entities = [_FakeEntity("space-1", None, _space_footprint(SQUARE, name="Lab"))]
    result = interpret_ifc_rooms(
        entities,
        devices=[DevicePlacement("d1", (5.0, 5.0)), DevicePlacement("out", (99.0, 99.0))],
        labels=[],
    )
    assert result.rooms[0].name == "Lab"
    assert [(a.device_id, a.room_id) for a in result.device_assignments] == [("d1", "space-1")]


def test_interpret_ifc_rooms_label_names_unnamed_space_only() -> None:
    # A space with no name is named from a tag inside it; a named space is untouched.
    entities = [
        _FakeEntity("named", None, _space_footprint(SQUARE, name="Server Room")),
        _FakeEntity(
            "unnamed",
            None,
            _space_footprint([(20.0, 0.0), (30.0, 0.0), (30.0, 10.0), (20.0, 10.0)]),
        ),
    ]
    result = interpret_ifc_rooms(
        entities,
        devices=[],
        labels=[RoomLabel("Tagged", (25.0, 5.0)), RoomLabel("Ignored", (5.0, 5.0))],
    )
    by_id = {room.id: room for room in result.rooms}
    assert by_id["named"].name == "Server Room"  # explicit space name kept
    assert by_id["unnamed"].name == "Tagged"  # label fills the unnamed space
