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
    assert labels == [RoomLabel(text="Kitchen", point=(5.0, 6.0), layer="A-ANNO")]


# --- label enrichment + label-only rooms (#549) ---


def test_labels_stamp_name_and_number_onto_containing_polygon_room() -> None:
    # A wall square room with a name + number label (on a room-label layer) inside it.
    entities = _square_wall_lines()
    labels = [
        RoomLabel("PH Plantroom", (5.0, 5.4), layer="A-IDEN"),
        RoomLabel("0.9.01", (5.0, 5.0), layer="A-IDEN"),
    ]
    result = interpret_rooms(entities, devices=[], labels=labels)
    named = [room for room in result.rooms if room.name == "PH Plantroom"]
    assert len(named) == 1
    assert named[0].number == "0.9.01"
    assert named[0].polygon is not None  # stamped onto the wall polygon


def test_label_only_room_surfaced_when_no_polygon() -> None:
    # No geometry → no polygon; the label cluster still yields a named, numbered room.
    labels = [
        RoomLabel("Telco Intake", (50.0, 50.4), layer="A-IDEN"),
        RoomLabel("0.9.09", (50.0, 50.0), layer="A-IDEN"),
    ]
    result = interpret_rooms([], devices=[], labels=labels)
    label_rooms = [room for room in result.rooms if room.source == "label_cluster"]
    assert len(label_rooms) == 1
    room = label_rooms[0]
    assert room.name == "Telco Intake"
    assert room.number == "0.9.09"
    assert room.polygon is None
    assert room.location == (50.0, 50.0)


def test_device_tag_layer_text_does_not_become_a_room() -> None:
    # Device-tag letters on a non-room-label layer must not be paired as room names.
    labels = [
        RoomLabel("AHU Plant", (5.0, 5.4), layer="A-IDEN"),
        RoomLabel("0.9.04", (5.0, 5.0), layer="A-IDEN"),
        RoomLabel("S", (5.1, 5.0), layer="Z000"),  # nearer device tag on another layer
        RoomLabel("DC", (5.2, 5.0), layer="Z000"),
    ]
    result = interpret_rooms([], devices=[], labels=labels)
    label_rooms = [room for room in result.rooms if room.source == "label_cluster"]
    assert len(label_rooms) == 1
    assert label_rooms[0].name == "AHU Plant"
    assert label_rooms[0].number == "0.9.04"


def test_devices_assigned_to_label_only_room_by_proximity() -> None:
    # No geometry → label-only rooms; a device near a labeled room point is assigned to it.
    labels = [
        RoomLabel("PH Plantroom", (10.0, 10.4), layer="A-IDEN"),
        RoomLabel("0.9.01", (10.0, 10.0), layer="A-IDEN"),
        RoomLabel("Telco Intake", (50.0, 50.4), layer="A-IDEN"),
        RoomLabel("0.9.09", (50.0, 50.0), layer="A-IDEN"),
    ]
    devices = [
        DevicePlacement("cam-1", (11.0, 11.0)),  # near PH Plantroom
        DevicePlacement("cam-2", (51.0, 49.0)),  # near Telco Intake
        DevicePlacement("cam-far", (500.0, 500.0)),  # beyond radius of any room
    ]
    result = interpret_rooms([], devices=devices, labels=labels)
    rooms_by_number = {room.number: room.id for room in result.rooms if room.number}
    assigned = {a.device_id: a.room_id for a in result.device_assignments}
    assert assigned["cam-1"] == rooms_by_number["0.9.01"]
    assert assigned["cam-2"] == rooms_by_number["0.9.09"]
    assert "cam-far" not in assigned  # beyond the heuristic radius → unassigned


def test_polygon_containment_wins_over_label_proximity() -> None:
    # A device inside a wall polygon is assigned by containment, not nearest label room.
    entities = _square_wall_lines()  # SQUARE 0..10
    labels = [
        RoomLabel("Plant", (5.0, 5.4), layer="A-IDEN"),
        RoomLabel("0.9.01", (5.0, 5.0), layer="A-IDEN"),
    ]
    devices = [DevicePlacement("d1", (5.0, 5.0))]
    result = interpret_rooms(entities, devices=devices, labels=labels)
    assigned = {a.device_id: a.room_id for a in result.device_assignments}
    target = next(r for r in result.rooms if r.polygon is not None and r.name == "Plant")
    assert assigned["d1"] == target.id


# --- #581: dedupe rooms by number (U-shape) + wall-aware confirmation ---


def _square_wall_lines_at(dx: float, dy: float) -> list[_FakeEntity]:
    square = [(x + dx, y + dy) for x, y in SQUARE]
    closed = [*square, square[0]]
    return [_wall_line(closed[i], closed[i + 1]) for i in range(len(square))]


def test_same_number_label_rooms_merge_into_one_with_all_anchors() -> None:
    # A U-shaped room labelled in each of its 3 arms (no internal walls → no polygon): one
    # number, three placements → ONE room retaining all three anchor points (#581).
    labels = [
        RoomLabel("AHU Plant", (10.0, 10.4), layer="A-IDEN"),
        RoomLabel("0.9.04", (10.0, 10.0), layer="A-IDEN"),
        RoomLabel("AHU Plant", (30.0, 10.4), layer="A-IDEN"),
        RoomLabel("0.9.04", (30.0, 10.0), layer="A-IDEN"),
        RoomLabel("AHU Plant", (50.0, 10.4), layer="A-IDEN"),
        RoomLabel("0.9.04", (50.0, 10.0), layer="A-IDEN"),
    ]
    result = interpret_rooms([], devices=[], labels=labels)
    ahu = [room for room in result.rooms if room.number == "0.9.04"]
    assert len(ahu) == 1
    assert ahu[0].name == "AHU Plant"
    assert set(ahu[0].anchors) == {(10.0, 10.0), (30.0, 10.0), (50.0, 10.0)}
    assert ahu[0].needs_review is False  # label-only → no wall evidence → merge, no flag


def test_device_near_any_arm_assigned_to_merged_room() -> None:
    # Device proximity uses the NEAREST anchor, so a device by any arm of the merged U-shape
    # room is assigned to it (#581).
    labels = [
        RoomLabel("AHU Plant", (10.0, 10.4), layer="A-IDEN"),
        RoomLabel("0.9.04", (10.0, 10.0), layer="A-IDEN"),
        RoomLabel("AHU Plant", (50.0, 10.4), layer="A-IDEN"),
        RoomLabel("0.9.04", (50.0, 10.0), layer="A-IDEN"),
    ]
    devices = [DevicePlacement("cam-far-arm", (50.5, 10.0))]  # near the SECOND arm only
    result = interpret_rooms([], devices=devices, labels=labels)
    ahu = next(room for room in result.rooms if room.number == "0.9.04")
    assigned = {a.device_id: a.room_id for a in result.device_assignments}
    assert assigned["cam-far-arm"] == ahu.id


def test_same_number_across_walls_is_flagged_not_merged() -> None:
    # The same number inside TWO wall-bounded rooms (separated by walls) is a likely numbering
    # error: surface both for review, do NOT silently merge them (#581).
    entities = [*_square_wall_lines(), *_square_wall_lines_at(100.0, 100.0)]
    labels = [
        RoomLabel("AHU Plant", (5.0, 5.4), layer="A-IDEN"),
        RoomLabel("0.9.04", (5.0, 5.0), layer="A-IDEN"),
        RoomLabel("AHU Plant", (105.0, 105.4), layer="A-IDEN"),
        RoomLabel("0.9.04", (105.0, 105.0), layer="A-IDEN"),
    ]
    result = interpret_rooms(entities, devices=[], labels=labels)
    numbered = [room for room in result.rooms if room.number == "0.9.04"]
    assert len(numbered) == 2  # NOT merged — two distinct wall-bounded rooms
    assert all(room.polygon is not None for room in numbered)
    assert all(room.needs_review for room in numbered)  # both flagged for review


def test_dedupe_label_rooms_by_number_unit() -> None:
    from app.interpretation.rooms import (
        dedupe_label_rooms_by_number,
        room_from_label,
    )

    rooms = [
        room_from_label("a", source="label_cluster", location=(0.0, 0.0), number="1", name="Lab"),
        room_from_label("b", source="label_cluster", location=(9.0, 0.0), number="1"),
        room_from_label("c", source="label_cluster", location=(5.0, 5.0), number="2", name="WC"),
        room_from_label("d", source="label_cluster", location=(7.0, 7.0), name="Hall"),  # no num
    ]
    deduped = dedupe_label_rooms_by_number(rooms)
    by_number = {room.number: room for room in deduped if room.number}
    assert len(deduped) == 3  # 1 (merged), 2, and the name-only Hall
    assert set(by_number["1"].anchors) == {(0.0, 0.0), (9.0, 0.0)}
    assert by_number["1"].name == "Lab"  # first non-null name retained
    assert by_number["2"].anchors == ((5.0, 5.0),)
    assert any(room.number is None and room.name == "Hall" for room in deduped)
