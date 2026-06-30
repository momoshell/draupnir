"""Unit tests for explicit-layer room interpretation (issue #427).

Fixture-driven over the EntityRow protocol; no DB, no mocks.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from app.interpretation.explicit_rooms import (
    ROOM_SOURCE_EXPLICIT_LAYER,
    build_rooms_anchored_by_labels,
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


# --- label-anchored path (#792): no dedicated boundary layer ---


# PDF-style large square (real-room size, 10x10 = 100 m2).
PDF_ROOM = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]
# Small symbol loop well below DEFAULT_LABEL_ANCHOR_MIN_AREA.
PDF_SYMBOL = [(1.0, 1.0), (1.1, 1.0), (1.1, 1.1), (1.0, 1.1)]


def _pdf_entity(entity_id: str, vertices: list[tuple[float, float]]) -> _FakeEntity:
    """Closed polyline on the PDF default layer (no dedicated room layer)."""
    return _FakeEntity(entity_id, "default", _polyline(vertices))


def test_build_rooms_anchored_by_labels_keeps_numbered_anchored_polygons() -> None:
    # One large polygon with a numbered label inside → becomes a room.
    entities = [_pdf_entity("room-poly", PDF_ROOM)]
    labels = [RoomLabel("0.2.01", (5.0, 5.0))]
    rooms = build_rooms_anchored_by_labels(entities, labels)
    assert len(rooms) == 1
    assert rooms[0].id == "room-poly"


def test_build_rooms_anchored_by_labels_excludes_symbol_loops() -> None:
    # Many small symbol/fixture polygons with no numbered label inside → all excluded.
    entities = [_pdf_entity(f"sym-{i}", PDF_SYMBOL) for i in range(20)]
    labels = [RoomLabel("0.2.01", (5.0, 5.0))]  # numbered label far outside all symbols
    rooms = build_rooms_anchored_by_labels(entities, labels)
    assert rooms == []


def test_build_rooms_anchored_by_labels_name_only_label_does_not_anchor() -> None:
    # A name-only label (no room number) inside a polygon → polygon is NOT a room.
    entities = [_pdf_entity("poly", PDF_ROOM)]
    labels = [RoomLabel("Plantroom", (5.0, 5.0))]  # name only, no number
    rooms = build_rooms_anchored_by_labels(entities, labels)
    assert rooms == []


def test_build_rooms_anchored_by_labels_min_area_drops_tiny_loops() -> None:
    # A polygon with a numbered label inside but below min_area is dropped.
    entities = [_pdf_entity("tiny", PDF_SYMBOL)]
    labels = [RoomLabel("0.2.01", (1.05, 1.05))]  # label inside the tiny polygon
    rooms = build_rooms_anchored_by_labels(entities, labels, min_area=1.0)
    assert rooms == []


def test_build_rooms_anchored_by_labels_no_numbered_labels_returns_empty() -> None:
    entities = [_pdf_entity("room-poly", PDF_ROOM)]
    labels: list[RoomLabel] = []
    rooms = build_rooms_anchored_by_labels(entities, labels)
    assert rooms == []


def test_interpret_explicit_rooms_pdf_path_uses_label_anchoring() -> None:
    # PDF scenario: many closed polylines (only one contains a numbered label);
    # no room-keyword layer detected → label-anchored path; only the numbered-anchored
    # polygon becomes a room.
    room_poly = _pdf_entity("room-1", PDF_ROOM)
    # Fourteen symbol/hatch/title-block loops scattered at different positions.
    symbol_entities = [
        _pdf_entity(
            f"sym-{i}",
            [(i * 20.0, 50.0), (i * 20.0 + 0.2, 50.0), (i * 20.0 + 0.2, 50.2), (i * 20.0, 50.2)],
        )
        for i in range(14)
    ]
    entities = [room_poly, *symbol_entities]
    labels = [
        RoomLabel("Plantroom", (5.0, 5.4)),  # name label (no number)
        RoomLabel("0.2.01", (5.0, 5.0)),  # numbered label inside PDF_ROOM
    ]
    result = interpret_explicit_rooms(entities, devices=[], labels=labels)
    assert result.boundary_layers == ()
    assert len(result.rooms) == 1
    assert result.rooms[0].id == "room-1"


def test_interpret_explicit_rooms_dwg_boundary_layer_unchanged() -> None:
    # DWG scenario: boundary layer detected → byte-identical behaviour to before.
    # Both polygons are on A-ROOM; the symbol polygon on 'default' is excluded.
    entities = [
        _FakeEntity("outer", "A-ROOM", _polyline(SQUARE)),
        _FakeEntity("inner", "A-ROOM", _polyline(INNER)),
        _FakeEntity("symbol", "default", _polyline(INNER)),  # different layer → excluded
    ]
    result = interpret_explicit_rooms(entities, devices=[], labels=[])
    assert result.boundary_layers == ("A-ROOM",)
    assert {r.id for r in result.rooms} == {"outer", "inner"}


def test_interpret_explicit_rooms_pdf_label_only_room_surfaces_once() -> None:
    # A numbered label with no containing polygon in the PDF path → surfaces as a
    # label-only room once via the pipeline (_enrich_with_labels).  This is handled
    # upstream in room_pipeline; here we verify the explicit module itself returns
    # no spurious rooms for the orphan number.
    entities: list[_FakeEntity] = []  # no polygons at all
    labels = [
        RoomLabel("Server Room", (20.0, 20.4)),
        RoomLabel("0.2.09", (20.0, 20.0)),
    ]
    result = interpret_explicit_rooms(entities, devices=[], labels=labels)
    assert result.rooms == []  # no polygon → nothing here; pipeline adds label-only room
