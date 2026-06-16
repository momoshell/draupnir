"""Room containment from IFC spaces (issue #429).

IFC models carry spaces natively (typed, bounded, named) — the definitive room
source. When IfcSpace footprint geometry is materialized (issue #462), this turns
those footprints into :class:`Room`s named from the space's own name/number, and
assigns devices by containment. Provenance is ``room_source = "ifc_space"``.

Pure over the :class:`EntityRow` protocol and the R0 primitives — unit-testable
with fixtures. IFC space footprints are identified by their canonical geometry
(``reason == "ifc_space_footprint"``), so this module needs no IFC-specific row
shape beyond ``geometry_json``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from app.interpretation.geometry import Point, build_polygon
from app.interpretation.models import EntityRow
from app.interpretation.rooms import (
    DevicePlacement,
    DeviceRoomAssignment,
    Room,
    RoomLabel,
    assign_devices_to_rooms,
    assign_labels_to_rooms,
    room_from_polygon,
)

ROOM_SOURCE_IFC_SPACE = "ifc_space"

_IFC_SPACE_FOOTPRINT_REASON = "ifc_space_footprint"


@dataclass(frozen=True, slots=True)
class IfcRoomInterpretation:
    """Result of deriving rooms from IFC spaces."""

    rooms: list[Room]
    device_assignments: list[DeviceRoomAssignment]


def has_ifc_space_footprints(entities: Sequence[EntityRow]) -> bool:
    """Return whether any entity carries an IfcSpace footprint geometry."""
    return any(_ifc_space_geometry(entity) is not None for entity in entities)


def build_rooms_from_ifc_spaces(
    entities: Sequence[EntityRow],
    *,
    source: str = ROOM_SOURCE_IFC_SPACE,
) -> list[Room]:
    """Build rooms from materialized IfcSpace footprints.

    Each space footprint becomes a room named from the space's own ``name``
    (LongName/Name) carried on the geometry; degenerate footprints are skipped.
    """
    rooms: list[Room] = []
    for entity in entities:
        geometry = _ifc_space_geometry(entity)
        if geometry is None:
            continue
        ring = _ring_from_geometry(geometry)
        if ring is None:
            continue
        polygon = build_polygon(ring)
        if polygon is None:
            continue
        rooms.append(
            room_from_polygon(
                entity.entity_id,
                polygon,
                source=source,
                name=_space_name(geometry),
            )
        )
    return rooms


def interpret_ifc_rooms(
    entities: Sequence[EntityRow],
    *,
    devices: Sequence[DevicePlacement],
    labels: Sequence[RoomLabel],
) -> IfcRoomInterpretation:
    """Build IFC-space rooms, name them, and assign devices by containment.

    Rooms are named from the IfcSpace name; any space left unnamed falls back to a
    room-tag label inside it (labels never override an explicit space name).
    """
    rooms = build_rooms_from_ifc_spaces(entities)
    rooms = assign_labels_to_rooms(rooms, labels)
    assignments = assign_devices_to_rooms(devices, rooms)
    return IfcRoomInterpretation(rooms=rooms, device_assignments=assignments)


def _ifc_space_geometry(entity: EntityRow) -> Mapping[str, Any] | None:
    """Return the entity's geometry mapping when it is an IfcSpace footprint."""
    geometry = entity.geometry_json
    if not isinstance(geometry, Mapping):
        return None
    if geometry.get("reason") != _IFC_SPACE_FOOTPRINT_REASON:
        return None
    return geometry


def _space_name(geometry: Mapping[str, Any]) -> str | None:
    name = geometry.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return None


def _ring_from_geometry(geometry: Mapping[str, Any]) -> list[Point] | None:
    raw_vertices = geometry.get("vertices")
    if not isinstance(raw_vertices, (list, tuple)) or not raw_vertices:
        return None
    ring: list[Point] = []
    for raw_vertex in raw_vertices:
        if not isinstance(raw_vertex, Mapping):
            return None
        x, y = raw_vertex.get("x"), raw_vertex.get("y")
        if (
            isinstance(x, (int, float))
            and not isinstance(x, bool)
            and isinstance(y, (int, float))
            and not isinstance(y, bool)
        ):
            ring.append((float(x), float(y)))
        else:
            return None
    return ring
