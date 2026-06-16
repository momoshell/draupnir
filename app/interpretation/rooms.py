"""Pure room model and label/device assignment (issue #461, Phase 2).

Builds on the geometry primitives (R0a, ``app.interpretation.geometry``). All
assignment is boundary-aware (point-in-polygon over the room geometry) and never
nearest-label: a label or device belongs to the *smallest* room whose polygon
contains its point, so nested spaces resolve to the innermost room. No DB or
FastAPI concerns, so every function is unit-testable with fakes.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace

from shapely.geometry import Polygon

from app.interpretation.geometry import (
    BoundingBox,
    Point,
    bounding_box,
    point_in_polygon,
    polygon_area,
)


@dataclass(frozen=True, slots=True)
class Room:
    """A room/space candidate with its geometry and (optional) resolved name."""

    id: str
    name: str | None
    source: str
    polygon: Polygon
    area: float
    bounds: BoundingBox


@dataclass(frozen=True, slots=True)
class RoomLabel:
    """A placed room-tag text (e.g. a "Room Tag" insert) at a world point."""

    text: str
    point: Point


@dataclass(frozen=True, slots=True)
class DevicePlacement:
    """A device's world point, adapted from the device enumeration by the caller."""

    device_id: str
    point: Point


@dataclass(frozen=True, slots=True)
class DeviceRoomAssignment:
    """A device assigned to the (smallest) room whose polygon contains it."""

    device_id: str
    room_id: str


def room_from_polygon(
    room_id: str,
    polygon: Polygon,
    *,
    source: str,
    name: str | None = None,
) -> Room:
    """Build a :class:`Room`, deriving area and bounds from the polygon."""
    return Room(
        id=room_id,
        name=name,
        source=source,
        polygon=polygon,
        area=polygon_area(polygon),
        bounds=bounding_box(polygon),
    )


def assign_labels_to_rooms(
    rooms: Sequence[Room],
    labels: Sequence[RoomLabel],
) -> list[Room]:
    """Name each room from the tag text whose point falls inside it.

    A label is matched to the smallest room containing its point. The first label
    (in input order) matched to a room wins; rooms with no matching label keep
    their existing name. Returns a new list; inputs are not mutated.
    """
    names: dict[str, str] = {}
    for label in labels:
        room = _smallest_containing_room(label.point, rooms)
        if room is not None:
            names.setdefault(room.id, label.text)
    return [replace(room, name=names.get(room.id, room.name)) for room in rooms]


def assign_devices_to_rooms(
    devices: Sequence[DevicePlacement],
    rooms: Sequence[Room],
) -> list[DeviceRoomAssignment]:
    """Assign each device to the smallest room whose polygon contains its point.

    Devices that fall outside every room are omitted (unassigned). Order follows
    the input device order.
    """
    assignments: list[DeviceRoomAssignment] = []
    for device in devices:
        room = _smallest_containing_room(device.point, rooms)
        if room is not None:
            assignments.append(DeviceRoomAssignment(device_id=device.device_id, room_id=room.id))
    return assignments


def _smallest_containing_room(point: Point, rooms: Sequence[Room]) -> Room | None:
    """Return the smallest-area room whose polygon contains ``point``, or ``None``."""
    containing = [room for room in rooms if point_in_polygon(point, room.polygon)]
    if not containing:
        return None
    return min(containing, key=lambda room: room.area)
