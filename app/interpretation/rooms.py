"""Pure room model and label/device assignment (issue #461, Phase 2).

Builds on the geometry primitives (R0a, ``app.interpretation.geometry``). All
assignment is boundary-aware (point-in-polygon over the room geometry) and never
nearest-label: a label or device belongs to the *smallest* room whose polygon
contains its point, so nested spaces resolve to the innermost room. No DB or
FastAPI concerns, so every function is unit-testable with fakes.
"""

from __future__ import annotations

import math
import re
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

# A room-number token: dot-separated alphanumeric segments (≥2 segments so plain words
# aren't matched), required to contain a digit (filters non-numeric dotted text like "U.S").
# Matches ``0.9.01``, ``G.04``, ``A1.02``.
_ROOM_NUMBER_RE = re.compile(r"\b[A-Za-z0-9]+(?:\.[A-Za-z0-9]+)+\b")

# A "plausible" room number (#828 PR-3): the stricter, PDF-only room-number form used to
# tell a genuine building-numbering token (e.g. "0.2.17", "0.0.09A", "0.2.C1", "0.8.02") apart
# from PDF interior-annotation noise that ``parse_room_number`` (deliberately loose, relied on
# by other consumers/buildings) accepts — e.g. "20.4" (only 2 dotted parts), "NO.14784" /
# "2NO.45" / "17.AIR" (not digit-led, or a unit/word suffix rather than a room segment),
# "1.5T" (2 parts), "1.621aaa" (3rd part isn't a valid short alphanumeric segment). Requires:
#   - at least 3 dot-separated segments (building.floor.room, the observed ground-truth shape);
#   - the first segment is purely digits (a floor/block index, never a word like "NO");
#   - every segment is short (<=3 chars) so a long numeric run ("14784") or word ("AIR") can't
#     pass as a segment.
_PLAUSIBLE_ROOM_NUMBER_RE = re.compile(r"\b\d{1,3}(?:\.[A-Za-z0-9]{1,3}){2,}\b")


@dataclass(frozen=True, slots=True)
class Room:
    """A room/space candidate with (optional) geometry, name, and number.

    Geometry is optional: a label-derived room (#549) has a ``location`` (the label
    point) but no ``polygon`` / ``area`` / ``bounds``. ``confidence`` is an optional
    provenance caveat in ``[0, 1]`` for heuristic sources (e.g. wall-derived rooms);
    ``None`` means "not scored" (e.g. an explicit room-boundary polygon).
    """

    id: str
    name: str | None
    source: str
    polygon: Polygon | None
    area: float | None
    bounds: BoundingBox | None
    confidence: float | None = None
    number: str | None = None
    location: Point | None = None
    # All label anchor points for a (possibly merged) label-only room (#581). A room labelled
    # several times under one number — e.g. the 3 arms of a U-shaped space with no internal
    # walls — is one room; its anchors retain every placement so device proximity covers the
    # whole room. ``location`` stays the representative point. Empty for polygon rooms.
    anchors: tuple[Point, ...] = ()
    # Wall-aware duplicate-number flag (#581): the same number was found in regions separated
    # by walls (>=2 distinct polygon rooms) — a likely numbering error surfaced, not merged.
    needs_review: bool = False


@dataclass(frozen=True, slots=True)
class RoomLabel:
    """A placed room-tag text (e.g. a "Room Tag" insert) at a world point.

    ``layer`` (when known) lets label-room identification scope to the room-label layer
    and avoid pairing room numbers with device-tag text on other layers (#549).
    """

    text: str
    point: Point
    layer: str | None = None


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
    confidence: float | None = None,
    number: str | None = None,
) -> Room:
    """Build a :class:`Room`, deriving area and bounds from the polygon."""
    return Room(
        id=room_id,
        name=name,
        source=source,
        polygon=polygon,
        area=polygon_area(polygon),
        bounds=bounding_box(polygon),
        confidence=confidence,
        number=number,
    )


def room_from_label(
    room_id: str,
    *,
    source: str,
    location: Point,
    name: str | None = None,
    number: str | None = None,
    confidence: float | None = None,
    anchors: tuple[Point, ...] | None = None,
    needs_review: bool = False,
) -> Room:
    """Build a label-derived room: a name/number at a location, with no boundary (#549).

    ``anchors`` defaults to ``(location,)`` so device proximity has at least the label point;
    a merged room (#581) passes every placement's point.
    """
    return Room(
        id=room_id,
        name=name,
        source=source,
        polygon=None,
        area=None,
        bounds=None,
        confidence=confidence,
        number=number,
        location=location,
        anchors=anchors if anchors is not None else (location,),
        needs_review=needs_review,
    )


def parse_room_number(text: str) -> str | None:
    """Extract a room-number token (e.g. ``0.9.01``, ``G.04``) from a label, or ``None``.

    Returns the first dotted alphanumeric token that contains a digit, so non-numeric
    dotted text (e.g. ``U.S``) is not mistaken for a room number.
    """
    for match in _ROOM_NUMBER_RE.finditer(text):
        token = match.group(0)
        if any(char.isdigit() for char in token):
            return token
    return None


def is_plausible_room_number(text: str) -> bool:
    """Return True when *text* is a plausible building room-number token (#828 PR-3).

    Stricter than :func:`parse_room_number` — additive, PDF-only use. Requires a digit-led
    token of >=3 short (<=3 char) dot-separated segments, e.g. ``0.2.17``, ``0.0.09A``,
    ``0.2.C1``, ``0.8.02`` (the observed building.floor.room ground-truth shape).

    Rejects PDF interior-annotation noise that ``parse_room_number`` wrongly accepts:
    ``20.4`` (only 2 segments), ``NO.14784`` / ``2NO.45`` (not digit-led), ``17.AIR`` (a
    word suffix, not a room segment), ``1.5T`` (2 segments), ``1.621aaa`` (no valid 3rd
    segment — too long and no separating dot).

    Does NOT change :func:`parse_room_number`, which stays intentionally loose for other
    consumers/buildings that rely on its looser 2-segment match.
    """
    return _PLAUSIBLE_ROOM_NUMBER_RE.search(text) is not None


def assign_labels_to_rooms(
    rooms: Sequence[Room],
    labels: Sequence[RoomLabel],
) -> list[Room]:
    """Name each *unnamed* room from the tag text whose point falls inside it.

    A label is matched to the smallest room containing its point. The first label
    (in input order) matched to a room wins. A room that already has a name (e.g.
    an authoritative IfcSpace name) is never overridden; rooms with no matching
    label keep their existing name. Returns a new list; inputs are not mutated.
    """
    names: dict[str, str] = {}
    for label in labels:
        room = _smallest_containing_room(label.point, rooms)
        if room is not None:
            names.setdefault(room.id, label.text)
    return [
        replace(room, name=room.name if room.name is not None else names.get(room.id))
        for room in rooms
    ]


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


def assign_devices_to_label_rooms(
    devices: Sequence[DevicePlacement],
    rooms: Sequence[Room],
    *,
    already_assigned: set[str],
    radius: float,
) -> list[DeviceRoomAssignment]:
    """Assign yet-unassigned devices to the nearest label-only room within ``radius`` (#555).

    Label-derived rooms have a location but no polygon, so containment can't apply. This is a
    deliberate heuristic for the no-polygon case: a device with no containing polygon room is
    attached to the nearest labeled room point within ``radius``. Polygon containment (when
    available) always takes precedence — devices already assigned are skipped.
    """
    label_rooms = [room for room in rooms if room.polygon is None and room.location is not None]
    if not label_rooms:
        return []
    assignments: list[DeviceRoomAssignment] = []
    for device in devices:
        if device.device_id in already_assigned:
            continue
        # Distance to a room is the distance to its NEAREST anchor (#581): a merged U-shaped
        # room covers several placements, so a device near any arm belongs to it.
        nearest = min(label_rooms, key=lambda room: _nearest_anchor_distance(device.point, room))
        if _nearest_anchor_distance(device.point, nearest) <= radius:
            assignments.append(DeviceRoomAssignment(device_id=device.device_id, room_id=nearest.id))
    return assignments


def merge_label_room_group(group: Sequence[Room]) -> Room:
    """Merge same-number label-only rooms into one, unioning their anchor points (#581).

    Name is the first non-null in the group; ``location`` becomes the first anchor; ``anchors``
    is the de-duplicated union of every member's anchors (so device proximity covers all
    placements). Keeps the first room's id (callers may re-id). ``needs_review`` is preserved
    if any member carried it.
    """
    first = group[0]
    name = next((room.name for room in group if room.name is not None), None)
    anchors: list[Point] = []
    for room in group:
        for anchor in _room_anchors(room):
            if anchor not in anchors:
                anchors.append(anchor)
    return room_from_label(
        first.id,
        source=first.source,
        location=anchors[0] if anchors else (first.location or (0.0, 0.0)),
        name=name,
        number=first.number,
        confidence=first.confidence,
        anchors=tuple(anchors),
        needs_review=any(room.needs_review for room in group),
    )


def dedupe_label_rooms_by_number(rooms: Sequence[Room]) -> list[Room]:
    """Merge label-only rooms sharing a room number into one room each (#581).

    Same-number labels are one room placed several times (e.g. the arms of a U-shaped space
    with no internal walls); the merge retains every placement as an anchor. Rooms without a
    number pass through unchanged. Output preserves first-appearance order and is deterministic.
    """
    groups: dict[str, list[Room]] = {}
    slot_of_number: dict[str, int] = {}
    result: list[Room] = []
    for room in rooms:
        if room.number is None:
            result.append(room)
            continue
        if room.number not in groups:
            groups[room.number] = [room]
            slot_of_number[room.number] = len(result)
            result.append(room)  # tentative; replaced below if the group grows
        else:
            groups[room.number].append(room)
    for number, group in groups.items():
        if len(group) > 1:
            result[slot_of_number[number]] = merge_label_room_group(group)
    return result


def _room_anchors(room: Room) -> tuple[Point, ...]:
    """A label room's anchor points: its explicit anchors, else its single location."""
    if room.anchors:
        return room.anchors
    return (room.location,) if room.location is not None else ()


def _nearest_anchor_distance(point: Point, room: Room) -> float:
    """Distance from ``point`` to the closest of ``room``'s anchor points (inf if none)."""
    return min((_distance(point, anchor) for anchor in _room_anchors(room)), default=float("inf"))


def _distance(point: Point, other: Point | None) -> float:
    if other is None:
        return float("inf")
    return math.hypot(point[0] - other[0], point[1] - other[1])


def _smallest_containing_room(point: Point, rooms: Sequence[Room]) -> Room | None:
    """Return the smallest-area room whose polygon contains ``point``, or ``None``.

    Label-derived rooms (no polygon) can't contain points and are skipped.
    """
    containing = [
        room
        for room in rooms
        if room.polygon is not None
        and room.area is not None
        and point_in_polygon(point, room.polygon)
    ]
    if not containing:
        return None
    return min(containing, key=lambda room: room.area if room.area is not None else 0.0)
