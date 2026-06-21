"""Room interpretation strategy selection (issue #479).

A thin pure coordinator over the three room sources, in descending authority:
- IFC spaces (#429), the definitive native source when present,
- explicit room/space-boundary layer (#427), and
- wall-linework polygonization (#428) as the fallback.

``interpret_rooms`` picks the strategy (``auto`` by default) and returns a unified
result. Pure over the :class:`EntityRow` protocol and the R0 primitives, so it is
unit-testable with fixtures; the route does the DB loading and adaptation.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace

from app.interpretation.explicit_rooms import interpret_explicit_rooms
from app.interpretation.geometry import point_in_polygon
from app.interpretation.ifc_rooms import interpret_ifc_rooms
from app.interpretation.label_rooms import identify_rooms_from_labels, room_label_layers
from app.interpretation.models import EntityRow
from app.interpretation.rooms import (
    DevicePlacement,
    DeviceRoomAssignment,
    Room,
    RoomLabel,
    assign_devices_to_label_rooms,
)
from app.interpretation.wall_rooms import (
    DEFAULT_WALL_MIN_AREA,
    DEFAULT_WALL_SNAP_TOLERANCE,
    interpret_wall_rooms,
)

ROOM_STRATEGY_AUTO = "auto"
ROOM_STRATEGY_IFC = "ifc_space"
ROOM_STRATEGY_EXPLICIT = "explicit_layer"
ROOM_STRATEGY_WALLS = "wall_polygonize"

ROOM_STRATEGIES = (
    ROOM_STRATEGY_AUTO,
    ROOM_STRATEGY_IFC,
    ROOM_STRATEGY_EXPLICIT,
    ROOM_STRATEGY_WALLS,
)

# Heuristic max distance from a device to a label-only room's point for nearest-room
# assignment (#555). Generous, since a polygon-less room has no boundary to contain points.
DEFAULT_DEVICE_LABEL_ROOM_RADIUS = 8.0


@dataclass(frozen=True, slots=True)
class RoomInterpretation:
    """Unified room interpretation result with the strategy that produced it."""

    rooms: list[Room]
    device_assignments: list[DeviceRoomAssignment]
    strategy: str
    source_layers: tuple[str, ...]


def interpret_rooms(
    entities: Sequence[EntityRow],
    *,
    devices: Sequence[DevicePlacement],
    labels: Sequence[RoomLabel],
    strategy: str = ROOM_STRATEGY_AUTO,
    snap_tolerance: float = 0.0,
    min_area: float = 0.0,
) -> RoomInterpretation:
    """Interpret rooms by geometry, then enrich + supplement with label identities (#549).

    The geometric strategy (``auto``/``ifc_space``/``explicit_layer``/``wall_polygonize``)
    produces polygon rooms; label clusters (name + room-number, e.g. ``PH Plantroom`` +
    ``0.9.01``) then (a) stamp name/number onto the polygon room that contains them and
    (b) surface as label-only rooms when no polygon contains them — so a fully-labeled but
    un-polygonizable drawing still yields named, numbered rooms.
    """
    # Scope to the room-label layer(s) (those bearing a room number) when present, so polygon
    # naming and label-room identification both ignore device-tag text on other layers (#549).
    allowed = room_label_layers(labels)
    room_labels = (
        labels if allowed is None else [label for label in labels if label.layer in allowed]
    )
    base = _interpret_geometric(
        entities,
        devices=devices,
        labels=room_labels,
        strategy=strategy,
        snap_tolerance=snap_tolerance,
        min_area=min_area,
    )
    enriched = _enrich_with_labels(base, room_labels)
    # Devices land in their containing polygon room (done by the strategy); any left over are
    # attached to the nearest label-only room — the no-polygon case (#555).
    extra = assign_devices_to_label_rooms(
        devices,
        enriched.rooms,
        already_assigned={a.device_id for a in enriched.device_assignments},
        radius=DEFAULT_DEVICE_LABEL_ROOM_RADIUS,
    )
    if not extra:
        return enriched
    return replace(enriched, device_assignments=[*enriched.device_assignments, *extra])


def _enrich_with_labels(
    base: RoomInterpretation, labels: Sequence[RoomLabel]
) -> RoomInterpretation:
    """Stamp label name/number onto containing polygon rooms; append unmatched as label rooms."""
    identities = identify_rooms_from_labels(labels)
    if not identities:
        return base

    rooms = list(base.rooms)
    unmatched: list[Room] = []
    for identity in identities:
        location = identity.location
        target = _containing_polygon_room(location, rooms) if location is not None else None
        if target is None:
            unmatched.append(identity)
            continue
        index = rooms.index(target)
        rooms[index] = replace(
            target,
            name=target.name if target.name is not None else identity.name,
            number=target.number if target.number is not None else identity.number,
        )

    if not unmatched:
        return replace(base, rooms=rooms)
    # Re-id the appended label rooms so ids stay unique + stable within the merged list.
    appended = [replace(room, id=f"label-room-{index}") for index, room in enumerate(unmatched)]
    return replace(base, rooms=[*rooms, *appended])


def _containing_polygon_room(point: tuple[float, float], rooms: Sequence[Room]) -> Room | None:
    """Smallest polygon room containing ``point`` (label-only rooms have no polygon)."""
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


def _interpret_geometric(
    entities: Sequence[EntityRow],
    *,
    devices: Sequence[DevicePlacement],
    labels: Sequence[RoomLabel],
    strategy: str = ROOM_STRATEGY_AUTO,
    snap_tolerance: float = 0.0,
    min_area: float = 0.0,
) -> RoomInterpretation:
    """Geometric room sources only (IFC / explicit / wall), in descending authority."""
    if strategy in (ROOM_STRATEGY_AUTO, ROOM_STRATEGY_IFC):
        ifc = interpret_ifc_rooms(entities, devices=devices, labels=labels)
        if ifc.rooms or strategy == ROOM_STRATEGY_IFC:
            return RoomInterpretation(
                rooms=ifc.rooms,
                device_assignments=ifc.device_assignments,
                strategy=ROOM_STRATEGY_IFC,
                source_layers=(),
            )

    if strategy in (ROOM_STRATEGY_AUTO, ROOM_STRATEGY_EXPLICIT):
        explicit = interpret_explicit_rooms(entities, devices=devices, labels=labels)
        if explicit.rooms or strategy == ROOM_STRATEGY_EXPLICIT:
            return RoomInterpretation(
                rooms=explicit.rooms,
                device_assignments=explicit.device_assignments,
                strategy=ROOM_STRATEGY_EXPLICIT,
                source_layers=explicit.boundary_layers,
            )

    # Real drawings need gap-closing + sliver-dropping to polygonize cleanly; when the caller
    # didn't specify, fall back to the validated robust defaults (#554) rather than 0 (which
    # yields no rooms on Revit-style exports). Explicit non-zero caller values still win.
    walls = interpret_wall_rooms(
        entities,
        devices=devices,
        labels=labels,
        snap_tolerance=snap_tolerance or DEFAULT_WALL_SNAP_TOLERANCE,
        min_area=min_area or DEFAULT_WALL_MIN_AREA,
    )
    return RoomInterpretation(
        rooms=walls.rooms,
        device_assignments=walls.device_assignments,
        strategy=ROOM_STRATEGY_WALLS,
        source_layers=walls.wall_layers,
    )
