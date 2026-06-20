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
from dataclasses import dataclass

from app.interpretation.explicit_rooms import interpret_explicit_rooms
from app.interpretation.ifc_rooms import interpret_ifc_rooms
from app.interpretation.models import EntityRow
from app.interpretation.rooms import DevicePlacement, DeviceRoomAssignment, Room, RoomLabel
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
    """Interpret rooms using the requested strategy.

    ``auto`` tries IFC spaces first, then an explicit room/space-boundary layer,
    then wall polygonization, using the first source that yields rooms.
    ``ifc_space`` / ``explicit_layer`` / ``wall_polygonize`` force a single source.
    ``snap_tolerance`` / ``min_area`` tune the wall-polygonize robustness and are
    ignored by the other sources.
    """
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
