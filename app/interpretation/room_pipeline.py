"""Room interpretation strategy selection (issue #479).

A thin pure coordinator over the two room sources:
- explicit room/space-boundary layer (#427), preferred when present, and
- wall-linework polygonization (#428) as the fallback.

``interpret_rooms`` picks the strategy (``auto`` by default) and returns a unified
result. Pure over the :class:`EntityRow` protocol and the R0 primitives, so it is
unit-testable with fixtures; the route does the DB loading and adaptation.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from app.interpretation.explicit_rooms import interpret_explicit_rooms
from app.interpretation.models import EntityRow
from app.interpretation.rooms import DevicePlacement, DeviceRoomAssignment, Room, RoomLabel
from app.interpretation.wall_rooms import interpret_wall_rooms

ROOM_STRATEGY_AUTO = "auto"
ROOM_STRATEGY_EXPLICIT = "explicit_layer"
ROOM_STRATEGY_WALLS = "wall_polygonize"

ROOM_STRATEGIES = (ROOM_STRATEGY_AUTO, ROOM_STRATEGY_EXPLICIT, ROOM_STRATEGY_WALLS)


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

    ``auto`` prefers an explicit room/space-boundary layer and falls back to wall
    polygonization only when the explicit source yields no rooms. ``explicit_layer``
    and ``wall_polygonize`` force a single source. ``snap_tolerance`` / ``min_area``
    tune the wall-polygonize robustness and are ignored by the explicit source.
    """
    if strategy in (ROOM_STRATEGY_AUTO, ROOM_STRATEGY_EXPLICIT):
        explicit = interpret_explicit_rooms(entities, devices=devices, labels=labels)
        if explicit.rooms or strategy == ROOM_STRATEGY_EXPLICIT:
            return RoomInterpretation(
                rooms=explicit.rooms,
                device_assignments=explicit.device_assignments,
                strategy=ROOM_STRATEGY_EXPLICIT,
                source_layers=explicit.boundary_layers,
            )

    walls = interpret_wall_rooms(
        entities,
        devices=devices,
        labels=labels,
        snap_tolerance=snap_tolerance,
        min_area=min_area,
    )
    return RoomInterpretation(
        rooms=walls.rooms,
        device_assignments=walls.device_assignments,
        strategy=ROOM_STRATEGY_WALLS,
        source_layers=walls.wall_layers,
    )
