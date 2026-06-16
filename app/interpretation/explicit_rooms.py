"""Room containment from an explicit room/space-boundary layer (issue #427).

When a drawing carries an explicit room layer (Revit room-separation lines or
closed room polylines), it is the cleanest containment source. This module is the
pure pipeline: detect candidate boundary layers, build room polygons from the
closed polylines on them, name each room from the tag text inside it, and assign
each device by point-in-polygon (innermost room for nested spaces). Provenance is
recorded as ``room_source = "explicit_layer"``.

Pure over the :class:`EntityRow` protocol and the R0 room primitives, so it is
unit-testable with fixtures. DB loading and the HTTP route wire this up separately.

Out of scope (other Tracking-F tasks): deriving rooms from wall linework (#428)
and from IFC spaces (#429).
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

ROOM_SOURCE_EXPLICIT_LAYER = "explicit_layer"

# Substrings (case-insensitive) that mark a layer as a room/space boundary by
# convention. Kept deliberately narrow to avoid false positives; callers can pass
# explicit ``layer_refs`` when a drawing uses a non-standard name.
ROOM_BOUNDARY_LAYER_KEYWORDS = ("room", "space", "rmsep")


@dataclass(frozen=True, slots=True)
class ExplicitRoomInterpretation:
    """Result of interpreting rooms from an explicit boundary layer."""

    rooms: list[Room]
    device_assignments: list[DeviceRoomAssignment]
    boundary_layers: tuple[str, ...]


def is_room_boundary_layer(layer_ref: str | None) -> bool:
    """Return whether a layer name looks like a room/space boundary by convention."""
    if not layer_ref:
        return False
    name = layer_ref.lower()
    return any(keyword in name for keyword in ROOM_BOUNDARY_LAYER_KEYWORDS)


def detect_room_boundary_layers(entities: Sequence[EntityRow]) -> set[str]:
    """Return the distinct layer names that look like room/space boundaries."""
    return {
        entity.layer_ref
        for entity in entities
        if entity.layer_ref is not None and is_room_boundary_layer(entity.layer_ref)
    }


def build_rooms_from_entities(
    entities: Sequence[EntityRow],
    *,
    layer_refs: set[str] | None = None,
    source: str = ROOM_SOURCE_EXPLICIT_LAYER,
) -> list[Room]:
    """Build room polygons from the closed polylines on the boundary layer(s).

    When ``layer_refs`` is given, only entities on those layers are considered;
    otherwise every entity is. Entities that are not closed polylines, or whose
    rings are degenerate, are skipped.
    """
    rooms: list[Room] = []
    for entity in entities:
        if layer_refs is not None and entity.layer_ref not in layer_refs:
            continue
        ring = _room_ring_from_entity(entity)
        if ring is None:
            continue
        polygon = build_polygon(ring)
        if polygon is None:
            continue
        rooms.append(room_from_polygon(entity.entity_id, polygon, source=source))
    return rooms


def interpret_explicit_rooms(
    entities: Sequence[EntityRow],
    *,
    devices: Sequence[DevicePlacement],
    labels: Sequence[RoomLabel],
    layer_refs: set[str] | None = None,
) -> ExplicitRoomInterpretation:
    """Detect boundary layers, build named rooms, and assign devices by containment.

    ``layer_refs`` overrides auto-detection when the drawing uses non-standard
    layer names. Returns the named rooms, the device→room assignments, and the
    boundary layers used.
    """
    boundary_layers = (
        layer_refs if layer_refs is not None else detect_room_boundary_layers(entities)
    )
    rooms = build_rooms_from_entities(entities, layer_refs=boundary_layers)
    rooms = assign_labels_to_rooms(rooms, labels)
    assignments = assign_devices_to_rooms(devices, rooms)
    return ExplicitRoomInterpretation(
        rooms=rooms,
        device_assignments=assignments,
        boundary_layers=tuple(sorted(boundary_layers)),
    )


def _room_ring_from_entity(entity: EntityRow) -> list[Point] | None:
    """Extract a closed-polyline ring (x, y vertices) from an entity, or ``None``.

    An entity explicitly marked ``closed = False`` is treated as open linework
    (not a room boundary) and skipped.
    """
    geometry = entity.geometry_json
    if not isinstance(geometry, Mapping):
        return None
    if geometry.get("closed") is False:
        return None
    raw_vertices = geometry.get("vertices")
    if not isinstance(raw_vertices, (list, tuple)) or not raw_vertices:
        return None

    ring: list[Point] = []
    for raw_vertex in raw_vertices:
        vertex = _vertex_xy(raw_vertex)
        if vertex is None:
            return None
        ring.append(vertex)
    return ring


def _vertex_xy(value: Any) -> Point | None:
    """Coerce one vertex (``{"x","y"}`` mapping or ``[x, y]`` sequence) to ``(x, y)``."""
    if isinstance(value, Mapping):
        x, y = value.get("x"), value.get("y")
    elif isinstance(value, (list, tuple)) and len(value) >= 2:
        x, y = value[0], value[1]
    else:
        return None
    if (
        isinstance(x, (int, float))
        and not isinstance(x, bool)
        and isinstance(y, (int, float))
        and not isinstance(y, bool)
    ):
        return (float(x), float(y))
    return None
