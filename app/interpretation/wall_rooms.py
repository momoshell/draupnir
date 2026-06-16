"""Room polygons derived from wall linework (issue #463, #428 core).

For drawings with no explicit room layer, room faces can be recovered by
polygonizing the wall linework: collect the line/polyline segments on the wall
layer(s), node them, and take the enclosed faces (R0a ``geometry.polygonize``).
The faces become :class:`Room`s named via room-tag labels (R0b) with devices
assigned by containment. Provenance is ``room_source = "wall_polygonize"``.

Pure over the :class:`EntityRow` protocol and the R0 primitives — unit-testable
with fixtures. This is the happy-path core; gap-closing / sliver / layer-selection
robustness is #464 (#428b).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from app.interpretation.geometry import Point, polygonize
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

ROOM_SOURCE_WALL_POLYGONIZE = "wall_polygonize"

# Substrings (case-insensitive) that mark a layer as wall linework by convention.
WALL_LAYER_KEYWORDS = ("wall",)

_Segment = tuple[Point, Point]


@dataclass(frozen=True, slots=True)
class WallRoomInterpretation:
    """Result of deriving rooms from wall linework."""

    rooms: list[Room]
    device_assignments: list[DeviceRoomAssignment]
    wall_layers: tuple[str, ...]


def is_wall_layer(layer_ref: str | None) -> bool:
    """Return whether a layer name looks like wall linework by convention."""
    if not layer_ref:
        return False
    name = layer_ref.lower()
    return any(keyword in name for keyword in WALL_LAYER_KEYWORDS)


def detect_wall_layers(entities: Sequence[EntityRow]) -> set[str]:
    """Return the distinct layer names that look like wall linework."""
    return {
        entity.layer_ref
        for entity in entities
        if entity.layer_ref is not None and is_wall_layer(entity.layer_ref)
    }


def wall_segments(
    entities: Sequence[EntityRow],
    *,
    layer_refs: set[str] | None = None,
) -> list[_Segment]:
    """Collect line/polyline segments from the wall-layer entities."""
    segments: list[_Segment] = []
    for entity in entities:
        if layer_refs is not None and entity.layer_ref not in layer_refs:
            continue
        segments.extend(_segments_from_entity(entity))
    return segments


def build_rooms_from_walls(
    entities: Sequence[EntityRow],
    *,
    layer_refs: set[str] | None = None,
    source: str = ROOM_SOURCE_WALL_POLYGONIZE,
) -> list[Room]:
    """Polygonize wall linework into deterministic room faces.

    Faces are ordered by (min_x, min_y, area) so the synthesized room ids are
    stable regardless of shapely's polygonization order.
    """
    faces = polygonize(wall_segments(entities, layer_refs=layer_refs))
    ordered = sorted(faces, key=lambda polygon: (*polygon.bounds, polygon.area))
    return [
        room_from_polygon(f"wall-room-{index}", polygon, source=source)
        for index, polygon in enumerate(ordered)
    ]


def interpret_wall_rooms(
    entities: Sequence[EntityRow],
    *,
    devices: Sequence[DevicePlacement],
    labels: Sequence[RoomLabel],
    layer_refs: set[str] | None = None,
) -> WallRoomInterpretation:
    """Detect wall layers, polygonize rooms, name them, and assign devices.

    ``layer_refs`` overrides auto-detection when the drawing uses non-standard
    wall-layer names.
    """
    wall_layers = layer_refs if layer_refs is not None else detect_wall_layers(entities)
    rooms = build_rooms_from_walls(entities, layer_refs=wall_layers)
    rooms = assign_labels_to_rooms(rooms, labels)
    assignments = assign_devices_to_rooms(devices, rooms)
    return WallRoomInterpretation(
        rooms=rooms,
        device_assignments=assignments,
        wall_layers=tuple(sorted(wall_layers)),
    )


def _segments_from_entity(entity: EntityRow) -> list[_Segment]:
    """Extract straight segments from a line or (poly)line entity."""
    geometry = entity.geometry_json
    if not isinstance(geometry, Mapping):
        return []

    start = _vertex_xy(geometry.get("start"))
    end = _vertex_xy(geometry.get("end"))
    if start is not None and end is not None:
        return [(start, end)] if start != end else []

    raw_vertices = geometry.get("vertices")
    if not isinstance(raw_vertices, (list, tuple)) or len(raw_vertices) < 2:
        return []
    points = [vertex for vertex in (_vertex_xy(raw) for raw in raw_vertices) if vertex is not None]
    if len(points) < 2:
        return []

    if geometry.get("closed") is True and points[0] != points[-1]:
        points = [*points, points[0]]
    return [
        (points[index], points[index + 1])
        for index in range(len(points) - 1)
        if points[index] != points[index + 1]
    ]


def _vertex_xy(value: Any) -> Point | None:
    """Coerce a vertex (``{"x","y"}`` mapping or ``[x, y]`` sequence) to ``(x, y)``."""
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
