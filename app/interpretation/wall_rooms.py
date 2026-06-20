"""Room polygons derived from wall linework (issue #463, #428 core).

For drawings with no explicit room layer, room faces can be recovered by
polygonizing the wall linework: collect the line/polyline segments on the wall
layer(s), node them, and take the enclosed faces (R0a ``geometry.polygonize``).
The faces become :class:`Room`s named via room-tag labels (R0b) with devices
assigned by containment. Provenance is ``room_source = "wall_polygonize"``.

Pure over the :class:`EntityRow` protocol and the R0 primitives — unit-testable
with fixtures. Real drawings rarely polygonize cleanly, so :func:`build_rooms_from_walls`
offers gap-closing (endpoint snapping across door openings) and sliver dropping
(area threshold), and tags each derived room with a confidence caveat (#464).
"""

from __future__ import annotations

import math
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

# Confidence caveats for the heuristic wall-derived rooms: a clean polygonization
# is more trustworthy than one that needed gap-closing to bridge door openings.
WALL_ROOM_CONFIDENCE = 0.8
GAP_CLOSED_ROOM_CONFIDENCE = 0.6

# Robust gap-close defaults for real drawings, where wall runs are many short segments
# with small endpoint near-misses + door openings. Validated on 670003/680003 (#554).
DEFAULT_WALL_SNAP_TOLERANCE = 0.1
DEFAULT_WALL_MIN_AREA = 1.0

# A candidate layer is selected as wall linework when its polygonized enclosed area is at
# least this fraction of the best-scoring layer's — so the architecture layer that actually
# forms rooms is picked over annotation/device/legend layers that form little or none.
_WALL_YIELD_RATIO = 0.2

# How wall layers were chosen, for provenance.
SELECTION_LAYER_NAME = "layer_name"
SELECTION_POLYGONIZE_YIELD = "polygonize_yield"
SELECTION_NONE = "none"

_Segment = tuple[Point, Point]


@dataclass(frozen=True, slots=True)
class WallRoomInterpretation:
    """Result of deriving rooms from wall linework."""

    rooms: list[Room]
    device_assignments: list[DeviceRoomAssignment]
    wall_layers: tuple[str, ...]
    selection_method: str = SELECTION_LAYER_NAME


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


def _layer_polygon_yield(
    entities: Sequence[EntityRow], layer: str, *, snap_tolerance: float, min_area: float
) -> float:
    """Total enclosed area a single layer's linework polygonizes into (with gap-closing)."""
    segments, _ = _snap_segments(wall_segments(entities, layer_refs={layer}), snap_tolerance)
    return float(sum(face.area for face in polygonize(segments) if face.area >= min_area))


def select_wall_layers(
    entities: Sequence[EntityRow],
    *,
    snap_tolerance: float = DEFAULT_WALL_SNAP_TOLERANCE,
    min_area: float = DEFAULT_WALL_MIN_AREA,
) -> tuple[set[str], str]:
    """Choose the wall/architecture layers to polygonize, with the method used.

    Named ``*wall*`` layers win when present (explicit convention). Otherwise — the common
    real-world case, where the architecture layer is named by a CAD standard (``A210``,
    ``Z000``, …) — layers are scored by how much enclosed area their linework polygonizes
    into, and those reaching ``_WALL_YIELD_RATIO`` of the best score are selected. Annotation,
    device-leader, and legend layers self-exclude because they don't form closed faces.
    """
    named = detect_wall_layers(entities)
    if named:
        return named, SELECTION_LAYER_NAME

    layers = {entity.layer_ref for entity in entities if entity.layer_ref is not None}
    scores = {
        layer: _layer_polygon_yield(
            entities, layer, snap_tolerance=snap_tolerance, min_area=min_area
        )
        for layer in layers
    }
    best = max(scores.values(), default=0.0)
    if best <= 0.0:
        return set(), SELECTION_NONE
    threshold = best * _WALL_YIELD_RATIO
    selected = {layer for layer, score in scores.items() if score >= threshold}
    return selected, SELECTION_POLYGONIZE_YIELD


def wall_segments(
    entities: Sequence[EntityRow],
    *,
    layer_refs: set[str] | None = None,
    exclude_dashed: bool = True,
) -> list[_Segment]:
    """Collect line/polyline segments from the wall-layer entities.

    ``exclude_dashed`` drops segments whose entity carries a dashed linetype (#573/#574):
    structural grids, match/property/hidden lines are typically dashed and are not walls, so
    excluding them keeps the solid architecture (e.g. a dashed grid layer that polygonizes
    into building-spanning faces no longer competes with the real wall layer). Entities with
    no style metadata are kept (we never drop geometry on absent information).
    """
    segments: list[_Segment] = []
    for entity in entities:
        if layer_refs is not None and entity.layer_ref not in layer_refs:
            continue
        if exclude_dashed and _entity_is_dashed(entity):
            continue
        segments.extend(_segments_from_entity(entity))
    return segments


def _entity_is_dashed(entity: EntityRow) -> bool:
    """Whether an entity's resolved linetype is dashed; False when style is absent/unknown.

    Read opportunistically (``style`` is an optional enrichment on RevisionEntity, not part of
    the EntityRow protocol), so interpretation degrades gracefully without linetype data.
    """
    style = getattr(entity, "style", None)
    if not isinstance(style, Mapping):
        return False
    linetype = style.get("linetype")
    return bool(linetype.get("dashed")) if isinstance(linetype, Mapping) else False


def build_rooms_from_walls(
    entities: Sequence[EntityRow],
    *,
    layer_refs: set[str] | None = None,
    source: str = ROOM_SOURCE_WALL_POLYGONIZE,
    snap_tolerance: float = 0.0,
    min_area: float = 0.0,
) -> list[Room]:
    """Polygonize wall linework into deterministic room faces.

    Robustness for real drawings:
    - ``snap_tolerance`` merges segment endpoints within that distance, bridging
      door-opening gaps and floating-point near-misses so loops still close.
    - ``min_area`` drops sliver faces below the threshold (wall-thickness gaps,
      hatch artefacts).

    Each room carries a confidence caveat: ``WALL_ROOM_CONFIDENCE`` for a clean
    polygonization, dropping to ``GAP_CLOSED_ROOM_CONFIDENCE`` when snapping had to
    bridge a gap. Faces are ordered by (min_x, min_y, area) so the synthesized room
    ids are stable regardless of shapely's polygonization order.
    """
    segments, gap_closed = _snap_segments(
        wall_segments(entities, layer_refs=layer_refs), snap_tolerance
    )
    faces = [face for face in polygonize(segments) if face.area >= min_area]
    ordered = sorted(faces, key=lambda polygon: (*polygon.bounds, polygon.area))
    confidence = GAP_CLOSED_ROOM_CONFIDENCE if gap_closed else WALL_ROOM_CONFIDENCE
    return [
        room_from_polygon(f"wall-room-{index}", polygon, source=source, confidence=confidence)
        for index, polygon in enumerate(ordered)
    ]


def interpret_wall_rooms(
    entities: Sequence[EntityRow],
    *,
    devices: Sequence[DevicePlacement],
    labels: Sequence[RoomLabel],
    layer_refs: set[str] | None = None,
    snap_tolerance: float = 0.0,
    min_area: float = 0.0,
) -> WallRoomInterpretation:
    """Detect wall layers, polygonize rooms, name them, and assign devices.

    ``layer_refs`` overrides auto-detection when the drawing uses non-standard
    wall-layer names; ``snap_tolerance`` / ``min_area`` tune the gap-closing and
    sliver-dropping robustness (see :func:`build_rooms_from_walls`).
    """
    if layer_refs is not None:
        wall_layers, selection_method = layer_refs, SELECTION_LAYER_NAME
    else:
        wall_layers, selection_method = select_wall_layers(
            entities, snap_tolerance=snap_tolerance, min_area=min_area
        )
    rooms = build_rooms_from_walls(
        entities,
        layer_refs=wall_layers,
        snap_tolerance=snap_tolerance,
        min_area=min_area,
    )
    rooms = assign_labels_to_rooms(rooms, labels)
    assignments = assign_devices_to_rooms(devices, rooms)
    return WallRoomInterpretation(
        rooms=rooms,
        device_assignments=assignments,
        wall_layers=tuple(sorted(wall_layers)),
        selection_method=selection_method,
    )


def _snap_segments(
    segments: Sequence[_Segment],
    tolerance: float,
) -> tuple[list[_Segment], bool]:
    """Merge segment endpoints within ``tolerance`` to close small gaps.

    Each endpoint is snapped to the first-seen representative within ``tolerance``
    (deterministic for a given input order). Returns the rewritten segments (with
    any now-degenerate ones dropped) and whether any endpoint was actually moved.
    """
    if tolerance <= 0:
        return list(segments), False

    representatives: list[Point] = []
    gap_closed = False

    def _representative(point: Point) -> Point:
        nonlocal gap_closed
        for existing in representatives:
            if math.hypot(point[0] - existing[0], point[1] - existing[1]) <= tolerance:
                if existing != point:
                    gap_closed = True
                return existing
        representatives.append(point)
        return point

    snapped: list[_Segment] = []
    for start, end in segments:
        new_start = _representative(start)
        new_end = _representative(end)
        if new_start != new_end:
            snapped.append((new_start, new_end))
    return snapped, gap_closed


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
