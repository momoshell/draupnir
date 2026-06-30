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

from app.interpretation.geometry import Point, build_polygon, point_in_polygon
from app.interpretation.models import EntityRow
from app.interpretation.rooms import (
    DevicePlacement,
    DeviceRoomAssignment,
    Room,
    RoomLabel,
    assign_devices_to_rooms,
    assign_labels_to_rooms,
    parse_room_number,
    room_from_polygon,
)

ROOM_SOURCE_EXPLICIT_LAYER = "explicit_layer"

# Substrings (case-insensitive) that mark a layer as a room/space boundary by
# convention. Kept deliberately narrow to avoid false positives; callers can pass
# explicit ``layer_refs`` when a drawing uses a non-standard name.
ROOM_BOUNDARY_LAYER_KEYWORDS = ("room", "space", "rmsep")

# Conservative area floor for the label-anchored no-boundary-layer path (#792).
# Geometry is stored in metres, so a real room is >> 1 m²; pipe/symbol loops are
# typically < 0.1 m². 0.5 m² drops tiny fixture outlines while keeping small
# bathrooms/closets. The primary gate (numbered-label containment) does the heavy
# lifting; this is a belt-and-suspenders backstop.
DEFAULT_LABEL_ANCHOR_MIN_AREA = 0.5


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


def build_rooms_anchored_by_labels(
    entities: Sequence[EntityRow],
    labels: Sequence[RoomLabel],
    *,
    min_area: float = DEFAULT_LABEL_ANCHOR_MIN_AREA,
    source: str = ROOM_SOURCE_EXPLICIT_LAYER,
) -> list[Room]:
    """Build rooms from closed polylines anchored by a numbered RoomLabel (#792).

    Used in the no-dedicated-boundary-layer fallback (PDF single-layer case): every
    closed polyline is a candidate, but only those that (a) pass the ``min_area``
    threshold AND (b) contain at least one ``RoomLabel`` bearing a room NUMBER (a
    dot-separated token such as ``0.2.xx``) survive as rooms.  Symbol outlines,
    fixture loops, hatch islands, and title-block boxes — which carry no numbered
    room tag inside them — are thereby excluded without any layer heuristic.

    Name-only labels (no number) do NOT anchor rooms; a polygon must contain a
    numbered label to be kept.  This is the primary gate; ``min_area`` is a
    belt-and-suspenders backstop for tiny loops that happen to nest inside a label.
    """
    numbered_points = [label.point for label in labels if parse_room_number(label.text) is not None]
    if not numbered_points:
        return []

    rooms: list[Room] = []
    for entity in entities:
        ring = _room_ring_from_entity(entity)
        if ring is None:
            continue
        polygon = build_polygon(ring)
        if polygon is None:
            continue
        if polygon.area < min_area:
            continue
        # PRIMARY GATE: polygon must contain at least one numbered label point.
        if not any(point_in_polygon(pt, polygon) for pt in numbered_points):
            continue
        rooms.append(room_from_polygon(entity.entity_id, polygon, source=source))
    return rooms


def interpret_explicit_rooms(
    entities: Sequence[EntityRow],
    *,
    devices: Sequence[DevicePlacement],
    labels: Sequence[RoomLabel],
    layer_refs: set[str] | None = None,
    min_area: float = 0.0,
) -> ExplicitRoomInterpretation:
    """Detect boundary layers, build named rooms, and assign devices by containment.

    ``layer_refs`` overrides auto-detection when the drawing uses non-standard
    layer names.  Returns the named rooms, the device→room assignments, and the
    boundary layers used.

    When no dedicated boundary layer is detected (the PDF single-layer fallback),
    rooms are instead built from closed polylines anchored by a numbered RoomLabel
    (#792): only polygons that contain a dot-separated room number survive.  The
    ``min_area`` guard is applied as a secondary backstop in that path.  DWG
    drawings with an explicit boundary layer are byte-identical to prior behaviour.
    """
    boundary_layers = (
        layer_refs if layer_refs is not None else detect_room_boundary_layers(entities)
    )
    if boundary_layers:
        # Dedicated boundary layer present (DWG path) — use it directly, unchanged.
        rooms = build_rooms_from_entities(entities, layer_refs=boundary_layers)
        rooms = assign_labels_to_rooms(rooms, labels)
        assignments = assign_devices_to_rooms(devices, rooms)
        return ExplicitRoomInterpretation(
            rooms=rooms,
            device_assignments=assignments,
            boundary_layers=tuple(sorted(boundary_layers)),
        )

    # No dedicated boundary layer (PDF / single-layer fallback) — anchor on numbered labels.
    effective_min_area = min_area if min_area > 0.0 else DEFAULT_LABEL_ANCHOR_MIN_AREA
    rooms = build_rooms_anchored_by_labels(
        entities, labels, min_area=effective_min_area, source=ROOM_SOURCE_EXPLICIT_LAYER
    )
    rooms = assign_labels_to_rooms(rooms, labels)
    assignments = assign_devices_to_rooms(devices, rooms)
    return ExplicitRoomInterpretation(
        rooms=rooms,
        device_assignments=assignments,
        boundary_layers=(),
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
