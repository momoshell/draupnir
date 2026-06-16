"""Pure geometry primitives for room interpretation (issue #460, Phase 2).

Backed by shapely. Deterministic and free of DB/FastAPI concerns, so each
primitive is unit-testable with coordinate fixtures. Coordinates are plain
``(x, y)`` float tuples; shapely types are kept at the boundary and never leak an
untyped value through the public signatures (bools/floats are coerced).
"""

from __future__ import annotations

from collections.abc import Sequence

from shapely.geometry import LineString, Polygon
from shapely.geometry import Point as _ShapelyPoint
from shapely.ops import polygonize as _shapely_polygonize
from shapely.ops import unary_union

Point = tuple[float, float]
"""A 2D coordinate as an ``(x, y)`` float tuple."""

BoundingBox = tuple[float, float, float, float]
"""An axis-aligned bounding box as ``(min_x, min_y, max_x, max_y)``."""

_MIN_RING_VERTICES = 3


def build_polygon(vertices: Sequence[Point]) -> Polygon | None:
    """Build a valid polygon from ring vertices, or ``None`` for degenerate input.

    Accepts an open or closed ring (a repeated closing vertex is fine). Returns
    ``None`` when there are fewer than three distinct vertices, when the ring has
    zero area (collinear/degenerate), or when shapely cannot form a valid polygon
    (a ``buffer(0)`` self-intersection repair is attempted first).
    """
    distinct = _distinct_ring(vertices)
    if len(distinct) < _MIN_RING_VERTICES:
        return None

    try:
        polygon = Polygon(distinct)
    except (ValueError, TypeError):
        return None

    if polygon.is_empty or polygon.area == 0:
        return None

    if not polygon.is_valid:
        repaired = polygon.buffer(0)
        if not isinstance(repaired, Polygon) or repaired.is_empty or repaired.area == 0:
            return None
        polygon = repaired

    return polygon


def polygonize(segments: Sequence[tuple[Point, Point]]) -> list[Polygon]:
    """Derive the closed faces (polygons) enclosed by a set of line segments.

    Segments are noded together before polygonization, so linework that only
    meets at crossings still yields faces. Zero-length segments are ignored.
    Returns an empty list when no closed face is formed.
    """
    lines = [LineString([start, end]) for start, end in segments if start != end]
    if not lines:
        return []

    noded = unary_union(lines)
    return [geom for geom in _shapely_polygonize(noded) if isinstance(geom, Polygon)]


def point_in_polygon(point: Point, polygon: Polygon) -> bool:
    """Return whether ``point`` lies inside or on the boundary of ``polygon``.

    Boundary-inclusive (uses ``covers``), so a label sitting exactly on a wall
    line still counts as contained.
    """
    return bool(polygon.covers(_ShapelyPoint(point)))


def smallest_containing(point: Point, polygons: Sequence[Polygon]) -> Polygon | None:
    """Return the smallest-area polygon containing ``point``, or ``None``.

    With nested spaces (e.g. a closet inside a room) the innermost polygon wins,
    which is the room a point most specifically belongs to.
    """
    shapely_point = _ShapelyPoint(point)
    containing = [polygon for polygon in polygons if polygon.covers(shapely_point)]
    if not containing:
        return None
    return min(containing, key=lambda polygon: float(polygon.area))


def polygon_area(polygon: Polygon) -> float:
    """Return the area of ``polygon``."""
    return float(polygon.area)


def bounding_box(polygon: Polygon) -> BoundingBox:
    """Return the axis-aligned bounding box of ``polygon`` as ``(min_x, min_y, max_x, max_y)``."""
    min_x, min_y, max_x, max_y = polygon.bounds
    return (float(min_x), float(min_y), float(max_x), float(max_y))


def _distinct_ring(vertices: Sequence[Point]) -> list[Point]:
    """Drop a repeated closing vertex and consecutive duplicate coordinates."""
    cleaned: list[Point] = []
    for vertex in vertices:
        coord = (float(vertex[0]), float(vertex[1]))
        if not cleaned or cleaned[-1] != coord:
            cleaned.append(coord)
    if len(cleaned) > 1 and cleaned[0] == cleaned[-1]:
        cleaned.pop()
    return cleaned
