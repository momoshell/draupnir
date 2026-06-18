"""Axis-aligned bounding box extraction from canonical entity geometry.

Computes a deterministic ``(min_x, min_y, max_x, max_y)`` extent from an entity's
``geometry_json`` so it can be persisted on the materialized row and used for
indexed spatial queries (bbox intersection / near-point). Pure and dependency-free;
returns ``None`` for geometry with no recoverable 2-D extent (e.g. a tag with no
position, or an unparseable payload).
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

BoundingBox = tuple[float, float, float, float]

# Keys that may carry a single representative point (point / text / insert entities).
_SINGLE_POINT_KEYS = ("position", "point", "center", "insertion", "location")
# Keys that may carry a list of points (polyline / lwpolyline / hatch boundaries).
_POINT_LIST_KEYS = ("points", "vertices")
# Keys that may carry a single point as start/end (line / segment).
_SEGMENT_POINT_KEYS = ("start", "end", "p1", "p2")


def compute_entity_bbox(geometry_json: Any) -> BoundingBox | None:
    """Return the AABB of an entity's geometry, or ``None`` if it has no 2-D extent."""
    if not isinstance(geometry_json, Mapping):
        return None

    xs: list[float] = []
    ys: list[float] = []

    for key in _SEGMENT_POINT_KEYS + _SINGLE_POINT_KEYS:
        point = _point(geometry_json.get(key))
        if point is not None:
            xs.append(point[0])
            ys.append(point[1])

    for key in _POINT_LIST_KEYS:
        for point in _points(geometry_json.get(key)):
            xs.append(point[0])
            ys.append(point[1])

    # A circle/arc contributes its full extent when a finite radius is present.
    center = _point(geometry_json.get("center"))
    radius = _finite(geometry_json.get("radius"))
    if center is not None and radius is not None and radius > 0:
        xs.extend((center[0] - radius, center[0] + radius))
        ys.extend((center[1] - radius, center[1] + radius))

    if xs and ys:
        return (min(xs), min(ys), max(xs), max(ys))

    # Fall back to an adapter-provided precomputed extent if present.
    return _summary_bbox(geometry_json.get("geometry_summary"))


def _finite(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _point(raw: Any) -> tuple[float, float] | None:
    if isinstance(raw, Mapping):
        x, y = _finite(raw.get("x")), _finite(raw.get("y"))
        return (x, y) if x is not None and y is not None else None
    if isinstance(raw, (list, tuple)) and len(raw) >= 2:
        x, y = _finite(raw[0]), _finite(raw[1])
        return (x, y) if x is not None and y is not None else None
    return None


def _points(raw: Any) -> list[tuple[float, float]]:
    if not isinstance(raw, (list, tuple)):
        return []
    out: list[tuple[float, float]] = []
    for item in raw:
        point = _point(item)
        if point is not None:
            out.append(point)
    return out


def _summary_bbox(summary: Any) -> BoundingBox | None:
    if not isinstance(summary, Mapping):
        return None
    raw_bbox = summary.get("bbox")
    if isinstance(raw_bbox, Mapping):
        values = [_finite(raw_bbox.get(axis)) for axis in ("min_x", "min_y", "max_x", "max_y")]
    elif isinstance(raw_bbox, (list, tuple)) and len(raw_bbox) >= 4:
        values = [_finite(component) for component in raw_bbox[:4]]
    else:
        return None
    if any(value is None for value in values):
        return None
    min_x, min_y, max_x, max_y = (v for v in values if v is not None)
    return (min_x, min_y, max_x, max_y)
