"""Pure-Python contract types for centerline routed-length results.

No cv2/skimage/shapely/numpy imports — this module must remain importable on
the read path where heavy vision dependencies are absent.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------
# Versioning constant -- bump when the algorithm OR the persisted output contract changes.
# "c3-geom-1" (#653): persists centerline polylines into geometry_json (was NULL); the bump
# invalidates pre-geometry rows so the version-gated read re-materializes them with geometry.
# "c4-rail-1" (#681): derived path rewritten to width-agnostic rail-pair synthesis at
# metre-scale; rung filter separates long rails from short perpendicular crossbars; pairing
# bounds are now metre-scale (not drawing-unit-tuned) so metre-scale geometry pairs correctly.
# ---------------------------------------------------------------------------

CURRENT_ALGO_VERSION: str = "c4-rail-1"


# ---------------------------------------------------------------------------
# Shared pure geometry helper (avoids tier inversion between producer + coordinator)
# ---------------------------------------------------------------------------


def _xy(point: Any) -> tuple[float, float] | None:
    """Extract an (x, y) pair from a coordinate in either real shape.

    Materialized canonical geometry uses **dict** coords (``{"x":.., "y":.., "z":..}``); some
    fixtures/legacy payloads use **list/tuple** coords (``[x, y, z]``). Returns ``None`` for
    anything unusable. Pure — no cv2/numpy; safe on the read path.
    """
    if isinstance(point, Mapping):
        x, y = point.get("x"), point.get("y")
    elif isinstance(point, (list, tuple)) and len(point) >= 2:
        x, y = point[0], point[1]
    else:
        return None
    if isinstance(x, (int, float)) and isinstance(y, (int, float)):
        return (float(x), float(y))
    return None


def _xy_list(pts: Any) -> list[tuple[float, float]]:
    """Extract a list of (x, y) pairs from a vertices/points sequence (dict or list coords)."""
    if not isinstance(pts, (list, tuple)):
        return []
    out: list[tuple[float, float]] = []
    for p in pts:
        xy = _xy(p)
        if xy is not None:
            out.append(xy)
    return out


def entity_group_drawn_length(
    entity_ids: tuple[str, ...],
    geometry_by_entity_id: Mapping[str, Mapping[str, Any]],
) -> float:
    """Sum drawn length for a group of entity IDs.

    Implements the same geometry-sum rule as ``service_takeoff._entity_drawn_length``
    but lives here so both the passthrough producer and any future caller can import
    it without creating a tier inversion (pure contract module <- impure worker).

    - line     -> Euclidean distance between start[:2] and end[:2]
    - polyline -> sum of consecutive-vertex distances over vertices/points [:2]
    - arc      -> 0 (arc length requires radius+span, not yet in schema; P4)
    - missing  -> 0
    """
    total = 0.0
    for eid in entity_ids:
        geom = geometry_by_entity_id.get(eid)
        if geom is None:
            continue
        if "start" in geom and "end" in geom:
            s = _xy(geom["start"])
            e = _xy(geom["end"])
            if s is not None and e is not None:
                total += math.hypot(e[0] - s[0], e[1] - s[1])
            continue
        coords = _xy_list(geom.get("vertices") or geom.get("points"))
        for i in range(len(coords) - 1):
            total += math.hypot(coords[i + 1][0] - coords[i][0], coords[i + 1][1] - coords[i][1])
    return total


# ---------------------------------------------------------------------------
# Shared geometry decomposition helper
# ---------------------------------------------------------------------------


def _segment_nonzero_length(sx: float, sy: float, ex: float, ey: float) -> bool:
    """Return True iff the segment has non-zero Euclidean length."""
    return math.hypot(ex - sx, ey - sy) != 0.0


def decompose_geometry(
    entity_ids: tuple[str, ...],
    geometry_by_entity_id: Mapping[str, Mapping[str, Any]],
) -> list[tuple[tuple[float, float], tuple[float, float]]]:
    """Decompose member geometries into (start, end) 2-D segments.

    Pure helper shared by the DWG and PDF producers.  No cv2/skimage.

    Dispatches on geometry type:
    - line     -> one segment (start[:2], end[:2])
    - polyline -> consecutive vertex/points[:2] pairs
    - arc      -> skipped (arc length requires radius+span)
    - zero-length or missing -> skipped
    """
    segments: list[tuple[tuple[float, float], tuple[float, float]]] = []
    for eid in entity_ids:
        geom = geometry_by_entity_id.get(eid)
        if geom is None:
            continue
        if "start" in geom and "end" in geom:
            s = _xy(geom["start"])
            e = _xy(geom["end"])
            if s is not None and e is not None and _segment_nonzero_length(*s, *e):
                segments.append((s, e))
            continue
        coords = _xy_list(geom.get("vertices") or geom.get("points"))
        for i in range(len(coords) - 1):
            p0 = coords[i]
            p1 = coords[i + 1]
            if _segment_nonzero_length(*p0, *p1):
                segments.append((p0, p1))
    return segments


# ---------------------------------------------------------------------------
# Contract dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CenterlineGeometry:
    """Geometric output of a single centerline run."""

    polylines: tuple[tuple[tuple[float, float], ...], ...]
    length_du: float


# ---------------------------------------------------------------------------
# Geometry persistence (#653) -- pure JSON (de)serialization, read-path safe
# ---------------------------------------------------------------------------

_GEOMETRY_JSON_SCHEMA_VERSION: str = "0.1"
_COORD_PRECISION: int = 3


def geometry_to_json(geometry: CenterlineGeometry) -> dict[str, Any] | None:
    """Serialize centerline polylines to a JSON-safe payload, or ``None`` when there are none.

    Coordinates are rounded to ``_COORD_PRECISION`` decimals (drawing units) to bound payload
    size while staying well below any measurement-relevant precision. Returns ``None`` for empty
    polylines (e.g. the passthrough producer) so the persisted ``geometry_json`` stays NULL there.
    """
    if not geometry.polylines:
        return None
    polylines = [
        [[round(float(x), _COORD_PRECISION), round(float(y), _COORD_PRECISION)] for (x, y) in pl]
        for pl in geometry.polylines
    ]
    return {"schema_version": _GEOMETRY_JSON_SCHEMA_VERSION, "polylines": polylines}


def polylines_from_geometry_json(
    payload: Any,
) -> tuple[tuple[tuple[float, float], ...], ...]:
    """Deserialize a persisted ``geometry_json`` payload back to polylines.

    Defensive: returns ``()`` for absent/malformed payloads, and drops polylines with fewer than
    two points (a clip needs segments). Pure — no cv2/skimage, safe on the read path.
    """
    if not isinstance(payload, Mapping):
        return ()
    raw = payload.get("polylines")
    if not isinstance(raw, (list, tuple)):
        return ()
    out: list[tuple[tuple[float, float], ...]] = []
    for pl in raw:
        if not isinstance(pl, (list, tuple)):
            continue
        pts = tuple(
            (float(p[0]), float(p[1])) for p in pl if isinstance(p, (list, tuple)) and len(p) >= 2
        )
        if len(pts) >= 2:
            out.append(pts)
    return tuple(out)


@dataclass(frozen=True)
class Centerline:
    """Contract value produced by a centerline producer for one group."""

    layer_ref: str | None
    colour_key: str | None
    geometry: CenterlineGeometry
    entity_count: int
    algo_version: str
    raster_params_hash: str
    producer_kind: str

    @property
    def group_key(self) -> tuple[str | None, str | None]:
        """Stable group key: (layer_ref, colour_key)."""
        return (self.layer_ref, self.colour_key)
