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
# Versioning constant -- bump when the passthrough algorithm changes
# ---------------------------------------------------------------------------

CURRENT_ALGO_VERSION: str = "c2-pdf-1"


# ---------------------------------------------------------------------------
# Shared pure geometry helper (avoids tier inversion between producer + coordinator)
# ---------------------------------------------------------------------------


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
            s = geom["start"]
            e = geom["end"]
            if len(s) >= 2 and len(e) >= 2:
                dx = e[0] - s[0]
                dy = e[1] - s[1]
                total += (dx * dx + dy * dy) ** 0.5
            continue
        pts: Any = geom.get("vertices") or geom.get("points")
        if pts:
            coords = [(p[0], p[1]) for p in pts if len(p) >= 2]
            for i in range(len(coords) - 1):
                dx = coords[i + 1][0] - coords[i][0]
                dy = coords[i + 1][1] - coords[i][1]
                total += (dx * dx + dy * dy) ** 0.5
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
            s = geom["start"]
            e = geom["end"]
            if len(s) >= 2 and len(e) >= 2:
                p0: tuple[float, float] = (float(s[0]), float(s[1]))
                p1: tuple[float, float] = (float(e[0]), float(e[1]))
                if _segment_nonzero_length(*p0, *p1):
                    segments.append((p0, p1))
            continue
        pts: Any = geom.get("vertices") or geom.get("points")
        if pts:
            coords = [(float(p[0]), float(p[1])) for p in pts if len(p) >= 2]
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
