"""DWG centerline producer — pure math, no cv2/skimage/shapely/SQLAlchemy.

Emits one :class:`~app.ingestion.centerline_contract.Centerline` per
:class:`~app.interpretation.routed_runs.RunGroup`.

Two paths per group:

* **Authored** — the group's layer name contains a known centerline-layer token
  (case-insensitive substring).  Length = sum of member drawn lengths.
  ``producer_kind="dwg_authored"``.

* **Derived** — greedy parallel-segment pairing finds double-line wall pairs;
  the midline of each pair is the inferred centerline.  Unpaired segments
  contribute 0.5 x their drawn length (honest fallback).
  ``producer_kind="dwg_derived"``.

No DB, ORM, FastAPI, or SQLAlchemy imports.  Safe to use anywhere in the
pipeline, including the read path.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from typing import Any

from app.ingestion.centerline_contract import (
    CURRENT_ALGO_VERSION,
    Centerline,
    CenterlineGeometry,
    entity_group_drawn_length,
)
from app.interpretation.routed_runs import RunGroup

# ---------------------------------------------------------------------------
# Centerline-layer tokens (local copy to avoid importing service_takeoff_loaders
# in this pure module — that module triggers a circular import + DB engine init
# in isolation).  The canonical definition lives at:
#   app.interpretation.service_takeoff_loaders._DEFAULT_CENTERLINE_LAYER_TOKENS
# Keep in sync with that source of truth.
# ---------------------------------------------------------------------------

_DEFAULT_CENTERLINE_LAYER_TOKENS: tuple[str, ...] = (
    "center line",
    "centre line",
    "centerline",
    "centreline",
)

# ---------------------------------------------------------------------------
# Tunable algorithm constants (all named; hash is load-bearing)
# ---------------------------------------------------------------------------

# Minimum |cos(angle)| between two unit vectors to consider them parallel.
# cos(10 deg) ~ 0.985 — rejects walls that are genuinely non-parallel.
_PARALLEL_DOT_MIN: float = 0.985

# Acceptable perpendicular offset range (drawing units) for a wall pair.
# Below 2 du the segments are likely the same line; above 80 du they are
# structurally separate runs, not a double-line representation of one pipe.
_PERP_OFFSET_MIN: float = 2.0
_PERP_OFFSET_MAX: float = 80.0

# Minimum projection-overlap fraction (overlap / min wall length) to accept a
# pair.  0.5 means at least half of the shorter wall must shadow the longer.
_OVERLAP_MIN: float = 0.5

# Unpaired segments count at this fraction of their drawn length.  Honest
# fallback: we know the segment is a wall, but we cannot confirm it is paired.
_UNPAIRED_LENGTH_FACTOR: float = 0.5

# ---------------------------------------------------------------------------
# Deterministic raster_params_hash for this producer
# (SHA-256 of a canonical JSON dump of the sorted tuning-constants dict)
# ---------------------------------------------------------------------------

_TUNING_CONSTANTS: dict[str, float] = {
    "overlap_min": _OVERLAP_MIN,
    "parallel_dot_min": _PARALLEL_DOT_MIN,
    "perp_offset_max": _PERP_OFFSET_MAX,
    "perp_offset_min": _PERP_OFFSET_MIN,
    "unpaired_length_factor": _UNPAIRED_LENGTH_FACTOR,
}

_DWG_RASTER_PARAMS_HASH: str = hashlib.sha256(
    json.dumps(_TUNING_CONSTANTS, sort_keys=True).encode()
).hexdigest()


# ---------------------------------------------------------------------------
# Internal geometry helpers
# ---------------------------------------------------------------------------


def _segment_unit_and_length(
    sx: float, sy: float, ex: float, ey: float
) -> tuple[tuple[float, float], float] | None:
    """Return (unit_vector, length) for a segment, or None if zero-length."""
    dx = ex - sx
    dy = ey - sy
    length = math.hypot(dx, dy)
    if length == 0.0:
        return None
    return (dx / length, dy / length), length


def _decompose_geometry(
    entity_ids: tuple[str, ...],
    geometry_by_entity_id: Mapping[str, Mapping[str, Any]],
) -> list[tuple[tuple[float, float], tuple[float, float]]]:
    """Decompose member geometries into (start, end) 2-D segments.

    Mirrors the same geometry-type dispatch as ``entity_group_drawn_length``:
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
                if _segment_unit_and_length(*p0, *p1) is not None:
                    segments.append((p0, p1))
            continue
        pts: Any = geom.get("vertices") or geom.get("points")
        if pts:
            coords = [(float(p[0]), float(p[1])) for p in pts if len(p) >= 2]
            for i in range(len(coords) - 1):
                p0 = coords[i]
                p1 = coords[i + 1]
                if _segment_unit_and_length(*p0, *p1) is not None:
                    segments.append((p0, p1))
    return segments


def _perpendicular_offset(
    sx: float,
    sy: float,
    ux: float,
    uy: float,
    px: float,
    py: float,
) -> float:
    """Signed perpendicular distance from point (px,py) to the infinite line
    through (sx,sy) with unit direction (ux,uy)."""
    # normal = (-uy, ux); offset = dot(point - base, normal)
    return (px - sx) * (-uy) + (py - sy) * ux


def _projection_overlap_pts(
    s_i: tuple[float, float],
    s_j: tuple[float, float],
    e_j: tuple[float, float],
    ux_i: float,
    uy_i: float,
    len_i: float,
    len_j: float,
) -> float:
    """Overlap of segment j projected onto segment i's infinite line,
    normalised by min(len_i, len_j).  Returns value in [0, inf)."""
    origin_x, origin_y = s_i
    # Scalar projections of all four endpoints along i's direction.
    proj_i0 = 0.0  # s_i projected = 0 (origin)
    proj_i1 = len_i
    proj_j0 = (s_j[0] - origin_x) * ux_i + (s_j[1] - origin_y) * uy_i
    proj_j1 = (e_j[0] - origin_x) * ux_i + (e_j[1] - origin_y) * uy_i
    lo_j = min(proj_j0, proj_j1)
    hi_j = max(proj_j0, proj_j1)
    overlap = max(0.0, min(proj_i1, hi_j) - max(proj_i0, lo_j))
    denom = min(len_i, len_j)
    if denom == 0.0:
        return 0.0
    return overlap / denom


def _midline_polyline(
    s_i: tuple[float, float],
    e_i: tuple[float, float],
    s_j: tuple[float, float],
    e_j: tuple[float, float],
    ux_i: float,
    uy_i: float,
) -> tuple[tuple[tuple[float, float], ...], float]:
    """Compute the 2-point midline polyline for a matched wall pair.

    Aligns j's direction to i's (handles antiparallel case) then averages
    corresponding endpoints.  Returns (polyline_coords, midline_length).
    """
    # Align j so its direction matches i (handles antiparallel: dot < 0).
    ux_j = e_j[0] - s_j[0]
    uy_j = e_j[1] - s_j[1]
    dot = ux_j * ux_i + uy_j * uy_i
    if dot < 0:
        # Flip j so start pairs with start.
        s_j, e_j = e_j, s_j

    mid_s: tuple[float, float] = ((s_i[0] + s_j[0]) / 2.0, (s_i[1] + s_j[1]) / 2.0)
    mid_e: tuple[float, float] = ((e_i[0] + e_j[0]) / 2.0, (e_i[1] + e_j[1]) / 2.0)
    dx = mid_e[0] - mid_s[0]
    dy = mid_e[1] - mid_s[1]
    length = math.hypot(dx, dy)
    # Canonical direction: (min_x, then min_y) at index 0 so the polyline is
    # invariant under which segment plays i vs j.
    if (mid_s[0], mid_s[1]) > (mid_e[0], mid_e[1]):
        mid_s, mid_e = mid_e, mid_s
    return (mid_s, mid_e), length


# ---------------------------------------------------------------------------
# Pairing algorithm
# ---------------------------------------------------------------------------


def _derive_centerline(
    entity_ids: tuple[str, ...],
    geometry_by_entity_id: Mapping[str, Mapping[str, Any]],
) -> tuple[tuple[tuple[tuple[float, float], ...], ...], float]:
    """Run the greedy double-line pairing algorithm for one group.

    Returns (polylines, total_length_du).
    """
    raw_segments = _decompose_geometry(entity_ids, geometry_by_entity_id)
    if not raw_segments:
        return (), 0.0

    n = len(raw_segments)

    # Pre-compute unit vectors and lengths for all segments.
    units: list[tuple[float, float]] = []
    lengths: list[float] = []
    for s, e in raw_segments:
        result = _segment_unit_and_length(s[0], s[1], e[0], e[1])
        if result is None:
            # Already filtered by _decompose_geometry, but be defensive.
            units.append((1.0, 0.0))
            lengths.append(0.0)
        else:
            u, ln = result
            units.append(u)
            lengths.append(ln)

    # Build candidate pairs (i < j) that pass all three geometric filters.
    candidates: list[tuple[float, int, int]] = []
    for i in range(n):
        if lengths[i] == 0.0:
            continue
        ux_i, uy_i = units[i]
        len_i = lengths[i]
        s_i, e_i = raw_segments[i]
        for j in range(i + 1, n):
            if lengths[j] == 0.0:
                continue
            ux_j, uy_j = units[j]
            len_j = lengths[j]
            s_j, e_j = raw_segments[j]

            # 1. Parallelism: |cos(angle)| >= threshold (handles antiparallel).
            dot = abs(ux_i * ux_j + uy_i * uy_j)
            if dot < _PARALLEL_DOT_MIN:
                continue

            # 2. Perpendicular offset of j's midpoint to i's infinite line.
            mid_jx = (s_j[0] + e_j[0]) / 2.0
            mid_jy = (s_j[1] + e_j[1]) / 2.0
            perp = abs(_perpendicular_offset(s_i[0], s_i[1], ux_i, uy_i, mid_jx, mid_jy))
            if perp < _PERP_OFFSET_MIN or perp > _PERP_OFFSET_MAX:
                continue

            # 3. Projection overlap fraction.
            overlap_frac = _projection_overlap_pts(s_i, s_j, e_j, ux_i, uy_i, len_i, len_j)
            if overlap_frac < _OVERLAP_MIN:
                continue

            candidates.append((perp, i, j))

    # Greedy match: sort by (perp_offset, i, j) asc; accept if both unmatched.
    candidates.sort()
    matched: set[int] = set()
    polylines: list[tuple[tuple[float, float], ...]] = []
    total_length = 0.0

    for _perp, i, j in candidates:
        if i in matched or j in matched:
            continue
        matched.add(i)
        matched.add(j)

        s_i, e_i = raw_segments[i]
        s_j, e_j = raw_segments[j]
        ux_i, uy_i = units[i]

        mid_polyline, mid_length = _midline_polyline(s_i, e_i, s_j, e_j, ux_i, uy_i)
        polylines.append(mid_polyline)
        total_length += mid_length

    # Unpaired segments: 0.5 x their drawn length.
    for k in range(n):
        if k not in matched and lengths[k] > 0.0:
            s_k, e_k = raw_segments[k]
            polylines.append((s_k, e_k))
            total_length += lengths[k] * _UNPAIRED_LENGTH_FACTOR

    return tuple(polylines), total_length


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------


def dwg_centerlines(
    run_groups: Sequence[RunGroup],
    geometry_by_entity_id: Mapping[str, Mapping[str, Any]],
) -> list[Centerline]:
    """Produce one :class:`Centerline` per group using DWG geometry.

    Parameters
    ----------
    run_groups:
        P1 RunGroup objects.  Each group defines a (layer_ref, colour_key) key
        and the set of member entity IDs whose geometries are consumed.
    geometry_by_entity_id:
        Geometry dict keyed by entity ID.  Missing entity IDs contribute 0 to
        the length sum (tolerant).

    Returns
    -------
    list[Centerline]
        One ``Centerline`` per group, in the same order as ``run_groups``.
    """
    results: list[Centerline] = []
    for group in run_groups:
        entity_ids: tuple[str, ...] = tuple(group.entity_ids)
        layer_ref_lower = (group.layer_ref or "").lower()

        # Authored path: layer name contains a recognised centerline token.
        is_authored = any(token in layer_ref_lower for token in _DEFAULT_CENTERLINE_LAYER_TOKENS)

        if is_authored:
            length_du = entity_group_drawn_length(entity_ids, geometry_by_entity_id)
            # Reconstruct polylines from member geometries for the authored path.
            raw_segs = _decompose_geometry(entity_ids, geometry_by_entity_id)
            polylines: tuple[tuple[tuple[float, float], ...], ...] = tuple(
                (s, e) for s, e in raw_segs
            )
            producer_kind = "dwg_authored"
        else:
            raw_polylines, length_du = _derive_centerline(entity_ids, geometry_by_entity_id)
            polylines = raw_polylines
            producer_kind = "dwg_derived"

        results.append(
            Centerline(
                layer_ref=group.layer_ref,
                colour_key=group.colour_key,
                geometry=CenterlineGeometry(
                    polylines=polylines,
                    length_du=length_du,
                ),
                entity_count=len(entity_ids),
                algo_version=CURRENT_ALGO_VERSION,
                raster_params_hash=_DWG_RASTER_PARAMS_HASH,
                producer_kind=producer_kind,
            )
        )
    return results
