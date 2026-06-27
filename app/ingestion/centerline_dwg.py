"""DWG centerline producer — pure math, no cv2/skimage/shapely/SQLAlchemy.

Emits one :class:`~app.ingestion.centerline_contract.Centerline` per
:class:`~app.interpretation.routed_runs.RunGroup`.

Two paths per group:

* **Authored** — the group's layer name contains a known centerline-layer token
  (case-insensitive substring).  Length = sum of member drawn lengths.
  ``producer_kind="dwg_authored"``.

* **Derived** — width-agnostic rail-pair synthesis (#681).  Separates short
  perpendicular rungs from long boundary rails, then pairs the two boundary
  rails of any width by finding the nearest parallel, overlapping counterpart
  within a generous metre-scale offset cap.  The midline of each pair is the
  inferred centerline.  Unpaired rails contribute 0.5 x their drawn length
  (honest fallback).  Rungs contribute neither length nor polylines.
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
    decompose_geometry,
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

# Rung filter: segments shorter than this (metres) are treated as perpendicular
# crossbars (rungs) and excluded from pairing.  A rung spans the containment
# WIDTH; rails run the LENGTH.  0.7 m sits above the widest containment width on
# the evidence sheet (E-610003: 650 mm max) and below real rail-segment lengths —
# the derived length is stable across [0.7, 0.9] m (plateau on E-610003 real
# data), confirming 0.7 m is where rung-leakage stops.
# SINGLE-BUILDING CALIBRATION: a containment type wider than ~0.7 m would have
# rungs that escape this filter.  The principled follow-on is orientation-based
# rung detection (rung perpendicular to local run direction) or an adaptive
# bimodal length split — flagged as a follow-on (#681), not built here.
_RUNG_MAX_LEN_M: float = 0.7  # above widest tray rung (650 mm); plateau [0.7, 0.9] m

# Acceptable perpendicular offset range (metres) for a boundary-rail pair.
# Below _PERP_OFFSET_MIN_M the segments are likely co-linear (same rail);
# above _PERP_OFFSET_MAX_M they are structurally separate runs, not a
# double-line representation of one service.  The upper bound is a generous
# sanity cap — NOT tuned to a particular pipe/tray width — so the algorithm
# is width-agnostic across all cable-tray, duct, and pipe scales.
_PERP_OFFSET_MIN_M: float = 0.01  # ~1 cm — rejects same-line co-incident rails
_PERP_OFFSET_MAX_M: float = 2.0  # ~2 m  — generous sanity cap; structurally separate beyond this

# Minimum projection-overlap fraction (overlap / min rail length) to accept a
# pair.  0.5 means at least half of the shorter rail must shadow the longer.
_OVERLAP_MIN: float = 0.5

# Unpaired rails count at this fraction of their drawn length.  Honest
# fallback for double-line groups: we know the segment is a boundary rail,
# but we cannot confirm it is paired (expected residual at junctions).
_UNPAIRED_LENGTH_FACTOR: float = 0.5

# Per-group double-line detection: if at least this fraction of rails found a
# match, the group is drawn double-line (paired boundary rails).  Unpaired
# residuals in a double-line group are junction artefacts -> count 0.5x.
# If the pair fraction is BELOW this threshold the group is drawn single-line
# (each rail IS the pipe centerline) -> unpaired rails count FULL (1.0x).
#
# Evidence:
#   med-gas (M-540003): pair_fraction ~0.99 (192 rails, 190 matched)
#     -> stays double-line -> 0.5x unchanged (no regression).
#   drainage: drawn single-line -> pair_fraction << 0.5
#     -> single-line -> 1.0x (fixes ~17% under-count).
# Both disciplines are far from 0.5 so the exact threshold value is robust.
_DOUBLE_LINE_PAIR_FRACTION_MIN: float = 0.5

# Factor applied to unpaired rails in a single-line group: the rail IS the
# centerline, so it should count in full.
_SINGLE_LINE_UNPAIRED_FACTOR: float = 1.0

# ---------------------------------------------------------------------------
# Deterministic raster_params_hash for this producer
# (SHA-256 of a canonical JSON dump of the sorted tuning-constants dict)
# ---------------------------------------------------------------------------

_TUNING_CONSTANTS: dict[str, float] = {
    "double_line_pair_fraction_min": _DOUBLE_LINE_PAIR_FRACTION_MIN,
    "overlap_min": _OVERLAP_MIN,
    "parallel_dot_min": _PARALLEL_DOT_MIN,
    "perp_offset_max_m": _PERP_OFFSET_MAX_M,
    "perp_offset_min_m": _PERP_OFFSET_MIN_M,
    "rung_max_len_m": _RUNG_MAX_LEN_M,
    "single_line_unpaired_factor": _SINGLE_LINE_UNPAIRED_FACTOR,
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
    """Thin local alias — delegates to the shared contract helper.

    Lifted to ``app.ingestion.centerline_contract.decompose_geometry`` so that
    the PDF raster producer can import it without depending on this module.
    """
    return decompose_geometry(entity_ids, geometry_by_entity_id)


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
    """Width-agnostic rail-pair synthesis for one group (#681).

    Algorithm
    ---------
    1. Decompose member geometries into (start, end) segments (metres).
    2. **Rung filter**: discard segments shorter than _RUNG_MAX_LEN_M.  These
       are perpendicular crossbars connecting the two boundary rails; they
       carry no run-length information.  The parallel test (step 3) is the
       real correctness guard — rungs that slip past the length filter fail
       _PARALLEL_DOT_MIN because they are perpendicular to the run direction.
    3. **Pair rails** greedily (longest first, deterministic tie-break):
       - parallel   : |û_i · û_j| >= _PARALLEL_DOT_MIN
       - offset     : _PERP_OFFSET_MIN_M < perp < _PERP_OFFSET_MAX_M (metres)
       - overlap    : projection_overlap / min_length >= _OVERLAP_MIN
       - nearest    : pick the smallest valid offset; each rail ≤ 1 partner.
    4. **Midline** via _midline_polyline; accumulate into total_length.
    5. **Unpaired rails**: factor depends on group double-line-ness.
       If pair_fraction >= _DOUBLE_LINE_PAIR_FRACTION_MIN, the group is
       double-line; unpaired rails are junction residuals -> 0.5x drawn length.
       Otherwise the group is single-line; each rail is its own centerline -> 1.0x.
       Rungs contribute neither length nor polylines.
    Conservation invariant: every rail ends up in exactly one of (a) an emitted
    midline or (b) the unpaired residual (0.5x or 1.0x per group type).  De-dup is
    intentionally absent:
    pairing is mutual+greedy so each rail has at most one partner — a given
    midline cannot be emitted twice.  Two distinct physical runs whose midpoints
    coincide are both kept (double-counting is their honest measurement).

    Returns (polylines, total_length_du).
    """
    raw_segments = _decompose_geometry(entity_ids, geometry_by_entity_id)
    if not raw_segments:
        return (), 0.0

    # Pre-compute unit vectors and lengths; collect rail indices (non-rungs).
    all_units: list[tuple[float, float]] = []
    all_lengths: list[float] = []
    rail_indices: list[int] = []  # indices into raw_segments that are rails

    for idx, (s, e) in enumerate(raw_segments):
        result = _segment_unit_and_length(s[0], s[1], e[0], e[1])
        if result is None:
            # Zero-length — already filtered by _decompose_geometry; defensive.
            all_units.append((1.0, 0.0))
            all_lengths.append(0.0)
        else:
            u, ln = result
            all_units.append(u)
            all_lengths.append(ln)
            if ln > _RUNG_MAX_LEN_M:
                rail_indices.append(idx)

    if not rail_indices:
        return (), 0.0

    # Sort rails longest-first for deterministic greedy ordering.
    rail_indices.sort(key=lambda idx: -all_lengths[idx])
    n_rails = len(rail_indices)

    # Build candidate pairs (ri < rj in sorted order) passing all three filters.
    candidates: list[tuple[float, int, int]] = []
    for ri in range(n_rails):
        i = rail_indices[ri]
        ux_i, uy_i = all_units[i]
        len_i = all_lengths[i]
        s_i, _ = raw_segments[i]
        for rj in range(ri + 1, n_rails):
            j = rail_indices[rj]
            ux_j, uy_j = all_units[j]
            len_j = all_lengths[j]
            s_j, e_j = raw_segments[j]

            # 1. Parallelism: |cos(angle)| >= threshold (handles antiparallel).
            dot = abs(ux_i * ux_j + uy_i * uy_j)
            if dot < _PARALLEL_DOT_MIN:
                continue

            # 2. Perpendicular offset of j's midpoint to i's infinite line (metres).
            mid_jx = (s_j[0] + e_j[0]) / 2.0
            mid_jy = (s_j[1] + e_j[1]) / 2.0
            perp = abs(_perpendicular_offset(s_i[0], s_i[1], ux_i, uy_i, mid_jx, mid_jy))
            if perp < _PERP_OFFSET_MIN_M or perp > _PERP_OFFSET_MAX_M:
                continue

            # 3. Projection overlap fraction.
            overlap_frac = _projection_overlap_pts(s_i, s_j, e_j, ux_i, uy_i, len_i, len_j)
            if overlap_frac < _OVERLAP_MIN:
                continue

            candidates.append((perp, i, j))

    # Greedy match: sort by (perp_offset, i, j) asc; accept if both unmatched.
    # Conservation: every accepted pair emits a midline; every rail that never
    # enters `matched` falls to the 0.5x unpaired residual below.  No rail is
    # silently zeroed.  De-dup is intentionally absent (see docstring).
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
        ux_i, uy_i = all_units[i]

        mid_polyline, mid_length = _midline_polyline(s_i, e_i, s_j, e_j, ux_i, uy_i)
        polylines.append(mid_polyline)
        total_length += mid_length

    # Determine whether this group is double-line or single-line by the fraction
    # of rails that found a match.  Double-line groups have paired boundary rails;
    # unpaired residuals are junction artefacts and count 0.5x.  Single-line
    # groups (e.g. drainage) have no opposite rail -- each rail IS the centerline
    # and must count 1.0x to avoid a systematic ~17% under-count.
    pair_fraction = len(matched) / n_rails  # matched contains both halves of each pair
    unpaired_factor = (
        _UNPAIRED_LENGTH_FACTOR
        if pair_fraction >= _DOUBLE_LINE_PAIR_FRACTION_MIN
        else _SINGLE_LINE_UNPAIRED_FACTOR
    )

    # Unpaired rails: apply per-group factor.  Rungs (not in rail_indices) contribute nothing.
    rail_set = set(rail_indices)
    for k in rail_set:
        if k not in matched and all_lengths[k] > 0.0:
            s_k, e_k = raw_segments[k]
            polylines.append((s_k, e_k))
            total_length += all_lengths[k] * unpaired_factor

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
