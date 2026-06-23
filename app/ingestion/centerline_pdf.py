"""PDF raster centerline producer.

Emits one :class:`~app.ingestion.centerline_contract.Centerline` per
:class:`~app.interpretation.routed_runs.RunGroup` by rasterizing member
segments onto a per-group bitmap, closing gaps with a morphological CLOSE,
skeletonizing with skimage, pruning spurs, then measuring skeleton arc length
directly from the binary mask as (orthogonal-neighbour edges) + sqrt(2) *
(diagonal-neighbour edges) summed over all skeleton pixels and halved (each
edge is counted from both endpoints).

``producer_kind="pdf_raster"``

Algorithm note: the raster-skeleton approach yields ~0.93x of the true
midline length (validated on real M-540003 at 1:50).  This is a systematic
undercount inherent to discrete skeletonization and is the accepted ground for
this producer version; be-641b may bump the algo version if a correction
factor is introduced.

The vectorize-then-Euclidean approach under-counts because the greedy
chain-tracer drops skeleton branches at junctions.  The adjacency-sum method
accounts for ALL skeleton pixels regardless of connectivity topology.

cv2 and skimage are imported LAZILY inside ``pdf_centerlines`` — this module
must remain importable without those packages present (ADR-008).  A missing
ingestion extra causes an honest ImportError; the job fails and the read stays
provisional.  Do NOT catch the ImportError.

No DB, ORM, or SQLAlchemy imports.
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
)
from app.interpretation.routed_runs import RunGroup

# ---------------------------------------------------------------------------
# Tunable raster constants (all named; feed the deterministic params hash)
# ---------------------------------------------------------------------------

# Drawing-unit to pixel scale for PDF producers.  PDF drawing units are POINTS
# (1/72 inch).  A 1:50 sheet has wall gaps of ~3 pt; R=3 px/pt gives a 9-px
# kernel that bridges double-line walls reliably without flooding the bitmap.
_RASTER_PX_PER_DU: float = 3.0

# cv2.line stroke thickness in pixels.  STROKE=1 at R=3 px/pt paints a
# 3-px-wide effective stripe (after anti-aliasing) that survives the CLOSE
# without thickening the skeleton beyond one pixel.
_STROKE_PX: int = 1

# Bitmap border padding in drawing units (points) added on every side of the
# group bounding box.  Keeps segments near the edge from being clipped.
_RASTER_MARGIN_DU: float = 5.0

# Default pipe/wall diameter prior in POINTS (PDF drawing units).
# At 1:50 scale a typical Ø54 mm wall gap is ~3 pt.  The closing kernel is
# round(diameter_du * px_per_du) -> 9 px (odd, so no adjustment needed).
# NOTE: this constant is scale-dependent; adaptive per-group tag sizing (using
# the Ø tag extracted from the PDF) is deferred to the #641-follow-up.
_DEFAULT_DIAMETER_DU: float = 3.0

# Morphological CLOSE kernel diameter clamp (pixels).  Must be odd.
_CLOSE_KERNEL_MIN_PX: int = 3
_CLOSE_KERNEL_MAX_PX: int = 201

# Skeleton spurs shorter than this (pixels) are pruned.  At R=3 px/pt, 5 px
# corresponds to ~1.7 pt which may over-prune very short branches; validated
# against M-540003 real data where 5 px prune has minimal effect on total length.
_PRUNE_MIN_PX: int = 5

# ---------------------------------------------------------------------------
# Deterministic params hash (SHA-256 of sorted canonical JSON)
# ---------------------------------------------------------------------------

_TUNING_CONSTANTS: dict[str, float | int] = {
    "close_kernel_max_px": _CLOSE_KERNEL_MAX_PX,
    "close_kernel_min_px": _CLOSE_KERNEL_MIN_PX,
    "default_diameter_du": _DEFAULT_DIAMETER_DU,
    "prune_min_px": _PRUNE_MIN_PX,
    "raster_margin_du": _RASTER_MARGIN_DU,
    "raster_px_per_du": _RASTER_PX_PER_DU,
    "stroke_px": _STROKE_PX,
}

_PDF_RASTER_PARAMS_HASH: str = hashlib.sha256(
    json.dumps(_TUNING_CONSTANTS, sort_keys=True).encode()
).hexdigest()


# ---------------------------------------------------------------------------
# Internal raster helpers (no cv2/skimage at module level)
# ---------------------------------------------------------------------------


def _kernel_diameter_px(diameter_du: float, px_per_du: float) -> int:
    """Convert a diameter in DU to the nearest odd pixel kernel size, clamped."""
    raw = round(diameter_du * px_per_du)
    # Morphological kernel size must be odd.
    if raw % 2 == 0:
        raw += 1
    return max(_CLOSE_KERNEL_MIN_PX, min(_CLOSE_KERNEL_MAX_PX, raw))


def _skeleton_arc_length_px(skeleton: Any) -> float:  # 2-D bool numpy array
    """Measure arc length of a binary skeleton via adjacency sums.

    For every skeleton pixel, count its orthogonal neighbours (distance 1) and
    diagonal neighbours (distance sqrt(2)).  Each edge is counted from both
    endpoints so we halve at the end.  This traverses ALL skeleton pixels
    regardless of connectivity topology, avoiding the branch-dropping that
    afflicts greedy chain-tracers at junctions.

    Returns the total arc length in pixels (float).
    """
    import numpy as np

    rows, cols = skeleton.shape
    ys_arr, xs_arr = np.where(skeleton)
    orth_count = 0
    diag_count = 0
    for row, col in zip(ys_arr.tolist(), xs_arr.tolist(), strict=True):
        for dr, dc in ((-1, 0), (1, 0), (0, -1), (0, 1)):
            nr, nc = row + dr, col + dc
            if 0 <= nr < rows and 0 <= nc < cols and skeleton[nr, nc]:
                orth_count += 1
        for dr, dc in ((-1, -1), (-1, 1), (1, -1), (1, 1)):
            nr, nc = row + dr, col + dc
            if 0 <= nr < rows and 0 <= nc < cols and skeleton[nr, nc]:
                diag_count += 1
    return (orth_count + diag_count * math.sqrt(2)) / 2.0


def _prune_spurs(
    skeleton: Any,  # 2-D bool numpy array
    min_px: int,
) -> Any:
    """Remove spur branches shorter than min_px from a skeleton.

    Deterministic: iterates over spur endpoints in sorted (row, col) order.
    Returns a new bool array (does not mutate input).

    A spur endpoint is a skeleton pixel with exactly one 8-connected neighbour.
    We trace inward until reaching a junction or exhausting the spur.  Spurs
    shorter than min_px are cleared.
    """
    import numpy as np

    pruned = skeleton.copy()
    rows, cols = pruned.shape

    def _nbrs(r: int, c: int) -> list[tuple[int, int]]:
        result: list[tuple[int, int]] = []
        for dr in (-1, 0, 1):
            for dc in (-1, 0, 1):
                if dr == 0 and dc == 0:
                    continue
                nr, nc = r + dr, c + dc
                if 0 <= nr < rows and 0 <= nc < cols and pruned[nr, nc]:
                    result.append((nr, nc))
        return result

    changed = True
    while changed:
        changed = False
        ys, xs = np.where(pruned)
        tips = sorted(
            (int(y), int(x)) for y, x in zip(ys, xs, strict=True) if len(_nbrs(y, x)) == 1
        )
        for tip_r, tip_c in tips:
            if not pruned[tip_r, tip_c]:
                continue  # already cleared in this pass
            spur: list[tuple[int, int]] = [(tip_r, tip_c)]
            prev: tuple[int, int] = (tip_r, tip_c)
            while True:
                candidates = [n for n in _nbrs(*spur[-1]) if n != prev]
                if len(candidates) != 1:
                    break  # junction (>1) or dead end (0)
                prev = spur[-1]
                spur.append(candidates[0])
            if len(spur) < min_px:
                for r, c in spur:
                    pruned[r, c] = False
                changed = True
    return pruned


def _skeleton_to_polylines(
    skeleton: Any,  # 2-D bool numpy array
    origin_x: float,
    origin_y: float,
    px_per_du: float,
) -> tuple[tuple[tuple[float, float], ...], ...]:
    """Vectorize a pruned skeleton into DU polylines.

    Uses a greedy chain-trace: starting from the earliest unvisited skeleton
    pixel (sorted by row then col), walks to the sorted-first unvisited
    8-neighbour at each step.  At a junction the walk abandons the other
    branches into separate polylines, so the polyline set is a faithful
    GEOMETRY rendering but its Euclidean sum can under-count vs the true arc
    length. The reported ``length_du`` is therefore measured independently on
    the skeleton mask (``_skeleton_arc_length_px``, orth + sqrt2*diag over all
    branches), NOT from this polyline sum.

    Returns a tuple of polylines (each polyline is a tuple of (x, y) DU coords).
    """
    import numpy as np

    rows, cols = skeleton.shape
    visited = np.zeros_like(skeleton, dtype=bool)

    ys, xs = np.where(skeleton)
    all_pixels = sorted(zip(ys.tolist(), xs.tolist(), strict=True))

    def _px_to_du(r: int, c: int) -> tuple[float, float]:
        return (origin_x + c / px_per_du, origin_y + r / px_per_du)

    def _unvisited_nbrs(r: int, c: int) -> list[tuple[int, int]]:
        result: list[tuple[int, int]] = []
        for dr in (-1, 0, 1):
            for dc in (-1, 0, 1):
                if dr == 0 and dc == 0:
                    continue
                nr, nc = r + dr, c + dc
                if 0 <= nr < rows and 0 <= nc < cols and skeleton[nr, nc] and not visited[nr, nc]:
                    result.append((nr, nc))
        return sorted(result)

    polylines: list[tuple[tuple[float, float], ...]] = []
    for start_r, start_c in all_pixels:
        if visited[start_r, start_c]:
            continue
        # Greedy chain walk: always pick sorted-first unvisited neighbour.
        path: list[tuple[float, float]] = [_px_to_du(start_r, start_c)]
        visited[start_r, start_c] = True
        cur_r, cur_c = start_r, start_c
        while True:
            nbrs = _unvisited_nbrs(cur_r, cur_c)
            if not nbrs:
                break
            cur_r, cur_c = nbrs[0]
            visited[cur_r, cur_c] = True
            path.append(_px_to_du(cur_r, cur_c))
        if len(path) >= 2:
            polylines.append(tuple(path))

    return tuple(polylines)


def _rasterize_group(
    segments: list[tuple[tuple[float, float], tuple[float, float]]],
) -> tuple[Any, float, float] | None:
    """Rasterize segments onto a per-group uint8 bitmap.

    Returns (bitmap, origin_x, origin_y) or None if no segments.
    cv2 must already be imported by the caller.
    """
    import cv2
    import numpy as np

    if not segments:
        return None

    all_xs = [p[0] for seg in segments for p in seg]
    all_ys = [p[1] for seg in segments for p in seg]
    min_x = min(all_xs) - _RASTER_MARGIN_DU
    min_y = min(all_ys) - _RASTER_MARGIN_DU
    max_x = max(all_xs) + _RASTER_MARGIN_DU
    max_y = max(all_ys) + _RASTER_MARGIN_DU

    width_px = max(1, math.ceil((max_x - min_x) * _RASTER_PX_PER_DU))
    height_px = max(1, math.ceil((max_y - min_y) * _RASTER_PX_PER_DU))

    bitmap = np.zeros((height_px, width_px), dtype=np.uint8)

    for (sx, sy), (ex, ey) in segments:
        px0 = round((sx - min_x) * _RASTER_PX_PER_DU)
        py0 = round((sy - min_y) * _RASTER_PX_PER_DU)
        px1 = round((ex - min_x) * _RASTER_PX_PER_DU)
        py1 = round((ey - min_y) * _RASTER_PX_PER_DU)
        cv2.line(bitmap, (px0, py0), (px1, py1), 255, _STROKE_PX)

    return bitmap, min_x, min_y


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------


def pdf_centerlines(
    run_groups: Sequence[RunGroup],
    geometry_by_entity_id: Mapping[str, Mapping[str, Any]],
) -> list[Centerline]:
    """Produce one :class:`Centerline` per group using PDF raster tracing.

    cv2 and skimage are imported here (lazy) so that importing this module
    does not pull heavy vision packages into sys.modules.

    Parameters
    ----------
    run_groups:
        P1 RunGroup objects.  Each group defines a (layer_ref, colour_key) key
        and the set of member entity IDs whose geometries are consumed.
    geometry_by_entity_id:
        Geometry dict keyed by entity ID.  Missing entity IDs contribute 0
        (tolerant).

    Returns
    -------
    list[Centerline]
        One ``Centerline`` per group, in the same order as ``run_groups``.
        ``producer_kind="pdf_raster"``.

    Raises
    ------
    ImportError
        If cv2 or skimage is not installed.  Propagated honestly — do NOT
        catch; a missing extra must cause the job to fail so the read stays
        provisional.
    """
    import cv2
    from skimage.morphology import skeletonize

    results: list[Centerline] = []
    for group in run_groups:
        entity_ids: tuple[str, ...] = tuple(group.entity_ids)
        segments = decompose_geometry(entity_ids, geometry_by_entity_id)

        if not segments:
            results.append(
                Centerline(
                    layer_ref=group.layer_ref,
                    colour_key=group.colour_key,
                    geometry=CenterlineGeometry(polylines=(), length_du=0.0),
                    entity_count=len(entity_ids),
                    algo_version=CURRENT_ALGO_VERSION,
                    raster_params_hash=_PDF_RASTER_PARAMS_HASH,
                    producer_kind="pdf_raster",
                )
            )
            continue

        raster_result = _rasterize_group(segments)
        if raster_result is None:
            results.append(
                Centerline(
                    layer_ref=group.layer_ref,
                    colour_key=group.colour_key,
                    geometry=CenterlineGeometry(polylines=(), length_du=0.0),
                    entity_count=len(entity_ids),
                    algo_version=CURRENT_ALGO_VERSION,
                    raster_params_hash=_PDF_RASTER_PARAMS_HASH,
                    producer_kind="pdf_raster",
                )
            )
            continue

        bitmap, origin_x, origin_y = raster_result

        # Morphological CLOSE to bridge double-line gaps.
        ksize = _kernel_diameter_px(_DEFAULT_DIAMETER_DU, _RASTER_PX_PER_DU)
        kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (ksize, ksize))
        closed = cv2.morphologyEx(bitmap, cv2.MORPH_CLOSE, kernel)

        # Skeletonize.
        skel_bool = skeletonize(closed > 0)  # type: ignore[no-untyped-call]

        # Prune spurs deterministically.
        pruned = _prune_spurs(skel_bool, _PRUNE_MIN_PX)

        # Vectorize to DU polylines via greedy chain-trace (for geometry output).
        polylines = _skeleton_to_polylines(pruned, origin_x, origin_y, _RASTER_PX_PER_DU)

        # Length = skeleton arc length via adjacency-sum on the binary mask.
        # This counts ALL skeleton pixels (including junction branches) whereas
        # the greedy chain-tracer drops branches at junctions, causing ~15-20%
        # under-count.  Arc length in px converted back to DU by dividing by scale.
        length_du = _skeleton_arc_length_px(pruned) / _RASTER_PX_PER_DU

        results.append(
            Centerline(
                layer_ref=group.layer_ref,
                colour_key=group.colour_key,
                geometry=CenterlineGeometry(polylines=polylines, length_du=length_du),
                entity_count=len(entity_ids),
                algo_version=CURRENT_ALGO_VERSION,
                raster_params_hash=_PDF_RASTER_PARAMS_HASH,
                producer_kind="pdf_raster",
            )
        )

    return results
