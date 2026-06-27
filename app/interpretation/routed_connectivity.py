"""Run-graph manifold attribution via connectivity (issue #667, PR-1).

Refines the output of ``compute_fill_attributed_lengths`` by attributing some
``__shared__``/manifold segments to a real service using run-graph connectivity.
Whatever stays ambiguous after a single conservative pass remains in the shared
bucket — no iteration to fixpoint (that is a later PR).

**Algorithm (CONSERVATIVE, single pass):**

1. Obtain per-segment verdicts via ``_segment_verdicts``.  If no definite colour
   appears anywhere, return the base-fn result unchanged.
1.5 Mid-span tee split (#669): for each node that lies in the interior of an edge,
   split that edge at the projection point so the run-graph sees the T-junction as
   a real connectivity. Each sub-segment inherits the parent verdict verbatim (D6).
2. Build a run-graph: snap each endpoint to a ``snap_tol_m`` grid, derive a node id
   from the integer pair, record which segment indices touch each node.
3. For each shared segment (ascending index), check neighbour colours from a FROZEN
   snapshot of the original verdicts (single-pass — refined neighbours do not
   retroactively change other lookups).  Refine shared→C iff exactly one distinct
   neighbour colour exists AND (near-collinear OR short connector).
4. Re-tally refined verdicts; rebuild per_colour; conserve total_length_m and
   centerline_segment_count.

Pure module — NO DB, ORM, FastAPI, SQLAlchemy, cv2, or skimage imports.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry

from app.interpretation.service_fill_takeoff import (
    FillAttributionResult,
    FillBand,
    FillColourLength,
    _build_colour_unions,
    _segment_verdicts,
)

# ---------------------------------------------------------------------------
# Internal split record
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _SplitSeg:
    """A sub-segment produced by the mid-span tee-split pass (or the unsplit parent).

    Fields
    ------
    start, end      : real-space coordinates in metres.
    start_node, end_node : integer grid-cell ids (same space as _snap).
    length          : geometric length (metres); Σ sub-lengths == parent length exactly.
    colour_key      : inherited verbatim from the parent segment verdict (D6 — never
                      re-classified against the sub-geometry).
    """

    start: tuple[float, float]
    end: tuple[float, float]
    start_node: tuple[int, int]
    end_node: tuple[int, int]
    length: float
    colour_key: str | None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def refine_shared_by_connectivity(
    *,
    centerline_segments: Sequence[tuple[tuple[float, float], tuple[float, float]]],
    fill_bands: Sequence[FillBand],
    fitting_bands: Sequence[FillBand] = (),
    snap_tol_m: float = 0.030,
    tee_tol_m: float = 0.030,
    connector_len_m: float = 0.15,
    collinear_dot_min: float = 0.98,
    overlap_ratio_min: float = 0.30,
    nearest_max_m: float = 0.100,
    nearest_margin_m: float = 0.020,
    band_buffer_m: float = 0.011,
) -> FillAttributionResult:
    """Refine shared/manifold segments to a definite colour via run-graph connectivity.

    Parameters mirror ``compute_fill_attributed_lengths`` plus connectivity knobs:

    - ``snap_tol_m``: grid cell size for endpoint snapping (node identity).
    - ``tee_tol_m``: perpendicular-distance tolerance for mid-span tee split (#669).
      Default 30 mm (D1). Pure distance gate only (OD-1: no angle gate in v1).
    - ``connector_len_m``: segments at or below this length qualify via the short-connector
      gate regardless of direction.
    - ``collinear_dot_min``: absolute dot-product threshold for the collinearity gate.

    Returns a ``FillAttributionResult`` with identical ``total_length_m`` and
    ``centerline_segment_count`` to the base fn; ``per_colour`` and ``shared_length_m``
    reflect the single-pass connectivity refinement and any fitting-bridge attribution.

    Empty or no-colour inputs return the base fn result unchanged.
    Empty ``fitting_bands`` produces output byte-identical to #667-only (regression guard).
    INVARIANT: Σ(per_colour) + shared_length_m == total_length_m within ±0.1 m.
    """
    # --- Step 1: per-segment verdicts (same thresholds as base fn) ---
    verdicts = _segment_verdicts(
        centerline_segments=centerline_segments,
        fill_bands=fill_bands,
        overlap_ratio_min=overlap_ratio_min,
        nearest_max_m=nearest_max_m,
        nearest_margin_m=nearest_margin_m,
        band_buffer_m=band_buffer_m,
    )

    # Non-degenerate segments only (verdicts is already filtered).
    # Build parallel list of non-degenerate segments aligned to verdicts.
    nondegen_segs: list[tuple[tuple[float, float], tuple[float, float]]] = []
    for start, end in centerline_segments:
        dx = end[0] - start[0]
        dy = end[1] - start[1]
        seg_len = math.hypot(dx, dy)
        if seg_len == 0.0:
            continue
        nondegen_segs.append((start, end))

    # Total and segment count always mirror the base fn (unaffected by connectivity).
    # (total_length sourced from verdicts for the no-op gate below; post-split total
    # is recomputed after Step 1.5 and passed to _build_result.)
    total_length = sum(seg_len for seg_len, _ in verdicts)
    # centerline_segment_count stays len(centerline_segments) (OD-2): post-split
    # cardinality intentionally diverges from the input count — splits are an internal
    # graph detail, not new measured segments.
    seg_count = len(centerline_segments)

    # No-op conditions: nothing to refine.
    if not verdicts or not any(ck is not None for _, ck in verdicts):
        return _build_result(verdicts, fill_bands, total_length, seg_count)

    # --- Step 1.5: mid-span tee split (#669) ---
    # For each interior node (a node that is NOT an endpoint of an edge), project onto
    # the edge's line; if the projection is strictly interior and within tee_tol_m
    # perpendicular distance, split the edge at that point.  Sub-segments inherit the
    # parent's verdict verbatim (D6).  SINGLE PASS — no fixpoint / no new vertices
    # seeded as tee sources.

    def _snap(coord: tuple[float, float]) -> tuple[int, int]:
        return (round(coord[0] / snap_tol_m), round(coord[1] / snap_tol_m))

    # Build node set and cell-center coordinates.
    # Resolution #1: representative coord = cell-center (nid[0]*snap_tol_m, nid[1]*snap_tol_m),
    # which is permutation-invariant by construction.
    node_coord: dict[tuple[int, int], tuple[float, float]] = {}
    for start, end in nondegen_segs:
        for pt in (start, end):
            nid = _snap(pt)
            if nid not in node_coord:
                node_coord[nid] = (nid[0] * snap_tol_m, nid[1] * snap_tol_m)

    eps = snap_tol_m

    # Collect split points per edge: edge_splits[i] = list of (t, px, py, nid).
    edge_splits: list[list[tuple[float, float, float, tuple[int, int]]]] = [
        [] for _ in nondegen_segs
    ]

    for i, (seg_start, seg_end) in enumerate(nondegen_segs):
        dx = seg_end[0] - seg_start[0]
        dy = seg_end[1] - seg_start[1]
        seg_len = math.hypot(dx, dy)
        l2 = dx * dx + dy * dy  # > 0 (zero-length excluded above)
        snap_start = _snap(seg_start)
        snap_end = _snap(seg_end)

        for nid, c in node_coord.items():
            # Skip nodes that are endpoints of this edge (split-on-endpoint guard).
            if nid in (snap_start, snap_end):
                continue

            # Parametric projection of cell-center c onto the line through seg_start→seg_end.
            t = ((c[0] - seg_start[0]) * dx + (c[1] - seg_start[1]) * dy) / l2

            # Must be strictly interior.
            if t <= 0.0 or t >= 1.0:
                continue

            # Arc-length clearance from BOTH endpoints (must be > snap_tol_m).
            arc_from_start = t * seg_len
            arc_from_end = (1.0 - t) * seg_len
            if arc_from_start <= eps or arc_from_end <= eps:
                continue

            # Foot of perpendicular.
            px = seg_start[0] + t * dx
            py = seg_start[1] + t * dy
            perp = math.hypot(c[0] - px, c[1] - py)

            if perp <= tee_tol_m:
                edge_splits[i].append((t, px, py, nid))

    # Build post_split: one _SplitSeg per sub-segment (or one per unsplit parent).
    post_split: list[_SplitSeg] = []

    for i, (seg_start, seg_end) in enumerate(nondegen_segs):
        parent_colour = verdicts[i][1]
        dx = seg_end[0] - seg_start[0]
        dy = seg_end[1] - seg_start[1]
        seg_len = math.hypot(dx, dy)
        snap_start = _snap(seg_start)
        snap_end = _snap(seg_end)

        splits = edge_splits[i]
        if not splits:
            # No qualifying split — emit single record identical to parent.
            post_split.append(
                _SplitSeg(
                    start=seg_start,
                    end=seg_end,
                    start_node=snap_start,
                    end_node=snap_end,
                    length=seg_len,
                    colour_key=parent_colour,
                )
            )
            continue

        # Sort split points by (t, x, y) for determinism (D3).
        splits.sort(key=lambda s: (s[0], s[1], s[2]))

        # Dedupe within snap_tol_m arc-length: walk sorted list; collapse a split point
        # whose t*seg_len is within eps of the previously KEPT point's projection.
        # First-in-sorted-order wins its nid (edge case 2 — coincident-with-third-node merge).
        kept: list[tuple[float, float, float, tuple[int, int]]] = []
        for sp in splits:
            if not kept:
                kept.append(sp)
            else:
                prev = kept[-1]
                if abs(sp[0] * seg_len - prev[0] * seg_len) < eps:
                    # Too close to the last kept point — merge; first-in wins, discard this.
                    pass
                else:
                    kept.append(sp)

        # Build sub-segments from ordered cut params [0, t1, t2, ..., 1].
        # Outer vertices use the parent's original start/end with their _snap node ids.
        # Cut vertices Pk = start + tk * d are on the line (=> Σ sub-lengths == parent length).
        cut_params = kept  # list of (t, px, py, nid) in ascending-t order

        # Assemble boundary points: (t, px, py, nid) including the two endpoints.
        boundaries: list[tuple[float, float, float, tuple[int, int]]] = []
        boundaries.append((0.0, seg_start[0], seg_start[1], snap_start))
        boundaries.extend(cut_params)
        boundaries.append((1.0, seg_end[0], seg_end[1], snap_end))

        for k in range(len(boundaries) - 1):
            bk = boundaries[k]
            bk1 = boundaries[k + 1]
            sub_start = (bk[1], bk[2])
            sub_end = (bk1[1], bk1[2])
            sub_len = math.hypot(sub_end[0] - sub_start[0], sub_end[1] - sub_start[1])

            # Defensive: drop any sub-segment with length < eps.
            # (The dedupe above should already prevent this, but guard defensively.)
            if sub_len < eps:
                continue

            post_split.append(
                _SplitSeg(
                    start=sub_start,
                    end=sub_end,
                    start_node=bk[3],
                    end_node=bk1[3],
                    length=sub_len,
                    colour_key=parent_colour,
                )
            )

    # Post-split total: sourced from post_split for _build_result (D5 — length conservation).
    # This equals total_length within floating-point precision since splits are length-conserving.
    post_split_total = sum(rec.length for rec in post_split)

    # --- Step 2: build run-graph from post_split ---
    # node_id → sorted list of segment indices into post_split.
    # Node ids are carried explicitly in _SplitSeg.start_node / .end_node.
    # _snap is NOT called here (Approach A — explicit node ids).
    node_to_segs: dict[tuple[int, int], list[int]] = {}

    for idx, rec in enumerate(post_split):
        for node_id in (rec.start_node, rec.end_node):
            node_to_segs.setdefault(node_id, []).append(idx)

    # Sort adjacency lists for determinism.
    for node_id in node_to_segs:
        node_to_segs[node_id].sort()

    # Build neighbour index: segment → set of neighbouring segment indices.
    seg_neighbours: list[set[int]] = [set() for _ in post_split]
    for _node_id, indices in node_to_segs.items():
        for i in indices:
            for j in indices:
                if i != j:
                    seg_neighbours[i].add(j)

    # --- Step 3: single-pass connectivity refinement ---
    # Frozen snapshot: connectivity lookups use original verdicts only.
    frozen_colours: list[str | None] = [rec.colour_key for rec in post_split]

    # Refined verdicts — start as a mutable copy; we write into this, not frozen_colours.
    refined: list[str | None] = list(frozen_colours)

    # Pre-compute unit directions for collinearity checks.
    unit_dirs: list[tuple[float, float] | None] = []
    for rec in post_split:
        dx = rec.end[0] - rec.start[0]
        dy = rec.end[1] - rec.start[1]
        length = math.hypot(dx, dy)
        if length > 0.0:
            unit_dirs.append((dx / length, dy / length))
        else:
            unit_dirs.append(None)  # pragma: no cover  (zero-len excluded above)

    for i in sorted(range(len(post_split))):
        seg_len = post_split[i].length
        original_ck = post_split[i].colour_key
        if original_ck is not None:
            # Already attributed — not shared; skip.
            continue

        neighbours = seg_neighbours[i]
        # Distinct definite colours from FROZEN snapshot only.
        neighbour_colours: set[str] = {
            ck for j in neighbours if (ck := frozen_colours[j]) is not None
        }

        if len(neighbour_colours) != 1:
            # Ambiguous (0 or ≥2 distinct colours) — stays shared.
            continue

        sole_colour = next(iter(neighbour_colours))

        # Gate: short connector OR near-collinear with at least one same-colour neighbour.
        qualifies = seg_len <= connector_len_m

        if not qualifies:
            u_i = unit_dirs[i]
            if u_i is not None:
                for j in neighbours:
                    if frozen_colours[j] != sole_colour:
                        continue
                    u_j = unit_dirs[j]
                    if u_j is None:
                        continue
                    dot = abs(u_i[0] * u_j[0] + u_i[1] * u_j[1])
                    if dot >= collinear_dot_min:
                        qualifies = True
                        break

        if qualifies:
            refined[i] = sole_colour

    # --- Step 4: fitting-bridge pass (#668) ---
    # Single frozen pass over still-shared segments; a segment whose endpoint(s) fall inside
    # exactly ONE fitting-colour grown union inherits that colour.  Two endpoints agreeing on
    # the same colour → inherit; disagreeing OR ≥2 colours at either endpoint → stays shared.
    # Empty fitting_bands → this block is a no-op (regression-safe vs #667-only).
    if fitting_bands:
        # band_buffer_m=0.011 per OD-3 (provisional; reopen on a 2nd sheet).
        fitting_unions = _build_colour_unions(fitting_bands, band_buffer_m=0.011)

        if fitting_unions:
            # Cast to typed dict so mypy resolves .contains() via BaseGeometry.
            typed_unions: dict[str, BaseGeometry] = dict(fitting_unions.items())

            def _colours_at_point(
                pt: tuple[float, float],
                unions: dict[str, BaseGeometry] = typed_unions,
            ) -> set[str]:
                """Return fitting colour_keys whose grown union contains pt."""
                shapely_pt = Point(pt[0], pt[1])
                return {ck for ck, geom in unions.items() if geom.contains(shapely_pt)}

            # Frozen snapshot of the post-#667 refined labels (single pass; no fixpoint).
            fitting_frozen: list[str | None] = list(refined)

            for i in range(len(refined)):
                if fitting_frozen[i] is not None:
                    # Already attributed by base fn or #667 pass — skip.
                    continue

                seg_start, seg_end = post_split[i].start, post_split[i].end

                colours_at_start = _colours_at_point(seg_start)
                colours_at_end = _colours_at_point(seg_end)

                # Candidate: union of colours seen at either endpoint.
                candidate_colours = colours_at_start | colours_at_end

                if len(candidate_colours) == 0:
                    # Neither endpoint near any fitting — stays shared.
                    continue

                if len(candidate_colours) >= 2:
                    # Ambiguous overlap (≥2 distinct fitting colours) — stays shared.
                    continue

                # Exactly one fitting colour touches this segment's endpoints.
                sole_fitting_colour = next(iter(candidate_colours))

                # Safety: if the two endpoints disagree (one sees the colour, the other sees
                # a different one via a separate union overlapping at that end) we've already
                # filtered above via candidate_colours >= 2.  The remaining case is that one
                # or both endpoints confirm the single colour — safe to inherit.
                refined[i] = sole_fitting_colour

    # --- Step 5: re-tally ---
    # refined_verdicts sourced from post_split (D5 — length conservation via post_split_total).
    refined_verdicts = [(post_split[i].length, refined[i]) for i in range(len(post_split))]
    # Metadata for _build_result needs both fill and fitting bands so all colour_keys
    # present in the output can resolve their colour_index / colour_rgb.
    all_bands: list[FillBand] = list(fill_bands) + list(fitting_bands)
    return _build_result(refined_verdicts, all_bands, post_split_total, seg_count)


# ---------------------------------------------------------------------------
# Internal tally helper
# ---------------------------------------------------------------------------


def _build_result(
    verdicts: list[tuple[float, str | None]],
    fill_bands: Sequence[FillBand],
    total_length: float,
    seg_count: int,
) -> FillAttributionResult:
    """Tally (seg_len, colour_key|None) verdicts into a FillAttributionResult."""
    length_by_colour: dict[str, float] = {}
    shared_length: float = 0.0

    for seg_len, ck in verdicts:
        if ck is None:
            shared_length += seg_len
        else:
            length_by_colour[ck] = length_by_colour.get(ck, 0.0) + seg_len

    # Metadata: first-occurrence per colour_key (same as base fn).
    meta: dict[str, tuple[int | None, str | None]] = {}
    for band in fill_bands:
        if band.colour_key not in meta:
            meta[band.colour_key] = (band.colour_index, band.colour_rgb)

    per_colour: tuple[FillColourLength, ...] = tuple(
        FillColourLength(
            colour_key=ck,
            colour_index=meta.get(ck, (None, None))[0],
            colour_rgb=meta.get(ck, (None, None))[1],
            length_m=round(length_by_colour[ck], 6),
        )
        for ck in sorted(length_by_colour)
    )

    return FillAttributionResult(
        per_colour=per_colour,
        shared_length_m=round(shared_length, 6),
        total_length_m=round(total_length, 6),
        centerline_segment_count=seg_count,
    )
