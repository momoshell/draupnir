"""Run-graph manifold attribution via connectivity (issue #667, PR-1).

Refines the output of ``compute_fill_attributed_lengths`` by attributing some
``__shared__``/manifold segments to a real service using run-graph connectivity.
Whatever stays ambiguous after a single conservative pass remains in the shared
bucket — no iteration to fixpoint (that is a later PR).

**Algorithm (CONSERVATIVE, single pass):**

1. Obtain per-segment verdicts via ``_segment_verdicts``.  If no definite colour
   appears anywhere, return the base-fn result unchanged.
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
# Public API
# ---------------------------------------------------------------------------


def refine_shared_by_connectivity(
    *,
    centerline_segments: Sequence[tuple[tuple[float, float], tuple[float, float]]],
    fill_bands: Sequence[FillBand],
    fitting_bands: Sequence[FillBand] = (),
    snap_tol_m: float = 0.030,
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
    total_length = sum(seg_len for seg_len, _ in verdicts)
    seg_count = len(centerline_segments)

    # No-op conditions: nothing to refine.
    if not verdicts or not any(ck is not None for _, ck in verdicts):
        return _build_result(verdicts, fill_bands, total_length, seg_count)

    # --- Step 2: build run-graph ---
    # node_id → sorted list of segment indices (into verdicts / nondegen_segs)
    node_to_segs: dict[tuple[int, int], list[int]] = {}

    def _snap(coord: tuple[float, float]) -> tuple[int, int]:
        return (round(coord[0] / snap_tol_m), round(coord[1] / snap_tol_m))

    for idx, (start, end) in enumerate(nondegen_segs):
        for node_id in (_snap(start), _snap(end)):
            node_to_segs.setdefault(node_id, []).append(idx)

    # Sort adjacency lists for determinism.
    for node_id in node_to_segs:
        node_to_segs[node_id].sort()

    # Build neighbour index: segment → set of neighbouring segment indices.
    seg_neighbours: list[set[int]] = [set() for _ in verdicts]
    for _node_id, indices in node_to_segs.items():
        for i in indices:
            for j in indices:
                if i != j:
                    seg_neighbours[i].add(j)

    # --- Step 3: single-pass connectivity refinement ---
    # Frozen snapshot: connectivity lookups use original verdicts only.
    frozen_colours: list[str | None] = [ck for _, ck in verdicts]

    # Refined verdicts — start as a mutable copy; we write into this, not frozen_colours.
    refined: list[str | None] = list(frozen_colours)

    # Pre-compute unit directions for collinearity checks.
    unit_dirs: list[tuple[float, float] | None] = []
    for start, end in nondegen_segs:
        dx = end[0] - start[0]
        dy = end[1] - start[1]
        length = math.hypot(dx, dy)
        if length > 0.0:
            unit_dirs.append((dx / length, dy / length))
        else:
            unit_dirs.append(None)  # pragma: no cover  (zero-len excluded above)

    for i in sorted(range(len(verdicts))):
        seg_len, original_ck = verdicts[i]
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

                seg_start, seg_end = nondegen_segs[i]

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
    refined_verdicts = [(verdicts[i][0], refined[i]) for i in range(len(verdicts))]
    # Metadata for _build_result needs both fill and fitting bands so all colour_keys
    # present in the output can resolve their colour_index / colour_rgb.
    all_bands: list[FillBand] = list(fill_bands) + list(fitting_bands)
    return _build_result(refined_verdicts, all_bands, total_length, seg_count)


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
