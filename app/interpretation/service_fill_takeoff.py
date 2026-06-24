"""Per-service routed length attributed by element fill-hatch colour (issue #663, Phase 1).

Attributes the drawing's clean ``Center Line`` geometry to coloured fill bands (from
``Pipes``/``Rise``/``Drop`` HATCH entities) by spatial overlap, producing per-colour-key
metres and a separate "shared/manifold" bucket.

Colour remains an opaque per-drawing key — NO global colour→service-name mapping (that is
Phase 2/3, out of scope here).

**Algorithm (validated by spike; all thresholds in METRES; geometry pre-scaled to metres):**

1. Per-colour fill geometry: for each :class:`FillBand`, build a
   ``shapely.geometry.Polygon`` from its ring, ``.buffer(0)`` to repair, then
   ``.buffer(band_buffer_m)``. Skip rings with <3 distinct points or polygons still
   invalid/empty after ``buffer(0)``. Union all polygons sharing a ``colour_key`` via
   ``shapely.ops.unary_union`` → one geometry per colour_key.

2. For each centerline segment (:class:`shapely.geometry.LineString`): find the
   ``colour_key`` whose union has the GREATEST
   ``segment.intersection(union).length``. If that overlap ≥
   ``overlap_ratio_min * segment_length`` → attribute the full segment length to that
   ``colour_key``.

3. Else fallback: for each ``colour_key`` compute ``segment.distance(union)``. If
   nearest < ``nearest_max_m`` AND nearest is ≥ ``nearest_margin_m`` clearer than
   the second-nearest → attribute to nearest.

4. Else → "shared/manifold" bucket (sentinel, NOT a ``colour_key`` in ``per_colour``).

5. Sum per colour_key + shared. ``per_colour`` sorted by ``colour_key``. INVARIANT:
   Σ(per-colour) + shared == total_length_m within ±0.1 m.
   ``total_length_m`` = sum of all segment lengths.

Pure module — NO DB, ORM, FastAPI, or SQLAlchemy imports.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from shapely.geometry import LineString, Polygon
from shapely.ops import unary_union

# ---------------------------------------------------------------------------
# Public dataclasses (frozen interface contract)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FillBand:
    """One HATCH polygon extracted from a Pipes/Rise/Drop layer, keyed by colour."""

    colour_key: str
    colour_index: int | None
    colour_rgb: str | None
    ring: tuple[tuple[float, float], ...]  # metres


@dataclass(frozen=True, slots=True)
class FillColourLength:
    """Attributed length for a single colour_key."""

    colour_key: str
    colour_index: int | None
    colour_rgb: str | None
    length_m: float


@dataclass(frozen=True, slots=True)
class FillAttributionResult:
    """Result of fill-colour attribution over the full centerline segment set."""

    per_colour: tuple[FillColourLength, ...]  # sorted by colour_key
    shared_length_m: float
    total_length_m: float
    centerline_segment_count: int


# ---------------------------------------------------------------------------
# Internal sentinel
# ---------------------------------------------------------------------------

_SHARED_SENTINEL: str = "__shared__"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _distinct_points(ring: tuple[tuple[float, float], ...]) -> list[tuple[float, float]]:
    """Return unique coordinate pairs from a ring (order-preserving, first-occurrence)."""
    seen: set[tuple[float, float]] = set()
    out: list[tuple[float, float]] = []
    for pt in ring:
        if pt not in seen:
            seen.add(pt)
            out.append(pt)
    return out


def _build_colour_unions(
    fill_bands: Sequence[FillBand],
    band_buffer_m: float,
) -> dict[str, Any]:
    """Build one merged Shapely geometry per colour_key.

    Each band ring is:
    - skipped when it has <3 distinct points
    - repaired via ``.buffer(0)``; skipped when still invalid or empty after repair
    - grown by ``band_buffer_m`` so nearby segments intersect cleanly
    All bands for the same ``colour_key`` are unioned.
    """
    # colour_key → list of per-band geometries
    band_geoms: dict[str, list[Any]] = {}
    for band in fill_bands:
        distinct = _distinct_points(band.ring)
        if len(distinct) < 3:
            continue
        try:
            poly = Polygon(band.ring).buffer(0)
        except Exception:
            continue
        if not poly.is_valid or poly.is_empty:
            continue
        try:
            grown = poly.buffer(band_buffer_m)
        except Exception:
            continue
        if grown.is_empty:
            continue
        band_geoms.setdefault(band.colour_key, []).append(grown)

    unions: dict[str, Any] = {}
    for ck, geoms in band_geoms.items():
        try:
            unions[ck] = unary_union(geoms)
        except Exception:
            continue
    return unions


# ---------------------------------------------------------------------------
# Internal per-segment classifier
# ---------------------------------------------------------------------------


def _segment_verdicts(
    *,
    centerline_segments: Sequence[tuple[tuple[float, float], tuple[float, float]]],
    fill_bands: Sequence[FillBand],
    overlap_ratio_min: float,
    nearest_max_m: float,
    nearest_margin_m: float,
    band_buffer_m: float,
) -> list[tuple[float, str | None]]:
    """One (seg_length_m, colour_key | None) per NON-degenerate segment, input order.

    None == shared. Exactly the current overlap->nearest->single-colour->shared logic.
    Zero-length segments skipped (excluded from output and total), matching current behaviour.
    """
    colour_unions = _build_colour_unions(fill_bands, band_buffer_m)
    colour_keys = sorted(colour_unions)  # deterministic order

    verdicts: list[tuple[float, str | None]] = []

    for start, end in centerline_segments:
        seg = LineString([start, end])
        seg_len = seg.length

        if seg_len == 0.0:
            # Zero-length degenerate segment — skip; excluded from output entirely.
            continue

        if not colour_keys:
            # No fill bands at all — shared.
            verdicts.append((seg_len, None))
            continue

        # --- Overlap phase ---
        overlap_lengths: dict[str, float] = {}
        for ck in colour_keys:
            try:
                inter = seg.intersection(colour_unions[ck])
                ol = inter.length if not inter.is_empty else 0.0
            except Exception:
                ol = 0.0
            overlap_lengths[ck] = ol

        best_ck = max(colour_keys, key=lambda k: overlap_lengths[k])
        best_overlap = overlap_lengths[best_ck]

        if best_overlap >= overlap_ratio_min * seg_len:
            verdicts.append((seg_len, best_ck))
            continue

        # --- Nearest-band fallback ---
        distances: dict[str, float] = {}
        for ck in colour_keys:
            try:
                d = seg.distance(colour_unions[ck])
            except Exception:
                d = math.inf
            distances[ck] = d

        sorted_by_dist = sorted(colour_keys, key=lambda k: distances[k])
        nearest_ck = sorted_by_dist[0]
        nearest_dist = distances[nearest_ck]

        if (
            nearest_dist < nearest_max_m
            and len(sorted_by_dist) >= 2
            and (distances[sorted_by_dist[1]] - nearest_dist) >= nearest_margin_m
        ):
            verdicts.append((seg_len, nearest_ck))
            continue

        # Single-colour fallback: when only one colour exists there is no competing service,
        # so the ambiguity-margin check is intentionally waived — attribution to the sole
        # colour is unambiguous by definition.
        if nearest_dist < nearest_max_m and len(sorted_by_dist) == 1:
            verdicts.append((seg_len, nearest_ck))
            continue

        # --- Shared/manifold ---
        verdicts.append((seg_len, None))

    return verdicts


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_fill_attributed_lengths(
    *,
    centerline_segments: Sequence[tuple[tuple[float, float], tuple[float, float]]],
    fill_bands: Sequence[FillBand],
    overlap_ratio_min: float = 0.30,
    nearest_max_m: float = 0.100,
    nearest_margin_m: float = 0.020,
    band_buffer_m: float = 0.011,
) -> FillAttributionResult:
    """Attribute centerline segment lengths to fill-colour bands.

    Geometry is expected pre-scaled to metres (conversion_factor=1.0; do NOT re-scale).

    ``nearest_max_m=0.100`` (100 mm): tight-corner connector segments at pipe elbows can
    sit 90-96 mm from their fill band and must still attribute to that service.  The
    ambiguity-margin guard (``nearest_margin_m=0.020``) independently protects the
    manifold core — segments equidistant from two bands stay in the shared bucket
    regardless of how wide the radius is, so widening to 100 mm cannot swallow genuine
    shared/manifold runs.

    Empty inputs return an empty result without raising.
    INVARIANT: Σ(per_colour lengths) + shared_length_m == total_length_m within ±0.1 m.
    """
    # --- Edge case: empty inputs ---
    if not centerline_segments:
        return FillAttributionResult(
            per_colour=(),
            shared_length_m=0.0,
            total_length_m=0.0,
            centerline_segment_count=0,
        )

    # --- Per-segment classification via shared helper ---
    verdicts = _segment_verdicts(
        centerline_segments=centerline_segments,
        fill_bands=fill_bands,
        overlap_ratio_min=overlap_ratio_min,
        nearest_max_m=nearest_max_m,
        nearest_margin_m=nearest_margin_m,
        band_buffer_m=band_buffer_m,
    )

    # --- Tally verdicts ---
    length_by_colour: dict[str, float] = {}
    shared_length: float = 0.0
    total_length: float = 0.0

    for seg_len, colour_key in verdicts:
        total_length += seg_len
        if colour_key is None:
            shared_length += seg_len
        else:
            length_by_colour[colour_key] = length_by_colour.get(colour_key, 0.0) + seg_len

    # --- Gather metadata (colour_index, colour_rgb) for each attributed colour_key ---
    # Prefer the first band we encounter for each key (bands are per-drawing, arbitrary order).
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
        centerline_segment_count=len(centerline_segments),
    )
