"""Per-segment nearest-label type attribution for colour-uniform containment (issue #687).

Attributes each synthesized centerline SEGMENT to its nearest parsed size-label, producing
per-(service) and per-(service, size) length totals. Segments farther than ``nearest_max_m``
from every label land in the ``unknown_length_m`` bucket.

**Algorithm (spike-verified):**

For each polyline, decompose into consecutive-point segments. For each segment:

1. Compute the midpoint (mean of endpoints) and the segment length via ``math.hypot``.
2. Find the NEAREST :class:`SegmentLabel` by Euclidean distance from midpoint to label.point.
3. If min distance ≤ ``nearest_max_m`` → add the segment length to that label's
   (service) bucket and (service, size_raw, size_kind) bucket.
4. Else → ``unknown_length_m``.

Tie-break: smallest distance (epsilon-stable — labels within ``_EQ_EPSILON`` metres of each
other are treated as tied), then lowest ``(service, size_raw, size_kind)`` lexicographically.
This makes winner selection order-independent of the ``labels`` input list.

**Invariants:**

- ``Σ(per_service lengths) + unknown_length_m == total_length_m`` within ±0.1 m.
- ``Σ(per_size lengths for a service) == per_service length for that service``.
- ``per_service`` sorted by ``service``; ``per_size`` sorted by ``(service, size_raw, size_kind)``.
- Deterministic (tie-break is fully ordered and label-order-independent).

Pure module — NO DB, ORM, FastAPI, SQLAlchemy, cv2, skimage, or shapely.
All distances are Euclidean in whatever units the caller supplies (expects metres).
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Public dataclasses (frozen interface contract)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SegmentLabel:
    """A parsed size-label with its world-coordinate anchor and service identity."""

    point: tuple[float, float]
    service: str
    size_raw: str | None
    size_kind: str | None


@dataclass(frozen=True, slots=True)
class SegmentServiceLength:
    """Total attributed length for a single service."""

    service: str
    length_m: float


@dataclass(frozen=True, slots=True)
class SegmentSizeLength:
    """Total attributed length for a single (service, size_raw, size_kind) bucket."""

    service: str
    size_raw: str | None
    size_kind: str | None
    length_m: float


@dataclass(frozen=True, slots=True)
class SegmentLabelResult:
    """Result of per-segment nearest-label attribution over a full set of centerline polylines."""

    per_service: tuple[SegmentServiceLength, ...]  # sorted by service
    per_size: tuple[SegmentSizeLength, ...]  # sorted by (service, size_raw)
    unknown_length_m: float
    total_length_m: float
    segment_count: int


# ---------------------------------------------------------------------------
# Internal constants
# ---------------------------------------------------------------------------

# Epsilon for float distance comparisons — labels within this many metres of each other
# are treated as tied and resolved lexicographically, matching run_service_identity precedent.
_EQ_EPSILON: float = 1e-9


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _midpoint(a: tuple[float, float], b: tuple[float, float]) -> tuple[float, float]:
    return ((a[0] + b[0]) * 0.5, (a[1] + b[1]) * 0.5)


def _dist(p: tuple[float, float], q: tuple[float, float]) -> float:
    return math.hypot(p[0] - q[0], p[1] - q[1])


def _label_lex_key(label: SegmentLabel) -> tuple[str, str, str]:
    """Lexicographic tie-break key: (service, size_raw or '', size_kind or '')."""
    return (label.service, label.size_raw or "", label.size_kind or "")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_segment_label_lengths(
    *,
    centerline_polylines: Sequence[Sequence[tuple[float, float]]],
    labels: Sequence[SegmentLabel],
    nearest_max_m: float = 5.0,
) -> SegmentLabelResult:
    """Attribute centerline segment lengths to nearest parsed size-labels.

    Geometry is expected pre-scaled to metres (conversion_factor=1.0; do NOT re-scale).

    ``nearest_max_m=5.0``: calibrated across E-610003 (labels ≤2.2 m) and M-540003
    (callouts at 3.85 m); plateau-stable 5-7 m; junk ≥16 m.

    Empty inputs return an empty result without raising.
    INVARIANT: Σ(per_service lengths) + unknown_length_m == total_length_m within ±0.1 m.
    """
    if not centerline_polylines:
        return SegmentLabelResult(
            per_service=(),
            per_size=(),
            unknown_length_m=0.0,
            total_length_m=0.0,
            segment_count=0,
        )

    # Accumulate lengths per service and per (service, size_raw, size_kind).
    service_lengths: dict[str, float] = {}
    size_lengths: dict[tuple[str, str | None, str | None], float] = {}
    unknown_length: float = 0.0
    total_length: float = 0.0
    segment_count: int = 0

    for polyline in centerline_polylines:
        pts = list(polyline)
        if len(pts) < 2:
            continue
        for i in range(len(pts) - 1):
            a = pts[i]
            b = pts[i + 1]
            seg_len = math.hypot(b[0] - a[0], b[1] - a[1])
            # Zero-length degenerate segments are still counted but contribute nothing.
            segment_count += 1
            if seg_len == 0.0:
                continue
            total_length += seg_len
            mid = _midpoint(a, b)

            if not labels:
                unknown_length += seg_len
                continue

            # Find nearest label by midpoint-to-point euclidean distance.
            # Epsilon-stable: labels within _EQ_EPSILON metres are tied; lexicographic
            # key (service, size_raw, size_kind) breaks ties order-independently.
            best_label: SegmentLabel | None = None
            best_dist: float = math.inf

            for lbl in labels:
                d = _dist(mid, lbl.point)
                if d < best_dist - _EQ_EPSILON:
                    # Clearly closer — new best unconditionally.
                    best_dist = d
                    best_label = lbl
                elif abs(d - best_dist) <= _EQ_EPSILON and best_label is not None:
                    # Tied within epsilon — pick lexicographically lower label.
                    if _label_lex_key(lbl) < _label_lex_key(best_label):
                        best_dist = d
                        best_label = lbl

            if best_label is not None and best_dist <= nearest_max_m:
                svc = best_label.service
                service_lengths[svc] = service_lengths.get(svc, 0.0) + seg_len
                size_key = (svc, best_label.size_raw, best_label.size_kind)
                size_lengths[size_key] = size_lengths.get(size_key, 0.0) + seg_len
            else:
                unknown_length += seg_len

    per_service = tuple(
        SegmentServiceLength(service=svc, length_m=round(length, 6))
        for svc, length in sorted(service_lengths.items())
    )

    per_size = tuple(
        SegmentSizeLength(service=svc, size_raw=sr, size_kind=sk, length_m=round(length, 6))
        for (svc, sr, sk), length in sorted(
            size_lengths.items(),
            key=lambda item: (item[0][0], item[0][1] or "", item[0][2] or ""),
        )
    )

    return SegmentLabelResult(
        per_service=per_service,
        per_size=per_size,
        unknown_length_m=round(unknown_length, 6),
        total_length_m=round(total_length, 6),
        segment_count=segment_count,
    )
