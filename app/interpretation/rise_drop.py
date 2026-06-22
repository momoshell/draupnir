"""Rise/drop symbol clustering for mechanical services drawings (issue #619).

Clusters ARC and HATCH entities that represent riser/drop symbols into distinct
symbol records by shared anchor point, then assigns discipline via the
:class:`~app.interpretation.service_legend.ServiceLegend` colour reducer.

Vertical length is intentionally absent: rise/drop layers carry zero text or
annotation on real sheets (the symbol is purely a graphic device -- concentric
arcs or a hatched circle).  No dimension data exists to extract; never guess.

Pure module -- NO DB, ORM, FastAPI, or SQLAlchemy imports.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from statistics import mean
from typing import Any

from app.interpretation.service_legend import ServiceLegend, colour_key

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

KIND_RISE = "rise"
KIND_DROP = "drop"
STATUS_RESOLVED = "resolved"
STATUS_UNKNOWN = "unknown"

_DEFAULT_ANCHOR_EPSILON: float = 1.0

# ---------------------------------------------------------------------------
# Input / output dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RiseDropEntity:
    """One ARC or HATCH entity that may be part of a rise/drop symbol."""

    entity_id: str
    entity_type: str  # "arc" | "hatch"
    layer_ref: str | None
    color: Mapping[str, Any] | None
    geometry: Mapping[str, Any] | None


@dataclass(frozen=True, slots=True)
class RiseDropSymbol:
    """One resolved rise or drop symbol (a cluster of co-located entities)."""

    kind: str  # KIND_RISE | KIND_DROP
    anchor_point: tuple[float, float]
    discipline: str | None
    colour_key: str | None
    entity_ids: tuple[str, ...]  # sorted
    status: str  # STATUS_RESOLVED | STATUS_UNKNOWN


@dataclass(frozen=True, slots=True)
class RiseDropResult:
    """Container returned by :func:`cluster_rise_drop_symbols`."""

    symbols: tuple[RiseDropSymbol, ...]


# ---------------------------------------------------------------------------
# Anchor extraction
# ---------------------------------------------------------------------------


def _symbol_anchor(geometry: Mapping[str, Any] | None) -> tuple[float, float] | None:
    """Return the canonical anchor point from entity geometry; None for degenerate input.

    ARC: ``geometry["center"]`` is a dict ``{"x", "y", "z"}``; returns ``(x, y)``.
    HATCH: ``geometry["vertices"]`` is a list of dicts ``{"x", "y", "z"}``; returns
    the centroid ``(mean(x), mean(y))``.  Falls back to bbox centre when vertices are
    absent or empty.  Never raises.
    """
    if geometry is None:
        return None
    try:
        # ARC path -- center dict.
        center = geometry.get("center")
        if isinstance(center, Mapping):
            x = center.get("x")
            y = center.get("y")
            if isinstance(x, (int, float)) and isinstance(y, (int, float)):
                return (float(x), float(y))

        # HATCH path -- vertex list of dicts.
        vertices = geometry.get("vertices")
        if isinstance(vertices, Sequence) and not isinstance(vertices, (str, bytes)):
            pts: list[tuple[float, float]] = [
                (float(v.get("x")), float(v.get("y")))  # type: ignore[arg-type]
                for v in vertices
                if isinstance(v, Mapping)
                and isinstance(v.get("x"), (int, float))
                and isinstance(v.get("y"), (int, float))
            ]
            if pts:
                return (mean(x for x, _ in pts), mean(y for _, y in pts))

        # Fallback: bbox centre.
        bbox = geometry.get("bbox")
        if isinstance(bbox, Mapping):
            bmin = bbox.get("min")
            bmax = bbox.get("max")
            if isinstance(bmin, Mapping) and isinstance(bmax, Mapping):
                x0, y0 = bmin.get("x"), bmin.get("y")
                x1, y1 = bmax.get("x"), bmax.get("y")
                if (
                    isinstance(x0, (int, float))
                    and isinstance(y0, (int, float))
                    and isinstance(x1, (int, float))
                    and isinstance(y1, (int, float))
                ):
                    return ((x0 + x1) / 2.0, (y0 + y1) / 2.0)
    except Exception:  # tolerant by contract -- never raise on degenerate geometry
        pass
    return None


# ---------------------------------------------------------------------------
# Clustering
# ---------------------------------------------------------------------------


def cluster_rise_drop_symbols(
    entities: Sequence[RiseDropEntity],
    legend: ServiceLegend,
    *,
    kind: str,
    epsilon: float = _DEFAULT_ANCHOR_EPSILON,
) -> RiseDropResult:
    """Cluster ARC+HATCH entities into distinct rise/drop symbols.

    Algorithm (deterministic, O(n), permutation-invariant):

    1. Each entity with a usable anchor is quantized to a grid key
       ``(round(x/epsilon), round(y/epsilon))``.  Concentric arcs at a shared
       centre land in the same bucket; smallest real-world radius (~7.5 drawing
       units) far exceeds epsilon (1.0), so distinct symbols never merge.
    2. Per cluster: ``anchor_point`` = mean of member anchors;
       ``entity_ids`` = ``tuple(sorted(member_ids))``.
    3. Discipline: derive cluster colour_key from the colour of the first member
       in sorted-id order.  When member colour_keys disagree, use
       ``min(colour_keys)`` for determinism.  Resolve via
       ``legend.by_colour().get(colour_key)``: RESOLVED when found, UNKNOWN
       when not (discipline stays None; raw colour_key preserved).
    4. ``kind`` is propagated onto every symbol.
    5. Results sorted by ``(round(anchor.x, 3), round(anchor.y, 3), entity_ids[0])``.
    """
    # Step 1: anchor extraction + grid bucketing.
    # bucket_key -> list of (entity_id, anchor)
    buckets: dict[tuple[int, int], list[tuple[str, tuple[float, float]]]] = {}
    # Keep colour by entity_id for later lookup.
    colour_by_id: dict[str, str | None] = {}

    safe_epsilon = epsilon if epsilon > 0 else _DEFAULT_ANCHOR_EPSILON

    for ent in entities:
        anchor = _symbol_anchor(ent.geometry)
        if anchor is None:
            continue
        colour_by_id[ent.entity_id] = colour_key(ent.color)
        gx = round(anchor[0] / safe_epsilon)
        gy = round(anchor[1] / safe_epsilon)
        key = (gx, gy)
        if key not in buckets:
            buckets[key] = []
        buckets[key].append((ent.entity_id, anchor))

    by_colour = legend.by_colour()

    # Step 2-4: build symbols from clusters.
    symbols: list[RiseDropSymbol] = []
    for members in buckets.values():
        sorted_ids = sorted(m[0] for m in members)
        anchors = [m[1] for m in members]
        ax = mean(a[0] for a in anchors)
        ay = mean(a[1] for a in anchors)

        # Collect colour keys for all members (in sorted-id order).
        member_ckeys = [colour_by_id.get(eid) for eid in sorted_ids]
        valid_ckeys = [ck for ck in member_ckeys if ck is not None]

        if valid_ckeys:
            # Primary colour key: first member (sorted order); tiebreak with min.
            primary_ck: str | None = member_ckeys[0]
            if primary_ck is None or len(set(valid_ckeys)) > 1:
                primary_ck = min(valid_ckeys)
        else:
            primary_ck = None

        entry = by_colour.get(primary_ck) if primary_ck is not None else None
        if entry is not None:
            status = STATUS_RESOLVED
            discipline = entry.discipline
        else:
            status = STATUS_UNKNOWN
            discipline = None

        symbols.append(
            RiseDropSymbol(
                kind=kind,
                anchor_point=(ax, ay),
                discipline=discipline,
                colour_key=primary_ck,
                entity_ids=tuple(sorted_ids),
                status=status,
            )
        )

    # Step 5: deterministic sort.
    symbols.sort(
        key=lambda s: (round(s.anchor_point[0], 3), round(s.anchor_point[1], 3), s.entity_ids[0])
    )

    return RiseDropResult(symbols=tuple(symbols))
