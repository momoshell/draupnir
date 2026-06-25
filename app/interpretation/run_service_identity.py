"""Tag-to-run association and service identity fusion (issue #610, Phase 2 / be-p2-02).

Consumes P1 :class:`~app.interpretation.routed_runs.RunGroup` objects and
:class:`TagPlacement` inputs (pre-placed pipe-tag world points) and produces a
:class:`RunServiceIdentity` for every run — either RESOLVED (discipline + >=1 service
from a nearby tag), PARTIAL (discipline only, no tag within radius), or UNKNOWN (neither).

**Association is proximity-only, NOT layer-equality.**  Pipe tags live on a different
layer (e.g. "Pipe Tags") from routed-run linework (e.g. "Pipes"); the layer-scoping
used by ``rooms.assign_devices_to_label_rooms`` therefore does NOT apply here.  We use
minimum point-to-segment distance from each tag to the run's member geometry.

**Minimum-segment distance** (per run): for each member entity the minimum distance from
the tag point to the entity's geometry segments is computed; the run distance is the
minimum across all members.  Per entity:

- line     -> point-to-segment distance over [start, end]
- polyline -> min over consecutive vertex segments (``vertices`` or ``points`` key);
              single-vertex polylines fall back to point distance
- arc/other -> representative_point fallback if available (arc approximation documented
               on the helper); a run whose members yield no usable geometry has distance
               None and cannot be a tag target.

The minimum-segment metric is strictly superior to centroid distance for L-shaped or
long runs where the centroid may lie far from any labelled leg.

**Equidistant ambiguity**: when a tag is equidistant (within floating-point epsilon) to
two or more runs, it is added to ``ambiguous_tags`` AND deterministically attached to
the run with the lowest sort key ``(layer_ref or "", colour_key or "")``.  The tag is
flagged so downstream consumers can choose to ignore the attachment.

Per ADR-006 a RunGroup carries a SET of services modelled as a tuple (deterministic,
immutable).  Per-service length attribution is deferred to P3.

Pure module -- NO DB, ORM, FastAPI, or SQLAlchemy imports.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from app.ingestion.centerline_contract import _xy, _xy_list
from app.interpretation.routed_runs import RunGroup
from app.interpretation.run_tags import BASIS_TAG_TEXT, PipeSize, TagObservation, parse_tag

# ---------------------------------------------------------------------------
# Constants -- frozen interface contract (authoritative for this module)
# ---------------------------------------------------------------------------

IDENTITY_RESOLVED: str = "resolved"  # discipline + >=1 service
IDENTITY_PARTIAL: str = "partial"  # discipline only, no tag attached
IDENTITY_UNKNOWN: str = "unknown"  # no discipline and no service
BASIS_LEGEND_AND_TAG: str = "legend_colour+tag_text"

# Epsilon for equidistant detection (same as floating-point noise floor for typical
# CAD coordinate magnitudes up to ~1e6 units).
_EQ_EPSILON: float = 1e-9

# Default search radius in metres (#661 — adapters pre-scale geometry to metres).
# Calibrated across M-540003 (pipe callout tag at 3.85 m) + E-610003 (labels ≤2.2 m);
# plateau-stable 5-7 m; junk >=16 m. Single-building-calibrated -- scale-relative radius
# is the follow-on.
_DEFAULT_RADIUS: float = 5.0

# ---------------------------------------------------------------------------
# Input / output dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TagPlacement:
    """A placed pipe-tag text at a world coordinate.

    ``text`` is passed through :func:`~app.interpretation.run_tags.parse_tag`
    internally; the caller does NOT need to pre-parse it.
    ``layer_ref`` is optional provenance metadata; it is NOT used for scoping
    (association is proximity-only -- see module docstring).
    """

    text: str
    point: tuple[float, float]
    layer_ref: str | None = None


@dataclass(frozen=True, slots=True)
class ServiceSize:
    """One service+size observation attached to a run from a nearby tag."""

    service: str
    size: PipeSize  # from run_tags
    source_tag_text: str
    basis: str  # BASIS_TAG_TEXT


@dataclass(frozen=True, slots=True)
class RunServiceIdentity:
    """Fused discipline + service identity for one RunGroup."""

    layer_ref: str | None
    colour_key: str | None
    discipline: str | None
    services: tuple[ServiceSize, ...]
    status: str  # IDENTITY_RESOLVED | IDENTITY_PARTIAL | IDENTITY_UNKNOWN
    basis: str  # legend_colour | legend_colour+tag_text
    source_layers: tuple[str, ...]
    confidence: float | None
    competing_disciplines: tuple[str, ...]
    entity_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RunServiceIdentityResult:
    """Immutable result of tag-to-run association and identity fusion."""

    identities: tuple[RunServiceIdentity, ...]  # deterministic order
    unassigned_tags: tuple[TagObservation, ...]  # beyond radius of any run
    ambiguous_tags: tuple[TagObservation, ...]  # equidistant to 2+ runs, flagged


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _representative_point(geometry: Mapping[str, Any]) -> tuple[float, float] | None:
    """Return a representative (x, y) for one entity's geometry dict.

    - line     -> midpoint of start[0:2] and end[0:2]
    - polyline -> centroid of all vertices (``vertices`` or ``points`` key)
    - other    -> None (entity is skipped)

    Coordinates beyond index 1 (z-axis etc.) are ignored.
    """
    # Line: start/end coords may be dicts ({"x","y","z"}) on real data or lists ([x,y,z]).
    if "start" in geometry and "end" in geometry:
        s = _xy(geometry["start"])
        e = _xy(geometry["end"])
        if s is not None and e is not None:
            return ((s[0] + e[0]) / 2.0, (s[1] + e[1]) / 2.0)

    # Polyline: {"vertices": [...]} or {"points": [...]} (dict or list coords).
    coords = _xy_list(geometry.get("vertices") or geometry.get("points"))
    if coords:
        xs = [c[0] for c in coords]
        ys = [c[1] for c in coords]
        return (sum(xs) / len(xs), sum(ys) / len(ys))

    return None


def _dist(a: tuple[float, float], b: tuple[float, float]) -> float:
    return math.hypot(a[0] - b[0], a[1] - b[1])


def _point_to_segment_dist(
    p: tuple[float, float], a: tuple[float, float], b: tuple[float, float]
) -> float:
    """Minimum distance from point ``p`` to segment [``a``, ``b``]."""
    ax, ay = a
    bx, by = b
    dx, dy = bx - ax, by - ay
    seg_len_sq = dx * dx + dy * dy
    if seg_len_sq == 0.0:
        # Degenerate segment (a == b): distance to the point.
        return _dist(p, a)
    # Project p onto the line through a,b; clamp t to [0,1] for the segment.
    t = ((p[0] - ax) * dx + (p[1] - ay) * dy) / seg_len_sq
    t = max(0.0, min(1.0, t))
    proj = (ax + t * dx, ay + t * dy)
    return _dist(p, proj)


def _run_min_distance(
    tag_point: tuple[float, float],
    run: RunGroup,
    geometry_by_entity_id: Mapping[str, Mapping[str, Any]],
) -> float | None:
    """Minimum point-to-segment distance from ``tag_point`` to any member of ``run``.

    Per-entity rules:
    - line:     point-to-segment over [start, end].
    - polyline: min over consecutive vertex pairs (``vertices`` or ``points``);
                single-vertex polyline falls back to point distance.
    - arc/other: representative_point fallback (arc approximation — uses midpoint of
                 arc chord; exact arc distance is a future refinement).

    Returns ``None`` when no member yields usable geometry (run cannot be a tag target).
    """
    best: float | None = None

    def _update(d: float) -> None:
        nonlocal best
        if best is None or d < best:
            best = d

    for eid in run.entity_ids:
        geom = geometry_by_entity_id.get(eid)
        if geom is None:
            continue

        # Line: point-to-segment over [start, end].
        if "start" in geom and "end" in geom:
            s = _xy(geom["start"])
            e = _xy(geom["end"])
            if s is not None and e is not None:
                _update(_point_to_segment_dist(tag_point, s, e))
                continue

        # Polyline: min over consecutive vertex segments.
        raw_pts = geom.get("vertices") or geom.get("points")
        coords = _xy_list(raw_pts)
        if coords:
            if len(coords) == 1:
                _update(_dist(tag_point, coords[0]))
            else:
                for i in range(len(coords) - 1):
                    _update(_point_to_segment_dist(tag_point, coords[i], coords[i + 1]))
            continue

        # Arc/other: representative_point fallback (chord approximation).
        pt = _representative_point(geom)
        if pt is not None:
            _update(_dist(tag_point, pt))

    return best


def _sort_key(run: RunGroup) -> tuple[str, str]:
    return (run.layer_ref or "", run.colour_key or "")


# ---------------------------------------------------------------------------
# Public coordinator
# ---------------------------------------------------------------------------


def fuse_run_service_identities(
    runs: Sequence[RunGroup],
    geometry_by_entity_id: Mapping[str, Mapping[str, Any]],
    tags: Sequence[TagPlacement],
    *,
    radius: float = _DEFAULT_RADIUS,
) -> RunServiceIdentityResult:
    """Associate pipe-tag placements to the nearest run and fuse service identities.

    Parameters
    ----------
    runs:
        P1 :class:`~app.interpretation.routed_runs.RunGroup` objects (any order; output
        is deterministic regardless).
    geometry_by_entity_id:
        Mapping from entity_id to its geometry dict -- used to compute run-segment distances.
    tags:
        Placed pipe-tag texts in world coordinates.  Tags on ANY layer are considered
        (proximity-only, not layer-scoped -- see module docstring).
    radius:
        Maximum distance in metres from a tag to a run's nearest segment for association.
        Default ``5.0`` m (#661 — adapters pre-scale geometry to metres).

    Returns
    -------
    RunServiceIdentityResult
        ``identities`` sorted by ``(layer_ref or "", colour_key or "")``, matching
        :func:`~app.interpretation.routed_runs.identify_routed_runs` ordering.
        ``unassigned_tags`` and ``ambiguous_tags`` in stable (input-filter) order.
    """
    if not runs:
        return RunServiceIdentityResult(identities=(), unassigned_tags=(), ambiguous_tags=())

    # Sort runs for deterministic output and equidistant tie-breaking.
    sorted_runs = sorted(runs, key=_sort_key)

    # services_by_run_idx: index into sorted_runs -> list[ServiceSize]
    services_by_run_idx: list[list[ServiceSize]] = [[] for _ in sorted_runs]

    # tag_layers_by_run_idx: collects tag layer_refs for source_layers union
    tag_layers_by_run_idx: list[set[str]] = [set() for _ in sorted_runs]

    unassigned: list[TagObservation] = []
    ambiguous: list[TagObservation] = []

    for tag in tags:
        observation = parse_tag(tag.text)
        if observation is None:
            # Non-parseable: skip entirely (do NOT add to unassigned -- it carries no info).
            continue

        # Find minimum point-to-segment distances to all runs.
        dists: list[tuple[float, int]] = []  # (distance, run_index)
        for idx, run in enumerate(sorted_runs):
            d = _run_min_distance(tag.point, run, geometry_by_entity_id)
            if d is None:
                continue
            if d <= radius:
                dists.append((d, idx))

        if not dists:
            # Beyond radius of every run.
            unassigned.append(observation)
            continue

        min_dist = min(d for d, _ in dists)
        nearest_indices = [idx for d, idx in dists if d - min_dist <= _EQ_EPSILON]

        if len(nearest_indices) >= 2:
            # Equidistant: flag as ambiguous AND attach to the first (lowest sort key --
            # sorted_runs is already sorted so index 0 of nearest_indices gives the
            # tie-breaking winner after we re-sort the ties by index).
            ambiguous.append(observation)
            winner = sorted(nearest_indices)[0]
        else:
            winner = nearest_indices[0]

        ss = ServiceSize(
            service=observation.service,
            size=observation.size,
            source_tag_text=observation.raw_text,
            basis=BASIS_TAG_TEXT,
        )
        services_by_run_idx[winner].append(ss)
        if tag.layer_ref is not None:
            tag_layers_by_run_idx[winner].add(tag.layer_ref)

    # Build identities.
    identities: list[RunServiceIdentity] = []
    for idx, run in enumerate(sorted_runs):
        raw_services = services_by_run_idx[idx]

        # Deterministic service order: (service, size.raw, source_tag_text)
        raw_services.sort(key=lambda s: (s.service, s.size.raw, s.source_tag_text))
        services_tuple = tuple(raw_services)

        # source_layers: union of run's own layers + tag layers
        extra_layers = tag_layers_by_run_idx[idx]
        all_layers: tuple[str, ...] = (
            tuple(sorted(set(run.source_layers) | extra_layers))
            if extra_layers
            else run.source_layers
        )

        has_discipline = run.discipline is not None
        has_services = bool(services_tuple)

        if has_discipline and has_services:
            status = IDENTITY_RESOLVED
        elif has_discipline:
            status = IDENTITY_PARTIAL
        else:
            status = IDENTITY_UNKNOWN

        basis = BASIS_LEGEND_AND_TAG if has_services else run.basis

        identities.append(
            RunServiceIdentity(
                layer_ref=run.layer_ref,
                colour_key=run.colour_key,
                discipline=run.discipline,
                services=services_tuple,
                status=status,
                basis=basis,
                source_layers=all_layers,
                confidence=run.confidence,
                competing_disciplines=run.competing_disciplines,
                entity_ids=run.entity_ids,
            )
        )

    return RunServiceIdentityResult(
        identities=tuple(identities),
        unassigned_tags=tuple(unassigned),
        ambiguous_tags=tuple(ambiguous),
    )
