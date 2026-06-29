"""Service takeoff coordinator (issue #606, Phase 3 / be-p3-02).

A thin pure coordinator that composes routed-run identities into per-(service, size,
room) drawn-length totals, with scale-gating, room scoping, and honest degradation.

**Bundle-length model (P2/P3 decision):**
A :class:`~app.interpretation.run_service_identity.RunServiceIdentity` carries a SET of
services running in the same corridor -- e.g. VAC@54 + AGSS@42 for a dual-service chase.
These are PARALLEL runs sharing the same drawn path; the full drawn length is attributed
to EACH service independently (a bundle of length L contributes L to VAC and L to AGSS,
NOT L/2).  This matches physical reality: both pipe branches exist over the full corridor
length.

**Discipline gate (D3c Part B, issue #798):**
Two attribution modes are selected by the ``colour_differentiated`` flag, computed
revision-level from the HATCH bundle-band colour evidence:

- ``colour_differentiated=True`` (default): genuine multi-service corridor with distinct
  colour bands per service (e.g. med-gas M-540003 has 5 distinct chromatic colours).
  Uses the existing per-service bundle model — unchanged behaviour.

- ``colour_differentiated=False``: achromatic / BYLAYER discipline (e.g. drainage) where
  all bundle bands share a single grey/black colour key.  Coarse runs merge multiple
  services (SVP+CDP) so per-run attribution inflates counts.  Instead, each segment of
  the run's centerline polylines is attributed to the nearest parsed tag label (within
  ``segment_label_max_m``), and orphan segments (no label in radius) land in the
  ``unknown`` service bucket.  This conserves: Σ per-service + unknown == centerline total.

**Arc geometry:**
Arcs contribute 0 to drawn length in this phase.  Arc-length computation requires the
arc's radius and angular span, which the current geometry schema does not supply in a
usable form.  This is a known gap, tracked as P4 work.

**Run anchor choice:**
The anchor is computed as the centroid of the representative points of a run's member
entities (first usable point per member, via ``_representative_point``).  This is the
same algorithm used by ``run_service_identity.py`` and is permutation-invariant under
member ordering.

Pure module -- NO DB, ORM, FastAPI, or SQLAlchemy imports.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from app.ingestion.centerline_contract import _xy, _xy_list
from app.interpretation.measurement import (
    ScaleContext,
    measure_length,
    path_length,
)
from app.interpretation.rise_drop import RiseDropSymbol
from app.interpretation.room_partition import (
    _LABEL_ROOM_RADIUS_M as _LABEL_ROOM_RADIUS_M,
)
from app.interpretation.room_partition import (
    _containing_room as _containing_room,
)
from app.interpretation.room_partition import (
    _nearest_label_room as _nearest_label_room,
)
from app.interpretation.room_partition import (
    partition_polylines_by_room as partition_polylines_by_room,
)
from app.interpretation.rooms import Room
from app.interpretation.routed_runs import RunGroup
from app.interpretation.run_service_identity import (
    IDENTITY_PARTIAL,
    IDENTITY_RESOLVED,
    IDENTITY_UNKNOWN,
    RunServiceIdentity,
    ServiceSize,
)
from app.interpretation.segment_label_takeoff import SegmentLabel

# ---------------------------------------------------------------------------
# Constants -- frozen interface contract
# ---------------------------------------------------------------------------

ROOM_UNASSIGNED_ID: str = "service-takeoff-unassigned"
SERVICE_UNKNOWN: str = "unknown"

# Bucket key type: (service, size_raw, size_kind, room_id).
_BucketKey = tuple[str, str | None, str | None, str]

# Identity status rank: lower rank = worse (drives worst-across-runs logic).
_STATUS_RANK: dict[str, int] = {
    IDENTITY_UNKNOWN: 0,
    IDENTITY_PARTIAL: 1,
    IDENTITY_RESOLVED: 2,
}


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ServiceTakeoffLine:
    """One aggregated (service, size, room) length bucket."""

    service: str  # abbreviation or SERVICE_UNKNOWN
    size_raw: str | None  # PipeSize.raw; None if unknown size
    size_kind: str | None  # "round" | "rect" | None
    discipline: str | None
    room_id: str  # Room.id or ROOM_UNASSIGNED_ID
    room_name: str | None
    room_number: str | None
    drawing_length: float  # summed raw drawn length (>=0)
    real_length_m: float | None  # metres only when scale-gated; else None
    basis: str  # "real_world" | "drawing_units_only"
    units_confidence: str
    run_count: int
    entity_ids: tuple[str, ...]  # sorted union (provenance)
    identity_status: str  # worst across contributing runs
    confidence: float | None  # None if any contributing run has competing_disciplines
    riser_count: int  # count of rise symbols in this (SERVICE_UNKNOWN, room) bucket
    drop_count: int  # count of drop symbols in this (SERVICE_UNKNOWN, room) bucket
    # True when a contributing run carried MULTIPLE services (a bundle, e.g. a shared-colour
    # med-gas chase). Under the bundle model each service in the bundle gets the FULL corridor
    # length, so this line's per-service length is "present in the bundle", NOT an individually
    # resolved split (#655). Group/discipline totals and per-room metres are unaffected.
    bundle: bool


@dataclass(frozen=True, slots=True)
class ServiceTakeoffResult:
    """Immutable takeoff result for one drawing revision."""

    # sorted by (service, size_raw or "", room_number or "", room_id)
    lines: tuple[ServiceTakeoffLine, ...]
    unscaled: bool  # any line is drawing_units_only
    unassigned_run_count: int
    unknown_service_run_count: int
    total_risers: int  # distinct rise symbols (len of rise_symbols input)
    total_drops: int  # distinct drop symbols (len of drop_symbols input)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _representative_point(geometry: Mapping[str, Any]) -> tuple[float, float] | None:
    """Return a representative (x, y) for one entity's geometry dict.

    - line     -> midpoint of start[0:2] and end[0:2]
    - polyline -> centroid of all vertices (``vertices`` or ``points`` key)
    - other    -> None (entity is skipped; arcs contribute 0 -- see module docstring)
    """
    if "start" in geometry and "end" in geometry:
        s = _xy(geometry["start"])
        e = _xy(geometry["end"])
        if s is not None and e is not None:
            return ((s[0] + e[0]) / 2.0, (s[1] + e[1]) / 2.0)

    coords = _xy_list(geometry.get("vertices") or geometry.get("points"))
    if coords:
        xs = [c[0] for c in coords]
        ys = [c[1] for c in coords]
        return (sum(xs) / len(xs), sum(ys) / len(ys))

    return None


def _run_anchor(
    entity_ids: tuple[str, ...],
    geometry_by_entity_id: Mapping[str, Mapping[str, Any]],
) -> tuple[float, float] | None:
    """Centroid of representative points across a run's member entities.

    Returns None when no member yields usable geometry.  Arc entities are
    excluded (no representative point -- see module docstring).
    """
    xs: list[float] = []
    ys: list[float] = []
    for eid in entity_ids:
        geom = geometry_by_entity_id.get(eid)
        if geom is None:
            continue
        pt = _representative_point(geom)
        if pt is not None:
            xs.append(pt[0])
            ys.append(pt[1])
    if not xs:
        return None
    return (sum(xs) / len(xs), sum(ys) / len(ys))


def _entity_drawn_length(
    entity_ids: tuple[str, ...],
    geometry_by_entity_id: Mapping[str, Mapping[str, Any]],
) -> float:
    """Sum drawn length across a run's member entities.

    - line     -> path_length([start[:2], end[:2]])
    - polyline -> path_length over all vertex [:2] pairs
    - arc      -> 0  (arc length requires radius + angular span, not yet in schema; P4)
    - missing  -> 0
    """
    total = 0.0
    for eid in entity_ids:
        geom = geometry_by_entity_id.get(eid)
        if geom is None:
            continue

        if "start" in geom and "end" in geom:
            s = _xy(geom["start"])
            e = _xy(geom["end"])
            if s is not None and e is not None:
                total += path_length([s, e])
            continue

        coords = _xy_list(geom.get("vertices") or geom.get("points"))
        if coords:
            total += path_length(coords)
            continue

        # arc or unrecognised -- contributes 0; see module docstring
    return total


def _anchor_partition(
    identity: RunServiceIdentity,
    rooms: Sequence[Room],
    geometry_by_entity_id: Mapping[str, Mapping[str, Any]],
    measured_length_by_group: Mapping[tuple[str | None, str | None], float] | None,
    run_key: tuple[str | None, str | None],
    label_radius: float | None = None,
) -> list[tuple[Room | None, float]]:
    """Fallback attribution: the whole run length to its single anchor-containing room.

    Used when the group has no persisted centerline geometry (passthrough / unmeasured) — the
    pre-LP2 behaviour, preserved unchanged so non-geometry groups never regress. The drawn length
    is the materialized scalar when present, else the naive entity-sum.
    """
    if measured_length_by_group is not None and run_key in measured_length_by_group:
        drawn = measured_length_by_group[run_key]
    else:
        drawn = _entity_drawn_length(identity.entity_ids, geometry_by_entity_id)
    anchor = _run_anchor(identity.entity_ids, geometry_by_entity_id)
    room: Room | None = None
    if anchor is not None:
        room = _containing_room(anchor, rooms)
        if room is None and label_radius is not None:
            room = _nearest_label_room(anchor, rooms, radius=label_radius)
    # Always one partition, even when drawn==0 (e.g. arc-only runs): the run still produces a
    # bucket entry so it is never silently dropped (pre-LP2 invariant).
    return [(room, drawn)]


def _worst_status(a: str, b: str) -> str:
    """Return the status with the lower rank (worse)."""
    return a if _STATUS_RANK.get(a, 0) <= _STATUS_RANK.get(b, 0) else b


# Minimum RGB channel spread (max - min) for a colour to be considered chromatic.
# Must match ``_DWG_SWATCH_MIN_CHROMA`` in ``service_takeoff_loaders.py`` (both = 30).
_MIN_CHROMA_SPREAD: int = 30

# Epsilon for float comparisons in segment nearest-label search (metres).
_LABEL_EQ_EPSILON: float = 1e-9


def _is_achromatic_colour_key(ck: str) -> bool:
    """Return True when ``ck`` represents an achromatic (grey/black/white) colour.

    A colour_key is the lowercased RGB hex string from the adapter (possibly with a
    leading 2-char alpha byte, e.g. ``"c3000007"`` → strip to ``"000007"``).  If it
    starts with ``"idx:"`` or cannot be parsed as hex, we conservatively return False
    (treat as chromatic — the safe direction for the bundle gate).

    Achromatic = max(R,G,B) - min(R,G,B) < ``_MIN_CHROMA_SPREAD``.
    """
    if ck.startswith("idx:"):
        # Index-only colour has no parseable RGB; treat as chromatic.
        return False
    rgb_str = ck.lstrip("#")
    # Strip leading alpha byte if 8-char hex (e.g. "c3000007" → "000007").
    if len(rgb_str) == 8:
        rgb_str = rgb_str[2:]
    if len(rgb_str) != 6:
        return False
    try:
        r = int(rgb_str[0:2], 16)
        g = int(rgb_str[2:4], 16)
        b = int(rgb_str[4:6], 16)
        return max(r, g, b) - min(r, g, b) < _MIN_CHROMA_SPREAD
    except ValueError:
        return False


def _nearest_label(
    midpoint: tuple[float, float],
    labels: Sequence[SegmentLabel],
    max_dist: float,
) -> SegmentLabel | None:
    """Return the nearest SegmentLabel within ``max_dist``, or None.

    Ties within ``_LABEL_EQ_EPSILON`` are broken lexicographically by
    ``(service, size_raw or "", size_kind or "")`` — order-independent.
    """
    best: SegmentLabel | None = None
    best_dist = math.inf
    for lbl in labels:
        d = math.hypot(midpoint[0] - lbl.point[0], midpoint[1] - lbl.point[1])
        if d > max_dist:
            continue
        if d < best_dist - _LABEL_EQ_EPSILON:
            best = lbl
            best_dist = d
        elif abs(d - best_dist) <= _LABEL_EQ_EPSILON and best is not None:
            # Tie: lexicographic (service, size_raw, size_kind) wins.
            lbl_key = (lbl.service, lbl.size_raw or "", lbl.size_kind or "")
            best_key = (best.service, best.size_raw or "", best.size_kind or "")
            if lbl_key < best_key:
                best = lbl
                best_dist = d
    return best


def _achromatic_apportionment(
    identity: RunServiceIdentity,
    run_key: tuple[str | None, str | None],
    rooms: Sequence[Room],
    geometry_by_entity_id: Mapping[str, Mapping[str, Any]],
    measured_geometry_by_group: (
        Mapping[tuple[str | None, str | None], tuple[tuple[tuple[float, float], ...], ...]] | None
    ),
    measured_length_by_group: Mapping[tuple[str | None, str | None], float] | None,
    segment_labels: Sequence[SegmentLabel],
    segment_label_max_m: float,
    label_radius: float | None,
) -> list[tuple[str, str | None, str | None, Room | None, float]]:
    """Apportion an achromatic run's length per segment → nearest tag → room.

    Returns a list of ``(service, size_raw, size_kind, room, length)`` tuples where
    ``service`` is ``SERVICE_UNKNOWN`` for orphan segments (no label in radius).

    When the run has no persisted centerline geometry, falls back to
    ``_anchor_partition`` and attributes the whole length to the identity's own
    services (collapsed) or ``SERVICE_UNKNOWN`` — the same as bundle mode.

    Conservation invariant: Σ lengths == total centerline length for this run (±float eps).
    """
    polylines = (
        measured_geometry_by_group.get(run_key) if measured_geometry_by_group is not None else None
    )

    if not polylines:
        # No geometry: fall back to anchor attribution using the run's own services.
        partitions = _anchor_partition(
            identity,
            rooms,
            geometry_by_entity_id,
            measured_length_by_group,
            run_key,
            label_radius=label_radius,
        )
        # Determine service/size from the identity (collapsed) or unknown.
        services_to_emit: tuple[ServiceSize, ...] = (
            _collapse_services_per_service(identity.services)
            if identity.services
            else identity.services
        )
        result: list[tuple[str, str | None, str | None, Room | None, float]] = []
        for room, drawn in partitions:
            if not services_to_emit:
                result.append((SERVICE_UNKNOWN, None, None, room, drawn))
            elif len(services_to_emit) == 1:
                ss = services_to_emit[0]
                result.append((ss.service, ss.size.raw, ss.size.kind, room, drawn))
            else:
                # Multi-service run with no geometry: no per-segment evidence to split.
                # Attributing the full length to each service would inflate Σ by N x drawn.
                # Honest fallback: emit once to SERVICE_UNKNOWN (conservation holds).
                result.append((SERVICE_UNKNOWN, None, None, room, drawn))
        return result

    # Segment-level apportionment: iterate every segment of every polyline.
    result_segs: list[tuple[str, str | None, str | None, Room | None, float]] = []

    for polyline in polylines:
        pts = list(polyline)
        if len(pts) < 2:
            continue
        for i in range(len(pts) - 1):
            a = pts[i]
            b = pts[i + 1]
            seg_len = math.hypot(b[0] - a[0], b[1] - a[1])
            if seg_len == 0.0:
                continue

            mid = ((a[0] + b[0]) * 0.5, (a[1] + b[1]) * 0.5)

            # Nearest tag attribution.
            nearest = _nearest_label(mid, segment_labels, segment_label_max_m)
            if nearest is not None:
                svc = nearest.service
                sz_raw = nearest.size_raw
                sz_kind = nearest.size_kind
            else:
                svc = SERVICE_UNKNOWN
                sz_raw = None
                sz_kind = None

            # Room attribution via midpoint containment.
            room = _containing_room(mid, rooms)
            if room is None and label_radius is not None:
                room = _nearest_label_room(mid, rooms, radius=label_radius)

            result_segs.append((svc, sz_raw, sz_kind, room, seg_len))

    return result_segs


def _collapse_services_per_service(
    services: tuple[ServiceSize, ...],
) -> tuple[ServiceSize, ...]:
    """Collapse over-tagged services to one dominant size PER DISTINCT SERVICE.

    Two physical models must coexist:

    - **Single-service run** (e.g. drainage: ``[SVP/100, SVP/100, SVP/75]``):
      multiple tags are over-tagging — the run is ONE pipe.  Collapse each distinct
      service to its dominant ``(service, size.raw)`` pair and count the run length ONCE.

    - **Multi-service bundle** (e.g. med-gas: ``[VAC/54, MA/42, OXY/42, AGSS/42, AGSS/42]``):
      distinct services share the corridor — EACH service must keep the full run length.
      Within a service that appears more than once (AGSS twice above) the duplicate is
      still collapsed to the dominant size, but the service itself is preserved.

    Algorithm:

    1. Group entries by ``service``.
    2. For each distinct service, if multiple ``(service, size.raw)`` pairs exist,
       pick the representative ``ServiceSize`` whose ``size.raw`` appears most
       frequently (tie-break: first in already-sorted ``services`` tuple, which is
       sorted by ``(service, size.raw, source_tag_text)`` in
       ``fuse_run_service_identities``).
    3. Emit exactly one ``ServiceSize`` per distinct service.

    Returns the collapsed tuple (same length as the number of distinct services).
    An empty input returns an empty tuple.
    """
    if not services:
        return ()

    # Group by service, preserving encounter order for tie-breaking.
    groups: dict[str, list[ServiceSize]] = {}
    for ss in services:
        groups.setdefault(ss.service, []).append(ss)

    result: list[ServiceSize] = []
    for svc_entries in groups.values():
        if len(svc_entries) == 1:
            result.append(svc_entries[0])
            continue
        # Multiple entries for this service — pick the size.raw with highest frequency.
        # Tie-break: first entry in the (already sorted) list wins.
        size_freq: Counter[str | None] = Counter(ss.size.raw for ss in svc_entries)
        max_freq = max(size_freq.values())
        for ss in svc_entries:
            if size_freq[ss.size.raw] == max_freq:
                result.append(ss)
                break

    return tuple(result)


# Keep the old name as an alias so existing direct-import tests that reference
# ``_dominant_service`` continue to resolve.  The alias is intentionally kept
# narrow: callers that imported it for unit-testing map the new semantics below.
def _dominant_service(
    services: tuple[ServiceSize, ...],
) -> ServiceSize | None:
    """Legacy helper — returns ``None`` when every ``(service, size.raw)`` pair is unique.

    Preserved for backward-compat with direct test imports.  The coordinator now calls
    :func:`_collapse_services_per_service` which applies per-service dedup.

    Returns the single dominant ``ServiceSize`` only when ALL services in the tuple share
    the same ``service`` name AND there are duplicates — i.e. it is a single-service
    over-tagged run.  For genuine multi-service bundles (distinct service names) it
    returns ``None`` (caller uses the bundle model).

    For the general per-service collapse use :func:`_collapse_services_per_service`.
    """
    if len(services) <= 1:
        return None

    freq: Counter[tuple[str, str | None]] = Counter((ss.service, ss.size.raw) for ss in services)
    max_freq = max(freq.values())

    # All pairs unique → true bundle; caller keeps the full iteration.
    if max_freq == 1:
        return None

    # If there are multiple distinct service names the run is a multi-service bundle even
    # if one service appears more than once (e.g. VAC+MA+OXY+AGSS/AGSS).  In that case
    # the old single-dominant collapse would have silently dropped services — return None
    # so the caller knows this is a bundle.
    distinct_services = {ss.service for ss in services}
    if len(distinct_services) > 1:
        return None

    # Single-service run with duplicate size tags → collapse to most-frequent size.
    for ss in services:
        if freq[(ss.service, ss.size.raw)] == max_freq:
            return ss

    # Unreachable.
    return services[0]


# ---------------------------------------------------------------------------
# Public coordinator
# ---------------------------------------------------------------------------


def compute_service_takeoff(
    *,
    runs: Sequence[RunGroup],
    identities: Sequence[RunServiceIdentity],
    geometry_by_entity_id: Mapping[str, Mapping[str, Any]],
    rooms: Sequence[Room],
    scale: ScaleContext,
    rise_symbols: Sequence[RiseDropSymbol] = (),
    drop_symbols: Sequence[RiseDropSymbol] = (),
    measured_length_by_group: Mapping[tuple[str | None, str | None], float] | None = None,
    measured_geometry_by_group: (
        Mapping[tuple[str | None, str | None], tuple[tuple[tuple[float, float], ...], ...]] | None
    ) = None,
    colour_differentiated: bool = True,
    segment_labels: Sequence[SegmentLabel] = (),
    segment_label_max_m: float = 5.0,
) -> ServiceTakeoffResult:
    """Compose run identities into per-(service, size, room) length totals.

    Matches each identity to its RunGroup by VALUE of (layer_ref, colour_key).
    Uses the bundle-length model: each service in a multi-service identity receives
    the full drawn length of the run (see module docstring).  A run with no services
    is counted in the SERVICE_UNKNOWN bucket.  No run is ever dropped.

    Room attribution (#654, LP2): when ``measured_geometry_by_group`` carries the group's
    persisted centerline polylines, the run's length is DISTRIBUTED across rooms by clipping
    (see :func:`~app.interpretation.room_partition.partition_polylines_by_room`) — a corridor
    spanning rooms is split per room,
    and any length outside all rooms lands in ROOM_UNASSIGNED_ID.  Without geometry the run
    falls back to the whole length in its single anchor-containing room (pre-LP2 behaviour).
    ``unassigned_run_count`` counts runs with ANY length outside all rooms;
    ``unknown_service_run_count`` counts service-less runs — both once per run, not per room.

    Rise/drop symbols (``rise_symbols`` / ``drop_symbols``) are counted per room and
    accumulated into a dedicated SERVICE_UNKNOWN bucket for each (discipline, room)
    pair.  Vertical length is intentionally absent (counts are scale-free).
    ``total_risers`` / ``total_drops`` equal ``len(rise_symbols)`` /
    ``len(drop_symbols)`` -- they count distinct physical symbols, NOT line sums.

    Parameters
    ----------
    runs:
        P1 RunGroup objects -- used only for competing_disciplines lookup.
    identities:
        P2 RunServiceIdentity objects -- primary driver of the rollup.
    geometry_by_entity_id:
        Entity-id to geometry dict.  Consumed for drawn-length computation and
        run-anchor scoping.
    rooms:
        Room objects with polygon geometry for spatial scoping.
    scale:
        ScaleContext driving the scale gate (ADR-004).
    rise_symbols:
        Clustered rise symbols from ``cluster_rise_drop_symbols``; default empty.
    drop_symbols:
        Clustered drop symbols from ``cluster_rise_drop_symbols``; default empty.
    measured_length_by_group:
        Optional mapping of ``(layer_ref, colour_key) -> skeleton_length_du`` from
        materialized centerline rows.  When a key is present, the measured length
        overrides the naive ``_entity_drawn_length`` sum for that group.  When
        ``None`` (default) the coordinator is byte-identical to its prior behaviour.
    colour_differentiated:
        When True (default), uses the bundle-length model: each service in a
        multi-service identity receives the full drawn length.  When False
        (achromatic / BYLAYER discipline), switches to segment-level nearest-tag
        apportionment — each centerline segment is attributed to its nearest parsed
        ``SegmentLabel`` within ``segment_label_max_m``; orphan segments land in
        ``SERVICE_UNKNOWN``.  Conserves: Σ per-service + unknown == centerline total.
        Default True keeps existing callers unaffected.
    segment_labels:
        Parsed tag labels with world-coordinate anchors.  Only consumed when
        ``colour_differentiated=False``.  Pass the ``SegmentLabel`` list built by the
        route's Step 5e logic.  Default empty (no-op for bundle mode).
    segment_label_max_m:
        Maximum metres from a segment midpoint to a tag label for the label to be
        eligible.  Default 5.0 (matches ``_SEGMENT_LABEL_MAX_M`` in the route).
    """
    if not identities and not rise_symbols and not drop_symbols:
        return ServiceTakeoffResult(
            lines=(),
            unscaled=False,
            unassigned_run_count=0,
            unknown_service_run_count=0,
            total_risers=len(rise_symbols),
            total_drops=len(drop_symbols),
        )

    # Build a lookup: (layer_ref, colour_key) -> RunGroup for competing_disciplines.
    runs_by_key: dict[tuple[str | None, str | None], RunGroup] = {
        (rg.layer_ref, rg.colour_key): rg for rg in runs
    }

    # #662: convert the metre radius into the coordinator's drawing-coordinate space so the
    # label-proximity fallback uses the same units as room polygons. None when scale gives no
    # usable factor -> fallback skipped (length stays unassigned, ADR-004).
    cf = scale.conversion_factor
    label_radius = _LABEL_ROOM_RADIUS_M / cf if cf is not None and cf > 0 else None

    # Accumulators keyed by (service, size_raw, size_kind, room_id).
    # Each bucket stores a mutable working state before conversion to ServiceTakeoffLine.
    # Type alias kept at module scope via _BucketKey below.
    acc_drawing_length: dict[_BucketKey, float] = {}
    acc_run_count: dict[_BucketKey, int] = {}
    acc_entity_ids: dict[_BucketKey, set[str]] = {}
    acc_identity_status: dict[_BucketKey, str] = {}
    # True if any contributing run has competing_disciplines
    acc_has_conflict: dict[_BucketKey, bool] = {}
    # True if any contributing run carried multiple services (bundle; #655)
    acc_is_bundle: dict[_BucketKey, bool] = {}
    acc_room_name: dict[_BucketKey, str | None] = {}
    acc_room_number: dict[_BucketKey, str | None] = {}
    acc_discipline: dict[_BucketKey, str | None] = {}
    # Rise/drop counts per SERVICE_UNKNOWN bucket -- keyed by room_id only (symbols carry
    # no service/size, so they land in (SERVICE_UNKNOWN, None, None, room_id)).
    acc_riser_count: dict[_BucketKey, int] = {}
    acc_drop_count: dict[_BucketKey, int] = {}

    unassigned_run_count = 0
    unknown_service_run_count = 0

    for identity in identities:
        run_key = (identity.layer_ref, identity.colour_key)

        # Determine competing_disciplines from the matched RunGroup.
        matched_run = runs_by_key.get(run_key)
        has_conflict = bool(matched_run and matched_run.competing_disciplines)

        if not colour_differentiated:
            # --- Achromatic mode (D3c Part B): per-segment nearest-tag apportionment ---
            # Each segment is attributed to its nearest SegmentLabel within
            # segment_label_max_m; orphan segments land in SERVICE_UNKNOWN.
            # Conservation invariant: Σ per-service + unknown == centerline total.
            seg_attributions = _achromatic_apportionment(
                identity=identity,
                run_key=run_key,
                rooms=rooms,
                geometry_by_entity_id=geometry_by_entity_id,
                measured_geometry_by_group=measured_geometry_by_group,
                measured_length_by_group=measured_length_by_group,
                segment_labels=segment_labels,
                segment_label_max_m=segment_label_max_m,
                label_radius=label_radius,
            )

            # Run-level diagnostic counters (once per run, not per segment).
            any_unknown_svc = any(svc == SERVICE_UNKNOWN for svc, _, _, _, _ in seg_attributions)
            any_unassigned = any(room is None for _, _, _, room, _ in seg_attributions)
            if any_unknown_svc and not identity.services:
                # Only increment when the run itself has no services (all segments are unknown
                # because the run carries no service identity, not because tags are missing).
                unknown_service_run_count += 1
            if any_unassigned:
                unassigned_run_count += 1

            # Accumulate each segment attribution into the bucket grid.
            # run_count semantics: a run is counted once per distinct (service,size,room)
            # bucket it contributes to (not per segment), so we track seen keys per run.
            seen_keys_this_run: set[_BucketKey] = set()
            for svc, sz_raw, sz_kind, room, seg_len in seg_attributions:
                room_id = room.id if room is not None else ROOM_UNASSIGNED_ID
                room_name_seg: str | None = room.name if room is not None else None
                room_number_seg: str | None = room.number if room is not None else None

                key: _BucketKey = (svc, sz_raw, sz_kind, room_id)
                # Use run_count=1 only the first time this run touches this bucket.
                is_first = key not in seen_keys_this_run
                seen_keys_this_run.add(key)

                if key not in acc_drawing_length:
                    acc_drawing_length[key] = 0.0
                    acc_run_count[key] = 0
                    acc_entity_ids[key] = set()
                    acc_identity_status[key] = identity.status
                    acc_has_conflict[key] = False
                    acc_is_bundle[key] = False
                    acc_room_name[key] = room_name_seg
                    acc_room_number[key] = room_number_seg
                    acc_discipline[key] = identity.discipline

                acc_drawing_length[key] += seg_len
                if is_first:
                    acc_run_count[key] += 1
                acc_entity_ids[key].update(identity.entity_ids)
                acc_identity_status[key] = _worst_status(acc_identity_status[key], identity.status)
                acc_has_conflict[key] = acc_has_conflict[key] or has_conflict
                if identity.discipline is not None:
                    current = acc_discipline[key]
                    acc_discipline[key] = (
                        identity.discipline
                        if current is None
                        else min(current, identity.discipline)
                    )

        else:
            # --- Bundle mode (colour_differentiated=True): existing per-run attribution ---
            # LP2 (#654): when the group has persisted centerline geometry, distribute its measured
            # length across rooms by clipping the polylines; otherwise fall back to the pre-LP2 rule
            # (whole length -> the single anchor-containing room). Honest degradation, no guessing.
            polylines = (
                measured_geometry_by_group.get(run_key)
                if measured_geometry_by_group is not None
                else None
            )
            partitions: list[tuple[Room | None, float]] = []
            if polylines:
                partitions = partition_polylines_by_room(
                    polylines, rooms, label_radius=label_radius
                )
            if not partitions:
                partitions = _anchor_partition(
                    identity,
                    rooms,
                    geometry_by_entity_id,
                    measured_length_by_group,
                    run_key,
                    label_radius=label_radius,
                )

            # Run-level diagnostic counters (counted once per run, not per room partition).
            if not identity.services:
                unknown_service_run_count += 1
            if any(room is None for room, _ in partitions):
                unassigned_run_count += 1

            # D3c conservation fix: collapse over-tagged runs to one size PER DISTINCT SERVICE
            # before the room loop so that is_bundle and services_to_emit are consistent.
            #
            # _collapse_services_per_service groups by service name and deduplicates sizes within
            # each group, preserving all distinct services:
            #   [SVP/100, SVP/100, SVP/75]               → [SVP/100]   (single-service, length once)
            #   [VAC/54, MA/42, OXY/42, AGSS/42, AGSS/42] → [VAC/54, MA/42, OXY/42, AGSS/42]
            services_to_emit: tuple[ServiceSize, ...] = (
                _collapse_services_per_service(identity.services)
                if identity.services
                else identity.services
            )
            # A bundle: run contributes full corridor length to EACH of multiple distinct services.
            is_bundle = len(services_to_emit) > 1

            for room, drawn in partitions:
                if room is None:
                    room_id = ROOM_UNASSIGNED_ID
                    room_name: str | None = None
                    room_number: str | None = None
                else:
                    room_id = room.id
                    room_name = room.name
                    room_number = room.number

                if not identity.services:
                    # No services -> SERVICE_UNKNOWN bucket.
                    key = (SERVICE_UNKNOWN, None, None, room_id)
                    _accumulate(
                        key=key,
                        drawn=drawn,
                        identity=identity,
                        has_conflict=has_conflict,
                        is_bundle=is_bundle,
                        room_name=room_name,
                        room_number=room_number,
                        acc_drawing_length=acc_drawing_length,
                        acc_run_count=acc_run_count,
                        acc_entity_ids=acc_entity_ids,
                        acc_identity_status=acc_identity_status,
                        acc_has_conflict=acc_has_conflict,
                        acc_is_bundle=acc_is_bundle,
                        acc_room_name=acc_room_name,
                        acc_room_number=acc_room_number,
                        acc_discipline=acc_discipline,
                    )
                else:
                    for ss in services_to_emit:
                        key = (ss.service, ss.size.raw, ss.size.kind, room_id)
                        _accumulate(
                            key=key,
                            drawn=drawn,
                            identity=identity,
                            has_conflict=has_conflict,
                            is_bundle=is_bundle,
                            room_name=room_name,
                            room_number=room_number,
                            acc_drawing_length=acc_drawing_length,
                            acc_run_count=acc_run_count,
                            acc_entity_ids=acc_entity_ids,
                            acc_identity_status=acc_identity_status,
                            acc_has_conflict=acc_has_conflict,
                            acc_is_bundle=acc_is_bundle,
                            acc_room_name=acc_room_name,
                            acc_room_number=acc_room_number,
                            acc_discipline=acc_discipline,
                        )

    # Process rise/drop symbols: map each to its containing room and accumulate counts
    # into the SERVICE_UNKNOWN bucket for that room.  Counts are scale-free; no length
    # is added.  Each physical symbol is counted exactly once (ADR-006).
    def _accumulate_symbol(symbol: RiseDropSymbol, *, is_rise: bool) -> None:
        anchor = symbol.anchor_point
        room = _containing_room(anchor, rooms)
        sym_room_id = room.id if room is not None else ROOM_UNASSIGNED_ID
        sym_room_name = room.name if room is not None else None
        sym_room_number = room.number if room is not None else None

        sym_key: _BucketKey = (SERVICE_UNKNOWN, None, None, sym_room_id)

        # Ensure the bucket exists (without adding drawn length or run_count).
        if sym_key not in acc_drawing_length:
            acc_drawing_length[sym_key] = 0.0
            acc_run_count[sym_key] = 0
            acc_entity_ids[sym_key] = set(symbol.entity_ids)
            acc_identity_status[sym_key] = IDENTITY_UNKNOWN
            acc_has_conflict[sym_key] = False
            acc_room_name[sym_key] = sym_room_name
            acc_room_number[sym_key] = sym_room_number
            acc_discipline[sym_key] = symbol.discipline
        else:
            # Merge entity provenance and discipline (min-alphabetical).
            acc_entity_ids[sym_key].update(symbol.entity_ids)
            if symbol.discipline is not None:
                current_disc = acc_discipline[sym_key]
                acc_discipline[sym_key] = (
                    symbol.discipline
                    if current_disc is None
                    else min(current_disc, symbol.discipline)
                )

        if is_rise:
            acc_riser_count[sym_key] = acc_riser_count.get(sym_key, 0) + 1
        else:
            acc_drop_count[sym_key] = acc_drop_count.get(sym_key, 0) + 1

    for sym in rise_symbols:
        _accumulate_symbol(sym, is_rise=True)
    for sym in drop_symbols:
        _accumulate_symbol(sym, is_rise=False)

    # Build ServiceTakeoffLine objects from accumulators.
    lines: list[ServiceTakeoffLine] = []
    has_unscaled = False

    for key, total_drawing in acc_drawing_length.items():
        service, size_raw, size_kind, room_id = key

        measurement = measure_length(total_drawing, scale)
        if measurement.basis == "drawing_units_only":
            has_unscaled = True

        # confidence is always None in P3: no scoring yet.  acc_has_conflict is retained
        # here so P4 can wire it without restructuring the accumulator -- it will drive
        # confidence once a scoring model is available.
        # When competing_disciplines are present the caller should treat confidence as absent.
        confidence: float | None = None

        lines.append(
            ServiceTakeoffLine(
                service=service,
                size_raw=size_raw,
                size_kind=size_kind,
                discipline=acc_discipline[key],
                room_id=room_id,
                room_name=acc_room_name[key],
                room_number=acc_room_number[key],
                drawing_length=measurement.drawing_length,
                real_length_m=measurement.real_length_m,
                basis=measurement.basis,
                units_confidence=measurement.units_confidence,
                run_count=acc_run_count[key],
                entity_ids=tuple(sorted(acc_entity_ids[key])),
                identity_status=acc_identity_status[key],
                confidence=confidence,
                riser_count=acc_riser_count.get(key, 0),
                drop_count=acc_drop_count.get(key, 0),
                bundle=acc_is_bundle.get(key, False),
            )
        )

    # Sort deterministically: (service, size_raw or "", room_number or "", room_id)
    lines.sort(key=lambda ln: (ln.service, ln.size_raw or "", ln.room_number or "", ln.room_id))

    return ServiceTakeoffResult(
        lines=tuple(lines),
        unscaled=has_unscaled,
        unassigned_run_count=unassigned_run_count,
        unknown_service_run_count=unknown_service_run_count,
        total_risers=len(rise_symbols),
        total_drops=len(drop_symbols),
    )


def _accumulate(
    *,
    key: _BucketKey,
    drawn: float,
    identity: RunServiceIdentity,
    has_conflict: bool,
    is_bundle: bool,
    room_name: str | None,
    room_number: str | None,
    acc_drawing_length: dict[_BucketKey, float],
    acc_run_count: dict[_BucketKey, int],
    acc_entity_ids: dict[_BucketKey, set[str]],
    acc_identity_status: dict[_BucketKey, str],
    acc_has_conflict: dict[_BucketKey, bool],
    acc_is_bundle: dict[_BucketKey, bool],
    acc_room_name: dict[_BucketKey, str | None],
    acc_room_number: dict[_BucketKey, str | None],
    acc_discipline: dict[_BucketKey, str | None],
) -> None:
    """Add one (identity, drawn-length) contribution to the bucket for ``key``."""
    if key not in acc_drawing_length:
        acc_drawing_length[key] = 0.0
        acc_run_count[key] = 0
        acc_entity_ids[key] = set()
        acc_identity_status[key] = identity.status
        acc_has_conflict[key] = False
        acc_is_bundle[key] = False
        acc_room_name[key] = room_name
        acc_room_number[key] = room_number
        acc_discipline[key] = identity.discipline

    acc_drawing_length[key] += drawn
    acc_run_count[key] += 1
    acc_is_bundle[key] = acc_is_bundle[key] or is_bundle
    acc_entity_ids[key].update(identity.entity_ids)
    acc_identity_status[key] = _worst_status(acc_identity_status[key], identity.status)
    acc_has_conflict[key] = acc_has_conflict[key] or has_conflict
    # discipline: deterministic rollup -- among all non-None contributors pick min()
    # (alphabetical).  This preserves permutation-invariance when runs with differing
    # disciplines collapse into the same bucket (e.g. SERVICE_UNKNOWN).  When all
    # contributors carry None, the result stays None (discipline genuinely unknown).
    if identity.discipline is not None:
        current = acc_discipline[key]
        acc_discipline[key] = (
            identity.discipline if current is None else min(current, identity.discipline)
        )
