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

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from app.interpretation.geometry import point_in_polygon
from app.interpretation.measurement import (
    ScaleContext,
    measure_length,
    path_length,
)
from app.interpretation.rise_drop import RiseDropSymbol
from app.interpretation.rooms import Room
from app.interpretation.routed_runs import RunGroup
from app.interpretation.run_service_identity import (
    IDENTITY_PARTIAL,
    IDENTITY_RESOLVED,
    IDENTITY_UNKNOWN,
    RunServiceIdentity,
)

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
        s = geometry["start"]
        e = geometry["end"]
        if len(s) >= 2 and len(e) >= 2:
            return ((s[0] + e[0]) / 2.0, (s[1] + e[1]) / 2.0)

    pts: Any = geometry.get("vertices") or geometry.get("points")
    if pts and len(pts) >= 1:
        xs = [p[0] for p in pts if len(p) >= 2]
        ys = [p[1] for p in pts if len(p) >= 2]
        if xs:
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
            s = geom["start"]
            e = geom["end"]
            if len(s) >= 2 and len(e) >= 2:
                total += path_length([(s[0], s[1]), (e[0], e[1])])
            continue

        pts: Any = geom.get("vertices") or geom.get("points")
        if pts:
            points = [(p[0], p[1]) for p in pts if len(p) >= 2]
            total += path_length(points)
            continue

        # arc or unrecognised -- contributes 0; see module docstring
    return total


def _containing_room(point: tuple[float, float], rooms: Sequence[Room]) -> Room | None:
    """Smallest polygon room whose polygon contains ``point``.

    Label-only rooms (no polygon) are excluded -- they have no boundary to test.
    Secondary key on ``r.id`` ensures the result is permutation-invariant when two
    rooms share exactly the same area.
    """
    containing = [
        room
        for room in rooms
        if room.polygon is not None
        and room.area is not None
        and point_in_polygon(point, room.polygon)
    ]
    if not containing:
        return None
    return min(containing, key=lambda r: (r.area if r.area is not None else 0.0, r.id))


def _worst_status(a: str, b: str) -> str:
    """Return the status with the lower rank (worse)."""
    return a if _STATUS_RANK.get(a, 0) <= _STATUS_RANK.get(b, 0) else b


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
) -> ServiceTakeoffResult:
    """Compose run identities into per-(service, size, room) length totals.

    Matches each identity to its RunGroup by VALUE of (layer_ref, colour_key).
    Uses the bundle-length model: each service in a multi-service identity receives
    the full drawn length of the run (see module docstring).  A run with no services
    is counted in the SERVICE_UNKNOWN bucket; a run whose anchor falls outside all
    rooms is counted in ROOM_UNASSIGNED_ID.  No run is ever dropped.

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

    # Accumulators keyed by (service, size_raw, size_kind, room_id).
    # Each bucket stores a mutable working state before conversion to ServiceTakeoffLine.
    # Type alias kept at module scope via _BucketKey below.
    acc_drawing_length: dict[_BucketKey, float] = {}
    acc_run_count: dict[_BucketKey, int] = {}
    acc_entity_ids: dict[_BucketKey, set[str]] = {}
    acc_identity_status: dict[_BucketKey, str] = {}
    # True if any contributing run has competing_disciplines
    acc_has_conflict: dict[_BucketKey, bool] = {}
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
        # Prefer materialized skeleton length when the group has been computed;
        # fall back to naive entity-sum when the key is absent (or mapping is None).
        run_key = (identity.layer_ref, identity.colour_key)
        if measured_length_by_group is not None and run_key in measured_length_by_group:
            drawn = measured_length_by_group[run_key]
        else:
            drawn = _entity_drawn_length(identity.entity_ids, geometry_by_entity_id)

        # Compute run anchor for room scoping.
        anchor = _run_anchor(identity.entity_ids, geometry_by_entity_id)
        room = _containing_room(anchor, rooms) if anchor is not None else None

        if room is None:
            room_id = ROOM_UNASSIGNED_ID
            room_name: str | None = None
            room_number: str | None = None
            unassigned_run_count += 1
        else:
            room_id = room.id
            room_name = room.name
            room_number = room.number

        # Determine competing_disciplines from the matched RunGroup.
        matched_run = runs_by_key.get(run_key)
        has_conflict = bool(matched_run and matched_run.competing_disciplines)

        if not identity.services:
            # No services -> SERVICE_UNKNOWN bucket.
            unknown_service_run_count += 1
            key: _BucketKey = (SERVICE_UNKNOWN, None, None, room_id)
            _accumulate(
                key=key,
                drawn=drawn,
                identity=identity,
                has_conflict=has_conflict,
                room_name=room_name,
                room_number=room_number,
                acc_drawing_length=acc_drawing_length,
                acc_run_count=acc_run_count,
                acc_entity_ids=acc_entity_ids,
                acc_identity_status=acc_identity_status,
                acc_has_conflict=acc_has_conflict,
                acc_room_name=acc_room_name,
                acc_room_number=acc_room_number,
                acc_discipline=acc_discipline,
            )
        else:
            # Bundle-length model: attribute FULL drawn length to EACH service.
            for ss in identity.services:
                key = (ss.service, ss.size.raw, ss.size.kind, room_id)
                _accumulate(
                    key=key,
                    drawn=drawn,
                    identity=identity,
                    has_conflict=has_conflict,
                    room_name=room_name,
                    room_number=room_number,
                    acc_drawing_length=acc_drawing_length,
                    acc_run_count=acc_run_count,
                    acc_entity_ids=acc_entity_ids,
                    acc_identity_status=acc_identity_status,
                    acc_has_conflict=acc_has_conflict,
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
    room_name: str | None,
    room_number: str | None,
    acc_drawing_length: dict[_BucketKey, float],
    acc_run_count: dict[_BucketKey, int],
    acc_entity_ids: dict[_BucketKey, set[str]],
    acc_identity_status: dict[_BucketKey, str],
    acc_has_conflict: dict[_BucketKey, bool],
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
        acc_room_name[key] = room_name
        acc_room_number[key] = room_number
        acc_discipline[key] = identity.discipline

    acc_drawing_length[key] += drawn
    acc_run_count[key] += 1
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
