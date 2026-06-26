"""Per-room measured contribution for fused floor takeoff (issue #718, Phase R-D).

Pure module — stdlib + shapely + app.interpretation.{measurement,room_fusion,room_partition}
ONLY. No DB, ORM, FastAPI, cv2, or skimage.

Algorithm
---------
For each ``(layer_ref, colour_key)`` group of persisted centerline polylines:

1. **Polygon clip** — ``partition_polylines_by_room(polylines, registry.polygon_rooms,
   label_radius=None)`` distributes the group's drawn length across polygon rooms.
   ``label_radius=None`` is REQUIRED: the primitive leaves the outside-polygon remainder
   as one ``(None, remainder)`` bucket so the registry (not the primitive) owns all Voronoi
   attribution.  Each polygon bucket → ``boundary_basis="polygon"``, the room's own
   confidence.

2. **Voronoi residual** — when ``registry`` has voronoi enabled, the outside-polygon
   remainder is geometrically re-computed (shapely difference), each straight segment's
   midpoint is passed to ``registry.classify``, and the segment length is bucketed to the
   returned room number with ``boundary_basis="voronoi"``.  Segments whose classify→None
   remain unassigned.  When Voronoi is **disabled** the remainder is left entirely
   unassigned and EVERY polygon bucket is byte-identical (the keystone invariant).

3. **Scale gate** — each bucket's drawing length is passed through
   ``measure_length(drawing_length, scale)``; ``real_length_m=None``,
   ``length_provisional=True``, and ``basis="drawing_units_only"`` when scale is
   unconfirmed.

4. **Bundle model** — a group with >1 service attributes the FULL per-room length to EACH
   service (do NOT divide).  ``SERVICE_UNKNOWN`` is used when no services are present for
   the group.

Result items are sorted deterministically; the function is permutation-invariant under
group-map ordering and polyline ordering within each group.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass

from shapely.geometry import LineString, MultiLineString

from app.interpretation.measurement import ScaleContext, measure_length
from app.interpretation.room_fusion import DEFAULT_VORONOI_CONFIDENCE, RoomRegistry
from app.interpretation.room_partition import _CLIP_EPS, partition_polylines_by_room
from app.interpretation.rooms import Room

# ---------------------------------------------------------------------------
# Constants — frozen interface contract
# ---------------------------------------------------------------------------

SERVICE_UNKNOWN: str = "unknown"
"""Sentinel service name used when a group carries no service identity."""

# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FusedMeasuredItem:
    """One (service, size, room) length bucket from the measured centerline geometry."""

    member_revision_id: str
    role: str
    service: str
    size_raw: str | None
    size_kind: str | None
    room_id: str | None  # None → unassigned
    room_number: str | None
    room_name: str | None
    boundary_basis: str | None  # "polygon" | "voronoi" | None
    confidence: float | None
    needs_review: bool
    drawing_length: float
    real_length_m: float | None
    basis: str  # "real_world" | "drawing_units_only"
    length_provisional: bool


@dataclass(frozen=True, slots=True)
class FusedMeasuredResult:
    """Immutable takeoff result from fused multi-member measured geometry."""

    items: tuple[FusedMeasuredItem, ...]  # room-assigned (polygon + voronoi)
    unassigned: tuple[FusedMeasuredItem, ...]  # room_id None
    unscaled: bool


# ---------------------------------------------------------------------------
# Sort key — deterministic, permutation-invariant
# ---------------------------------------------------------------------------

_SORT_KEY_UNASSIGNED = "~"  # sorts last after any real room_id


def _item_sort_key(
    item: FusedMeasuredItem,
) -> tuple[str, str, str, str, str, str, str]:
    return (
        item.member_revision_id,
        item.role,
        item.service,
        item.size_raw or "",
        item.size_kind or "",
        item.room_number or "",
        item.room_id or _SORT_KEY_UNASSIGNED,
    )


# ---------------------------------------------------------------------------
# Internal helper — Voronoi residual distribution
# ---------------------------------------------------------------------------


def _voronoi_distribute(
    *,
    polylines: tuple[tuple[tuple[float, float], ...], ...],
    registry: RoomRegistry,
    member_revision_id: str,
    role: str,
    service: str,
    size_raw: str | None,
    size_kind: str | None,
    scale: ScaleContext,
    length_provisional: bool,
) -> tuple[list[FusedMeasuredItem], list[FusedMeasuredItem]]:
    """Distribute the Voronoi residual (outside-polygon remainder) across rooms.

    Returns ``(assigned_items, unassigned_items)``.  Caller has already established
    that ``registry._voronoi_fallback`` is True before calling this function.

    Uses the shapely difference of the group's MultiLineString against the union
    of all polygon-room polygons.  Each straight segment's midpoint is classified via
    ``registry.classify``; its length contributes to the matching room or to the
    unassigned bucket when classify returns no room.
    """
    lines = [LineString(pl) for pl in polylines if len(pl) >= 2]
    if not lines:
        return [], []

    full_geom: MultiLineString | LineString = MultiLineString(lines) if len(lines) > 1 else lines[0]

    # Build the union of polygon-room polygons.
    polygon_rooms = registry.polygon_rooms
    if polygon_rooms:
        from shapely.ops import unary_union as _unary_union

        poly_union = _unary_union([r.polygon for r in polygon_rooms if r.polygon is not None])
        remainder = full_geom.difference(poly_union)
    else:
        remainder = full_geom

    if remainder.is_empty:
        return [], []

    # Walk each straight segment of the remainder.
    # Bucket: room_number -> (room_id, room_name, boundary_basis, confidence, needs_review, length)
    room_buckets: dict[str, tuple[str | None, str | None, str, float | None, bool, float]] = {}
    unassigned_length = 0.0

    # Normalise the remainder to a list of LineStrings. shapely's difference() returns a
    # LineString or MultiLineString for typical centerline input, but a GEOS topology edge
    # case can yield a GeometryCollection (a stray boundary point alongside the lines) — its
    # parts have no .coords, so handle only line geometries and drop the rest explicitly
    # rather than letting an AttributeError be swallowed by the loader's per-member guard.
    if remainder.geom_type == "MultiLineString":
        geoms = list(remainder.geoms)
    elif remainder.geom_type == "LineString":
        geoms = [remainder]
    elif remainder.geom_type == "GeometryCollection":
        geoms = [g for g in remainder.geoms if g.geom_type == "LineString"]
    else:
        geoms = []
    for line in geoms:
        coords = list(line.coords)
        for i in range(len(coords) - 1):
            x0, y0 = coords[i][0], coords[i][1]
            x1, y1 = coords[i + 1][0], coords[i + 1][1]
            seg_len = math.hypot(x1 - x0, y1 - y0)
            if seg_len <= _CLIP_EPS:
                continue
            midpoint = ((x0 + x1) / 2.0, (y0 + y1) / 2.0)
            assignment = registry.classify(midpoint)
            if assignment.room_number is None:
                unassigned_length += seg_len
            else:
                rn = assignment.room_number
                if rn in room_buckets:
                    # Accumulate length; keep first-seen metadata (all same room).
                    rid, rname, bb, conf, nr, prev_len = room_buckets[rn]
                    room_buckets[rn] = (rid, rname, bb, conf, nr, prev_len + seg_len)
                else:
                    room_buckets[rn] = (
                        assignment.room_id,
                        assignment.room_name,
                        assignment.boundary_basis or "voronoi",
                        assignment.confidence
                        if assignment.confidence is not None
                        else DEFAULT_VORONOI_CONFIDENCE,
                        assignment.needs_review,
                        seg_len,
                    )

    assigned: list[FusedMeasuredItem] = []
    for room_number, bucket_val in room_buckets.items():
        room_id, room_name, boundary_basis, confidence, needs_review, total_len = bucket_val
        if total_len <= _CLIP_EPS:
            continue
        m = measure_length(total_len, scale)
        assigned.append(
            FusedMeasuredItem(
                member_revision_id=member_revision_id,
                role=role,
                service=service,
                size_raw=size_raw,
                size_kind=size_kind,
                room_id=room_id,
                room_number=room_number,
                room_name=room_name,
                boundary_basis=boundary_basis,
                confidence=confidence,
                needs_review=needs_review,
                drawing_length=m.drawing_length,
                real_length_m=m.real_length_m,
                basis=m.basis,
                length_provisional=length_provisional,
            )
        )

    unassigned: list[FusedMeasuredItem] = []
    if unassigned_length > _CLIP_EPS:
        m = measure_length(unassigned_length, scale)
        unassigned.append(
            FusedMeasuredItem(
                member_revision_id=member_revision_id,
                role=role,
                service=service,
                size_raw=size_raw,
                size_kind=size_kind,
                room_id=None,
                room_number=None,
                room_name=None,
                boundary_basis=None,
                confidence=None,
                needs_review=False,
                drawing_length=m.drawing_length,
                real_length_m=m.real_length_m,
                basis=m.basis,
                length_provisional=length_provisional,
            )
        )

    return assigned, unassigned


# ---------------------------------------------------------------------------
# Public coordinator
# ---------------------------------------------------------------------------


def bucket_measured_for_member(
    *,
    member_revision_id: str,
    role: str,
    polylines_by_group: Mapping[
        tuple[str | None, str | None],
        tuple[tuple[tuple[float, float], ...], ...],
    ],
    services_by_group: Mapping[
        tuple[str | None, str | None],
        tuple[tuple[str, str | None, str | None], ...],
    ],
    registry: RoomRegistry,
    scale: ScaleContext,
) -> FusedMeasuredResult:
    """Bucket persisted centerline polylines into per-(service, size, room) length items.

    Parameters
    ----------
    member_revision_id:
        The UUID string of the drawing revision this geometry came from.
    role:
        Caller-defined role label (e.g. "containment", "power").
    polylines_by_group:
        ``(layer_ref, colour_key) -> polylines`` mapping.  Polylines are ALREADY
        transformed into the reference frame by the caller; this function does NOT
        apply any coordinate transform.
    services_by_group:
        ``(layer_ref, colour_key) -> ((service, size_raw, size_kind), ...)`` mapping.
        Empty tuple means no services resolved for this group → SERVICE_UNKNOWN used.
    registry:
        Frozen :class:`~app.interpretation.room_fusion.RoomRegistry` owning the
        polygon-first + Voronoi-fallback classification.
    scale:
        :class:`~app.interpretation.measurement.ScaleContext` driving the scale gate.

    Returns
    -------
    FusedMeasuredResult
        ``items`` are room-assigned; ``unassigned`` have ``room_id=None``.
        Both are sorted deterministically.  ``unscaled=True`` when any item's
        ``basis`` is ``"drawing_units_only"``.
    """
    # length_provisional: always False for the measured path — geometry is materialized
    # centerlines, not provisional double-line entity sums.
    length_provisional = False

    assigned_items: list[FusedMeasuredItem] = []
    unassigned_items: list[FusedMeasuredItem] = []

    for group_key, polylines in polylines_by_group.items():
        if not polylines:
            continue

        service_tuples = services_by_group.get(group_key, ())
        if not service_tuples:
            service_tuples = ((SERVICE_UNKNOWN, None, None),)

        # --- Step 1: Polygon clip (label_radius=None so remainder is one bucket) ---
        partitions = partition_polylines_by_room(
            polylines, registry.polygon_rooms, label_radius=None
        )

        # Separate polygon assignments from the remainder bucket.
        polygon_partitions: list[tuple[Room, float]] = []
        remainder_length: float = 0.0
        for room, length in partitions:
            if room is None:
                remainder_length += length
            else:
                polygon_partitions.append((room, length))

        # --- Step 2: Voronoi residual (only when voronoi enabled) ---
        voronoi_assigned: list[FusedMeasuredItem] = []
        voronoi_unassigned: list[FusedMeasuredItem] = []
        if registry._voronoi_fallback and remainder_length > _CLIP_EPS:  # noqa: SLF001  # private field: voronoi gate
            for svc, size_raw, size_kind in service_tuples:
                va, vu = _voronoi_distribute(
                    polylines=polylines,
                    registry=registry,
                    member_revision_id=member_revision_id,
                    role=role,
                    service=svc,
                    size_raw=size_raw,
                    size_kind=size_kind,
                    scale=scale,
                    length_provisional=length_provisional,
                )
                voronoi_assigned.extend(va)
                voronoi_unassigned.extend(vu)
        elif remainder_length > _CLIP_EPS:
            # Voronoi disabled — remainder stays entirely unassigned.
            m = measure_length(remainder_length, scale)
            for svc, size_raw, size_kind in service_tuples:
                unassigned_items.append(
                    FusedMeasuredItem(
                        member_revision_id=member_revision_id,
                        role=role,
                        service=svc,
                        size_raw=size_raw,
                        size_kind=size_kind,
                        room_id=None,
                        room_number=None,
                        room_name=None,
                        boundary_basis=None,
                        confidence=None,
                        needs_review=False,
                        drawing_length=m.drawing_length,
                        real_length_m=m.real_length_m,
                        basis=m.basis,
                        length_provisional=length_provisional,
                    )
                )

        # --- Step 3 + 4: Scale-gate polygon buckets x bundle model ---
        for r, drawing_len in polygon_partitions:
            m = measure_length(drawing_len, scale)
            for svc, size_raw, size_kind in service_tuples:
                assigned_items.append(
                    FusedMeasuredItem(
                        member_revision_id=member_revision_id,
                        role=role,
                        service=svc,
                        size_raw=size_raw,
                        size_kind=size_kind,
                        room_id=r.id,
                        room_number=r.number,
                        room_name=r.name,
                        boundary_basis="polygon",
                        confidence=r.confidence,
                        needs_review=r.needs_review,
                        drawing_length=m.drawing_length,
                        real_length_m=m.real_length_m,
                        basis=m.basis,
                        length_provisional=length_provisional,
                    )
                )

        assigned_items.extend(voronoi_assigned)
        unassigned_items.extend(voronoi_unassigned)

    # Deterministic sort.
    assigned_items.sort(key=_item_sort_key)
    unassigned_items.sort(key=_item_sort_key)

    has_unscaled = any(
        item.basis == "drawing_units_only" for item in (*assigned_items, *unassigned_items)
    )

    return FusedMeasuredResult(
        items=tuple(assigned_items),
        unassigned=tuple(unassigned_items),
        unscaled=has_unscaled,
    )
