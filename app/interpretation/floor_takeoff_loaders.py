"""Floor takeoff assembler — Phase R-G (issue #721).

DB-seam loader that orchestrates the 5 floor loaders and maps their results to
:class:`~app.schemas.floor_takeoff.FloorTakeoffResponse`.

Pure mapping over the existing loaders — no new bucketing math.

Module-level helper
-------------------
``parse_member(raw)`` parses a single ``"<uuid>:<role>"`` query-param token and is
importable for unit testing without a DB.
"""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.interpretation.floor_counted_loaders import load_counted_fusion
from app.interpretation.floor_estimated_loaders import load_estimated_fusion
from app.interpretation.floor_measured_loaders import load_fused_measured
from app.interpretation.floor_registration import FloorMember
from app.interpretation.floor_registration_loaders import load_floor_registration
from app.schemas.floor_takeoff import (
    EstimatedCircuitItem,
    EstimatedMeta,
    FailedMemberRead,
    FloorTakeoffResponse,
    FloorTakeoffSummary,
    MemberRegistrationRead,
    RoomBlock,
    RoomCountedItem,
    RoomEstimatedBlock,
    RoomMeasuredItem,
    UnassignedBlock,
)

try:
    from app.interpretation.room_fusion_loaders import load_room_registry
except ImportError:
    # Deferred — avoids interpretation→api cycle at module-load time.
    load_room_registry = None  # type: ignore[assignment]


def parse_member(raw: str) -> tuple[UUID, str]:
    """Parse a single ``"<uuid>:<role>"`` query-param token.

    Parameters
    ----------
    raw:
        A string in the form ``"<uuid>:<role>"``.  The UUID portion is everything
        before the first colon; the role is everything after.

    Returns
    -------
    tuple[UUID, str]
        ``(revision_id, role)``.

    Raises
    ------
    ValueError
        If ``raw`` does not contain a colon or the UUID portion is not a valid UUID.
    """
    if ":" not in raw:
        raise ValueError(
            f"Invalid member token {raw!r}: expected '<uuid>:<role>' (colon-separated)"
        )
    idx = raw.index(":")
    uuid_part = raw[:idx]
    role_part = raw[idx + 1 :]
    if not role_part:
        raise ValueError(f"Invalid member token {raw!r}: role is empty")
    try:
        revision_id = UUID(uuid_part)
    except ValueError as exc:
        raise ValueError(
            f"Invalid member token {raw!r}: UUID part {uuid_part!r} is not a valid UUID"
        ) from exc
    return revision_id, role_part


async def assemble_floor_takeoff(
    db: AsyncSession,
    *,
    reference_revision_id: UUID,
    reference_role: str,
    members: list[tuple[UUID, str]],
    containment_revision_id: UUID | None,
    scope: Literal["sheet", "modelspace"],
    strategy: str,
    snap_tolerance: float,
    min_area: float,
    voronoi_fallback: bool,
) -> FloorTakeoffResponse:
    """Assemble a :class:`FloorTakeoffResponse` from the 5 floor loaders.

    Parameters
    ----------
    db:
        Async DB session.
    reference_revision_id:
        UUID of the reference revision (defines the registration frame).
    reference_role:
        Role label for the reference revision.
    members:
        List of ``(revision_id, role)`` tuples for the OTHER member revisions
        (reference excluded — it is handled separately).
    containment_revision_id:
        UUID of the containment sheet, or None for honest-absent home runs.
    scope:
        ``"sheet"`` or ``"modelspace"``.
    strategy:
        Room detection strategy forwarded to ``load_room_registry``.
    snap_tolerance:
        Snap tolerance forwarded to ``load_room_registry``.
    min_area:
        Min area forwarded to ``load_room_registry``.
    voronoi_fallback:
        Whether to enable Voronoi fallback in the room registry.

    Returns
    -------
    FloorTakeoffResponse
        Fully assembled floor takeoff; honest empty absences for all degenerate inputs.

    Raises
    ------
    ValueError
        Propagated from ``load_floor_registration`` on project-mismatch or no-fiducials.
    """
    # --- Step 1: Floor registration ---
    floor_members = [FloorMember(revision_id=rid, role=role) for rid, role in members]
    registration = await load_floor_registration(
        db,
        reference_revision_id=reference_revision_id,
        reference_role=reference_role,
        members=floor_members,
    )

    # --- Step 2: Room registry (reference frame) ---
    # Lazy import to avoid interpretation→api cycle at module level.
    from app.interpretation.room_fusion_loaders import load_room_registry as _load_room_registry

    registry = await _load_room_registry(
        db,
        reference_revision_id,
        strategy=strategy,
        snap_tolerance=snap_tolerance,
        min_area=min_area,
        scope=scope,
        voronoi_fallback=voronoi_fallback,
    )

    # --- Step 3: Measured, counted, estimated (parallel load) ---
    measured_result = await load_fused_measured(
        db,
        registration=registration,
        registry=registry,
        scope=scope,
    )

    counted_result = await load_counted_fusion(
        db,
        registration,
        registry,
        scope=scope,
    )

    estimated_result = await load_estimated_fusion(
        db,
        registration,
        registry,
        lighting_revision_id=reference_revision_id,
        containment_revision_id=containment_revision_id,
        scope=scope,
    )

    # --- Step 4: Union room_numbers across the three result kinds ---
    # Keys for the room-indexed data.
    measured_by_room: dict[str | None, list[RoomMeasuredItem]] = {}
    for item in measured_result.items:
        measured_by_room.setdefault(item.room_number, []).append(
            RoomMeasuredItem(
                member_revision_id=item.member_revision_id,
                role=item.role,
                service=item.service,
                size_raw=item.size_raw,
                size_kind=item.size_kind,
                boundary_basis=item.boundary_basis,
                confidence=item.confidence,
                needs_review=item.needs_review,
                drawing_length=item.drawing_length,
                real_length_m=item.real_length_m,
                basis=item.basis,
                length_provisional=item.length_provisional,
            )
        )

    counted_by_room: dict[str | None, list[RoomCountedItem]] = {}
    for room_number, type_counts in counted_result.counts_by_room.items():
        room_items = [
            RoomCountedItem(type_name=type_name, count=count, source_revision_id=None)
            for type_name, count in type_counts.items()
        ]
        counted_by_room[room_number] = room_items

    estimated_by_room: dict[str | None, RoomEstimatedBlock] = {}
    for contrib in estimated_result.per_room:
        estimated_by_room[contrib.room_number] = RoomEstimatedBlock(
            quantity_kind="estimated",
            device_drop_m=contrib.device_drop_m,
            home_run_m=contrib.home_run_m,
            circuit_ids=list(contrib.circuit_ids),
        )

    # Room metadata: prefer polygon-basis when sources disagree.
    # We collect room metadata from measured items (which carry full room metadata).
    room_meta: dict[
        str,
        tuple[str | None, str | None, str | None, float | None, bool],
    ] = {}  # room_number → (room_id, room_name, boundary_basis, confidence, needs_review)

    for item in (*measured_result.items, *measured_result.unassigned):
        rn = item.room_number
        if rn is None:
            continue
        if rn not in room_meta:
            room_meta[rn] = (
                item.room_id,
                item.room_name,
                item.boundary_basis,
                item.confidence,
                item.needs_review,
            )
        else:
            # Prefer polygon-basis when sources disagree.
            existing_basis = room_meta[rn][2]
            if existing_basis != "polygon" and item.boundary_basis == "polygon":
                room_meta[rn] = (
                    item.room_id,
                    item.room_name,
                    item.boundary_basis,
                    item.confidence,
                    item.needs_review,
                )

    # Collect room metadata from counted assignments (fills counted-only rooms whose
    # room_id/boundary_basis/confidence are not available from measured or estimated).
    # Group by room_number; take the first non-None room_id per room (deterministic
    # because assignments are sorted by source_revision_id inside bucket_counted_devices).
    for assignment in counted_result.assignments:
        rn = assignment.room_number
        if rn is None:
            continue
        if rn not in room_meta:
            room_meta[rn] = (
                assignment.room_id,
                None,  # CountedDeviceAssignment has no room_name field
                assignment.boundary_basis,
                assignment.confidence,
                assignment.needs_review,
            )
        else:
            # Prefer polygon-basis; otherwise keep existing (measured/estimated wins).
            existing_basis = room_meta[rn][2]
            if existing_basis != "polygon" and assignment.boundary_basis == "polygon":
                room_meta[rn] = (
                    assignment.room_id,
                    room_meta[rn][1],  # preserve room_name from prior source
                    assignment.boundary_basis,
                    assignment.confidence,
                    assignment.needs_review,
                )

    # Also collect room metadata from estimated contributions (for rooms with only
    # estimated data — no measured/counted assignments).
    for contrib in estimated_result.per_room:
        rn = contrib.room_number
        if rn is None:
            continue
        if rn not in room_meta:
            room_meta[rn] = (
                contrib.room_id,
                None,
                contrib.boundary_basis,
                contrib.confidence,
                contrib.needs_review,
            )

    # Union of ALL room_numbers across the three kinds (excluding None = unassigned).
    all_room_numbers: set[str] = set()
    for rn in measured_by_room:
        if rn is not None:
            all_room_numbers.add(rn)
    for rn in counted_by_room:
        if rn is not None:
            all_room_numbers.add(rn)
    for rn in estimated_by_room:
        if rn is not None:
            all_room_numbers.add(rn)

    # --- Step 5: Build RoomBlock list, sorted by room_number ---
    room_blocks: list[RoomBlock] = []
    for rn in sorted(all_room_numbers):
        meta = room_meta.get(rn, (None, None, None, None, False))
        room_id, room_name, boundary_basis, confidence, needs_review = meta
        room_blocks.append(
            RoomBlock(
                room_id=room_id,
                room_number=rn,
                room_name=room_name,
                boundary_basis=boundary_basis,
                confidence=confidence,
                needs_review=needs_review,
                measured=measured_by_room.get(rn, []),
                counted=counted_by_room.get(rn, []),
                estimated=estimated_by_room.get(rn),
            )
        )

    # --- Step 6: Build unassigned block ---
    unassigned_measured = [
        RoomMeasuredItem(
            member_revision_id=item.member_revision_id,
            role=item.role,
            service=item.service,
            size_raw=item.size_raw,
            size_kind=item.size_kind,
            boundary_basis=item.boundary_basis,
            confidence=item.confidence,
            needs_review=item.needs_review,
            drawing_length=item.drawing_length,
            real_length_m=item.real_length_m,
            basis=item.basis,
            length_provisional=item.length_provisional,
        )
        for item in measured_result.unassigned
    ]
    unassigned_counted = counted_by_room.get(None, [])
    unassigned_est_contrib = estimated_result.unassigned
    unassigned_estimated: RoomEstimatedBlock | None = None
    _has_unassigned_est = (
        unassigned_est_contrib.device_drop_m > 0.0
        or unassigned_est_contrib.home_run_m > 0.0
        or bool(unassigned_est_contrib.circuit_ids)
    )
    if _has_unassigned_est:
        unassigned_estimated = RoomEstimatedBlock(
            quantity_kind="estimated",
            device_drop_m=unassigned_est_contrib.device_drop_m,
            home_run_m=unassigned_est_contrib.home_run_m,
            circuit_ids=list(unassigned_est_contrib.circuit_ids),
        )

    unassigned_block = UnassignedBlock(
        measured=unassigned_measured,
        counted=unassigned_counted,
        estimated=unassigned_estimated,
    )

    # --- Step 7: Floor-level estimated circuits (top-level) ---
    estimated_circuits = [
        EstimatedCircuitItem(
            circuit_id=ec.circuit_id,
            in_plan_length_m=ec.in_plan_length_m,
            rooms_touched=list(ec.rooms_touched),
        )
        for ec in estimated_result.estimated_circuits
    ]

    # --- Step 8: Estimated meta (floor-level) ---
    estimated_meta = EstimatedMeta(
        quantity_kind=estimated_result.quantity_kind,
        reliability=estimated_result.reliability,
        params_stamp=dict(estimated_result.params_stamp),
        source=estimated_result.source,
    )

    # --- Step 9: Members + failed members from registration ---
    members_read: list[MemberRegistrationRead] = []
    for m in registration.members_ok:
        audit = m.audit
        members_read.append(
            MemberRegistrationRead(
                revision_id=m.revision_id,
                role=m.role,
                quality=str(audit.get("quality", "")),
                dx=float(audit.get("dx", 0.0)),  # type: ignore[arg-type]
                dy=float(audit.get("dy", 0.0)),  # type: ignore[arg-type]
                rotation_rad=float(audit.get("rotation_rad", 0.0)),  # type: ignore[arg-type]
                scale=float(audit.get("scale", 1.0)),  # type: ignore[arg-type]
                matched_count=_int_or_zero(audit.get("matched_count")),
                median_residual_m=_float_or_none(audit.get("median_residual_m")),
                max_residual_m=_float_or_none(audit.get("max_residual_m")),
            )
        )

    failed_members_read: list[FailedMemberRead] = [
        FailedMemberRead(
            revision_id=m.revision_id,
            role=m.role,
            reason=m.reason,
        )
        for m in registration.members_failed
    ]

    # --- Step 10: Summary ---
    # Voronoi fallback rooms: rooms where any measured item has boundary_basis="voronoi"
    voronoi_room_set: set[str] = set()
    for item in measured_result.items:
        if item.boundary_basis == "voronoi" and item.room_number is not None:
            voronoi_room_set.add(item.room_number)

    total_home_run = sum(
        (rb.estimated.home_run_m if rb.estimated else 0.0) for rb in room_blocks
    ) + (unassigned_block.estimated.home_run_m if unassigned_block.estimated else 0.0)
    unassigned_home_run = (
        unassigned_block.estimated.home_run_m if unassigned_block.estimated else 0.0
    )
    no_anchor_fraction = (unassigned_home_run / total_home_run) if total_home_run > 0.0 else 0.0

    total_measured_m_by_discipline: dict[str, float] = {}
    for item in measured_result.items:
        if item.basis == "real_world" and item.real_length_m is not None:
            total_measured_m_by_discipline[item.role] = (
                total_measured_m_by_discipline.get(item.role, 0.0) + item.real_length_m
            )

    summary = FloorTakeoffSummary(
        rooms=len(room_blocks),
        named_rooms=sum(1 for rb in room_blocks if rb.room_name is not None),
        members_registered=len(members_read),
        members_failed=len(failed_members_read),
        voronoi_fallback_rooms=len(voronoi_room_set),
        unscaled=measured_result.unscaled,
        no_anchor_fraction=no_anchor_fraction,
        total_measured_m_by_discipline=total_measured_m_by_discipline,
    )

    return FloorTakeoffResponse(
        reference_revision_id=reference_revision_id,
        reference_role=reference_role,
        members=members_read,
        members_failed=failed_members_read,
        rooms=room_blocks,
        unassigned=unassigned_block,
        estimated_circuits=estimated_circuits,
        estimated_meta=estimated_meta,
        room_registry_strategy=strategy,
        voronoi_fallback_enabled=voronoi_fallback,
        summary=summary,
    )


def _float_or_none(val: object) -> float | None:
    """Convert a value to float, returning None for None input."""
    if val is None:
        return None
    return float(val)  # type: ignore[arg-type]


def _int_or_zero(val: object) -> int:
    """Convert a value to int, returning 0 for None input."""
    if val is None:
        return 0
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)
    if isinstance(val, str):
        return int(val)
    return 0
