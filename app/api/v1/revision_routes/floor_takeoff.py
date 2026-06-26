"""Per-floor fused multi-discipline quantity takeoff route (issue #721, Phase R-G).

GET /floors/takeoff

Compute-on-read: no persistence, no migration. All five floor loaders are invoked
and their results are assembled into a :class:`FloorTakeoffResponse`.

The route is mounted inside ``revisions_router`` so its operationId is derived
from the handler name ``get_floor_takeoff``.

Fail-loud rules
---------------
- 404: any revision id not found or not active (pre-validated before registration).
- 409: entities not materialized for a revision (_get_active_revision_manifest_or_409).
- 422: member parse error; reference not in member list; fewer than 2 distinct members;
  containment_revision_id supplied but not listed as a member; ``load_floor_registration``
  ValueError (project-mismatch / no-reference-fiducials); containment supplied +
  registration quality != "good".
- 200 honest absence: degenerate input (no rooms → all in unassigned) is a valid 200.
"""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.revision_lineage import _get_active_revision_manifest_or_409
from app.db.session import get_db
from app.interpretation.floor_takeoff_loaders import assemble_floor_takeoff, parse_member
from app.schemas.floor_takeoff import FloorTakeoffResponse

floor_takeoff_router = APIRouter()

ServiceTakeoffScope = Literal["sheet", "modelspace"]


@floor_takeoff_router.get(
    "/floors/takeoff",
    response_model=FloorTakeoffResponse,
)
async def get_floor_takeoff(
    db: Annotated[AsyncSession, Depends(get_db)],
    reference_revision_id: Annotated[UUID, Query()],
    member: Annotated[list[str], Query()] = [],  # noqa: B006
    containment_revision_id: Annotated[UUID | None, Query()] = None,
    scope: Annotated[ServiceTakeoffScope, Query()] = "sheet",
    strategy: Annotated[str, Query()] = "auto",
    snap_tolerance: Annotated[float, Query(ge=0)] = 0.0,
    min_area: Annotated[float, Query(ge=0)] = 0.0,
    voronoi_fallback: Annotated[bool, Query()] = True,
) -> FloorTakeoffResponse:
    """Fused per-room multi-discipline quantity takeoff across a floor's discipline sheets.

    Builds a per-room takeoff by co-registering all supplied ``member`` revisions into
    the ``reference_revision_id`` coordinate frame, then fusing measured (centerline
    lengths), counted (device tallies), and estimated (cable circuit) quantities by
    room.

    ``member`` is a repeated query parameter in the form ``<uuid>:<role>``
    (e.g. ``?member=<uuid>:containment&member=<uuid>:power``). The
    ``reference_revision_id`` MUST appear in the ``member`` list so its role can be
    determined (HTTP 422 otherwise). At least 2 distinct member UUIDs are required.

    ``containment_revision_id``: when supplied, home-run routing is performed along
    the tray centerline. It MUST also be listed as a ``member=`` token (HTTP 422 if
    not). Its registration quality must be "good" (HTTP 422 if not). When omitted,
    every circuit receives an honest "no_containment_sheet" status.

    HTTP 409 is returned when entities for any revision have not been materialized
    (raised by the pre-validation manifest check).

    Honest degradation: degenerate input (e.g. no rooms detected) yields a 200 with
    all items in the ``unassigned`` block — never a 500.
    """
    # --- Step 1: Parse member tokens ---
    parsed_members: list[tuple[UUID, str]] = []
    for raw in member:
        try:
            parsed_members.append(parse_member(raw))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    # --- Step 2: Validate reference is in member list ---
    member_ids = {rid for rid, _ in parsed_members}
    if reference_revision_id not in member_ids:
        raise HTTPException(
            status_code=422,
            detail=(
                f"reference_revision_id {reference_revision_id} must appear in the member list "
                "so its role can be determined. "
                "Add it as e.g. ?member=<uuid>:<role>."
            ),
        )

    # --- Step 3: At least 2 distinct members required ---
    if len(member_ids) < 2:
        raise HTTPException(
            status_code=422,
            detail=(
                f"At least 2 distinct member revision UUIDs are required; "
                f"got {len(member_ids)}. "
                "Supply the reference and at least one additional discipline sheet."
            ),
        )

    # --- Step 3b: containment_revision_id must also be in the member list ---
    # Without this guard, the cable-pair quality gate (post-assembly) would find
    # containment_member=None and short-circuit to pass, silently producing unreliable
    # home-run data.  Requiring containment to be a listed member ensures the quality
    # gate always has an audit entry to evaluate.
    if containment_revision_id is not None and containment_revision_id not in member_ids:
        raise HTTPException(
            status_code=422,
            detail=(
                f"containment_revision_id {containment_revision_id} must also be listed as a "
                "member token (e.g. ?member=<uuid>:<role>) so it can be co-registered and its "
                "registration quality evaluated."
            ),
        )

    # --- Step 4: Extract reference role; other members are the non-reference entries ---
    reference_role: str | None = None
    other_members: list[tuple[UUID, str]] = []
    for rid, role in parsed_members:
        if rid == reference_revision_id:
            reference_role = role  # last occurrence wins if duplicated
        else:
            other_members.append((rid, role))

    assert reference_role is not None  # guaranteed by step 2

    # --- Step 5: Pre-validate all revision ids (404 before loader's ValueError) ---
    await _get_active_revision_manifest_or_409(reference_revision_id, db)
    for rid, _ in other_members:
        await _get_active_revision_manifest_or_409(rid, db)
    if containment_revision_id is not None:
        await _get_active_revision_manifest_or_409(containment_revision_id, db)

    # --- Step 6: Assemble (ValueError → 422) ---
    try:
        response = await assemble_floor_takeoff(
            db,
            reference_revision_id=reference_revision_id,
            reference_role=reference_role,
            members=other_members,
            containment_revision_id=containment_revision_id,
            scope=scope,
            strategy=strategy,
            snap_tolerance=snap_tolerance,
            min_area=min_area,
            voronoi_fallback=voronoi_fallback,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # --- Step 7: Cable-pair quality gate (mirror cable_estimate route) ---
    # Intentionally post-assembly: registration runs inside assemble_floor_takeoff and
    # its audit results are only available after the call returns.  Step 3b's cheap
    # pre-assembly check (containment must be a listed member) guarantees that
    # containment_member is always found here when containment_revision_id is supplied.
    if containment_revision_id is not None and response.estimated_meta is not None:
        containment_member = next(
            (m for m in response.members if m.revision_id == containment_revision_id),
            None,
        )
        if containment_member is not None and containment_member.quality != "good":
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Containment registration quality is {containment_member.quality!r} — "
                    "cannot produce reliable home-run lengths. "
                    f"matched_count={containment_member.matched_count}, "
                    f"median_residual_m={containment_member.median_residual_m}. "
                    "Provide a containment sheet that shares grid fiducials "
                    "with the reference sheet."
                ),
            )

    return response
