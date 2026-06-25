"""Cable-estimate compute-on-read route (issue #698b).

ADR-005: compute-on-read only. No persistence.

GET /revisions/{revision_id}/cable-estimate

The primary revision is the lighting sheet. When containment_revision_id is supplied
the two-revision coordinator co-registers the containment geometry and routes home runs
along the tray centerline. When not supplied every circuit receives status
"no_containment_sheet" (honest absence, not a routing failure).
"""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.revision_lineage import _get_active_revision_manifest_or_409
from app.db.session import get_db
from app.interpretation.cable_circuits import partition_circuits
from app.interpretation.cable_estimate import estimate_cable_in_plan
from app.interpretation.cable_estimate_assembly import assemble_cable_estimate
from app.interpretation.cable_estimate_params import default_estimate_params
from app.interpretation.cable_home_run import HomeRunResult, no_containment_home_run_result
from app.interpretation.cable_home_run_loaders import load_and_compute_home_runs
from app.interpretation.cable_topology import build_cable_graph
from app.interpretation.cable_topology_loaders import load_device_footprints, load_spline_inputs
from app.schemas.cable_estimate import CableEstimateResponse, CircuitCableEstimateRead

cable_estimate_router = APIRouter()

ServiceTakeoffScope = Literal["sheet", "modelspace"]


@cable_estimate_router.get(
    "/revisions/{revision_id}/cable-estimate",
    response_model=CableEstimateResponse,
)
async def get_revision_cable_estimate(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    containment_revision_id: Annotated[UUID | None, Query()] = None,
    scope: Annotated[ServiceTakeoffScope, Query()] = "sheet",
) -> CableEstimateResponse:
    """Compute the cable-length estimate for a lighting drawing revision (compute-on-read).

    Builds per-circuit estimates from device drops (reliable), in-plan spline lengths
    (schematic/provisional), and optionally home-run routing along a cable-tray containment
    sheet (estimated_routed). All inputs are loaded from the canonical entity store; nothing
    is persisted.

    Honest degradation: a revision with no splines or devices yields empty circuits and an
    honest empty estimate (200). Never 500 on degenerate input.

    ``scope`` restricts geometry to the printed sheet (``sheet``, default) or interprets the
    full modelspace (``modelspace``).

    ``containment_revision_id``: when supplied, the containment sheet's tray centerlines are
    registered into the lighting frame and home-run routing is performed. The two revisions
    must belong to the same project (HTTP 422 if not). A registration quality below "good" is
    also rejected with HTTP 422 — an explicit pairing that cannot be registered must not
    silently produce wrong lengths.
    """
    # Step 1: load primary (lighting) manifest — 409 if not active.
    await _get_active_revision_manifest_or_409(revision_id, db)
    exclude_off_sheet = scope == "sheet"

    # Step 2: build lighting circuits + in-plan estimate.
    splines = await load_spline_inputs(db, revision_id, exclude_off_sheet=exclude_off_sheet)
    devices = await load_device_footprints(db, revision_id, exclude_off_sheet=False)

    graph = build_cable_graph(splines=splines, devices=devices)
    circuits = partition_circuits(graph)

    params = default_estimate_params()
    in_plan = estimate_cable_in_plan(
        circuits=circuits,
        splines=splines,
        devices=devices,
        params=params,
    )

    # Step 3: home-run computation (or honest-absent placeholder).
    home_run: HomeRunResult
    registration_audit: dict[str, object] | None

    if containment_revision_id is None:
        # No containment sheet supplied — return honest-absent result.
        circuit_ids = [c.circuit_id for c in circuits.circuits]
        home_run = no_containment_home_run_result(circuit_ids)
        registration_audit = None
    else:
        # Validate containment revision exists and is visible (404 if not).
        await _get_active_revision_manifest_or_409(containment_revision_id, db)

        try:
            home_run, registration_audit = await load_and_compute_home_runs(
                db,
                lighting_revision_id=revision_id,
                containment_revision_id=containment_revision_id,
                exclude_off_sheet=exclude_off_sheet,
            )
        except ValueError as exc:
            # Coordinator raises ValueError when revisions belong to different projects.
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        # Fail loud: an explicit pairing with bad registration must not silently produce
        # wrong home-run lengths. The architect gate: quality must be "good".
        if registration_audit.get("quality") != "good":
            quality = registration_audit.get("quality")
            matched_count = registration_audit.get("matched_count")
            median_residual_m = registration_audit.get("median_residual_m")
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Containment registration quality is {quality!r} — cannot produce reliable "
                    f"home-run lengths. matched_count={matched_count}, "
                    f"median_residual_m={median_residual_m}. "
                    "Provide a containment sheet that shares grid fiducials "
                    "with the lighting sheet."
                ),
            )

    # Step 4: assemble the full cable estimate.
    assembly = assemble_cable_estimate(
        in_plan=in_plan,
        home_run=home_run,
        params=params,
        registration_audit=registration_audit,
    )

    # Step 5: map CableAssembly → CableEstimateResponse.
    per_circuit_read = [
        CircuitCableEstimateRead(
            circuit_id=c.circuit_id,
            device_drop_m=c.device_drop_m,
            in_plan_length_m=c.in_plan_length_m,
            home_run_m=c.home_run_m,
            home_run_status=c.home_run_status,
            base_total_m=c.base_total_m,
            total_with_spare_m=c.total_with_spare_m,
        )
        for c in assembly.per_circuit
    ]

    return CableEstimateResponse(
        per_circuit=per_circuit_read,
        total_device_drop_m=assembly.total_device_drop_m,
        total_in_plan_length_m=assembly.total_in_plan_length_m,
        total_home_run_m=assembly.total_home_run_m,
        unattributed_device_drop_m=assembly.unattributed_device_drop_m,
        grand_base_m=assembly.grand_base_m,
        grand_with_spare_m=assembly.grand_with_spare_m,
        spare_fraction=assembly.spare_fraction,
        quantity_kind=assembly.quantity_kind,
        reliability=assembly.reliability,
        params_stamp=assembly.params_stamp,
        home_run_suppressed_counts=assembly.home_run_suppressed_counts,
        registration_audit=assembly.registration_audit,
        containment_revision_id=containment_revision_id,
    )
