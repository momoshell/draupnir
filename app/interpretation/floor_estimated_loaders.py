"""DB seam for the ESTIMATED (cable) floor fusion (issue #720, Phase R-F).

Single entry point: ``load_estimated_fusion`` reuses the shipped cable seams to
compute a ``CableAssembly``, then delegates to the pure ``bucket_estimated_cable``.

The ``RoomRegistry`` and ``FloorRegistration`` are passed in by the caller —
this module does NOT build them (separation of concerns: shared across R-E / R-F / R-G).

``app.api`` imports are lazy (inside function bodies only) to avoid an
interpretation→api cycle (#705 class pattern).  All cable seam loaders live in
``app.interpretation`` (no api dependency) and are imported at module level.

Reference frame
---------------
All computation uses the lighting frame.  The cable estimate + R-E map are both
produced in the lighting frame, so no GridTransform is needed inside R-F.
"""

from __future__ import annotations

import logging
from typing import Literal
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.interpretation.cable_circuits import partition_circuits
from app.interpretation.cable_estimate import estimate_cable_in_plan
from app.interpretation.cable_estimate_assembly import assemble_cable_estimate
from app.interpretation.cable_estimate_params import default_estimate_params
from app.interpretation.cable_home_run import HomeRunResult, no_containment_home_run_result
from app.interpretation.cable_topology import build_cable_graph
from app.interpretation.cable_topology_loaders import load_device_footprints, load_spline_inputs
from app.interpretation.floor_counted_loaders import load_counted_fusion
from app.interpretation.floor_estimated import FusedEstimatedResult, bucket_estimated_cable
from app.interpretation.floor_registration import FloorRegistration
from app.interpretation.room_fusion import RoomRegistry

_log = logging.getLogger(__name__)


async def load_estimated_fusion(
    db: AsyncSession,
    registration: FloorRegistration,
    registry: RoomRegistry,
    *,
    lighting_revision_id: UUID,
    containment_revision_id: UUID | None,
    scope: Literal["sheet", "modelspace"] = "sheet",
) -> FusedEstimatedResult:
    """Load and compute the per-room estimated cable contribution for a floor.

    Reuses all shipped cable seams (no SQL duplication).  The call sequence mirrors
    the cable-estimate route (``app/api/v1/revision_routes/cable_estimate.py``) exactly,
    keeping ``exclude_off_sheet`` consistent between in-plan and home-run computations
    so the circuit partition (and circuit_ids) aligns across both paths.

    Parameters
    ----------
    db:
        Async DB session (read-only usage).
    registration:
        Floor registration result from the floor-registration loader.  Passed through
        to ``load_counted_fusion`` for R-E device→room map construction.
    registry:
        Frozen ``RoomRegistry`` for device room classification (shared with R-E).
    lighting_revision_id:
        UUID of the lighting (primary) drawing revision — defines the routing frame.
    containment_revision_id:
        UUID of the containment (cable-tray) drawing revision for home-run routing,
        or ``None`` to produce honest-absent home-run results (no_containment_sheet).
    scope:
        ``"sheet"`` (default) restricts geometry to the printed sheet;
        ``"modelspace"`` walks the full modelspace.  Applied consistently to in-plan
        splines and home-run loading.

    Returns
    -------
    FusedEstimatedResult
        Per-room estimated cable contributions with conservation enforced.
    """
    exclude_off_sheet = scope == "sheet"

    # --- Step 1: load lighting circuits + in-plan estimate ---
    # (mirrors the cable-estimate route exactly)
    splines = await load_spline_inputs(
        db, lighting_revision_id, exclude_off_sheet=exclude_off_sheet
    )
    device_footprints = await load_device_footprints(
        db,
        lighting_revision_id,
        exclude_off_sheet=False,  # fixed False — matches cable route
    )

    graph = build_cable_graph(splines=splines, devices=device_footprints)
    circuits = partition_circuits(graph)

    params = default_estimate_params()
    in_plan = estimate_cable_in_plan(
        circuits=circuits,
        splines=splines,
        devices=device_footprints,
        params=params,
    )

    # --- Step 2: home-run computation or honest-absent placeholder ---
    home_run: HomeRunResult
    registration_audit: dict[str, object] | None

    if containment_revision_id is None:
        circuit_ids = [c.circuit_id for c in circuits.circuits]
        home_run = no_containment_home_run_result(circuit_ids)
        registration_audit = None
    else:
        # Lazy import to avoid interpretation→api cycle (#705).
        from app.interpretation.cable_home_run_loaders import load_and_compute_home_runs

        home_run, registration_audit = await load_and_compute_home_runs(
            db,
            lighting_revision_id=lighting_revision_id,
            containment_revision_id=containment_revision_id,
            exclude_off_sheet=exclude_off_sheet,
        )

    # --- Step 3: assemble the full cable estimate ---
    assembly = assemble_cable_estimate(
        in_plan=in_plan,
        home_run=home_run,
        params=params,
        registration_audit=registration_audit,
    )

    # --- Step 4: load R-E device→room map ---
    counted_result = await load_counted_fusion(db, registration, registry, scope=scope)
    device_room_map = counted_result.by_device_id()

    # --- Step 5: bucket estimated cable across rooms ---
    source: dict[str, str | None] = {
        "lighting_revision_id": str(lighting_revision_id),
        "containment_revision_id": (
            str(containment_revision_id) if containment_revision_id is not None else None
        ),
    }

    return bucket_estimated_cable(
        assembly=assembly,
        circuits=circuits,
        device_footprints=device_footprints,
        params=params,
        device_room_map=device_room_map,
        source=source,
    )
