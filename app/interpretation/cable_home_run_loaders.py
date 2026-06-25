"""Two-revision coordinator for cable home-run estimation (issue #697b).

Loads the lighting (primary) and containment revisions, co-registers them via
grid fiducials, and calls :func:`compute_home_runs` in the registered lighting frame.

This is a DB seam — all ORM access lives here; the pure builder in
:mod:`app.interpretation.cable_home_run` has no DB/ORM imports.

Architecture: two-revision coordinator, NOT a fusion-materialization layer.
- Containment geometry (tray polylines) is registered INTO the lighting frame.
- All routing runs entirely in the lighting frame.

Notes on units
--------------
Geometry is metre-scaled by ingestion (conversion_factor=1.0). This coordinator
passes coordinates through without rescaling and ASSUMES both revisions share the
same metre scale. A full per-revision units re-resolution (reading conversion_factor
from the extraction profile) is out of scope for v1.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.ingestion.centerline_contract import polylines_from_geometry_json
from app.interpretation.cable_circuits import partition_circuits
from app.interpretation.cable_home_run import HomeRunResult, compute_home_runs
from app.interpretation.cable_topology import build_cable_graph
from app.interpretation.cable_topology_loaders import load_device_footprints, load_spline_inputs
from app.interpretation.grid_registration import estimate_grid_transform
from app.interpretation.grid_registration_loaders import load_grid_fiducials
from app.models.drawing_revision import DrawingRevision
from app.models.revision_routed_length import RevisionRoutedLength


async def load_and_compute_home_runs(
    db: AsyncSession,
    *,
    lighting_revision_id: UUID,
    containment_revision_id: UUID,
    rot_tol_rad: float = 0.02,
) -> tuple[HomeRunResult, dict[str, object]]:
    """Load two revisions, co-register them, and compute circuit home-run lengths.

    Parameters
    ----------
    db:
        Async SQLAlchemy session.
    lighting_revision_id:
        UUID of the lighting (primary) drawing revision — defines the routing frame.
    containment_revision_id:
        UUID of the containment (cable-tray) drawing revision — supplies tray geometry.
    rot_tol_rad:
        Maximum tolerated rotation between revisions (radians). Exceeding this marks
        ``registration_failed=True`` so all circuits receive status "bad_registration".

    Returns
    -------
    (HomeRunResult, registration_audit)
        ``registration_audit`` carries the transform diagnostics for surfacing by
        the API caller (#698):
        {dx, dy, rotation_rad, scale, matched_count, matched_labels,
         median_residual_m, quality, lighting_revision_id, containment_revision_id}

    Raises
    ------
    ValueError
        If the two revisions belong to different projects.

    Notes on units
    --------------
    Geometry is assumed to be in metres (conversion_factor=1.0 on ingestion).
    A full per-revision units re-resolution is out of scope for v1.
    """

    # --- Assert both revisions share the same project ---
    rev_rows = (
        await db.execute(
            select(DrawingRevision.id, DrawingRevision.project_id).where(
                DrawingRevision.id.in_([lighting_revision_id, containment_revision_id])
            )
        )
    ).all()

    project_by_rev: dict[UUID, UUID] = {row.id: row.project_id for row in rev_rows}
    lighting_proj = project_by_rev.get(lighting_revision_id)
    containment_proj = project_by_rev.get(containment_revision_id)

    if lighting_proj is None or containment_proj is None:
        missing = [
            str(r)
            for r, p in [
                (lighting_revision_id, lighting_proj),
                (containment_revision_id, containment_proj),
            ]
            if p is None
        ]
        raise ValueError(f"DrawingRevision(s) not found: {missing}")

    if lighting_proj != containment_proj:
        raise ValueError(
            f"Revisions belong to different projects: "
            f"lighting={lighting_proj} containment={containment_proj}"
        )

    # --- Step 1: lighting side ---
    splines = await load_spline_inputs(db, lighting_revision_id)
    device_footprints = await load_device_footprints(db, lighting_revision_id)

    graph = build_cable_graph(splines=splines, devices=device_footprints)
    circuits = partition_circuits(graph)

    # Build entity_id → DeviceFootprint mapping for the builder.
    device_footprints_by_id = {fp.entity_id: fp for fp in device_footprints}

    # Grid fiducials for the lighting revision.
    lighting_fiducials = await load_grid_fiducials(db, lighting_revision_id)

    # --- Step 2: containment side — load tray polylines ---
    routed_rows = (
        (
            await db.execute(
                select(RevisionRoutedLength).where(
                    RevisionRoutedLength.drawing_revision_id == containment_revision_id,
                )
            )
        )
        .scalars()
        .all()
    )

    tray_polylines_raw: list[tuple[tuple[float, float], ...]] = []
    for row in routed_rows:
        for pl in polylines_from_geometry_json(row.geometry_json):
            tray_polylines_raw.append(pl)

    # Grid fiducials for the containment revision.
    containment_fiducials = await load_grid_fiducials(db, containment_revision_id)

    # --- Step 3: estimate grid transform (containment → lighting) ---
    transform = estimate_grid_transform(containment_fiducials, lighting_fiducials)

    # --- Step 4: fail-closed quality gates ---
    # v1 routes only on a GOOD registration; degraded/failed → all circuits bad_registration.
    # "degraded" already covers: <3 fiducials, scale mismatch, or residual in warn band.
    # GridTransform.apply() is translation-only — it ignores scale — so a degraded scale
    # mismatch would silently mis-place every tray vertex and produce confidently-wrong
    # home_run_m values. Explicit rotation check is kept for belt-and-suspenders clarity.
    registration_failed = (transform.quality != "good") or (
        abs(transform.rotation_rad) > rot_tol_rad
    )

    # --- Step 5: register tray polylines into the lighting frame ---
    registered_tray_polylines: list[list[tuple[float, float]]] = []
    for pl in tray_polylines_raw:
        registered_pl = [transform.apply((x, y)) for (x, y) in pl]
        registered_tray_polylines.append(registered_pl)

    # --- Step 6: compute home runs ---
    result = compute_home_runs(
        circuits=circuits,
        device_footprints_by_id=device_footprints_by_id,
        tray_polylines=registered_tray_polylines,
        registration_failed=registration_failed,
    )

    # --- Step 7: build registration audit ---
    registration_audit: dict[str, object] = {
        "dx": transform.dx,
        "dy": transform.dy,
        "rotation_rad": transform.rotation_rad,
        "scale": transform.scale,
        "matched_count": transform.matched_count,
        "matched_labels": list(transform.matched_labels),
        "median_residual_m": transform.median_residual_m,
        "quality": transform.quality,
        "lighting_revision_id": str(lighting_revision_id),
        "containment_revision_id": str(containment_revision_id),
    }

    return result, registration_audit
