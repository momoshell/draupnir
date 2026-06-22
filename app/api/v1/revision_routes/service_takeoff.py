"""Routed-service takeoff compute-on-read route (issue #606, P3 / be-p3-03).

ADR-005: compute-on-read only. No persistence. Does NOT import or reference
app.estimating.quantities, QuantityTakeoff, QuantityItem, result_builders,
estimate_execution_input, or estimate_assembly.
"""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.revision_lineage import _get_active_revision_manifest_or_409
from app.api.v1.revision_routes.rooms import _resolve_rooms
from app.db.session import get_db
from app.interpretation.rise_drop import KIND_DROP, KIND_RISE, cluster_rise_drop_symbols
from app.interpretation.routed_runs import identify_routed_runs
from app.interpretation.run_service_identity import fuse_run_service_identities
from app.interpretation.service_takeoff import compute_service_takeoff
from app.interpretation.service_takeoff_loaders import load_service_takeoff_inputs
from app.schemas.revision import RevisionEntityManifestRead
from app.schemas.service_takeoff import (
    ServiceTakeoffLineRead,
    ServiceTakeoffResponse,
    ServiceTakeoffScaleRead,
    ServiceTakeoffSummaryRead,
)

service_takeoff_router = APIRouter()

ServiceTakeoffScope = Literal["sheet", "modelspace"]
_DEFAULT_SCOPE: ServiceTakeoffScope = "sheet"

# Default tag-association radius (drawing units -- typically mm in metric MEP drawings).
_DEFAULT_TAG_RADIUS: float = 2000.0


@service_takeoff_router.get(
    "/revisions/{revision_id}/service-takeoff",
    response_model=ServiceTakeoffResponse,
)
async def get_revision_service_takeoff(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    layer_refs: Annotated[list[str] | None, Query()] = None,
    tag_layers: Annotated[list[str] | None, Query()] = None,
    legend_layers: Annotated[list[str] | None, Query()] = None,
    scope: Annotated[ServiceTakeoffScope, Query()] = _DEFAULT_SCOPE,
    snap_tolerance: Annotated[float, Query(ge=0.0)] = 0.0,
    min_area: Annotated[float, Query(ge=0.0)] = 0.0,
    radius: Annotated[float, Query(gt=0.0)] = _DEFAULT_TAG_RADIUS,
) -> ServiceTakeoffResponse:
    """Compute the routed-service takeoff for a drawing revision (compute-on-read).

    Groups linework by (layer, colour), fuses pipe-tag text annotations, scopes runs to
    room polygons, and aggregates drawn + real-world lengths per (service, size, room)
    bucket. All inputs are loaded from the canonical entity store; nothing is persisted.

    Honest degradation: a revision with no adapter run, no pipe layers, or no confirmed
    scale returns 200 with empty or ``drawing_units_only`` lines and ``unscaled=True``.
    Unknown-service and unassigned-room lines appear in ``items`` and are counted in
    ``summary``. Never 500 on degenerate input.

    ``scope`` mirrors the rooms endpoint: ``sheet`` (default) restricts routed linework and
    room geometry to the printed sheet; ``modelspace`` interprets the full modelspace.
    ``radius`` is the tag-to-run association radius in drawing units.
    """
    manifest = await _get_active_revision_manifest_or_409(revision_id, db)
    exclude_off_sheet = scope == "sheet"

    # Step 1 -- load all takeoff inputs atomically.
    inputs = await load_service_takeoff_inputs(
        db,
        revision_id,
        layer_refs=layer_refs,
        tag_layers=tag_layers,
        legend_layers=legend_layers,
        exclude_off_sheet=exclude_off_sheet,
    )

    # Step 2 -- identify routed runs (P1).
    runs = identify_routed_runs(inputs.routed_entities, inputs.legend).groups

    # Step 3 -- fuse service identities from tags (P2).
    identities = fuse_run_service_identities(
        runs,
        inputs.geometry_by_entity_id,
        inputs.tag_placements,
        radius=radius,
    ).identities

    # Step 4 -- resolve rooms (reuses the rooms pipeline).
    room_result = await _resolve_rooms(
        db,
        revision_id,
        snap_tolerance=snap_tolerance,
        min_area=min_area,
        exclude_off_sheet=exclude_off_sheet,
    )

    # Step 5a -- cluster rise/drop symbols from loaded ARC+HATCH entities.
    rise_symbols = cluster_rise_drop_symbols(
        inputs.rise_entities, inputs.legend, kind=KIND_RISE
    ).symbols
    drop_symbols = cluster_rise_drop_symbols(
        inputs.drop_entities, inputs.legend, kind=KIND_DROP
    ).symbols

    # Step 5b -- compute takeoff (P3 coordinator).
    result = compute_service_takeoff(
        runs=runs,
        identities=identities,
        geometry_by_entity_id=inputs.geometry_by_entity_id,
        rooms=room_result.rooms,
        scale=inputs.scale,
        rise_symbols=rise_symbols,
        drop_symbols=drop_symbols,
    )

    # Step 6 -- adapt result to response (explicit kwargs, no from_attributes across frozen
    # dataclass boundary).
    items = [
        ServiceTakeoffLineRead(
            service=line.service,
            size_raw=line.size_raw,
            size_kind=line.size_kind,
            discipline=line.discipline,
            room_id=line.room_id,
            room_name=line.room_name,
            room_number=line.room_number,
            drawing_length=line.drawing_length,
            real_length_m=line.real_length_m,
            basis=line.basis,
            units_confidence=line.units_confidence,
            run_count=line.run_count,
            identity_status=line.identity_status,
            confidence=line.confidence,
            riser_count=line.riser_count,
            drop_count=line.drop_count,
        )
        for line in result.lines
    ]

    distinct_services = len({item.service for item in items})
    distinct_sizes = len({(item.service, item.size_raw) for item in items})
    distinct_rooms = len({item.room_id for item in items})

    scale_read = ServiceTakeoffScaleRead(
        units_confidence=inputs.scale.units_confidence,
        real_world_available=inputs.scale.real_world_available,
        contradicted=inputs.scale.contradicted,
        conversion_factor=inputs.scale.conversion_factor,
    )

    summary = ServiceTakeoffSummaryRead(
        services=distinct_services,
        sizes=distinct_sizes,
        rooms=distinct_rooms,
        lines=len(items),
        unassigned_runs=result.unassigned_run_count,
        unknown_service_runs=result.unknown_service_run_count,
        total_risers=result.total_risers,
        total_drops=result.total_drops,
    )

    return ServiceTakeoffResponse(
        manifest=RevisionEntityManifestRead.model_validate(manifest),
        items=items,
        summary=summary,
        scale=scale_read,
        unscaled=result.unscaled,
    )
