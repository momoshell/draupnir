"""Containment-type takeoff compute-on-read route (issue #756, Phase 752c).

ADR-005: compute-on-read only. No persistence. Does NOT import or reference
app.estimating.quantities, QuantityTakeoff, QuantityItem, result_builders,
estimate_execution_input, or estimate_assembly.
"""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.revision_lineage import _get_active_revision_manifest_or_409
from app.db.session import get_db
from app.interpretation.service_takeoff_loaders import assemble_containment_takeoff
from app.schemas.containment_takeoff import ContainmentTakeoffResponse, ContainmentTypeRead

containment_takeoff_router = APIRouter()


@containment_takeoff_router.get(
    "/revisions/{revision_id}/containment-takeoff",
    response_model=ContainmentTakeoffResponse,
)
async def get_revision_containment_takeoff(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    layer_refs: Annotated[list[str] | None, Query()] = None,
) -> ContainmentTakeoffResponse:
    """Compute the containment-type takeoff for a drawing revision (compute-on-read).

    Attributes persisted centerline segments to HATCH containment-band polygons (tray,
    duct, conduit, …), resolving each band to a containment type via the drawing legend.
    All inputs are loaded from the canonical entity store; nothing is persisted.

    Honest degradation: a revision with no containment bands or no centerline segments
    returns 200 with an empty per_type list and zero lengths. Never 500 on degenerate input.

    ``layer_refs`` optionally overrides the default container layer tokens used to filter
    containment HATCH entities.
    """
    await _get_active_revision_manifest_or_409(revision_id, db)

    if layer_refs is not None:
        result = await assemble_containment_takeoff(db, revision_id, layer_tokens=tuple(layer_refs))
    else:
        result = await assemble_containment_takeoff(db, revision_id)

    return ContainmentTakeoffResponse(
        per_type=[
            ContainmentTypeRead(
                containment_type=entry.containment_type,
                length_m=entry.length_m,
                member_colour_keys=list(entry.member_colour_keys),
                member_pattern_names=list(entry.member_pattern_names),
            )
            for entry in result.per_type
        ],
        shared_length_m=result.shared_length_m,
        total_length_m=result.total_length_m,
        centerline_segment_count=result.centerline_segment_count,
    )
