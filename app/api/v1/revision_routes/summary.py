"""Compact per-revision orientation summary route (aggregation of existing signals)."""

from collections import Counter
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.revision_lineage import (
    _get_active_revision_manifest_or_409,
    _get_active_validation_report,
    _manifest_counts,
)
from app.api.v1.revision_routes.rooms import _resolve_rooms_with_family
from app.api.v1.revision_routes.scale import resolve_revision_scale
from app.db.session import get_db
from app.interpretation.devices import enumerate_devices
from app.interpretation.label_rooms import has_genuine_room_identity
from app.interpretation.layer_roles import classify_layer_role
from app.interpretation.room_loaders import load_room_summary, load_rooms
from app.models.revision_materialization import RevisionLayer
from app.schemas.revision_summary import RevisionSummaryRead
from app.schemas.validation_report import ValidationReportCoverage

summary_router = APIRouter()


@summary_router.get(
    "/revisions/{revision_id}/summary",
    response_model=RevisionSummaryRead,
)
async def get_revision_summary(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> RevisionSummaryRead:
    """Return a compact one-call orientation for an active revision.

    Aggregates the already-computed signals exposed by the dedicated read
    resources (entity counts, layers / layer-roles, devices, rooms, scale/units,
    and the extraction-coverage block) so an agent can orient in a single request.
    Each count matches the default response of its dedicated endpoint.
    """

    manifest = await _get_active_revision_manifest_or_409(revision_id, db)
    counts = _manifest_counts(manifest)

    # Layers + derived semantic roles (mirrors /layer-roles).
    layer_rows = (
        await db.scalars(
            select(RevisionLayer)
            .where(RevisionLayer.drawing_revision_id == revision_id)
            .order_by(RevisionLayer.sequence_index.asc(), RevisionLayer.id.asc())
        )
    ).all()
    role_counts: Counter[str] = Counter()
    for row in layer_rows:
        name = row.payload_json.get("name") if isinstance(row.payload_json, dict) else None
        role_counts[classify_layer_role(name if isinstance(name, str) else None).role] += 1

    # Devices (mirrors /devices default enumeration).
    devices = await enumerate_devices(db, revision_id)

    # Rooms — read-path flip (#831): a version-scoped RevisionRoomSummary row is the
    # materialized signal. A hit serves the persisted genuine room set directly (no
    # re-filter — persisted rows are already genuine); a miss lazily enqueues
    # materialization and falls back to the live resolver (#584/#792), keeping /summary
    # and /rooms in agreement either way.
    summary_row = await load_room_summary(db, revision_id)
    if summary_row is not None:
        genuine_rooms, _assignments = await load_rooms(db, revision_id)
    else:
        # Lazy import avoids a summary.py <-> service_takeoff.py import cycle
        # (service_takeoff.py imports _resolve_rooms from rooms.py).
        from app.api.v1.revision_routes.service_takeoff import _enqueue_rooms_materialization

        await _enqueue_rooms_materialization(
            revision_id=revision_id,
            project_id=manifest.project_id,
            source_file_id=manifest.source_file_id,
        )
        # Apply the same presentation filter as /rooms, family-aware (#828 PR-3): only genuine
        # rooms (valid number or real name on DWG/other; PDF requires a plausible number, see
        # has_genuine_room_identity).
        rooms_result, input_family = await _resolve_rooms_with_family(db, revision_id)
        genuine_rooms = [
            room
            for room in rooms_result.rooms
            if has_genuine_room_identity(room, input_family=input_family)
        ]
    named_rooms = sum(1 for room in genuine_rooms if room.name is not None)

    # Scale/units (A3) + the extraction-coverage block (validation report).
    scale = await resolve_revision_scale(revision_id, db)
    report = await _get_active_validation_report(revision_id, db)
    coverage: ValidationReportCoverage | None = None
    entities_by_type: dict[str, int] = {}
    if report is not None and isinstance(report.report_json, dict):
        coverage_json = report.report_json.get("coverage")
        if isinstance(coverage_json, dict):
            coverage = ValidationReportCoverage.model_validate(coverage_json)
            entities_by_type = dict(coverage.entities.by_type)

    return RevisionSummaryRead(
        revision_id=revision_id,
        entity_counts=counts,
        entities_by_type=entities_by_type,
        layer_count=len(layer_rows),
        layer_roles=dict(role_counts),
        device_count=len(devices),
        room_count=len(genuine_rooms),
        named_room_count=named_rooms,
        scale=scale,
        coverage=coverage,
    )
