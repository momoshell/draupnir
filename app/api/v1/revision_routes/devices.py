"""Device / fixture-schedule interpretation route (tier-3, derived from the canonical model)."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.pagination import DEFAULT_PAGE_SIZE as _DEFAULT_PAGE_SIZE
from app.api.pagination import MAX_PAGE_SIZE as _MAX_PAGE_SIZE
from app.api.pagination import paginate_overfetched as _paginate_overfetched
from app.api.v1.revision_cursors import (
    _decode_materialization_cursor,
    _encode_materialization_cursor,
)
from app.api.v1.revision_lineage import _get_active_revision_manifest_or_409
from app.db.session import get_db
from app.interpretation.devices import associate_devices, device_schedule
from app.models.revision_materialization import RevisionEntity
from app.schemas.devices import (
    DeviceRead,
    DeviceScheduleEntry,
    RevisionDeviceListResponse,
)
from app.schemas.revision import RevisionEntityManifestRead

devices_router = APIRouter()


@devices_router.get(
    "/revisions/{revision_id}/devices",
    response_model=RevisionDeviceListResponse,
)
async def list_revision_devices(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
    device_layer: Annotated[list[str] | None, Query()] = None,
    tag_layer: Annotated[list[str] | None, Query()] = None,
    max_tag_distance: Annotated[float | None, Query(gt=0.0)] = None,
) -> RevisionDeviceListResponse:
    """List device instances (INSERTs) with nearest-tag association + the fixture schedule.

    Devices and tags are related only by position in the drawing, so each device is matched to the
    nearest tag text (default: text on layers whose name contains ``tag``/``device``; falls back to
    any text) within an optional ``max_tag_distance``. The ``schedule`` aggregates device counts by
    block reference across the whole revision; ``items`` is the paginated, tag-associated detail.
    """

    manifest = await _get_active_revision_manifest_or_409(revision_id, db)
    pagination_cursor = _decode_materialization_cursor(cursor) if cursor else None

    query = select(RevisionEntity).where(
        RevisionEntity.drawing_revision_id == revision_id,
        RevisionEntity.entity_type == "insert",
    )
    if device_layer:
        query = query.where(RevisionEntity.layer_ref.in_(device_layer))
    if pagination_cursor is not None:
        sequence_index, row_id = pagination_cursor
        query = query.where(
            (RevisionEntity.sequence_index > sequence_index)
            | ((RevisionEntity.sequence_index == sequence_index) & (RevisionEntity.id > row_id))
        )

    result = await db.execute(
        query.order_by(RevisionEntity.sequence_index.asc(), RevisionEntity.id.asc()).limit(
            limit + 1
        )
    )
    rows = result.scalars().all()
    page, next_cursor = _paginate_overfetched(
        rows,
        limit=limit,
        encode_cursor=lambda last_row: _encode_materialization_cursor(
            last_row.sequence_index, last_row.id
        ),
    )

    devices = await associate_devices(
        db,
        page,
        revision_id=revision_id,
        tag_layers=tag_layer,
        max_distance=max_tag_distance,
    )
    schedule = await device_schedule(db, revision_id, device_layers=device_layer)

    return RevisionDeviceListResponse(
        manifest=RevisionEntityManifestRead.model_validate(manifest),
        schedule=[DeviceScheduleEntry(**entry) for entry in schedule],
        items=[DeviceRead.model_validate(device) for device in devices],
        next_cursor=next_cursor,
        association={
            "tag_layers": tag_layer,
            "device_layers": device_layer,
            "max_tag_distance": max_tag_distance,
            "default_tag_layer_tokens": ["tag", "device"],
        },
    )
