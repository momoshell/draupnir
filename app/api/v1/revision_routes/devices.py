"""Device / fixture-schedule interpretation route (tier-3, derived from the canonical model)."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.pagination import DEFAULT_PAGE_SIZE as _DEFAULT_PAGE_SIZE
from app.api.pagination import MAX_PAGE_SIZE as _MAX_PAGE_SIZE
from app.api.pagination import (
    decode_cursor_payload,
    encode_cursor_payload,
    read_cursor_int,
)
from app.api.v1.revision_lineage import _get_active_revision_manifest_or_409
from app.db.session import get_db
from app.interpretation.devices import (
    attach_tags,
    enumerate_devices,
    load_tag_candidates,
    schedule_from_devices,
)
from app.schemas.devices import (
    DeviceRead,
    DeviceScheduleEntry,
    RevisionDeviceListResponse,
)
from app.schemas.revision import RevisionEntityManifestRead

devices_router = APIRouter()

_MAX_NESTING_DEPTH = 8


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
    max_depth: Annotated[int, Query(ge=0, le=_MAX_NESTING_DEPTH)] = _MAX_NESTING_DEPTH,
) -> RevisionDeviceListResponse:
    """List device instances (the block-instance tree) with nearest-tag association + schedule.

    Devices are enumerated by walking placed blocks down to ``max_depth`` (0 = top-level INSERTs
    only), composing placement transforms so each instance gets a world position; the ``schedule``
    aggregates counts by block reference across all enumerated instances. Each device is matched to
    the nearest tag text (default: layers whose name contains ``tag``/``device``; falls back to any
    text) within an optional ``max_tag_distance``.
    """

    manifest = await _get_active_revision_manifest_or_409(revision_id, db)

    devices = await enumerate_devices(
        db, revision_id, device_layers=device_layer, max_depth=max_depth
    )
    schedule = schedule_from_devices(devices)

    offset = read_cursor_int(decode_cursor_payload(cursor), "offset") if cursor else 0
    page = devices[offset : offset + limit]
    next_offset = offset + limit
    next_cursor = (
        encode_cursor_payload({"offset": next_offset}) if next_offset < len(devices) else None
    )

    candidates = await load_tag_candidates(db, revision_id, tag_layers=tag_layer)
    tagged_page = attach_tags(page, candidates, max_distance=max_tag_distance)

    return RevisionDeviceListResponse(
        manifest=RevisionEntityManifestRead.model_validate(manifest),
        schedule=[DeviceScheduleEntry(**entry) for entry in schedule],
        items=[DeviceRead.model_validate(device) for device in tagged_page],
        next_cursor=next_cursor,
        association={
            "tag_layers": tag_layer,
            "device_layers": device_layer,
            "max_tag_distance": max_tag_distance,
            "max_depth": max_depth,
            "total_devices": len(devices),
            "default_tag_layer_tokens": ["tag", "device"],
        },
    )
