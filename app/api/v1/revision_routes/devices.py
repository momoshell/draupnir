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
from app.api.v1.revision_lineage import (
    _get_active_revision,
    _get_active_revision_manifest_or_409,
)
from app.core.exceptions import raise_not_found
from app.db.session import get_db
from app.interpretation.devices import (
    attach_tags,
    enumerate_devices,
    load_tag_candidates,
    schedule_from_devices,
)
from app.interpretation.legend import resolve_legend_devices, schedule_from_legend_devices
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.schemas.devices import (
    DeviceRead,
    DeviceScheduleEntry,
    LegendDeviceRead,
    LegendDeviceScheduleEntry,
    RevisionDeviceListResponse,
    RevisionLegendDeviceListResponse,
)
from app.schemas.revision import RevisionEntityManifestRead

devices_router = APIRouter()

_MAX_NESTING_DEPTH = 8


async def _load_text_blocks(db: AsyncSession, revision: DrawingRevision) -> list[dict[str, object]]:
    """Return the revision's extracted text blocks (from its adapter run output), or []."""

    if revision.adapter_run_output_id is None:
        return []
    output = await db.get(AdapterRunOutput, revision.adapter_run_output_id)
    if output is None or not output.canonical_json:
        return []
    metadata = output.canonical_json.get("metadata")
    if not isinstance(metadata, dict):
        return []
    text_blocks = metadata.get("text_blocks")
    return list(text_blocks) if isinstance(text_blocks, list) else []


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


@devices_router.get(
    "/revisions/{revision_id}/legend-devices",
    response_model=RevisionLegendDeviceListResponse,
)
async def list_revision_legend_devices(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
) -> RevisionLegendDeviceListResponse:
    """List devices located by resolving drawing-body tags against the legend (vector PDFs).

    The legend is parsed into a symbol dictionary (abbreviation -> device type); body text blocks
    whose text matches an abbreviation are emitted as located, typed devices, and ``schedule``
    aggregates counts per type. Revisions without a parseable legend return an empty schedule.
    """

    revision = await _get_active_revision(revision_id, db)
    if revision is None:
        raise_not_found("Drawing revision", str(revision_id))
    assert revision is not None

    text_blocks = await _load_text_blocks(db, revision)
    dictionary, devices = resolve_legend_devices(text_blocks)
    schedule = schedule_from_legend_devices(devices)

    offset = read_cursor_int(decode_cursor_payload(cursor), "offset") if cursor else 0
    page = devices[offset : offset + limit]
    next_offset = offset + limit
    next_cursor = (
        encode_cursor_payload({"offset": next_offset}) if next_offset < len(devices) else None
    )

    return RevisionLegendDeviceListResponse(
        schedule=[LegendDeviceScheduleEntry(**entry) for entry in schedule],
        items=[
            LegendDeviceRead(
                abbreviation=device.abbreviation,
                type_name=device.type_name,
                position={"x": device.x, "y": device.y},
            )
            for device in page
        ],
        next_cursor=next_cursor,
        summary={
            "legend_size": len(dictionary),
            "total_devices": len(devices),
        },
    )
