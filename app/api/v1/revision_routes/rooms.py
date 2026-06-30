"""Room-containment interpretation route (tier-3, derived from the canonical model)."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.pagination import DEFAULT_PAGE_SIZE as _DEFAULT_PAGE_SIZE
from app.api.pagination import MAX_PAGE_SIZE as _MAX_PAGE_SIZE
from app.api.pagination import (
    decode_cursor_payload,
    encode_cursor_payload,
    read_cursor_int,
)
from app.api.v1.revision_lineage import _get_active_revision_manifest_or_409
from app.api.v1.revision_routes.materialization import _entity_summary, _parse_entity_fields
from app.core.exceptions import raise_not_found
from app.db.session import get_db
from app.interpretation.label_rooms import _looks_like_room_name
from app.interpretation.room_pipeline import ROOM_STRATEGY_AUTO
from app.interpretation.room_resolution import (
    DEFAULT_ROOM_SCOPE,
    MAX_NESTING_DEPTH,
    RoomScope,
)
from app.interpretation.room_resolution import (
    resolve_rooms as _resolve_rooms,
)
from app.interpretation.rooms import (
    Room,
    _smallest_containing_room,
    parse_room_number,
)
from app.models.revision_materialization import RevisionEntity
from app.schemas.revision import RevisionEntityManifestRead
from app.schemas.rooms import (
    DeviceRoomAssignmentRead,
    RevisionRoomEntityListResponse,
    RevisionRoomListResponse,
    RoomBoundsRead,
    RoomLocationRead,
    RoomRead,
)

rooms_router = APIRouter()


@rooms_router.get(
    "/revisions/{revision_id}/rooms",
    response_model=RevisionRoomListResponse,
)
async def list_revision_rooms(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    strategy: Annotated[str, Query()] = ROOM_STRATEGY_AUTO,
    device_layer: Annotated[list[str] | None, Query()] = None,
    tag_layer: Annotated[list[str] | None, Query()] = None,
    snap_tolerance: Annotated[float, Query(ge=0.0)] = 0.0,
    min_area: Annotated[float, Query(ge=0.0)] = 0.0,
    max_depth: Annotated[int, Query(ge=0, le=MAX_NESTING_DEPTH)] = MAX_NESTING_DEPTH,
    scope: Annotated[RoomScope, Query()] = DEFAULT_ROOM_SCOPE,
) -> RevisionRoomListResponse:
    """Derive rooms and assign devices to them by boundary containment.

    ``strategy`` selects the room source: ``auto`` (default) prefers an explicit
    room/space-boundary layer and falls back to wall-linework polygonization;
    ``explicit_layer`` / ``wall_polygonize`` force one source. Each device is
    assigned to the smallest room whose polygon contains its world position, and
    each room is named from the room-tag text inside it. ``snap_tolerance`` and
    ``min_area`` tune the wall-polygonize robustness (gap-closing, sliver dropping).
    ``scope`` restricts interpretation to the printed sheet (``sheet``, default) or the
    full ``modelspace`` (#583).
    """
    manifest = await _get_active_revision_manifest_or_409(revision_id, db)
    result = await _resolve_rooms(
        db,
        revision_id,
        strategy=strategy,
        device_layer=device_layer,
        tag_layer=tag_layer,
        snap_tolerance=snap_tolerance,
        min_area=min_area,
        max_depth=max_depth,
        exclude_off_sheet=scope == "sheet",
    )

    named_rooms = sum(1 for room in result.rooms if room.name is not None)

    # Presentation filter (#778): surface only rooms with a genuine identity.
    # A room has genuine identity when it carries a valid room number (parse_room_number
    # succeeds — e.g. "1.9.01") OR a genuine short room name (_looks_like_room_name passes —
    # e.g. "Cooling Plantroom", "Heating & Hot Water").
    #
    # What we suppress:
    #   - Anonymous wall-polygon cells (name=None AND number=None): internal registry geometry
    #     load-bearing for Phase-R conservation and the Voronoi byte-identity guard.
    #   - Spec-prose-named cells: wall-polygon cells whose name was stamped from note text
    #     (e.g. "ALL PIPEWORK SHALL BE PR", "ACCORDANCE WITH BUILDING REGULATIONS") — these
    #     slip past the name=None check but are not reportable rooms.
    # The registry retains ALL polygons unchanged; this filter only affects the API response.
    def _has_genuine_identity(room: Room) -> bool:
        return (room.number is not None and parse_room_number(room.number) is not None) or (
            room.name is not None and _looks_like_room_name(room.name)
        )

    identified_rooms = [room for room in result.rooms if _has_genuine_identity(room)]
    suppressed_count = len(result.rooms) - len(identified_rooms)

    return RevisionRoomListResponse(
        manifest=RevisionEntityManifestRead.model_validate(manifest),
        strategy=result.strategy,
        source_layers=list(result.source_layers),
        items=[_room_read(room) for room in identified_rooms],
        assignments=[
            DeviceRoomAssignmentRead(device_id=assignment.device_id, room_id=assignment.room_id)
            for assignment in result.device_assignments
        ],
        summary={
            "rooms": len(identified_rooms),
            "named_rooms": named_rooms,
            "assigned_devices": len(result.device_assignments),
            # Count of all suppressed non-room entries: anonymous polygons + spec-prose-named
            # cells. Kept for transparency so consumers can see internal registry size.
            "unlabeled_polygon_count": suppressed_count,
        },
    )


@rooms_router.get(
    "/revisions/{revision_id}/rooms/{room_id}/entities",
    response_model=RevisionRoomEntityListResponse,
)
async def list_revision_room_entities(
    revision_id: UUID,
    room_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
    entity_type: str | None = Query(default=None),
    layer_ref: str | None = Query(default=None),
    strategy: Annotated[str, Query()] = ROOM_STRATEGY_AUTO,
    device_layer: Annotated[list[str] | None, Query()] = None,
    tag_layer: Annotated[list[str] | None, Query()] = None,
    snap_tolerance: Annotated[float, Query(ge=0.0)] = 0.0,
    min_area: Annotated[float, Query(ge=0.0)] = 0.0,
    max_depth: Annotated[int, Query(ge=0, le=MAX_NESTING_DEPTH)] = MAX_NESTING_DEPTH,
    scope: Annotated[RoomScope, Query()] = DEFAULT_ROOM_SCOPE,
    fields: Annotated[
        str | None,
        Query(
            description=(
                "Comma-separated heavy blocks to include "
                "(geometry, properties, provenance, confidence, canonical). "
                "Default: none — a compact row spine."
            ),
        ),
    ] = None,
) -> RevisionRoomEntityListResponse:
    """List entities contained in a given room (centroid-in-polygon, smallest-containing).

    Rooms are recomputed with the same strategy/parameters as ``/rooms`` (room ids are
    deterministic), then ``room_id`` is resolved among them. An entity belongs to the room
    when the centroid of its persisted bounding box lies in the room polygon AND that room
    is the smallest one containing the centroid — so an entity inside a nested room is
    attributed to the inner room, not this one. Compact rows by default; heavy blocks
    opt-in via ``fields``; offset-paginated.
    """

    include_fields = _parse_entity_fields(fields)
    manifest = await _get_active_revision_manifest_or_409(revision_id, db)
    result = await _resolve_rooms(
        db,
        revision_id,
        strategy=strategy,
        device_layer=device_layer,
        tag_layer=tag_layer,
        snap_tolerance=snap_tolerance,
        min_area=min_area,
        max_depth=max_depth,
        exclude_off_sheet=scope == "sheet",
    )

    room = next((candidate for candidate in result.rooms if candidate.id == room_id), None)
    if room is None:
        raise_not_found("Room", room_id)
    assert room is not None

    # A label-derived room (#549) has a name/number + location but no boundary, so it
    # contains no geometry — return an empty (but valid) page rather than querying a polygon.
    if room.bounds is None or room.polygon is None:
        return RevisionRoomEntityListResponse(
            manifest=RevisionEntityManifestRead.model_validate(manifest),
            room=_room_read(room),
            items=[],
            total=0,
            next_cursor=None,
        )

    # Indexed prefilter: only entities whose bbox-centroid falls in the room's AABB can be
    # contained in (or nested within) the room. The Python pass below refines to the polygon.
    centroid_x = (RevisionEntity.bbox_min_x + RevisionEntity.bbox_max_x) / 2
    centroid_y = (RevisionEntity.bbox_min_y + RevisionEntity.bbox_max_y) / 2
    query = (
        select(RevisionEntity)
        .where(
            RevisionEntity.drawing_revision_id == revision_id,
            RevisionEntity.bbox_min_x.is_not(None),
            centroid_x >= room.bounds[0],
            centroid_x <= room.bounds[2],
            centroid_y >= room.bounds[1],
            centroid_y <= room.bounds[3],
        )
        .order_by(RevisionEntity.sequence_index.asc(), RevisionEntity.id.asc())
    )
    if entity_type is not None:
        query = query.where(RevisionEntity.entity_type == entity_type)
    if layer_ref is not None:
        query = query.where(RevisionEntity.layer_ref == layer_ref)
    if scope == "sheet":
        # Match the room-derivation scope: don't list entities known to be off-sheet (#583).
        query = query.where(RevisionEntity.on_sheet.isnot(False))

    rows = (await db.execute(query)).scalars().all()
    contained = [
        row
        for row in rows
        if (centroid := _entity_centroid(row)) is not None
        and (smallest := _smallest_containing_room(centroid, result.rooms)) is not None
        and smallest.id == room_id
    ]

    total = len(contained)
    offset = read_cursor_int(decode_cursor_payload(cursor), "offset") if cursor else 0
    page = contained[offset : offset + limit]
    next_offset = offset + limit
    next_cursor = encode_cursor_payload({"offset": next_offset}) if next_offset < total else None

    return RevisionRoomEntityListResponse(
        manifest=RevisionEntityManifestRead.model_validate(manifest),
        room=_room_read(room),
        items=[_entity_summary(row, include=include_fields) for row in page],
        total=total,
        next_cursor=next_cursor,
    )


def _room_read(room: Room) -> RoomRead:
    """Serialize an interpreted room to its public read model."""
    bounds = (
        RoomBoundsRead(
            min_x=room.bounds[0],
            min_y=room.bounds[1],
            max_x=room.bounds[2],
            max_y=room.bounds[3],
        )
        if room.bounds is not None
        else None
    )
    location = (
        RoomLocationRead(x=room.location[0], y=room.location[1])
        if room.location is not None
        else None
    )
    return RoomRead(
        id=room.id,
        name=room.name,
        number=room.number,
        source=room.source,
        area=room.area,
        bounds=bounds,
        location=location,
        anchors=[RoomLocationRead(x=ax, y=ay) for ax, ay in room.anchors],
        needs_review=room.needs_review,
        confidence=room.confidence,
    )


def _entity_centroid(entity: RevisionEntity) -> tuple[float, float] | None:
    """Return the centroid of an entity's persisted AABB, or None if it has no bbox."""
    corners = (entity.bbox_min_x, entity.bbox_min_y, entity.bbox_max_x, entity.bbox_max_y)
    if any(corner is None for corner in corners):
        return None
    min_x, min_y, max_x, max_y = (float(corner) for corner in corners if corner is not None)
    return ((min_x + max_x) / 2, (min_y + max_y) / 2)
