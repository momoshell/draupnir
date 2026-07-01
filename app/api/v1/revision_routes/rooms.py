"""Room-containment interpretation route (tier-3, derived from the canonical model)."""

from collections.abc import Sequence
from typing import Annotated, Literal
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
from app.interpretation.devices import (
    Device,
    _TagCandidate,
    enumerate_devices,
    load_tag_candidates,
    load_text_candidates,
)
from app.interpretation.label_rooms import has_genuine_room_identity
from app.interpretation.loaders import load_revision_entities_by_type
from app.interpretation.room_pipeline import (
    ROOM_STRATEGIES,
    ROOM_STRATEGY_AUTO,
    RoomInterpretation,
    interpret_rooms,
)
from app.interpretation.rooms import (
    DevicePlacement,
    Room,
    RoomLabel,
    _smallest_containing_room,
)
from app.interpretation.service_takeoff_loaders import _resolve_input_family
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

# Entity types that can form room geometry: closed polylines / wall linework, plus
# IFC products (IfcSpace footprints carry their geometry on an ifc_product entity).
_ROOM_GEOMETRY_ENTITY_TYPES = ("polyline", "line", "ifc_product")

# Room interpretation scope (#583). "sheet" (default) restricts geometry + labels to the
# printed sheet (drops off-sheet title-block/key-plan content known to be off-sheet, keeping
# on-sheet and undetermined entities); "modelspace" interprets the full modelspace.
RoomScope = Literal["sheet", "modelspace"]
_DEFAULT_ROOM_SCOPE: RoomScope = "sheet"
_MAX_NESTING_DEPTH = 8


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
    max_depth: Annotated[int, Query(ge=0, le=_MAX_NESTING_DEPTH)] = _MAX_NESTING_DEPTH,
    scope: Annotated[RoomScope, Query()] = _DEFAULT_ROOM_SCOPE,
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
    result, input_family = await _resolve_rooms_with_family(
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

    # Presentation filter (#778/#792, tightened PDF-only #828 PR-3): surface only rooms with
    # a genuine identity. Shared predicate with /summary — see has_genuine_room_identity in
    # label_rooms.py. The registry retains ALL polygons unchanged; this filter only affects
    # the API response.
    identified_rooms = [
        room for room in result.rooms if has_genuine_room_identity(room, input_family=input_family)
    ]
    suppressed_count = len(result.rooms) - len(identified_rooms)
    # named_rooms counts over the CONFIRMED set (identified_rooms), not all result.rooms, so
    # it stays consistent with the surfaced `rooms` count — otherwise name-only rooms demoted
    # to needs_review (PDF, #828 PR-3) would inflate named_rooms above the surfaced item count.
    named_rooms = sum(1 for room in identified_rooms if room.name is not None)

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
    max_depth: Annotated[int, Query(ge=0, le=_MAX_NESTING_DEPTH)] = _MAX_NESTING_DEPTH,
    scope: Annotated[RoomScope, Query()] = _DEFAULT_ROOM_SCOPE,
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


async def _resolve_rooms(
    db: AsyncSession,
    revision_id: UUID,
    *,
    strategy: str = ROOM_STRATEGY_AUTO,
    device_layer: list[str] | None = None,
    tag_layer: list[str] | None = None,
    snap_tolerance: float = 0.0,
    min_area: float = 0.0,
    max_depth: int = _MAX_NESTING_DEPTH,
    exclude_off_sheet: bool = True,
) -> RoomInterpretation:
    """Load the room-geometry inputs and run the room interpretation pipeline.

    ``exclude_off_sheet`` (default True) scopes room geometry + labels to the printed sheet
    (#583): off-sheet title-block/key-plan linework otherwise degrades polygonization and
    merges rooms. Devices are NOT scoped here — that nested-walk scoping is #588.

    Thin wrapper over :func:`_resolve_rooms_with_family` that drops the resolved input
    family, for callers that don't need the family-aware confirmed-room rule (#828 PR-3).
    """
    result, _input_family = await _resolve_rooms_with_family(
        db,
        revision_id,
        strategy=strategy,
        device_layer=device_layer,
        tag_layer=tag_layer,
        snap_tolerance=snap_tolerance,
        min_area=min_area,
        max_depth=max_depth,
        exclude_off_sheet=exclude_off_sheet,
    )
    return result


async def _resolve_rooms_with_family(
    db: AsyncSession,
    revision_id: UUID,
    *,
    strategy: str = ROOM_STRATEGY_AUTO,
    device_layer: list[str] | None = None,
    tag_layer: list[str] | None = None,
    snap_tolerance: float = 0.0,
    min_area: float = 0.0,
    max_depth: int = _MAX_NESTING_DEPTH,
    exclude_off_sheet: bool = True,
) -> tuple[RoomInterpretation, str | None]:
    """Like :func:`_resolve_rooms`, but also returns the resolved ``input_family`` (#828 PR-3).

    Callers that apply ``has_genuine_room_identity`` as a presentation filter (``/rooms``,
    ``/summary``) need the same ``input_family`` used internally by ``interpret_rooms`` so
    the family-aware confirmed-room rule agrees end to end.
    """
    resolved_strategy = strategy if strategy in ROOM_STRATEGIES else ROOM_STRATEGY_AUTO
    entities = await load_revision_entities_by_type(
        db, revision_id, _ROOM_GEOMETRY_ENTITY_TYPES, exclude_off_sheet=exclude_off_sheet
    )
    devices = await enumerate_devices(
        db, revision_id, device_layers=device_layer, max_depth=max_depth
    )
    # Room naming/identification: an explicit ``tag_layer`` scopes the room-label source;
    # otherwise use all text (room labels live on a room-label layer, not a device-tag layer)
    # and let the pipeline auto-scope to the number-bearing layer (#549).
    label_candidates = (
        await load_tag_candidates(
            db, revision_id, tag_layers=tag_layer, exclude_off_sheet=exclude_off_sheet
        )
        if tag_layer
        else await load_text_candidates(db, revision_id, exclude_off_sheet=exclude_off_sheet)
    )
    # PDF-gated annotation-zone exclusion (#828 PR-2) + confirmed-room rule (#828 PR-3):
    # input_family is resolved from the revision's adapter run (same helper
    # service_takeoff_loaders uses); None (unknown origin) or any non-pdf family leaves
    # interpret_rooms's gates a no-op — DWG stays byte-identical.
    input_family = await _resolve_input_family(db, revision_id)
    result = interpret_rooms(
        entities,
        devices=_device_placements(devices),
        labels=_room_labels(label_candidates),
        strategy=resolved_strategy,
        snap_tolerance=snap_tolerance,
        min_area=min_area,
        input_family=input_family,
    )
    return result, input_family


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


def _device_placements(devices: Sequence[Device]) -> list[DevicePlacement]:
    """Adapt enumerated devices with a world position into room-assignment inputs."""
    placements: list[DevicePlacement] = []
    for device in devices:
        position = device.position
        if position is None:
            continue
        x, y = position.get("x"), position.get("y")
        if x is None or y is None:
            continue
        placements.append(DevicePlacement(device_id=device.entity_id, point=(float(x), float(y))))
    return placements


def _room_labels(candidates: Sequence[_TagCandidate]) -> list[RoomLabel]:
    """Adapt text candidates into room labels (text + placement + layer)."""
    return [
        RoomLabel(text=candidate.text, point=(candidate.x, candidate.y), layer=candidate.layer_ref)
        for candidate in candidates
    ]
