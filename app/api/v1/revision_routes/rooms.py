"""Room-containment interpretation route (tier-3, derived from the canonical model)."""

from collections.abc import Sequence
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.revision_lineage import _get_active_revision_manifest_or_409
from app.db.session import get_db
from app.interpretation.devices import (
    Device,
    _TagCandidate,
    enumerate_devices,
    load_tag_candidates,
)
from app.interpretation.loaders import load_revision_entities_by_type
from app.interpretation.room_pipeline import (
    ROOM_STRATEGIES,
    ROOM_STRATEGY_AUTO,
    interpret_rooms,
)
from app.interpretation.rooms import DevicePlacement, RoomLabel
from app.schemas.revision import RevisionEntityManifestRead
from app.schemas.rooms import (
    DeviceRoomAssignmentRead,
    RevisionRoomListResponse,
    RoomBoundsRead,
    RoomRead,
)

rooms_router = APIRouter()

# Entity types that can form room geometry: closed polylines / wall linework, plus
# IFC products (IfcSpace footprints carry their geometry on an ifc_product entity).
_ROOM_GEOMETRY_ENTITY_TYPES = ("polyline", "line", "ifc_product")
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
) -> RevisionRoomListResponse:
    """Derive rooms and assign devices to them by boundary containment.

    ``strategy`` selects the room source: ``auto`` (default) prefers an explicit
    room/space-boundary layer and falls back to wall-linework polygonization;
    ``explicit_layer`` / ``wall_polygonize`` force one source. Each device is
    assigned to the smallest room whose polygon contains its world position, and
    each room is named from the room-tag text inside it. ``snap_tolerance`` and
    ``min_area`` tune the wall-polygonize robustness (gap-closing, sliver dropping).
    """
    resolved_strategy = strategy if strategy in ROOM_STRATEGIES else ROOM_STRATEGY_AUTO

    manifest = await _get_active_revision_manifest_or_409(revision_id, db)
    entities = await load_revision_entities_by_type(db, revision_id, _ROOM_GEOMETRY_ENTITY_TYPES)
    devices = await enumerate_devices(
        db, revision_id, device_layers=device_layer, max_depth=max_depth
    )
    candidates = await load_tag_candidates(db, revision_id, tag_layers=tag_layer)

    result = interpret_rooms(
        entities,
        devices=_device_placements(devices),
        labels=_room_labels(candidates),
        strategy=resolved_strategy,
        snap_tolerance=snap_tolerance,
        min_area=min_area,
    )

    named_rooms = sum(1 for room in result.rooms if room.name is not None)
    return RevisionRoomListResponse(
        manifest=RevisionEntityManifestRead.model_validate(manifest),
        strategy=result.strategy,
        source_layers=list(result.source_layers),
        items=[
            RoomRead(
                id=room.id,
                name=room.name,
                source=room.source,
                area=room.area,
                bounds=RoomBoundsRead(
                    min_x=room.bounds[0],
                    min_y=room.bounds[1],
                    max_x=room.bounds[2],
                    max_y=room.bounds[3],
                ),
                confidence=room.confidence,
            )
            for room in result.rooms
        ],
        assignments=[
            DeviceRoomAssignmentRead(device_id=assignment.device_id, room_id=assignment.room_id)
            for assignment in result.device_assignments
        ],
        summary={
            "rooms": len(result.rooms),
            "named_rooms": named_rooms,
            "assigned_devices": len(result.device_assignments),
        },
    )


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
    """Adapt tag-text candidates into room labels (text + placement)."""
    return [
        RoomLabel(text=candidate.text, point=(candidate.x, candidate.y)) for candidate in candidates
    ]
