"""DB-backed room interpretation service.

This module owns the room-resolution DB seam so API routes and interpretation
loaders can share it without importing route modules.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.interpretation.devices import (
    Device,
    _TagCandidate,
    enumerate_devices,
    load_tag_candidates,
    load_text_candidates,
)
from app.interpretation.loaders import load_revision_entities_by_type
from app.interpretation.room_pipeline import (
    ROOM_STRATEGIES,
    ROOM_STRATEGY_AUTO,
    RoomInterpretation,
    interpret_rooms,
)
from app.interpretation.rooms import DevicePlacement, RoomLabel

# Entity types that can form room geometry: closed polylines / wall linework,
# plus IFC products (IfcSpace footprints carry their geometry on an ifc_product entity).
ROOM_GEOMETRY_ENTITY_TYPES = ("polyline", "line", "ifc_product")

# Room interpretation scope (#583).
RoomScope = Literal["sheet", "modelspace"]
DEFAULT_ROOM_SCOPE: RoomScope = "sheet"
MAX_NESTING_DEPTH = 8


async def resolve_rooms(
    db: AsyncSession,
    revision_id: UUID,
    *,
    strategy: str = ROOM_STRATEGY_AUTO,
    device_layer: list[str] | None = None,
    tag_layer: list[str] | None = None,
    snap_tolerance: float = 0.0,
    min_area: float = 0.0,
    max_depth: int = MAX_NESTING_DEPTH,
    exclude_off_sheet: bool = True,
) -> RoomInterpretation:
    """Load room inputs and run the room interpretation pipeline.

    ``exclude_off_sheet`` (default True) scopes room geometry + labels to the printed sheet
    (#583): off-sheet title-block/key-plan linework otherwise degrades polygonization and
    merges rooms. Devices are NOT scoped here; that nested-walk scoping is #588.
    """
    resolved_strategy = strategy if strategy in ROOM_STRATEGIES else ROOM_STRATEGY_AUTO
    entities = await load_revision_entities_by_type(
        db, revision_id, ROOM_GEOMETRY_ENTITY_TYPES, exclude_off_sheet=exclude_off_sheet
    )
    devices = await enumerate_devices(
        db, revision_id, device_layers=device_layer, max_depth=max_depth
    )
    # Room naming/identification: an explicit ``tag_layer`` scopes the room-label source;
    # otherwise use all text and let the pipeline auto-scope to the number-bearing layer.
    label_candidates = (
        await load_tag_candidates(
            db, revision_id, tag_layers=tag_layer, exclude_off_sheet=exclude_off_sheet
        )
        if tag_layer
        else await load_text_candidates(db, revision_id, exclude_off_sheet=exclude_off_sheet)
    )
    return interpret_rooms(
        entities,
        devices=device_placements(devices),
        labels=room_labels(label_candidates),
        strategy=resolved_strategy,
        snap_tolerance=snap_tolerance,
        min_area=min_area,
    )


def device_placements(devices: Sequence[Device]) -> list[DevicePlacement]:
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


def room_labels(candidates: Sequence[_TagCandidate]) -> list[RoomLabel]:
    """Adapt text candidates into room labels."""
    return [
        RoomLabel(text=candidate.text, point=(candidate.x, candidate.y), layer=candidate.layer_ref)
        for candidate in candidates
    ]
