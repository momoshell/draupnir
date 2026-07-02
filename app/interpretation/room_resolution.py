"""Room-resolution service (tier-3 room interpretation, extracted from the rooms route, #852).

Pure interpretation logic: loads room-geometry / device / label inputs and runs the
:func:`~app.interpretation.room_pipeline.interpret_rooms` pipeline. No FastAPI/route
concerns live here — those stay in ``app.api.v1.revision_routes.rooms`` as thin glue.

Room-id determinism is load-bearing (``list_revision_room_entities`` matches ``room_id``
against a freshly recomputed set; the room-materializer persists ``room_key`` derived
from the same ids), so these bodies are copied verbatim from the route module with no
behavior change.
"""

from collections.abc import Sequence
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
from app.interpretation.service_takeoff_loaders import _resolve_input_family

# Entity types that can form room geometry: closed polylines / wall linework, plus
# IFC products (IfcSpace footprints carry their geometry on an ifc_product entity).
_ROOM_GEOMETRY_ENTITY_TYPES = ("polyline", "line", "ifc_product")

# Max nesting depth used by the resolver's device enumeration. The Query-facing
# RoomScope / _DEFAULT_ROOM_SCOPE / _MAX_NESTING_DEPTH stay in rooms.py (route-facing
# concerns: FastAPI Query bounds), only the resolver internals moved here (#852).
_MAX_NESTING_DEPTH = 8


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
