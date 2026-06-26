"""DB seam for the room registry (issue #717, Phase R-C).

Single entry point: ``load_room_registry`` fetches rooms from the DB and hands them
to the pure ``build_room_registry``.

The import of ``_resolve_rooms`` from ``app.api.v1.revision_routes.rooms`` is LAZY
(inside the function body) to avoid an interpretation→api module cycle (#705 class).
This mirrors the pattern used by ``service_takeoff_loaders``.
"""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.interpretation.room_fusion import (
    DEFAULT_VORONOI_CONFIDENCE,
    RoomRegistry,
    build_room_registry,
)
from app.interpretation.room_pipeline import ROOM_STRATEGY_AUTO
from app.interpretation.room_voronoi import DEFAULT_BOUND_MARGIN_M


async def load_room_registry(
    db: AsyncSession,
    reference_revision_id: UUID,
    *,
    strategy: str = ROOM_STRATEGY_AUTO,
    snap_tolerance: float = 0.0,
    min_area: float = 0.0,
    scope: Literal["sheet", "modelspace"] = "sheet",
    voronoi_fallback: bool = True,
    margin: float = DEFAULT_BOUND_MARGIN_M,
    voronoi_confidence: float = DEFAULT_VORONOI_CONFIDENCE,
) -> RoomRegistry:
    """Load rooms from the DB and build a frozen :class:`RoomRegistry`.

    ``scope`` maps to the ``exclude_off_sheet`` flag: ``"sheet"`` (default)
    restricts room geometry + labels to the printed sheet; ``"modelspace"``
    interprets the full modelspace.

    The import of ``_resolve_rooms`` is deferred to the function body to avoid
    an interpretation→api import cycle (#705 class).
    """
    # Lazy import — keeps this module importable without app.api at module level.
    from app.api.v1.revision_routes.rooms import _resolve_rooms

    result = await _resolve_rooms(
        db,
        reference_revision_id,
        strategy=strategy,
        snap_tolerance=snap_tolerance,
        min_area=min_area,
        exclude_off_sheet=scope == "sheet",
    )

    return build_room_registry(
        result.rooms,
        voronoi_fallback=voronoi_fallback,
        margin=margin,
        voronoi_confidence=voronoi_confidence,
    )
