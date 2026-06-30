"""DB seam for the room registry (issue #717, Phase R-C).

Single entry point: ``load_room_registry`` fetches rooms from the DB and hands them
to the pure ``build_room_registry``.
"""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.interpretation.geometry import point_in_polygon
from app.interpretation.room_fusion import (
    DEFAULT_VORONOI_CONFIDENCE,
    RoomRegistry,
    build_room_registry,
)
from app.interpretation.room_pipeline import ROOM_STRATEGY_AUTO
from app.interpretation.room_resolution import resolve_rooms, room_labels
from app.interpretation.room_voronoi import DEFAULT_BOUND_MARGIN_M, RoomTag
from app.interpretation.rooms import Room


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

    Multi-tag polygon subdivision (issue #733): after resolving rooms, compute
    which polygons contain ≥2 distinct numbered tags and build the per-polygon
    RoomTag map.  Passed to ``build_room_registry`` so ``classify`` can
    sub-partition under-segmented polygons.
    """
    from app.interpretation.devices import load_text_candidates
    from app.interpretation.label_rooms import identify_rooms_from_labels

    exclude_off_sheet = scope == "sheet"

    result = await resolve_rooms(
        db,
        reference_revision_id,
        strategy=strategy,
        snap_tolerance=snap_tolerance,
        min_area=min_area,
        exclude_off_sheet=exclude_off_sheet,
    )

    # -- Multi-tag polygon subdivision (issue #733) ---------------------------
    # Re-load text candidates (same call as resolve_rooms uses for labels) and
    # identify room label rooms so we can find which polygon rooms contain ≥2
    # distinct numbered tags.
    label_candidates = await load_text_candidates(
        db, reference_revision_id, exclude_off_sheet=exclude_off_sheet
    )
    raw_labels = room_labels(label_candidates)
    identities = identify_rooms_from_labels(raw_labels)

    # Collect polygon rooms (those with an actual polygon boundary).
    polygon_rooms: list[Room] = [r for r in result.rooms if r.polygon is not None]

    # Map each identity with a number + location to the smallest containing polygon room.
    number_anchors_by_polygon: dict[str, dict[str, list[tuple[float, float]]]] = {}
    for identity in identities:
        if identity.number is None or identity.location is None:
            continue
        # Find the smallest polygon room containing this label's location.
        containing_room = _smallest_polygon_room(identity.location, polygon_rooms)
        if containing_room is None:
            continue
        pid = containing_room.id
        num = identity.number
        if pid not in number_anchors_by_polygon:
            number_anchors_by_polygon[pid] = {}
        anchor_map = number_anchors_by_polygon[pid]
        # Collect anchor points that fall inside this polygon.
        all_anchors = list(identity.anchors) if identity.anchors else [identity.location]
        for pt in all_anchors:
            poly = containing_room.polygon
            if poly is not None and point_in_polygon(pt, poly):
                anchor_map.setdefault(num, []).append(pt)

    # Build the polygon_tags_by_room_id map: only include polygons with ≥2 distinct numbers.
    polygon_tags_by_room_id: dict[str, tuple[RoomTag, ...]] = {}
    for pid, anchor_map in number_anchors_by_polygon.items():
        if len(anchor_map) < 2:
            continue
        tags: list[RoomTag] = []
        for number, pts in anchor_map.items():
            # Dedupe anchor points and sort by (x, y) for determinism.
            seen: set[tuple[float, float]] = set()
            deduped: list[tuple[float, float]] = []
            for pt in pts:
                if pt not in seen:
                    seen.add(pt)
                    deduped.append(pt)
            deduped.sort(key=lambda p: (p[0], p[1]))
            if deduped:
                tags.append(RoomTag(number=number, anchors=tuple(deduped)))
        if len(tags) >= 2:
            tags.sort(key=lambda t: t.number)
            polygon_tags_by_room_id[pid] = tuple(tags)

    return build_room_registry(
        result.rooms,
        voronoi_fallback=voronoi_fallback,
        margin=margin,
        voronoi_confidence=voronoi_confidence,
        polygon_tags_by_room_id=polygon_tags_by_room_id,
    )


def _smallest_polygon_room(
    point: tuple[float, float],
    polygon_rooms: list[Room],
) -> Room | None:
    """Return the smallest polygon room containing ``point``, or None.

    Mirrors ``_containing_polygon_room`` from ``room_pipeline`` — kept as a
    module-private helper here to avoid importing room_pipeline at load time.
    ``point_in_polygon`` is imported at module level (pure, no DB/API).
    """
    containing = [
        room
        for room in polygon_rooms
        if room.polygon is not None
        and room.area is not None
        and point_in_polygon(point, room.polygon)
    ]
    if not containing:
        return None
    return min(containing, key=lambda r: r.area if r.area is not None else 0.0)
