"""Read-path loaders for materialized rooms (#831 read-path flip).

Reconstructs interpretation-shaped :class:`~app.interpretation.rooms.Room` and
:class:`~app.interpretation.rooms.DeviceRoomAssignment` values from persisted
:class:`~app.models.revision_room.RevisionRoom` rows rather than recomputing
``interpret_rooms``. Device assignments are EXPANDED from each row's
``assigned_device_ids_json`` — never recomputed — because recomputation diverges
via the ``already_assigned`` interaction in ``assign_devices_to_label_rooms``
(``room_pipeline.py:152``).

:func:`load_room_summary`'s return value (``None`` vs a row) IS the version-scoped
materialized signal the read path checks: a row means the genuine room set for that
``algo_version`` has been persisted and can be loaded directly; ``None`` means the
caller should fall back to live computation and lazily enqueue materialization.

Both loaders are read-path safe: they never raise. Any DB or data-shape error is
logged and swallowed, returning the documented "miss"/"empty" sentinel so a
corrupt or partial row can never turn into a 500.
"""

from __future__ import annotations

from uuid import UUID

from shapely.geometry import shape
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.interpretation.rooms import DeviceRoomAssignment, Room
from app.models.revision_room import RevisionRoom
from app.models.revision_room_summary import RevisionRoomSummary

logger = get_logger(__name__)


def _resolve_algo_version(algo_version: str | None) -> str:
    """Resolve the version to query, defaulting to the current materializer version.

    Lazy import avoids a read-path -> app.jobs.room_materialization -> (worker-side)
    import cycle: room_materialization.py imports from app.api.v1.revision_routes.rooms.
    """
    if algo_version is not None:
        return algo_version
    from app.jobs.room_materialization import ROOM_ALGO_VERSION

    return ROOM_ALGO_VERSION


async def load_room_summary(
    db: AsyncSession,
    revision_id: UUID,
    *,
    algo_version: str | None = None,
) -> RevisionRoomSummary | None:
    """Return the version-scoped materialization marker for a revision, or ``None``.

    Its existence IS the materialized signal: a row present for
    (``revision_id``, resolved ``algo_version``) means the genuine room set has
    been persisted as :class:`~app.models.revision_room.RevisionRoom` rows and
    :func:`load_rooms` can be used instead of recomputing ``interpret_rooms``.

    Never raises — any error is logged and treated as a miss (``None``), so the
    caller falls back to the live path.
    """
    resolved_version = _resolve_algo_version(algo_version)
    try:
        row: RevisionRoomSummary | None = await db.scalar(
            select(RevisionRoomSummary)
            .where(
                RevisionRoomSummary.drawing_revision_id == revision_id,
                RevisionRoomSummary.algo_version == resolved_version,
            )
            .limit(1)
        )
        return row
    except Exception:
        logger.warning(
            "room_summary_load_failed",
            revision_id=str(revision_id),
            algo_version=resolved_version,
            exc_info=True,
        )
        return None


async def load_rooms(
    db: AsyncSession,
    revision_id: UUID,
    *,
    algo_version: str | None = None,
) -> tuple[list[Room], list[DeviceRoomAssignment]]:
    """Reconstruct the persisted genuine room set + expanded device assignments.

    Reconstructs each :class:`~app.interpretation.rooms.Room` from its
    :class:`~app.models.revision_room.RevisionRoom` row (polygon, bounds, anchors,
    ``location``). Device assignments are EXPANDED from each row's
    ``assigned_device_ids_json`` (NOT recomputed), preserving row order (by ``ordinal``)
    and then intra-row (per-room) order — the same order the live pipeline produced when
    the row was stamped.

    Never raises — any error is logged and treated as empty (``([], [])``).
    """
    resolved_version = _resolve_algo_version(algo_version)
    try:
        rows = (
            await db.scalars(
                select(RevisionRoom)
                .where(
                    RevisionRoom.drawing_revision_id == revision_id,
                    RevisionRoom.algo_version == resolved_version,
                )
                # ordinal preserves the genuine `interpret_rooms` item order stamped at
                # materialization time (created_at/id ties within a batch insert are
                # otherwise unordered).
                .order_by(RevisionRoom.ordinal.asc())
            )
        ).all()

        rooms: list[Room] = []
        assignments: list[DeviceRoomAssignment] = []
        for row in rows:
            rooms.append(_room_from_row(row))
            for device_id in row.assigned_device_ids_json or []:
                assignments.append(DeviceRoomAssignment(device_id=device_id, room_id=row.room_key))
        return rooms, assignments
    except Exception:
        logger.warning(
            "rooms_load_failed",
            revision_id=str(revision_id),
            algo_version=resolved_version,
            exc_info=True,
        )
        return [], []


def _room_from_row(row: RevisionRoom) -> Room:
    """Reconstruct a :class:`Room` from a persisted :class:`RevisionRoom` row.

    Field order matches ``rooms.py:45`` (``Room`` dataclass declaration order).
    """
    polygon = None
    if row.polygon_geometry_json is not None:
        geometry = row.polygon_geometry_json.get("geometry")
        if geometry is not None:
            polygon = shape(geometry)
    bounds: tuple[float, float, float, float] | None = None
    if (
        row.bounds_min_x is not None
        and row.bounds_min_y is not None
        and row.bounds_max_x is not None
        and row.bounds_max_y is not None
    ):
        bounds = (row.bounds_min_x, row.bounds_min_y, row.bounds_max_x, row.bounds_max_y)
    anchors = tuple((float(x), float(y)) for x, y in (row.anchors_json or []))
    return Room(
        id=row.room_key,
        name=row.name,
        source=row.source,
        polygon=polygon,
        area=row.area,
        bounds=bounds,
        confidence=row.confidence,
        number=row.number,
        location=anchors[0] if anchors else None,
        anchors=anchors,
        needs_review=row.needs_review,
    )
