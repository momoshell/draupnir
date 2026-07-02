"""Room materialization orchestrator (worker-side, impure).

Loads the same room-interpretation inputs the ``/rooms`` route loads (entities,
devices, labels, ``input_family``), runs :func:`~app.interpretation.room_pipeline.
interpret_rooms`, and persists only the GENUINE (surfaced) room set — the same
family-aware filter (:func:`~app.interpretation.label_rooms.has_genuine_room_identity`)
``/rooms`` applies at read time — as :class:`~app.models.revision_room.RevisionRoom` rows
via an idempotent INSERT ... ON CONFLICT DO NOTHING. Anonymous wall-polygon junk and
spec-prose-named cells are never stored; the persisted set is exactly what ``/rooms`` and
``/summary`` serve, and it stays regenerable via ``ROOM_ALGO_VERSION`` if the filter or
interpretation logic changes.

This module is intentionally impure: it accesses the DB and imports ORM models. It is
NOT imported on the read path — the read path (``/rooms``) still recomputes
``interpret_rooms`` and ``has_genuine_room_identity`` directly.
"""

from __future__ import annotations

import uuid
from typing import Any
from uuid import UUID

from shapely.geometry import mapping
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.interpretation.label_rooms import has_genuine_room_identity
from app.interpretation.room_resolution import _resolve_rooms_with_family
from app.interpretation.rooms import Room
from app.models.revision_room import RevisionRoom
from app.models.revision_room_summary import RevisionRoomSummary

logger = get_logger(__name__)

# Room extraction algorithm version string. Bump this when a room-derivation logic change
# (geometry, label enrichment, PDF confirmed-room rule, etc.) should invalidate previously
# materialized rows for a revision — a new version simply produces new rows alongside the
# old ones (append-only), never mutates them.
#
# Bumped 1 -> 2: rooms now persist assigned_device_ids_json and a sibling
# RevisionRoomSummary row exists per (revision, algo_version) as the read-path
# materialized signal (#831 read-path flip).
ROOM_ALGO_VERSION: str = "2"

_ROOM_POLYGON_JSON_SCHEMA_VERSION: str = "0.1"

# Chunk size for the batched INSERT below: each row binds ~20 columns, and asyncpg caps a
# single query at 32767 bind parameters, so a revision with a very large room registry (e.g. a
# dense PDF label cloud) must be split across statements rather than inserted in one VALUES list.
_ROOM_INSERT_CHUNK_SIZE = 500


def _polygon_to_json(room: Room) -> dict[str, Any] | None:
    """Serialize a room's polygon to a JSON-safe payload, or ``None`` for label-only rooms."""
    if room.polygon is None:
        return None
    return {
        "schema_version": _ROOM_POLYGON_JSON_SCHEMA_VERSION,
        "geometry": mapping(room.polygon),
    }


def _anchors_to_json(room: Room) -> list[list[float]]:
    """Serialize a room's label anchor points to a JSON-safe payload."""
    return [[float(x), float(y)] for (x, y) in room.anchors]


def _device_ids_by_room(
    device_assignments: list[Any],
) -> dict[str, list[str]]:
    """Group ordered device_ids by room_id, preserving per-room append order."""
    grouped: dict[str, list[str]] = {}
    for assignment in device_assignments:
        grouped.setdefault(assignment.room_id, []).append(assignment.device_id)
    return grouped


async def materialize_rooms(
    session: AsyncSession,
    *,
    job_id: UUID,
    project_id: UUID,
    source_file_id: UUID,
    drawing_revision_id: UUID,
    adapter_run_output_id: UUID | None,
    canonical_entity_schema_version: str,
) -> list[Room]:
    """Load room-interpretation inputs, run ``interpret_rooms``, and persist the genuine set.

    Idempotent: uses ``INSERT ... ON CONFLICT (uq_revision_rooms_group_version) DO NOTHING`` so
    duplicate calls for the same (revision, room_key, algo_version) are safe. Append-only trigger
    on the table prevents UPDATE/DELETE.

    Persists only the rooms that pass ``has_genuine_room_identity`` (family-aware) — the exact
    same presentation filter the ``/rooms`` and ``/summary`` routes apply at read time. Anonymous
    wall-polygon cells and spec-prose-named cells from the interpretation registry
    (``RoomInterpretation.rooms``) are filtered out here and never stored; they remain
    regenerable (not lost) by re-running ``interpret_rooms``.

    Parameters
    ----------
    session:
        Active async session (must NOT be inside the ingest transaction).
    job_id:
        The ROOMS job that triggered this materialization (lineage).
    project_id, source_file_id, drawing_revision_id:
        Revision lineage columns.
    adapter_run_output_id:
        Nullable lineage column from the revision's manifest.
    canonical_entity_schema_version:
        Schema version from the revision's manifest.

    Returns
    -------
    list[Room]
        The genuine (surfaced) room set — the same rooms ``/rooms`` returns — which may be
        empty if the revision has no room geometry or labels with a genuine identity.
    """
    result, input_family = await _resolve_rooms_with_family(session, drawing_revision_id)
    rooms = [
        room for room in result.rooms if has_genuine_room_identity(room, input_family=input_family)
    ]
    device_ids_by_room = _device_ids_by_room(result.device_assignments)

    values: list[dict[str, Any]] = []
    for ordinal, room in enumerate(rooms):
        values.append(
            {
                "id": uuid.uuid4(),
                "project_id": project_id,
                "source_file_id": source_file_id,
                "extraction_profile_id": None,
                "source_job_id": job_id,
                "drawing_revision_id": drawing_revision_id,
                "adapter_run_output_id": adapter_run_output_id,
                "canonical_entity_schema_version": canonical_entity_schema_version,
                "algo_version": ROOM_ALGO_VERSION,
                "room_key": room.id,
                "name": room.name,
                "number": room.number,
                "source": room.source,
                "area": room.area,
                "bounds_min_x": room.bounds[0] if room.bounds is not None else None,
                "bounds_min_y": room.bounds[1] if room.bounds is not None else None,
                "bounds_max_x": room.bounds[2] if room.bounds is not None else None,
                "bounds_max_y": room.bounds[3] if room.bounds is not None else None,
                "polygon_geometry_json": _polygon_to_json(room),
                "anchors_json": _anchors_to_json(room),
                "needs_review": room.needs_review,
                "confidence": room.confidence,
                "strategy": result.strategy,
                "source_layers_json": list(result.source_layers),
                "input_family": input_family,
                "assigned_device_ids_json": device_ids_by_room.get(room.id, []),
                "ordinal": ordinal,
            }
        )

    for start in range(0, len(values), _ROOM_INSERT_CHUNK_SIZE):
        chunk = values[start : start + _ROOM_INSERT_CHUNK_SIZE]
        stmt = (
            insert(RevisionRoom)
            .values(chunk)
            .on_conflict_do_nothing(constraint="uq_revision_rooms_group_version")
        )
        await session.execute(stmt)

    # Summary row inserted AFTER the room rows, in the same transaction/session: a crash
    # between the two can only leave rooms-without-marker (reads as a miss -> harmless
    # re-enqueue), never marker-without-rooms (which would read as materialized-empty).
    summary_stmt = (
        insert(RevisionRoomSummary)
        .values(
            id=uuid.uuid4(),
            project_id=project_id,
            source_file_id=source_file_id,
            extraction_profile_id=None,
            source_job_id=job_id,
            drawing_revision_id=drawing_revision_id,
            adapter_run_output_id=adapter_run_output_id,
            canonical_entity_schema_version=canonical_entity_schema_version,
            algo_version=ROOM_ALGO_VERSION,
            full_registry_size=len(result.rooms),
        )
        .on_conflict_do_nothing(constraint="uq_revision_room_summary_version")
    )
    await session.execute(summary_stmt)

    logger.info(
        "room_materialization_persisted",
        job_id=str(job_id),
        drawing_revision_id=str(drawing_revision_id),
        room_count=len(rooms),
        full_registry_size=len(result.rooms),
        algo_version=ROOM_ALGO_VERSION,
    )
    return rooms
