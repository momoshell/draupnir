"""Tests for RevisionRoom model, unique-constraint idempotency, and append-only triggers."""

from __future__ import annotations

import uuid
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy import text

import app.db.session as session_module
from tests.conftest import requires_database

# ---------------------------------------------------------------------------
# DB helpers (conn is AsyncConnection from engine.begin())
# ---------------------------------------------------------------------------


async def _create_minimal_schema(conn: Any, schema_name: str) -> dict[str, uuid.UUID]:
    """Create a minimal schema with the tables needed for revision_rooms inserts."""

    await conn.execute(text(f'SET search_path TO "{schema_name}"'))

    await conn.execute(
        text(
            """
            CREATE TABLE projects (
                id UUID PRIMARY KEY
            )
            """
        )
    )
    await conn.execute(
        text(
            """
            CREATE TABLE files (
                id UUID PRIMARY KEY,
                project_id UUID NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
                UNIQUE (id, project_id)
            )
            """
        )
    )
    await conn.execute(
        text(
            """
            CREATE TABLE jobs (
                id UUID PRIMARY KEY,
                project_id UUID NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
                file_id UUID NOT NULL,
                job_type VARCHAR(64) NOT NULL,
                status VARCHAR(32) NOT NULL DEFAULT 'pending',
                FOREIGN KEY (file_id, project_id)
                    REFERENCES files(id, project_id) ON DELETE RESTRICT
            )
            """
        )
    )
    await conn.execute(
        text(
            """
            CREATE TABLE drawing_revisions (
                id UUID PRIMARY KEY,
                project_id UUID NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
                source_file_id UUID NOT NULL,
                FOREIGN KEY (source_file_id, project_id)
                    REFERENCES files(id, project_id) ON DELETE RESTRICT,
                UNIQUE (id, project_id)
            )
            """
        )
    )
    await conn.execute(
        text(
            """
            CREATE TABLE revision_rooms (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                project_id UUID NOT NULL
                    REFERENCES projects(id) ON DELETE RESTRICT,
                source_file_id UUID NOT NULL,
                extraction_profile_id UUID,
                source_job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
                drawing_revision_id UUID NOT NULL,
                adapter_run_output_id UUID,
                canonical_entity_schema_version VARCHAR(16) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                algo_version VARCHAR(32) NOT NULL,
                room_key VARCHAR(255) NOT NULL,
                name VARCHAR(255),
                number VARCHAR(64),
                source VARCHAR(32) NOT NULL,
                area DOUBLE PRECISION,
                bounds_min_x DOUBLE PRECISION,
                bounds_min_y DOUBLE PRECISION,
                bounds_max_x DOUBLE PRECISION,
                bounds_max_y DOUBLE PRECISION,
                polygon_geometry_json JSON,
                anchors_json JSON NOT NULL,
                needs_review BOOLEAN NOT NULL,
                confidence DOUBLE PRECISION,
                strategy VARCHAR(64) NOT NULL,
                source_layers_json JSON NOT NULL,
                input_family VARCHAR(32),
                FOREIGN KEY (source_file_id, project_id)
                    REFERENCES files(id, project_id) ON DELETE RESTRICT,
                FOREIGN KEY (drawing_revision_id, project_id)
                    REFERENCES drawing_revisions(id, project_id) ON DELETE RESTRICT,
                UNIQUE (id, project_id),
                UNIQUE (drawing_revision_id, room_key, algo_version)
            )
            """
        )
    )

    # Attach append-only triggers (functions already exist in public schema).
    await conn.execute(
        text(
            """
            CREATE TRIGGER trg_append_only_row_guard
            BEFORE UPDATE OR DELETE ON revision_rooms
            FOR EACH ROW
            EXECUTE FUNCTION public.enforce_append_only_lineage_row()
            """
        )
    )
    await conn.execute(
        text(
            """
            CREATE TRIGGER trg_append_only_truncate_guard
            BEFORE TRUNCATE ON revision_rooms
            FOR EACH STATEMENT
            EXECUTE FUNCTION public.enforce_append_only_lineage_truncate()
            """
        )
    )

    # Seed reference rows.
    ids: dict[str, uuid.UUID] = {
        "project": uuid.uuid4(),
        "file": uuid.uuid4(),
        "revision": uuid.uuid4(),
        "job": uuid.uuid4(),
    }

    await conn.execute(
        text("INSERT INTO projects (id) VALUES (:project)"),
        {"project": ids["project"]},
    )
    await conn.execute(
        text("INSERT INTO files (id, project_id) VALUES (:file, :project)"),
        {"file": ids["file"], "project": ids["project"]},
    )
    await conn.execute(
        text(
            "INSERT INTO drawing_revisions (id, project_id, source_file_id) "
            "VALUES (:revision, :project, :file)"
        ),
        {"revision": ids["revision"], "project": ids["project"], "file": ids["file"]},
    )
    await conn.execute(
        text(
            "INSERT INTO jobs (id, project_id, file_id, job_type) "
            "VALUES (:job, :project, :file, 'ingest')"
        ),
        {"job": ids["job"], "project": ids["project"], "file": ids["file"]},
    )

    return ids


async def _insert_room(
    conn: Any,
    ids: dict[str, uuid.UUID],
    *,
    row_id: uuid.UUID | None = None,
    room_key: str = "room-001",
    algo_version: str = "v1",
) -> uuid.UUID:
    row_id = row_id or uuid.uuid4()
    await conn.execute(
        text(
            """
            INSERT INTO revision_rooms (
                id, project_id, source_file_id, source_job_id,
                drawing_revision_id, canonical_entity_schema_version,
                algo_version, room_key, name, number, source,
                area, bounds_min_x, bounds_min_y, bounds_max_x, bounds_max_y,
                polygon_geometry_json, anchors_json, needs_review,
                confidence, strategy, source_layers_json, input_family
            ) VALUES (
                :id, :project, :file, :job,
                :revision, '1.0',
                :algo_version, :room_key, 'Room 101', '101', 'polygon',
                12.5, 0.0, 0.0, 5.0, 2.5,
                CAST(:polygon_geometry_json AS json), CAST(:anchors_json AS json), FALSE,
                0.9, 'wall_derived', CAST(:source_layers_json AS json), 'dwg'
            )
            """
        ),
        {
            "id": row_id,
            "project": ids["project"],
            "file": ids["file"],
            "job": ids["job"],
            "revision": ids["revision"],
            "algo_version": algo_version,
            "room_key": room_key,
            "polygon_geometry_json": '{"type": "polygon", "coordinates": [[0.0, 0.0]]}',
            "anchors_json": '[{"x": 2.5, "y": 1.25}]',
            "source_layers_json": '["A-AREA"]',
        },
    )
    return row_id


# ---------------------------------------------------------------------------
# Insert + unique-constraint idempotency tests
# ---------------------------------------------------------------------------


@requires_database
async def test_insert_revision_room_succeeds() -> None:
    """A basic revision_rooms insert should succeed and be readable."""

    engine = session_module.get_engine()
    assert engine is not None

    schema_name = f"t_room_ins_{uuid.uuid4().hex[:12]}"

    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

        async with engine.begin() as conn:
            ids = await _create_minimal_schema(conn, schema_name)
            row_id = await _insert_room(conn, ids)

            result = await conn.execute(
                text("SELECT room_key, algo_version FROM revision_rooms WHERE id = :id"),
                {"id": row_id},
            )
            row = result.fetchone()
            assert row is not None
            assert row[0] == "room-001"
            assert row[1] == "v1"

    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))


@requires_database
async def test_unique_constraint_rejects_duplicate_group_version() -> None:
    """A second insert with the same (revision, room_key, algo_version) must be rejected."""

    engine = session_module.get_engine()
    assert engine is not None

    schema_name = f"t_room_uniq_{uuid.uuid4().hex[:12]}"

    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

        async with engine.begin() as conn:
            ids = await _create_minimal_schema(conn, schema_name)
            await _insert_room(conn, ids, room_key="room-001", algo_version="v1")

        with pytest.raises(sa.exc.IntegrityError):
            async with engine.begin() as conn:
                await conn.execute(text(f'SET search_path TO "{schema_name}"'))
                await _insert_room(conn, ids, room_key="room-001", algo_version="v1")

    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))


# ---------------------------------------------------------------------------
# Append-only trigger tests
# ---------------------------------------------------------------------------


@requires_database
async def test_append_only_trigger_rejects_update_on_revision_rooms() -> None:
    """UPDATE on revision_rooms must be blocked by the append-only trigger."""

    engine = session_module.get_engine()
    assert engine is not None

    schema_name = f"t_room_upd_{uuid.uuid4().hex[:12]}"

    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

        async with engine.begin() as conn:
            ids = await _create_minimal_schema(conn, schema_name)
            row_id = await _insert_room(conn, ids)

        with pytest.raises(sa.exc.DBAPIError) as exc_info:
            async with engine.begin() as conn:
                await conn.execute(text(f'SET search_path TO "{schema_name}"'))
                await conn.execute(
                    text("UPDATE revision_rooms SET needs_review = TRUE WHERE id = :id"),
                    {"id": row_id},
                )

        assert getattr(exc_info.value.orig, "sqlstate", None) == "55000"
        assert "append-only trigger blocked UPDATE" in str(exc_info.value)

    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))


@requires_database
async def test_append_only_trigger_rejects_delete_on_revision_rooms() -> None:
    """DELETE on revision_rooms must be blocked by the append-only trigger."""

    engine = session_module.get_engine()
    assert engine is not None

    schema_name = f"t_room_del_{uuid.uuid4().hex[:12]}"

    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

        async with engine.begin() as conn:
            ids = await _create_minimal_schema(conn, schema_name)
            row_id = await _insert_room(conn, ids)

        with pytest.raises(sa.exc.DBAPIError) as exc_info:
            async with engine.begin() as conn:
                await conn.execute(text(f'SET search_path TO "{schema_name}"'))
                await conn.execute(
                    text("DELETE FROM revision_rooms WHERE id = :id"),
                    {"id": row_id},
                )

        assert getattr(exc_info.value.orig, "sqlstate", None) == "55000"
        assert "append-only trigger blocked DELETE" in str(exc_info.value)

    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
