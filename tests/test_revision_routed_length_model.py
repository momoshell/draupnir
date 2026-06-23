"""Tests for RevisionRoutedLength model, append-only triggers, and JobType.CENTERLINE."""

from __future__ import annotations

import uuid
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy import text

import app.db.session as session_module
from app.models.job import (
    _BASE_REQUIRED_JOB_TYPE_VALUES,
    _EXTRACTION_PROFILE_FORBIDDEN_JOB_TYPE_VALUES,
    JobType,
)
from tests.conftest import requires_database

# ---------------------------------------------------------------------------
# JobType enum round-trip (pure unit, no DB needed)
# ---------------------------------------------------------------------------


def test_job_type_centerline_value() -> None:
    """JobType.CENTERLINE must serialize to the string 'centerline'."""
    assert JobType.CENTERLINE.value == "centerline"
    assert str(JobType.CENTERLINE) == "centerline"


def test_centerline_in_base_required() -> None:
    """centerline must appear in _BASE_REQUIRED_JOB_TYPE_VALUES."""
    assert "centerline" in _BASE_REQUIRED_JOB_TYPE_VALUES


def test_centerline_in_extraction_profile_forbidden() -> None:
    """centerline must appear in _EXTRACTION_PROFILE_FORBIDDEN_JOB_TYPE_VALUES."""
    assert "centerline" in _EXTRACTION_PROFILE_FORBIDDEN_JOB_TYPE_VALUES


# ---------------------------------------------------------------------------
# DB helpers (conn is AsyncConnection from engine.begin())
# ---------------------------------------------------------------------------


async def _create_minimal_schema(conn: Any, schema_name: str) -> dict[str, uuid.UUID]:
    """Create a minimal schema with the tables needed for routed-length inserts."""

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
            CREATE TABLE revision_routed_lengths (
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
                layer_ref VARCHAR(255),
                colour_key VARCHAR(255),
                algo_version VARCHAR(32) NOT NULL,
                raster_params_hash VARCHAR(64) NOT NULL,
                producer_kind VARCHAR(32) NOT NULL,
                skeleton_length_du DOUBLE PRECISION NOT NULL,
                entity_count INTEGER NOT NULL,
                geometry_json JSON,
                FOREIGN KEY (source_file_id, project_id)
                    REFERENCES files(id, project_id) ON DELETE RESTRICT,
                FOREIGN KEY (drawing_revision_id, project_id)
                    REFERENCES drawing_revisions(id, project_id) ON DELETE RESTRICT,
                UNIQUE (id, project_id),
                UNIQUE (
                    drawing_revision_id, layer_ref, colour_key,
                    algo_version, raster_params_hash
                )
            )
            """
        )
    )

    # Attach append-only triggers (functions already exist in public schema).
    await conn.execute(
        text(
            """
            CREATE TRIGGER trg_append_only_row_guard
            BEFORE UPDATE OR DELETE ON revision_routed_lengths
            FOR EACH ROW
            EXECUTE FUNCTION public.enforce_append_only_lineage_row()
            """
        )
    )
    await conn.execute(
        text(
            """
            CREATE TRIGGER trg_append_only_truncate_guard
            BEFORE TRUNCATE ON revision_routed_lengths
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
            "VALUES (:job, :project, :file, 'centerline')"
        ),
        {"job": ids["job"], "project": ids["project"], "file": ids["file"]},
    )

    return ids


async def _insert_routed_length(
    conn: Any,
    ids: dict[str, uuid.UUID],
    *,
    row_id: uuid.UUID | None = None,
) -> uuid.UUID:
    row_id = row_id or uuid.uuid4()
    await conn.execute(
        text(
            """
            INSERT INTO revision_routed_lengths (
                id, project_id, source_file_id, source_job_id,
                drawing_revision_id, canonical_entity_schema_version,
                layer_ref, colour_key, algo_version, raster_params_hash,
                producer_kind, skeleton_length_du, entity_count
            ) VALUES (
                :id, :project, :file, :job,
                :revision, '1.0',
                'A-PIPE', 'RED', 'v1',
                :raster_hash,
                'raster_centerline', 42.5, 7
            )
            """
        ),
        {
            "id": row_id,
            "project": ids["project"],
            "file": ids["file"],
            "job": ids["job"],
            "revision": ids["revision"],
            "raster_hash": "a" * 64,
        },
    )
    return row_id


# ---------------------------------------------------------------------------
# Append-only trigger tests
# ---------------------------------------------------------------------------


@requires_database
async def test_append_only_trigger_rejects_update_on_revision_routed_lengths() -> None:
    """UPDATE on revision_routed_lengths must be blocked by the append-only trigger."""

    engine = session_module.get_engine()
    assert engine is not None

    schema_name = f"t_rrl_upd_{uuid.uuid4().hex[:12]}"

    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

        async with engine.begin() as conn:
            ids = await _create_minimal_schema(conn, schema_name)
            row_id = await _insert_routed_length(conn, ids)

        with pytest.raises(sa.exc.DBAPIError) as exc_info:
            async with engine.begin() as conn:
                await conn.execute(text(f'SET search_path TO "{schema_name}"'))
                await conn.execute(
                    text(
                        "UPDATE revision_routed_lengths "
                        "SET skeleton_length_du = 99.0 "
                        "WHERE id = :id"
                    ),
                    {"id": row_id},
                )

        assert getattr(exc_info.value.orig, "sqlstate", None) == "55000"
        assert "append-only trigger blocked UPDATE" in str(exc_info.value)

    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))


@requires_database
async def test_append_only_trigger_rejects_delete_on_revision_routed_lengths() -> None:
    """DELETE on revision_routed_lengths must be blocked by the append-only trigger."""

    engine = session_module.get_engine()
    assert engine is not None

    schema_name = f"t_rrl_del_{uuid.uuid4().hex[:12]}"

    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

        async with engine.begin() as conn:
            ids = await _create_minimal_schema(conn, schema_name)
            row_id = await _insert_routed_length(conn, ids)

        with pytest.raises(sa.exc.DBAPIError) as exc_info:
            async with engine.begin() as conn:
                await conn.execute(text(f'SET search_path TO "{schema_name}"'))
                await conn.execute(
                    text("DELETE FROM revision_routed_lengths WHERE id = :id"),
                    {"id": row_id},
                )

        assert getattr(exc_info.value.orig, "sqlstate", None) == "55000"
        assert "append-only trigger blocked DELETE" in str(exc_info.value)

    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))


# ---------------------------------------------------------------------------
# Job check-constraint accepts centerline rows
# ---------------------------------------------------------------------------


@requires_database
async def test_jobs_check_constraint_accepts_centerline_job() -> None:
    """The upgraded jobs check constraints must accept a centerline job row.

    Uses an isolated schema with a minimal jobs table that carries only the
    three constraints under test, avoiding the full FK chain of the live DB.
    """

    engine = session_module.get_engine()
    assert engine is not None

    schema_name = f"t_rrl_ck_{uuid.uuid4().hex[:12]}"

    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

        async with engine.begin() as conn:
            await conn.execute(text(f'SET search_path TO "{schema_name}"'))

            # Minimal jobs table with only the three constraints being tested.
            await conn.execute(
                text(
                    """
                    CREATE TABLE jobs (
                        id UUID PRIMARY KEY,
                        job_type VARCHAR(64) NOT NULL,
                        base_revision_id UUID,
                        extraction_profile_id UUID,
                        CONSTRAINT ck_jobs_job_type_valid
                            CHECK (job_type IN (
                                'ingest', 'reprocess', 'quantity_takeoff',
                                'estimate', 'export', 'changeset_apply', 'centerline'
                            )),
                        CONSTRAINT ck_jobs_revision_scoped_base_revision_required
                            CHECK (
                                job_type NOT IN (
                                    'reprocess', 'quantity_takeoff', 'estimate',
                                    'export', 'changeset_apply', 'centerline'
                                ) OR base_revision_id IS NOT NULL
                            ),
                        CONSTRAINT ck_jobs_revision_scoped_extraction_profile_forbidden
                            CHECK (
                                job_type NOT IN (
                                    'quantity_takeoff', 'estimate', 'export',
                                    'changeset_apply', 'centerline'
                                ) OR extraction_profile_id IS NULL
                            )
                    )
                    """
                )
            )

            revision_id = uuid.uuid4()
            job_id = uuid.uuid4()

            # centerline: base_revision_id required, extraction_profile_id must be NULL.
            await conn.execute(
                text(
                    "INSERT INTO jobs (id, job_type, base_revision_id) "
                    "VALUES (:job_id, 'centerline', :revision_id)"
                ),
                {"job_id": job_id, "revision_id": revision_id},
            )

            result = await conn.execute(
                text("SELECT job_type FROM jobs WHERE id = :job_id"),
                {"job_id": job_id},
            )
            row = result.fetchone()
            assert row is not None
            assert row[0] == "centerline"

            # Verify centerline with missing base_revision_id is rejected.
            with pytest.raises(sa.exc.DBAPIError):
                await conn.execute(
                    text("INSERT INTO jobs (id, job_type) VALUES (:job_id2, 'centerline')"),
                    {"job_id2": uuid.uuid4()},
                )

    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
