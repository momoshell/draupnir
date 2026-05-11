"""Guard tests for the jobs base-revision pin migration."""

from __future__ import annotations

import importlib.util
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text

import app.db.session as session_module
from tests.conftest import requires_database


def _load_jobs_base_revision_migration() -> Any:
    """Load the jobs base-revision migration module directly from disk."""

    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "2026_05_11_0012_add_jobs_base_revision_id.py"
    )
    spec = importlib.util.spec_from_file_location(
        "migration_2026_05_11_0012_add_jobs_base_revision_id",
        migration_path,
    )
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _install_pre_0012_schema(sync_conn: sa.Connection) -> None:
    """Create the minimal pre-0012 schema needed to exercise the migration."""

    metadata = sa.MetaData()
    sa.Table(
        "files",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("initial_job_id", sa.Uuid(), nullable=False),
    )
    sa.Table(
        "drawing_revisions",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("file_id", sa.Uuid(), nullable=False),
    )
    sa.Table(
        "jobs",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("file_id", sa.Uuid(), nullable=False),
        sa.Column("job_type", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    metadata.create_all(sync_conn)


def _run_migration_upgrade(sync_conn: sa.Connection, migration: Any) -> None:
    """Run the migration upgrade against a live connection."""

    migration_context = MigrationContext.configure(sync_conn)
    migration.op = Operations(migration_context)
    migration.upgrade()


def _run_migration_downgrade(sync_conn: sa.Connection, migration: Any) -> None:
    """Run the migration downgrade against a live connection."""

    migration_context = MigrationContext.configure(sync_conn)
    migration.op = Operations(migration_context)
    migration.downgrade()


async def _job_column_exists(conn: Any, schema_name: str, column_name: str) -> bool:
    """Return whether the jobs table contains the requested column."""

    result = await conn.execute(
        text(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = :schema_name
                  AND table_name = 'jobs'
                  AND column_name = :column_name
            )
            """
        ),
        {"schema_name": schema_name, "column_name": column_name},
    )
    return cast(bool, result.scalar())


async def _get_job_base_revision_constraints(conn: Any, schema_name: str) -> dict[str, bool]:
    """Return base-revision check constraints and their validation state."""

    rows = (
        (
            await conn.execute(
                text(
                    """
                    SELECT con.conname, con.convalidated
                    FROM pg_constraint AS con
                    JOIN pg_class AS rel
                      ON rel.oid = con.conrelid
                    JOIN pg_namespace AS nsp
                      ON nsp.oid = rel.relnamespace
                    WHERE nsp.nspname = :schema_name
                      AND rel.relname = 'jobs'
                      AND con.conname IN (
                          'ck_jobs_ingest_base_revision_forbidden',
                          'ck_jobs_reprocess_base_revision_required'
                      )
                    ORDER BY con.conname ASC
                    """
                ),
                {"schema_name": schema_name},
            )
        )
        .mappings()
        .all()
    )
    return {
        cast(str, row["conname"]): cast(bool, row["convalidated"])
        for row in rows
    }


@requires_database
@pytest.mark.asyncio
async def test_jobs_base_revision_migration_upgrade_allows_terminal_legacy_rows() -> None:
    """Upgrade should preserve historical legacy rows while adding the new base-pin guards."""

    engine = session_module.get_engine()
    assert engine is not None

    schema_name = f"t_jbr_up_{uuid.uuid4().hex}"
    migration = _load_jobs_base_revision_migration()
    file_id = uuid.uuid4()
    initial_job_id = uuid.uuid4()
    legacy_terminal_job_id = uuid.uuid4()

    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

        async with engine.begin() as conn:
            await conn.execute(text(f'SET search_path TO "{schema_name}"'))
            await conn.run_sync(_install_pre_0012_schema)

            await conn.execute(
                text(
                    """
                    INSERT INTO files (id, initial_job_id)
                    VALUES (:id, :initial_job_id)
                    """
                ),
                {"id": file_id, "initial_job_id": initial_job_id},
            )
            await conn.execute(
                text(
                    """
                    INSERT INTO jobs (id, file_id, job_type, status, created_at)
                    VALUES (:id, :file_id, :job_type, :status, :created_at)
                    """
                ),
                [
                    {
                        "id": initial_job_id,
                        "file_id": file_id,
                        "job_type": "ingest",
                        "status": "pending",
                        "created_at": datetime(2026, 5, 11, tzinfo=UTC),
                    },
                    {
                        "id": legacy_terminal_job_id,
                        "file_id": file_id,
                        "job_type": "ingest",
                        "status": "succeeded",
                        "created_at": datetime(2026, 5, 12, tzinfo=UTC),
                    },
                ],
            )

            await conn.run_sync(lambda sync_conn: _run_migration_upgrade(sync_conn, migration))

            assert await _job_column_exists(conn, schema_name, "base_revision_id")
            assert await _get_job_base_revision_constraints(conn, schema_name) == {
                "ck_jobs_ingest_base_revision_forbidden": False,
                "ck_jobs_reprocess_base_revision_required": False,
            }

            jobs = (
                (
                    await conn.execute(
                        text(
                            """
                            SELECT id, job_type, status, base_revision_id
                            FROM jobs
                            ORDER BY created_at ASC, id ASC
                            """
                        )
                    )
                )
                .mappings()
                .all()
            )
            assert [dict(row) for row in jobs] == [
                {
                    "id": initial_job_id,
                    "job_type": "ingest",
                    "status": "pending",
                    "base_revision_id": None,
                },
                {
                    "id": legacy_terminal_job_id,
                    "job_type": "ingest",
                    "status": "succeeded",
                    "base_revision_id": None,
                },
            ]
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))


@requires_database
@pytest.mark.asyncio
async def test_jobs_base_revision_migration_upgrade_raises_for_pending_legacy_reprocess_rows(
) -> None:
    """Upgrade should refuse legacy queued reprocess rows that cannot be pinned safely."""

    engine = session_module.get_engine()
    assert engine is not None

    schema_name = f"t_jbr_guard_{uuid.uuid4().hex}"
    migration = _load_jobs_base_revision_migration()
    file_id = uuid.uuid4()
    initial_job_id = uuid.uuid4()
    legacy_pending_job_id = uuid.uuid4()

    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

        async with engine.begin() as conn:
            await conn.execute(text(f'SET search_path TO "{schema_name}"'))
            await conn.run_sync(_install_pre_0012_schema)

            await conn.execute(
                text(
                    """
                    INSERT INTO files (id, initial_job_id)
                    VALUES (:id, :initial_job_id)
                    """
                ),
                {"id": file_id, "initial_job_id": initial_job_id},
            )
            await conn.execute(
                text(
                    """
                    INSERT INTO jobs (id, file_id, job_type, status, created_at)
                    VALUES (:id, :file_id, :job_type, :status, :created_at)
                    """
                ),
                [
                    {
                        "id": initial_job_id,
                        "file_id": file_id,
                        "job_type": "ingest",
                        "status": "pending",
                        "created_at": datetime(2026, 5, 11, tzinfo=UTC),
                    },
                    {
                        "id": legacy_pending_job_id,
                        "file_id": file_id,
                        "job_type": "ingest",
                        "status": "running",
                        "created_at": datetime(2026, 5, 12, tzinfo=UTC),
                    },
                ],
            )

        with pytest.raises(
            RuntimeError,
            match="legacy pre-#133 reprocess jobs",
        ):
            async with engine.begin() as conn:
                await conn.execute(text(f'SET search_path TO "{schema_name}"'))
                await conn.run_sync(lambda sync_conn: _run_migration_upgrade(sync_conn, migration))

        async with engine.begin() as conn:
            assert not await _job_column_exists(conn, schema_name, "base_revision_id")
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))


@requires_database
@pytest.mark.asyncio
async def test_jobs_base_revision_migration_downgrade_raises_for_pending_reprocess_jobs() -> None:
    """Downgrade should refuse to drop stale-base fencing while reprocess work is active."""

    engine = session_module.get_engine()
    assert engine is not None

    schema_name = f"t_jbr_down_guard_{uuid.uuid4().hex}"
    migration = _load_jobs_base_revision_migration()
    file_id = uuid.uuid4()
    initial_job_id = uuid.uuid4()
    base_revision_id = uuid.uuid4()
    reprocess_job_id = uuid.uuid4()

    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

        async with engine.begin() as conn:
            await conn.execute(text(f'SET search_path TO "{schema_name}"'))
            await conn.run_sync(_install_pre_0012_schema)

            await conn.execute(
                text(
                    """
                    INSERT INTO files (id, initial_job_id)
                    VALUES (:id, :initial_job_id)
                    """
                ),
                {"id": file_id, "initial_job_id": initial_job_id},
            )
            await conn.execute(
                text(
                    """
                    INSERT INTO drawing_revisions (id, file_id)
                    VALUES (:id, :file_id)
                    """
                ),
                {"id": base_revision_id, "file_id": file_id},
            )
            await conn.execute(
                text(
                    """
                    INSERT INTO jobs (id, file_id, job_type, status, created_at)
                    VALUES (:id, :file_id, :job_type, :status, :created_at)
                    """
                ),
                {
                    "id": initial_job_id,
                    "file_id": file_id,
                    "job_type": "ingest",
                    "status": "succeeded",
                    "created_at": datetime(2026, 5, 11, tzinfo=UTC),
                },
            )

            await conn.run_sync(lambda sync_conn: _run_migration_upgrade(sync_conn, migration))
            await conn.execute(
                text(
                    """
                    INSERT INTO jobs (
                        id,
                        file_id,
                        job_type,
                        status,
                        created_at,
                        base_revision_id
                    ) VALUES (
                        :id,
                        :file_id,
                        'reprocess',
                        :status,
                        :created_at,
                        :base_revision_id
                    )
                    """
                ),
                {
                    "id": reprocess_job_id,
                    "file_id": file_id,
                    "status": "pending",
                    "created_at": datetime(2026, 5, 12, tzinfo=UTC),
                    "base_revision_id": base_revision_id,
                },
            )

        with pytest.raises(
            RuntimeError,
            match="Manual data-preserving rollback is required",
        ):
            async with engine.begin() as conn:
                await conn.execute(text(f'SET search_path TO "{schema_name}"'))
                await conn.run_sync(
                    lambda sync_conn: _run_migration_downgrade(sync_conn, migration)
                )

        async with engine.begin() as conn:
            assert await _job_column_exists(conn, schema_name, "base_revision_id")
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))


@requires_database
@pytest.mark.asyncio
async def test_jobs_base_revision_migration_downgrade_drops_base_pin_without_active_reprocess_jobs(
) -> None:
    """Downgrade should remain rollback-safe after active reprocess work has drained."""

    engine = session_module.get_engine()
    assert engine is not None

    schema_name = f"t_jbr_down_{uuid.uuid4().hex}"
    migration = _load_jobs_base_revision_migration()
    file_id = uuid.uuid4()
    initial_job_id = uuid.uuid4()
    base_revision_id = uuid.uuid4()
    reprocess_job_id = uuid.uuid4()

    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

        async with engine.begin() as conn:
            await conn.execute(text(f'SET search_path TO "{schema_name}"'))
            await conn.run_sync(_install_pre_0012_schema)

            await conn.execute(
                text(
                    """
                    INSERT INTO files (id, initial_job_id)
                    VALUES (:id, :initial_job_id)
                    """
                ),
                {"id": file_id, "initial_job_id": initial_job_id},
            )
            await conn.execute(
                text(
                    """
                    INSERT INTO drawing_revisions (id, file_id)
                    VALUES (:id, :file_id)
                    """
                ),
                {"id": base_revision_id, "file_id": file_id},
            )
            await conn.execute(
                text(
                    """
                    INSERT INTO jobs (id, file_id, job_type, status, created_at)
                    VALUES (:id, :file_id, :job_type, :status, :created_at)
                    """
                ),
                {
                    "id": initial_job_id,
                    "file_id": file_id,
                    "job_type": "ingest",
                    "status": "succeeded",
                    "created_at": datetime(2026, 5, 11, tzinfo=UTC),
                },
            )

            await conn.run_sync(lambda sync_conn: _run_migration_upgrade(sync_conn, migration))
            await conn.execute(
                text(
                    """
                    INSERT INTO jobs (
                        id,
                        file_id,
                        job_type,
                        status,
                        created_at,
                        base_revision_id
                    ) VALUES (
                        :id,
                        :file_id,
                        'reprocess',
                        :status,
                        :created_at,
                        :base_revision_id
                    )
                    """
                ),
                {
                    "id": reprocess_job_id,
                    "file_id": file_id,
                    "status": "succeeded",
                    "created_at": datetime(2026, 5, 12, tzinfo=UTC),
                    "base_revision_id": base_revision_id,
                },
            )

            await conn.run_sync(lambda sync_conn: _run_migration_downgrade(sync_conn, migration))

            assert not await _job_column_exists(conn, schema_name, "base_revision_id")
            assert await _get_job_base_revision_constraints(conn, schema_name) == {}
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
