"""Guard tests for the revision-scoped jobs contract migration."""

from __future__ import annotations

import importlib.util
import uuid
from pathlib import Path
from typing import Any, TypedDict, cast

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text

import app.db.session as session_module
from tests.conftest import requires_database


def _load_revision_scoped_jobs_migration() -> Any:
    """Load the revision-scoped jobs migration module directly from disk."""

    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "2026_05_14_0016_add_revision_scoped_job_contract.py"
    )
    spec = importlib.util.spec_from_file_location(
        "migration_2026_05_14_0016_add_revision_scoped_job_contract",
        migration_path,
    )
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _install_pre_0016_schema(sync_conn: sa.Connection) -> None:
    """Create the minimal pre-0016 schema needed to exercise the migration."""

    metadata = sa.MetaData()

    projects = sa.Table(
        "projects",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
    )
    sa.Table(
        "files",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "project_id",
            sa.Uuid(),
            sa.ForeignKey(projects.c.id, ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.UniqueConstraint("id", "project_id", name="uq_files_id_project_id"),
    )
    sa.Table(
        "extraction_profiles",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "project_id",
            sa.Uuid(),
            sa.ForeignKey(projects.c.id, ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "id",
            "project_id",
            name="uq_extraction_profiles_id_project_id",
        ),
    )
    sa.Table(
        "drawing_revisions",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "project_id",
            sa.Uuid(),
            sa.ForeignKey(projects.c.id, ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("source_file_id", sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="RESTRICT",
            name="fk_drawing_revisions_source_file_id_project_id_files",
        ),
        sa.UniqueConstraint(
            "id",
            "project_id",
            name="uq_drawing_revisions_id_project_id",
        ),
    )
    sa.Table(
        "jobs",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "project_id",
            sa.Uuid(),
            sa.ForeignKey(projects.c.id, ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("file_id", sa.Uuid(), nullable=False),
        sa.Column("extraction_profile_id", sa.Uuid(), nullable=True),
        sa.Column("base_revision_id", sa.Uuid(), nullable=True),
        sa.Column("job_type", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.ForeignKeyConstraint(
            ["file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="RESTRICT",
            name="fk_jobs_file_id_project_id_files",
        ),
        sa.ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            ondelete="RESTRICT",
            name="fk_jobs_extraction_profile_id_project_id_extraction_profiles",
        ),
        sa.ForeignKeyConstraint(
            ["base_revision_id"],
            ["drawing_revisions.id"],
            ondelete="RESTRICT",
            name="fk_jobs_base_revision_id_drawing_revisions",
        ),
        sa.CheckConstraint(
            "job_type IN ('ingest', 'reprocess')",
            name="ck_jobs_job_type_valid",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'succeeded', 'failed', 'cancelled')",
            name="ck_jobs_status_valid",
        ),
        sa.CheckConstraint(
            "job_type NOT IN ('ingest', 'reprocess') OR extraction_profile_id IS NOT NULL",
            name="ck_jobs_ingest_extraction_profile_required",
        ),
        sa.CheckConstraint(
            "job_type != 'reprocess' OR base_revision_id IS NOT NULL",
            name="ck_jobs_reprocess_base_revision_required",
        ),
        sa.CheckConstraint(
            "job_type != 'ingest' OR base_revision_id IS NULL",
            name="ck_jobs_ingest_base_revision_forbidden",
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


async def _seed_revision_scoped_references(conn: Any) -> dict[str, uuid.UUID]:
    """Insert the shared project/file/profile/revision reference rows."""

    ids = {
        "project_a": uuid.uuid4(),
        "project_b": uuid.uuid4(),
        "file_a1": uuid.uuid4(),
        "file_a2": uuid.uuid4(),
        "file_b1": uuid.uuid4(),
        "profile_a1": uuid.uuid4(),
        "profile_a2": uuid.uuid4(),
        "profile_b1": uuid.uuid4(),
        "revision_a1": uuid.uuid4(),
        "revision_a2": uuid.uuid4(),
        "revision_b1": uuid.uuid4(),
    }

    await conn.execute(
        text("INSERT INTO projects (id) VALUES (:project_a), (:project_b)"),
        {"project_a": ids["project_a"], "project_b": ids["project_b"]},
    )
    await conn.execute(
        text(
            """
            INSERT INTO files (id, project_id)
            VALUES
                (:file_a1, :project_a),
                (:file_a2, :project_a),
                (:file_b1, :project_b)
            """
        ),
        ids,
    )
    await conn.execute(
        text(
            """
            INSERT INTO extraction_profiles (id, project_id)
            VALUES
                (:profile_a1, :project_a),
                (:profile_a2, :project_a),
                (:profile_b1, :project_b)
            """
        ),
        ids,
    )
    await conn.execute(
        text(
            """
            INSERT INTO drawing_revisions (id, project_id, source_file_id)
            VALUES
                (:revision_a1, :project_a, :file_a1),
                (:revision_a2, :project_a, :file_a2),
                (:revision_b1, :project_b, :file_b1)
            """
        ),
        ids,
    )
    return ids


async def _insert_job(
    conn: Any,
    *,
    job_id: uuid.UUID,
    project_id: uuid.UUID,
    file_id: uuid.UUID,
    extraction_profile_id: uuid.UUID | None,
    base_revision_id: uuid.UUID | None,
    parent_job_id: uuid.UUID | None,
    job_type: str,
    status: str = "pending",
) -> None:
    """Insert a job row into the migrated jobs table."""

    await conn.execute(
        text(
            """
            INSERT INTO jobs (
                id,
                project_id,
                file_id,
                extraction_profile_id,
                base_revision_id,
                parent_job_id,
                job_type,
                status
            ) VALUES (
                :job_id,
                :project_id,
                :file_id,
                :extraction_profile_id,
                :base_revision_id,
                :parent_job_id,
                :job_type,
                :status
            )
            """
        ),
        {
            "job_id": job_id,
            "project_id": project_id,
            "file_id": file_id,
            "extraction_profile_id": extraction_profile_id,
            "base_revision_id": base_revision_id,
            "parent_job_id": parent_job_id,
            "job_type": job_type,
            "status": status,
        },
    )


class _JobInsertParams(TypedDict):
    job_id: uuid.UUID
    project_id: uuid.UUID
    file_id: uuid.UUID
    extraction_profile_id: uuid.UUID | None
    base_revision_id: uuid.UUID | None
    parent_job_id: uuid.UUID | None
    job_type: str


@requires_database
@pytest.mark.asyncio
async def test_jobs_revision_scoped_contract_upgrade_enforces_revision_and_parent_guards() -> None:
    """Upgrade should accept valid revision-scoped jobs and reject cross-scope pins."""

    engine = session_module.get_engine()
    assert engine is not None

    schema_name = f"t_jrsc_up_{uuid.uuid4().hex}"
    migration = _load_revision_scoped_jobs_migration()

    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

        async with engine.begin() as conn:
            await conn.execute(text(f'SET search_path TO "{schema_name}"'))
            await conn.run_sync(_install_pre_0016_schema)
            await conn.run_sync(lambda sync_conn: _run_migration_upgrade(sync_conn, migration))

            ids = await _seed_revision_scoped_references(conn)
            parent_reprocess_id = uuid.uuid4()
            quantity_child_id = uuid.uuid4()
            quantity_unparented_id = uuid.uuid4()
            sibling_parent_id = uuid.uuid4()
            foreign_parent_id = uuid.uuid4()

            await _insert_job(
                conn,
                job_id=uuid.uuid4(),
                project_id=ids["project_a"],
                file_id=ids["file_a1"],
                extraction_profile_id=ids["profile_a1"],
                base_revision_id=None,
                parent_job_id=None,
                job_type="ingest",
            )
            await _insert_job(
                conn,
                job_id=parent_reprocess_id,
                project_id=ids["project_a"],
                file_id=ids["file_a1"],
                extraction_profile_id=ids["profile_a1"],
                base_revision_id=ids["revision_a1"],
                parent_job_id=None,
                job_type="reprocess",
            )
            await _insert_job(
                conn,
                job_id=quantity_child_id,
                project_id=ids["project_a"],
                file_id=ids["file_a1"],
                extraction_profile_id=None,
                base_revision_id=ids["revision_a1"],
                parent_job_id=parent_reprocess_id,
                job_type="quantity_takeoff",
            )
            await _insert_job(
                conn,
                job_id=quantity_unparented_id,
                project_id=ids["project_a"],
                file_id=ids["file_a1"],
                extraction_profile_id=None,
                base_revision_id=ids["revision_a1"],
                parent_job_id=None,
                job_type="quantity_takeoff",
            )
            await _insert_job(
                conn,
                job_id=sibling_parent_id,
                project_id=ids["project_a"],
                file_id=ids["file_a2"],
                extraction_profile_id=ids["profile_a2"],
                base_revision_id=None,
                parent_job_id=None,
                job_type="ingest",
            )
            await _insert_job(
                conn,
                job_id=foreign_parent_id,
                project_id=ids["project_b"],
                file_id=ids["file_b1"],
                extraction_profile_id=ids["profile_b1"],
                base_revision_id=None,
                parent_job_id=None,
                job_type="ingest",
            )

            assert await _job_column_exists(conn, schema_name, "parent_job_id")

            rows = (
                (
                    await conn.execute(
                        text(
                            """
                            SELECT
                                id,
                                job_type,
                                extraction_profile_id,
                                base_revision_id,
                                parent_job_id
                            FROM jobs
                            WHERE id IN (
                                :parent_reprocess_id,
                                :quantity_child_id,
                                :quantity_unparented_id
                            )
                            """
                        ),
                        {
                            "parent_reprocess_id": parent_reprocess_id,
                            "quantity_child_id": quantity_child_id,
                            "quantity_unparented_id": quantity_unparented_id,
                        },
                    )
                )
                .mappings()
                .all()
            )
            rows_by_id = {row["id"]: dict(row) for row in rows}
            assert rows_by_id == {
                quantity_unparented_id: {
                    "id": quantity_unparented_id,
                    "job_type": "quantity_takeoff",
                    "extraction_profile_id": None,
                    "base_revision_id": ids["revision_a1"],
                    "parent_job_id": None,
                },
                quantity_child_id: {
                    "id": quantity_child_id,
                    "job_type": "quantity_takeoff",
                    "extraction_profile_id": None,
                    "base_revision_id": ids["revision_a1"],
                    "parent_job_id": parent_reprocess_id,
                },
                parent_reprocess_id: {
                    "id": parent_reprocess_id,
                    "job_type": "reprocess",
                    "extraction_profile_id": ids["profile_a1"],
                    "base_revision_id": ids["revision_a1"],
                    "parent_job_id": None,
                },
            }

        invalid_cases: list[_JobInsertParams] = [
            {
                "job_id": uuid.uuid4(),
                "project_id": ids["project_a"],
                "file_id": ids["file_a1"],
                "extraction_profile_id": None,
                "base_revision_id": None,
                "parent_job_id": None,
                "job_type": "quantity_takeoff",
            },
            {
                "job_id": uuid.uuid4(),
                "project_id": ids["project_a"],
                "file_id": ids["file_a1"],
                "extraction_profile_id": ids["profile_a1"],
                "base_revision_id": ids["revision_a1"],
                "parent_job_id": None,
                "job_type": "quantity_takeoff",
            },
            {
                "job_id": uuid.uuid4(),
                "project_id": ids["project_a"],
                "file_id": ids["file_a1"],
                "extraction_profile_id": ids["profile_a1"],
                "base_revision_id": ids["revision_a1"],
                "parent_job_id": None,
                "job_type": "ingest",
            },
            {
                "job_id": uuid.uuid4(),
                "project_id": ids["project_a"],
                "file_id": ids["file_a1"],
                "extraction_profile_id": None,
                "base_revision_id": ids["revision_a2"],
                "parent_job_id": None,
                "job_type": "quantity_takeoff",
            },
            {
                "job_id": uuid.uuid4(),
                "project_id": ids["project_a"],
                "file_id": ids["file_a1"],
                "extraction_profile_id": None,
                "base_revision_id": ids["revision_b1"],
                "parent_job_id": None,
                "job_type": "quantity_takeoff",
            },
            {
                "job_id": uuid.uuid4(),
                "project_id": ids["project_a"],
                "file_id": ids["file_a1"],
                "extraction_profile_id": ids["profile_a1"],
                "base_revision_id": ids["revision_a2"],
                "parent_job_id": None,
                "job_type": "reprocess",
            },
            {
                "job_id": uuid.uuid4(),
                "project_id": ids["project_a"],
                "file_id": ids["file_a1"],
                "extraction_profile_id": ids["profile_a1"],
                "base_revision_id": ids["revision_b1"],
                "parent_job_id": None,
                "job_type": "reprocess",
            },
            {
                "job_id": uuid.uuid4(),
                "project_id": ids["project_a"],
                "file_id": ids["file_a1"],
                "extraction_profile_id": ids["profile_a1"],
                "base_revision_id": None,
                "parent_job_id": sibling_parent_id,
                "job_type": "ingest",
            },
            {
                "job_id": uuid.uuid4(),
                "project_id": ids["project_a"],
                "file_id": ids["file_a1"],
                "extraction_profile_id": ids["profile_a1"],
                "base_revision_id": None,
                "parent_job_id": foreign_parent_id,
                "job_type": "ingest",
            },
        ]

        self_parent_id = uuid.uuid4()
        invalid_cases.append(
            {
                "job_id": self_parent_id,
                "project_id": ids["project_a"],
                "file_id": ids["file_a1"],
                "extraction_profile_id": ids["profile_a1"],
                "base_revision_id": None,
                "parent_job_id": self_parent_id,
                "job_type": "ingest",
            }
        )

        for params in invalid_cases:
            with pytest.raises(sa.exc.IntegrityError):
                async with engine.begin() as conn:
                    await conn.execute(text(f'SET search_path TO "{schema_name}"'))
                    await _insert_job(conn, **params)
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))


@requires_database
@pytest.mark.asyncio
async def test_jobs_revision_scoped_contract_downgrade_rejects_quantity_takeoff_rows() -> None:
    """Downgrade should refuse to remove quantity-takeoff rows."""

    engine = session_module.get_engine()
    assert engine is not None

    schema_name = f"t_jrsc_down_qty_{uuid.uuid4().hex}"
    migration = _load_revision_scoped_jobs_migration()

    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

        async with engine.begin() as conn:
            await conn.execute(text(f'SET search_path TO "{schema_name}"'))
            await conn.run_sync(_install_pre_0016_schema)
            await conn.run_sync(lambda sync_conn: _run_migration_upgrade(sync_conn, migration))

            ids = await _seed_revision_scoped_references(conn)
            await _insert_job(
                conn,
                job_id=uuid.uuid4(),
                project_id=ids["project_a"],
                file_id=ids["file_a1"],
                extraction_profile_id=None,
                base_revision_id=ids["revision_a1"],
                parent_job_id=None,
                job_type="quantity_takeoff",
            )

        with pytest.raises(RuntimeError, match="quantity_takeoff jobs"):
            async with engine.begin() as conn:
                await conn.execute(text(f'SET search_path TO "{schema_name}"'))
                await conn.run_sync(
                    lambda sync_conn: _run_migration_downgrade(sync_conn, migration)
                )

        async with engine.begin() as conn:
            assert await _job_column_exists(conn, schema_name, "parent_job_id")
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))


@requires_database
@pytest.mark.asyncio
async def test_jobs_revision_scoped_contract_downgrade_rejects_parented_rows() -> None:
    """Downgrade should refuse to remove parent lineage rows."""

    engine = session_module.get_engine()
    assert engine is not None

    schema_name = f"t_jrsc_down_parent_{uuid.uuid4().hex}"
    migration = _load_revision_scoped_jobs_migration()

    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

        async with engine.begin() as conn:
            await conn.execute(text(f'SET search_path TO "{schema_name}"'))
            await conn.run_sync(_install_pre_0016_schema)
            await conn.run_sync(lambda sync_conn: _run_migration_upgrade(sync_conn, migration))

            ids = await _seed_revision_scoped_references(conn)
            parent_job_id = uuid.uuid4()
            await _insert_job(
                conn,
                job_id=parent_job_id,
                project_id=ids["project_a"],
                file_id=ids["file_a1"],
                extraction_profile_id=ids["profile_a1"],
                base_revision_id=None,
                parent_job_id=None,
                job_type="ingest",
            )
            await _insert_job(
                conn,
                job_id=uuid.uuid4(),
                project_id=ids["project_a"],
                file_id=ids["file_a1"],
                extraction_profile_id=ids["profile_a1"],
                base_revision_id=ids["revision_a1"],
                parent_job_id=parent_job_id,
                job_type="reprocess",
            )

        with pytest.raises(RuntimeError, match="parent_job_id lineage rows"):
            async with engine.begin() as conn:
                await conn.execute(text(f'SET search_path TO "{schema_name}"'))
                await conn.run_sync(
                    lambda sync_conn: _run_migration_downgrade(sync_conn, migration)
                )

        async with engine.begin() as conn:
            assert await _job_column_exists(conn, schema_name, "parent_job_id")
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))


@requires_database
@pytest.mark.asyncio
async def test_jobs_revision_scoped_contract_downgrade_succeeds_for_legacy_rows() -> None:
    """Downgrade should succeed once only legacy ingest/reprocess rows remain."""

    engine = session_module.get_engine()
    assert engine is not None

    schema_name = f"t_jrsc_down_ok_{uuid.uuid4().hex}"
    migration = _load_revision_scoped_jobs_migration()

    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

        async with engine.begin() as conn:
            await conn.execute(text(f'SET search_path TO "{schema_name}"'))
            await conn.run_sync(_install_pre_0016_schema)
            await conn.run_sync(lambda sync_conn: _run_migration_upgrade(sync_conn, migration))

            ids = await _seed_revision_scoped_references(conn)
            await _insert_job(
                conn,
                job_id=uuid.uuid4(),
                project_id=ids["project_a"],
                file_id=ids["file_a1"],
                extraction_profile_id=ids["profile_a1"],
                base_revision_id=None,
                parent_job_id=None,
                job_type="ingest",
            )
            await _insert_job(
                conn,
                job_id=uuid.uuid4(),
                project_id=ids["project_a"],
                file_id=ids["file_a1"],
                extraction_profile_id=ids["profile_a1"],
                base_revision_id=ids["revision_a1"],
                parent_job_id=None,
                job_type="reprocess",
                status="succeeded",
            )

            await conn.run_sync(lambda sync_conn: _run_migration_downgrade(sync_conn, migration))

            assert not await _job_column_exists(conn, schema_name, "parent_job_id")
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
