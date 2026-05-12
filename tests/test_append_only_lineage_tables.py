"""Integration tests for append-only lineage/history table enforcement."""

from __future__ import annotations

import os
import subprocess
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from json import dumps
from pathlib import Path
from typing import Any

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

import app.db.session as session_module
import app.jobs.worker as worker_module
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunRequest
from app.jobs.worker import process_ingest_job
from tests.conftest import (
    APPEND_ONLY_PROTECTED_TABLES,
    APPEND_ONLY_ROW_TRIGGER_NAME,
    APPEND_ONLY_TRUNCATE_TRIGGER_NAME,
    requires_database,
    truncate_projects_cascade_for_cleanup,
)
from tests.test_ingest_output_persistence import (
    _as_uuid,
    _load_job_events,
    _load_project_outputs,
)
from tests.test_jobs import (
    _build_fake_ingest_payload,
    _create_project,
    _get_job_for_file,
    _upload_file,
)

_APPEND_ONLY_SQLSTATE = "55000"
_DOWNGRADE_TARGET_REVISION = "2026_05_11_0013"
_REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class _ProtectedRowIds:
    project_id: uuid.UUID
    file_id: uuid.UUID
    extraction_profile_id: uuid.UUID
    job_id: uuid.UUID
    adapter_run_output_id: uuid.UUID
    drawing_revision_id: uuid.UUID
    validation_report_id: uuid.UUID
    generated_artifact_id: uuid.UUID
    job_event_id: uuid.UUID


@pytest.fixture(autouse=True)
def fake_ingestion_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> list[IngestionRunRequest]:
    """Patch worker ingestion with deterministic persisted outputs."""

    recorded_requests: list[IngestionRunRequest] = []

    async def _fake_run_ingestion(request: IngestionRunRequest) -> IngestFinalizationPayload:
        recorded_requests.append(request)
        return _build_fake_ingest_payload(request)

    monkeypatch.setattr(worker_module, "run_ingestion", _fake_run_ingestion)
    return recorded_requests


def _database_url() -> URL:
    """Return the configured database URL for local Alembic validation."""

    raw_database_url = os.environ.get("DATABASE_URL")
    if raw_database_url is None:
        raise RuntimeError("DATABASE_URL not set")

    return make_url(raw_database_url)


def _url_string(url: URL) -> str:
    """Render a SQLAlchemy URL with the password preserved."""

    return url.render_as_string(hide_password=False)


def _extract_sqlstate(error: BaseException) -> str | None:
    """Best-effort extraction of a PostgreSQL SQLSTATE from wrapped DB errors."""

    candidates = [
        error,
        getattr(error, "orig", None),
        getattr(getattr(error, "orig", None), "__cause__", None),
        getattr(error, "__cause__", None),
    ]
    for candidate in candidates:
        if candidate is None:
            continue

        for attribute_name in ("sqlstate", "pgcode"):
            value = getattr(candidate, attribute_name, None)
            if isinstance(value, str):
                return value

    return None


def _assert_append_only_error(
    error: DBAPIError,
    *,
    operation: str,
    table_name: str | None,
) -> None:
    """Assert a database error came from the append-only trigger guard."""

    assert _extract_sqlstate(error) == _APPEND_ONLY_SQLSTATE
    assert f"append-only trigger blocked {operation}" in str(error)
    if table_name is not None:
        assert f"on {table_name}" in str(error)


def _row_id_for_table(row_ids: _ProtectedRowIds, table_name: str) -> uuid.UUID:
    """Map protected table names to seeded row identifiers."""

    ids_by_table = {
        "files": row_ids.file_id,
        "extraction_profiles": row_ids.extraction_profile_id,
        "adapter_run_outputs": row_ids.adapter_run_output_id,
        "drawing_revisions": row_ids.drawing_revision_id,
        "validation_reports": row_ids.validation_report_id,
        "generated_artifacts": row_ids.generated_artifact_id,
        "job_events": row_ids.job_event_id,
    }
    return ids_by_table[table_name]


async def _seed_protected_rows(async_client: httpx.AsyncClient) -> _ProtectedRowIds:
    """Create one persisted row in each protected lineage/history table."""

    project = await _create_project(async_client)
    uploaded = await _upload_file(async_client, project["id"])
    job = await _get_job_for_file(str(uploaded["id"]))

    await process_ingest_job(job.id)

    adapter_outputs, drawing_revisions, validation_reports, generated_artifacts = (
        await _load_project_outputs(project["id"])
    )
    job_events = await _load_job_events(job.id)

    assert len(adapter_outputs) == 1
    assert len(drawing_revisions) == 1
    assert len(validation_reports) == 1
    assert len(generated_artifacts) == 1
    assert job.extraction_profile_id is not None
    assert job_events

    return _ProtectedRowIds(
        project_id=_as_uuid(project["id"]),
        file_id=_as_uuid(uploaded["id"]),
        extraction_profile_id=job.extraction_profile_id,
        job_id=job.id,
        adapter_run_output_id=adapter_outputs[0].id,
        drawing_revision_id=drawing_revisions[0].id,
        validation_report_id=validation_reports[0].id,
        generated_artifact_id=generated_artifacts[0].id,
        job_event_id=job_events[0].id,
    )


async def _run_sql_and_expect_append_only_failure(
    statement: str,
    parameters: Mapping[str, Any] | None,
    *,
    operation: str,
    table_name: str | None,
) -> None:
    """Execute a SQL mutation and assert the append-only trigger rejects it."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        with pytest.raises(DBAPIError) as exc_info:
            execution_parameters = {} if parameters is None else dict(parameters)
            await session.execute(sa.text(statement), execution_parameters)
            await session.commit()

        await session.rollback()

    _assert_append_only_error(exc_info.value, operation=operation, table_name=table_name)


async def _load_append_only_trigger_states() -> dict[tuple[str, str], str | None]:
    """Load installed append-only trigger enabled states from PostgreSQL."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    trigger_states: dict[tuple[str, str], str | None] = {}
    async with session_maker() as session:
        for table_name in APPEND_ONLY_PROTECTED_TABLES:
            for trigger_name in (APPEND_ONLY_ROW_TRIGGER_NAME, APPEND_ONLY_TRUNCATE_TRIGGER_NAME):
                result = await session.execute(
                    sa.text(
                        """
                        SELECT tgenabled
                        FROM pg_trigger
                        WHERE tgrelid = to_regclass(:table_name)
                          AND tgname = :trigger_name
                        """
                    ),
                    {"table_name": table_name, "trigger_name": trigger_name},
                )
                raw_state = result.scalar_one_or_none()
                if isinstance(raw_state, bytes):
                    trigger_states[(table_name, trigger_name)] = raw_state.decode()
                else:
                    trigger_states[(table_name, trigger_name)] = raw_state

    return trigger_states


def _run_alembic_command(*args: str, database_url: str) -> subprocess.CompletedProcess[str]:
    """Run Alembic against an explicitly selected database URL."""

    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    return subprocess.run(
        ["uv", "run", "alembic", *args],
        cwd=_REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


async def _create_temp_database() -> tuple[str, str]:
    """Create an isolated PostgreSQL database for downgrade validation."""

    base_url = _database_url()
    admin_url = base_url.set(drivername="postgresql", database="postgres")
    database_name = f"draupnir_append_only_{uuid.uuid4().hex}"

    import asyncpg  # type: ignore[import-untyped]

    connection = await asyncpg.connect(_url_string(admin_url))
    try:
        await connection.execute(f'CREATE DATABASE "{database_name}"')
    finally:
        await connection.close()

    return database_name, _url_string(base_url.set(database=database_name))


async def _drop_temp_database(database_name: str) -> None:
    """Drop an isolated PostgreSQL database created for a downgrade test."""

    admin_url = _database_url().set(drivername="postgresql", database="postgres")

    import asyncpg

    connection = await asyncpg.connect(_url_string(admin_url))
    try:
        await connection.execute(
            (
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = $1 AND pid <> pg_backend_pid()"
            ),
            database_name,
        )
        await connection.execute(f'DROP DATABASE IF EXISTS "{database_name}"')
    finally:
        await connection.close()


async def _insert_protected_file_row(database_url: str) -> None:
    """Insert one protected file row for downgrade guard validation."""

    engine = create_async_engine(database_url)
    project_id = uuid.uuid4()
    file_id = uuid.uuid4()

    try:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO projects (
                        id,
                        name,
                        description,
                        default_unit_system,
                        default_currency
                    )
                    VALUES (:id, :name, :description, :default_unit_system, :default_currency)
                    """
                ),
                {
                    "id": project_id,
                    "name": "append-only downgrade guard",
                    "description": None,
                    "default_unit_system": None,
                    "default_currency": None,
                },
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO files (
                        id,
                        project_id,
                        original_filename,
                        media_type,
                        detected_format,
                        storage_uri,
                        size_bytes,
                        checksum_sha256
                    ) VALUES (
                        :id,
                        :project_id,
                        :original_filename,
                        :media_type,
                        :detected_format,
                        :storage_uri,
                        :size_bytes,
                        :checksum_sha256
                    )
                    """
                ),
                {
                    "id": file_id,
                    "project_id": project_id,
                    "original_filename": "plan.pdf",
                    "media_type": "application/pdf",
                    "detected_format": "pdf",
                    "storage_uri": "file:///tmp/plan.pdf",
                    "size_bytes": 1,
                    "checksum_sha256": "a" * 64,
                },
            )
    finally:
        await engine.dispose()


@requires_database
class TestAppendOnlyLineageTables:
    """Tests for DB-enforced append-only lineage/history protections."""

    async def test_append_only_triggers_are_installed_and_enabled(self) -> None:
        """Every protected table should have both append-only triggers enabled."""

        _ = self
        trigger_states = await _load_append_only_trigger_states()

        for table_name in APPEND_ONLY_PROTECTED_TABLES:
            assert trigger_states[(table_name, APPEND_ONLY_ROW_TRIGGER_NAME)] == "O"
            assert trigger_states[(table_name, APPEND_ONLY_TRUNCATE_TRIGGER_NAME)] == "O"

    async def test_cleanup_helper_reenables_append_only_triggers(
        self,
        async_client: httpx.AsyncClient,
        enqueued_job_ids: list[str],
    ) -> None:
        """Cleanup helper should restore trigger state after truncating seeded lineage rows."""

        _ = self
        _ = enqueued_job_ids

        _ = await _seed_protected_rows(async_client)
        await truncate_projects_cascade_for_cleanup()

        trigger_states = await _load_append_only_trigger_states()
        for table_name in APPEND_ONLY_PROTECTED_TABLES:
            assert trigger_states[(table_name, APPEND_ONLY_ROW_TRIGGER_NAME)] == "O"
            assert trigger_states[(table_name, APPEND_ONLY_TRUNCATE_TRIGGER_NAME)] == "O"

    async def test_non_allowlisted_updates_are_rejected(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Protected lineage/history rows should reject non-allowlisted updates."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        row_ids = await _seed_protected_rows(async_client)
        update_cases: tuple[tuple[str, str, str], ...] = (
            ("files", "original_filename", "mutated-plan.pdf"),
            ("extraction_profiles", "layout_mode", "manual"),
            ("adapter_run_outputs", "adapter_version", "mutated"),
            ("drawing_revisions", "review_state", "superseded"),
            ("validation_reports", "validator_version", "mutated"),
            ("generated_artifacts", "name", "mutated-debug-overlay.svg"),
            ("job_events", "message", "mutated job event"),
        )

        for table_name, column_name, replacement_value in update_cases:
            await _run_sql_and_expect_append_only_failure(
                (
                    f'UPDATE "{table_name}" SET "{column_name}" = :replacement_value '
                    "WHERE id = :row_id"
                ),
                {
                    "replacement_value": replacement_value,
                    "row_id": _row_id_for_table(row_ids, table_name),
                },
                operation="UPDATE",
                table_name=table_name,
            )

    async def test_semantically_equivalent_json_rewrites_are_rejected(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Protected JSON payload columns should reject text-only equivalent rewrites."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        row_ids = await _seed_protected_rows(async_client)
        equivalent_canonical_json = dumps(
            {
                "schema_version": "0.1",
                "canonical_entity_schema_version": "0.1",
                "layers": [{"name": "A-WALL"}],
                "layouts": [{"name": "Model"}],
                "blocks": [],
                "entities": [{"layer": "A-WALL", "kind": "line"}],
                "entity_counts": {
                    "entities": 1,
                    "blocks": 0,
                    "layers": 1,
                    "layouts": 1,
                },
            },
            separators=(",", ":"),
        )

        await _run_sql_and_expect_append_only_failure(
            (
                'UPDATE "adapter_run_outputs" '
                'SET canonical_json = CAST(:replacement_value AS json) '
                'WHERE id = :row_id'
            ),
            {
                "replacement_value": equivalent_canonical_json,
                "row_id": row_ids.adapter_run_output_id,
            },
            operation="UPDATE",
            table_name="adapter_run_outputs",
        )

    async def test_soft_delete_markers_are_write_once(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """File and artifact soft-delete markers may only transition from NULL once."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        row_ids = await _seed_protected_rows(async_client)

        delete_response = await async_client.delete(f"/v1/projects/{row_ids.project_id}")
        assert delete_response.status_code == 204

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            deleted_file_at = await session.scalar(
                sa.text('SELECT deleted_at FROM "files" WHERE id = :row_id'),
                {"row_id": row_ids.file_id},
            )
            deleted_artifact_at = await session.scalar(
                sa.text('SELECT deleted_at FROM "generated_artifacts" WHERE id = :row_id'),
                {"row_id": row_ids.generated_artifact_id},
            )

        assert deleted_file_at is not None
        assert deleted_artifact_at is not None

        for table_name, row_id in (
            ("files", row_ids.file_id),
            ("generated_artifacts", row_ids.generated_artifact_id),
        ):
            await _run_sql_and_expect_append_only_failure(
                f'UPDATE "{table_name}" SET deleted_at = NULL WHERE id = :row_id',
                {"row_id": row_id},
                operation="UPDATE",
                table_name=table_name,
            )
            await _run_sql_and_expect_append_only_failure(
                (
                    f'UPDATE "{table_name}" '
                    "SET deleted_at = now() + interval '1 second' WHERE id = :row_id"
                ),
                {"row_id": row_id},
                operation="UPDATE",
                table_name=table_name,
            )

    async def test_protected_rows_reject_delete(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """DELETE should fail with append-only trigger errors, not FK false positives."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        row_ids = await _seed_protected_rows(async_client)

        for table_name in APPEND_ONLY_PROTECTED_TABLES:
            await _run_sql_and_expect_append_only_failure(
                f'DELETE FROM "{table_name}" WHERE id = :row_id',
                {"row_id": _row_id_for_table(row_ids, table_name)},
                operation="DELETE",
                table_name=table_name,
            )

    async def test_protected_tables_reject_truncate(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Every protected table should reject direct TRUNCATE attempts."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        _ = await _seed_protected_rows(async_client)

        for table_name in APPEND_ONLY_PROTECTED_TABLES:
            await _run_sql_and_expect_append_only_failure(
                f'TRUNCATE TABLE "{table_name}" CASCADE',
                None,
                operation="TRUNCATE",
                table_name=table_name,
            )

    async def test_cascaded_project_truncate_is_blocked(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """TRUNCATE projects CASCADE should still be blocked by protected child tables."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        _ = await _seed_protected_rows(async_client)

        await _run_sql_and_expect_append_only_failure(
            'TRUNCATE TABLE "projects" CASCADE',
            None,
            operation="TRUNCATE",
            table_name=None,
        )

    async def test_downgrade_fails_closed_on_populated_protected_tables(self) -> None:
        """Downgrade should refuse to remove append-only guards while protected rows exist."""

        _ = self
        database_name, database_url = await _create_temp_database()

        try:
            upgrade_result = _run_alembic_command("upgrade", "head", database_url=database_url)
            assert upgrade_result.returncode == 0, upgrade_result.stdout + upgrade_result.stderr

            await _insert_protected_file_row(database_url)

            downgrade_result = _run_alembic_command(
                "downgrade",
                _DOWNGRADE_TARGET_REVISION,
                database_url=database_url,
            )
            assert downgrade_result.returncode != 0

            combined_output = downgrade_result.stdout + downgrade_result.stderr
            assert "Refusing to downgrade migration 2026_05_12_0014" in combined_output
            assert "files" in combined_output
        finally:
            await _drop_temp_database(database_name)

    async def test_downgrade_succeeds_on_empty_database(self) -> None:
        """Downgrade should succeed when every protected table is empty."""

        _ = self
        database_name, database_url = await _create_temp_database()

        try:
            upgrade_result = _run_alembic_command("upgrade", "head", database_url=database_url)
            assert upgrade_result.returncode == 0, upgrade_result.stdout + upgrade_result.stderr

            downgrade_result = _run_alembic_command(
                "downgrade",
                _DOWNGRADE_TARGET_REVISION,
                database_url=database_url,
            )
            assert (
                downgrade_result.returncode == 0
            ), downgrade_result.stdout + downgrade_result.stderr
        finally:
            await _drop_temp_database(database_name)
