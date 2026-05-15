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
from tests.conftest import (
    APPEND_ONLY_PROTECTED_TABLES,
    APPEND_ONLY_ROW_TRIGGER_NAME,
    APPEND_ONLY_TRUNCATE_TRIGGER_NAME,
    requires_database,
    truncate_projects_cascade_for_cleanup,
)
from tests.test_ingest_output_persistence import (
    _build_contract_entity,
    _load_job_events,
    _load_project_materialization,
    _load_project_outputs,
    _replace_fake_canonical_payload,
)
from tests.test_jobs import (
    _FAKE_RUNNER_ADAPTER_KEY,
    _FAKE_RUNNER_ADAPTER_VERSION,
    _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
    _FAKE_RUNNER_CONFIDENCE_SCORE,
    _build_fake_ingest_payload,
)
from tests.test_quantity_takeoff_persistence import _seed_quantity_lineage

_APPEND_ONLY_SQLSTATE = "55000"
_DOWNGRADE_TARGET_REVISION = "2026_05_11_0013"
_MATERIALIZATION_DOWNGRADE_TARGET_REVISION = "2026_05_12_0014"
_QUANTITY_DOWNGRADE_TARGET_REVISION = "2026_05_14_0016"
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
    quantity_takeoff_id: uuid.UUID
    quantity_item_id: uuid.UUID
    revision_entity_manifest_id: uuid.UUID
    revision_layout_id: uuid.UUID
    revision_layer_id: uuid.UUID
    revision_block_id: uuid.UUID
    revision_entity_id: uuid.UUID


@pytest.fixture(autouse=True)
def fake_ingestion_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> list[IngestionRunRequest]:
    """Patch worker ingestion with deterministic persisted outputs."""

    recorded_requests: list[IngestionRunRequest] = []

    async def _fake_run_ingestion(request: IngestionRunRequest) -> IngestFinalizationPayload:
        recorded_requests.append(request)
        payload = _build_fake_ingest_payload(request)
        return _replace_fake_canonical_payload(
            payload,
            blocks=[{"block_ref": "DOOR-1", "name": "DOOR-1"}],
            entities=[
                _build_contract_entity(
                    entity_id="entity-append-only-001",
                    entity_type="line",
                    layer_ref="A-WALL",
                    block_ref="DOOR-1",
                    source_id="entity-source-append-only-001",
                )
            ],
        )

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
        "quantity_takeoffs": row_ids.quantity_takeoff_id,
        "quantity_items": row_ids.quantity_item_id,
        "revision_entity_manifests": row_ids.revision_entity_manifest_id,
        "revision_layouts": row_ids.revision_layout_id,
        "revision_layers": row_ids.revision_layer_id,
        "revision_blocks": row_ids.revision_block_id,
        "revision_entities": row_ids.revision_entity_id,
    }
    return ids_by_table[table_name]


async def _seed_protected_rows(async_client: httpx.AsyncClient) -> _ProtectedRowIds:
    """Create one persisted row in each protected lineage/history table."""

    quantity_seed = await _seed_quantity_lineage(async_client)

    adapter_outputs, drawing_revisions, validation_reports, generated_artifacts = (
        await _load_project_outputs(str(quantity_seed.project_id))
    )
    manifests, layouts, layers, blocks, entities = await _load_project_materialization(
        str(quantity_seed.project_id)
    )
    job_events = await _load_job_events(quantity_seed.ingest_job_id)

    assert len(adapter_outputs) == 1
    assert len(drawing_revisions) == 1
    assert len(validation_reports) == 1
    assert len(generated_artifacts) == 1
    assert len(manifests) == 1
    assert len(layouts) == 1
    assert len(layers) == 1
    assert len(blocks) == 1
    assert len(entities) == 1
    assert job_events

    return _ProtectedRowIds(
        project_id=quantity_seed.project_id,
        file_id=quantity_seed.file_id,
        extraction_profile_id=adapter_outputs[0].extraction_profile_id,
        job_id=quantity_seed.ingest_job_id,
        adapter_run_output_id=adapter_outputs[0].id,
        drawing_revision_id=drawing_revisions[0].id,
        validation_report_id=validation_reports[0].id,
        generated_artifact_id=generated_artifacts[0].id,
        job_event_id=job_events[0].id,
        quantity_takeoff_id=quantity_seed.quantity_takeoff_id,
        quantity_item_id=quantity_seed.quantity_item_id,
        revision_entity_manifest_id=manifests[0].id,
        revision_layout_id=layouts[0].id,
        revision_layer_id=layers[0].id,
        revision_block_id=blocks[0].id,
        revision_entity_id=entities[0].id,
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


async def _insert_materialized_manifest_row(database_url: str) -> None:
    """Insert one revision materialization manifest row for downgrade guard validation."""

    engine = create_async_engine(database_url)
    project_id = uuid.uuid4()
    file_id = uuid.uuid4()
    extraction_profile_id = uuid.uuid4()
    job_id = uuid.uuid4()
    adapter_run_output_id = uuid.uuid4()
    drawing_revision_id = uuid.uuid4()
    manifest_id = uuid.uuid4()
    canonical_json = dumps(
        {
            "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            "schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            "layouts": [],
            "layers": [],
            "blocks": [],
            "entities": [],
            "entity_counts": {
                "layouts": 0,
                "layers": 0,
                "blocks": 0,
                "entities": 0,
            },
        },
        separators=(",", ":"),
    )
    provenance_json = dumps(
        {
            "schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            "adapter": {
                "key": _FAKE_RUNNER_ADAPTER_KEY,
                "version": _FAKE_RUNNER_ADAPTER_VERSION,
            },
            "source": {
                "file_id": str(file_id),
                "job_id": str(job_id),
                "extraction_profile_id": str(extraction_profile_id),
                "input_family": "pdf_vector",
                "revision_kind": "ingest",
            },
            "records": [],
            "generated_at": "2026-01-02T03:04:05+00:00",
        },
        separators=(",", ":"),
    )
    confidence_json = dumps({"score": _FAKE_RUNNER_CONFIDENCE_SCORE}, separators=(",", ":"))
    counts_json = dumps(
        {"layouts": 0, "layers": 0, "blocks": 0, "entities": 0},
        separators=(",", ":"),
    )

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
                    ) VALUES (
                        :id,
                        :name,
                        :description,
                        :default_unit_system,
                        :default_currency
                    )
                    """
                ),
                {
                    "id": project_id,
                    "name": "materialization downgrade guard",
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
                        checksum_sha256,
                        immutable,
                        initial_job_id,
                        initial_extraction_profile_id
                    ) VALUES (
                        :id,
                        :project_id,
                        :original_filename,
                        :media_type,
                        :detected_format,
                        :storage_uri,
                        :size_bytes,
                        :checksum_sha256,
                        :immutable,
                        :initial_job_id,
                        :initial_extraction_profile_id
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
                    "immutable": True,
                    "initial_job_id": job_id,
                    "initial_extraction_profile_id": extraction_profile_id,
                },
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO extraction_profiles (
                        id,
                        project_id,
                        profile_version,
                        units_override,
                        layout_mode,
                        xref_handling,
                        block_handling,
                        text_extraction,
                        dimension_extraction,
                        pdf_page_range,
                        raster_calibration,
                        confidence_threshold
                    ) VALUES (
                        :id,
                        :project_id,
                        :profile_version,
                        :units_override,
                        :layout_mode,
                        :xref_handling,
                        :block_handling,
                        :text_extraction,
                        :dimension_extraction,
                        :pdf_page_range,
                        CAST(:raster_calibration AS json),
                        :confidence_threshold
                    )
                    """
                ),
                {
                    "id": extraction_profile_id,
                    "project_id": project_id,
                    "profile_version": "0.1",
                    "units_override": None,
                    "layout_mode": "all",
                    "xref_handling": "embed",
                    "block_handling": "expand",
                    "text_extraction": True,
                    "dimension_extraction": True,
                    "pdf_page_range": None,
                    "raster_calibration": "null",
                    "confidence_threshold": 0.6,
                },
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO jobs (
                        id,
                        project_id,
                        file_id,
                        extraction_profile_id,
                        base_revision_id,
                        job_type,
                        status,
                        attempts,
                        max_attempts,
                        attempt_token,
                        attempt_lease_expires_at,
                        enqueue_status,
                        enqueue_attempts,
                        enqueue_owner_token,
                        enqueue_lease_expires_at,
                        enqueue_last_attempted_at,
                        enqueue_published_at,
                        cancel_requested,
                        error_code,
                        error_message,
                        started_at,
                        finished_at
                    ) VALUES (
                        :id,
                        :project_id,
                        :file_id,
                        :extraction_profile_id,
                        :base_revision_id,
                        :job_type,
                        :status,
                        :attempts,
                        :max_attempts,
                        :attempt_token,
                        :attempt_lease_expires_at,
                        :enqueue_status,
                        :enqueue_attempts,
                        :enqueue_owner_token,
                        :enqueue_lease_expires_at,
                        :enqueue_last_attempted_at,
                        :enqueue_published_at,
                        :cancel_requested,
                        :error_code,
                        :error_message,
                        :started_at,
                        :finished_at
                    )
                    """
                ),
                {
                    "id": job_id,
                    "project_id": project_id,
                    "file_id": file_id,
                    "extraction_profile_id": extraction_profile_id,
                    "base_revision_id": None,
                    "job_type": "ingest",
                    "status": "succeeded",
                    "attempts": 1,
                    "max_attempts": 3,
                    "attempt_token": None,
                    "attempt_lease_expires_at": None,
                    "enqueue_status": "published",
                    "enqueue_attempts": 1,
                    "enqueue_owner_token": None,
                    "enqueue_lease_expires_at": None,
                    "enqueue_last_attempted_at": None,
                    "enqueue_published_at": None,
                    "cancel_requested": False,
                    "error_code": None,
                    "error_message": None,
                    "started_at": None,
                    "finished_at": None,
                },
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO adapter_run_outputs (
                        id,
                        project_id,
                        source_file_id,
                        extraction_profile_id,
                        source_job_id,
                        adapter_key,
                        adapter_version,
                        input_family,
                        canonical_entity_schema_version,
                        canonical_json,
                        provenance_json,
                        confidence_json,
                        confidence_score,
                        warnings_json,
                        diagnostics_json,
                        result_checksum_sha256
                    ) VALUES (
                        :id,
                        :project_id,
                        :source_file_id,
                        :extraction_profile_id,
                        :source_job_id,
                        :adapter_key,
                        :adapter_version,
                        :input_family,
                        :canonical_entity_schema_version,
                        CAST(:canonical_json AS json),
                        CAST(:provenance_json AS json),
                        CAST(:confidence_json AS json),
                        :confidence_score,
                        CAST(:warnings_json AS json),
                        CAST(:diagnostics_json AS json),
                        :result_checksum_sha256
                    )
                    """
                ),
                {
                    "id": adapter_run_output_id,
                    "project_id": project_id,
                    "source_file_id": file_id,
                    "extraction_profile_id": extraction_profile_id,
                    "source_job_id": job_id,
                    "adapter_key": _FAKE_RUNNER_ADAPTER_KEY,
                    "adapter_version": _FAKE_RUNNER_ADAPTER_VERSION,
                    "input_family": "pdf_vector",
                    "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                    "canonical_json": canonical_json,
                    "provenance_json": provenance_json,
                    "confidence_json": confidence_json,
                    "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                    "warnings_json": "[]",
                    "diagnostics_json": '{"adapter":"tests","diagnostics":[]}',
                    "result_checksum_sha256": "b" * 64,
                },
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO drawing_revisions (
                        id,
                        project_id,
                        source_file_id,
                        extraction_profile_id,
                        source_job_id,
                        adapter_run_output_id,
                        predecessor_revision_id,
                        revision_sequence,
                        revision_kind,
                        review_state,
                        canonical_entity_schema_version,
                        confidence_score
                    ) VALUES (
                        :id,
                        :project_id,
                        :source_file_id,
                        :extraction_profile_id,
                        :source_job_id,
                        :adapter_run_output_id,
                        :predecessor_revision_id,
                        :revision_sequence,
                        :revision_kind,
                        :review_state,
                        :canonical_entity_schema_version,
                        :confidence_score
                    )
                    """
                ),
                {
                    "id": drawing_revision_id,
                    "project_id": project_id,
                    "source_file_id": file_id,
                    "extraction_profile_id": extraction_profile_id,
                    "source_job_id": job_id,
                    "adapter_run_output_id": adapter_run_output_id,
                    "predecessor_revision_id": None,
                    "revision_sequence": 1,
                    "revision_kind": "ingest",
                    "review_state": "approved",
                    "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                    "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                },
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO revision_entity_manifests (
                        id,
                        project_id,
                        source_file_id,
                        extraction_profile_id,
                        source_job_id,
                        drawing_revision_id,
                        adapter_run_output_id,
                        canonical_entity_schema_version,
                        counts_json
                    ) VALUES (
                        :id,
                        :project_id,
                        :source_file_id,
                        :extraction_profile_id,
                        :source_job_id,
                        :drawing_revision_id,
                        :adapter_run_output_id,
                        :canonical_entity_schema_version,
                        CAST(:counts_json AS json)
                    )
                    """
                ),
                {
                    "id": manifest_id,
                    "project_id": project_id,
                    "source_file_id": file_id,
                    "extraction_profile_id": extraction_profile_id,
                    "source_job_id": job_id,
                    "drawing_revision_id": drawing_revision_id,
                    "adapter_run_output_id": adapter_run_output_id,
                    "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                    "counts_json": counts_json,
                },
            )
    finally:
        await engine.dispose()


async def _insert_quantity_takeoff_row(database_url: str) -> None:
    """Insert one quantity takeoff row for downgrade guard validation."""

    engine = create_async_engine(database_url)
    project_id = uuid.uuid4()
    file_id = uuid.uuid4()
    extraction_profile_id = uuid.uuid4()
    ingest_job_id = uuid.uuid4()
    adapter_run_output_id = uuid.uuid4()
    drawing_revision_id = uuid.uuid4()
    quantity_job_id = uuid.uuid4()
    quantity_takeoff_id = uuid.uuid4()
    canonical_json = dumps(
        {
            "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            "schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            "layouts": [],
            "layers": [],
            "blocks": [],
            "entities": [],
            "entity_counts": {
                "layouts": 0,
                "layers": 0,
                "blocks": 0,
                "entities": 0,
            },
        },
        separators=(",", ":"),
    )
    provenance_json = dumps(
        {
            "schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            "adapter": {
                "key": _FAKE_RUNNER_ADAPTER_KEY,
                "version": _FAKE_RUNNER_ADAPTER_VERSION,
            },
            "source": {
                "file_id": str(file_id),
                "job_id": str(ingest_job_id),
                "extraction_profile_id": str(extraction_profile_id),
                "input_family": "pdf_vector",
                "revision_kind": "ingest",
            },
            "records": [],
            "generated_at": "2026-01-02T03:04:05+00:00",
        },
        separators=(",", ":"),
    )
    confidence_json = dumps({"score": _FAKE_RUNNER_CONFIDENCE_SCORE}, separators=(",", ":"))

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
                    ) VALUES (
                        :id,
                        :name,
                        :description,
                        :default_unit_system,
                        :default_currency
                    )
                    """
                ),
                {
                    "id": project_id,
                    "name": "quantity downgrade guard",
                    "description": None,
                    "default_unit_system": None,
                    "default_currency": None,
                },
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO extraction_profiles (
                        id,
                        project_id,
                        profile_version,
                        units_override,
                        layout_mode,
                        xref_handling,
                        block_handling,
                        text_extraction,
                        dimension_extraction,
                        pdf_page_range,
                        raster_calibration,
                        confidence_threshold
                    ) VALUES (
                        :id,
                        :project_id,
                        :profile_version,
                        :units_override,
                        :layout_mode,
                        :xref_handling,
                        :block_handling,
                        :text_extraction,
                        :dimension_extraction,
                        :pdf_page_range,
                        CAST(:raster_calibration AS json),
                        :confidence_threshold
                    )
                    """
                ),
                {
                    "id": extraction_profile_id,
                    "project_id": project_id,
                    "profile_version": "0.1",
                    "units_override": None,
                    "layout_mode": "all",
                    "xref_handling": "embed",
                    "block_handling": "expand",
                    "text_extraction": True,
                    "dimension_extraction": True,
                    "pdf_page_range": None,
                    "raster_calibration": "null",
                    "confidence_threshold": 0.6,
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
                        checksum_sha256,
                        immutable,
                        initial_job_id,
                        initial_extraction_profile_id
                    ) VALUES (
                        :id,
                        :project_id,
                        :original_filename,
                        :media_type,
                        :detected_format,
                        :storage_uri,
                        :size_bytes,
                        :checksum_sha256,
                        :immutable,
                        :initial_job_id,
                        :initial_extraction_profile_id
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
                    "immutable": True,
                    "initial_job_id": ingest_job_id,
                    "initial_extraction_profile_id": extraction_profile_id,
                },
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO jobs (
                        id,
                        project_id,
                        file_id,
                        extraction_profile_id,
                        base_revision_id,
                        job_type,
                        status,
                        attempts,
                        max_attempts,
                        attempt_token,
                        attempt_lease_expires_at,
                        enqueue_status,
                        enqueue_attempts,
                        enqueue_owner_token,
                        enqueue_lease_expires_at,
                        enqueue_last_attempted_at,
                        enqueue_published_at,
                        cancel_requested,
                        error_code,
                        error_message,
                        started_at,
                        finished_at
                    ) VALUES (
                        :id,
                        :project_id,
                        :file_id,
                        :extraction_profile_id,
                        :base_revision_id,
                        :job_type,
                        :status,
                        :attempts,
                        :max_attempts,
                        :attempt_token,
                        :attempt_lease_expires_at,
                        :enqueue_status,
                        :enqueue_attempts,
                        :enqueue_owner_token,
                        :enqueue_lease_expires_at,
                        :enqueue_last_attempted_at,
                        :enqueue_published_at,
                        :cancel_requested,
                        :error_code,
                        :error_message,
                        :started_at,
                        :finished_at
                    )
                    """
                ),
                {
                    "id": ingest_job_id,
                    "project_id": project_id,
                    "file_id": file_id,
                    "extraction_profile_id": extraction_profile_id,
                    "base_revision_id": None,
                    "job_type": "ingest",
                    "status": "succeeded",
                    "attempts": 1,
                    "max_attempts": 3,
                    "attempt_token": None,
                    "attempt_lease_expires_at": None,
                    "enqueue_status": "published",
                    "enqueue_attempts": 1,
                    "enqueue_owner_token": None,
                    "enqueue_lease_expires_at": None,
                    "enqueue_last_attempted_at": None,
                    "enqueue_published_at": None,
                    "cancel_requested": False,
                    "error_code": None,
                    "error_message": None,
                    "started_at": None,
                    "finished_at": None,
                },
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO adapter_run_outputs (
                        id,
                        project_id,
                        source_file_id,
                        extraction_profile_id,
                        source_job_id,
                        adapter_key,
                        adapter_version,
                        input_family,
                        canonical_entity_schema_version,
                        canonical_json,
                        provenance_json,
                        confidence_json,
                        confidence_score,
                        warnings_json,
                        diagnostics_json,
                        result_checksum_sha256
                    ) VALUES (
                        :id,
                        :project_id,
                        :source_file_id,
                        :extraction_profile_id,
                        :source_job_id,
                        :adapter_key,
                        :adapter_version,
                        :input_family,
                        :canonical_entity_schema_version,
                        CAST(:canonical_json AS json),
                        CAST(:provenance_json AS json),
                        CAST(:confidence_json AS json),
                        :confidence_score,
                        CAST(:warnings_json AS json),
                        CAST(:diagnostics_json AS json),
                        :result_checksum_sha256
                    )
                    """
                ),
                {
                    "id": adapter_run_output_id,
                    "project_id": project_id,
                    "source_file_id": file_id,
                    "extraction_profile_id": extraction_profile_id,
                    "source_job_id": ingest_job_id,
                    "adapter_key": _FAKE_RUNNER_ADAPTER_KEY,
                    "adapter_version": _FAKE_RUNNER_ADAPTER_VERSION,
                    "input_family": "pdf_vector",
                    "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                    "canonical_json": canonical_json,
                    "provenance_json": provenance_json,
                    "confidence_json": confidence_json,
                    "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                    "warnings_json": "[]",
                    "diagnostics_json": '{"adapter":"tests","diagnostics":[]}',
                    "result_checksum_sha256": "b" * 64,
                },
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO drawing_revisions (
                        id,
                        project_id,
                        source_file_id,
                        extraction_profile_id,
                        source_job_id,
                        adapter_run_output_id,
                        predecessor_revision_id,
                        revision_sequence,
                        revision_kind,
                        review_state,
                        canonical_entity_schema_version,
                        confidence_score
                    ) VALUES (
                        :id,
                        :project_id,
                        :source_file_id,
                        :extraction_profile_id,
                        :source_job_id,
                        :adapter_run_output_id,
                        :predecessor_revision_id,
                        :revision_sequence,
                        :revision_kind,
                        :review_state,
                        :canonical_entity_schema_version,
                        :confidence_score
                    )
                    """
                ),
                {
                    "id": drawing_revision_id,
                    "project_id": project_id,
                    "source_file_id": file_id,
                    "extraction_profile_id": extraction_profile_id,
                    "source_job_id": ingest_job_id,
                    "adapter_run_output_id": adapter_run_output_id,
                    "predecessor_revision_id": None,
                    "revision_sequence": 1,
                    "revision_kind": "ingest",
                    "review_state": "approved",
                    "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                    "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                },
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO jobs (
                        id,
                        project_id,
                        file_id,
                        extraction_profile_id,
                        base_revision_id,
                        job_type,
                        status,
                        attempts,
                        max_attempts,
                        attempt_token,
                        attempt_lease_expires_at,
                        enqueue_status,
                        enqueue_attempts,
                        enqueue_owner_token,
                        enqueue_lease_expires_at,
                        enqueue_last_attempted_at,
                        enqueue_published_at,
                        cancel_requested,
                        error_code,
                        error_message,
                        started_at,
                        finished_at
                    ) VALUES (
                        :id,
                        :project_id,
                        :file_id,
                        :extraction_profile_id,
                        :base_revision_id,
                        :job_type,
                        :status,
                        :attempts,
                        :max_attempts,
                        :attempt_token,
                        :attempt_lease_expires_at,
                        :enqueue_status,
                        :enqueue_attempts,
                        :enqueue_owner_token,
                        :enqueue_lease_expires_at,
                        :enqueue_last_attempted_at,
                        :enqueue_published_at,
                        :cancel_requested,
                        :error_code,
                        :error_message,
                        :started_at,
                        :finished_at
                    )
                    """
                ),
                {
                    "id": quantity_job_id,
                    "project_id": project_id,
                    "file_id": file_id,
                    "extraction_profile_id": None,
                    "base_revision_id": drawing_revision_id,
                    "job_type": "quantity_takeoff",
                    "status": "succeeded",
                    "attempts": 1,
                    "max_attempts": 3,
                    "attempt_token": None,
                    "attempt_lease_expires_at": None,
                    "enqueue_status": "published",
                    "enqueue_attempts": 1,
                    "enqueue_owner_token": None,
                    "enqueue_lease_expires_at": None,
                    "enqueue_last_attempted_at": None,
                    "enqueue_published_at": None,
                    "cancel_requested": False,
                    "error_code": None,
                    "error_message": None,
                    "started_at": None,
                    "finished_at": None,
                },
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO quantity_takeoffs (
                        id,
                        project_id,
                        source_file_id,
                        drawing_revision_id,
                        source_job_id,
                        source_job_type,
                        review_state,
                        validation_status,
                        quantity_gate,
                        trusted_totals
                    ) VALUES (
                        :id,
                        :project_id,
                        :source_file_id,
                        :drawing_revision_id,
                        :source_job_id,
                        :source_job_type,
                        :review_state,
                        :validation_status,
                        :quantity_gate,
                        :trusted_totals
                    )
                    """
                ),
                {
                    "id": quantity_takeoff_id,
                    "project_id": project_id,
                    "source_file_id": file_id,
                    "drawing_revision_id": drawing_revision_id,
                    "source_job_id": quantity_job_id,
                    "source_job_type": "quantity_takeoff",
                    "review_state": "approved",
                    "validation_status": "valid",
                    "quantity_gate": "allowed",
                    "trusted_totals": True,
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
            ("quantity_takeoffs", "review_state", "review_required"),
            ("quantity_items", "quantity_type", "mutated_quantity_type"),
            (
                "revision_entity_manifests",
                "canonical_entity_schema_version",
                "mutated",
            ),
            ("revision_layouts", "layout_ref", "Paper"),
            ("revision_layers", "layer_ref", "B-WALL"),
            ("revision_blocks", "block_ref", "DOOR-2"),
            ("revision_entities", "entity_type", "polyline"),
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
                "layers": [{"layer_ref": "A-WALL", "name": "A-WALL"}],
                "layouts": [{"layout_ref": "Model", "name": "Model"}],
                "blocks": [{"block_ref": "DOOR-1", "name": "DOOR-1"}],
                "entities": [
                    {
                        "entity_id": "entity-append-only-001",
                        "entity_type": "line",
                        "entity_schema_version": "0.1",
                        "layout_ref": "Model",
                        "layer_ref": "A-WALL",
                        "block_ref": "DOOR-1",
                        "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                        "confidence_json": {
                            "score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                            "basis": "adapter",
                        },
                        "geometry_json": {
                            "type": "line",
                            "coordinates": [[0.0, 0.0], [1.0, 1.0]],
                        },
                        "properties_json": {"layer": "A-WALL"},
                        "provenance_json": {
                            "source_id": "entity-source-append-only-001"
                        },
                    }
                ],
                "entity_counts": {
                    "entities": 1,
                    "blocks": 1,
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

        equivalent_entity_payload_json = dumps(
            {
                "entity_id": "entity-append-only-001",
                "entity_type": "line",
                "entity_schema_version": "0.1",
                "layout_ref": "Model",
                "layer_ref": "A-WALL",
                "block_ref": "DOOR-1",
                "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                "confidence_json": {
                    "score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                    "basis": "adapter",
                },
                "geometry_json": {
                    "type": "line",
                    "coordinates": [[0.0, 0.0], [1.0, 1.0]],
                },
                "properties_json": {"layer": "A-WALL"},
                "provenance_json": {"source_id": "entity-source-append-only-001"},
            },
            separators=(",", ":"),
        )

        await _run_sql_and_expect_append_only_failure(
            (
                'UPDATE "revision_entities" '
                'SET canonical_entity_json = CAST(:replacement_value AS json) '
                'WHERE id = :row_id'
            ),
            {
                "replacement_value": equivalent_entity_payload_json,
                "row_id": row_ids.revision_entity_id,
            },
            operation="UPDATE",
            table_name="revision_entities",
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

    async def test_materialization_downgrade_fails_closed_on_populated_tables(self) -> None:
        """Downgrade should refuse to drop materialization tables while rows exist."""

        _ = self
        database_name, database_url = await _create_temp_database()

        try:
            upgrade_result = _run_alembic_command("upgrade", "head", database_url=database_url)
            assert upgrade_result.returncode == 0, upgrade_result.stdout + upgrade_result.stderr

            await _insert_materialized_manifest_row(database_url)

            downgrade_result = _run_alembic_command(
                "downgrade",
                _MATERIALIZATION_DOWNGRADE_TARGET_REVISION,
                database_url=database_url,
            )
            assert downgrade_result.returncode != 0

            combined_output = downgrade_result.stdout + downgrade_result.stderr
            assert "Refusing to downgrade migration 2026_05_13_0015" in combined_output
            assert "revision_entity_manifests" in combined_output
        finally:
            await _drop_temp_database(database_name)

    async def test_quantity_downgrade_fails_closed_on_populated_tables(self) -> None:
        """Downgrade should refuse to drop quantity persistence while rows exist."""

        _ = self
        database_name, database_url = await _create_temp_database()

        try:
            upgrade_result = _run_alembic_command("upgrade", "head", database_url=database_url)
            assert upgrade_result.returncode == 0, upgrade_result.stdout + upgrade_result.stderr

            await _insert_quantity_takeoff_row(database_url)

            downgrade_result = _run_alembic_command(
                "downgrade",
                _QUANTITY_DOWNGRADE_TARGET_REVISION,
                database_url=database_url,
            )
            assert downgrade_result.returncode != 0

            combined_output = downgrade_result.stdout + downgrade_result.stderr
            assert "Refusing to downgrade migration 2026_05_15_0017" in combined_output
            assert "quantity_takeoffs" in combined_output
        finally:
            await _drop_temp_database(database_name)
