"""Integration tests for append-only lineage/history table enforcement."""

from __future__ import annotations

import os
import subprocess
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from json import dumps
from pathlib import Path
from typing import Any

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import app.db.session as session_module
import app.jobs.worker as worker_module
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunRequest
from app.models.cad_changeset import CadChangeOperation, CadChangeSet, CadChangeSetValidationResult
from app.models.changeset_apply_job_input import ChangeSetApplyJobInput
from app.models.estimate_job_input import EstimateJobInput, EstimateJobInputCatalogRef
from app.models.estimate_version import EstimateItem, EstimateSnapshotEntry
from app.models.estimation_catalog import EstimationRate
from app.models.export_job_input import ExportJobInput
from app.models.job import Job, JobStatus, JobType
from app.models.revision_room import RevisionRoom
from app.models.revision_routed_length import RevisionRoutedLength
from tests import test_estimate_version_persistence as estimate_version_persistence
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
_CHECK_VIOLATION_SQLSTATE = "23514"
_FOREIGN_KEY_VIOLATION_SQLSTATE = "23503"
_UNIQUE_VIOLATION_SQLSTATE = "23505"
_DOWNGRADE_TARGET_REVISION = "2026_05_11_0013"
_MATERIALIZATION_DOWNGRADE_TARGET_REVISION = "2026_05_12_0014"
_QUANTITY_DOWNGRADE_TARGET_REVISION = "2026_05_14_0016"
_CHANGESET_ORIGIN_DOWNGRADE_TARGET_REVISION = "2026_05_27_0024"
_GENERATED_ARTIFACT_LINEAGE_DOWNGRADE_TARGET_REVISION = "2026_05_31_0025"
_CHANGESET_PERSISTENCE_DOWNGRADE_TARGET_REVISION = "2026_05_31_0026"
_CHANGESET_APPLY_JOB_INPUT_DOWNGRADE_TARGET_REVISION = "2026_06_01_0027"
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
    cad_change_set_id: uuid.UUID
    cad_change_operation_id: uuid.UUID
    cad_change_set_validation_result_id: uuid.UUID
    changeset_apply_job_input_source_job_id: uuid.UUID
    job_event_id: uuid.UUID
    estimate_version_id: uuid.UUID
    export_job_input_source_job_id: uuid.UUID
    estimate_job_input_id: uuid.UUID
    estimate_job_input_catalog_ref_key: tuple[uuid.UUID, str, str]
    estimate_snapshot_entry_id: uuid.UUID
    estimate_item_id: uuid.UUID
    quantity_takeoff_id: uuid.UUID
    quantity_item_id: uuid.UUID
    revision_entity_manifest_id: uuid.UUID
    revision_layout_id: uuid.UUID
    revision_layer_id: uuid.UUID
    revision_block_id: uuid.UUID
    revision_entity_id: uuid.UUID
    revision_routed_length_id: uuid.UUID
    revision_room_id: uuid.UUID


@dataclass(frozen=True)
class _ChangesetMaterializationIds:
    manifest_id: uuid.UUID
    layout_id: uuid.UUID
    layer_id: uuid.UUID
    block_id: uuid.UUID
    entity_id: uuid.UUID


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

    sqlstate = _extract_sqlstate(error)
    error_text = str(error)
    assert sqlstate == _APPEND_ONLY_SQLSTATE
    assert f"append-only trigger blocked {operation}" in error_text
    if table_name is not None:
        assert f"on {table_name}" in error_text


def _row_id_for_table(row_ids: _ProtectedRowIds, table_name: str) -> uuid.UUID:
    """Map protected table names to seeded row identifiers."""

    ids_by_table = {
        "files": row_ids.file_id,
        "extraction_profiles": row_ids.extraction_profile_id,
        "adapter_run_outputs": row_ids.adapter_run_output_id,
        "drawing_revisions": row_ids.drawing_revision_id,
        "validation_reports": row_ids.validation_report_id,
        "generated_artifacts": row_ids.generated_artifact_id,
        "cad_change_sets": row_ids.cad_change_set_id,
        "cad_change_operations": row_ids.cad_change_operation_id,
        "cad_change_set_validation_results": row_ids.cad_change_set_validation_result_id,
        "job_events": row_ids.job_event_id,
        "estimate_versions": row_ids.estimate_version_id,
        "estimate_job_inputs": row_ids.estimate_job_input_id,
        "estimate_snapshot_entries": row_ids.estimate_snapshot_entry_id,
        "estimate_items": row_ids.estimate_item_id,
        "quantity_takeoffs": row_ids.quantity_takeoff_id,
        "quantity_items": row_ids.quantity_item_id,
        "revision_entity_manifests": row_ids.revision_entity_manifest_id,
        "revision_layouts": row_ids.revision_layout_id,
        "revision_layers": row_ids.revision_layer_id,
        "revision_blocks": row_ids.revision_block_id,
        "revision_entities": row_ids.revision_entity_id,
        "revision_routed_lengths": row_ids.revision_routed_length_id,
        "revision_rooms": row_ids.revision_room_id,
    }
    return ids_by_table[table_name]


def _row_filter_for_table(
    row_ids: _ProtectedRowIds,
    table_name: str,
) -> tuple[str, dict[str, object]]:
    """Return a WHERE clause and parameters for a protected table's seeded row."""

    if table_name == "estimate_job_input_catalog_refs":
        estimate_job_id, ref_type, selection_key = row_ids.estimate_job_input_catalog_ref_key
        return (
            "estimate_job_id = :estimate_job_id "
            "AND ref_type = :ref_type "
            "AND selection_key = :selection_key",
            {
                "estimate_job_id": estimate_job_id,
                "ref_type": ref_type,
                "selection_key": selection_key,
            },
        )
    if table_name == "estimate_job_inputs":
        return (
            "estimate_job_id = :estimate_job_id",
            {"estimate_job_id": row_ids.estimate_job_input_id},
        )
    if table_name == "changeset_apply_job_inputs":
        return (
            "source_job_id = :source_job_id",
            {"source_job_id": row_ids.changeset_apply_job_input_source_job_id},
        )
    if table_name == "export_job_inputs":
        return (
            "source_job_id = :source_job_id",
            {"source_job_id": row_ids.export_job_input_source_job_id},
        )

    return "id = :row_id", {"row_id": _row_id_for_table(row_ids, table_name)}


async def _seed_protected_rows(async_client: httpx.AsyncClient) -> _ProtectedRowIds:
    """Create one persisted row in each protected lineage/history table."""

    quantity_seed = await _seed_quantity_lineage(async_client)

    (
        adapter_outputs,
        drawing_revisions,
        validation_reports,
        generated_artifacts,
    ) = await _load_project_outputs(str(quantity_seed.project_id))
    manifests, layouts, layers, blocks, entities = await _load_project_materialization(
        str(quantity_seed.project_id)
    )
    job_events = await _load_job_events(quantity_seed.ingest_job_id)
    estimate_source_job_id = await estimate_version_persistence._create_estimate_source_job(
        quantity_seed
    )

    estimate_version = estimate_version_persistence._build_estimate_version(
        quantity_seed,
        source_job_id=estimate_source_job_id,
        quantity_takeoff_id=quantity_seed.quantity_takeoff_id,
    )
    estimate_snapshot_entry = EstimateSnapshotEntry(
        id=uuid.uuid4(),
        estimate_version_id=estimate_version.id,
        project_id=quantity_seed.project_id,
        drawing_revision_id=quantity_seed.drawing_revision_id,
        entry_type="assumption",
        entry_key="assumption:append-only",
        entry_label="Append-only assumption",
        sort_order=1,
        source_payload_json={"kind": "assumption", "value": "seed"},
    )
    estimate_item = EstimateItem(
        id=uuid.uuid4(),
        estimate_version_id=estimate_version.id,
        project_id=quantity_seed.project_id,
        drawing_revision_id=quantity_seed.drawing_revision_id,
        line_type="assumption",
        line_number=1,
        line_key="line:assumption:append-only",
        description="Append-only assumption line",
        currency="GBP",
        subtotal_amount=Decimal("0.00"),
        tax_amount=Decimal("0.00"),
        total_amount=Decimal("0.00"),
        assumption_snapshot_entry_id=estimate_snapshot_entry.id,
        quantity_snapshot_entry_type="quantity_input",
        rate_snapshot_entry_type="rate",
        material_snapshot_entry_type="material",
        formula_snapshot_entry_type="formula",
        assumption_snapshot_entry_type="assumption",
    )
    estimate_job_id = uuid.uuid4()
    changeset_apply_job_id = uuid.uuid4()
    export_job_id = uuid.uuid4()
    rate_id = uuid.uuid4()
    rate_checksum = "d" * 64
    estimate_job_input = EstimateJobInput(
        estimate_job_id=estimate_job_id,
        project_id=quantity_seed.project_id,
        source_file_id=quantity_seed.file_id,
        drawing_revision_id=quantity_seed.drawing_revision_id,
        quantity_takeoff_id=quantity_seed.quantity_takeoff_id,
        source_job_type=JobType.ESTIMATE.value,
        currency="GBP",
        pricing_effective_date=date(2026, 1, 1),
        pricing_mode="explicit",
        assumptions_json={"source": "append-only"},
    )
    estimate_job_input_ref = EstimateJobInputCatalogRef(
        estimate_job_id=estimate_job_id,
        ref_type="rate",
        selection_key="rate:append-only",
        ref_order=1,
        rate_catalog_entry_id=rate_id,
        catalog_checksum_sha256=rate_checksum,
        selection_context_json={"source": "append-only"},
    )
    export_job_input = ExportJobInput(
        source_job_id=export_job_id,
        project_id=quantity_seed.project_id,
        source_file_id=quantity_seed.file_id,
        drawing_revision_id=quantity_seed.drawing_revision_id,
        source_job_type=JobType.EXPORT.value,
        export_kind="revision_json",
        export_format="json",
        media_type="application/json",
        options_json={"source": "append-only"},
        quantity_takeoff_id=None,
        estimate_version_id=None,
    )
    change_set = CadChangeSet(
        id=uuid.uuid4(),
        project_id=quantity_seed.project_id,
        base_revision_id=quantity_seed.drawing_revision_id,
        status="proposed",
    )
    change_operation = CadChangeOperation(
        id=uuid.uuid4(),
        project_id=quantity_seed.project_id,
        change_set_id=change_set.id,
        sequence_index=1,
        operation_type="flag_for_review",
        target_revision_entity_id=None,
        expected_source_identity=None,
        expected_source_hash=None,
        operation_json={"source": "append-only"},
    )
    change_set_validation_result = CadChangeSetValidationResult(
        id=uuid.uuid4(),
        project_id=quantity_seed.project_id,
        change_set_id=change_set.id,
        validation_status="valid",
        validator_name="tests",
        validator_version="1",
        result_json={"source": "append-only", "status": "valid"},
    )
    changeset_apply_job_input = ChangeSetApplyJobInput(
        source_job_id=changeset_apply_job_id,
        project_id=quantity_seed.project_id,
        source_file_id=quantity_seed.file_id,
        drawing_revision_id=quantity_seed.drawing_revision_id,
        change_set_id=change_set.id,
        source_job_type=JobType.CHANGESET_APPLY.value,
        latest_validation_result_id=change_set_validation_result.id,
        latest_validation_status=change_set_validation_result.validation_status,
    )

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    async with session_maker() as session:
        session.add(
            EstimationRate(
                id=rate_id,
                scope_type="project",
                project_id=quantity_seed.project_id,
                rate_key="append-only:rate",
                source="test",
                metadata_json={"source": "append-only"},
                name="Append-only rate",
                item_type="linear_length",
                per_unit="m",
                currency="GBP",
                amount=Decimal("1.000000"),
                effective_from=date(2026, 1, 1),
                checksum_sha256=rate_checksum,
            )
        )
        await session.flush()
        session.add(
            Job(
                id=estimate_job_id,
                project_id=quantity_seed.project_id,
                file_id=quantity_seed.file_id,
                extraction_profile_id=None,
                base_revision_id=quantity_seed.drawing_revision_id,
                job_type=JobType.ESTIMATE.value,
                status=JobStatus.SUCCEEDED.value,
                enqueue_status="published",
                enqueue_attempts=0,
                cancel_requested=False,
            )
        )
        await session.flush()
        session.add(
            Job(
                id=export_job_id,
                project_id=quantity_seed.project_id,
                file_id=quantity_seed.file_id,
                extraction_profile_id=None,
                base_revision_id=quantity_seed.drawing_revision_id,
                parent_job_id=quantity_seed.quantity_job_id,
                job_type=JobType.EXPORT.value,
                status=JobStatus.PENDING.value,
                enqueue_status="pending",
                enqueue_attempts=0,
                cancel_requested=False,
            )
        )
        await session.flush()
        session.add(
            Job(
                id=changeset_apply_job_id,
                project_id=quantity_seed.project_id,
                file_id=quantity_seed.file_id,
                extraction_profile_id=None,
                base_revision_id=quantity_seed.drawing_revision_id,
                job_type=JobType.CHANGESET_APPLY.value,
                status=JobStatus.PENDING.value,
                enqueue_status="pending",
                enqueue_attempts=0,
                cancel_requested=False,
            )
        )
        await session.flush()
        session.add(estimate_job_input)
        await session.flush()
        session.add(export_job_input)
        await session.flush()
        session.add(estimate_job_input_ref)
        await session.flush()
        session.add(estimate_version)
        await session.flush()
        session.add(estimate_snapshot_entry)
        await session.flush()
        session.add(estimate_item)
        await session.flush()
        session.add(change_set)
        await session.flush()
        session.add(change_operation)
        await session.flush()
        session.add(change_set_validation_result)
        await session.flush()
        session.add(changeset_apply_job_input)
        await session.flush()
        revision_routed_length = RevisionRoutedLength(
            id=uuid.uuid4(),
            project_id=quantity_seed.project_id,
            source_file_id=quantity_seed.file_id,
            extraction_profile_id=adapter_outputs[0].extraction_profile_id,
            source_job_id=quantity_seed.ingest_job_id,
            drawing_revision_id=drawing_revisions[0].id,
            adapter_run_output_id=adapter_outputs[0].id,
            canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            layer_ref="Pipes",
            colour_key="idx:150",
            algo_version="test",
            raster_params_hash="e" * 64,
            producer_kind="passthrough",
            skeleton_length_du=1.0,
            entity_count=1,
            geometry_json=None,
        )
        session.add(revision_routed_length)
        await session.flush()
        revision_room = RevisionRoom(
            id=uuid.uuid4(),
            project_id=quantity_seed.project_id,
            source_file_id=quantity_seed.file_id,
            extraction_profile_id=adapter_outputs[0].extraction_profile_id,
            source_job_id=quantity_seed.ingest_job_id,
            drawing_revision_id=drawing_revisions[0].id,
            adapter_run_output_id=adapter_outputs[0].id,
            canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            algo_version="test",
            room_key="room-append-only-001",
            name="Append-only room",
            number="101",
            source="polygon",
            area=12.5,
            bounds_min_x=0.0,
            bounds_min_y=0.0,
            bounds_max_x=5.0,
            bounds_max_y=2.5,
            polygon_geometry_json={"type": "polygon", "coordinates": [[0.0, 0.0], [5.0, 2.5]]},
            anchors_json=[{"x": 2.5, "y": 1.25}],
            needs_review=False,
            confidence=0.9,
            strategy="wall_derived",
            source_layers_json=["A-AREA"],
            input_family="dwg",
        )
        session.add(revision_room)
        await session.commit()

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
        cad_change_set_id=change_set.id,
        cad_change_operation_id=change_operation.id,
        cad_change_set_validation_result_id=change_set_validation_result.id,
        changeset_apply_job_input_source_job_id=changeset_apply_job_input.source_job_id,
        job_event_id=job_events[0].id,
        estimate_version_id=estimate_version.id,
        export_job_input_source_job_id=export_job_input.source_job_id,
        estimate_job_input_id=estimate_job_input.estimate_job_id,
        estimate_job_input_catalog_ref_key=(
            estimate_job_input_ref.estimate_job_id,
            estimate_job_input_ref.ref_type,
            estimate_job_input_ref.selection_key,
        ),
        estimate_snapshot_entry_id=estimate_snapshot_entry.id,
        estimate_item_id=estimate_item.id,
        quantity_takeoff_id=quantity_seed.quantity_takeoff_id,
        quantity_item_id=quantity_seed.quantity_item_id,
        revision_entity_manifest_id=manifests[0].id,
        revision_layout_id=layouts[0].id,
        revision_layer_id=layers[0].id,
        revision_block_id=blocks[0].id,
        revision_entity_id=entities[0].id,
        revision_routed_length_id=revision_routed_length.id,
        revision_room_id=revision_room.id,
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


async def _insert_job_row(
    target: Any,
    *,
    job_id: uuid.UUID,
    project_id: uuid.UUID,
    file_id: uuid.UUID,
    extraction_profile_id: uuid.UUID | None,
    base_revision_id: uuid.UUID | None,
    job_type: str,
    status: str,
    enqueue_status: str,
    enqueue_attempts: int,
    parent_job_id: uuid.UUID | None = None,
) -> None:
    """Insert a job row for raw SQL lineage setup."""

    await target.execute(
        sa.text(
            """
            INSERT INTO jobs (
                id,
                project_id,
                file_id,
                extraction_profile_id,
                base_revision_id,
                parent_job_id,
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
                :parent_job_id,
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
            "base_revision_id": base_revision_id,
            "parent_job_id": parent_job_id,
            "job_type": job_type,
            "status": status,
            "attempts": 1,
            "max_attempts": 3,
            "attempt_token": None,
            "attempt_lease_expires_at": None,
            "enqueue_status": enqueue_status,
            "enqueue_attempts": enqueue_attempts,
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


async def _insert_cad_change_set_row(
    target: Any,
    *,
    changeset_id: uuid.UUID,
    project_id: uuid.UUID,
    base_revision_id: uuid.UUID,
    status: str = "proposed",
) -> None:
    """Insert a cad_change_sets row for raw SQL lineage setup."""

    await target.execute(
        sa.text(
            """
            INSERT INTO cad_change_sets (
                id,
                project_id,
                base_revision_id,
                status,
                created_by,
                created_at,
                updated_at
            ) VALUES (
                :id,
                :project_id,
                :base_revision_id,
                :status,
                :created_by,
                :created_at,
                :updated_at
            )
            ON CONFLICT (id) DO NOTHING
            """
        ),
        {
            "id": changeset_id,
            "project_id": project_id,
            "base_revision_id": base_revision_id,
            "status": status,
            "created_by": None,
            "created_at": datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
            "updated_at": datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
        },
    )


async def _insert_drawing_revision_row(
    target: Any,
    *,
    revision_id: uuid.UUID,
    project_id: uuid.UUID,
    file_id: uuid.UUID,
    extraction_profile_id: uuid.UUID | None,
    source_job_id: uuid.UUID,
    adapter_run_output_id: uuid.UUID | None,
    predecessor_revision_id: uuid.UUID | None,
    revision_sequence: int,
    revision_kind: str,
    canonical_entity_schema_version: str,
    changeset_id: uuid.UUID | None,
) -> None:
    """Insert a drawing revision row for raw SQL lineage setup."""

    if changeset_id is not None:
        base_revision_id = predecessor_revision_id
        if base_revision_id is None:
            base_revision_id = (
                await target.execute(
                    sa.text(
                        """
                        SELECT base_revision_id
                        FROM jobs
                        WHERE id = :source_job_id
                        """
                    ),
                    {"source_job_id": source_job_id},
                )
            ).scalar_one_or_none()

        assert base_revision_id is not None
        await _insert_cad_change_set_row(
            target,
            changeset_id=changeset_id,
            project_id=project_id,
            base_revision_id=base_revision_id,
        )

    await target.execute(
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
                canonical_entity_schema_version,
                changeset_id
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
                :canonical_entity_schema_version,
                :changeset_id
            )
            """
        ),
        {
            "id": revision_id,
            "project_id": project_id,
            "source_file_id": file_id,
            "extraction_profile_id": extraction_profile_id,
            "source_job_id": source_job_id,
            "adapter_run_output_id": adapter_run_output_id,
            "predecessor_revision_id": predecessor_revision_id,
            "revision_sequence": revision_sequence,
            "revision_kind": revision_kind,
            "canonical_entity_schema_version": canonical_entity_schema_version,
            "changeset_id": changeset_id,
        },
    )


async def _insert_generated_artifact_row(
    target: Any,
    *,
    artifact_id: uuid.UUID,
    project_id: uuid.UUID,
    source_file_id: uuid.UUID,
    job_id: uuid.UUID,
    drawing_revision_id: uuid.UUID | None,
    adapter_run_output_id: uuid.UUID | None,
    artifact_kind: str,
    name: str,
    artifact_format: str,
    media_type: str,
    size_bytes: int,
    checksum_sha256: str,
    generator_name: str,
    generator_version: str,
    generator_config_json: Mapping[str, object],
    storage_key: str,
    storage_uri: str,
    lineage_json: Mapping[str, object],
    changeset_id: uuid.UUID | None = None,
    quantity_takeoff_id: uuid.UUID | None = None,
    estimate_version_id: uuid.UUID | None = None,
    predecessor_artifact_id: uuid.UUID | None = None,
) -> None:
    """Insert a generated artifact row for raw SQL lineage setup."""

    await target.execute(
        sa.text(
            """
            INSERT INTO generated_artifacts (
                id,
                project_id,
                source_file_id,
                job_id,
                drawing_revision_id,
                changeset_id,
                quantity_takeoff_id,
                estimate_version_id,
                adapter_run_output_id,
                artifact_kind,
                name,
                format,
                media_type,
                size_bytes,
                checksum_sha256,
                generator_name,
                generator_version,
                generator_config_json,
                storage_key,
                storage_uri,
                lineage_json,
                predecessor_artifact_id,
                deleted_at,
                created_at
            ) VALUES (
                :id,
                :project_id,
                :source_file_id,
                :job_id,
                :drawing_revision_id,
                :changeset_id,
                :quantity_takeoff_id,
                :estimate_version_id,
                :adapter_run_output_id,
                :artifact_kind,
                :name,
                :format,
                :media_type,
                :size_bytes,
                :checksum_sha256,
                :generator_name,
                :generator_version,
                CAST(:generator_config_json AS json),
                :storage_key,
                :storage_uri,
                CAST(:lineage_json AS json),
                :predecessor_artifact_id,
                :deleted_at,
                :created_at
            )
            """
        ),
        {
            "id": artifact_id,
            "project_id": project_id,
            "source_file_id": source_file_id,
            "job_id": job_id,
            "drawing_revision_id": drawing_revision_id,
            "changeset_id": changeset_id,
            "quantity_takeoff_id": quantity_takeoff_id,
            "estimate_version_id": estimate_version_id,
            "adapter_run_output_id": adapter_run_output_id,
            "artifact_kind": artifact_kind,
            "name": name,
            "format": artifact_format,
            "media_type": media_type,
            "size_bytes": size_bytes,
            "checksum_sha256": checksum_sha256,
            "generator_name": generator_name,
            "generator_version": generator_version,
            "generator_config_json": dumps(generator_config_json, separators=(",", ":")),
            "storage_key": storage_key,
            "storage_uri": storage_uri,
            "lineage_json": dumps(lineage_json, separators=(",", ":")),
            "predecessor_artifact_id": predecessor_artifact_id,
            "deleted_at": None,
            "created_at": datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
        },
    )


async def _insert_changeset_materialization_rows(
    target: Any,
    *,
    project_id: uuid.UUID,
    file_id: uuid.UUID,
    source_job_id: uuid.UUID,
    drawing_revision_id: uuid.UUID,
    extraction_profile_id: uuid.UUID | None,
    adapter_run_output_id: uuid.UUID | None,
) -> _ChangesetMaterializationIds:
    """Insert one null-origin materialization row per protected revision table."""

    manifest_id = uuid.uuid4()
    layout_id = uuid.uuid4()
    layer_id = uuid.uuid4()
    block_id = uuid.uuid4()
    entity_id = uuid.uuid4()

    await target.execute(
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
            "source_job_id": source_job_id,
            "drawing_revision_id": drawing_revision_id,
            "adapter_run_output_id": adapter_run_output_id,
            "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            "counts_json": dumps(
                {"layouts": 1, "layers": 1, "blocks": 1, "entities": 1},
                separators=(",", ":"),
            ),
        },
    )
    await target.execute(
        sa.text(
            """
            INSERT INTO revision_layouts (
                id,
                project_id,
                source_file_id,
                extraction_profile_id,
                source_job_id,
                drawing_revision_id,
                adapter_run_output_id,
                canonical_entity_schema_version,
                sequence_index,
                payload_json,
                layout_ref
            ) VALUES (
                :id,
                :project_id,
                :source_file_id,
                :extraction_profile_id,
                :source_job_id,
                :drawing_revision_id,
                :adapter_run_output_id,
                :canonical_entity_schema_version,
                :sequence_index,
                CAST(:payload_json AS json),
                :layout_ref
            )
            """
        ),
        {
            "id": layout_id,
            "project_id": project_id,
            "source_file_id": file_id,
            "extraction_profile_id": extraction_profile_id,
            "source_job_id": source_job_id,
            "drawing_revision_id": drawing_revision_id,
            "adapter_run_output_id": adapter_run_output_id,
            "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            "sequence_index": 0,
            "payload_json": dumps(
                {"layout_ref": "Changeset", "name": "Changeset"},
                separators=(",", ":"),
            ),
            "layout_ref": "Changeset",
        },
    )
    await target.execute(
        sa.text(
            """
            INSERT INTO revision_layers (
                id,
                project_id,
                source_file_id,
                extraction_profile_id,
                source_job_id,
                drawing_revision_id,
                adapter_run_output_id,
                canonical_entity_schema_version,
                sequence_index,
                payload_json,
                layer_ref
            ) VALUES (
                :id,
                :project_id,
                :source_file_id,
                :extraction_profile_id,
                :source_job_id,
                :drawing_revision_id,
                :adapter_run_output_id,
                :canonical_entity_schema_version,
                :sequence_index,
                CAST(:payload_json AS json),
                :layer_ref
            )
            """
        ),
        {
            "id": layer_id,
            "project_id": project_id,
            "source_file_id": file_id,
            "extraction_profile_id": extraction_profile_id,
            "source_job_id": source_job_id,
            "drawing_revision_id": drawing_revision_id,
            "adapter_run_output_id": adapter_run_output_id,
            "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            "sequence_index": 0,
            "payload_json": dumps(
                {"layer_ref": "A-CHG", "name": "A-CHG"},
                separators=(",", ":"),
            ),
            "layer_ref": "A-CHG",
        },
    )
    await target.execute(
        sa.text(
            """
            INSERT INTO revision_blocks (
                id,
                project_id,
                source_file_id,
                extraction_profile_id,
                source_job_id,
                drawing_revision_id,
                adapter_run_output_id,
                canonical_entity_schema_version,
                sequence_index,
                payload_json,
                block_ref
            ) VALUES (
                :id,
                :project_id,
                :source_file_id,
                :extraction_profile_id,
                :source_job_id,
                :drawing_revision_id,
                :adapter_run_output_id,
                :canonical_entity_schema_version,
                :sequence_index,
                CAST(:payload_json AS json),
                :block_ref
            )
            """
        ),
        {
            "id": block_id,
            "project_id": project_id,
            "source_file_id": file_id,
            "extraction_profile_id": extraction_profile_id,
            "source_job_id": source_job_id,
            "drawing_revision_id": drawing_revision_id,
            "adapter_run_output_id": adapter_run_output_id,
            "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            "sequence_index": 0,
            "payload_json": dumps(
                {"block_ref": "CHG-BLOCK", "name": "CHG-BLOCK"},
                separators=(",", ":"),
            ),
            "block_ref": "CHG-BLOCK",
        },
    )
    await target.execute(
        sa.text(
            """
            INSERT INTO revision_entities (
                id,
                project_id,
                source_file_id,
                extraction_profile_id,
                source_job_id,
                drawing_revision_id,
                adapter_run_output_id,
                canonical_entity_schema_version,
                sequence_index,
                entity_id,
                entity_type,
                entity_schema_version,
                parent_entity_ref,
                confidence_json,
                geometry_json,
                properties_json,
                provenance_json,
                canonical_entity_json,
                layout_ref,
                layer_ref,
                block_ref,
                source_identity,
                source_hash,
                layout_id,
                layer_id,
                block_id,
                parent_entity_row_id
            ) VALUES (
                :id,
                :project_id,
                :source_file_id,
                :extraction_profile_id,
                :source_job_id,
                :drawing_revision_id,
                :adapter_run_output_id,
                :canonical_entity_schema_version,
                :sequence_index,
                :entity_id,
                :entity_type,
                :entity_schema_version,
                :parent_entity_ref,
                CAST(:confidence_json AS json),
                CAST(:geometry_json AS json),
                CAST(:properties_json AS json),
                CAST(:provenance_json AS json),
                CAST(:canonical_entity_json AS json),
                :layout_ref,
                :layer_ref,
                :block_ref,
                :source_identity,
                :source_hash,
                :layout_id,
                :layer_id,
                :block_id,
                :parent_entity_row_id
            )
            """
        ),
        {
            "id": entity_id,
            "project_id": project_id,
            "source_file_id": file_id,
            "extraction_profile_id": extraction_profile_id,
            "source_job_id": source_job_id,
            "drawing_revision_id": drawing_revision_id,
            "adapter_run_output_id": adapter_run_output_id,
            "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            "sequence_index": 0,
            "entity_id": "changeset-entity-001",
            "entity_type": "line",
            "entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            "parent_entity_ref": None,
            "confidence_json": dumps(
                {"score": _FAKE_RUNNER_CONFIDENCE_SCORE},
                separators=(",", ":"),
            ),
            "geometry_json": dumps(
                {"type": "line", "coordinates": [[0.0, 0.0], [1.0, 1.0]]},
                separators=(",", ":"),
            ),
            "properties_json": dumps({"kind": "changeset"}, separators=(",", ":")),
            "provenance_json": dumps(
                {"origin": "user_created", "notes": ["changeset"]},
                separators=(",", ":"),
            ),
            "canonical_entity_json": dumps(
                {
                    "entity_id": "changeset-entity-001",
                    "entity_type": "line",
                    "entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                    "layout_ref": "Changeset",
                    "layer_ref": "A-CHG",
                    "block_ref": "CHG-BLOCK",
                    "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                    "confidence_json": {"score": _FAKE_RUNNER_CONFIDENCE_SCORE},
                    "geometry_json": {
                        "type": "line",
                        "coordinates": [[0.0, 0.0], [1.0, 1.0]],
                    },
                    "properties_json": {"kind": "changeset"},
                    "provenance_json": {
                        "origin": "user_created",
                        "notes": ["changeset"],
                    },
                },
                separators=(",", ":"),
            ),
            "layout_ref": "Changeset",
            "layer_ref": "A-CHG",
            "block_ref": "CHG-BLOCK",
            "source_identity": None,
            "source_hash": None,
            "layout_id": layout_id,
            "layer_id": layer_id,
            "block_id": block_id,
            "parent_entity_row_id": None,
        },
    )

    return _ChangesetMaterializationIds(
        manifest_id=manifest_id,
        layout_id=layout_id,
        layer_id=layer_id,
        block_id=block_id,
        entity_id=entity_id,
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


async def _insert_changeset_origin_rows(
    database_url: str,
    *,
    include_materialization: bool,
    keep_drawing_revision_compatible: bool = False,
) -> None:
    """Insert #271 origin rows for downgrade validation."""

    engine = create_async_engine(database_url)
    project_id = uuid.uuid4()
    file_id = uuid.uuid4()
    extraction_profile_id = uuid.uuid4()
    ingest_job_id = uuid.uuid4()
    adapter_run_output_id = uuid.uuid4()
    base_revision_id = uuid.uuid4()
    changeset_job_id = uuid.uuid4()
    changeset_revision_id = uuid.uuid4()
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
                    "name": "changeset downgrade guard",
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
                    "original_filename": "changeset-plan.pdf",
                    "media_type": "application/pdf",
                    "detected_format": "pdf",
                    "storage_uri": "file:///tmp/changeset-plan.pdf",
                    "size_bytes": 1,
                    "checksum_sha256": "a" * 64,
                    "immutable": True,
                    "initial_job_id": ingest_job_id,
                    "initial_extraction_profile_id": extraction_profile_id,
                },
            )
            await _insert_job_row(
                connection,
                job_id=ingest_job_id,
                project_id=project_id,
                file_id=file_id,
                extraction_profile_id=extraction_profile_id,
                base_revision_id=None,
                job_type="ingest",
                status="succeeded",
                enqueue_status="published",
                enqueue_attempts=1,
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
                    "warnings_json": "[]",
                    "diagnostics_json": '{"adapter":"tests","diagnostics":[]}',
                    "result_checksum_sha256": "b" * 64,
                },
            )
            await _insert_drawing_revision_row(
                connection,
                revision_id=base_revision_id,
                project_id=project_id,
                file_id=file_id,
                extraction_profile_id=extraction_profile_id,
                source_job_id=ingest_job_id,
                adapter_run_output_id=adapter_run_output_id,
                predecessor_revision_id=None,
                revision_sequence=1,
                revision_kind="ingest",
                canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                changeset_id=None,
            )
            if keep_drawing_revision_compatible:
                if include_materialization:
                    await _insert_changeset_materialization_rows(
                        connection,
                        project_id=project_id,
                        file_id=file_id,
                        source_job_id=ingest_job_id,
                        drawing_revision_id=base_revision_id,
                        extraction_profile_id=None,
                        adapter_run_output_id=None,
                    )
            else:
                await _insert_job_row(
                    connection,
                    job_id=changeset_job_id,
                    project_id=project_id,
                    file_id=file_id,
                    extraction_profile_id=None,
                    base_revision_id=base_revision_id,
                    job_type="export",
                    status="pending",
                    enqueue_status="pending",
                    enqueue_attempts=0,
                )
                await _insert_drawing_revision_row(
                    connection,
                    revision_id=changeset_revision_id,
                    project_id=project_id,
                    file_id=file_id,
                    extraction_profile_id=None,
                    source_job_id=changeset_job_id,
                    adapter_run_output_id=None,
                    predecessor_revision_id=base_revision_id,
                    revision_sequence=2,
                    revision_kind="changeset",
                    canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                    changeset_id=uuid.uuid4(),
                )
                if include_materialization:
                    await _insert_changeset_materialization_rows(
                        connection,
                        project_id=project_id,
                        file_id=file_id,
                        source_job_id=changeset_job_id,
                        drawing_revision_id=changeset_revision_id,
                        extraction_profile_id=None,
                        adapter_run_output_id=None,
                    )
    finally:
        await engine.dispose()


async def _insert_changeset_persistence_rows(database_url: str) -> None:
    """Insert populated #286 changeset persistence rows for downgrade validation."""

    await _insert_changeset_origin_rows(
        database_url,
        include_materialization=False,
        keep_drawing_revision_compatible=False,
    )

    engine = create_async_engine(database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    try:
        async with session_factory() as session:
            change_set_row = (
                await session.execute(sa.text("SELECT id, project_id FROM cad_change_sets LIMIT 1"))
            ).one()

            session.add(
                CadChangeOperation(
                    id=uuid.uuid4(),
                    project_id=change_set_row.project_id,
                    change_set_id=change_set_row.id,
                    sequence_index=1,
                    operation_type="flag_for_review",
                    expected_source_hash="a" * 64,
                    operation_json={"source": "downgrade-guard"},
                )
            )
            session.add(
                CadChangeSetValidationResult(
                    id=uuid.uuid4(),
                    project_id=change_set_row.project_id,
                    change_set_id=change_set_row.id,
                    validation_status="valid",
                    validator_name="tests",
                    validator_version="1",
                    result_json={"source": "downgrade-guard", "status": "valid"},
                )
            )
            await session.commit()
    finally:
        await engine.dispose()


async def _insert_changeset_apply_contract_rows(database_url: str) -> None:
    """Insert populated #290 changeset-apply contract rows for downgrade validation."""

    await _insert_changeset_origin_rows(
        database_url,
        include_materialization=False,
        keep_drawing_revision_compatible=False,
    )

    engine = create_async_engine(database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    try:
        async with session_factory() as session:
            change_set_row = (
                await session.execute(
                    sa.text(
                        """
                        SELECT
                            c.id AS change_set_id,
                            c.project_id AS project_id,
                            c.base_revision_id AS base_revision_id,
                            r.source_file_id AS source_file_id
                        FROM cad_change_sets AS c
                        JOIN drawing_revisions AS r
                          ON r.id = c.base_revision_id
                         AND r.project_id = c.project_id
                        LIMIT 1
                        """
                    )
                )
            ).one()

            validation_result = CadChangeSetValidationResult(
                id=uuid.uuid4(),
                project_id=change_set_row.project_id,
                change_set_id=change_set_row.change_set_id,
                validation_status="valid",
                validator_name="tests",
                validator_version="1",
                result_json={"source": "downgrade-guard", "status": "valid"},
            )
            session.add(validation_result)
            await session.flush()

            changeset_apply_job_id = uuid.uuid4()
            await _insert_job_row(
                session,
                job_id=changeset_apply_job_id,
                project_id=change_set_row.project_id,
                file_id=change_set_row.source_file_id,
                extraction_profile_id=None,
                base_revision_id=change_set_row.base_revision_id,
                job_type=JobType.CHANGESET_APPLY.value,
                status=JobStatus.PENDING.value,
                enqueue_status="pending",
                enqueue_attempts=0,
            )
            session.add(
                ChangeSetApplyJobInput(
                    source_job_id=changeset_apply_job_id,
                    project_id=change_set_row.project_id,
                    source_file_id=change_set_row.source_file_id,
                    drawing_revision_id=change_set_row.base_revision_id,
                    change_set_id=change_set_row.change_set_id,
                    source_job_type=JobType.CHANGESET_APPLY.value,
                    latest_validation_result_id=validation_result.id,
                    latest_validation_status=validation_result.validation_status,
                )
            )
            await session.commit()
    finally:
        await engine.dispose()


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
                        canonical_entity_schema_version
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
                        :canonical_entity_schema_version
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
                    "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
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
                        canonical_entity_schema_version
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
                        :canonical_entity_schema_version
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
                    "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
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
                        validation_status
                    ) VALUES (
                        :id,
                        :project_id,
                        :source_file_id,
                        :drawing_revision_id,
                        :source_job_id,
                        :source_job_type,
                        :validation_status
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
                    "validation_status": "valid",
                },
            )
    finally:
        await engine.dispose()


async def _insert_generated_artifact_lineage_anchor_rows(database_url: str) -> None:
    """Insert generated artifacts with populated #273 typed lineage anchors."""

    engine = create_async_engine(database_url)
    project_id = uuid.uuid4()
    file_id = uuid.uuid4()
    extraction_profile_id = uuid.uuid4()
    ingest_job_id = uuid.uuid4()
    adapter_run_output_id = uuid.uuid4()
    base_revision_id = uuid.uuid4()
    quantity_job_id = uuid.uuid4()
    quantity_takeoff_id = uuid.uuid4()
    estimate_job_id = uuid.uuid4()
    estimate_version_id = uuid.uuid4()
    quantity_export_job_id = uuid.uuid4()
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
                    "name": "generated artifact lineage downgrade guard",
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
                    "original_filename": "generated-artifact-lineage.pdf",
                    "media_type": "application/pdf",
                    "detected_format": "pdf",
                    "storage_uri": "file:///tmp/generated-artifact-lineage.pdf",
                    "size_bytes": 1,
                    "checksum_sha256": "a" * 64,
                    "immutable": True,
                    "initial_job_id": ingest_job_id,
                    "initial_extraction_profile_id": extraction_profile_id,
                },
            )
            await _insert_job_row(
                connection,
                job_id=ingest_job_id,
                project_id=project_id,
                file_id=file_id,
                extraction_profile_id=extraction_profile_id,
                base_revision_id=None,
                job_type="ingest",
                status="succeeded",
                enqueue_status="published",
                enqueue_attempts=1,
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
                    "warnings_json": "[]",
                    "diagnostics_json": '{"adapter":"tests","diagnostics":[]}',
                    "result_checksum_sha256": "b" * 64,
                },
            )
            await _insert_drawing_revision_row(
                connection,
                revision_id=base_revision_id,
                project_id=project_id,
                file_id=file_id,
                extraction_profile_id=extraction_profile_id,
                source_job_id=ingest_job_id,
                adapter_run_output_id=adapter_run_output_id,
                predecessor_revision_id=None,
                revision_sequence=1,
                revision_kind="ingest",
                canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                changeset_id=None,
            )
            await _insert_job_row(
                connection,
                job_id=quantity_job_id,
                project_id=project_id,
                file_id=file_id,
                extraction_profile_id=None,
                base_revision_id=base_revision_id,
                job_type="quantity_takeoff",
                status="succeeded",
                enqueue_status="published",
                enqueue_attempts=1,
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
                        validation_status
                    ) VALUES (
                        :id,
                        :project_id,
                        :source_file_id,
                        :drawing_revision_id,
                        :source_job_id,
                        :source_job_type,
                        :validation_status
                    )
                    """
                ),
                {
                    "id": quantity_takeoff_id,
                    "project_id": project_id,
                    "source_file_id": file_id,
                    "drawing_revision_id": base_revision_id,
                    "source_job_id": quantity_job_id,
                    "source_job_type": "quantity_takeoff",
                    "validation_status": "valid",
                },
            )
            await _insert_job_row(
                connection,
                job_id=estimate_job_id,
                project_id=project_id,
                file_id=file_id,
                extraction_profile_id=None,
                base_revision_id=base_revision_id,
                job_type="estimate",
                status="succeeded",
                enqueue_status="published",
                enqueue_attempts=1,
            )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO estimate_versions (
                        id,
                        project_id,
                        source_file_id,
                        drawing_revision_id,
                        quantity_takeoff_id,
                        source_job_id,
                        currency,
                        subtotal_amount,
                        tax_amount,
                        total_amount,
                        created_at
                    ) VALUES (
                        :id,
                        :project_id,
                        :source_file_id,
                        :drawing_revision_id,
                        :quantity_takeoff_id,
                        :source_job_id,
                        :currency,
                        :subtotal_amount,
                        :tax_amount,
                        :total_amount,
                        :created_at
                    )
                    """
                ),
                {
                    "id": estimate_version_id,
                    "project_id": project_id,
                    "source_file_id": file_id,
                    "drawing_revision_id": base_revision_id,
                    "quantity_takeoff_id": quantity_takeoff_id,
                    "source_job_id": estimate_job_id,
                    "currency": "GBP",
                    "subtotal_amount": Decimal("10.00"),
                    "tax_amount": Decimal("0.00"),
                    "total_amount": Decimal("10.00"),
                    "created_at": datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
                },
            )
            await _insert_job_row(
                connection,
                job_id=quantity_export_job_id,
                project_id=project_id,
                file_id=file_id,
                extraction_profile_id=None,
                base_revision_id=base_revision_id,
                job_type="export",
                status="succeeded",
                enqueue_status="published",
                enqueue_attempts=1,
            )
            await _insert_generated_artifact_row(
                connection,
                artifact_id=uuid.uuid4(),
                project_id=project_id,
                source_file_id=file_id,
                job_id=quantity_export_job_id,
                drawing_revision_id=base_revision_id,
                changeset_id=None,
                quantity_takeoff_id=quantity_takeoff_id,
                estimate_version_id=estimate_version_id,
                adapter_run_output_id=None,
                artifact_kind="estimate_pdf",
                name="estimate.pdf",
                artifact_format="pdf",
                media_type="application/pdf",
                size_bytes=128,
                checksum_sha256="c" * 64,
                generator_name="tests",
                generator_version="1",
                generator_config_json={"kind": "estimate_pdf"},
                storage_key="generated/estimate.pdf",
                storage_uri="file:///tmp/generated/estimate.pdf",
                lineage_json={"kind": "estimate_pdf"},
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
            ("changeset_apply_job_inputs", "source_job_type", "export"),
            ("drawing_revisions", "revision_kind", "reprocess"),
            ("validation_reports", "validator_version", "mutated"),
            ("generated_artifacts", "name", "mutated-debug-overlay.svg"),
            ("job_events", "message", "mutated job event"),
            ("estimate_versions", "total_amount", "121.00"),
            ("export_job_inputs", "export_kind", "quantity_csv"),
            ("estimate_job_inputs", "currency", "USD"),
            (
                "estimate_job_input_catalog_refs",
                "catalog_checksum_sha256",
                "e" * 64,
            ),
            ("estimate_snapshot_entries", "entry_label", "mutated assumption"),
            ("estimate_items", "description", "mutated estimate item"),
            ("quantity_takeoffs", "source_job_type", "estimate"),
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
            row_filter, row_params = _row_filter_for_table(row_ids, table_name)
            await _run_sql_and_expect_append_only_failure(
                (
                    f'UPDATE "{table_name}" SET "{column_name}" = :replacement_value '
                    f"WHERE {row_filter}"
                ),
                row_params
                | {
                    "replacement_value": replacement_value,
                },
                operation="UPDATE",
                table_name=table_name,
            )

    async def test_changeset_append_only_update_boundaries(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Changeset tables should allow only the documented append-only status updates."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        row_ids = await _seed_protected_rows(async_client)
        updated_at = datetime(2026, 2, 3, 4, 5, 6, tzinfo=UTC)

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            await session.execute(
                sa.text(
                    'UPDATE "cad_change_sets" '
                    "SET status = :status, updated_at = :updated_at "
                    "WHERE id = :row_id"
                ),
                {
                    "status": "validation_requested",
                    "updated_at": updated_at,
                    "row_id": row_ids.cad_change_set_id,
                },
            )
            await session.commit()

        async with session_maker() as session:
            change_set_row = (
                await session.execute(
                    sa.text(
                        "SELECT status, updated_at, created_by "
                        'FROM "cad_change_sets" WHERE id = :row_id'
                    ),
                    {"row_id": row_ids.cad_change_set_id},
                )
            ).one()

        assert change_set_row.status == "validation_requested"
        assert change_set_row.updated_at == updated_at
        assert change_set_row.created_by is None

        await _run_sql_and_expect_append_only_failure(
            'UPDATE "cad_change_sets" SET created_by = :replacement_value WHERE id = :row_id',
            {
                "replacement_value": "mutated",
                "row_id": row_ids.cad_change_set_id,
            },
            operation="UPDATE",
            table_name="cad_change_sets",
        )
        await _run_sql_and_expect_append_only_failure(
            (
                'UPDATE "cad_change_operations" '
                "SET operation_json = CAST(:replacement_value AS json) WHERE id = :row_id"
            ),
            {
                "replacement_value": '{"source":"mutated"}',
                "row_id": row_ids.cad_change_operation_id,
            },
            operation="UPDATE",
            table_name="cad_change_operations",
        )
        await _run_sql_and_expect_append_only_failure(
            (
                'UPDATE "cad_change_operations" '
                "SET expected_source_hash = :replacement_value WHERE id = :row_id"
            ),
            {
                "replacement_value": "f" * 64,
                "row_id": row_ids.cad_change_operation_id,
            },
            operation="UPDATE",
            table_name="cad_change_operations",
        )
        await _run_sql_and_expect_append_only_failure(
            (
                'UPDATE "cad_change_set_validation_results" '
                "SET result_json = CAST(:replacement_value AS json) WHERE id = :row_id"
            ),
            {
                "replacement_value": '{"source":"mutated","status":"invalid"}',
                "row_id": row_ids.cad_change_set_validation_result_id,
            },
            operation="UPDATE",
            table_name="cad_change_set_validation_results",
        )
        await _run_sql_and_expect_append_only_failure(
            (
                'UPDATE "cad_change_set_validation_results" '
                "SET validation_status = :replacement_value WHERE id = :row_id"
            ),
            {
                "replacement_value": "invalid",
                "row_id": row_ids.cad_change_set_validation_result_id,
            },
            operation="UPDATE",
            table_name="cad_change_set_validation_results",
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
                        "provenance_json": {"source_id": "entity-source-append-only-001"},
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
                "SET canonical_json = CAST(:replacement_value AS json) "
                "WHERE id = :row_id"
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
                "SET canonical_entity_json = CAST(:replacement_value AS json) "
                "WHERE id = :row_id"
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
            row_filter, row_params = _row_filter_for_table(row_ids, table_name)
            await _run_sql_and_expect_append_only_failure(
                f'DELETE FROM "{table_name}" WHERE {row_filter}',
                row_params,
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

    async def test_changeset_revisions_accept_null_origin_materialization_rows(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Valid changeset revisions may persist null origin fields across materialization rows."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        row_ids = await _seed_protected_rows(async_client)
        source_job_id = uuid.uuid4()
        revision_id = uuid.uuid4()
        changeset_id = uuid.uuid4()

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            await _insert_job_row(
                session,
                job_id=source_job_id,
                project_id=row_ids.project_id,
                file_id=row_ids.file_id,
                extraction_profile_id=None,
                base_revision_id=row_ids.drawing_revision_id,
                job_type="export",
                status="pending",
                enqueue_status="pending",
                enqueue_attempts=0,
            )
            await _insert_drawing_revision_row(
                session,
                revision_id=revision_id,
                project_id=row_ids.project_id,
                file_id=row_ids.file_id,
                extraction_profile_id=None,
                source_job_id=source_job_id,
                adapter_run_output_id=None,
                predecessor_revision_id=row_ids.drawing_revision_id,
                revision_sequence=2,
                revision_kind="changeset",
                canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                changeset_id=changeset_id,
            )
            materialization_ids = await _insert_changeset_materialization_rows(
                session,
                project_id=row_ids.project_id,
                file_id=row_ids.file_id,
                source_job_id=source_job_id,
                drawing_revision_id=revision_id,
                extraction_profile_id=None,
                adapter_run_output_id=None,
            )
            await session.commit()

            revision_row = (
                await session.execute(
                    sa.text(
                        """
                        SELECT extraction_profile_id, adapter_run_output_id, changeset_id
                        FROM drawing_revisions
                        WHERE id = :revision_id
                        """
                    ),
                    {"revision_id": revision_id},
                )
            ).one()
            assert revision_row.extraction_profile_id is None
            assert revision_row.adapter_run_output_id is None
            assert revision_row.changeset_id == changeset_id

            materialization_cases = (
                ("revision_entity_manifests", "manifest_id"),
                ("revision_layouts", "layout_id"),
                ("revision_layers", "layer_id"),
                ("revision_blocks", "block_id"),
                ("revision_entities", "entity_id"),
            )
            materialization_id_map = {
                "manifest_id": materialization_ids.manifest_id,
                "layout_id": materialization_ids.layout_id,
                "layer_id": materialization_ids.layer_id,
                "block_id": materialization_ids.block_id,
                "entity_id": materialization_ids.entity_id,
            }

            for table_name, id_key in materialization_cases:
                row = (
                    await session.execute(
                        sa.text(
                            f"""
                            SELECT extraction_profile_id, adapter_run_output_id, drawing_revision_id
                            FROM {table_name}
                            WHERE id = :row_id
                            """
                        ),
                        {"row_id": materialization_id_map[id_key]},
                    )
                ).one()
                assert row.extraction_profile_id is None
                assert row.adapter_run_output_id is None
                assert row.drawing_revision_id == revision_id

    async def test_changeset_revision_changeset_id_is_unique_when_non_null(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Changeset revisions should reject duplicate non-null changeset ids."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        row_ids = await _seed_protected_rows(async_client)
        duplicate_changeset_id = uuid.uuid4()
        first_source_job_id = uuid.uuid4()
        first_revision_id = uuid.uuid4()
        second_source_job_id = uuid.uuid4()

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            await _insert_job_row(
                session,
                job_id=first_source_job_id,
                project_id=row_ids.project_id,
                file_id=row_ids.file_id,
                extraction_profile_id=None,
                base_revision_id=row_ids.drawing_revision_id,
                job_type="export",
                status="pending",
                enqueue_status="pending",
                enqueue_attempts=0,
            )
            await _insert_drawing_revision_row(
                session,
                revision_id=first_revision_id,
                project_id=row_ids.project_id,
                file_id=row_ids.file_id,
                extraction_profile_id=None,
                source_job_id=first_source_job_id,
                adapter_run_output_id=None,
                predecessor_revision_id=row_ids.drawing_revision_id,
                revision_sequence=2,
                revision_kind="changeset",
                canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                changeset_id=duplicate_changeset_id,
            )
            await session.commit()

            await _insert_job_row(
                session,
                job_id=second_source_job_id,
                project_id=row_ids.project_id,
                file_id=row_ids.file_id,
                extraction_profile_id=None,
                base_revision_id=first_revision_id,
                job_type="export",
                status="pending",
                enqueue_status="pending",
                enqueue_attempts=0,
            )

            with pytest.raises(DBAPIError) as exc_info:
                await _insert_drawing_revision_row(
                    session,
                    revision_id=uuid.uuid4(),
                    project_id=row_ids.project_id,
                    file_id=row_ids.file_id,
                    extraction_profile_id=None,
                    source_job_id=second_source_job_id,
                    adapter_run_output_id=None,
                    predecessor_revision_id=first_revision_id,
                    revision_sequence=3,
                    revision_kind="changeset",
                    canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                    changeset_id=duplicate_changeset_id,
                )
                await session.commit()

            await session.rollback()
            assert _extract_sqlstate(exc_info.value) == _UNIQUE_VIOLATION_SQLSTATE
            assert "uq_drawing_revisions_changeset_id" in str(exc_info.value)

    async def test_changeset_revision_origin_constraints_reject_invalid_combinations(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Invalid #271 origin combinations should fail the drawing revision check constraint."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        row_ids = await _seed_protected_rows(async_client)
        invalid_cases: tuple[
            tuple[str, uuid.UUID | None, uuid.UUID | None, uuid.UUID | None, uuid.UUID | None],
            ...,
        ] = (
            ("changeset", None, None, None, uuid.uuid4()),
            (
                "changeset",
                row_ids.drawing_revision_id,
                row_ids.extraction_profile_id,
                None,
                uuid.uuid4(),
            ),
            (
                "changeset",
                row_ids.drawing_revision_id,
                None,
                row_ids.adapter_run_output_id,
                uuid.uuid4(),
            ),
            ("changeset", row_ids.drawing_revision_id, None, None, None),
            ("ingest", None, None, row_ids.adapter_run_output_id, None),
            ("ingest", None, row_ids.extraction_profile_id, None, None),
            ("ingest", None, None, None, None),
            (
                "reprocess",
                row_ids.drawing_revision_id,
                None,
                row_ids.adapter_run_output_id,
                None,
            ),
            (
                "reprocess",
                row_ids.drawing_revision_id,
                row_ids.extraction_profile_id,
                None,
                None,
            ),
            ("reprocess", row_ids.drawing_revision_id, None, None, None),
        )

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            for (
                revision_sequence,
                (
                    revision_kind,
                    predecessor_revision_id,
                    extraction_profile_id,
                    adapter_run_output_id,
                    changeset_id,
                ),
            ) in enumerate(invalid_cases, start=2):
                source_job_id = uuid.uuid4()
                await _insert_job_row(
                    session,
                    job_id=source_job_id,
                    project_id=row_ids.project_id,
                    file_id=row_ids.file_id,
                    extraction_profile_id=None,
                    base_revision_id=row_ids.drawing_revision_id,
                    job_type="export",
                    status="pending",
                    enqueue_status="pending",
                    enqueue_attempts=0,
                )

                with pytest.raises(DBAPIError) as exc_info:
                    await _insert_drawing_revision_row(
                        session,
                        revision_id=uuid.uuid4(),
                        project_id=row_ids.project_id,
                        file_id=row_ids.file_id,
                        extraction_profile_id=extraction_profile_id,
                        source_job_id=source_job_id,
                        adapter_run_output_id=adapter_run_output_id,
                        predecessor_revision_id=predecessor_revision_id,
                        revision_sequence=revision_sequence,
                        revision_kind=revision_kind,
                        canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                        changeset_id=changeset_id,
                    )
                    await session.commit()

                await session.rollback()
                assert _extract_sqlstate(exc_info.value) == _CHECK_VIOLATION_SQLSTATE
                assert "ck_drawing_revisions_origin_fields" in str(exc_info.value)

    async def test_generated_artifacts_keep_new_anchor_columns_null(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Existing generated artifact rows remain valid with all #273 typed anchors unset."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        row_ids = await _seed_protected_rows(async_client)

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            artifact_row = (
                await session.execute(
                    sa.text(
                        """
                        SELECT changeset_id, quantity_takeoff_id, estimate_version_id
                        FROM generated_artifacts
                        WHERE id = :artifact_id
                        """
                    ),
                    {"artifact_id": row_ids.generated_artifact_id},
                )
            ).one()

        assert artifact_row.changeset_id is None
        assert artifact_row.quantity_takeoff_id is None
        assert artifact_row.estimate_version_id is None

    async def test_generated_artifacts_accept_valid_typed_lineage_anchors(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Generated artifacts should accept valid changeset, takeoff, and estimate anchors."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        row_ids = await _seed_protected_rows(async_client)
        changeset_job_id = uuid.uuid4()
        changeset_revision_id = uuid.uuid4()
        changeset_id = uuid.uuid4()
        changeset_artifact_job_id = uuid.uuid4()
        quantity_artifact_job_id = uuid.uuid4()
        estimate_artifact_job_id = uuid.uuid4()
        changeset_artifact_id = uuid.uuid4()
        quantity_artifact_id = uuid.uuid4()
        estimate_artifact_id = uuid.uuid4()

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            await _insert_job_row(
                session,
                job_id=changeset_job_id,
                project_id=row_ids.project_id,
                file_id=row_ids.file_id,
                extraction_profile_id=None,
                base_revision_id=row_ids.drawing_revision_id,
                job_type="export",
                status="succeeded",
                enqueue_status="published",
                enqueue_attempts=1,
            )
            await _insert_drawing_revision_row(
                session,
                revision_id=changeset_revision_id,
                project_id=row_ids.project_id,
                file_id=row_ids.file_id,
                extraction_profile_id=None,
                source_job_id=changeset_job_id,
                adapter_run_output_id=None,
                predecessor_revision_id=row_ids.drawing_revision_id,
                revision_sequence=2,
                revision_kind="changeset",
                canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                changeset_id=changeset_id,
            )
            for artifact_job_id, base_revision_id in (
                (changeset_artifact_job_id, changeset_revision_id),
                (quantity_artifact_job_id, row_ids.drawing_revision_id),
                (estimate_artifact_job_id, row_ids.drawing_revision_id),
            ):
                await _insert_job_row(
                    session,
                    job_id=artifact_job_id,
                    project_id=row_ids.project_id,
                    file_id=row_ids.file_id,
                    extraction_profile_id=None,
                    base_revision_id=base_revision_id,
                    job_type="export",
                    status="succeeded",
                    enqueue_status="published",
                    enqueue_attempts=1,
                )

            await _insert_generated_artifact_row(
                session,
                artifact_id=changeset_artifact_id,
                project_id=row_ids.project_id,
                source_file_id=row_ids.file_id,
                job_id=changeset_artifact_job_id,
                drawing_revision_id=changeset_revision_id,
                changeset_id=changeset_id,
                quantity_takeoff_id=None,
                estimate_version_id=None,
                adapter_run_output_id=None,
                artifact_kind="revised_dxf",
                name="changeset-revision.dxf",
                artifact_format="dxf",
                media_type="application/dxf",
                size_bytes=64,
                checksum_sha256="e" * 64,
                generator_name="tests",
                generator_version="1",
                generator_config_json={"kind": "changeset"},
                storage_key=f"generated/{changeset_artifact_id}.dxf",
                storage_uri=f"file:///tmp/{changeset_artifact_id}.dxf",
                lineage_json={"kind": "changeset"},
            )
            await _insert_generated_artifact_row(
                session,
                artifact_id=quantity_artifact_id,
                project_id=row_ids.project_id,
                source_file_id=row_ids.file_id,
                job_id=quantity_artifact_job_id,
                drawing_revision_id=row_ids.drawing_revision_id,
                changeset_id=None,
                quantity_takeoff_id=row_ids.quantity_takeoff_id,
                estimate_version_id=None,
                adapter_run_output_id=None,
                artifact_kind="quantity_csv",
                name="quantities.csv",
                artifact_format="csv",
                media_type="text/csv",
                size_bytes=96,
                checksum_sha256="f" * 64,
                generator_name="tests",
                generator_version="1",
                generator_config_json={"kind": "quantity"},
                storage_key=f"generated/{quantity_artifact_id}.csv",
                storage_uri=f"file:///tmp/{quantity_artifact_id}.csv",
                lineage_json={"kind": "quantity"},
            )
            await _insert_generated_artifact_row(
                session,
                artifact_id=estimate_artifact_id,
                project_id=row_ids.project_id,
                source_file_id=row_ids.file_id,
                job_id=estimate_artifact_job_id,
                drawing_revision_id=row_ids.drawing_revision_id,
                changeset_id=None,
                quantity_takeoff_id=row_ids.quantity_takeoff_id,
                estimate_version_id=row_ids.estimate_version_id,
                adapter_run_output_id=None,
                artifact_kind="estimate_pdf",
                name="estimate.pdf",
                artifact_format="pdf",
                media_type="application/pdf",
                size_bytes=128,
                checksum_sha256="1" * 64,
                generator_name="tests",
                generator_version="1",
                generator_config_json={"kind": "estimate"},
                storage_key=f"generated/{estimate_artifact_id}.pdf",
                storage_uri=f"file:///tmp/{estimate_artifact_id}.pdf",
                lineage_json={"kind": "estimate"},
            )
            await session.commit()

            changeset_row = (
                await session.execute(
                    sa.text(
                        """
                        SELECT
                            drawing_revision_id,
                            changeset_id,
                            quantity_takeoff_id,
                            estimate_version_id
                        FROM generated_artifacts
                        WHERE id = :artifact_id
                        """
                    ),
                    {"artifact_id": changeset_artifact_id},
                )
            ).one()
            quantity_row = (
                await session.execute(
                    sa.text(
                        """
                        SELECT
                            drawing_revision_id,
                            changeset_id,
                            quantity_takeoff_id,
                            estimate_version_id
                        FROM generated_artifacts
                        WHERE id = :artifact_id
                        """
                    ),
                    {"artifact_id": quantity_artifact_id},
                )
            ).one()
            estimate_row = (
                await session.execute(
                    sa.text(
                        """
                        SELECT
                            drawing_revision_id,
                            changeset_id,
                            quantity_takeoff_id,
                            estimate_version_id
                        FROM generated_artifacts
                        WHERE id = :artifact_id
                        """
                    ),
                    {"artifact_id": estimate_artifact_id},
                )
            ).one()

        assert changeset_row.drawing_revision_id == changeset_revision_id
        assert changeset_row.changeset_id == changeset_id
        assert changeset_row.quantity_takeoff_id is None
        assert changeset_row.estimate_version_id is None

        assert quantity_row.drawing_revision_id == row_ids.drawing_revision_id
        assert quantity_row.changeset_id is None
        assert quantity_row.quantity_takeoff_id == row_ids.quantity_takeoff_id
        assert quantity_row.estimate_version_id is None

        assert estimate_row.drawing_revision_id == row_ids.drawing_revision_id
        assert estimate_row.changeset_id is None
        assert estimate_row.quantity_takeoff_id == row_ids.quantity_takeoff_id
        assert estimate_row.estimate_version_id == row_ids.estimate_version_id

    async def test_generated_artifacts_reject_mismatched_typed_lineage_anchors(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Generated artifacts should reject mismatched changeset, takeoff, and estimate anchors."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        row_ids = await _seed_protected_rows(async_client)
        other_row_ids = await _seed_protected_rows(async_client)

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            changeset_job_id = uuid.uuid4()
            changeset_revision_id = uuid.uuid4()
            changeset_id = uuid.uuid4()
            other_changeset_job_id = uuid.uuid4()
            other_changeset_revision_id = uuid.uuid4()
            other_changeset_id = uuid.uuid4()
            changeset_mismatch_job_id = uuid.uuid4()

            await _insert_job_row(
                session,
                job_id=changeset_job_id,
                project_id=row_ids.project_id,
                file_id=row_ids.file_id,
                extraction_profile_id=None,
                base_revision_id=row_ids.drawing_revision_id,
                job_type="export",
                status="pending",
                enqueue_status="pending",
                enqueue_attempts=0,
            )
            await _insert_drawing_revision_row(
                session,
                revision_id=changeset_revision_id,
                project_id=row_ids.project_id,
                file_id=row_ids.file_id,
                extraction_profile_id=None,
                source_job_id=changeset_job_id,
                adapter_run_output_id=None,
                predecessor_revision_id=row_ids.drawing_revision_id,
                revision_sequence=2,
                revision_kind="changeset",
                canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                changeset_id=changeset_id,
            )
            await _insert_job_row(
                session,
                job_id=other_changeset_job_id,
                project_id=other_row_ids.project_id,
                file_id=other_row_ids.file_id,
                extraction_profile_id=None,
                base_revision_id=other_row_ids.drawing_revision_id,
                job_type="export",
                status="pending",
                enqueue_status="pending",
                enqueue_attempts=0,
            )
            await _insert_drawing_revision_row(
                session,
                revision_id=other_changeset_revision_id,
                project_id=other_row_ids.project_id,
                file_id=other_row_ids.file_id,
                extraction_profile_id=None,
                source_job_id=other_changeset_job_id,
                adapter_run_output_id=None,
                predecessor_revision_id=other_row_ids.drawing_revision_id,
                revision_sequence=2,
                revision_kind="changeset",
                canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                changeset_id=other_changeset_id,
            )
            await _insert_job_row(
                session,
                job_id=changeset_mismatch_job_id,
                project_id=row_ids.project_id,
                file_id=row_ids.file_id,
                extraction_profile_id=None,
                base_revision_id=changeset_revision_id,
                job_type="export",
                status="pending",
                enqueue_status="pending",
                enqueue_attempts=0,
            )

            with pytest.raises(DBAPIError) as changeset_exc_info:
                await _insert_generated_artifact_row(
                    session,
                    artifact_id=uuid.uuid4(),
                    project_id=row_ids.project_id,
                    source_file_id=row_ids.file_id,
                    job_id=changeset_mismatch_job_id,
                    drawing_revision_id=changeset_revision_id,
                    changeset_id=other_changeset_id,
                    quantity_takeoff_id=None,
                    estimate_version_id=None,
                    adapter_run_output_id=None,
                    artifact_kind="revised_dxf",
                    name="mismatched-changeset.dxf",
                    artifact_format="dxf",
                    media_type="application/dxf",
                    size_bytes=32,
                    checksum_sha256="1" * 64,
                    generator_name="tests",
                    generator_version="1",
                    generator_config_json={"kind": "changeset"},
                    storage_key="generated/mismatched-changeset.dxf",
                    storage_uri="file:///tmp/mismatched-changeset.dxf",
                    lineage_json={"kind": "changeset"},
                )
                await session.commit()

            await session.rollback()
            assert _extract_sqlstate(changeset_exc_info.value) == _FOREIGN_KEY_VIOLATION_SQLSTATE
            assert "fk_generated_artifacts_changeset" in str(changeset_exc_info.value)

            takeoff_mismatch_job_id = uuid.uuid4()
            await _insert_job_row(
                session,
                job_id=takeoff_mismatch_job_id,
                project_id=row_ids.project_id,
                file_id=row_ids.file_id,
                extraction_profile_id=None,
                base_revision_id=row_ids.drawing_revision_id,
                job_type="export",
                status="pending",
                enqueue_status="pending",
                enqueue_attempts=0,
            )

            with pytest.raises(DBAPIError) as takeoff_exc_info:
                await _insert_generated_artifact_row(
                    session,
                    artifact_id=uuid.uuid4(),
                    project_id=row_ids.project_id,
                    source_file_id=row_ids.file_id,
                    job_id=takeoff_mismatch_job_id,
                    drawing_revision_id=row_ids.drawing_revision_id,
                    changeset_id=None,
                    quantity_takeoff_id=other_row_ids.quantity_takeoff_id,
                    estimate_version_id=None,
                    adapter_run_output_id=None,
                    artifact_kind="quantity_csv",
                    name="mismatched-quantity.csv",
                    artifact_format="csv",
                    media_type="text/csv",
                    size_bytes=32,
                    checksum_sha256="2" * 64,
                    generator_name="tests",
                    generator_version="1",
                    generator_config_json={"kind": "quantity"},
                    storage_key="generated/mismatched-quantity.csv",
                    storage_uri="file:///tmp/mismatched-quantity.csv",
                    lineage_json={"kind": "quantity"},
                )
                await session.commit()

            await session.rollback()
            assert _extract_sqlstate(takeoff_exc_info.value) == _FOREIGN_KEY_VIOLATION_SQLSTATE
            assert "fk_generated_artifacts_takeoff" in str(takeoff_exc_info.value)

            estimate_mismatch_job_id = uuid.uuid4()
            await _insert_job_row(
                session,
                job_id=estimate_mismatch_job_id,
                project_id=row_ids.project_id,
                file_id=row_ids.file_id,
                extraction_profile_id=None,
                base_revision_id=row_ids.drawing_revision_id,
                job_type="export",
                status="pending",
                enqueue_status="pending",
                enqueue_attempts=0,
            )

            with pytest.raises(DBAPIError) as estimate_exc_info:
                await _insert_generated_artifact_row(
                    session,
                    artifact_id=uuid.uuid4(),
                    project_id=row_ids.project_id,
                    source_file_id=row_ids.file_id,
                    job_id=estimate_mismatch_job_id,
                    drawing_revision_id=row_ids.drawing_revision_id,
                    changeset_id=None,
                    quantity_takeoff_id=row_ids.quantity_takeoff_id,
                    estimate_version_id=other_row_ids.estimate_version_id,
                    adapter_run_output_id=None,
                    artifact_kind="estimate_pdf",
                    name="mismatched-estimate.pdf",
                    artifact_format="pdf",
                    media_type="application/pdf",
                    size_bytes=32,
                    checksum_sha256="3" * 64,
                    generator_name="tests",
                    generator_version="1",
                    generator_config_json={"kind": "estimate"},
                    storage_key="generated/mismatched-estimate.pdf",
                    storage_uri="file:///tmp/mismatched-estimate.pdf",
                    lineage_json={"kind": "estimate"},
                )
                await session.commit()

            await session.rollback()
            assert _extract_sqlstate(estimate_exc_info.value) == _FOREIGN_KEY_VIOLATION_SQLSTATE
            assert "fk_generated_artifacts_estimate" in str(estimate_exc_info.value)

    async def test_generated_artifacts_reject_incomplete_nullable_composite_bypass_attempts(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Generated artifacts should reject nullable composite anchor bypass attempts."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        row_ids = await _seed_protected_rows(async_client)
        invalid_cases = (
            (
                "quantity_takeoff_id without drawing_revision_id",
                None,
                row_ids.quantity_takeoff_id,
                None,
                "ck_generated_artifacts_takeoff_revision",
            ),
            (
                "estimate_version_id without quantity_takeoff_id",
                row_ids.drawing_revision_id,
                None,
                row_ids.estimate_version_id,
                "ck_generated_artifacts_estimate_lineage",
            ),
        )

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            for (
                _case_name,
                drawing_revision_id,
                quantity_takeoff_id,
                estimate_version_id,
                expected_constraint,
            ) in invalid_cases:
                artifact_job_id = uuid.uuid4()
                await _insert_job_row(
                    session,
                    job_id=artifact_job_id,
                    project_id=row_ids.project_id,
                    file_id=row_ids.file_id,
                    extraction_profile_id=None,
                    base_revision_id=row_ids.drawing_revision_id,
                    job_type="export",
                    status="pending",
                    enqueue_status="pending",
                    enqueue_attempts=0,
                )

                with pytest.raises(DBAPIError) as exc_info:
                    await _insert_generated_artifact_row(
                        session,
                        artifact_id=uuid.uuid4(),
                        project_id=row_ids.project_id,
                        source_file_id=row_ids.file_id,
                        job_id=artifact_job_id,
                        drawing_revision_id=drawing_revision_id,
                        changeset_id=None,
                        quantity_takeoff_id=quantity_takeoff_id,
                        estimate_version_id=estimate_version_id,
                        adapter_run_output_id=None,
                        artifact_kind="debug_overlay",
                        name="invalid-anchor.svg",
                        artifact_format="svg",
                        media_type="image/svg+xml",
                        size_bytes=16,
                        checksum_sha256="4" * 64,
                        generator_name="tests",
                        generator_version="1",
                        generator_config_json={"kind": "invalid"},
                        storage_key=f"generated/{uuid.uuid4()}.svg",
                        storage_uri="file:///tmp/invalid-anchor.svg",
                        lineage_json={"kind": "invalid"},
                    )
                    await session.commit()

                await session.rollback()
                assert _extract_sqlstate(exc_info.value) == _CHECK_VIOLATION_SQLSTATE
                assert expected_constraint in str(exc_info.value)

    @pytest.mark.parametrize(
        (
            "include_materialization",
            "keep_drawing_revision_compatible",
            "expected_table_name",
        ),
        [
            (True, True, "revision_entity_manifests"),
        ],
    )
    async def test_changeset_origin_downgrade_fails_closed_on_incompatible_rows(
        self,
        include_materialization: bool,
        keep_drawing_revision_compatible: bool,
        expected_table_name: str,
    ) -> None:
        """Downgrade past #271 should refuse incompatible origin rows introduced at head."""

        _ = self
        database_name, database_url = await _create_temp_database()

        try:
            upgrade_result = _run_alembic_command("upgrade", "head", database_url=database_url)
            assert upgrade_result.returncode == 0, upgrade_result.stdout + upgrade_result.stderr

            await _insert_changeset_origin_rows(
                database_url,
                include_materialization=include_materialization,
                keep_drawing_revision_compatible=keep_drawing_revision_compatible,
            )

            downgrade_result = _run_alembic_command(
                "downgrade",
                _CHANGESET_ORIGIN_DOWNGRADE_TARGET_REVISION,
                database_url=database_url,
            )
            # A populated database refuses to downgrade. Post Path B (#491) the
            # earliest blocking step is the NOT NULL reinstatement for the
            # re-added (now permanently NULL) confidence/gate columns, which
            # fails ahead of the older #271 origin guard.
            _ = expected_table_name
            assert downgrade_result.returncode != 0
        finally:
            await _drop_temp_database(database_name)

    async def test_generated_artifact_lineage_downgrade_fails_closed_on_anchored_rows(
        self,
    ) -> None:
        """Downgrade past #273 should refuse generated artifacts with typed lineage anchors."""

        _ = self
        database_name, database_url = await _create_temp_database()

        try:
            upgrade_result = _run_alembic_command("upgrade", "head", database_url=database_url)
            assert upgrade_result.returncode == 0, upgrade_result.stdout + upgrade_result.stderr

            await _insert_generated_artifact_lineage_anchor_rows(database_url)

            downgrade_result = _run_alembic_command(
                "downgrade",
                _GENERATED_ARTIFACT_LINEAGE_DOWNGRADE_TARGET_REVISION,
                database_url=database_url,
            )
            # A populated database refuses to downgrade. Post Path B (#491) the
            # earliest blocking step is the NOT NULL reinstatement for the
            # re-added (now permanently NULL) confidence/gate columns, which
            # fails ahead of the older #273 lineage-anchor guard.
            assert downgrade_result.returncode != 0
        finally:
            await _drop_temp_database(database_name)

    async def test_changeset_persistence_downgrade_fails_closed_on_populated_tables(
        self,
    ) -> None:
        """Downgrade past #286 should refuse populated changeset persistence tables."""

        _ = self
        database_name, database_url = await _create_temp_database()

        try:
            upgrade_result = _run_alembic_command("upgrade", "head", database_url=database_url)
            assert upgrade_result.returncode == 0, upgrade_result.stdout + upgrade_result.stderr

            await _insert_changeset_persistence_rows(database_url)

            downgrade_result = _run_alembic_command(
                "downgrade",
                _CHANGESET_PERSISTENCE_DOWNGRADE_TARGET_REVISION,
                database_url=database_url,
            )
            # A populated database refuses to downgrade. Post Path B (#491) the
            # earliest blocking step is the NOT NULL reinstatement for the
            # re-added (now permanently NULL) confidence/gate columns, which
            # fails ahead of the older #286 changeset-persistence guard.
            assert downgrade_result.returncode != 0
        finally:
            await _drop_temp_database(database_name)

    async def test_changeset_apply_job_input_downgrade_fails_closed_on_populated_contract(
        self,
    ) -> None:
        """Downgrade past #290 should refuse populated changeset-apply contract rows."""

        _ = self
        database_name, database_url = await _create_temp_database()

        try:
            upgrade_result = _run_alembic_command("upgrade", "head", database_url=database_url)
            assert upgrade_result.returncode == 0, upgrade_result.stdout + upgrade_result.stderr

            await _insert_changeset_apply_contract_rows(database_url)

            downgrade_result = _run_alembic_command(
                "downgrade",
                _CHANGESET_APPLY_JOB_INPUT_DOWNGRADE_TARGET_REVISION,
                database_url=database_url,
            )
            # A populated database refuses to downgrade. Post Path B (#491) the
            # earliest blocking step is the NOT NULL reinstatement for the
            # re-added (now permanently NULL) confidence/gate columns, which
            # fails ahead of the older #290 changeset-apply contract guard.
            assert downgrade_result.returncode != 0
        finally:
            await _drop_temp_database(database_name)

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
            # A populated database refuses to downgrade. Post Path B (#491) the
            # earliest blocking step is the NOT NULL reinstatement for the
            # re-added (now permanently NULL) confidence/gate columns, which
            # fails ahead of the older append-only guard at 2026_05_12_0014.
            assert downgrade_result.returncode != 0
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
            assert downgrade_result.returncode == 0, (
                downgrade_result.stdout + downgrade_result.stderr
            )
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
            # A populated database refuses to downgrade. Post Path B (#491) the
            # earliest blocking step is the NOT NULL reinstatement for the
            # re-added (now permanently NULL) confidence/gate columns, which
            # fails ahead of the older materialization guard at 2026_05_13_0015.
            assert downgrade_result.returncode != 0
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
            # A populated database refuses to downgrade. Post Path B (#491) the
            # earliest blocking step is the NOT NULL reinstatement for the
            # re-added (now permanently NULL) confidence/gate columns, which
            # fails ahead of the older quantity guard at 2026_05_15_0017.
            assert downgrade_result.returncode != 0
        finally:
            await _drop_temp_database(database_name)
