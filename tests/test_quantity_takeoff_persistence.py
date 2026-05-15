"""Integration tests for append-only quantity persistence schema."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from json import dumps
from typing import Any

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError, IntegrityError

import app.db.session as session_module
import app.jobs.worker as worker_module
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunRequest
from app.jobs.worker import process_ingest_job
from app.models.job import Job, JobStatus, JobType
from app.models.quantity_takeoff import QuantityItem, QuantityItemKind, QuantityTakeoff
from tests.conftest import requires_database
from tests.test_ingest_output_persistence import (
    _build_contract_entity,
    _load_project_materialization,
    _load_project_outputs,
    _replace_fake_canonical_payload,
)
from tests.test_jobs import (
    _build_fake_ingest_payload,
    _create_project,
    _get_job_for_file,
    _upload_file,
)

_SEED_SOURCE_ENTITY_ID = "__seed_source_entity_id__"


@dataclass(frozen=True)
class _QuantityPersistenceSeed:
    project_id: uuid.UUID
    file_id: uuid.UUID
    ingest_job_id: uuid.UUID
    drawing_revision_id: uuid.UUID
    extraction_profile_id: uuid.UUID
    source_entity_id: str
    quantity_job_id: uuid.UUID
    quantity_takeoff_id: uuid.UUID
    quantity_item_id: uuid.UUID


@pytest.fixture(autouse=True)
def fake_ingestion_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> list[IngestionRunRequest]:
    """Patch worker ingestion with deterministic persisted outputs."""

    recorded_requests: list[IngestionRunRequest] = []

    async def _fake_run_ingestion(request: IngestionRunRequest) -> IngestFinalizationPayload:
        recorded_requests.append(request)
        entity_suffix = f"{len(recorded_requests):03d}"
        payload = _build_fake_ingest_payload(request)
        return _replace_fake_canonical_payload(
            payload,
            entities=[
                _build_contract_entity(
                    entity_id=f"entity-quantity-{entity_suffix}",
                    entity_type="line",
                    layer_ref="A-WALL",
                    source_id=f"entity-source-quantity-{entity_suffix}",
                )
            ],
        )

    monkeypatch.setattr(worker_module, "run_ingestion", _fake_run_ingestion)
    return recorded_requests


def _normalize_ondelete(raw_foreign_key: Mapping[str, Any]) -> str:
    options = raw_foreign_key.get("options")
    if isinstance(options, dict):
        return str(options.get("ondelete") or "").upper()
    return ""


def _normalize_constraint_sqltext(sqltext: str) -> str:
    normalized = sqltext
    for cast in (
        "::character varying[]",
        "::character varying",
        "::double precision",
        "::text[]",
        "::text",
    ):
        normalized = normalized.replace(cast, "")
    normalized = normalized.replace('"', " ")
    normalized = normalized.replace("(", " ").replace(")", " ")
    return " ".join(normalized.split())


def _inspect_quantity_schema(sync_connection: sa.Connection) -> dict[str, Any]:
    inspector = sa.inspect(sync_connection)
    tables = ("jobs", "quantity_takeoffs", "quantity_items")
    return {
        "columns": {
            table_name: {
                str(column["name"]): {"nullable": bool(column["nullable"])}
                for column in inspector.get_columns(table_name)
            }
            for table_name in tables
        },
        "unique_constraints": {
            table_name: {
                tuple(constraint["column_names"]): str(constraint["name"])
                for constraint in inspector.get_unique_constraints(table_name)
            }
            for table_name in tables
        },
        "indexes": {
            table_name: {
                tuple(index["column_names"]): str(index["name"])
                for index in inspector.get_indexes(table_name)
            }
            for table_name in tables
        },
        "check_constraints": {
            table_name: {
                str(constraint["name"]): str(constraint["sqltext"])
                for constraint in inspector.get_check_constraints(table_name)
            }
            for table_name in tables
        },
        "foreign_keys": {
            table_name: {
                (
                    tuple(foreign_key["constrained_columns"]),
                    str(foreign_key["referred_table"]),
                    tuple(foreign_key["referred_columns"]),
                ): _normalize_ondelete(foreign_key)
                for foreign_key in inspector.get_foreign_keys(table_name)
            }
            for table_name in ("quantity_takeoffs", "quantity_items")
        },
    }


async def _load_quantity_schema() -> dict[str, Any]:
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        connection = await session.connection()
        return await connection.run_sync(_inspect_quantity_schema)


async def _seed_quantity_lineage(async_client: httpx.AsyncClient) -> _QuantityPersistenceSeed:
    project = await _create_project(async_client)
    uploaded = await _upload_file(async_client, project["id"])
    ingest_job = await _get_job_for_file(str(uploaded["id"]))

    await process_ingest_job(ingest_job.id)

    _, drawing_revisions, _, _ = await _load_project_outputs(project["id"])
    _, _, _, _, entities = await _load_project_materialization(project["id"])

    assert len(drawing_revisions) == 1
    assert len(entities) == 1

    quantity_job_id = uuid.uuid4()
    quantity_takeoff_id = uuid.uuid4()
    quantity_item_id = uuid.uuid4()
    drawing_revision_id = drawing_revisions[0].id
    source_entity_id = entities[0].entity_id

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        session.add(
            Job(
                id=quantity_job_id,
                project_id=uuid.UUID(project["id"]),
                file_id=uuid.UUID(str(uploaded["id"])),
                base_revision_id=drawing_revision_id,
                job_type=JobType.QUANTITY_TAKEOFF.value,
                status=JobStatus.SUCCEEDED.value,
                enqueue_status="published",
                enqueue_attempts=0,
                cancel_requested=False,
            )
        )
        await session.flush()
        session.add(
            QuantityTakeoff(
                id=quantity_takeoff_id,
                project_id=uuid.UUID(project["id"]),
                source_file_id=uuid.UUID(str(uploaded["id"])),
                drawing_revision_id=drawing_revision_id,
                source_job_id=quantity_job_id,
                source_job_type=JobType.QUANTITY_TAKEOFF.value,
                review_state="approved",
                validation_status="valid",
                quantity_gate="allowed",
                trusted_totals=True,
            )
        )
        await session.flush()
        session.add(
            QuantityItem(
                id=quantity_item_id,
                quantity_takeoff_id=quantity_takeoff_id,
                project_id=uuid.UUID(project["id"]),
                drawing_revision_id=drawing_revision_id,
                item_kind="contributor",
                quantity_type="linear_length",
                value=12.5,
                unit="m",
                review_state="approved",
                validation_status="valid",
                quantity_gate="allowed",
                source_entity_id=source_entity_id,
                excluded_source_entity_ids_json=[],
            )
        )
        await session.commit()

    return _QuantityPersistenceSeed(
        project_id=uuid.UUID(project["id"]),
        file_id=uuid.UUID(str(uploaded["id"])),
        ingest_job_id=ingest_job.id,
        drawing_revision_id=drawing_revision_id,
        extraction_profile_id=drawing_revisions[0].extraction_profile_id,
        source_entity_id=source_entity_id,
        quantity_job_id=quantity_job_id,
        quantity_takeoff_id=quantity_takeoff_id,
        quantity_item_id=quantity_item_id,
    )


def _assert_constraint_failure(error: BaseException, *, expected_fragment: str) -> None:
    message = str(error.orig) if isinstance(error, (IntegrityError, DBAPIError)) else str(error)
    assert expected_fragment in message


async def _create_reprocess_job(
    seed: _QuantityPersistenceSeed,
    *,
    base_revision_id: uuid.UUID,
) -> uuid.UUID:
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    job_id = uuid.uuid4()
    async with session_maker() as session:
        session.add(
            Job(
                id=job_id,
                project_id=seed.project_id,
                file_id=seed.file_id,
                extraction_profile_id=seed.extraction_profile_id,
                base_revision_id=base_revision_id,
                job_type="reprocess",
                status=JobStatus.SUCCEEDED.value,
                enqueue_status="published",
                enqueue_attempts=0,
                cancel_requested=False,
            )
        )
        await session.commit()

    return job_id


async def _create_additional_drawing_revision(seed: _QuantityPersistenceSeed) -> uuid.UUID:
    adapter_outputs, drawing_revisions, _, _ = await _load_project_outputs(str(seed.project_id))

    assert len(adapter_outputs) == 1
    assert len(drawing_revisions) == 1

    adapter_output = adapter_outputs[0]
    base_revision = drawing_revisions[0]
    adapter_run_output_id = uuid.uuid4()
    drawing_revision_id = uuid.uuid4()
    source_job_id = await _create_reprocess_job(seed, base_revision_id=base_revision.id)

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        await session.execute(
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
                "project_id": seed.project_id,
                "source_file_id": seed.file_id,
                "extraction_profile_id": adapter_output.extraction_profile_id,
                "source_job_id": source_job_id,
                "adapter_key": adapter_output.adapter_key,
                "adapter_version": adapter_output.adapter_version,
                "input_family": adapter_output.input_family,
                "canonical_entity_schema_version": adapter_output.canonical_entity_schema_version,
                "canonical_json": dumps(adapter_output.canonical_json, separators=(",", ":")),
                "provenance_json": dumps(adapter_output.provenance_json, separators=(",", ":")),
                "confidence_json": dumps(adapter_output.confidence_json, separators=(",", ":")),
                "confidence_score": adapter_output.confidence_score,
                "warnings_json": dumps(adapter_output.warnings_json, separators=(",", ":")),
                "diagnostics_json": dumps(adapter_output.diagnostics_json, separators=(",", ":")),
                "result_checksum_sha256": f"{uuid.uuid4().hex}{uuid.uuid4().hex}",
            },
        )
        await session.execute(
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
                "project_id": seed.project_id,
                "source_file_id": seed.file_id,
                "extraction_profile_id": base_revision.extraction_profile_id,
                "source_job_id": source_job_id,
                "adapter_run_output_id": adapter_run_output_id,
                "predecessor_revision_id": base_revision.id,
                "revision_sequence": base_revision.revision_sequence + 1,
                "revision_kind": "reprocess",
                "review_state": base_revision.review_state,
                "canonical_entity_schema_version": base_revision.canonical_entity_schema_version,
                "confidence_score": base_revision.confidence_score,
            },
        )
        await session.commit()

    return drawing_revision_id


async def _create_quantity_takeoff(
    seed: _QuantityPersistenceSeed,
    *,
    review_state: str,
    validation_status: str,
    quantity_gate: str,
    trusted_totals: bool,
) -> uuid.UUID:
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    quantity_job_id = uuid.uuid4()
    quantity_takeoff_id = uuid.uuid4()

    async with session_maker() as session:
        session.add(
            Job(
                id=quantity_job_id,
                project_id=seed.project_id,
                file_id=seed.file_id,
                base_revision_id=seed.drawing_revision_id,
                job_type=JobType.QUANTITY_TAKEOFF.value,
                status=JobStatus.SUCCEEDED.value,
                enqueue_status="published",
                enqueue_attempts=0,
                cancel_requested=False,
            )
        )
        await session.flush()
        session.add(
            QuantityTakeoff(
                id=quantity_takeoff_id,
                project_id=seed.project_id,
                source_file_id=seed.file_id,
                drawing_revision_id=seed.drawing_revision_id,
                source_job_id=quantity_job_id,
                source_job_type=JobType.QUANTITY_TAKEOFF.value,
                review_state=review_state,
                validation_status=validation_status,
                quantity_gate=quantity_gate,
                trusted_totals=trusted_totals,
            )
        )
        await session.commit()

    return quantity_takeoff_id


@requires_database
class TestQuantityTakeoffPersistence:
    """Tests for quantity takeoff persistence schema and constraints."""

    async def test_quantity_persistence_schema_matches_contract(self) -> None:
        _ = self

        schema = await _load_quantity_schema()
        columns = schema["columns"]
        unique_constraints = schema["unique_constraints"]
        indexes = schema["indexes"]
        check_constraints = schema["check_constraints"]
        foreign_keys = schema["foreign_keys"]
        quantity_item_checks = check_constraints["quantity_items"]

        assert tuple(kind.value for kind in QuantityItemKind) == (
            "contributor",
            "aggregate",
            "exclusion",
            "conflict",
        )

        assert (
            "id",
            "project_id",
            "file_id",
            "base_revision_id",
            "job_type",
        ) in unique_constraints["jobs"]

        takeoff_columns = columns["quantity_takeoffs"]
        for required_column in (
            "project_id",
            "source_file_id",
            "drawing_revision_id",
            "source_job_id",
            "source_job_type",
            "review_state",
            "validation_status",
            "quantity_gate",
            "trusted_totals",
            "created_at",
        ):
            assert required_column in takeoff_columns
            assert takeoff_columns[required_column]["nullable"] is False

        assert (
            "id",
            "project_id",
            "drawing_revision_id",
        ) in unique_constraints["quantity_takeoffs"]
        assert (
            "id",
            "project_id",
            "drawing_revision_id",
            "quantity_gate",
        ) in unique_constraints["quantity_takeoffs"]
        assert ("source_job_id",) in unique_constraints["quantity_takeoffs"]
        assert ("project_id",) in indexes["quantity_takeoffs"]
        assert ("source_file_id",) in indexes["quantity_takeoffs"]
        for constraint_name in (
            "ck_quantity_takeoffs_review_state_valid",
            "ck_quantity_takeoffs_validation_status_valid",
            "ck_quantity_takeoffs_quantity_gate_valid",
            "ck_quantity_takeoffs_source_job_type_quantity_takeoff",
            "ck_quantity_takeoffs_trusted_totals_allowed_gate",
        ):
            assert constraint_name in check_constraints["quantity_takeoffs"]

        item_columns = columns["quantity_items"]
        for required_column in (
            "quantity_takeoff_id",
            "project_id",
            "drawing_revision_id",
            "item_kind",
            "quantity_type",
            "unit",
            "review_state",
            "validation_status",
            "quantity_gate",
            "excluded_source_entity_ids_json",
            "created_at",
        ):
            assert required_column in item_columns

        assert item_columns["source_entity_id"]["nullable"] is True
        assert item_columns["quantity_takeoff_id"]["nullable"] is False
        assert item_columns["project_id"]["nullable"] is False
        assert item_columns["drawing_revision_id"]["nullable"] is False
        assert ("quantity_takeoff_id",) in indexes["quantity_items"]
        assert ("project_id",) in indexes["quantity_items"]
        assert ("drawing_revision_id",) in indexes["quantity_items"]
        assert ("drawing_revision_id", "source_entity_id") in indexes["quantity_items"]
        for constraint_name in (
            "ck_quantity_items_item_kind_valid",
            "ck_quantity_items_review_state_valid",
            "ck_quantity_items_validation_status_valid",
            "ck_quantity_items_quantity_gate_valid",
            "ck_quantity_items_value_nonnegative_finite",
            "ck_quantity_items_kind_source_entity_contract",
            "ck_quantity_items_kind_value_contract",
            "ck_quantity_items_conflict_gate_review_only",
            "ck_quantity_items_quantity_type_nonempty",
            "ck_quantity_items_unit_nonempty",
            "ck_quantity_items_excluded_source_entity_ids_json_array",
        ):
            assert constraint_name in quantity_item_checks

        item_kind_valid_sql = _normalize_constraint_sqltext(
            quantity_item_checks["ck_quantity_items_item_kind_valid"]
        )
        for expected_kind in ("contributor", "aggregate", "exclusion", "conflict"):
            assert expected_kind in item_kind_valid_sql
        assert "total" not in item_kind_valid_sql
        assert "excluded" not in item_kind_valid_sql

        source_entity_contract_sql = _normalize_constraint_sqltext(
            quantity_item_checks["ck_quantity_items_kind_source_entity_contract"]
        )
        for expected_fragment in (
            "item_kind = 'contributor' AND source_entity_id IS NOT NULL",
            "item_kind = 'aggregate' AND source_entity_id IS NULL",
            "item_kind = 'exclusion' AND source_entity_id IS NOT NULL",
            "item_kind = 'conflict' AND source_entity_id IS NOT NULL",
        ):
            assert expected_fragment in source_entity_contract_sql

        value_contract_sql = _normalize_constraint_sqltext(
            quantity_item_checks["ck_quantity_items_kind_value_contract"]
        )
        for expected_fragment in (
            "item_kind = 'contributor' AND value IS NOT NULL",
            "item_kind = 'aggregate' AND value IS NOT NULL",
            "item_kind = 'exclusion' AND value IS NULL",
            "item_kind = 'conflict' AND value IS NULL",
        ):
            assert expected_fragment in value_contract_sql

        excluded_refs_sql = _normalize_constraint_sqltext(
            quantity_item_checks["ck_quantity_items_excluded_source_entity_ids_json_array"]
        )
        assert "json_typeof excluded_source_entity_ids_json = 'array'" in excluded_refs_sql

        conflict_gate_sql = _normalize_constraint_sqltext(
            quantity_item_checks["ck_quantity_items_conflict_gate_review_only"]
        )
        assert "item_kind <> 'conflict'" in conflict_gate_sql
        assert "quantity_gate" in conflict_gate_sql
        for expected_gate in ("review_gated", "blocked"):
            assert expected_gate in conflict_gate_sql

        assert foreign_keys["quantity_takeoffs"][
            (("project_id",), "projects", ("id",))
        ] == "RESTRICT"
        assert foreign_keys["quantity_takeoffs"][
            (("source_file_id", "project_id"), "files", ("id", "project_id"))
        ] == "RESTRICT"
        assert foreign_keys["quantity_takeoffs"][
            (
                ("drawing_revision_id", "project_id", "source_file_id"),
                "drawing_revisions",
                ("id", "project_id", "source_file_id"),
            )
        ] == "RESTRICT"
        assert foreign_keys["quantity_takeoffs"][
            (
                (
                    "source_job_id",
                    "project_id",
                    "source_file_id",
                    "drawing_revision_id",
                    "source_job_type",
                ),
                "jobs",
                ("id", "project_id", "file_id", "base_revision_id", "job_type"),
            )
        ] == "RESTRICT"
        assert foreign_keys["quantity_items"][
            (("project_id",), "projects", ("id",))
        ] == "RESTRICT"
        assert foreign_keys["quantity_items"][
            (
                ("quantity_takeoff_id", "project_id", "drawing_revision_id"),
                "quantity_takeoffs",
                ("id", "project_id", "drawing_revision_id"),
            )
        ] == "RESTRICT"
        assert foreign_keys["quantity_items"][
            (
                (
                    "quantity_takeoff_id",
                    "project_id",
                    "drawing_revision_id",
                    "quantity_gate",
                ),
                "quantity_takeoffs",
                ("id", "project_id", "drawing_revision_id", "quantity_gate"),
            )
        ] == "RESTRICT"
        assert foreign_keys["quantity_items"][
            (
                ("drawing_revision_id", "source_entity_id"),
                "revision_entities",
                ("drawing_revision_id", "entity_id"),
            )
        ] == "RESTRICT"

    async def test_quantity_takeoff_constraints_reject_invalid_gate(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        seed = await _seed_quantity_lineage(async_client)

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            invalid_job_id = uuid.uuid4()
            session.add(
                Job(
                    id=invalid_job_id,
                    project_id=seed.project_id,
                    file_id=seed.file_id,
                    base_revision_id=seed.drawing_revision_id,
                    job_type=JobType.QUANTITY_TAKEOFF.value,
                    status=JobStatus.SUCCEEDED.value,
                    enqueue_status="published",
                    enqueue_attempts=0,
                    cancel_requested=False,
                )
            )
            await session.flush()
            session.add(
                QuantityTakeoff(
                    id=uuid.uuid4(),
                    project_id=seed.project_id,
                    source_file_id=seed.file_id,
                    drawing_revision_id=seed.drawing_revision_id,
                    source_job_id=invalid_job_id,
                    source_job_type=JobType.QUANTITY_TAKEOFF.value,
                    review_state="approved",
                    validation_status="valid",
                    quantity_gate="review_gated",
                    trusted_totals=True,
                )
            )

            with pytest.raises((IntegrityError, DBAPIError)) as exc_info:
                await session.commit()

            await session.rollback()

        _assert_constraint_failure(
            exc_info.value,
            expected_fragment="ck_quantity_takeoffs_trusted_totals_allowed_gate",
        )

    async def test_quantity_takeoff_source_job_contract_rejects_base_revision_mismatch(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        seed = await _seed_quantity_lineage(async_client)
        alternate_revision_id = await _create_additional_drawing_revision(seed)
        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            invalid_job_id = uuid.uuid4()
            session.add(
                Job(
                    id=invalid_job_id,
                    project_id=seed.project_id,
                    file_id=seed.file_id,
                    base_revision_id=alternate_revision_id,
                    job_type=JobType.QUANTITY_TAKEOFF.value,
                    status=JobStatus.SUCCEEDED.value,
                    enqueue_status="published",
                    enqueue_attempts=0,
                    cancel_requested=False,
                )
            )
            await session.flush()
            session.add(
                QuantityTakeoff(
                    id=uuid.uuid4(),
                    project_id=seed.project_id,
                    source_file_id=seed.file_id,
                    drawing_revision_id=seed.drawing_revision_id,
                    source_job_id=invalid_job_id,
                    source_job_type=JobType.QUANTITY_TAKEOFF.value,
                    review_state="approved",
                    validation_status="valid",
                    quantity_gate="allowed",
                    trusted_totals=False,
                )
            )

            with pytest.raises((IntegrityError, DBAPIError)) as exc_info:
                await session.commit()

            await session.rollback()

        _assert_constraint_failure(
            exc_info.value,
            expected_fragment="fk_quantity_takeoffs_source_job_contract",
        )

    async def test_quantity_takeoff_source_job_contract_rejects_wrong_job_type(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        seed = await _seed_quantity_lineage(async_client)
        invalid_job_id = await _create_reprocess_job(
            seed,
            base_revision_id=seed.drawing_revision_id,
        )

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            session.add(
                QuantityTakeoff(
                    id=uuid.uuid4(),
                    project_id=seed.project_id,
                    source_file_id=seed.file_id,
                    drawing_revision_id=seed.drawing_revision_id,
                    source_job_id=invalid_job_id,
                    source_job_type=JobType.QUANTITY_TAKEOFF.value,
                    review_state="approved",
                    validation_status="valid",
                    quantity_gate="allowed",
                    trusted_totals=False,
                )
            )

            with pytest.raises((IntegrityError, DBAPIError)) as exc_info:
                await session.commit()

            await session.rollback()

        _assert_constraint_failure(
            exc_info.value,
            expected_fragment="fk_quantity_takeoffs_source_job_contract",
        )

    @pytest.mark.parametrize(
        ("item_kwargs", "expected_fragment"),
        [
            (
                {
                    "item_kind": "invalid",
                    "quantity_type": "linear_length",
                    "value": 1.0,
                    "unit": "m",
                    "source_entity_id": _SEED_SOURCE_ENTITY_ID,
                },
                "ck_quantity_items_item_kind_valid",
            ),
            (
                {
                    "item_kind": "contributor",
                    "quantity_type": "linear_length",
                    "value": -1.0,
                    "unit": "m",
                    "source_entity_id": _SEED_SOURCE_ENTITY_ID,
                },
                "ck_quantity_items_value_nonnegative_finite",
            ),
            (
                {
                    "item_kind": "aggregate",
                    "quantity_type": "linear_length",
                    "value": 1.0,
                    "unit": "m",
                    "source_entity_id": _SEED_SOURCE_ENTITY_ID,
                },
                "ck_quantity_items_kind_source_entity_contract",
            ),
            (
                {
                    "item_kind": "contributor",
                    "quantity_type": "linear_length",
                    "value": 1.0,
                    "unit": "m",
                    "source_entity_id": None,
                },
                "ck_quantity_items_kind_source_entity_contract",
            ),
            (
                {
                    "item_kind": "contributor",
                    "quantity_type": "linear_length",
                    "value": None,
                    "unit": "m",
                    "source_entity_id": _SEED_SOURCE_ENTITY_ID,
                },
                "ck_quantity_items_kind_value_contract",
            ),
            (
                {
                    "item_kind": "aggregate",
                    "quantity_type": "linear_length",
                    "value": None,
                    "unit": "m",
                    "source_entity_id": None,
                },
                "ck_quantity_items_kind_value_contract",
            ),
            (
                {
                    "item_kind": "exclusion",
                    "quantity_type": "linear_length",
                    "value": 1.0,
                    "unit": "m",
                    "source_entity_id": _SEED_SOURCE_ENTITY_ID,
                },
                "ck_quantity_items_kind_value_contract",
            ),
            (
                {
                    "item_kind": "exclusion",
                    "quantity_type": "linear_length",
                    "value": None,
                    "unit": "m",
                    "source_entity_id": None,
                },
                "ck_quantity_items_kind_source_entity_contract",
            ),
            (
                {
                    "item_kind": "contributor",
                    "quantity_type": "linear_length",
                    "value": float("nan"),
                    "unit": "m",
                    "source_entity_id": _SEED_SOURCE_ENTITY_ID,
                },
                "ck_quantity_items_value_nonnegative_finite",
            ),
            (
                {
                    "item_kind": "contributor",
                    "quantity_type": "linear_length",
                    "value": float("inf"),
                    "unit": "m",
                    "source_entity_id": _SEED_SOURCE_ENTITY_ID,
                },
                "ck_quantity_items_value_nonnegative_finite",
            ),
            (
                {
                    "item_kind": "contributor",
                    "quantity_type": "linear_length",
                    "value": float("-inf"),
                    "unit": "m",
                    "source_entity_id": _SEED_SOURCE_ENTITY_ID,
                },
                "ck_quantity_items_value_nonnegative_finite",
            ),
        ],
    )
    async def test_quantity_item_constraints_reject_invalid_rows(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        item_kwargs: dict[str, Any],
        expected_fragment: str,
    ) -> None:
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        seed = await _seed_quantity_lineage(async_client)
        resolved_item_kwargs = dict(item_kwargs)
        if resolved_item_kwargs.get("source_entity_id") == _SEED_SOURCE_ENTITY_ID:
            resolved_item_kwargs["source_entity_id"] = seed.source_entity_id

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            session.add(
                QuantityItem(
                    id=uuid.uuid4(),
                    quantity_takeoff_id=seed.quantity_takeoff_id,
                    project_id=seed.project_id,
                    drawing_revision_id=seed.drawing_revision_id,
                    review_state="approved",
                    validation_status="valid",
                    quantity_gate="allowed",
                    excluded_source_entity_ids_json=[],
                    **resolved_item_kwargs,
                )
            )

            with pytest.raises((IntegrityError, DBAPIError)) as exc_info:
                await session.commit()

            await session.rollback()

        _assert_constraint_failure(exc_info.value, expected_fragment=expected_fragment)

    async def test_quantity_conflict_item_requires_fk_backed_source_entity(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        seed = await _seed_quantity_lineage(async_client)
        review_takeoff_id = await _create_quantity_takeoff(
            seed,
            review_state="review_required",
            validation_status="needs_review",
            quantity_gate="review_gated",
            trusted_totals=False,
        )

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            session.add(
                QuantityItem(
                    id=uuid.uuid4(),
                    quantity_takeoff_id=review_takeoff_id,
                    project_id=seed.project_id,
                    drawing_revision_id=seed.drawing_revision_id,
                    item_kind="conflict",
                    quantity_type="linear_length",
                    value=None,
                    unit="m",
                    review_state="review_required",
                    validation_status="needs_review",
                    quantity_gate="review_gated",
                    source_entity_id=None,
                    excluded_source_entity_ids_json=[],
                )
            )

            with pytest.raises((IntegrityError, DBAPIError)) as exc_info:
                await session.commit()

            await session.rollback()

        _assert_constraint_failure(
            exc_info.value,
            expected_fragment="ck_quantity_items_kind_source_entity_contract",
        )

    async def test_quantity_conflict_item_rejects_non_review_gate_under_trusted_takeoff(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        seed = await _seed_quantity_lineage(async_client)

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            session.add(
                QuantityItem(
                    id=uuid.uuid4(),
                    quantity_takeoff_id=seed.quantity_takeoff_id,
                    project_id=seed.project_id,
                    drawing_revision_id=seed.drawing_revision_id,
                    item_kind="conflict",
                    quantity_type="linear_length",
                    value=None,
                    unit="m",
                    review_state="approved",
                    validation_status="valid",
                    quantity_gate="allowed",
                    source_entity_id=seed.source_entity_id,
                    excluded_source_entity_ids_json=[],
                )
            )

            with pytest.raises((IntegrityError, DBAPIError)) as exc_info:
                await session.commit()

            await session.rollback()

        _assert_constraint_failure(
            exc_info.value,
            expected_fragment="ck_quantity_items_conflict_gate_review_only",
        )

    async def test_quantity_item_rejects_parent_gate_mismatch(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        seed = await _seed_quantity_lineage(async_client)

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            session.add(
                QuantityItem(
                    id=uuid.uuid4(),
                    quantity_takeoff_id=seed.quantity_takeoff_id,
                    project_id=seed.project_id,
                    drawing_revision_id=seed.drawing_revision_id,
                    item_kind="contributor",
                    quantity_type="linear_length",
                    value=1.0,
                    unit="m",
                    review_state="approved",
                    validation_status="valid",
                    quantity_gate="review_gated",
                    source_entity_id=seed.source_entity_id,
                    excluded_source_entity_ids_json=[],
                )
            )

            with pytest.raises((IntegrityError, DBAPIError)) as exc_info:
                await session.commit()

            await session.rollback()

        _assert_constraint_failure(
            exc_info.value,
            expected_fragment="fk_quantity_items_takeoff_gate_contract",
        )

    @pytest.mark.parametrize(
        ("review_state", "validation_status", "quantity_gate"),
        [
            ("review_required", "needs_review", "review_gated"),
            ("rejected", "invalid", "blocked"),
        ],
    )
    async def test_quantity_conflict_item_persists_when_gate_matches_review_only_parent(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        review_state: str,
        validation_status: str,
        quantity_gate: str,
    ) -> None:
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        seed = await _seed_quantity_lineage(async_client)
        review_takeoff_id = await _create_quantity_takeoff(
            seed,
            review_state=review_state,
            validation_status=validation_status,
            quantity_gate=quantity_gate,
            trusted_totals=False,
        )

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        conflict_item_id = uuid.uuid4()
        async with session_maker() as session:
            session.add(
                QuantityItem(
                    id=conflict_item_id,
                    quantity_takeoff_id=review_takeoff_id,
                    project_id=seed.project_id,
                    drawing_revision_id=seed.drawing_revision_id,
                    item_kind="conflict",
                    quantity_type="linear_length",
                    value=None,
                    unit="m",
                    review_state=review_state,
                    validation_status=validation_status,
                    quantity_gate=quantity_gate,
                    source_entity_id=seed.source_entity_id,
                    excluded_source_entity_ids_json=[seed.source_entity_id],
                )
            )
            await session.commit()

        async with session_maker() as session:
            persisted_item = await session.get(QuantityItem, conflict_item_id)

        assert persisted_item is not None
        assert persisted_item.item_kind == "conflict"
        assert persisted_item.quantity_gate == quantity_gate
        assert persisted_item.source_entity_id == seed.source_entity_id

    async def test_quantity_item_takeoff_lineage_rejects_revision_mismatch(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        seed = await _seed_quantity_lineage(async_client)

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            session.add(
                QuantityItem(
                    id=uuid.uuid4(),
                    quantity_takeoff_id=seed.quantity_takeoff_id,
                    project_id=seed.project_id,
                    drawing_revision_id=uuid.uuid4(),
                    item_kind="aggregate",
                    quantity_type="linear_length",
                    value=1.0,
                    unit="m",
                    review_state="approved",
                    validation_status="valid",
                    quantity_gate="allowed",
                    source_entity_id=None,
                    excluded_source_entity_ids_json=[],
                )
            )

            with pytest.raises((IntegrityError, DBAPIError)) as exc_info:
                await session.commit()

            await session.rollback()

        _assert_constraint_failure(
            exc_info.value,
            expected_fragment="fk_quantity_items_takeoff_lineage",
        )

    async def test_quantity_item_source_entity_fk_rejects_cross_revision_entity(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        source_seed = await _seed_quantity_lineage(async_client)
        target_seed = await _seed_quantity_lineage(async_client)

        assert source_seed.source_entity_id != target_seed.source_entity_id

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            session.add(
                QuantityItem(
                    id=uuid.uuid4(),
                    quantity_takeoff_id=target_seed.quantity_takeoff_id,
                    project_id=target_seed.project_id,
                    drawing_revision_id=target_seed.drawing_revision_id,
                    item_kind="contributor",
                    quantity_type="linear_length",
                    value=1.0,
                    unit="m",
                    review_state="approved",
                    validation_status="valid",
                    quantity_gate="allowed",
                    source_entity_id=source_seed.source_entity_id,
                    excluded_source_entity_ids_json=[],
                )
            )

            with pytest.raises((IntegrityError, DBAPIError)) as exc_info:
                await session.commit()

            await session.rollback()

        _assert_constraint_failure(
            exc_info.value,
            expected_fragment="fk_quantity_items_source_entity",
        )

    async def test_quantity_item_source_entity_fk_rejects_unknown_entity(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        seed = await _seed_quantity_lineage(async_client)

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            session.add(
                QuantityItem(
                    id=uuid.uuid4(),
                    quantity_takeoff_id=seed.quantity_takeoff_id,
                    project_id=seed.project_id,
                    drawing_revision_id=seed.drawing_revision_id,
                    item_kind="contributor",
                    quantity_type="linear_length",
                    value=1.0,
                    unit="m",
                    review_state="approved",
                    validation_status="valid",
                    quantity_gate="allowed",
                    source_entity_id="missing-entity",
                    excluded_source_entity_ids_json=[],
                )
            )

            with pytest.raises((IntegrityError, DBAPIError)) as exc_info:
                await session.commit()

            await session.rollback()

        _assert_constraint_failure(
            exc_info.value,
            expected_fragment="fk_quantity_items_source_entity",
        )
