"""Integration tests for CAD changeset persistence helpers."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

import httpx
import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

import app.db.session as session_module
from app.cad.changesets import (
    CadChangeOperationCreate,
    CadChangeSetValidationResultCreate,
    append_change_set_validation_result,
    create_change_set,
    get_change_set,
    list_change_set_operations,
    list_change_set_validation_results,
    normalize_change_operation_type,
    normalize_change_set_status,
    normalize_change_set_validation_status,
    update_change_set_status,
)
from app.jobs.worker import process_ingest_job
from app.models.cad_changeset import (
    CadChangeOperation,
    CadChangeSet,
    CadChangeSetValidationResult,
)
from tests.conftest import requires_database
from tests.jobs_test_helpers import (
    _create_project,
    _get_job_for_file,
    _upload_file,
    fake_ingestion_runner,
)
from tests.test_ingest_output_persistence import _load_project_outputs

pytestmark = [
    requires_database,
    pytest.mark.db_lineage,
    pytest.mark.usefixtures(fake_ingestion_runner.__name__),
]

_VALID_SOURCE_HASH = "a" * 64


@dataclass(frozen=True)
class _ChangeSetSeed:
    project_id: uuid.UUID
    file_id: uuid.UUID
    drawing_revision_id: uuid.UUID


async def _seed_changeset_lineage(async_client: httpx.AsyncClient) -> _ChangeSetSeed:
    project = await _create_project(async_client)
    uploaded = await _upload_file(async_client, project["id"])
    ingest_job = await _get_job_for_file(str(uploaded["id"]))

    await process_ingest_job(ingest_job.id)

    _, drawing_revisions, _, _ = await _load_project_outputs(project["id"])
    assert len(drawing_revisions) == 1

    return _ChangeSetSeed(
        project_id=uuid.UUID(project["id"]),
        file_id=uuid.UUID(str(uploaded["id"])),
        drawing_revision_id=drawing_revisions[0].id,
    )


async def _open_session() -> AsyncSession:
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    return session_maker()


async def _assert_flush_rejected(
    session: AsyncSession,
    instance: object,
    *,
    expected_constraint: str | None = None,
) -> None:
    session.add(instance)
    with pytest.raises(IntegrityError) as exc_info:
        await session.flush()
    await session.rollback()
    if expected_constraint is not None:
        assert expected_constraint in str(exc_info.value)


@pytest.mark.asyncio
async def test_changeset_helpers_persist_operations_and_validation_results(
    async_client: httpx.AsyncClient,
) -> None:
    seed = await _seed_changeset_lineage(async_client)

    async with await _open_session() as session:
        change_set = await create_change_set(
            session,
            project_id=seed.project_id,
            base_revision_id=seed.drawing_revision_id,
            status=" Proposed ",
            created_by=" api ",
            operations=(
                CadChangeOperationCreate(
                    operation_type=" Change_Layer ",
                    target_revision_entity_id=uuid.uuid4(),
                    expected_source_identity=" source-001 ",
                    expected_source_hash=f" {_VALID_SOURCE_HASH} ",
                    operation_json={"layer": "A-ANNO"},
                ),
                CadChangeOperationCreate(
                    operation_type="FLAG_FOR_REVIEW",
                    operation_json={"reason": "manual-review"},
                ),
            ),
        )

        updated = await update_change_set_status(
            session,
            project_id=seed.project_id,
            change_set_id=change_set.id,
            status=" validation_requested ",
        )
        assert updated is not None
        assert updated.status == "validation_requested"

        validation_result = await append_change_set_validation_result(
            session,
            project_id=seed.project_id,
            change_set_id=change_set.id,
            validation_result=CadChangeSetValidationResultCreate(
                validation_status=" Valid_With_Warnings ",
                validator_name=" validator ",
                validator_version=" 1 ",
                result_json={"warnings": ["missing_metadata"]},
            ),
        )

        assert change_set.status == "validation_requested"
        assert change_set.created_by == "api"
        assert validation_result.validation_status == "valid_with_warnings"
        await session.commit()

    async with await _open_session() as session:
        persisted_change_set = await get_change_set(
            session,
            project_id=seed.project_id,
            change_set_id=change_set.id,
        )
        persisted_operations = await list_change_set_operations(
            session,
            project_id=seed.project_id,
            change_set_id=change_set.id,
        )
        persisted_validation_results = await list_change_set_validation_results(
            session,
            project_id=seed.project_id,
            change_set_id=change_set.id,
        )

    assert persisted_change_set is not None
    assert persisted_change_set.base_revision_id == seed.drawing_revision_id
    assert persisted_change_set.status == "validation_requested"
    assert persisted_change_set.created_by == "api"

    assert [operation.sequence_index for operation in persisted_operations] == [1, 2]
    assert [operation.operation_type for operation in persisted_operations] == [
        "change_layer",
        "flag_for_review",
    ]
    assert persisted_operations[0].expected_source_identity == "source-001"
    assert persisted_operations[0].expected_source_hash == _VALID_SOURCE_HASH
    assert persisted_operations[0].operation_json == {"layer": "A-ANNO"}
    assert persisted_operations[1].operation_json == {"reason": "manual-review"}

    assert len(persisted_validation_results) == 1
    assert persisted_validation_results[0].validation_status == "valid_with_warnings"
    assert persisted_validation_results[0].validator_name == "validator"
    assert persisted_validation_results[0].validator_version == "1"
    assert persisted_validation_results[0].result_json == {
        "warnings": ["missing_metadata"],
    }


def test_changeset_helpers_reject_unsupported_choices() -> None:
    with pytest.raises(ValueError, match="Unsupported operation_type"):
        normalize_change_operation_type("unsupported_operation")

    with pytest.raises(ValueError, match="Unsupported status"):
        normalize_change_set_status("unsupported_status")

    with pytest.raises(ValueError, match="Unsupported validation_status"):
        normalize_change_set_validation_status("unsupported_validation_status")


@pytest.mark.asyncio
async def test_update_change_set_status_returns_none_for_missing_changeset() -> None:
    async with await _open_session() as session:
        updated = await update_change_set_status(
            session,
            project_id=uuid.uuid4(),
            change_set_id=uuid.uuid4(),
            status="approved",
        )

    assert updated is None


@pytest.mark.asyncio
async def test_changeset_db_constraints_reject_invalid_rows(
    async_client: httpx.AsyncClient,
) -> None:
    seed = await _seed_changeset_lineage(async_client)

    async with await _open_session() as session:
        change_set = await create_change_set(
            session,
            project_id=seed.project_id,
            base_revision_id=seed.drawing_revision_id,
            status="proposed",
            operations=(
                CadChangeOperationCreate(
                    operation_type="change_layer",
                    operation_json={"layer": "A-ANNO"},
                ),
            ),
        )
        change_set_id = change_set.id
        await session.commit()

        await _assert_flush_rejected(
            session,
            CadChangeSet(
                project_id=seed.project_id,
                base_revision_id=seed.drawing_revision_id,
                status="unsupported_status",
            ),
        )
        await _assert_flush_rejected(
            session,
            CadChangeSet(
                project_id=seed.project_id,
                base_revision_id=uuid.uuid4(),
                status="proposed",
            ),
        )
        await _assert_flush_rejected(
            session,
            CadChangeOperation(
                project_id=seed.project_id,
                change_set_id=change_set_id,
                sequence_index=1,
                operation_type="flag_for_review",
                operation_json={"reason": "duplicate-sequence"},
            ),
        )
        await _assert_flush_rejected(
            session,
            CadChangeOperation(
                project_id=seed.project_id,
                change_set_id=change_set_id,
                sequence_index=2,
                operation_type="unsupported_operation",
                operation_json={},
            ),
        )
        await _assert_flush_rejected(
            session,
            CadChangeOperation(
                project_id=seed.project_id,
                change_set_id=change_set_id,
                sequence_index=0,
                operation_type="flag_for_review",
                operation_json={},
            ),
        )
        await _assert_flush_rejected(
            session,
            CadChangeOperation(
                project_id=seed.project_id,
                change_set_id=change_set_id,
                sequence_index=2,
                operation_type="flag_for_review",
                expected_source_hash="A" * 64,
                operation_json={},
            ),
            expected_constraint="ck_cad_change_operations_expected_source_hash_sha256",
        )
        await _assert_flush_rejected(
            session,
            CadChangeOperation(
                project_id=seed.project_id,
                change_set_id=change_set_id,
                sequence_index=2,
                operation_type="flag_for_review",
                operation_json=["not", "an", "object"],
            ),
            expected_constraint="ck_cad_change_operations_operation_json_object",
        )
        await _assert_flush_rejected(
            session,
            CadChangeSetValidationResult(
                project_id=seed.project_id,
                change_set_id=change_set_id,
                validation_status="unsupported_validation_status",
                result_json={},
            ),
        )
        await _assert_flush_rejected(
            session,
            CadChangeSetValidationResult(
                project_id=seed.project_id,
                change_set_id=change_set_id,
                validation_status="valid",
                result_json=["not", "an", "object"],
            ),
            expected_constraint="ck_cad_change_set_validation_results_result_json_object",
        )
