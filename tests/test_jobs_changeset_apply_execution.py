"""Unit tests for changeset-apply execution input helpers."""

from __future__ import annotations

import uuid
from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.cad.changeset import ChangeSetApplyError
from app.core.errors import ErrorCode
from app.jobs import changeset_apply_execution
from app.models.changeset_apply_job_input import ChangeSetApplyJobInput
from app.models.job import Job, JobType


def _build_job(*, job_id: uuid.UUID | None = None) -> Job:
    return Job(
        id=job_id or uuid.uuid4(),
        project_id=uuid.uuid4(),
        file_id=uuid.uuid4(),
        base_revision_id=uuid.uuid4(),
        job_type=JobType.CHANGESET_APPLY.value,
        status="running",
    )


def _build_apply_input(job: Job) -> ChangeSetApplyJobInput:
    assert job.base_revision_id is not None
    return ChangeSetApplyJobInput(
        source_job_id=job.id,
        project_id=job.project_id,
        source_file_id=job.file_id,
        drawing_revision_id=job.base_revision_id,
        change_set_id=uuid.uuid4(),
        source_job_type=JobType.CHANGESET_APPLY.value,
        latest_validation_result_id=uuid.uuid4(),
        latest_validation_status="valid",
    )


@pytest.mark.asyncio
async def test_load_changeset_apply_job_input_rejects_missing_input() -> None:
    job = _build_job()

    async def query_inputs(
        session: AsyncSession,
        *,
        job_id: uuid.UUID,
    ) -> list[ChangeSetApplyJobInput]:
        _ = (session, job_id)
        return []

    with pytest.raises(changeset_apply_execution.ChangeSetApplyJobError) as exc_info:
        await changeset_apply_execution.load_changeset_apply_job_input(
            cast(AsyncSession, object()),
            job=job,
            query_job_inputs=query_inputs,
        )

    assert exc_info.value.error_code is ErrorCode.NOT_FOUND
    assert exc_info.value.message == "Changeset apply job input is missing."


@pytest.mark.asyncio
async def test_load_changeset_apply_job_input_rejects_lineage_mismatch() -> None:
    job = _build_job()
    apply_input = _build_apply_input(job)
    apply_input.source_file_id = uuid.uuid4()

    async def query_inputs(
        session: AsyncSession,
        *,
        job_id: uuid.UUID,
    ) -> list[ChangeSetApplyJobInput]:
        _ = (session, job_id)
        return [apply_input]

    with pytest.raises(changeset_apply_execution.ChangeSetApplyJobError) as exc_info:
        await changeset_apply_execution.load_changeset_apply_job_input(
            cast(AsyncSession, object()),
            job=job,
            query_job_inputs=query_inputs,
        )

    assert exc_info.value.error_code is ErrorCode.INPUT_INVALID
    assert exc_info.value.message == (
        "Changeset apply job input lineage does not match the persisted job."
    )
    assert exc_info.value.details == {
        "source_job_type": JobType.CHANGESET_APPLY.value,
        "source_file_id": str(apply_input.source_file_id),
        "file_id": str(job.file_id),
        "drawing_revision_id": str(apply_input.drawing_revision_id),
        "base_revision_id": str(job.base_revision_id),
    }


@pytest.mark.asyncio
async def test_load_changeset_apply_job_input_returns_matching_input() -> None:
    job = _build_job()
    apply_input = _build_apply_input(job)

    async def query_inputs(
        session: AsyncSession,
        *,
        job_id: uuid.UUID,
    ) -> list[ChangeSetApplyJobInput]:
        _ = session
        assert job_id == job.id
        return [apply_input]

    loaded = await changeset_apply_execution.load_changeset_apply_job_input(
        cast(AsyncSession, object()),
        job=job,
        query_job_inputs=query_inputs,
    )

    assert loaded is apply_input


def test_parse_uuid_value_rejects_invalid_values() -> None:
    parsed = uuid.uuid4()

    assert changeset_apply_execution.parse_uuid_value(parsed) == parsed
    assert changeset_apply_execution.parse_uuid_value(str(parsed)) == parsed
    assert changeset_apply_execution.parse_uuid_value("not-a-uuid") is None
    assert changeset_apply_execution.parse_uuid_value(None) is None


def test_build_changeset_apply_error_details_merges_engine_details() -> None:
    change_set_id = uuid.uuid4()
    details = changeset_apply_execution.build_changeset_apply_error_details(
        ChangeSetApplyError(
            change_set_id=change_set_id,
            error_code="INVALID_OPERATION",
            message="Invalid operation.",
            details={"operation_id": "op-1"},
        )
    )

    assert details == {
        "change_set_id": str(change_set_id),
        "apply_error_code": "INVALID_OPERATION",
        "operation_id": "op-1",
    }
