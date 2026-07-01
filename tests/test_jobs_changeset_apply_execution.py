"""Unit tests for changeset-apply execution input helpers."""

from __future__ import annotations

import uuid
from typing import Any, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.cad.changeset import (
    ChangeSetApplyConflict,
    ChangeSetApplyConflictDetails,
    ChangeSetApplyError,
    ChangeSetApplyLoadError,
    ChangeSetApplySuccess,
    RevisionRef,
)
from app.core.errors import ErrorCode
from app.jobs import changeset_apply_execution
from app.jobs.lifecycle import _RevisionConflictError, _StaleJobAttemptError
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


class _FakeSession:
    def __init__(self, job: Job | None) -> None:
        self.job = job

    async def get(self, model_type: type[Any], row_id: uuid.UUID) -> Any:
        _ = (model_type, row_id)
        return self.job


class _FakeSessionContext:
    def __init__(self, session: _FakeSession) -> None:
        self.session = session

    async def __aenter__(self) -> _FakeSession:
        return self.session

    async def __aexit__(self, *_exc_info: object) -> None:
        return None


class _FakeSessionMaker:
    def __init__(self, session: _FakeSession) -> None:
        self.session = session

    def __call__(self) -> _FakeSessionContext:
        return _FakeSessionContext(self.session)


def _apply_success(change_set_id: uuid.UUID) -> ChangeSetApplySuccess:
    revision = RevisionRef(revision_id=uuid.uuid4(), revision_sequence=1)
    return ChangeSetApplySuccess(
        change_set_id=change_set_id,
        base_revision=revision,
        current_revision=revision,
        operations=(),
    )


async def _execute_apply_attempt(
    job: Job | None,
    *,
    attempt_token: uuid.UUID,
    load_apply_job_input: Any,
    apply_change_set: Any,
    job_attempt_current: bool = True,
) -> ChangeSetApplySuccess:
    def job_attempt_is_current(_job: Job, *, attempt_token: uuid.UUID) -> bool:
        assert isinstance(attempt_token, uuid.UUID)
        return job_attempt_current

    return await changeset_apply_execution.execute_changeset_apply_job_attempt(
        job.id if job is not None else uuid.uuid4(),
        attempt_token=attempt_token,
        session_maker_factory=lambda: _FakeSessionMaker(_FakeSession(job)),
        job_attempt_is_current=job_attempt_is_current,
        load_apply_job_input=load_apply_job_input,
        build_conflict_details=lambda result: {"change_set_id": str(result.details.change_set_id)},
        stale_job_attempt_error_type=_StaleJobAttemptError,
        revision_conflict_error_type=_RevisionConflictError,
        apply_change_set=apply_change_set,
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


@pytest.mark.asyncio
async def test_execute_changeset_apply_job_attempt_returns_apply_success() -> None:
    job = _build_job()
    attempt_token = uuid.uuid4()
    apply_input = _build_apply_input(job)
    apply_success = _apply_success(apply_input.change_set_id)
    calls: list[tuple[Any, uuid.UUID, uuid.UUID]] = []

    async def load_apply_job_input(session: Any, *, job: Job) -> ChangeSetApplyJobInput:
        calls.append((session, job.id, apply_input.change_set_id))
        return apply_input

    async def apply_change_set(
        session: Any,
        *,
        project_id: uuid.UUID,
        change_set_id: uuid.UUID,
    ) -> ChangeSetApplySuccess:
        calls.append((session, project_id, change_set_id))
        return apply_success

    result = await _execute_apply_attempt(
        job,
        attempt_token=attempt_token,
        load_apply_job_input=load_apply_job_input,
        apply_change_set=apply_change_set,
    )

    assert result is apply_success
    assert calls[0][1:] == (job.id, apply_input.change_set_id)
    assert calls[1][1:] == (job.project_id, apply_input.change_set_id)


@pytest.mark.asyncio
async def test_execute_changeset_apply_job_attempt_rejects_stale_attempt() -> None:
    job = _build_job()

    async def load_apply_job_input(*_args: Any, **_kwargs: Any) -> ChangeSetApplyJobInput:
        raise AssertionError("stale attempts should not load input")

    async def apply_change_set(*_args: Any, **_kwargs: Any) -> ChangeSetApplySuccess:
        raise AssertionError("stale attempts should not apply changeset")

    with pytest.raises(_StaleJobAttemptError):
        await _execute_apply_attempt(
            job,
            attempt_token=uuid.uuid4(),
            load_apply_job_input=load_apply_job_input,
            apply_change_set=apply_change_set,
            job_attempt_current=False,
        )


@pytest.mark.asyncio
async def test_execute_changeset_apply_job_attempt_requires_base_revision() -> None:
    job = _build_job()
    job.base_revision_id = None

    async def load_apply_job_input(*_args: Any, **_kwargs: Any) -> ChangeSetApplyJobInput:
        raise AssertionError("missing base should not load input")

    async def apply_change_set(*_args: Any, **_kwargs: Any) -> ChangeSetApplySuccess:
        raise AssertionError("missing base should not apply changeset")

    with pytest.raises(_RevisionConflictError) as exc_info:
        await _execute_apply_attempt(
            job,
            attempt_token=uuid.uuid4(),
            load_apply_job_input=load_apply_job_input,
            apply_change_set=apply_change_set,
        )

    assert exc_info.value.details == {
        "base_revision_id": None,
        "current_revision_id": None,
    }


@pytest.mark.asyncio
async def test_execute_changeset_apply_job_attempt_maps_apply_load_error() -> None:
    job = _build_job()
    apply_input = _build_apply_input(job)

    async def load_apply_job_input(*_args: Any, **_kwargs: Any) -> ChangeSetApplyJobInput:
        return apply_input

    async def apply_change_set(*_args: Any, **_kwargs: Any) -> ChangeSetApplySuccess:
        raise ChangeSetApplyLoadError("Changeset could not be loaded.")

    with pytest.raises(changeset_apply_execution.ChangeSetApplyJobError) as exc_info:
        await _execute_apply_attempt(
            job,
            attempt_token=uuid.uuid4(),
            load_apply_job_input=load_apply_job_input,
            apply_change_set=apply_change_set,
        )

    assert exc_info.value.error_code is ErrorCode.INPUT_INVALID
    assert exc_info.value.message == "Changeset could not be loaded."
    assert exc_info.value.details == {"change_set_id": str(apply_input.change_set_id)}


@pytest.mark.asyncio
async def test_execute_changeset_apply_job_attempt_maps_revision_conflict() -> None:
    job = _build_job()
    assert job.base_revision_id is not None
    apply_input = _build_apply_input(job)
    conflict = ChangeSetApplyConflict(
        details=ChangeSetApplyConflictDetails(
            base_revision_id=job.base_revision_id,
            base_revision_sequence=1,
            current_revision_id=uuid.uuid4(),
            current_revision_sequence=2,
            change_set_id=apply_input.change_set_id,
        )
    )

    async def load_apply_job_input(*_args: Any, **_kwargs: Any) -> ChangeSetApplyJobInput:
        return apply_input

    async def apply_change_set(*_args: Any, **_kwargs: Any) -> ChangeSetApplyConflict:
        return conflict

    with pytest.raises(_RevisionConflictError) as exc_info:
        await _execute_apply_attempt(
            job,
            attempt_token=uuid.uuid4(),
            load_apply_job_input=load_apply_job_input,
            apply_change_set=apply_change_set,
        )

    assert exc_info.value.message == (
        "Changeset apply base revision is stale relative to the current revision."
    )
    assert exc_info.value.details == {"change_set_id": str(apply_input.change_set_id)}


@pytest.mark.asyncio
async def test_execute_changeset_apply_job_attempt_maps_apply_error() -> None:
    job = _build_job()
    apply_input = _build_apply_input(job)
    apply_error = ChangeSetApplyError(
        change_set_id=apply_input.change_set_id,
        error_code="INVALID_OPERATION",
        message="Invalid operation.",
        details={"operation_id": "op-1"},
    )

    async def load_apply_job_input(*_args: Any, **_kwargs: Any) -> ChangeSetApplyJobInput:
        return apply_input

    async def apply_change_set(*_args: Any, **_kwargs: Any) -> ChangeSetApplyError:
        return apply_error

    with pytest.raises(changeset_apply_execution.ChangeSetApplyJobError) as exc_info:
        await _execute_apply_attempt(
            job,
            attempt_token=uuid.uuid4(),
            load_apply_job_input=load_apply_job_input,
            apply_change_set=apply_change_set,
        )

    assert exc_info.value.error_code is ErrorCode.INPUT_INVALID
    assert exc_info.value.message == "Invalid operation."
    assert exc_info.value.details == {
        "change_set_id": str(apply_input.change_set_id),
        "apply_error_code": "INVALID_OPERATION",
        "operation_id": "op-1",
    }


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
