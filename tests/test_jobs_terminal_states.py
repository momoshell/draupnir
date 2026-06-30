"""Unit tests for terminal job state logging helpers."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest

from app.core.errors import ErrorCode
from app.jobs import terminal_states


class _FakeLogger:
    def __init__(self) -> None:
        self.infos: list[tuple[str, dict[str, Any]]] = []
        self.warnings: list[tuple[str, dict[str, Any]]] = []
        self.errors: list[tuple[str, dict[str, Any]]] = []

    def info(self, event: str, **fields: Any) -> None:
        self.infos.append((event, fields))

    def warning(self, event: str, **fields: Any) -> None:
        self.warnings.append((event, fields))

    def error(self, event: str, **fields: Any) -> None:
        self.errors.append((event, fields))


@dataclass(frozen=True, slots=True)
class _RevisionConflict:
    message: str
    details: dict[str, Any]


@dataclass(slots=True)
class _FakeJob:
    id: uuid.UUID
    job_type: str = "ingest"
    status: str = "pending"


@dataclass(slots=True)
class _FakeProject:
    deleted_at: datetime | None = None


@dataclass(slots=True)
class _FakeSourceFile:
    deleted_at: datetime | None = None


@dataclass(slots=True)
class _FakeLockedSource:
    job: _FakeJob
    project: _FakeProject
    source_file: _FakeSourceFile | None


class _FakeSession:
    def __init__(self) -> None:
        self.committed = False

    async def commit(self) -> None:
        self.committed = True


class _FakeSessionContext:
    def __init__(self, session: _FakeSession) -> None:
        self.session = session

    async def __aenter__(self) -> _FakeSession:
        return self.session

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        _ = (exc_type, exc, traceback)


def _session_maker_factory(session: _FakeSession) -> Any:
    return lambda: lambda: _FakeSessionContext(session)


@pytest.mark.asyncio
async def test_mark_job_failed_for_revision_conflict_persists_and_logs() -> None:
    job_id = uuid.uuid4()
    attempt_token = uuid.uuid4()
    logger = _FakeLogger()
    marked: list[tuple[uuid.UUID, dict[str, Any]]] = []

    async def mark_job_failed(job_id: uuid.UUID, **kwargs: Any) -> bool:
        marked.append((job_id, kwargs))
        return True

    await terminal_states.mark_job_failed_for_revision_conflict(
        job_id,
        attempt_token=attempt_token,
        log_event="revision_conflict",
        exc=_RevisionConflict(
            message="Base revision is stale.",
            details={"base_revision_id": "base", "current_revision_id": "current"},
        ),
        mark_job_failed=mark_job_failed,
        logger_instance=logger,
    )

    assert marked == [
        (
            job_id,
            {
                "error_message": "Base revision is stale.",
                "error_code": ErrorCode.REVISION_CONFLICT,
                "attempt_token": attempt_token,
                "error_details": {"base_revision_id": "base", "current_revision_id": "current"},
            },
        )
    ]
    assert logger.warnings == [
        (
            "revision_conflict",
            {
                "job_id": str(job_id),
                "error_code": ErrorCode.REVISION_CONFLICT.value,
                "base_revision_id": "base",
                "current_revision_id": "current",
            },
        )
    ]


@pytest.mark.asyncio
async def test_mark_job_failed_with_internal_error_log_uses_default_safe_fields() -> None:
    job_id = uuid.uuid4()
    attempt_token = uuid.uuid4()
    logger = _FakeLogger()
    marked: list[tuple[uuid.UUID, dict[str, Any]]] = []

    async def mark_job_failed(job_id: uuid.UUID, **kwargs: Any) -> bool:
        marked.append((job_id, kwargs))
        return True

    await terminal_states.mark_job_failed_with_internal_error_log(
        job_id,
        attempt_token=attempt_token,
        error_message="Worker failed.",
        log_event="worker_failed",
        mark_job_failed=mark_job_failed,
        logger_instance=logger,
    )

    assert marked == [
        (
            job_id,
            {
                "error_message": "Worker failed.",
                "attempt_token": attempt_token,
            },
        )
    ]
    assert logger.errors == [
        (
            "worker_failed",
            {
                "job_id": str(job_id),
                "error_code": ErrorCode.INTERNAL_ERROR.value,
                "error_message": "Worker failed.",
                "exc_info": True,
            },
        )
    ]


@pytest.mark.asyncio
async def test_mark_job_failed_if_recovery_safe_persists_and_commits() -> None:
    job_id = uuid.uuid4()
    session = _FakeSession()
    job = _FakeJob(id=job_id)
    locked_source = _FakeLockedSource(
        job=job,
        project=_FakeProject(),
        source_file=_FakeSourceFile(),
    )
    logger = _FakeLogger()
    persisted: list[tuple[_FakeSession, _FakeJob, dict[str, Any]]] = []

    async def lock_source(session: _FakeSession, job_id: uuid.UUID) -> _FakeLockedSource:
        _ = job_id
        return locked_source

    async def cancel_inactive(*args: Any, **kwargs: Any) -> bool:
        raise AssertionError("active source should not be cancelled")

    async def persist_failed(session: _FakeSession, job: _FakeJob, **kwargs: Any) -> None:
        persisted.append((session, job, kwargs))

    marked = await terminal_states.mark_job_failed_if_recovery_safe(
        job_id,
        error_message="Failed to enqueue ingest job",
        session_maker_factory=_session_maker_factory(session),
        lock_job_source_for_terminal_mutation=lock_source,
        cancel_job_for_inactive_source=cancel_inactive,
        job_is_safe_recovery_failure_target=lambda _job: True,
        persist_job_failed=persist_failed,
        logger_instance=logger,
    )

    assert marked is True
    assert session.committed is True
    assert persisted == [
        (
            session,
            job,
            {
                "error_message": "Failed to enqueue ingest job",
                "error_code": ErrorCode.INTERNAL_ERROR,
                "error_details": None,
            },
        )
    ]
    assert logger.infos == []


@pytest.mark.asyncio
async def test_mark_job_failed_if_recovery_safe_skips_inactive_source() -> None:
    job_id = uuid.uuid4()
    session = _FakeSession()
    job = _FakeJob(id=job_id)
    locked_source = _FakeLockedSource(
        job=job,
        project=_FakeProject(deleted_at=datetime.now(UTC)),
        source_file=_FakeSourceFile(),
    )
    logger = _FakeLogger()
    cancelled: list[tuple[_FakeSession, _FakeJob, dict[str, Any]]] = []

    async def lock_source(session: _FakeSession, job_id: uuid.UUID) -> _FakeLockedSource:
        _ = job_id
        return locked_source

    async def cancel_inactive(session: _FakeSession, job: _FakeJob, **kwargs: Any) -> bool:
        cancelled.append((session, job, kwargs))
        return True

    async def persist_failed(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("inactive source should not be failed")

    marked = await terminal_states.mark_job_failed_if_recovery_safe(
        job_id,
        error_message="Failed to enqueue ingest job",
        session_maker_factory=_session_maker_factory(session),
        lock_job_source_for_terminal_mutation=lock_source,
        cancel_job_for_inactive_source=cancel_inactive,
        job_is_safe_recovery_failure_target=lambda _job: True,
        persist_job_failed=persist_failed,
        logger_instance=logger,
    )

    assert marked is False
    assert cancelled == [(session, job, {"reason": "source_deleted"})]
    assert logger.infos == [
        (
            "job_recovery_enqueue_failure_mark_skipped_inactive_source",
            {
                "job_id": str(job_id),
                "job_type": "ingest",
                "status": "pending",
            },
        )
    ]
