"""Persisted job lifecycle primitives for worker orchestration."""

import uuid
from collections.abc import Awaitable, Callable, Collection
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ErrorCode
from app.core.logging import get_logger
from app.db.session import get_session_maker
from app.models.file import File
from app.models.job import Job, JobType
from app.models.job_event import JobEvent
from app.models.project import Project

logger = get_logger("app.jobs.worker")


class _InactiveSourceError(Exception):
    """Raised when a job source project or file is no longer active."""


class _StaleJobAttemptError(Exception):
    """Raised when a worker attempt no longer owns the job lease."""


@dataclass(frozen=True, slots=True)
class _JobAttemptLease:
    """Persisted ownership token for a claimed job attempt."""

    token: UUID
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class _JobLockBootstrap:
    """Non-locking job metadata needed to acquire ordered row locks."""

    project_id: UUID
    file_id: UUID


@dataclass(frozen=True, slots=True)
class _LockedJobSource:
    """Project/job/file rows locked in the approved terminal-mutation order."""

    project: Project
    job: Job
    source_file: File | None


@dataclass(frozen=True, slots=True)
class _BeginOrResumeLogKeys:
    """Structured log keys for one begin/resume lifecycle entrypoint."""

    unsupported_type: str
    terminal_status: str
    inactive_source: str
    reclaimed_stale_running: str
    duplicate_delivery: str
    cancelled: str


def _utcnow() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(UTC)


def _clear_job_attempt_lease(job: Job) -> None:
    """Clear persisted ownership fencing for a job attempt."""
    job.attempt_token = None
    job.attempt_lease_expires_at = None


def _claim_job_attempt_lease(
    job: Job,
    *,
    now: datetime,
    increment_attempt: bool,
    stale_after: timedelta,
) -> _JobAttemptLease:
    """Mint and persist a fresh job-attempt ownership lease."""
    attempt_token = uuid.uuid4()
    lease_expires_at = now + stale_after

    if increment_attempt:
        job.attempts += 1

    job.status = "running"
    job.started_at = now
    job.finished_at = None
    job.error_code = None
    job.error_message = None
    job.attempt_token = attempt_token
    job.attempt_lease_expires_at = lease_expires_at

    return _JobAttemptLease(token=attempt_token, lease_expires_at=lease_expires_at)


def _job_attempt_is_current(job: Job, *, attempt_token: UUID) -> bool:
    """Return whether a worker still owns the persisted job attempt lease."""
    return job.status == "running" and job.attempt_token == attempt_token


def _is_stale_running_job(job: Job, *, now: datetime, stale_after: timedelta) -> bool:
    """Return whether a running job is old enough to treat as orphaned."""
    lease_expires_at = job.attempt_lease_expires_at
    if lease_expires_at is not None:
        if lease_expires_at.tzinfo is None:
            lease_expires_at = lease_expires_at.replace(tzinfo=UTC)
        return lease_expires_at <= now

    if job.started_at is None:
        return True

    started_at = job.started_at
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=UTC)

    return started_at <= now - stale_after


async def _get_job_for_update(session: AsyncSession, job_id: UUID) -> Job | None:
    """Load and lock a persisted job row."""
    return await _get_job_for_update_with_metadata(session, job_id)


async def _get_job_lock_bootstrap(
    session: AsyncSession,
    job_id: UUID,
) -> _JobLockBootstrap | None:
    """Load job metadata without taking locks."""
    result = await session.execute(select(Job.project_id, Job.file_id).where(Job.id == job_id))
    row = result.one_or_none()
    if row is None:
        return None

    project_id, file_id = row
    return _JobLockBootstrap(project_id=project_id, file_id=file_id)


async def _get_project(
    session: AsyncSession,
    project_id: UUID,
    *,
    for_update: bool = False,
) -> Project | None:
    """Load a persisted project row, optionally under a row lock."""
    statement = select(Project).where(Project.id == project_id)
    if for_update:
        statement = statement.with_for_update(of=Project)

    result = await session.execute(statement)
    return result.scalar_one_or_none()


async def _get_job_for_update_with_metadata(
    session: AsyncSession,
    job_id: UUID,
    *,
    expected_project_id: UUID | None = None,
    expected_file_id: UUID | None = None,
) -> Job | None:
    """Load and lock a persisted job row, revalidating stable metadata."""
    result = await session.execute(select(Job).where(Job.id == job_id).with_for_update(of=Job))
    job = result.scalar_one_or_none()
    if job is None:
        return None
    if expected_project_id is not None and job.project_id != expected_project_id:
        return None
    if expected_file_id is not None and job.file_id != expected_file_id:
        return None

    return job


async def _get_source_file(
    session: AsyncSession,
    *,
    project_id: UUID,
    file_id: UUID,
    for_update: bool = False,
) -> File | None:
    """Load a source file row, optionally under a row lock."""
    statement = select(File).where((File.project_id == project_id) & (File.id == file_id))
    if for_update:
        statement = statement.with_for_update(of=File)

    result = await session.execute(statement)
    return result.scalar_one_or_none()


async def _lock_job_source_for_terminal_mutation(
    session: AsyncSession,
    job_id: UUID,
    *,
    get_job_lock_bootstrap_func: Callable[..., Awaitable[_JobLockBootstrap | None]] | None = None,
    get_project_func: Callable[..., Awaitable[Project | None]] | None = None,
    get_job_for_update_with_metadata_func: Callable[..., Awaitable[Job | None]] | None = None,
    get_source_file_func: Callable[..., Awaitable[File | None]] | None = None,
) -> _LockedJobSource:
    """Lock project/job/file rows in the approved order for terminal writes."""
    get_job_lock_bootstrap_impl = get_job_lock_bootstrap_func or _get_job_lock_bootstrap
    get_project_impl = get_project_func or _get_project
    get_job_for_update_with_metadata_impl = (
        get_job_for_update_with_metadata_func or _get_job_for_update_with_metadata
    )
    get_source_file_impl = get_source_file_func or _get_source_file

    bootstrap = await get_job_lock_bootstrap_impl(session, job_id)
    if bootstrap is None:
        raise LookupError(f"Job with identifier '{job_id}' not found")

    project = await get_project_impl(session, bootstrap.project_id, for_update=True)
    if project is None:
        raise LookupError(
            f"Project with identifier '{bootstrap.project_id}' for job '{job_id}' not found"
        )

    job = await get_job_for_update_with_metadata_impl(
        session,
        job_id,
        expected_project_id=bootstrap.project_id,
        expected_file_id=bootstrap.file_id,
    )
    if job is None:
        raise LookupError(f"Job with identifier '{job_id}' not found")

    source_file = await get_source_file_impl(
        session,
        project_id=bootstrap.project_id,
        file_id=bootstrap.file_id,
        for_update=True,
    )
    return _LockedJobSource(project=project, job=job, source_file=source_file)


async def emit_job_event(
    job_id: UUID,
    *,
    level: str,
    message: str,
    data_json: dict[str, Any] | None = None,
    attempt_token: UUID | None = None,
    session: AsyncSession | None = None,
    get_job_for_update_func: Callable[[AsyncSession, UUID], Awaitable[Job | None]] | None = None,
    job_attempt_is_current_func: Callable[..., bool] | None = None,
    session_maker_factory: Callable[[], Any] | None = None,
) -> bool:
    """Persist a job lifecycle event."""
    get_job_for_update_impl = get_job_for_update_func or _get_job_for_update
    job_attempt_is_current_impl = job_attempt_is_current_func or _job_attempt_is_current
    event = JobEvent(
        job_id=job_id,
        level=level,
        message=message,
        data_json=data_json,
    )
    if session is not None:
        if attempt_token is not None:
            job = await get_job_for_update_impl(session, job_id)
            if job is None:
                raise LookupError(f"Job with identifier '{job_id}' not found")
            if not job_attempt_is_current_impl(job, attempt_token=attempt_token):
                return False
        session.add(event)
        return True

    session_maker = (session_maker_factory or get_session_maker)()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as managed_session:
        if attempt_token is not None:
            job = await get_job_for_update_impl(managed_session, job_id)
            if job is None:
                raise LookupError(f"Job with identifier '{job_id}' not found")
            if not job_attempt_is_current_impl(job, attempt_token=attempt_token):
                return False
        managed_session.add(event)
        await managed_session.commit()

    return True


def _finalize_job_cancelled(
    job: Job,
    *,
    cancelled_error_code: str = ErrorCode.JOB_CANCELLED.value,
    utcnow_func: Callable[[], datetime] | None = None,
    clear_job_attempt_lease_func: Callable[[Job], None] | None = None,
) -> None:
    """Apply the persisted cancelled terminal state to a job."""
    job.status = "cancelled"
    job.error_code = cancelled_error_code
    job.error_message = None
    job.finished_at = (utcnow_func or _utcnow)()
    (clear_job_attempt_lease_func or _clear_job_attempt_lease)(job)


async def _cancel_job_for_inactive_source(
    session: AsyncSession,
    job: Job,
    *,
    reason: str,
    attempt_token: UUID | None = None,
    terminal_job_statuses: Collection[str],
    job_attempt_is_current_func: Callable[..., bool] | None = None,
    finalize_job_cancelled_func: Callable[[Job], None] | None = None,
    emit_job_event_func: Callable[..., Awaitable[bool]] | None = None,
) -> bool:
    """Persist cancellation when a job source project/file is no longer active."""
    if job.status in terminal_job_statuses:
        return False

    job_attempt_is_current_impl = job_attempt_is_current_func or _job_attempt_is_current
    if attempt_token is not None and not job_attempt_is_current_impl(
        job,
        attempt_token=attempt_token,
    ):
        return False

    job.cancel_requested = True
    (finalize_job_cancelled_func or _finalize_job_cancelled)(job)
    await (emit_job_event_func or emit_job_event)(
        job.id,
        level="warning",
        message="Job cancelled",
        data_json={"status": "cancelled", "reason": reason},
        session=session,
    )
    await session.commit()

    return True


async def _persist_job_failed(
    session: AsyncSession,
    job: Job,
    *,
    error_message: str,
    error_code: ErrorCode,
    error_details: dict[str, Any] | None = None,
    utcnow_func: Callable[[], datetime] | None = None,
    clear_job_attempt_lease_func: Callable[[Job], None] | None = None,
    emit_job_event_func: Callable[..., Awaitable[bool]] | None = None,
) -> None:
    """Persist a failed job state and matching event within an active session."""
    job.status = "failed"
    job.error_code = error_code.value
    job.error_message = error_message
    job.finished_at = (utcnow_func or _utcnow)()
    (clear_job_attempt_lease_func or _clear_job_attempt_lease)(job)
    await (emit_job_event_func or emit_job_event)(
        job.id,
        level="error",
        message="Job failed",
        data_json={
            "status": "failed",
            "error_code": error_code.value,
            "error_message": error_message,
            **({"details": error_details} if error_details is not None else {}),
        },
        session=session,
    )


async def _mark_job_failed(
    job_id: UUID,
    *,
    error_message: str,
    error_code: ErrorCode,
    terminal_job_statuses: Collection[str],
    attempt_token: UUID | None = None,
    error_details: dict[str, Any] | None = None,
    session_maker_factory: Callable[[], Any] | None = None,
    lock_job_source_for_terminal_mutation_func: (
        Callable[..., Awaitable[_LockedJobSource]] | None
    ) = None,
    job_attempt_is_current_func: Callable[..., bool] | None = None,
    cancel_job_for_inactive_source_func: Callable[..., Awaitable[bool]] | None = None,
    finalize_job_cancelled_func: Callable[[Job], None] | None = None,
    persist_job_failed_func: Callable[..., Awaitable[None]] | None = None,
    emit_job_event_func: Callable[..., Awaitable[bool]] | None = None,
    logger_instance: Any | None = None,
) -> bool:
    """Persist a failed job state with the supplied message."""
    session_maker = (session_maker_factory or get_session_maker)()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    lock_job_source_for_terminal_mutation_impl = (
        lock_job_source_for_terminal_mutation_func or _lock_job_source_for_terminal_mutation
    )
    job_attempt_is_current_impl = job_attempt_is_current_func or _job_attempt_is_current
    if cancel_job_for_inactive_source_func is None:

        async def _default_cancel_job_for_inactive_source(
            session: AsyncSession,
            job: Job,
            *,
            reason: str,
            attempt_token: UUID | None = None,
        ) -> bool:
            return await _cancel_job_for_inactive_source(
                session,
                job,
                reason=reason,
                attempt_token=attempt_token,
                terminal_job_statuses=terminal_job_statuses,
            )

        cancel_job_for_inactive_source_impl: Callable[..., Awaitable[bool]] = (
            _default_cancel_job_for_inactive_source
        )

    else:
        cancel_job_for_inactive_source_impl = cancel_job_for_inactive_source_func
    finalize_job_cancelled_impl = finalize_job_cancelled_func or _finalize_job_cancelled
    persist_job_failed_impl = persist_job_failed_func or _persist_job_failed
    emit_job_event_impl = emit_job_event_func or emit_job_event
    logger_impl = logger_instance or logger

    async with session_maker() as session:
        locked_source = await lock_job_source_for_terminal_mutation_impl(session, job_id)
        job = locked_source.job

        if job.status in terminal_job_statuses:
            logger_impl.info(
                "ingest_job_failure_mark_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False
        if attempt_token is not None and not job_attempt_is_current_impl(
            job,
            attempt_token=attempt_token,
        ):
            logger_impl.info(
                "ingest_job_failure_mark_skipped_stale_attempt",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if (
            locked_source.project.deleted_at is not None
            or locked_source.source_file is None
            or locked_source.source_file.deleted_at is not None
        ):
            await cancel_job_for_inactive_source_impl(
                session,
                job,
                reason="source_deleted",
                attempt_token=attempt_token,
            )
            logger_impl.info(
                "ingest_job_failure_mark_skipped_inactive_source",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if attempt_token is not None and job.cancel_requested:
            finalize_job_cancelled_impl(job)
            await emit_job_event_impl(
                job.id,
                level="warning",
                message="Job cancelled",
                data_json={"status": "cancelled"},
                session=session,
            )
            await session.commit()
            logger_impl.info(
                "ingest_job_failure_mark_preferred_cancelled",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        await persist_job_failed_impl(
            session,
            job,
            error_message=error_message,
            error_code=error_code,
            error_details=error_details,
        )
        await session.commit()

    return True


async def _mark_job_cancelled(
    job_id: UUID,
    *,
    terminal_job_statuses: Collection[str],
    attempt_token: UUID | None = None,
    session_maker_factory: Callable[[], Any] | None = None,
    get_job_for_update_func: Callable[[AsyncSession, UUID], Awaitable[Job | None]] | None = None,
    job_attempt_is_current_func: Callable[..., bool] | None = None,
    finalize_job_cancelled_func: Callable[[Job], None] | None = None,
    emit_job_event_func: Callable[..., Awaitable[bool]] | None = None,
    logger_instance: Any | None = None,
) -> bool:
    """Persist a cancelled job state."""
    session_maker = (session_maker_factory or get_session_maker)()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    get_job_for_update_impl = get_job_for_update_func or _get_job_for_update
    job_attempt_is_current_impl = job_attempt_is_current_func or _job_attempt_is_current
    finalize_job_cancelled_impl = finalize_job_cancelled_func or _finalize_job_cancelled
    emit_job_event_impl = emit_job_event_func or emit_job_event
    logger_impl = logger_instance or logger

    async with session_maker() as session:
        job = await get_job_for_update_impl(session, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if job.status in terminal_job_statuses:
            logger_impl.info(
                "ingest_job_cancel_mark_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if attempt_token is not None and not job_attempt_is_current_impl(
            job,
            attempt_token=attempt_token,
        ):
            logger_impl.info(
                "ingest_job_cancel_mark_skipped_stale_attempt",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        finalize_job_cancelled_impl(job)
        await emit_job_event_impl(
            job_id,
            level="warning",
            message="Job cancelled",
            data_json={"status": "cancelled"},
            session=session,
        )
        await session.commit()

    return True


async def _begin_or_resume_job(
    job_id: UUID,
    *,
    supported_job_types: Collection[str],
    terminal_job_statuses: Collection[str],
    stale_after: timedelta,
    log_keys: _BeginOrResumeLogKeys,
    session_maker_factory: Callable[[], Any] | None = None,
    lock_job_source_for_terminal_mutation_func: (
        Callable[..., Awaitable[_LockedJobSource]] | None
    ) = None,
    cancel_job_for_inactive_source_func: Callable[..., Awaitable[bool]] | None = None,
    claim_job_attempt_lease_func: Callable[..., _JobAttemptLease] | None = None,
    is_stale_running_job_func: Callable[..., bool] | None = None,
    finalize_job_cancelled_func: Callable[[Job], None] | None = None,
    emit_job_event_func: Callable[..., Awaitable[bool]] | None = None,
    logger_instance: Any | None = None,
) -> _JobAttemptLease | None:
    """Claim, resume, or cancel a persisted worker job under ordered row locks."""
    session_maker = (session_maker_factory or get_session_maker)()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    now = _utcnow()
    lock_job_source_for_terminal_mutation_impl = (
        lock_job_source_for_terminal_mutation_func or _lock_job_source_for_terminal_mutation
    )
    if cancel_job_for_inactive_source_func is None:

        async def _default_cancel_job_for_inactive_source(
            session: AsyncSession,
            job: Job,
            *,
            reason: str,
            attempt_token: UUID | None = None,
        ) -> bool:
            return await _cancel_job_for_inactive_source(
                session,
                job,
                reason=reason,
                attempt_token=attempt_token,
                terminal_job_statuses=terminal_job_statuses,
            )

        cancel_job_for_inactive_source_impl: Callable[..., Awaitable[bool]] = (
            _default_cancel_job_for_inactive_source
        )

    else:
        cancel_job_for_inactive_source_impl = cancel_job_for_inactive_source_func
    claim_job_attempt_lease_impl = claim_job_attempt_lease_func or _claim_job_attempt_lease
    is_stale_running_job_impl = is_stale_running_job_func or _is_stale_running_job
    finalize_job_cancelled_impl = finalize_job_cancelled_func or _finalize_job_cancelled
    emit_job_event_impl = emit_job_event_func or emit_job_event
    logger_impl = logger_instance or logger

    async with session_maker() as session:
        locked_source = await lock_job_source_for_terminal_mutation_impl(session, job_id)
        project = locked_source.project
        job = locked_source.job
        source_file = locked_source.source_file

        if job.job_type not in supported_job_types:
            logger_impl.info(
                log_keys.unsupported_type,
                job_id=str(job_id),
                job_type=job.job_type,
                status=job.status,
            )
            return None

        if job.status in terminal_job_statuses:
            logger_impl.info(
                log_keys.terminal_status,
                job_id=str(job_id),
                status=job.status,
            )
            return None

        if project.deleted_at is not None:
            await cancel_job_for_inactive_source_impl(
                session,
                job,
                reason="source_deleted",
            )
            logger_impl.info(log_keys.inactive_source, job_id=str(job_id))
            return None

        if source_file is None or source_file.deleted_at is not None:
            await cancel_job_for_inactive_source_impl(
                session,
                job,
                reason="source_deleted",
            )
            logger_impl.info(log_keys.inactive_source, job_id=str(job_id))
            return None

        if not job.cancel_requested:
            if job.status == "running":
                if is_stale_running_job_impl(job, now=now, stale_after=stale_after):
                    lease = claim_job_attempt_lease_impl(
                        job,
                        now=now,
                        increment_attempt=True,
                        stale_after=stale_after,
                    )
                    await emit_job_event_impl(
                        job.id,
                        level="info",
                        message="Job started",
                        data_json={
                            "status": "running",
                            "attempts": job.attempts,
                            "reclaimed": True,
                        },
                        session=session,
                    )
                    await session.commit()
                    logger_impl.warning(
                        log_keys.reclaimed_stale_running,
                        job_id=str(job_id),
                        status=job.status,
                    )
                    return lease

                logger_impl.info(
                    log_keys.duplicate_delivery,
                    job_id=str(job_id),
                    status=job.status,
                )
                return None

            lease = claim_job_attempt_lease_impl(
                job,
                now=now,
                increment_attempt=True,
                stale_after=stale_after,
            )
            await emit_job_event_impl(
                job.id,
                level="info",
                message="Job started",
                data_json={"status": "running", "attempts": job.attempts, "reclaimed": False},
                session=session,
            )
            await session.commit()
            return lease

        finalize_job_cancelled_impl(job)
        await emit_job_event_impl(
            job.id,
            level="warning",
            message="Job cancelled",
            data_json={"status": "cancelled"},
            session=session,
        )
        await session.commit()

    logger_impl.info(log_keys.cancelled, job_id=str(job_id))
    return None


async def _begin_or_resume_ingest_job(
    job_id: UUID,
    *,
    terminal_job_statuses: Collection[str],
    stale_after: timedelta,
    session_maker_factory: Callable[[], Any] | None = None,
    lock_job_source_for_terminal_mutation_func: (
        Callable[..., Awaitable[_LockedJobSource]] | None
    ) = None,
    cancel_job_for_inactive_source_func: Callable[..., Awaitable[bool]] | None = None,
    claim_job_attempt_lease_func: Callable[..., _JobAttemptLease] | None = None,
    is_stale_running_job_func: Callable[..., bool] | None = None,
    finalize_job_cancelled_func: Callable[[Job], None] | None = None,
    emit_job_event_func: Callable[..., Awaitable[bool]] | None = None,
    logger_instance: Any | None = None,
) -> _JobAttemptLease | None:
    """Claim, resume, or cancel a persisted ingest job under a row lock."""
    return await _begin_or_resume_job(
        job_id,
        supported_job_types={JobType.INGEST.value, JobType.REPROCESS.value},
        terminal_job_statuses=terminal_job_statuses,
        stale_after=stale_after,
        log_keys=_BeginOrResumeLogKeys(
            unsupported_type="ingest_job_unsupported_type_skipped",
            terminal_status="ingest_job_cancel_skipped_terminal_status",
            inactive_source="ingest_job_cancelled_inactive_source",
            reclaimed_stale_running="ingest_job_reclaimed_stale_running_status",
            duplicate_delivery="ingest_job_duplicate_delivery_skipped_running_attempt",
            cancelled="ingest_job_cancelled",
        ),
        session_maker_factory=session_maker_factory,
        lock_job_source_for_terminal_mutation_func=lock_job_source_for_terminal_mutation_func,
        cancel_job_for_inactive_source_func=cancel_job_for_inactive_source_func,
        claim_job_attempt_lease_func=claim_job_attempt_lease_func,
        is_stale_running_job_func=is_stale_running_job_func,
        finalize_job_cancelled_func=finalize_job_cancelled_func,
        emit_job_event_func=emit_job_event_func,
        logger_instance=logger_instance,
    )


async def _begin_or_resume_quantity_takeoff_job(
    job_id: UUID,
    *,
    terminal_job_statuses: Collection[str],
    stale_after: timedelta,
    session_maker_factory: Callable[[], Any] | None = None,
    lock_job_source_for_terminal_mutation_func: (
        Callable[..., Awaitable[_LockedJobSource]] | None
    ) = None,
    cancel_job_for_inactive_source_func: Callable[..., Awaitable[bool]] | None = None,
    claim_job_attempt_lease_func: Callable[..., _JobAttemptLease] | None = None,
    is_stale_running_job_func: Callable[..., bool] | None = None,
    finalize_job_cancelled_func: Callable[[Job], None] | None = None,
    emit_job_event_func: Callable[..., Awaitable[bool]] | None = None,
    logger_instance: Any | None = None,
) -> _JobAttemptLease | None:
    """Claim, resume, or cancel a persisted quantity takeoff job under a row lock."""
    return await _begin_or_resume_job(
        job_id,
        supported_job_types={JobType.QUANTITY_TAKEOFF.value},
        terminal_job_statuses=terminal_job_statuses,
        stale_after=stale_after,
        log_keys=_BeginOrResumeLogKeys(
            unsupported_type="quantity_takeoff_job_unsupported_type_skipped",
            terminal_status="quantity_takeoff_job_skipped_terminal_status",
            inactive_source="quantity_takeoff_job_cancelled_inactive_source",
            reclaimed_stale_running="quantity_takeoff_job_reclaimed_stale_running_status",
            duplicate_delivery="quantity_takeoff_job_duplicate_delivery_skipped_running_attempt",
            cancelled="quantity_takeoff_job_cancelled",
        ),
        session_maker_factory=session_maker_factory,
        lock_job_source_for_terminal_mutation_func=lock_job_source_for_terminal_mutation_func,
        cancel_job_for_inactive_source_func=cancel_job_for_inactive_source_func,
        claim_job_attempt_lease_func=claim_job_attempt_lease_func,
        is_stale_running_job_func=is_stale_running_job_func,
        finalize_job_cancelled_func=finalize_job_cancelled_func,
        emit_job_event_func=emit_job_event_func,
        logger_instance=logger_instance,
    )


async def _begin_or_resume_estimate_job(
    job_id: UUID,
    *,
    terminal_job_statuses: Collection[str],
    stale_after: timedelta,
    session_maker_factory: Callable[[], Any] | None = None,
    lock_job_source_for_terminal_mutation_func: (
        Callable[..., Awaitable[_LockedJobSource]] | None
    ) = None,
    cancel_job_for_inactive_source_func: Callable[..., Awaitable[bool]] | None = None,
    claim_job_attempt_lease_func: Callable[..., _JobAttemptLease] | None = None,
    is_stale_running_job_func: Callable[..., bool] | None = None,
    finalize_job_cancelled_func: Callable[[Job], None] | None = None,
    emit_job_event_func: Callable[..., Awaitable[bool]] | None = None,
    logger_instance: Any | None = None,
) -> _JobAttemptLease | None:
    """Claim, resume, or cancel a persisted estimate job under a row lock."""
    return await _begin_or_resume_job(
        job_id,
        supported_job_types={JobType.ESTIMATE.value},
        terminal_job_statuses=terminal_job_statuses,
        stale_after=stale_after,
        log_keys=_BeginOrResumeLogKeys(
            unsupported_type="estimate_job_unsupported_type_skipped",
            terminal_status="estimate_job_skipped_terminal_status",
            inactive_source="estimate_job_cancelled_inactive_source",
            reclaimed_stale_running="estimate_job_reclaimed_stale_running_status",
            duplicate_delivery="estimate_job_duplicate_delivery_skipped_running_attempt",
            cancelled="estimate_job_cancelled",
        ),
        session_maker_factory=session_maker_factory,
        lock_job_source_for_terminal_mutation_func=lock_job_source_for_terminal_mutation_func,
        cancel_job_for_inactive_source_func=cancel_job_for_inactive_source_func,
        claim_job_attempt_lease_func=claim_job_attempt_lease_func,
        is_stale_running_job_func=is_stale_running_job_func,
        finalize_job_cancelled_func=finalize_job_cancelled_func,
        emit_job_event_func=emit_job_event_func,
        logger_instance=logger_instance,
    )


async def _begin_or_resume_export_job(
    job_id: UUID,
    *,
    terminal_job_statuses: Collection[str],
    stale_after: timedelta,
    session_maker_factory: Callable[[], Any] | None = None,
    lock_job_source_for_terminal_mutation_func: (
        Callable[..., Awaitable[_LockedJobSource]] | None
    ) = None,
    cancel_job_for_inactive_source_func: Callable[..., Awaitable[bool]] | None = None,
    claim_job_attempt_lease_func: Callable[..., _JobAttemptLease] | None = None,
    is_stale_running_job_func: Callable[..., bool] | None = None,
    finalize_job_cancelled_func: Callable[[Job], None] | None = None,
    emit_job_event_func: Callable[..., Awaitable[bool]] | None = None,
    logger_instance: Any | None = None,
) -> _JobAttemptLease | None:
    """Claim, resume, or cancel a persisted export job under a row lock."""
    return await _begin_or_resume_job(
        job_id,
        supported_job_types={JobType.EXPORT.value},
        terminal_job_statuses=terminal_job_statuses,
        stale_after=stale_after,
        log_keys=_BeginOrResumeLogKeys(
            unsupported_type="export_job_unsupported_type_skipped",
            terminal_status="export_job_skipped_terminal_status",
            inactive_source="export_job_cancelled_inactive_source",
            reclaimed_stale_running="export_job_reclaimed_stale_running_status",
            duplicate_delivery="export_job_duplicate_delivery_skipped_running_attempt",
            cancelled="export_job_cancelled",
        ),
        session_maker_factory=session_maker_factory,
        lock_job_source_for_terminal_mutation_func=lock_job_source_for_terminal_mutation_func,
        cancel_job_for_inactive_source_func=cancel_job_for_inactive_source_func,
        claim_job_attempt_lease_func=claim_job_attempt_lease_func,
        is_stale_running_job_func=is_stale_running_job_func,
        finalize_job_cancelled_func=finalize_job_cancelled_func,
        emit_job_event_func=emit_job_event_func,
        logger_instance=logger_instance,
    )
