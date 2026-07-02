"""Persisted job recovery and durable enqueue-intent seam.

Owns the pending/running staleness recovery sweep that runs on worker startup
(``recover_incomplete_jobs``) plus the durable enqueue-intent lifecycle
(claim/publish/release) that backs at-least-once broker publication for
recoverable job types.

#841: ``reap_stale_running_jobs()`` lives here, beside the staleness detection
primitives it depends on (``_is_stale_running_job`` / lease staleness in
``app.jobs.lifecycle``). It is called from a celery ``task_failure`` handler
in ``app.jobs.worker`` when the failure is a ``WorkerLostError`` (fork
SIGKILLed mid-job), now that the #850 heartbeat lease makes attempt staleness
detectable without waiting for the coarse ``recover_incomplete_jobs`` startup
sweep — a job orphaned by a crashed fork on a MainProcess that survives is
otherwise never reaped, since worker-start recovery only runs once, at
startup.
"""

import uuid
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ErrorCode
from app.core.logging import get_logger
from app.db.session import get_session_maker
from app.jobs import runner as job_runner
from app.jobs.lifecycle import (
    _clear_job_attempt_lease,
    _get_job_for_update,
    _is_stale_running_job,
    _LockedJobSource,
    _persist_job_failed,
    _utcnow,
)
from app.models.job import Job

logger = get_logger("app.jobs.worker")

# Staleness home: a running job whose attempt lease has expired (or, absent a
# lease, whose ``started_at`` predates this window) is treated as orphaned by
# worker-startup recovery. INVARIANT: keep this equal to 2x the default adapter
# timeout (``_DEFAULT_ADAPTER_TIMEOUT`` in ``app.jobs.worker``, 5 min) — it was
# literally ``_DEFAULT_ADAPTER_TIMEOUT * 2`` before the #851 extraction; inlined
# here to avoid a worker import. If the adapter default changes, change this too.
# worker.py aliases this back so ``worker_module._RUNNING_JOB_STALE_AFTER`` resolves.
_RUNNING_JOB_STALE_AFTER = timedelta(minutes=10)

_ENQUEUE_STATUS_PENDING = "pending"
_ENQUEUE_STATUS_PUBLISHING = "publishing"
_ENQUEUE_STATUS_PUBLISHED = "published"

_ENQUEUE_LEASE_DURATION = timedelta(minutes=1)

# Capped exponential backoff between job attempts. The first attempt runs with no
# delay; each subsequent re-enqueue waits base * 2**(attempt-1), capped, so a job
# that keeps failing is spaced out instead of being retried as fast as the broker
# can redeliver it (the attempt count is still bounded by ``Job.max_attempts``).
_ENQUEUE_BACKOFF_BASE_SECONDS = 5.0
_ENQUEUE_BACKOFF_MAX_SECONDS = 300.0


def _enqueue_backoff_seconds(attempts: int) -> float:
    """Return the retry delay for a job that has already made ``attempts`` tries."""
    if attempts <= 1:
        return 0.0
    delay = _ENQUEUE_BACKOFF_BASE_SECONDS * (2.0 ** (attempts - 2))
    return min(delay, _ENQUEUE_BACKOFF_MAX_SECONDS)


# Carries the computed backoff to the real ``enqueue_*`` publishers without changing
# their ``(job_id)`` call signature (test fakes and direct calls see the 0.0 default).
_ENQUEUE_COUNTDOWN_SECONDS: ContextVar[float] = ContextVar("enqueue_countdown_seconds", default=0.0)


def _current_enqueue_countdown() -> float | None:
    """Return the active enqueue backoff in seconds, or None when there is none."""
    countdown = _ENQUEUE_COUNTDOWN_SECONDS.get()
    return countdown if countdown > 0.0 else None


@dataclass(frozen=True, slots=True)
class _EnqueueIntentLease:
    """Persisted ownership token for a claimed durable enqueue intent."""

    token: UUID
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class _ClaimedJobEnqueueIntent:
    """Persisted enqueue claim bundled with the routed worker job type."""

    lease: _EnqueueIntentLease
    job_type: str
    attempts: int


def _clear_enqueue_intent_lease(job: Job) -> None:
    """Clear persisted ownership fencing for a durable enqueue intent."""
    job.enqueue_owner_token = None
    job.enqueue_lease_expires_at = None


def prepare_job_enqueue_intent(job: Job) -> None:
    """Reset a job's durable enqueue intent to the pending outbox state."""
    job.enqueue_status = _ENQUEUE_STATUS_PENDING
    job.enqueue_attempts = 0
    job.enqueue_last_attempted_at = None
    job.enqueue_published_at = None
    _clear_enqueue_intent_lease(job)


def _job_is_safe_recovery_failure_target(job: Job) -> bool:
    """Return whether recovery can still safely mark the job failed."""
    return (
        job.status == "pending"
        and job.attempt_token is None
        and job.attempt_lease_expires_at is None
        and job.enqueue_status == _ENQUEUE_STATUS_PENDING
        and job.enqueue_owner_token is None
        and job.enqueue_lease_expires_at is None
    )


def _is_stale_enqueue_intent(job: Job, *, now: datetime) -> bool:
    """Return whether an in-flight enqueue publish claim can be reclaimed."""
    lease_expires_at = job.enqueue_lease_expires_at
    if lease_expires_at is None:
        return True
    if lease_expires_at.tzinfo is None:
        lease_expires_at = lease_expires_at.replace(tzinfo=UTC)
    return lease_expires_at <= now


def _claim_enqueue_intent_lease(job: Job, *, now: datetime) -> _EnqueueIntentLease:
    """Mint and persist a fresh ownership lease for broker publication."""
    token = uuid.uuid4()
    lease_expires_at = now + _ENQUEUE_LEASE_DURATION
    job.enqueue_status = _ENQUEUE_STATUS_PUBLISHING
    job.enqueue_attempts += 1
    job.enqueue_owner_token = token
    job.enqueue_lease_expires_at = lease_expires_at
    job.enqueue_last_attempted_at = now
    return _EnqueueIntentLease(token=token, lease_expires_at=lease_expires_at)


def _get_enqueue_job_error_message(job_type: str) -> str:
    """Return the persisted enqueue failure message for a worker job type."""
    return job_runner.ENQUEUE_ERROR_MESSAGES_BY_JOB_TYPE.get(job_type, "Failed to enqueue job")


async def _mark_job_failed_if_recovery_safe(
    job_id: UUID,
    *,
    error_message: str,
    error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
    error_details: dict[str, Any] | None = None,
    lock_job_source_for_terminal_mutation_func: Callable[
        [AsyncSession, UUID], Awaitable[_LockedJobSource]
    ],
    cancel_job_for_inactive_source_func: Callable[..., Awaitable[bool]],
    persist_job_failed_func: Callable[..., Awaitable[None]],
    logger_instance: Any | None = None,
) -> bool:
    """Fail a recovered job only if it is still pending and unowned."""
    logger_impl = logger_instance or logger
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        locked_source = await lock_job_source_for_terminal_mutation_func(session, job_id)
        job = locked_source.job

        if (
            locked_source.project.deleted_at is not None
            or locked_source.source_file is None
            or locked_source.source_file.deleted_at is not None
        ):
            await cancel_job_for_inactive_source_func(
                session,
                job,
                reason="source_deleted",
            )
            logger_impl.info(
                "job_recovery_enqueue_failure_mark_skipped_inactive_source",
                job_id=str(job_id),
                job_type=job.job_type,
                status=job.status,
            )
            return False

        if not _job_is_safe_recovery_failure_target(job):
            logger_impl.info(
                "job_recovery_enqueue_failure_mark_skipped_changed_state",
                job_id=str(job_id),
                job_type=job.job_type,
                status=job.status,
            )
            return False

        await persist_job_failed_func(
            session,
            job,
            error_message=error_message,
            error_code=error_code,
            error_details=error_details,
        )
        await session.commit()

    return True


async def _claim_job_enqueue_intent(
    job_id: UUID,
    *,
    is_recoverable_enqueue_job_type_func: Callable[[str], bool],
) -> _ClaimedJobEnqueueIntent | None:
    """Claim a durable enqueue intent for best-effort or recovery publication."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    now = _utcnow()
    async with session_maker() as session:
        job = await _get_job_for_update(session, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if not is_recoverable_enqueue_job_type_func(job.job_type) or job.status != "pending":
            return None

        if job.enqueue_status == _ENQUEUE_STATUS_PUBLISHED:
            return None

        if job.enqueue_status == _ENQUEUE_STATUS_PUBLISHING and not _is_stale_enqueue_intent(
            job,
            now=now,
        ):
            return None

        lease = _claim_enqueue_intent_lease(job, now=now)
        attempts = job.attempts
        await session.commit()
        return _ClaimedJobEnqueueIntent(lease=lease, job_type=job.job_type, attempts=attempts)


async def _release_job_enqueue_intent(job_id: UUID, *, lease_token: UUID) -> bool:
    """Release a claimed durable enqueue intent after a publish failure."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await _get_job_for_update(session, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if (
            job.enqueue_status != _ENQUEUE_STATUS_PUBLISHING
            or job.enqueue_owner_token != lease_token
        ):
            return False

        if job.status == "pending":
            job.enqueue_status = _ENQUEUE_STATUS_PENDING
            _clear_enqueue_intent_lease(job)
            await session.commit()
            return True

        job.enqueue_status = _ENQUEUE_STATUS_PUBLISHED
        job.enqueue_published_at = _utcnow()
        _clear_enqueue_intent_lease(job)
        await session.commit()
        return True


async def _mark_job_enqueue_published(job_id: UUID, *, lease_token: UUID) -> bool:
    """Finalize a claimed durable enqueue intent after broker publication."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await _get_job_for_update(session, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if (
            job.enqueue_status != _ENQUEUE_STATUS_PUBLISHING
            or job.enqueue_owner_token != lease_token
        ):
            return False

        job.enqueue_status = _ENQUEUE_STATUS_PUBLISHED
        job.enqueue_published_at = _utcnow()
        _clear_enqueue_intent_lease(job)
        await session.commit()
        return True


async def _mark_recovery_enqueue_failed(
    job_id: UUID,
    *,
    job_type: str,
    mark_job_failed_if_recovery_safe_func: Callable[..., Awaitable[bool]],
    logger_instance: Any | None = None,
) -> bool:
    """Persist and log a sanitized worker-recovery enqueue failure."""
    logger_impl = logger_instance or logger
    marked_failed = await mark_job_failed_if_recovery_safe_func(
        job_id,
        error_message=_get_enqueue_job_error_message(job_type),
    )
    if not marked_failed:
        return False

    logger_impl.error(
        "job_recovery_enqueue_failed",
        job_id=str(job_id),
        job_type=job_type,
        error_code=ErrorCode.INTERNAL_ERROR.value,
        recovery_action="mark_failed",
    )
    return True


async def publish_job_enqueue_intent(
    job_id: UUID,
    *,
    recovery: bool = False,
    publisher: Callable[[UUID], None] | None = None,
    suppress_exceptions: bool = False,
    publisher_resolver: Callable[[str], Callable[[UUID], None] | None],
    claim_job_enqueue_intent_func: Callable[[UUID], Awaitable[_ClaimedJobEnqueueIntent | None]],
    release_job_enqueue_intent_func: Callable[..., Awaitable[bool]],
    mark_job_enqueue_published_func: Callable[..., Awaitable[bool]],
    mark_recovery_enqueue_failed_func: Callable[..., Awaitable[bool]],
) -> bool:
    """Best-effort publish for a durable enqueue intent recorded in Postgres."""
    claimed_intent: _ClaimedJobEnqueueIntent | None = None
    try:
        claimed_intent = await claim_job_enqueue_intent_func(job_id)
        if claimed_intent is None:
            return False

        publish = publisher or publisher_resolver(claimed_intent.job_type)
        if publish is None:
            await release_job_enqueue_intent_func(job_id, lease_token=claimed_intent.lease.token)
            return False
        countdown_token = _ENQUEUE_COUNTDOWN_SECONDS.set(
            _enqueue_backoff_seconds(claimed_intent.attempts)
        )
        try:
            publish(job_id)
        except Exception:
            _ENQUEUE_COUNTDOWN_SECONDS.reset(countdown_token)
            await release_job_enqueue_intent_func(job_id, lease_token=claimed_intent.lease.token)
            if recovery:
                await mark_recovery_enqueue_failed_func(
                    job_id,
                    job_type=claimed_intent.job_type,
                )
            else:
                logger.warning(
                    "job_enqueue_deferred",
                    job_id=str(job_id),
                    job_type=claimed_intent.job_type,
                    recovery_action="worker_start_recovery",
                )
            return False
        else:
            _ENQUEUE_COUNTDOWN_SECONDS.reset(countdown_token)

        published = await mark_job_enqueue_published_func(
            job_id, lease_token=claimed_intent.lease.token
        )
        if not published:
            # The broker publish already succeeded; the enqueue intent is at-least-once and
            # job execution is idempotent, so we still report success. But the PUBLISHED
            # transition was skipped because another owner reclaimed the lease (or the status
            # moved off PUBLISHING) between claim and mark — surface that race for observability.
            logger.warning(
                "job_enqueue_publish_mark_skipped",
                job_id=str(job_id),
                job_type=claimed_intent.job_type,
                reason="stale_enqueue_lease",
            )
        return True
    except Exception:
        if not suppress_exceptions:
            raise

        logger.warning(
            "job_enqueue_deferred",
            job_id=str(job_id),
            job_type=claimed_intent.job_type if claimed_intent is not None else None,
            recovery_action="worker_start_recovery",
        )
        return False


async def recover_incomplete_jobs(
    *,
    recoverable_enqueue_job_types: Any,
    publish_job_enqueue_intent_func: Callable[..., Awaitable[bool]],
) -> list[UUID]:
    """Requeue incomplete persisted worker jobs on worker startup."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    now = _utcnow()

    async with session_maker() as session:
        result = await session.execute(
            select(Job)
            .where(
                (Job.job_type.in_(recoverable_enqueue_job_types))
                & (
                    (Job.status == "running")
                    | (
                        (Job.status == "pending")
                        & (
                            Job.enqueue_status.in_(
                                (_ENQUEUE_STATUS_PENDING, _ENQUEUE_STATUS_PUBLISHING)
                            )
                        )
                    )
                )
            )
            .order_by(Job.created_at.asc(), Job.id.asc())
            .with_for_update(skip_locked=True)
        )
        jobs = result.scalars().all()

        recovered_job_ids: list[UUID] = []
        for job in jobs:
            if job.status == "running":
                if not _is_stale_running_job(job, now=now, stale_after=_RUNNING_JOB_STALE_AFTER):
                    continue
                job.status = "pending"
                job.started_at = None
                job.finished_at = None
                job.error_code = None
                job.error_message = None
                _clear_job_attempt_lease(job)
                prepare_job_enqueue_intent(job)
                recovered_job_ids.append(job.id)
                continue

            if job.enqueue_status == _ENQUEUE_STATUS_PUBLISHING and not _is_stale_enqueue_intent(
                job,
                now=now,
            ):
                continue

            if job.enqueue_status == _ENQUEUE_STATUS_PUBLISHING:
                job.enqueue_status = _ENQUEUE_STATUS_PENDING
                _clear_enqueue_intent_lease(job)

            if job.enqueue_status != _ENQUEUE_STATUS_PENDING:
                continue

            recovered_job_ids.append(job.id)

        await session.commit()

    enqueued_job_ids: list[UUID] = []
    for job_id in recovered_job_ids:
        if await publish_job_enqueue_intent_func(job_id, recovery=True):
            enqueued_job_ids.append(job_id)

    return enqueued_job_ids


async def reap_stale_running_jobs(
    *,
    recoverable_enqueue_job_types: Any,
    publish_job_enqueue_intent_func: Callable[..., Awaitable[bool]],
    job_id: UUID | None = None,
) -> list[UUID]:
    """Reap ``running`` jobs whose attempt lease has expired without waiting for a worker restart.

    This is the per-job core of ``recover_incomplete_jobs`` (same requeue-or-fail
    semantics, same lease-staleness primitive) filtered to lease-stale ``running``
    jobs only, callable outside the worker-startup sweep — notably from a celery
    ``task_failure`` handler when a forked job process is SIGKILLed and the
    MainProcess survives, so worker-start recovery never runs again to catch it.

    When ``job_id`` is given, only that job is considered (still re-validating its
    status/lease under the row lock, so a concurrent redelivery/reclaim wins the
    race exactly as it would under ``recover_incomplete_jobs``).
    """
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    now = _utcnow()

    async with session_maker() as session:
        statement = (
            select(Job)
            .where((Job.job_type.in_(recoverable_enqueue_job_types)) & (Job.status == "running"))
            .order_by(Job.created_at.asc(), Job.id.asc())
            .with_for_update(skip_locked=True)
        )
        if job_id is not None:
            statement = statement.where(Job.id == job_id)

        result = await session.execute(statement)
        jobs = result.scalars().all()

        recovered_job_ids: list[UUID] = []
        failed_job_ids: list[UUID] = []
        for job in jobs:
            if not _is_stale_running_job(job, now=now, stale_after=_RUNNING_JOB_STALE_AFTER):
                continue

            if job.attempts >= job.max_attempts:
                await _persist_job_failed(
                    session,
                    job,
                    error_message=(
                        f"Job exhausted its {job.max_attempts} attempt(s) after its worker"
                        " process was lost."
                    ),
                    error_code=ErrorCode.MAX_ATTEMPTS_EXCEEDED,
                    error_details={"attempts": job.attempts, "max_attempts": job.max_attempts},
                )
                failed_job_ids.append(job.id)
                continue

            job.status = "pending"
            job.started_at = None
            job.finished_at = None
            job.error_code = None
            job.error_message = None
            _clear_job_attempt_lease(job)
            prepare_job_enqueue_intent(job)
            recovered_job_ids.append(job.id)

        await session.commit()

    for failed_job_id in failed_job_ids:
        logger.warning(
            "job_reaped_after_worker_lost_max_attempts_exceeded",
            job_id=str(failed_job_id),
        )

    enqueued_job_ids: list[UUID] = []
    for recovered_job_id in recovered_job_ids:
        if await publish_job_enqueue_intent_func(recovered_job_id, recovery=True):
            enqueued_job_ids.append(recovered_job_id)

    return enqueued_job_ids


async def recover_incomplete_ingest_jobs(
    *,
    recoverable_enqueue_job_types: Any,
    publish_job_enqueue_intent_func: Callable[..., Awaitable[bool]],
) -> list[UUID]:
    """Compatibility alias for incomplete worker job recovery."""
    return await recover_incomplete_jobs(
        recoverable_enqueue_job_types=recoverable_enqueue_job_types,
        publish_job_enqueue_intent_func=publish_job_enqueue_intent_func,
    )


def recover_incomplete_ingest_jobs_on_worker_start(
    *,
    run_worker_loop_func: Callable[..., list[UUID]],
    recover_incomplete_ingest_jobs_func: Callable[[], Awaitable[list[UUID]]],
    **_: object,
) -> None:
    """Requeue incomplete persisted worker jobs when a worker starts."""
    try:
        recovered_job_ids = run_worker_loop_func(recover_incomplete_ingest_jobs_func)
    except Exception as exc:
        logger.error("ingest_job_recovery_failed", error=str(exc), exc_info=True)
        return

    if recovered_job_ids:
        logger.info(
            "ingest_job_recovery_completed",
            recovered_job_ids=[str(job_id) for job_id in recovered_job_ids],
        )
