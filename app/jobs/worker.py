"""Celery worker application and persisted job handlers."""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from celery import Celery
from celery.signals import worker_ready
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logging import get_logger
from app.db.session import get_session_maker
from app.models.job import Job
from app.models.job_event import JobEvent

logger = get_logger(__name__)

_INGEST_NOOP_DELAY_SECONDS = 0.01
_INCOMPLETE_JOB_STATUSES = ("pending", "running")
_TERMINAL_JOB_STATUSES = {"failed", "succeeded", "cancelled"}
_DEFAULT_ADAPTER_TIMEOUT = timedelta(minutes=5)
_RUNNING_JOB_STALE_AFTER = _DEFAULT_ADAPTER_TIMEOUT * 2
_JOB_CANCELLED_ERROR_CODE = "JOB_CANCELLED"

celery_app = Celery(
    "draupnir",
    broker=settings.broker_url,
)
celery_app.conf.update(
    task_ignore_result=True,
    task_store_eager_result=False,
    task_publish_retry=False,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
)

# Auto-discover tasks from the jobs module
celery_app.autodiscover_tasks(["app.jobs"], force=True)


def _utcnow() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(UTC)


def _is_stale_running_job(job: Job, *, now: datetime) -> bool:
    """Return whether a running job is old enough to treat as orphaned."""
    if job.started_at is None:
        return True

    started_at = job.started_at
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=UTC)

    return started_at <= now - _RUNNING_JOB_STALE_AFTER


async def _get_job_for_update(session: AsyncSession, job_id: UUID) -> Job | None:
    """Load and lock a persisted job row."""
    result = await session.execute(select(Job).where(Job.id == job_id).with_for_update())
    return result.scalar_one_or_none()


async def emit_job_event(
    job_id: UUID,
    *,
    level: str,
    message: str,
    data_json: dict[str, Any] | None = None,
    session: AsyncSession | None = None,
) -> None:
    """Persist a job lifecycle event."""
    event = JobEvent(
        job_id=job_id,
        level=level,
        message=message,
        data_json=data_json,
    )
    if session is not None:
        session.add(event)
        return

    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as managed_session:
        managed_session.add(event)
        await managed_session.commit()


async def _mark_job_failed(job_id: UUID, *, error_message: str) -> None:
    """Persist a failed job state with the supplied message."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await _get_job_for_update(session, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "ingest_job_failure_mark_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return

        job.status = "failed"
        job.error_code = None
        job.error_message = error_message
        job.finished_at = _utcnow()
        await emit_job_event(
            job_id,
            level="error",
            message="Job failed",
            data_json={"status": "failed", "error_message": error_message},
            session=session,
        )
        await session.commit()


def _finalize_job_cancelled(job: Job) -> None:
    """Apply the persisted cancelled terminal state to a job."""
    job.status = "cancelled"
    job.error_code = _JOB_CANCELLED_ERROR_CODE
    job.error_message = None
    job.finished_at = _utcnow()


async def _begin_or_resume_ingest_job(job_id: UUID) -> bool:
    """Claim, resume, or cancel a persisted ingest job under a row lock."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    now = _utcnow()

    async with session_maker() as session:
        job = await _get_job_for_update(session, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "ingest_job_cancel_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if not job.cancel_requested:
            if job.status == "running":
                if _is_stale_running_job(job, now=now):
                    job.attempts += 1
                    job.started_at = now
                    job.finished_at = None
                    job.error_code = None
                    job.error_message = None
                    await emit_job_event(
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
                    logger.warning(
                        "ingest_job_reclaimed_stale_running_status",
                        job_id=str(job_id),
                        status=job.status,
                    )
                    return True

                logger.info(
                    "ingest_job_continuing_running_status",
                    job_id=str(job_id),
                    status=job.status,
                )
                return True

            job.attempts += 1
            job.status = "running"
            job.started_at = now
            job.finished_at = None
            job.error_code = None
            job.error_message = None
            await emit_job_event(
                job.id,
                level="info",
                message="Job started",
                data_json={"status": "running", "attempts": job.attempts, "reclaimed": False},
                session=session,
            )
            await session.commit()
            return True

        _finalize_job_cancelled(job)
        await emit_job_event(
            job.id,
            level="warning",
            message="Job cancelled",
            data_json={"status": "cancelled"},
            session=session,
        )
        await session.commit()

    logger.info("ingest_job_cancelled", job_id=str(job_id))
    return False


async def process_ingest_job(job_id: UUID) -> None:
    """Load a persisted ingest job, perform a no-op, and persist state transitions."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    if not await _begin_or_resume_ingest_job(job_id):
        return

    try:
        await asyncio.sleep(_INGEST_NOOP_DELAY_SECONDS)
    except Exception as exc:
        await _mark_job_failed(job_id, error_message=str(exc))
        logger.error("ingest_job_failed", job_id=str(job_id), error=str(exc), exc_info=True)
        raise

    async with session_maker() as session:
        job = await _get_job_for_update(session, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "ingest_job_completion_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return

        if job.cancel_requested:
            _finalize_job_cancelled(job)
            await emit_job_event(
                job.id,
                level="warning",
                message="Job cancelled",
                data_json={"status": "cancelled"},
                session=session,
            )
            await session.commit()
            logger.info("ingest_job_cancelled", job_id=str(job_id))
            return

        if job.status != "running":
            logger.info(
                "ingest_job_completion_skipped_non_running_status",
                job_id=str(job_id),
                status=job.status,
            )
            return

        job.status = "succeeded"
        job.finished_at = _utcnow()
        job.error_code = None
        job.error_message = None
        await emit_job_event(
            job.id,
            level="info",
            message="Job succeeded",
            data_json={"status": "succeeded", "attempts": job.attempts},
            session=session,
        )
        await session.commit()

    logger.info("ingest_job_succeeded", job_id=str(job_id))



async def recover_incomplete_ingest_jobs() -> list[UUID]:
    """Requeue incomplete persisted ingest jobs on worker startup."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    now = _utcnow()

    async with session_maker() as session:
        result = await session.execute(
            select(Job)
            .where(
                (Job.job_type == "ingest")
                & (Job.status.in_(_INCOMPLETE_JOB_STATUSES))
            )
            .order_by(Job.created_at.asc(), Job.id.asc())
        )
        jobs = result.scalars().all()

        recovered_job_ids: list[UUID] = []
        for job in jobs:
            if job.status == "running":
                if not _is_stale_running_job(job, now=now):
                    continue
                job.status = "pending"
                job.started_at = None
                job.finished_at = None
                job.error_code = None
                job.error_message = None
            recovered_job_ids.append(job.id)

        await session.commit()

    enqueued_job_ids: list[UUID] = []
    for job_id in recovered_job_ids:
        try:
            enqueue_ingest_job(job_id)
        except Exception as exc:
            await _mark_job_failed(job_id, error_message=f"Failed to enqueue ingest job: {exc}")
            logger.error(
                "ingest_job_recovery_enqueue_failed",
                job_id=str(job_id),
                error=str(exc),
                exc_info=True,
            )
        else:
            enqueued_job_ids.append(job_id)

    return enqueued_job_ids


def recover_incomplete_ingest_jobs_on_worker_start(**_: object) -> None:
    """Requeue incomplete ingest jobs when a worker starts."""
    try:
        recovered_job_ids = asyncio.run(recover_incomplete_ingest_jobs())
    except Exception as exc:
        logger.error("ingest_job_recovery_failed", error=str(exc), exc_info=True)
        return

    if recovered_job_ids:
        logger.info(
            "ingest_job_recovery_completed",
            recovered_job_ids=[str(job_id) for job_id in recovered_job_ids],
        )


worker_ready.connect(recover_incomplete_ingest_jobs_on_worker_start)


@celery_app.task(
    name="app.jobs.worker.run_ingest_job",
    ignore_result=True,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_ingest_job(job_id: str) -> None:
    """Celery task wrapper for the persisted ingest job processor."""
    asyncio.run(process_ingest_job(UUID(job_id)))


def enqueue_ingest_job(job_id: UUID) -> None:
    """Publish a persisted ingest job to Celery."""
    run_ingest_job.apply_async(args=(str(job_id),), task_id=str(job_id), retry=False)
