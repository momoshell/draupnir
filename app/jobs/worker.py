"""Celery worker application and persisted job handlers."""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID

from celery import Celery
from celery.signals import worker_ready
from sqlalchemy import select

from app.core.config import settings
from app.core.logging import get_logger
from app.db.session import get_session_maker
from app.models.job import Job

logger = get_logger(__name__)

_INGEST_NOOP_DELAY_SECONDS = 0.01
_INCOMPLETE_JOB_STATUSES = ("pending", "running")
_TERMINAL_JOB_STATUSES = {"failed", "succeeded"}
_DEFAULT_ADAPTER_TIMEOUT = timedelta(minutes=5)
_RUNNING_JOB_STALE_AFTER = _DEFAULT_ADAPTER_TIMEOUT * 2

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


async def _mark_job_failed(job_id: UUID, *, error_message: str) -> None:
    """Persist a failed job state with the supplied message."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await session.get(Job, job_id)
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
        await session.commit()


async def process_ingest_job(job_id: UUID) -> None:
    """Load a persisted ingest job, perform a no-op, and persist state transitions."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await session.get(Job, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "ingest_job_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return

        job.attempts += 1
        job.status = "running"
        job.started_at = _utcnow()
        job.finished_at = None
        job.error_code = None
        job.error_message = None
        await session.commit()

    try:
        await asyncio.sleep(_INGEST_NOOP_DELAY_SECONDS)
    except Exception as exc:
        await _mark_job_failed(job_id, error_message=str(exc))
        logger.error("ingest_job_failed", job_id=str(job_id), error=str(exc), exc_info=True)
        raise

    async with session_maker() as session:
        job = await session.get(Job, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "ingest_job_completion_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return

        job.status = "succeeded"
        job.finished_at = _utcnow()
        job.error_code = None
        job.error_message = None
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
