"""Job status endpoints."""

from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import raise_not_found
from app.db.session import get_db
from app.jobs.worker import enqueue_ingest_job
from app.models.job import Job
from app.schemas.job import JobRead

jobs_router = APIRouter()

_TERMINAL_JOB_STATUSES = {"failed", "succeeded", "cancelled"}


async def _get_job_or_404(db: AsyncSession, job_id: UUID) -> Job:
    """Return a persisted job or raise a not-found error."""
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if job is None:
        raise_not_found("Job", str(job_id))
    assert job is not None

    return job


async def _get_job_for_update_or_404(db: AsyncSession, job_id: UUID) -> Job:
    """Return a persisted job with a row lock or raise not found."""
    result = await db.execute(select(Job).where(Job.id == job_id).with_for_update())
    job = result.scalar_one_or_none()
    if job is None:
        raise_not_found("Job", str(job_id))
    assert job is not None

    return job


@jobs_router.get(
    "/jobs/{job_id}",
    response_model=JobRead,
)
async def get_job(
    job_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> Job:
    """Return persisted status for a single job."""
    return await _get_job_or_404(db, job_id)


@jobs_router.post(
    "/jobs/{job_id}/cancel",
    response_model=JobRead,
    status_code=status.HTTP_202_ACCEPTED,
)
async def cancel_job(
    job_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> Job:
    """Request cancellation for a persisted job."""
    job = await _get_job_for_update_or_404(db, job_id)
    if job.status in _TERMINAL_JOB_STATUSES:
        return job

    job.cancel_requested = True
    await db.commit()
    await db.refresh(job)
    return job


@jobs_router.post(
    "/jobs/{job_id}/retry",
    response_model=JobRead,
    status_code=status.HTTP_202_ACCEPTED,
)
async def retry_job(
    job_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> Job:
    """Requeue a failed persisted job when attempts remain."""
    job = await _get_job_for_update_or_404(db, job_id)
    if job.status != "failed" or job.attempts >= job.max_attempts:
        return job

    job.status = "pending"
    job.cancel_requested = False
    job.error_code = None
    job.error_message = None
    job.started_at = None
    job.finished_at = None
    await db.commit()

    try:
        enqueue_ingest_job(job.id)
    except Exception as exc:
        job.status = "failed"
        job.error_code = None
        job.error_message = f"Failed to enqueue ingest job: {exc}"
        job.finished_at = datetime.now(UTC)
        await db.commit()
        raise

    await db.refresh(job)
    return job
