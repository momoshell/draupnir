"""Job status endpoints."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import raise_not_found
from app.db.session import get_db
from app.models.job import Job
from app.schemas.job import JobRead

jobs_router = APIRouter()


@jobs_router.get(
    "/jobs/{job_id}",
    response_model=JobRead,
)
async def get_job(
    job_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> Job:
    """Return persisted status for a single job."""
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if job is None:
        raise_not_found("Job", str(job_id))

    assert job is not None
    return job
