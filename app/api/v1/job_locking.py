"""Ordered row locking for job-mutating endpoints.

A single source of truth for the project -> job -> (file) lock order used by the cancel/retry
endpoints (and any future job mutation), so the order and not-found semantics live in one place.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import raise_not_found
from app.models.file import File
from app.models.job import Job
from app.models.project import Project


async def get_job_lock_metadata_or_404(db: AsyncSession, job_id: UUID) -> tuple[UUID, UUID]:
    """Return the (project_id, file_id) needed to lock a job's rows in order, or 404."""

    result = await db.execute(select(Job.project_id, Job.file_id).where(Job.id == job_id))
    row = result.one_or_none()
    if row is None:
        raise_not_found("Job", str(job_id))
    assert row is not None
    project_id, file_id = row
    return project_id, file_id


async def lock_job_for_mutation(
    db: AsyncSession,
    job_id: UUID,
    *,
    project_id: UUID,
    file_id: UUID,
    lock_source_file: bool,
) -> Job:
    """Lock a job's rows in project -> job -> (file) order and return the locked job.

    ``lock_source_file`` additionally locks the source file and rejects the request when the
    project or file is soft-deleted. Raises a not-found error for any missing/mismatched row.
    """

    project = await _lock_project_or_404(db, job_id=job_id, project_id=project_id)
    job = await _lock_job_or_404(
        db, job_id, expected_project_id=project_id, expected_file_id=file_id
    )
    if lock_source_file:
        source_file = await _lock_file_or_404(
            db, job_id=job_id, project_id=project_id, file_id=file_id
        )
        if project.deleted_at is not None or source_file.deleted_at is not None:
            raise_not_found("Job", str(job_id))
    return job


async def _lock_project_or_404(db: AsyncSession, *, job_id: UUID, project_id: UUID) -> Project:
    result = await db.execute(
        select(Project).where(Project.id == project_id).with_for_update(of=Project)
    )
    project = result.scalar_one_or_none()
    if project is None:
        raise_not_found("Job", str(job_id))
    assert project is not None
    return project


async def _lock_job_or_404(
    db: AsyncSession,
    job_id: UUID,
    *,
    expected_project_id: UUID | None = None,
    expected_file_id: UUID | None = None,
) -> Job:
    result = await db.execute(select(Job).where(Job.id == job_id).with_for_update(of=Job))
    job = result.scalar_one_or_none()
    if job is None:
        raise_not_found("Job", str(job_id))
    assert job is not None
    if expected_project_id is not None and job.project_id != expected_project_id:
        raise_not_found("Job", str(job_id))
    if expected_file_id is not None and job.file_id != expected_file_id:
        raise_not_found("Job", str(job_id))
    return job


async def _lock_file_or_404(
    db: AsyncSession, *, job_id: UUID, project_id: UUID, file_id: UUID
) -> File:
    result = await db.execute(
        select(File)
        .where((File.project_id == project_id) & (File.id == file_id))
        .with_for_update(of=File)
    )
    source_file = result.scalar_one_or_none()
    if source_file is None:
        raise_not_found("Job", str(job_id))
    assert source_file is not None
    return source_file
