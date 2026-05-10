"""Job status endpoints."""

import base64
import binascii
import json
from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response, raise_not_found
from app.db.session import get_db
from app.models.file import File
from app.jobs.worker import enqueue_ingest_job
from app.models.job import Job
from app.models.job_event import JobEvent
from app.models.project import Project
from app.schemas.job import JobEventPage, JobEventRead, JobRead

jobs_router = APIRouter()

_DEFAULT_EVENTS_LIMIT = 50
_MAX_EVENTS_LIMIT = 200
_MAX_PUBLIC_JOB_ERROR_MESSAGE_LENGTH = 255
_PUBLIC_ENQUEUE_FAILURE_MESSAGE = "Failed to enqueue ingest job"
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


async def _get_active_job_for_retry_or_404(db: AsyncSession, job_id: UUID) -> Job:
    """Return a job row with active project/file visibility for retries."""
    result = await db.execute(
        select(Job)
        .join(
            File,
            (File.id == Job.file_id) & (File.project_id == Job.project_id),
        )
        .join(Project, Project.id == Job.project_id)
        .where(
            (Job.id == job_id)
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
        .with_for_update()
    )
    job = result.scalar_one_or_none()
    if job is None:
        raise_not_found("Job", str(job_id))
    assert job is not None

    return job


def _encode_job_events_cursor(event: JobEvent) -> str:
    """Encode a pagination cursor from a job event row."""
    created_at = event.created_at
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)

    payload = json.dumps(
        {
            "created_at": created_at.astimezone(UTC).isoformat(),
            "sequence_id": event.sequence_id,
            "id": str(event.id),
        },
        separators=(",", ":"),
    )
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("utf-8").rstrip("=")


def _decode_job_events_cursor(cursor: str) -> tuple[datetime, int | None, UUID]:
    """Decode a pagination cursor into its sort key values."""
    try:
        padded_cursor = cursor + ("=" * (-len(cursor) % 4))
        decoded = base64.urlsafe_b64decode(padded_cursor.encode("utf-8")).decode("utf-8")
        payload = json.loads(decoded)
        created_at = datetime.fromisoformat(payload["created_at"])
        sequence_id_raw = payload.get("sequence_id")
        sequence_id = int(sequence_id_raw) if sequence_id_raw is not None else None
        if sequence_id is not None and sequence_id < 0:
            raise ValueError("sequence_id must be non-negative")
        cursor_id = UUID(payload["id"])
    except (ValueError, TypeError, KeyError, json.JSONDecodeError, binascii.Error) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=create_error_response(
                code=ErrorCode.INVALID_CURSOR,
                message="Invalid cursor format",
                details=None,
            ),
        ) from exc

    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)

    return created_at.astimezone(UTC), sequence_id, cursor_id


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


@jobs_router.get(
    "/jobs/{job_id}/events",
    response_model=JobEventPage,
)
async def list_job_events(
    job_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    cursor: Annotated[str | None, Query(description="Opaque pagination cursor")] = None,
    limit: Annotated[
        int,
        Query(ge=1, le=_MAX_EVENTS_LIMIT, description="Page size"),
    ] = _DEFAULT_EVENTS_LIMIT,
) -> JobEventPage:
    """Return cursor-paginated lifecycle events for a single job."""
    await _get_job_or_404(db, job_id)

    statement = select(JobEvent).where(JobEvent.job_id == job_id)
    if cursor is not None:
        created_at, sequence_id, cursor_id = _decode_job_events_cursor(cursor)
        if sequence_id is None:
            statement = statement.where(
                or_(
                    JobEvent.created_at > created_at,
                    and_(JobEvent.created_at == created_at, JobEvent.id > cursor_id),
                )
            )
        else:
            statement = statement.where(
                or_(
                    JobEvent.created_at > created_at,
                    and_(JobEvent.created_at == created_at, JobEvent.sequence_id > sequence_id),
                    and_(
                        JobEvent.created_at == created_at,
                        JobEvent.sequence_id == sequence_id,
                        JobEvent.id > cursor_id,
                    ),
                )
            )

    statement = statement.order_by(
        JobEvent.created_at.asc(),
        JobEvent.sequence_id.asc(),
        JobEvent.id.asc(),
    ).limit(limit + 1)
    result = await db.execute(statement)
    events = list(result.scalars())

    next_cursor: str | None = None
    if len(events) > limit:
        next_cursor = _encode_job_events_cursor(events[limit - 1])
        events = events[:limit]

    return JobEventPage(
        items=[JobEventRead.model_validate(event) for event in events],
        next_cursor=next_cursor,
    )


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
    job = await _get_active_job_for_retry_or_404(db, job_id)
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
        job.error_code = ErrorCode.INTERNAL_ERROR.value
        job.error_message = _PUBLIC_ENQUEUE_FAILURE_MESSAGE[:_MAX_PUBLIC_JOB_ERROR_MESSAGE_LENGTH]
        job.finished_at = datetime.now(UTC)
        await db.commit()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=create_error_response(
                code=ErrorCode.INTERNAL_ERROR,
                message="Failed to enqueue ingest job",
                details=None,
            ),
        ) from exc

    await db.refresh(job)
    return job
