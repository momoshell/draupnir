"""Job status endpoints."""

from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, status
from fastapi.responses import Response
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.idempotency import (
    IdempotencyReservation,
    build_idempotency_fingerprint,
    claim_idempotency_response,
    complete_idempotency_response,
    get_idempotency_key,
    replay_idempotency_response,
)
from app.api.pagination import (
    decode_cursor_payload,
    encode_cursor_payload,
    raise_invalid_cursor,
)
from app.core.errors import ErrorCode
from app.core.exceptions import raise_not_found
from app.db.session import get_db
from app.jobs.worker import (
    is_recoverable_enqueue_job_type,
    prepare_job_enqueue_intent,
    publish_job_enqueue_intent,
)
from app.models.file import File
from app.models.job import Job
from app.models.job_event import JobEvent
from app.models.project import Project
from app.schemas.job import JobEventPage, JobEventRead, JobRead

jobs_router = APIRouter()

_DEFAULT_EVENTS_LIMIT = 50
_MAX_EVENTS_LIMIT = 200
_TERMINAL_JOB_STATUSES = {"failed", "succeeded", "cancelled"}


async def _get_job_or_404(db: AsyncSession, job_id: UUID) -> Job:
    """Return a persisted job or raise a not-found error."""
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if job is None:
        raise_not_found("Job", str(job_id))
    assert job is not None

    return job


async def _get_job_lock_metadata_or_404(
    db: AsyncSession,
    job_id: UUID,
) -> tuple[UUID, UUID]:
    """Return project/file identifiers needed for ordered job locking."""
    result = await db.execute(select(Job.project_id, Job.file_id).where(Job.id == job_id))
    row = result.one_or_none()
    if row is None:
        raise_not_found("Job", str(job_id))

    assert row is not None
    project_id, file_id = row
    return project_id, file_id


async def _get_project_for_job_update_or_404(
    db: AsyncSession,
    *,
    job_id: UUID,
    project_id: UUID,
) -> Project:
    """Lock a job's project row before any job/file row locks."""
    result = await db.execute(
        select(Project).where(Project.id == project_id).with_for_update(of=Project)
    )
    project = result.scalar_one_or_none()
    if project is None:
        raise_not_found("Job", str(job_id))
    assert project is not None

    return project


async def _get_job_for_update_or_404(
    db: AsyncSession,
    job_id: UUID,
    *,
    expected_project_id: UUID | None = None,
    expected_file_id: UUID | None = None,
) -> Job:
    """Return a persisted job with a row lock or raise not found."""
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


async def _get_file_for_job_update_or_404(
    db: AsyncSession,
    *,
    job_id: UUID,
    project_id: UUID,
    file_id: UUID,
) -> File:
    """Lock a job's source file row after project and job locks."""
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


async def _lock_retry_source_or_404(
    db: AsyncSession,
    job_id: UUID,
    *,
    project_id: UUID,
    file_id: UUID,
) -> Job:
    """Lock retry source rows in Project -> Job -> File order."""
    project = await _get_project_for_job_update_or_404(
        db,
        job_id=job_id,
        project_id=project_id,
    )
    job = await _get_job_for_update_or_404(
        db,
        job_id,
        expected_project_id=project_id,
        expected_file_id=file_id,
    )
    source_file = await _get_file_for_job_update_or_404(
        db,
        job_id=job_id,
        project_id=project_id,
        file_id=file_id,
    )
    if project.deleted_at is not None or source_file.deleted_at is not None:
        raise_not_found("Job", str(job_id))
    assert job is not None

    return job


def _encode_job_events_cursor(event: JobEvent) -> str:
    """Encode a pagination cursor from a job event row."""
    created_at = event.created_at
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)

    return encode_cursor_payload(
        {
            "created_at": created_at.astimezone(UTC).isoformat(),
            "sequence_id": event.sequence_id,
            "id": str(event.id),
        },
        compact=True,
    )


def _decode_job_events_cursor(cursor: str) -> tuple[datetime, int | None, UUID]:
    """Decode a pagination cursor into its sort key values."""
    try:
        payload = decode_cursor_payload(cursor)
        created_at = datetime.fromisoformat(str(payload["created_at"]))
        sequence_id_raw = payload.get("sequence_id")
        sequence_id = int(sequence_id_raw) if sequence_id_raw is not None else None
        if sequence_id is not None and sequence_id < 0:
            raise ValueError("sequence_id must be non-negative")
        cursor_id = UUID(str(payload["id"]))
    except (ValueError, TypeError, KeyError) as exc:
        raise_invalid_cursor(exc)

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
    idempotency_key: Annotated[str | None, Depends(get_idempotency_key)] = None,
) -> Job | Response:
    """Request cancellation for a persisted job."""
    reservation: IdempotencyReservation | None = None
    fingerprint: str | None = None
    if idempotency_key is not None:
        fingerprint = build_idempotency_fingerprint(
            f"jobs.cancel:{job_id}",
            {"job_id": str(job_id)},
        )
        replay = await replay_idempotency_response(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
        )
        if replay is not None:
            return replay

    project_id, file_id = await _get_job_lock_metadata_or_404(db, job_id)

    if idempotency_key is not None:
        assert fingerprint is not None
        claim = await claim_idempotency_response(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
            method="POST",
            path=f"/jobs/{job_id}/cancel",
        )
        if isinstance(claim, Response):
            return claim
        reservation = claim

    await _get_project_for_job_update_or_404(
        db,
        job_id=job_id,
        project_id=project_id,
    )
    job = await _get_job_for_update_or_404(
        db,
        job_id,
        expected_project_id=project_id,
        expected_file_id=file_id,
    )
    if job.status in _TERMINAL_JOB_STATUSES:
        if reservation is not None:
            response = await complete_idempotency_response(
                db,
                reservation,
                status_code=status.HTTP_202_ACCEPTED,
                response_body=JobRead.model_validate(job).model_dump(mode="json"),
            )
            await db.commit()
            return response
        return job

    job.cancel_requested = True
    if reservation is not None:
        await db.flush()
        await db.refresh(job)
        response = await complete_idempotency_response(
            db,
            reservation,
            status_code=status.HTTP_202_ACCEPTED,
            response_body=JobRead.model_validate(job).model_dump(mode="json"),
        )
        await db.commit()
        return response

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
    idempotency_key: Annotated[str | None, Depends(get_idempotency_key)] = None,
) -> Job | Response:
    """Requeue a failed persisted job when attempts remain."""
    reservation: IdempotencyReservation | None = None
    fingerprint: str | None = None
    if idempotency_key is not None:
        fingerprint = build_idempotency_fingerprint(
            f"jobs.retry:{job_id}",
            {"job_id": str(job_id)},
        )
        replay = await replay_idempotency_response(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
        )
        if replay is not None:
            return replay

    project_id, file_id = await _get_job_lock_metadata_or_404(db, job_id)

    if idempotency_key is not None:
        assert fingerprint is not None
        claim = await claim_idempotency_response(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
            method="POST",
            path=f"/jobs/{job_id}/retry",
        )
        if isinstance(claim, Response):
            return claim
        reservation = claim

    job = await _lock_retry_source_or_404(
        db,
        job_id,
        project_id=project_id,
        file_id=file_id,
    )
    if job.status != "failed" or job.attempts >= job.max_attempts:
        if reservation is not None:
            response = await complete_idempotency_response(
                db,
                reservation,
                status_code=status.HTTP_202_ACCEPTED,
                response_body=JobRead.model_validate(job).model_dump(mode="json"),
            )
            await db.commit()
            return response
        return job

    if job.error_code == ErrorCode.REVISION_CONFLICT.value:
        if reservation is not None:
            response = await complete_idempotency_response(
                db,
                reservation,
                status_code=status.HTTP_202_ACCEPTED,
                response_body=JobRead.model_validate(job).model_dump(mode="json"),
            )
            await db.commit()
            return response
        return job

    if not is_recoverable_enqueue_job_type(job.job_type):
        if reservation is not None:
            response = await complete_idempotency_response(
                db,
                reservation,
                status_code=status.HTTP_202_ACCEPTED,
                response_body=JobRead.model_validate(job).model_dump(mode="json"),
            )
            await db.commit()
            return response
        return job

    job.status = "pending"
    job.cancel_requested = False
    job.error_code = None
    job.error_message = None
    job.started_at = None
    job.finished_at = None
    prepare_job_enqueue_intent(job)
    await db.flush()
    await db.refresh(job)

    retry_response: Response | None = None
    if reservation is not None:
        retry_response = await complete_idempotency_response(
            db,
            reservation,
            status_code=status.HTTP_202_ACCEPTED,
            response_body=JobRead.model_validate(job).model_dump(mode="json"),
        )

    await db.commit()
    await publish_job_enqueue_intent(job.id, suppress_exceptions=True)

    if reservation is not None:
        assert retry_response is not None
        return retry_response

    return job
