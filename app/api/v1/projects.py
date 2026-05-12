"""Project CRUD endpoints."""

from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, status
from fastapi.responses import JSONResponse, Response
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.idempotency import (
    IdempotencyReplay,
    IdempotencyReservation,
    build_idempotency_fingerprint,
    claim_idempotency,
    get_idempotency_key,
    mark_idempotency_completed,
    replay_idempotency,
)
from app.api.pagination import (
    decode_cursor_payload,
    encode_cursor_payload,
    raise_invalid_cursor,
)
from app.core.errors import ErrorCode
from app.core.exceptions import raise_not_found
from app.db.session import get_db
from app.models.file import File
from app.models.generated_artifact import GeneratedArtifact
from app.models.job import Job
from app.models.project import Project
from app.schemas.project import (
    ProjectCreate,
    ProjectListResponse,
    ProjectRead,
    ProjectUpdate,
)

project_router = APIRouter()


async def _get_active_project_or_404(
    db: AsyncSession,
    project_id: UUID,
    *,
    for_update: bool = False,
) -> Project:
    """Return an active project row or raise not found."""
    query = select(Project).where(
        (Project.id == project_id) & (Project.deleted_at.is_(None))
    )
    if for_update:
        query = query.with_for_update()
    project = (await db.execute(query)).scalar_one_or_none()
    if project is None:
        raise_not_found("Project", str(project_id))

    assert project is not None
    return project


def _encode_cursor(created_at: datetime, project_id: UUID) -> str:
    """Encode cursor from created_at and project_id."""
    return encode_cursor_payload(
        {
            "created_at": created_at.isoformat(),
            "id": str(project_id),
        }
    )


def _decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    """Decode cursor to typed created_at and id values."""
    try:
        cursor_data = decode_cursor_payload(cursor)
        return datetime.fromisoformat(str(cursor_data["created_at"])), UUID(
            str(cursor_data["id"])
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise_invalid_cursor(exc)


@project_router.post(
    "",
    response_model=ProjectRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_project(
    project_in: ProjectCreate,
    db: Annotated[AsyncSession, Depends(get_db)],
    idempotency_key: Annotated[str | None, Depends(get_idempotency_key)] = None,
) -> Project | Response:
    """Create a new project."""
    reservation: IdempotencyReservation | None = None
    if idempotency_key is not None:
        claim = await claim_idempotency(
            db,
            key=idempotency_key,
            fingerprint=build_idempotency_fingerprint(
                "projects.create",
                project_in.model_dump(mode="json", exclude_unset=True),
            ),
            method="POST",
            path="/projects",
        )
        if isinstance(claim, IdempotencyReplay):
            return claim.response
        reservation = claim

    project = Project(
        name=project_in.name,
        description=project_in.description,
        default_unit_system=project_in.default_unit_system,
        default_currency=project_in.default_currency,
    )
    db.add(project)
    if reservation is not None:
        await db.flush()
        await db.refresh(project)
        body = ProjectRead.model_validate(project).model_dump(mode="json")
        await mark_idempotency_completed(
            db,
            reservation,
            status_code=status.HTTP_201_CREATED,
            response_body=body,
        )
        await db.commit()
        return JSONResponse(status_code=status.HTTP_201_CREATED, content=body)

    await db.commit()
    await db.refresh(project)
    return project


@project_router.get(
    "",
    response_model=ProjectListResponse,
)
async def list_projects(
    db: Annotated[AsyncSession, Depends(get_db)],
    cursor: Annotated[str | None, Query(description="Cursor for pagination")] = None,
    limit: Annotated[int, Query(ge=1, le=200, description="Number of items to return")] = 50,
) -> ProjectListResponse:
    """List projects with cursor pagination.

    Projects are ordered by created_at DESC, id DESC.
    Cursor should be the next_cursor value from a previous response.
    """
    # Build query ordered by created_at DESC, id DESC
    query = (
        select(Project)
        .where(Project.deleted_at.is_(None))
        .order_by(Project.created_at.desc(), Project.id.desc())
    )

    # Apply cursor filter if provided
    if cursor:
        created_at, project_id = _decode_cursor(cursor)
        # Filter for items after the cursor position
        query = query.filter(
            (Project.created_at < created_at)
            | (
                (Project.created_at == created_at)
                & (Project.id < project_id)
            )
        )

    # Fetch limit + 1 to determine if there's a next page
    items = (await db.execute(query.limit(limit + 1))).scalars().all()

    # Check if there's a next page
    has_next = len(items) > limit
    if has_next:
        items = items[:-1]  # Remove the extra item

    # Generate next cursor if there's a next page
    next_cursor = None
    if has_next and items:
        last_item = items[-1]
        next_cursor = _encode_cursor(last_item.created_at, last_item.id)

    # Convert Sequence[Project] to list[ProjectRead] for response
    project_reads = [ProjectRead.model_validate(project) for project in items]
    return ProjectListResponse(items=project_reads, next_cursor=next_cursor)


@project_router.get(
    "/{project_id}",
    response_model=ProjectRead,
)
async def get_project(
    project_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> Project:
    """Get a project by ID."""
    return await _get_active_project_or_404(db, project_id)


@project_router.patch(
    "/{project_id}",
    response_model=ProjectRead,
)
async def update_project(
    project_id: UUID,
    project_in: ProjectUpdate,
    db: Annotated[AsyncSession, Depends(get_db)],
    idempotency_key: Annotated[str | None, Depends(get_idempotency_key)] = None,
) -> Project | Response:
    """Update an existing project."""
    reservation: IdempotencyReservation | None = None
    fingerprint: str | None = None
    if idempotency_key is not None:
        fingerprint = build_idempotency_fingerprint(
            f"projects.update:{project_id}",
            project_in.model_dump(mode="json", exclude_unset=True),
        )
        replay = await replay_idempotency(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
        )
        if replay is not None:
            return replay.response

    project = await _get_active_project_or_404(db, project_id)
    if idempotency_key is not None:
        assert fingerprint is not None
        claim = await claim_idempotency(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
            method="PATCH",
            path=f"/projects/{project_id}",
        )
        if isinstance(claim, IdempotencyReplay):
            return claim.response
        reservation = claim
        project = await _get_active_project_or_404(db, project_id)

    # Update only provided fields
    update_data = project_in.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(project, field, value)

    if reservation is not None:
        await db.flush()
        await db.refresh(project)
        body = ProjectRead.model_validate(project).model_dump(mode="json")
        await mark_idempotency_completed(
            db,
            reservation,
            status_code=status.HTTP_200_OK,
            response_body=body,
        )
        await db.commit()
        return JSONResponse(status_code=status.HTTP_200_OK, content=body)

    await db.commit()
    await db.refresh(project)
    return project


@project_router.delete(
    "/{project_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_project(
    project_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    idempotency_key: Annotated[str | None, Depends(get_idempotency_key)] = None,
) -> Response:
    """Delete a project."""
    reservation: IdempotencyReservation | None = None
    fingerprint: str | None = None
    if idempotency_key is not None:
        fingerprint = build_idempotency_fingerprint(
            f"projects.delete:{project_id}",
            {"project_id": str(project_id)},
        )
        replay = await replay_idempotency(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
        )
        if replay is not None:
            return replay.response

    await _get_active_project_or_404(db, project_id)

    if idempotency_key is not None:
        assert fingerprint is not None
        claim = await claim_idempotency(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
            method="DELETE",
            path=f"/projects/{project_id}",
        )
        if isinstance(claim, IdempotencyReplay):
            return claim.response
        reservation = claim

    project = await _get_active_project_or_404(db, project_id, for_update=True)
    locked_jobs = (
        await db.execute(
            select(Job)
            .where(
                (Job.project_id == project_id)
                & (Job.status.in_(("pending", "running")))
            )
            .order_by(Job.created_at.asc(), Job.id.asc())
            .with_for_update(of=Job)
        )
    ).scalars().all()
    deleted_at = datetime.now(UTC)
    project.deleted_at = deleted_at
    for job in locked_jobs:
        job.status = "cancelled"
        job.cancel_requested = True
        job.error_code = ErrorCode.JOB_CANCELLED.value
        job.error_message = None
        job.finished_at = deleted_at
        job.attempt_token = None
        job.attempt_lease_expires_at = None
    await db.execute(
        update(File)
        .where((File.project_id == project_id) & (File.deleted_at.is_(None)))
        .values(deleted_at=deleted_at)
    )
    await db.execute(
        update(GeneratedArtifact)
        .where(
            (GeneratedArtifact.project_id == project_id)
            & (GeneratedArtifact.deleted_at.is_(None))
        )
        .values(deleted_at=deleted_at)
    )
    if reservation is not None:
        await mark_idempotency_completed(
            db,
            reservation,
            status_code=status.HTTP_204_NO_CONTENT,
            response_body=None,
        )
    await db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
