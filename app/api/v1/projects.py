"""Project CRUD endpoints."""

import base64
import binascii
import json
from datetime import UTC, datetime
from typing import Annotated, Any, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response, raise_not_found
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
    cursor_data = {
        "created_at": created_at.isoformat(),
        "id": str(project_id),
    }
    return base64.urlsafe_b64encode(json.dumps(cursor_data).encode()).decode().rstrip("=")


def _decode_cursor(cursor: str) -> dict[str, Any]:
    """Decode cursor to dict with created_at and id."""
    try:
        # Add padding if needed
        padding = 4 - (len(cursor) % 4)
        if padding != 4:
            cursor += "=" * padding

        decoded = base64.urlsafe_b64decode(cursor)
        cursor_data_raw = json.loads(decoded.decode("utf-8"))
        if not isinstance(cursor_data_raw, dict):
            raise TypeError("Cursor payload must be a JSON object")
        cursor_data = cast(dict[str, Any], cursor_data_raw)

        # Validate required keys and value formats.
        _ = datetime.fromisoformat(str(cursor_data["created_at"]))
        _ = UUID(str(cursor_data["id"]))
        return cursor_data
    except (
        binascii.Error,
        UnicodeDecodeError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
        ValueError,
    ) as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=create_error_response(
                code=ErrorCode.INVALID_CURSOR,
                message="Invalid cursor format",
                details=None,
            ),
        ) from e


@project_router.post(
    "",
    response_model=ProjectRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_project(
    project_in: ProjectCreate,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> Project:
    """Create a new project."""
    project = Project(
        name=project_in.name,
        description=project_in.description,
        default_unit_system=project_in.default_unit_system,
        default_currency=project_in.default_currency,
    )
    db.add(project)
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
        cursor_data = _decode_cursor(cursor)
        created_at = datetime.fromisoformat(str(cursor_data["created_at"]))
        project_id = UUID(str(cursor_data["id"]))
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
) -> Project:
    """Update an existing project."""
    project = await _get_active_project_or_404(db, project_id)

    # Update only provided fields
    update_data = project_in.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(project, field, value)

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
) -> None:
    """Delete a project."""
    project = await _get_active_project_or_404(db, project_id, for_update=True)
    deleted_at = datetime.now(UTC)
    project.deleted_at = deleted_at
    await db.execute(
        update(Job)
        .where(
            (Job.project_id == project_id)
            & (Job.status.in_(("pending", "running")))
        )
        .values(
            status="cancelled",
            cancel_requested=True,
            error_code=ErrorCode.JOB_CANCELLED.value,
            error_message=None,
            finished_at=deleted_at,
        )
    )
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
    await db.commit()
