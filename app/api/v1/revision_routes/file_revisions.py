"""Revision file-read routes."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.pagination import DEFAULT_PAGE_SIZE as _DEFAULT_PAGE_SIZE
from app.api.pagination import MAX_PAGE_SIZE as _MAX_PAGE_SIZE
from app.api.pagination import paginate_overfetched as _paginate_overfetched
from app.api.v1.revision_cursors import (
    _decode_revision_cursor,
    _DrawingRevisionCursor,
    _encode_cursor,
)
from app.api.v1.revision_lineage import _get_active_file
from app.core.exceptions import raise_not_found
from app.db.session import get_db
from app.models.drawing_revision import DrawingRevision
from app.models.file import File
from app.models.project import Project
from app.schemas.revision import DrawingRevisionListResponse, DrawingRevisionRead

file_revisions_router = APIRouter()


@file_revisions_router.get("/files/{file_id}/revisions", response_model=DrawingRevisionListResponse)
async def list_file_revisions(
    file_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
) -> DrawingRevisionListResponse:
    """List active drawing revisions for a file."""

    file = await _get_active_file(file_id, db)
    if file is None:
        raise_not_found("File", str(file_id))

    pagination_cursor = _decode_revision_cursor(cursor) if cursor else None

    query = (
        select(DrawingRevision)
        .join(
            File,
            (File.id == DrawingRevision.source_file_id)
            & (File.project_id == DrawingRevision.project_id),
        )
        .join(Project, Project.id == DrawingRevision.project_id)
        .where(
            (DrawingRevision.source_file_id == file_id)
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )

    if pagination_cursor is not None:
        query = query.where(
            (DrawingRevision.revision_sequence > pagination_cursor.revision_sequence)
            | (
                (DrawingRevision.revision_sequence == pagination_cursor.revision_sequence)
                & (DrawingRevision.created_at > pagination_cursor.created_at)
            )
            | (
                (DrawingRevision.revision_sequence == pagination_cursor.revision_sequence)
                & (DrawingRevision.created_at == pagination_cursor.created_at)
                & (DrawingRevision.id > pagination_cursor.id)
            )
        )

    result = await db.execute(
        query.order_by(
            DrawingRevision.revision_sequence.asc(),
            DrawingRevision.created_at.asc(),
            DrawingRevision.id.asc(),
        ).limit(limit + 1)
    )
    page, next_cursor = _paginate_overfetched(
        result.scalars().all(),
        limit=limit,
        encode_cursor=lambda last_revision: _encode_cursor(
            _DrawingRevisionCursor(
                revision_sequence=last_revision.revision_sequence,
                created_at=last_revision.created_at,
                id=last_revision.id,
            )
        ),
    )

    return DrawingRevisionListResponse(
        items=[DrawingRevisionRead.model_validate(revision) for revision in page],
        next_cursor=next_cursor,
    )
