"""Revision API routes."""

import base64
import binascii
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import raise_not_found
from app.db.session import get_db
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.file import File
from app.models.generated_artifact import GeneratedArtifact
from app.models.project import Project
from app.models.validation_report import ValidationReport
from app.schemas.revision import (
    AdapterRunOutputRead,
    DrawingRevisionListResponse,
    DrawingRevisionRead,
    GeneratedArtifactListResponse,
    GeneratedArtifactRead,
)
from app.schemas.validation_report import (
    ValidationReportResponse,
    build_validation_report_response,
)

revisions_router = APIRouter()

_DEFAULT_PAGE_SIZE = 50
_MAX_PAGE_SIZE = 200


class _DrawingRevisionCursor(BaseModel):
    """Opaque cursor payload for drawing revision pagination."""

    revision_sequence: int
    created_at: datetime
    id: UUID


class _GeneratedArtifactCursor(BaseModel):
    """Opaque cursor payload for generated artifact pagination."""

    created_at: datetime
    id: UUID


def _encode_cursor(payload: BaseModel) -> str:
    """Encode a pagination cursor payload as an opaque token."""

    return base64.urlsafe_b64encode(payload.model_dump_json().encode("utf-8")).decode(
        "utf-8"
    ).rstrip("=")


def _decode_revision_cursor(cursor: str) -> _DrawingRevisionCursor:
    """Decode and validate an opaque drawing revision cursor."""

    try:
        decoded = base64.urlsafe_b64decode(f"{cursor}{'=' * (-len(cursor) % 4)}")
        return _DrawingRevisionCursor.model_validate_json(decoded)
    except (ValueError, ValidationError, binascii.Error) as exc:
        raise HTTPException(status_code=422, detail="Invalid cursor") from exc


def _decode_artifact_cursor(cursor: str) -> _GeneratedArtifactCursor:
    """Decode and validate an opaque generated artifact cursor."""

    try:
        decoded = base64.urlsafe_b64decode(f"{cursor}{'=' * (-len(cursor) % 4)}")
        return _GeneratedArtifactCursor.model_validate_json(decoded)
    except (ValueError, ValidationError, binascii.Error) as exc:
        raise HTTPException(status_code=422, detail="Invalid cursor") from exc


async def _get_active_file(file_id: UUID, db: AsyncSession) -> File | None:
    """Return an active file visible through a non-deleted project."""

    result = await db.execute(
        select(File)
        .join(Project, Project.id == File.project_id)
        .where(
            (File.id == file_id)
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )
    return result.scalar_one_or_none()


async def _get_active_revision(
    revision_id: UUID,
    db: AsyncSession,
) -> DrawingRevision | None:
    """Return an active revision visible through a non-deleted file and project."""

    result = await db.execute(
        select(DrawingRevision)
        .join(
            File,
            (File.id == DrawingRevision.source_file_id)
            & (File.project_id == DrawingRevision.project_id),
        )
        .join(Project, Project.id == DrawingRevision.project_id)
        .where(
            (DrawingRevision.id == revision_id)
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )
    return result.scalar_one_or_none()


@revisions_router.get("/files/{file_id}/revisions", response_model=DrawingRevisionListResponse)
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
        query
        .order_by(
            DrawingRevision.revision_sequence.asc(),
            DrawingRevision.created_at.asc(),
            DrawingRevision.id.asc(),
        )
        .limit(limit + 1)
    )
    revisions = result.scalars().all()
    page = revisions[:limit]
    next_cursor = None

    if len(revisions) > limit and page:
        last_revision = page[-1]
        next_cursor = _encode_cursor(
            _DrawingRevisionCursor(
                revision_sequence=last_revision.revision_sequence,
                created_at=last_revision.created_at,
                id=last_revision.id,
            )
        )

    return DrawingRevisionListResponse(
        items=[DrawingRevisionRead.model_validate(revision) for revision in page],
        next_cursor=next_cursor,
    )


@revisions_router.get(
    "/revisions/{revision_id}/adapter-output",
    response_model=AdapterRunOutputRead,
)
async def get_revision_adapter_output(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> AdapterRunOutputRead:
    """Return adapter output metadata for an active drawing revision."""

    result = await db.execute(
        select(AdapterRunOutput)
        .join(DrawingRevision, DrawingRevision.adapter_run_output_id == AdapterRunOutput.id)
        .join(
            File,
            (File.id == DrawingRevision.source_file_id)
            & (File.project_id == DrawingRevision.project_id),
        )
        .join(Project, Project.id == DrawingRevision.project_id)
        .where(
            (DrawingRevision.id == revision_id)
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )
    adapter_output = result.scalar_one_or_none()
    if adapter_output is None:
        revision = await _get_active_revision(revision_id, db)
        if revision is None:
            raise_not_found("Drawing revision", str(revision_id))
        raise_not_found("Adapter run output", str(revision_id))

    return AdapterRunOutputRead.model_validate(adapter_output)


@revisions_router.get(
    "/adapter-outputs/{adapter_output_id}",
    response_model=AdapterRunOutputRead,
)
async def get_adapter_output(
    adapter_output_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> AdapterRunOutputRead:
    """Return adapter output metadata by identifier."""

    result = await db.execute(
        select(AdapterRunOutput)
        .join(
            File,
            (File.id == AdapterRunOutput.source_file_id)
            & (File.project_id == AdapterRunOutput.project_id),
        )
        .join(Project, Project.id == AdapterRunOutput.project_id)
        .where(
            (AdapterRunOutput.id == adapter_output_id)
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )
    adapter_output = result.scalar_one_or_none()
    if adapter_output is None:
        raise_not_found("Adapter run output", str(adapter_output_id))

    return AdapterRunOutputRead.model_validate(adapter_output)


@revisions_router.get(
    "/files/{file_id}/generated-artifacts",
    response_model=GeneratedArtifactListResponse,
)
async def list_file_generated_artifacts(
    file_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
) -> GeneratedArtifactListResponse:
    """List active generated artifacts for a file."""

    file = await _get_active_file(file_id, db)
    if file is None:
        raise_not_found("File", str(file_id))

    pagination_cursor = _decode_artifact_cursor(cursor) if cursor else None

    query = (
        select(GeneratedArtifact)
        .join(
            File,
            (File.id == GeneratedArtifact.source_file_id)
            & (File.project_id == GeneratedArtifact.project_id),
        )
        .join(Project, Project.id == GeneratedArtifact.project_id)
        .where(
            (GeneratedArtifact.source_file_id == file_id)
            & (GeneratedArtifact.deleted_at.is_(None))
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )

    if pagination_cursor is not None:
        query = query.where(
            (GeneratedArtifact.created_at > pagination_cursor.created_at)
            | (
                (GeneratedArtifact.created_at == pagination_cursor.created_at)
                & (GeneratedArtifact.id > pagination_cursor.id)
            )
        )

    result = await db.execute(
        query.order_by(GeneratedArtifact.created_at.asc(), GeneratedArtifact.id.asc()).limit(
            limit + 1
        )
    )
    artifacts = result.scalars().all()
    page = artifacts[:limit]
    next_cursor = None

    if len(artifacts) > limit and page:
        last_artifact = page[-1]
        next_cursor = _encode_cursor(
            _GeneratedArtifactCursor(
                created_at=last_artifact.created_at,
                id=last_artifact.id,
            )
        )

    return GeneratedArtifactListResponse(
        items=[GeneratedArtifactRead.model_validate(artifact) for artifact in page],
        next_cursor=next_cursor,
    )


@revisions_router.get(
    "/revisions/{revision_id}/generated-artifacts",
    response_model=GeneratedArtifactListResponse,
)
async def list_revision_generated_artifacts(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
) -> GeneratedArtifactListResponse:
    """List active generated artifacts associated with a drawing revision."""

    revision = await _get_active_revision(revision_id, db)
    if revision is None:
        raise_not_found("Drawing revision", str(revision_id))

    pagination_cursor = _decode_artifact_cursor(cursor) if cursor else None

    query = (
        select(GeneratedArtifact)
        .join(
            File,
            (File.id == GeneratedArtifact.source_file_id)
            & (File.project_id == GeneratedArtifact.project_id),
        )
        .join(Project, Project.id == GeneratedArtifact.project_id)
        .where(
            (GeneratedArtifact.drawing_revision_id == revision_id)
            & (GeneratedArtifact.deleted_at.is_(None))
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )

    if pagination_cursor is not None:
        query = query.where(
            (GeneratedArtifact.created_at > pagination_cursor.created_at)
            | (
                (GeneratedArtifact.created_at == pagination_cursor.created_at)
                & (GeneratedArtifact.id > pagination_cursor.id)
            )
        )

    result = await db.execute(
        query.order_by(GeneratedArtifact.created_at.asc(), GeneratedArtifact.id.asc()).limit(
            limit + 1
        )
    )
    artifacts = result.scalars().all()
    page = artifacts[:limit]
    next_cursor = None

    if len(artifacts) > limit and page:
        last_artifact = page[-1]
        next_cursor = _encode_cursor(
            _GeneratedArtifactCursor(
                created_at=last_artifact.created_at,
                id=last_artifact.id,
            )
        )

    return GeneratedArtifactListResponse(
        items=[GeneratedArtifactRead.model_validate(artifact) for artifact in page],
        next_cursor=next_cursor,
    )


@revisions_router.get(
    "/revisions/{revision_id}/validation-report",
    response_model=ValidationReportResponse,
)
async def get_validation_report(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> ValidationReportResponse:
    """Return the persisted canonical validation report for a drawing revision."""
    result = await db.execute(
        select(ValidationReport)
        .join(
            DrawingRevision,
            DrawingRevision.id == ValidationReport.drawing_revision_id,
        )
        .join(
            File,
            (File.id == DrawingRevision.source_file_id)
            & (File.project_id == DrawingRevision.project_id),
        )
        .join(Project, Project.id == DrawingRevision.project_id)
        .where(
            (ValidationReport.drawing_revision_id == revision_id)
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )
    report = result.scalar_one_or_none()
    if report is None:
        revision_result = await db.execute(
            select(DrawingRevision)
            .join(
                File,
                (File.id == DrawingRevision.source_file_id)
                & (File.project_id == DrawingRevision.project_id),
            )
            .join(Project, Project.id == DrawingRevision.project_id)
            .where(
                (DrawingRevision.id == revision_id)
                & (File.deleted_at.is_(None))
                & (Project.deleted_at.is_(None))
            )
        )
        revision = revision_result.scalar_one_or_none()
        if revision is None:
            raise_not_found("Drawing revision", str(revision_id))
        raise_not_found("Validation report", str(revision_id))

    assert report is not None

    return build_validation_report_response(report)
