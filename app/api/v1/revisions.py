"""Revision API routes."""

import base64
import binascii
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.pagination import (
    decode_cursor_payload,
    encode_cursor_payload,
    raise_invalid_cursor,
)
from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response, raise_not_found
from app.db.session import get_db
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.file import File
from app.models.generated_artifact import GeneratedArtifact
from app.models.project import Project
from app.models.revision_materialization import (
    RevisionBlock,
    RevisionEntity,
    RevisionEntityManifest,
    RevisionLayer,
    RevisionLayout,
)
from app.models.validation_report import ValidationReport
from app.schemas.revision import (
    AdapterRunOutputRead,
    DrawingRevisionListResponse,
    DrawingRevisionRead,
    GeneratedArtifactListResponse,
    GeneratedArtifactRead,
    RevisionBlockListResponse,
    RevisionBlockRead,
    RevisionEntityListResponse,
    RevisionEntityManifestRead,
    RevisionEntityRead,
    RevisionLayerListResponse,
    RevisionLayerRead,
    RevisionLayoutListResponse,
    RevisionLayoutRead,
    RevisionMaterializationCounts,
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


def _encode_materialization_cursor(sequence_index: int, row_id: UUID) -> str:
    """Encode a materialization cursor from sequence index and row identifier."""

    return encode_cursor_payload(
        {
            "sequence_index": sequence_index,
            "id": str(row_id),
        }
    )


def _decode_materialization_cursor(cursor: str) -> tuple[int, UUID]:
    """Decode a materialization cursor into typed values."""

    try:
        cursor_data = decode_cursor_payload(cursor)
        return int(cursor_data["sequence_index"]), UUID(str(cursor_data["id"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise_invalid_cursor(exc)


def _raise_entities_not_materialized(revision_id: UUID) -> None:
    """Raise the standard conflict when revision entities are not materialized."""

    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=create_error_response(
            code=ErrorCode.NORMALIZED_ENTITIES_NOT_MATERIALIZED,
            message=(
                f"Normalized entities for drawing revision '{revision_id}' "
                "have not been materialized"
            ),
            details=None,
        ),
    )


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


async def _get_revision_manifest(
    revision_id: UUID,
    db: AsyncSession,
) -> RevisionEntityManifest | None:
    """Return the persisted materialization manifest for a revision, if present."""

    result = await db.execute(
        select(RevisionEntityManifest).where(
            RevisionEntityManifest.drawing_revision_id == revision_id
        )
    )
    return result.scalar_one_or_none()


async def _get_active_revision_manifest_or_409(
    revision_id: UUID,
    db: AsyncSession,
) -> RevisionEntityManifest:
    """Return an active revision manifest or raise the standard missing errors."""

    revision = await _get_active_revision(revision_id, db)
    if revision is None:
        raise_not_found("Drawing revision", str(revision_id))

    manifest = await _get_revision_manifest(revision_id, db)
    if manifest is None:
        _raise_entities_not_materialized(revision_id)

    assert manifest is not None
    return manifest


def _manifest_counts(
    manifest: RevisionEntityManifest,
) -> RevisionMaterializationCounts:
    """Convert persisted manifest counts to the public schema."""

    return RevisionMaterializationCounts.model_validate(manifest.counts_json)


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
    "/revisions/{revision_id}/layouts",
    response_model=RevisionLayoutListResponse,
)
async def list_revision_layouts(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
) -> RevisionLayoutListResponse:
    """List materialized layout rows for a drawing revision."""

    manifest = await _get_active_revision_manifest_or_409(revision_id, db)
    pagination_cursor = _decode_materialization_cursor(cursor) if cursor else None

    query = select(RevisionLayout).where(RevisionLayout.drawing_revision_id == revision_id)
    if pagination_cursor is not None:
        sequence_index, row_id = pagination_cursor
        query = query.where(
            (RevisionLayout.sequence_index > sequence_index)
            | (
                (RevisionLayout.sequence_index == sequence_index)
                & (RevisionLayout.id > row_id)
            )
        )

    result = await db.execute(
        query.order_by(
            RevisionLayout.sequence_index.asc(),
            RevisionLayout.id.asc(),
        ).limit(limit + 1)
    )
    rows = result.scalars().all()
    page = rows[:limit]
    next_cursor = None

    if len(rows) > limit and page:
        last_row = page[-1]
        next_cursor = _encode_materialization_cursor(last_row.sequence_index, last_row.id)

    counts = _manifest_counts(manifest)
    return RevisionLayoutListResponse(
        manifest=RevisionEntityManifestRead.model_validate(manifest),
        counts=counts,
        items=[RevisionLayoutRead.model_validate(row) for row in page],
        next_cursor=next_cursor,
    )


@revisions_router.get(
    "/revisions/{revision_id}/layers",
    response_model=RevisionLayerListResponse,
)
async def list_revision_layers(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
) -> RevisionLayerListResponse:
    """List materialized layer rows for a drawing revision."""

    manifest = await _get_active_revision_manifest_or_409(revision_id, db)
    pagination_cursor = _decode_materialization_cursor(cursor) if cursor else None

    query = select(RevisionLayer).where(RevisionLayer.drawing_revision_id == revision_id)
    if pagination_cursor is not None:
        sequence_index, row_id = pagination_cursor
        query = query.where(
            (RevisionLayer.sequence_index > sequence_index)
            | ((RevisionLayer.sequence_index == sequence_index) & (RevisionLayer.id > row_id))
        )

    result = await db.execute(
        query.order_by(RevisionLayer.sequence_index.asc(), RevisionLayer.id.asc()).limit(limit + 1)
    )
    rows = result.scalars().all()
    page = rows[:limit]
    next_cursor = None

    if len(rows) > limit and page:
        last_row = page[-1]
        next_cursor = _encode_materialization_cursor(last_row.sequence_index, last_row.id)

    counts = _manifest_counts(manifest)
    return RevisionLayerListResponse(
        manifest=RevisionEntityManifestRead.model_validate(manifest),
        counts=counts,
        items=[RevisionLayerRead.model_validate(row) for row in page],
        next_cursor=next_cursor,
    )


@revisions_router.get(
    "/revisions/{revision_id}/blocks",
    response_model=RevisionBlockListResponse,
)
async def list_revision_blocks(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
) -> RevisionBlockListResponse:
    """List materialized block rows for a drawing revision."""

    manifest = await _get_active_revision_manifest_or_409(revision_id, db)
    pagination_cursor = _decode_materialization_cursor(cursor) if cursor else None

    query = select(RevisionBlock).where(RevisionBlock.drawing_revision_id == revision_id)
    if pagination_cursor is not None:
        sequence_index, row_id = pagination_cursor
        query = query.where(
            (RevisionBlock.sequence_index > sequence_index)
            | ((RevisionBlock.sequence_index == sequence_index) & (RevisionBlock.id > row_id))
        )

    result = await db.execute(
        query.order_by(RevisionBlock.sequence_index.asc(), RevisionBlock.id.asc()).limit(limit + 1)
    )
    rows = result.scalars().all()
    page = rows[:limit]
    next_cursor = None

    if len(rows) > limit and page:
        last_row = page[-1]
        next_cursor = _encode_materialization_cursor(last_row.sequence_index, last_row.id)

    counts = _manifest_counts(manifest)
    return RevisionBlockListResponse(
        manifest=RevisionEntityManifestRead.model_validate(manifest),
        counts=counts,
        items=[RevisionBlockRead.model_validate(row) for row in page],
        next_cursor=next_cursor,
    )


@revisions_router.get(
    "/revisions/{revision_id}/entities",
    response_model=RevisionEntityListResponse,
)
async def list_revision_entities(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
    entity_type: str | None = Query(default=None),
    layout_ref: str | None = Query(default=None),
    layer_ref: str | None = Query(default=None),
    block_ref: str | None = Query(default=None),
    parent_entity_ref: str | None = Query(default=None),
    source_identity: str | None = Query(default=None),
    source_hash: str | None = Query(default=None),
) -> RevisionEntityListResponse:
    """List materialized entity rows for a drawing revision."""

    manifest = await _get_active_revision_manifest_or_409(revision_id, db)
    pagination_cursor = _decode_materialization_cursor(cursor) if cursor else None

    query = select(RevisionEntity).where(RevisionEntity.drawing_revision_id == revision_id)
    if entity_type is not None:
        query = query.where(RevisionEntity.entity_type == entity_type)
    if layout_ref is not None:
        query = query.where(RevisionEntity.layout_ref == layout_ref)
    if layer_ref is not None:
        query = query.where(RevisionEntity.layer_ref == layer_ref)
    if block_ref is not None:
        query = query.where(RevisionEntity.block_ref == block_ref)
    if parent_entity_ref is not None:
        query = query.where(RevisionEntity.parent_entity_ref == parent_entity_ref)
    if source_identity is not None:
        query = query.where(RevisionEntity.source_identity == source_identity)
    if source_hash is not None:
        query = query.where(RevisionEntity.source_hash == source_hash)
    if pagination_cursor is not None:
        sequence_index, row_id = pagination_cursor
        query = query.where(
            (RevisionEntity.sequence_index > sequence_index)
            | ((RevisionEntity.sequence_index == sequence_index) & (RevisionEntity.id > row_id))
        )

    result = await db.execute(
        query.order_by(
            RevisionEntity.sequence_index.asc(),
            RevisionEntity.id.asc(),
        ).limit(limit + 1)
    )
    rows = result.scalars().all()
    page = rows[:limit]
    next_cursor = None

    if len(rows) > limit and page:
        last_row = page[-1]
        next_cursor = _encode_materialization_cursor(last_row.sequence_index, last_row.id)

    counts = _manifest_counts(manifest)
    return RevisionEntityListResponse(
        manifest=RevisionEntityManifestRead.model_validate(manifest),
        counts=counts,
        items=[RevisionEntityRead.model_validate(row) for row in page],
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
