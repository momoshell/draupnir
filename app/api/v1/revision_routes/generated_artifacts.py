"""Revision generated-artifact read routes."""

import asyncio
import shutil
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Annotated, NoReturn
from urllib.parse import quote
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.pagination import DEFAULT_PAGE_SIZE as _DEFAULT_PAGE_SIZE
from app.api.pagination import MAX_PAGE_SIZE as _MAX_PAGE_SIZE
from app.api.pagination import paginate_overfetched as _paginate_overfetched
from app.api.v1.revision_cursors import (
    _decode_artifact_cursor,
    _encode_cursor,
    _GeneratedArtifactCursor,
)
from app.api.v1.revision_lineage import _get_active_file, _get_active_revision
from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response, raise_not_found
from app.db.session import get_db
from app.models.file import File
from app.models.generated_artifact import GeneratedArtifact
from app.models.project import Project
from app.schemas.revision import GeneratedArtifactListResponse, GeneratedArtifactRead
from app.storage import Storage, get_storage
from app.storage.base import StorageChecksumMismatchError, StorageReadError

generated_artifacts_router = APIRouter()

_DOWNLOAD_CHUNK_SIZE_BYTES = 1024 * 1024


def _content_disposition(filename: str) -> str:
    """Return a safe attachment Content-Disposition for an artifact name."""

    ascii_fallback = (
        "".join(char for char in filename if 32 <= ord(char) < 127 and char not in '"\\').strip()
        or "artifact"
    )
    encoded = quote(filename, safe="")
    return f"attachment; filename=\"{ascii_fallback}\"; filename*=UTF-8''{encoded}"


def _raise_artifact_storage_unavailable(artifact_id: UUID) -> NoReturn:
    """Raise a sanitized 500 when stored artifact bytes are missing or corrupt."""

    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=create_error_response(
            code=ErrorCode.STORAGE_FAILED,
            message=f"Stored bytes for generated artifact '{artifact_id}' are unavailable",
            details=None,
        ),
    )


async def _get_visible_generated_artifact(
    artifact_id: UUID,
    db: AsyncSession,
) -> GeneratedArtifact | None:
    """Return an active generated artifact visible through non-deleted lineage."""

    result = await db.execute(
        select(GeneratedArtifact)
        .join(
            File,
            (File.id == GeneratedArtifact.source_file_id)
            & (File.project_id == GeneratedArtifact.project_id),
        )
        .join(Project, Project.id == GeneratedArtifact.project_id)
        .where(
            (GeneratedArtifact.id == artifact_id)
            & (GeneratedArtifact.deleted_at.is_(None))
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )
    return result.scalar_one_or_none()


@generated_artifacts_router.get(
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
    page, next_cursor = _paginate_overfetched(
        result.scalars().all(),
        limit=limit,
        encode_cursor=lambda last_artifact: _encode_cursor(
            _GeneratedArtifactCursor(
                created_at=last_artifact.created_at,
                id=last_artifact.id,
            )
        ),
    )

    return GeneratedArtifactListResponse(
        items=[GeneratedArtifactRead.model_validate(artifact) for artifact in page],
        next_cursor=next_cursor,
    )


@generated_artifacts_router.get("/generated-artifacts/{artifact_id}/download")
async def download_generated_artifact(
    artifact_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    storage: Annotated[Storage, Depends(get_storage)],
) -> Response:
    """Stream stored bytes for a generated artifact visible through active lineage."""

    artifact = await _get_visible_generated_artifact(artifact_id, db)
    if artifact is None:
        raise_not_found("Generated artifact", str(artifact_id))
    assert artifact is not None

    # Stage the object to a private temp file and stream that to the client, instead of
    # loading the whole body into memory. ``copy_to_path`` verifies the checksum during
    # the copy and raises before any bytes reach the client, so the integrity guarantee
    # is preserved. ``copy_to_path`` creates the destination exclusively, so we hand it
    # a fresh path inside a temp directory we own and clean up.
    temp_dir = Path(await asyncio.to_thread(tempfile.mkdtemp, prefix="artifact-download-"))
    temp_path = temp_dir / "artifact"
    try:
        meta = await storage.copy_to_path(
            artifact.storage_key,
            temp_path,
            expected_checksum_sha256=artifact.checksum_sha256,
        )
    except (FileNotFoundError, KeyError, StorageChecksumMismatchError, StorageReadError):
        await asyncio.to_thread(shutil.rmtree, temp_dir, ignore_errors=True)
        # Immutable lineage guarantees bytes exist; absence or corruption is a
        # server-side integrity failure, never a client-facing not-found.
        _raise_artifact_storage_unavailable(artifact_id)

    async def _stream_and_cleanup() -> AsyncIterator[bytes]:
        try:
            with temp_path.open("rb") as stream:
                while chunk := await asyncio.to_thread(stream.read, _DOWNLOAD_CHUNK_SIZE_BYTES):
                    yield chunk
        finally:
            # Runs on normal completion and on client disconnect (generator close).
            await asyncio.to_thread(shutil.rmtree, temp_dir, ignore_errors=True)

    return StreamingResponse(
        _stream_and_cleanup(),
        media_type=artifact.media_type,
        headers={
            "Content-Disposition": _content_disposition(artifact.name),
            "Content-Length": str(meta.size_bytes),
            "ETag": f'"{artifact.checksum_sha256}"',
            "X-Checksum-SHA256": artifact.checksum_sha256,
        },
    )


@generated_artifacts_router.get(
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
    page, next_cursor = _paginate_overfetched(
        result.scalars().all(),
        limit=limit,
        encode_cursor=lambda last_artifact: _encode_cursor(
            _GeneratedArtifactCursor(
                created_at=last_artifact.created_at,
                id=last_artifact.id,
            )
        ),
    )

    return GeneratedArtifactListResponse(
        items=[GeneratedArtifactRead.model_validate(artifact) for artifact in page],
        next_cursor=next_cursor,
    )
