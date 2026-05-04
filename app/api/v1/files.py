"""Project-scoped file upload and retrieval endpoints."""

import base64
import binascii
import hashlib
import json
import uuid
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, status
from fastapi import File as FilePart
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response, raise_not_found
from app.db.session import get_db
from app.jobs.worker import enqueue_ingest_job
from app.models.file import File as FileModel
from app.models.job import Job
from app.models.project import Project
from app.schemas.file import FileListResponse, FileRead
from app.storage import Storage, get_storage

files_router = APIRouter()
_UPLOAD_CHUNK_SIZE_BYTES = 1024 * 1024
_UPLOAD_SNIFF_BYTES = 4096
_UTF8_BOM = b"\xef\xbb\xbf"
_SUPPORTED_FORMATS_MESSAGE = "Unsupported file format. Supported formats: pdf, dwg, dxf, ifc."
_MAX_ORIGINAL_FILENAME_LENGTH = 512
_MAX_MEDIA_TYPE_LENGTH = 255


def _encode_cursor(created_at: datetime, file_id: UUID) -> str:
    """Encode cursor from created_at and file_id."""
    cursor_data = {
        "created_at": created_at.isoformat(),
        "id": str(file_id),
    }
    return base64.urlsafe_b64encode(json.dumps(cursor_data).encode()).decode().rstrip("=")


def _decode_cursor(cursor: str) -> dict[str, Any]:
    """Decode cursor to dict with created_at and id."""
    try:
        padding = 4 - (len(cursor) % 4)
        if padding != 4:
            cursor += "=" * padding

        decoded = base64.urlsafe_b64decode(cursor)
        cursor_data_raw = json.loads(decoded.decode("utf-8"))
        if not isinstance(cursor_data_raw, dict):
            raise TypeError("Cursor payload must be a JSON object")
        cursor_data = cast(dict[str, Any], cursor_data_raw)

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


def _sniff_format(initial_bytes: bytes) -> str | None:
    """Infer format using file header/early bytes."""
    if initial_bytes.startswith(b"%PDF-"):
        return "pdf"
    if initial_bytes.startswith(b"AC10"):
        return "dwg"
    if initial_bytes.startswith(b"ISO-10303-21"):
        return "ifc"

    dxf_probe = initial_bytes
    if dxf_probe.startswith(_UTF8_BOM):
        dxf_probe = dxf_probe[len(_UTF8_BOM) :]

    dxf_probe = dxf_probe.lstrip(b" \t\n\r\f\v")
    if dxf_probe.startswith((b"0\nSECTION", b"0\r\nSECTION")):
        return "dxf"

    return None


def _unsupported_format_exception() -> HTTPException:
    """Construct a consistent unsupported format error response."""
    return HTTPException(
        status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
        detail=create_error_response(
            code=ErrorCode.INPUT_UNSUPPORTED_FORMAT,
            message=_SUPPORTED_FORMATS_MESSAGE,
            details=None,
        ),
    )


def _staging_path(file_id: UUID) -> Path:
    """Build a temporary staging path for upload bytes before promotion."""
    upload_root = Path(settings.upload_storage_root).resolve()
    return upload_root / ".staging" / f"{file_id}.{uuid.uuid4().hex}.part"


def _storage_key(file_id: UUID, checksum: str) -> str:
    """Build the server-derived immutable storage key for an uploaded file."""
    return f"originals/{file_id}/{checksum}"


def _cleanup_uploaded_path(storage_path: Path) -> None:
    """Best-effort cleanup of a partially or fully written upload path."""
    upload_root = Path(settings.upload_storage_root).resolve()
    with suppress(OSError):
        storage_path.unlink(missing_ok=True)

    current = storage_path.parent
    while current != upload_root and upload_root in current.parents:
        with suppress(OSError):
            current.rmdir()
        current = current.parent


def _ensure_private_directory(path: Path, *, include_parents_until: Path | None = None) -> None:
    """Ensure a directory exists with owner-only permissions."""
    path.mkdir(parents=True, exist_ok=True)
    targets = [path]
    if include_parents_until is not None:
        parent = path.parent
        while include_parents_until in parent.parents or parent == include_parents_until:
            targets.append(parent)
            if parent == include_parents_until:
                break
            parent = parent.parent

    try:
        for target in targets:
            target.chmod(0o700)
    except OSError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=create_error_response(
                code=ErrorCode.STORAGE_FAILED,
                message="Failed to persist uploaded file.",
                details=None,
            ),
        ) from exc


async def _cleanup_persisted_upload(storage: Storage, storage_key: str, storage_uri: str) -> None:
    """Best-effort cleanup for a persisted upload after downstream failure."""
    if storage_uri.startswith("file://"):
        upload_root = Path(settings.upload_storage_root).resolve()
        stored_path = Path(storage_uri.removeprefix("file://")).resolve()
        try:
            stored_path.relative_to(upload_root)
        except ValueError:
            pass
        else:
            _cleanup_uploaded_path(stored_path)
            return

    with suppress(Exception):
        await storage.delete(storage_key)


async def _raise_input_invalid_for_upload_metadata(file: UploadFile, message: str) -> None:
    """Close upload and raise standardized client validation error envelope."""
    await file.close()
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=create_error_response(
            code=ErrorCode.INPUT_INVALID,
            message=message,
            details=None,
        ),
    )


@files_router.post(
    "/projects/{project_id}/files",
    response_model=FileRead,
    status_code=status.HTTP_201_CREATED,
)
async def upload_project_file(
    project_id: UUID,
    file: Annotated[UploadFile, FilePart(...)],
    db: Annotated[AsyncSession, Depends(get_db)],
    storage: Annotated[Storage, Depends(get_storage)],
) -> FileModel:
    """Upload immutable source file bytes for a project and create ingest job."""
    project = await db.get(Project, project_id)
    if project is None:
        raise_not_found("Project", str(project_id))

    original_filename = file.filename or "upload.bin"
    media_type = file.content_type or "application/octet-stream"

    if len(original_filename) > _MAX_ORIGINAL_FILENAME_LENGTH:
        await _raise_input_invalid_for_upload_metadata(
            file,
            "original_filename exceeds maximum length of 512 characters.",
        )
    if len(media_type) > _MAX_MEDIA_TYPE_LENGTH:
        await _raise_input_invalid_for_upload_metadata(
            file,
            "media_type exceeds maximum length of 255 characters.",
        )

    file_id = uuid.uuid4()
    staging_path = _staging_path(file_id)
    storage_key: str | None = None
    storage_uri: str | None = None
    detected_format: str | None = None
    upload_root = Path(settings.upload_storage_root).resolve()
    _ensure_private_directory(upload_root)
    _ensure_private_directory(staging_path.parent)

    max_upload_bytes = settings.max_upload_mb * 1024 * 1024
    total_bytes = 0
    checksum_builder = hashlib.sha256()
    try:
        with staging_path.open("xb") as stream:
            initial_bytes = await file.read(_UPLOAD_SNIFF_BYTES)
            sniffed_format = _sniff_format(initial_bytes)
            if sniffed_format is None:
                _cleanup_uploaded_path(staging_path)
                raise _unsupported_format_exception()
            detected_format = sniffed_format

            if initial_bytes:
                if len(initial_bytes) > max_upload_bytes:
                    _cleanup_uploaded_path(staging_path)
                    raise HTTPException(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        detail=create_error_response(
                            code=ErrorCode.INPUT_INVALID,
                            message="Uploaded file exceeds maximum allowed size.",
                            details=None,
                        ),
                    )
                stream.write(initial_bytes)
                checksum_builder.update(initial_bytes)
                total_bytes = len(initial_bytes)

            while True:
                chunk = await file.read(_UPLOAD_CHUNK_SIZE_BYTES)
                if not chunk:
                    break

                next_total = total_bytes + len(chunk)
                if next_total > max_upload_bytes:
                    _cleanup_uploaded_path(staging_path)
                    raise HTTPException(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        detail=create_error_response(
                            code=ErrorCode.INPUT_INVALID,
                            message="Uploaded file exceeds maximum allowed size.",
                            details=None,
                        ),
                    )

                stream.write(chunk)
                checksum_builder.update(chunk)
                total_bytes = next_total

        checksum = checksum_builder.hexdigest()
        storage_key = _storage_key(file_id, checksum)

        try:
            stored_object = await storage.put(storage_key, staging_path, immutable=True)
            storage_uri = stored_object.storage_uri
        except FileExistsError as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=create_error_response(
                    code=ErrorCode.STORAGE_FAILED,
                    message="Storage collision occurred during upload.",
                    details=None,
                ),
            ) from exc
        except OSError as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=create_error_response(
                    code=ErrorCode.STORAGE_FAILED,
                    message="Failed to persist uploaded file.",
                    details=None,
                ),
            ) from exc
    except HTTPException:
        _cleanup_uploaded_path(staging_path)
        raise
    except BaseException:
        _cleanup_uploaded_path(staging_path)
        raise
    finally:
        _cleanup_uploaded_path(staging_path)
        await file.close()

    assert detected_format is not None
    assert storage_key is not None
    assert storage_uri is not None

    file_row = FileModel(
        id=file_id,
        project_id=project_id,
        original_filename=original_filename,
        media_type=media_type,
        detected_format=detected_format,
        storage_uri=storage_uri,
        size_bytes=total_bytes,
        checksum_sha256=checksum,
        immutable=True,
    )
    db.add(file_row)

    ingest_job = Job(
        project_id=project_id,
        file_id=file_id,
        job_type="ingest",
        status="pending",
        attempts=0,
        max_attempts=3,
        cancel_requested=False,
    )
    db.add(ingest_job)

    try:
        await db.commit()
    except BaseException:
        assert storage_key is not None
        assert storage_uri is not None
        await _cleanup_persisted_upload(storage, storage_key, storage_uri)
        raise

    await db.refresh(file_row)
    await db.refresh(ingest_job)

    try:
        enqueue_ingest_job(ingest_job.id)
    except Exception as exc:
        ingest_job.status = "failed"
        ingest_job.error_code = ErrorCode.INTERNAL_ERROR.value
        ingest_job.error_message = f"Failed to enqueue ingest job: {exc}"
        ingest_job.finished_at = datetime.now(UTC)
        await db.commit()

    return file_row


@files_router.get(
    "/projects/{project_id}/files",
    response_model=FileListResponse,
)
async def list_project_files(
    project_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    cursor: Annotated[str | None, Query(description="Cursor for pagination")] = None,
    limit: Annotated[int, Query(ge=1, le=200, description="Number of items to return")] = 50,
) -> FileListResponse:
    """List files for a project with cursor pagination."""
    project = await db.get(Project, project_id)
    if project is None:
        raise_not_found("Project", str(project_id))

    query = (
        select(FileModel)
        .where(FileModel.project_id == project_id)
        .order_by(FileModel.created_at.desc(), FileModel.id.desc())
    )

    if cursor:
        cursor_data = _decode_cursor(cursor)
        created_at = datetime.fromisoformat(str(cursor_data["created_at"]))
        file_id = UUID(str(cursor_data["id"]))
        query = query.filter(
            (FileModel.created_at < created_at)
            | ((FileModel.created_at == created_at) & (FileModel.id < file_id))
        )

    rows = (await db.execute(query.limit(limit + 1))).scalars().all()

    has_next = len(rows) > limit
    if has_next:
        rows = rows[:-1]

    next_cursor = None
    if has_next and rows:
        last_item = rows[-1]
        next_cursor = _encode_cursor(last_item.created_at, last_item.id)

    items = [FileRead.model_validate(row) for row in rows]
    return FileListResponse(items=items, next_cursor=next_cursor)


@files_router.get(
    "/projects/{project_id}/files/{file_id}",
    response_model=FileRead,
)
async def get_project_file(
    project_id: UUID,
    file_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> FileModel:
    """Get a single file by id within a project scope."""
    query = select(FileModel).where(
        (FileModel.project_id == project_id) & (FileModel.id == file_id)
    )
    file_row = (await db.execute(query)).scalar_one_or_none()
    if file_row is None:
        raise_not_found("File", str(file_id))
    assert file_row is not None
    return file_row
