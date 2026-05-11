"""Project-scoped file upload and retrieval endpoints."""

import hashlib
import uuid
from collections.abc import Sequence
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, status
from fastapi import File as FilePart
from fastapi.responses import JSONResponse, Response
from sqlalchemy import select
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
from app.core.config import settings
from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response, raise_not_found
from app.db.session import get_db
from app.jobs.worker import enqueue_ingest_job
from app.models.drawing_revision import DrawingRevision
from app.models.extraction_profile import ExtractionProfile
from app.models.file import File as FileModel
from app.models.job import Job
from app.models.project import Project
from app.schemas.extraction_profile import ExtractionProfileCreate, FileReprocessRequest
from app.schemas.file import FileListResponse, FileRead
from app.schemas.job import JobRead
from app.storage import Storage, get_storage
from app.storage.keys import build_original_storage_key

files_router = APIRouter()
_UPLOAD_CHUNK_SIZE_BYTES = 1024 * 1024
_UPLOAD_SNIFF_BYTES = 4096
_MAX_PUBLIC_JOB_ERROR_MESSAGE_LENGTH = 255
_PUBLIC_ENQUEUE_FAILURE_MESSAGE = "Failed to enqueue ingest job"
# UPLOAD_FORMAT_SIGNATURES:
# _sniff_format accepts these leading-byte signatures for upload detection:
# - PDF: b"%PDF-"
# - DWG: b"AC10"
# - IFC: b"ISO-10303-21"
# - Binary DXF: b"AutoCAD Binary DXF\r\n\x1a\x00"
# - Text DXF: optional UTF-8 BOM, then optional ASCII whitespace, then the
#   DXF group header for group code 0 and SECTION (b"0\nSECTION" or
#   b"0\r\nSECTION").
_BINARY_DXF_SENTINEL = b"AutoCAD Binary DXF\r\n\x1a\x00"
_UTF8_BOM = b"\xef\xbb\xbf"
_SUPPORTED_FORMATS_MESSAGE = "Unsupported file format. Supported formats: pdf, dwg, dxf, ifc."
_MAX_ORIGINAL_FILENAME_LENGTH = 512
_MAX_MEDIA_TYPE_LENGTH = 255


def _encode_cursor(created_at: datetime, file_id: UUID) -> str:
    """Encode cursor from created_at and file_id."""
    return encode_cursor_payload(
        {
            "created_at": created_at.isoformat(),
            "id": str(file_id),
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


def _sniff_format(initial_bytes: bytes) -> str | None:
    """Infer format using file header/early bytes."""
    if initial_bytes.startswith(b"%PDF-"):
        return "pdf"
    if initial_bytes.startswith(b"AC10"):
        return "dwg"
    if initial_bytes.startswith(b"ISO-10303-21"):
        return "ifc"
    if initial_bytes.startswith(_BINARY_DXF_SENTINEL):
        return "dxf"

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


def _upload_size_limit_message(max_upload_mb: int) -> str:
    """Build an upload-size validation message that includes configured cap."""
    return f"Uploaded file exceeds maximum allowed size of {max_upload_mb} MB."


def _staging_path(file_id: UUID) -> Path:
    """Build a temporary staging path for upload bytes before promotion."""
    return _upload_root() / ".staging" / f"{file_id}.{uuid.uuid4().hex}.part"


def _upload_root() -> Path:
    """Return the canonical local storage root for uploads and artifacts."""
    return Path(settings.storage_local_root).resolve()


def _cleanup_uploaded_path(storage_path: Path) -> None:
    """Best-effort cleanup of a partially or fully written upload path."""
    upload_root = _upload_root()
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
    with suppress(Exception):
        await storage.delete_failed_put(storage_key, storage_uri=storage_uri)


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


def _build_extraction_profile(
    project_id: UUID,
    profile: ExtractionProfileCreate | None = None,
) -> ExtractionProfile:
    """Construct an immutable extraction profile row."""
    profile_input = profile or ExtractionProfileCreate()
    return ExtractionProfile(
        id=uuid.uuid4(),
        project_id=project_id,
        **profile_input.model_dump(),
    )


async def _get_project_file_or_404(
    db: AsyncSession,
    project_id: UUID,
    file_id: UUID,
    *,
    for_update: bool = False,
) -> FileModel:
    """Return a project-scoped file row or raise not found."""
    query = (
        select(FileModel)
        .join(Project, Project.id == FileModel.project_id)
        .where(
            (FileModel.project_id == project_id)
            & (FileModel.id == file_id)
            & (FileModel.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )
    if for_update:
        query = query.with_for_update()
    file_row = (await db.execute(query)).scalar_one_or_none()
    if file_row is None:
        raise_not_found("File", str(file_id))
    assert file_row is not None
    return file_row


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


async def _resolve_project_extraction_profile(
    db: AsyncSession,
    project_id: UUID,
    request: FileReprocessRequest,
) -> ExtractionProfile:
    """Return an existing project profile or stage a new immutable one."""
    if request.extraction_profile_id is not None:
        return await _get_existing_project_extraction_profile_or_404(
            db,
            project_id,
            request.extraction_profile_id,
        )

    assert request.extraction_profile is not None
    profile_row = _build_extraction_profile(project_id, request.extraction_profile)
    db.add(profile_row)
    return profile_row


async def _get_existing_project_extraction_profile_or_404(
    db: AsyncSession,
    project_id: UUID,
    extraction_profile_id: UUID,
) -> ExtractionProfile:
    """Return an existing extraction profile for the project or raise not found."""

    query = select(ExtractionProfile).where(
        (ExtractionProfile.project_id == project_id)
        & (ExtractionProfile.id == extraction_profile_id)
    )
    profile_row = (await db.execute(query)).scalar_one_or_none()
    if profile_row is None:
        raise_not_found("ExtractionProfile", str(extraction_profile_id))
    assert profile_row is not None
    return profile_row


async def _get_latest_finalized_revision(
    db: AsyncSession,
    project_id: UUID,
    file_id: UUID,
) -> DrawingRevision | None:
    """Return the latest finalized drawing revision for a file."""

    query = (
        select(DrawingRevision)
        .where(
            (DrawingRevision.project_id == project_id)
            & (DrawingRevision.source_file_id == file_id)
        )
        .order_by(DrawingRevision.revision_sequence.desc())
        .limit(1)
    )
    return (await db.execute(query)).scalar_one_or_none()


def _raise_reprocess_base_revision_conflict() -> None:
    """Raise the standardized missing-base revision conflict response."""

    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=create_error_response(
            code=ErrorCode.REVISION_CONFLICT,
            message="Reprocess requires a finalized base revision.",
            details=None,
        ),
    )


async def _mark_job_enqueue_failed(db: AsyncSession, job: Job, exc: Exception) -> None:
    """Persist a visible failed job after enqueue publish errors."""
    _ = exc
    job.status = "failed"
    job.error_code = ErrorCode.INTERNAL_ERROR.value
    job.error_message = _PUBLIC_ENQUEUE_FAILURE_MESSAGE[:_MAX_PUBLIC_JOB_ERROR_MESSAGE_LENGTH]
    job.finished_at = datetime.now(UTC)
    await db.commit()
    await db.refresh(job)


async def _mark_job_enqueue_failed_with_snapshot(
    db: AsyncSession,
    job: Job,
    exc: Exception,
    *,
    reservation: IdempotencyReservation | None,
) -> dict[str, Any]:
    """Persist enqueue failure state and finalize a replay snapshot when present."""

    _ = exc
    job.status = "failed"
    job.error_code = ErrorCode.INTERNAL_ERROR.value
    job.error_message = _PUBLIC_ENQUEUE_FAILURE_MESSAGE[:_MAX_PUBLIC_JOB_ERROR_MESSAGE_LENGTH]
    job.finished_at = datetime.now(UTC)
    error_body = create_error_response(
        code=ErrorCode.INTERNAL_ERROR,
        message=_PUBLIC_ENQUEUE_FAILURE_MESSAGE,
        details=_enqueue_failure_details(job),
    )
    if reservation is not None:
        await mark_idempotency_completed(
            db,
            reservation,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            response_body=error_body,
        )
    await db.commit()
    await db.refresh(job)
    return error_body


async def _raise_upload_storage_failure(
    db: AsyncSession,
    reservation: IdempotencyReservation | None,
    *,
    message: str,
    cause: Exception | None = None,
) -> None:
    """Raise a sanitized storage error and finalize idempotency when reserved."""

    error_body = create_error_response(
        code=ErrorCode.STORAGE_FAILED,
        message=message,
        details=None,
    )
    if reservation is not None:
        await mark_idempotency_completed(
            db,
            reservation,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            response_body=error_body,
        )
        await db.commit()

    if cause is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_body,
        )
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=error_body,
    ) from cause


def _enqueue_failure_details(job: Job) -> dict[str, str]:
    """Return safe durable identifiers and status for a failed enqueue response."""
    assert job.extraction_profile_id is not None
    return {
        "file_id": str(job.file_id),
        "job_id": str(job.id),
        "extraction_profile_id": str(job.extraction_profile_id),
        "status": job.status,
    }


def _attach_initial_upload_metadata(file_row: FileModel) -> FileModel:
    """Attach durable initial-ingest metadata fields for response serialization."""
    response_file = cast(Any, file_row)
    response_file.initial_job_id = file_row.initial_job_id
    response_file.initial_extraction_profile_id = file_row.initial_extraction_profile_id
    return file_row


async def _attach_initial_upload_metadata_for_files(
    db: AsyncSession,
    file_rows: Sequence[FileModel],
) -> list[FileModel]:
    """Attach durable initial ingest identifiers for file responses."""
    _ = db
    if not file_rows:
        return []

    return [_attach_initial_upload_metadata(file_row) for file_row in file_rows]


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
    idempotency_key: Annotated[str | None, Depends(get_idempotency_key)] = None,
) -> FileModel | Response:
    """Upload immutable source file bytes for a project and create ingest job."""
    if idempotency_key is None:
        await _get_active_project_or_404(db, project_id)

    original_filename = file.filename or "upload.bin"
    media_type = file.content_type or "application/octet-stream"
    reservation: IdempotencyReservation | None = None
    fingerprint: str | None = None

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
    upload_root = _upload_root()
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
                            message=_upload_size_limit_message(settings.max_upload_mb),
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
                            message=_upload_size_limit_message(settings.max_upload_mb),
                            details=None,
                        ),
                    )

                stream.write(chunk)
                checksum_builder.update(chunk)
                total_bytes = next_total

        checksum = checksum_builder.hexdigest()
        storage_key = build_original_storage_key(file_id, checksum)

        if idempotency_key is not None:
            fingerprint = build_idempotency_fingerprint(
                f"files.upload:{project_id}",
                {
                    "project_id": str(project_id),
                    "original_filename": original_filename,
                    "media_type": media_type,
                    "detected_format": detected_format,
                    "size_bytes": total_bytes,
                    "checksum_sha256": checksum,
                },
            )
            replay = await replay_idempotency(
                db,
                key=idempotency_key,
                fingerprint=fingerprint,
            )
            if replay is not None:
                return replay.response
            await _get_active_project_or_404(db, project_id)
            claim = await claim_idempotency(
                db,
                key=idempotency_key,
                fingerprint=fingerprint,
                method="POST",
                path=f"/projects/{project_id}/files",
            )
            if isinstance(claim, IdempotencyReplay):
                return claim.response
            reservation = claim

        try:
            stored_object = await storage.put(storage_key, staging_path, immutable=True)
            if stored_object.checksum_sha256 != checksum:
                await _cleanup_persisted_upload(storage, storage_key, stored_object.storage_uri)
                await _raise_upload_storage_failure(
                    db,
                    reservation,
                    message="Stored file checksum mismatch detected.",
                )
            storage_uri = stored_object.storage_uri
        except FileExistsError as exc:
            await _raise_upload_storage_failure(
                db,
                reservation,
                message="Storage collision occurred during upload.",
                cause=exc,
            )
        except OSError as exc:
            await _raise_upload_storage_failure(
                db,
                reservation,
                message="Failed to persist uploaded file.",
                cause=exc,
            )
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

    try:
        await _get_active_project_or_404(db, project_id, for_update=True)
    except Exception:
        await db.rollback()
        await _cleanup_persisted_upload(storage, storage_key, storage_uri)
        raise

    extraction_profile = _build_extraction_profile(project_id)
    db.add(extraction_profile)

    ingest_job = Job(
        id=uuid.uuid4(),
        project_id=project_id,
        file_id=file_id,
        extraction_profile_id=extraction_profile.id,
        job_type="ingest",
        status="pending",
        attempts=0,
        max_attempts=3,
        cancel_requested=False,
    )

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
        initial_job_id=ingest_job.id,
        initial_extraction_profile_id=extraction_profile.id,
    )
    db.add(file_row)
    db.add(ingest_job)

    await db.commit()

    await db.refresh(file_row)
    await db.refresh(ingest_job)

    try:
        enqueue_ingest_job(ingest_job.id)
    except Exception as exc:
        error_body = await _mark_job_enqueue_failed_with_snapshot(
            db,
            ingest_job,
            exc,
            reservation=reservation,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_body,
        ) from exc

    if reservation is not None:
        success_body = FileRead.model_validate(
            _attach_initial_upload_metadata(file_row)
        ).model_dump(mode="json")
        await mark_idempotency_completed(
            db,
            reservation,
            status_code=status.HTTP_201_CREATED,
            response_body=success_body,
        )
        await db.commit()
        return JSONResponse(status_code=status.HTTP_201_CREATED, content=success_body)

    return _attach_initial_upload_metadata(file_row)


@files_router.post(
    "/projects/{project_id}/files/{file_id}/reprocess",
    response_model=JobRead,
    status_code=status.HTTP_202_ACCEPTED,
)
async def reprocess_project_file(
    project_id: UUID,
    file_id: UUID,
    request: FileReprocessRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    idempotency_key: Annotated[str | None, Depends(get_idempotency_key)] = None,
) -> Job | Response:
    """Create a new pending reprocess job for an existing file and profile selection."""
    reservation: IdempotencyReservation | None = None
    fingerprint: str | None = None
    if idempotency_key is not None:
        fingerprint = build_idempotency_fingerprint(
            f"files.reprocess:{project_id}:{file_id}",
            request.model_dump(mode="json"),
        )
        replay = await replay_idempotency(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
        )
        if replay is not None:
            return replay.response

    await _get_active_project_or_404(db, project_id)
    await _get_project_file_or_404(db, project_id, file_id)
    if request.extraction_profile_id is not None:
        await _get_existing_project_extraction_profile_or_404(
            db,
            project_id,
            request.extraction_profile_id,
        )

    if idempotency_key is not None:
        await _get_active_project_or_404(db, project_id, for_update=True)
        await _get_project_file_or_404(db, project_id, file_id, for_update=True)
        if await _get_latest_finalized_revision(db, project_id, file_id) is None:
            await db.rollback()
            _raise_reprocess_base_revision_conflict()

    if idempotency_key is not None:
        assert fingerprint is not None
        claim = await claim_idempotency(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
            method="POST",
            path=f"/projects/{project_id}/files/{file_id}/reprocess",
        )
        if isinstance(claim, IdempotencyReplay):
            return claim.response
        reservation = claim
    await _get_active_project_or_404(db, project_id, for_update=True)
    await _get_project_file_or_404(db, project_id, file_id, for_update=True)
    base_revision = await _get_latest_finalized_revision(db, project_id, file_id)
    if base_revision is None:
        await db.rollback()
        _raise_reprocess_base_revision_conflict()
    assert base_revision is not None

    extraction_profile = await _resolve_project_extraction_profile(db, project_id, request)

    ingest_job = Job(
        project_id=project_id,
        file_id=file_id,
        extraction_profile_id=extraction_profile.id,
        base_revision_id=base_revision.id,
        job_type="reprocess",
        status="pending",
        attempts=0,
        max_attempts=3,
        cancel_requested=False,
    )
    db.add(ingest_job)
    await db.commit()
    await db.refresh(ingest_job)

    try:
        enqueue_ingest_job(ingest_job.id)
    except Exception as exc:
        error_body = await _mark_job_enqueue_failed_with_snapshot(
            db,
            ingest_job,
            exc,
            reservation=reservation,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_body,
        ) from exc

    if reservation is not None:
        success_body = JobRead.model_validate(ingest_job).model_dump(mode="json")
        await mark_idempotency_completed(
            db,
            reservation,
            status_code=status.HTTP_202_ACCEPTED,
            response_body=success_body,
        )
        await db.commit()
        return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=success_body)

    return ingest_job


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
    await _get_active_project_or_404(db, project_id)

    query = (
        select(FileModel)
        .where(
            (FileModel.project_id == project_id) & (FileModel.deleted_at.is_(None))
        )
        .order_by(FileModel.created_at.desc(), FileModel.id.desc())
    )

    if cursor:
        created_at, file_id = _decode_cursor(cursor)
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

    items = [
        FileRead.model_validate(row)
        for row in await _attach_initial_upload_metadata_for_files(db, rows)
    ]
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
    file_row = await _get_project_file_or_404(db, project_id, file_id)
    await _attach_initial_upload_metadata_for_files(db, [file_row])
    return file_row
