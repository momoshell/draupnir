"""Project-scoped file upload and retrieval endpoints."""

import uuid
from collections.abc import Sequence
from contextlib import suppress
from datetime import datetime
from typing import Annotated, Any, cast
from uuid import UUID

from fastapi import APIRouter, Depends, Form, HTTPException, Query, UploadFile, status
from fastapi import File as FilePart
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.idempotency import (
    IdempotentMutationKnownError,
    IdempotentMutationSuccess,
    build_idempotency_fingerprint,
    get_idempotency_key,
    run_idempotent_mutation,
)
from app.api.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    decode_cursor_payload,
    encode_keyset_cursor,
    paginate_overfetched,
    read_cursor_datetime,
    read_cursor_uuid,
)
from app.api.v1.file_uploads import (
    cleanup_uploaded_path as _cleanup_uploaded_path,
)
from app.api.v1.file_uploads import (
    dump_extraction_profile_payload as _dump_extraction_profile_payload,
)
from app.api.v1.file_uploads import (
    parse_upload_extraction_profile as _parse_upload_extraction_profile,
)
from app.api.v1.file_uploads import (
    stage_upload_file,
    validate_upload_metadata,
)
from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response, raise_not_found
from app.db.session import get_db
from app.jobs.worker import (
    enqueue_ingest_job as _enqueue_ingest_job,
)
from app.jobs.worker import (
    prepare_job_enqueue_intent,
    publish_job_enqueue_intent,
)
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


def enqueue_ingest_job(job_id: UUID) -> None:
    """Compatibility wrapper kept for test fixture patching."""
    _enqueue_ingest_job(job_id)


def _encode_cursor(created_at: datetime, file_id: UUID) -> str:
    """Encode cursor from created_at and file_id."""
    return encode_keyset_cursor(
        {
            "created_at": created_at.isoformat(),
            "id": str(file_id),
        }
    )


def _decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    """Decode cursor to typed created_at and id values."""
    cursor_data = decode_cursor_payload(cursor)
    return read_cursor_datetime(cursor_data, "created_at"), read_cursor_uuid(cursor_data, "id")


async def _cleanup_persisted_upload(storage: Storage, storage_key: str, storage_uri: str) -> None:
    """Best-effort cleanup for a persisted upload after downstream failure."""
    with suppress(Exception):
        await storage.delete_failed_put(storage_key, storage_uri=storage_uri)


async def _rollback_upload_cleanup_transaction(db: AsyncSession) -> None:
    """Best-effort rollback before upload cleanup paths."""
    with suppress(BaseException):
        await db.rollback()


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
    query = select(Project).where((Project.id == project_id) & (Project.deleted_at.is_(None)))
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
            (DrawingRevision.project_id == project_id) & (DrawingRevision.source_file_id == file_id)
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


def _upload_storage_failure_response_body(message: str) -> dict[str, Any]:
    """Build a sanitized known upload storage failure response body."""

    return create_error_response(
        code=ErrorCode.STORAGE_FAILED,
        message=message,
        details=None,
    )


def _known_upload_storage_failure(message: str) -> IdempotentMutationKnownError:
    """Build an explicit replay-safe upload storage failure snapshot."""

    return IdempotentMutationKnownError(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        response_body=_upload_storage_failure_response_body(message),
    )


def _raise_upload_storage_failure(message: str, *, cause: Exception | None = None) -> None:
    """Raise a sanitized upload storage failure response."""

    error_body = _upload_storage_failure_response_body(message)
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=error_body,
    ) from cause


def _serialize_uploaded_file(file_row: FileModel) -> dict[str, Any]:
    """Serialize a file row using the public upload response contract."""

    return FileRead.model_validate(_attach_initial_upload_metadata(file_row)).model_dump(
        mode="json"
    )


def _serialize_job(job: Job) -> dict[str, Any]:
    """Serialize a job row using the public jobs response contract."""

    return JobRead.model_validate(job).model_dump(mode="json")


async def _publish_ingest_job_after_commit(job_id: UUID) -> None:
    """Best-effort publish of a committed ingest/reprocess enqueue intent."""

    await publish_job_enqueue_intent(
        job_id,
        publisher=enqueue_ingest_job,
        suppress_exceptions=True,
    )


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
    extraction_profile: Annotated[str | None, Form()] = None,
    idempotency_key: Annotated[str | None, Depends(get_idempotency_key)] = None,
) -> FileModel | Response:
    """Upload immutable source file bytes for a project and create ingest job."""
    if idempotency_key is None:
        try:
            await _get_active_project_or_404(db, project_id)
        except BaseException:
            await file.close()
            raise

    original_filename = file.filename or "upload.bin"
    media_type = file.content_type or "application/octet-stream"

    await validate_upload_metadata(
        file,
        original_filename=original_filename,
        media_type=media_type,
    )

    upload_extraction_profile = await _parse_upload_extraction_profile(file, extraction_profile)

    file_id = uuid.uuid4()
    staged_upload = await stage_upload_file(file, file_id=file_id)
    staging_path = staged_upload.staging_path
    detected_format = staged_upload.detected_format
    total_bytes = staged_upload.size_bytes
    checksum = staged_upload.checksum_sha256
    persisted_upload_uri: str | None = None
    try:
        storage_key = build_original_storage_key(file_id, checksum)

        async def _finalize_upload(persisted_storage_uri: str) -> FileModel:
            try:
                await _get_active_project_or_404(db, project_id, for_update=True)
                extraction_profile_row = _build_extraction_profile(
                    project_id,
                    upload_extraction_profile,
                )
                db.add(extraction_profile_row)

                ingest_job = Job(
                    id=uuid.uuid4(),
                    project_id=project_id,
                    file_id=file_id,
                    extraction_profile_id=extraction_profile_row.id,
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
                    storage_uri=persisted_storage_uri,
                    size_bytes=total_bytes,
                    checksum_sha256=checksum,
                    immutable=True,
                    initial_job_id=ingest_job.id,
                    initial_extraction_profile_id=extraction_profile_row.id,
                )
                db.add(file_row)
                db.add(ingest_job)
                prepare_job_enqueue_intent(ingest_job)
                await db.flush()
                await db.refresh(file_row)
                await db.refresh(ingest_job)

                return _attach_initial_upload_metadata(file_row)
            except BaseException:
                nonlocal persisted_upload_uri
                await _rollback_upload_cleanup_transaction(db)
                await _cleanup_persisted_upload(storage, storage_key, persisted_storage_uri)
                if persisted_upload_uri == persisted_storage_uri:
                    persisted_upload_uri = None
                raise

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
                    "extraction_profile": _dump_extraction_profile_payload(
                        upload_extraction_profile
                    ),
                },
            )

            async def _preclaim_upload() -> Response | None:
                await _get_active_project_or_404(db, project_id)
                return None

            async def _cleanup_pre_commit_failed_upload() -> None:
                nonlocal persisted_upload_uri
                if persisted_upload_uri is None:
                    return
                await _rollback_upload_cleanup_transaction(db)
                await _cleanup_persisted_upload(storage, storage_key, persisted_upload_uri)
                persisted_upload_uri = None

            async def _mutate_upload() -> (
                IdempotentMutationSuccess[FileModel] | IdempotentMutationKnownError
            ):
                nonlocal persisted_upload_uri
                try:
                    stored_object = await storage.put(storage_key, staging_path, immutable=True)
                except FileExistsError:
                    return _known_upload_storage_failure(
                        "Storage collision occurred during upload."
                    )
                except OSError:
                    return _known_upload_storage_failure("Failed to persist uploaded file.")

                if stored_object.checksum_sha256 != checksum:
                    await _cleanup_persisted_upload(storage, storage_key, stored_object.storage_uri)
                    return _known_upload_storage_failure("Stored file checksum mismatch detected.")

                persisted_upload_uri = stored_object.storage_uri
                file_row = await _finalize_upload(persisted_upload_uri)
                return IdempotentMutationSuccess(
                    value=file_row,
                    status_code=status.HTTP_201_CREATED,
                    after_commit=lambda: _publish_ingest_job_after_commit(
                        cast(UUID, file_row.initial_job_id)
                    ),
                )

            return await run_idempotent_mutation(
                db,
                key=idempotency_key,
                fingerprint=fingerprint,
                method="POST",
                path=f"/projects/{project_id}/files",
                preclaim=_preclaim_upload,
                mutate=_mutate_upload,
                serialize_result=_serialize_uploaded_file,
                pre_commit_failure_cleanup=_cleanup_pre_commit_failed_upload,
            )

        try:
            stored_object = await storage.put(storage_key, staging_path, immutable=True)
            if stored_object.checksum_sha256 != checksum:
                await _cleanup_persisted_upload(storage, storage_key, stored_object.storage_uri)
                _raise_upload_storage_failure("Stored file checksum mismatch detected.")
            storage_uri = stored_object.storage_uri
        except FileExistsError as exc:
            _raise_upload_storage_failure(
                "Storage collision occurred during upload.",
                cause=exc,
            )
        except OSError as exc:
            _raise_upload_storage_failure(
                "Failed to persist uploaded file.",
                cause=exc,
            )

        file_row = await _finalize_upload(storage_uri)
        await db.commit()
        await _publish_ingest_job_after_commit(cast(UUID, file_row.initial_job_id))
        return file_row
    except HTTPException:
        _cleanup_uploaded_path(staging_path)
        raise
    except BaseException:
        _cleanup_uploaded_path(staging_path)
        raise
    finally:
        _cleanup_uploaded_path(staging_path)
        await file.close()


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
    if idempotency_key is None:
        await _get_active_project_or_404(db, project_id)
        await _get_project_file_or_404(db, project_id, file_id)
        if request.extraction_profile_id is not None:
            await _get_existing_project_extraction_profile_or_404(
                db,
                project_id,
                request.extraction_profile_id,
            )

    if idempotency_key is not None:
        fingerprint = build_idempotency_fingerprint(
            f"files.reprocess:{project_id}:{file_id}",
            request.model_dump(mode="json"),
        )

        async def _preclaim_reprocess() -> Response | None:
            await _get_active_project_or_404(db, project_id)
            await _get_project_file_or_404(db, project_id, file_id)
            if request.extraction_profile_id is not None:
                await _get_existing_project_extraction_profile_or_404(
                    db,
                    project_id,
                    request.extraction_profile_id,
                )

            await _get_active_project_or_404(db, project_id, for_update=True)
            await _get_project_file_or_404(db, project_id, file_id, for_update=True)
            if await _get_latest_finalized_revision(db, project_id, file_id) is None:
                await db.rollback()
                _raise_reprocess_base_revision_conflict()
            return None

        async def _mutate_reprocess() -> IdempotentMutationSuccess[Job]:
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
            prepare_job_enqueue_intent(ingest_job)
            await db.flush()
            await db.refresh(ingest_job)

            return IdempotentMutationSuccess(
                value=ingest_job,
                status_code=status.HTTP_202_ACCEPTED,
                after_commit=lambda: _publish_ingest_job_after_commit(ingest_job.id),
            )

        return await run_idempotent_mutation(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
            method="POST",
            path=f"/projects/{project_id}/files/{file_id}/reprocess",
            preclaim=_preclaim_reprocess,
            mutate=_mutate_reprocess,
            serialize_result=_serialize_job,
        )

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
    prepare_job_enqueue_intent(ingest_job)
    await db.flush()
    await db.refresh(ingest_job)

    await db.commit()
    await _publish_ingest_job_after_commit(ingest_job.id)

    return ingest_job


@files_router.get(
    "/projects/{project_id}/files",
    response_model=FileListResponse,
)
async def list_project_files(
    project_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    cursor: Annotated[str | None, Query(description="Cursor for pagination")] = None,
    limit: Annotated[
        int,
        Query(ge=1, le=MAX_PAGE_SIZE, description="Number of items to return"),
    ] = DEFAULT_PAGE_SIZE,
) -> FileListResponse:
    """List files for a project with cursor pagination."""
    await _get_active_project_or_404(db, project_id)

    query = (
        select(FileModel)
        .where((FileModel.project_id == project_id) & (FileModel.deleted_at.is_(None)))
        .order_by(FileModel.created_at.desc(), FileModel.id.desc())
    )

    if cursor:
        created_at, file_id = _decode_cursor(cursor)
        query = query.filter(
            (FileModel.created_at < created_at)
            | ((FileModel.created_at == created_at) & (FileModel.id < file_id))
        )

    rows = (await db.execute(query.limit(limit + 1))).scalars().all()
    page, next_cursor = paginate_overfetched(
        rows,
        limit=limit,
        encode_cursor=lambda file_row: _encode_cursor(file_row.created_at, file_row.id),
    )

    items = [
        FileRead.model_validate(row)
        for row in await _attach_initial_upload_metadata_for_files(db, page)
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
