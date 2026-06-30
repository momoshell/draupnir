"""Upload intake helpers for project file routes."""

from __future__ import annotations

import hashlib
import json
import uuid
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn
from uuid import UUID

from fastapi import HTTPException, UploadFile, status
from pydantic import ValidationError

from app.core.config import settings
from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response
from app.schemas.extraction_profile import ExtractionProfileCreate

UPLOAD_CHUNK_SIZE_BYTES = 1024 * 1024
UPLOAD_SNIFF_BYTES = 4096
# UPLOAD_FORMAT_SIGNATURES:
# sniff_format accepts these leading-byte signatures for upload detection:
# - PDF: b"%PDF-"
# - DWG: b"AC10"
# - IFC: b"ISO-10303-21"
# - Binary DXF: b"AutoCAD Binary DXF\r\n\x1a\x00"
# - Text DXF: optional UTF-8 BOM, then optional ASCII whitespace, then the
#   DXF group header for group code 0 and SECTION (b"0\nSECTION" or
#   b"0\r\nSECTION").
BINARY_DXF_SENTINEL = b"AutoCAD Binary DXF\r\n\x1a\x00"
UTF8_BOM = b"\xef\xbb\xbf"
SUPPORTED_FORMATS_MESSAGE = "Unsupported file format. Supported formats: pdf, dwg, dxf, ifc."
MAX_ORIGINAL_FILENAME_LENGTH = 512
MAX_MEDIA_TYPE_LENGTH = 255


@dataclass(frozen=True, slots=True)
class StagedUpload:
    """Validated upload bytes staged on local disk before storage promotion."""

    file_id: UUID
    staging_path: Path
    detected_format: str
    size_bytes: int
    checksum_sha256: str


def sniff_format(initial_bytes: bytes) -> str | None:
    """Infer format using file header/early bytes."""
    if initial_bytes.startswith(b"%PDF-"):
        return "pdf"
    if initial_bytes.startswith(b"AC10"):
        return "dwg"
    if initial_bytes.startswith(b"ISO-10303-21"):
        return "ifc"
    if initial_bytes.startswith(BINARY_DXF_SENTINEL):
        return "dxf"

    dxf_probe = initial_bytes
    if dxf_probe.startswith(UTF8_BOM):
        dxf_probe = dxf_probe[len(UTF8_BOM) :]

    dxf_probe = dxf_probe.lstrip(b" \t\n\r\f\v")
    if dxf_probe.startswith((b"0\nSECTION", b"0\r\nSECTION")):
        return "dxf"

    return None


def unsupported_format_exception() -> HTTPException:
    """Construct a consistent unsupported format error response."""
    return HTTPException(
        status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
        detail=create_error_response(
            code=ErrorCode.INPUT_UNSUPPORTED_FORMAT,
            message=SUPPORTED_FORMATS_MESSAGE,
            details=None,
        ),
    )


def upload_size_limit_message(max_upload_mb: int) -> str:
    """Build an upload-size validation message that includes configured cap."""
    return f"Uploaded file exceeds maximum allowed size of {max_upload_mb} MB."


def staging_path(file_id: UUID) -> Path:
    """Build a temporary staging path for upload bytes before promotion."""
    return upload_root() / ".staging" / f"{file_id}.{uuid.uuid4().hex}.part"


def upload_root() -> Path:
    """Return the canonical local storage root for uploads and artifacts."""
    return Path(settings.storage_local_root).resolve()


def cleanup_uploaded_path(storage_path: Path) -> None:
    """Best-effort cleanup of a partially or fully written upload path."""
    root = upload_root()
    with suppress(OSError):
        storage_path.unlink(missing_ok=True)

    current = storage_path.parent
    while current != root and root in current.parents:
        with suppress(OSError):
            current.rmdir()
        current = current.parent


def ensure_private_directory(path: Path, *, include_parents_until: Path | None = None) -> None:
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


async def parse_upload_extraction_profile(
    file: UploadFile,
    extraction_profile: str | None,
) -> ExtractionProfileCreate | None:
    """Validate an optional multipart upload extraction profile payload."""
    if extraction_profile is None:
        return None

    try:
        profile_payload = json.loads(extraction_profile)
    except json.JSONDecodeError:
        await raise_input_invalid_for_upload_metadata(
            file,
            "extraction_profile must be valid JSON.",
        )

    if not isinstance(profile_payload, dict):
        await raise_input_invalid_for_upload_metadata(
            file,
            "extraction_profile must be a JSON object.",
        )

    try:
        return ExtractionProfileCreate.model_validate(profile_payload)
    except ValidationError as exc:
        await raise_input_invalid_for_upload_metadata(
            file,
            "extraction_profile is invalid.",
            details=exc.errors(),
        )


def dump_extraction_profile_payload(
    profile: ExtractionProfileCreate | None,
) -> dict[str, Any] | None:
    """Serialize an optional extraction profile for stable request fingerprinting."""
    if profile is None:
        return None
    return profile.model_dump(mode="json")


async def validate_upload_metadata(
    file: UploadFile,
    *,
    original_filename: str,
    media_type: str,
) -> None:
    """Validate upload metadata before idempotency claims or byte staging."""
    if len(original_filename) > MAX_ORIGINAL_FILENAME_LENGTH:
        await raise_input_invalid_for_upload_metadata(
            file,
            "original_filename exceeds maximum length of 512 characters.",
        )
    if len(media_type) > MAX_MEDIA_TYPE_LENGTH:
        await raise_input_invalid_for_upload_metadata(
            file,
            "media_type exceeds maximum length of 255 characters.",
        )


async def raise_input_invalid_for_upload_metadata(
    file: UploadFile,
    message: str,
    *,
    details: Any = None,
) -> NoReturn:
    """Close upload and raise standardized client validation error envelope."""
    await file.close()
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=create_error_response(
            code=ErrorCode.INPUT_INVALID,
            message=message,
            details=details,
        ),
    )


async def stage_upload_file(file: UploadFile, *, file_id: UUID) -> StagedUpload:
    """Stream upload bytes into a private staging file with format/size checks."""
    path = staging_path(file_id)
    root = upload_root()
    ensure_private_directory(root)
    ensure_private_directory(path.parent)

    max_upload_bytes = settings.max_upload_mb * 1024 * 1024
    total_bytes = 0
    checksum_builder = hashlib.sha256()
    detected_format: str | None = None
    try:
        with path.open("xb") as stream:
            initial_bytes = await file.read(UPLOAD_SNIFF_BYTES)
            sniffed_format = sniff_format(initial_bytes)
            if sniffed_format is None:
                cleanup_uploaded_path(path)
                raise unsupported_format_exception()
            detected_format = sniffed_format

            if initial_bytes:
                if len(initial_bytes) > max_upload_bytes:
                    cleanup_uploaded_path(path)
                    raise _upload_too_large_exception()
                stream.write(initial_bytes)
                checksum_builder.update(initial_bytes)
                total_bytes = len(initial_bytes)

            while True:
                chunk = await file.read(UPLOAD_CHUNK_SIZE_BYTES)
                if not chunk:
                    break

                next_total = total_bytes + len(chunk)
                if next_total > max_upload_bytes:
                    cleanup_uploaded_path(path)
                    raise _upload_too_large_exception()

                stream.write(chunk)
                checksum_builder.update(chunk)
                total_bytes = next_total
    except BaseException:
        cleanup_uploaded_path(path)
        with suppress(Exception):
            await file.close()
        raise

    assert detected_format is not None
    return StagedUpload(
        file_id=file_id,
        staging_path=path,
        detected_format=detected_format,
        size_bytes=total_bytes,
        checksum_sha256=checksum_builder.hexdigest(),
    )


def _upload_too_large_exception() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
        detail=create_error_response(
            code=ErrorCode.INPUT_INVALID,
            message=upload_size_limit_message(settings.max_upload_mb),
            details=None,
        ),
    )
