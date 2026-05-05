"""Pydantic schemas for project file endpoints."""

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class FileRead(BaseModel):
    """Schema for reading a file resource."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID = Field(..., description="Unique file identifier (UUID)")
    project_id: uuid.UUID = Field(..., description="Owning project identifier")
    original_filename: str = Field(..., description="Original uploaded filename")
    media_type: str = Field(..., description="Uploaded media type")
    detected_format: str | None = Field(None, description="Detected drawing/document format")
    size_bytes: int = Field(..., ge=0, description="Uploaded file size in bytes")
    checksum_sha256: str = Field(..., min_length=64, max_length=64, description="SHA-256 checksum")
    immutable: bool = Field(..., description="Whether original upload is immutable")
    initial_job_id: uuid.UUID | None = Field(
        None,
        description="Initial ingest job identifier created during upload",
    )
    initial_extraction_profile_id: uuid.UUID | None = Field(
        None,
        description="Initial extraction profile identifier created during upload",
    )
    created_at: datetime = Field(..., description="File creation timestamp")


class FileListResponse(BaseModel):
    """Schema for project files list responses."""

    items: list[FileRead] = Field(..., description="List of project files")
    next_cursor: str | None = Field(None, description="Cursor for next page, null if last page")
