"""Pydantic schemas for persisted jobs."""

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.core.errors import ErrorCode
from app.models.job import JobStatus, JobType


class JobRead(BaseModel):
    """Schema for reading a persisted background job."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID = Field(..., description="Unique job identifier (UUID)")
    project_id: uuid.UUID = Field(..., description="Owning project identifier")
    file_id: uuid.UUID = Field(..., description="Associated file identifier")
    extraction_profile_id: uuid.UUID | None = Field(
        default=None,
        description=(
            "Immutable extraction profile identifier. Nullable for historical jobs "
            "during the migration rollback window; persisted ingest/reprocess jobs "
            "require a profile and a future contract migration can enforce non-null."
        ),
    )
    job_type: JobType = Field(..., description="Job type")
    status: JobStatus = Field(..., description="Job status")
    attempts: int = Field(..., ge=0, description="Current attempt count")
    max_attempts: int = Field(..., ge=1, description="Maximum retry attempts")
    cancel_requested: bool = Field(..., description="Whether cancellation was requested")
    error_code: ErrorCode | None = Field(None, description="Machine-readable error code")
    error_message: str | None = Field(None, description="Human-readable error message")
    started_at: datetime | None = Field(None, description="Job start timestamp")
    finished_at: datetime | None = Field(None, description="Job completion timestamp")
    created_at: datetime = Field(..., description="Job creation timestamp")


class JobEventRead(BaseModel):
    """Schema for reading a persisted job event."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID = Field(..., description="Unique job event identifier (UUID)")
    job_id: uuid.UUID = Field(..., description="Owning job identifier")
    level: str = Field(..., description="Structured event level")
    message: str = Field(..., description="Human-readable event message")
    data_json: dict[str, Any] | None = Field(None, description="Optional structured event payload")
    created_at: datetime = Field(..., description="Event creation timestamp")


class JobEventPage(BaseModel):
    """Cursor-paginated job event response."""

    items: list[JobEventRead] = Field(default_factory=list, description="Ordered job events")
    next_cursor: str | None = Field(None, description="Opaque cursor for the next page")
