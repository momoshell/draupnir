"""Pydantic schemas for persisted jobs."""

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class JobRead(BaseModel):
    """Schema for reading a persisted background job."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID = Field(..., description="Unique job identifier (UUID)")
    project_id: uuid.UUID = Field(..., description="Owning project identifier")
    file_id: uuid.UUID = Field(..., description="Associated file identifier")
    job_type: str = Field(..., description="Job type")
    status: str = Field(..., description="Job status")
    attempts: int = Field(..., ge=0, description="Current attempt count")
    max_attempts: int = Field(..., ge=1, description="Maximum retry attempts")
    cancel_requested: bool = Field(..., description="Whether cancellation was requested")
    error_code: str | None = Field(None, description="Machine-readable error code")
    error_message: str | None = Field(None, description="Human-readable error message")
    started_at: datetime | None = Field(None, description="Job start timestamp")
    finished_at: datetime | None = Field(None, description="Job completion timestamp")
    created_at: datetime = Field(..., description="Job creation timestamp")
