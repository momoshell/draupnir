"""Pydantic schemas for Project CRUD operations."""

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class ProjectCreate(BaseModel):
    """Schema for creating a new project."""

    name: str = Field(..., min_length=1, max_length=255, description="Project name")
    description: str | None = Field(
        None, max_length=1024, description="Optional project description"
    )
    default_unit_system: str | None = Field(
        None, max_length=64, description="Default unit system (e.g., 'metric', 'imperial')"
    )
    default_currency: str | None = Field(
        None, min_length=3, max_length=3, description="Default currency code (ISO 4217)"
    )


class ProjectRead(BaseModel):
    """Schema for reading a project."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID = Field(..., description="Unique project identifier (UUID)")
    name: str = Field(..., description="Project name")
    description: str | None = Field(None, description="Optional project description")
    default_unit_system: str | None = Field(None, description="Default unit system")
    default_currency: str | None = Field(None, description="Default currency code")
    created_at: datetime = Field(..., description="Project creation timestamp")
    updated_at: datetime = Field(..., description="Project last update timestamp")


class ProjectUpdate(BaseModel):
    """Schema for updating an existing project."""

    name: str | None = Field(None, min_length=1, max_length=255, description="Project name")
    description: str | None = Field(
        None, max_length=1024, description="Optional project description"
    )
    default_unit_system: str | None = Field(
        None, max_length=64, description="Default unit system"
    )
    default_currency: str | None = Field(
        None, min_length=3, max_length=3, description="Default currency code"
    )


class ProjectListResponse(BaseModel):
    """Schema for paginated list of projects."""

    items: list[ProjectRead] = Field(..., description="List of projects")
    next_cursor: str | None = Field(None, description="Cursor for next page, null if last page")
