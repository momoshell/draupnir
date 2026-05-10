"""Pydantic schemas for Project CRUD operations."""

import uuid
from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

ProjectUnitSystem = Literal["metric", "imperial"]
CurrencyCode = Annotated[
    str,
    StringConstraints(min_length=3, max_length=3, pattern=r"^[A-Z]{3}$"),
]


class ProjectCreate(BaseModel):
    """Schema for creating a new project."""

    name: str = Field(..., min_length=1, max_length=255, description="Project name")
    description: str | None = Field(
        None, max_length=1024, description="Optional project description"
    )
    default_unit_system: ProjectUnitSystem | None = Field(
        None, max_length=64, description="Default unit system (e.g., 'metric', 'imperial')"
    )
    default_currency: CurrencyCode | None = Field(
        None,
        min_length=3,
        max_length=3,
        description="Default 3-letter uppercase currency code",
    )


class ProjectRead(BaseModel):
    """Schema for reading a project."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID = Field(..., description="Unique project identifier (UUID)")
    name: str = Field(..., description="Project name")
    description: str | None = Field(None, description="Optional project description")
    default_unit_system: str | None = Field(None, description="Default unit system")
    default_currency: str | None = Field(
        None, description="Default 3-letter uppercase currency code"
    )
    created_at: datetime = Field(..., description="Project creation timestamp")
    updated_at: datetime = Field(..., description="Project last update timestamp")


class ProjectUpdate(BaseModel):
    """Schema for updating an existing project."""

    name: str | None = Field(None, min_length=1, max_length=255, description="Project name")
    description: str | None = Field(
        None, max_length=1024, description="Optional project description"
    )
    default_unit_system: ProjectUnitSystem | None = Field(
        None, max_length=64, description="Default unit system"
    )
    default_currency: CurrencyCode | None = Field(
        None,
        min_length=3,
        max_length=3,
        description="Default 3-letter uppercase currency code",
    )


class ProjectListResponse(BaseModel):
    """Schema for paginated list of projects."""

    items: list[ProjectRead] = Field(..., description="List of projects")
    next_cursor: str | None = Field(None, description="Cursor for next page, null if last page")
