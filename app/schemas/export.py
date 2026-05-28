"""Pydantic schemas for export job inputs."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ExportKind(StrEnum):
    """Export kind string constants."""

    REVISION_JSON = "revision_json"
    QUANTITY_CSV = "quantity_csv"
    ESTIMATE_CSV = "estimate_csv"
    ESTIMATE_PDF = "estimate_pdf"


class ExportFormat(StrEnum):
    """Export format string constants."""

    JSON = "json"
    CSV = "csv"
    PDF = "pdf"


EXPORT_KIND_MATRIX: dict[ExportKind, tuple[ExportFormat, str]] = {
    ExportKind.REVISION_JSON: (ExportFormat.JSON, "application/json"),
    ExportKind.QUANTITY_CSV: (ExportFormat.CSV, "text/csv"),
    ExportKind.ESTIMATE_CSV: (ExportFormat.CSV, "text/csv"),
    ExportKind.ESTIMATE_PDF: (ExportFormat.PDF, "application/pdf"),
}
ESTIMATE_EXPORT_KINDS = {ExportKind.ESTIMATE_CSV, ExportKind.ESTIMATE_PDF}


class ExportJobInputBase(BaseModel):
    """Shared immutable export job input payload fields."""

    model_config = ConfigDict(extra="forbid")

    export_kind: ExportKind
    export_format: ExportFormat
    media_type: str
    options_json: dict[str, Any] = Field(default_factory=dict)
    quantity_takeoff_id: UUID | None = None
    estimate_version_id: UUID | None = None

    @model_validator(mode="after")
    def validate_export_contract(self) -> ExportJobInputBase:
        matrix_entry = EXPORT_KIND_MATRIX.get(self.export_kind)
        if matrix_entry is None:
            raise ValueError(f"Unsupported export_kind: {self.export_kind}")

        expected_format, expected_media_type = matrix_entry
        if self.export_format != expected_format or self.media_type != expected_media_type:
            raise ValueError(
                "export_kind, export_format, and media_type must match the supported matrix"
            )

        if self.export_kind == ExportKind.QUANTITY_CSV:
            if self.quantity_takeoff_id is None or self.estimate_version_id is not None:
                raise ValueError(
                    "quantity_csv exports require quantity_takeoff_id and forbid "
                    "estimate_version_id"
                )
        elif self.export_kind in ESTIMATE_EXPORT_KINDS:
            if self.estimate_version_id is None or self.quantity_takeoff_id is None:
                raise ValueError(
                    "estimate exports require both quantity_takeoff_id and estimate_version_id"
                )
        elif self.quantity_takeoff_id is not None or self.estimate_version_id is not None:
            raise ValueError(
                "revision_json exports forbid quantity_takeoff_id and estimate_version_id"
            )

        return self


class ExportJobInputCreate(ExportJobInputBase):
    """Schema used when persisting immutable export job inputs."""


class ExportJobInputRead(ExportJobInputBase):
    """Schema returned when reading persisted immutable export job inputs."""

    source_job_id: UUID
    project_id: UUID
    source_file_id: UUID
    drawing_revision_id: UUID
    source_job_type: str
    quantity_gate: str | None = None
    trusted_totals: bool | None = None
    created_at: datetime
