"""Immutable export job input model."""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.job import JobType, _sql_in_list


class ExportKind(StrEnum):
    """Supported export payload kinds."""

    REVISION_JSON = "revision_json"
    DXF = "dxf"
    REVISED_DXF = "revised_dxf"
    QUANTITY_CSV = "quantity_csv"
    ESTIMATE_CSV = "estimate_csv"
    ESTIMATE_PDF = "estimate_pdf"


class ExportFormat(StrEnum):
    """Supported export artifact formats."""

    JSON = "json"
    DXF = "dxf"
    CSV = "csv"
    PDF = "pdf"


_EXPORT_KIND_VALUES = tuple(kind.value for kind in ExportKind)
_EXPORT_FORMAT_VALUES = tuple(export_format.value for export_format in ExportFormat)
_EXPORT_MEDIA_TYPES = (
    "application/json",
    "application/dxf",
    "text/csv",
    "application/pdf",
)


class ExportJobInput(Base):
    """Immutable export job inputs pinned to a single export job."""

    __tablename__ = "export_job_inputs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="RESTRICT",
            name="fk_export_job_inputs_source_file_id_project_id_files",
        ),
        ForeignKeyConstraint(
            ["drawing_revision_id", "project_id", "source_file_id"],
            [
                "drawing_revisions.id",
                "drawing_revisions.project_id",
                "drawing_revisions.source_file_id",
            ],
            ondelete="RESTRICT",
            name="fk_export_job_inputs_revision_lineage",
        ),
        ForeignKeyConstraint(
            [
                "source_job_id",
                "project_id",
                "source_file_id",
                "drawing_revision_id",
                "source_job_type",
            ],
            [
                "jobs.id",
                "jobs.project_id",
                "jobs.file_id",
                "jobs.base_revision_id",
                "jobs.job_type",
            ],
            ondelete="RESTRICT",
            name="fk_export_job_inputs_source_job_lineage_jobs",
        ),
        ForeignKeyConstraint(
            [
                "quantity_takeoff_id",
                "project_id",
                "drawing_revision_id",
                "quantity_gate",
                "trusted_totals",
            ],
            [
                "quantity_takeoffs.id",
                "quantity_takeoffs.project_id",
                "quantity_takeoffs.drawing_revision_id",
                "quantity_takeoffs.quantity_gate",
                "quantity_takeoffs.trusted_totals",
            ],
            ondelete="RESTRICT",
            name="fk_export_job_inputs_trusted_quantity_takeoff",
        ),
        ForeignKeyConstraint(
            [
                "estimate_version_id",
                "project_id",
                "drawing_revision_id",
                "quantity_takeoff_id",
            ],
            [
                "estimate_versions.id",
                "estimate_versions.project_id",
                "estimate_versions.drawing_revision_id",
                "estimate_versions.quantity_takeoff_id",
            ],
            ondelete="RESTRICT",
            name="fk_export_job_inputs_estimate_version_lineage",
        ),
        CheckConstraint(
            f"source_job_type = '{JobType.EXPORT.value}'",
            name="ck_export_job_inputs_source_job_type_export",
        ),
        CheckConstraint(
            f"export_kind IN ({_sql_in_list(_EXPORT_KIND_VALUES)})",
            name="ck_export_job_inputs_export_kind_valid",
        ),
        CheckConstraint(
            f"export_format IN ({_sql_in_list(_EXPORT_FORMAT_VALUES)})",
            name="ck_export_job_inputs_export_format_valid",
        ),
        CheckConstraint(
            f"media_type IN ({_sql_in_list(_EXPORT_MEDIA_TYPES)})",
            name="ck_export_job_inputs_media_type_valid",
        ),
        CheckConstraint(
            "jsonb_typeof(options_json) = 'object'",
            name="ck_export_job_inputs_options_json_object",
        ),
        CheckConstraint(
            "(export_kind = 'revision_json' AND export_format = 'json' "
            "AND media_type = 'application/json') OR "
            "(export_kind = 'dxf' AND export_format = 'dxf' "
            "AND media_type = 'application/dxf') OR "
            "(export_kind = 'revised_dxf' AND export_format = 'dxf' "
            "AND media_type = 'application/dxf') OR "
            "(export_kind = 'quantity_csv' AND export_format = 'csv' "
            "AND media_type = 'text/csv') OR "
            "(export_kind = 'estimate_csv' AND export_format = 'csv' "
            "AND media_type = 'text/csv') OR "
            "(export_kind = 'estimate_pdf' AND export_format = 'pdf' "
            "AND media_type = 'application/pdf')",
            name="ck_export_job_inputs_kind_format_media_type_matrix",
        ),
        CheckConstraint(
            "(export_kind = 'quantity_csv' AND quantity_takeoff_id IS NOT NULL "
            "AND quantity_gate = 'allowed' AND trusted_totals IS TRUE) OR "
            "(export_kind IN ('estimate_csv', 'estimate_pdf') AND quantity_takeoff_id IS NOT NULL "
            "AND quantity_gate = 'allowed' AND trusted_totals IS TRUE) OR "
            "(export_kind IN ('revision_json', 'dxf', 'revised_dxf') "
            "AND quantity_takeoff_id IS NULL "
            "AND quantity_gate IS NULL AND trusted_totals IS NULL)",
            name="ck_export_job_inputs_quantity_lineage",
        ),
        CheckConstraint(
            "(export_kind IN ('estimate_csv', 'estimate_pdf') "
            "AND quantity_takeoff_id IS NOT NULL AND estimate_version_id IS NOT NULL) OR "
            "(export_kind IN ('quantity_csv', 'revision_json', 'dxf', 'revised_dxf') "
            "AND estimate_version_id IS NULL)",
            name="ck_export_job_inputs_estimate_lineage",
        ),
    )

    source_job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "jobs.id",
            name="fk_export_job_inputs_source_job_id_jobs",
            ondelete="RESTRICT",
        ),
        primary_key=True,
        comment="Owning export job identifier.",
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "projects.id",
            name="fk_export_job_inputs_project_id_projects",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Owning project identifier copied from the source export job.",
    )
    source_file_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Owning file identifier copied from the source export job.",
    )
    drawing_revision_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Pinned revision identifier copied from the source export job.",
    )
    source_job_type: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        default=JobType.EXPORT.value,
        comment="Mirrored job type used to pin this row to export jobs only.",
    )
    export_kind: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment=(
            "Export kind selector (revision_json, dxf, revised_dxf, quantity_csv, "
            "estimate_csv, estimate_pdf)."
        ),
    )
    export_format: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Resolved export format required by the export kind.",
    )
    media_type: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="Resolved artifact media type required by the export kind.",
    )
    options_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        comment="Immutable export options JSON object captured at enqueue time.",
    )
    quantity_takeoff_id: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        index=True,
        comment=(
            "Trusted quantity takeoff required for quantity_csv, estimate_csv, and "
            "estimate_pdf exports."
        ),
    )
    quantity_gate: Mapped[str | None] = mapped_column(
        String(32),
        nullable=True,
        comment="Mirrored quantity gate used to enforce trusted quantity lineage.",
    )
    trusted_totals: Mapped[bool | None] = mapped_column(
        Boolean,
        nullable=True,
        comment="Mirrored trusted-totals flag used to enforce trusted quantity lineage.",
    )
    estimate_version_id: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        index=True,
        comment="Finalized estimate version required for estimate_csv and estimate_pdf exports.",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Row creation timestamp.",
    )
