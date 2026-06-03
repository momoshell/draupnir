"""Immutable changeset-apply job input model."""

import uuid
from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base

from .job import JobType

_CHANGESET_APPLY_VALIDATION_STATUS_VALUES = (
    "valid",
    "valid_with_warnings",
)


def _sql_in_list(values: tuple[str, ...]) -> str:
    """Render a SQL string list for check constraints."""

    return ", ".join(f"'{value}'" for value in values)


class ChangeSetApplyJobInput(Base):
    """Immutable persisted input captured when a changeset-apply job is created."""

    __tablename__ = "changeset_apply_job_inputs"
    __table_args__ = (
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
            name="fk_changeset_apply_job_inputs_job_lineage_jobs",
        ),
        ForeignKeyConstraint(
            ["drawing_revision_id", "project_id", "source_file_id"],
            [
                "drawing_revisions.id",
                "drawing_revisions.project_id",
                "drawing_revisions.source_file_id",
            ],
            ondelete="RESTRICT",
            name="fk_changeset_apply_job_inputs_revision_lineage",
        ),
        ForeignKeyConstraint(
            ["project_id", "change_set_id", "drawing_revision_id"],
            [
                "cad_change_sets.project_id",
                "cad_change_sets.id",
                "cad_change_sets.base_revision_id",
            ],
            ondelete="RESTRICT",
            name="fk_changeset_apply_job_inputs_base_changeset",
        ),
        ForeignKeyConstraint(
            [
                "project_id",
                "change_set_id",
                "latest_validation_result_id",
                "latest_validation_status",
            ],
            [
                "cad_change_set_validation_results.project_id",
                "cad_change_set_validation_results.change_set_id",
                "cad_change_set_validation_results.id",
                "cad_change_set_validation_results.validation_status",
            ],
            ondelete="RESTRICT",
            name="fk_changeset_apply_job_inputs_validation_result",
        ),
        CheckConstraint(
            f"source_job_type = '{JobType.CHANGESET_APPLY.value}'",
            name="ck_changeset_apply_job_inputs_source_job_type",
        ),
        CheckConstraint(
            "latest_validation_status IN "
            f"({_sql_in_list(_CHANGESET_APPLY_VALIDATION_STATUS_VALUES)})",
            name="ck_changeset_apply_job_inputs_validation_status",
        ),
        UniqueConstraint(
            "project_id",
            "change_set_id",
            name="uq_changeset_apply_job_inputs_project_id_change_set_id",
        ),
    )

    source_job_id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        comment="Owning changeset-apply job identifier",
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "projects.id",
            name="fk_changeset_apply_job_inputs_project_id_projects",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Owning project identifier",
    )
    source_file_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Pinned source file identifier from the apply job lineage",
    )
    drawing_revision_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Pinned base drawing revision identifier for the apply job",
    )
    change_set_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Pinned changeset identifier to apply to the base revision",
    )
    source_job_type: Mapped[str] = mapped_column(
        String(64),
        default=JobType.CHANGESET_APPLY.value,
        nullable=False,
        comment="Shadow job type constrained to changeset_apply for composite job FK proof",
    )
    latest_validation_result_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Latest persisted validation result identifier pinned at job creation",
    )
    latest_validation_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Latest persisted validation status snapshot pinned at job creation",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Job input creation timestamp",
    )
