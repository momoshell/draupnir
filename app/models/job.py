"""Job model for background ingestion/export workflows."""

import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    String,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.core.errors import ErrorCode
from app.db.base import Base


class JobType(StrEnum):
    """Supported persisted job types."""

    INGEST = "ingest"
    REPROCESS = "reprocess"


class JobStatus(StrEnum):
    """Supported persisted job lifecycle states."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


_JOB_TYPE_VALUES = tuple(job_type.value for job_type in JobType)
_JOB_STATUS_VALUES = tuple(status.value for status in JobStatus)
_JOB_ERROR_CODE_VALUES = tuple(error_code.value for error_code in ErrorCode)
_PROFILE_REQUIRED_JOB_TYPE_VALUES = (JobType.INGEST.value, JobType.REPROCESS.value)


def _sql_in_list(values: tuple[str, ...]) -> str:
    """Render a SQL string list for check constraints."""

    return ", ".join(f"'{value}'" for value in values)


class Job(Base):
    """SQLAlchemy ORM model for async jobs associated with files/projects."""

    __tablename__ = "jobs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="CASCADE",
            name="fk_jobs_file_id_project_id_files",
        ),
        ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            ondelete="CASCADE",
            name="fk_jobs_extraction_profile_id_project_id_extraction_profiles",
        ),
        CheckConstraint(
            f"job_type IN ({_sql_in_list(_JOB_TYPE_VALUES)})",
            name="ck_jobs_job_type_valid",
        ),
        CheckConstraint(
            f"status IN ({_sql_in_list(_JOB_STATUS_VALUES)})",
            name="ck_jobs_status_valid",
        ),
        CheckConstraint(
            "error_code IS NULL "
            f"OR error_code IN ({_sql_in_list(_JOB_ERROR_CODE_VALUES)})",
            name="ck_jobs_error_code_valid",
        ),
        CheckConstraint(
            "job_type NOT IN "
            f"({_sql_in_list(_PROFILE_REQUIRED_JOB_TYPE_VALUES)}) OR extraction_profile_id IS NOT NULL",
            name="ck_jobs_ingest_extraction_profile_required",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique job identifier (UUID v4)",
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Owning project identifier",
    )
    file_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Associated file identifier",
    )
    extraction_profile_id: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        index=True,
        comment=(
            "Immutable extraction profile identifier. Nullable only during the "
            "expand/rollback window; persisted ingest/reprocess jobs require a "
            "profile and a future contract migration can enforce NOT NULL."
        ),
    )
    job_type: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="Job type (e.g. ingest)",
    )
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Job status (e.g. pending, running, failed, succeeded)",
    )
    attempts: Mapped[int] = mapped_column(
        Integer,
        default=0,
        nullable=False,
        comment="Current attempt count",
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer,
        default=3,
        nullable=False,
        comment="Maximum retry attempts",
    )
    cancel_requested: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        nullable=False,
        comment="Whether cancellation was requested",
    )
    error_code: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        comment="Machine-readable error code",
    )
    error_message: Mapped[str | None] = mapped_column(
        String(2048),
        nullable=True,
        comment="Human-readable error message",
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Job start timestamp",
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Job completion timestamp",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Job creation timestamp",
    )
