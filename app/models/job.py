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
_ENQUEUE_STATUS_VALUES = ("pending", "publishing", "published")
_PROFILE_REQUIRED_JOB_TYPE_VALUES = (JobType.INGEST.value, JobType.REPROCESS.value)
_BASE_REQUIRED_JOB_TYPE_VALUES = (JobType.REPROCESS.value,)


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
            ondelete="RESTRICT",
            name="fk_jobs_file_id_project_id_files",
        ),
        ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            ondelete="RESTRICT",
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
            f"enqueue_status IN ({_sql_in_list(_ENQUEUE_STATUS_VALUES)})",
            name="ck_jobs_enqueue_status_valid",
        ),
        CheckConstraint(
            "job_type NOT IN "
            f"({_sql_in_list(_PROFILE_REQUIRED_JOB_TYPE_VALUES)}) "
            "OR extraction_profile_id IS NOT NULL",
            name="ck_jobs_ingest_extraction_profile_required",
        ),
        CheckConstraint(
            "job_type NOT IN "
            f"({_sql_in_list(_BASE_REQUIRED_JOB_TYPE_VALUES)}) "
            "OR base_revision_id IS NOT NULL",
            name="ck_jobs_reprocess_base_revision_required",
        ),
        CheckConstraint(
            f"job_type != '{JobType.INGEST.value}' OR base_revision_id IS NULL",
            name="ck_jobs_ingest_base_revision_forbidden",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique job identifier (UUID v4)",
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "projects.id",
            name="fk_jobs_project_id_projects",
            ondelete="RESTRICT",
        ),
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
    base_revision_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(
            "drawing_revisions.id",
            name="fk_jobs_base_revision_id_drawing_revisions",
            ondelete="RESTRICT",
        ),
        nullable=True,
        index=True,
        comment=(
            "Pinned latest finalized drawing revision captured when a reprocess "
            "job was created. Null for initial ingest jobs."
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
    attempt_token: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        comment="Current running attempt ownership token fence",
    )
    attempt_lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Current running attempt lease expiry used to reclaim stale deliveries",
    )
    enqueue_status: Mapped[str] = mapped_column(
        String(32),
        default="pending",
        nullable=False,
        comment="Durable enqueue intent state (pending, publishing, published)",
    )
    enqueue_attempts: Mapped[int] = mapped_column(
        Integer,
        default=0,
        nullable=False,
        comment="Broker publish attempts for the current durable enqueue intent",
    )
    enqueue_owner_token: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        comment="Current enqueue publisher ownership token for stranded-intent recovery",
    )
    enqueue_lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Current enqueue publisher lease expiry used to reclaim stranded intents",
    )
    enqueue_last_attempted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Most recent broker publish attempt timestamp for the current intent",
    )
    enqueue_published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp when the current durable enqueue intent was last published",
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
