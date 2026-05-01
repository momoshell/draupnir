"""Job model for background ingestion/export workflows."""

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    String,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


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
