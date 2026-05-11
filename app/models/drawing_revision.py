"""Append-only drawing revision aggregates."""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DrawingRevision(Base):
    """SQLAlchemy ORM model for immutable drawing revisions."""

    __tablename__ = "drawing_revisions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="RESTRICT",
            name="fk_drawing_revisions_source_file_id_project_id_files",
        ),
        ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            ondelete="RESTRICT",
            name="fk_drawing_revisions_extraction_profile_proj_profiles",
        ),
        ForeignKeyConstraint(
            ["adapter_run_output_id", "project_id"],
            ["adapter_run_outputs.id", "adapter_run_outputs.project_id"],
            ondelete="RESTRICT",
            name="fk_drawing_revisions_adapter_run_output_id_project_id_outputs",
        ),
        UniqueConstraint(
            "id",
            "project_id",
            name="uq_drawing_revisions_id_project_id",
        ),
        UniqueConstraint(
            "adapter_run_output_id",
            name="uq_drawing_revisions_adapter_run_output_id",
        ),
        UniqueConstraint(
            "source_job_id",
            name="uq_drawing_revisions_source_job_id",
        ),
        UniqueConstraint(
            "project_id",
            "source_file_id",
            "revision_sequence",
            name="uq_drawing_revisions_project_file_revision_sequence",
        ),
        CheckConstraint(
            "revision_sequence >= 1",
            name="ck_drawing_revisions_seq_ge_1",
        ),
        CheckConstraint(
            "revision_kind IN ('ingest', 'reprocess')",
            name="ck_drawing_revisions_kind",
        ),
        CheckConstraint(
            "review_state IN "
            "('approved', 'provisional', 'review_required', 'rejected', 'superseded')",
            name="ck_drawing_revisions_review_state",
        ),
        CheckConstraint(
            "confidence_score >= 0.0 AND confidence_score <= 1.0",
            name="ck_drawing_revisions_conf_0_1",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique drawing revision identifier (UUID v4)",
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "projects.id",
            name="fk_drawing_revisions_project_id_projects",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Owning project identifier",
    )
    source_file_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Immutable source file identifier for this revision",
    )
    extraction_profile_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Immutable extraction profile identifier used to derive this revision",
    )
    source_job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "jobs.id",
            name="fk_drawing_revisions_source_job_id_jobs",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Job identifier that committed this drawing revision",
    )
    adapter_run_output_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Committed adapter output envelope consumed by this drawing revision",
    )
    predecessor_revision_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("drawing_revisions.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
        comment="Immediate predecessor drawing revision identifier for append-only lineage",
    )
    revision_sequence: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Monotonic revision sequence per source file within a project",
    )
    revision_kind: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Drawing revision kind recorded for this append-only revision",
    )
    review_state: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Review state recorded for this drawing revision",
    )
    canonical_entity_schema_version: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        comment="Canonical entity schema version stored on this drawing revision",
    )
    confidence_score: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        comment="Overall drawing revision confidence score for review workflows",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Drawing revision creation timestamp",
    )
