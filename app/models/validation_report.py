"""Canonical validation reports for drawing revisions."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ValidationReport(Base):
    """SQLAlchemy ORM model for immutable validation reports."""

    __tablename__ = "validation_reports"
    __table_args__ = (
        ForeignKeyConstraint(
            ["drawing_revision_id", "project_id"],
            ["drawing_revisions.id", "drawing_revisions.project_id"],
            ondelete="CASCADE",
            name="fk_validation_reports_drawing_revision_id_project_id_revisions",
        ),
        UniqueConstraint(
            "id",
            "project_id",
            name="uq_validation_reports_id_project_id",
        ),
        UniqueConstraint(
            "drawing_revision_id",
            name="uq_validation_reports_drawing_revision_id",
        ),
        UniqueConstraint(
            "source_job_id",
            name="uq_validation_reports_source_job_id",
        ),
        CheckConstraint(
            "validation_status IN "
            "('valid', 'valid_with_warnings', 'invalid', 'needs_review')",
            name="ck_validation_reports_status",
        ),
        CheckConstraint(
            "review_state IN "
            "('approved', 'provisional', 'review_required', 'rejected', 'superseded')",
            name="ck_validation_reports_review_state",
        ),
        CheckConstraint(
            "quantity_gate IN "
            "('allowed', 'allowed_provisional', 'review_gated', 'blocked')",
            name="ck_validation_reports_quantity_gate",
        ),
        CheckConstraint(
            "effective_confidence >= 0.0 AND effective_confidence <= 1.0",
            name="ck_validation_reports_conf_0_1",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique validation report identifier (UUID v4)",
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Owning project identifier",
    )
    drawing_revision_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Drawing revision identifier addressed by this canonical validation report",
    )
    source_job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Job identifier that produced this validation report",
    )
    validation_report_schema_version: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        comment="Validation report schema version",
    )
    canonical_entity_schema_version: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        comment="Canonical entity schema version validated by this report",
    )
    validation_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Technical validation status for the drawing revision",
    )
    review_state: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Inherited review state recorded on the validation report",
    )
    quantity_gate: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Derived quantity gate outcome for the drawing revision",
    )
    effective_confidence: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        comment="Conservative effective confidence used for quantity gate decisions",
    )
    validator_name: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="Validator implementation name",
    )
    validator_version: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="Validator implementation version",
    )
    report_json: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
        comment=(
            "Validation report payload containing summary, checks, findings, "
            "and adapter warnings"
        ),
    )
    generated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="Validation report generation timestamp",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Validation report record creation timestamp",
    )
