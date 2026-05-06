"""Immutable persisted adapter run output envelopes."""

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


class AdapterRunOutput(Base):
    """SQLAlchemy ORM model for committed adapter output envelopes."""

    __tablename__ = "adapter_run_outputs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="CASCADE",
            name="fk_adapter_run_outputs_source_file_id_project_id_files",
        ),
        ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            ondelete="CASCADE",
            name="fk_adapter_run_outputs_extraction_profile_proj_profiles",
        ),
        UniqueConstraint(
            "id",
            "project_id",
            name="uq_adapter_run_outputs_id_project_id",
        ),
        UniqueConstraint(
            "source_job_id",
            name="uq_adapter_run_outputs_source_job_id",
        ),
        CheckConstraint(
            "confidence_score >= 0.0 AND confidence_score <= 1.0",
            name="ck_adapter_outputs_confidence_0_1",
        ),
        CheckConstraint(
            "length(result_checksum_sha256) = 64 "
            "AND result_checksum_sha256 = lower(result_checksum_sha256)",
            name="ck_adapter_outputs_checksum",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique adapter run output identifier (UUID v4)",
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Owning project identifier",
    )
    source_file_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Immutable source file identifier used for this adapter run",
    )
    extraction_profile_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Immutable extraction profile identifier used for this adapter run",
    )
    source_job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
        comment="Job identifier that produced this committed adapter output",
    )
    adapter_key: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="Stable adapter registry key used for this adapter run",
    )
    adapter_version: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="Adapter version recorded in adapter diagnostics",
    )
    input_family: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Normalized input family processed by this adapter run",
    )
    canonical_entity_schema_version: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        comment="Canonical entity schema version for the adapter output payload",
    )
    canonical_json: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
        comment="Canonical adapter output payload including layouts, layers, blocks, and entities",
    )
    provenance_json: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
        comment="Structured adapter provenance payload",
    )
    confidence_json: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
        comment="Structured confidence payload for adapter output review workflows",
    )
    confidence_score: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        comment="Overall adapter confidence score for this committed output",
    )
    warnings_json: Mapped[list[Any]] = mapped_column(
        JSON,
        nullable=False,
        comment="Adapter-emitted warnings carried with the committed output envelope",
    )
    diagnostics_json: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
        comment="Adapter diagnostics payload including timing metadata",
    )
    result_checksum_sha256: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="SHA-256 checksum of the committed adapter result envelope",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Adapter output record creation timestamp",
    )
