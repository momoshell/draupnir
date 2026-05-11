"""Immutable generated artifact lineage records."""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    JSON,
    BigInteger,
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


class GeneratedArtifact(Base):
    """SQLAlchemy ORM model for append-only generated artifacts."""

    __tablename__ = "generated_artifacts"
    __table_args__ = (
        ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="RESTRICT",
            name="fk_generated_artifacts_source_file_id_project_id_files",
        ),
        ForeignKeyConstraint(
            ["drawing_revision_id", "project_id"],
            ["drawing_revisions.id", "drawing_revisions.project_id"],
            ondelete="RESTRICT",
            name="fk_generated_artifacts_drawing_revision_id_project_id_revisions",
        ),
        ForeignKeyConstraint(
            ["adapter_run_output_id", "project_id"],
            ["adapter_run_outputs.id", "adapter_run_outputs.project_id"],
            ondelete="RESTRICT",
            name="fk_generated_artifacts_adapter_run_output_id_project_id_outputs",
        ),
        ForeignKeyConstraint(
            ["predecessor_artifact_id"],
            ["generated_artifacts.id"],
            ondelete="RESTRICT",
            name="fk_generated_artifacts_predecessor_artifact_id_self",
        ),
        UniqueConstraint(
            "id",
            "project_id",
            name="uq_generated_artifacts_id_project_id",
        ),
        UniqueConstraint(
            "storage_key",
            name="uq_generated_artifacts_storage_key",
        ),
        CheckConstraint(
            "size_bytes >= 0",
            name="ck_generated_artifacts_size_ge_0",
        ),
        CheckConstraint(
            "length(checksum_sha256) = 64 AND checksum_sha256 = lower(checksum_sha256)",
            name="ck_generated_artifacts_checksum",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique generated artifact identifier (UUID v4)",
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "projects.id",
            name="fk_generated_artifacts_project_id_projects",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Owning project identifier",
    )
    source_file_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Source file identifier for artifact lineage",
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "jobs.id",
            name="fk_generated_artifacts_job_id_jobs",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Job identifier that produced this generated artifact",
    )
    drawing_revision_id: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        index=True,
        comment="Drawing revision identifier used to generate this artifact",
    )
    adapter_run_output_id: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        index=True,
        comment="Adapter run output identifier used to generate this artifact",
    )
    artifact_kind: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="Generated artifact kind",
    )
    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Human-readable generated artifact name",
    )
    format: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="Generated artifact format identifier",
    )
    media_type: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Generated artifact media type",
    )
    size_bytes: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        comment="Generated artifact byte size",
    )
    checksum_sha256: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="SHA-256 checksum for generated artifact bytes",
    )
    generator_name: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="Artifact generator implementation name",
    )
    generator_version: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="Artifact generator implementation version",
    )
    generator_config_json: Mapped[dict[str, object]] = mapped_column(
        JSON,
        nullable=False,
        comment="Artifact generator configuration payload",
    )
    storage_key: Mapped[str] = mapped_column(
        String(1024),
        nullable=False,
        comment="Immutable generated artifact storage key",
    )
    storage_uri: Mapped[str] = mapped_column(
        String(1024),
        nullable=False,
        comment="Immutable generated artifact storage URI",
    )
    lineage_json: Mapped[dict[str, object]] = mapped_column(
        JSON,
        nullable=False,
        comment="Structured generated artifact lineage payload",
    )
    predecessor_artifact_id: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        index=True,
        comment="Immediate predecessor artifact identifier for append-only lineage",
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Soft deletion timestamp for retention workflows",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Generated artifact creation timestamp",
    )
