"""Revision-scoped centerline routed-length aggregate table."""

from __future__ import annotations

import uuid
from typing import Any, ClassVar

from sqlalchemy import (
    JSON,
    Float,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.mixins import ProjectScopedMixin, RevisionLineageMixin, TimestampMixin, sha256_column


class RevisionRoutedLength(RevisionLineageMixin, ProjectScopedMixin, TimestampMixin, Base):
    """Aggregate routed-length row for a (revision, layer_ref, colour_key) group."""

    __source_file_comment__: ClassVar[str] = (
        "Immutable source file identifier for this routed-length row"
    )
    __extraction_profile_comment__: ClassVar[str] = (
        "Immutable extraction profile identifier used for this routed-length row"
    )
    __source_job_comment__: ClassVar[str] = "Job identifier that produced this routed-length row"
    __drawing_revision_comment__: ClassVar[str] = (
        "Drawing revision identifier that owns this routed-length row"
    )
    __adapter_run_output_comment__: ClassVar[str] = (
        "Adapter run output identifier that produced this routed-length row"
    )
    __canonical_entity_schema_version_comment__: ClassVar[str] = (
        "Canonical entity schema version for this routed-length row"
    )
    __created_at_comment__: ClassVar[str] = "Routed-length row creation timestamp"

    __tablename__ = "revision_routed_lengths"
    __table_args__ = (
        ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_routed_lengths_source_file_project_files",
        ),
        ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_routed_lengths_profile_project_profiles",
        ),
        ForeignKeyConstraint(
            ["adapter_run_output_id", "project_id"],
            ["adapter_run_outputs.id", "adapter_run_outputs.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_routed_lengths_output_project_outputs",
        ),
        ForeignKeyConstraint(
            ["drawing_revision_id", "project_id"],
            ["drawing_revisions.id", "drawing_revisions.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_routed_lengths_revision_project_revisions",
        ),
        UniqueConstraint(
            "id",
            "project_id",
            name="uq_revision_routed_lengths_id_project_id",
        ),
        UniqueConstraint(
            "drawing_revision_id",
            "layer_ref",
            "colour_key",
            "algo_version",
            "raster_params_hash",
            name="uq_revision_routed_lengths_group_version",
        ),
        Index(
            "ix_revision_routed_lengths_revision_group",
            "drawing_revision_id",
            "layer_ref",
            "colour_key",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique routed-length row identifier (UUID v4)",
    )
    layer_ref: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        comment="Layer group key; NULL means all layers",
    )
    colour_key: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        comment="Colour group key; NULL means all colours",
    )
    algo_version: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Centerline algorithm version string",
    )
    raster_params_hash: Mapped[str] = sha256_column(
        nullable=False,
        index=True,
        comment="SHA-256 hash of the raster parameters used to produce this row",
    )
    producer_kind: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Producer kind identifier (e.g. raster_centerline)",
    )
    skeleton_length_du: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        comment="Total skeleton centerline length in drawing units",
    )
    entity_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Number of source entities contributing to this routed-length group",
    )
    geometry_json: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Reserved geometry payload (NULL for LP1; populated in LP2+)",
    )
