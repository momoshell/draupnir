"""Revision-scoped room materialization summary (version-scoped marker)."""

from __future__ import annotations

import uuid
from typing import ClassVar

from sqlalchemy import ForeignKeyConstraint, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.mixins import ProjectScopedMixin, RevisionLineageMixin, TimestampMixin


class RevisionRoomSummary(RevisionLineageMixin, ProjectScopedMixin, TimestampMixin, Base):
    """Version-scoped materialization marker for a revision's room registry.

    Its existence for a (drawing_revision_id, algo_version) pair IS the
    materialized signal the read path checks: a row here means the genuine
    room set for that version has been persisted as :class:`~app.models.
    revision_room.RevisionRoom` rows and can be loaded directly rather than
    recomputed.
    """

    __source_file_comment__: ClassVar[str] = (
        "Immutable source file identifier for this room summary row"
    )
    __extraction_profile_comment__: ClassVar[str] = (
        "Immutable extraction profile identifier used for this room summary row"
    )
    __source_job_comment__: ClassVar[str] = "Job identifier that produced this room summary row"
    __drawing_revision_comment__: ClassVar[str] = (
        "Drawing revision identifier that owns this room summary row"
    )
    __adapter_run_output_comment__: ClassVar[str] = (
        "Adapter run output identifier that produced this room summary row"
    )
    __canonical_entity_schema_version_comment__: ClassVar[str] = (
        "Canonical entity schema version for this room summary row"
    )
    __created_at_comment__: ClassVar[str] = "Room summary row creation timestamp"

    __tablename__ = "revision_room_summary"
    __table_args__ = (
        ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_room_summary_source_file_project_files",
        ),
        ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_room_summary_profile_project_profiles",
        ),
        ForeignKeyConstraint(
            ["adapter_run_output_id", "project_id"],
            ["adapter_run_outputs.id", "adapter_run_outputs.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_room_summary_output_project_outputs",
        ),
        ForeignKeyConstraint(
            ["drawing_revision_id", "project_id"],
            ["drawing_revisions.id", "drawing_revisions.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_room_summary_revision_project_revisions",
        ),
        UniqueConstraint(
            "id",
            "project_id",
            name="uq_revision_room_summary_id_project_id",
        ),
        UniqueConstraint(
            "drawing_revision_id",
            "algo_version",
            name="uq_revision_room_summary_version",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique room summary row identifier (UUID v4)",
    )
    algo_version: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Room extraction algorithm version string",
    )
    full_registry_size: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Total size of the interpretation room registry before genuine filtering",
    )
