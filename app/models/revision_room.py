"""Revision-scoped room persistence table."""

from __future__ import annotations

import uuid
from typing import Any, ClassVar

from sqlalchemy import (
    JSON,
    Boolean,
    Float,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.mixins import ProjectScopedMixin, RevisionLineageMixin, TimestampMixin


class RevisionRoom(RevisionLineageMixin, ProjectScopedMixin, TimestampMixin, Base):
    """Persisted room row for a (revision, room_key, algo_version) identity."""

    __source_file_comment__: ClassVar[str] = "Immutable source file identifier for this room row"
    __extraction_profile_comment__: ClassVar[str] = (
        "Immutable extraction profile identifier used for this room row"
    )
    __source_job_comment__: ClassVar[str] = "Job identifier that produced this room row"
    __drawing_revision_comment__: ClassVar[str] = (
        "Drawing revision identifier that owns this room row"
    )
    __adapter_run_output_comment__: ClassVar[str] = (
        "Adapter run output identifier that produced this room row"
    )
    __canonical_entity_schema_version_comment__: ClassVar[str] = (
        "Canonical entity schema version for this room row"
    )
    __created_at_comment__: ClassVar[str] = "Room row creation timestamp"

    __tablename__ = "revision_rooms"
    __table_args__ = (
        ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_rooms_source_file_project_files",
        ),
        ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_rooms_profile_project_profiles",
        ),
        ForeignKeyConstraint(
            ["adapter_run_output_id", "project_id"],
            ["adapter_run_outputs.id", "adapter_run_outputs.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_rooms_output_project_outputs",
        ),
        ForeignKeyConstraint(
            ["drawing_revision_id", "project_id"],
            ["drawing_revisions.id", "drawing_revisions.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_rooms_revision_project_revisions",
        ),
        UniqueConstraint(
            "id",
            "project_id",
            name="uq_revision_rooms_id_project_id",
        ),
        UniqueConstraint(
            "drawing_revision_id",
            "room_key",
            "algo_version",
            name="uq_revision_rooms_group_version",
        ),
        Index(
            "ix_revision_rooms_revision_room",
            "drawing_revision_id",
            "room_key",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique room row identifier (UUID v4)",
    )
    algo_version: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Room extraction algorithm version string",
    )
    room_key: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Stable per-room identifier from the interpretation Room.id",
    )
    name: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        comment="Room name, if present in the drawing",
    )
    number: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="Room number, if present in the drawing",
    )
    source: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Room derivation source (e.g. polygon, label)",
    )
    area: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        comment="Room area in drawing units squared; NULL for label-only rooms",
    )
    bounds_min_x: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        comment="Room bounding-box minimum x; NULL for label-only rooms",
    )
    bounds_min_y: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        comment="Room bounding-box minimum y; NULL for label-only rooms",
    )
    bounds_max_x: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        comment="Room bounding-box maximum x; NULL for label-only rooms",
    )
    bounds_max_y: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        comment="Room bounding-box maximum y; NULL for label-only rooms",
    )
    polygon_geometry_json: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Room polygon geometry payload; NULL for label-only rooms",
    )
    anchors_json: Mapped[Any] = mapped_column(
        JSON,
        nullable=False,
        comment="All label anchor points for this (possibly merged) room",
    )
    needs_review: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        comment="True when a duplicate room number was found across distinct polygon rooms",
    )
    confidence: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        comment="Optional provenance confidence in [0, 1]; NULL means not scored",
    )
    strategy: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="Room-derivation strategy identifier",
    )
    source_layers_json: Mapped[Any] = mapped_column(
        JSON,
        nullable=False,
        comment="Source layer references contributing to this room",
    )
    input_family: Mapped[str | None] = mapped_column(
        String(32),
        nullable=True,
        comment="Input family of the source drawing (e.g. dwg, pdf_vector)",
    )
    assigned_device_ids_json: Mapped[Any] = mapped_column(
        JSON,
        nullable=False,
        default=list,
        comment=(
            "device_ids assigned to this genuine room (persisted from live "
            "result.device_assignments; expanded on read)."
        ),
    )
    ordinal: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment=(
            "Position of this room in the genuine `interpret_rooms` order; read path "
            "orders by this to reproduce live item order."
        ),
    )
