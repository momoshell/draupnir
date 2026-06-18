"""Revision-scoped normalized entity materialization tables."""

from __future__ import annotations

import uuid
from typing import Any, ClassVar

from sqlalchemy import (
    JSON,
    CheckConstraint,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.mixins import ProjectScopedMixin, RevisionLineageMixin, TimestampMixin, sha256_column


class RevisionEntityManifest(RevisionLineageMixin, ProjectScopedMixin, TimestampMixin, Base):
    """Manifest row recording whether revision entities were materialized."""

    __source_file_comment__: ClassVar[str] = (
        "Immutable source file identifier for the materialized revision"
    )
    __extraction_profile_comment__: ClassVar[str] = (
        "Immutable extraction profile identifier used for materialization"
    )
    __source_job_comment__: ClassVar[str] = (
        "Job identifier that materialized normalized revision entities"
    )
    __drawing_revision_comment__: ClassVar[str] = (
        "Drawing revision identifier whose normalized entities were materialized"
    )
    __adapter_run_output_comment__: ClassVar[str] = (
        "Adapter run output identifier materialized into revision-scoped entity tables"
    )
    __canonical_entity_schema_version_comment__: ClassVar[str] = (
        "Canonical entity schema version for the materialized revision payloads"
    )
    __created_at_comment__: ClassVar[str] = "Manifest creation timestamp"

    __tablename__ = "revision_entity_manifests"
    __table_args__ = (
        ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_entity_manifests_source_file_project_files",
        ),
        ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_entity_manifests_profile_project_profiles",
        ),
        ForeignKeyConstraint(
            ["adapter_run_output_id", "project_id"],
            ["adapter_run_outputs.id", "adapter_run_outputs.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_entity_manifests_output_project_outputs",
        ),
        ForeignKeyConstraint(
            ["drawing_revision_id", "project_id"],
            ["drawing_revisions.id", "drawing_revisions.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_entity_manifests_revision_project_revisions",
        ),
        UniqueConstraint(
            "id",
            "project_id",
            name="uq_revision_entity_manifests_id_project_id",
        ),
        UniqueConstraint(
            "adapter_run_output_id",
            name="uq_revision_entity_manifests_adapter_run_output_id",
        ),
        UniqueConstraint(
            "drawing_revision_id",
            name="uq_revision_entity_manifests_drawing_revision_id",
        ),
        UniqueConstraint(
            "source_job_id",
            name="uq_revision_entity_manifests_source_job_id",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique revision entity materialization manifest identifier (UUID v4)",
    )
    counts_json: Mapped[dict[str, int]] = mapped_column(
        JSON,
        nullable=False,
        comment="Materialized normalized row counts keyed by layouts, layers, blocks, and entities",
    )


class RevisionLayout(RevisionLineageMixin, ProjectScopedMixin, TimestampMixin, Base):
    """Materialized layout payload rows scoped to a drawing revision."""

    __source_file_comment__: ClassVar[str] = (
        "Immutable source file identifier for this materialized layout row"
    )
    __extraction_profile_comment__: ClassVar[str] = (
        "Immutable extraction profile identifier used for this layout row"
    )
    __source_job_comment__: ClassVar[str] = "Job identifier that materialized this layout row"
    __drawing_revision_comment__: ClassVar[str] = (
        "Drawing revision identifier that owns this layout row"
    )
    __adapter_run_output_comment__: ClassVar[str] = (
        "Adapter run output identifier that produced this layout row"
    )
    __canonical_entity_schema_version_comment__: ClassVar[str] = (
        "Canonical entity schema version for this layout row payload"
    )
    __created_at_comment__: ClassVar[str] = "Revision layout materialization timestamp"

    __tablename__ = "revision_layouts"
    __table_args__ = (
        ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_layouts_source_file_project_files",
        ),
        ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_layouts_profile_project_profiles",
        ),
        ForeignKeyConstraint(
            ["adapter_run_output_id", "project_id"],
            ["adapter_run_outputs.id", "adapter_run_outputs.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_layouts_output_project_outputs",
        ),
        ForeignKeyConstraint(
            ["drawing_revision_id", "project_id"],
            ["drawing_revisions.id", "drawing_revisions.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_layouts_revision_project_revisions",
        ),
        UniqueConstraint("id", "project_id", name="uq_revision_layouts_id_project_id"),
        UniqueConstraint(
            "drawing_revision_id",
            "sequence_index",
            name="uq_revision_layouts_revision_sequence_index",
        ),
        UniqueConstraint(
            "drawing_revision_id",
            "layout_ref",
            name="uq_revision_layouts_revision_layout_ref",
        ),
        CheckConstraint("sequence_index >= 0", name="ck_revision_layouts_sequence_index_ge_0"),
        Index(
            "ix_revision_layouts_revision_layout_ref",
            "drawing_revision_id",
            "layout_ref",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique materialized revision layout row identifier (UUID v4)",
    )
    sequence_index: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Zero-based layout position from the canonical payload",
    )
    payload_json: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
        comment="Materialized canonical layout payload",
    )
    layout_ref: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
        comment="Stable non-null layout reference extracted from the canonical layout payload",
    )


class RevisionLayer(RevisionLineageMixin, ProjectScopedMixin, TimestampMixin, Base):
    """Materialized layer payload rows scoped to a drawing revision."""

    __source_file_comment__: ClassVar[str] = (
        "Immutable source file identifier for this materialized layer row"
    )
    __extraction_profile_comment__: ClassVar[str] = (
        "Immutable extraction profile identifier used for this layer row"
    )
    __source_job_comment__: ClassVar[str] = "Job identifier that materialized this layer row"
    __drawing_revision_comment__: ClassVar[str] = (
        "Drawing revision identifier that owns this layer row"
    )
    __adapter_run_output_comment__: ClassVar[str] = (
        "Adapter run output identifier that produced this layer row"
    )
    __canonical_entity_schema_version_comment__: ClassVar[str] = (
        "Canonical entity schema version for this layer row payload"
    )
    __created_at_comment__: ClassVar[str] = "Revision layer materialization timestamp"

    __tablename__ = "revision_layers"
    __table_args__ = (
        ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_layers_source_file_project_files",
        ),
        ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_layers_profile_project_profiles",
        ),
        ForeignKeyConstraint(
            ["adapter_run_output_id", "project_id"],
            ["adapter_run_outputs.id", "adapter_run_outputs.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_layers_output_project_outputs",
        ),
        ForeignKeyConstraint(
            ["drawing_revision_id", "project_id"],
            ["drawing_revisions.id", "drawing_revisions.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_layers_revision_project_revisions",
        ),
        UniqueConstraint("id", "project_id", name="uq_revision_layers_id_project_id"),
        UniqueConstraint(
            "drawing_revision_id",
            "sequence_index",
            name="uq_revision_layers_revision_sequence_index",
        ),
        UniqueConstraint(
            "drawing_revision_id",
            "layer_ref",
            name="uq_revision_layers_revision_layer_ref",
        ),
        CheckConstraint("sequence_index >= 0", name="ck_revision_layers_sequence_index_ge_0"),
        Index(
            "ix_revision_layers_revision_layer_ref",
            "drawing_revision_id",
            "layer_ref",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique materialized revision layer row identifier (UUID v4)",
    )
    sequence_index: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Zero-based layer position from the canonical payload",
    )
    payload_json: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
        comment="Materialized canonical layer payload",
    )
    layer_ref: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
        comment="Stable non-null layer reference extracted from the canonical layer payload",
    )


class RevisionBlock(RevisionLineageMixin, ProjectScopedMixin, TimestampMixin, Base):
    """Materialized block payload rows scoped to a drawing revision."""

    __source_file_comment__: ClassVar[str] = (
        "Immutable source file identifier for this materialized block row"
    )
    __extraction_profile_comment__: ClassVar[str] = (
        "Immutable extraction profile identifier used for this block row"
    )
    __source_job_comment__: ClassVar[str] = "Job identifier that materialized this block row"
    __drawing_revision_comment__: ClassVar[str] = (
        "Drawing revision identifier that owns this block row"
    )
    __adapter_run_output_comment__: ClassVar[str] = (
        "Adapter run output identifier that produced this block row"
    )
    __canonical_entity_schema_version_comment__: ClassVar[str] = (
        "Canonical entity schema version for this block row payload"
    )
    __created_at_comment__: ClassVar[str] = "Revision block materialization timestamp"

    __tablename__ = "revision_blocks"
    __table_args__ = (
        ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_blocks_source_file_project_files",
        ),
        ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_blocks_profile_project_profiles",
        ),
        ForeignKeyConstraint(
            ["adapter_run_output_id", "project_id"],
            ["adapter_run_outputs.id", "adapter_run_outputs.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_blocks_output_project_outputs",
        ),
        ForeignKeyConstraint(
            ["drawing_revision_id", "project_id"],
            ["drawing_revisions.id", "drawing_revisions.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_blocks_revision_project_revisions",
        ),
        UniqueConstraint("id", "project_id", name="uq_revision_blocks_id_project_id"),
        UniqueConstraint(
            "drawing_revision_id",
            "sequence_index",
            name="uq_revision_blocks_revision_sequence_index",
        ),
        UniqueConstraint(
            "drawing_revision_id",
            "block_ref",
            name="uq_revision_blocks_revision_block_ref",
        ),
        CheckConstraint("sequence_index >= 0", name="ck_revision_blocks_sequence_index_ge_0"),
        Index(
            "ix_revision_blocks_revision_block_ref",
            "drawing_revision_id",
            "block_ref",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique materialized revision block row identifier (UUID v4)",
    )
    sequence_index: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Zero-based block position from the canonical payload",
    )
    payload_json: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
        comment="Materialized canonical block payload",
    )
    block_ref: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
        comment="Stable non-null block reference extracted from the canonical block payload",
    )


class RevisionEntity(RevisionLineageMixin, ProjectScopedMixin, TimestampMixin, Base):
    """Materialized entity payload rows scoped to a drawing revision."""

    __source_file_comment__: ClassVar[str] = (
        "Immutable source file identifier for this materialized entity row"
    )
    __extraction_profile_comment__: ClassVar[str] = (
        "Immutable extraction profile identifier used for this entity row"
    )
    __source_job_comment__: ClassVar[str] = "Job identifier that materialized this entity row"
    __drawing_revision_comment__: ClassVar[str] = (
        "Drawing revision identifier that owns this entity row"
    )
    __adapter_run_output_comment__: ClassVar[str] = (
        "Adapter run output identifier that produced this entity row"
    )
    __canonical_entity_schema_version_comment__: ClassVar[str] = (
        "Canonical entity schema version for this entity row payload"
    )
    __created_at_comment__: ClassVar[str] = "Revision entity materialization timestamp"

    __tablename__ = "revision_entities"
    __table_args__ = (
        ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_entities_source_file_project_files",
        ),
        ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_entities_profile_project_profiles",
        ),
        ForeignKeyConstraint(
            ["adapter_run_output_id", "project_id"],
            ["adapter_run_outputs.id", "adapter_run_outputs.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_entities_output_project_outputs",
        ),
        ForeignKeyConstraint(
            ["drawing_revision_id", "project_id"],
            ["drawing_revisions.id", "drawing_revisions.project_id"],
            ondelete="RESTRICT",
            name="fk_revision_entities_revision_project_revisions",
        ),
        ForeignKeyConstraint(
            ["layout_id"],
            ["revision_layouts.id"],
            ondelete="RESTRICT",
            name="fk_revision_entities_layout_id_revision_layouts",
        ),
        ForeignKeyConstraint(
            ["layer_id"],
            ["revision_layers.id"],
            ondelete="RESTRICT",
            name="fk_revision_entities_layer_id_revision_layers",
        ),
        ForeignKeyConstraint(
            ["block_id"],
            ["revision_blocks.id"],
            ondelete="RESTRICT",
            name="fk_revision_entities_block_id_revision_blocks",
        ),
        ForeignKeyConstraint(
            ["parent_entity_row_id"],
            ["revision_entities.id"],
            ondelete="RESTRICT",
            name="fk_revision_entities_parent_entity_row_id_revision_entities",
        ),
        UniqueConstraint("id", "project_id", name="uq_revision_entities_id_project_id"),
        UniqueConstraint(
            "drawing_revision_id",
            "entity_id",
            name="uq_revision_entities_revision_entity_id",
        ),
        UniqueConstraint(
            "drawing_revision_id",
            "sequence_index",
            name="uq_revision_entities_revision_sequence_index",
        ),
        CheckConstraint("sequence_index >= 0", name="ck_revision_entities_sequence_index_ge_0"),
        CheckConstraint(
            "source_hash IS NULL OR "
            "(length(source_hash) = 64 AND source_hash = lower(source_hash))",
            name="ck_revision_entities_source_hash",
        ),
        Index(
            "ix_revision_entities_revision_entity_type_sequence_id",
            "drawing_revision_id",
            "entity_type",
            "sequence_index",
            "id",
        ),
        Index(
            "ix_revision_entities_revision_layout_ref",
            "drawing_revision_id",
            "layout_ref",
        ),
        Index(
            "ix_revision_entities_revision_layer_ref",
            "drawing_revision_id",
            "layer_ref",
        ),
        Index(
            "ix_revision_entities_revision_block_ref",
            "drawing_revision_id",
            "block_ref",
        ),
        Index(
            "ix_revision_entities_revision_parent_entity_ref",
            "drawing_revision_id",
            "parent_entity_ref",
        ),
        Index(
            "ix_revision_entities_revision_source_identity",
            "drawing_revision_id",
            "source_identity",
        ),
        Index(
            "ix_revision_entities_revision_source_hash",
            "drawing_revision_id",
            "source_hash",
        ),
        # Spatial prefilter for bbox-intersection / near-point queries within a revision.
        Index(
            "ix_revision_entities_revision_bbox",
            "drawing_revision_id",
            "bbox_min_x",
            "bbox_min_y",
            "bbox_max_x",
            "bbox_max_y",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique materialized revision entity row identifier (UUID v4)",
    )
    sequence_index: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Zero-based entity position from the canonical payload",
    )
    entity_id: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
        comment=(
            "Stable non-null entity identifier extracted or derived from the "
            "canonical entity payload"
        ),
    )
    entity_type: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        index=True,
        comment="Canonical entity type extracted from the canonical entity payload",
    )
    entity_schema_version: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        comment="Canonical entity schema version for this entity row",
    )
    parent_entity_ref: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        index=True,
        comment="Optional raw parent entity reference extracted from the canonical entity payload",
    )
    confidence_json: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
        comment="Entity confidence payload extracted from the canonical entity payload",
    )
    geometry_json: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
        comment="Entity geometry payload extracted from the canonical entity payload",
    )
    properties_json: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
        comment="Entity properties payload extracted from the canonical entity payload",
    )
    provenance_json: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
        comment="Entity provenance payload extracted from the canonical entity payload",
    )
    canonical_entity_json: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Optional full canonical entity payload retained alongside split contract fields",
    )
    # Axis-aligned bounding box (drawing coordinate units) for indexed spatial queries.
    # NULL when the geometry has no recoverable 2-D extent.
    bbox_min_x: Mapped[float | None] = mapped_column(nullable=True, comment="AABB min x")
    bbox_min_y: Mapped[float | None] = mapped_column(nullable=True, comment="AABB min y")
    bbox_max_x: Mapped[float | None] = mapped_column(nullable=True, comment="AABB max x")
    bbox_max_y: Mapped[float | None] = mapped_column(nullable=True, comment="AABB max y")
    layout_ref: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        index=True,
        comment="Optional layout reference extracted from the canonical entity payload",
    )
    layer_ref: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        index=True,
        comment="Optional layer reference extracted from the canonical entity payload",
    )
    block_ref: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        index=True,
        comment="Optional block reference extracted from the canonical entity payload",
    )
    source_identity: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        index=True,
        comment="Stable source identity derived from canonical entity top-level or provenance refs",
    )
    source_hash: Mapped[str | None] = sha256_column(
        nullable=True,
        index=True,
        comment="Stable source hash derived from canonical entity top-level or provenance refs",
    )
    layout_id: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        index=True,
        comment=(
            "Best-effort resolved materialized layout row for layout_ref within "
            "the same drawing revision"
        ),
    )
    layer_id: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        index=True,
        comment=(
            "Best-effort resolved materialized layer row for layer_ref within "
            "the same drawing revision"
        ),
    )
    block_id: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        index=True,
        comment=(
            "Best-effort resolved materialized block row for block_ref within "
            "the same drawing revision"
        ),
    )
    parent_entity_row_id: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        index=True,
        comment=(
            "Best-effort resolved materialized parent entity row for "
            "parent_entity_ref within the same drawing revision"
        ),
    )
