"""Revision-scoped normalized entity materialization tables."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class RevisionEntityManifest(Base):
    """Manifest row recording whether revision entities were materialized."""

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
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "projects.id",
            name="fk_revision_entity_manifests_project_id_projects",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Owning project identifier",
    )
    source_file_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Immutable source file identifier for the materialized revision",
    )
    extraction_profile_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Immutable extraction profile identifier used for materialization",
    )
    source_job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "jobs.id",
            name="fk_revision_entity_manifests_source_job_id_jobs",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Job identifier that materialized normalized revision entities",
    )
    drawing_revision_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Drawing revision identifier whose normalized entities were materialized",
    )
    adapter_run_output_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Adapter run output identifier materialized into revision-scoped entity tables",
    )
    canonical_entity_schema_version: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        comment="Canonical entity schema version for the materialized revision payloads",
    )
    counts_json: Mapped[dict[str, int]] = mapped_column(
        JSON,
        nullable=False,
        comment="Materialized normalized row counts keyed by layouts, layers, blocks, and entities",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Manifest creation timestamp",
    )


class RevisionLayout(Base):
    """Materialized layout payload rows scoped to a drawing revision."""

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
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "projects.id",
            name="fk_revision_layouts_project_id_projects",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Owning project identifier",
    )
    source_file_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Immutable source file identifier for this materialized layout row",
    )
    extraction_profile_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Immutable extraction profile identifier used for this layout row",
    )
    source_job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "jobs.id",
            name="fk_revision_layouts_source_job_id_jobs",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Job identifier that materialized this layout row",
    )
    drawing_revision_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Drawing revision identifier that owns this layout row",
    )
    adapter_run_output_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Adapter run output identifier that produced this layout row",
    )
    canonical_entity_schema_version: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        comment="Canonical entity schema version for this layout row payload",
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
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Revision layout materialization timestamp",
    )


class RevisionLayer(Base):
    """Materialized layer payload rows scoped to a drawing revision."""

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
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "projects.id",
            name="fk_revision_layers_project_id_projects",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Owning project identifier",
    )
    source_file_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Immutable source file identifier for this materialized layer row",
    )
    extraction_profile_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Immutable extraction profile identifier used for this layer row",
    )
    source_job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "jobs.id",
            name="fk_revision_layers_source_job_id_jobs",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Job identifier that materialized this layer row",
    )
    drawing_revision_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Drawing revision identifier that owns this layer row",
    )
    adapter_run_output_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Adapter run output identifier that produced this layer row",
    )
    canonical_entity_schema_version: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        comment="Canonical entity schema version for this layer row payload",
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
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Revision layer materialization timestamp",
    )


class RevisionBlock(Base):
    """Materialized block payload rows scoped to a drawing revision."""

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
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "projects.id",
            name="fk_revision_blocks_project_id_projects",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Owning project identifier",
    )
    source_file_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Immutable source file identifier for this materialized block row",
    )
    extraction_profile_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Immutable extraction profile identifier used for this block row",
    )
    source_job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "jobs.id",
            name="fk_revision_blocks_source_job_id_jobs",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Job identifier that materialized this block row",
    )
    drawing_revision_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Drawing revision identifier that owns this block row",
    )
    adapter_run_output_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Adapter run output identifier that produced this block row",
    )
    canonical_entity_schema_version: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        comment="Canonical entity schema version for this block row payload",
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
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Revision block materialization timestamp",
    )


class RevisionEntity(Base):
    """Materialized entity payload rows scoped to a drawing revision."""

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
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique materialized revision entity row identifier (UUID v4)",
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "projects.id",
            name="fk_revision_entities_project_id_projects",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Owning project identifier",
    )
    source_file_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Immutable source file identifier for this materialized entity row",
    )
    extraction_profile_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Immutable extraction profile identifier used for this entity row",
    )
    source_job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "jobs.id",
            name="fk_revision_entities_source_job_id_jobs",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Job identifier that materialized this entity row",
    )
    drawing_revision_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Drawing revision identifier that owns this entity row",
    )
    adapter_run_output_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Adapter run output identifier that produced this entity row",
    )
    canonical_entity_schema_version: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        comment="Canonical entity schema version for this entity row payload",
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
    confidence_score: Mapped[float] = mapped_column(
        nullable=False,
        comment="Entity confidence score extracted from the canonical entity payload",
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
    source_hash: Mapped[str | None] = mapped_column(
        String(64),
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
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Revision entity materialization timestamp",
    )
