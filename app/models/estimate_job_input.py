"""Immutable estimate job input persistence models."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.job import JobType

_ESTIMATE_JOB_INPUT_REF_TYPED_CATALOG_CONTRACT = (
    "(ref_type = 'rate' AND rate_catalog_entry_id IS NOT NULL "
    "AND material_catalog_entry_id IS NULL AND formula_definition_id IS NULL) OR "
    "(ref_type = 'material' AND material_catalog_entry_id IS NOT NULL "
    "AND rate_catalog_entry_id IS NULL AND formula_definition_id IS NULL) OR "
    "(ref_type = 'formula' AND formula_definition_id IS NOT NULL "
    "AND rate_catalog_entry_id IS NULL AND material_catalog_entry_id IS NULL)"
)


class EstimateJobInput(Base):
    """Immutable estimate job lineage captured when an estimate job is queued."""

    __tablename__ = "estimate_job_inputs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="RESTRICT",
            name="fk_estimate_job_inputs_source_file_id_project_id_files",
        ),
        ForeignKeyConstraint(
            ["drawing_revision_id", "project_id", "source_file_id"],
            [
                "drawing_revisions.id",
                "drawing_revisions.project_id",
                "drawing_revisions.source_file_id",
            ],
            ondelete="RESTRICT",
            name="fk_estimate_job_inputs_revision_lineage",
        ),
        ForeignKeyConstraint(
            [
                "estimate_job_id",
                "project_id",
                "source_file_id",
                "drawing_revision_id",
                "source_job_type",
            ],
            [
                "jobs.id",
                "jobs.project_id",
                "jobs.file_id",
                "jobs.base_revision_id",
                "jobs.job_type",
            ],
            ondelete="RESTRICT",
            name="fk_estimate_job_inputs_source_job_contract",
        ),
        ForeignKeyConstraint(
            [
                "quantity_takeoff_id",
                "project_id",
                "drawing_revision_id",
                "quantity_gate",
                "trusted_totals",
            ],
            [
                "quantity_takeoffs.id",
                "quantity_takeoffs.project_id",
                "quantity_takeoffs.drawing_revision_id",
                "quantity_takeoffs.quantity_gate",
                "quantity_takeoffs.trusted_totals",
            ],
            ondelete="RESTRICT",
            name="fk_estimate_job_inputs_takeoff_contract",
        ),
        CheckConstraint(
            f"source_job_type = '{JobType.ESTIMATE.value}'",
            name="ck_estimate_job_inputs_source_job_type_estimate",
        ),
        # Path B 3: estimates are no longer gated to allowed + trusted takeoffs.
        # quantity_gate / trusted_totals columns stay (dropped in Path B stage 6).
        CheckConstraint(
            "currency = 'GBP'",
            name="ck_estimate_job_inputs_currency_gbp",
        ),
        CheckConstraint(
            "pricing_mode IN ('explicit', 'derived_from_job_created_at_utc')",
            name="ck_estimate_job_inputs_pricing_mode_valid",
        ),
        CheckConstraint(
            "jsonb_typeof(assumptions_json) = 'object'",
            name="ck_estimate_job_inputs_assumptions_json_object",
        ),
        UniqueConstraint(
            "estimate_job_id",
            "project_id",
            "drawing_revision_id",
            name="uq_estimate_job_inputs_id_project_id_drawing_revision_id",
        ),
        Index("ix_estimate_job_inputs_quantity_takeoff_id", "quantity_takeoff_id"),
        Index("ix_estimate_job_inputs_drawing_revision_id", "drawing_revision_id"),
    )

    estimate_job_id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        comment="Owning immutable estimate job identifier",
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "projects.id",
            name="fk_estimate_job_inputs_project_id_projects",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Owning project identifier copied from the estimate job",
    )
    source_file_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Pinned source file identifier copied from the estimate job",
    )
    drawing_revision_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Pinned drawing revision identifier copied from the estimate job",
    )
    quantity_takeoff_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Trusted allowed quantity takeoff selected for the estimate job",
    )
    source_job_type: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        default=JobType.ESTIMATE.value,
        comment="Denormalized job type used by the composite estimate-job contract",
    )
    # Path B 5b: copied from the (now-NULL) takeoff gate; vestigial, dropped in stage 6.
    quantity_gate: Mapped[str | None] = mapped_column(
        String(32),
        nullable=True,
        comment="Vestigial quantity gate copied from the takeoff (dropped in Path B stage 6)",
    )
    trusted_totals: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        nullable=False,
        comment="Persisted trusted-total contract copied from the selected takeoff",
    )
    currency: Mapped[str] = mapped_column(
        String(3),
        nullable=False,
        comment="Pinned estimate currency contract resolved when the job was queued",
    )
    pricing_effective_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        comment="Resolved pricing effective date captured when the estimate job was queued",
    )
    pricing_mode: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="Pricing-date resolution mode captured when the estimate job was queued",
    )
    assumptions_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        comment="Estimate assumptions JSON object captured when the estimate job was queued",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Estimate job input capture timestamp",
    )


class EstimateJobInputCatalogRef(Base):
    """Immutable typed catalog references captured for an estimate job input."""

    __tablename__ = "estimate_job_input_catalog_refs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["estimate_job_id"],
            ["estimate_job_inputs.estimate_job_id"],
            ondelete="RESTRICT",
            name="fk_estimate_job_input_catalog_refs_estimate_job_id",
        ),
        CheckConstraint(
            "length(btrim(selection_key)) > 0",
            name="ck_estimate_job_input_catalog_refs_selection_key_nonblank",
        ),
        CheckConstraint(
            "ref_order >= 0",
            name="ck_estimate_job_input_catalog_refs_ref_order_nonnegative",
        ),
        CheckConstraint(
            "ref_type IN ('rate', 'material', 'formula')",
            name="ck_estimate_job_input_catalog_refs_ref_type_valid",
        ),
        CheckConstraint(
            _ESTIMATE_JOB_INPUT_REF_TYPED_CATALOG_CONTRACT,
            name="ck_estimate_job_input_catalog_refs_typed_catalog_ref_match",
        ),
        CheckConstraint(
            "catalog_checksum_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_estimate_job_input_catalog_refs_checksum_sha256_format",
        ),
        CheckConstraint(
            "jsonb_typeof(selection_context_json) = 'object'",
            name="ck_est_job_input_catalog_refs_context_json_object",
        ),
        UniqueConstraint(
            "estimate_job_id",
            "ref_type",
            "selection_key",
            name="uq_estimate_job_input_catalog_refs_job_ref_type_selection_key",
        ),
        UniqueConstraint(
            "estimate_job_id",
            "ref_order",
            name="uq_estimate_job_input_catalog_refs_estimate_job_id_ref_order",
        ),
    )

    estimate_job_id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        comment="Owning immutable estimate job identifier",
    )
    ref_type: Mapped[str] = mapped_column(
        String(16),
        primary_key=True,
        comment="Typed catalog reference discriminator captured for deterministic selection",
    )
    selection_key: Mapped[str] = mapped_column(
        String(255),
        primary_key=True,
        comment="Deterministic selection key within the estimate job input",
    )
    ref_order: Mapped[int] = mapped_column(
        Integer,
        comment="Deterministic catalog reference order within the estimate job input",
    )
    rate_catalog_entry_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(
            "rate_catalog_entries.id",
            name="fk_estimate_job_input_catalog_refs_rate_catalog_entry_id",
            ondelete="RESTRICT",
        ),
        nullable=True,
        comment="Selected rate catalog entry identifier, when this ref is rate-typed",
    )
    material_catalog_entry_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(
            "material_catalog_entries.id",
            name="fk_estimate_job_input_catalog_refs_material_catalog_entry_id",
            ondelete="RESTRICT",
        ),
        nullable=True,
        comment="Selected material catalog entry identifier, when this ref is material-typed",
    )
    formula_definition_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(
            "formula_definitions.id",
            name="fk_estimate_job_input_catalog_refs_formula_definition_id",
            ondelete="RESTRICT",
        ),
        nullable=True,
        comment="Selected formula definition identifier, when this ref is formula-typed",
    )
    catalog_checksum_sha256: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="Immutable catalog checksum snapshot copied when the ref was selected",
    )
    selection_context_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        comment="Selection context JSON object captured when the catalog ref was chosen",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Catalog reference capture timestamp",
    )
