"""Append-only estimate version header persistence model."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, validates

from app.db.base import Base

ESTIMATE_SNAPSHOT_ENTRIES_CURRENCY_BY_TYPE = (
    "((entry_type IN ('rate', 'material') AND currency = 'GBP') "
    "OR (entry_type NOT IN ('rate', 'material') AND currency IS NULL))"
)
ESTIMATE_SNAPSHOT_ENTRIES_UNIT_AMOUNT_BY_TYPE = (
    "((entry_type IN ('rate', 'material') AND unit_amount IS NOT NULL) "
    "OR (entry_type NOT IN ('rate', 'material') AND unit_amount IS NULL))"
)
ESTIMATE_SNAPSHOT_ENTRIES_SOURCE_FIELDS_BY_TYPE = (
    "((entry_type = 'rate' "
    "AND source_rate_id IS NOT NULL "
    "AND source_material_id IS NULL "
    "AND source_formula_id IS NULL "
    "AND source_quantity_takeoff_id IS NULL "
    "AND source_quantity_item_id IS NULL "
    "AND source_checksum_sha256 IS NOT NULL "
    "AND quantity_value IS NULL "
    "AND unit IS NOT NULL "
    "AND length(btrim(unit)) > 0 "
    "AND effective_date IS NOT NULL) "
    "OR (entry_type = 'material' "
    "AND source_rate_id IS NULL "
    "AND source_material_id IS NOT NULL "
    "AND source_formula_id IS NULL "
    "AND source_quantity_takeoff_id IS NULL "
    "AND source_quantity_item_id IS NULL "
    "AND source_checksum_sha256 IS NOT NULL "
    "AND quantity_value IS NULL "
    "AND unit IS NOT NULL "
    "AND length(btrim(unit)) > 0 "
    "AND effective_date IS NOT NULL) "
    "OR (entry_type = 'formula' "
    "AND source_rate_id IS NULL "
    "AND source_material_id IS NULL "
    "AND source_formula_id IS NOT NULL "
    "AND source_quantity_takeoff_id IS NULL "
    "AND source_quantity_item_id IS NULL "
    "AND source_checksum_sha256 IS NOT NULL "
    "AND quantity_value IS NULL "
    "AND unit IS NULL "
    "AND effective_date IS NULL) "
    "OR (entry_type = 'assumption' "
    "AND source_rate_id IS NULL "
    "AND source_material_id IS NULL "
    "AND source_formula_id IS NULL "
    "AND source_quantity_takeoff_id IS NULL "
    "AND source_quantity_item_id IS NULL "
    "AND source_checksum_sha256 IS NULL "
    "AND quantity_value IS NULL "
    "AND unit IS NULL "
    "AND effective_date IS NULL) "
    "OR (entry_type = 'quantity_input' "
    "AND source_rate_id IS NULL "
    "AND source_material_id IS NULL "
    "AND source_formula_id IS NULL "
    "AND source_quantity_takeoff_id IS NOT NULL "
    "AND source_checksum_sha256 IS NULL "
    "AND quantity_value IS NOT NULL "
    "AND unit IS NOT NULL "
    "AND length(btrim(unit)) > 0 "
    "AND effective_date IS NULL))"
)
ESTIMATE_ITEMS_SNAPSHOT_ENTRY_TYPE_CONSTANTS = (
    "rate_snapshot_entry_type = 'rate' "
    "AND material_snapshot_entry_type = 'material' "
    "AND formula_snapshot_entry_type = 'formula' "
    "AND assumption_snapshot_entry_type = 'assumption' "
    "AND quantity_snapshot_entry_type = 'quantity_input'"
)
ESTIMATE_ITEMS_SOURCE_FIELDS_BY_LINE_TYPE = (
    "((line_type = 'rate' "
    "AND rate_snapshot_entry_id IS NOT NULL "
    "AND material_snapshot_entry_id IS NULL "
    "AND formula_snapshot_entry_id IS NULL "
    "AND assumption_snapshot_entry_id IS NULL "
    "AND quantity_snapshot_entry_id IS NOT NULL "
    "AND quantity_value IS NOT NULL "
    "AND quantity_unit IS NOT NULL "
    "AND length(btrim(quantity_unit)) > 0 "
    "AND unit_rate_amount IS NOT NULL "
    "AND effective_date IS NOT NULL) "
    "OR (line_type = 'material' "
    "AND rate_snapshot_entry_id IS NULL "
    "AND material_snapshot_entry_id IS NOT NULL "
    "AND formula_snapshot_entry_id IS NULL "
    "AND assumption_snapshot_entry_id IS NULL "
    "AND quantity_snapshot_entry_id IS NOT NULL "
    "AND quantity_value IS NOT NULL "
    "AND quantity_unit IS NOT NULL "
    "AND length(btrim(quantity_unit)) > 0 "
    "AND unit_rate_amount IS NOT NULL "
    "AND effective_date IS NOT NULL) "
    "OR (line_type = 'formula' "
    "AND rate_snapshot_entry_id IS NULL "
    "AND material_snapshot_entry_id IS NULL "
    "AND formula_snapshot_entry_id IS NOT NULL "
    "AND assumption_snapshot_entry_id IS NULL "
    "AND ((quantity_snapshot_entry_id IS NULL "
    "AND quantity_value IS NULL "
    "AND quantity_unit IS NULL) "
    "OR (quantity_snapshot_entry_id IS NOT NULL "
    "AND quantity_value IS NOT NULL "
    "AND quantity_unit IS NOT NULL "
    "AND length(btrim(quantity_unit)) > 0)) "
    "AND unit_rate_amount IS NULL "
    "AND TRUE) "
    "OR (line_type = 'assumption' "
    "AND rate_snapshot_entry_id IS NULL "
    "AND material_snapshot_entry_id IS NULL "
    "AND formula_snapshot_entry_id IS NULL "
    "AND assumption_snapshot_entry_id IS NOT NULL "
    "AND quantity_snapshot_entry_id IS NULL "
    "AND quantity_value IS NULL "
    "AND quantity_unit IS NULL "
    "AND unit_rate_amount IS NULL "
    "AND effective_date IS NULL) "
    "OR (line_type = 'adjustment' "
    "AND rate_snapshot_entry_id IS NULL "
    "AND material_snapshot_entry_id IS NULL "
    "AND ((formula_snapshot_entry_id IS NOT NULL "
    "AND assumption_snapshot_entry_id IS NULL) "
    "OR (formula_snapshot_entry_id IS NULL "
    "AND assumption_snapshot_entry_id IS NOT NULL)) "
    "AND quantity_snapshot_entry_id IS NULL "
    "AND quantity_value IS NULL "
    "AND quantity_unit IS NULL "
    "AND unit_rate_amount IS NULL "
    "AND effective_date IS NULL))"
)


class EstimateVersion(Base):
    """Immutable estimate header persisted from finalized estimation runs."""

    __tablename__ = "estimate_versions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="RESTRICT",
            name="fk_estimate_versions_source_file_id_project_id_files",
        ),
        ForeignKeyConstraint(
            ["drawing_revision_id", "project_id", "source_file_id"],
            [
                "drawing_revisions.id",
                "drawing_revisions.project_id",
                "drawing_revisions.source_file_id",
            ],
            ondelete="RESTRICT",
            name="fk_estimate_versions_revision_lineage",
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
            name="fk_estimate_versions_takeoff_contract",
        ),
        ForeignKeyConstraint(
            ["source_job_id", "project_id", "source_file_id"],
            ["jobs.id", "jobs.project_id", "jobs.file_id"],
            ondelete="RESTRICT",
            name="fk_estimate_versions_source_job_lineage",
        ),
        # Path B 3: estimate versions are no longer gated to allowed + trusted takeoffs.
        # quantity_gate / trusted_totals columns stay (dropped in Path B stage 6).
        CheckConstraint(
            "currency = 'GBP'",
            name="ck_estimate_versions_currency_gbp",
        ),
        CheckConstraint(
            "subtotal_amount::text <> 'NaN' AND subtotal_amount >= 0::numeric",
            name="ck_estimate_versions_subtotal_amount_nonnegative",
        ),
        CheckConstraint(
            "tax_amount::text <> 'NaN' AND tax_amount >= 0::numeric",
            name="ck_estimate_versions_tax_amount_nonnegative",
        ),
        CheckConstraint(
            "total_amount::text <> 'NaN' AND total_amount >= 0::numeric",
            name="ck_estimate_versions_total_amount_nonnegative",
        ),
        UniqueConstraint(
            "id",
            "project_id",
            "drawing_revision_id",
            name="uq_estimate_versions_id_project_id_drawing_revision_id",
        ),
        UniqueConstraint(
            "id",
            "project_id",
            "drawing_revision_id",
            "quantity_takeoff_id",
            name="uq_estimate_versions_id_project_rev_quantity_takeoff_id",
        ),
        UniqueConstraint(
            "source_job_id",
            name="uq_estimate_versions_source_job_id",
        ),
        Index("ix_estimate_versions_source_file_id", "source_file_id"),
        Index("ix_estimate_versions_quantity_takeoff_id", "quantity_takeoff_id"),
        Index("ix_estimate_versions_drawing_revision_id", "drawing_revision_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique estimate version identifier (UUID v4)",
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "projects.id",
            name="fk_estimate_versions_project_id_projects",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Owning project identifier",
    )
    source_file_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Immutable source file identifier inherited from the pinned revision",
    )
    drawing_revision_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Pinned drawing revision identifier used to finalize this estimate",
    )
    quantity_takeoff_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Pinned quantity takeoff identifier used as the estimate input",
    )
    source_job_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Job identifier that finalized this immutable estimate version",
    )
    quantity_gate: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Frozen estimate input gate copied from the referenced quantity takeoff",
    )
    trusted_totals: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        comment="Frozen trusted-input posture copied from the referenced quantity takeoff",
    )
    currency: Mapped[str] = mapped_column(
        String(3),
        nullable=False,
        default="GBP",
        comment="Frozen estimate currency. MVP estimates persist GBP only",
    )
    subtotal_amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 2),
        nullable=False,
        comment="Frozen subtotal monetary amount for this estimate version",
    )
    tax_amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 2),
        nullable=False,
        comment="Frozen tax monetary amount for this estimate version",
    )
    total_amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 2),
        nullable=False,
        comment="Frozen total monetary amount for this estimate version",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Estimate version creation timestamp",
    )

    @validates("subtotal_amount", "tax_amount", "total_amount")
    def _validate_monetary_amount(self, field_name: str, value: Decimal) -> Decimal:
        if value.is_nan():
            raise ValueError(f"{field_name} must not be NaN")
        return value


class EstimateSnapshotEntry(Base):
    """Immutable snapshot entry frozen into an estimate version."""

    __tablename__ = "estimate_snapshot_entries"
    __table_args__ = (
        ForeignKeyConstraint(
            ["estimate_version_id", "project_id", "drawing_revision_id"],
            [
                "estimate_versions.id",
                "estimate_versions.project_id",
                "estimate_versions.drawing_revision_id",
            ],
            ondelete="RESTRICT",
            name="fk_estimate_snapshot_entries_estimate_version_lineage",
        ),
        ForeignKeyConstraint(
            [
                "estimate_version_id",
                "project_id",
                "drawing_revision_id",
                "source_quantity_takeoff_id",
            ],
            [
                "estimate_versions.id",
                "estimate_versions.project_id",
                "estimate_versions.drawing_revision_id",
                "estimate_versions.quantity_takeoff_id",
            ],
            ondelete="RESTRICT",
            name="fk_estimate_snapshot_entries_quantity_parent_takeoff",
        ),
        ForeignKeyConstraint(
            [
                "source_quantity_item_id",
                "source_quantity_takeoff_id",
                "project_id",
                "drawing_revision_id",
            ],
            [
                "quantity_items.id",
                "quantity_items.quantity_takeoff_id",
                "quantity_items.project_id",
                "quantity_items.drawing_revision_id",
            ],
            ondelete="RESTRICT",
            name="fk_est_snapshot_entries_source_quantity_item",
        ),
        CheckConstraint(
            "entry_type IN ('rate', 'material', 'formula', 'assumption', 'quantity_input')",
            name="ck_estimate_snapshot_entries_entry_type_valid",
        ),
        CheckConstraint(
            "length(btrim(entry_key)) > 0",
            name="ck_estimate_snapshot_entries_entry_key_nonempty",
        ),
        CheckConstraint(
            "length(btrim(entry_label)) > 0",
            name="ck_estimate_snapshot_entries_entry_label_nonempty",
        ),
        CheckConstraint(
            "sort_order >= 1",
            name="ck_estimate_snapshot_entries_sort_order_positive",
        ),
        CheckConstraint(
            "jsonb_typeof(source_payload_json) = 'object'",
            name="ck_estimate_snapshot_entries_source_payload_json_object",
        ),
        CheckConstraint(
            "rounding_json IS NULL OR jsonb_typeof(rounding_json) = 'object'",
            name="ck_estimate_snapshot_entries_rounding_json_object",
        ),
        CheckConstraint(
            "source_checksum_sha256 IS NULL OR source_checksum_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_estimate_snapshot_entries_source_checksum_sha256_format",
        ),
        CheckConstraint(
            "quantity_value IS NULL OR (quantity_value::text <> 'NaN' "
            "AND quantity_value >= 0::numeric)",
            name="ck_estimate_snapshot_entries_quantity_value_nonnegative",
        ),
        CheckConstraint(
            "unit_amount IS NULL OR (unit_amount::text <> 'NaN' AND unit_amount >= 0::numeric)",
            name="ck_estimate_snapshot_entries_unit_amount_nonnegative",
        ),
        CheckConstraint(
            ESTIMATE_SNAPSHOT_ENTRIES_CURRENCY_BY_TYPE,
            name="ck_estimate_snapshot_entries_currency_by_type",
        ),
        CheckConstraint(
            ESTIMATE_SNAPSHOT_ENTRIES_UNIT_AMOUNT_BY_TYPE,
            name="ck_estimate_snapshot_entries_unit_amount_by_type",
        ),
        CheckConstraint(
            ESTIMATE_SNAPSHOT_ENTRIES_SOURCE_FIELDS_BY_TYPE,
            name="ck_estimate_snapshot_entries_source_fields_by_type",
        ),
        UniqueConstraint(
            "id",
            "estimate_version_id",
            "project_id",
            "drawing_revision_id",
            "entry_type",
            name="uq_est_snapshot_entries_id_estimate_version_project_rev_type",
        ),
        UniqueConstraint(
            "estimate_version_id",
            "entry_key",
            name="uq_estimate_snapshot_entries_estimate_version_id_entry_key",
        ),
        UniqueConstraint(
            "estimate_version_id",
            "sort_order",
            name="uq_estimate_snapshot_entries_estimate_version_id_sort_order",
        ),
        Index(
            "ix_estimate_snapshot_entries_estimate_version_id",
            "estimate_version_id",
        ),
        Index(
            "ix_estimate_snapshot_entries_source_quantity_takeoff_id",
            "source_quantity_takeoff_id",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique estimate snapshot entry identifier (UUID v4)",
    )
    estimate_version_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Owning immutable estimate version identifier",
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Owning project identifier copied from the estimate version",
    )
    drawing_revision_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Pinned drawing revision identifier copied from the estimate version",
    )
    entry_type: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Frozen snapshot entry type",
    )
    entry_key: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="Deterministic snapshot entry key within the estimate version",
    )
    entry_label: Mapped[str] = mapped_column(
        String(512),
        nullable=False,
        comment="Human-readable frozen snapshot entry label",
    )
    sort_order: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Deterministic snapshot entry order within the estimate version",
    )
    currency: Mapped[str | None] = mapped_column(
        String(3),
        nullable=True,
        comment="Frozen currency for rate/material snapshots; null for non-monetary entries",
    )
    quantity_value: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 6),
        nullable=True,
        comment="Frozen quantity value for quantity-input snapshots",
    )
    unit: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="Frozen unit captured with the snapshot entry",
    )
    effective_date: Mapped[date | None] = mapped_column(
        Date,
        nullable=True,
        comment="Frozen effective date captured for catalog-backed snapshot entries",
    )
    unit_amount: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 6),
        nullable=True,
        comment="Frozen per-unit amount for rate/material snapshots",
    )
    source_payload_json: Mapped[dict[str, object]] = mapped_column(
        JSONB,
        nullable=False,
        comment="Frozen source payload used to reproduce estimate calculations",
    )
    rounding_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
        comment="Frozen rounding metadata for deterministic replay when applicable",
    )
    source_rate_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(
            "rate_catalog_entries.id",
            name="fk_est_snapshot_entries_source_rate",
            ondelete="RESTRICT",
        ),
        nullable=True,
        comment="Pinned source rate identifier for rate snapshot entries",
    )
    source_material_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(
            "material_catalog_entries.id",
            name="fk_est_snapshot_entries_source_material",
            ondelete="RESTRICT",
        ),
        nullable=True,
        comment="Pinned source material identifier for material snapshot entries",
    )
    source_formula_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(
            "formula_definitions.id",
            name="fk_est_snapshot_entries_source_formula",
            ondelete="RESTRICT",
        ),
        nullable=True,
        comment="Pinned source formula identifier for formula snapshot entries",
    )
    source_quantity_takeoff_id: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        comment="Pinned quantity takeoff source for quantity-input snapshot entries",
    )
    source_quantity_item_id: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        comment="Pinned quantity item source for quantity-input snapshot entries",
    )
    source_checksum_sha256: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="Raw lowercase SHA-256 checksum for catalog-backed snapshot entries",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Estimate snapshot entry creation timestamp",
    )

    @validates("quantity_value", "unit_amount")
    def _validate_numeric_amounts(
        self,
        field_name: str,
        value: Decimal | None,
    ) -> Decimal | None:
        if value is not None and value.is_nan():
            raise ValueError(f"{field_name} must not be NaN")
        return value


class EstimateItem(Base):
    """Immutable estimate line item frozen from deterministic inputs."""

    __tablename__ = "estimate_items"
    __table_args__ = (
        ForeignKeyConstraint(
            ["estimate_version_id", "project_id", "drawing_revision_id"],
            [
                "estimate_versions.id",
                "estimate_versions.project_id",
                "estimate_versions.drawing_revision_id",
            ],
            ondelete="RESTRICT",
            name="fk_estimate_items_estimate_version_lineage",
        ),
        ForeignKeyConstraint(
            [
                "rate_snapshot_entry_id",
                "estimate_version_id",
                "project_id",
                "drawing_revision_id",
                "rate_snapshot_entry_type",
            ],
            [
                "estimate_snapshot_entries.id",
                "estimate_snapshot_entries.estimate_version_id",
                "estimate_snapshot_entries.project_id",
                "estimate_snapshot_entries.drawing_revision_id",
                "estimate_snapshot_entries.entry_type",
            ],
            ondelete="RESTRICT",
            name="fk_estimate_items_rate_snapshot_entry",
        ),
        ForeignKeyConstraint(
            [
                "material_snapshot_entry_id",
                "estimate_version_id",
                "project_id",
                "drawing_revision_id",
                "material_snapshot_entry_type",
            ],
            [
                "estimate_snapshot_entries.id",
                "estimate_snapshot_entries.estimate_version_id",
                "estimate_snapshot_entries.project_id",
                "estimate_snapshot_entries.drawing_revision_id",
                "estimate_snapshot_entries.entry_type",
            ],
            ondelete="RESTRICT",
            name="fk_estimate_items_material_snapshot_entry",
        ),
        ForeignKeyConstraint(
            [
                "formula_snapshot_entry_id",
                "estimate_version_id",
                "project_id",
                "drawing_revision_id",
                "formula_snapshot_entry_type",
            ],
            [
                "estimate_snapshot_entries.id",
                "estimate_snapshot_entries.estimate_version_id",
                "estimate_snapshot_entries.project_id",
                "estimate_snapshot_entries.drawing_revision_id",
                "estimate_snapshot_entries.entry_type",
            ],
            ondelete="RESTRICT",
            name="fk_estimate_items_formula_snapshot_entry",
        ),
        ForeignKeyConstraint(
            [
                "assumption_snapshot_entry_id",
                "estimate_version_id",
                "project_id",
                "drawing_revision_id",
                "assumption_snapshot_entry_type",
            ],
            [
                "estimate_snapshot_entries.id",
                "estimate_snapshot_entries.estimate_version_id",
                "estimate_snapshot_entries.project_id",
                "estimate_snapshot_entries.drawing_revision_id",
                "estimate_snapshot_entries.entry_type",
            ],
            ondelete="RESTRICT",
            name="fk_estimate_items_assumption_snapshot_entry",
        ),
        ForeignKeyConstraint(
            [
                "quantity_snapshot_entry_id",
                "estimate_version_id",
                "project_id",
                "drawing_revision_id",
                "quantity_snapshot_entry_type",
            ],
            [
                "estimate_snapshot_entries.id",
                "estimate_snapshot_entries.estimate_version_id",
                "estimate_snapshot_entries.project_id",
                "estimate_snapshot_entries.drawing_revision_id",
                "estimate_snapshot_entries.entry_type",
            ],
            ondelete="RESTRICT",
            name="fk_estimate_items_quantity_snapshot_entry",
        ),
        CheckConstraint(
            "line_type IN ('rate', 'material', 'formula', 'assumption', 'adjustment')",
            name="ck_estimate_items_line_type_valid",
        ),
        CheckConstraint(
            "line_number >= 1",
            name="ck_estimate_items_line_number_positive",
        ),
        CheckConstraint(
            "length(btrim(line_key)) > 0",
            name="ck_estimate_items_line_key_nonempty",
        ),
        CheckConstraint(
            "length(btrim(description)) > 0",
            name="ck_estimate_items_description_nonempty",
        ),
        CheckConstraint(
            "currency = 'GBP'",
            name="ck_estimate_items_currency_gbp",
        ),
        CheckConstraint(
            "rounding_json IS NULL OR jsonb_typeof(rounding_json) = 'object'",
            name="ck_estimate_items_rounding_json_object",
        ),
        CheckConstraint(
            "quantity_value IS NULL OR (quantity_value::text <> 'NaN' "
            "AND quantity_value >= 0::numeric)",
            name="ck_estimate_items_quantity_value_nonnegative",
        ),
        CheckConstraint(
            "unit_rate_amount IS NULL OR (unit_rate_amount::text <> 'NaN' "
            "AND unit_rate_amount >= 0::numeric)",
            name="ck_estimate_items_unit_rate_amount_nonnegative",
        ),
        CheckConstraint(
            "subtotal_amount::text <> 'NaN' AND subtotal_amount >= 0::numeric",
            name="ck_estimate_items_subtotal_amount_nonnegative",
        ),
        CheckConstraint(
            "tax_amount::text <> 'NaN' AND tax_amount >= 0::numeric",
            name="ck_estimate_items_tax_amount_nonnegative",
        ),
        CheckConstraint(
            "total_amount::text <> 'NaN' AND total_amount >= 0::numeric",
            name="ck_estimate_items_total_amount_nonnegative",
        ),
        CheckConstraint(
            ESTIMATE_ITEMS_SNAPSHOT_ENTRY_TYPE_CONSTANTS,
            name="ck_estimate_items_snapshot_entry_type_constants",
        ),
        CheckConstraint(
            ESTIMATE_ITEMS_SOURCE_FIELDS_BY_LINE_TYPE,
            name="ck_estimate_items_source_fields_by_line_type",
        ),
        UniqueConstraint(
            "estimate_version_id",
            "line_number",
            name="uq_estimate_items_estimate_version_id_line_number",
        ),
        UniqueConstraint(
            "estimate_version_id",
            "line_key",
            name="uq_estimate_items_estimate_version_id_line_key",
        ),
        Index("ix_estimate_items_estimate_version_id", "estimate_version_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique estimate line item identifier (UUID v4)",
    )
    estimate_version_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Owning immutable estimate version identifier",
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Owning project identifier copied from the estimate version",
    )
    drawing_revision_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Pinned drawing revision identifier copied from the estimate version",
    )
    line_type: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Frozen estimate line item type",
    )
    line_number: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Deterministic line number within the estimate version",
    )
    line_key: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="Deterministic line key within the estimate version",
    )
    description: Mapped[str] = mapped_column(
        String(512),
        nullable=False,
        comment="Human-readable frozen estimate line item description",
    )
    currency: Mapped[str] = mapped_column(
        String(3),
        nullable=False,
        default="GBP",
        comment="Frozen estimate line item currency. MVP estimates persist GBP only",
    )
    quantity_value: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 6),
        nullable=True,
        comment="Frozen quantity basis for quantity-driven estimate line items",
    )
    quantity_unit: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="Frozen quantity unit for quantity-driven estimate line items",
    )
    unit_rate_amount: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 6),
        nullable=True,
        comment="Frozen per-unit rate amount for rate/material line items",
    )
    effective_date: Mapped[date | None] = mapped_column(
        Date,
        nullable=True,
        comment="Frozen effective date captured for priced estimate line items",
    )
    subtotal_amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 2),
        nullable=False,
        comment="Frozen line subtotal monetary amount",
    )
    tax_amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 2),
        nullable=False,
        comment="Frozen line tax monetary amount",
    )
    total_amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 2),
        nullable=False,
        comment="Frozen line total monetary amount",
    )
    rounding_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
        comment="Frozen line rounding metadata for deterministic replay when applicable",
    )
    quantity_snapshot_entry_id: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        comment="Referenced quantity-input snapshot entry for quantity-driven line items",
    )
    quantity_snapshot_entry_type: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="quantity_input",
        comment="Constant snapshot entry type discriminator for quantity-input refs",
    )
    rate_snapshot_entry_id: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        comment="Referenced rate snapshot entry when line_type=rate",
    )
    rate_snapshot_entry_type: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="rate",
        comment="Constant snapshot entry type discriminator for rate refs",
    )
    material_snapshot_entry_id: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        comment="Referenced material snapshot entry when line_type=material",
    )
    material_snapshot_entry_type: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="material",
        comment="Constant snapshot entry type discriminator for material refs",
    )
    formula_snapshot_entry_id: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        comment="Referenced formula snapshot entry when line_type=formula",
    )
    formula_snapshot_entry_type: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="formula",
        comment="Constant snapshot entry type discriminator for formula refs",
    )
    assumption_snapshot_entry_id: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        comment="Referenced assumption snapshot entry when line_type=assumption",
    )
    assumption_snapshot_entry_type: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="assumption",
        comment="Constant snapshot entry type discriminator for assumption refs",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Estimate line item creation timestamp",
    )

    @validates(
        "quantity_value",
        "unit_rate_amount",
        "subtotal_amount",
        "tax_amount",
        "total_amount",
    )
    def _validate_numeric_amounts(
        self,
        field_name: str,
        value: Decimal | None,
    ) -> Decimal | None:
        if value is not None and value.is_nan():
            raise ValueError(f"{field_name} must not be NaN")
        return value
