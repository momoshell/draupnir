"""add estimate snapshot entries and line items

Revision ID: 2026_05_18_0021
Revises: 2026_05_17_0020
Create Date: 2026-05-18 10:35:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "2026_05_18_0021"
down_revision = "2026_05_17_0020"
branch_labels = None
depends_on = None


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


def _attach_append_only_triggers(table_name: str) -> None:
    op.execute(
        sa.text(
            f"""
            CREATE TRIGGER trg_append_only_row_guard
            BEFORE UPDATE OR DELETE ON "{table_name}"
            FOR EACH ROW
            EXECUTE FUNCTION enforce_append_only_lineage_row()
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            CREATE TRIGGER trg_append_only_truncate_guard
            BEFORE TRUNCATE ON "{table_name}"
            FOR EACH STATEMENT
            EXECUTE FUNCTION enforce_append_only_lineage_truncate()
            """
        )
    )


def _drop_append_only_triggers(table_name: str) -> None:
    op.execute(sa.text(f'DROP TRIGGER IF EXISTS trg_append_only_row_guard ON "{table_name}"'))
    op.execute(
        sa.text(f'DROP TRIGGER IF EXISTS trg_append_only_truncate_guard ON "{table_name}"')
    )


def _assert_estimate_reproducibility_tables_empty() -> None:
    bind = op.get_bind()
    for table_name in ("estimate_items", "estimate_snapshot_entries"):
        row_count = bind.execute(sa.text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar_one()
        if row_count:
            raise RuntimeError(
                "Refusing to downgrade migration 2026_05_18_0021 while "
                f"{table_name} contains {row_count} row(s)."
            )


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_estimate_versions_id_project_rev_quantity_takeoff_id",
        "estimate_versions",
        ["id", "project_id", "drawing_revision_id", "quantity_takeoff_id"],
    )
    op.create_unique_constraint(
        "uq_quantity_items_id_takeoff_project_drawing_revision",
        "quantity_items",
        ["id", "quantity_takeoff_id", "project_id", "drawing_revision_id"],
    )

    op.create_table(
        "estimate_snapshot_entries",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="Unique estimate snapshot entry identifier (UUID v4)",
        ),
        sa.Column(
            "estimate_version_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="Owning immutable estimate version identifier",
        ),
        sa.Column(
            "project_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="Owning project identifier copied from the estimate version",
        ),
        sa.Column(
            "drawing_revision_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="Pinned drawing revision identifier copied from the estimate version",
        ),
        sa.Column(
            "entry_type",
            sa.String(length=32),
            nullable=False,
            comment="Frozen snapshot entry type",
        ),
        sa.Column(
            "entry_key",
            sa.String(length=128),
            nullable=False,
            comment="Deterministic snapshot entry key within the estimate version",
        ),
        sa.Column(
            "entry_label",
            sa.String(length=512),
            nullable=False,
            comment="Human-readable frozen snapshot entry label",
        ),
        sa.Column(
            "sort_order",
            sa.Integer(),
            nullable=False,
            comment="Deterministic snapshot entry order within the estimate version",
        ),
        sa.Column(
            "currency",
            sa.String(length=3),
            nullable=True,
            comment="Frozen currency for rate/material snapshots; null for non-monetary entries",
        ),
        sa.Column(
            "quantity_value",
            sa.Numeric(precision=18, scale=6),
            nullable=True,
            comment="Frozen quantity value for quantity-input snapshots",
        ),
        sa.Column(
            "unit",
            sa.String(length=64),
            nullable=True,
            comment="Frozen unit captured with the snapshot entry",
        ),
        sa.Column(
            "effective_date",
            sa.Date(),
            nullable=True,
            comment="Frozen effective date captured for catalog-backed snapshot entries",
        ),
        sa.Column(
            "unit_amount",
            sa.Numeric(precision=18, scale=6),
            nullable=True,
            comment="Frozen per-unit amount for rate/material snapshots",
        ),
        sa.Column(
            "source_payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            comment="Frozen source payload used to reproduce estimate calculations",
        ),
        sa.Column(
            "rounding_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            comment="Frozen rounding metadata for deterministic replay when applicable",
        ),
        sa.Column(
            "source_rate_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
            comment="Pinned source rate identifier for rate snapshot entries",
        ),
        sa.Column(
            "source_material_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
            comment="Pinned source material identifier for material snapshot entries",
        ),
        sa.Column(
            "source_formula_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
            comment="Pinned source formula identifier for formula snapshot entries",
        ),
        sa.Column(
            "source_quantity_takeoff_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
            comment="Pinned quantity takeoff source for quantity-input snapshot entries",
        ),
        sa.Column(
            "source_quantity_item_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
            comment="Pinned quantity item source for quantity-input snapshot entries",
        ),
        sa.Column(
            "source_checksum_sha256",
            sa.String(length=64),
            nullable=True,
            comment="Raw lowercase SHA-256 checksum for catalog-backed snapshot entries",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
            comment="Estimate snapshot entry creation timestamp",
        ),
        sa.CheckConstraint(
            "entry_type IN ('rate', 'material', 'formula', 'assumption', 'quantity_input')",
            name="ck_estimate_snapshot_entries_entry_type_valid",
        ),
        sa.CheckConstraint(
            "length(btrim(entry_key)) > 0",
            name="ck_estimate_snapshot_entries_entry_key_nonempty",
        ),
        sa.CheckConstraint(
            "length(btrim(entry_label)) > 0",
            name="ck_estimate_snapshot_entries_entry_label_nonempty",
        ),
        sa.CheckConstraint(
            "sort_order >= 1",
            name="ck_estimate_snapshot_entries_sort_order_positive",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(source_payload_json) = 'object'",
            name="ck_estimate_snapshot_entries_source_payload_json_object",
        ),
        sa.CheckConstraint(
            "rounding_json IS NULL OR jsonb_typeof(rounding_json) = 'object'",
            name="ck_estimate_snapshot_entries_rounding_json_object",
        ),
        sa.CheckConstraint(
            "source_checksum_sha256 IS NULL OR source_checksum_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_estimate_snapshot_entries_source_checksum_sha256_format",
        ),
        sa.CheckConstraint(
            "quantity_value IS NULL OR (quantity_value::text <> 'NaN' "
            "AND quantity_value >= 0::numeric)",
            name="ck_estimate_snapshot_entries_quantity_value_nonnegative",
        ),
        sa.CheckConstraint(
            "unit_amount IS NULL OR (unit_amount::text <> 'NaN' AND unit_amount >= 0::numeric)",
            name="ck_estimate_snapshot_entries_unit_amount_nonnegative",
        ),
        sa.CheckConstraint(
            ESTIMATE_SNAPSHOT_ENTRIES_CURRENCY_BY_TYPE,
            name="ck_estimate_snapshot_entries_currency_by_type",
        ),
        sa.CheckConstraint(
            ESTIMATE_SNAPSHOT_ENTRIES_UNIT_AMOUNT_BY_TYPE,
            name="ck_estimate_snapshot_entries_unit_amount_by_type",
        ),
        sa.CheckConstraint(
            ESTIMATE_SNAPSHOT_ENTRIES_SOURCE_FIELDS_BY_TYPE,
            name="ck_estimate_snapshot_entries_source_fields_by_type",
        ),
        sa.ForeignKeyConstraint(
            ["estimate_version_id", "project_id", "drawing_revision_id"],
            [
                "estimate_versions.id",
                "estimate_versions.project_id",
                "estimate_versions.drawing_revision_id",
            ],
            name="fk_estimate_snapshot_entries_estimate_version_lineage",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_rate_id"],
            ["rate_catalog_entries.id"],
            name="fk_est_snapshot_entries_source_rate",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_material_id"],
            ["material_catalog_entries.id"],
            name="fk_est_snapshot_entries_source_material",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_formula_id"],
            ["formula_definitions.id"],
            name="fk_est_snapshot_entries_source_formula",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
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
            name="fk_estimate_snapshot_entries_quantity_parent_takeoff",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
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
            name="fk_est_snapshot_entries_source_quantity_item",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_estimate_snapshot_entries")),
        sa.UniqueConstraint(
            "id",
            "estimate_version_id",
            "project_id",
            "drawing_revision_id",
            "entry_type",
            name="uq_est_snapshot_entries_id_estimate_version_project_rev_type",
        ),
        sa.UniqueConstraint(
            "estimate_version_id",
            "entry_key",
            name="uq_estimate_snapshot_entries_estimate_version_id_entry_key",
        ),
        sa.UniqueConstraint(
            "estimate_version_id",
            "sort_order",
            name="uq_estimate_snapshot_entries_estimate_version_id_sort_order",
        ),
    )
    op.create_index(
        "ix_estimate_snapshot_entries_estimate_version_id",
        "estimate_snapshot_entries",
        ["estimate_version_id"],
    )
    op.create_index(
        "ix_estimate_snapshot_entries_source_quantity_takeoff_id",
        "estimate_snapshot_entries",
        ["source_quantity_takeoff_id"],
    )

    op.create_table(
        "estimate_items",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="Unique estimate line item identifier (UUID v4)",
        ),
        sa.Column(
            "estimate_version_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="Owning immutable estimate version identifier",
        ),
        sa.Column(
            "project_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="Owning project identifier copied from the estimate version",
        ),
        sa.Column(
            "drawing_revision_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="Pinned drawing revision identifier copied from the estimate version",
        ),
        sa.Column(
            "line_type",
            sa.String(length=32),
            nullable=False,
            comment="Frozen estimate line item type",
        ),
        sa.Column(
            "line_number",
            sa.Integer(),
            nullable=False,
            comment="Deterministic line number within the estimate version",
        ),
        sa.Column(
            "line_key",
            sa.String(length=128),
            nullable=False,
            comment="Deterministic line key within the estimate version",
        ),
        sa.Column(
            "description",
            sa.String(length=512),
            nullable=False,
            comment="Human-readable frozen estimate line item description",
        ),
        sa.Column(
            "currency",
            sa.String(length=3),
            nullable=False,
            server_default="GBP",
            comment="Frozen estimate line item currency. MVP estimates persist GBP only",
        ),
        sa.Column(
            "quantity_value",
            sa.Numeric(precision=18, scale=6),
            nullable=True,
            comment="Frozen quantity basis for quantity-driven estimate line items",
        ),
        sa.Column(
            "quantity_unit",
            sa.String(length=64),
            nullable=True,
            comment="Frozen quantity unit for quantity-driven estimate line items",
        ),
        sa.Column(
            "unit_rate_amount",
            sa.Numeric(precision=18, scale=6),
            nullable=True,
            comment="Frozen per-unit rate amount for rate/material line items",
        ),
        sa.Column(
            "effective_date",
            sa.Date(),
            nullable=True,
            comment="Frozen effective date captured for priced estimate line items",
        ),
        sa.Column(
            "subtotal_amount",
            sa.Numeric(precision=18, scale=2),
            nullable=False,
            comment="Frozen line subtotal monetary amount",
        ),
        sa.Column(
            "tax_amount",
            sa.Numeric(precision=18, scale=2),
            nullable=False,
            comment="Frozen line tax monetary amount",
        ),
        sa.Column(
            "total_amount",
            sa.Numeric(precision=18, scale=2),
            nullable=False,
            comment="Frozen line total monetary amount",
        ),
        sa.Column(
            "rounding_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            comment="Frozen line rounding metadata for deterministic replay when applicable",
        ),
        sa.Column(
            "quantity_snapshot_entry_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
            comment="Referenced quantity-input snapshot entry for quantity-driven line items",
        ),
        sa.Column(
            "quantity_snapshot_entry_type",
            sa.String(length=32),
            nullable=False,
            server_default="quantity_input",
            comment="Constant snapshot entry type discriminator for quantity-input refs",
        ),
        sa.Column(
            "rate_snapshot_entry_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
            comment="Referenced rate snapshot entry when line_type=rate",
        ),
        sa.Column(
            "rate_snapshot_entry_type",
            sa.String(length=32),
            nullable=False,
            server_default="rate",
            comment="Constant snapshot entry type discriminator for rate refs",
        ),
        sa.Column(
            "material_snapshot_entry_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
            comment="Referenced material snapshot entry when line_type=material",
        ),
        sa.Column(
            "material_snapshot_entry_type",
            sa.String(length=32),
            nullable=False,
            server_default="material",
            comment="Constant snapshot entry type discriminator for material refs",
        ),
        sa.Column(
            "formula_snapshot_entry_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
            comment="Referenced formula snapshot entry when line_type=formula",
        ),
        sa.Column(
            "formula_snapshot_entry_type",
            sa.String(length=32),
            nullable=False,
            server_default="formula",
            comment="Constant snapshot entry type discriminator for formula refs",
        ),
        sa.Column(
            "assumption_snapshot_entry_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
            comment="Referenced assumption snapshot entry when line_type=assumption",
        ),
        sa.Column(
            "assumption_snapshot_entry_type",
            sa.String(length=32),
            nullable=False,
            server_default="assumption",
            comment="Constant snapshot entry type discriminator for assumption refs",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
            comment="Estimate line item creation timestamp",
        ),
        sa.CheckConstraint(
            "line_type IN ('rate', 'material', 'formula', 'assumption', 'adjustment')",
            name="ck_estimate_items_line_type_valid",
        ),
        sa.CheckConstraint(
            "line_number >= 1",
            name="ck_estimate_items_line_number_positive",
        ),
        sa.CheckConstraint(
            "length(btrim(line_key)) > 0",
            name="ck_estimate_items_line_key_nonempty",
        ),
        sa.CheckConstraint(
            "length(btrim(description)) > 0",
            name="ck_estimate_items_description_nonempty",
        ),
        sa.CheckConstraint(
            "currency = 'GBP'",
            name="ck_estimate_items_currency_gbp",
        ),
        sa.CheckConstraint(
            "rounding_json IS NULL OR jsonb_typeof(rounding_json) = 'object'",
            name="ck_estimate_items_rounding_json_object",
        ),
        sa.CheckConstraint(
            "quantity_value IS NULL OR (quantity_value::text <> 'NaN' "
            "AND quantity_value >= 0::numeric)",
            name="ck_estimate_items_quantity_value_nonnegative",
        ),
        sa.CheckConstraint(
            "unit_rate_amount IS NULL OR (unit_rate_amount::text <> 'NaN' "
            "AND unit_rate_amount >= 0::numeric)",
            name="ck_estimate_items_unit_rate_amount_nonnegative",
        ),
        sa.CheckConstraint(
            "subtotal_amount::text <> 'NaN' AND subtotal_amount >= 0::numeric",
            name="ck_estimate_items_subtotal_amount_nonnegative",
        ),
        sa.CheckConstraint(
            "tax_amount::text <> 'NaN' AND tax_amount >= 0::numeric",
            name="ck_estimate_items_tax_amount_nonnegative",
        ),
        sa.CheckConstraint(
            "total_amount::text <> 'NaN' AND total_amount >= 0::numeric",
            name="ck_estimate_items_total_amount_nonnegative",
        ),
        sa.CheckConstraint(
            ESTIMATE_ITEMS_SNAPSHOT_ENTRY_TYPE_CONSTANTS,
            name="ck_estimate_items_snapshot_entry_type_constants",
        ),
        sa.CheckConstraint(
            ESTIMATE_ITEMS_SOURCE_FIELDS_BY_LINE_TYPE,
            name="ck_estimate_items_source_fields_by_line_type",
        ),
        sa.ForeignKeyConstraint(
            ["estimate_version_id", "project_id", "drawing_revision_id"],
            [
                "estimate_versions.id",
                "estimate_versions.project_id",
                "estimate_versions.drawing_revision_id",
            ],
            name="fk_estimate_items_estimate_version_lineage",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
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
            name="fk_estimate_items_quantity_snapshot_entry",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
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
            name="fk_estimate_items_rate_snapshot_entry",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
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
            name="fk_estimate_items_material_snapshot_entry",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
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
            name="fk_estimate_items_formula_snapshot_entry",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
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
            name="fk_estimate_items_assumption_snapshot_entry",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_estimate_items")),
        sa.UniqueConstraint(
            "estimate_version_id",
            "line_number",
            name="uq_estimate_items_estimate_version_id_line_number",
        ),
        sa.UniqueConstraint(
            "estimate_version_id",
            "line_key",
            name="uq_estimate_items_estimate_version_id_line_key",
        ),
    )
    op.create_index(
        "ix_estimate_items_estimate_version_id",
        "estimate_items",
        ["estimate_version_id"],
    )

    _attach_append_only_triggers("estimate_snapshot_entries")
    _attach_append_only_triggers("estimate_items")


def downgrade() -> None:
    _assert_estimate_reproducibility_tables_empty()

    _drop_append_only_triggers("estimate_items")
    _drop_append_only_triggers("estimate_snapshot_entries")

    op.drop_index("ix_estimate_items_estimate_version_id", table_name="estimate_items")
    op.drop_table("estimate_items")

    op.drop_index(
        "ix_estimate_snapshot_entries_source_quantity_takeoff_id",
        table_name="estimate_snapshot_entries",
    )
    op.drop_index(
        "ix_estimate_snapshot_entries_estimate_version_id",
        table_name="estimate_snapshot_entries",
    )
    op.drop_table("estimate_snapshot_entries")

    op.drop_constraint(
        "uq_quantity_items_id_takeoff_project_drawing_revision",
        "quantity_items",
        type_="unique",
    )

    op.drop_constraint(
        "uq_estimate_versions_id_project_rev_quantity_takeoff_id",
        "estimate_versions",
        type_="unique",
    )
