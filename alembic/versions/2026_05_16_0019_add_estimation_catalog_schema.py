# ruff: noqa: I001

"""add estimation catalog schema

Revision ID: 2026_05_16_0019
Revises: 2026_05_15_0018
Create Date: 2026-05-16 00:19:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "2026_05_16_0019"
down_revision = "2026_05_15_0018"
branch_labels = None
depends_on = None


CHECKSUM_PATTERN = "^[0-9a-f]{64}$"


def _attach_append_only_guards(table_name: str) -> None:
    op.execute(
        sa.text(
            f"""
            CREATE TRIGGER trg_{table_name}_append_only_row_guard
            BEFORE UPDATE OR DELETE ON {table_name}
            FOR EACH ROW
            EXECUTE FUNCTION enforce_append_only_lineage_row()
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            CREATE TRIGGER trg_{table_name}_append_only_truncate_guard
            BEFORE TRUNCATE ON {table_name}
            FOR EACH STATEMENT
            EXECUTE FUNCTION enforce_append_only_lineage_truncate()
            """
        )
    )


def _detach_append_only_guards(table_name: str) -> None:
    op.execute(
        sa.text(
            f"DROP TRIGGER IF EXISTS trg_{table_name}_append_only_truncate_guard "
            f"ON {table_name}"
        )
    )
    op.execute(
        sa.text(
            f"DROP TRIGGER IF EXISTS trg_{table_name}_append_only_row_guard "
            f"ON {table_name}"
        )
    )


def _assert_table_empty(table_name: str) -> None:
    op.execute(
        sa.text(
            f"""
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM {table_name} LIMIT 1) THEN
                    RAISE EXCEPTION 'downgrade blocked: table {table_name} contains data';
                END IF;
            END
            $$;
            """
        )
    )


def upgrade() -> None:
    op.execute(sa.text("CREATE EXTENSION IF NOT EXISTS btree_gist"))

    op.create_table(
        "rate_catalog_entries",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("scope_type", sa.String(length=16), nullable=False),
        sa.Column("project_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("rate_key", sa.String(length=255), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("item_type", sa.String(length=64), nullable=False),
        sa.Column("per_unit", sa.String(length=64), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("amount", sa.Numeric(18, 6), nullable=False),
        sa.Column("effective_from", sa.Date(), nullable=False),
        sa.Column("effective_to", sa.Date(), nullable=True),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "scope_type IN ('global', 'project')",
            name="ck_rate_catalog_entries_scope_type",
        ),
        sa.CheckConstraint(
            "(scope_type = 'global' AND project_id IS NULL) OR "
            "(scope_type = 'project' AND project_id IS NOT NULL)",
            name="ck_rate_catalog_entries_scope_project_id",
        ),
        sa.CheckConstraint(
            "effective_to IS NULL OR effective_to > effective_from",
            name="ck_rate_catalog_entries_effective_window",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(metadata_json) = 'object'",
            name="ck_rate_catalog_entries_metadata_json_object",
        ),
        sa.CheckConstraint(
            f"checksum_sha256 ~ '{CHECKSUM_PATTERN}'",
            name="ck_rate_catalog_entries_checksum_sha256",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_rate_catalog_entries_project_id_projects",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_rate_catalog_entries"),
    )
    op.create_index(
        "ix_rate_catalog_entries_lookup",
        "rate_catalog_entries",
        [
            "scope_type",
            "project_id",
            "rate_key",
            "item_type",
            "per_unit",
            "currency",
            "effective_from",
        ],
        unique=False,
    )
    op.execute(
        sa.text(
            """
            ALTER TABLE rate_catalog_entries
            ADD CONSTRAINT ex_rate_catalog_entries_global_no_overlap
            EXCLUDE USING gist (
                rate_key WITH =,
                item_type WITH =,
                per_unit WITH =,
                currency WITH =,
                daterange(effective_from, effective_to, '[)') WITH &&
            )
            WHERE (scope_type = 'global' AND project_id IS NULL)
            """
        )
    )
    op.execute(
        sa.text(
            """
            ALTER TABLE rate_catalog_entries
            ADD CONSTRAINT ex_rate_catalog_entries_project_no_overlap
            EXCLUDE USING gist (
                project_id WITH =,
                rate_key WITH =,
                item_type WITH =,
                per_unit WITH =,
                currency WITH =,
                daterange(effective_from, effective_to, '[)') WITH &&
            )
            WHERE (scope_type = 'project' AND project_id IS NOT NULL)
            """
        )
    )

    op.create_table(
        "material_catalog_entries",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("scope_type", sa.String(length=16), nullable=False),
        sa.Column("project_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("material_key", sa.String(length=255), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("unit", sa.String(length=64), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("unit_cost", sa.Numeric(18, 6), nullable=False),
        sa.Column("effective_from", sa.Date(), nullable=False),
        sa.Column("effective_to", sa.Date(), nullable=True),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "scope_type IN ('global', 'project')",
            name="ck_material_catalog_entries_scope_type",
        ),
        sa.CheckConstraint(
            "(scope_type = 'global' AND project_id IS NULL) OR "
            "(scope_type = 'project' AND project_id IS NOT NULL)",
            name="ck_material_catalog_entries_scope_project_id",
        ),
        sa.CheckConstraint(
            "effective_to IS NULL OR effective_to > effective_from",
            name="ck_material_catalog_entries_effective_window",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(metadata_json) = 'object'",
            name="ck_material_catalog_entries_metadata_json_object",
        ),
        sa.CheckConstraint(
            f"checksum_sha256 ~ '{CHECKSUM_PATTERN}'",
            name="ck_material_catalog_entries_checksum_sha256",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_material_catalog_entries_project_id_projects",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_material_catalog_entries"),
    )
    op.create_index(
        "ix_material_catalog_entries_lookup",
        "material_catalog_entries",
        ["scope_type", "project_id", "material_key", "unit", "currency", "effective_from"],
        unique=False,
    )
    op.execute(
        sa.text(
            """
            ALTER TABLE material_catalog_entries
            ADD CONSTRAINT ex_material_catalog_entries_global_no_overlap
            EXCLUDE USING gist (
                material_key WITH =,
                unit WITH =,
                currency WITH =,
                daterange(effective_from, effective_to, '[)') WITH &&
            )
            WHERE (scope_type = 'global' AND project_id IS NULL)
            """
        )
    )
    op.execute(
        sa.text(
            """
            ALTER TABLE material_catalog_entries
            ADD CONSTRAINT ex_material_catalog_entries_project_no_overlap
            EXCLUDE USING gist (
                project_id WITH =,
                material_key WITH =,
                unit WITH =,
                currency WITH =,
                daterange(effective_from, effective_to, '[)') WITH &&
            )
            WHERE (scope_type = 'project' AND project_id IS NOT NULL)
            """
        )
    )

    op.create_table(
        "formula_definitions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("scope_type", sa.String(length=16), nullable=False),
        sa.Column("project_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("formula_id", sa.String(length=255), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("dsl_version", sa.String(length=32), nullable=False),
        sa.Column("output_key", sa.String(length=255), nullable=False),
        sa.Column("output_contract_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("declared_inputs_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("expression_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("rounding_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "scope_type IN ('global', 'project')",
            name="ck_formula_definitions_scope_type",
        ),
        sa.CheckConstraint(
            "(scope_type = 'global' AND project_id IS NULL) OR "
            "(scope_type = 'project' AND project_id IS NOT NULL)",
            name="ck_formula_definitions_scope_project_id",
        ),
        sa.CheckConstraint(
            "version > 0",
            name="ck_formula_definitions_version_positive",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(output_contract_json) = 'object'",
            name="ck_formula_definitions_output_contract_json_object",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(declared_inputs_json) = 'array'",
            name="ck_formula_definitions_declared_inputs_json_array",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(expression_json) = 'object'",
            name="ck_formula_definitions_expression_json_object",
        ),
        sa.CheckConstraint(
            "rounding_json IS NULL OR jsonb_typeof(rounding_json) = 'object'",
            name="ck_formula_definitions_rounding_json_object",
        ),
        sa.CheckConstraint(
            f"checksum_sha256 ~ '{CHECKSUM_PATTERN}'",
            name="ck_formula_definitions_checksum_sha256",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_formula_definitions_project_id_projects",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_formula_definitions"),
    )
    op.create_index(
        "ix_formula_definitions_lookup",
        "formula_definitions",
        ["scope_type", "project_id", "formula_id", "version"],
        unique=False,
    )
    op.create_index(
        "uq_formula_definitions_global_version",
        "formula_definitions",
        ["formula_id", "version"],
        unique=True,
        postgresql_where=sa.text("scope_type = 'global' AND project_id IS NULL"),
    )
    op.create_index(
        "uq_formula_definitions_project_version",
        "formula_definitions",
        ["project_id", "formula_id", "version"],
        unique=True,
        postgresql_where=sa.text("scope_type = 'project' AND project_id IS NOT NULL"),
    )
    op.create_index(
        "uq_formula_definitions_global_checksum",
        "formula_definitions",
        ["formula_id", "checksum_sha256"],
        unique=True,
        postgresql_where=sa.text("scope_type = 'global' AND project_id IS NULL"),
    )
    op.create_index(
        "uq_formula_definitions_project_checksum",
        "formula_definitions",
        ["project_id", "formula_id", "checksum_sha256"],
        unique=True,
        postgresql_where=sa.text("scope_type = 'project' AND project_id IS NOT NULL"),
    )

    op.create_table(
        "rate_catalog_entry_supersessions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("predecessor_rate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("successor_rate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "predecessor_rate_id <> successor_rate_id",
            name="ck_rate_catalog_entry_supersessions_distinct_nodes",
        ),
        sa.ForeignKeyConstraint(
            ["predecessor_rate_id"],
            ["rate_catalog_entries.id"],
            name="fk_rate_catalog_entry_supersessions_predecessor_rate_id",
        ),
        sa.ForeignKeyConstraint(
            ["successor_rate_id"],
            ["rate_catalog_entries.id"],
            name="fk_rate_catalog_entry_supersessions_successor_rate_id",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_rate_catalog_entry_supersessions"),
        sa.UniqueConstraint(
            "predecessor_rate_id",
            name="uq_rate_catalog_entry_supersessions_predecessor_rate_id",
        ),
        sa.UniqueConstraint(
            "successor_rate_id",
            name="uq_rate_catalog_entry_supersessions_successor_rate_id",
        ),
    )

    op.create_table(
        "material_catalog_entry_supersessions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("predecessor_material_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("successor_material_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "predecessor_material_id <> successor_material_id",
            name="ck_material_catalog_entry_supersessions_distinct_nodes",
        ),
        sa.ForeignKeyConstraint(
            ["predecessor_material_id"],
            ["material_catalog_entries.id"],
            name="fk_material_catalog_entry_supersessions_predecessor_material_id",
        ),
        sa.ForeignKeyConstraint(
            ["successor_material_id"],
            ["material_catalog_entries.id"],
            name="fk_material_catalog_entry_supersessions_successor_material_id",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_material_catalog_entry_supersessions"),
        sa.UniqueConstraint(
            "predecessor_material_id",
            name="uq_material_catalog_entry_supersessions_predecessor_material_id",
        ),
        sa.UniqueConstraint(
            "successor_material_id",
            name="uq_material_catalog_entry_supersessions_successor_material_id",
        ),
    )

    op.create_table(
        "formula_definition_supersessions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("predecessor_formula_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("successor_formula_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "predecessor_formula_id <> successor_formula_id",
            name="ck_formula_definition_supersessions_distinct_nodes",
        ),
        sa.ForeignKeyConstraint(
            ["predecessor_formula_id"],
            ["formula_definitions.id"],
            name="fk_formula_definition_supersessions_predecessor_formula_id",
        ),
        sa.ForeignKeyConstraint(
            ["successor_formula_id"],
            ["formula_definitions.id"],
            name="fk_formula_definition_supersessions_successor_formula_id",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_formula_definition_supersessions"),
        sa.UniqueConstraint(
            "predecessor_formula_id",
            name="uq_formula_definition_supersessions_predecessor_formula_id",
        ),
        sa.UniqueConstraint(
            "successor_formula_id",
            name="uq_formula_definition_supersessions_successor_formula_id",
        ),
    )

    op.execute(
        sa.text(
            """
            CREATE FUNCTION validate_rate_catalog_entry_supersession()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            DECLARE
                predecessor rate_catalog_entries%ROWTYPE;
                successor rate_catalog_entries%ROWTYPE;
            BEGIN
                SELECT * INTO predecessor
                FROM rate_catalog_entries
                WHERE id = NEW.predecessor_rate_id;

                SELECT * INTO successor
                FROM rate_catalog_entries
                WHERE id = NEW.successor_rate_id;

                IF predecessor.scope_type <> successor.scope_type
                    OR predecessor.project_id IS DISTINCT FROM successor.project_id
                    OR predecessor.rate_key <> successor.rate_key
                    OR predecessor.item_type <> successor.item_type
                    OR predecessor.per_unit <> successor.per_unit
                    OR predecessor.currency <> successor.currency THEN
                    RAISE EXCEPTION 'rate supersession must preserve rate key and scope';
                END IF;

                RETURN NEW;
            END;
            $$
            """
        )
    )
    op.execute(
        sa.text(
            """
            CREATE FUNCTION validate_material_catalog_entry_supersession()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            DECLARE
                predecessor material_catalog_entries%ROWTYPE;
                successor material_catalog_entries%ROWTYPE;
            BEGIN
                SELECT * INTO predecessor
                FROM material_catalog_entries
                WHERE id = NEW.predecessor_material_id;

                SELECT * INTO successor
                FROM material_catalog_entries
                WHERE id = NEW.successor_material_id;

                IF predecessor.scope_type <> successor.scope_type
                    OR predecessor.project_id IS DISTINCT FROM successor.project_id
                    OR predecessor.material_key <> successor.material_key
                    OR predecessor.unit <> successor.unit
                    OR predecessor.currency <> successor.currency THEN
                    RAISE EXCEPTION 'material supersession must preserve material key and scope';
                END IF;

                RETURN NEW;
            END;
            $$
            """
        )
    )
    op.execute(
        sa.text(
            """
            CREATE FUNCTION validate_formula_definition_supersession()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            DECLARE
                predecessor formula_definitions%ROWTYPE;
                successor formula_definitions%ROWTYPE;
            BEGIN
                SELECT * INTO predecessor
                FROM formula_definitions
                WHERE id = NEW.predecessor_formula_id;

                SELECT * INTO successor
                FROM formula_definitions
                WHERE id = NEW.successor_formula_id;

                IF predecessor.scope_type <> successor.scope_type
                    OR predecessor.project_id IS DISTINCT FROM successor.project_id
                    OR predecessor.formula_id <> successor.formula_id THEN
                    RAISE EXCEPTION 'formula supersession must preserve formula key and scope';
                END IF;

                RETURN NEW;
            END;
            $$
            """
        )
    )

    op.execute(
        sa.text(
            """
            CREATE TRIGGER trg_rate_catalog_entry_supersessions_validate_lineage
            BEFORE INSERT ON rate_catalog_entry_supersessions
            FOR EACH ROW
            EXECUTE FUNCTION validate_rate_catalog_entry_supersession()
            """
        )
    )
    op.execute(
        sa.text(
            """
            CREATE TRIGGER trg_material_catalog_entry_supersessions_validate_lineage
            BEFORE INSERT ON material_catalog_entry_supersessions
            FOR EACH ROW
            EXECUTE FUNCTION validate_material_catalog_entry_supersession()
            """
        )
    )
    op.execute(
        sa.text(
            """
            CREATE TRIGGER trg_formula_definition_supersessions_validate_lineage
            BEFORE INSERT ON formula_definition_supersessions
            FOR EACH ROW
            EXECUTE FUNCTION validate_formula_definition_supersession()
            """
        )
    )

    for table_name in (
        "rate_catalog_entries",
        "material_catalog_entries",
        "formula_definitions",
        "rate_catalog_entry_supersessions",
        "material_catalog_entry_supersessions",
        "formula_definition_supersessions",
    ):
        _attach_append_only_guards(table_name)


def downgrade() -> None:
    for table_name in (
        "formula_definition_supersessions",
        "material_catalog_entry_supersessions",
        "rate_catalog_entry_supersessions",
        "formula_definitions",
        "material_catalog_entries",
        "rate_catalog_entries",
    ):
        _assert_table_empty(table_name)

    for table_name in (
        "formula_definition_supersessions",
        "material_catalog_entry_supersessions",
        "rate_catalog_entry_supersessions",
        "formula_definitions",
        "material_catalog_entries",
        "rate_catalog_entries",
    ):
        _detach_append_only_guards(table_name)

    op.execute(
        sa.text(
            "DROP TRIGGER IF EXISTS "
            "trg_formula_definition_supersessions_validate_lineage "
            "ON formula_definition_supersessions"
        )
    )
    op.execute(
        sa.text(
            "DROP TRIGGER IF EXISTS "
            "trg_material_catalog_entry_supersessions_validate_lineage "
            "ON material_catalog_entry_supersessions"
        )
    )
    op.execute(
        sa.text(
            "DROP TRIGGER IF EXISTS "
            "trg_rate_catalog_entry_supersessions_validate_lineage "
            "ON rate_catalog_entry_supersessions"
        )
    )

    op.execute(sa.text("DROP FUNCTION IF EXISTS validate_formula_definition_supersession()"))
    op.execute(sa.text("DROP FUNCTION IF EXISTS validate_material_catalog_entry_supersession()"))
    op.execute(sa.text("DROP FUNCTION IF EXISTS validate_rate_catalog_entry_supersession()"))

    op.drop_table("formula_definition_supersessions")
    op.drop_table("material_catalog_entry_supersessions")
    op.drop_table("rate_catalog_entry_supersessions")
    op.drop_index("uq_formula_definitions_project_checksum", table_name="formula_definitions")
    op.drop_index("uq_formula_definitions_global_checksum", table_name="formula_definitions")
    op.drop_index("uq_formula_definitions_project_version", table_name="formula_definitions")
    op.drop_index("uq_formula_definitions_global_version", table_name="formula_definitions")
    op.drop_index("ix_formula_definitions_lookup", table_name="formula_definitions")
    op.drop_table("formula_definitions")
    op.drop_index("ix_material_catalog_entries_lookup", table_name="material_catalog_entries")
    op.drop_table("material_catalog_entries")
    op.drop_index("ix_rate_catalog_entries_lookup", table_name="rate_catalog_entries")
    op.drop_table("rate_catalog_entries")
