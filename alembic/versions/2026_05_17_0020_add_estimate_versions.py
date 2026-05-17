"""add estimate versions

Revision ID: 2026_05_17_0020
Revises: 2026_05_16_0019
Create Date: 2026-05-17 21:10:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "2026_05_17_0020"
down_revision = "2026_05_16_0019"
branch_labels = None
depends_on = None


def _attach_append_only_triggers(table_name: str) -> None:
    op.execute(
        sa.text(
            f"""
            CREATE TRIGGER trg_append_only_row_guard
            BEFORE UPDATE OR DELETE ON \"{table_name}\"
            FOR EACH ROW
            EXECUTE FUNCTION enforce_append_only_lineage_row()
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            CREATE TRIGGER trg_append_only_truncate_guard
            BEFORE TRUNCATE ON \"{table_name}\"
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


def _assert_estimate_versions_empty() -> None:
    bind = op.get_bind()
    row_count = bind.execute(sa.text('SELECT COUNT(*) FROM "estimate_versions"')).scalar_one()
    if row_count:
        raise RuntimeError(
            "Refusing to downgrade migration 2026_05_17_0020 while estimate_versions "
            f"contains {row_count} row(s)."
        )


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_quantity_takeoffs_id_project_rev_gate_trusted",
        "quantity_takeoffs",
        ["id", "project_id", "drawing_revision_id", "quantity_gate", "trusted_totals"],
    )

    op.create_table(
        "estimate_versions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("source_file_id", sa.Uuid(), nullable=False),
        sa.Column("drawing_revision_id", sa.Uuid(), nullable=False),
        sa.Column("quantity_takeoff_id", sa.Uuid(), nullable=False),
        sa.Column("source_job_id", sa.Uuid(), nullable=False),
        sa.Column("quantity_gate", sa.String(length=32), nullable=False),
        sa.Column("trusted_totals", sa.Boolean(), nullable=False),
        sa.Column(
            "currency",
            sa.String(length=3),
            nullable=False,
            server_default=sa.text("'GBP'"),
        ),
        sa.Column("subtotal_amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("tax_amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("total_amount", sa.Numeric(18, 2), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "quantity_gate = 'allowed'",
            name="ck_estimate_versions_quantity_gate_allowed",
        ),
        sa.CheckConstraint(
            "trusted_totals = TRUE",
            name="ck_estimate_versions_trusted_totals_true",
        ),
        sa.CheckConstraint(
            "currency = 'GBP'",
            name="ck_estimate_versions_currency_gbp",
        ),
        sa.CheckConstraint(
            "subtotal_amount::text <> 'NaN' AND subtotal_amount >= 0::numeric",
            name="ck_estimate_versions_subtotal_amount_nonnegative",
        ),
        sa.CheckConstraint(
            "tax_amount::text <> 'NaN' AND tax_amount >= 0::numeric",
            name="ck_estimate_versions_tax_amount_nonnegative",
        ),
        sa.CheckConstraint(
            "total_amount::text <> 'NaN' AND total_amount >= 0::numeric",
            name="ck_estimate_versions_total_amount_nonnegative",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_estimate_versions_project_id_projects",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            name="fk_estimate_versions_source_file_id_project_id_files",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["drawing_revision_id", "project_id", "source_file_id"],
            [
                "drawing_revisions.id",
                "drawing_revisions.project_id",
                "drawing_revisions.source_file_id",
            ],
            name="fk_estimate_versions_revision_lineage",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
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
            name="fk_estimate_versions_takeoff_contract",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_job_id", "project_id", "source_file_id"],
            ["jobs.id", "jobs.project_id", "jobs.file_id"],
            name="fk_estimate_versions_source_job_lineage",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "id",
            "project_id",
            "drawing_revision_id",
            name="uq_estimate_versions_id_project_id_drawing_revision_id",
        ),
        sa.UniqueConstraint(
            "source_job_id",
            name="uq_estimate_versions_source_job_id",
        ),
    )
    op.create_index(
        "ix_estimate_versions_project_id",
        "estimate_versions",
        ["project_id"],
        unique=False,
    )
    op.create_index(
        "ix_estimate_versions_source_file_id",
        "estimate_versions",
        ["source_file_id"],
        unique=False,
    )
    op.create_index(
        "ix_estimate_versions_quantity_takeoff_id",
        "estimate_versions",
        ["quantity_takeoff_id"],
        unique=False,
    )
    op.create_index(
        "ix_estimate_versions_drawing_revision_id",
        "estimate_versions",
        ["drawing_revision_id"],
        unique=False,
    )

    _attach_append_only_triggers("estimate_versions")


def downgrade() -> None:
    _assert_estimate_versions_empty()

    _drop_append_only_triggers("estimate_versions")

    op.drop_index("ix_estimate_versions_drawing_revision_id", table_name="estimate_versions")
    op.drop_index("ix_estimate_versions_quantity_takeoff_id", table_name="estimate_versions")
    op.drop_index("ix_estimate_versions_source_file_id", table_name="estimate_versions")
    op.drop_index("ix_estimate_versions_project_id", table_name="estimate_versions")
    op.drop_table("estimate_versions")

    op.drop_constraint(
        "uq_quantity_takeoffs_id_project_rev_gate_trusted",
        "quantity_takeoffs",
        type_="unique",
    )
