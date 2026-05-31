"""add generated artifact lineage foreign keys

Revision ID: 2026_05_31_0026
Revises: 2026_05_31_0025
Create Date: 2026-05-31 00:26:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "2026_05_31_0026"
down_revision = "2026_05_31_0025"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_drawing_revisions_id_project_changeset",
        "drawing_revisions",
        ["id", "project_id", "changeset_id"],
    )

    op.add_column(
        "generated_artifacts",
        sa.Column("changeset_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "generated_artifacts",
        sa.Column("quantity_takeoff_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "generated_artifacts",
        sa.Column("estimate_version_id", sa.Uuid(), nullable=True),
    )

    op.create_index(
        "ix_generated_artifacts_changeset_id",
        "generated_artifacts",
        ["changeset_id"],
        unique=False,
    )
    op.create_index(
        "ix_generated_artifacts_quantity_takeoff_id",
        "generated_artifacts",
        ["quantity_takeoff_id"],
        unique=False,
    )
    op.create_index(
        "ix_generated_artifacts_estimate_version_id",
        "generated_artifacts",
        ["estimate_version_id"],
        unique=False,
    )

    op.create_foreign_key(
        "fk_generated_artifacts_changeset",
        "generated_artifacts",
        "drawing_revisions",
        ["drawing_revision_id", "project_id", "changeset_id"],
        ["id", "project_id", "changeset_id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_generated_artifacts_takeoff",
        "generated_artifacts",
        "quantity_takeoffs",
        ["quantity_takeoff_id", "project_id", "drawing_revision_id"],
        ["id", "project_id", "drawing_revision_id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_generated_artifacts_estimate",
        "generated_artifacts",
        "estimate_versions",
        [
            "estimate_version_id",
            "project_id",
            "drawing_revision_id",
            "quantity_takeoff_id",
        ],
        ["id", "project_id", "drawing_revision_id", "quantity_takeoff_id"],
        ondelete="RESTRICT",
    )

    op.create_check_constraint(
        "ck_generated_artifacts_changeset_revision",
        "generated_artifacts",
        "changeset_id IS NULL OR drawing_revision_id IS NOT NULL",
    )
    op.create_check_constraint(
        "ck_generated_artifacts_takeoff_revision",
        "generated_artifacts",
        "quantity_takeoff_id IS NULL OR drawing_revision_id IS NOT NULL",
    )
    op.create_check_constraint(
        "ck_generated_artifacts_estimate_lineage",
        "generated_artifacts",
        "estimate_version_id IS NULL OR "
        "(drawing_revision_id IS NOT NULL AND quantity_takeoff_id IS NOT NULL)",
    )


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(sa.text("LOCK TABLE generated_artifacts IN ACCESS EXCLUSIVE MODE"))
    anchored_rows = bind.scalar(
        sa.text(
            """
            SELECT count(*)
            FROM generated_artifacts
            WHERE changeset_id IS NOT NULL
               OR quantity_takeoff_id IS NOT NULL
               OR estimate_version_id IS NOT NULL
            """
        )
    )
    if anchored_rows:
        raise RuntimeError(
            "Cannot downgrade: generated_artifacts has non-null typed lineage anchors."
        )

    op.drop_constraint(
        "ck_generated_artifacts_estimate_lineage",
        "generated_artifacts",
        type_="check",
    )
    op.drop_constraint(
        "ck_generated_artifacts_takeoff_revision",
        "generated_artifacts",
        type_="check",
    )
    op.drop_constraint(
        "ck_generated_artifacts_changeset_revision",
        "generated_artifacts",
        type_="check",
    )
    op.drop_constraint(
        "fk_generated_artifacts_estimate",
        "generated_artifacts",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_generated_artifacts_takeoff",
        "generated_artifacts",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_generated_artifacts_changeset",
        "generated_artifacts",
        type_="foreignkey",
    )
    op.drop_constraint(
        "uq_drawing_revisions_id_project_changeset",
        "drawing_revisions",
        type_="unique",
    )

    op.drop_index("ix_generated_artifacts_estimate_version_id", table_name="generated_artifacts")
    op.drop_index("ix_generated_artifacts_quantity_takeoff_id", table_name="generated_artifacts")
    op.drop_index("ix_generated_artifacts_changeset_id", table_name="generated_artifacts")

    op.drop_column("generated_artifacts", "estimate_version_id")
    op.drop_column("generated_artifacts", "quantity_takeoff_id")
    op.drop_column("generated_artifacts", "changeset_id")
