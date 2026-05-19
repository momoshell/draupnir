"""repair quantity gate contract constraints

Revision ID: 2026_05_18_0023
Revises: 2026_05_18_0022
Create Date: 2026-05-18 23:30:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "2026_05_18_0023"
down_revision = "2026_05_18_0022"
branch_labels = None
depends_on = None


def _constraint_exists(table_name: str, constraint_name: str) -> bool:
    bind = op.get_bind()
    return (
        bind.execute(
            sa.text(
                """
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = to_regclass(:table_name)
                  AND conname = :constraint_name
                """
            ),
            {"table_name": table_name, "constraint_name": constraint_name},
        ).scalar_one_or_none()
        is not None
    )


def upgrade() -> None:
    """Restore the quantity item parent-gate contract when older DBs lack it."""

    if not _constraint_exists(
        "quantity_takeoffs",
        "uq_quantity_takeoffs_id_project_rev_gate",
    ):
        op.create_unique_constraint(
            "uq_quantity_takeoffs_id_project_rev_gate",
            "quantity_takeoffs",
            ["id", "project_id", "drawing_revision_id", "quantity_gate"],
        )

    if not _constraint_exists(
        "quantity_items",
        "fk_quantity_items_takeoff_gate_contract",
    ):
        op.create_foreign_key(
            "fk_quantity_items_takeoff_gate_contract",
            "quantity_items",
            "quantity_takeoffs",
            ["quantity_takeoff_id", "project_id", "drawing_revision_id", "quantity_gate"],
            ["id", "project_id", "drawing_revision_id", "quantity_gate"],
            ondelete="RESTRICT",
        )


def downgrade() -> None:
    """Leave constraints owned by 2026_05_15_0017 for its downgrade step."""
