"""Add projects table.

Revision ID: 2026_05_01_0001
Revises: baseline
Create Date: 2026-05-01 00:00:00.000000

"""
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_05_01_0001"
down_revision: str | None = "baseline"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Apply migration changes.

    Creates the projects table with UUID primary key and timestamps.
    """
    op.create_table(
        "projects",
        sa.Column(
            "id",
            sa.Uuid(),
            nullable=False,
            comment="Unique project identifier (UUID v4)",
        ),
        sa.Column(
            "name",
            sa.String(length=255),
            nullable=False,
            comment="Project name",
        ),
        sa.Column(
            "description",
            sa.String(length=1024),
            nullable=True,
            comment="Optional project description",
        ),
        sa.Column(
            "default_unit_system",
            sa.String(length=64),
            nullable=True,
            comment="Default unit system (e.g., 'metric', 'imperial')",
        ),
        sa.Column(
            "default_currency",
            sa.String(length=3),
            nullable=True,
            comment="Default currency code (ISO 4217, e.g., 'USD', 'EUR')",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            comment="Project creation timestamp",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            comment="Project last update timestamp",
        ),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    """Revert migration changes.

    Drops the projects table.
    """
    op.drop_table("projects")
