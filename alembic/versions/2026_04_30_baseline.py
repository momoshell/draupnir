"""Baseline migration - initial database setup.

Revision ID: baseline
Revises:
Create Date: 2026-04-30 00:00:00.000000

"""
from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "baseline"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Apply migration changes.

    This is a baseline migration with no schema changes.
    Future migrations will add actual tables.
    """


def downgrade() -> None:
    """Revert migration changes.

    Nothing to revert for baseline.
    """
