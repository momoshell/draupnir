"""add job attempt lease fencing

Revision ID: 2026_05_11_0010
Revises: 2026_05_10_0009
Create Date: 2026-05-11 12:30:00
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_05_11_0010"
down_revision: str | None = "2026_05_10_0009"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    """Add persisted worker-attempt lease fencing to jobs."""

    op.add_column(
        "jobs",
        sa.Column(
            "attempt_token",
            sa.Uuid(),
            nullable=True,
            comment="Current running attempt ownership token fence",
        ),
    )
    op.add_column(
        "jobs",
        sa.Column(
            "attempt_lease_expires_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="Current running attempt lease expiry used to reclaim stale deliveries",
        ),
    )


def downgrade() -> None:
    """Drop persisted worker-attempt lease fencing from jobs."""

    op.drop_column("jobs", "attempt_lease_expires_at")
    op.drop_column("jobs", "attempt_token")
