"""add jobs enqueue outbox columns

Revision ID: 2026_05_11_0013
Revises: 2026_05_11_0012
Create Date: 2026-05-11 19:15:00
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_05_11_0013"
down_revision: str | None = "2026_05_11_0012"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    """Persist durable enqueue intent state for jobs."""

    op.add_column(
        "jobs",
        sa.Column(
            "enqueue_status",
            sa.String(length=32),
            nullable=True,
            server_default=sa.text("'pending'"),
            comment="Durable enqueue intent state (pending, publishing, published)",
        ),
    )
    op.add_column(
        "jobs",
        sa.Column(
            "enqueue_attempts",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
            comment="Broker publish attempts for the current durable enqueue intent",
        ),
    )
    op.add_column(
        "jobs",
        sa.Column(
            "enqueue_owner_token",
            sa.Uuid(),
            nullable=True,
            comment="Current enqueue publisher ownership token for stranded-intent recovery",
        ),
    )
    op.add_column(
        "jobs",
        sa.Column(
            "enqueue_lease_expires_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="Current enqueue publisher lease expiry used to reclaim stranded intents",
        ),
    )
    op.add_column(
        "jobs",
        sa.Column(
            "enqueue_last_attempted_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="Most recent broker publish attempt timestamp for the current intent",
        ),
    )
    op.add_column(
        "jobs",
        sa.Column(
            "enqueue_published_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="Timestamp when the current durable enqueue intent was last published",
        ),
    )

    op.execute(
        sa.text(
            """
            UPDATE jobs
            SET enqueue_status = CASE
                    WHEN status = 'pending' THEN 'pending'
                    ELSE 'published'
                END,
                enqueue_published_at = CASE
                    WHEN status = 'pending' THEN NULL
                    ELSE created_at
                END,
                enqueue_attempts = 0,
                enqueue_owner_token = NULL,
                enqueue_lease_expires_at = NULL,
                enqueue_last_attempted_at = NULL
            """
        )
    )

    op.alter_column("jobs", "enqueue_status", nullable=False, server_default=None)
    op.alter_column("jobs", "enqueue_attempts", server_default=None)
    op.create_check_constraint(
        "ck_jobs_enqueue_status_valid",
        "jobs",
        "enqueue_status IN ('pending', 'publishing', 'published')",
    )


def downgrade() -> None:
    """Drop durable enqueue intent state from jobs."""

    op.drop_constraint("ck_jobs_enqueue_status_valid", "jobs", type_="check")
    op.drop_column("jobs", "enqueue_published_at")
    op.drop_column("jobs", "enqueue_last_attempted_at")
    op.drop_column("jobs", "enqueue_lease_expires_at")
    op.drop_column("jobs", "enqueue_owner_token")
    op.drop_column("jobs", "enqueue_attempts")
    op.drop_column("jobs", "enqueue_status")
