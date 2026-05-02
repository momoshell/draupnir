"""add job events

Revision ID: 2026_05_02_0003
Revises: 2026_05_01_0002
Create Date: 2026-05-02 00:03:00
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "2026_05_02_0003"
down_revision: str | None = "2026_05_01_0002"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    """Create persisted job event storage."""
    op.create_table(
        "job_events",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("job_id", sa.UUID(), nullable=False),
        sa.Column("level", sa.String(length=32), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("data_json", sa.JSON(), nullable=True),
        sa.Column("sequence_id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_job_events_job_id_created_at_sequence_id_id",
        "job_events",
        ["job_id", "created_at", "sequence_id", "id"],
        unique=False,
    )


def downgrade() -> None:
    """Drop persisted job event storage."""
    op.drop_index("ix_job_events_job_id_created_at_sequence_id_id", table_name="job_events")
    op.drop_table("job_events")
