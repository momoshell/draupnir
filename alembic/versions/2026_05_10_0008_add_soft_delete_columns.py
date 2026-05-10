"""add soft-delete columns for retention

Revision ID: 2026_05_10_0008
Revises: 2026_05_10_0007
Create Date: 2026-05-10 12:20:00
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_05_10_0008"
down_revision: str | None = "2026_05_10_0007"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    """Add nullable soft-delete timestamps for retained resources."""
    op.add_column(
        "projects",
        sa.Column(
            "deleted_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="Soft deletion timestamp for retention workflows",
        ),
    )
    op.add_column(
        "files",
        sa.Column(
            "deleted_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="Soft deletion timestamp for retention workflows",
        ),
    )


def downgrade() -> None:
    """Remove soft-delete timestamps only when no retained markers exist."""
    bind = op.get_bind()
    project_markers_exist = bool(
        bind.execute(
            sa.text("SELECT EXISTS (SELECT 1 FROM projects WHERE deleted_at IS NOT NULL)")
        ).scalar()
    )
    file_markers_exist = bool(
        bind.execute(
            sa.text("SELECT EXISTS (SELECT 1 FROM files WHERE deleted_at IS NOT NULL)")
        ).scalar()
    )
    if project_markers_exist or file_markers_exist:
        raise RuntimeError(
            "Manual data-preserving rollback is required before dropping soft-delete markers."
        )

    op.drop_column("files", "deleted_at")
    op.drop_column("projects", "deleted_at")
