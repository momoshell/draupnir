"""Set projects timestamps NOT NULL.

The projects table was created with ``created_at``/``updated_at`` as nullable
columns (only a server_default was set), but the ORM model declares them as
non-Optional ``Mapped[datetime]`` (i.e. NOT NULL). This migration closes that
model/DB drift by enforcing NOT NULL at the database, matching the convention
used by ``TimestampMixin`` and every other timestamped table.

Revision ID: 2026_06_08_0031
Revises: 2026_06_07_0030
Create Date: 2026-06-08 21:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_06_08_0031"
down_revision: str | None = "2026_06_07_0030"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    # Defensive backfill: the columns have carried a server_default since creation, so
    # no NULLs are expected, but guard against any rows inserted via paths that bypassed
    # it before tightening the constraint.
    op.execute(sa.text("UPDATE projects SET created_at = now() WHERE created_at IS NULL"))
    op.execute(sa.text("UPDATE projects SET updated_at = now() WHERE updated_at IS NULL"))
    op.alter_column(
        "projects",
        "created_at",
        existing_type=sa.DateTime(timezone=True),
        existing_server_default=sa.func.now(),
        nullable=False,
    )
    op.alter_column(
        "projects",
        "updated_at",
        existing_type=sa.DateTime(timezone=True),
        existing_server_default=sa.func.now(),
        nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "projects",
        "updated_at",
        existing_type=sa.DateTime(timezone=True),
        existing_server_default=sa.func.now(),
        nullable=True,
    )
    op.alter_column(
        "projects",
        "created_at",
        existing_type=sa.DateTime(timezone=True),
        existing_server_default=sa.func.now(),
        nullable=True,
    )
