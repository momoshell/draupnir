"""Add materialization_tier column to revision_entities (#831 PR-1).

Purely additive foundation for the entity-reduction epic: adds a non-null
``materialization_tier`` column (``primary`` | ``fill_noise``) with a server-side default of
``'primary'``. Every existing and newly materialized row is ``primary`` until a later PR adds
the predicate that stamps ``fill_noise`` — so this migration backfills via the column default
rather than a row UPDATE, which keeps it compatible with the append-only row-guard trigger on
``revision_entities``.

Revision ID: 2026_07_01_0042
Revises: 2026_06_23_0041
Create Date: 2026-07-01 00:00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_07_01_0042"
down_revision: str | None = "2026_06_23_0041"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

_TABLE_NAME = "revision_entities"
_COLUMN_NAME = "materialization_tier"
_CHECK_CONSTRAINT_NAME = "ck_revision_entities_materialization_tier"
_TIER_PRIMARY = "primary"
_TIER_FILL_NOISE = "fill_noise"


def upgrade() -> None:
    op.add_column(
        _TABLE_NAME,
        sa.Column(
            _COLUMN_NAME,
            sa.String(length=16),
            nullable=False,
            server_default=_TIER_PRIMARY,
        ),
    )
    op.create_check_constraint(
        _CHECK_CONSTRAINT_NAME,
        _TABLE_NAME,
        f"{_COLUMN_NAME} IN ('{_TIER_PRIMARY}', '{_TIER_FILL_NOISE}')",
    )


def downgrade() -> None:
    op.drop_constraint(_CHECK_CONSTRAINT_NAME, _TABLE_NAME, type_="check")
    op.drop_column(_TABLE_NAME, _COLUMN_NAME)
