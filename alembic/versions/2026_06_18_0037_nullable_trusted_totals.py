"""Make vestigial ``trusted_totals`` columns nullable (Path B 5c).

The quantity/estimate/export lineage no longer derives or writes
``trusted_totals``; those values now flow through as NULL. This relaxes the
remaining NOT NULL ``trusted_totals`` columns (``quantity_takeoffs``,
``estimate_job_inputs``, ``estimate_versions``) to nullable so inserts can omit
them (``export_job_inputs.trusted_totals`` is already nullable). The columns
themselves are dropped in Path B stage 6 (#491).

Revision ID: 2026_06_18_0037
Revises: 2026_06_17_0036
Create Date: 2026-06-18 00:37:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_06_18_0037"
down_revision: str | None = "2026_06_17_0036"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

# (table, column, type) for ``trusted_totals`` columns relaxed to nullable.
_BOOL = sa.Boolean()
_COLUMNS: tuple[tuple[str, str, sa.types.TypeEngine[object]], ...] = (
    ("quantity_takeoffs", "trusted_totals", _BOOL),
    ("estimate_job_inputs", "trusted_totals", _BOOL),
    ("estimate_versions", "trusted_totals", _BOOL),
)


def upgrade() -> None:
    for table, column, column_type in _COLUMNS:
        op.alter_column(table, column, existing_type=column_type, nullable=True)


def downgrade() -> None:
    # Reinstating NOT NULL requires no NULL rows to exist (the column is
    # vestigial; only rows created after the upgrade carry NULLs).
    for table, column, column_type in _COLUMNS:
        op.alter_column(table, column, existing_type=column_type, nullable=False)
