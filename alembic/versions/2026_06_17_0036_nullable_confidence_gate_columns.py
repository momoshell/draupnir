"""Make vestigial confidence/gate columns nullable (Path B 5b).

The validation pipeline no longer derives or persists ``confidence_score`` /
``effective_confidence`` / ``review_state`` / ``quantity_gate``; those NULLs flow
through the quantity/estimate lineage. This relaxes the affected columns to
nullable so inserts can omit them. ``trusted_totals`` stays NOT NULL until 5c;
the columns themselves are dropped in Path B stage 6 (#491).

Revision ID: 2026_06_17_0036
Revises: 2026_06_17_0035
Create Date: 2026-06-17 00:36:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_06_17_0036"
down_revision: str | None = "2026_06_17_0035"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

# (table, column, type) for columns relaxed to nullable.
_STR32 = sa.String(32)
_FLOAT = sa.Float()
_COLUMNS: tuple[tuple[str, str, sa.types.TypeEngine[object]], ...] = (
    ("adapter_run_outputs", "confidence_score", _FLOAT),
    ("drawing_revisions", "confidence_score", _FLOAT),
    ("drawing_revisions", "review_state", _STR32),
    ("revision_entities", "confidence_score", _FLOAT),
    ("validation_reports", "review_state", _STR32),
    ("validation_reports", "quantity_gate", _STR32),
    ("validation_reports", "effective_confidence", _FLOAT),
    ("quantity_takeoffs", "review_state", _STR32),
    ("quantity_takeoffs", "quantity_gate", _STR32),
    ("quantity_items", "review_state", _STR32),
    ("quantity_items", "quantity_gate", _STR32),
    ("estimate_job_inputs", "quantity_gate", _STR32),
    ("estimate_versions", "quantity_gate", _STR32),
)


def upgrade() -> None:
    for table, column, column_type in _COLUMNS:
        op.alter_column(table, column, existing_type=column_type, nullable=True)


def downgrade() -> None:
    # Reinstating NOT NULL requires no NULL rows to exist (the columns are
    # vestigial; only rows created after the upgrade carry NULLs).
    for table, column, column_type in _COLUMNS:
        op.alter_column(table, column, existing_type=column_type, nullable=False)
