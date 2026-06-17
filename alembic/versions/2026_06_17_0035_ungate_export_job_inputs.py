"""Ungate quantity/estimate exports (Path B 4).

Relaxes the ``ck_export_job_inputs_quantity_lineage`` CHECK matrix so quantity
and estimate exports (``quantity_csv`` / ``estimate_csv`` / ``estimate_pdf``) no
longer require an ``allowed`` gate + ``trusted_totals``. The export_kind <-> lineage
shape is preserved (quantity/estimate exports carry a takeoff; revision/dxf exports
do not). The ``quantity_gate`` / ``trusted_totals`` columns remain (dropped in Path B
stage 6).

Revision ID: 2026_06_17_0035
Revises: 2026_06_16_0034
Create Date: 2026-06-17 00:35:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_06_17_0035"
down_revision: str | None = "2026_06_16_0034"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

_CONSTRAINT = "ck_export_job_inputs_quantity_lineage"

# Ungated: only the export_kind <-> takeoff lineage shape is enforced.
_UNGATED_LINEAGE = (
    "(export_kind IN ('quantity_csv', 'estimate_csv', 'estimate_pdf') "
    "AND quantity_takeoff_id IS NOT NULL) OR "
    "(export_kind IN ('revision_json', 'dxf', 'revised_dxf') "
    "AND quantity_takeoff_id IS NULL "
    "AND quantity_gate IS NULL AND trusted_totals IS NULL)"
)

# Gated: original matrix requiring allowed gate + trusted totals for quantity exports.
_GATED_LINEAGE = (
    "(export_kind = 'quantity_csv' AND quantity_takeoff_id IS NOT NULL "
    "AND quantity_gate = 'allowed' AND trusted_totals IS TRUE) OR "
    "(export_kind IN ('estimate_csv', 'estimate_pdf') AND quantity_takeoff_id IS NOT NULL "
    "AND quantity_gate = 'allowed' AND trusted_totals IS TRUE) OR "
    "(export_kind IN ('revision_json', 'dxf', 'revised_dxf') "
    "AND quantity_takeoff_id IS NULL "
    "AND quantity_gate IS NULL AND trusted_totals IS NULL)"
)


def _assert_downgrade_safe() -> None:
    bind = op.get_bind()
    bind.execute(sa.text("LOCK TABLE export_job_inputs IN ACCESS EXCLUSIVE MODE"))
    ungated_row_exists = bind.execute(
        sa.text(
            """
            SELECT 1
            FROM export_job_inputs
            WHERE export_kind IN ('quantity_csv', 'estimate_csv', 'estimate_pdf')
              AND (quantity_gate <> 'allowed' OR trusted_totals IS NOT TRUE)
            LIMIT 1
            """
        )
    ).scalar()
    if ungated_row_exists is not None:
        raise RuntimeError(
            "Cannot downgrade: persisted quantity/estimate export_job_inputs rows with a "
            "non-allowed gate or untrusted totals are present"
        )


def upgrade() -> None:
    op.drop_constraint(_CONSTRAINT, "export_job_inputs", type_="check")
    op.create_check_constraint(_CONSTRAINT, "export_job_inputs", _UNGATED_LINEAGE)


def downgrade() -> None:
    _assert_downgrade_safe()
    op.drop_constraint(_CONSTRAINT, "export_job_inputs", type_="check")
    op.create_check_constraint(_CONSTRAINT, "export_job_inputs", _GATED_LINEAGE)
