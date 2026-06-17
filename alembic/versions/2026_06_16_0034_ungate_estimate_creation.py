"""Ungate estimate creation (Path B 3).

Drops the gate CHECK constraints that required estimates to originate from an
``allowed`` + ``trusted_totals`` takeoff, on both the estimate request table
(``estimate_job_inputs``) and the persisted output table (``estimate_versions``).
Estimates are now created from any takeoff regardless of gate. The
``quantity_gate`` / ``trusted_totals`` columns remain (recorded as informational
lineage; dropped in Path B stage 6).

Revision ID: 2026_06_16_0034
Revises: 2026_06_14_0033
Create Date: 2026-06-16 00:34:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_06_16_0034"
down_revision: str | None = "2026_06_14_0033"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


# (table, gate-constraint name, trusted-constraint name)
_GATED_TABLES = (
    (
        "estimate_job_inputs",
        "ck_estimate_job_inputs_quantity_gate_allowed",
        "ck_estimate_job_inputs_trusted_totals_true",
    ),
    (
        "estimate_versions",
        "ck_estimate_versions_quantity_gate_allowed",
        "ck_estimate_versions_trusted_totals_true",
    ),
)


def _assert_downgrade_safe() -> None:
    bind = op.get_bind()
    for table, _gate_name, _trusted_name in _GATED_TABLES:
        bind.execute(sa.text(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"))
        ungated_row_exists = bind.execute(
            sa.text(
                f"""
                SELECT 1
                FROM {table}
                WHERE quantity_gate <> 'allowed' OR trusted_totals IS NOT TRUE
                LIMIT 1
                """
            )
        ).scalar()
        if ungated_row_exists is not None:
            raise RuntimeError(
                f"Cannot downgrade: persisted {table} rows with a non-allowed "
                "gate or untrusted totals are present"
            )


def upgrade() -> None:
    for table, gate_name, trusted_name in _GATED_TABLES:
        op.drop_constraint(gate_name, table, type_="check")
        op.drop_constraint(trusted_name, table, type_="check")


def downgrade() -> None:
    _assert_downgrade_safe()
    for table, gate_name, trusted_name in _GATED_TABLES:
        op.create_check_constraint(gate_name, table, "quantity_gate = 'allowed'")
        op.create_check_constraint(trusted_name, table, "trusted_totals = TRUE")
