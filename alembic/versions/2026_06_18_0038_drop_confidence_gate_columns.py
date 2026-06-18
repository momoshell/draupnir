"""Drop confidence/gate columns + dead constraints (Path B 6 / final).

Physically removes the retired columns (no longer read or written after Path B
stages 5a-5c) and their CHECK/FK/UNIQUE constraints:
  confidence_score, effective_confidence, review_state, quantity_gate, trusted_totals.

The composite takeoff-lineage FKs (estimate_job_inputs / estimate_versions /
export_job_inputs) that keyed on quantity_gate/trusted_totals are replaced with
gate-free FKs on (id, project_id, drawing_revision_id). quantity_items already
has a gate-free lineage FK, so its gate FK is simply dropped. The
export_job_inputs quantity-lineage CHECK is recreated without the gate columns.

Revision ID: 2026_06_18_0038
Revises: 2026_06_18_0037
Create Date: 2026-06-18 00:38:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_06_18_0038"
down_revision: str | None = "2026_06_18_0037"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

_REVIEW_STATES = "'approved', 'provisional', 'review_required', 'rejected', 'superseded'"
_GATES = "'allowed', 'allowed_provisional', 'review_gated', 'blocked'"

# (table, column, re-add type) — columns dropped on upgrade, re-added nullable on downgrade.
_DROPPED_COLUMNS: tuple[tuple[str, str, sa.types.TypeEngine[object]], ...] = (
    ("adapter_run_outputs", "confidence_score", sa.Float()),
    ("drawing_revisions", "confidence_score", sa.Float()),
    ("drawing_revisions", "review_state", sa.String(32)),
    ("revision_entities", "confidence_score", sa.Float()),
    ("validation_reports", "effective_confidence", sa.Float()),
    ("validation_reports", "quantity_gate", sa.String(32)),
    ("validation_reports", "review_state", sa.String(32)),
    ("quantity_takeoffs", "quantity_gate", sa.String(32)),
    ("quantity_takeoffs", "review_state", sa.String(32)),
    ("quantity_takeoffs", "trusted_totals", sa.Boolean()),
    ("quantity_items", "quantity_gate", sa.String(32)),
    ("quantity_items", "review_state", sa.String(32)),
    ("estimate_job_inputs", "quantity_gate", sa.String(32)),
    ("estimate_job_inputs", "trusted_totals", sa.Boolean()),
    ("estimate_versions", "quantity_gate", sa.String(32)),
    ("estimate_versions", "trusted_totals", sa.Boolean()),
    ("export_job_inputs", "quantity_gate", sa.String(32)),
    ("export_job_inputs", "trusted_totals", sa.Boolean()),
)

# Composite gate FKs dropped on upgrade (re-created on downgrade).
_GATE_FK_CONTRACTS = (
    (
        "fk_estimate_job_inputs_takeoff_contract",
        "estimate_job_inputs",
        [
            "quantity_takeoff_id",
            "project_id",
            "drawing_revision_id",
            "quantity_gate",
            "trusted_totals",
        ],
        ["id", "project_id", "drawing_revision_id", "quantity_gate", "trusted_totals"],
    ),
    (
        "fk_estimate_versions_takeoff_contract",
        "estimate_versions",
        [
            "quantity_takeoff_id",
            "project_id",
            "drawing_revision_id",
            "quantity_gate",
            "trusted_totals",
        ],
        ["id", "project_id", "drawing_revision_id", "quantity_gate", "trusted_totals"],
    ),
    (
        "fk_export_job_inputs_trusted_quantity_takeoff",
        "export_job_inputs",
        [
            "quantity_takeoff_id",
            "project_id",
            "drawing_revision_id",
            "quantity_gate",
            "trusted_totals",
        ],
        ["id", "project_id", "drawing_revision_id", "quantity_gate", "trusted_totals"],
    ),
)

# Gate-free replacement FKs created on upgrade (dropped on downgrade).
_GATE_FREE_FKS = (
    ("fk_estimate_job_inputs_takeoff_lineage", "estimate_job_inputs"),
    ("fk_estimate_versions_takeoff_lineage", "estimate_versions"),
    ("fk_export_job_inputs_takeoff_lineage", "export_job_inputs"),
)

# Single-column CHECK constraints dropped on upgrade / recreated on downgrade.
_DROPPED_CHECKS: tuple[tuple[str, str, str], ...] = (
    (
        "ck_adapter_outputs_confidence_0_1",
        "adapter_run_outputs",
        "confidence_score >= 0.0 AND confidence_score <= 1.0",
    ),
    (
        "ck_drawing_revisions_conf_0_1",
        "drawing_revisions",
        "confidence_score >= 0.0 AND confidence_score <= 1.0",
    ),
    (
        "ck_drawing_revisions_review_state",
        "drawing_revisions",
        f"review_state IN ({_REVIEW_STATES})",
    ),
    (
        "ck_quantity_items_conflict_gate_review_only",
        "quantity_items",
        "item_kind <> 'conflict' OR quantity_gate IN ('review_gated', 'blocked')",
    ),
    ("ck_quantity_items_quantity_gate_valid", "quantity_items", f"quantity_gate IN ({_GATES})"),
    (
        "ck_quantity_items_review_state_valid",
        "quantity_items",
        f"review_state IN ({_REVIEW_STATES})",
    ),
    (
        "ck_quantity_takeoffs_quantity_gate_valid",
        "quantity_takeoffs",
        f"quantity_gate IN ({_GATES})",
    ),
    (
        "ck_quantity_takeoffs_review_state_valid",
        "quantity_takeoffs",
        f"review_state IN ({_REVIEW_STATES})",
    ),
    (
        "ck_quantity_takeoffs_trusted_totals_allowed_gate",
        "quantity_takeoffs",
        "trusted_totals = FALSE OR quantity_gate = 'allowed'",
    ),
    (
        "ck_validation_reports_conf_0_1",
        "validation_reports",
        "effective_confidence >= 0.0 AND effective_confidence <= 1.0",
    ),
    ("ck_validation_reports_quantity_gate", "validation_reports", f"quantity_gate IN ({_GATES})"),
    (
        "ck_validation_reports_review_state",
        "validation_reports",
        f"review_state IN ({_REVIEW_STATES})",
    ),
)

_GATE_UNIQUES = (
    (
        "uq_quantity_takeoffs_id_project_rev_gate",
        ["id", "project_id", "drawing_revision_id", "quantity_gate"],
    ),
    (
        "uq_quantity_takeoffs_id_project_rev_gate_trusted",
        ["id", "project_id", "drawing_revision_id", "quantity_gate", "trusted_totals"],
    ),
)

_EXPORT_LINEAGE = "ck_export_job_inputs_quantity_lineage"
_EXPORT_LINEAGE_GATE_FREE = (
    "(export_kind IN ('quantity_csv', 'estimate_csv', 'estimate_pdf') "
    "AND quantity_takeoff_id IS NOT NULL) OR "
    "(export_kind IN ('revision_json', 'dxf', 'revised_dxf') AND quantity_takeoff_id IS NULL)"
)
_EXPORT_LINEAGE_GATED = (
    "(export_kind IN ('quantity_csv', 'estimate_csv', 'estimate_pdf') "
    "AND quantity_takeoff_id IS NOT NULL) OR "
    "(export_kind IN ('revision_json', 'dxf', 'revised_dxf') AND quantity_takeoff_id IS NULL "
    "AND quantity_gate IS NULL AND trusted_totals IS NULL)"
)


def upgrade() -> None:
    # 1. Drop composite gate FKs + the gate-keyed quantity_items FK (plain lineage FK remains).
    for name, table, _local, _remote in _GATE_FK_CONTRACTS:
        op.drop_constraint(name, table, type_="foreignkey")
    op.drop_constraint(
        "fk_quantity_items_takeoff_gate_contract", "quantity_items", type_="foreignkey"
    )

    # 2. Drop gate-keyed unique targets.
    for name, _cols in _GATE_UNIQUES:
        op.drop_constraint(name, "quantity_takeoffs", type_="unique")

    # 3. Drop CHECK constraints on the retired columns (incl. the gated export lineage).
    for name, table, _sql in _DROPPED_CHECKS:
        op.drop_constraint(name, table, type_="check")
    op.drop_constraint(_EXPORT_LINEAGE, "export_job_inputs", type_="check")

    # 4. Drop the retired columns.
    for table, column, _type in _DROPPED_COLUMNS:
        op.drop_column(table, column)

    # 5. Recreate the export lineage CHECK without the gate columns.
    op.create_check_constraint(_EXPORT_LINEAGE, "export_job_inputs", _EXPORT_LINEAGE_GATE_FREE)

    # 6. Recreate gate-free takeoff-lineage FKs (preserve referential integrity).
    for name, table in _GATE_FREE_FKS:
        op.create_foreign_key(
            name,
            table,
            "quantity_takeoffs",
            ["quantity_takeoff_id", "project_id", "drawing_revision_id"],
            ["id", "project_id", "drawing_revision_id"],
            ondelete="RESTRICT",
        )


def downgrade() -> None:
    # Reverse: drop gate-free FKs + gate-free export lineage CHECK.
    for name, table in _GATE_FREE_FKS:
        op.drop_constraint(name, table, type_="foreignkey")
    op.drop_constraint(_EXPORT_LINEAGE, "export_job_inputs", type_="check")

    # Re-add the retired columns (nullable; data was permanently removed).
    for table, column, column_type in _DROPPED_COLUMNS:
        op.add_column(table, sa.Column(column, column_type, nullable=True))

    # Re-add gate-keyed unique targets.
    for name, cols in _GATE_UNIQUES:
        op.create_unique_constraint(name, "quantity_takeoffs", cols)

    # Re-add CHECK constraints + the gated export lineage.
    for name, table, sql in _DROPPED_CHECKS:
        op.create_check_constraint(name, table, sql)
    op.create_check_constraint(_EXPORT_LINEAGE, "export_job_inputs", _EXPORT_LINEAGE_GATED)

    # Re-add the composite gate FKs; drop the gate-keyed quantity_items lineage FK.
    for name, table, local_cols, remote_cols in _GATE_FK_CONTRACTS:
        op.create_foreign_key(
            name, table, "quantity_takeoffs", local_cols, remote_cols, ondelete="RESTRICT"
        )
    op.create_foreign_key(
        "fk_quantity_items_takeoff_gate_contract",
        "quantity_items",
        "quantity_takeoffs",
        ["quantity_takeoff_id", "project_id", "drawing_revision_id", "quantity_gate"],
        ["id", "project_id", "drawing_revision_id", "quantity_gate"],
        ondelete="RESTRICT",
    )
