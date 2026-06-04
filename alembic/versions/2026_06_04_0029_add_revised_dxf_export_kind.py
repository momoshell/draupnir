"""Add revised DXF export kind contract.

Revision ID: 2026_06_04_0029
Revises: 2026_06_03_0028
Create Date: 2026-06-04 00:29:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_06_04_0029"
down_revision: str | None = "2026_06_03_0028"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def _drop_export_contract_constraints() -> None:
    op.drop_constraint(
        "ck_export_job_inputs_export_kind_valid",
        "export_job_inputs",
        type_="check",
    )
    op.drop_constraint(
        "ck_export_job_inputs_export_format_valid",
        "export_job_inputs",
        type_="check",
    )
    op.drop_constraint(
        "ck_export_job_inputs_media_type_valid",
        "export_job_inputs",
        type_="check",
    )
    op.drop_constraint(
        "ck_export_job_inputs_kind_format_media_type_matrix",
        "export_job_inputs",
        type_="check",
    )
    op.drop_constraint(
        "ck_export_job_inputs_quantity_lineage",
        "export_job_inputs",
        type_="check",
    )
    op.drop_constraint(
        "ck_export_job_inputs_estimate_lineage",
        "export_job_inputs",
        type_="check",
    )


def _create_export_contract_constraints(*, include_revised_dxf: bool) -> None:
    export_kind_values = "'revision_json', 'quantity_csv', 'estimate_csv', 'estimate_pdf'"
    export_format_values = "'json', 'csv', 'pdf'"
    media_type_values = "'application/json', 'text/csv', 'application/pdf'"
    kind_format_media_type_matrix = (
        "(export_kind = 'revision_json' AND export_format = 'json' "
        "AND media_type = 'application/json') OR "
        "(export_kind = 'quantity_csv' AND export_format = 'csv' "
        "AND media_type = 'text/csv') OR "
        "(export_kind = 'estimate_csv' AND export_format = 'csv' "
        "AND media_type = 'text/csv') OR "
        "(export_kind = 'estimate_pdf' AND export_format = 'pdf' "
        "AND media_type = 'application/pdf')"
    )
    quantity_lineage = (
        "(export_kind = 'quantity_csv' AND quantity_takeoff_id IS NOT NULL "
        "AND quantity_gate = 'allowed' AND trusted_totals IS TRUE) OR "
        "(export_kind IN ('estimate_csv', 'estimate_pdf') AND quantity_takeoff_id IS NOT NULL "
        "AND quantity_gate = 'allowed' AND trusted_totals IS TRUE) OR "
        "(export_kind = 'revision_json' AND quantity_takeoff_id IS NULL "
        "AND quantity_gate IS NULL AND trusted_totals IS NULL)"
    )
    estimate_lineage = (
        "(export_kind IN ('estimate_csv', 'estimate_pdf') "
        "AND quantity_takeoff_id IS NOT NULL AND estimate_version_id IS NOT NULL) OR "
        "(export_kind IN ('quantity_csv', 'revision_json') "
        "AND estimate_version_id IS NULL)"
    )

    if include_revised_dxf:
        export_kind_values = (
            "'revision_json', 'revised_dxf', 'quantity_csv', 'estimate_csv', 'estimate_pdf'"
        )
        export_format_values = "'json', 'dxf', 'csv', 'pdf'"
        media_type_values = "'application/json', 'application/dxf', 'text/csv', 'application/pdf'"
        kind_format_media_type_matrix = (
            "(export_kind = 'revision_json' AND export_format = 'json' "
            "AND media_type = 'application/json') OR "
            "(export_kind = 'revised_dxf' AND export_format = 'dxf' "
            "AND media_type = 'application/dxf') OR "
            "(export_kind = 'quantity_csv' AND export_format = 'csv' "
            "AND media_type = 'text/csv') OR "
            "(export_kind = 'estimate_csv' AND export_format = 'csv' "
            "AND media_type = 'text/csv') OR "
            "(export_kind = 'estimate_pdf' AND export_format = 'pdf' "
            "AND media_type = 'application/pdf')"
        )
        quantity_lineage = (
            "(export_kind = 'quantity_csv' AND quantity_takeoff_id IS NOT NULL "
            "AND quantity_gate = 'allowed' AND trusted_totals IS TRUE) OR "
            "(export_kind IN ('estimate_csv', 'estimate_pdf') AND quantity_takeoff_id IS NOT NULL "
            "AND quantity_gate = 'allowed' AND trusted_totals IS TRUE) OR "
            "(export_kind IN ('revision_json', 'revised_dxf') AND quantity_takeoff_id IS NULL "
            "AND quantity_gate IS NULL AND trusted_totals IS NULL)"
        )
        estimate_lineage = (
            "(export_kind IN ('estimate_csv', 'estimate_pdf') "
            "AND quantity_takeoff_id IS NOT NULL AND estimate_version_id IS NOT NULL) OR "
            "(export_kind IN ('quantity_csv', 'revision_json', 'revised_dxf') "
            "AND estimate_version_id IS NULL)"
        )

    op.create_check_constraint(
        "ck_export_job_inputs_export_kind_valid",
        "export_job_inputs",
        f"export_kind IN ({export_kind_values})",
    )
    op.create_check_constraint(
        "ck_export_job_inputs_export_format_valid",
        "export_job_inputs",
        f"export_format IN ({export_format_values})",
    )
    op.create_check_constraint(
        "ck_export_job_inputs_media_type_valid",
        "export_job_inputs",
        f"media_type IN ({media_type_values})",
    )
    op.create_check_constraint(
        "ck_export_job_inputs_kind_format_media_type_matrix",
        "export_job_inputs",
        kind_format_media_type_matrix,
    )
    op.create_check_constraint(
        "ck_export_job_inputs_quantity_lineage",
        "export_job_inputs",
        quantity_lineage,
    )
    op.create_check_constraint(
        "ck_export_job_inputs_estimate_lineage",
        "export_job_inputs",
        estimate_lineage,
    )


def _assert_downgrade_safe() -> None:
    bind = op.get_bind()
    bind.execute(sa.text("LOCK TABLE export_job_inputs IN ACCESS EXCLUSIVE MODE"))

    revised_dxf_row_exists = bind.execute(
        sa.text(
            """
            SELECT 1
            FROM export_job_inputs
            WHERE export_kind = 'revised_dxf'
            LIMIT 1
            """
        )
    ).scalar()
    if revised_dxf_row_exists is not None:
        raise RuntimeError("Cannot downgrade: persisted revised_dxf export_job_inputs rows present")


def upgrade() -> None:
    _drop_export_contract_constraints()
    _create_export_contract_constraints(include_revised_dxf=True)


def downgrade() -> None:
    _assert_downgrade_safe()
    _drop_export_contract_constraints()
    _create_export_contract_constraints(include_revised_dxf=False)
