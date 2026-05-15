"""Align quantity item constraints and job error codes.

Revision ID: 2026_05_15_0018
Revises: 2026_05_15_0017
Create Date: 2026-05-15 21:00:00.000000
"""

from collections.abc import Iterable

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "2026_05_15_0018"
down_revision = "2026_05_15_0017"
branch_labels = None
depends_on = None

_QUANTITY_ITEM_CONSTRAINTS_TO_DROP = (
    "ck_quantity_items_item_kind_valid",
    "ck_quantity_items_source_entity_required",
    "ck_quantity_items_total_without_source_entity",
    "ck_quantity_items_kind_source_entity_contract",
    "ck_quantity_items_kind_value_contract",
    "ck_quantity_items_conflict_gate_review_only",
)
_JOB_CONSTRAINTS_TO_DROP = ("ck_jobs_error_code_valid",)


def _drop_constraints_if_exists(table_name: str, constraint_names: Iterable[str]) -> None:
    for constraint_name in constraint_names:
        op.execute(
            sa.text(
                f"ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {constraint_name}"
            )
        )


def upgrade() -> None:
    _drop_constraints_if_exists("quantity_items", _QUANTITY_ITEM_CONSTRAINTS_TO_DROP)
    _drop_constraints_if_exists("jobs", _JOB_CONSTRAINTS_TO_DROP)

    op.create_check_constraint(
        "ck_quantity_items_item_kind_valid",
        "quantity_items",
        "item_kind IN ('contributor', 'aggregate', 'exclusion', 'conflict')",
    )
    op.create_check_constraint(
        "ck_quantity_items_kind_source_entity_contract",
        "quantity_items",
        "((item_kind = 'contributor' AND source_entity_id IS NOT NULL) "
        "OR (item_kind = 'aggregate' AND source_entity_id IS NULL) "
        "OR (item_kind = 'exclusion' AND source_entity_id IS NOT NULL) "
        "OR (item_kind = 'conflict' AND source_entity_id IS NOT NULL))",
    )
    op.create_check_constraint(
        "ck_quantity_items_kind_value_contract",
        "quantity_items",
        "((item_kind = 'contributor' AND value IS NOT NULL) "
        "OR (item_kind = 'aggregate' AND value IS NOT NULL) "
        "OR (item_kind = 'exclusion' AND value IS NULL) "
        "OR (item_kind = 'conflict' AND value IS NULL))",
    )
    op.create_check_constraint(
        "ck_quantity_items_conflict_gate_review_only",
        "quantity_items",
        "item_kind <> 'conflict' OR quantity_gate IN ('review_gated', 'blocked')",
    )
    op.create_check_constraint(
        "ck_jobs_error_code_valid",
        "jobs",
        "error_code IS NULL OR error_code IN ("
        "'NOT_FOUND', "
        "'INVALID_CURSOR', "
        "'VALIDATION_ERROR', "
        "'INPUT_INVALID', "
        "'INPUT_UNSUPPORTED_FORMAT', "
        "'ADAPTER_UNAVAILABLE', "
        "'ADAPTER_TIMEOUT', "
        "'ADAPTER_FAILED', "
        "'STORAGE_FAILED', "
        "'DB_CONFLICT', "
        "'IDEMPOTENCY_CONFLICT', "
        "'REVISION_CONFLICT', "
        "'NORMALIZED_ENTITIES_NOT_MATERIALIZED', "
        "'JOB_CANCELLED', "
        "'INTERNAL_ERROR')",
    )


def downgrade() -> None:
    # The expanded quantity item and job error code constraints are
    # backward-compatible. Downgrading by restoring the prior contract would
    # reject valid aggregate/exclusion rows and current job error codes, so
    # this revision intentionally leaves the current constraints in place.
    return None
