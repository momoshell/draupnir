"""add quantity takeoff persistence

Revision ID: 2026_05_15_0017
Revises: 2026_05_14_0016
Create Date: 2026-05-15 10:00:00.000000
"""

from __future__ import annotations

from typing import Final

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "2026_05_15_0017"
down_revision = "2026_05_14_0016"
branch_labels = None
depends_on = None

_QUANTITY_REVIEW_STATE_VALUES: Final[tuple[str, ...]] = (
    "approved",
    "provisional",
    "review_required",
    "rejected",
    "superseded",
)
_QUANTITY_VALIDATION_STATUS_VALUES: Final[tuple[str, ...]] = (
    "valid",
    "valid_with_warnings",
    "invalid",
    "needs_review",
)
_QUANTITY_GATE_VALUES: Final[tuple[str, ...]] = (
    "allowed",
    "allowed_provisional",
    "review_gated",
    "blocked",
)
_QUANTITY_ITEM_KIND_VALUES: Final[tuple[str, ...]] = (
    "contributor",
    "aggregate",
    "exclusion",
    "conflict",
)
_QUANTITY_CONFLICT_ITEM_GATE_VALUES: Final[tuple[str, ...]] = (
    "review_gated",
    "blocked",
)
_APPEND_ONLY_TABLES: Final[tuple[str, ...]] = (
    "quantity_takeoffs",
    "quantity_items",
)
_QUANTITY_ITEM_SOURCE_ENTITY_CONTRACT: Final[str] = (
    "((item_kind = 'contributor' AND source_entity_id IS NOT NULL) "
    "OR (item_kind = 'aggregate' AND source_entity_id IS NULL) "
    "OR (item_kind = 'exclusion' AND source_entity_id IS NOT NULL) "
    "OR (item_kind = 'conflict' AND source_entity_id IS NOT NULL))"
)
_QUANTITY_ITEM_VALUE_CONTRACT: Final[str] = (
    "((item_kind = 'contributor' AND value IS NOT NULL) "
    "OR (item_kind = 'aggregate' AND value IS NOT NULL) "
    "OR (item_kind = 'exclusion' AND value IS NULL) "
    "OR (item_kind = 'conflict' AND value IS NULL))"
)
_QUANTITY_ITEM_CONFLICT_GATE_CONTRACT: Final[str] = (
    "(item_kind <> 'conflict' OR quantity_gate IN ('review_gated', 'blocked'))"
)


def _sql_in_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def _attach_append_only_triggers(table_name: str) -> None:
    op.execute(
        sa.text(
            f"""
            CREATE TRIGGER trg_append_only_row_guard
            BEFORE UPDATE OR DELETE ON "{table_name}"
            FOR EACH ROW
            EXECUTE FUNCTION enforce_append_only_lineage_row()
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            CREATE TRIGGER trg_append_only_truncate_guard
            BEFORE TRUNCATE ON "{table_name}"
            FOR EACH STATEMENT
            EXECUTE FUNCTION enforce_append_only_lineage_truncate()
            """
        )
    )


def _drop_append_only_triggers(table_name: str) -> None:
    op.execute(sa.text(f'DROP TRIGGER IF EXISTS trg_append_only_row_guard ON "{table_name}"'))
    op.execute(
        sa.text(f'DROP TRIGGER IF EXISTS trg_append_only_truncate_guard ON "{table_name}"')
    )


def _assert_quantity_tables_empty() -> None:
    bind = op.get_bind()
    for table_name in _APPEND_ONLY_TABLES:
        row_count = bind.execute(
            sa.text(f'SELECT COUNT(*) FROM "{table_name}"')
        ).scalar_one()
        if row_count:
            raise RuntimeError(
                "Refusing to downgrade migration 2026_05_15_0017 while "
                f"{table_name} contains {row_count} row(s)."
            )


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_jobs_id_project_id_file_id_base_revision_id_job_type",
        "jobs",
        ["id", "project_id", "file_id", "base_revision_id", "job_type"],
    )

    op.create_table(
        "quantity_takeoffs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("source_file_id", sa.Uuid(), nullable=False),
        sa.Column("drawing_revision_id", sa.Uuid(), nullable=False),
        sa.Column("source_job_id", sa.Uuid(), nullable=False),
        sa.Column(
            "source_job_type",
            sa.String(length=64),
            nullable=False,
            server_default=sa.text("'quantity_takeoff'"),
        ),
        sa.Column("review_state", sa.String(length=32), nullable=False),
        sa.Column("validation_status", sa.String(length=32), nullable=False),
        sa.Column("quantity_gate", sa.String(length=32), nullable=False),
        sa.Column(
            "trusted_totals",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            f"review_state IN ({_sql_in_list(_QUANTITY_REVIEW_STATE_VALUES)})",
            name="ck_quantity_takeoffs_review_state_valid",
        ),
        sa.CheckConstraint(
            "validation_status IN "
            f"({_sql_in_list(_QUANTITY_VALIDATION_STATUS_VALUES)})",
            name="ck_quantity_takeoffs_validation_status_valid",
        ),
        sa.CheckConstraint(
            f"quantity_gate IN ({_sql_in_list(_QUANTITY_GATE_VALUES)})",
            name="ck_quantity_takeoffs_quantity_gate_valid",
        ),
        sa.CheckConstraint(
            "source_job_type = 'quantity_takeoff'",
            name="ck_quantity_takeoffs_source_job_type_quantity_takeoff",
        ),
        sa.CheckConstraint(
            "trusted_totals = FALSE OR quantity_gate = 'allowed'",
            name="ck_quantity_takeoffs_trusted_totals_allowed_gate",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_quantity_takeoffs_project_id_projects",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            name="fk_quantity_takeoffs_source_file_id_project_id_files",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["drawing_revision_id", "project_id", "source_file_id"],
            [
                "drawing_revisions.id",
                "drawing_revisions.project_id",
                "drawing_revisions.source_file_id",
            ],
            name="fk_quantity_takeoffs_revision_lineage",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            [
                "source_job_id",
                "project_id",
                "source_file_id",
                "drawing_revision_id",
                "source_job_type",
            ],
            [
                "jobs.id",
                "jobs.project_id",
                "jobs.file_id",
                "jobs.base_revision_id",
                "jobs.job_type",
            ],
            name="fk_quantity_takeoffs_source_job_contract",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "id",
            "project_id",
            "drawing_revision_id",
            name="uq_quantity_takeoffs_id_project_id_drawing_revision_id",
        ),
        sa.UniqueConstraint(
            "id",
            "project_id",
            "drawing_revision_id",
            "quantity_gate",
            name="uq_quantity_takeoffs_id_project_rev_gate",
        ),
        sa.UniqueConstraint(
            "source_job_id",
            name="uq_quantity_takeoffs_source_job_id",
        ),
    )
    op.create_index(
        "ix_quantity_takeoffs_project_id",
        "quantity_takeoffs",
        ["project_id"],
        unique=False,
    )
    op.create_index(
        "ix_quantity_takeoffs_source_file_id",
        "quantity_takeoffs",
        ["source_file_id"],
        unique=False,
    )

    op.create_table(
        "quantity_items",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("quantity_takeoff_id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("drawing_revision_id", sa.Uuid(), nullable=False),
        sa.Column("item_kind", sa.String(length=32), nullable=False),
        sa.Column("quantity_type", sa.String(length=128), nullable=False),
        sa.Column("value", sa.Float(), nullable=True),
        sa.Column("unit", sa.String(length=64), nullable=False),
        sa.Column("review_state", sa.String(length=32), nullable=False),
        sa.Column("validation_status", sa.String(length=32), nullable=False),
        sa.Column("quantity_gate", sa.String(length=32), nullable=False),
        sa.Column("source_entity_id", sa.String(length=255), nullable=True),
        sa.Column(
            "excluded_source_entity_ids_json",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'[]'::json"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            f"item_kind IN ({_sql_in_list(_QUANTITY_ITEM_KIND_VALUES)})",
            name="ck_quantity_items_item_kind_valid",
        ),
        sa.CheckConstraint(
            f"review_state IN ({_sql_in_list(_QUANTITY_REVIEW_STATE_VALUES)})",
            name="ck_quantity_items_review_state_valid",
        ),
        sa.CheckConstraint(
            "validation_status IN "
            f"({_sql_in_list(_QUANTITY_VALIDATION_STATUS_VALUES)})",
            name="ck_quantity_items_validation_status_valid",
        ),
        sa.CheckConstraint(
            f"quantity_gate IN ({_sql_in_list(_QUANTITY_GATE_VALUES)})",
            name="ck_quantity_items_quantity_gate_valid",
        ),
        sa.CheckConstraint(
            "value IS NULL OR (value >= 0::float8 AND value < 'Infinity'::float8)",
            name="ck_quantity_items_value_nonnegative_finite",
        ),
        sa.CheckConstraint(
            _QUANTITY_ITEM_SOURCE_ENTITY_CONTRACT,
            name="ck_quantity_items_kind_source_entity_contract",
        ),
        sa.CheckConstraint(
            _QUANTITY_ITEM_VALUE_CONTRACT,
            name="ck_quantity_items_kind_value_contract",
        ),
        sa.CheckConstraint(
            _QUANTITY_ITEM_CONFLICT_GATE_CONTRACT,
            name="ck_quantity_items_conflict_gate_review_only",
        ),
        sa.CheckConstraint(
            "quantity_type <> ''",
            name="ck_quantity_items_quantity_type_nonempty",
        ),
        sa.CheckConstraint(
            "unit <> ''",
            name="ck_quantity_items_unit_nonempty",
        ),
        sa.CheckConstraint(
            "json_typeof(excluded_source_entity_ids_json) = 'array'",
            name="ck_quantity_items_excluded_source_entity_ids_json_array",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_quantity_items_project_id_projects",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["quantity_takeoff_id", "project_id", "drawing_revision_id"],
            [
                "quantity_takeoffs.id",
                "quantity_takeoffs.project_id",
                "quantity_takeoffs.drawing_revision_id",
            ],
            name="fk_quantity_items_takeoff_lineage",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["quantity_takeoff_id", "project_id", "drawing_revision_id", "quantity_gate"],
            [
                "quantity_takeoffs.id",
                "quantity_takeoffs.project_id",
                "quantity_takeoffs.drawing_revision_id",
                "quantity_takeoffs.quantity_gate",
            ],
            name="fk_quantity_items_takeoff_gate_contract",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["drawing_revision_id", "source_entity_id"],
            ["revision_entities.drawing_revision_id", "revision_entities.entity_id"],
            name="fk_quantity_items_source_entity",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_quantity_items_quantity_takeoff_id",
        "quantity_items",
        ["quantity_takeoff_id"],
        unique=False,
    )
    op.create_index(
        "ix_quantity_items_project_id",
        "quantity_items",
        ["project_id"],
        unique=False,
    )
    op.create_index(
        "ix_quantity_items_drawing_revision_id",
        "quantity_items",
        ["drawing_revision_id"],
        unique=False,
    )
    op.create_index(
        "ix_quantity_items_drawing_revision_id_source_entity_id",
        "quantity_items",
        ["drawing_revision_id", "source_entity_id"],
        unique=False,
    )

    for table_name in _APPEND_ONLY_TABLES:
        _attach_append_only_triggers(table_name)


def downgrade() -> None:
    _assert_quantity_tables_empty()

    for table_name in reversed(_APPEND_ONLY_TABLES):
        _drop_append_only_triggers(table_name)

    op.drop_constraint(
        "fk_quantity_items_takeoff_gate_contract",
        "quantity_items",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_quantity_items_source_entity",
        "quantity_items",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_quantity_items_takeoff_lineage",
        "quantity_items",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_quantity_items_project_id_projects",
        "quantity_items",
        type_="foreignkey",
    )
    op.drop_index(
        "ix_quantity_items_drawing_revision_id_source_entity_id",
        table_name="quantity_items",
    )
    op.drop_index("ix_quantity_items_drawing_revision_id", table_name="quantity_items")
    op.drop_index("ix_quantity_items_project_id", table_name="quantity_items")
    op.drop_index("ix_quantity_items_quantity_takeoff_id", table_name="quantity_items")
    op.drop_table("quantity_items")

    op.drop_constraint(
        "fk_quantity_takeoffs_source_job_contract",
        "quantity_takeoffs",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_quantity_takeoffs_revision_lineage",
        "quantity_takeoffs",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_quantity_takeoffs_source_file_id_project_id_files",
        "quantity_takeoffs",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_quantity_takeoffs_project_id_projects",
        "quantity_takeoffs",
        type_="foreignkey",
    )
    op.drop_index("ix_quantity_takeoffs_source_file_id", table_name="quantity_takeoffs")
    op.drop_index("ix_quantity_takeoffs_project_id", table_name="quantity_takeoffs")
    op.drop_table("quantity_takeoffs")

    op.drop_constraint(
        "uq_jobs_id_project_id_file_id_base_revision_id_job_type",
        "jobs",
        type_="unique",
    )
