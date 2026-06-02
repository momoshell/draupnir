"""add changeset persistence

Revision ID: 2026_06_01_0027
Revises: 2026_05_31_0026
Create Date: 2026-06-01 00:27:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "2026_06_01_0027"
down_revision = "2026_05_31_0026"
branch_labels = None
depends_on = None

CAD_CHANGE_OPERATION_TYPES: tuple[str, ...] = (
    "annotate_entity",
    "change_layer",
    "add_entity",
    "remove_entity",
    "replace_block",
    "replace_profile_material_candidate",
    "update_property",
    "flag_for_review",
)

CAD_CHANGE_SET_STATUSES: tuple[str, ...] = (
    "proposed",
    "validation_requested",
    "approved",
    "rejected",
    "validating",
    "validated",
    "validation_failed",
    "applying",
    "applied",
    "apply_failed",
    "revision_conflict",
)

CAD_CHANGE_SET_VALIDATION_STATUSES: tuple[str, ...] = (
    "valid",
    "valid_with_warnings",
    "invalid",
    "needs_review",
)


def _sql_in_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def _create_append_only_triggers(
    table_name: str,
    allowlisted_columns: tuple[str, ...] = (),
) -> None:
    row_guard_call = "enforce_append_only_lineage_row()"
    if allowlisted_columns:
        allowlist_sql = ", ".join(f"'{column_name}'" for column_name in allowlisted_columns)
        row_guard_call = f"enforce_append_only_lineage_row({allowlist_sql})"

    op.execute(
        sa.text(
            f"""
            CREATE TRIGGER trg_append_only_row_guard
            BEFORE UPDATE OR DELETE ON {table_name}
            FOR EACH ROW
            EXECUTE FUNCTION {row_guard_call}
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            CREATE TRIGGER trg_append_only_truncate_guard
            BEFORE TRUNCATE ON {table_name}
            FOR EACH STATEMENT
            EXECUTE FUNCTION enforce_append_only_lineage_truncate()
            """
        )
    )


def _drop_append_only_triggers(table_name: str) -> None:
    op.execute(sa.text(f"DROP TRIGGER IF EXISTS trg_append_only_row_guard ON {table_name}"))
    op.execute(sa.text(f"DROP TRIGGER IF EXISTS trg_append_only_truncate_guard ON {table_name}"))


def _verify_existing_drawing_revision_changeset_refs() -> None:
    bind = op.get_bind()
    missing_refs = bind.execute(
        sa.text(
            """
            SELECT count(*)
            FROM drawing_revisions AS revisions
            LEFT JOIN cad_change_sets AS change_sets
              ON change_sets.project_id = revisions.project_id
             AND change_sets.id = revisions.changeset_id
            WHERE revisions.changeset_id IS NOT NULL
              AND change_sets.id IS NULL
            """
        )
    ).scalar_one()
    if missing_refs:
        raise RuntimeError(
            "cannot add drawing_revisions changeset FK: existing rows lack "
            "same-project cad_change_sets"
        )


def _fail_if_changeset_tables_populated() -> None:
    bind = op.get_bind()
    row_counts: dict[str, int] = {}

    for table_name in (
        "cad_change_set_validation_results",
        "cad_change_operations",
        "cad_change_sets",
    ):
        bind.execute(sa.text(f"LOCK TABLE {table_name} IN ACCESS EXCLUSIVE MODE"))
        row_counts[table_name] = bind.execute(
            sa.text(f"SELECT count(*) FROM {table_name}")
        ).scalar_one()

    populated_tables = [
        f"{table_name}={row_count}" for table_name, row_count in row_counts.items() if row_count > 0
    ]
    if populated_tables:
        raise RuntimeError(
            "cannot downgrade 2026_06_01_0027: populated changeset tables present ("
            + ", ".join(populated_tables)
            + ")"
        )


def upgrade() -> None:
    op.create_table(
        "cad_change_sets",
        sa.Column("id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column(
            "project_id",
            sa.Uuid(as_uuid=True),
            nullable=False,
            comment="Owning project identifier",
        ),
        sa.Column("base_revision_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_by", sa.String(length=128), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            default=sa.func.now(),
            comment="Changeset creation timestamp",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            default=sa.func.now(),
            comment="Most recent mutable status update timestamp",
        ),
        sa.CheckConstraint(
            f"status IN ({_sql_in_list(CAD_CHANGE_SET_STATUSES)})",
            name="ck_cad_change_sets_status",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_cad_change_sets_project_id_projects",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["base_revision_id", "project_id"],
            ["drawing_revisions.id", "drawing_revisions.project_id"],
            name="fk_cad_change_sets_base_revision_id_project_id_revisions",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("project_id", "id", name="uq_cad_change_sets_project_id_id"),
    )
    op.create_index(
        "ix_cad_change_sets_project_id", "cad_change_sets", ["project_id"], unique=False
    )
    op.create_index(
        "ix_cad_change_sets_base_revision_id",
        "cad_change_sets",
        ["base_revision_id"],
        unique=False,
    )

    op.create_table(
        "cad_change_operations",
        sa.Column("id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column(
            "project_id",
            sa.Uuid(as_uuid=True),
            nullable=False,
            comment="Owning project identifier",
        ),
        sa.Column("change_set_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("sequence_index", sa.Integer(), nullable=False),
        sa.Column("operation_type", sa.String(length=64), nullable=False),
        sa.Column("target_revision_entity_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("expected_source_identity", sa.String(length=255), nullable=True),
        sa.Column("expected_source_hash", sa.String(length=64), nullable=True),
        sa.Column("operation_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            default=sa.func.now(),
            comment="Operation creation timestamp",
        ),
        sa.CheckConstraint("sequence_index >= 1", name="ck_cad_change_operations_sequence_ge_1"),
        sa.CheckConstraint(
            f"operation_type IN ({_sql_in_list(CAD_CHANGE_OPERATION_TYPES)})",
            name="ck_cad_change_operations_type",
        ),
        sa.CheckConstraint(
            "expected_source_hash IS NULL OR expected_source_hash ~ '^[0-9a-f]{64}$'",
            name="ck_cad_change_operations_expected_source_hash_sha256",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(operation_json) = 'object'",
            name="ck_cad_change_operations_operation_json_object",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_cad_change_operations_project_id_projects",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["project_id", "change_set_id"],
            ["cad_change_sets.project_id", "cad_change_sets.id"],
            name="fk_cad_change_operations_project_id_change_set_id_sets",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "project_id",
            "change_set_id",
            "id",
            name="uq_cad_change_ops_project_set_id",
        ),
        sa.UniqueConstraint(
            "project_id",
            "change_set_id",
            "sequence_index",
            name="uq_cad_change_ops_project_set_sequence",
        ),
        sa.UniqueConstraint("project_id", "id", name="uq_cad_change_operations_project_id_id"),
    )
    op.create_index(
        "ix_cad_change_operations_project_id",
        "cad_change_operations",
        ["project_id"],
        unique=False,
    )
    op.create_index(
        "ix_cad_change_operations_change_set_id",
        "cad_change_operations",
        ["change_set_id"],
        unique=False,
    )

    op.create_table(
        "cad_change_set_validation_results",
        sa.Column("id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column(
            "project_id",
            sa.Uuid(as_uuid=True),
            nullable=False,
            comment="Owning project identifier",
        ),
        sa.Column("change_set_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("validation_status", sa.String(length=32), nullable=False),
        sa.Column("validator_name", sa.String(length=128), nullable=True),
        sa.Column("validator_version", sa.String(length=32), nullable=True),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            default=sa.func.now(),
            comment="Validation result creation timestamp",
        ),
        sa.CheckConstraint(
            f"validation_status IN ({_sql_in_list(CAD_CHANGE_SET_VALIDATION_STATUSES)})",
            name="ck_cad_change_set_validation_results_status",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(result_json) = 'object'",
            name="ck_cad_change_set_validation_results_result_json_object",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_cad_change_set_validation_results_project_id_projects",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["project_id", "change_set_id"],
            ["cad_change_sets.project_id", "cad_change_sets.id"],
            name="fk_cad_change_set_validation_results_project_id_change_set_id",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "project_id",
            "change_set_id",
            "id",
            name="uq_cad_change_validation_project_set_id",
        ),
        sa.UniqueConstraint(
            "project_id",
            "id",
            name="uq_cad_change_set_validation_results_project_id_id",
        ),
    )
    op.create_index(
        "ix_cad_change_set_validation_results_project_id",
        "cad_change_set_validation_results",
        ["project_id"],
        unique=False,
    )
    op.create_index(
        "ix_cad_change_set_validation_results_change_set_id",
        "cad_change_set_validation_results",
        ["change_set_id"],
        unique=False,
    )

    _create_append_only_triggers("cad_change_sets", ("status", "updated_at"))
    _create_append_only_triggers("cad_change_operations")
    _create_append_only_triggers("cad_change_set_validation_results")

    _verify_existing_drawing_revision_changeset_refs()
    op.create_foreign_key(
        "fk_drawing_revisions_project_id_changeset_id_change_sets",
        "drawing_revisions",
        "cad_change_sets",
        ["project_id", "changeset_id"],
        ["project_id", "id"],
    )


def downgrade() -> None:
    _fail_if_changeset_tables_populated()

    op.drop_constraint(
        "fk_drawing_revisions_project_id_changeset_id_change_sets",
        "drawing_revisions",
        type_="foreignkey",
    )

    for table_name in (
        "cad_change_set_validation_results",
        "cad_change_operations",
        "cad_change_sets",
    ):
        _drop_append_only_triggers(table_name)

    op.drop_index(
        "ix_cad_change_set_validation_results_change_set_id",
        table_name="cad_change_set_validation_results",
    )
    op.drop_index(
        "ix_cad_change_set_validation_results_project_id",
        table_name="cad_change_set_validation_results",
    )
    op.drop_table("cad_change_set_validation_results")

    op.drop_index("ix_cad_change_operations_change_set_id", table_name="cad_change_operations")
    op.drop_index("ix_cad_change_operations_project_id", table_name="cad_change_operations")
    op.drop_table("cad_change_operations")

    op.drop_index("ix_cad_change_sets_base_revision_id", table_name="cad_change_sets")
    op.drop_index("ix_cad_change_sets_project_id", table_name="cad_change_sets")
    op.drop_table("cad_change_sets")
