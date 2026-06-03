"""add changeset apply job input

Revision ID: 2026_06_03_0028
Revises: 2026_06_01_0027
Create Date: 2026-06-03 00:28:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "2026_06_03_0028"
down_revision = "2026_06_01_0027"
branch_labels = None
depends_on = None

JOB_TYPE_VALUES: tuple[str, ...] = (
    "ingest",
    "reprocess",
    "quantity_takeoff",
    "estimate",
    "export",
    "changeset_apply",
)
BASE_REQUIRED_JOB_TYPE_VALUES: tuple[str, ...] = (
    "reprocess",
    "quantity_takeoff",
    "estimate",
    "export",
    "changeset_apply",
)
EXTRACTION_PROFILE_FORBIDDEN_JOB_TYPE_VALUES: tuple[str, ...] = (
    "quantity_takeoff",
    "estimate",
    "export",
    "changeset_apply",
)
CHANGESET_APPLY_VALIDATION_STATUS_VALUES: tuple[str, ...] = (
    "valid",
    "valid_with_warnings",
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


def _fail_if_changeset_apply_contract_populated() -> None:
    bind = op.get_bind()
    bind.execute(sa.text("LOCK TABLE jobs, changeset_apply_job_inputs IN ACCESS EXCLUSIVE MODE"))

    changeset_apply_job_count = bind.execute(
        sa.text("SELECT count(*) FROM jobs WHERE job_type = 'changeset_apply'")
    ).scalar_one()
    changeset_apply_input_count = bind.execute(
        sa.text("SELECT count(*) FROM changeset_apply_job_inputs")
    ).scalar_one()

    populated_parts: list[str] = []
    if changeset_apply_job_count > 0:
        populated_parts.append(f"jobs={changeset_apply_job_count}")
    if changeset_apply_input_count > 0:
        populated_parts.append(f"changeset_apply_job_inputs={changeset_apply_input_count}")

    if populated_parts:
        raise RuntimeError(
            "cannot downgrade 2026_06_03_0028: populated changeset apply contract present ("
            + ", ".join(populated_parts)
            + ")"
        )


def upgrade() -> None:
    op.drop_constraint("ck_jobs_job_type_valid", "jobs", type_="check")
    op.drop_constraint(
        "ck_jobs_revision_scoped_base_revision_required",
        "jobs",
        type_="check",
    )
    op.drop_constraint(
        "ck_jobs_revision_scoped_extraction_profile_forbidden",
        "jobs",
        type_="check",
    )

    op.create_check_constraint(
        "ck_jobs_job_type_valid",
        "jobs",
        f"job_type IN ({_sql_in_list(JOB_TYPE_VALUES)})",
    )
    op.create_check_constraint(
        "ck_jobs_revision_scoped_base_revision_required",
        "jobs",
        "job_type NOT IN "
        f"({_sql_in_list(BASE_REQUIRED_JOB_TYPE_VALUES)}) "
        "OR base_revision_id IS NOT NULL",
    )
    op.create_check_constraint(
        "ck_jobs_revision_scoped_extraction_profile_forbidden",
        "jobs",
        "job_type NOT IN "
        f"({_sql_in_list(EXTRACTION_PROFILE_FORBIDDEN_JOB_TYPE_VALUES)}) "
        "OR extraction_profile_id IS NULL",
    )
    op.create_unique_constraint(
        "uq_cad_change_sets_project_id_id_base_revision_id",
        "cad_change_sets",
        ["project_id", "id", "base_revision_id"],
    )
    op.create_unique_constraint(
        "uq_cad_change_validation_project_set_id_status",
        "cad_change_set_validation_results",
        ["project_id", "change_set_id", "id", "validation_status"],
    )

    op.create_table(
        "changeset_apply_job_inputs",
        sa.Column(
            "source_job_id",
            sa.Uuid(as_uuid=True),
            nullable=False,
            comment="Owning changeset-apply job identifier",
        ),
        sa.Column(
            "project_id",
            sa.Uuid(as_uuid=True),
            nullable=False,
            comment="Owning project identifier",
        ),
        sa.Column(
            "source_file_id",
            sa.Uuid(as_uuid=True),
            nullable=False,
            comment="Pinned source file identifier from the apply job lineage",
        ),
        sa.Column(
            "drawing_revision_id",
            sa.Uuid(as_uuid=True),
            nullable=False,
            comment="Pinned base drawing revision identifier for the apply job",
        ),
        sa.Column(
            "change_set_id",
            sa.Uuid(as_uuid=True),
            nullable=False,
            comment="Pinned changeset identifier to apply to the base revision",
        ),
        sa.Column(
            "source_job_type",
            sa.String(length=64),
            nullable=False,
            server_default=sa.text("'changeset_apply'"),
            comment="Shadow job type constrained to changeset_apply for composite job FK proof",
        ),
        sa.Column(
            "latest_validation_result_id",
            sa.Uuid(as_uuid=True),
            nullable=False,
            comment="Latest persisted validation result identifier pinned at job creation",
        ),
        sa.Column(
            "latest_validation_status",
            sa.String(length=32),
            nullable=False,
            comment="Latest persisted validation status snapshot pinned at job creation",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            default=sa.func.now(),
            comment="Job input creation timestamp",
        ),
        sa.CheckConstraint(
            "source_job_type = 'changeset_apply'",
            name="ck_changeset_apply_job_inputs_source_job_type",
        ),
        sa.CheckConstraint(
            "latest_validation_status IN "
            f"({_sql_in_list(CHANGESET_APPLY_VALIDATION_STATUS_VALUES)})",
            name="ck_changeset_apply_job_inputs_validation_status",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_changeset_apply_job_inputs_project_id_projects",
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
            name="fk_changeset_apply_job_inputs_job_lineage_jobs",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["drawing_revision_id", "project_id", "source_file_id"],
            [
                "drawing_revisions.id",
                "drawing_revisions.project_id",
                "drawing_revisions.source_file_id",
            ],
            name="fk_changeset_apply_job_inputs_revision_lineage",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["project_id", "change_set_id", "drawing_revision_id"],
            [
                "cad_change_sets.project_id",
                "cad_change_sets.id",
                "cad_change_sets.base_revision_id",
            ],
            name="fk_changeset_apply_job_inputs_base_changeset",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            [
                "project_id",
                "change_set_id",
                "latest_validation_result_id",
                "latest_validation_status",
            ],
            [
                "cad_change_set_validation_results.project_id",
                "cad_change_set_validation_results.change_set_id",
                "cad_change_set_validation_results.id",
                "cad_change_set_validation_results.validation_status",
            ],
            name="fk_changeset_apply_job_inputs_validation_result",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("source_job_id"),
    )
    op.create_unique_constraint(
        "uq_changeset_apply_job_inputs_project_id_change_set_id",
        "changeset_apply_job_inputs",
        ["project_id", "change_set_id"],
    )
    op.create_index(
        "ix_changeset_apply_job_inputs_project_id",
        "changeset_apply_job_inputs",
        ["project_id"],
        unique=False,
    )
    op.create_index(
        "ix_changeset_apply_job_inputs_source_file_id",
        "changeset_apply_job_inputs",
        ["source_file_id"],
        unique=False,
    )
    op.create_index(
        "ix_changeset_apply_job_inputs_drawing_revision_id",
        "changeset_apply_job_inputs",
        ["drawing_revision_id"],
        unique=False,
    )
    op.create_index(
        "ix_changeset_apply_job_inputs_change_set_id",
        "changeset_apply_job_inputs",
        ["change_set_id"],
        unique=False,
    )
    op.create_index(
        "ix_changeset_apply_job_inputs_latest_validation_result_id",
        "changeset_apply_job_inputs",
        ["latest_validation_result_id"],
        unique=False,
    )

    _create_append_only_triggers("changeset_apply_job_inputs")


def downgrade() -> None:
    _fail_if_changeset_apply_contract_populated()

    _drop_append_only_triggers("changeset_apply_job_inputs")

    op.drop_index(
        "ix_changeset_apply_job_inputs_latest_validation_result_id",
        table_name="changeset_apply_job_inputs",
    )
    op.drop_index(
        "ix_changeset_apply_job_inputs_change_set_id",
        table_name="changeset_apply_job_inputs",
    )
    op.drop_index(
        "ix_changeset_apply_job_inputs_drawing_revision_id",
        table_name="changeset_apply_job_inputs",
    )
    op.drop_index(
        "ix_changeset_apply_job_inputs_source_file_id",
        table_name="changeset_apply_job_inputs",
    )
    op.drop_index(
        "ix_changeset_apply_job_inputs_project_id",
        table_name="changeset_apply_job_inputs",
    )
    op.drop_constraint(
        "uq_changeset_apply_job_inputs_project_id_change_set_id",
        "changeset_apply_job_inputs",
        type_="unique",
    )
    op.drop_table("changeset_apply_job_inputs")
    op.drop_constraint(
        "uq_cad_change_validation_project_set_id_status",
        "cad_change_set_validation_results",
        type_="unique",
    )
    op.drop_constraint(
        "uq_cad_change_sets_project_id_id_base_revision_id",
        "cad_change_sets",
        type_="unique",
    )

    op.drop_constraint(
        "ck_jobs_revision_scoped_extraction_profile_forbidden",
        "jobs",
        type_="check",
    )
    op.drop_constraint(
        "ck_jobs_revision_scoped_base_revision_required",
        "jobs",
        type_="check",
    )
    op.drop_constraint("ck_jobs_job_type_valid", "jobs", type_="check")

    op.create_check_constraint(
        "ck_jobs_job_type_valid",
        "jobs",
        "job_type IN ('ingest', 'reprocess', 'quantity_takeoff', 'estimate', 'export')",
    )
    op.create_check_constraint(
        "ck_jobs_revision_scoped_base_revision_required",
        "jobs",
        "job_type NOT IN ('reprocess', 'quantity_takeoff', 'estimate', 'export') "
        "OR base_revision_id IS NOT NULL",
    )
    op.create_check_constraint(
        "ck_jobs_revision_scoped_extraction_profile_forbidden",
        "jobs",
        "job_type NOT IN ('quantity_takeoff', 'estimate', 'export') "
        "OR extraction_profile_id IS NULL",
    )
