"""Add export job input contract."""

import alembic.op as op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "2026_05_27_0024"
down_revision: str | None = "2026_05_18_0023"
branch_labels: str | tuple[str, ...] | None = None
depends_on: str | tuple[str, ...] | None = None

_JOB_TYPE_VALUES = (
    "ingest",
    "reprocess",
    "quantity_takeoff",
    "estimate",
    "export",
)
_BASE_REQUIRED_JOB_TYPE_VALUES = (
    "reprocess",
    "quantity_takeoff",
    "estimate",
    "export",
)
_EXTRACTION_PROFILE_FORBIDDEN_JOB_TYPE_VALUES = (
    "quantity_takeoff",
    "estimate",
    "export",
)
_PRE_EXPORT_JOB_TYPE_VALUES = (
    "ingest",
    "reprocess",
    "quantity_takeoff",
    "estimate",
)
_PRE_EXPORT_BASE_REQUIRED_JOB_TYPE_VALUES = (
    "reprocess",
    "quantity_takeoff",
    "estimate",
)
_PRE_EXPORT_EXTRACTION_PROFILE_FORBIDDEN_JOB_TYPE_VALUES = (
    "quantity_takeoff",
    "estimate",
)


def _sql_in_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def _replace_jobs_constraints(
    *,
    job_type_values: tuple[str, ...],
    base_required_job_type_values: tuple[str, ...],
    extraction_profile_forbidden_job_type_values: tuple[str, ...],
) -> None:
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
        f"job_type IN ({_sql_in_list(job_type_values)})",
    )
    op.create_check_constraint(
        "ck_jobs_revision_scoped_base_revision_required",
        "jobs",
        "job_type NOT IN "
        f"({_sql_in_list(base_required_job_type_values)}) "
        "OR base_revision_id IS NOT NULL",
    )
    op.create_check_constraint(
        "ck_jobs_revision_scoped_extraction_profile_forbidden",
        "jobs",
        "job_type NOT IN "
        f"({_sql_in_list(extraction_profile_forbidden_job_type_values)}) "
        "OR extraction_profile_id IS NULL",
    )


def _create_export_job_inputs_append_only_triggers() -> None:
    op.execute(
        sa.text(
            """
        CREATE TRIGGER trg_append_only_row_guard
        BEFORE UPDATE OR DELETE ON export_job_inputs
        FOR EACH ROW
        EXECUTE FUNCTION enforce_append_only_lineage_row();
        """
        )
    )
    op.execute(
        sa.text(
            """
        CREATE TRIGGER trg_append_only_truncate_guard
        BEFORE TRUNCATE ON export_job_inputs
        FOR EACH STATEMENT
        EXECUTE FUNCTION enforce_append_only_lineage_truncate();
        """
        )
    )


def _drop_export_job_inputs_append_only_triggers() -> None:
    op.execute(sa.text("DROP TRIGGER IF EXISTS trg_append_only_row_guard ON export_job_inputs"))
    op.execute(
        sa.text("DROP TRIGGER IF EXISTS trg_append_only_truncate_guard ON export_job_inputs")
    )


def _assert_downgrade_safe() -> None:
    bind = op.get_bind()
    export_job_input_exists = bind.execute(
        sa.text("SELECT 1 FROM export_job_inputs LIMIT 1")
    ).scalar_one_or_none()
    if export_job_input_exists is not None:
        raise RuntimeError("Cannot downgrade with persisted export_job_inputs rows present.")

    export_job_exists = bind.execute(
        sa.text("SELECT 1 FROM jobs WHERE job_type = 'export' LIMIT 1")
    ).scalar_one_or_none()
    if export_job_exists is not None:
        raise RuntimeError("Cannot downgrade with persisted export jobs present.")


def upgrade() -> None:
    _replace_jobs_constraints(
        job_type_values=_JOB_TYPE_VALUES,
        base_required_job_type_values=_BASE_REQUIRED_JOB_TYPE_VALUES,
        extraction_profile_forbidden_job_type_values=(
            _EXTRACTION_PROFILE_FORBIDDEN_JOB_TYPE_VALUES
        ),
    )

    op.create_table(
        "export_job_inputs",
        sa.Column(
            "source_job_id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="Owning export job identifier.",
        ),
        sa.Column(
            "project_id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="Owning project identifier copied from the source export job.",
        ),
        sa.Column(
            "source_file_id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="Owning file identifier copied from the source export job.",
        ),
        sa.Column(
            "drawing_revision_id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="Pinned revision identifier copied from the source export job.",
        ),
        sa.Column(
            "source_job_type",
            sa.String(length=64),
            nullable=False,
            server_default=sa.text("'export'"),
            comment="Mirrored job type used to pin this row to export jobs only.",
        ),
        sa.Column(
            "export_kind",
            sa.String(length=64),
            nullable=False,
            comment=(
                "Export kind selector (revision_json, quantity_csv, estimate_csv, estimate_pdf)."
            ),
        ),
        sa.Column(
            "export_format",
            sa.String(length=32),
            nullable=False,
            comment="Resolved export format required by the export kind.",
        ),
        sa.Column(
            "media_type",
            sa.String(length=128),
            nullable=False,
            comment="Resolved artifact media type required by the export kind.",
        ),
        sa.Column(
            "options_json",
            sa.dialects.postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            comment="Immutable export options JSON object captured at enqueue time.",
        ),
        sa.Column(
            "quantity_takeoff_id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            nullable=True,
            comment=(
                "Trusted quantity takeoff required for quantity_csv, estimate_csv, "
                "and estimate_pdf exports."
            ),
        ),
        sa.Column(
            "quantity_gate",
            sa.String(length=32),
            nullable=True,
            comment="Mirrored quantity gate used to enforce trusted quantity lineage.",
        ),
        sa.Column(
            "trusted_totals",
            sa.Boolean(),
            nullable=True,
            comment="Mirrored trusted-totals flag used to enforce trusted quantity lineage.",
        ),
        sa.Column(
            "estimate_version_id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            nullable=True,
            comment=(
                "Finalized estimate version required for estimate_csv and estimate_pdf exports."
            ),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
            comment="Row creation timestamp.",
        ),
        sa.CheckConstraint(
            "source_job_type = 'export'",
            name="ck_export_job_inputs_source_job_type_export",
        ),
        sa.CheckConstraint(
            "export_kind IN ('revision_json', 'quantity_csv', 'estimate_csv', 'estimate_pdf')",
            name="ck_export_job_inputs_export_kind_valid",
        ),
        sa.CheckConstraint(
            "export_format IN ('json', 'csv', 'pdf')",
            name="ck_export_job_inputs_export_format_valid",
        ),
        sa.CheckConstraint(
            "media_type IN ('application/json', 'text/csv', 'application/pdf')",
            name="ck_export_job_inputs_media_type_valid",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(options_json) = 'object'",
            name="ck_export_job_inputs_options_json_object",
        ),
        sa.CheckConstraint(
            "(export_kind = 'revision_json' AND export_format = 'json' "
            "AND media_type = 'application/json') OR "
            "(export_kind = 'quantity_csv' AND export_format = 'csv' "
            "AND media_type = 'text/csv') OR "
            "(export_kind = 'estimate_csv' AND export_format = 'csv' "
            "AND media_type = 'text/csv') OR "
            "(export_kind = 'estimate_pdf' AND export_format = 'pdf' "
            "AND media_type = 'application/pdf')",
            name="ck_export_job_inputs_kind_format_media_type_matrix",
        ),
        sa.CheckConstraint(
            "(export_kind = 'quantity_csv' AND quantity_takeoff_id IS NOT NULL "
            "AND quantity_gate = 'allowed' AND trusted_totals IS TRUE) OR "
            "(export_kind IN ('estimate_csv', 'estimate_pdf') "
            "AND quantity_takeoff_id IS NOT NULL AND quantity_gate = 'allowed' "
            "AND trusted_totals IS TRUE) OR "
            "(export_kind = 'revision_json' AND quantity_takeoff_id IS NULL "
            "AND quantity_gate IS NULL AND trusted_totals IS NULL)",
            name="ck_export_job_inputs_quantity_lineage",
        ),
        sa.CheckConstraint(
            "(export_kind IN ('estimate_csv', 'estimate_pdf') "
            "AND quantity_takeoff_id IS NOT NULL AND estimate_version_id IS NOT NULL) OR "
            "(export_kind IN ('quantity_csv', 'revision_json') "
            "AND estimate_version_id IS NULL)",
            name="ck_export_job_inputs_estimate_lineage",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_export_job_inputs_project_id_projects",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_job_id"],
            ["jobs.id"],
            name="fk_export_job_inputs_source_job_id_jobs",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            name="fk_export_job_inputs_source_file_id_project_id_files",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["drawing_revision_id", "project_id", "source_file_id"],
            [
                "drawing_revisions.id",
                "drawing_revisions.project_id",
                "drawing_revisions.source_file_id",
            ],
            name="fk_export_job_inputs_revision_lineage",
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
            name="fk_export_job_inputs_source_job_lineage_jobs",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            [
                "quantity_takeoff_id",
                "project_id",
                "drawing_revision_id",
                "quantity_gate",
                "trusted_totals",
            ],
            [
                "quantity_takeoffs.id",
                "quantity_takeoffs.project_id",
                "quantity_takeoffs.drawing_revision_id",
                "quantity_takeoffs.quantity_gate",
                "quantity_takeoffs.trusted_totals",
            ],
            name="fk_export_job_inputs_trusted_quantity_takeoff",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            [
                "estimate_version_id",
                "project_id",
                "drawing_revision_id",
                "quantity_takeoff_id",
            ],
            [
                "estimate_versions.id",
                "estimate_versions.project_id",
                "estimate_versions.drawing_revision_id",
                "estimate_versions.quantity_takeoff_id",
            ],
            name="fk_export_job_inputs_estimate_version_lineage",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("source_job_id", name="pk_export_job_inputs"),
    )
    op.create_index(
        op.f("ix_export_job_inputs_project_id"),
        "export_job_inputs",
        ["project_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_export_job_inputs_source_file_id"),
        "export_job_inputs",
        ["source_file_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_export_job_inputs_drawing_revision_id"),
        "export_job_inputs",
        ["drawing_revision_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_export_job_inputs_quantity_takeoff_id"),
        "export_job_inputs",
        ["quantity_takeoff_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_export_job_inputs_estimate_version_id"),
        "export_job_inputs",
        ["estimate_version_id"],
        unique=False,
    )
    _create_export_job_inputs_append_only_triggers()


def downgrade() -> None:
    _assert_downgrade_safe()
    _drop_export_job_inputs_append_only_triggers()
    op.drop_index(op.f("ix_export_job_inputs_estimate_version_id"), table_name="export_job_inputs")
    op.drop_index(op.f("ix_export_job_inputs_quantity_takeoff_id"), table_name="export_job_inputs")
    op.drop_index(op.f("ix_export_job_inputs_drawing_revision_id"), table_name="export_job_inputs")
    op.drop_index(op.f("ix_export_job_inputs_source_file_id"), table_name="export_job_inputs")
    op.drop_index(op.f("ix_export_job_inputs_project_id"), table_name="export_job_inputs")
    op.drop_table("export_job_inputs")
    _replace_jobs_constraints(
        job_type_values=_PRE_EXPORT_JOB_TYPE_VALUES,
        base_required_job_type_values=_PRE_EXPORT_BASE_REQUIRED_JOB_TYPE_VALUES,
        extraction_profile_forbidden_job_type_values=(
            _PRE_EXPORT_EXTRACTION_PROFILE_FORBIDDEN_JOB_TYPE_VALUES
        ),
    )
