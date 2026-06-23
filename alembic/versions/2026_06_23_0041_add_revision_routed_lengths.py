"""add revision routed lengths

Revision ID: 2026_06_23_0041
Revises: 2026_06_20_0040
Create Date: 2026-06-23 00:00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_06_23_0041"
down_revision: str | None = "2026_06_20_0040"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

_TABLE_NAME = "revision_routed_lengths"
_ROW_GUARD_FUNCTION_NAME = "enforce_append_only_lineage_row"
_TRUNCATE_GUARD_FUNCTION_NAME = "enforce_append_only_lineage_truncate"
_ROW_TRIGGER_NAME = "trg_append_only_row_guard"
_TRUNCATE_TRIGGER_NAME = "trg_append_only_truncate_guard"

# Current job type lists (after 2026_06_03_0028).
_PREV_JOB_TYPE_VALUES: tuple[str, ...] = (
    "ingest",
    "reprocess",
    "quantity_takeoff",
    "estimate",
    "export",
    "changeset_apply",
)
_PREV_BASE_REQUIRED_JOB_TYPE_VALUES: tuple[str, ...] = (
    "reprocess",
    "quantity_takeoff",
    "estimate",
    "export",
    "changeset_apply",
)
_PREV_EXTRACTION_PROFILE_FORBIDDEN_JOB_TYPE_VALUES: tuple[str, ...] = (
    "quantity_takeoff",
    "estimate",
    "export",
    "changeset_apply",
)

# New job type lists with centerline added.
_NEW_JOB_TYPE_VALUES: tuple[str, ...] = (
    "ingest",
    "reprocess",
    "quantity_takeoff",
    "estimate",
    "export",
    "changeset_apply",
    "centerline",
)
_NEW_BASE_REQUIRED_JOB_TYPE_VALUES: tuple[str, ...] = (
    "reprocess",
    "quantity_takeoff",
    "estimate",
    "export",
    "changeset_apply",
    "centerline",
)
_NEW_EXTRACTION_PROFILE_FORBIDDEN_JOB_TYPE_VALUES: tuple[str, ...] = (
    "quantity_takeoff",
    "estimate",
    "export",
    "changeset_apply",
    "centerline",
)


def _sql_in_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def _attach_append_only_triggers() -> None:
    """Attach existing append-only trigger functions to the new table."""

    op.execute(
        sa.text(
            f"""
            CREATE TRIGGER {_ROW_TRIGGER_NAME}
            BEFORE UPDATE OR DELETE ON "{_TABLE_NAME}"
            FOR EACH ROW
            EXECUTE FUNCTION {_ROW_GUARD_FUNCTION_NAME}()
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            CREATE TRIGGER {_TRUNCATE_TRIGGER_NAME}
            BEFORE TRUNCATE ON "{_TABLE_NAME}"
            FOR EACH STATEMENT
            EXECUTE FUNCTION {_TRUNCATE_GUARD_FUNCTION_NAME}()
            """
        )
    )


def _drop_append_only_triggers() -> None:
    """Remove append-only triggers from the table."""

    op.execute(sa.text(f'DROP TRIGGER IF EXISTS {_TRUNCATE_TRIGGER_NAME} ON "{_TABLE_NAME}"'))
    op.execute(sa.text(f'DROP TRIGGER IF EXISTS {_ROW_TRIGGER_NAME} ON "{_TABLE_NAME}"'))


def _fail_if_centerline_rows_exist() -> None:
    """Refuse downgrade while centerline rows or jobs exist."""

    bind = op.get_bind()

    # EXISTS (fail-fast) rather than count(*) full scans — the guard only needs presence, and
    # revision_routed_lengths / jobs may be large on a populated database.
    has_routed_lengths = bool(
        bind.execute(sa.text(f"SELECT 1 FROM {_TABLE_NAME} LIMIT 1")).scalar()
    )
    has_centerline_jobs = bool(
        bind.execute(sa.text("SELECT 1 FROM jobs WHERE job_type = 'centerline' LIMIT 1")).scalar()
    )

    populated_parts: list[str] = []
    if has_routed_lengths:
        populated_parts.append(_TABLE_NAME)
    if has_centerline_jobs:
        populated_parts.append("centerline_jobs")

    if populated_parts:
        raise RuntimeError(
            "cannot downgrade 2026_06_23_0041: populated centerline contract present ("
            + ", ".join(populated_parts)
            + ")"
        )


def upgrade() -> None:
    """Create revision_routed_lengths table and extend job type constraints."""

    # Extend job type check constraints.
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
        f"job_type IN ({_sql_in_list(_NEW_JOB_TYPE_VALUES)})",
    )
    op.create_check_constraint(
        "ck_jobs_revision_scoped_base_revision_required",
        "jobs",
        "job_type NOT IN "
        f"({_sql_in_list(_NEW_BASE_REQUIRED_JOB_TYPE_VALUES)}) "
        "OR base_revision_id IS NOT NULL",
    )
    op.create_check_constraint(
        "ck_jobs_revision_scoped_extraction_profile_forbidden",
        "jobs",
        "job_type NOT IN "
        f"({_sql_in_list(_NEW_EXTRACTION_PROFILE_FORBIDDEN_JOB_TYPE_VALUES)}) "
        "OR extraction_profile_id IS NULL",
    )

    # Create the routed-length aggregate table.
    op.create_table(
        _TABLE_NAME,
        sa.Column(
            "id",
            sa.Uuid(),
            nullable=False,
            comment="Unique routed-length row identifier (UUID v4)",
        ),
        sa.Column(
            "project_id",
            sa.Uuid(),
            nullable=False,
            comment="Owning project identifier",
        ),
        sa.Column(
            "source_file_id",
            sa.Uuid(),
            nullable=False,
            comment="Immutable source file identifier for this routed-length row",
        ),
        sa.Column(
            "extraction_profile_id",
            sa.Uuid(),
            nullable=True,
            comment="Immutable extraction profile identifier used for this routed-length row",
        ),
        sa.Column(
            "source_job_id",
            sa.Uuid(),
            nullable=False,
            comment="Job identifier that produced this routed-length row",
        ),
        sa.Column(
            "drawing_revision_id",
            sa.Uuid(),
            nullable=False,
            comment="Drawing revision identifier that owns this routed-length row",
        ),
        sa.Column(
            "adapter_run_output_id",
            sa.Uuid(),
            nullable=True,
            comment="Adapter run output identifier that produced this routed-length row",
        ),
        sa.Column(
            "canonical_entity_schema_version",
            sa.String(length=16),
            nullable=False,
            comment="Canonical entity schema version for this routed-length row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="Routed-length row creation timestamp",
        ),
        sa.Column(
            "layer_ref",
            sa.String(length=255),
            nullable=True,
            comment="Layer group key; NULL means all layers",
        ),
        sa.Column(
            "colour_key",
            sa.String(length=255),
            nullable=True,
            comment="Colour group key; NULL means all colours",
        ),
        sa.Column(
            "algo_version",
            sa.String(length=32),
            nullable=False,
            comment="Centerline algorithm version string",
        ),
        sa.Column(
            "raster_params_hash",
            sa.String(length=64),
            nullable=False,
            comment="SHA-256 hash of the raster parameters used to produce this row",
        ),
        sa.Column(
            "producer_kind",
            sa.String(length=32),
            nullable=False,
            comment="Producer kind identifier (e.g. raster_centerline)",
        ),
        sa.Column(
            "skeleton_length_du",
            sa.Float(),
            nullable=False,
            comment="Total skeleton centerline length in drawing units",
        ),
        sa.Column(
            "entity_count",
            sa.Integer(),
            nullable=False,
            comment="Number of source entities contributing to this routed-length group",
        ),
        sa.Column(
            "geometry_json",
            sa.JSON(),
            nullable=True,
            comment="Reserved geometry payload (NULL for LP1; populated in LP2+)",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_revision_routed_lengths_project_id_projects",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            name="fk_revision_routed_lengths_source_file_project_files",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            name="fk_revision_routed_lengths_profile_project_profiles",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_job_id"],
            ["jobs.id"],
            name="fk_revision_routed_lengths_source_job_id_jobs",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["drawing_revision_id", "project_id"],
            ["drawing_revisions.id", "drawing_revisions.project_id"],
            name="fk_revision_routed_lengths_revision_project_revisions",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["adapter_run_output_id", "project_id"],
            ["adapter_run_outputs.id", "adapter_run_outputs.project_id"],
            name="fk_revision_routed_lengths_output_project_outputs",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "id",
            "project_id",
            name="uq_revision_routed_lengths_id_project_id",
        ),
        sa.UniqueConstraint(
            "drawing_revision_id",
            "layer_ref",
            "colour_key",
            "algo_version",
            "raster_params_hash",
            name="uq_revision_routed_lengths_group_version",
        ),
    )

    op.create_index(
        op.f("ix_revision_routed_lengths_project_id"),
        _TABLE_NAME,
        ["project_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_revision_routed_lengths_source_file_id"),
        _TABLE_NAME,
        ["source_file_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_revision_routed_lengths_extraction_profile_id"),
        _TABLE_NAME,
        ["extraction_profile_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_revision_routed_lengths_source_job_id"),
        _TABLE_NAME,
        ["source_job_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_revision_routed_lengths_adapter_run_output_id"),
        _TABLE_NAME,
        ["adapter_run_output_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_revision_routed_lengths_raster_params_hash"),
        _TABLE_NAME,
        ["raster_params_hash"],
        unique=False,
    )
    op.create_index(
        "ix_revision_routed_lengths_revision_group",
        _TABLE_NAME,
        ["drawing_revision_id", "layer_ref", "colour_key"],
        unique=False,
    )

    _attach_append_only_triggers()


def downgrade() -> None:
    """Drop revision_routed_lengths table and revert job type constraints."""

    _fail_if_centerline_rows_exist()

    _drop_append_only_triggers()

    op.drop_index("ix_revision_routed_lengths_revision_group", table_name=_TABLE_NAME)
    op.drop_index(op.f("ix_revision_routed_lengths_raster_params_hash"), table_name=_TABLE_NAME)
    op.drop_index(op.f("ix_revision_routed_lengths_adapter_run_output_id"), table_name=_TABLE_NAME)
    op.drop_index(op.f("ix_revision_routed_lengths_source_job_id"), table_name=_TABLE_NAME)
    op.drop_index(op.f("ix_revision_routed_lengths_extraction_profile_id"), table_name=_TABLE_NAME)
    op.drop_index(op.f("ix_revision_routed_lengths_source_file_id"), table_name=_TABLE_NAME)
    op.drop_index(op.f("ix_revision_routed_lengths_project_id"), table_name=_TABLE_NAME)
    op.drop_table(_TABLE_NAME)

    # Revert job type check constraints.
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
        f"job_type IN ({_sql_in_list(_PREV_JOB_TYPE_VALUES)})",
    )
    op.create_check_constraint(
        "ck_jobs_revision_scoped_base_revision_required",
        "jobs",
        "job_type NOT IN "
        f"({_sql_in_list(_PREV_BASE_REQUIRED_JOB_TYPE_VALUES)}) "
        "OR base_revision_id IS NOT NULL",
    )
    op.create_check_constraint(
        "ck_jobs_revision_scoped_extraction_profile_forbidden",
        "jobs",
        "job_type NOT IN "
        f"({_sql_in_list(_PREV_EXTRACTION_PROFILE_FORBIDDEN_JOB_TYPE_VALUES)}) "
        "OR extraction_profile_id IS NULL",
    )
