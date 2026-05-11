"""replace lineage fk cascades with restrict

Revision ID: 2026_05_11_0010
Revises: 2026_05_10_0009
Create Date: 2026-05-11 08:45:00
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import NamedTuple

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_05_11_0010"
down_revision: str | None = "2026_05_10_0009"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


class _ForeignKeySpec(NamedTuple):
    table_name: str
    constrained_columns: tuple[str, ...]
    referred_table: str
    referred_columns: tuple[str, ...]
    name: str


_LINEAGE_FOREIGN_KEYS: tuple[_ForeignKeySpec, ...] = (
    _ForeignKeySpec(
        table_name="files",
        constrained_columns=("project_id",),
        referred_table="projects",
        referred_columns=("id",),
        name="fk_files_project_id_projects",
    ),
    _ForeignKeySpec(
        table_name="extraction_profiles",
        constrained_columns=("project_id",),
        referred_table="projects",
        referred_columns=("id",),
        name="fk_extraction_profiles_project_id_projects",
    ),
    _ForeignKeySpec(
        table_name="jobs",
        constrained_columns=("file_id", "project_id"),
        referred_table="files",
        referred_columns=("id", "project_id"),
        name="fk_jobs_file_id_project_id_files",
    ),
    _ForeignKeySpec(
        table_name="jobs",
        constrained_columns=("extraction_profile_id", "project_id"),
        referred_table="extraction_profiles",
        referred_columns=("id", "project_id"),
        name="fk_jobs_extraction_profile_id_project_id_extraction_profiles",
    ),
    _ForeignKeySpec(
        table_name="jobs",
        constrained_columns=("project_id",),
        referred_table="projects",
        referred_columns=("id",),
        name="fk_jobs_project_id_projects",
    ),
    _ForeignKeySpec(
        table_name="job_events",
        constrained_columns=("job_id",),
        referred_table="jobs",
        referred_columns=("id",),
        name="fk_job_events_job_id_jobs",
    ),
    _ForeignKeySpec(
        table_name="adapter_run_outputs",
        constrained_columns=("source_file_id", "project_id"),
        referred_table="files",
        referred_columns=("id", "project_id"),
        name="fk_adapter_run_outputs_source_file_id_project_id_files",
    ),
    _ForeignKeySpec(
        table_name="adapter_run_outputs",
        constrained_columns=("extraction_profile_id", "project_id"),
        referred_table="extraction_profiles",
        referred_columns=("id", "project_id"),
        name="fk_adapter_run_outputs_extraction_profile_proj_profiles",
    ),
    _ForeignKeySpec(
        table_name="adapter_run_outputs",
        constrained_columns=("project_id",),
        referred_table="projects",
        referred_columns=("id",),
        name="fk_adapter_run_outputs_project_id_projects",
    ),
    _ForeignKeySpec(
        table_name="adapter_run_outputs",
        constrained_columns=("source_job_id",),
        referred_table="jobs",
        referred_columns=("id",),
        name="fk_adapter_run_outputs_source_job_id_jobs",
    ),
    _ForeignKeySpec(
        table_name="drawing_revisions",
        constrained_columns=("source_file_id", "project_id"),
        referred_table="files",
        referred_columns=("id", "project_id"),
        name="fk_drawing_revisions_source_file_id_project_id_files",
    ),
    _ForeignKeySpec(
        table_name="drawing_revisions",
        constrained_columns=("extraction_profile_id", "project_id"),
        referred_table="extraction_profiles",
        referred_columns=("id", "project_id"),
        name="fk_drawing_revisions_extraction_profile_proj_profiles",
    ),
    _ForeignKeySpec(
        table_name="drawing_revisions",
        constrained_columns=("adapter_run_output_id", "project_id"),
        referred_table="adapter_run_outputs",
        referred_columns=("id", "project_id"),
        name="fk_drawing_revisions_adapter_run_output_id_project_id_outputs",
    ),
    _ForeignKeySpec(
        table_name="drawing_revisions",
        constrained_columns=("project_id",),
        referred_table="projects",
        referred_columns=("id",),
        name="fk_drawing_revisions_project_id_projects",
    ),
    _ForeignKeySpec(
        table_name="drawing_revisions",
        constrained_columns=("source_job_id",),
        referred_table="jobs",
        referred_columns=("id",),
        name="fk_drawing_revisions_source_job_id_jobs",
    ),
    _ForeignKeySpec(
        table_name="validation_reports",
        constrained_columns=("drawing_revision_id", "project_id"),
        referred_table="drawing_revisions",
        referred_columns=("id", "project_id"),
        name="fk_validation_reports_drawing_revision_id_project_id_revisions",
    ),
    _ForeignKeySpec(
        table_name="validation_reports",
        constrained_columns=("project_id",),
        referred_table="projects",
        referred_columns=("id",),
        name="fk_validation_reports_project_id_projects",
    ),
    _ForeignKeySpec(
        table_name="validation_reports",
        constrained_columns=("source_job_id",),
        referred_table="jobs",
        referred_columns=("id",),
        name="fk_validation_reports_source_job_id_jobs",
    ),
    _ForeignKeySpec(
        table_name="generated_artifacts",
        constrained_columns=("source_file_id", "project_id"),
        referred_table="files",
        referred_columns=("id", "project_id"),
        name="fk_generated_artifacts_source_file_id_project_id_files",
    ),
    _ForeignKeySpec(
        table_name="generated_artifacts",
        constrained_columns=("drawing_revision_id", "project_id"),
        referred_table="drawing_revisions",
        referred_columns=("id", "project_id"),
        name="fk_generated_artifacts_drawing_revision_id_project_id_revisions",
    ),
    _ForeignKeySpec(
        table_name="generated_artifacts",
        constrained_columns=("adapter_run_output_id", "project_id"),
        referred_table="adapter_run_outputs",
        referred_columns=("id", "project_id"),
        name="fk_generated_artifacts_adapter_run_output_id_project_id_outputs",
    ),
    _ForeignKeySpec(
        table_name="generated_artifacts",
        constrained_columns=("project_id",),
        referred_table="projects",
        referred_columns=("id",),
        name="fk_generated_artifacts_project_id_projects",
    ),
    _ForeignKeySpec(
        table_name="generated_artifacts",
        constrained_columns=("job_id",),
        referred_table="jobs",
        referred_columns=("id",),
        name="fk_generated_artifacts_job_id_jobs",
    ),
)

_DOWNGRADE_GUARD_TABLES: tuple[str, ...] = tuple(
    sorted(
        {spec.table_name for spec in _LINEAGE_FOREIGN_KEYS}
        | {spec.referred_table for spec in _LINEAGE_FOREIGN_KEYS}
    )
)


def _find_fk_name(spec: _ForeignKeySpec) -> str:
    """Resolve the currently installed foreign key name for a spec."""

    inspector = sa.inspect(op.get_bind())
    for foreign_key in inspector.get_foreign_keys(spec.table_name):
        if (
            tuple(foreign_key["constrained_columns"]) == spec.constrained_columns
            and foreign_key["referred_table"] == spec.referred_table
            and tuple(foreign_key["referred_columns"]) == spec.referred_columns
        ):
            foreign_key_name = foreign_key.get("name")
            if foreign_key_name is None:
                raise RuntimeError(
                    "Cannot replace foreign key without a name for "
                    f"{spec.table_name}({', '.join(spec.constrained_columns)})"
                )
            return foreign_key_name

    raise RuntimeError(
        "Expected foreign key not found for "
        f"{spec.table_name}({', '.join(spec.constrained_columns)}) -> "
        f"{spec.referred_table}({', '.join(spec.referred_columns)})"
    )


def _replace_foreign_keys(*, ondelete: str) -> None:
    """Recreate configured lineage foreign keys with the given delete action."""

    for spec in _LINEAGE_FOREIGN_KEYS:
        op.drop_constraint(_find_fk_name(spec), spec.table_name, type_="foreignkey")
        op.create_foreign_key(
            spec.name,
            spec.table_name,
            spec.referred_table,
            list(spec.constrained_columns),
            list(spec.referred_columns),
            ondelete=ondelete,
        )


def _assert_lineage_tables_empty_for_downgrade() -> None:
    """Refuse to restore destructive cascades while lineage data exists."""

    bind = op.get_bind()
    non_empty_tables: list[str] = []

    for table_name in _DOWNGRADE_GUARD_TABLES:
        has_rows = bind.execute(
            sa.select(sa.literal(1)).select_from(sa.table(table_name)).limit(1)
        ).first()
        if has_rows is not None:
            non_empty_tables.append(table_name)

    if non_empty_tables:
        joined_tables = ", ".join(non_empty_tables)
        raise RuntimeError(
            "Refusing to downgrade migration 2026_05_11_0010: restoring CASCADE "
            "lineage foreign keys can destroy persisted lineage data. Empty the "
            f"following tables before retrying: {joined_tables}."
        )


def upgrade() -> None:
    """Replace destructive lineage cascades with restrictive delete policies."""

    _replace_foreign_keys(ondelete="RESTRICT")


def downgrade() -> None:
    """Restore lineage foreign keys to cascading deletes."""

    _assert_lineage_tables_empty_for_downgrade()
    _replace_foreign_keys(ondelete="CASCADE")
