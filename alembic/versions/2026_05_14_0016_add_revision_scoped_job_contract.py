"""add revision scoped job contract

Revision ID: 2026_05_14_0016
Revises: 2026_05_13_0015
Create Date: 2026-05-14 12:55:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_05_14_0016"
down_revision: str | None = "2026_05_13_0015"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def _require_no_revision_scoped_rows_before_downgrade() -> None:
    """Refuse downgrade while new revision-scoped job semantics still exist."""

    bind = op.get_bind()
    quantity_takeoff_jobs_exist = bool(
        bind.execute(
            sa.text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM jobs
                    WHERE job_type = 'quantity_takeoff'
                )
                """
            )
        ).scalar()
    )
    if quantity_takeoff_jobs_exist:
        raise RuntimeError(
            "Manual data-preserving rollback is required before downgrading "
            "migration 2026_05_14_0016; quantity_takeoff jobs still depend on the "
            "revision-scoped job contract."
        )

    parented_jobs_exist = bool(
        bind.execute(
            sa.text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM jobs
                    WHERE parent_job_id IS NOT NULL
                )
                """
            )
        ).scalar()
    )
    if parented_jobs_exist:
        raise RuntimeError(
            "Manual data-preserving rollback is required before downgrading "
            "migration 2026_05_14_0016; parent_job_id lineage rows still depend on "
            "the revision-scoped job contract."
        )


def upgrade() -> None:
    """Constrain jobs to revision-scoped base revisions and parent lineage."""

    op.alter_column(
        "jobs",
        "base_revision_id",
        existing_type=sa.Uuid(),
        comment=(
            "Pinned latest finalized drawing revision captured when a revision-"
            "scoped job was created. Null for initial ingest jobs."
        ),
    )
    op.create_unique_constraint(
        "uq_drawing_revisions_id_project_id_source_file_id",
        "drawing_revisions",
        ["id", "project_id", "source_file_id"],
    )
    op.create_unique_constraint(
        "uq_jobs_id_project_id_file_id",
        "jobs",
        ["id", "project_id", "file_id"],
    )

    op.drop_constraint(
        "fk_jobs_base_revision_id_drawing_revisions",
        "jobs",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_jobs_base_revision_id_project_id_file_id_drawing_revisions",
        "jobs",
        "drawing_revisions",
        ["base_revision_id", "project_id", "file_id"],
        ["id", "project_id", "source_file_id"],
        ondelete="RESTRICT",
    )

    op.add_column(
        "jobs",
        sa.Column(
            "parent_job_id",
            sa.Uuid(),
            nullable=True,
            comment="Optional parent job identifier for same-project, same-file job lineage.",
        ),
    )
    op.create_index("ix_jobs_parent_job_id", "jobs", ["parent_job_id"], unique=False)
    op.create_foreign_key(
        "fk_jobs_parent_job_id_project_id_file_id_jobs",
        "jobs",
        "jobs",
        ["parent_job_id", "project_id", "file_id"],
        ["id", "project_id", "file_id"],
        ondelete="RESTRICT",
    )

    op.drop_constraint("ck_jobs_job_type_valid", "jobs", type_="check")
    op.create_check_constraint(
        "ck_jobs_job_type_valid",
        "jobs",
        "job_type IN ('ingest', 'reprocess', 'quantity_takeoff')",
    )

    op.drop_constraint("ck_jobs_reprocess_base_revision_required", "jobs", type_="check")
    op.create_check_constraint(
        "ck_jobs_reprocess_base_revision_required",
        "jobs",
        "job_type NOT IN ('reprocess', 'quantity_takeoff') OR base_revision_id IS NOT NULL",
    )

    op.create_check_constraint(
        "ck_jobs_quantity_takeoff_extraction_profile_forbidden",
        "jobs",
        "job_type != 'quantity_takeoff' OR extraction_profile_id IS NULL",
    )
    op.create_check_constraint(
        "ck_jobs_parent_job_id_not_self",
        "jobs",
        "parent_job_id IS NULL OR parent_job_id != id",
    )


def downgrade() -> None:
    """Drop revision-scoped job constraints and parent lineage support."""

    _require_no_revision_scoped_rows_before_downgrade()

    op.alter_column(
        "jobs",
        "base_revision_id",
        existing_type=sa.Uuid(),
        comment=(
            "Pinned latest finalized drawing revision captured when a reprocess "
            "job was created. Null for initial ingest jobs."
        ),
    )

    op.drop_constraint("ck_jobs_parent_job_id_not_self", "jobs", type_="check")
    op.drop_constraint(
        "ck_jobs_quantity_takeoff_extraction_profile_forbidden",
        "jobs",
        type_="check",
    )

    op.drop_constraint("ck_jobs_reprocess_base_revision_required", "jobs", type_="check")
    op.create_check_constraint(
        "ck_jobs_reprocess_base_revision_required",
        "jobs",
        "job_type != 'reprocess' OR base_revision_id IS NOT NULL",
    )

    op.drop_constraint("ck_jobs_job_type_valid", "jobs", type_="check")
    op.create_check_constraint(
        "ck_jobs_job_type_valid",
        "jobs",
        "job_type IN ('ingest', 'reprocess')",
    )

    op.drop_constraint(
        "fk_jobs_parent_job_id_project_id_file_id_jobs",
        "jobs",
        type_="foreignkey",
    )
    op.drop_index("ix_jobs_parent_job_id", table_name="jobs")
    op.drop_column("jobs", "parent_job_id")

    op.drop_constraint(
        "fk_jobs_base_revision_id_project_id_file_id_drawing_revisions",
        "jobs",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_jobs_base_revision_id_drawing_revisions",
        "jobs",
        "drawing_revisions",
        ["base_revision_id"],
        ["id"],
        ondelete="RESTRICT",
    )

    op.drop_constraint("uq_jobs_id_project_id_file_id", "jobs", type_="unique")
    op.drop_constraint(
        "uq_drawing_revisions_id_project_id_source_file_id",
        "drawing_revisions",
        type_="unique",
    )
