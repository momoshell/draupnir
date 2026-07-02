"""add rooms job type

Revision ID: 2026_07_01_0044
Revises: 2026_07_01_0043
Create Date: 2026-07-01 00:00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_07_01_0044"
down_revision: str | None = "2026_07_01_0043"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

# Current job type lists (after 2026_06_23_0041).
_PREV_JOB_TYPE_VALUES: tuple[str, ...] = (
    "ingest",
    "reprocess",
    "quantity_takeoff",
    "estimate",
    "export",
    "changeset_apply",
    "centerline",
)
_PREV_BASE_REQUIRED_JOB_TYPE_VALUES: tuple[str, ...] = (
    "reprocess",
    "quantity_takeoff",
    "estimate",
    "export",
    "changeset_apply",
    "centerline",
)
_PREV_EXTRACTION_PROFILE_FORBIDDEN_JOB_TYPE_VALUES: tuple[str, ...] = (
    "quantity_takeoff",
    "estimate",
    "export",
    "changeset_apply",
    "centerline",
)

# New job type lists with rooms added.
_NEW_JOB_TYPE_VALUES: tuple[str, ...] = (
    "ingest",
    "reprocess",
    "quantity_takeoff",
    "estimate",
    "export",
    "changeset_apply",
    "centerline",
    "rooms",
)
_NEW_BASE_REQUIRED_JOB_TYPE_VALUES: tuple[str, ...] = (
    "reprocess",
    "quantity_takeoff",
    "estimate",
    "export",
    "changeset_apply",
    "centerline",
    "rooms",
)
_NEW_EXTRACTION_PROFILE_FORBIDDEN_JOB_TYPE_VALUES: tuple[str, ...] = (
    "quantity_takeoff",
    "estimate",
    "export",
    "changeset_apply",
    "centerline",
    "rooms",
)


def _sql_in_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def _fail_if_rooms_jobs_exist() -> None:
    """Refuse downgrade while rooms jobs exist."""

    bind = op.get_bind()

    has_rooms_jobs = bool(
        bind.execute(sa.text("SELECT 1 FROM jobs WHERE job_type = 'rooms' LIMIT 1")).scalar()
    )

    if has_rooms_jobs:
        raise RuntimeError("cannot downgrade 2026_07_01_0044: populated rooms_jobs present")


def upgrade() -> None:
    """Extend job type constraints to allow the rooms job type."""

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


def downgrade() -> None:
    """Revert job type check constraints to drop the rooms job type."""

    _fail_if_rooms_jobs_exist()

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
