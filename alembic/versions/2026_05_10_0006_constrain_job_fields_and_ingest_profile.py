"""constrain job fields and ingest profile invariant

Revision ID: 2026_05_10_0006
Revises: 2026_05_05_0005
Create Date: 2026-05-10 12:30:00
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_05_10_0006"
down_revision: str | None = "2026_05_05_0005"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

_JOB_TYPE_VALUES = ("ingest", "reprocess")
_JOB_STATUS_VALUES = ("pending", "running", "succeeded", "failed", "cancelled")
_JOB_ERROR_CODE_VALUES = (
    "NOT_FOUND",
    "INVALID_CURSOR",
    "VALIDATION_ERROR",
    "INPUT_INVALID",
    "INPUT_UNSUPPORTED_FORMAT",
    "ADAPTER_UNAVAILABLE",
    "ADAPTER_TIMEOUT",
    "ADAPTER_FAILED",
    "STORAGE_FAILED",
    "DB_CONFLICT",
    "REVISION_CONFLICT",
    "JOB_CANCELLED",
    "INTERNAL_ERROR",
)
_PROFILE_REQUIRED_JOB_TYPE_VALUES = ("ingest", "reprocess")


def _sql_in_list(values: tuple[str, ...]) -> str:
    """Render a SQL string list for check constraints."""

    return ", ".join(f"'{value}'" for value in values)


def _profile_required_constraint_sql() -> str:
    """Render the extraction-profile invariant for persisted ingestion jobs."""

    return (
        "job_type NOT IN "
        f"({_sql_in_list(_PROFILE_REQUIRED_JOB_TYPE_VALUES)}) OR extraction_profile_id IS NOT NULL"
    )


def upgrade() -> None:
    """Constrain persisted job strings and ingest profile writes."""
    bind = op.get_bind()

    bind.execute(
        sa.text(
            """
            UPDATE jobs AS jobs
            SET extraction_profile_id = files.initial_extraction_profile_id
            FROM files
            WHERE jobs.file_id = files.id
              AND jobs.project_id = files.project_id
              AND jobs.job_type = 'ingest'
              AND jobs.extraction_profile_id IS NULL
              AND files.initial_extraction_profile_id IS NOT NULL
            """
        )
    )

    op.create_check_constraint(
        "ck_jobs_job_type_valid",
        "jobs",
        f"job_type IN ({_sql_in_list(_JOB_TYPE_VALUES)})",
    )
    op.create_check_constraint(
        "ck_jobs_status_valid",
        "jobs",
        f"status IN ({_sql_in_list(_JOB_STATUS_VALUES)})",
    )
    op.create_check_constraint(
        "ck_jobs_error_code_valid",
        "jobs",
        "error_code IS NULL "
        f"OR error_code IN ({_sql_in_list(_JOB_ERROR_CODE_VALUES)})",
    )
    op.execute(
        sa.text(
            """
            ALTER TABLE jobs
            ADD CONSTRAINT ck_jobs_ingest_extraction_profile_required
            CHECK ({constraint_sql})
            NOT VALID
            """
            .format(constraint_sql=_profile_required_constraint_sql())
        )
    )

    invalid_profile_required_jobs = bind.execute(
        sa.text(
            """
            SELECT COUNT(*)
            FROM jobs
            WHERE job_type IN ({job_type_values})
              AND extraction_profile_id IS NULL
            """
            .format(job_type_values=_sql_in_list(_PROFILE_REQUIRED_JOB_TYPE_VALUES))
        )
    ).scalar_one()
    if invalid_profile_required_jobs != 0:
        raise RuntimeError(
            "Migration 2026_05_10_0006 requires extraction_profile_id for persisted "
            "ingest/reprocess jobs, but "
            f"{invalid_profile_required_jobs} row(s) still have NULL extraction_profile_id "
            "after backfill. Populate jobs.extraction_profile_id (for ingest jobs, "
            "typically from files.initial_extraction_profile_id) before rerunning this "
            "migration."
        )

    op.execute(
        sa.text(
            """
            ALTER TABLE jobs
            VALIDATE CONSTRAINT ck_jobs_ingest_extraction_profile_required
            """
        )
    )


def downgrade() -> None:
    """Drop job field and ingest profile constraints."""
    op.drop_constraint(
        "ck_jobs_ingest_extraction_profile_required",
        "jobs",
        type_="check",
    )
    op.drop_constraint("ck_jobs_error_code_valid", "jobs", type_="check")
    op.drop_constraint("ck_jobs_status_valid", "jobs", type_="check")
    op.drop_constraint("ck_jobs_job_type_valid", "jobs", type_="check")
