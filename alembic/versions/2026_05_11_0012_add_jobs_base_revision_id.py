"""add jobs base revision pin

Revision ID: 2026_05_11_0012
Revises: 2026_05_11_0011
Create Date: 2026-05-11 15:45:00
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_05_11_0012"
down_revision: str | None = "2026_05_11_0011"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def _require_no_pending_legacy_reprocess_jobs() -> None:
    """Refuse upgrade when legacy queued reprocess jobs cannot be pinned safely."""

    bind = op.get_bind()
    legacy_job_count = int(
        bind.execute(
            sa.text(
                """
                SELECT COUNT(*)
                FROM jobs
                JOIN files
                  ON files.id = jobs.file_id
                WHERE jobs.job_type = 'ingest'
                  AND jobs.id != files.initial_job_id
                  AND jobs.status IN ('pending', 'running')
                """
            )
        ).scalar()
        or 0
    )
    if legacy_job_count:
        raise RuntimeError(
            "Drain or cancel pending/running legacy pre-#133 reprocess jobs before "
            "upgrading; jobs.base_revision_id cannot be pinned safely for those rows."
        )


def _require_no_pending_reprocess_jobs_before_downgrade() -> None:
    """Refuse downgrade while reprocess jobs still rely on stale-base fencing."""

    bind = op.get_bind()
    pending_reprocess_jobs_exist = bool(
        bind.execute(
            sa.text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM jobs
                    WHERE job_type = 'reprocess'
                      AND status IN ('pending', 'running')
                )
                """
            )
        ).scalar()
    )
    if pending_reprocess_jobs_exist:
        raise RuntimeError(
            "Manual data-preserving rollback is required before dropping "
            "jobs.base_revision_id; pending/running reprocess jobs still rely on "
            "stale-base fencing."
        )


def upgrade() -> None:
    """Persist the pinned finalized base revision for reprocess jobs."""

    _require_no_pending_legacy_reprocess_jobs()

    op.add_column(
        "jobs",
        sa.Column(
            "base_revision_id",
            sa.Uuid(),
            nullable=True,
            comment=(
                "Pinned latest finalized drawing revision captured when a reprocess "
                "job was created. Null for initial ingest jobs."
            ),
        ),
    )
    op.create_index("ix_jobs_base_revision_id", "jobs", ["base_revision_id"], unique=False)
    op.create_foreign_key(
        "fk_jobs_base_revision_id_drawing_revisions",
        "jobs",
        "drawing_revisions",
        ["base_revision_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.execute(
        sa.text(
            """
            ALTER TABLE jobs
            ADD CONSTRAINT ck_jobs_reprocess_base_revision_required
            CHECK (job_type != 'reprocess' OR base_revision_id IS NOT NULL)
            NOT VALID
            """
        )
    )
    op.execute(
        sa.text(
            """
            ALTER TABLE jobs
            ADD CONSTRAINT ck_jobs_ingest_base_revision_forbidden
            CHECK (job_type != 'ingest' OR base_revision_id IS NULL)
            NOT VALID
            """
        )
    )


def downgrade() -> None:
    """Drop the persisted reprocess base revision pin."""

    _require_no_pending_reprocess_jobs_before_downgrade()

    op.drop_constraint(
        "ck_jobs_ingest_base_revision_forbidden",
        "jobs",
        type_="check",
    )
    op.drop_constraint(
        "ck_jobs_reprocess_base_revision_required",
        "jobs",
        type_="check",
    )
    op.drop_constraint(
        "fk_jobs_base_revision_id_drawing_revisions",
        "jobs",
        type_="foreignkey",
    )
    op.drop_index("ix_jobs_base_revision_id", table_name="jobs")
    op.drop_column("jobs", "base_revision_id")
