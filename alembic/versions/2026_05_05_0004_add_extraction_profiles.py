"""add extraction profiles

Revision ID: 2026_05_05_0004
Revises: 2026_05_02_0003
Create Date: 2026-05-05 17:10:00
"""

import uuid
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_05_05_0004"
down_revision: str | None = "2026_05_02_0003"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    """Create immutable extraction profiles and thread them through jobs."""
    op.create_table(
        "extraction_profiles",
        sa.Column(
            "id",
            sa.Uuid(),
            nullable=False,
            comment="Unique extraction profile identifier (UUID v4)",
        ),
        sa.Column(
            "project_id",
            sa.Uuid(),
            nullable=False,
            comment="Owning project identifier",
        ),
        sa.Column(
            "profile_version",
            sa.String(length=16),
            nullable=False,
            comment="Extraction profile schema version",
        ),
        sa.Column(
            "units_override",
            sa.String(length=64),
            nullable=True,
            comment="Optional extraction units override",
        ),
        sa.Column(
            "layout_mode",
            sa.String(length=64),
            nullable=False,
            comment="Layout extraction mode",
        ),
        sa.Column(
            "xref_handling",
            sa.String(length=64),
            nullable=False,
            comment="External reference handling mode",
        ),
        sa.Column(
            "block_handling",
            sa.String(length=64),
            nullable=False,
            comment="Block extraction handling mode",
        ),
        sa.Column(
            "text_extraction",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
            comment="Whether text extraction is enabled",
        ),
        sa.Column(
            "dimension_extraction",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
            comment="Whether dimension extraction is enabled",
        ),
        sa.Column(
            "pdf_page_range",
            sa.String(length=255),
            nullable=True,
            comment="Optional PDF page range selector",
        ),
        sa.Column(
            "raster_calibration",
            sa.JSON(),
            nullable=True,
            comment="Optional raster calibration payload",
        ),
        sa.Column(
            "confidence_threshold",
            sa.Float(),
            nullable=False,
            server_default=sa.text("0.6"),
            comment="Minimum confidence threshold for extracted results",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="Extraction profile creation timestamp",
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("id", "project_id", name="uq_extraction_profiles_id_project_id"),
    )
    op.create_index(
        op.f("ix_extraction_profiles_project_id"),
        "extraction_profiles",
        ["project_id"],
        unique=False,
    )

    op.add_column(
        "jobs",
        sa.Column(
            "extraction_profile_id",
            sa.Uuid(),
            nullable=True,
            comment="Immutable extraction profile identifier",
        ),
    )
    op.add_column(
        "files",
        sa.Column(
            "initial_job_id",
            sa.Uuid(),
            nullable=True,
            comment="Initial ingest job identifier created during upload",
        ),
    )
    op.add_column(
        "files",
        sa.Column(
            "initial_extraction_profile_id",
            sa.Uuid(),
            nullable=True,
            comment="Initial extraction profile identifier created during upload",
        ),
    )

    bind = op.get_bind()
    existing_files = bind.execute(sa.text("SELECT id, project_id FROM files")).mappings().all()
    for file_row in existing_files:
        profile_id = uuid.uuid4()
        initial_job_row = bind.execute(
            sa.text(
                """
                SELECT id
                FROM jobs
                WHERE file_id = :file_id
                  AND project_id = :project_id
                  AND job_type = 'ingest'
                ORDER BY created_at ASC, id ASC
                LIMIT 1
                """
            ),
            {
                "file_id": file_row["id"],
                "project_id": file_row["project_id"],
            },
        ).mappings().first()
        bind.execute(
            sa.text(
                """
                INSERT INTO extraction_profiles (
                    id,
                    project_id,
                    profile_version,
                    units_override,
                    layout_mode,
                    xref_handling,
                    block_handling,
                    text_extraction,
                    dimension_extraction,
                    pdf_page_range,
                    raster_calibration,
                    confidence_threshold
                ) VALUES (
                    :id,
                    :project_id,
                    :profile_version,
                    :units_override,
                    :layout_mode,
                    :xref_handling,
                    :block_handling,
                    :text_extraction,
                    :dimension_extraction,
                    :pdf_page_range,
                    :raster_calibration,
                    :confidence_threshold
                )
                """
            ),
            {
                "id": profile_id,
                "project_id": file_row["project_id"],
                "profile_version": "v0.1",
                "units_override": None,
                "layout_mode": "auto",
                "xref_handling": "preserve",
                "block_handling": "expand",
                "text_extraction": True,
                "dimension_extraction": True,
                "pdf_page_range": None,
                "raster_calibration": None,
                "confidence_threshold": 0.6,
            },
        )
        bind.execute(
            sa.text(
                """
                UPDATE jobs
                SET extraction_profile_id = :profile_id
                WHERE file_id = :file_id AND project_id = :project_id
                """
            ),
            {
                "profile_id": profile_id,
                "file_id": file_row["id"],
                "project_id": file_row["project_id"],
            },
        )
        bind.execute(
            sa.text(
                """
                UPDATE files
                SET initial_job_id = :initial_job_id,
                    initial_extraction_profile_id = :profile_id
                WHERE id = :file_id AND project_id = :project_id
                """
            ),
            {
                "initial_job_id": None if initial_job_row is None else initial_job_row["id"],
                "profile_id": profile_id,
                "file_id": file_row["id"],
                "project_id": file_row["project_id"],
            },
        )

    # Keep this column nullable for the expand/rollback window so mixed-version
    # nodes and old app binaries can still create jobs. A later contract
    # migration can enforce NOT NULL after the rollback window closes.
    op.create_index(
        op.f("ix_jobs_extraction_profile_id"),
        "jobs",
        ["extraction_profile_id"],
        unique=False,
    )
    op.create_foreign_key(
        "fk_jobs_extraction_profile_id_project_id_extraction_profiles",
        "jobs",
        "extraction_profiles",
        ["extraction_profile_id", "project_id"],
        ["id", "project_id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    """Drop immutable extraction profiles and job references."""
    bind = op.get_bind()
    profile_rows_exist = bool(
        bind.execute(sa.text("SELECT EXISTS (SELECT 1 FROM extraction_profiles)")).scalar()
    )
    if profile_rows_exist:
        raise RuntimeError(
            "Cannot downgrade revision 2026_05_05_0004 while extraction_profiles contains "
            "rows. Manual data-preserving rollback is required."
        )

    op.drop_constraint(
        "fk_jobs_extraction_profile_id_project_id_extraction_profiles",
        "jobs",
        type_="foreignkey",
    )
    op.drop_index(op.f("ix_jobs_extraction_profile_id"), table_name="jobs")
    op.drop_column("jobs", "extraction_profile_id")
    op.drop_column("files", "initial_extraction_profile_id")
    op.drop_column("files", "initial_job_id")
    op.drop_index(op.f("ix_extraction_profiles_project_id"), table_name="extraction_profiles")
    op.drop_table("extraction_profiles")
