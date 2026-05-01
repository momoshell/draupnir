"""Add files and jobs tables.

Revision ID: 2026_05_01_0002
Revises: 2026_05_01_0001
Create Date: 2026-05-01 00:02:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_05_01_0002"
down_revision: str | None = "2026_05_01_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Apply migration changes for files and jobs."""
    op.create_table(
        "files",
        sa.Column(
            "id",
            sa.Uuid(),
            nullable=False,
            comment="Unique file identifier (UUID v4)",
        ),
        sa.Column(
            "project_id",
            sa.Uuid(),
            nullable=False,
            comment="Owning project identifier",
        ),
        sa.Column(
            "original_filename",
            sa.String(length=512),
            nullable=False,
            comment="Original uploaded filename",
        ),
        sa.Column(
            "media_type",
            sa.String(length=255),
            nullable=False,
            comment="Uploaded media type",
        ),
        sa.Column(
            "detected_format",
            sa.String(length=32),
            nullable=True,
            comment="Detected drawing/document format",
        ),
        sa.Column(
            "storage_uri",
            sa.String(length=1024),
            nullable=False,
            comment="Immutable local/object storage URI",
        ),
        sa.Column(
            "size_bytes",
            sa.Integer(),
            nullable=False,
            comment="Uploaded file size in bytes",
        ),
        sa.Column(
            "checksum_sha256",
            sa.String(length=64),
            nullable=False,
            comment="SHA-256 checksum of uploaded bytes",
        ),
        sa.Column(
            "immutable",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
            comment="Whether the original upload is immutable",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="File creation timestamp",
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("id", "project_id", name="uq_files_id_project_id"),
    )
    op.create_index(
        op.f("ix_files_project_id"),
        "files",
        ["project_id"],
        unique=False,
    )
    op.create_index(
        "ix_files_project_id_created_at_id_desc",
        "files",
        ["project_id", sa.text("created_at DESC"), sa.text("id DESC")],
        unique=False,
    )

    op.create_table(
        "jobs",
        sa.Column(
            "id",
            sa.Uuid(),
            nullable=False,
            comment="Unique job identifier (UUID v4)",
        ),
        sa.Column(
            "project_id",
            sa.Uuid(),
            nullable=False,
            comment="Owning project identifier",
        ),
        sa.Column(
            "file_id",
            sa.Uuid(),
            nullable=False,
            comment="Associated file identifier",
        ),
        sa.Column(
            "job_type",
            sa.String(length=64),
            nullable=False,
            comment="Job type (e.g. ingest)",
        ),
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            comment="Job status (e.g. pending, running, failed, succeeded)",
        ),
        sa.Column(
            "attempts",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
            comment="Current attempt count",
        ),
        sa.Column(
            "max_attempts",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("3"),
            comment="Maximum retry attempts",
        ),
        sa.Column(
            "cancel_requested",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
            comment="Whether cancellation was requested",
        ),
        sa.Column(
            "error_code",
            sa.String(length=128),
            nullable=True,
            comment="Machine-readable error code",
        ),
        sa.Column(
            "error_message",
            sa.String(length=2048),
            nullable=True,
            comment="Human-readable error message",
        ),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="Job start timestamp",
        ),
        sa.Column(
            "finished_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="Job completion timestamp",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="Job creation timestamp",
        ),
        sa.ForeignKeyConstraint(
            ["file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="CASCADE",
            name="fk_jobs_file_id_project_id_files",
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_jobs_file_id"), "jobs", ["file_id"], unique=False)
    op.create_index(
        op.f("ix_jobs_project_id"),
        "jobs",
        ["project_id"],
        unique=False,
    )


def downgrade() -> None:
    """Revert migration changes for files and jobs."""
    op.drop_index(op.f("ix_jobs_project_id"), table_name="jobs")
    op.drop_index(op.f("ix_jobs_file_id"), table_name="jobs")
    op.drop_table("jobs")
    op.drop_index("ix_files_project_id_created_at_id_desc", table_name="files")
    op.drop_index(op.f("ix_files_project_id"), table_name="files")
    op.drop_table("files")
