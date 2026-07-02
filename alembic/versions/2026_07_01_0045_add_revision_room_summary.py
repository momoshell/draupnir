"""add revision room summary

Revision ID: 2026_07_01_0045
Revises: 2026_07_01_0044
Create Date: 2026-07-01 00:00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_07_01_0045"
down_revision: str | None = "2026_07_01_0044"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

_TABLE_NAME = "revision_room_summary"
_ROOMS_TABLE_NAME = "revision_rooms"
_ROW_GUARD_FUNCTION_NAME = "enforce_append_only_lineage_row"
_TRUNCATE_GUARD_FUNCTION_NAME = "enforce_append_only_lineage_truncate"
_ROW_TRIGGER_NAME = "trg_append_only_row_guard"
_TRUNCATE_TRIGGER_NAME = "trg_append_only_truncate_guard"


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


def _fail_if_summaries_exist() -> None:
    """Refuse downgrade while revision_room_summary rows exist."""

    bind = op.get_bind()

    # EXISTS (fail-fast) rather than count(*) full scans — the guard only needs presence, and
    # revision_room_summary may be large on a populated database.
    has_summaries = bool(bind.execute(sa.text(f"SELECT 1 FROM {_TABLE_NAME} LIMIT 1")).scalar())

    if has_summaries:
        raise RuntimeError(f"cannot downgrade 2026_07_01_0045: populated {_TABLE_NAME} present")


def upgrade() -> None:
    """Create revision_room_summary table and add assigned_device_ids_json to revision_rooms."""

    op.create_table(
        _TABLE_NAME,
        sa.Column(
            "id",
            sa.Uuid(),
            nullable=False,
            comment="Unique room summary row identifier (UUID v4)",
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
            comment="Immutable source file identifier for this room summary row",
        ),
        sa.Column(
            "extraction_profile_id",
            sa.Uuid(),
            nullable=True,
            comment="Immutable extraction profile identifier used for this room summary row",
        ),
        sa.Column(
            "source_job_id",
            sa.Uuid(),
            nullable=False,
            comment="Job identifier that produced this room summary row",
        ),
        sa.Column(
            "drawing_revision_id",
            sa.Uuid(),
            nullable=False,
            comment="Drawing revision identifier that owns this room summary row",
        ),
        sa.Column(
            "adapter_run_output_id",
            sa.Uuid(),
            nullable=True,
            comment="Adapter run output identifier that produced this room summary row",
        ),
        sa.Column(
            "canonical_entity_schema_version",
            sa.String(length=16),
            nullable=False,
            comment="Canonical entity schema version for this room summary row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="Room summary row creation timestamp",
        ),
        sa.Column(
            "algo_version",
            sa.String(length=32),
            nullable=False,
            comment="Room extraction algorithm version string",
        ),
        sa.Column(
            "full_registry_size",
            sa.Integer(),
            nullable=False,
            comment="Total size of the interpretation room registry before genuine filtering",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_revision_room_summary_project_id_projects",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            name="fk_revision_room_summary_source_file_project_files",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            name="fk_revision_room_summary_profile_project_profiles",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_job_id"],
            ["jobs.id"],
            name="fk_revision_room_summary_source_job_id_jobs",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["drawing_revision_id", "project_id"],
            ["drawing_revisions.id", "drawing_revisions.project_id"],
            name="fk_revision_room_summary_revision_project_revisions",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["adapter_run_output_id", "project_id"],
            ["adapter_run_outputs.id", "adapter_run_outputs.project_id"],
            name="fk_revision_room_summary_output_project_outputs",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "id",
            "project_id",
            name="uq_revision_room_summary_id_project_id",
        ),
        sa.UniqueConstraint(
            "drawing_revision_id",
            "algo_version",
            name="uq_revision_room_summary_version",
        ),
    )

    op.create_index(
        op.f("ix_revision_room_summary_project_id"),
        _TABLE_NAME,
        ["project_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_revision_room_summary_source_file_id"),
        _TABLE_NAME,
        ["source_file_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_revision_room_summary_extraction_profile_id"),
        _TABLE_NAME,
        ["extraction_profile_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_revision_room_summary_source_job_id"),
        _TABLE_NAME,
        ["source_job_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_revision_room_summary_adapter_run_output_id"),
        _TABLE_NAME,
        ["adapter_run_output_id"],
        unique=False,
    )

    _attach_append_only_triggers()

    op.add_column(
        _ROOMS_TABLE_NAME,
        sa.Column(
            "assigned_device_ids_json",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'[]'::json"),
            comment=(
                "device_ids assigned to this genuine room (persisted from live "
                "result.device_assignments; expanded on read)."
            ),
        ),
    )

    op.add_column(
        _ROOMS_TABLE_NAME,
        sa.Column(
            "ordinal",
            sa.Integer(),
            nullable=False,
            server_default="0",
            comment=(
                "Position of this room in the genuine `interpret_rooms` order; read path "
                "orders by this to reproduce live item order."
            ),
        ),
    )


def downgrade() -> None:
    """Drop revision_room_summary table and the revision_rooms columns it added."""

    op.drop_column(_ROOMS_TABLE_NAME, "ordinal")
    op.drop_column(_ROOMS_TABLE_NAME, "assigned_device_ids_json")

    _fail_if_summaries_exist()

    _drop_append_only_triggers()

    op.drop_index(op.f("ix_revision_room_summary_adapter_run_output_id"), table_name=_TABLE_NAME)
    op.drop_index(op.f("ix_revision_room_summary_source_job_id"), table_name=_TABLE_NAME)
    op.drop_index(op.f("ix_revision_room_summary_extraction_profile_id"), table_name=_TABLE_NAME)
    op.drop_index(op.f("ix_revision_room_summary_source_file_id"), table_name=_TABLE_NAME)
    op.drop_index(op.f("ix_revision_room_summary_project_id"), table_name=_TABLE_NAME)
    op.drop_table(_TABLE_NAME)
