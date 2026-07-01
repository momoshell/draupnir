"""add revision rooms

Revision ID: 2026_07_01_0043
Revises: 2026_07_01_0042
Create Date: 2026-07-01 00:00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_07_01_0043"
down_revision: str | None = "2026_07_01_0042"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

_TABLE_NAME = "revision_rooms"
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


def _fail_if_rooms_exist() -> None:
    """Refuse downgrade while revision_rooms rows exist."""

    bind = op.get_bind()

    # EXISTS (fail-fast) rather than count(*) full scans — the guard only needs presence, and
    # revision_rooms may be large on a populated database.
    has_rooms = bool(bind.execute(sa.text(f"SELECT 1 FROM {_TABLE_NAME} LIMIT 1")).scalar())

    if has_rooms:
        raise RuntimeError(f"cannot downgrade 2026_07_01_0043: populated {_TABLE_NAME} present")


def upgrade() -> None:
    """Create revision_rooms table."""

    op.create_table(
        _TABLE_NAME,
        sa.Column(
            "id",
            sa.Uuid(),
            nullable=False,
            comment="Unique room row identifier (UUID v4)",
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
            comment="Immutable source file identifier for this room row",
        ),
        sa.Column(
            "extraction_profile_id",
            sa.Uuid(),
            nullable=True,
            comment="Immutable extraction profile identifier used for this room row",
        ),
        sa.Column(
            "source_job_id",
            sa.Uuid(),
            nullable=False,
            comment="Job identifier that produced this room row",
        ),
        sa.Column(
            "drawing_revision_id",
            sa.Uuid(),
            nullable=False,
            comment="Drawing revision identifier that owns this room row",
        ),
        sa.Column(
            "adapter_run_output_id",
            sa.Uuid(),
            nullable=True,
            comment="Adapter run output identifier that produced this room row",
        ),
        sa.Column(
            "canonical_entity_schema_version",
            sa.String(length=16),
            nullable=False,
            comment="Canonical entity schema version for this room row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="Room row creation timestamp",
        ),
        sa.Column(
            "algo_version",
            sa.String(length=32),
            nullable=False,
            comment="Room extraction algorithm version string",
        ),
        sa.Column(
            "room_key",
            sa.String(length=255),
            nullable=False,
            comment="Stable per-room identifier from the interpretation Room.id",
        ),
        sa.Column(
            "name",
            sa.String(length=255),
            nullable=True,
            comment="Room name, if present in the drawing",
        ),
        sa.Column(
            "number",
            sa.String(length=64),
            nullable=True,
            comment="Room number, if present in the drawing",
        ),
        sa.Column(
            "source",
            sa.String(length=32),
            nullable=False,
            comment="Room derivation source (e.g. polygon, label)",
        ),
        sa.Column(
            "area",
            sa.Float(),
            nullable=True,
            comment="Room area in drawing units squared; NULL for label-only rooms",
        ),
        sa.Column(
            "bounds_min_x",
            sa.Float(),
            nullable=True,
            comment="Room bounding-box minimum x; NULL for label-only rooms",
        ),
        sa.Column(
            "bounds_min_y",
            sa.Float(),
            nullable=True,
            comment="Room bounding-box minimum y; NULL for label-only rooms",
        ),
        sa.Column(
            "bounds_max_x",
            sa.Float(),
            nullable=True,
            comment="Room bounding-box maximum x; NULL for label-only rooms",
        ),
        sa.Column(
            "bounds_max_y",
            sa.Float(),
            nullable=True,
            comment="Room bounding-box maximum y; NULL for label-only rooms",
        ),
        sa.Column(
            "polygon_geometry_json",
            sa.JSON(),
            nullable=True,
            comment="Room polygon geometry payload; NULL for label-only rooms",
        ),
        sa.Column(
            "anchors_json",
            sa.JSON(),
            nullable=False,
            comment="All label anchor points for this (possibly merged) room",
        ),
        sa.Column(
            "needs_review",
            sa.Boolean(),
            nullable=False,
            comment="True when a duplicate room number was found across distinct polygon rooms",
        ),
        sa.Column(
            "confidence",
            sa.Float(),
            nullable=True,
            comment="Optional provenance confidence in [0, 1]; NULL means not scored",
        ),
        sa.Column(
            "strategy",
            sa.String(length=64),
            nullable=False,
            comment="Room-derivation strategy identifier",
        ),
        sa.Column(
            "source_layers_json",
            sa.JSON(),
            nullable=False,
            comment="Source layer references contributing to this room",
        ),
        sa.Column(
            "input_family",
            sa.String(length=32),
            nullable=True,
            comment="Input family of the source drawing (e.g. dwg, pdf_vector)",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_revision_rooms_project_id_projects",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            name="fk_revision_rooms_source_file_project_files",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            name="fk_revision_rooms_profile_project_profiles",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_job_id"],
            ["jobs.id"],
            name="fk_revision_rooms_source_job_id_jobs",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["drawing_revision_id", "project_id"],
            ["drawing_revisions.id", "drawing_revisions.project_id"],
            name="fk_revision_rooms_revision_project_revisions",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["adapter_run_output_id", "project_id"],
            ["adapter_run_outputs.id", "adapter_run_outputs.project_id"],
            name="fk_revision_rooms_output_project_outputs",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "id",
            "project_id",
            name="uq_revision_rooms_id_project_id",
        ),
        sa.UniqueConstraint(
            "drawing_revision_id",
            "room_key",
            "algo_version",
            name="uq_revision_rooms_group_version",
        ),
    )

    op.create_index(
        op.f("ix_revision_rooms_project_id"),
        _TABLE_NAME,
        ["project_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_revision_rooms_source_file_id"),
        _TABLE_NAME,
        ["source_file_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_revision_rooms_extraction_profile_id"),
        _TABLE_NAME,
        ["extraction_profile_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_revision_rooms_source_job_id"),
        _TABLE_NAME,
        ["source_job_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_revision_rooms_adapter_run_output_id"),
        _TABLE_NAME,
        ["adapter_run_output_id"],
        unique=False,
    )
    op.create_index(
        "ix_revision_rooms_revision_room",
        _TABLE_NAME,
        ["drawing_revision_id", "room_key"],
        unique=False,
    )

    _attach_append_only_triggers()


def downgrade() -> None:
    """Drop revision_rooms table."""

    _fail_if_rooms_exist()

    _drop_append_only_triggers()

    op.drop_index("ix_revision_rooms_revision_room", table_name=_TABLE_NAME)
    op.drop_index(op.f("ix_revision_rooms_adapter_run_output_id"), table_name=_TABLE_NAME)
    op.drop_index(op.f("ix_revision_rooms_source_job_id"), table_name=_TABLE_NAME)
    op.drop_index(op.f("ix_revision_rooms_extraction_profile_id"), table_name=_TABLE_NAME)
    op.drop_index(op.f("ix_revision_rooms_source_file_id"), table_name=_TABLE_NAME)
    op.drop_index(op.f("ix_revision_rooms_project_id"), table_name=_TABLE_NAME)
    op.drop_table(_TABLE_NAME)
