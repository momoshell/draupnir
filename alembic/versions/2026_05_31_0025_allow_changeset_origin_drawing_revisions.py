"""allow changeset-origin drawing revisions

Revision ID: 2026_05_31_0025
Revises: 2026_05_27_0024
Create Date: 2026-05-31 00:25:00
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "2026_05_31_0025"
down_revision = "2026_05_27_0024"
branch_labels = None
depends_on = None


_MATERIALIZATION_TABLES = (
    "revision_entity_manifests",
    "revision_layouts",
    "revision_layers",
    "revision_blocks",
    "revision_entities",
)


def _ensure_no_incompatible_rows_for_downgrade() -> None:
    bind = op.get_bind()
    drawing_revisions = sa.table(
        "drawing_revisions",
        sa.column("revision_kind"),
        sa.column("changeset_id"),
        sa.column("extraction_profile_id"),
        sa.column("adapter_run_output_id"),
    )

    drawing_revisions_incompatible = bind.execute(
        sa.select(sa.literal(1))
        .select_from(drawing_revisions)
        .where(
            sa.or_(
                drawing_revisions.c.revision_kind == "changeset",
                drawing_revisions.c.changeset_id.is_not(None),
                drawing_revisions.c.extraction_profile_id.is_(None),
                drawing_revisions.c.adapter_run_output_id.is_(None),
            )
        )
        .limit(1)
    ).scalar_one_or_none()
    if drawing_revisions_incompatible is not None:
        raise RuntimeError(
            "Cannot downgrade: drawing_revisions contains rows incompatible with "
            "ingest/reprocess-only origin constraints."
        )

    for table_name in _MATERIALIZATION_TABLES:
        table = sa.table(
            table_name,
            sa.column("extraction_profile_id"),
            sa.column("adapter_run_output_id"),
        )
        incompatible = bind.execute(
            sa.select(sa.literal(1))
            .select_from(table)
            .where(
                sa.or_(
                    table.c.extraction_profile_id.is_(None),
                    table.c.adapter_run_output_id.is_(None),
                )
            )
            .limit(1)
        ).scalar_one_or_none()
        if incompatible is not None:
            raise RuntimeError(
                f"Cannot downgrade: {table_name} contains rows incompatible with "
                "non-null origin columns."
            )


def upgrade() -> None:
    op.add_column(
        "drawing_revisions",
        sa.Column(
            "changeset_id",
            sa.Uuid(),
            nullable=True,
            comment=(
                "Forward-reference changeset identifier for changeset-origin drawing revisions"
            ),
        ),
    )
    op.alter_column(
        "drawing_revisions",
        "extraction_profile_id",
        existing_type=sa.Uuid(),
        nullable=True,
    )
    op.alter_column(
        "drawing_revisions",
        "adapter_run_output_id",
        existing_type=sa.Uuid(),
        nullable=True,
    )
    op.create_unique_constraint(
        "uq_drawing_revisions_changeset_id",
        "drawing_revisions",
        ["changeset_id"],
    )
    op.drop_constraint("ck_drawing_revisions_kind", "drawing_revisions", type_="check")
    op.create_check_constraint(
        "ck_drawing_revisions_kind",
        "drawing_revisions",
        "revision_kind IN ('ingest', 'reprocess', 'changeset')",
    )
    op.create_check_constraint(
        "ck_drawing_revisions_origin_fields",
        "drawing_revisions",
        "(revision_kind = 'changeset' AND predecessor_revision_id IS NOT NULL "
        "AND changeset_id IS NOT NULL "
        "AND extraction_profile_id IS NULL AND adapter_run_output_id IS NULL) "
        "OR (revision_kind IN ('ingest', 'reprocess') AND changeset_id IS NULL "
        "AND extraction_profile_id IS NOT NULL AND adapter_run_output_id IS NOT NULL)",
    )

    for table_name in _MATERIALIZATION_TABLES:
        op.alter_column(
            table_name,
            "extraction_profile_id",
            existing_type=sa.Uuid(),
            nullable=True,
        )
        op.alter_column(
            table_name,
            "adapter_run_output_id",
            existing_type=sa.Uuid(),
            nullable=True,
        )


def downgrade() -> None:
    _ensure_no_incompatible_rows_for_downgrade()

    for table_name in reversed(_MATERIALIZATION_TABLES):
        op.alter_column(
            table_name,
            "adapter_run_output_id",
            existing_type=sa.Uuid(),
            nullable=False,
        )
        op.alter_column(
            table_name,
            "extraction_profile_id",
            existing_type=sa.Uuid(),
            nullable=False,
        )

    op.drop_constraint(
        "ck_drawing_revisions_origin_fields",
        "drawing_revisions",
        type_="check",
    )
    op.drop_constraint("ck_drawing_revisions_kind", "drawing_revisions", type_="check")
    op.drop_constraint(
        "uq_drawing_revisions_changeset_id",
        "drawing_revisions",
        type_="unique",
    )
    op.alter_column(
        "drawing_revisions",
        "adapter_run_output_id",
        existing_type=sa.Uuid(),
        nullable=False,
    )
    op.alter_column(
        "drawing_revisions",
        "extraction_profile_id",
        existing_type=sa.Uuid(),
        nullable=False,
    )
    op.drop_column("drawing_revisions", "changeset_id")
    op.create_check_constraint(
        "ck_drawing_revisions_kind",
        "drawing_revisions",
        "revision_kind IN ('ingest', 'reprocess')",
    )
