"""add revision scoped entity materialization

Revision ID: 2026_05_13_0015
Revises: 2026_05_12_0014
Create Date: 2026-05-13 09:00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_05_13_0015"
down_revision: str | None = "2026_05_12_0014"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

_ROW_GUARD_FUNCTION_NAME = "enforce_append_only_lineage_row"
_TRUNCATE_GUARD_FUNCTION_NAME = "enforce_append_only_lineage_truncate"
_ROW_TRIGGER_NAME = "trg_append_only_row_guard"
_TRUNCATE_TRIGGER_NAME = "trg_append_only_truncate_guard"
_MATERIALIZATION_TABLES: tuple[str, ...] = (
    "revision_entity_manifests",
    "revision_layouts",
    "revision_layers",
    "revision_blocks",
    "revision_entities",
)


def _attach_append_only_triggers() -> None:
    """Attach existing append-only trigger functions to new lineage tables."""

    for table_name in _MATERIALIZATION_TABLES:
        op.execute(
            sa.text(
                f"""
                CREATE TRIGGER {_ROW_TRIGGER_NAME}
                BEFORE UPDATE OR DELETE ON \"{table_name}\"
                FOR EACH ROW
                EXECUTE FUNCTION {_ROW_GUARD_FUNCTION_NAME}()
                """
            )
        )
        op.execute(
            sa.text(
                f"""
                CREATE TRIGGER {_TRUNCATE_TRIGGER_NAME}
                BEFORE TRUNCATE ON \"{table_name}\"
                FOR EACH STATEMENT
                EXECUTE FUNCTION {_TRUNCATE_GUARD_FUNCTION_NAME}()
                """
            )
        )


def _drop_append_only_triggers() -> None:
    """Remove append-only triggers from new lineage tables."""

    for table_name in _MATERIALIZATION_TABLES:
        op.execute(sa.text(f'DROP TRIGGER IF EXISTS {_TRUNCATE_TRIGGER_NAME} ON "{table_name}"'))
        op.execute(sa.text(f'DROP TRIGGER IF EXISTS {_ROW_TRIGGER_NAME} ON "{table_name}"'))


def _assert_materialization_tables_empty_for_downgrade() -> None:
    """Refuse to drop materialized lineage tables while rows exist."""

    bind = op.get_bind()
    non_empty_tables: list[str] = []

    for table_name in _MATERIALIZATION_TABLES:
        has_rows = bind.execute(
            sa.select(sa.literal(1)).select_from(sa.table(table_name)).limit(1)
        ).first()
        if has_rows is not None:
            non_empty_tables.append(table_name)

    if non_empty_tables:
        raise RuntimeError(
            "Refusing to downgrade migration 2026_05_13_0015: dropping revision-scoped "
            "normalized entity materializations while rows exist would destroy append-only "
            "lineage. Empty the following tables before retrying: "
            f"{', '.join(non_empty_tables)}."
        )


def upgrade() -> None:
    """Create revision-scoped normalized entity materialization tables."""

    op.create_table(
        "revision_entity_manifests",
        sa.Column(
            "id",
            sa.Uuid(),
            nullable=False,
            comment="Unique revision entity materialization manifest identifier (UUID v4)",
        ),
        sa.Column("project_id", sa.Uuid(), nullable=False, comment="Owning project identifier"),
        sa.Column(
            "source_file_id",
            sa.Uuid(),
            nullable=False,
            comment="Immutable source file identifier for the materialized revision",
        ),
        sa.Column(
            "extraction_profile_id",
            sa.Uuid(),
            nullable=False,
            comment="Immutable extraction profile identifier used for materialization",
        ),
        sa.Column(
            "source_job_id",
            sa.Uuid(),
            nullable=False,
            comment="Job identifier that materialized normalized revision entities",
        ),
        sa.Column(
            "drawing_revision_id",
            sa.Uuid(),
            nullable=False,
            comment="Drawing revision identifier whose normalized entities were materialized",
        ),
        sa.Column(
            "adapter_run_output_id",
            sa.Uuid(),
            nullable=False,
            comment="Adapter run output identifier materialized into revision-scoped entity tables",
        ),
        sa.Column(
            "canonical_entity_schema_version",
            sa.String(length=16),
            nullable=False,
            comment="Canonical entity schema version for the materialized revision payloads",
        ),
        sa.Column(
            "counts_json",
            sa.JSON(),
            nullable=False,
            comment="Materialized normalized row counts keyed by layouts, layers, blocks, and entities",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="Manifest creation timestamp",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_revision_entity_manifests_project_id_projects",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            name="fk_revision_entity_manifests_source_file_project_files",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            name="fk_revision_entity_manifests_profile_project_profiles",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_job_id"],
            ["jobs.id"],
            name="fk_revision_entity_manifests_source_job_id_jobs",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["drawing_revision_id", "project_id"],
            ["drawing_revisions.id", "drawing_revisions.project_id"],
            name="fk_revision_entity_manifests_revision_project_revisions",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["adapter_run_output_id", "project_id"],
            ["adapter_run_outputs.id", "adapter_run_outputs.project_id"],
            name="fk_revision_entity_manifests_output_project_outputs",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "id",
            "project_id",
            name="uq_revision_entity_manifests_id_project_id",
        ),
        sa.UniqueConstraint(
            "adapter_run_output_id",
            name="uq_revision_entity_manifests_adapter_run_output_id",
        ),
        sa.UniqueConstraint(
            "drawing_revision_id",
            name="uq_revision_entity_manifests_drawing_revision_id",
        ),
        sa.UniqueConstraint(
            "source_job_id",
            name="uq_revision_entity_manifests_source_job_id",
        ),
    )
    op.create_index(
        op.f("ix_revision_entity_manifests_adapter_run_output_id"),
        "revision_entity_manifests",
        ["adapter_run_output_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_revision_entity_manifests_extraction_profile_id"),
        "revision_entity_manifests",
        ["extraction_profile_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_revision_entity_manifests_project_id"),
        "revision_entity_manifests",
        ["project_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_revision_entity_manifests_source_file_id"),
        "revision_entity_manifests",
        ["source_file_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_revision_entity_manifests_source_job_id"),
        "revision_entity_manifests",
        ["source_job_id"],
        unique=False,
    )

    for table_name, table_label, ref_column in (
        ("revision_layouts", "layout", "layout_ref"),
        ("revision_layers", "layer", "layer_ref"),
        ("revision_blocks", "block", "block_ref"),
    ):
        op.create_table(
            table_name,
            sa.Column(
                "id",
                sa.Uuid(),
                nullable=False,
                comment=f"Unique materialized revision {table_label} row identifier (UUID v4)",
            ),
            sa.Column("project_id", sa.Uuid(), nullable=False, comment="Owning project identifier"),
            sa.Column(
                "source_file_id",
                sa.Uuid(),
                nullable=False,
                comment=f"Immutable source file identifier for this materialized {table_label} row",
            ),
            sa.Column(
                "extraction_profile_id",
                sa.Uuid(),
                nullable=False,
                comment=f"Immutable extraction profile identifier used for this {table_label} row",
            ),
            sa.Column(
                "source_job_id",
                sa.Uuid(),
                nullable=False,
                comment=f"Job identifier that materialized this {table_label} row",
            ),
            sa.Column(
                "drawing_revision_id",
                sa.Uuid(),
                nullable=False,
                comment=f"Drawing revision identifier that owns this {table_label} row",
            ),
            sa.Column(
                "adapter_run_output_id",
                sa.Uuid(),
                nullable=False,
                comment=f"Adapter run output identifier that produced this {table_label} row",
            ),
            sa.Column(
                "canonical_entity_schema_version",
                sa.String(length=16),
                nullable=False,
                comment=f"Canonical entity schema version for this {table_label} row payload",
            ),
            sa.Column(
                "sequence_index",
                sa.Integer(),
                nullable=False,
                comment=f"Zero-based {table_label} position from the canonical payload",
            ),
            sa.Column(
                "payload_json",
                sa.JSON(),
                nullable=False,
                comment=f"Materialized canonical {table_label} payload",
            ),
            sa.Column(
                ref_column,
                sa.String(length=255),
                nullable=False,
                comment=f"Stable non-null {table_label} reference extracted from the canonical {table_label} payload",
            ),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
                comment=f"Revision {table_label} materialization timestamp",
            ),
            sa.ForeignKeyConstraint(
                ["project_id"],
                ["projects.id"],
                name=f"fk_{table_name}_project_id_projects",
                ondelete="RESTRICT",
            ),
            sa.ForeignKeyConstraint(
                ["source_file_id", "project_id"],
                ["files.id", "files.project_id"],
                name=f"fk_{table_name}_source_file_project_files",
                ondelete="RESTRICT",
            ),
            sa.ForeignKeyConstraint(
                ["extraction_profile_id", "project_id"],
                ["extraction_profiles.id", "extraction_profiles.project_id"],
                name=f"fk_{table_name}_profile_project_profiles",
                ondelete="RESTRICT",
            ),
            sa.ForeignKeyConstraint(
                ["source_job_id"],
                ["jobs.id"],
                name=f"fk_{table_name}_source_job_id_jobs",
                ondelete="RESTRICT",
            ),
            sa.ForeignKeyConstraint(
                ["drawing_revision_id", "project_id"],
                ["drawing_revisions.id", "drawing_revisions.project_id"],
                name=f"fk_{table_name}_revision_project_revisions",
                ondelete="RESTRICT",
            ),
            sa.ForeignKeyConstraint(
                ["adapter_run_output_id", "project_id"],
                ["adapter_run_outputs.id", "adapter_run_outputs.project_id"],
                name=f"fk_{table_name}_output_project_outputs",
                ondelete="RESTRICT",
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("id", "project_id", name=f"uq_{table_name}_id_project_id"),
            sa.UniqueConstraint(
                "drawing_revision_id",
                "sequence_index",
                name=f"uq_{table_name}_revision_sequence_index",
            ),
            sa.UniqueConstraint(
                "drawing_revision_id",
                ref_column,
                name=f"uq_{table_name}_revision_{ref_column}",
            ),
            sa.CheckConstraint(
                "sequence_index >= 0",
                name=f"ck_{table_name}_sequence_index_ge_0",
            ),
        )
        op.create_index(op.f(f"ix_{table_name}_adapter_run_output_id"), table_name, ["adapter_run_output_id"], unique=False)
        op.create_index(op.f(f"ix_{table_name}_extraction_profile_id"), table_name, ["extraction_profile_id"], unique=False)
        op.create_index(op.f(f"ix_{table_name}_project_id"), table_name, ["project_id"], unique=False)
        op.create_index(
            op.f(f"ix_{table_name}_{ref_column}"),
            table_name,
            [ref_column],
            unique=False,
        )
        op.create_index(
            op.f(f"ix_{table_name}_source_file_id"),
            table_name,
            ["source_file_id"],
            unique=False,
        )
        op.create_index(
            op.f(f"ix_{table_name}_source_job_id"),
            table_name,
            ["source_job_id"],
            unique=False,
        )
        op.create_index(
            f"ix_{table_name}_revision_{ref_column}",
            table_name,
            ["drawing_revision_id", ref_column],
            unique=False,
        )

    op.create_table(
        "revision_entities",
        sa.Column(
            "id",
            sa.Uuid(),
            nullable=False,
            comment="Unique materialized revision entity row identifier (UUID v4)",
        ),
        sa.Column("project_id", sa.Uuid(), nullable=False, comment="Owning project identifier"),
        sa.Column(
            "source_file_id",
            sa.Uuid(),
            nullable=False,
            comment="Immutable source file identifier for this materialized entity row",
        ),
        sa.Column(
            "extraction_profile_id",
            sa.Uuid(),
            nullable=False,
            comment="Immutable extraction profile identifier used for this entity row",
        ),
        sa.Column(
            "source_job_id",
            sa.Uuid(),
            nullable=False,
            comment="Job identifier that materialized this entity row",
        ),
        sa.Column(
            "drawing_revision_id",
            sa.Uuid(),
            nullable=False,
            comment="Drawing revision identifier that owns this entity row",
        ),
        sa.Column(
            "adapter_run_output_id",
            sa.Uuid(),
            nullable=False,
            comment="Adapter run output identifier that produced this entity row",
        ),
        sa.Column(
            "canonical_entity_schema_version",
            sa.String(length=16),
            nullable=False,
            comment="Canonical entity schema version for this entity row payload",
        ),
        sa.Column(
            "sequence_index",
            sa.Integer(),
            nullable=False,
            comment="Zero-based entity position from the canonical payload",
        ),
        sa.Column(
            "entity_id",
            sa.String(length=255),
            nullable=False,
            comment="Stable non-null entity identifier extracted or derived from the canonical entity payload",
        ),
        sa.Column(
            "entity_type",
            sa.String(length=64),
            nullable=False,
            comment="Canonical entity type extracted from the canonical entity payload",
        ),
        sa.Column(
            "entity_schema_version",
            sa.String(length=16),
            nullable=False,
            comment="Canonical entity schema version for this entity row",
        ),
        sa.Column(
            "parent_entity_ref",
            sa.String(length=255),
            nullable=True,
            comment="Optional raw parent entity reference extracted from the canonical entity payload",
        ),
        sa.Column(
            "confidence_score",
            sa.Float(),
            nullable=False,
            comment="Entity confidence score extracted from the canonical entity payload",
        ),
        sa.Column(
            "confidence_json",
            sa.JSON(),
            nullable=False,
            comment="Entity confidence payload extracted from the canonical entity payload",
        ),
        sa.Column(
            "geometry_json",
            sa.JSON(),
            nullable=False,
            comment="Entity geometry payload extracted from the canonical entity payload",
        ),
        sa.Column(
            "properties_json",
            sa.JSON(),
            nullable=False,
            comment="Entity properties payload extracted from the canonical entity payload",
        ),
        sa.Column(
            "provenance_json",
            sa.JSON(),
            nullable=False,
            comment="Entity provenance payload extracted from the canonical entity payload",
        ),
        sa.Column(
            "canonical_entity_json",
            sa.JSON(),
            nullable=True,
            comment="Optional full canonical entity payload retained alongside split contract fields",
        ),
        sa.Column(
            "layout_ref",
            sa.String(length=255),
            nullable=True,
            comment="Optional layout reference extracted from the canonical entity payload",
        ),
        sa.Column(
            "layer_ref",
            sa.String(length=255),
            nullable=True,
            comment="Optional layer reference extracted from the canonical entity payload",
        ),
        sa.Column(
            "block_ref",
            sa.String(length=255),
            nullable=True,
            comment="Optional block reference extracted from the canonical entity payload",
        ),
        sa.Column(
            "source_identity",
            sa.String(length=255),
            nullable=True,
            comment="Stable source identity derived from canonical entity top-level or provenance refs",
        ),
        sa.Column(
            "source_hash",
            sa.String(length=64),
            nullable=True,
            comment="Stable source hash derived from canonical entity top-level or provenance refs",
        ),
        sa.Column(
            "layout_id",
            sa.Uuid(),
            nullable=True,
            comment="Best-effort resolved materialized layout row for layout_ref within the same drawing revision",
        ),
        sa.Column(
            "layer_id",
            sa.Uuid(),
            nullable=True,
            comment="Best-effort resolved materialized layer row for layer_ref within the same drawing revision",
        ),
        sa.Column(
            "block_id",
            sa.Uuid(),
            nullable=True,
            comment="Best-effort resolved materialized block row for block_ref within the same drawing revision",
        ),
        sa.Column(
            "parent_entity_row_id",
            sa.Uuid(),
            nullable=True,
            comment="Best-effort resolved materialized parent entity row for parent_entity_ref within the same drawing revision",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="Revision entity materialization timestamp",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_revision_entities_project_id_projects",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            name="fk_revision_entities_source_file_project_files",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            name="fk_revision_entities_profile_project_profiles",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_job_id"],
            ["jobs.id"],
            name="fk_revision_entities_source_job_id_jobs",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["drawing_revision_id", "project_id"],
            ["drawing_revisions.id", "drawing_revisions.project_id"],
            name="fk_revision_entities_revision_project_revisions",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["adapter_run_output_id", "project_id"],
            ["adapter_run_outputs.id", "adapter_run_outputs.project_id"],
            name="fk_revision_entities_output_project_outputs",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["layout_id"],
            ["revision_layouts.id"],
            name="fk_revision_entities_layout_id_revision_layouts",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["layer_id"],
            ["revision_layers.id"],
            name="fk_revision_entities_layer_id_revision_layers",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["block_id"],
            ["revision_blocks.id"],
            name="fk_revision_entities_block_id_revision_blocks",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["parent_entity_row_id"],
            ["revision_entities.id"],
            name="fk_revision_entities_parent_entity_row_id_revision_entities",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("id", "project_id", name="uq_revision_entities_id_project_id"),
        sa.UniqueConstraint(
            "drawing_revision_id",
            "entity_id",
            name="uq_revision_entities_revision_entity_id",
        ),
        sa.UniqueConstraint(
            "drawing_revision_id",
            "sequence_index",
            name="uq_revision_entities_revision_sequence_index",
        ),
        sa.CheckConstraint("sequence_index >= 0", name="ck_revision_entities_sequence_index_ge_0"),
        sa.CheckConstraint(
            "source_hash IS NULL OR (length(source_hash) = 64 AND source_hash = lower(source_hash))",
            name="ck_revision_entities_source_hash",
        ),
    )
    for column_name in (
        "adapter_run_output_id",
        "block_id",
        "entity_id",
        "entity_type",
        "extraction_profile_id",
        "layout_ref",
        "layout_id",
        "layer_ref",
        "layer_id",
        "block_ref",
        "parent_entity_ref",
        "parent_entity_row_id",
        "project_id",
        "source_identity",
        "source_file_id",
        "source_hash",
        "source_job_id",
    ):
        op.create_index(
            op.f(f"ix_revision_entities_{column_name}"),
            "revision_entities",
            [column_name],
            unique=False,
        )
    op.create_index(
        "ix_revision_entities_revision_entity_type_sequence_id",
        "revision_entities",
        ["drawing_revision_id", "entity_type", "sequence_index", "id"],
        unique=False,
    )
    op.create_index(
        "ix_revision_entities_revision_layout_ref",
        "revision_entities",
        ["drawing_revision_id", "layout_ref"],
        unique=False,
    )
    op.create_index(
        "ix_revision_entities_revision_layer_ref",
        "revision_entities",
        ["drawing_revision_id", "layer_ref"],
        unique=False,
    )
    op.create_index(
        "ix_revision_entities_revision_block_ref",
        "revision_entities",
        ["drawing_revision_id", "block_ref"],
        unique=False,
    )
    op.create_index(
        "ix_revision_entities_revision_parent_entity_ref",
        "revision_entities",
        ["drawing_revision_id", "parent_entity_ref"],
        unique=False,
    )
    op.create_index(
        "ix_revision_entities_revision_source_identity",
        "revision_entities",
        ["drawing_revision_id", "source_identity"],
        unique=False,
    )
    op.create_index(
        "ix_revision_entities_revision_source_hash",
        "revision_entities",
        ["drawing_revision_id", "source_hash"],
        unique=False,
    )

    _attach_append_only_triggers()


def downgrade() -> None:
    """Drop revision-scoped normalized entity materialization tables when empty."""

    _assert_materialization_tables_empty_for_downgrade()
    _drop_append_only_triggers()
    op.drop_table("revision_entities")
    op.drop_table("revision_blocks")
    op.drop_table("revision_layers")
    op.drop_table("revision_layouts")
    op.drop_table("revision_entity_manifests")
