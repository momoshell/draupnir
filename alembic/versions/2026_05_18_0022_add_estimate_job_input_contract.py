"""add estimate job input contract

Revision ID: 2026_05_18_0022
Revises: 2026_05_18_0021
Create Date: 2026-05-18 11:20:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "2026_05_18_0022"
down_revision = "2026_05_18_0021"
branch_labels = None
depends_on = None

_ESTIMATE_JOB_INPUT_CATALOG_REF_TYPED_CONTRACT = (
    "(ref_type = 'rate' AND rate_catalog_entry_id IS NOT NULL "
    "AND material_catalog_entry_id IS NULL AND formula_definition_id IS NULL) OR "
    "(ref_type = 'material' AND material_catalog_entry_id IS NOT NULL "
    "AND rate_catalog_entry_id IS NULL AND formula_definition_id IS NULL) OR "
    "(ref_type = 'formula' AND formula_definition_id IS NOT NULL "
    "AND rate_catalog_entry_id IS NULL AND material_catalog_entry_id IS NULL)"
)


def _attach_append_only_triggers(table_name: str) -> None:
    op.execute(
        sa.text(
            f"""
            CREATE TRIGGER trg_append_only_row_guard
            BEFORE UPDATE OR DELETE ON \"{table_name}\"
            FOR EACH ROW
            EXECUTE FUNCTION enforce_append_only_lineage_row()
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            CREATE TRIGGER trg_append_only_truncate_guard
            BEFORE TRUNCATE ON \"{table_name}\"
            FOR EACH STATEMENT
            EXECUTE FUNCTION enforce_append_only_lineage_truncate()
            """
        )
    )


def _drop_append_only_triggers(table_name: str) -> None:
    op.execute(sa.text(f'DROP TRIGGER IF EXISTS trg_append_only_row_guard ON "{table_name}"'))
    op.execute(sa.text(f'DROP TRIGGER IF EXISTS trg_append_only_truncate_guard ON "{table_name}"'))


def _assert_estimate_job_input_tables_empty() -> None:
    bind = op.get_bind()
    for table_name in ("estimate_job_input_catalog_refs", "estimate_job_inputs"):
        row_count = bind.execute(sa.text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar_one()
        if row_count:
            raise RuntimeError(
                "Refusing to downgrade migration 2026_05_18_0022 while "
                f"{table_name} contains {row_count} row(s)."
            )

    estimate_job_count = bind.execute(
        sa.text("SELECT COUNT(*) FROM jobs WHERE job_type = 'estimate'")
    ).scalar_one()
    if estimate_job_count:
        raise RuntimeError(
            "Refusing to downgrade migration 2026_05_18_0022 while jobs contains "
            f"{estimate_job_count} estimate job row(s)."
        )


def upgrade() -> None:
    op.drop_constraint("ck_jobs_job_type_valid", "jobs", type_="check")
    op.create_check_constraint(
        "ck_jobs_job_type_valid",
        "jobs",
        "job_type IN ('ingest', 'reprocess', 'quantity_takeoff', 'estimate')",
    )
    op.drop_constraint("ck_jobs_reprocess_base_revision_required", "jobs", type_="check")
    op.create_check_constraint(
        "ck_jobs_revision_scoped_base_revision_required",
        "jobs",
        "job_type NOT IN ('reprocess', 'quantity_takeoff', 'estimate') "
        "OR base_revision_id IS NOT NULL",
    )
    op.drop_constraint(
        "ck_jobs_quantity_takeoff_extraction_profile_forbidden",
        "jobs",
        type_="check",
    )
    op.create_check_constraint(
        "ck_jobs_revision_scoped_extraction_profile_forbidden",
        "jobs",
        "job_type NOT IN ('quantity_takeoff', 'estimate') "
        "OR extraction_profile_id IS NULL",
    )

    op.create_table(
        "estimate_job_inputs",
        sa.Column(
            "estimate_job_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="Owning immutable estimate job identifier",
        ),
        sa.Column(
            "project_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="Owning project identifier copied from the estimate job",
        ),
        sa.Column(
            "source_file_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="Pinned source file identifier copied from the estimate job",
        ),
        sa.Column(
            "drawing_revision_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="Pinned drawing revision identifier copied from the estimate job",
        ),
        sa.Column(
            "quantity_takeoff_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="Trusted allowed quantity takeoff selected for the estimate job",
        ),
        sa.Column(
            "source_job_type",
            sa.String(length=64),
            nullable=False,
            comment="Denormalized job type used by the composite estimate-job contract",
        ),
        sa.Column(
            "quantity_gate",
            sa.String(length=32),
            nullable=False,
            comment="Persisted quantity gate contract copied from the selected takeoff",
        ),
        sa.Column(
            "trusted_totals",
            sa.Boolean(),
            nullable=False,
            comment="Persisted trusted-total contract copied from the selected takeoff",
        ),
        sa.Column(
            "currency",
            sa.String(length=3),
            nullable=False,
            comment="Pinned estimate currency contract resolved when the job was queued",
        ),
        sa.Column(
            "pricing_effective_date",
            sa.Date(),
            nullable=False,
            comment="Resolved pricing effective date captured when the estimate job was queued",
        ),
        sa.Column(
            "pricing_mode",
            sa.String(length=64),
            nullable=False,
            comment="Pricing-date resolution mode captured when the estimate job was queued",
        ),
        sa.Column(
            "assumptions_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            comment="Estimate assumptions JSON object captured when the estimate job was queued",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
            comment="Estimate job input capture timestamp",
        ),
        sa.CheckConstraint(
            "source_job_type = 'estimate'",
            name="ck_estimate_job_inputs_source_job_type_estimate",
        ),
        sa.CheckConstraint(
            "quantity_gate = 'allowed'",
            name="ck_estimate_job_inputs_quantity_gate_allowed",
        ),
        sa.CheckConstraint(
            "trusted_totals = TRUE",
            name="ck_estimate_job_inputs_trusted_totals_true",
        ),
        sa.CheckConstraint(
            "currency = 'GBP'",
            name="ck_estimate_job_inputs_currency_gbp",
        ),
        sa.CheckConstraint(
            "pricing_mode IN ('explicit', 'derived_from_job_created_at_utc')",
            name="ck_estimate_job_inputs_pricing_mode_valid",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(assumptions_json) = 'object'",
            name="ck_estimate_job_inputs_assumptions_json_object",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_estimate_job_inputs_project_id_projects",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            name="fk_estimate_job_inputs_source_file_id_project_id_files",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["drawing_revision_id", "project_id", "source_file_id"],
            [
                "drawing_revisions.id",
                "drawing_revisions.project_id",
                "drawing_revisions.source_file_id",
            ],
            name="fk_estimate_job_inputs_revision_lineage",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            [
                "estimate_job_id",
                "project_id",
                "source_file_id",
                "drawing_revision_id",
                "source_job_type",
            ],
            [
                "jobs.id",
                "jobs.project_id",
                "jobs.file_id",
                "jobs.base_revision_id",
                "jobs.job_type",
            ],
            name="fk_estimate_job_inputs_source_job_contract",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            [
                "quantity_takeoff_id",
                "project_id",
                "drawing_revision_id",
                "quantity_gate",
                "trusted_totals",
            ],
            [
                "quantity_takeoffs.id",
                "quantity_takeoffs.project_id",
                "quantity_takeoffs.drawing_revision_id",
                "quantity_takeoffs.quantity_gate",
                "quantity_takeoffs.trusted_totals",
            ],
            name="fk_estimate_job_inputs_takeoff_contract",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("estimate_job_id"),
        sa.UniqueConstraint(
            "estimate_job_id",
            "project_id",
            "drawing_revision_id",
            name="uq_estimate_job_inputs_id_project_id_drawing_revision_id",
        ),
    )
    op.create_index(
        "ix_estimate_job_inputs_project_id",
        "estimate_job_inputs",
        ["project_id"],
        unique=False,
    )
    op.create_index(
        "ix_estimate_job_inputs_source_file_id",
        "estimate_job_inputs",
        ["source_file_id"],
        unique=False,
    )
    op.create_index(
        "ix_estimate_job_inputs_quantity_takeoff_id",
        "estimate_job_inputs",
        ["quantity_takeoff_id"],
        unique=False,
    )
    op.create_index(
        "ix_estimate_job_inputs_drawing_revision_id",
        "estimate_job_inputs",
        ["drawing_revision_id"],
        unique=False,
    )

    op.create_table(
        "estimate_job_input_catalog_refs",
        sa.Column(
            "estimate_job_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="Owning immutable estimate job identifier",
        ),
        sa.Column(
            "ref_type",
            sa.String(length=16),
            nullable=False,
            comment="Typed catalog reference discriminator captured for deterministic selection",
        ),
        sa.Column(
            "selection_key",
            sa.String(length=255),
            nullable=False,
            comment="Deterministic selection key within the estimate job input",
        ),
        sa.Column(
            "ref_order",
            sa.Integer(),
            nullable=False,
            comment="Deterministic catalog reference order within the estimate job input",
        ),
        sa.Column(
            "rate_catalog_entry_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
            comment="Selected rate catalog entry identifier, when this ref is rate-typed",
        ),
        sa.Column(
            "material_catalog_entry_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
            comment="Selected material catalog entry identifier, when this ref is material-typed",
        ),
        sa.Column(
            "formula_definition_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
            comment="Selected formula definition identifier, when this ref is formula-typed",
        ),
        sa.Column(
            "catalog_checksum_sha256",
            sa.String(length=64),
            nullable=False,
            comment="Immutable catalog checksum snapshot copied when the ref was selected",
        ),
        sa.Column(
            "selection_context_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            comment="Selection context JSON object captured when the catalog ref was chosen",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
            comment="Catalog reference capture timestamp",
        ),
        sa.CheckConstraint(
            "length(btrim(selection_key)) > 0",
            name="ck_estimate_job_input_catalog_refs_selection_key_nonblank",
        ),
        sa.CheckConstraint(
            "ref_order >= 0",
            name="ck_estimate_job_input_catalog_refs_ref_order_nonnegative",
        ),
        sa.CheckConstraint(
            "ref_type IN ('rate', 'material', 'formula')",
            name="ck_estimate_job_input_catalog_refs_ref_type_valid",
        ),
        sa.CheckConstraint(
            _ESTIMATE_JOB_INPUT_CATALOG_REF_TYPED_CONTRACT,
            name="ck_estimate_job_input_catalog_refs_typed_catalog_ref_match",
        ),
        sa.CheckConstraint(
            "catalog_checksum_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_estimate_job_input_catalog_refs_checksum_sha256_format",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(selection_context_json) = 'object'",
            name="ck_est_job_input_catalog_refs_context_json_object",
        ),
        sa.ForeignKeyConstraint(
            ["estimate_job_id"],
            ["estimate_job_inputs.estimate_job_id"],
            name="fk_estimate_job_input_catalog_refs_estimate_job_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["rate_catalog_entry_id"],
            ["rate_catalog_entries.id"],
            name="fk_estimate_job_input_catalog_refs_rate_catalog_entry_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["material_catalog_entry_id"],
            ["material_catalog_entries.id"],
            name="fk_estimate_job_input_catalog_refs_material_catalog_entry_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["formula_definition_id"],
            ["formula_definitions.id"],
            name="fk_estimate_job_input_catalog_refs_formula_definition_id",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("estimate_job_id", "ref_type", "selection_key"),
        sa.UniqueConstraint(
            "estimate_job_id",
            "ref_type",
            "selection_key",
            name="uq_estimate_job_input_catalog_refs_job_ref_type_selection_key",
        ),
        sa.UniqueConstraint(
            "estimate_job_id",
            "ref_order",
            name="uq_estimate_job_input_catalog_refs_estimate_job_id_ref_order",
        ),
    )

    _attach_append_only_triggers("estimate_job_inputs")
    _attach_append_only_triggers("estimate_job_input_catalog_refs")


def downgrade() -> None:
    _assert_estimate_job_input_tables_empty()

    _drop_append_only_triggers("estimate_job_input_catalog_refs")
    _drop_append_only_triggers("estimate_job_inputs")

    op.drop_table("estimate_job_input_catalog_refs")
    op.drop_table("estimate_job_inputs")

    op.drop_constraint(
        "ck_jobs_revision_scoped_extraction_profile_forbidden",
        "jobs",
        type_="check",
    )
    op.create_check_constraint(
        "ck_jobs_quantity_takeoff_extraction_profile_forbidden",
        "jobs",
        "job_type != 'quantity_takeoff' OR extraction_profile_id IS NULL",
    )
    op.drop_constraint(
        "ck_jobs_revision_scoped_base_revision_required",
        "jobs",
        type_="check",
    )
    op.create_check_constraint(
        "ck_jobs_reprocess_base_revision_required",
        "jobs",
        "job_type NOT IN ('reprocess', 'quantity_takeoff') OR base_revision_id IS NOT NULL",
    )
    op.drop_constraint("ck_jobs_job_type_valid", "jobs", type_="check")
    op.create_check_constraint(
        "ck_jobs_job_type_valid",
        "jobs",
        "job_type IN ('ingest', 'reprocess', 'quantity_takeoff')",
    )
