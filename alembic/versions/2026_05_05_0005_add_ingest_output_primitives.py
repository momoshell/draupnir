"""add ingest output primitives

Revision ID: 2026_05_05_0005
Revises: 2026_05_05_0004
Create Date: 2026-05-05 19:55:00
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_05_05_0005"
down_revision: str | None = "2026_05_05_0004"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def _raise_if_table_non_empty(table_name: str) -> None:
    """Prevent destructive downgrade of append-only output tables with data."""
    bind = op.get_bind()
    has_rows = bool(bind.execute(sa.text(f"SELECT EXISTS (SELECT 1 FROM {table_name})")).scalar())
    if has_rows:
        raise RuntimeError(
            "Cannot downgrade revision 2026_05_05_0005 while "
            f"{table_name} contains rows. Manual data-preserving rollback is required."
        )


def upgrade() -> None:
    """Create append-only ingest output persistence primitives."""
    op.create_table(
        "adapter_run_outputs",
        sa.Column(
            "id",
            sa.Uuid(),
            nullable=False,
            comment="Unique adapter run output identifier (UUID v4)",
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
            comment="Immutable source file identifier used for this adapter run",
        ),
        sa.Column(
            "extraction_profile_id",
            sa.Uuid(),
            nullable=False,
            comment="Immutable extraction profile identifier used for this adapter run",
        ),
        sa.Column(
            "source_job_id",
            sa.Uuid(),
            nullable=False,
            comment="Job identifier that produced this committed adapter output",
        ),
        sa.Column(
            "adapter_key",
            sa.String(length=128),
            nullable=False,
            comment="Stable adapter registry key used for this adapter run",
        ),
        sa.Column(
            "adapter_version",
            sa.String(length=64),
            nullable=False,
            comment="Adapter version recorded in adapter diagnostics",
        ),
        sa.Column(
            "input_family",
            sa.String(length=32),
            nullable=False,
            comment="Normalized input family processed by this adapter run",
        ),
        sa.Column(
            "canonical_entity_schema_version",
            sa.String(length=16),
            nullable=False,
            comment="Canonical entity schema version for the adapter output payload",
        ),
        sa.Column(
            "canonical_json",
            sa.JSON(),
            nullable=False,
            comment=(
                "Canonical adapter output payload including layouts, layers, "
                "blocks, and entities"
            ),
        ),
        sa.Column(
            "provenance_json",
            sa.JSON(),
            nullable=False,
            comment="Structured adapter provenance payload",
        ),
        sa.Column(
            "confidence_json",
            sa.JSON(),
            nullable=False,
            comment="Structured confidence payload for adapter output review workflows",
        ),
        sa.Column(
            "confidence_score",
            sa.Float(),
            nullable=False,
            comment="Overall adapter confidence score for this committed output",
        ),
        sa.Column(
            "warnings_json",
            sa.JSON(),
            nullable=False,
            comment="Adapter-emitted warnings carried with the committed output envelope",
        ),
        sa.Column(
            "diagnostics_json",
            sa.JSON(),
            nullable=False,
            comment="Adapter diagnostics payload including timing metadata",
        ),
        sa.Column(
            "result_checksum_sha256",
            sa.String(length=64),
            nullable=False,
            comment="SHA-256 checksum of the committed adapter result envelope",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="Adapter output record creation timestamp",
        ),
        sa.ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            ondelete="CASCADE",
            name="fk_adapter_run_outputs_extraction_profile_proj_profiles",
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="CASCADE",
            name="fk_adapter_run_outputs_source_file_id_project_id_files",
        ),
        sa.ForeignKeyConstraint(
            ["source_job_id"],
            ["jobs.id"],
            ondelete="CASCADE",
            name="fk_adapter_run_outputs_source_job_id_jobs",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("id", "project_id", name="uq_adapter_run_outputs_id_project_id"),
        sa.UniqueConstraint("source_job_id", name="uq_adapter_run_outputs_source_job_id"),
        sa.CheckConstraint(
            "confidence_score >= 0.0 AND confidence_score <= 1.0",
            name="ck_adapter_outputs_confidence_0_1",
        ),
        sa.CheckConstraint(
            "length(result_checksum_sha256) = 64 "
            "AND result_checksum_sha256 = lower(result_checksum_sha256)",
            name="ck_adapter_outputs_checksum",
        ),
    )
    op.create_index(
        op.f("ix_adapter_run_outputs_extraction_profile_id"),
        "adapter_run_outputs",
        ["extraction_profile_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_adapter_run_outputs_project_id"),
        "adapter_run_outputs",
        ["project_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_adapter_run_outputs_source_file_id"),
        "adapter_run_outputs",
        ["source_file_id"],
        unique=False,
    )

    op.create_table(
        "drawing_revisions",
        sa.Column(
            "id",
            sa.Uuid(),
            nullable=False,
            comment="Unique drawing revision identifier (UUID v4)",
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
            comment="Immutable source file identifier for this revision",
        ),
        sa.Column(
            "extraction_profile_id",
            sa.Uuid(),
            nullable=False,
            comment="Immutable extraction profile identifier used to derive this revision",
        ),
        sa.Column(
            "source_job_id",
            sa.Uuid(),
            nullable=False,
            comment="Job identifier that committed this drawing revision",
        ),
        sa.Column(
            "adapter_run_output_id",
            sa.Uuid(),
            nullable=False,
            comment="Committed adapter output envelope consumed by this drawing revision",
        ),
        sa.Column(
            "predecessor_revision_id",
            sa.Uuid(),
            nullable=True,
            comment="Immediate predecessor drawing revision identifier for append-only lineage",
        ),
        sa.Column(
            "revision_sequence",
            sa.Integer(),
            nullable=False,
            comment="Monotonic revision sequence per source file within a project",
        ),
        sa.Column(
            "revision_kind",
            sa.String(length=32),
            nullable=False,
            comment="Drawing revision kind recorded for this append-only revision",
        ),
        sa.Column(
            "review_state",
            sa.String(length=32),
            nullable=False,
            comment="Review state recorded for this drawing revision",
        ),
        sa.Column(
            "canonical_entity_schema_version",
            sa.String(length=16),
            nullable=False,
            comment="Canonical entity schema version stored on this drawing revision",
        ),
        sa.Column(
            "confidence_score",
            sa.Float(),
            nullable=False,
            comment="Overall drawing revision confidence score for review workflows",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="Drawing revision creation timestamp",
        ),
        sa.ForeignKeyConstraint(
            ["adapter_run_output_id", "project_id"],
            ["adapter_run_outputs.id", "adapter_run_outputs.project_id"],
            ondelete="CASCADE",
            name="fk_drawing_revisions_adapter_run_output_id_project_id_outputs",
        ),
        sa.ForeignKeyConstraint(
            ["extraction_profile_id", "project_id"],
            ["extraction_profiles.id", "extraction_profiles.project_id"],
            ondelete="CASCADE",
            name="fk_drawing_revisions_extraction_profile_proj_profiles",
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["predecessor_revision_id"],
            ["drawing_revisions.id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="CASCADE",
            name="fk_drawing_revisions_source_file_id_project_id_files",
        ),
        sa.ForeignKeyConstraint(
            ["source_job_id"],
            ["jobs.id"],
            ondelete="CASCADE",
            name="fk_drawing_revisions_source_job_id_jobs",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "adapter_run_output_id",
            name="uq_drawing_revisions_adapter_run_output_id",
        ),
        sa.UniqueConstraint("id", "project_id", name="uq_drawing_revisions_id_project_id"),
        sa.UniqueConstraint(
            "project_id",
            "source_file_id",
            "revision_sequence",
            name="uq_drawing_revisions_project_file_revision_sequence",
        ),
        sa.UniqueConstraint("source_job_id", name="uq_drawing_revisions_source_job_id"),
        sa.CheckConstraint(
            "revision_sequence >= 1",
            name="ck_drawing_revisions_seq_ge_1",
        ),
        sa.CheckConstraint(
            "revision_kind IN ('ingest', 'reprocess')",
            name="ck_drawing_revisions_kind",
        ),
        sa.CheckConstraint(
            "review_state IN "
            "('approved', 'provisional', 'review_required', 'rejected', 'superseded')",
            name="ck_drawing_revisions_review_state",
        ),
        sa.CheckConstraint(
            "confidence_score >= 0.0 AND confidence_score <= 1.0",
            name="ck_drawing_revisions_conf_0_1",
        ),
    )
    op.create_index(
        op.f("ix_drawing_revisions_extraction_profile_id"),
        "drawing_revisions",
        ["extraction_profile_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_drawing_revisions_predecessor_revision_id"),
        "drawing_revisions",
        ["predecessor_revision_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_drawing_revisions_project_id"),
        "drawing_revisions",
        ["project_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_drawing_revisions_source_file_id"),
        "drawing_revisions",
        ["source_file_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_drawing_revisions_source_job_id"),
        "drawing_revisions",
        ["source_job_id"],
        unique=False,
    )

    op.create_table(
        "validation_reports",
        sa.Column(
            "id",
            sa.Uuid(),
            nullable=False,
            comment="Unique validation report identifier (UUID v4)",
        ),
        sa.Column(
            "project_id",
            sa.Uuid(),
            nullable=False,
            comment="Owning project identifier",
        ),
        sa.Column(
            "drawing_revision_id",
            sa.Uuid(),
            nullable=False,
            comment="Drawing revision identifier addressed by this canonical validation report",
        ),
        sa.Column(
            "source_job_id",
            sa.Uuid(),
            nullable=False,
            comment="Job identifier that produced this validation report",
        ),
        sa.Column(
            "validation_report_schema_version",
            sa.String(length=16),
            nullable=False,
            comment="Validation report schema version",
        ),
        sa.Column(
            "canonical_entity_schema_version",
            sa.String(length=16),
            nullable=False,
            comment="Canonical entity schema version validated by this report",
        ),
        sa.Column(
            "validation_status",
            sa.String(length=32),
            nullable=False,
            comment="Technical validation status for the drawing revision",
        ),
        sa.Column(
            "review_state",
            sa.String(length=32),
            nullable=False,
            comment="Inherited review state recorded on the validation report",
        ),
        sa.Column(
            "quantity_gate",
            sa.String(length=32),
            nullable=False,
            comment="Derived quantity gate outcome for the drawing revision",
        ),
        sa.Column(
            "effective_confidence",
            sa.Float(),
            nullable=False,
            comment="Conservative effective confidence used for quantity gate decisions",
        ),
        sa.Column(
            "validator_name",
            sa.String(length=128),
            nullable=False,
            comment="Validator implementation name",
        ),
        sa.Column(
            "validator_version",
            sa.String(length=64),
            nullable=False,
            comment="Validator implementation version",
        ),
        sa.Column(
            "report_json",
            sa.JSON(),
            nullable=False,
            comment=(
                "Validation report payload containing summary, checks, findings, "
                "and adapter warnings"
            ),
        ),
        sa.Column(
            "generated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            comment="Validation report generation timestamp",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="Validation report record creation timestamp",
        ),
        sa.ForeignKeyConstraint(
            ["drawing_revision_id", "project_id"],
            ["drawing_revisions.id", "drawing_revisions.project_id"],
            ondelete="CASCADE",
            name="fk_validation_reports_drawing_revision_id_project_id_revisions",
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["source_job_id"],
            ["jobs.id"],
            ondelete="CASCADE",
            name="fk_validation_reports_source_job_id_jobs",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "drawing_revision_id",
            name="uq_validation_reports_drawing_revision_id",
        ),
        sa.UniqueConstraint("id", "project_id", name="uq_validation_reports_id_project_id"),
        sa.UniqueConstraint("source_job_id", name="uq_validation_reports_source_job_id"),
        sa.CheckConstraint(
            "validation_status IN "
            "('valid', 'valid_with_warnings', 'invalid', 'needs_review')",
            name="ck_validation_reports_status",
        ),
        sa.CheckConstraint(
            "review_state IN "
            "('approved', 'provisional', 'review_required', 'rejected', 'superseded')",
            name="ck_validation_reports_review_state",
        ),
        sa.CheckConstraint(
            "quantity_gate IN "
            "('allowed', 'allowed_provisional', 'review_gated', 'blocked')",
            name="ck_validation_reports_quantity_gate",
        ),
        sa.CheckConstraint(
            "effective_confidence >= 0.0 AND effective_confidence <= 1.0",
            name="ck_validation_reports_conf_0_1",
        ),
    )
    op.create_index(
        op.f("ix_validation_reports_project_id"),
        "validation_reports",
        ["project_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_validation_reports_source_job_id"),
        "validation_reports",
        ["source_job_id"],
        unique=False,
    )

    op.create_table(
        "generated_artifacts",
        sa.Column(
            "id",
            sa.Uuid(),
            nullable=False,
            comment="Unique generated artifact identifier (UUID v4)",
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
            comment="Source file identifier for artifact lineage",
        ),
        sa.Column(
            "job_id",
            sa.Uuid(),
            nullable=False,
            comment="Job identifier that produced this generated artifact",
        ),
        sa.Column(
            "drawing_revision_id",
            sa.Uuid(),
            nullable=True,
            comment="Drawing revision identifier used to generate this artifact",
        ),
        sa.Column(
            "adapter_run_output_id",
            sa.Uuid(),
            nullable=True,
            comment="Adapter run output identifier used to generate this artifact",
        ),
        sa.Column(
            "artifact_kind",
            sa.String(length=64),
            nullable=False,
            comment="Generated artifact kind",
        ),
        sa.Column(
            "name",
            sa.String(length=255),
            nullable=False,
            comment="Human-readable generated artifact name",
        ),
        sa.Column(
            "format",
            sa.String(length=64),
            nullable=False,
            comment="Generated artifact format identifier",
        ),
        sa.Column(
            "media_type",
            sa.String(length=255),
            nullable=False,
            comment="Generated artifact media type",
        ),
        sa.Column(
            "size_bytes",
            sa.BigInteger(),
            nullable=False,
            comment="Generated artifact byte size",
        ),
        sa.Column(
            "checksum_sha256",
            sa.String(length=64),
            nullable=False,
            comment="SHA-256 checksum for generated artifact bytes",
        ),
        sa.Column(
            "generator_name",
            sa.String(length=128),
            nullable=False,
            comment="Artifact generator implementation name",
        ),
        sa.Column(
            "generator_version",
            sa.String(length=64),
            nullable=False,
            comment="Artifact generator implementation version",
        ),
        sa.Column(
            "generator_config_json",
            sa.JSON(),
            nullable=False,
            comment="Artifact generator configuration payload",
        ),
        sa.Column(
            "storage_key",
            sa.String(length=1024),
            nullable=False,
            comment="Immutable generated artifact storage key",
        ),
        sa.Column(
            "storage_uri",
            sa.String(length=1024),
            nullable=False,
            comment="Immutable generated artifact storage URI",
        ),
        sa.Column(
            "lineage_json",
            sa.JSON(),
            nullable=False,
            comment="Structured generated artifact lineage payload",
        ),
        sa.Column(
            "predecessor_artifact_id",
            sa.Uuid(),
            nullable=True,
            comment="Immediate predecessor artifact identifier for append-only lineage",
        ),
        sa.Column(
            "deleted_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="Soft deletion timestamp for retention workflows",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="Generated artifact creation timestamp",
        ),
        sa.ForeignKeyConstraint(
            ["adapter_run_output_id", "project_id"],
            ["adapter_run_outputs.id", "adapter_run_outputs.project_id"],
            ondelete="CASCADE",
            name="fk_generated_artifacts_adapter_run_output_id_project_id_outputs",
        ),
        sa.ForeignKeyConstraint(
            ["drawing_revision_id", "project_id"],
            ["drawing_revisions.id", "drawing_revisions.project_id"],
            ondelete="CASCADE",
            name="fk_generated_artifacts_drawing_revision_id_project_id_revisions",
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="CASCADE",
            name="fk_generated_artifacts_source_file_id_project_id_files",
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["jobs.id"],
            ondelete="CASCADE",
            name="fk_generated_artifacts_job_id_jobs",
        ),
        sa.ForeignKeyConstraint(
            ["predecessor_artifact_id"],
            ["generated_artifacts.id"],
            ondelete="RESTRICT",
            name="fk_generated_artifacts_predecessor_artifact_id_self",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("id", "project_id", name="uq_generated_artifacts_id_project_id"),
        sa.UniqueConstraint("storage_key", name="uq_generated_artifacts_storage_key"),
        sa.CheckConstraint(
            "size_bytes >= 0",
            name="ck_generated_artifacts_size_ge_0",
        ),
        sa.CheckConstraint(
            "length(checksum_sha256) = 64 AND checksum_sha256 = lower(checksum_sha256)",
            name="ck_generated_artifacts_checksum",
        ),
    )
    op.create_index(
        op.f("ix_generated_artifacts_adapter_run_output_id"),
        "generated_artifacts",
        ["adapter_run_output_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_generated_artifacts_drawing_revision_id"),
        "generated_artifacts",
        ["drawing_revision_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_generated_artifacts_project_id"),
        "generated_artifacts",
        ["project_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_generated_artifacts_predecessor_artifact_id"),
        "generated_artifacts",
        ["predecessor_artifact_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_generated_artifacts_source_file_id"),
        "generated_artifacts",
        ["source_file_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_generated_artifacts_job_id"),
        "generated_artifacts",
        ["job_id"],
        unique=False,
    )


def downgrade() -> None:
    """Drop append-only ingest output persistence primitives."""
    _raise_if_table_non_empty("generated_artifacts")
    _raise_if_table_non_empty("validation_reports")
    _raise_if_table_non_empty("drawing_revisions")
    _raise_if_table_non_empty("adapter_run_outputs")

    op.drop_index(op.f("ix_generated_artifacts_job_id"), table_name="generated_artifacts")
    op.drop_index(op.f("ix_generated_artifacts_source_file_id"), table_name="generated_artifacts")
    op.drop_index(op.f("ix_generated_artifacts_project_id"), table_name="generated_artifacts")
    op.drop_index(
        op.f("ix_generated_artifacts_predecessor_artifact_id"),
        table_name="generated_artifacts",
    )
    op.drop_index(
        op.f("ix_generated_artifacts_drawing_revision_id"),
        table_name="generated_artifacts",
    )
    op.drop_index(
        op.f("ix_generated_artifacts_adapter_run_output_id"),
        table_name="generated_artifacts",
    )
    op.drop_table("generated_artifacts")

    op.drop_index(op.f("ix_validation_reports_source_job_id"), table_name="validation_reports")
    op.drop_index(op.f("ix_validation_reports_project_id"), table_name="validation_reports")
    op.drop_table("validation_reports")

    op.drop_index(op.f("ix_drawing_revisions_source_job_id"), table_name="drawing_revisions")
    op.drop_index(op.f("ix_drawing_revisions_source_file_id"), table_name="drawing_revisions")
    op.drop_index(op.f("ix_drawing_revisions_project_id"), table_name="drawing_revisions")
    op.drop_index(
        op.f("ix_drawing_revisions_predecessor_revision_id"),
        table_name="drawing_revisions",
    )
    op.drop_index(
        op.f("ix_drawing_revisions_extraction_profile_id"),
        table_name="drawing_revisions",
    )
    op.drop_table("drawing_revisions")

    op.drop_index(
        op.f("ix_adapter_run_outputs_source_file_id"),
        table_name="adapter_run_outputs",
    )
    op.drop_index(op.f("ix_adapter_run_outputs_project_id"), table_name="adapter_run_outputs")
    op.drop_index(
        op.f("ix_adapter_run_outputs_extraction_profile_id"),
        table_name="adapter_run_outputs",
    )
    op.drop_table("adapter_run_outputs")
