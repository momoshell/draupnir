"""CAD changeset persistence models."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    ForeignKeyConstraint,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.mixins import ProjectScopedMixin, TimestampMixin

CAD_CHANGE_OPERATION_TYPES: tuple[str, ...] = (
    "annotate_entity",
    "change_layer",
    "add_entity",
    "remove_entity",
    "replace_block",
    "replace_profile_material_candidate",
    "update_property",
    "flag_for_review",
)

CAD_CHANGE_SET_STATUSES: tuple[str, ...] = (
    "proposed",
    "validation_requested",
    "approved",
    "rejected",
    "validating",
    "validated",
    "validation_failed",
    "applying",
    "applied",
    "apply_failed",
    "revision_conflict",
)

CAD_CHANGE_SET_VALIDATION_STATUSES: tuple[str, ...] = (
    "valid",
    "valid_with_warnings",
    "invalid",
    "needs_review",
)


def _sql_in_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


class CadChangeSet(ProjectScopedMixin, TimestampMixin, Base):
    __tablename__ = "cad_change_sets"
    __created_at_comment__ = "Changeset creation timestamp"

    __table_args__ = (
        ForeignKeyConstraint(
            ["base_revision_id", "project_id"],
            ["drawing_revisions.id", "drawing_revisions.project_id"],
            name="fk_cad_change_sets_base_revision_id_project_id_revisions",
        ),
        CheckConstraint(
            f"status IN ({_sql_in_list(CAD_CHANGE_SET_STATUSES)})",
            name="ck_cad_change_sets_status",
        ),
        UniqueConstraint("project_id", "id", name="uq_cad_change_sets_project_id_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    base_revision_id: Mapped[uuid.UUID] = mapped_column(nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    created_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        nullable=False,
        default=func.now(),
        onupdate=func.now(),
        comment="Most recent mutable status update timestamp",
    )


class CadChangeOperation(ProjectScopedMixin, TimestampMixin, Base):
    __tablename__ = "cad_change_operations"
    __created_at_comment__ = "Operation creation timestamp"

    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "change_set_id"],
            ["cad_change_sets.project_id", "cad_change_sets.id"],
            name="fk_cad_change_operations_project_id_change_set_id_sets",
        ),
        CheckConstraint("sequence_index >= 1", name="ck_cad_change_operations_sequence_ge_1"),
        CheckConstraint(
            f"operation_type IN ({_sql_in_list(CAD_CHANGE_OPERATION_TYPES)})",
            name="ck_cad_change_operations_type",
        ),
        CheckConstraint(
            "expected_source_hash IS NULL OR expected_source_hash ~ '^[0-9a-f]{64}$'",
            name="ck_cad_change_operations_expected_source_hash_sha256",
        ),
        CheckConstraint(
            "jsonb_typeof(operation_json) = 'object'",
            name="ck_cad_change_operations_operation_json_object",
        ),
        UniqueConstraint("project_id", "id", name="uq_cad_change_operations_project_id_id"),
        UniqueConstraint(
            "project_id",
            "change_set_id",
            "id",
            name="uq_cad_change_ops_project_set_id",
        ),
        UniqueConstraint(
            "project_id",
            "change_set_id",
            "sequence_index",
            name="uq_cad_change_ops_project_set_sequence",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    change_set_id: Mapped[uuid.UUID] = mapped_column(nullable=False, index=True)
    sequence_index: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_type: Mapped[str] = mapped_column(String(64), nullable=False)
    target_revision_entity_id: Mapped[uuid.UUID | None] = mapped_column(nullable=True)
    expected_source_identity: Mapped[str | None] = mapped_column(String(255), nullable=True)
    expected_source_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    operation_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class CadChangeSetValidationResult(ProjectScopedMixin, TimestampMixin, Base):
    __tablename__ = "cad_change_set_validation_results"
    __created_at_comment__ = "Validation result creation timestamp"

    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "change_set_id"],
            ["cad_change_sets.project_id", "cad_change_sets.id"],
            name="fk_cad_change_set_validation_results_project_id_change_set_id",
        ),
        CheckConstraint(
            f"validation_status IN ({_sql_in_list(CAD_CHANGE_SET_VALIDATION_STATUSES)})",
            name="ck_cad_change_set_validation_results_status",
        ),
        CheckConstraint(
            "jsonb_typeof(result_json) = 'object'",
            name="ck_cad_change_set_validation_results_result_json_object",
        ),
        UniqueConstraint(
            "project_id",
            "id",
            name="uq_cad_change_set_validation_results_project_id_id",
        ),
        UniqueConstraint(
            "project_id",
            "change_set_id",
            "id",
            name="uq_cad_change_validation_project_set_id",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    change_set_id: Mapped[uuid.UUID] = mapped_column(nullable=False, index=True)
    validation_status: Mapped[str] = mapped_column(String(32), nullable=False)
    validator_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    validator_version: Mapped[str | None] = mapped_column(String(32), nullable=True)
    result_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


__all__ = [
    "CAD_CHANGE_OPERATION_TYPES",
    "CAD_CHANGE_SET_STATUSES",
    "CAD_CHANGE_SET_VALIDATION_STATUSES",
    "CadChangeOperation",
    "CadChangeSet",
    "CadChangeSetValidationResult",
]
