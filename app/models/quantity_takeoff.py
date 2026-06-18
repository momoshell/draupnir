"""Append-only quantity takeoff persistence models."""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.job import JobType, _sql_in_list


class QuantityValidationStatus(StrEnum):
    """Validation statuses allowed for persisted quantity outputs."""

    VALID = "valid"
    VALID_WITH_WARNINGS = "valid_with_warnings"
    INVALID = "invalid"
    NEEDS_REVIEW = "needs_review"


class QuantityItemKind(StrEnum):
    """Kinds of persisted quantity item lineage rows."""

    CONTRIBUTOR = "contributor"
    AGGREGATE = "aggregate"
    EXCLUSION = "exclusion"
    CONFLICT = "conflict"


_QUANTITY_VALIDATION_STATUS_VALUES = tuple(status.value for status in QuantityValidationStatus)
_QUANTITY_ITEM_KIND_VALUES = tuple(kind.value for kind in QuantityItemKind)
_QUANTITY_ITEM_SOURCE_ENTITY_CONTRACT = (
    "((item_kind = 'contributor' AND source_entity_id IS NOT NULL) "
    "OR (item_kind = 'aggregate' AND source_entity_id IS NULL) "
    "OR (item_kind = 'exclusion' AND source_entity_id IS NOT NULL) "
    "OR (item_kind = 'conflict' AND source_entity_id IS NOT NULL))"
)
_QUANTITY_ITEM_VALUE_CONTRACT = (
    "((item_kind = 'contributor' AND value IS NOT NULL) "
    "OR (item_kind = 'aggregate' AND value IS NOT NULL) "
    "OR (item_kind = 'exclusion' AND value IS NULL) "
    "OR (item_kind = 'conflict' AND value IS NULL))"
)


class QuantityTakeoff(Base):
    """Immutable persisted quantity takeoff metadata and lineage."""

    __tablename__ = "quantity_takeoffs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="RESTRICT",
            name="fk_quantity_takeoffs_source_file_id_project_id_files",
        ),
        ForeignKeyConstraint(
            ["drawing_revision_id", "project_id", "source_file_id"],
            [
                "drawing_revisions.id",
                "drawing_revisions.project_id",
                "drawing_revisions.source_file_id",
            ],
            ondelete="RESTRICT",
            name="fk_quantity_takeoffs_revision_lineage",
        ),
        ForeignKeyConstraint(
            [
                "source_job_id",
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
            ondelete="RESTRICT",
            name="fk_quantity_takeoffs_source_job_contract",
        ),
        CheckConstraint(
            f"validation_status IN ({_sql_in_list(_QUANTITY_VALIDATION_STATUS_VALUES)})",
            name="ck_quantity_takeoffs_validation_status_valid",
        ),
        CheckConstraint(
            f"source_job_type = '{JobType.QUANTITY_TAKEOFF.value}'",
            name="ck_quantity_takeoffs_source_job_type_quantity_takeoff",
        ),
        UniqueConstraint(
            "id",
            "project_id",
            "drawing_revision_id",
            name="uq_quantity_takeoffs_id_project_id_drawing_revision_id",
        ),
        UniqueConstraint(
            "source_job_id",
            name="uq_quantity_takeoffs_source_job_id",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique quantity takeoff identifier (UUID v4)",
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "projects.id",
            name="fk_quantity_takeoffs_project_id_projects",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Owning project identifier",
    )
    source_file_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Immutable source file identifier for the takeoff lineage",
    )
    drawing_revision_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Pinned drawing revision identifier used for the takeoff",
    )
    source_job_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Quantity job that produced this immutable takeoff",
    )
    source_job_type: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        default=JobType.QUANTITY_TAKEOFF.value,
        comment="Denormalized job type used by the composite source-job contract",
    )
    validation_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Validation status inherited by downstream takeoff consumers",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Quantity takeoff creation timestamp",
    )


class QuantityItem(Base):
    """Immutable itemized quantity lineage rows for a takeoff."""

    __tablename__ = "quantity_items"
    __table_args__ = (
        ForeignKeyConstraint(
            ["quantity_takeoff_id", "project_id", "drawing_revision_id"],
            [
                "quantity_takeoffs.id",
                "quantity_takeoffs.project_id",
                "quantity_takeoffs.drawing_revision_id",
            ],
            ondelete="RESTRICT",
            name="fk_quantity_items_takeoff_lineage",
        ),
        ForeignKeyConstraint(
            ["drawing_revision_id", "source_entity_id"],
            ["revision_entities.drawing_revision_id", "revision_entities.entity_id"],
            ondelete="RESTRICT",
            name="fk_quantity_items_source_entity",
        ),
        CheckConstraint(
            f"item_kind IN ({_sql_in_list(_QUANTITY_ITEM_KIND_VALUES)})",
            name="ck_quantity_items_item_kind_valid",
        ),
        CheckConstraint(
            f"validation_status IN ({_sql_in_list(_QUANTITY_VALIDATION_STATUS_VALUES)})",
            name="ck_quantity_items_validation_status_valid",
        ),
        CheckConstraint(
            "value IS NULL OR (value >= 0::float8 AND value < 'Infinity'::float8)",
            name="ck_quantity_items_value_nonnegative_finite",
        ),
        CheckConstraint(
            _QUANTITY_ITEM_SOURCE_ENTITY_CONTRACT,
            name="ck_quantity_items_kind_source_entity_contract",
        ),
        CheckConstraint(
            _QUANTITY_ITEM_VALUE_CONTRACT,
            name="ck_quantity_items_kind_value_contract",
        ),
        CheckConstraint(
            "quantity_type <> ''",
            name="ck_quantity_items_quantity_type_nonempty",
        ),
        CheckConstraint(
            "unit <> ''",
            name="ck_quantity_items_unit_nonempty",
        ),
        CheckConstraint(
            "json_typeof(excluded_source_entity_ids_json) = 'array'",
            name="ck_quantity_items_excluded_source_entity_ids_json_array",
        ),
        UniqueConstraint(
            "id",
            "quantity_takeoff_id",
            "project_id",
            "drawing_revision_id",
            name="uq_quantity_items_id_takeoff_project_drawing_revision",
        ),
        Index(
            "ix_quantity_items_drawing_revision_id_source_entity_id",
            "drawing_revision_id",
            "source_entity_id",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique quantity item identifier (UUID v4)",
    )
    quantity_takeoff_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Parent quantity takeoff identifier",
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "projects.id",
            name="fk_quantity_items_project_id_projects",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Owning project identifier",
    )
    drawing_revision_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        index=True,
        comment="Pinned drawing revision identifier for entity provenance lookups",
    )
    item_kind: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Quantity lineage row kind (contributor, aggregate, exclusion, conflict)",
    )
    quantity_type: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="Deterministic quantity classification label",
    )
    value: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        comment="Non-negative finite quantity value when a numeric contribution exists",
    )
    unit: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="Display/storage unit captured for the persisted quantity value",
    )
    validation_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Validation status for this itemized quantity row",
    )
    source_entity_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        comment=(
            "Contributor/exclusion/conflict entity identifier when the item kind "
            "requires FK-backed entity provenance"
        ),
    )
    excluded_source_entity_ids_json: Mapped[Any] = mapped_column(
        JSON,
        nullable=False,
        server_default=text("'[]'::json"),
        comment="JSON array of exclusion/conflict source entity identifiers traced for the item",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Quantity item creation timestamp",
    )
