# ruff: noqa: I001

"""Versioned estimation catalog models."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from sqlalchemy import CheckConstraint, Date, DateTime, ForeignKey, Index, Integer
from sqlalchemy import Numeric, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

from app.db.base import Base


class CatalogScopeType(StrEnum):
    GLOBAL = "global"
    PROJECT = "project"


class EstimationRate(Base):
    __tablename__ = "rate_catalog_entries"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    scope_type: Mapped[str] = mapped_column(String(length=16), nullable=False)
    project_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("projects.id", name="fk_rate_catalog_entries_project_id_projects"),
        nullable=True,
    )
    rate_key: Mapped[str] = mapped_column(String(length=255), nullable=False)
    source: Mapped[str] = mapped_column(String(length=64), nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    name: Mapped[str] = mapped_column(String(length=255), nullable=False)
    item_type: Mapped[str] = mapped_column(String(length=64), nullable=False)
    per_unit: Mapped[str] = mapped_column(String(length=64), nullable=False)
    currency: Mapped[str] = mapped_column(String(length=3), nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    effective_from: Mapped[date] = mapped_column(Date, nullable=False)
    effective_to: Mapped[date | None] = mapped_column(Date, nullable=True)
    checksum_sha256: Mapped[str] = mapped_column(String(length=64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )

    __table_args__ = (
        CheckConstraint(
            "scope_type IN ('global', 'project')",
            name="ck_rate_catalog_entries_scope_type",
        ),
        CheckConstraint(
            "(scope_type = 'global' AND project_id IS NULL) OR "
            "(scope_type = 'project' AND project_id IS NOT NULL)",
            name="ck_rate_catalog_entries_scope_project_id",
        ),
        CheckConstraint(
            "effective_to IS NULL OR effective_to > effective_from",
            name="ck_rate_catalog_entries_effective_window",
        ),
        CheckConstraint(
            "jsonb_typeof(metadata_json) = 'object'",
            name="ck_rate_catalog_entries_metadata_json_object",
        ),
        CheckConstraint(
            "checksum_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_rate_catalog_entries_checksum_sha256",
        ),
        Index(
            "ix_rate_catalog_entries_lookup",
            "scope_type",
            "project_id",
            "rate_key",
            "item_type",
            "per_unit",
            "currency",
            "effective_from",
        ),
    )


class EstimationMaterial(Base):
    __tablename__ = "material_catalog_entries"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    scope_type: Mapped[str] = mapped_column(String(length=16), nullable=False)
    project_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("projects.id", name="fk_material_catalog_entries_project_id_projects"),
        nullable=True,
    )
    material_key: Mapped[str] = mapped_column(String(length=255), nullable=False)
    source: Mapped[str] = mapped_column(String(length=64), nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    name: Mapped[str] = mapped_column(String(length=255), nullable=False)
    unit: Mapped[str] = mapped_column(String(length=64), nullable=False)
    currency: Mapped[str] = mapped_column(String(length=3), nullable=False)
    unit_cost: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    effective_from: Mapped[date] = mapped_column(Date, nullable=False)
    effective_to: Mapped[date | None] = mapped_column(Date, nullable=True)
    checksum_sha256: Mapped[str] = mapped_column(String(length=64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )

    __table_args__ = (
        CheckConstraint(
            "scope_type IN ('global', 'project')",
            name="ck_material_catalog_entries_scope_type",
        ),
        CheckConstraint(
            "(scope_type = 'global' AND project_id IS NULL) OR "
            "(scope_type = 'project' AND project_id IS NOT NULL)",
            name="ck_material_catalog_entries_scope_project_id",
        ),
        CheckConstraint(
            "effective_to IS NULL OR effective_to > effective_from",
            name="ck_material_catalog_entries_effective_window",
        ),
        CheckConstraint(
            "jsonb_typeof(metadata_json) = 'object'",
            name="ck_material_catalog_entries_metadata_json_object",
        ),
        CheckConstraint(
            "checksum_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_material_catalog_entries_checksum_sha256",
        ),
        Index(
            "ix_material_catalog_entries_lookup",
            "scope_type",
            "project_id",
            "material_key",
            "unit",
            "currency",
            "effective_from",
        ),
    )


class EstimationFormula(Base):
    __tablename__ = "formula_definitions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    scope_type: Mapped[str] = mapped_column(String(length=16), nullable=False)
    project_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("projects.id", name="fk_formula_definitions_project_id_projects"),
        nullable=True,
    )
    formula_id: Mapped[str] = mapped_column(String(length=255), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str] = mapped_column(String(length=255), nullable=False)
    dsl_version: Mapped[str] = mapped_column(String(length=32), nullable=False)
    output_key: Mapped[str] = mapped_column(String(length=255), nullable=False)
    output_contract_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    declared_inputs_json: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    expression_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    rounding_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    checksum_sha256: Mapped[str] = mapped_column(String(length=64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )

    __table_args__ = (
        CheckConstraint(
            "scope_type IN ('global', 'project')",
            name="ck_formula_definitions_scope_type",
        ),
        CheckConstraint(
            "(scope_type = 'global' AND project_id IS NULL) OR "
            "(scope_type = 'project' AND project_id IS NOT NULL)",
            name="ck_formula_definitions_scope_project_id",
        ),
        CheckConstraint(
            "version > 0",
            name="ck_formula_definitions_version_positive",
        ),
        CheckConstraint(
            "jsonb_typeof(output_contract_json) = 'object'",
            name="ck_formula_definitions_output_contract_json_object",
        ),
        CheckConstraint(
            "jsonb_typeof(declared_inputs_json) = 'array'",
            name="ck_formula_definitions_declared_inputs_json_array",
        ),
        CheckConstraint(
            "jsonb_typeof(expression_json) = 'object'",
            name="ck_formula_definitions_expression_json_object",
        ),
        CheckConstraint(
            "rounding_json IS NULL OR jsonb_typeof(rounding_json) = 'object'",
            name="ck_formula_definitions_rounding_json_object",
        ),
        CheckConstraint(
            "checksum_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_formula_definitions_checksum_sha256",
        ),
        Index(
            "ix_formula_definitions_lookup",
            "scope_type",
            "project_id",
            "formula_id",
            "version",
        ),
        Index(
            "uq_formula_definitions_global_version",
            "formula_id",
            "version",
            unique=True,
            postgresql_where=text("scope_type = 'global' AND project_id IS NULL"),
        ),
        Index(
            "uq_formula_definitions_project_version",
            "project_id",
            "formula_id",
            "version",
            unique=True,
            postgresql_where=text("scope_type = 'project' AND project_id IS NOT NULL"),
        ),
        Index(
            "uq_formula_definitions_global_checksum",
            "formula_id",
            "checksum_sha256",
            unique=True,
            postgresql_where=text("scope_type = 'global' AND project_id IS NULL"),
        ),
        Index(
            "uq_formula_definitions_project_checksum",
            "project_id",
            "formula_id",
            "checksum_sha256",
            unique=True,
            postgresql_where=text("scope_type = 'project' AND project_id IS NOT NULL"),
        ),
    )


class EstimationRateSupersession(Base):
    __tablename__ = "rate_catalog_entry_supersessions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    predecessor_rate_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey(
            "rate_catalog_entries.id",
            name="fk_rate_catalog_entry_supersessions_predecessor_rate_id",
        ),
        nullable=False,
        unique=True,
    )
    successor_rate_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey(
            "rate_catalog_entries.id",
            name="fk_rate_catalog_entry_supersessions_successor_rate_id",
        ),
        nullable=False,
        unique=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )

    __table_args__ = (
        CheckConstraint(
            "predecessor_rate_id <> successor_rate_id",
            name="ck_rate_catalog_entry_supersessions_distinct_nodes",
        ),
    )


class EstimationMaterialSupersession(Base):
    __tablename__ = "material_catalog_entry_supersessions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    predecessor_material_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey(
            "material_catalog_entries.id",
            name="fk_material_catalog_entry_supersessions_predecessor_material_id",
        ),
        nullable=False,
        unique=True,
    )
    successor_material_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey(
            "material_catalog_entries.id",
            name="fk_material_catalog_entry_supersessions_successor_material_id",
        ),
        nullable=False,
        unique=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )

    __table_args__ = (
        CheckConstraint(
            "predecessor_material_id <> successor_material_id",
            name="ck_material_catalog_entry_supersessions_distinct_nodes",
        ),
    )


class EstimationFormulaSupersession(Base):
    __tablename__ = "formula_definition_supersessions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    predecessor_formula_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey(
            "formula_definitions.id",
            name="fk_formula_definition_supersessions_predecessor_formula_id",
        ),
        nullable=False,
        unique=True,
    )
    successor_formula_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey(
            "formula_definitions.id",
            name="fk_formula_definition_supersessions_successor_formula_id",
        ),
        nullable=False,
        unique=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )

    __table_args__ = (
        CheckConstraint(
            "predecessor_formula_id <> successor_formula_id",
            name="ck_formula_definition_supersessions_distinct_nodes",
        ),
    )
