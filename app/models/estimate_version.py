"""Append-only estimate version header persistence model."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Numeric,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, validates

from app.db.base import Base


class EstimateVersion(Base):
    """Immutable estimate header persisted from finalized estimation runs."""

    __tablename__ = "estimate_versions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["source_file_id", "project_id"],
            ["files.id", "files.project_id"],
            ondelete="RESTRICT",
            name="fk_estimate_versions_source_file_id_project_id_files",
        ),
        ForeignKeyConstraint(
            ["drawing_revision_id", "project_id", "source_file_id"],
            [
                "drawing_revisions.id",
                "drawing_revisions.project_id",
                "drawing_revisions.source_file_id",
            ],
            ondelete="RESTRICT",
            name="fk_estimate_versions_revision_lineage",
        ),
        ForeignKeyConstraint(
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
            ondelete="RESTRICT",
            name="fk_estimate_versions_takeoff_contract",
        ),
        ForeignKeyConstraint(
            ["source_job_id", "project_id", "source_file_id"],
            ["jobs.id", "jobs.project_id", "jobs.file_id"],
            ondelete="RESTRICT",
            name="fk_estimate_versions_source_job_lineage",
        ),
        CheckConstraint(
            "quantity_gate = 'allowed'",
            name="ck_estimate_versions_quantity_gate_allowed",
        ),
        CheckConstraint(
            "trusted_totals = TRUE",
            name="ck_estimate_versions_trusted_totals_true",
        ),
        CheckConstraint(
            "currency = 'GBP'",
            name="ck_estimate_versions_currency_gbp",
        ),
        CheckConstraint(
            "subtotal_amount::text <> 'NaN' AND subtotal_amount >= 0::numeric",
            name="ck_estimate_versions_subtotal_amount_nonnegative",
        ),
        CheckConstraint(
            "tax_amount::text <> 'NaN' AND tax_amount >= 0::numeric",
            name="ck_estimate_versions_tax_amount_nonnegative",
        ),
        CheckConstraint(
            "total_amount::text <> 'NaN' AND total_amount >= 0::numeric",
            name="ck_estimate_versions_total_amount_nonnegative",
        ),
        UniqueConstraint(
            "id",
            "project_id",
            "drawing_revision_id",
            name="uq_estimate_versions_id_project_id_drawing_revision_id",
        ),
        UniqueConstraint(
            "source_job_id",
            name="uq_estimate_versions_source_job_id",
        ),
        Index("ix_estimate_versions_source_file_id", "source_file_id"),
        Index("ix_estimate_versions_quantity_takeoff_id", "quantity_takeoff_id"),
        Index("ix_estimate_versions_drawing_revision_id", "drawing_revision_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique estimate version identifier (UUID v4)",
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            "projects.id",
            name="fk_estimate_versions_project_id_projects",
            ondelete="RESTRICT",
        ),
        nullable=False,
        index=True,
        comment="Owning project identifier",
    )
    source_file_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Immutable source file identifier inherited from the pinned revision",
    )
    drawing_revision_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Pinned drawing revision identifier used to finalize this estimate",
    )
    quantity_takeoff_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Pinned quantity takeoff identifier used as the estimate input",
    )
    source_job_id: Mapped[uuid.UUID] = mapped_column(
        nullable=False,
        comment="Job identifier that finalized this immutable estimate version",
    )
    quantity_gate: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Frozen estimate input gate copied from the referenced quantity takeoff",
    )
    trusted_totals: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        comment="Frozen trusted-input posture copied from the referenced quantity takeoff",
    )
    currency: Mapped[str] = mapped_column(
        String(3),
        nullable=False,
        default="GBP",
        comment="Frozen estimate currency. MVP estimates persist GBP only",
    )
    subtotal_amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 2),
        nullable=False,
        comment="Frozen subtotal monetary amount for this estimate version",
    )
    tax_amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 2),
        nullable=False,
        comment="Frozen tax monetary amount for this estimate version",
    )
    total_amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 2),
        nullable=False,
        comment="Frozen total monetary amount for this estimate version",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Estimate version creation timestamp",
    )

    @validates("subtotal_amount", "tax_amount", "total_amount")
    def _validate_monetary_amount(self, field_name: str, value: Decimal) -> Decimal:
        if value.is_nan():
            raise ValueError(f"{field_name} must not be NaN")
        return value
