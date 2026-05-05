"""Immutable extraction profile model for ingestion settings."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ExtractionProfile(Base):
    """SQLAlchemy ORM model for immutable extraction profiles."""

    __tablename__ = "extraction_profiles"
    __table_args__ = (
        UniqueConstraint(
            "id",
            "project_id",
            name="uq_extraction_profiles_id_project_id",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique extraction profile identifier (UUID v4)",
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Owning project identifier",
    )
    profile_version: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        comment="Extraction profile schema version",
    )
    units_override: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="Optional extraction units override",
    )
    layout_mode: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="Layout extraction mode",
    )
    xref_handling: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="External reference handling mode",
    )
    block_handling: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="Block extraction handling mode",
    )
    text_extraction: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        comment="Whether text extraction is enabled",
    )
    dimension_extraction: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        comment="Whether dimension extraction is enabled",
    )
    pdf_page_range: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        comment="Optional PDF page range selector",
    )
    raster_calibration: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Optional raster calibration payload",
    )
    confidence_threshold: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        default=0.6,
        comment="Minimum confidence threshold for extracted results",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Extraction profile creation timestamp",
    )
