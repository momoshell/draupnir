"""Project model for the draupnir CAD/BIM ingestion system."""

import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class Project(Base):
    """SQLAlchemy ORM model for a Project.

    A project represents a top-level container for drawings, revisions,
    and estimates. Each project has a unique UUID identifier.
    """

    __tablename__ = "projects"
    __table_args__ = (
        CheckConstraint(
            "default_unit_system IS NULL OR default_unit_system IN ('metric', 'imperial')",
            name="ck_projects_default_unit_system",
        ),
        CheckConstraint(
            "default_currency IS NULL OR default_currency ~ '^[A-Z]{3}$'",
            name="ck_projects_default_currency",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique project identifier (UUID v4)",
    )
    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Project name",
    )
    description: Mapped[str | None] = mapped_column(
        String(1024),
        nullable=True,
        comment="Optional project description",
    )
    default_unit_system: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="Default unit system (e.g., 'metric', 'imperial')",
    )
    default_currency: Mapped[str | None] = mapped_column(
        String(3),
        nullable=True,
        comment="Default 3-letter uppercase currency code (e.g., 'USD', 'EUR')",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        comment="Project creation timestamp",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        onupdate=func.now(),
        comment="Project last update timestamp",
    )
