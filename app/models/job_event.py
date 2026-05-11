"""SQLAlchemy ORM model for persisted job events."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, BigInteger, DateTime, ForeignKey, Identity, Index, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class JobEvent(Base):
    """Persisted lifecycle event for a background job."""

    __tablename__ = "job_events"
    __table_args__ = (
        Index(
            "ix_job_events_job_id_created_at_sequence_id_id",
            "job_id",
            "created_at",
            "sequence_id",
            "id",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey(
            "jobs.id",
            name="fk_job_events_job_id_jobs",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    level: Mapped[str] = mapped_column(String(length=32), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    data_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    sequence_id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=False),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
