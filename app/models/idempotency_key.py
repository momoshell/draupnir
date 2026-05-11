"""Idempotency record model for mutating API requests."""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class IdempotencyStatus(StrEnum):
    """Persisted lifecycle states for request idempotency reservations."""

    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


_IDEMPOTENCY_STATUS_VALUES = tuple(state.value for state in IdempotencyStatus)


def _sql_in_list(values: tuple[str, ...]) -> str:
    """Render a SQL string list for check constraints."""

    return ", ".join(f"'{value}'" for value in values)


class IdempotencyKey(Base):
    """Stored reservation and replay snapshots for mutating requests."""

    __tablename__ = "idempotency_keys"
    __table_args__ = (
        UniqueConstraint("key_hash", name="uq_idempotency_keys_key_hash"),
        CheckConstraint(
            f"status IN ({_sql_in_list(_IDEMPOTENCY_STATUS_VALUES)})",
            name="ck_idempotency_keys_status_valid",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique idempotency record identifier (UUID v4)",
    )
    key_hash: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="HMAC-SHA256 hash of the raw Idempotency-Key header value",
    )
    request_fingerprint: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="Canonical SHA-256 fingerprint of the mutating request payload",
    )
    request_method: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        comment="HTTP method used for the reserved idempotent request",
    )
    request_path: Mapped[str] = mapped_column(
        String(512),
        nullable=False,
        comment="Application route path used for the reserved idempotent request",
    )
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=IdempotencyStatus.IN_PROGRESS.value,
        comment="Reservation status: in_progress until a replay snapshot is finalized",
    )
    response_status_code: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="Replayed HTTP status code for completed idempotent requests",
    )
    response_body_json: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Stored JSON response body snapshot for completed idempotent requests",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="Reservation creation timestamp",
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Completion timestamp when a replay snapshot becomes durable",
    )
