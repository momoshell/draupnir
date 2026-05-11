"""add idempotency key reservations

Revision ID: 2026_05_10_0009
Revises: 2026_05_10_0008
Create Date: 2026-05-10 20:40:00
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_05_10_0009"
down_revision: str | None = "2026_05_10_0008"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    """Create request idempotency reservations and replay snapshots."""

    op.create_table(
        "idempotency_keys",
        sa.Column(
            "id",
            sa.Uuid(),
            nullable=False,
            comment="Unique idempotency record identifier (UUID v4)",
        ),
        sa.Column(
            "key_hash",
            sa.String(length=64),
            nullable=False,
            comment="HMAC-SHA256 hash of the raw Idempotency-Key header value",
        ),
        sa.Column(
            "request_fingerprint",
            sa.String(length=64),
            nullable=False,
            comment="Canonical SHA-256 fingerprint of the mutating request payload",
        ),
        sa.Column(
            "request_method",
            sa.String(length=16),
            nullable=False,
            comment="HTTP method used for the reserved idempotent request",
        ),
        sa.Column(
            "request_path",
            sa.String(length=512),
            nullable=False,
            comment="Application route path used for the reserved idempotent request",
        ),
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text("'in_progress'"),
            comment="Reservation status: in_progress until a replay snapshot is finalized",
        ),
        sa.Column(
            "response_status_code",
            sa.Integer(),
            nullable=True,
            comment="Replayed HTTP status code for completed idempotent requests",
        ),
        sa.Column(
            "response_body_json",
            sa.JSON(),
            nullable=True,
            comment="Stored JSON response body snapshot for completed idempotent requests",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="Reservation creation timestamp",
        ),
        sa.Column(
            "completed_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="Completion timestamp when a replay snapshot becomes durable",
        ),
        sa.CheckConstraint(
            "status IN ('in_progress', 'completed')",
            name="ck_idempotency_keys_status_valid",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("key_hash", name="uq_idempotency_keys_key_hash"),
    )


def downgrade() -> None:
    """Drop request idempotency reservations and replay snapshots."""

    op.drop_table("idempotency_keys")
