"""File model for immutable project uploads."""

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    desc,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class File(Base):
    """SQLAlchemy ORM model for uploaded source files."""

    __tablename__ = "files"
    __table_args__ = (
        UniqueConstraint("id", "project_id", name="uq_files_id_project_id"),
        Index(
            "ix_files_project_id_created_at_id_desc",
            "project_id",
            desc("created_at"),
            desc("id"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique file identifier (UUID v4)",
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Owning project identifier",
    )
    original_filename: Mapped[str] = mapped_column(
        String(512),
        nullable=False,
        comment="Original uploaded filename",
    )
    media_type: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Uploaded media type",
    )
    detected_format: Mapped[str | None] = mapped_column(
        String(32),
        nullable=True,
        comment="Detected drawing/document format",
    )
    storage_uri: Mapped[str] = mapped_column(
        String(1024),
        nullable=False,
        comment="Immutable local/object storage URI",
    )
    size_bytes: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Uploaded file size in bytes",
    )
    checksum_sha256: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="SHA-256 checksum of uploaded bytes",
    )
    immutable: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        nullable=False,
        comment="Whether the original upload is immutable",
    )
    initial_job_id: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        comment="Initial ingest job identifier created during upload",
    )
    initial_extraction_profile_id: Mapped[uuid.UUID | None] = mapped_column(
        nullable=True,
        comment="Initial extraction profile identifier created during upload",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        comment="File creation timestamp",
    )
