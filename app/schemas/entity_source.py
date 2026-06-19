"""Schema for the source-anchored entity drill-down endpoint."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class RevisionEntitySourceRead(BaseModel):
    """The raw source fragment behind a canonical entity, re-derived from the original.

    ``available`` is true when the original source was re-opened and the addressed
    native object (DXF entity, IFC product) was found; otherwise ``reason`` explains
    why (unsupported format, no recorded address, or fragment not found).
    """

    revision_id: UUID
    entity_id: str = Field(..., description="Canonical entity id")
    source_identity: str | None = Field(None, description="Recorded source address (handle/GUID)")
    source_hash: str | None = Field(None, description="Source content hash recorded at ingestion")
    upload_format: str | None = Field(None, description="Source upload format (dxf/dwg/ifc/pdf)")
    available: bool = Field(..., description="Whether the raw fragment was re-derived")
    fragment: dict[str, Any] | None = Field(
        None, description="The raw native object (type + attributes) when available"
    )
    reason: str | None = Field(None, description="Why the fragment is unavailable, if not")
