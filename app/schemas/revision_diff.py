"""Schemas for the revision-to-revision semantic diff endpoint."""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, Field


class RevisionDiffEntities(BaseModel):
    """Entity-level diff keyed by source identity (source_hash detects changes)."""

    added: int = Field(..., ge=0, description="Entities present in target but not base")
    removed: int = Field(..., ge=0, description="Entities present in base but not target")
    changed: int = Field(..., ge=0, description="Same source identity, different source_hash")
    unchanged: int = Field(..., ge=0, description="Same source identity and source_hash")
    unkeyed_base: int = Field(..., ge=0, description="Base entities with no source identity")
    unkeyed_target: int = Field(..., ge=0, description="Target entities with no source identity")
    added_ids: list[str] | None = Field(None, description="Added source identities (fields=added)")
    removed_ids: list[str] | None = Field(
        None, description="Removed source identities (fields=removed)"
    )
    changed_ids: list[str] | None = Field(
        None, description="Changed source identities (fields=changed)"
    )


class RevisionDiffLayers(BaseModel):
    """Layer-set delta (by layer reference)."""

    added: list[str] = Field(default_factory=list)
    removed: list[str] = Field(default_factory=list)


class RevisionDiffTypeDelta(BaseModel):
    """Per-entity-type count delta."""

    base: int = Field(..., ge=0)
    target: int = Field(..., ge=0)
    delta: int = Field(..., description="target - base")


class RevisionDiffCoverage(BaseModel):
    """Extraction-coverage delta between the two revisions (null when unavailable)."""

    base_mapped_ratio: float | None = None
    target_mapped_ratio: float | None = None
    mapped_ratio_delta: float | None = None


class RevisionDiffRead(BaseModel):
    """Deterministic structural diff between two revisions of the same file."""

    base_revision_id: UUID = Field(..., description="The 'against' (older) revision")
    target_revision_id: UUID = Field(..., description="The path (newer) revision")
    entities: RevisionDiffEntities
    layers: RevisionDiffLayers
    counts_by_type: dict[str, RevisionDiffTypeDelta] = Field(default_factory=dict)
    coverage: RevisionDiffCoverage
