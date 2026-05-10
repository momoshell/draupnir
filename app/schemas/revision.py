"""Pydantic schemas for revision discoverability endpoints."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class DrawingRevisionRead(BaseModel):
    """Schema for reading drawing revision metadata."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(..., description="Unique drawing revision identifier")
    project_id: UUID = Field(..., description="Owning project identifier")
    source_file_id: UUID = Field(..., description="Source file identifier")
    extraction_profile_id: UUID = Field(..., description="Extraction profile identifier")
    source_job_id: UUID = Field(..., description="Source ingest or reprocess job identifier")
    adapter_run_output_id: UUID = Field(..., description="Associated adapter output identifier")
    predecessor_revision_id: UUID | None = Field(
        None,
        description="Previous revision identifier when this revision descends from another",
    )
    revision_sequence: int = Field(..., ge=1, description="Monotonic revision sequence")
    revision_kind: str = Field(..., description="Revision lifecycle kind")
    review_state: str = Field(..., description="Revision review state")
    canonical_entity_schema_version: str = Field(
        ...,
        description="Canonical entity schema version used by the revision",
    )
    confidence_score: float = Field(..., description="Effective revision confidence score")
    created_at: datetime = Field(..., description="Revision creation timestamp")


class DrawingRevisionListResponse(BaseModel):
    """Schema for file revision list responses."""

    items: list[DrawingRevisionRead] = Field(
        default_factory=list,
        description="Ordered drawing revisions for the requested file",
    )
    next_cursor: str | None = Field(
        None,
        description="Opaque pagination cursor for the next revision page when more results exist",
    )


class AdapterRunOutputRead(BaseModel):
    """Schema for reading adapter output metadata."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(..., description="Unique adapter output identifier")
    project_id: UUID = Field(..., description="Owning project identifier")
    source_file_id: UUID = Field(..., description="Source file identifier")
    extraction_profile_id: UUID = Field(..., description="Extraction profile identifier")
    source_job_id: UUID = Field(..., description="Source ingest or reprocess job identifier")
    adapter_key: str = Field(..., description="Adapter implementation key")
    adapter_version: str = Field(..., description="Adapter implementation version")
    input_family: str = Field(..., description="Normalized source input family")
    canonical_entity_schema_version: str = Field(
        ...,
        description="Canonical entity schema version emitted by the adapter",
    )
    confidence_score: float = Field(..., description="Adapter output confidence score")
    result_checksum_sha256: str = Field(
        ...,
        min_length=64,
        max_length=64,
        description="Checksum for the persisted adapter result payload",
    )
    created_at: datetime = Field(..., description="Adapter output creation timestamp")


class GeneratedArtifactRead(BaseModel):
    """Schema for reading generated artifact metadata."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(..., description="Unique generated artifact identifier")
    project_id: UUID = Field(..., description="Owning project identifier")
    source_file_id: UUID = Field(..., description="Source file identifier")
    job_id: UUID = Field(..., description="Source job identifier")
    drawing_revision_id: UUID | None = Field(
        None,
        description="Associated drawing revision identifier when applicable",
    )
    adapter_run_output_id: UUID | None = Field(
        None,
        description="Associated adapter output identifier when applicable",
    )
    artifact_kind: str = Field(..., description="Artifact kind")
    name: str = Field(..., description="Artifact display name")
    format: str = Field(..., description="Artifact output format")
    media_type: str = Field(..., description="Artifact media type")
    size_bytes: int = Field(..., ge=0, description="Artifact size in bytes")
    checksum_sha256: str = Field(
        ...,
        min_length=64,
        max_length=64,
        description="Artifact checksum",
    )
    generator_name: str = Field(..., description="Artifact generator name")
    generator_version: str = Field(..., description="Artifact generator version")
    predecessor_artifact_id: UUID | None = Field(
        None,
        description="Previous artifact identifier when this artifact supersedes another",
    )
    created_at: datetime = Field(..., description="Artifact creation timestamp")


class GeneratedArtifactListResponse(BaseModel):
    """Schema for generated artifact list responses."""

    items: list[GeneratedArtifactRead] = Field(
        default_factory=list,
        description="Generated artifacts visible for the requested resource",
    )
    next_cursor: str | None = Field(
        None,
        description="Opaque pagination cursor for the next artifact page when more results exist",
    )
