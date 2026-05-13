"""Pydantic schemas for revision discoverability endpoints."""

from datetime import datetime
from typing import Any
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


class RevisionMaterializationCounts(BaseModel):
    """Summary counts for revision-scoped normalized materialization rows."""

    layouts: int = Field(..., ge=0, description="Total materialized layout rows")
    layers: int = Field(..., ge=0, description="Total materialized layer rows")
    blocks: int = Field(..., ge=0, description="Total materialized block rows")
    entities: int = Field(..., ge=0, description="Total materialized entity rows")


class RevisionEntityManifestRead(BaseModel):
    """Schema for normalized-entity materialization manifest metadata."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(..., description="Unique revision entity materialization manifest identifier")
    project_id: UUID = Field(..., description="Owning project identifier")
    source_file_id: UUID = Field(..., description="Source file identifier")
    extraction_profile_id: UUID = Field(..., description="Extraction profile identifier")
    source_job_id: UUID = Field(..., description="Source materialization job identifier")
    drawing_revision_id: UUID = Field(..., description="Drawing revision identifier")
    adapter_run_output_id: UUID = Field(..., description="Associated adapter output identifier")
    canonical_entity_schema_version: str = Field(
        ...,
        description="Canonical entity schema version used by the materialized revision",
    )
    counts: RevisionMaterializationCounts = Field(
        validation_alias="counts_json",
        description="Materialized normalized row counts for the revision",
    )
    created_at: datetime = Field(..., description="Manifest creation timestamp")


class RevisionLayoutRead(BaseModel):
    """Schema for reading a materialized revision layout row."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(..., description="Unique materialized layout row identifier")
    project_id: UUID = Field(..., description="Owning project identifier")
    source_file_id: UUID = Field(..., description="Source file identifier")
    extraction_profile_id: UUID = Field(..., description="Extraction profile identifier")
    source_job_id: UUID = Field(..., description="Source materialization job identifier")
    drawing_revision_id: UUID = Field(..., description="Drawing revision identifier")
    adapter_run_output_id: UUID = Field(..., description="Associated adapter output identifier")
    canonical_entity_schema_version: str = Field(
        ...,
        description="Canonical entity schema version for this layout row",
    )
    sequence_index: int = Field(..., ge=0, description="Zero-based layout position")
    layout_ref: str = Field(..., description="Stable layout reference")
    payload: dict[str, Any] = Field(
        validation_alias="payload_json",
        description="Curated canonical layout payload",
    )
    created_at: datetime = Field(..., description="Materialization timestamp")


class RevisionLayerRead(BaseModel):
    """Schema for reading a materialized revision layer row."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(..., description="Unique materialized layer row identifier")
    project_id: UUID = Field(..., description="Owning project identifier")
    source_file_id: UUID = Field(..., description="Source file identifier")
    extraction_profile_id: UUID = Field(..., description="Extraction profile identifier")
    source_job_id: UUID = Field(..., description="Source materialization job identifier")
    drawing_revision_id: UUID = Field(..., description="Drawing revision identifier")
    adapter_run_output_id: UUID = Field(..., description="Associated adapter output identifier")
    canonical_entity_schema_version: str = Field(
        ...,
        description="Canonical entity schema version for this layer row",
    )
    sequence_index: int = Field(..., ge=0, description="Zero-based layer position")
    layer_ref: str = Field(..., description="Stable layer reference")
    payload: dict[str, Any] = Field(
        validation_alias="payload_json",
        description="Curated canonical layer payload",
    )
    created_at: datetime = Field(..., description="Materialization timestamp")


class RevisionBlockRead(BaseModel):
    """Schema for reading a materialized revision block row."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(..., description="Unique materialized block row identifier")
    project_id: UUID = Field(..., description="Owning project identifier")
    source_file_id: UUID = Field(..., description="Source file identifier")
    extraction_profile_id: UUID = Field(..., description="Extraction profile identifier")
    source_job_id: UUID = Field(..., description="Source materialization job identifier")
    drawing_revision_id: UUID = Field(..., description="Drawing revision identifier")
    adapter_run_output_id: UUID = Field(..., description="Associated adapter output identifier")
    canonical_entity_schema_version: str = Field(
        ...,
        description="Canonical entity schema version for this block row",
    )
    sequence_index: int = Field(..., ge=0, description="Zero-based block position")
    block_ref: str = Field(..., description="Stable block reference")
    payload: dict[str, Any] = Field(
        validation_alias="payload_json",
        description="Curated canonical block payload",
    )
    created_at: datetime = Field(..., description="Materialization timestamp")


class RevisionEntityRead(BaseModel):
    """Schema for reading a materialized revision entity row."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(..., description="Unique materialized entity row identifier")
    project_id: UUID = Field(..., description="Owning project identifier")
    source_file_id: UUID = Field(..., description="Source file identifier")
    extraction_profile_id: UUID = Field(..., description="Extraction profile identifier")
    source_job_id: UUID = Field(..., description="Source materialization job identifier")
    drawing_revision_id: UUID = Field(..., description="Drawing revision identifier")
    adapter_run_output_id: UUID = Field(..., description="Associated adapter output identifier")
    canonical_entity_schema_version: str = Field(
        ...,
        description="Canonical entity schema version for this entity row",
    )
    sequence_index: int = Field(..., ge=0, description="Zero-based entity position")
    entity_id: str = Field(..., description="Stable entity identifier")
    entity_type: str = Field(..., description="Canonical entity type")
    entity_schema_version: str = Field(..., description="Entity schema version")
    parent_entity_ref: str | None = Field(
        None,
        description="Raw parent entity reference from the canonical payload",
    )
    confidence_score: float = Field(..., description="Entity confidence score")
    confidence: dict[str, Any] = Field(
        validation_alias="confidence_json",
        description="Entity confidence payload",
    )
    geometry: dict[str, Any] = Field(
        validation_alias="geometry_json",
        description="Entity geometry payload",
    )
    properties: dict[str, Any] = Field(
        validation_alias="properties_json",
        description="Entity properties payload",
    )
    provenance: dict[str, Any] = Field(
        validation_alias="provenance_json",
        description="Entity provenance payload",
    )
    layout_ref: str | None = Field(None, description="Raw layout reference")
    layer_ref: str | None = Field(None, description="Raw layer reference")
    block_ref: str | None = Field(None, description="Raw block reference")
    source_identity: str | None = Field(None, description="Stable source identity")
    source_hash: str | None = Field(None, description="Stable source hash")
    layout_id: UUID | None = Field(None, description="Resolved materialized layout row identifier")
    layer_id: UUID | None = Field(None, description="Resolved materialized layer row identifier")
    block_id: UUID | None = Field(None, description="Resolved materialized block row identifier")
    parent_entity_row_id: UUID | None = Field(
        None,
        description="Resolved materialized parent entity row identifier",
    )
    created_at: datetime = Field(..., description="Materialization timestamp")


class RevisionMaterializationListResponseBase(BaseModel):
    """Shared metadata for revision materialization list responses."""

    manifest: RevisionEntityManifestRead = Field(
        ...,
        description="Manifest metadata for the materialized revision",
    )
    counts: RevisionMaterializationCounts = Field(
        ...,
        description="Revision-level materialized row counts",
    )
    next_cursor: str | None = Field(
        None,
        description="Opaque pagination cursor for the next page when more results exist",
    )


class RevisionLayoutListResponse(RevisionMaterializationListResponseBase):
    """Schema for revision layout list responses."""

    items: list[RevisionLayoutRead] = Field(
        default_factory=list,
        description="Materialized layout rows for the requested revision",
    )


class RevisionLayerListResponse(RevisionMaterializationListResponseBase):
    """Schema for revision layer list responses."""

    items: list[RevisionLayerRead] = Field(
        default_factory=list,
        description="Materialized layer rows for the requested revision",
    )


class RevisionBlockListResponse(RevisionMaterializationListResponseBase):
    """Schema for revision block list responses."""

    items: list[RevisionBlockRead] = Field(
        default_factory=list,
        description="Materialized block rows for the requested revision",
    )


class RevisionEntityListResponse(RevisionMaterializationListResponseBase):
    """Schema for revision entity list responses."""

    items: list[RevisionEntityRead] = Field(
        default_factory=list,
        description="Materialized entity rows for the requested revision",
    )
