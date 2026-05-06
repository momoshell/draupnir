"""Pydantic schemas for system capability and health endpoints."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field

from app.core.errors import ErrorCode
from app.ingestion.contracts import (
    AdapterStatus,
    AvailabilityReason,
    InputFamily,
    LicenseState,
    UploadFormat,
)


class SystemHealthStatus(StrEnum):
    """Top-level system health states."""

    OK = "ok"
    DEGRADED = "degraded"
    DOWN = "down"


class SystemCheckStatus(StrEnum):
    """Dependency and adapter health states."""

    OK = "ok"
    DEGRADED = "degraded"
    DOWN = "down"
    UNKNOWN = "unknown"


class ConfidenceRange(BaseModel):
    """Expected extraction confidence range for an adapter."""

    min: float = Field(..., ge=0, le=1, description="Lower bound of expected confidence")
    max: float = Field(..., ge=0, le=1, description="Upper bound of expected confidence")


class AdapterCapabilityRead(BaseModel):
    """Capability registry entry exposed by the system API."""

    adapter_key: str = Field(..., description="Stable adapter registry key")
    input_family: InputFamily = Field(..., description="Normalized input family")
    adapter_name: str = Field(..., description="Operator-facing adapter name")
    adapter_version: str | None = Field(None, description="Resolved adapter version if known")
    input_formats: list[UploadFormat] = Field(
        ...,
        description="Concrete source formats accepted",
    )
    output_formats: list[str] = Field(
        ...,
        description="Concrete normalized or export formats emitted",
    )
    status: AdapterStatus = Field(..., description="Resolved adapter availability")
    availability_reason: AvailabilityReason | None = Field(
        None,
        description="Primary availability reason when not fully available",
    )
    license_state: LicenseState = Field(..., description="Resolved runtime license posture")
    license_name: str | None = Field(
        None,
        description="Short license identifier for the adapter",
    )
    can_read: bool = Field(..., description="Whether the adapter can read inputs")
    can_write: bool = Field(..., description="Whether the adapter can write outputs")
    extracts_geometry: bool = Field(..., description="Whether the adapter extracts geometry")
    extracts_text: bool = Field(..., description="Whether the adapter extracts text")
    extracts_layers: bool = Field(..., description="Whether the adapter extracts layers")
    extracts_blocks: bool = Field(..., description="Whether the adapter extracts blocks")
    extracts_materials: bool = Field(..., description="Whether the adapter extracts materials")
    supports_exports: bool = Field(..., description="Whether the adapter supports exports")
    supports_quantity_hints: bool = Field(
        ...,
        description="Whether the adapter emits quantity hints",
    )
    supports_layout_selection: bool = Field(
        ...,
        description="Whether the adapter supports layout selection",
    )
    supports_xref_resolution: bool = Field(
        ...,
        description="Whether the adapter supports xref resolution",
    )
    experimental: bool = Field(
        ...,
        description="Whether the adapter is experimental or review-first",
    )
    confidence_range: ConfidenceRange | None = Field(
        None,
        description="Expected extraction confidence range",
    )
    bounded_probe_ms: int = Field(
        ...,
        ge=1,
        description="Maximum bounded probe budget in milliseconds",
    )
    last_checked_at: datetime | None = Field(
        None,
        description="Timestamp of the latest availability probe",
    )
    details: dict[str, object] | None = Field(
        None,
        description="Safe structured diagnostic metadata for operators",
    )


class SystemCapabilitiesResponse(BaseModel):
    """Response model for the system capabilities endpoint."""

    adapters: list[AdapterCapabilityRead] = Field(
        ...,
        description="Registered ingestion adapters",
    )


class DependencyHealthCheck(BaseModel):
    """Health check payload for a core dependency."""

    status: SystemCheckStatus = Field(..., description="Resolved dependency health")
    latency_ms: float | None = Field(
        default=None,
        ge=0,
        description="Probe latency in milliseconds",
    )
    details: dict[str, object] | None = Field(
        default=None,
        description="Safe structured dependency diagnostics",
    )


class AdapterHealthCheck(BaseModel):
    """Health check payload for a single adapter."""

    adapter_key: str = Field(..., description="Stable adapter registry key")
    status: SystemCheckStatus = Field(..., description="Resolved adapter health")
    error_code: ErrorCode | None = Field(
        default=None,
        description="Operator-safe adapter error code when the adapter is not healthy",
    )
    latency_ms: float | None = Field(
        default=None,
        ge=0,
        description="Probe latency in milliseconds",
    )
    details: dict[str, object] | None = Field(
        default=None,
        description="Safe structured adapter diagnostics",
    )


class SystemHealthChecks(BaseModel):
    """Grouped dependency and adapter health checks."""

    database: DependencyHealthCheck
    storage: DependencyHealthCheck
    broker: DependencyHealthCheck
    adapters: list[AdapterHealthCheck]


class SystemHealthResponse(BaseModel):
    """Response model for the system health endpoint."""

    status: SystemHealthStatus
    checks: SystemHealthChecks
