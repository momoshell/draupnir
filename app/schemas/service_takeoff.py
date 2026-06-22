"""Schemas for the routed-service takeoff endpoint (issue #606, P3 / be-p3-03).

Compute-on-read: no persistence. ADR-005 compliant -- no QuantityTakeoff / QuantityItem
types imported or referenced.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.revision import RevisionEntityManifestRead


class ServiceTakeoffLineRead(BaseModel):
    """One aggregated (service, size, room) length bucket from the takeoff coordinator."""

    service: str = Field(..., description="Service abbreviation or 'unknown'")
    size_raw: str | None = Field(None, description="Raw size string from tag (e.g. '54', '54x42')")
    size_kind: str | None = Field(None, description="'round' | 'rect' | None")
    discipline: str | None = Field(None, description="Resolved discipline (e.g. 'medical-gas')")
    room_id: str = Field(
        ..., description="Containing room id, or 'service-takeoff-unassigned' when outside rooms"
    )
    room_name: str | None = Field(None, description="Room name, if resolved")
    room_number: str | None = Field(None, description="Room number, if resolved")
    drawing_length: float = Field(..., ge=0.0, description="Summed drawn length in drawing units")
    real_length_m: float | None = Field(
        None, description="Real-world length in metres; null when unscaled"
    )
    basis: str = Field(
        ..., description="'real_world' when scale-gated; 'drawing_units_only' otherwise"
    )
    units_confidence: str = Field(
        ..., description="declared | confirmed | inferred | unknown (echoed from ScaleContext)"
    )
    run_count: int = Field(..., ge=0, description="Number of run groups aggregated in this line")
    identity_status: str = Field(
        ...,
        description="Worst identity status across contributing runs: resolved | partial | unknown",
    )
    confidence: float | None = Field(
        None, ge=0.0, le=1.0, description="Null in P3 (no scoring yet)"
    )
    riser_count: int = Field(0, ge=0, description="Rise symbols in this (unknown, room) bucket")
    drop_count: int = Field(0, ge=0, description="Drop symbols in this (unknown, room) bucket")


class ServiceTakeoffScaleRead(BaseModel):
    """Scale signals surfaced on the takeoff response (mirrors ScaleContext)."""

    units_confidence: str = Field(..., description="declared | confirmed | inferred | unknown")
    real_world_available: bool = Field(
        ..., description="True when a real-world conversion factor is present and not contradicted"
    )
    contradicted: bool = Field(
        ..., description="True when the units signal is internally contradicted (#558)"
    )
    conversion_factor: float | None = Field(
        None, description="Drawing-units to metres factor; null when unavailable"
    )


class ServiceTakeoffSummaryRead(BaseModel):
    """Aggregate counts for the takeoff response."""

    services: int = Field(..., ge=0, description="Distinct service abbreviations in items")
    sizes: int = Field(..., ge=0, description="Distinct (service, size_raw) pairs in items")
    rooms: int = Field(
        ..., ge=0, description="Distinct room_id values in items (including unassigned)"
    )
    lines: int = Field(..., ge=0, description="Total line items returned")
    unassigned_runs: int = Field(
        ..., ge=0, description="Run groups whose anchor fell outside all room polygons"
    )
    unknown_service_runs: int = Field(
        ..., ge=0, description="Run groups with no resolved service identity"
    )
    total_risers: int = Field(..., ge=0, description="Distinct rise symbols in this revision")
    total_drops: int = Field(..., ge=0, description="Distinct drop symbols in this revision")


class ServiceTakeoffResponse(BaseModel):
    """Routed-service takeoff for one drawing revision (compute-on-read)."""

    model_config = ConfigDict(extra="forbid")

    manifest: RevisionEntityManifestRead
    items: list[ServiceTakeoffLineRead] = Field(default_factory=list)
    summary: ServiceTakeoffSummaryRead
    scale: ServiceTakeoffScaleRead
    unscaled: bool = Field(
        ...,
        description=(
            "True when any line is drawing_units_only (no confirmed scale or "
            "contradicted units). Clients must treat real_length_m as informational."
        ),
    )
    length_provisional: bool = Field(
        default=False,
        description=(
            "True for PDF-vector revisions: lengths may double-count double-line walls "
            "and lack centerline separation (accuracy pending #618)."
        ),
    )
