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
    bundle: bool = Field(
        False,
        description=(
            "True when a contributing run carried multiple services (a bundle, e.g. a "
            "shared-colour med-gas chase). Each service in the bundle is reported at the FULL "
            "corridor length, so this line's per-service length means 'present in the bundle', "
            "not an individually resolved split (#655). Group/discipline totals and per-room "
            "metres are unaffected; do not sum per-service bundle lengths as distinct pipe runs."
        ),
    )


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
    bundle_abstained: bool = Field(
        default=False,
        description=(
            "True when the tag-stack bundle detector abstained (ambiguous result or no "
            "confident multi-service sets found). When True, at least one multi-service "
            "run may be present but the system could not confirm bundle identity — "
            "reviewer must check bundle_evidence_absent_lines for affected items."
        ),
    )
    bundle_evidence_absent_lines: int = Field(
        default=0,
        ge=0,
        description=(
            "Count of output lines where bundle_evidence_absent=True: multi-service runs "
            "that did NOT overlap any confidently-matched bundle_service_sets by >=2 services "
            "and were therefore apportioned per-segment (not multiplied). "
            "Non-zero only when bundle_service_sets evidence is active (DWG, non-PDF)."
        ),
    )


class ServiceFillColourRead(BaseModel):
    """Attributed length for a single fill colour key (Phase 1 — opaque colour key only)."""

    colour_key: str = Field(..., description="Opaque per-drawing colour key (rgb hex or idx:<n>)")
    colour_index: int | None = Field(None, description="DWG ACI colour index, if present")
    colour_rgb: str | None = Field(None, description="RGB hex string, if present")
    length_m: float = Field(..., ge=0.0, description="Total attributed length in metres")
    service_name: str | None = Field(
        None,
        description=(
            "Service name resolved from the mechanical colour-key legend (#775); "
            "null when the colour has no legend entry or the legend is absent."
        ),
    )


class ServiceFillAttributionRead(BaseModel):
    """Fill-colour attribution result for one drawing revision (compute-on-read, Phase 1)."""

    per_colour: list[ServiceFillColourRead] = Field(
        default_factory=list,
        description="Per-colour attributed lengths, sorted by colour_key",
    )
    shared_length_m: float = Field(
        ..., ge=0.0, description="Length not clearly attributable to a single colour (manifold)"
    )
    total_length_m: float = Field(
        ..., ge=0.0, description="Total centerline length in metres (Σ per_colour + shared)"
    )
    centerline_segment_count: int = Field(
        ..., ge=0, description="Number of centerline line segments processed"
    )


class ServiceTagColourRead(BaseModel):
    """Tag-stack service+size assignment for one fill colour (Phase 3 / #674)."""

    colour_key: str = Field(..., description="Opaque per-drawing colour key (rgb hex or idx:<n>)")
    service: str = Field(..., description="Service abbreviation derived from tag-stack match")
    sizes: list[str] = Field(
        default_factory=list,
        description="Pipe size raw strings for this colour's stack run, in stack order",
    )
    size_kind: str | None = Field(None, description="'round' | 'rect' | None when mixed")
    discipline: str | None = Field(
        None,
        description=(
            "Legend discipline for this colour (context only — tag service is primary). "
            "Null when the colour has no legend entry."
        ),
    )


class ServiceTagAttributionRead(BaseModel):
    """Tag-stack service attribution result for one drawing revision (DWG only, #674)."""

    per_colour: list[ServiceTagColourRead] = Field(
        default_factory=list,
        description="Confident tag-stack matches, sorted by colour_key",
    )
    unmatched_colour_keys: list[str] = Field(
        default_factory=list,
        description="Colour keys present in bundle bands but not confidently matched",
    )
    matched_stack_count: int = Field(
        0, ge=0, description="Number of tag stacks successfully matched to bundle colours"
    )
    ambiguous: bool = Field(
        False,
        description="True when at least one abstain case was triggered during matching",
    )


class ServiceSegmentServiceRead(BaseModel):
    """Attributed centerline length for a single service (nearest-label, #687)."""

    service: str = Field(..., description="Service abbreviation from the parsed tag")
    length_m: float = Field(..., ge=0.0, description="Total attributed length in metres")


class ServiceSegmentSizeRead(BaseModel):
    """Attributed centerline length for a single (service, size) bucket (nearest-label, #687)."""

    service: str = Field(..., description="Service abbreviation from the parsed tag")
    size_raw: str | None = Field(None, description="Raw size string from tag (e.g. '54', '54x42')")
    size_kind: str | None = Field(None, description="'round' | 'rect' | None")
    length_m: float = Field(..., ge=0.0, description="Total attributed length in metres")


class ServiceSegmentLabelAttributionRead(BaseModel):
    """Per-segment nearest-label attribution result for one drawing revision (#687).

    Each synthesized centerline segment is attributed to its nearest parsed size-label
    within ``nearest_max_m`` metres. Segments beyond the radius land in ``unknown_length_m``.
    INVARIANT: Σ(per_service lengths) + unknown_length_m == total_length_m within ±0.1 m.
    """

    per_service: list[ServiceSegmentServiceRead] = Field(
        default_factory=list,
        description="Per-service attributed lengths, sorted by service",
    )
    per_size: list[ServiceSegmentSizeRead] = Field(
        default_factory=list,
        description="Per-(service, size) attributed lengths, sorted by (service, size_raw)",
    )
    unknown_length_m: float = Field(
        ..., ge=0.0, description="Length not attributed to any label (beyond nearest_max_m)"
    )
    total_length_m: float = Field(
        ..., ge=0.0, description="Total segment length in metres (Σ per_service + unknown)"
    )
    segment_count: int = Field(..., ge=0, description="Number of centerline segments processed")


class ServiceTakeoffResponse(BaseModel):
    """Routed-service takeoff for one drawing revision (compute-on-read)."""

    model_config = ConfigDict(extra="forbid")

    manifest: RevisionEntityManifestRead
    items: list[ServiceTakeoffLineRead] = Field(default_factory=list)
    summary: ServiceTakeoffSummaryRead
    scale: ServiceTakeoffScaleRead
    fill_attribution: ServiceFillAttributionRead | None = Field(
        default=None,
        description=(
            "Per-fill-colour attributed centerline lengths (DWG only; null for PDF or "
            "when no centerline segments are present). Phase 1 — colour key is opaque."
        ),
    )
    tag_service_attribution: ServiceTagAttributionRead | None = Field(
        default=None,
        description=(
            "Tag-stack service+size assignments per fill colour (DWG only; null for PDF or "
            "when no tag stacks are present). Tag-derived service OVERRIDES legend discipline "
            "for routed colours. Phase 3 / #674."
        ),
    )
    segment_label_attribution: ServiceSegmentLabelAttributionRead | None = Field(
        default=None,
        description=(
            "Per-segment nearest-label type attribution (DWG only; null for PDF or when no "
            "materialized geometry is present). Each centerline segment is attributed to its "
            "nearest parsed size-label within the tag radius. #687."
        ),
    )
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
