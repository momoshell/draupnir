"""Schemas for the containment-type takeoff endpoint (issue #756, Phase 752c).

Compute-on-read: no persistence. ADR-005 compliant — no QuantityTakeoff / QuantityItem
types imported or referenced.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class ContainmentTypeRead(BaseModel):
    """Attributed length for a single resolved containment type."""

    containment_type: str | None = Field(
        None,
        description=(
            "Legend-resolved containment type; null = honest-absent "
            "(band present but unmapped/unlabelled)."
        ),
    )
    length_m: float = Field(..., ge=0.0, description="Attributed length in metres")
    member_colour_keys: list[str] = Field(
        default_factory=list,
        description="Sorted, deduplicated colour keys of bands that mapped to this type",
    )
    member_pattern_names: list[str] = Field(
        default_factory=list,
        description="Sorted, deduplicated HATCH pattern names of bands that mapped to this type",
    )


class ContainmentLabelTypeRead(BaseModel):
    """Attributed length recovered from an unmapped segment via its nearest run-label."""

    containment_type: str = Field(..., description="Run-label SERVICE token, e.g. 'FA DECTN ALM'")
    length_m: float = Field(..., ge=0.0, description="Attributed length in metres")
    basis: str = Field(
        ...,
        description="Provenance marker; 'run_label' for lengths recovered via the label mechanism",
    )


class ContainmentTakeoffResponse(BaseModel):
    """Containment-type takeoff for one drawing revision (compute-on-read)."""

    model_config = ConfigDict(extra="forbid")

    per_type: list[ContainmentTypeRead] = Field(
        default_factory=list,
        description=(
            "Per-containment-type attributed lengths, sorted by containment_type "
            "(None sorts last, after all real strings)."
        ),
    )
    shared_length_m: float = Field(
        ...,
        ge=0.0,
        description=(
            "Geometrically-ambiguous centerline length (engine __shared__ bucket); "
            "distinct from honest-absent."
        ),
    )
    total_length_m: float = Field(
        ...,
        ge=0.0,
        description="Total centerline length in metres (Σ per_type + shared_length_m)",
    )
    centerline_segment_count: int = Field(
        ..., ge=0, description="Number of centerline line segments processed"
    )
    label_attributed: list[ContainmentLabelTypeRead] = Field(
        default_factory=list,
        description=(
            "Lengths recovered from unmapped segments via the run-label mechanism; "
            "sorted by containment_type; basis='run_label'. "
            "Never merged into legend totals."
        ),
    )
    label_unknown_length_m: float = Field(
        0.0,
        ge=0.0,
        description=(
            "Centerline length the legend left unmapped AND with no run-label within "
            "the distance cap — honest-UNKNOWN; never a fabricated type."
        ),
    )
