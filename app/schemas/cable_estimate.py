"""Response schema for the cable-estimate endpoint (issue #698b).

Compute-on-read only — no persistence. Maps CableAssembly fields to a JSON-friendly
Pydantic model.
"""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class CircuitCableEstimateRead(BaseModel):
    """Combined cable estimate for a single circuit."""

    circuit_id: int = Field(..., description="Circuit identifier")
    device_drop_m: float = Field(
        ..., ge=0.0, description="Per-device vertical drop total (reliable)"
    )
    in_plan_length_m: float = Field(
        ..., ge=0.0, description="Spline/polyline chord lengths on drawing (schematic_provisional)"
    )
    home_run_m: float | None = Field(
        None, description="Panel→area routing along tray (estimated_routed); null when suppressed"
    )
    home_run_status: str = Field(
        ...,
        description=(
            "ok | no_anchor | unreachable_tray | disconnected_tray | bad_registration "
            "| no_containment_sheet"
        ),
    )
    base_total_m: float = Field(
        ..., ge=0.0, description="device_drop_m + in_plan_length_m + (home_run_m or 0)"
    )
    total_with_spare_m: float = Field(
        ..., ge=0.0, description="base_total_m * (1 + spare_fraction)"
    )


class CableEstimateResponse(BaseModel):
    """Cable-length estimate for one lighting drawing revision (compute-on-read, #698b)."""

    model_config = ConfigDict(extra="forbid")

    per_circuit: list[CircuitCableEstimateRead] = Field(
        default_factory=list, description="Per-circuit estimates, sorted by circuit_id"
    )
    total_device_drop_m: float = Field(
        ..., ge=0.0, description="Σ device drops over all distinct categorized devices"
    )
    total_in_plan_length_m: float = Field(
        ..., ge=0.0, description="Σ in-plan spline lengths across all circuits"
    )
    total_home_run_m: float = Field(
        ..., ge=0.0, description="Σ home-run lengths for status='ok' circuits"
    )
    unattributed_device_drop_m: float = Field(
        ...,
        ge=0.0,
        description="Drop total for categorized devices that appear in no circuit",
    )
    grand_base_m: float = Field(
        ..., ge=0.0, description="Σ per-circuit base totals + unattributed_device_drop_m"
    )
    grand_with_spare_m: float = Field(
        ..., ge=0.0, description="grand_base_m * (1 + spare_fraction)"
    )
    spare_fraction: float = Field(..., ge=0.0, description="Spare/waste fraction (e.g. 0.10 = 10%)")
    quantity_kind: str = Field(..., description="Always 'estimated' — never folded into MEASURED")
    reliability: dict[str, str] = Field(
        ..., description="Fixed reliability map per term (device_drop_m, in_plan_length_m, etc.)"
    )
    params_stamp: dict[str, object] = Field(
        ..., description="JSON-serialisable parameter stamp for full provenance"
    )
    home_run_suppressed_counts: dict[str, int] = Field(
        ..., description="Count of circuits suppressed per non-ok status"
    )
    registration_audit: dict[str, object] | None = Field(
        None,
        description=(
            "Grid-registration diagnostics from the two-revision coordinator; "
            "null when containment_revision_id was not supplied"
        ),
    )
    containment_revision_id: UUID | None = Field(
        None,
        description="Echo of the containment_revision_id query parameter (null when not supplied)",
    )
