"""Response schema for the per-floor takeoff endpoint (issue #717, Phase R-C/R-G).

This module defines the read schema for a floor-level takeoff response, fully
populated for Phase R-G.

Do NOT add ``extra="forbid"`` to any populatable block; leaf metadata models are fine.
"""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

# ---------------------------------------------------------------------------
# Leaf metadata models (extra="forbid" acceptable — small, stable)
# ---------------------------------------------------------------------------


class MemberRegistrationRead(BaseModel):
    """A member revision successfully co-registered for this floor takeoff."""

    model_config = ConfigDict(extra="forbid")

    revision_id: UUID = Field(..., description="Drawing revision UUID")
    role: str = Field(..., description="Caller-defined role label (e.g. containment, power)")
    quality: str = Field(..., description="Registration quality: good | degraded | failed")
    dx: float = Field(..., description="X translation in drawing units")
    dy: float = Field(..., description="Y translation in drawing units")
    rotation_rad: float = Field(..., description="Rotation in radians applied to align member")
    scale: float = Field(..., description="Scale factor (1.0 = no scale)")
    matched_count: int = Field(..., description="Number of matched grid fiducials")
    median_residual_m: float | None = Field(
        None, description="Median residual in metres after registration"
    )
    max_residual_m: float | None = Field(
        None, description="Max residual in metres after registration"
    )


class FailedMemberRead(BaseModel):
    """A member revision that failed the floor registration gate."""

    model_config = ConfigDict(extra="forbid")

    revision_id: UUID = Field(..., description="Drawing revision UUID")
    role: str = Field(..., description="Caller-defined role label")
    reason: str = Field(..., description="Reason for registration failure")


# ---------------------------------------------------------------------------
# Per-room kind sub-blocks (R-D/E/F populate fields; no extra="forbid")
# ---------------------------------------------------------------------------


class RoomMeasuredItem(BaseModel):
    """One measured quantity item attributed to a room (R-D, length in metres)."""

    member_revision_id: str = Field(
        ..., description="UUID of the drawing revision this geometry came from"
    )
    role: str = Field(..., description="Caller-defined role label for this member")
    service: str = Field(..., description="Resolved service name (or 'unknown')")
    size_raw: str | None = Field(None, description="Raw size string from the drawing")
    size_kind: str | None = Field(None, description="Normalised size kind")
    boundary_basis: str | None = Field(
        None, description="Evidence path: polygon | voronoi | null (unassigned)"
    )
    confidence: float | None = Field(None, ge=0.0, le=1.0, description="Assignment confidence")
    needs_review: bool = Field(False, description="True when the assignment is heuristic")
    drawing_length: float = Field(..., ge=0.0, description="Measured length in drawing units")
    real_length_m: float | None = Field(None, ge=0.0, description="Real-world length in metres")
    basis: str = Field(..., description="real_world | drawing_units_only")
    length_provisional: bool = Field(
        False, description="True when the length is provisional (e.g. from a double-line estimate)"
    )


class RoomCountedItem(BaseModel):
    """One counted item attributed to a room (R-E, device/fitting count)."""

    type_name: str = Field(..., description="Device/fitting type name")
    count: int = Field(..., ge=0, description="Number of items in this room")
    source_revision_id: str | None = Field(
        None,
        description="Source revision UUID for this count; null in v1 (counts_by_room doesn't "
        "retain per-(type,room) source)",
    )


class RoomEstimatedBlock(BaseModel):
    """Estimated-circuit data for a room (R-F, cable estimate terms).

    Populated incrementally by R-F; no extra="forbid" so future fields can be added.
    """

    quantity_kind: str = Field(
        "estimated",
        description="Always 'estimated' — NEVER summed with MEASURED",
    )
    device_drop_m: float = Field(
        0.0, ge=0.0, description="Total device-drop cable length attributed to this room"
    )
    home_run_m: float = Field(
        0.0, ge=0.0, description="Total home-run cable length attributed to this room"
    )
    circuit_ids: list[int] = Field(
        default_factory=list,
        description="Circuit identifiers whose estimate is attributed to this room",
    )


class RoomBlock(BaseModel):
    """All takeoff data attributed to one room.

    Three optional kind sub-blocks — measured (R-D), counted (R-E), estimated (R-F) —
    are separate fields so each phase can populate independently. Do NOT add any
    field that sums across kinds; aggregation belongs in the API layer.
    """

    room_id: str | None = Field(None, description="Room id; null for the unassigned block")
    room_number: str | None = Field(None, description="Room number (e.g. 0.9.01)")
    room_name: str | None = Field(None, description="Room name from the drawing tag")
    boundary_basis: str | None = Field(
        None, description="Evidence path: polygon | voronoi | null (unassigned)"
    )
    confidence: float | None = Field(
        None,
        ge=0.0,
        le=1.0,
        description=(
            "Assignment confidence: polygon-basis forwards the room's own confidence "
            "(may be non-null, e.g. 0.75 for wall-polygonize); voronoi-basis uses the "
            "lowered fallback value; null when no confidence is recorded."
        ),
    )
    needs_review: bool = Field(
        False,
        description=(
            "True when the assignment is heuristic (nested room, Voronoi, or numbering error)"
        ),
    )

    # Kind sub-blocks — populated by later phases; absent until then.
    measured: list[RoomMeasuredItem] = Field(
        default_factory=list,
        description="Measured quantity items for this room (R-D)",
    )
    counted: list[RoomCountedItem] = Field(
        default_factory=list,
        description="Counted items for this room (R-E)",
    )
    estimated: RoomEstimatedBlock | None = Field(
        None,
        description="Estimated-circuit block for this room (R-F)",
    )


class EstimatedCircuitItem(BaseModel):
    """A single estimated-circuit record at floor level (R-F, in-plan length)."""

    circuit_id: int = Field(..., description="Circuit identifier")
    in_plan_length_m: float = Field(
        ..., ge=0.0, description="In-plan length for this circuit in metres"
    )
    rooms_touched: list[str | None] = Field(
        default_factory=list,
        description=(
            "Room numbers touched by this circuit's devices (context only; may include null)"
        ),
    )


class UnassignedBlock(BaseModel):
    """Geometry or circuits that could not be attributed to any room.

    Populated by R-D/E/F phases; no extra="forbid" so future fields can be added.
    """

    measured: list[RoomMeasuredItem] = Field(
        default_factory=list,
        description="Measured items that could not be assigned to a room",
    )
    counted: list[RoomCountedItem] = Field(
        default_factory=list,
        description="Counted items that could not be assigned to a room",
    )
    estimated: RoomEstimatedBlock | None = Field(
        None,
        description="Estimated-circuit unassigned bucket for this floor (R-F)",
    )


class EstimatedMeta(BaseModel):
    """Floor-level metadata for estimated quantities."""

    model_config = ConfigDict(extra="forbid")

    quantity_kind: str = Field(..., description="Always 'estimated'")
    reliability: dict[str, str] = Field(..., description="Reliability labels from CableAssembly")
    params_stamp: dict[str, Any] = Field(
        ..., description="CableEstimateParams snapshot used to produce this estimate"
    )
    source: dict[str, str | None] = Field(
        ..., description="Provenance: lighting_revision_id, containment_revision_id"
    )


class FloorTakeoffSummary(BaseModel):
    """Summary statistics for the floor takeoff response."""

    model_config = ConfigDict(extra="forbid")

    rooms: int = Field(..., description="Total number of rooms in this takeoff")
    named_rooms: int = Field(..., description="Rooms with a non-null room_name (human label)")
    members_registered: int = Field(..., description="Members that passed registration")
    members_failed: int = Field(..., description="Members that failed registration")
    voronoi_fallback_rooms: int = Field(
        ..., description="Rooms that received at least one voronoi-basis item"
    )
    unscaled: bool = Field(
        ...,
        description="True when any measured item could not be converted to real-world metres",
    )
    no_anchor_fraction: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description=(
            "Fraction of total home-run length that is unassigned "
            "(home_run_in_unassigned / total_home_run). See no_anchor_status for "
            "the derived provisional gate classification."
        ),
    )
    no_anchor_status: Literal["ok", "elevated", "critical"] = Field(
        ...,
        description=(
            "Provisional gate derived from no_anchor_fraction (issue #735). "
            "Bands: ok (<=0.50), elevated (0.50-0.95, warn), critical (>0.95, regression). "
            "Thresholds are deliberately generous — single-building observed 0.92 → 'elevated'. "
            "'critical' only fires on near-total non-resolution. "
            "Re-evaluate once room coverage improves or a second building is available."
        ),
    )
    total_measured_m_by_discipline: dict[str, float] = Field(
        ...,
        description=(
            "Total real-world measured length in metres grouped by role "
            "(discipline). Sums real_length_m for items where basis=='real_world'."
        ),
    )


# ---------------------------------------------------------------------------
# Top-level response
# ---------------------------------------------------------------------------


class FloorTakeoffResponse(BaseModel):
    """Per-floor fused multi-discipline quantity takeoff (Phase R-G, issue #721).

    Aggregates measured, counted, and estimated quantities across all registered
    discipline sheets for a single floor, attributed to individual rooms.
    """

    reference_revision_id: UUID = Field(
        ..., description="Reference drawing revision that defines the registration frame"
    )
    reference_role: str = Field(
        ..., description="Role label for the reference revision (e.g. 'lighting')"
    )
    members: list[MemberRegistrationRead] = Field(
        default_factory=list,
        description="Member revisions that passed co-registration",
    )
    members_failed: list[FailedMemberRead] = Field(
        default_factory=list,
        description="Member revisions that failed co-registration",
    )
    rooms: list[RoomBlock] = Field(
        default_factory=list,
        description="Takeoff data grouped by room, in room-number order",
    )
    unassigned: UnassignedBlock = Field(
        default_factory=lambda: UnassignedBlock(estimated=None),
        description="Items that could not be attributed to any room",
    )
    estimated_circuits: list[EstimatedCircuitItem] = Field(
        default_factory=list,
        description="Floor-level in-plan circuit contributions (not per-room)",
    )
    estimated_meta: EstimatedMeta | None = Field(
        None,
        description="Floor-level estimated metadata (reliability, params_stamp, source); "
        "null when no estimated data is available",
    )
    room_registry_strategy: str = Field(
        ...,
        description=(
            "Room source strategy used: explicit_layer | wall_polygonize | ifc_space | auto"
        ),
    )
    voronoi_fallback_enabled: bool = Field(
        True,
        description="Whether Voronoi fallback was enabled when building the room registry",
    )
    summary: FloorTakeoffSummary = Field(
        ..., description="Aggregate statistics for this floor takeoff"
    )
