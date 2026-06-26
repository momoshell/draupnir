"""Response schema for the per-floor takeoff endpoint (issue #717, Phase R-C skeleton).

This module defines the read schema for a floor-level takeoff response. It is a
skeleton — R-D (measured quantities), R-E (counted items), and R-F (estimated
circuits) will each add fields to their respective sub-blocks. Do NOT add
``extra="forbid"`` to any populatable block; leaf metadata models are fine.

This module must NOT be imported by ``main.py`` or any router until R-G.
"""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

# ---------------------------------------------------------------------------
# Leaf metadata models (extra="forbid" acceptable — small, stable)
# ---------------------------------------------------------------------------


class MemberRegistrationRead(BaseModel):
    """A member type registered for takeoff on this floor."""

    model_config = ConfigDict(extra="forbid")

    member_type: str = Field(..., description="Member type identifier (e.g. cable, pipe, duct)")
    display_name: str = Field(..., description="Human-readable display label")
    unit: str = Field(..., description="Unit of measure for quantities (e.g. m, ea)")


class FloorMemberRead(BaseModel):
    """A takeoff member instance on this floor."""

    model_config = ConfigDict(extra="forbid")

    member_id: str = Field(..., description="Canonical member entity id")
    member_type: str = Field(..., description="Member type identifier")
    display_name: str | None = Field(None, description="Display label override for this member")


# ---------------------------------------------------------------------------
# Per-room kind sub-blocks (R-D/E/F populate fields; no extra="forbid")
# ---------------------------------------------------------------------------


class RoomMeasuredItem(BaseModel):
    """One measured quantity item attributed to a room (R-D, length in metres)."""

    member_type: str = Field(..., description="Member type this measurement covers")
    length_m: float = Field(..., ge=0.0, description="Total measured length in metres")
    reliability: str = Field(
        ..., description="Reliability label: reliable | schematic_provisional | estimated_routed"
    )


class RoomCountedItem(BaseModel):
    """One counted item attributed to a room (R-E, device/fitting count)."""

    member_type: str = Field(..., description="Member type this count covers")
    count: int = Field(..., ge=0, description="Number of items in this room")


class RoomEstimatedBlock(BaseModel):
    """Estimated-circuit data for a room (R-F, cable estimate terms).

    Populated incrementally by R-F; no extra="forbid" so future fields can be added.
    """

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
    """A single estimated-circuit record in the unassigned block (R-F overflow)."""

    circuit_id: int = Field(..., description="Circuit identifier")
    length_m: float = Field(..., ge=0.0, description="Estimated length for this circuit in metres")
    reliability: str = Field(..., description="Reliability label for the estimate")


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
    estimated_circuits: list[EstimatedCircuitItem] = Field(
        default_factory=list,
        description="Estimated circuits that could not be assigned to a room (R-F)",
    )


# ---------------------------------------------------------------------------
# Top-level response
# ---------------------------------------------------------------------------


class FloorTakeoffResponse(BaseModel):
    """Per-floor takeoff response — skeleton for Phase R (issue #676).

    Each room is represented by a :class:`RoomBlock` under ``rooms``. Items
    that cannot be attributed to any room appear in ``unassigned``. Future
    phases (R-D through R-G) will populate the kind sub-blocks.
    """

    revision_id: UUID = Field(..., description="Drawing revision this takeoff covers")
    floor_id: str | None = Field(
        None, description="Floor / storey identifier, if known from the drawing"
    )
    rooms: list[RoomBlock] = Field(
        default_factory=list,
        description="Takeoff data grouped by room, in room-number order",
    )
    unassigned: UnassignedBlock = Field(
        default_factory=UnassignedBlock,
        description="Items that could not be attributed to any room",
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
