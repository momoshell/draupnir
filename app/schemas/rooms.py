"""Schemas for the room-containment interpretation endpoint."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.revision import RevisionEntityManifestRead, RevisionEntitySummary


class RoomBoundsRead(BaseModel):
    """Axis-aligned bounding box of a room polygon."""

    min_x: float
    min_y: float
    max_x: float
    max_y: float


class RoomRead(BaseModel):
    """A derived room with its geometry summary, name, and provenance."""

    id: str = Field(..., description="Room id (boundary entity id, or synthesized for wall faces)")
    name: str | None = Field(None, description="Room name from the tag text inside it, if any")
    source: str = Field(..., description="Provenance: explicit_layer | wall_polygonize")
    area: float = Field(..., ge=0.0, description="Polygon area in drawing units")
    bounds: RoomBoundsRead = Field(..., description="Axis-aligned bounding box")
    confidence: float | None = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Provenance caveat for heuristic sources; null when taken at face value",
    )


class DeviceRoomAssignmentRead(BaseModel):
    """A device assigned to the smallest room whose polygon contains it."""

    device_id: str = Field(..., description="Canonical device entity id")
    room_id: str = Field(..., description="Containing room id")


class RevisionRoomEntityListResponse(BaseModel):
    """Entities contained in a given room (centroid-in-polygon, smallest-containing)."""

    model_config = ConfigDict(from_attributes=True)

    manifest: RevisionEntityManifestRead
    room: RoomRead = Field(..., description="The resolved room the entities are contained in")
    items: list[RevisionEntitySummary] = Field(default_factory=list)
    total: int = Field(
        ..., ge=0, description="Total entities contained in the room (before pagination)"
    )
    next_cursor: str | None = Field(None, description="Opaque cursor for the next page, if any")


class RevisionRoomListResponse(BaseModel):
    """Rooms and device→room assignments derived for a revision."""

    model_config = ConfigDict(from_attributes=True)

    manifest: RevisionEntityManifestRead
    strategy: str = Field(..., description="Strategy used: explicit_layer | wall_polygonize")
    source_layers: list[str] = Field(
        default_factory=list, description="Layers the rooms were derived from"
    )
    items: list[RoomRead] = Field(default_factory=list)
    assignments: list[DeviceRoomAssignmentRead] = Field(default_factory=list)
    summary: dict[str, int] = Field(
        default_factory=dict, description="Counts (rooms, named_rooms, assigned_devices)"
    )
