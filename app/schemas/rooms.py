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


class RoomLocationRead(BaseModel):
    """A label-derived room's point location (when it has no polygon)."""

    x: float
    y: float


class RoomRead(BaseModel):
    """A derived room with its (optional) geometry, name/number, and provenance.

    Label-derived rooms (#549) have a ``number`` and a ``location`` but no polygon, so
    ``area`` / ``bounds`` are null; geometric rooms carry ``area`` / ``bounds`` and may
    have ``number`` stamped from a contained label.
    """

    id: str = Field(..., description="Room id (boundary entity id, or synthesized)")
    name: str | None = Field(None, description="Room name from the tag text, if resolved")
    number: str | None = Field(None, description="Room number from the tag (e.g. 0.9.01), if any")
    source: str = Field(
        ..., description="Provenance: explicit_layer | wall_polygonize | label_cluster"
    )
    area: float | None = Field(
        None, ge=0.0, description="Polygon area in drawing units; null for label-only rooms"
    )
    bounds: RoomBoundsRead | None = Field(
        None, description="Axis-aligned bounding box; null for label-only rooms"
    )
    location: RoomLocationRead | None = Field(
        None, description="Label point for label-only rooms; null when a polygon is present"
    )
    anchors: list[RoomLocationRead] = Field(
        default_factory=list,
        description=(
            "All label anchor points for a (possibly merged) label-only room (#581); a room "
            "labelled several times under one number keeps every placement. Empty for polygons."
        ),
    )
    needs_review: bool = Field(
        False,
        description=(
            "The same room number was found in regions separated by walls (a likely numbering "
            "error) — surfaced for review, not silently merged (#581)."
        ),
    )
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
