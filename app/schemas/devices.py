"""Schemas for the device / fixture-schedule interpretation endpoint."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.revision import RevisionEntityManifestRead


class DeviceTagRead(BaseModel):
    """A text label associated with a device by spatial proximity."""

    model_config = ConfigDict(from_attributes=True)

    entity_id: str = Field(..., description="Canonical entity id of the tag text")
    text: str = Field(..., description="Tag text content")
    layer_ref: str | None = Field(None, description="Layer of the tag text")
    distance: float = Field(..., ge=0.0, description="Distance from the device placement")


class DeviceRead(BaseModel):
    """A device instance (INSERT) with its resolved type, placement, and associated tag."""

    model_config = ConfigDict(from_attributes=True)

    entity_id: str = Field(..., description="Canonical entity id of the device INSERT")
    sequence_index: int = Field(..., ge=0, description="Zero-based entity position")
    block_ref: str | None = Field(None, description="Referenced block (device type)")
    layer_ref: str | None = Field(None, description="Device layer")
    position: dict[str, float] | None = Field(None, description="Planar placement (x, y)")
    tag: DeviceTagRead | None = Field(None, description="Nearest associated tag text, if any")


class DeviceScheduleEntry(BaseModel):
    """Device count grouped by block reference (device type)."""

    block_ref: str | None = Field(None, description="Referenced block (device type)")
    count: int = Field(..., ge=0, description="Number of placements of this device type")


class RevisionDeviceListResponse(BaseModel):
    """Paginated devices plus the fixture schedule for a drawing revision."""

    manifest: RevisionEntityManifestRead
    schedule: list[DeviceScheduleEntry] = Field(
        ..., description="Device counts by block reference across the whole revision"
    )
    items: list[DeviceRead] = Field(..., description="Device instances on this page")
    next_cursor: str | None = Field(None, description="Opaque cursor for the next page")
    association: dict[str, Any] = Field(
        ..., description="The association parameters used (tag layers, max distance, filters)"
    )
