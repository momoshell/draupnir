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


class DeviceSemanticsRead(BaseModel):
    """Legend-resolved identity for one placed block instance.

    Built by the route via explicit kwargs (it crosses the frozen-dataclass boundary), so no
    ``from_attributes`` — it would be inert here (#588).
    """

    entity_id: str
    kind: str
    status: str
    type_name: str | None
    abbreviation: str | None
    description: str | None
    basis: str
    source_layers: list[str]
    confidence: float | None
    competing_type_names: list[str]


class DeviceRead(BaseModel):
    """A device instance (INSERT) with its resolved type, placement, and associated tag."""

    model_config = ConfigDict(from_attributes=True)

    entity_id: str = Field(..., description="Canonical entity id (nested instances use parent/idx)")
    sequence_index: int = Field(..., ge=0, description="Top-level INSERT position of this subtree")
    depth: int = Field(..., ge=0, description="Nesting depth in the block-instance tree (0 = top)")
    block_ref: str | None = Field(None, description="Referenced block (device type)")
    layer_ref: str | None = Field(None, description="Device layer")
    position: dict[str, float] | None = Field(None, description="World placement (x, y)")
    tag: DeviceTagRead | None = Field(None, description="Nearest associated tag text, if any")
    semantics: DeviceSemanticsRead = Field(..., description="Legend-resolved identity")


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
    legend: dict[str, Any] = Field(
        ..., description="Legend summary: legend_size, sources, resolved_count, unresolved_count"
    )
    schedule_by_type: list[dict[str, Any]] = Field(
        ..., description="Device counts by type_name (incl. unresolved and architecture buckets)"
    )
    kind: str = Field(..., description="Kind filter applied (device/architecture/all)")


class LegendDeviceRead(BaseModel):
    """A device located in a PDF body and typed via the legend dictionary."""

    abbreviation: str = Field(..., description="Legend tag found in the drawing body")
    type_name: str = Field(..., description="Device type from the legend description")
    position: dict[str, float] = Field(..., description="Tag placement (x, y) in page space")


class LegendDeviceScheduleEntry(BaseModel):
    """Count of one device type resolved from the legend."""

    abbreviation: str = Field(..., description="Legend tag")
    type_name: str = Field(..., description="Device type from the legend description")
    count: int = Field(..., ge=0, description="Number of located instances of this type")


class RevisionLegendDeviceListResponse(BaseModel):
    """Legend-anchored device schedule for a (vector PDF) revision."""

    model_config = ConfigDict(extra="forbid")

    schedule: list[LegendDeviceScheduleEntry] = Field(
        ..., description="Counts per device type, most frequent first"
    )
    items: list[LegendDeviceRead] = Field(..., description="Located device instances on this page")
    next_cursor: str | None = Field(None, description="Opaque cursor for the next page")
    summary: dict[str, Any] = Field(..., description="Legend size + total located devices")
