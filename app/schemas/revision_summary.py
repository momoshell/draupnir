"""Compact per-revision orientation summary schema (aggregation of existing signals)."""

from uuid import UUID

from pydantic import BaseModel, Field

from app.schemas.revision import RevisionMaterializationCounts, RevisionScaleRead
from app.schemas.validation_report import ValidationReportCoverage


class RevisionSummaryRead(BaseModel):
    """One-call orientation for a revision: already-computed signals, aggregated.

    Pure aggregation of the dedicated read resources — entity/layer/device/room
    counts, scale/units, and the extraction-coverage block — so an agent can get
    its bearings in a single request instead of fanning out across endpoints.
    """

    revision_id: UUID = Field(..., description="Drawing revision identifier")
    entity_counts: RevisionMaterializationCounts = Field(
        ...,
        description="Materialized row totals (layouts / layers / blocks / entities)",
    )
    entities_by_type: dict[str, int] = Field(
        default_factory=dict,
        description="Entity counts keyed by canonical type (from coverage); empty if unavailable",
    )
    layer_count: int = Field(..., ge=0, description="Number of materialized layers")
    layer_roles: dict[str, int] = Field(
        default_factory=dict,
        description="Layer counts keyed by derived semantic role (background/foreground/...)",
    )
    device_count: int = Field(..., ge=0, description="Enumerated device instances (default depth)")
    room_count: int = Field(..., ge=0, description="Interpreted rooms (default auto strategy)")
    named_room_count: int = Field(..., ge=0, description="Interpreted rooms with a resolved name")
    scale: RevisionScaleRead = Field(..., description="Drawing scale + units (see /scale)")
    coverage: ValidationReportCoverage | None = Field(
        None,
        description="Extraction-coverage block (see /validation-report); null if no report",
    )
