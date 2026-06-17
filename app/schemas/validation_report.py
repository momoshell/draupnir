"""Validation report response schemas."""

from copy import deepcopy
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.models.validation_report import ValidationReport


class ValidationReportValidator(BaseModel):
    """Validator metadata for a persisted validation report."""

    name: str
    version: str

    model_config = ConfigDict(extra="allow")


class ValidationReportSummary(BaseModel):
    """Authoritative validation summary details."""

    validation_status: str
    entity_counts: dict[str, int] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow")


class CoverageEntities(BaseModel):
    """Entity-level extraction coverage counts."""

    total: int
    mapped: int
    unmapped: int
    mapped_ratio: float
    by_type: dict[str, int] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow")


class CoverageLayers(BaseModel):
    """Layer resolution coverage."""

    count: int
    entities_with_layer_ref: int
    # Provenance of the layer set: native ("ocg") vs derived ("pen_signature") vs the
    # source adapter's own table; null when the adapter does not record it.
    source: str | None = None

    model_config = ConfigDict(extra="allow")


class CoverageBlocks(BaseModel):
    """Block reference coverage."""

    count: int
    child_geometry_count: int

    model_config = ConfigDict(extra="allow")


class ValidationReportCoverage(BaseModel):
    """Honest extraction-coverage metrics (replacement signal for the confidence score)."""

    schema_version: str
    entities: CoverageEntities
    unmapped_by_reason: dict[str, int] = Field(default_factory=dict)
    layers: CoverageLayers
    blocks: CoverageBlocks
    review_flagged_entities: int
    # Present only when the adapter records per-type entity counts in metadata.
    adapter_counts: dict[str, int] | None = None

    model_config = ConfigDict(extra="allow")


class ValidationReportResponse(BaseModel):
    """Canonical validation report returned by the API."""

    validation_report_id: UUID
    drawing_revision_id: UUID
    source_job_id: UUID
    validation_report_schema_version: str
    canonical_entity_schema_version: str
    validation_status: str
    validator: ValidationReportValidator
    generated_at: datetime
    summary: ValidationReportSummary
    coverage: ValidationReportCoverage | None = None
    checks: list[Any] = Field(default_factory=list)
    findings: list[Any] = Field(default_factory=list)
    adapter_warnings: list[Any] = Field(default_factory=list)
    provenance: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow")


def build_validation_report_response(report: ValidationReport) -> ValidationReportResponse:
    """Merge persisted JSON with authoritative database columns for API output."""
    report_json = deepcopy(report.report_json)

    validator_json = report_json.get("validator")
    validator = dict(validator_json) if isinstance(validator_json, dict) else {}
    validator["name"] = report.validator_name
    validator["version"] = report.validator_version

    summary_json = report_json.get("summary")
    summary = dict(summary_json) if isinstance(summary_json, dict) else {}
    summary["validation_status"] = report.validation_status
    # Path B 5a: confidence/gate fields are no longer exposed. Strip any persisted
    # copies so they don't leak through the model's extra="allow" passthrough.
    for vestigial_key in ("review_state", "quantity_gate", "effective_confidence"):
        summary.pop(vestigial_key, None)
        report_json.pop(vestigial_key, None)
    report_json.pop("confidence", None)

    report_json["validation_report_id"] = report.id
    report_json["drawing_revision_id"] = report.drawing_revision_id
    report_json["source_job_id"] = report.source_job_id
    report_json["validation_report_schema_version"] = report.validation_report_schema_version
    report_json["canonical_entity_schema_version"] = report.canonical_entity_schema_version
    report_json["validation_status"] = report.validation_status
    report_json["validator"] = validator
    report_json["generated_at"] = report.generated_at
    report_json["summary"] = summary

    return ValidationReportResponse.model_validate(report_json)
