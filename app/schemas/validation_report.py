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
    review_state: str
    quantity_gate: str
    effective_confidence: float
    entity_counts: dict[str, int] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow")


class ValidationReportResponse(BaseModel):
    """Canonical validation report returned by the API."""

    validation_report_id: UUID
    drawing_revision_id: UUID
    source_job_id: UUID
    validation_report_schema_version: str
    canonical_entity_schema_version: str
    validation_status: str
    review_state: str
    quantity_gate: str
    effective_confidence: float
    validator: ValidationReportValidator
    generated_at: datetime
    summary: ValidationReportSummary
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
    summary["review_state"] = report.review_state
    summary["quantity_gate"] = report.quantity_gate
    summary["effective_confidence"] = report.effective_confidence

    confidence_json = report_json.get("confidence")
    confidence = dict(confidence_json) if isinstance(confidence_json, dict) else {}
    confidence["effective_confidence"] = report.effective_confidence
    confidence["review_state"] = report.review_state
    confidence["review_required"] = report.review_state == "review_required"

    report_json["validation_report_id"] = report.id
    report_json["drawing_revision_id"] = report.drawing_revision_id
    report_json["source_job_id"] = report.source_job_id
    report_json["validation_report_schema_version"] = report.validation_report_schema_version
    report_json["canonical_entity_schema_version"] = report.canonical_entity_schema_version
    report_json["validation_status"] = report.validation_status
    report_json["review_state"] = report.review_state
    report_json["quantity_gate"] = report.quantity_gate
    report_json["effective_confidence"] = report.effective_confidence
    report_json["validator"] = validator
    report_json["confidence"] = confidence
    report_json["generated_at"] = report.generated_at
    report_json["summary"] = summary

    return ValidationReportResponse.model_validate(report_json)
