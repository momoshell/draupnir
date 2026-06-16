"""Unit tests for validation-report response schema coverage typing (no DB)."""

import uuid
from datetime import UTC, datetime

from app.models.validation_report import ValidationReport
from app.schemas.validation_report import (
    ValidationReportCoverage,
    build_validation_report_response,
)


def _coverage_block() -> dict[str, object]:
    """A representative coverage block as emitted by ``_build_coverage``."""

    return {
        "schema_version": "0.1",
        "entities": {
            "total": 10,
            "mapped": 8,
            "unmapped": 2,
            "mapped_ratio": 0.8,
            "by_type": {"line": 6, "text": 2, "unknown": 2},
        },
        "unmapped_by_reason": {"unsupported_record": 2},
        "layers": {
            "count": 3,
            "entities_with_layer_ref": 9,
            "source": "ocg",
        },
        "blocks": {
            "count": 1,
            "child_geometry_count": 4,
        },
        "review_flagged_entities": 1,
        "adapter_counts": {"line": 6, "text": 2},
    }


def test_coverage_model_validates_full_shape() -> None:
    """The typed coverage model should accept the full emitted shape."""

    coverage = ValidationReportCoverage.model_validate(_coverage_block())

    assert coverage.schema_version == "0.1"
    assert coverage.entities.total == 10
    assert coverage.entities.mapped == 8
    assert coverage.entities.unmapped == 2
    assert coverage.entities.mapped_ratio == 0.8
    assert coverage.entities.by_type == {"line": 6, "text": 2, "unknown": 2}
    assert coverage.unmapped_by_reason == {"unsupported_record": 2}
    assert coverage.layers.count == 3
    assert coverage.layers.entities_with_layer_ref == 9
    assert coverage.layers.source == "ocg"
    assert coverage.blocks.count == 1
    assert coverage.blocks.child_geometry_count == 4
    assert coverage.review_flagged_entities == 1
    assert coverage.adapter_counts == {"line": 6, "text": 2}


def test_coverage_model_allows_absent_adapter_counts_and_layer_source() -> None:
    """Optional fields should default cleanly when the adapter omits them."""

    block = _coverage_block()
    del block["adapter_counts"]
    block["layers"]["source"] = None  # type: ignore[index]

    coverage = ValidationReportCoverage.model_validate(block)

    assert coverage.adapter_counts is None
    assert coverage.layers.source is None


def _build_report(report_json: dict[str, object]) -> ValidationReport:
    """Construct an in-memory validation report row for response building."""

    return ValidationReport(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        drawing_revision_id=uuid.uuid4(),
        source_job_id=uuid.uuid4(),
        validation_report_schema_version="1.0",
        canonical_entity_schema_version="1.0",
        validation_status="valid",
        review_state="approved",
        quantity_gate="allowed",
        effective_confidence=0.9,
        validator_name="test-validator",
        validator_version="0.1",
        report_json=report_json,
        generated_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
    )


def test_build_response_exposes_typed_coverage() -> None:
    """A persisted coverage block should surface as a typed top-level field."""

    report = _build_report({"coverage": _coverage_block()})

    response = build_validation_report_response(report)

    assert response.coverage is not None
    assert response.coverage.entities.total == 10
    assert response.coverage.layers.source == "ocg"


def test_build_response_coverage_optional_when_absent() -> None:
    """Reports without a coverage block should remain valid with coverage=None."""

    report = _build_report({})

    response = build_validation_report_response(report)

    assert response.coverage is None
