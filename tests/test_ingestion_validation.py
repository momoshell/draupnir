"""Tests for ingest validation policy and report construction."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from app.ingestion.contracts import (
    AdapterResult,
    AdapterWarning,
    ConfidenceSummary,
    InputFamily,
    JSONValue,
    ProvenanceRecord,
)
from app.ingestion.finalization import (
    IngestFinalizationContext,
    build_ingest_finalization_payload,
)
from app.ingestion.validation import (
    ValidationOutcome,
    _has_valid_polygon_area_geometry,
    build_validation_outcome,
)

_GENERATED_AT = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
_EXPECTED_CHECK_KEYS = [
    "units_presence_normalization",
    "coordinate_system_capture",
    "geometry_validity",
    "closed_polygon_eligibility_for_area_quantities",
    "block_transform_validity",
    "layer_mapping_completeness",
    "xref_resolution_status",
    "pdf_scale_presence_calibration_status",
    "ifc_schema_support",
]


def _build_complete_canonical(
    *,
    entities: tuple[Mapping[str, JSONValue], ...] | None = None,
    geometry_validity: JSONValue | None = None,
    pdf_scale: JSONValue | None = None,
    ifc_schema: str | None = None,
    xrefs: tuple[Mapping[str, JSONValue], ...] | None = (),
) -> dict[str, JSONValue]:
    canonical_entities = entities or (
        {
            "kind": "line",
            "layer": "A-WALL",
            "start": {"x": 0.0, "y": 0.0},
            "end": {"x": 1.0, "y": 0.0},
        },
    )
    canonical: dict[str, JSONValue] = {
        "units": {"normalized": "meter"},
        "coordinate_system": {"name": "local"},
        "layouts": ({"name": "Model"},),
        "layers": ({"name": "A-WALL"},),
        "blocks": (),
        "entities": canonical_entities,
        "xrefs": xrefs,
    }
    if geometry_validity is not None:
        canonical["geometry_validity"] = geometry_validity
    if pdf_scale is not None:
        canonical["pdf_scale"] = pdf_scale
    if ifc_schema is not None:
        canonical["ifc_schema"] = ifc_schema

    return canonical


def _build_result(
    *,
    score: float = 0.95,
    input_family: InputFamily = InputFamily.DXF,
    canonical: Mapping[str, JSONValue] | None = None,
    review_required: bool = False,
    warnings: tuple[AdapterWarning, ...] = (),
) -> AdapterResult:
    if canonical is None:
        canonical = {"entities": ({"kind": "line"},)}

    return AdapterResult(
        canonical=canonical,
        provenance=(
            ProvenanceRecord(
                stage="extract",
                adapter_key=input_family.value,
                source_ref="originals/source.dat",
            ),
        ),
        confidence=ConfidenceSummary(
            score=score,
            review_required=review_required,
            basis=input_family.value,
        ),
        warnings=warnings,
    )


@pytest.mark.parametrize(
    ("score", "validation_status", "review_state", "quantity_gate"),
    [
        (0.59, "needs_review", "review_required", "review_gated"),
        (0.60, "valid", "provisional", "allowed_provisional"),
        (0.949, "valid", "provisional", "allowed_provisional"),
        (0.95, "valid", "approved", "allowed"),
    ],
)
def test_build_validation_outcome_applies_confidence_thresholds(
    score: float,
    validation_status: str,
    review_state: str,
    quantity_gate: str,
) -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json=_build_complete_canonical(),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=score),
        generated_at=_GENERATED_AT,
    )

    assert outcome.validation_status == validation_status
    assert outcome.review_state == review_state
    assert outcome.quantity_gate == quantity_gate


def test_build_validation_outcome_raster_is_review_first() -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.PDF_RASTER,
        canonical_json=_build_complete_canonical(pdf_scale={"ratio": "1:100"}),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.99, input_family=InputFamily.PDF_RASTER),
        generated_at=_GENERATED_AT,
    )

    assert outcome.confidence_score == 0.99
    assert outcome.effective_confidence == 0.59
    assert outcome.validation_status == "needs_review"
    assert outcome.review_state == "review_required"
    assert outcome.quantity_gate == "review_gated"
    assert outcome.report_json["findings"][0]["check_key"] == "raster_review_policy"


def test_build_validation_outcome_missing_pdf_scale_requires_review() -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.PDF_VECTOR,
        canonical_json=_build_complete_canonical(),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.99, input_family=InputFamily.PDF_VECTOR),
        generated_at=_GENERATED_AT,
    )

    pdf_scale_check = outcome.report_json["checks"][7]

    assert pdf_scale_check["check_key"] == "pdf_scale_presence_calibration_status"
    assert pdf_scale_check["status"] == "review_required"
    assert pdf_scale_check["details"] == {
        "applicable": True,
        "scale_present": False,
        "calibration_status": "missing",
    }
    assert outcome.validation_status == "needs_review"
    assert outcome.review_state == "review_required"
    assert outcome.quantity_gate == "review_gated"


def test_build_validation_outcome_missing_ifc_schema_blocks_quantities() -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.IFC,
        canonical_json=_build_complete_canonical(
            entities=({"kind": "product", "layer": "A-WALL"},),
            geometry_validity="valid",
        ),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.99, input_family=InputFamily.IFC),
        generated_at=_GENERATED_AT,
    )

    ifc_schema_check = outcome.report_json["checks"][8]

    assert ifc_schema_check["check_key"] == "ifc_schema_support"
    assert ifc_schema_check["status"] == "fail"
    assert ifc_schema_check["details"] == {
        "applicable": True,
        "schema_present": False,
        "supported": False,
    }
    assert outcome.effective_confidence == 0.0
    assert outcome.validation_status == "invalid"
    assert outcome.review_state == "rejected"
    assert outcome.quantity_gate == "blocked"


def test_build_validation_outcome_aggregates_adapter_warnings() -> None:
    warnings = (
        AdapterWarning(code="layer-map", message="Layer map incomplete"),
        AdapterWarning(code="xref", message="Xref unresolved", details={"source_ref": "xref:A"}),
    )
    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json=_build_complete_canonical(),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.95, warnings=warnings),
        generated_at=_GENERATED_AT,
    )

    assert outcome.validation_status == "valid_with_warnings"
    assert outcome.review_state == "approved"
    assert outcome.report_json["adapter_warnings"] == [
        {"code": "layer-map", "message": "Layer map incomplete", "details": None},
        {
            "code": "xref",
            "message": "Xref unresolved",
            "details": {"source_ref": "xref:A"},
        },
    ]
    assert [finding["source"] for finding in outcome.report_json["findings"]] == [
        "adapter_warning",
        "adapter_warning",
    ]
    assert outcome.report_json["summary"]["warnings_total"] == 2


def test_build_validation_outcome_finding_ids_are_deterministic_for_mixed_findings() -> None:
    warnings = (
        AdapterWarning(code="layer-map", message="Layer map incomplete"),
        AdapterWarning(code="xref", message="Xref unresolved", details={"source_ref": "xref:A"}),
    )
    canonical_json = _build_complete_canonical(
        entities=({"kind": "product", "layer": "A-WALL"},),
        geometry_validity="valid",
    )

    first_outcome = build_validation_outcome(
        input_family=InputFamily.IFC,
        canonical_json=canonical_json,
        canonical_entity_schema_version="0.1",
        result=_build_result(
            score=0.42,
            input_family=InputFamily.IFC,
            review_required=True,
            warnings=warnings,
        ),
        generated_at=_GENERATED_AT,
    )
    second_outcome = build_validation_outcome(
        input_family=InputFamily.IFC,
        canonical_json=canonical_json,
        canonical_entity_schema_version="0.1",
        result=_build_result(
            score=0.42,
            input_family=InputFamily.IFC,
            review_required=True,
            warnings=warnings,
        ),
        generated_at=_GENERATED_AT,
    )

    first_findings = first_outcome.report_json["findings"]
    second_findings = second_outcome.report_json["findings"]

    assert [finding["finding_id"] for finding in first_findings] == [
        f"finding-{index:03d}" for index in range(1, len(first_findings) + 1)
    ]
    assert [finding["check_key"] for finding in first_findings] == [
        "confidence_threshold",
        "adapter_review_required",
        "ifc_schema_support",
        "adapter_warning",
        "adapter_warning",
    ]
    assert [finding["severity"] for finding in first_findings] == [
        "warning",
        "warning",
        "error",
        "warning",
        "warning",
    ]
    assert first_findings == second_findings


def test_build_validation_outcome_emits_required_checks_in_deterministic_order() -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json=_build_complete_canonical(),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.95),
        generated_at=_GENERATED_AT,
    )

    checks = outcome.report_json["checks"]

    assert [check["check_key"] for check in checks] == _EXPECTED_CHECK_KEYS
    assert [check["status"] for check in checks[:7]] == ["pass"] * 7
    assert checks[3]["details"] == {"applicable": False}
    assert checks[4]["details"] == {"applicable": False}
    assert checks[6]["details"] == {"applicable": False}
    assert checks[7]["details"] == {"applicable": False}
    assert checks[8]["details"] == {"applicable": False}


def test_build_validation_outcome_minimal_dxf_requires_review_for_missing_mvp_checks() -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json={"entities": ({"kind": "line"},)},
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.99),
        generated_at=_GENERATED_AT,
    )

    checks_by_key = {check["check_key"]: check for check in outcome.report_json["checks"]}

    assert outcome.validation_status == "needs_review"
    assert outcome.review_state == "review_required"
    assert outcome.quantity_gate == "review_gated"
    assert checks_by_key["units_presence_normalization"]["status"] == "review_required"
    assert checks_by_key["coordinate_system_capture"]["status"] == "review_required"
    assert checks_by_key["geometry_validity"]["status"] == "review_required"
    assert checks_by_key["layer_mapping_completeness"]["status"] == "review_required"
    assert checks_by_key["xref_resolution_status"]["status"] == "review_required"


def test_build_validation_outcome_requires_geometry_evidence_for_line_entities() -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json=_build_complete_canonical(
            entities=({"kind": "line", "layer": "A-WALL"},),
        ),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.99),
        generated_at=_GENERATED_AT,
    )

    geometry_check = outcome.report_json["checks"][2]

    assert geometry_check["check_key"] == "geometry_validity"
    assert geometry_check["status"] == "review_required"
    assert outcome.validation_status == "needs_review"
    assert outcome.review_state == "review_required"
    assert outcome.quantity_gate == "review_gated"


@pytest.mark.parametrize(
    ("kind", "coordinate_field"),
    [("polygon", "points"), ("hatch", "points"), ("solid", "vertices")],
)
def test_build_validation_outcome_review_gates_two_point_closed_area_entities(
    kind: str,
    coordinate_field: str,
) -> None:
    entity: dict[str, JSONValue] = {
        "kind": kind,
        "layer": "A-WALL",
        coordinate_field: (
            {"x": 0.0, "y": 0.0},
            {"x": 1.0, "y": 0.0},
        ),
        "closed": True,
    }

    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json=_build_complete_canonical(entities=(entity,)),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.99),
        generated_at=_GENERATED_AT,
    )

    checks_by_key = {check["check_key"]: check for check in outcome.report_json["checks"]}

    assert checks_by_key["geometry_validity"]["status"] == "review_required"
    assert (
        checks_by_key["closed_polygon_eligibility_for_area_quantities"]["status"]
        == "pass"
    )
    assert outcome.validation_status == "needs_review"
    assert outcome.review_state == "review_required"
    assert outcome.quantity_gate == "review_gated"


def test_build_validation_outcome_review_gates_degenerate_closed_polygon() -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json=_build_complete_canonical(
            entities=(
                {
                    "kind": "polygon",
                    "layer": "A-WALL",
                    "points": (
                        {"x": 0.0, "y": 0.0},
                        {"x": 1.0, "y": 0.0},
                        {"x": 2.0, "y": 0.0},
                    ),
                    "closed": True,
                },
            ),
        ),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.99),
        generated_at=_GENERATED_AT,
    )

    checks_by_key = {check["check_key"]: check for check in outcome.report_json["checks"]}

    assert checks_by_key["geometry_validity"]["status"] == "review_required"
    assert outcome.validation_status == "needs_review"
    assert outcome.review_state == "review_required"
    assert outcome.quantity_gate == "review_gated"


@pytest.mark.parametrize(
    "points",
    [
        (
            {"x": 1e308, "y": 1e308},
            {"x": -1e308, "y": -1e308},
            {"x": 5e307, "y": 5e307},
        ),
        (
            {"x": 1e308, "y": 0.0},
            {"x": 0.0, "y": 1e308},
            {"x": 0.0, "y": 0.0},
        ),
    ],
)
def test_has_valid_polygon_area_geometry_rejects_nan_and_infinite_area(
    points: tuple[dict[str, float], ...],
) -> None:
    assert _has_valid_polygon_area_geometry(points) is False


def test_build_validation_outcome_review_gates_high_magnitude_collinear_polygon() -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json=_build_complete_canonical(
            entities=(
                {
                    "kind": "polygon",
                    "layer": "A-WALL",
                    "points": (
                        {"x": 1e308, "y": 1e308},
                        {"x": -1e308, "y": -1e308},
                        {"x": 5e307, "y": 5e307},
                    ),
                    "closed": True,
                },
            ),
        ),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.99),
        generated_at=_GENERATED_AT,
    )

    checks_by_key = {check["check_key"]: check for check in outcome.report_json["checks"]}

    assert checks_by_key["geometry_validity"]["status"] == "review_required"
    assert outcome.validation_status == "needs_review"
    assert outcome.review_state == "review_required"
    assert outcome.quantity_gate == "review_gated"


def test_build_validation_outcome_accepts_non_degenerate_closed_polygon() -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json=_build_complete_canonical(
            entities=(
                {
                    "kind": "polygon",
                    "layer": "A-WALL",
                    "points": (
                        {"x": 0.0, "y": 0.0},
                        {"x": 1.0, "y": 0.0},
                        {"x": 0.0, "y": 1.0},
                    ),
                    "closed": True,
                },
            ),
        ),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.95),
        generated_at=_GENERATED_AT,
    )

    checks_by_key = {check["check_key"]: check for check in outcome.report_json["checks"]}

    assert checks_by_key["geometry_validity"]["status"] == "pass"
    assert (
        checks_by_key["closed_polygon_eligibility_for_area_quantities"]["status"]
        == "pass"
    )
    assert outcome.validation_status == "valid"
    assert outcome.review_state == "approved"
    assert outcome.quantity_gate == "allowed"


def test_build_validation_outcome_keeps_explicit_geometry_validity_override() -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json=_build_complete_canonical(
            entities=(
                {
                    "kind": "polygon",
                    "layer": "A-WALL",
                    "points": (
                        {"x": 0.0, "y": 0.0},
                        {"x": 1.0, "y": 0.0},
                    ),
                    "closed": True,
                },
            ),
            geometry_validity="valid",
        ),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.99),
        generated_at=_GENERATED_AT,
    )

    checks_by_key = {check["check_key"]: check for check in outcome.report_json["checks"]}

    assert checks_by_key["geometry_validity"]["status"] == "pass"
    assert (
        checks_by_key["closed_polygon_eligibility_for_area_quantities"]["status"]
        == "pass"
    )
    assert outcome.validation_status == "valid"
    assert outcome.review_state == "approved"
    assert outcome.quantity_gate == "allowed"


@pytest.mark.parametrize(
    ("pdf_scale", "check_status", "validation_status", "review_state", "quantity_gate"),
    [
        ("", "fail", "invalid", "rejected", "blocked"),
        ({}, "fail", "invalid", "rejected", "blocked"),
        (False, "fail", "invalid", "rejected", "blocked"),
        ("placeholder", "review_required", "needs_review", "review_required", "review_gated"),
        (
            {"calibration_status": "unconfirmed", "ratio": "1:100"},
            "review_required",
            "needs_review",
            "review_required",
            "review_gated",
        ),
        ({"status": "invalid", "ratio": "1:100"}, "fail", "invalid", "rejected", "blocked"),
    ],
)
def test_build_validation_outcome_rejects_invalid_or_unconfirmed_pdf_scale_values(
    pdf_scale: JSONValue,
    check_status: str,
    validation_status: str,
    review_state: str,
    quantity_gate: str,
) -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.PDF_VECTOR,
        canonical_json=_build_complete_canonical(pdf_scale=pdf_scale),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.99, input_family=InputFamily.PDF_VECTOR),
        generated_at=_GENERATED_AT,
    )

    pdf_scale_check = outcome.report_json["checks"][7]

    assert pdf_scale_check["status"] == check_status
    assert outcome.validation_status == validation_status
    assert outcome.review_state == review_state
    assert outcome.quantity_gate == quantity_gate


def test_build_validation_outcome_includes_raw_provenance_in_report_json() -> None:
    result = _build_result(score=0.95)

    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json=_build_complete_canonical(),
        canonical_entity_schema_version="0.1",
        result=result,
        generated_at=_GENERATED_AT,
    )

    assert outcome.report_json["provenance"] == [
        {
            "stage": "extract",
            "adapter_key": "dxf",
            "source_ref": "originals/source.dat",
            "details": None,
        }
    ]


def test_build_ingest_finalization_payload_calls_validation_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[InputFamily, str]] = []
    fake_outcome = ValidationOutcome(
        confidence_score=0.95,
        effective_confidence=0.95,
        validation_status="valid",
        review_state="approved",
        quantity_gate="allowed",
        validator_name="ingestion.runner",
        validator_version="0.1",
        confidence_json={
            "score": 0.95,
            "effective_confidence": 0.95,
            "review_state": "approved",
            "review_required": False,
            "basis": "dxf",
        },
        adapter_warnings_json=[],
        report_json={
            "validation_report_schema_version": "0.1",
            "canonical_entity_schema_version": "0.1",
            "validation_status": "valid",
            "review_state": "approved",
            "quantity_gate": "allowed",
            "effective_confidence": 0.95,
            "validator_name": "ingestion.runner",
            "validator_version": "0.1",
            "confidence": {},
            "summary": {
                "checks_total": 9,
                "findings_total": 0,
                "warnings_total": 0,
                "errors_total": 0,
                "critical_total": 0,
                "check_status_totals": {
                    "pass": 9,
                    "warning": 0,
                    "review_required": 0,
                    "fail": 0,
                },
                "entity_counts": {
                    "layouts": 0,
                    "layers": 0,
                    "blocks": 0,
                    "entities": 1,
                },
            },
            "checks": [],
            "findings": [],
            "adapter_warnings": [],
            "generated_at": _GENERATED_AT.isoformat(),
        },
    )

    def fake_build_validation_outcome(
        *,
        input_family: InputFamily,
        canonical_json: Mapping[str, JSONValue],
        canonical_entity_schema_version: str,
        result: AdapterResult,
        generated_at: datetime,
    ) -> ValidationOutcome:
        _ = canonical_json
        _ = result
        _ = generated_at
        calls.append((input_family, canonical_entity_schema_version))
        return fake_outcome

    monkeypatch.setattr(
        "app.ingestion.finalization.build_validation_outcome",
        fake_build_validation_outcome,
    )

    payload = build_ingest_finalization_payload(
        IngestFinalizationContext(
            job_id=uuid4(),
            file_id=uuid4(),
            extraction_profile_id=None,
            initial_job_id=None,
            input_family=InputFamily.DXF,
            adapter_key="ezdxf",
            adapter_version="test-1.0",
        ),
        result=_build_result(score=0.95),
        generated_at=_GENERATED_AT,
    )

    assert calls == [(InputFamily.DXF, "0.1")]
    assert payload.validation_status == "valid"
    assert payload.review_state == "approved"
