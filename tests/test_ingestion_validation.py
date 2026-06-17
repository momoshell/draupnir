"""Tests for ingest validation policy and report construction."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from app.ingestion.canonical.entity_provenance import build_entity_provenance
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
    "entity_provenance_contract",
    "pdf_scale_presence_calibration_status",
    "ifc_schema_support",
]


def _valid_entity_provenance(index: int) -> dict[str, JSONValue]:
    source_ref = f"entities/{index}"
    source_identity = f"entity-{index}"
    source_hash = f"{index:064x}"

    _ = build_entity_provenance(
        origin="adapter_normalized",
        adapter="dxf",
        source_ref=source_ref,
        source_identity=source_identity,
        source_hash=source_hash,
        extraction_path=["entities", index - 1],
        notes=[],
    )
    return {
        "origin": "adapter_normalized",
        "adapter": "dxf",
        "source_ref": source_ref,
        "source_identity": source_identity,
        "source_hash": source_hash,
        "extraction_path": ("entities", index - 1),
        "notes": (),
    }


def _with_valid_provenance(entity: Mapping[str, JSONValue], *, index: int) -> dict[str, JSONValue]:
    return {**entity, "provenance": _valid_entity_provenance(index)}


def _build_complete_canonical(
    *,
    entities: tuple[Mapping[str, JSONValue], ...] | None = None,
    layers: tuple[Mapping[str, JSONValue], ...] | None = None,
    geometry_validity: JSONValue | None = None,
    pdf_scale: JSONValue | None = None,
    ifc_schema: str | None = None,
    xrefs: tuple[Mapping[str, JSONValue], ...] | None = (),
) -> dict[str, JSONValue]:
    default_entities: tuple[Mapping[str, JSONValue], ...] = (
        {
            "kind": "line",
            "layer": "A-WALL",
            "start": {"x": 0.0, "y": 0.0},
            "end": {"x": 1.0, "y": 0.0},
        },
    )
    source_entities = entities if entities is not None else default_entities
    source_layers: tuple[Mapping[str, JSONValue], ...] = (
        layers if layers is not None else ({"name": "A-WALL"},)
    )
    canonical_entities = tuple(
        _with_valid_provenance(entity, index=index)
        for index, entity in enumerate(source_entities, start=1)
    )
    canonical: dict[str, JSONValue] = {
        "units": {"normalized": "meter"},
        "coordinate_system": {"name": "local"},
        "layouts": ({"name": "Model"},),
        "layers": source_layers,
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
        canonical = {"entities": (_with_valid_provenance({"kind": "line"}, index=1),)}

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
    ("score", "validation_status"),
    [
        (0.59, "needs_review"),
        (0.60, "valid"),
        (0.949, "valid"),
        (0.95, "valid"),
    ],
)
def test_build_validation_outcome_applies_confidence_thresholds(
    score: float,
    validation_status: str,
) -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json=_build_complete_canonical(),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=score),
        generated_at=_GENERATED_AT,
    )

    assert outcome.validation_status == validation_status


def test_build_validation_outcome_raster_is_review_first() -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.PDF_RASTER,
        canonical_json=_build_complete_canonical(pdf_scale={"ratio": "1:100"}),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.99, input_family=InputFamily.PDF_RASTER),
        generated_at=_GENERATED_AT,
    )

    assert outcome.validation_status == "needs_review"
    assert outcome.report_json["findings"][0]["check_key"] == "raster_review_policy"


def test_build_validation_outcome_missing_pdf_scale_requires_review() -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.PDF_VECTOR,
        canonical_json=_build_complete_canonical(),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.99, input_family=InputFamily.PDF_VECTOR),
        generated_at=_GENERATED_AT,
    )

    pdf_scale_check = {check["check_key"]: check for check in outcome.report_json["checks"]}[
        "pdf_scale_presence_calibration_status"
    ]

    assert pdf_scale_check["check_key"] == "pdf_scale_presence_calibration_status"
    assert pdf_scale_check["status"] == "review_required"
    assert pdf_scale_check["details"] == {
        "applicable": True,
        "scale_present": False,
        "calibration_status": "missing",
    }
    assert outcome.validation_status == "needs_review"


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

    ifc_schema_check = {check["check_key"]: check for check in outcome.report_json["checks"]}[
        "ifc_schema_support"
    ]

    assert ifc_schema_check["check_key"] == "ifc_schema_support"
    assert ifc_schema_check["status"] == "fail"
    assert ifc_schema_check["details"] == {
        "applicable": True,
        "schema_present": False,
        "supported": False,
    }
    assert outcome.validation_status == "invalid"


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
    assert [check["status"] for check in checks[:8]] == ["pass"] * 8
    assert checks[3]["details"] == {"applicable": False}
    assert checks[4]["details"] == {"applicable": False}
    assert checks[6]["details"] == {"applicable": False}
    assert checks[7]["details"] == {
        "applicable": True,
        "entity_count": 1,
        "validated_entity_count": 1,
    }
    assert checks[8]["details"] == {"applicable": False}
    assert checks[9]["details"] == {"applicable": False}


@pytest.mark.parametrize(
    ("entity", "layers"),
    [
        (
            {
                "kind": "line",
                "layer_ref": "layer-a-wall",
                "start": {"x": 0.0, "y": 0.0},
                "end": {"x": 1.0, "y": 0.0},
            },
            ({"ref": "layer-a-wall", "name": "A-WALL"},),
        ),
        (
            {
                "kind": "line",
                "layer_ref": "A-WALL",
                "start": {"x": 0.0, "y": 0.0},
                "end": {"x": 1.0, "y": 0.0},
            },
            ({"name": "A-WALL"},),
        ),
        (
            {
                "kind": "line",
                "layer": "A-WALL",
                "layer_ref": "A-WALL",
                "start": {"x": 0.0, "y": 0.0},
                "end": {"x": 1.0, "y": 0.0},
            },
            ({"name": "A-WALL"},),
        ),
        (
            {
                "kind": "line",
                "layer_name": "A-WALL",
                "start": {"x": 0.0, "y": 0.0},
                "end": {"x": 1.0, "y": 0.0},
            },
            ({"name": "A-WALL"},),
        ),
    ],
)
def test_build_validation_outcome_accepts_declared_canonical_layer_identifiers(
    entity: Mapping[str, JSONValue],
    layers: tuple[Mapping[str, JSONValue], ...],
) -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json=_build_complete_canonical(
            entities=(entity,),
            layers=layers,
            geometry_validity="valid",
        ),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.99),
        generated_at=_GENERATED_AT,
    )

    layer_check = {check["check_key"]: check for check in outcome.report_json["checks"]}[
        "layer_mapping_completeness"
    ]

    assert layer_check["status"] == "pass"
    assert outcome.validation_status == "valid"


@pytest.mark.parametrize(
    ("entity", "layers", "expected_missing"),
    [
        (
            {
                "kind": "line",
                "layer_ref": "layer-a-wall",
                "start": {"x": 0.0, "y": 0.0},
                "end": {"x": 1.0, "y": 0.0},
            },
            ({"ref": "layer-other", "name": "A-WALL"},),
            ["layer-a-wall"],
        ),
        (
            {
                "kind": "line",
                "layer_name": "A-DOOR",
                "start": {"x": 0.0, "y": 0.0},
                "end": {"x": 1.0, "y": 0.0},
            },
            ({"name": "A-WALL"},),
            ["A-DOOR"],
        ),
    ],
)
def test_build_validation_outcome_reports_missing_canonical_layer_identifiers(
    entity: Mapping[str, JSONValue],
    layers: tuple[Mapping[str, JSONValue], ...],
    expected_missing: list[str],
) -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json=_build_complete_canonical(
            entities=(entity,),
            layers=layers,
            geometry_validity="valid",
        ),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.99),
        generated_at=_GENERATED_AT,
    )

    layer_check = {check["check_key"]: check for check in outcome.report_json["checks"]}[
        "layer_mapping_completeness"
    ]

    assert layer_check["status"] == "review_required"
    assert layer_check["details"]["missing_layers"] == expected_missing
    assert outcome.validation_status == "needs_review"


def test_build_validation_outcome_minimal_dxf_requires_review_for_missing_mvp_checks() -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json={"entities": (_with_valid_provenance({"kind": "line"}, index=1),)},
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.99),
        generated_at=_GENERATED_AT,
    )

    checks_by_key = {check["check_key"]: check for check in outcome.report_json["checks"]}

    assert outcome.validation_status == "needs_review"
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
    assert checks_by_key["closed_polygon_eligibility_for_area_quantities"]["status"] == "pass"
    assert outcome.validation_status == "needs_review"


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
    assert checks_by_key["closed_polygon_eligibility_for_area_quantities"]["status"] == "pass"
    assert outcome.validation_status == "valid"


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
    assert checks_by_key["closed_polygon_eligibility_for_area_quantities"]["status"] == "pass"
    assert outcome.validation_status == "valid"


@pytest.mark.parametrize(
    ("pdf_scale", "check_status", "validation_status"),
    [
        ("", "fail", "invalid"),
        ({}, "fail", "invalid"),
        (False, "fail", "invalid"),
        ("placeholder", "review_required", "needs_review"),
        (
            {"calibration_status": "unconfirmed", "ratio": "1:100"},
            "review_required",
            "needs_review",
        ),
        ({"status": "invalid", "ratio": "1:100"}, "fail", "invalid"),
    ],
)
def test_build_validation_outcome_rejects_invalid_or_unconfirmed_pdf_scale_values(
    pdf_scale: JSONValue,
    check_status: str,
    validation_status: str,
) -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.PDF_VECTOR,
        canonical_json=_build_complete_canonical(pdf_scale=pdf_scale),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.99, input_family=InputFamily.PDF_VECTOR),
        generated_at=_GENERATED_AT,
    )

    pdf_scale_check = {check["check_key"]: check for check in outcome.report_json["checks"]}[
        "pdf_scale_presence_calibration_status"
    ]

    assert pdf_scale_check["status"] == check_status
    assert outcome.validation_status == validation_status


def test_build_validation_outcome_review_gates_placeholder_mode_without_placeholder_semantics() -> (
    None
):
    outcome = build_validation_outcome(
        input_family=InputFamily.DWG,
        canonical_json={
            "entities": (),
            "metadata": {
                "adapter_mode": "placeholder",
                "empty_entities_reason": "placeholder_canonical_no_entity_mapping",
            },
        },
        canonical_entity_schema_version="0.1",
        result=_build_result(
            score=0.99,
            input_family=InputFamily.DWG,
            canonical={
                "entities": (),
                "metadata": {
                    "adapter_mode": "placeholder",
                    "empty_entities_reason": "placeholder_canonical_no_entity_mapping",
                },
            },
        ),
        generated_at=_GENERATED_AT,
    )

    findings = {finding["check_key"]: finding for finding in outcome.report_json["findings"]}

    assert outcome.validation_status == "needs_review"
    assert findings["placeholder_semantics"]["details"] == {
        "status": "placeholder",
        "quantity_gate": "review_gated",
        "reason": "placeholder_canonical_no_entity_mapping",
        "adapter_mode": "placeholder",
        "empty_entities_reason": "placeholder_canonical_no_entity_mapping",
        "derived_from": ["adapter_mode", "empty_entities_reason"],
        "placeholder_semantics": None,
    }
    assert findings["placeholder_semantics_contract_violation"]["details"] == {
        "contract_violation_codes": ["missing_placeholder_semantics"],
        "status": "placeholder",
        "quantity_gate": "review_gated",
        "reason": "placeholder_canonical_no_entity_mapping",
        "adapter_mode": "placeholder",
        "empty_entities_reason": "placeholder_canonical_no_entity_mapping",
        "placeholder_semantics": None,
    }


def test_build_validation_outcome_forces_review_gated_placeholder_semantics_when_inconsistent() -> (
    None
):
    canonical_json: dict[str, JSONValue] = {
        "entities": (),
        "metadata": {
            "adapter_mode": "sparse_placeholder",
            "empty_entities_reason": "raster_vectorization_deferred",
            "placeholder_semantics": {
                "status": "complete",
                "review_required": False,
                "quantity_gate": "allowed",
                "reason": "wrong_reason",
                "coverage": {"entities": "none"},
            },
        },
    }
    outcome = build_validation_outcome(
        input_family=InputFamily.PDF_RASTER,
        canonical_json=canonical_json,
        canonical_entity_schema_version="0.1",
        result=_build_result(
            score=0.99,
            input_family=InputFamily.PDF_RASTER,
            canonical=canonical_json,
        ),
        generated_at=_GENERATED_AT,
    )

    findings = {finding["check_key"]: finding for finding in outcome.report_json["findings"]}

    assert outcome.validation_status == "needs_review"
    assert findings["placeholder_semantics"]["details"] == {
        "status": "sparse",
        "quantity_gate": "review_gated",
        "reason": "wrong_reason",
        "adapter_mode": "sparse_placeholder",
        "empty_entities_reason": "raster_vectorization_deferred",
        "derived_from": ["placeholder_semantics", "adapter_mode", "empty_entities_reason"],
        "placeholder_semantics": {
            "status": "complete",
            "review_required": False,
            "quantity_gate": "allowed",
            "reason": "wrong_reason",
            "coverage": {"entities": "none"},
        },
    }
    assert findings["placeholder_semantics_contract_violation"]["details"] == {
        "contract_violation_codes": [
            "placeholder_status_not_recognized",
            "review_required_not_true",
            "quantity_gate_not_review_gated",
            "reason_mismatch",
        ],
        "status": "sparse",
        "quantity_gate": "review_gated",
        "reason": "wrong_reason",
        "adapter_mode": "sparse_placeholder",
        "empty_entities_reason": "raster_vectorization_deferred",
        "placeholder_semantics": {
            "status": "complete",
            "review_required": False,
            "quantity_gate": "allowed",
            "reason": "wrong_reason",
            "coverage": {"entities": "none"},
        },
    }


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


@pytest.mark.parametrize(
    ("entity", "error_fragment"),
    [
        (
            {
                "kind": "line",
                "layer": "A-WALL",
                "start": {"x": 0.0, "y": 0.0},
                "end": {"x": 1.0, "y": 0.0},
            },
            "must be a mapping",
        ),
        (
            {
                "kind": "line",
                "layer": "A-WALL",
                "start": {"x": 0.0, "y": 0.0},
                "end": {"x": 1.0, "y": 0.0},
                "provenance": {
                    "origin": "adapter_normalized",
                    "adapter": {"key": "dxf"},
                    "source_ref": "entities/1",
                    "source_identity": "entity-1",
                    "extraction_path": ["entities", 0],
                    "notes": [],
                },
            },
            "missing required key",
        ),
        (
            {
                "kind": "line",
                "layer": "A-WALL",
                "start": {"x": 0.0, "y": 0.0},
                "end": {"x": 1.0, "y": 0.0},
                "provenance": {
                    **_valid_entity_provenance(1),
                    "origin": "not_allowed",
                },
            },
            "origin must be one of",
        ),
        (
            {
                "kind": "line",
                "layer": "A-WALL",
                "start": {"x": 0.0, "y": 0.0},
                "end": {"x": 1.0, "y": 0.0},
                "provenance": {
                    **_valid_entity_provenance(1),
                    "source_hash": "not-a-hash",
                },
            },
            "source_hash must be a raw 64-character SHA-256 hex string",
        ),
    ],
)
def test_build_validation_outcome_rejects_entity_provenance_contract_failures(
    entity: Mapping[str, JSONValue],
    error_fragment: str,
) -> None:
    canonical_json: dict[str, JSONValue] = {
        "units": {"normalized": "meter"},
        "coordinate_system": {"name": "local"},
        "layouts": ({"name": "Model"},),
        "layers": ({"name": "A-WALL"},),
        "blocks": (),
        "entities": (entity,),
        "xrefs": (),
    }
    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json=canonical_json,
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.99, canonical=canonical_json),
        generated_at=_GENERATED_AT,
    )

    checks_by_key = {check["check_key"]: check for check in outcome.report_json["checks"]}
    provenance_findings = [
        finding
        for finding in outcome.report_json["findings"]
        if finding["check_key"] == "entity_provenance_contract"
    ]

    assert checks_by_key["entity_provenance_contract"]["status"] == "fail"
    assert outcome.validation_status == "invalid"
    assert provenance_findings
    assert error_fragment in provenance_findings[0]["details"]["invalid_entities"][0]["error"]


def test_build_ingest_finalization_payload_calls_validation_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[InputFamily, str]] = []
    fake_outcome = ValidationOutcome(
        validation_status="valid",
        validator_name="ingestion.runner",
        validator_version="0.1",
        adapter_warnings_json=[],
        report_json={
            "validation_report_schema_version": "0.1",
            "canonical_entity_schema_version": "0.1",
            "validation_status": "valid",
            "validator_name": "ingestion.runner",
            "validator_version": "0.1",
            "summary": {
                "checks_total": 10,
                "findings_total": 0,
                "warnings_total": 0,
                "errors_total": 0,
                "critical_total": 0,
                "check_status_totals": {
                    "pass": 10,
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


def test_build_ingest_finalization_payload_rejects_invalid_entity_provenance() -> None:
    context = IngestFinalizationContext(
        job_id=uuid4(),
        file_id=uuid4(),
        extraction_profile_id=None,
        initial_job_id=None,
        input_family=InputFamily.DXF,
        adapter_key="ezdxf",
        adapter_version="test-1.0",
    )
    canonical_json: dict[str, JSONValue] = {
        "units": {"normalized": "meter"},
        "coordinate_system": {"name": "local"},
        "layouts": ({"name": "Model"},),
        "layers": ({"name": "A-WALL"},),
        "blocks": (),
        "entities": (
            {
                "kind": "line",
                "layer": "A-WALL",
                "start": {"x": 0.0, "y": 0.0},
                "end": {"x": 1.0, "y": 0.0},
                "provenance": {
                    **_valid_entity_provenance(1),
                    "source_hash": "not-a-hash",
                },
            },
        ),
        "xrefs": (),
    }

    payload = build_ingest_finalization_payload(
        context,
        result=_build_result(score=0.99, canonical=canonical_json),
        generated_at=_GENERATED_AT,
    )

    checks_by_key = {check["check_key"]: check for check in payload.report_json["checks"]}
    provenance_findings = [
        finding
        for finding in payload.report_json["findings"]
        if finding["check_key"] == "entity_provenance_contract"
    ]

    assert checks_by_key["entity_provenance_contract"]["status"] == "fail"
    assert payload.validation_status == "invalid"
    assert provenance_findings
    assert (
        "source_hash must be a raw 64-character SHA-256 hex string"
        in (provenance_findings[0]["details"]["invalid_entities"][0]["error"])
    )
    assert payload.provenance_json == {
        "schema_version": "0.1",
        "adapter": {"key": "ezdxf", "version": "test-1.0"},
        "source": {
            "file_id": str(context.file_id),
            "job_id": str(context.job_id),
            "extraction_profile_id": None,
            "input_family": InputFamily.DXF.value,
            "revision_kind": "reprocess",
        },
        "records": [
            {
                "stage": "extract",
                "adapter_key": InputFamily.DXF.value,
                "source_ref": "originals/source.dat",
                "details": None,
            }
        ],
        "generated_at": _GENERATED_AT.isoformat(),
    }


def _block_reference_entity() -> dict[str, JSONValue]:
    return {
        "kind": "insert",
        "layer": "A-WALL",
        "block_name": "Door-Block",
        "insert": {"x": 1.0, "y": 2.0},
        "transform": {"supported": True},
    }


def test_build_validation_outcome_review_gates_block_reference_without_hint() -> None:
    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json=_build_complete_canonical(entities=(_block_reference_entity(),)),
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.99),
        generated_at=_GENERATED_AT,
    )

    checks_by_key = {check["check_key"]: check for check in outcome.report_json["checks"]}
    block_check = checks_by_key["block_transform_validity"]

    assert block_check["status"] == "review_required"
    assert block_check["details"]["block_references_present"] is True
    assert outcome.validation_status == "needs_review"


def test_build_validation_outcome_passes_block_reference_with_confirmed_hint() -> None:
    canonical = _build_complete_canonical(entities=(_block_reference_entity(),))
    canonical["block_transform_validity"] = True

    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json=canonical,
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.95),
        generated_at=_GENERATED_AT,
    )

    checks_by_key = {check["check_key"]: check for check in outcome.report_json["checks"]}

    assert checks_by_key["block_transform_validity"]["status"] == "pass"


def test_build_validation_outcome_fails_block_reference_with_invalid_hint() -> None:
    canonical = _build_complete_canonical(entities=(_block_reference_entity(),))
    canonical["block_transform_validity"] = False

    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json=canonical,
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.99),
        generated_at=_GENERATED_AT,
    )

    checks_by_key = {check["check_key"]: check for check in outcome.report_json["checks"]}

    assert checks_by_key["block_transform_validity"]["status"] == "fail"
    assert outcome.validation_status == "invalid"


def test_build_validation_outcome_includes_extraction_coverage() -> None:
    canonical = _build_complete_canonical(
        entities=(
            {"kind": "line", "layer": "A-WALL", "start": {"x": 0, "y": 0}, "end": {"x": 1, "y": 0}},
            {
                "kind": "unknown",
                "layer": "A-WALL",
                "geometry": {"geometry_summary": {"reason": "unsupported_hatch_record"}},
            },
        ),
    )

    outcome = build_validation_outcome(
        input_family=InputFamily.DXF,
        canonical_json=canonical,
        canonical_entity_schema_version="0.1",
        result=_build_result(score=0.95),
        generated_at=_GENERATED_AT,
    )

    coverage = outcome.report_json["coverage"]
    assert coverage["entities"]["total"] == 2
    assert coverage["entities"]["mapped"] == 1
    assert coverage["entities"]["unmapped"] == 1
    assert coverage["entities"]["mapped_ratio"] == 0.5
    assert coverage["entities"]["by_type"] == {"line": 1, "unknown": 1}
    assert coverage["unmapped_by_reason"] == {"unsupported_hatch_record": 1}
    assert coverage["layers"]["count"] == 1
