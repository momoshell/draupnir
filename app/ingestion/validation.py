"""Validation policy helpers for ingest finalization payloads."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime
from math import isfinite
from typing import Any, Final
from uuid import UUID

from app.ingestion.contracts import AdapterResult, InputFamily

VALIDATION_REPORT_SCHEMA_VERSION: Final[str] = "0.1"
RUNNER_VALIDATOR_NAME: Final[str] = "ingestion.runner"
RUNNER_VALIDATOR_VERSION: Final[str] = "0.1"

_REVIEW_THRESHOLD: Final[float] = 0.60
_APPROVED_THRESHOLD: Final[float] = 0.95
_REVIEW_CAPPED_CONFIDENCE: Final[float] = 0.59
_SUPPORTED_IFC_SCHEMAS: Final[frozenset[str]] = frozenset({"IFC2X3", "IFC4", "IFC4X3"})
_SOURCE_DOCUMENT_REF: Final[str] = "source-document"
_REVIEW_STATUS_VALUES: Final[frozenset[str]] = frozenset(
    {
        "incomplete",
        "manual_required",
        "missing",
        "pending",
        "review_required",
        "unconfirmed",
        "unknown",
        "unresolved",
    }
)
_FAIL_STATUS_VALUES: Final[frozenset[str]] = frozenset(
    {"error", "fail", "failed", "false", "invalid", "rejected", "unsupported"}
)
_PASS_STATUS_VALUES: Final[frozenset[str]] = frozenset(
    {
        "captured",
        "complete",
        "confirmed",
        "normalized",
        "ok",
        "pass",
        "passed",
        "present",
        "resolved",
        "supported",
        "true",
        "valid",
    }
)
_PDF_SCALE_UNCONFIRMED_VALUES: Final[frozenset[str]] = frozenset(
    {"manual_required", "pending", "placeholder", "tbd", "unconfirmed", "unknown"}
)
_PLACEHOLDER_ADAPTER_MODE_VALUES: Final[frozenset[str]] = frozenset(
    {"placeholder", "sparse_placeholder"}
)
_PLACEHOLDER_STATUS_VALUES: Final[frozenset[str]] = frozenset(
    {"placeholder", "scaffold", "sparse", "synthetic"}
)
_PLACEHOLDER_EMPTY_ENTITY_REASONS: Final[frozenset[str]] = frozenset(
    {"placeholder_canonical_no_entity_mapping", "raster_vectorization_deferred"}
)
_CENTER_RADIUS_ENTITY_KINDS: Final[frozenset[str]] = frozenset({"arc", "circle"})
_LINE_ENTITY_KINDS: Final[frozenset[str]] = frozenset({"line"})
_POINT_ENTITY_KINDS: Final[frozenset[str]] = frozenset({"point"})
_POLYGON_ENTITY_KINDS: Final[frozenset[str]] = frozenset(
    {"hatch", "lwpolyline", "polygon", "polyline", "solid"}
)
_BLOCK_REFERENCE_ENTITY_KINDS: Final[frozenset[str]] = frozenset({"block_reference", "insert"})
_REQUIRED_CHECK_KEYS: Final[tuple[str, ...]] = (
    "units_presence_normalization",
    "coordinate_system_capture",
    "geometry_validity",
    "closed_polygon_eligibility_for_area_quantities",
    "block_transform_validity",
    "layer_mapping_completeness",
    "xref_resolution_status",
    "pdf_scale_presence_calibration_status",
    "ifc_schema_support",
)


@dataclass(frozen=True, slots=True)
class ValidationOutcome:
    """Typed validation policy result for ingest finalization."""

    confidence_score: float
    effective_confidence: float
    validation_status: str
    review_state: str
    quantity_gate: str
    validator_name: str
    validator_version: str
    confidence_json: dict[str, Any]
    adapter_warnings_json: list[Any]
    report_json: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _PlaceholderReviewPolicy:
    requires_review: bool
    status: str
    quantity_gate: str
    reason: str | None
    placeholder_semantics: Mapping[str, Any] | None
    adapter_mode: str | None
    empty_entities_reason: str | None
    derived_from: tuple[str, ...]
    contract_violation_codes: tuple[str, ...]


def build_validation_outcome(
    *,
    input_family: InputFamily,
    canonical_json: Mapping[str, Any],
    canonical_entity_schema_version: str,
    result: AdapterResult,
    generated_at: datetime,
) -> ValidationOutcome:
    """Build the canonical v0.1 validation report and review policy outcome."""

    confidence_score = _confidence_score(result)
    effective_confidence = confidence_score
    adapter_review_required = bool(
        result.confidence.review_required if result.confidence is not None else True
    )
    confidence_basis = result.confidence.basis if result.confidence is not None else None
    adapter_warnings_json = [_json_compatible(warning) for warning in result.warnings]
    findings: list[dict[str, Any]] = []

    def add_finding(
        *,
        check_key: str,
        severity: str,
        message: str,
        target_type: str,
        target_ref: str,
        quantity_effect: str,
        source: str,
        details: Mapping[str, Any] | None = None,
    ) -> str:
        finding_id = f"finding-{len(findings) + 1:03d}"
        finding: dict[str, Any] = {
            "finding_id": finding_id,
            "check_key": check_key,
            "severity": severity,
            "message": message,
            "target_type": target_type,
            "target_ref": target_ref,
            "quantity_effect": quantity_effect,
            "source": source,
        }
        if details is not None:
            finding["details"] = _json_compatible(dict(details))
        findings.append(finding)
        return finding_id

    if input_family == InputFamily.PDF_RASTER:
        effective_confidence = min(effective_confidence, _REVIEW_CAPPED_CONFIDENCE)
        add_finding(
            check_key="raster_review_policy",
            severity="warning",
            message="Raster inputs remain review-first regardless of confidence score.",
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={"input_family": input_family.value},
        )

    if confidence_score < _REVIEW_THRESHOLD:
        effective_confidence = min(effective_confidence, confidence_score)
        add_finding(
            check_key="confidence_threshold",
            severity="warning",
            message="Confidence is below the review threshold.",
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={
                "reported_confidence": confidence_score,
                "review_threshold": _REVIEW_THRESHOLD,
            },
        )

    if adapter_review_required:
        effective_confidence = min(effective_confidence, _REVIEW_CAPPED_CONFIDENCE)
        add_finding(
            check_key="adapter_review_required",
            severity="warning",
            message="Adapter marked the result as requiring review.",
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={"reported_confidence": confidence_score},
        )

    placeholder_requires_review = _apply_placeholder_review_policy(
        canonical_json=canonical_json,
        add_finding=add_finding,
    )
    if placeholder_requires_review:
        effective_confidence = min(effective_confidence, _REVIEW_CAPPED_CONFIDENCE)

    checks, required_checks_require_review, required_checks_invalid = _build_required_checks(
        canonical_json=canonical_json,
        add_finding=add_finding,
    )
    if required_checks_require_review:
        effective_confidence = min(effective_confidence, _REVIEW_CAPPED_CONFIDENCE)
    if required_checks_invalid:
        effective_confidence = 0.0

    pdf_scale_check, pdf_scale_requires_review = _build_pdf_scale_check(
        input_family=input_family,
        canonical_json=canonical_json,
        add_finding=add_finding,
    )
    if pdf_scale_requires_review:
        effective_confidence = min(effective_confidence, _REVIEW_CAPPED_CONFIDENCE)
    if pdf_scale_check["status"] == "fail":
        effective_confidence = 0.0
    checks.append(pdf_scale_check)

    ifc_schema_check, ifc_schema_invalid = _build_ifc_schema_check(
        input_family=input_family,
        canonical_json=canonical_json,
        add_finding=add_finding,
    )
    if ifc_schema_invalid:
        effective_confidence = 0.0
    checks.append(ifc_schema_check)

    for index, warning in enumerate(adapter_warnings_json, start=1):
        normalized_warning = warning if isinstance(warning, Mapping) else {"value": warning}
        add_finding(
            check_key="adapter_warning",
            severity="warning",
            message=_adapter_warning_message(normalized_warning, index=index),
            target_type="adapter_warning",
            target_ref=_adapter_warning_target_ref(normalized_warning, index=index),
            quantity_effect="warning_only",
            source="adapter_warning",
            details={"warning": normalized_warning},
        )

    validation_status = _derive_validation_status(
        checks=checks,
        findings=findings,
        review_required=(
            input_family == InputFamily.PDF_RASTER
            or confidence_score < _REVIEW_THRESHOLD
            or adapter_review_required
            or placeholder_requires_review
            or required_checks_require_review
            or pdf_scale_requires_review
        ),
    )
    review_state = _derive_review_state(
        validation_status=validation_status,
        effective_confidence=effective_confidence,
        review_required=(
            input_family == InputFamily.PDF_RASTER
            or confidence_score < _REVIEW_THRESHOLD
            or adapter_review_required
            or placeholder_requires_review
            or required_checks_require_review
            or pdf_scale_requires_review
        ),
    )
    quantity_gate = _derive_quantity_gate(
        validation_status=validation_status,
        review_state=review_state,
    )
    confidence_json = {
        "score": result.confidence.score if result.confidence is not None else None,
        "effective_confidence": effective_confidence,
        "review_state": review_state,
        "review_required": review_state == "review_required",
        "basis": confidence_basis,
    }
    report_json = {
        "validation_report_schema_version": VALIDATION_REPORT_SCHEMA_VERSION,
        "canonical_entity_schema_version": canonical_entity_schema_version,
        "validation_status": validation_status,
        "review_state": review_state,
        "quantity_gate": quantity_gate,
        "effective_confidence": effective_confidence,
        "validator_name": RUNNER_VALIDATOR_NAME,
        "validator_version": RUNNER_VALIDATOR_VERSION,
        "confidence": confidence_json,
        "provenance": _json_compatible(result.provenance),
        "summary": _build_summary(canonical_json=canonical_json, checks=checks, findings=findings),
        "checks": checks,
        "findings": findings,
        "adapter_warnings": adapter_warnings_json,
        "generated_at": generated_at.isoformat(),
    }

    return ValidationOutcome(
        confidence_score=confidence_score,
        effective_confidence=effective_confidence,
        validation_status=validation_status,
        review_state=review_state,
        quantity_gate=quantity_gate,
        validator_name=RUNNER_VALIDATOR_NAME,
        validator_version=RUNNER_VALIDATOR_VERSION,
        confidence_json=confidence_json,
        adapter_warnings_json=adapter_warnings_json,
        report_json=report_json,
    )


def _pass_check(check_key: str, summary_message: str) -> dict[str, Any]:
    return _check(
        check_key=check_key,
        status="pass",
        summary_message=summary_message,
        details={"applicable": True},
    )


def _check(
    *,
    check_key: str,
    status: str,
    summary_message: str,
    finding_refs: list[str] | None = None,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "check_key": check_key,
        "status": status,
        "summary_message": summary_message,
        "finding_refs": [] if finding_refs is None else finding_refs,
        "details": {"applicable": True} if details is None else _json_compatible(dict(details)),
    }


def _not_applicable_check(check_key: str, summary_message: str) -> dict[str, Any]:
    return _check(
        check_key=check_key,
        status="pass",
        summary_message=summary_message,
        details={"applicable": False},
    )


def _build_required_checks(
    *,
    canonical_json: Mapping[str, Any],
    add_finding: Any,
) -> tuple[list[dict[str, Any]], bool, bool]:
    checks: list[dict[str, Any]] = []
    requires_review = False
    invalid = False

    for builder in (
        _build_units_check,
        _build_coordinate_system_check,
        _build_geometry_validity_check,
        _build_closed_polygon_check,
        _build_block_transform_check,
        _build_layer_mapping_check,
        _build_xref_check,
    ):
        check, check_requires_review, check_invalid = builder(
            canonical_json=canonical_json,
            add_finding=add_finding,
        )
        checks.append(check)
        requires_review = requires_review or check_requires_review
        invalid = invalid or check_invalid

    return checks, requires_review, invalid


def _apply_placeholder_review_policy(
    *, canonical_json: Mapping[str, Any], add_finding: Any
) -> bool:
    placeholder_policy = _derive_placeholder_review_policy(canonical_json)
    if placeholder_policy is None or not placeholder_policy.requires_review:
        return False

    add_finding(
        check_key="placeholder_semantics",
        severity="warning",
        message=(
            "Adapter reported or implied placeholder/scaffold extraction semantics; "
            "quantities remain review-gated until non-placeholder coverage is available."
        ),
        target_type="revision",
        target_ref=_SOURCE_DOCUMENT_REF,
        quantity_effect="blocks_quantity",
        source="validator",
        details={
            "status": placeholder_policy.status,
            "quantity_gate": placeholder_policy.quantity_gate,
            "reason": placeholder_policy.reason,
            "adapter_mode": placeholder_policy.adapter_mode,
            "empty_entities_reason": placeholder_policy.empty_entities_reason,
            "derived_from": list(placeholder_policy.derived_from),
            "placeholder_semantics": (
                None
                if placeholder_policy.placeholder_semantics is None
                else _json_compatible(dict(placeholder_policy.placeholder_semantics))
            ),
        },
    )

    if placeholder_policy.contract_violation_codes:
        add_finding(
            check_key="placeholder_semantics_contract_violation",
            severity="warning",
            message=(
                "Placeholder/scaffold output metadata was missing or inconsistent; "
                "validator forced review-gated quantities from adapter metadata."
            ),
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={
                "contract_violation_codes": list(placeholder_policy.contract_violation_codes),
                "status": placeholder_policy.status,
                "quantity_gate": placeholder_policy.quantity_gate,
                "reason": placeholder_policy.reason,
                "adapter_mode": placeholder_policy.adapter_mode,
                "empty_entities_reason": placeholder_policy.empty_entities_reason,
                "placeholder_semantics": (
                    None
                    if placeholder_policy.placeholder_semantics is None
                    else _json_compatible(dict(placeholder_policy.placeholder_semantics))
                ),
            },
        )

    return True


def _derive_placeholder_review_policy(
    canonical_json: Mapping[str, Any],
) -> _PlaceholderReviewPolicy | None:
    placeholder_semantics = _extract_placeholder_semantics(canonical_json)
    adapter_mode = _normalized_string(_metadata_candidate(canonical_json, "adapter_mode"))
    empty_entities_reason = _normalized_string(
        _metadata_candidate(canonical_json, "empty_entities_reason")
    )

    derived_from: list[str] = []
    if placeholder_semantics is not None:
        derived_from.append("placeholder_semantics")
    if adapter_mode in _PLACEHOLDER_ADAPTER_MODE_VALUES:
        derived_from.append("adapter_mode")
    if empty_entities_reason in _PLACEHOLDER_EMPTY_ENTITY_REASONS:
        derived_from.append("empty_entities_reason")

    if not derived_from:
        return None

    status = _placeholder_status(
        placeholder_semantics=placeholder_semantics,
        adapter_mode=adapter_mode,
    )
    reason = _placeholder_reason(
        placeholder_semantics=placeholder_semantics,
        empty_entities_reason=empty_entities_reason,
    )

    contract_violation_codes: list[str] = []
    if placeholder_semantics is None:
        contract_violation_codes.append("missing_placeholder_semantics")
    else:
        if _normalized_string(placeholder_semantics.get("status")) not in (
            _PLACEHOLDER_STATUS_VALUES
        ):
            contract_violation_codes.append("placeholder_status_not_recognized")
        if placeholder_semantics.get("review_required") is not True:
            contract_violation_codes.append("review_required_not_true")
        if _normalized_string(placeholder_semantics.get("quantity_gate")) != "review_gated":
            contract_violation_codes.append("quantity_gate_not_review_gated")
        if (
            empty_entities_reason is not None
            and _normalized_string(placeholder_semantics.get("reason")) != empty_entities_reason
        ):
            contract_violation_codes.append("reason_mismatch")

    return _PlaceholderReviewPolicy(
        requires_review=True,
        status=status,
        quantity_gate="review_gated",
        reason=reason,
        placeholder_semantics=placeholder_semantics,
        adapter_mode=adapter_mode,
        empty_entities_reason=empty_entities_reason,
        derived_from=tuple(derived_from),
        contract_violation_codes=tuple(contract_violation_codes),
    )


def _placeholder_status(
    *, placeholder_semantics: Mapping[str, Any] | None, adapter_mode: str | None
) -> str:
    if placeholder_semantics is not None:
        status = _normalized_string(placeholder_semantics.get("status"))
        if status in _PLACEHOLDER_STATUS_VALUES:
            return status

    if adapter_mode == "sparse_placeholder":
        return "sparse"
    return "placeholder"


def _placeholder_reason(
    *,
    placeholder_semantics: Mapping[str, Any] | None,
    empty_entities_reason: str | None,
) -> str | None:
    if placeholder_semantics is not None:
        reason = _normalized_string(placeholder_semantics.get("reason"))
        if reason is not None:
            return reason
    return empty_entities_reason


def _build_units_check(
    *, canonical_json: Mapping[str, Any], add_finding: Any
) -> tuple[dict[str, Any], bool, bool]:
    check_key = "units_presence_normalization"
    units = _extract_meaningful_metadata(
        canonical_json,
        "units",
        "normalized_units",
        "unit",
        "unit_system",
        "measurement_unit",
    )
    if units is not None:
        return (
            _check(
                check_key=check_key,
                status="pass",
                summary_message="Units metadata is present and normalized.",
                details={"applicable": True, "units": units},
            ),
            False,
            False,
        )

    finding_ref = add_finding(
        check_key=check_key,
        severity="warning",
        message=(
            "Units metadata is missing or unnormalized and requires review before "
            "quantities run."
        ),
        target_type="revision",
        target_ref=_SOURCE_DOCUMENT_REF,
        quantity_effect="blocks_quantity",
        source="validator",
        details={"units_present": False},
    )
    return (
        _check(
            check_key=check_key,
            status="review_required",
            summary_message="Units metadata is missing or unnormalized.",
            finding_refs=[finding_ref],
            details={"applicable": True, "units_present": False},
        ),
        True,
        False,
    )


def _build_coordinate_system_check(
    *, canonical_json: Mapping[str, Any], add_finding: Any
) -> tuple[dict[str, Any], bool, bool]:
    check_key = "coordinate_system_capture"
    coordinate_system = _extract_meaningful_metadata(
        canonical_json,
        "coordinate_system",
        "coordinate_reference_system",
        "crs",
        "spatial_reference",
    )
    if coordinate_system is not None:
        return (
            _check(
                check_key=check_key,
                status="pass",
                summary_message="Coordinate system metadata is present.",
                details={"applicable": True, "coordinate_system": coordinate_system},
            ),
            False,
            False,
        )

    finding_ref = add_finding(
        check_key=check_key,
        severity="warning",
        message="Coordinate system metadata is missing and requires review before quantities run.",
        target_type="revision",
        target_ref=_SOURCE_DOCUMENT_REF,
        quantity_effect="blocks_quantity",
        source="validator",
        details={"coordinate_system_present": False},
    )
    return (
        _check(
            check_key=check_key,
            status="review_required",
            summary_message="Coordinate system metadata is missing.",
            finding_refs=[finding_ref],
            details={"applicable": True, "coordinate_system_present": False},
        ),
        True,
        False,
    )


def _build_geometry_validity_check(
    *, canonical_json: Mapping[str, Any], add_finding: Any
) -> tuple[dict[str, Any], bool, bool]:
    check_key = "geometry_validity"
    entities = _entity_mappings(canonical_json)
    geometry_hint = _normalize_status_hint(
        _metadata_candidate(canonical_json, "geometry_validity"),
        _metadata_candidate(canonical_json, "geometry_validation"),
        _metadata_candidate(canonical_json, "geometry_status"),
        _metadata_candidate(canonical_json, "geometry_valid"),
    )
    if geometry_hint is False:
        finding_ref = add_finding(
            check_key=check_key,
            severity="error",
            message="Geometry validity check reported invalid geometry.",
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={"entity_count": len(entities)},
        )
        return (
            _check(
                check_key=check_key,
                status="fail",
                summary_message="Geometry validity check failed.",
                finding_refs=[finding_ref],
                details={
                    "applicable": True,
                    "entity_count": len(entities),
                    "geometry_valid": False,
                },
            ),
            False,
            True,
        )

    geometry_states = [_entity_has_valid_geometry(entity) for entity in entities]
    validated_entity_count = sum(state is True for state in geometry_states)

    if geometry_hint is True or (
        geometry_states and validated_entity_count == len(geometry_states)
    ):
        return (
            _check(
                check_key=check_key,
                status="pass",
                summary_message="Geometry validity reported no blocking issues.",
                details={
                    "applicable": True,
                    "entity_count": len(entities),
                    "validated_entity_count": validated_entity_count,
                    "geometry_valid": True,
                },
            ),
            False,
            False,
        )

    finding_ref = add_finding(
        check_key=check_key,
        severity="warning",
        message=(
            "Geometry validity could not be confirmed and requires review before "
            "quantities run."
        ),
        target_type="revision",
        target_ref=_SOURCE_DOCUMENT_REF,
        quantity_effect="blocks_quantity",
        source="validator",
        details={
            "entity_count": len(entities),
            "validated_entity_count": validated_entity_count,
        },
    )
    return (
        _check(
            check_key=check_key,
            status="review_required",
            summary_message="Geometry validity could not be confirmed.",
            finding_refs=[finding_ref],
            details={
                "applicable": True,
                "entity_count": len(entities),
                "validated_entity_count": validated_entity_count,
                "geometry_valid": None,
            },
        ),
        True,
        False,
    )


def _build_closed_polygon_check(
    *, canonical_json: Mapping[str, Any], add_finding: Any
) -> tuple[dict[str, Any], bool, bool]:
    check_key = "closed_polygon_eligibility_for_area_quantities"
    polygon_entities = _polygon_entities(canonical_json)
    polygon_hint = _normalize_status_hint(
        _metadata_candidate(canonical_json, "closed_polygon_eligibility_for_area_quantities"),
        _metadata_candidate(canonical_json, "polygon_closure"),
        _metadata_candidate(canonical_json, "area_quantity_eligibility"),
    )
    if not polygon_entities and polygon_hint is None:
        return (
            _not_applicable_check(
                check_key,
                "Closed polygon eligibility is not applicable because no area-bearing "
                "polygons were reported.",
            ),
            False,
            False,
        )

    if polygon_hint is False or any(
        _polygon_closed_state(entity) is False for entity in polygon_entities
    ):
        finding_ref = add_finding(
            check_key=check_key,
            severity="error",
            message=(
                "Area-bearing polygons are not closed and cannot drive deterministic "
                "quantities."
            ),
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={"polygon_count": len(polygon_entities)},
        )
        return (
            _check(
                check_key=check_key,
                status="fail",
                summary_message="Closed polygon eligibility failed.",
                finding_refs=[finding_ref],
                details={
                    "applicable": True,
                    "polygon_count": len(polygon_entities),
                    "eligible": False,
                },
            ),
            False,
            True,
        )

    polygon_states = [_polygon_closed_state(entity) for entity in polygon_entities]
    if polygon_hint is True or (
        polygon_entities and all(state is True for state in polygon_states)
    ):
        return (
            _check(
                check_key=check_key,
                status="pass",
                summary_message="Closed polygon eligibility reported no blocking issues.",
                details={
                    "applicable": True,
                    "polygon_count": len(polygon_entities),
                    "eligible": True,
                },
            ),
            False,
            False,
        )

    finding_ref = add_finding(
        check_key=check_key,
        severity="warning",
        message=(
            "Polygon closure eligibility is incomplete and requires review before area "
            "quantities run."
        ),
        target_type="revision",
        target_ref=_SOURCE_DOCUMENT_REF,
        quantity_effect="blocks_quantity",
        source="validator",
        details={"polygon_count": len(polygon_entities)},
    )
    return (
        _check(
            check_key=check_key,
            status="review_required",
            summary_message="Polygon closure eligibility could not be confirmed.",
            finding_refs=[finding_ref],
            details={"applicable": True, "polygon_count": len(polygon_entities), "eligible": None},
        ),
        True,
        False,
    )


def _build_block_transform_check(
    *, canonical_json: Mapping[str, Any], add_finding: Any
) -> tuple[dict[str, Any], bool, bool]:
    check_key = "block_transform_validity"
    has_block_transforms = _has_block_transform_content(canonical_json)
    transform_hint = _normalize_status_hint(
        _metadata_candidate(canonical_json, "block_transform_validity"),
        _metadata_candidate(canonical_json, "transform_validity"),
        _metadata_candidate(canonical_json, "transforms_valid"),
    )
    if not has_block_transforms and transform_hint is None:
        return (
            _not_applicable_check(
                check_key,
                "Block transform validity is not applicable because no block references "
                "were reported.",
            ),
            False,
            False,
        )

    if transform_hint is False:
        finding_ref = add_finding(
            check_key=check_key,
            severity="error",
            message="Block transform validation reported invalid transforms.",
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={"block_references_present": has_block_transforms},
        )
        return (
            _check(
                check_key=check_key,
                status="fail",
                summary_message="Block transform validity failed.",
                finding_refs=[finding_ref],
                details={
                    "applicable": True,
                    "block_references_present": has_block_transforms,
                    "transforms_valid": False,
                },
            ),
            False,
            True,
        )

    if transform_hint is True:
        return (
            _check(
                check_key=check_key,
                status="pass",
                summary_message="Block transform validity reported no blocking issues.",
                details={
                    "applicable": True,
                    "block_references_present": has_block_transforms,
                    "transforms_valid": True,
                },
            ),
            False,
            False,
        )

    finding_ref = add_finding(
        check_key=check_key,
        severity="warning",
        message=(
            "Block transform validity could not be confirmed and requires review before "
            "quantities run."
        ),
        target_type="revision",
        target_ref=_SOURCE_DOCUMENT_REF,
        quantity_effect="blocks_quantity",
        source="validator",
        details={"block_references_present": has_block_transforms},
    )
    return (
        _check(
            check_key=check_key,
            status="review_required",
            summary_message="Block transform validity could not be confirmed.",
            finding_refs=[finding_ref],
            details={
                "applicable": True,
                "block_references_present": has_block_transforms,
                "transforms_valid": None,
            },
        ),
        True,
        False,
    )


def _build_layer_mapping_check(
    *, canonical_json: Mapping[str, Any], add_finding: Any
) -> tuple[dict[str, Any], bool, bool]:
    check_key = "layer_mapping_completeness"
    entities = _entity_mappings(canonical_json)
    layer_mapping_hint = _normalize_status_hint(
        _metadata_candidate(canonical_json, "layer_mapping_completeness"),
        _metadata_candidate(canonical_json, "layer_mapping"),
        _metadata_candidate(canonical_json, "layer_map"),
    )
    used_layers = {
        str(layer).strip()
        for entity in entities
        for layer in [entity.get("layer")]
        if isinstance(layer, str) and layer.strip()
    }
    declared_layers = {
        str(name).strip()
        for layer in _sequence_mappings(canonical_json.get("layers"))
        for name in [layer.get("name")]
        if isinstance(name, str) and name.strip()
    }
    missing_layers = (
        sorted(used_layers - declared_layers) if declared_layers else sorted(used_layers)
    )

    if layer_mapping_hint is True:
        return (
            _check(
                check_key=check_key,
                status="pass",
                summary_message="Layer mapping completeness reported no blocking issues.",
                details={
                    "applicable": True,
                    "used_layers": sorted(used_layers),
                    "declared_layers": sorted(declared_layers),
                },
            ),
            False,
            False,
        )

    if layer_mapping_hint is False:
        finding_ref = add_finding(
            check_key=check_key,
            severity="warning",
            message=(
                "Layer mapping completeness reported incomplete mappings and requires "
                "review before quantities run."
            ),
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={
                "used_layers": sorted(used_layers),
                "declared_layers": sorted(declared_layers),
            },
        )
        return (
            _check(
                check_key=check_key,
                status="review_required",
                summary_message="Layer mapping completeness reported incomplete mappings.",
                finding_refs=[finding_ref],
                details={
                    "applicable": True,
                    "used_layers": sorted(used_layers),
                    "declared_layers": sorted(declared_layers),
                },
            ),
            True,
            False,
        )

    if entities and declared_layers and used_layers and not missing_layers:
        return (
            _check(
                check_key=check_key,
                status="pass",
                summary_message="Layer mapping completeness reported no blocking issues.",
                details={
                    "applicable": True,
                    "used_layers": sorted(used_layers),
                    "declared_layers": sorted(declared_layers),
                },
            ),
            False,
            False,
        )

    finding_ref = add_finding(
        check_key=check_key,
        severity="warning",
        message=(
            "Layer mapping completeness is missing or incomplete and requires review "
            "before quantities run."
        ),
        target_type="revision",
        target_ref=_SOURCE_DOCUMENT_REF,
        quantity_effect="blocks_quantity",
        source="validator",
        details={
            "used_layers": sorted(used_layers),
            "declared_layers": sorted(declared_layers),
            "missing_layers": missing_layers,
        },
    )
    return (
        _check(
            check_key=check_key,
            status="review_required",
            summary_message="Layer mapping completeness is missing or incomplete.",
            finding_refs=[finding_ref],
            details={
                "applicable": True,
                "used_layers": sorted(used_layers),
                "declared_layers": sorted(declared_layers),
                "missing_layers": missing_layers,
            },
        ),
        True,
        False,
    )


def _build_xref_check(
    *, canonical_json: Mapping[str, Any], add_finding: Any
) -> tuple[dict[str, Any], bool, bool]:
    check_key = "xref_resolution_status"
    xref_value = _metadata_candidate(
        canonical_json,
        "xref_resolution_status",
        "xrefs",
        "external_references",
    )
    xref_hint = _normalize_status_hint(xref_value)
    xrefs = _sequence_mappings(xref_value)
    if xref_hint is True:
        return (
            _check(
                check_key=check_key,
                status="pass",
                summary_message="Xref resolution reported no blocking issues.",
                details={"applicable": True, "xref_count": len(xrefs)},
            ),
            False,
            False,
        )

    if isinstance(xref_value, (list, tuple)) and not xrefs:
        return (
            _not_applicable_check(
                check_key,
                "Xref resolution is not applicable because no xrefs were reported.",
            ),
            False,
            False,
        )

    if xrefs:
        unresolved_refs = [
            _xref_ref(xref, index=index)
            for index, xref in enumerate(xrefs, start=1)
            if _normalize_status_hint(
                xref.get("resolved"),
                xref.get("status"),
                xref.get("resolution_status"),
            )
            is not True
        ]
        if not unresolved_refs:
            return (
                _check(
                    check_key=check_key,
                    status="pass",
                    summary_message="Xref resolution reported no blocking issues.",
                    details={"applicable": True, "xref_count": len(xrefs), "unresolved_refs": []},
                ),
                False,
                False,
            )

        finding_ref = add_finding(
            check_key=check_key,
            severity="warning",
            message="One or more xrefs are unresolved and require review before quantities run.",
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={"xref_count": len(xrefs), "unresolved_refs": unresolved_refs},
        )
        return (
            _check(
                check_key=check_key,
                status="review_required",
                summary_message="Xref resolution is incomplete.",
                finding_refs=[finding_ref],
                details={
                    "applicable": True,
                    "xref_count": len(xrefs),
                    "unresolved_refs": unresolved_refs,
                },
            ),
            True,
            False,
        )

    finding_ref = add_finding(
        check_key=check_key,
        severity="warning",
        message="Xref resolution status is missing and requires review before quantities run.",
        target_type="revision",
        target_ref=_SOURCE_DOCUMENT_REF,
        quantity_effect="blocks_quantity",
        source="validator",
        details={"xref_present": False},
    )
    return (
        _check(
            check_key=check_key,
            status="review_required",
            summary_message="Xref resolution status is missing.",
            finding_refs=[finding_ref],
            details={"applicable": True, "xref_present": False},
        ),
        True,
        False,
    )


def _build_pdf_scale_check(
    *,
    input_family: InputFamily,
    canonical_json: Mapping[str, Any],
    add_finding: Any,
) -> tuple[dict[str, Any], bool]:
    check_key = "pdf_scale_presence_calibration_status"
    if input_family not in {InputFamily.PDF_VECTOR, InputFamily.PDF_RASTER}:
        return (
            _not_applicable_check(
                check_key,
                "PDF scale calibration is not applicable for this input family.",
            ),
            False,
        )

    scale_status, scale_value = _extract_pdf_scale(canonical_json)
    if scale_status == "present" and scale_value is not None:
        return (
            _check(
                check_key=check_key,
                status="pass",
                summary_message="PDF scale metadata is present.",
                details={"applicable": True, "scale_present": True, "scale": scale_value},
            ),
            False,
        )

    if scale_status == "invalid":
        finding_ref = add_finding(
            check_key=check_key,
            severity="error",
            message="PDF scale metadata is invalid and blocks deterministic quantities.",
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={"input_family": input_family.value, "scale_present": True},
        )
        return (
            _check(
                check_key=check_key,
                status="fail",
                summary_message="PDF scale metadata is invalid.",
                finding_refs=[finding_ref],
                details={
                    "applicable": True,
                    "scale_present": True,
                    "calibration_status": "invalid",
                },
            ),
            False,
        )

    if scale_status == "unconfirmed":
        finding_ref = add_finding(
            check_key=check_key,
            severity="warning",
            message="PDF scale metadata is unconfirmed and requires review before quantities run.",
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={"input_family": input_family.value, "scale_present": True},
        )
        return (
            _check(
                check_key=check_key,
                status="review_required",
                summary_message="PDF scale metadata is unconfirmed.",
                finding_refs=[finding_ref],
                details={
                    "applicable": True,
                    "scale_present": True,
                    "calibration_status": "unconfirmed",
                },
            ),
            True,
        )

    finding_ref = add_finding(
        check_key=check_key,
        severity="warning",
        message="PDF scale metadata is missing and requires review before quantities run.",
        target_type="revision",
        target_ref=_SOURCE_DOCUMENT_REF,
        quantity_effect="blocks_quantity",
        source="validator",
        details={"input_family": input_family.value, "scale_present": False},
    )
    return (
        _check(
            check_key=check_key,
            status="review_required",
            summary_message="PDF scale metadata is missing.",
            finding_refs=[finding_ref],
            details={"applicable": True, "scale_present": False, "calibration_status": "missing"},
        ),
        True,
    )


def _build_ifc_schema_check(
    *,
    input_family: InputFamily,
    canonical_json: Mapping[str, Any],
    add_finding: Any,
) -> tuple[dict[str, Any], bool]:
    check_key = "ifc_schema_support"
    if input_family != InputFamily.IFC:
        return (
            _not_applicable_check(
                check_key,
                "IFC schema support is not applicable for this input family.",
            ),
            False,
        )

    schema = _extract_ifc_schema(canonical_json)
    if schema is None:
        finding_ref = add_finding(
            check_key=check_key,
            severity="error",
            message="IFC schema metadata is missing.",
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={"schema_present": False, "supported_schemas": sorted(_SUPPORTED_IFC_SCHEMAS)},
        )
        return (
            {
                "check_key": check_key,
                "status": "fail",
                "summary_message": "IFC schema metadata is missing.",
                "finding_refs": [finding_ref],
                "details": {
                    "applicable": True,
                    "schema_present": False,
                    "supported": False,
                },
            },
            True,
        )

    normalized_schema = schema.upper()
    if normalized_schema not in _SUPPORTED_IFC_SCHEMAS:
        finding_ref = add_finding(
            check_key=check_key,
            severity="error",
            message="IFC schema is not supported for deterministic quantity generation.",
            target_type="revision",
            target_ref=_SOURCE_DOCUMENT_REF,
            quantity_effect="blocks_quantity",
            source="validator",
            details={
                "schema_present": True,
                "schema": schema,
                "supported_schemas": sorted(_SUPPORTED_IFC_SCHEMAS),
            },
        )
        return (
            {
                "check_key": check_key,
                "status": "fail",
                "summary_message": "IFC schema is not supported.",
                "finding_refs": [finding_ref],
                "details": {
                    "applicable": True,
                    "schema_present": True,
                    "schema": schema,
                    "supported": False,
                },
            },
            True,
        )

    return (
        {
            "check_key": check_key,
            "status": "pass",
            "summary_message": "IFC schema is supported.",
            "finding_refs": [],
            "details": {
                "applicable": True,
                "schema_present": True,
                "schema": schema,
                "supported": True,
            },
        },
        False,
    )


def _extract_pdf_scale(canonical_json: Mapping[str, Any]) -> tuple[str, Any | None]:
    candidates: tuple[Any | None, ...] = (
        canonical_json.get("pdf_scale"),
        canonical_json.get("document_scale"),
        canonical_json.get("scale"),
        _mapping_value(canonical_json.get("metadata"), "pdf_scale"),
        _mapping_value(canonical_json.get("metadata"), "document_scale"),
        _mapping_value(canonical_json.get("metadata"), "scale"),
    )
    for candidate in candidates:
        if candidate is None:
            continue
        explicit_status = _explicit_pdf_scale_status(candidate)
        if explicit_status is not None:
            return explicit_status, None
        if _is_valid_pdf_scale(candidate):
            return "present", _json_compatible(candidate)
        if _is_unconfirmed_pdf_scale(candidate):
            return "unconfirmed", None
        return "invalid", None

    return "missing", None


def _extract_ifc_schema(canonical_json: Mapping[str, Any]) -> str | None:
    candidates = (
        canonical_json.get("ifc_schema"),
        canonical_json.get("ifc_schema_version"),
        _mapping_value(canonical_json.get("metadata"), "ifc_schema"),
        _mapping_value(canonical_json.get("metadata"), "ifc_schema_version"),
    )
    for candidate in candidates:
        if candidate is not None:
            return str(candidate)

    return None


def _mapping_value(value: Any, key: str) -> Any | None:
    if isinstance(value, Mapping):
        return value.get(key)

    return None


def _normalized_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None

    normalized = value.strip().lower()
    return normalized or None


def _extract_placeholder_semantics(canonical_json: Mapping[str, Any]) -> Mapping[str, Any] | None:
    candidates: tuple[Any | None, ...] = (
        canonical_json.get("placeholder_semantics"),
        _mapping_value(canonical_json.get("metadata"), "placeholder_semantics"),
    )
    for candidate in candidates:
        if isinstance(candidate, Mapping):
            return candidate

    return None


def _placeholder_semantics_requires_review(placeholder_semantics: Mapping[str, Any]) -> bool:
    if placeholder_semantics.get("review_required") is True:
        return True

    if _normalized_string(placeholder_semantics.get("quantity_gate")) == "review_gated":
        return True

    return _normalized_string(placeholder_semantics.get("status")) in _PLACEHOLDER_STATUS_VALUES


def _metadata_candidate(canonical_json: Mapping[str, Any], *keys: str) -> Any | None:
    metadata = canonical_json.get("metadata")
    for key in keys:
        value = canonical_json.get(key)
        if value is not None:
            return value
        mapped_value = _mapping_value(metadata, key)
        if mapped_value is not None:
            return mapped_value

    return None


def _extract_meaningful_metadata(canonical_json: Mapping[str, Any], *keys: str) -> Any | None:
    for key in keys:
        candidate = _metadata_candidate(canonical_json, key)
        if _is_meaningful_value(candidate):
            return _json_compatible(candidate)

    return None


def _normalize_status_hint(*candidates: Any) -> bool | None:
    for candidate in candidates:
        if candidate is None:
            continue
        normalized = _normalize_status_value(candidate)
        if normalized is not None:
            return normalized
        if isinstance(candidate, Mapping):
            for key in (
                "status",
                "state",
                "validation_status",
                "resolution_status",
                "calibration_status",
                "review_state",
                "supported",
                "valid",
                "complete",
                "captured",
                "normalized",
                "resolved",
                "confirmed",
            ):
                nested = _normalize_status_value(candidate.get(key))
                if nested is not None:
                    return nested

    return None


def _normalize_status_value(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return None
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _PASS_STATUS_VALUES:
            return True
        if normalized in _FAIL_STATUS_VALUES:
            return False

    return None


def _is_meaningful_value(value: Any) -> bool:
    if value is None or value is False:
        return False
    if isinstance(value, str):
        normalized = value.strip().lower()
        return (
            bool(normalized)
            and normalized not in _REVIEW_STATUS_VALUES
            and normalized not in _FAIL_STATUS_VALUES
        )
    if isinstance(value, (int, float)):
        return value > 0
    if isinstance(value, Mapping):
        return bool(value) and any(_is_meaningful_value(item) for item in value.values())
    if isinstance(value, (list, tuple, set, frozenset)):
        return any(_is_meaningful_value(item) for item in value)

    return True


def _sequence_mappings(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, (list, tuple)):
        return []

    return [item for item in value if isinstance(item, Mapping)]


def _entity_mappings(canonical_json: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return _sequence_mappings(canonical_json.get("entities"))


def _entity_has_valid_geometry(entity: Mapping[str, Any]) -> bool | None:
    kind = str(entity.get("kind", "")).strip().lower()
    if kind in _LINE_ENTITY_KINDS:
        return (
            (
                _has_coordinate_value(entity.get("start"))
                and _has_coordinate_value(entity.get("end"))
            )
            or _has_coordinate_sequence(entity.get("points"), minimum_points=2)
            or _has_coordinate_sequence(entity.get("vertices"), minimum_points=2)
            or _has_numeric_fields(entity, ("x1", "y1", "x2", "y2"))
        )
    if kind in _POLYGON_ENTITY_KINDS:
        return _has_valid_polygon_area_geometry(
            entity.get("points")
        ) or _has_valid_polygon_area_geometry(
            entity.get("vertices")
        )
    if kind in _POINT_ENTITY_KINDS:
        return (
            _has_coordinate_value(entity.get("point"))
            or _has_coordinate_value(entity.get("position"))
            or _has_coordinate_value(entity.get("location"))
            or _has_numeric_fields(entity, ("x", "y"))
        )
    if kind in _CENTER_RADIUS_ENTITY_KINDS:
        return (
            (
                _has_coordinate_value(entity.get("center"))
                or _has_coordinate_value(entity.get("origin"))
            )
            and (
                _is_positive_number(entity.get("radius"))
                or _is_positive_number(entity.get("diameter"))
            )
        )
    if kind in _BLOCK_REFERENCE_ENTITY_KINDS:
        return (
            _has_coordinate_value(entity.get("insert"))
            or _has_coordinate_value(entity.get("position"))
            or _has_coordinate_value(entity.get("location"))
            or _has_numeric_fields(entity, ("x", "y"))
        )

    return None


def _has_coordinate_sequence(candidate: Any, *, minimum_points: int) -> bool:
    if not isinstance(candidate, (list, tuple)):
        return False

    points = sum(_has_coordinate_value(item) for item in candidate)
    return points >= minimum_points


def _has_valid_polygon_area_geometry(candidate: Any) -> bool:
    if not isinstance(candidate, (list, tuple)):
        return False

    points = [point for item in candidate if (point := _coordinate_xy(item)) is not None]
    if len(points) < 3:
        return False

    distinct_points = list(dict.fromkeys(points))
    if len(distinct_points) < 3:
        return False

    ring_points = points[:-1] if points[0] == points[-1] else points
    if len(ring_points) < 3:
        return False

    area_twice = _polygon_signed_area_twice(ring_points)
    return isfinite(area_twice) and area_twice != 0.0


def _coordinate_xy(candidate: Any) -> tuple[float, float] | None:
    if isinstance(candidate, Mapping):
        x_value = _finite_float(candidate.get("x"))
        y_value = _finite_float(candidate.get("y"))
        if x_value is not None and y_value is not None:
            return x_value, y_value
        return None
    if not isinstance(candidate, (list, tuple)) or len(candidate) < 2:
        return None

    x_value = _finite_float(candidate[0])
    y_value = _finite_float(candidate[1])
    if x_value is not None and y_value is not None:
        return x_value, y_value

    return None


def _polygon_signed_area_twice(points: list[tuple[float, float]]) -> float:
    signed_area_twice = 0.0
    for index, (x_value, y_value) in enumerate(points):
        next_x, next_y = points[(index + 1) % len(points)]
        signed_area_twice += (x_value * next_y) - (next_x * y_value)

    return signed_area_twice


def _has_coordinate_value(candidate: Any) -> bool:
    if isinstance(candidate, Mapping):
        return _has_numeric_fields(candidate, ("x", "y"))
    if not isinstance(candidate, (list, tuple)):
        return False

    numeric_components = sum(_is_number(component) for component in candidate)
    return numeric_components >= 2


def _has_numeric_fields(candidate: Mapping[str, Any], fields: tuple[str, ...]) -> bool:
    return all(_is_number(candidate.get(field)) for field in fields)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _finite_float(value: Any) -> float | None:
    if not _is_finite_number(value):
        return None

    return float(value)


def _is_finite_number(value: Any) -> bool:
    return _is_number(value) and isfinite(value)


def _is_positive_number(value: Any) -> bool:
    return _is_number(value) and value > 0


def _polygon_entities(canonical_json: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    polygon_entities: list[Mapping[str, Any]] = []
    for entity in _entity_mappings(canonical_json):
        kind = str(entity.get("kind", "")).strip().lower()
        if kind in _POLYGON_ENTITY_KINDS:
            polygon_entities.append(entity)

    return polygon_entities


def _polygon_closed_state(entity: Mapping[str, Any]) -> bool | None:
    return _normalize_status_hint(
        entity.get("closed"),
        entity.get("is_closed"),
        entity.get("area_quantity_eligible"),
    )


def _has_block_transform_content(canonical_json: Mapping[str, Any]) -> bool:
    blocks = canonical_json.get("blocks")
    if isinstance(blocks, (list, tuple)) and bool(blocks):
        return True
    for entity in _entity_mappings(canonical_json):
        kind = str(entity.get("kind", "")).strip().lower()
        if kind in _BLOCK_REFERENCE_ENTITY_KINDS:
            return True
        if entity.get("transform") is not None or entity.get("block_name") is not None:
            return True

    return False


def _xref_ref(xref: Mapping[str, Any], *, index: int) -> str:
    for key in ("ref", "name", "path", "source_ref"):
        value = xref.get(key)
        if isinstance(value, str) and value.strip():
            return value

    return f"xref-{index}"


def _is_valid_pdf_scale(candidate: Any) -> bool:
    if isinstance(candidate, bool):
        return False
    if isinstance(candidate, (int, float)):
        return candidate > 0
    if isinstance(candidate, str):
        return (
            bool(candidate.strip())
            and candidate.strip().lower() not in _PDF_SCALE_UNCONFIRMED_VALUES
        )
    if isinstance(candidate, Mapping):
        if not candidate:
            return False
        if _normalize_status_hint(candidate.get("confirmed"), candidate.get("calibrated")) is False:
            return False
        return any(
            _is_meaningful_value(candidate.get(key))
            for key in ("scale", "ratio", "value", "factor", "numerator", "denominator")
        )
    if isinstance(candidate, (list, tuple)):
        return bool(candidate)

    return False


def _explicit_pdf_scale_status(candidate: Any) -> str | None:
    if not isinstance(candidate, Mapping):
        return None

    explicit_status = _normalize_status_hint(
        candidate.get("status"),
        candidate.get("calibration_status"),
    )
    if explicit_status is False:
        return "invalid"
    for value in (candidate.get("status"), candidate.get("calibration_status")):
        if isinstance(value, str) and value.strip().lower() in _PDF_SCALE_UNCONFIRMED_VALUES:
            return "unconfirmed"
    if _normalize_status_hint(candidate.get("confirmed"), candidate.get("calibrated")) is False:
        return "unconfirmed"

    return None


def _is_unconfirmed_pdf_scale(candidate: Any) -> bool:
    if isinstance(candidate, str):
        return candidate.strip().lower() in _PDF_SCALE_UNCONFIRMED_VALUES
    if isinstance(candidate, Mapping):
        status = candidate.get("status") or candidate.get("calibration_status")
        if isinstance(status, str) and status.strip().lower() in _PDF_SCALE_UNCONFIRMED_VALUES:
            return True
        return (
            _normalize_status_hint(candidate.get("confirmed"), candidate.get("calibrated")) is False
        )

    return False


def _adapter_warning_message(warning: Mapping[str, Any], *, index: int) -> str:
    for key in ("message", "summary", "warning"):
        value = warning.get(key)
        if value is not None:
            return str(value)

    return f"Adapter warning {index} was reported."


def _adapter_warning_target_ref(warning: Mapping[str, Any], *, index: int) -> str:
    for key in ("target_ref", "source_ref", "entity_ref", "ref", "code"):
        value = warning.get(key)
        if value is not None:
            return str(value)

    return f"adapter-warning-{index}"


def _derive_validation_status(
    *,
    checks: list[dict[str, Any]],
    findings: list[dict[str, Any]],
    review_required: bool,
) -> str:
    check_statuses = {str(check["status"]) for check in checks}
    if "fail" in check_statuses:
        return "invalid"

    if review_required or "review_required" in check_statuses:
        return "needs_review"

    if "warning" in check_statuses or any(finding["severity"] == "warning" for finding in findings):
        return "valid_with_warnings"

    return "valid"


def _derive_review_state(
    *,
    validation_status: str,
    effective_confidence: float,
    review_required: bool,
) -> str:
    if validation_status == "invalid":
        return "rejected"

    if review_required or validation_status == "needs_review":
        return "review_required"

    if effective_confidence < _APPROVED_THRESHOLD:
        return "provisional"

    return "approved"


def _derive_quantity_gate(*, validation_status: str, review_state: str) -> str:
    if validation_status == "invalid" or review_state in {"rejected", "superseded"}:
        return "blocked"

    if review_state == "approved" and validation_status in {"valid", "valid_with_warnings"}:
        return "allowed"

    if review_state == "provisional" and validation_status in {
        "valid",
        "valid_with_warnings",
        "needs_review",
    }:
        return "allowed_provisional"

    if review_state == "review_required":
        return "review_gated"

    return "blocked"


def _build_summary(
    *,
    canonical_json: Mapping[str, Any],
    checks: list[dict[str, Any]],
    findings: list[dict[str, Any]],
) -> dict[str, Any]:
    check_status_totals = {
        "pass": 0,
        "warning": 0,
        "review_required": 0,
        "fail": 0,
    }
    for check in checks:
        status = str(check["status"])
        check_status_totals[status] = check_status_totals.get(status, 0) + 1

    severity_totals = {"info": 0, "warning": 0, "error": 0, "critical": 0}
    for finding in findings:
        severity = str(finding["severity"])
        severity_totals[severity] = severity_totals.get(severity, 0) + 1

    return {
        "checks_total": len(checks),
        "findings_total": len(findings),
        "warnings_total": severity_totals["warning"],
        "errors_total": severity_totals["error"],
        "critical_total": severity_totals["critical"],
        "check_status_totals": check_status_totals,
        "entity_counts": _entity_counts(canonical_json),
    }


def _entity_counts(canonical_json: Mapping[str, Any]) -> dict[str, int]:
    return {
        "layouts": _sequence_length(canonical_json.get("layouts")),
        "layers": _sequence_length(canonical_json.get("layers")),
        "blocks": _sequence_length(canonical_json.get("blocks")),
        "entities": _sequence_length(canonical_json.get("entities")),
    }


def _sequence_length(value: Any) -> int:
    if isinstance(value, (list, tuple)):
        return len(value)

    return 0


def _confidence_score(result: AdapterResult) -> float:
    if result.confidence is None or result.confidence.score is None:
        return 0.0

    return float(result.confidence.score)


def _json_compatible(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _json_compatible(asdict(value))

    if isinstance(value, Mapping):
        return {str(key): _json_compatible(item) for key, item in value.items()}

    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_compatible(item) for item in value]

    if isinstance(value, UUID):
        return str(value)

    return value
