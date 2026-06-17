"""Validation outcome orchestration helpers."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from app.ingestion.contracts import AdapterResult, InputFamily

from ._constants import (
    _REVIEW_THRESHOLD,
    _SOURCE_DOCUMENT_REF,
    RUNNER_VALIDATOR_NAME,
    RUNNER_VALIDATOR_VERSION,
    VALIDATION_REPORT_SCHEMA_VERSION,
)
from ._types import ValidationOutcome
from ._utils import (
    _adapter_warning_message,
    _adapter_warning_target_ref,
    _build_coverage,
    _build_summary,
    _confidence_score,
    _json_compatible,
)
from .checks import (
    _build_ifc_schema_check,
    _build_pdf_scale_check,
    _build_required_checks,
)
from .policy import (
    _apply_placeholder_review_policy,
    _derive_validation_status,
)


def build_validation_outcome(
    *,
    input_family: InputFamily,
    canonical_json: Mapping[str, Any],
    canonical_entity_schema_version: str,
    result: AdapterResult,
    generated_at: datetime,
) -> ValidationOutcome:
    """Build the canonical v0.1 validation report and review policy outcome."""

    # confidence_score is a transient review signal (feeds validation_status + findings);
    # it is no longer capped, derived into a gate, or persisted (Path B 5b).
    confidence_score = _confidence_score(result)
    adapter_review_required = bool(
        result.confidence.review_required if result.confidence is not None else True
    )
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

    checks, required_checks_require_review, _required_checks_invalid = _build_required_checks(
        canonical_json=canonical_json,
        add_finding=add_finding,
    )

    pdf_scale_check, pdf_scale_requires_review = _build_pdf_scale_check(
        input_family=input_family,
        canonical_json=canonical_json,
        add_finding=add_finding,
    )
    checks.append(pdf_scale_check)

    ifc_schema_check, _ifc_schema_invalid = _build_ifc_schema_check(
        input_family=input_family,
        canonical_json=canonical_json,
        add_finding=add_finding,
    )
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

    review_required = (
        input_family == InputFamily.PDF_RASTER
        or confidence_score < _REVIEW_THRESHOLD
        or adapter_review_required
        or placeholder_requires_review
        or required_checks_require_review
        or pdf_scale_requires_review
    )
    validation_status = _derive_validation_status(
        checks=checks,
        findings=findings,
        review_required=review_required,
    )
    report_json = {
        "validation_report_schema_version": VALIDATION_REPORT_SCHEMA_VERSION,
        "canonical_entity_schema_version": canonical_entity_schema_version,
        "validation_status": validation_status,
        "validator_name": RUNNER_VALIDATOR_NAME,
        "validator_version": RUNNER_VALIDATOR_VERSION,
        "provenance": _json_compatible(result.provenance),
        "summary": _build_summary(canonical_json=canonical_json, checks=checks, findings=findings),
        "coverage": _build_coverage(canonical_json),
        "checks": checks,
        "findings": findings,
        "adapter_warnings": adapter_warnings_json,
        "generated_at": generated_at.isoformat(),
    }

    return ValidationOutcome(
        validation_status=validation_status,
        validator_name=RUNNER_VALIDATOR_NAME,
        validator_version=RUNNER_VALIDATOR_VERSION,
        adapter_warnings_json=adapter_warnings_json,
        report_json=report_json,
    )
