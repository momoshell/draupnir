"""Validation policy derivation helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ._constants import (
    _PLACEHOLDER_ADAPTER_MODE_VALUES,
    _PLACEHOLDER_EMPTY_ENTITY_REASONS,
    _PLACEHOLDER_STATUS_VALUES,
    _SOURCE_DOCUMENT_REF,
)
from ._types import _PlaceholderReviewPolicy
from ._utils import (
    _extract_placeholder_semantics,
    _json_compatible,
    _metadata_candidate,
    _normalized_string,
)


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
