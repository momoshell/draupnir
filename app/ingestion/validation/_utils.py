"""Shared validation utility helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from typing import Any
from uuid import UUID

from app.ingestion.contracts import AdapterResult

from ._constants import _FAIL_STATUS_VALUES, _PLACEHOLDER_STATUS_VALUES, _REVIEW_STATUS_VALUES


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
