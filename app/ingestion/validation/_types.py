"""Validation dataclasses for ingest validation policy."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


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
