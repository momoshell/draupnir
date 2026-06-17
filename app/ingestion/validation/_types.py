"""Validation dataclasses for ingest validation policy."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ValidationOutcome:
    """Typed validation policy result for ingest finalization."""

    validation_status: str
    validator_name: str
    validator_version: str
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
