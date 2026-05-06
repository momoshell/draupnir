"""Helpers for normalizing adapter results into finalization payloads."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from app.ingestion.contracts import AdapterResult, InputFamily
from app.ingestion.validation import (
    VALIDATION_REPORT_SCHEMA_VERSION,
    build_validation_outcome,
)

_CANONICAL_ENTITY_SCHEMA_VERSION = "0.1"
_INITIAL_INGEST_REVISION_KIND = "ingest"
_REPROCESS_REVISION_KIND = "reprocess"


@dataclass(frozen=True, slots=True)
class IngestFinalizationPayload:
    """Prepared ingest payload inserted during finalization."""

    revision_kind: str
    adapter_key: str
    adapter_version: str
    input_family: str
    canonical_entity_schema_version: str
    canonical_json: dict[str, Any]
    provenance_json: dict[str, Any]
    confidence_json: dict[str, Any]
    confidence_score: float
    warnings_json: list[Any]
    diagnostics_json: dict[str, Any]
    result_checksum_sha256: str
    validation_report_schema_version: str
    validation_status: str
    review_state: str
    quantity_gate: str
    effective_confidence: float
    validator_name: str
    validator_version: str
    report_json: dict[str, Any]
    generated_at: datetime


@dataclass(frozen=True, slots=True)
class IngestFinalizationContext:
    """Immutable context required to build a finalization payload."""

    job_id: UUID
    file_id: UUID
    extraction_profile_id: UUID | None
    initial_job_id: UUID | None
    input_family: InputFamily
    adapter_key: str
    adapter_version: str


def utcnow() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(UTC)


def resolve_revision_kind(job_id: UUID, *, initial_job_id: UUID | None) -> str:
    """Map file linkage to the correct ingest revision kind."""
    if initial_job_id == job_id:
        return _INITIAL_INGEST_REVISION_KIND

    return _REPROCESS_REVISION_KIND


def compute_adapter_result_checksum(result_envelope: Mapping[str, Any]) -> str:
    """Return a stable SHA-256 checksum for a committed result envelope."""
    payload = json.dumps(
        _json_compatible(result_envelope),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_ingest_finalization_payload(
    context: IngestFinalizationContext,
    *,
    result: AdapterResult,
    generated_at: datetime | None = None,
) -> IngestFinalizationPayload:
    """Build a deterministic finalization payload from an adapter result."""
    emitted_at = generated_at or utcnow()
    canonical_json = _coerce_dict(result.canonical)
    canonical_entity_schema_version = _resolve_canonical_schema_version(canonical_json)
    revision_kind = resolve_revision_kind(context.job_id, initial_job_id=context.initial_job_id)
    provenance_records = [_json_compatible(record) for record in result.provenance]
    diagnostics = [_json_compatible(diagnostic) for diagnostic in result.diagnostics]
    validation_outcome = build_validation_outcome(
        input_family=context.input_family,
        canonical_json=canonical_json,
        canonical_entity_schema_version=canonical_entity_schema_version,
        result=result,
        generated_at=emitted_at,
    )
    warnings_json = validation_outcome.adapter_warnings_json
    confidence_json = validation_outcome.confidence_json
    provenance_json = {
        "schema_version": canonical_entity_schema_version,
        "adapter": {
            "key": context.adapter_key,
            "version": context.adapter_version,
        },
        "source": {
            "file_id": str(context.file_id),
            "job_id": str(context.job_id),
            "extraction_profile_id": (
                str(context.extraction_profile_id)
                if context.extraction_profile_id is not None
                else None
            ),
            "input_family": context.input_family.value,
            "revision_kind": revision_kind,
        },
        "records": provenance_records,
        "generated_at": emitted_at.isoformat(),
    }
    diagnostics_json = {
        "adapter": context.adapter_key,
        "adapter_version": context.adapter_version,
        "diagnostics": diagnostics,
    }
    report_json = validation_outcome.report_json
    result_envelope = {
        "adapter_key": context.adapter_key,
        "adapter_version": context.adapter_version,
        "input_family": context.input_family.value,
        "canonical_entity_schema_version": canonical_entity_schema_version,
        "canonical_json": canonical_json,
        "provenance_json": provenance_json,
        "confidence_json": confidence_json,
        "confidence_score": validation_outcome.confidence_score,
        "warnings_json": warnings_json,
        "diagnostics_json": diagnostics_json,
    }

    return IngestFinalizationPayload(
        revision_kind=revision_kind,
        adapter_key=context.adapter_key,
        adapter_version=context.adapter_version,
        input_family=context.input_family.value,
        canonical_entity_schema_version=canonical_entity_schema_version,
        canonical_json=canonical_json,
        provenance_json=provenance_json,
        confidence_json=confidence_json,
        confidence_score=validation_outcome.confidence_score,
        warnings_json=warnings_json,
        diagnostics_json=diagnostics_json,
        result_checksum_sha256=compute_adapter_result_checksum(result_envelope),
        validation_report_schema_version=VALIDATION_REPORT_SCHEMA_VERSION,
        validation_status=validation_outcome.validation_status,
        review_state=validation_outcome.review_state,
        quantity_gate=validation_outcome.quantity_gate,
        effective_confidence=validation_outcome.effective_confidence,
        validator_name=validation_outcome.validator_name,
        validator_version=validation_outcome.validator_version,
        report_json=report_json,
        generated_at=emitted_at,
    )


def _coerce_dict(value: Mapping[str, Any]) -> dict[str, Any]:
    payload = _json_compatible(value)
    if not isinstance(payload, dict):
        raise TypeError("Adapter canonical payload must serialize to an object.")

    return payload


def _resolve_canonical_schema_version(canonical_json: dict[str, Any]) -> str:
    if "canonical_entity_schema_version" in canonical_json:
        return str(canonical_json["canonical_entity_schema_version"])

    if "schema_version" in canonical_json:
        schema_version = str(canonical_json["schema_version"])
        canonical_json.setdefault("canonical_entity_schema_version", schema_version)
        return schema_version

    canonical_json["canonical_entity_schema_version"] = _CANONICAL_ENTITY_SCHEMA_VERSION
    canonical_json.setdefault("schema_version", _CANONICAL_ENTITY_SCHEMA_VERSION)
    return _CANONICAL_ENTITY_SCHEMA_VERSION
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
