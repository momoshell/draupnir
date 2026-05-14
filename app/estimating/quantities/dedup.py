from __future__ import annotations

import json
import math
from dataclasses import dataclass

from app.estimating.quantities.contracts import (
    BoundingBoxSummary,
    ContributorTrust,
    JSONValue,
    QuantityConflict,
    QuantityContributor,
    RevisionEntityInput,
)
from app.estimating.quantities.geometry import GeometryQuantity


@dataclass(frozen=True, slots=True)
class CandidateContribution:
    entity: RevisionEntityInput
    quantity: GeometryQuantity
    geometry_fingerprint: str
    bbox: BoundingBoxSummary | None
    provenance: dict[str, JSONValue]


@dataclass(frozen=True, slots=True)
class DeduplicationResult:
    contributors: tuple[QuantityContributor, ...]
    conflicts: tuple[QuantityConflict, ...]


def deduplicate_contributors(
    candidates: list[CandidateContribution],
) -> DeduplicationResult:
    groups: dict[str, list[CandidateContribution]] = {}
    for candidate in candidates:
        key = dedup_key(candidate)
        groups.setdefault(key, []).append(candidate)

    contributors: list[QuantityContributor] = []
    conflicts: list[QuantityConflict] = []
    for key in sorted(groups):
        group = groups[key]
        payloads = {_comparison_payload(candidate) for candidate in group}
        if len(payloads) > 1:
            conflicts.append(
                QuantityConflict(
                    dedup_key=key,
                    entity_ids=tuple(sorted(candidate.entity.entity_id for candidate in group)),
                    reason="dedup_conflict",
                    details={
                        "detail": "contributors share a dedup key but differ after normalization",
                    },
                )
            )
            continue

        primary = min(group, key=lambda candidate: candidate.entity.sequence_index)
        trust = _candidate_trust(primary)
        contributors.append(
            QuantityContributor(
                entity_id=primary.entity.entity_id,
                entity_type=primary.entity.entity_type,
                sequence_index=primary.entity.sequence_index,
                quantity_type=primary.quantity.quantity_type,
                value=primary.quantity.value,
                unit=primary.quantity.unit,
                context=primary.quantity.context,
                method=primary.quantity.method,
                trust=trust,
                dedup_key=key,
                geometry_fingerprint=primary.geometry_fingerprint,
                bbox=primary.bbox,
                source_identity=primary.entity.source_identity,
                source_hash=primary.entity.source_hash,
                provenance=primary.provenance,
                duplicate_entity_ids=tuple(
                    sorted(
                        candidate.entity.entity_id
                        for candidate in group
                        if candidate.entity.entity_id != primary.entity.entity_id
                    )
                ),
            )
        )

    contributors.sort(
        key=lambda contributor: (contributor.sequence_index, contributor.quantity_type)
    )
    return DeduplicationResult(
        contributors=tuple(contributors),
        conflicts=tuple(conflicts),
    )


def dedup_key(candidate: CandidateContribution) -> str:
    quantity = candidate.quantity
    source_hash = _normalize_hash(candidate.entity.source_hash)
    if source_hash is not None:
        payload: dict[str, JSONValue] = {
            "mode": "source_hash",
            "quantity_type": quantity.quantity_type,
            "source_hash": source_hash,
            "unit": quantity.unit,
            "context": quantity.context,
        }
        return _canonical(payload)

    fallback_source = candidate.entity.source_identity or _provenance_source_ref(
        candidate.provenance
    )
    payload = {
        "mode": "missing_source_hash",
        "quantity_type": quantity.quantity_type,
        "entity_id": candidate.entity.entity_id,
        "fallback_source": fallback_source,
        "unit": quantity.unit,
        "context": quantity.context,
        "geometry_fingerprint": candidate.geometry_fingerprint,
    }
    return _canonical(payload)


def _candidate_trust(candidate: CandidateContribution) -> ContributorTrust:
    if _normalize_hash(candidate.entity.source_hash) is not None:
        return "preferred"
    return "lower_trust"


def _comparison_payload(candidate: CandidateContribution) -> str:
    payload: dict[str, JSONValue] = {
        "entity_type": candidate.entity.entity_type,
        "quantity_type": candidate.quantity.quantity_type,
        "value": candidate.quantity.value,
        "unit": candidate.quantity.unit,
        "context": candidate.quantity.context,
        "method": candidate.quantity.method,
        "geometry_fingerprint": candidate.geometry_fingerprint,
        "source_identity": candidate.entity.source_identity,
        "source_hash": _normalize_hash(candidate.entity.source_hash),
        "canonical_entity_json": _normalize_json_value(candidate.entity.canonical_entity_json),
        "geometry_json": _normalize_json_value(candidate.entity.geometry_json),
        "provenance": _normalize_json_value(candidate.provenance),
    }
    return _canonical(payload)


def _normalize_hash(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if len(normalized) != 64:
        return None
    if any(character not in "0123456789abcdef" for character in normalized):
        return None
    return normalized


def _provenance_source_ref(provenance: dict[str, JSONValue]) -> str | None:
    source_ref = provenance.get("source_ref")
    return source_ref if isinstance(source_ref, str) and source_ref.strip() else None


def _canonical(payload: dict[str, JSONValue]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _normalize_json_value(value: JSONValue) -> JSONValue:
    if isinstance(value, dict):
        return {key: _normalize_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_json_value(item) for item in value]
    if isinstance(value, int | float) and not isinstance(value, bool):
        numeric = float(value)
        if not math.isfinite(numeric):
            if math.isnan(numeric):
                return "nan"
            if numeric > 0:
                return "infinity"
            return "-infinity"
        if isinstance(value, int):
            return value
        return numeric
    return value
