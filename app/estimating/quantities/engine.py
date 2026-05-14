from __future__ import annotations

import math
from typing import TypedDict

from app.estimating.quantities.contracts import (
    JSONValue,
    QuantityAggregate,
    QuantityContributor,
    QuantityEngineResult,
    QuantityExclusion,
    QuantityType,
    RevisionEntityInput,
    RevisionGateMetadata,
)
from app.estimating.quantities.dedup import (
    CandidateContribution,
    deduplicate_contributors,
)
from app.estimating.quantities.geometry import GeometryQuantity, extract_geometry_quantities


class NormalizedHint(TypedDict):
    quantity_type: QuantityType
    value: float
    unit: str
    context: str | None
    strict: bool
    provenance: dict[str, JSONValue]


def compute_quantities(
    gate: RevisionGateMetadata,
    entities: list[RevisionEntityInput],
) -> QuantityEngineResult:
    exclusions: list[QuantityExclusion] = []
    candidates: list[CandidateContribution] = []

    for entity in sorted(entities, key=lambda item: item.sequence_index):
        extraction = extract_geometry_quantities(entity)
        exclusions.extend(extraction.exclusions)

        geometry_quantities: dict[
            tuple[QuantityType, str, str | None],
            GeometryQuantity,
        ] = {
            (quantity.quantity_type, quantity.unit, quantity.context): quantity
            for quantity in extraction.quantities
        }
        hints = _normalize_hints(entity)

        for quantity in _count_quantities(entity):
            candidates.append(
                CandidateContribution(
                    entity=entity,
                    quantity=quantity,
                    geometry_fingerprint=extraction.fingerprint,
                    bbox=extraction.bbox,
                    provenance=_provenance_object(entity),
                )
            )

        for quantity in extraction.quantities:
            if _is_ineligible_geometric_unit(quantity):
                exclusions.append(
                    QuantityExclusion(
                        entity_id=entity.entity_id,
                        quantity_type=quantity.quantity_type,
                        reason="ineligible_unit",
                        details={"unit": quantity.unit},
                    )
                )
                continue
            candidates.append(
                CandidateContribution(
                    entity=entity,
                    quantity=quantity,
                    geometry_fingerprint=extraction.fingerprint,
                    bbox=extraction.bbox,
                    provenance=_provenance_object(entity),
                )
            )

        if not extraction.quantities:
            for hint in hints:
                if not _hint_supports_fallback(hint):
                    continue
                fallback_quantity = GeometryQuantity(
                    quantity_type=hint["quantity_type"],
                    value=hint["value"],
                    unit=hint["unit"],
                    context=hint["context"],
                    method="hint_fallback",
                )
                candidates.append(
                    CandidateContribution(
                        entity=entity,
                        quantity=fallback_quantity,
                        geometry_fingerprint=extraction.fingerprint,
                        bbox=extraction.bbox,
                        provenance=_hint_provenance(entity, hint),
                    )
                )
                break
            continue

        for hint in hints:
            key = (hint["quantity_type"], hint["unit"], hint["context"])
            geometry_quantity = geometry_quantities.get(key)
            if geometry_quantity is None:
                continue
            if geometry_quantity.value != hint["value"]:
                exclusions.append(
                    QuantityExclusion(
                        entity_id=entity.entity_id,
                        quantity_type=hint["quantity_type"],
                        reason="hint_mismatch",
                        details={
                            "hint_value": hint["value"],
                            "geometry_value": geometry_quantity.value,
                        },
                    )
                )

    deduped = deduplicate_contributors(candidates)
    trusted_totals = gate.status == "allowed"
    aggregates = (
        _aggregate(deduped.contributors, trusted=trusted_totals)
        if gate.status in {"allowed", "allowed_provisional"}
        else ()
    )
    return QuantityEngineResult(
        gate=gate,
        aggregates=aggregates,
        contributors=deduped.contributors,
        exclusions=tuple(exclusions),
        conflicts=deduped.conflicts,
        trusted_totals=trusted_totals,
    )


def _count_quantities(entity: RevisionEntityInput) -> tuple[GeometryQuantity, GeometryQuantity]:
    entity_type = _normalize_entity_type(entity.entity_type)
    return (
        GeometryQuantity(
            quantity_type="count",
            value=1.0,
            unit="each",
            context="entity_count",
            method="entity_count",
        ),
        GeometryQuantity(
            quantity_type="count",
            value=1.0,
            unit="each",
            context=f"{entity_type}_count",
            method="entity_count",
        ),
    )


def _aggregate(
    contributors: tuple[QuantityContributor, ...],
    trusted: bool,
) -> tuple[QuantityAggregate, ...]:
    grouped: dict[tuple[QuantityType, str, str | None], list[float]] = {}
    for contributor in contributors:
        key = (contributor.quantity_type, contributor.unit, contributor.context)
        grouped.setdefault(key, []).append(contributor.value)

    aggregates = [
        QuantityAggregate(
            quantity_type=quantity_type,
            unit=unit,
            context=context,
            total=math.fsum(values),
            contributor_count=len(values),
            trusted=trusted,
        )
        for (quantity_type, unit, context), values in sorted(grouped.items())
    ]
    return tuple(aggregates)


def _normalize_hints(entity: RevisionEntityInput) -> list[NormalizedHint]:
    properties = entity.properties_json
    if not isinstance(properties, dict):
        return []
    raw_hints = properties.get("quantity_hints")
    if isinstance(raw_hints, list):
        return [hint for hint in (_normalize_hint(item) for item in raw_hints) if hint is not None]
    if isinstance(raw_hints, dict):
        hints: list[NormalizedHint] = []
        for quantity_type, raw_value in raw_hints.items():
            if isinstance(raw_value, dict):
                merged_hint = {"quantity_type": quantity_type, **raw_value}
                hint = _normalize_hint(merged_hint)
            else:
                hint = _normalize_hint({"quantity_type": quantity_type, "value": raw_value})
            if hint is not None:
                hints.append(hint)
        return hints
    return []


def _normalize_hint(raw_hint: JSONValue) -> NormalizedHint | None:
    if not isinstance(raw_hint, dict):
        return None
    quantity_type = _parse_quantity_type(raw_hint.get("quantity_type") or raw_hint.get("type"))
    if quantity_type is None:
        return None
    raw_value = raw_hint.get("value")
    if isinstance(raw_value, bool) or not isinstance(raw_value, int | float):
        return None
    value = float(raw_value)
    if not math.isfinite(value) or value <= 0.0:
        return None
    raw_unit = raw_hint.get("unit")
    if raw_unit is not None and not isinstance(raw_unit, str):
        return None
    raw_context = raw_hint.get("context")
    if raw_context is not None and not isinstance(raw_context, str):
        return None
    provenance = raw_hint.get("provenance")
    if provenance is not None and not isinstance(provenance, dict):
        return None
    unit = "unitless"
    if isinstance(raw_unit, str) and raw_unit.strip():
        unit = raw_unit.strip().lower()
    context: str | None = None
    if isinstance(raw_context, str) and raw_context.strip():
        context = raw_context.strip().lower()
    return {
        "quantity_type": quantity_type,
        "value": value,
        "unit": unit,
        "context": context,
        "strict": raw_hint.get("strict") is True,
        "provenance": provenance if isinstance(provenance, dict) else {},
    }


def _parse_quantity_type(raw_value: JSONValue) -> QuantityType | None:
    if raw_value == "count":
        return "count"
    if raw_value == "length":
        return "length"
    if raw_value == "perimeter":
        return "perimeter"
    if raw_value == "area":
        return "area"
    return None


def _normalize_entity_type(raw_value: str) -> str:
    normalized = "_".join(raw_value.strip().lower().split())
    return normalized or "unknown"


def _is_ineligible_geometric_unit(quantity: GeometryQuantity) -> bool:
    if quantity.method != "geometry":
        return False
    if quantity.quantity_type == "count":
        return False
    return quantity.unit in {"unknown", "unitless", "point"}


def _hint_supports_fallback(hint: NormalizedHint) -> bool:
    if hint["strict"] is not True:
        return False
    if hint["quantity_type"] == "count":
        return False
    if hint["unit"] in {"unknown", "unitless", "point"}:
        return False
    provenance = hint["provenance"]
    return bool(provenance)


def _provenance_object(entity: RevisionEntityInput) -> dict[str, JSONValue]:
    if isinstance(entity.provenance_json, dict):
        return entity.provenance_json
    return {}


def _hint_provenance(
    entity: RevisionEntityInput,
    hint: NormalizedHint,
) -> dict[str, JSONValue]:
    provenance = _provenance_object(entity).copy()
    hint_provenance = hint["provenance"]
    if hint_provenance:
        provenance["hint_provenance"] = hint_provenance
    return provenance
