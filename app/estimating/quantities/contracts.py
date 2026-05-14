from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

type JSONScalar = None | bool | int | float | str
type JSONValue = JSONScalar | list[JSONValue] | dict[str, JSONValue]
type GateStatus = Literal[
    "allowed",
    "allowed_provisional",
    "review_gated",
    "blocked",
]
type QuantityType = Literal["length", "perimeter", "area", "count"]
type ContributorTrust = Literal["preferred", "lower_trust"]
type ContributorMethod = Literal["geometry", "hint_fallback", "entity_count"]


@dataclass(frozen=True, slots=True)
class RevisionGateMetadata:
    status: GateStatus
    validation_status: str | None = None
    reason: str | None = None
    details: dict[str, JSONValue] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RevisionEntityInput:
    entity_id: str
    entity_type: str
    sequence_index: int
    geometry_json: JSONValue
    properties_json: JSONValue
    provenance_json: JSONValue
    canonical_entity_json: JSONValue
    source_identity: str | None = None
    source_hash: str | None = None


@dataclass(frozen=True, slots=True)
class BoundingBoxSummary:
    min_x: float
    min_y: float
    max_x: float
    max_y: float
    point_count: int


@dataclass(frozen=True, slots=True)
class QuantityContributor:
    entity_id: str
    entity_type: str
    sequence_index: int
    quantity_type: QuantityType
    value: float
    unit: str
    context: str | None
    method: ContributorMethod
    trust: ContributorTrust
    dedup_key: str
    geometry_fingerprint: str
    bbox: BoundingBoxSummary | None
    source_identity: str | None
    source_hash: str | None
    provenance: dict[str, JSONValue]
    duplicate_entity_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class QuantityAggregate:
    quantity_type: QuantityType
    unit: str
    context: str | None
    total: float
    contributor_count: int
    trusted: bool


@dataclass(frozen=True, slots=True)
class QuantityExclusion:
    entity_id: str
    quantity_type: QuantityType | None
    reason: str
    details: dict[str, JSONValue] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class QuantityConflict:
    dedup_key: str
    entity_ids: tuple[str, ...]
    reason: str
    details: dict[str, JSONValue] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class QuantityEngineResult:
    gate: RevisionGateMetadata
    aggregates: tuple[QuantityAggregate, ...]
    contributors: tuple[QuantityContributor, ...]
    exclusions: tuple[QuantityExclusion, ...]
    conflicts: tuple[QuantityConflict, ...]
    trusted_totals: bool
