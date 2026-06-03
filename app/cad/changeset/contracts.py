"""Pure DTO contracts for CAD changeset apply workflows."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class RevisionRef:
    revision_id: uuid.UUID
    revision_sequence: int


@dataclass(frozen=True, slots=True)
class ChangeSetOperationTarget:
    target_revision_entity_id: uuid.UUID | None = None
    entity_id: str | None = None
    expected_source_identity: str | None = None
    expected_source_hash: str | None = None


@dataclass(frozen=True, slots=True)
class ChangeSetOperation:
    operation_id: uuid.UUID
    sequence_index: int
    operation_type: str
    operation_json: Mapping[str, Any]
    target: ChangeSetOperationTarget = field(default_factory=ChangeSetOperationTarget)


@dataclass(frozen=True, slots=True)
class RevisionEntitySnapshot:
    id: uuid.UUID
    sequence_index: int
    entity_id: str
    entity_type: str
    entity_schema_version: str
    confidence_score: float
    confidence_json: Mapping[str, Any]
    geometry_json: Mapping[str, Any]
    properties_json: Mapping[str, Any]
    provenance_json: Mapping[str, Any]
    parent_entity_ref: str | None = None
    canonical_entity_json: Mapping[str, Any] | None = None
    layout_ref: str | None = None
    layer_ref: str | None = None
    block_ref: str | None = None
    source_identity: str | None = None
    source_hash: str | None = None


@dataclass(frozen=True, slots=True)
class AppliedEntity:
    operation_id: uuid.UUID
    effect: Literal["unchanged", "changed", "added", "removed", "metadata_only"]
    entity: RevisionEntitySnapshot


@dataclass(frozen=True, slots=True)
class ChangeSetApplyEffects:
    unchanged_entities: tuple[AppliedEntity, ...] = ()
    changed_entities: tuple[AppliedEntity, ...] = ()
    added_entities: tuple[AppliedEntity, ...] = ()
    metadata_only_entities: tuple[AppliedEntity, ...] = ()
    removed_entities: tuple[AppliedEntity, ...] = ()


@dataclass(frozen=True, slots=True)
class ChangeSetApplyConflictTarget:
    operation_id: uuid.UUID
    sequence_index: int
    operation_type: str
    target_revision_entity_id: uuid.UUID | None = None
    entity_id: str | None = None
    expected_source_identity: str | None = None
    expected_source_hash: str | None = None


@dataclass(frozen=True, slots=True)
class ChangeSetApplyConflictDetails:
    base_revision_id: uuid.UUID
    base_revision_sequence: int
    current_revision_id: uuid.UUID
    current_revision_sequence: int
    change_set_id: uuid.UUID
    conflicting_targets: tuple[ChangeSetApplyConflictTarget, ...] = ()


@dataclass(frozen=True, slots=True)
class ChangeSetApplySuccess:
    change_set_id: uuid.UUID
    base_revision: RevisionRef
    current_revision: RevisionRef
    operations: tuple[ChangeSetOperation, ...]
    entities: tuple[RevisionEntitySnapshot, ...] = ()
    effects: ChangeSetApplyEffects = field(default_factory=ChangeSetApplyEffects)
    status: Literal["applied"] = "applied"


@dataclass(frozen=True, slots=True)
class ChangeSetApplyConflict:
    details: ChangeSetApplyConflictDetails
    status: Literal["revision_conflict"] = "revision_conflict"


@dataclass(frozen=True, slots=True)
class ChangeSetApplyError:
    change_set_id: uuid.UUID
    error_code: str
    message: str
    details: Mapping[str, Any] | None = None
    status: Literal["apply_failed"] = "apply_failed"


type ChangeSetApplyResult = ChangeSetApplySuccess | ChangeSetApplyConflict | ChangeSetApplyError
