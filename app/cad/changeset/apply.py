"""Pure copy-on-write changeset apply engine."""

from __future__ import annotations

import hashlib
import math
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any, Literal, cast

from app.cad.changeset.conflicts import build_stale_base_conflict
from app.cad.changeset.contracts import (
    AppliedEntity,
    ChangeSetApplyConflictTarget,
    ChangeSetApplyEffects,
    ChangeSetApplyError,
    ChangeSetApplyResult,
    ChangeSetApplySuccess,
    ChangeSetOperation,
    ChangeSetOperationTarget,
    RevisionEntitySnapshot,
    RevisionRef,
)

SUPPORTED_OPERATION_TYPES = frozenset(
    {
        "change_layer",
        "update_property",
        "add_entity",
        "remove_entity",
        "annotate_entity",
        "flag_for_review",
    }
)

ALLOWED_LAYER_KEYS = ("layer_ref", "layer", "layer_name", "new_layer")
ALLOWED_PROPERTY_PATHS = frozenset(
    {
        "properties.description",
        "properties.label",
        "properties.mark",
        "properties.notes",
        "properties.review_status",
        "properties.metadata",
    }
)

_EFFECT_PRIORITY = {
    "unchanged": 0,
    "metadata_only": 1,
    "changed": 2,
    "added": 3,
    "removed": 4,
}
_ADD_ENTITY_NAMESPACE = uuid.UUID("1e9ec7ab-0355-44a5-b69f-c34790709a44")
_CHANGESET_ADAPTER_KEY = "changeset"
_CHANGESET_PROVENANCE_ORIGIN = "user_created"

type _AppliedEffect = Literal["unchanged", "changed", "added", "removed", "metadata_only"]

_MISSING = object()


def apply_change_set(
    *,
    change_set_id: uuid.UUID,
    base_revision: RevisionRef,
    current_revision: RevisionRef,
    operations: Sequence[ChangeSetOperation],
    entities: Sequence[RevisionEntitySnapshot],
) -> ChangeSetApplyResult:
    if current_revision != base_revision:
        conflict = build_stale_base_conflict(
            change_set_id=change_set_id,
            base_revision=base_revision,
            current_revision=current_revision,
            conflicting_targets=tuple(
                _build_conflict_target(operation) for operation in operations
            ),
        )
        if conflict is None:
            return ChangeSetApplyError(
                change_set_id=change_set_id,
                error_code="INVALID_OPERATION",
                message="Stale revision check returned no conflict for mismatched revisions.",
            )
        return conflict

    working_entities = list(entities)
    survivor_effects: dict[uuid.UUID, tuple[uuid.UUID, str]] = {}
    removed_entities: dict[uuid.UUID, AppliedEntity] = {}

    for operation in operations:
        if operation.operation_type not in SUPPORTED_OPERATION_TYPES:
            return _operation_error(
                change_set_id=change_set_id,
                operation=operation,
                error_code="UNSUPPORTED_OPERATION",
                message=f"Unsupported changeset operation '{operation.operation_type}'.",
            )

        if operation.operation_type == "add_entity":
            apply_error = _apply_add_entity(
                change_set_id=change_set_id,
                operation=operation,
                working_entities=working_entities,
                survivor_effects=survivor_effects,
            )
            if apply_error is not None:
                return apply_error
            continue

        target_index = _resolve_target_index(
            change_set_id=change_set_id,
            operation=operation,
            entities=working_entities,
        )
        if isinstance(target_index, ChangeSetApplyError):
            return target_index

        if operation.operation_type == "change_layer":
            apply_error = _apply_change_layer(
                change_set_id=change_set_id,
                operation=operation,
                target_index=target_index,
                working_entities=working_entities,
                survivor_effects=survivor_effects,
            )
            if apply_error is not None:
                return apply_error
            continue

        if operation.operation_type == "update_property":
            apply_error = _apply_update_property(
                change_set_id=change_set_id,
                operation=operation,
                target_index=target_index,
                working_entities=working_entities,
                survivor_effects=survivor_effects,
            )
            if apply_error is not None:
                return apply_error
            continue

        if operation.operation_type == "remove_entity":
            removed_entity = working_entities.pop(target_index)
            removed_entities[removed_entity.id] = AppliedEntity(
                operation_id=operation.operation_id,
                effect="removed",
                entity=removed_entity,
            )
            survivor_effects.pop(removed_entity.id, None)
            continue

        if operation.operation_type == "annotate_entity":
            apply_error = _apply_metadata_append(
                change_set_id=change_set_id,
                operation=operation,
                target_index=target_index,
                working_entities=working_entities,
                survivor_effects=survivor_effects,
                metadata_key="annotations",
                payload_key="annotation",
            )
            if apply_error is not None:
                return apply_error
            continue

        if operation.operation_type == "flag_for_review":
            apply_error = _apply_metadata_append(
                change_set_id=change_set_id,
                operation=operation,
                target_index=target_index,
                working_entities=working_entities,
                survivor_effects=survivor_effects,
                metadata_key="review_flags",
                payload_key="review_flag",
            )
            if apply_error is not None:
                return apply_error
            continue

    normalized_entities = _normalize_sequence_indexes(working_entities)
    normalized_by_id = {entity.id: entity for entity in normalized_entities}

    unchanged_entities: list[AppliedEntity] = []
    changed_entities: list[AppliedEntity] = []
    added_entities: list[AppliedEntity] = []
    metadata_only_entities: list[AppliedEntity] = []

    for entity in normalized_entities:
        effect_record = survivor_effects.get(entity.id)
        if effect_record is None:
            unchanged_entities.append(
                AppliedEntity(
                    operation_id=uuid.UUID(int=0),
                    effect="unchanged",
                    entity=entity,
                )
            )
            continue

        operation_id, effect = effect_record
        applied_entity = AppliedEntity(
            operation_id=operation_id,
            effect=cast(_AppliedEffect, effect),
            entity=normalized_by_id[entity.id],
        )
        if effect == "changed":
            changed_entities.append(applied_entity)
        elif effect == "added":
            added_entities.append(applied_entity)
        elif effect == "metadata_only":
            metadata_only_entities.append(applied_entity)
        else:
            unchanged_entities.append(applied_entity)

    return ChangeSetApplySuccess(
        change_set_id=change_set_id,
        base_revision=base_revision,
        current_revision=current_revision,
        operations=tuple(operations),
        entities=tuple(normalized_entities),
        effects=ChangeSetApplyEffects(
            unchanged_entities=tuple(unchanged_entities),
            changed_entities=tuple(changed_entities),
            added_entities=tuple(added_entities),
            metadata_only_entities=tuple(metadata_only_entities),
            removed_entities=tuple(removed_entities.values()),
        ),
    )


def _apply_add_entity(
    *,
    change_set_id: uuid.UUID,
    operation: ChangeSetOperation,
    working_entities: list[RevisionEntitySnapshot],
    survivor_effects: dict[uuid.UUID, tuple[uuid.UUID, str]],
) -> ChangeSetApplyError | None:
    if _target_has_selectors(operation):
        return _operation_error(
            change_set_id=change_set_id,
            operation=operation,
            error_code="INVALID_OPERATION",
            message="add_entity must not target an existing entity.",
        )

    operation_payload = operation.operation_json
    top_level_canonical_entity = _mapping_field(operation_payload, "canonical_entity")
    entity_payload = (
        _mapping_field(operation_payload, "entity")
        or top_level_canonical_entity
        or operation_payload
    )
    canonical_entity_payload = (
        _mapping_field(entity_payload, "canonical_entity_json")
        or _mapping_field(entity_payload, "canonical_entity")
        or (top_level_canonical_entity if entity_payload is top_level_canonical_entity else None)
    )
    if not entity_payload:
        return _operation_error(
            change_set_id=change_set_id,
            operation=operation,
            error_code="INVALID_OPERATION",
            message="add_entity requires an entity payload.",
        )

    entity_type = _required_string_field(entity_payload, "entity_type")
    if isinstance(entity_type, ChangeSetApplyError):
        return _with_operation_context(change_set_id, operation, entity_type)

    entity_schema_version = _optional_string_field(entity_payload, "entity_schema_version") or "1"
    confidence_score = _number_field(entity_payload, "confidence_score", default=1.0)
    if isinstance(confidence_score, ChangeSetApplyError):
        return _with_operation_context(change_set_id, operation, confidence_score)

    confidence_json = _mapping_field(entity_payload, "confidence_json") or {}
    geometry_json = _mapping_field(entity_payload, "geometry_json") or _mapping_field(
        entity_payload, "geometry"
    )
    if geometry_json is None and entity_payload is not operation_payload:
        geometry_json = _mapping_field(operation_payload, "geometry")
    if geometry_json is None:
        geometry_json = {}
    properties_json = _mapping_field(entity_payload, "properties_json") or {}
    source_identity = _derived_added_entity_source_identity(
        change_set_id=change_set_id,
        operation_id=operation.operation_id,
    )
    source_hash = _derived_added_entity_source_hash(source_identity)
    provenance_json = _build_added_entity_provenance(
        source_identity=source_identity,
        source_hash=source_hash,
    )

    entity_id = _optional_string_field(entity_payload, "entity_id") or _derived_entity_id(
        change_set_id=change_set_id,
        operation_id=operation.operation_id,
    )
    revision_entity_id = _uuid_field(
        entity_payload,
        "revision_entity_id",
    ) or _derived_revision_entity_id(
        change_set_id=change_set_id,
        operation_id=operation.operation_id,
    )

    if any(entity.id == revision_entity_id for entity in working_entities):
        return _operation_error(
            change_set_id=change_set_id,
            operation=operation,
            error_code="DUPLICATE_ENTITY",
            message=f"Revision entity '{revision_entity_id}' already exists.",
        )
    if any(entity.entity_id == entity_id for entity in working_entities):
        return _operation_error(
            change_set_id=change_set_id,
            operation=operation,
            error_code="DUPLICATE_ENTITY",
            message=f"Entity '{entity_id}' already exists.",
        )

    added_entity = RevisionEntitySnapshot(
        id=revision_entity_id,
        sequence_index=len(working_entities),
        entity_id=entity_id,
        entity_type=entity_type,
        entity_schema_version=entity_schema_version,
        confidence_score=confidence_score,
        confidence_json=_clone_json_mapping(confidence_json),
        geometry_json=_clone_json_mapping(geometry_json),
        properties_json=_clone_json_mapping(properties_json),
        provenance_json=_clone_json_mapping(provenance_json),
        parent_entity_ref=_optional_string_field(entity_payload, "parent_entity_ref"),
        canonical_entity_json=_clone_optional_mapping(canonical_entity_payload),
        layout_ref=_optional_string_field(entity_payload, "layout_ref"),
        layer_ref=_read_layer_value(entity_payload),
        block_ref=_optional_string_field(entity_payload, "block_ref"),
        source_identity=source_identity,
        source_hash=source_hash,
    )
    working_entities.append(added_entity)
    _record_survivor_effect(
        survivor_effects=survivor_effects,
        entity_id=added_entity.id,
        operation_id=operation.operation_id,
        effect="added",
    )
    return None


def _apply_change_layer(
    *,
    change_set_id: uuid.UUID,
    operation: ChangeSetOperation,
    target_index: int,
    working_entities: list[RevisionEntitySnapshot],
    survivor_effects: dict[uuid.UUID, tuple[uuid.UUID, str]],
) -> ChangeSetApplyError | None:
    entity = working_entities[target_index]
    layer_ref = _read_layer_value(operation.operation_json)
    if layer_ref is None:
        return _operation_error(
            change_set_id=change_set_id,
            operation=operation,
            error_code="INVALID_OPERATION",
            message="change_layer requires a non-empty layer value.",
        )

    updated_entity = replace(
        entity,
        layer_ref=layer_ref,
        properties_json=_sync_layer_aliases(entity.properties_json, layer_ref),
        canonical_entity_json=_sync_canonical_layer_aliases(
            entity.canonical_entity_json,
            layer_ref,
        ),
    )
    if updated_entity == entity:
        _record_survivor_effect(
            survivor_effects=survivor_effects,
            entity_id=entity.id,
            operation_id=operation.operation_id,
            effect="unchanged",
        )
        return None

    working_entities[target_index] = updated_entity
    _record_survivor_effect(
        survivor_effects=survivor_effects,
        entity_id=updated_entity.id,
        operation_id=operation.operation_id,
        effect="changed",
    )
    return None


def _apply_update_property(
    *,
    change_set_id: uuid.UUID,
    operation: ChangeSetOperation,
    target_index: int,
    working_entities: list[RevisionEntitySnapshot],
    survivor_effects: dict[uuid.UUID, tuple[uuid.UUID, str]],
) -> ChangeSetApplyError | None:
    entity = working_entities[target_index]
    property_path = _property_path(operation.operation_json)
    if property_path not in ALLOWED_PROPERTY_PATHS:
        return _operation_error(
            change_set_id=change_set_id,
            operation=operation,
            error_code="INVALID_OPERATION",
            message=f"Unsupported property path '{property_path}'.",
            details={"allowed_property_paths": tuple(sorted(ALLOWED_PROPERTY_PATHS))},
        )
    new_value = _operation_value(operation.operation_json)
    if new_value is _MISSING:
        return _operation_error(
            change_set_id=change_set_id,
            operation=operation,
            error_code="INVALID_OPERATION",
            message="update_property requires a value.",
        )

    property_key = property_path.removeprefix("properties.")
    properties_json = _clone_json_mapping(entity.properties_json)
    properties_json[property_key] = new_value
    updated_entity = replace(
        entity,
        properties_json=properties_json,
        canonical_entity_json=_sync_canonical_property(
            entity.canonical_entity_json,
            property_key,
            new_value,
        ),
    )
    if updated_entity == entity:
        _record_survivor_effect(
            survivor_effects=survivor_effects,
            entity_id=entity.id,
            operation_id=operation.operation_id,
            effect="unchanged",
        )
        return None

    working_entities[target_index] = updated_entity
    _record_survivor_effect(
        survivor_effects=survivor_effects,
        entity_id=updated_entity.id,
        operation_id=operation.operation_id,
        effect="metadata_only",
    )
    return None


def _apply_metadata_append(
    *,
    change_set_id: uuid.UUID,
    operation: ChangeSetOperation,
    target_index: int,
    working_entities: list[RevisionEntitySnapshot],
    survivor_effects: dict[uuid.UUID, tuple[uuid.UUID, str]],
    metadata_key: str,
    payload_key: str,
) -> ChangeSetApplyError | None:
    entity = working_entities[target_index]
    payload = _mapping_field(operation.operation_json, payload_key) or operation.operation_json
    if not payload:
        return _operation_error(
            change_set_id=change_set_id,
            operation=operation,
            error_code="INVALID_OPERATION",
            message=f"{operation.operation_type} requires a metadata payload.",
        )

    properties_json = _clone_json_mapping(entity.properties_json)
    metadata_value = properties_json.get("metadata")
    if metadata_value is None:
        metadata: dict[str, Any] = {}
    elif isinstance(metadata_value, Mapping):
        metadata = _clone_json_mapping(metadata_value)
    else:
        return _operation_error(
            change_set_id=change_set_id,
            operation=operation,
            error_code="INVALID_OPERATION",
            message="Entity properties.metadata must be an object for metadata-only operations.",
        )

    existing_entries = metadata.get(metadata_key)
    if existing_entries is None:
        entries: list[Any] = []
    elif isinstance(existing_entries, list):
        entries = cast(list[Any], _clone_json_value(existing_entries))
    else:
        return _operation_error(
            change_set_id=change_set_id,
            operation=operation,
            error_code="INVALID_OPERATION",
            message=f"Entity properties.metadata.{metadata_key} must be an array when present.",
        )

    entries.append(_clone_json_mapping(payload))
    metadata[metadata_key] = entries
    properties_json["metadata"] = metadata

    updated_entity = replace(entity, properties_json=properties_json)
    working_entities[target_index] = updated_entity
    _record_survivor_effect(
        survivor_effects=survivor_effects,
        entity_id=updated_entity.id,
        operation_id=operation.operation_id,
        effect="metadata_only",
    )
    return None


def _resolve_target_index(
    *,
    change_set_id: uuid.UUID,
    operation: ChangeSetOperation,
    entities: Sequence[RevisionEntitySnapshot],
) -> int | ChangeSetApplyError:
    target = operation.target
    selectors = (
        target.target_revision_entity_id,
        target.entity_id,
        target.expected_source_identity,
        target.expected_source_hash,
    )
    if not any(selector is not None for selector in selectors):
        return _operation_error(
            change_set_id=change_set_id,
            operation=operation,
            error_code="INVALID_OPERATION",
            message="Entity-targeted changeset operations require a target selector.",
        )

    matches = [
        index
        for index, entity in enumerate(entities)
        if _matches_target(entity=entity, target=target)
    ]
    if not matches:
        return _operation_error(
            change_set_id=change_set_id,
            operation=operation,
            error_code="TARGET_NOT_FOUND",
            message="Target entity was not found.",
        )
    if len(matches) > 1:
        return _operation_error(
            change_set_id=change_set_id,
            operation=operation,
            error_code="AMBIGUOUS_TARGET",
            message="Target selectors matched multiple entities.",
        )
    return matches[0]


def _matches_target(
    *,
    entity: RevisionEntitySnapshot,
    target: ChangeSetOperationTarget,
) -> bool:
    if (
        target.target_revision_entity_id is not None
        and entity.id != target.target_revision_entity_id
    ):
        return False
    if target.entity_id is not None and entity.entity_id != target.entity_id:
        return False
    if (
        target.expected_source_identity is not None
        and entity.source_identity != target.expected_source_identity
    ):
        return False
    return target.expected_source_hash is None or entity.source_hash == target.expected_source_hash


def _build_conflict_target(operation: ChangeSetOperation) -> ChangeSetApplyConflictTarget:
    return ChangeSetApplyConflictTarget(
        operation_id=operation.operation_id,
        sequence_index=operation.sequence_index,
        operation_type=operation.operation_type,
        target_revision_entity_id=operation.target.target_revision_entity_id,
        entity_id=operation.target.entity_id,
        expected_source_identity=operation.target.expected_source_identity,
        expected_source_hash=operation.target.expected_source_hash,
    )


def _record_survivor_effect(
    *,
    survivor_effects: dict[uuid.UUID, tuple[uuid.UUID, str]],
    entity_id: uuid.UUID,
    operation_id: uuid.UUID,
    effect: str,
) -> None:
    current = survivor_effects.get(entity_id)
    if current is None or _EFFECT_PRIORITY[effect] > _EFFECT_PRIORITY[current[1]]:
        survivor_effects[entity_id] = (operation_id, effect)


def _normalize_sequence_indexes(
    entities: Sequence[RevisionEntitySnapshot],
) -> tuple[RevisionEntitySnapshot, ...]:
    return tuple(
        entity if entity.sequence_index == index else replace(entity, sequence_index=index)
        for index, entity in enumerate(entities)
    )


def _sync_layer_aliases(payload: Mapping[str, Any], layer_ref: str) -> dict[str, Any]:
    updated_payload = _clone_json_mapping(payload)
    for key in ALLOWED_LAYER_KEYS:
        if key in updated_payload:
            updated_payload[key] = layer_ref
    return updated_payload


def _sync_canonical_layer_aliases(
    canonical_entity_json: Mapping[str, Any] | None,
    layer_ref: str,
) -> dict[str, Any] | None:
    if canonical_entity_json is None:
        return None

    updated_canonical = _sync_layer_aliases(canonical_entity_json, layer_ref)
    properties_value = updated_canonical.get("properties")
    if isinstance(properties_value, Mapping):
        updated_canonical["properties"] = _sync_layer_aliases(
            cast(Mapping[str, Any], properties_value),
            layer_ref,
        )
    return updated_canonical


def _sync_canonical_property(
    canonical_entity_json: Mapping[str, Any] | None,
    property_key: str,
    value: Any,
) -> dict[str, Any] | None:
    if canonical_entity_json is None:
        return None

    updated_canonical = _clone_json_mapping(canonical_entity_json)
    properties_value = updated_canonical.get("properties")
    if not isinstance(properties_value, Mapping):
        return updated_canonical

    updated_properties = _clone_json_mapping(cast(Mapping[str, Any], properties_value))
    if property_key not in updated_properties:
        return updated_canonical

    updated_properties[property_key] = _clone_json_value(value)
    updated_canonical["properties"] = updated_properties
    return updated_canonical


def _property_path(operation_json: Mapping[str, Any]) -> str:
    for key in ("property_path", "path", "property"):
        value = operation_json.get(key)
        if isinstance(value, str):
            return value

    for key in ("property_name", "property_key", "name"):
        value = operation_json.get(key)
        if isinstance(value, str) and value.strip():
            property_key = value.strip()
            if property_key.startswith("properties."):
                return property_key
            return f"properties.{property_key}"
    return ""


def _operation_value(operation_json: Mapping[str, Any]) -> Any:
    for key in ("value", "new_value"):
        if key in operation_json:
            return _clone_json_value(operation_json[key])
    return _MISSING


def _target_has_selectors(operation: ChangeSetOperation) -> bool:
    return any(
        value is not None
        for value in (
            operation.target.target_revision_entity_id,
            operation.target.entity_id,
            operation.target.expected_source_identity,
            operation.target.expected_source_hash,
        )
    )


def _read_layer_value(payload: Mapping[str, Any]) -> str | None:
    for key in ALLOWED_LAYER_KEYS:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _required_string_field(payload: Mapping[str, Any], key: str) -> str | ChangeSetApplyError:
    value = _optional_string_field(payload, key)
    if value is None:
        return ChangeSetApplyError(
            change_set_id=uuid.UUID(int=0),
            error_code="INVALID_OPERATION",
            message=f"Field '{key}' must be a non-empty string.",
        )
    return value


def _optional_string_field(payload: Mapping[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, str) and value.strip():
        return value
    return None


def _number_field(
    payload: Mapping[str, Any],
    key: str,
    *,
    default: float,
) -> float | ChangeSetApplyError:
    value = payload.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return ChangeSetApplyError(
            change_set_id=uuid.UUID(int=0),
            error_code="INVALID_OPERATION",
            message=f"Field '{key}' must be a finite number.",
        )
    value_float = float(value)
    if not math.isfinite(value_float):
        return ChangeSetApplyError(
            change_set_id=uuid.UUID(int=0),
            error_code="INVALID_OPERATION",
            message=f"Field '{key}' must be a finite number.",
        )
    return value_float


def _uuid_field(payload: Mapping[str, Any], key: str) -> uuid.UUID | None:
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, uuid.UUID):
        return value
    if isinstance(value, str):
        try:
            return uuid.UUID(value)
        except ValueError:
            return None
    return None


def _mapping_field(payload: Mapping[str, Any], key: str) -> Mapping[str, Any] | None:
    value = payload.get(key)
    if isinstance(value, Mapping):
        return cast(Mapping[str, Any], value)
    return None


def _clone_optional_mapping(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    return _clone_json_mapping(value)


def _clone_json_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): _clone_json_value(item) for key, item in value.items()}


def _clone_json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _clone_json_mapping(cast(Mapping[str, Any], value))
    if isinstance(value, list):
        return [_clone_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [_clone_json_value(item) for item in value]
    return value


def _derived_entity_id(*, change_set_id: uuid.UUID, operation_id: uuid.UUID) -> str:
    return f"changeset:{change_set_id}:operation:{operation_id}"


def _derived_added_entity_source_identity(
    *,
    change_set_id: uuid.UUID,
    operation_id: uuid.UUID,
) -> str:
    return _derived_entity_id(change_set_id=change_set_id, operation_id=operation_id)


def _derived_added_entity_source_hash(source_identity: str) -> str:
    return hashlib.sha256(source_identity.encode("utf-8")).hexdigest()


def _build_added_entity_provenance(
    *,
    source_identity: str,
    source_hash: str,
) -> dict[str, Any]:
    return {
        "origin": _CHANGESET_PROVENANCE_ORIGIN,
        "adapter": _CHANGESET_ADAPTER_KEY,
        "source_identity": source_identity,
        "source_hash": source_hash,
    }


def _derived_revision_entity_id(*, change_set_id: uuid.UUID, operation_id: uuid.UUID) -> uuid.UUID:
    return uuid.uuid5(
        _ADD_ENTITY_NAMESPACE,
        f"changeset:{change_set_id}:operation:{operation_id}:revision-entity",
    )


def _with_operation_context(
    change_set_id: uuid.UUID,
    operation: ChangeSetOperation,
    error: ChangeSetApplyError,
) -> ChangeSetApplyError:
    return _operation_error(
        change_set_id=change_set_id,
        operation=operation,
        error_code=error.error_code,
        message=error.message,
        details=error.details,
    )


def _operation_error(
    *,
    change_set_id: uuid.UUID,
    operation: ChangeSetOperation,
    error_code: str,
    message: str,
    details: Mapping[str, Any] | None = None,
) -> ChangeSetApplyError:
    base_details: dict[str, Any] = {
        "operation_id": str(operation.operation_id),
        "sequence_index": operation.sequence_index,
        "operation_type": operation.operation_type,
    }
    if details is not None:
        base_details.update(dict(details))
    return ChangeSetApplyError(
        change_set_id=change_set_id,
        error_code=error_code,
        message=message,
        details=base_details,
    )
