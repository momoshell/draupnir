from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime
from typing import Any, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.cad.changesets import CadChangeOperationCreate as DomainCadChangeOperationCreate
from app.models.cad_changeset import CAD_CHANGE_OPERATION_TYPES

_ALLOWED_OPERATION_TYPES = frozenset(CAD_CHANGE_OPERATION_TYPES)
_SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")
_REVISION_ENTITY_TARGETED_OPERATION_TYPES = frozenset(
    {
        "annotate_entity",
        "change_layer",
        "remove_entity",
    }
)
_NON_EMPTY_TARGET_OPERATION_TYPES = frozenset(
    {
        "replace_block",
        "replace_profile_material_candidate",
        "update_property",
    }
)
# Accepted payload keys for annotate_entity, shared with the apply-time validator so the create
# contract and the validator never drift. A non-empty string under a text key, or a JSON object
# under an object key, is a valid annotation.
ANNOTATE_ENTITY_TEXT_KEYS = ("annotation", "text", "label", "note")
ANNOTATE_ENTITY_OBJECT_KEYS = ("annotation", "metadata")


def _normalize_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("must be a string")
    normalized = value.strip()
    return normalized or None


def _normalize_operation_type(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("must be a string")
    normalized = value.strip().lower()
    if normalized not in _ALLOWED_OPERATION_TYPES:
        allowed = ", ".join(sorted(_ALLOWED_OPERATION_TYPES))
        raise ValueError(f"must be one of: {allowed}")
    return normalized


def _normalize_json_object(value: Any, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a JSON object")
    return dict(value)


def _normalize_expected_source_hash(value: Any) -> str | None:
    normalized = _normalize_optional_text(value)
    if normalized is None:
        return None
    normalized = normalized.lower()
    if _SHA256_HEX_RE.fullmatch(normalized) is None:
        raise ValueError("must be a lowercase 64-character SHA-256 hex string")
    return normalized


def _normalize_target(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    target = _normalize_json_object(value, field_name="target")
    revision_entity_id = target.get("revision_entity_id")
    if revision_entity_id is not None:
        try:
            target["revision_entity_id"] = str(UUID(str(revision_entity_id)))
        except (TypeError, ValueError) as exc:
            raise ValueError("target.revision_entity_id must be a valid UUID") from exc
    return target


def _read_target_revision_entity_id(target: Mapping[str, Any] | None) -> UUID | None:
    if target is None:
        return None
    value = target.get("revision_entity_id")
    if value is None:
        return None
    return UUID(str(value))


def _normalize_payload_version(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("must be the integer 1")
    normalized = cast(int, value)
    if normalized != 1:
        raise ValueError("must be 1")
    return normalized


def _normalize_sequence_index(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("must be a positive integer")
    normalized = cast(int, value)
    if normalized < 1:
        raise ValueError("must be greater than or equal to 1")
    return normalized


def _has_non_empty_text(payload: Mapping[str, Any], *keys: str) -> bool:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return True
    return False


def _has_json_object(payload: Mapping[str, Any], *keys: str) -> bool:
    return any(isinstance(payload.get(key), Mapping) for key in keys)


def _has_key(payload: Mapping[str, Any], *keys: str) -> bool:
    return any(key in payload for key in keys)


def _require_payload_keys(
    payload: Mapping[str, Any],
    *,
    operation_type: str,
    text_keys: tuple[str, ...] = (),
    object_keys: tuple[str, ...] = (),
    key_keys: tuple[str, ...] = (),
) -> None:
    if (
        _has_non_empty_text(payload, *text_keys)
        or _has_json_object(payload, *object_keys)
        or _has_key(payload, *key_keys)
    ):
        return

    required_fields = [*text_keys, *object_keys, *key_keys]
    required_fields_text = ", ".join(required_fields)
    raise ValueError(
        f"payload must include at least one required field for {operation_type}: "
        f"{required_fields_text}"
    )


class CadChangeOperationCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    payload_version: int
    sequence_index: int | None = None
    operation_type: str
    target: dict[str, Any] | None = None
    expected_source_identity: str | None = Field(default=None, max_length=255)
    expected_source_hash: str | None = None
    payload: dict[str, Any]
    reason: str | None = None
    provenance: dict[str, Any] | None = None

    @model_validator(mode="before")
    @classmethod
    def validate_no_operation_id(cls, value: Any) -> Any:
        if isinstance(value, Mapping) and "operation_id" in value:
            raise ValueError("operation_id is server-assigned and must not be provided")
        return value

    @field_validator("payload_version", mode="before")
    @classmethod
    def validate_payload_version(cls, value: Any) -> int:
        return _normalize_payload_version(value)

    @field_validator("sequence_index", mode="before")
    @classmethod
    def validate_sequence_index(cls, value: Any) -> int | None:
        return _normalize_sequence_index(value)

    @field_validator("operation_type", mode="before")
    @classmethod
    def validate_operation_type(cls, value: Any) -> str:
        return _normalize_operation_type(value)

    @field_validator("target", mode="before")
    @classmethod
    def validate_target(cls, value: Any) -> dict[str, Any] | None:
        return _normalize_target(value)

    @field_validator("expected_source_identity", mode="before")
    @classmethod
    def validate_expected_source_identity(cls, value: Any) -> str | None:
        return _normalize_optional_text(value)

    @field_validator("expected_source_hash", mode="before")
    @classmethod
    def validate_expected_source_hash(cls, value: Any) -> str | None:
        return _normalize_expected_source_hash(value)

    @field_validator("payload", mode="before")
    @classmethod
    def validate_payload(cls, value: Any) -> dict[str, Any]:
        return _normalize_json_object(value, field_name="payload")

    @field_validator("reason", mode="before")
    @classmethod
    def validate_reason(cls, value: Any) -> str | None:
        return _normalize_optional_text(value)

    @field_validator("provenance", mode="before")
    @classmethod
    def validate_provenance(cls, value: Any) -> dict[str, Any] | None:
        if value is None:
            return None
        return _normalize_json_object(value, field_name="provenance")

    @model_validator(mode="after")
    def validate_contract(self) -> CadChangeOperationCreateRequest:
        revision_entity_id = _read_target_revision_entity_id(self.target)

        if (
            self.operation_type in _REVISION_ENTITY_TARGETED_OPERATION_TYPES
            and revision_entity_id is None
        ):
            raise ValueError(f"target.revision_entity_id is required for {self.operation_type}")

        if self.operation_type in _NON_EMPTY_TARGET_OPERATION_TYPES and not self.target:
            raise ValueError(f"target must be a non-empty JSON object for {self.operation_type}")

        if self.operation_type == "annotate_entity":
            _require_payload_keys(
                self.payload,
                operation_type=self.operation_type,
                text_keys=ANNOTATE_ENTITY_TEXT_KEYS,
                object_keys=ANNOTATE_ENTITY_OBJECT_KEYS,
            )
        elif self.operation_type == "change_layer":
            _require_payload_keys(
                self.payload,
                operation_type=self.operation_type,
                text_keys=("layer", "layer_name", "new_layer"),
            )
        elif self.operation_type == "remove_entity":
            _require_payload_keys(
                self.payload,
                operation_type=self.operation_type,
                text_keys=("reason", "removal_reason", "note"),
                object_keys=("context", "metadata"),
            )
        elif self.operation_type == "add_entity":
            _require_payload_keys(
                self.payload,
                operation_type=self.operation_type,
                object_keys=("entity", "canonical_entity", "geometry"),
            )
        elif self.operation_type == "replace_block":
            _require_payload_keys(
                self.payload,
                operation_type=self.operation_type,
                text_keys=("block_name", "new_block_name"),
                object_keys=("block", "replacement"),
            )
        elif self.operation_type == "replace_profile_material_candidate":
            _require_payload_keys(
                self.payload,
                operation_type=self.operation_type,
                text_keys=(
                    "profile",
                    "profile_name",
                    "material",
                    "material_name",
                    "candidate",
                ),
                object_keys=("candidate", "replacement", "profile", "material"),
            )
        elif self.operation_type == "update_property":
            _require_payload_keys(
                self.payload,
                operation_type=self.operation_type,
                text_keys=("property", "property_name", "property_key", "path", "name"),
            )
            if not _has_key(self.payload, "value", "new_value"):
                raise ValueError("payload must include value or new_value for update_property")
        elif self.operation_type == "flag_for_review":
            if not self.target and not (
                _has_non_empty_text(self.payload, "subject")
                or _has_json_object(self.payload, "subject", "context")
                or _has_key(self.payload, "context")
            ):
                raise ValueError(
                    "flag_for_review requires a target object or payload subject/context"
                )
            _require_payload_keys(
                self.payload,
                operation_type=self.operation_type,
                text_keys=("reason", "flag", "code", "note"),
                object_keys=("review", "metadata"),
            )

        return self

    def to_domain(self) -> DomainCadChangeOperationCreate:
        return DomainCadChangeOperationCreate(
            operation_type=self.operation_type,
            operation_json={
                "payload_version": self.payload_version,
                "target": self.target,
                "payload": self.payload,
                "reason": self.reason,
                "provenance": self.provenance,
            },
            target_revision_entity_id=_read_target_revision_entity_id(self.target),
            expected_source_identity=self.expected_source_identity,
            expected_source_hash=self.expected_source_hash,
        )


class CadChangeSetCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operations: list[CadChangeOperationCreateRequest] = Field(min_length=1)
    created_by: str | None = Field(default=None, max_length=128)

    @field_validator("created_by", mode="before")
    @classmethod
    def validate_created_by(cls, value: Any) -> str | None:
        return _normalize_optional_text(value)

    @model_validator(mode="after")
    def validate_operation_sequence_indexes(self) -> CadChangeSetCreateRequest:
        for index, operation in enumerate(self.operations, start=1):
            if operation.sequence_index is not None and operation.sequence_index != index:
                raise ValueError("operation sequence_index values must match 1-based list order")
        return self


class CadChangeOperationRead(BaseModel):
    operation_id: UUID
    sequence_index: int
    payload_version: int
    operation_type: str
    target: dict[str, Any] | None = None
    expected_source_identity: str | None = None
    expected_source_hash: str | None = None
    payload: dict[str, Any]
    reason: str | None = None
    provenance: dict[str, Any] | None = None
    created_at: datetime


class CadChangeSetRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    project_id: UUID
    base_revision_id: UUID
    status: str
    created_by: str | None = None
    created_at: datetime
    updated_at: datetime
    operations: list[CadChangeOperationRead]


class CadChangeSetValidationResultRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    project_id: UUID
    change_set_id: UUID
    validation_status: str
    validator_name: str | None = None
    validator_version: str | None = None
    result_json: dict[str, Any]
    created_at: datetime


class CadChangeSetValidationActionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    change_set: CadChangeSetRead
    validation_result: CadChangeSetValidationResultRead


class CadChangeSetListResponse(BaseModel):
    items: list[CadChangeSetRead]
    next_cursor: str | None = None


class CadChangeSetCursor(BaseModel):
    created_at: datetime
    id: UUID
