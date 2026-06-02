"""Transaction-neutral CAD changeset persistence helpers."""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.cad_changeset import (
    CAD_CHANGE_OPERATION_TYPES,
    CAD_CHANGE_SET_STATUSES,
    CAD_CHANGE_SET_VALIDATION_STATUSES,
    CadChangeOperation,
    CadChangeSet,
    CadChangeSetValidationResult,
)

_ALLOWED_OPERATION_TYPES = frozenset(CAD_CHANGE_OPERATION_TYPES)
_ALLOWED_CHANGE_SET_STATUSES = frozenset(CAD_CHANGE_SET_STATUSES)
_ALLOWED_VALIDATION_STATUSES = frozenset(CAD_CHANGE_SET_VALIDATION_STATUSES)


@dataclass(frozen=True, slots=True)
class CadChangeOperationCreate:
    operation_type: str
    operation_json: Mapping[str, Any]
    target_revision_entity_id: uuid.UUID | None = None
    expected_source_identity: str | None = None
    expected_source_hash: str | None = None


@dataclass(frozen=True, slots=True)
class CadChangeSetValidationResultCreate:
    validation_status: str
    result_json: Mapping[str, Any]
    validator_name: str | None = None
    validator_version: str | None = None


def normalize_change_operation_type(operation_type: str) -> str:
    return _normalize_choice(
        value=operation_type,
        field_name="operation_type",
        allowed_values=_ALLOWED_OPERATION_TYPES,
    )


def normalize_change_set_status(status: str) -> str:
    return _normalize_choice(
        value=status,
        field_name="status",
        allowed_values=_ALLOWED_CHANGE_SET_STATUSES,
    )


def normalize_change_set_validation_status(validation_status: str) -> str:
    return _normalize_choice(
        value=validation_status,
        field_name="validation_status",
        allowed_values=_ALLOWED_VALIDATION_STATUSES,
    )


async def create_change_set(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
    base_revision_id: uuid.UUID,
    status: str,
    operations: Sequence[CadChangeOperationCreate],
    created_by: str | None = None,
    change_set_id: uuid.UUID | None = None,
) -> CadChangeSet:
    normalized_status = normalize_change_set_status(status)
    normalized_operations = tuple(
        _normalize_operation_create(operation) for operation in operations
    )
    normalized_change_set_id = change_set_id or uuid.uuid4()

    change_set = CadChangeSet(
        id=normalized_change_set_id,
        project_id=project_id,
        base_revision_id=base_revision_id,
        status=normalized_status,
        created_by=_normalize_optional_text(created_by),
    )
    session.add(change_set)
    await session.flush()

    session.add_all(
        CadChangeOperation(
            project_id=project_id,
            change_set_id=normalized_change_set_id,
            sequence_index=sequence_index,
            operation_type=operation.operation_type,
            target_revision_entity_id=operation.target_revision_entity_id,
            expected_source_identity=operation.expected_source_identity,
            expected_source_hash=operation.expected_source_hash,
            operation_json=operation.operation_json,
        )
        for sequence_index, operation in enumerate(normalized_operations, start=1)
    )
    await session.flush()
    return change_set


async def update_change_set_status(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
    change_set_id: uuid.UUID,
    status: str,
) -> CadChangeSet | None:
    change_set = await get_change_set(
        session,
        project_id=project_id,
        change_set_id=change_set_id,
    )
    if change_set is None:
        return None

    change_set.status = normalize_change_set_status(status)
    await session.flush()
    return change_set


async def append_change_set_validation_result(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
    change_set_id: uuid.UUID,
    validation_result: CadChangeSetValidationResultCreate,
    validation_result_id: uuid.UUID | None = None,
) -> CadChangeSetValidationResult:
    normalized_result = _normalize_validation_result_create(validation_result)
    result = CadChangeSetValidationResult(
        id=validation_result_id or uuid.uuid4(),
        project_id=project_id,
        change_set_id=change_set_id,
        validation_status=normalized_result.validation_status,
        validator_name=normalized_result.validator_name,
        validator_version=normalized_result.validator_version,
        result_json=normalized_result.result_json,
    )
    session.add(result)
    await session.flush()
    return result


async def get_change_set(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
    change_set_id: uuid.UUID,
) -> CadChangeSet | None:
    result = await session.execute(
        select(CadChangeSet).where(
            CadChangeSet.project_id == project_id,
            CadChangeSet.id == change_set_id,
        )
    )
    return result.scalar_one_or_none()


async def list_change_set_operations(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
    change_set_id: uuid.UUID,
) -> tuple[CadChangeOperation, ...]:
    result = await session.execute(
        select(CadChangeOperation)
        .where(
            CadChangeOperation.project_id == project_id,
            CadChangeOperation.change_set_id == change_set_id,
        )
        .order_by(CadChangeOperation.sequence_index.asc(), CadChangeOperation.id.asc())
    )
    return tuple(result.scalars().all())


async def list_change_set_validation_results(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
    change_set_id: uuid.UUID,
) -> tuple[CadChangeSetValidationResult, ...]:
    result = await session.execute(
        select(CadChangeSetValidationResult)
        .where(
            CadChangeSetValidationResult.project_id == project_id,
            CadChangeSetValidationResult.change_set_id == change_set_id,
        )
        .order_by(
            CadChangeSetValidationResult.created_at.asc(),
            CadChangeSetValidationResult.id.asc(),
        )
    )
    return tuple(result.scalars().all())


def _normalize_choice(
    *,
    value: str,
    field_name: str,
    allowed_values: frozenset[str],
) -> str:
    normalized = value.strip().lower()
    if normalized not in allowed_values:
        allowed = ", ".join(sorted(allowed_values))
        raise ValueError(f"Unsupported {field_name} {value!r}; expected one of: {allowed}")
    return normalized


def _normalize_operation_create(operation: CadChangeOperationCreate) -> CadChangeOperationCreate:
    return CadChangeOperationCreate(
        operation_type=normalize_change_operation_type(operation.operation_type),
        operation_json=_coerce_json_mapping(operation.operation_json, field_name="operation_json"),
        target_revision_entity_id=operation.target_revision_entity_id,
        expected_source_identity=_normalize_optional_text(operation.expected_source_identity),
        expected_source_hash=_normalize_optional_text(operation.expected_source_hash),
    )


def _normalize_validation_result_create(
    validation_result: CadChangeSetValidationResultCreate,
) -> CadChangeSetValidationResultCreate:
    return CadChangeSetValidationResultCreate(
        validation_status=normalize_change_set_validation_status(
            validation_result.validation_status
        ),
        result_json=_coerce_json_mapping(validation_result.result_json, field_name="result_json"),
        validator_name=_normalize_optional_text(validation_result.validator_name),
        validator_version=_normalize_optional_text(validation_result.validator_version),
    )


def _coerce_json_mapping(
    value: Mapping[str, Any],
    *,
    field_name: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping")
    return dict(value)


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


__all__ = [
    "CadChangeOperationCreate",
    "CadChangeSetValidationResultCreate",
    "append_change_set_validation_result",
    "create_change_set",
    "get_change_set",
    "list_change_set_operations",
    "list_change_set_validation_results",
    "normalize_change_operation_type",
    "normalize_change_set_status",
    "normalize_change_set_validation_status",
    "update_change_set_status",
]
