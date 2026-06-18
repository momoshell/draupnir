from __future__ import annotations

import copy
import uuid
from dataclasses import dataclass
from typing import cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.cad.changeset.apply import apply_change_set
from app.cad.changeset.conflicts import build_stale_base_conflict
from app.cad.changeset.contracts import (
    ChangeSetApplyConflict,
    ChangeSetApplyConflictTarget,
    ChangeSetApplyResult,
    ChangeSetOperation,
    ChangeSetOperationTarget,
    RevisionEntitySnapshot,
    RevisionRef,
)
from app.models.cad_changeset import (
    CadChangeOperation,
    CadChangeSet,
    CadChangeSetValidationResult,
)
from app.models.drawing_revision import DrawingRevision
from app.models.revision_materialization import RevisionEntity

_VALID_CHANGE_SET_VALIDATION_STATUSES = frozenset({"valid", "valid_with_warnings"})


class ChangeSetApplyLoadError(ValueError):
    """Raised when persisted changeset state cannot be safely applied."""


@dataclass(frozen=True, slots=True)
class LoadedChangeSetApplyInput:
    change_set_id: uuid.UUID
    base_revision: RevisionRef
    current_revision: RevisionRef
    operations: tuple[ChangeSetOperation, ...]
    entities: tuple[RevisionEntitySnapshot, ...]


async def load_change_set_apply_input(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
    change_set_id: uuid.UUID,
) -> LoadedChangeSetApplyInput | ChangeSetApplyConflict:
    """Load persisted changeset state into the pure apply contract."""

    change_set = await _load_change_set(session, project_id=project_id, change_set_id=change_set_id)
    # No separate human-approval gate: a changeset whose latest validation is valid (or
    # valid_with_warnings) is apply-ready. The prior `approved`-status requirement had no
    # API transition to satisfy it, so it only blocked the export path. Validation remains
    # the sole guard.
    await _require_latest_valid_validation(
        session,
        project_id=project_id,
        change_set_id=change_set_id,
    )

    base_revision_model = await _load_base_revision(
        session,
        project_id=project_id,
        base_revision_id=change_set.base_revision_id,
    )
    base_revision = RevisionRef(
        revision_id=base_revision_model.id,
        revision_sequence=base_revision_model.revision_sequence,
    )

    current_revision_model = await _load_current_revision(
        session,
        project_id=project_id,
        source_file_id=base_revision_model.source_file_id,
    )
    current_revision = RevisionRef(
        revision_id=current_revision_model.id,
        revision_sequence=current_revision_model.revision_sequence,
    )

    operations = await _load_change_set_operations(
        session,
        project_id=project_id,
        change_set_id=change_set_id,
    )
    if current_revision != base_revision:
        conflict = build_stale_base_conflict(
            change_set_id=change_set_id,
            base_revision=base_revision,
            current_revision=current_revision,
            conflicting_targets=_build_conflict_targets(operations),
        )
        assert conflict is not None
        return conflict

    entities = await _load_revision_entities(
        session,
        project_id=project_id,
        drawing_revision_id=current_revision_model.id,
    )
    return LoadedChangeSetApplyInput(
        change_set_id=change_set_id,
        base_revision=base_revision,
        current_revision=current_revision,
        operations=operations,
        entities=entities,
    )


async def load_and_apply_change_set(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
    change_set_id: uuid.UUID,
) -> ChangeSetApplyResult:
    """Load persisted changeset state and delegate application to the pure engine."""

    loaded = await load_change_set_apply_input(
        session,
        project_id=project_id,
        change_set_id=change_set_id,
    )
    if isinstance(loaded, ChangeSetApplyConflict):
        return loaded

    return apply_change_set(
        change_set_id=loaded.change_set_id,
        base_revision=loaded.base_revision,
        current_revision=loaded.current_revision,
        operations=loaded.operations,
        entities=loaded.entities,
    )


async def _load_change_set(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
    change_set_id: uuid.UUID,
) -> CadChangeSet:
    with session.no_autoflush:
        result = await session.execute(
            select(CadChangeSet).where(
                CadChangeSet.project_id == project_id,
                CadChangeSet.id == change_set_id,
            )
        )
    change_set = result.scalar_one_or_none()
    if change_set is None:
        raise ChangeSetApplyLoadError(f"Changeset {change_set_id} was not found.")
    return change_set


async def _require_latest_valid_validation(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
    change_set_id: uuid.UUID,
) -> None:
    with session.no_autoflush:
        result = await session.execute(
            select(CadChangeSetValidationResult)
            .where(
                CadChangeSetValidationResult.project_id == project_id,
                CadChangeSetValidationResult.change_set_id == change_set_id,
            )
            .order_by(
                CadChangeSetValidationResult.created_at.desc(),
                CadChangeSetValidationResult.id.desc(),
            )
            .limit(1)
        )
    latest_validation = result.scalar_one_or_none()
    if latest_validation is None:
        raise ChangeSetApplyLoadError(
            f"Changeset {change_set_id} requires a latest validation result before apply loading."
        )
    normalized_validation_status = _normalize_status(latest_validation.validation_status)
    if normalized_validation_status not in _VALID_CHANGE_SET_VALIDATION_STATUSES:
        raise ChangeSetApplyLoadError(
            "Changeset "
            f"{change_set_id} latest validation must be valid or "
            "valid_with_warnings before apply loading."
        )


async def _load_base_revision(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
    base_revision_id: uuid.UUID,
) -> DrawingRevision:
    with session.no_autoflush:
        result = await session.execute(
            select(DrawingRevision).where(
                DrawingRevision.project_id == project_id,
                DrawingRevision.id == base_revision_id,
            )
        )
    base_revision = result.scalar_one_or_none()
    if base_revision is None:
        raise ChangeSetApplyLoadError(f"Base revision {base_revision_id} was not found.")
    return base_revision


async def _load_current_revision(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
    source_file_id: uuid.UUID,
) -> DrawingRevision:
    with session.no_autoflush:
        result = await session.execute(
            select(DrawingRevision)
            .where(
                DrawingRevision.project_id == project_id,
                DrawingRevision.source_file_id == source_file_id,
            )
            .order_by(DrawingRevision.revision_sequence.desc(), DrawingRevision.id.desc())
            .limit(1)
        )
    current_revision = result.scalar_one_or_none()
    if current_revision is None:
        raise ChangeSetApplyLoadError(
            f"Current revision for project {project_id} file {source_file_id} was not found."
        )
    return current_revision


async def _load_change_set_operations(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
    change_set_id: uuid.UUID,
) -> tuple[ChangeSetOperation, ...]:
    with session.no_autoflush:
        result = await session.execute(
            select(CadChangeOperation)
            .where(
                CadChangeOperation.project_id == project_id,
                CadChangeOperation.change_set_id == change_set_id,
            )
            .order_by(CadChangeOperation.sequence_index.asc(), CadChangeOperation.id.asc())
        )
    return tuple(_change_set_operation_from_model(operation) for operation in result.scalars())


def _change_set_operation_from_model(operation: CadChangeOperation) -> ChangeSetOperation:
    persisted_operation_json = operation.operation_json
    return ChangeSetOperation(
        operation_id=operation.id,
        sequence_index=operation.sequence_index,
        operation_type=operation.operation_type,
        operation_json=_extract_operation_payload(persisted_operation_json),
        target=ChangeSetOperationTarget(
            target_revision_entity_id=operation.target_revision_entity_id,
            entity_id=_extract_operation_target_entity_id(persisted_operation_json),
            expected_source_identity=operation.expected_source_identity,
            expected_source_hash=operation.expected_source_hash,
        ),
    )


def _extract_operation_payload(operation_json: object) -> dict[str, object]:
    if _is_persisted_operation_envelope(operation_json):
        envelope = cast(dict[str, object], operation_json)
        payload = envelope.get("payload")
        if isinstance(payload, dict):
            return copy.deepcopy(payload)
        raise ChangeSetApplyLoadError(
            "Persisted changeset operation envelope payload must be a JSON object."
        )
    if not isinstance(operation_json, dict):
        raise ChangeSetApplyLoadError(
            "Persisted changeset operation payload must be a JSON object."
        )
    return copy.deepcopy(operation_json)


def _extract_operation_target_entity_id(operation_json: object) -> str | None:
    if not _is_persisted_operation_envelope(operation_json):
        return None
    envelope = cast(dict[str, object], operation_json)
    target = envelope.get("target")
    if not isinstance(target, dict):
        return None
    entity_id = target.get("entity_id")
    return entity_id if isinstance(entity_id, str) else None


def _is_persisted_operation_envelope(operation_json: object) -> bool:
    return (
        isinstance(operation_json, dict)
        and "payload_version" in operation_json
        and "payload" in operation_json
    )


def _build_conflict_targets(
    operations: tuple[ChangeSetOperation, ...],
) -> tuple[ChangeSetApplyConflictTarget, ...]:
    return tuple(
        ChangeSetApplyConflictTarget(
            operation_id=operation.operation_id,
            sequence_index=operation.sequence_index,
            operation_type=operation.operation_type,
            target_revision_entity_id=operation.target.target_revision_entity_id,
            entity_id=operation.target.entity_id,
            expected_source_identity=operation.target.expected_source_identity,
            expected_source_hash=operation.target.expected_source_hash,
        )
        for operation in operations
    )


async def _load_revision_entities(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
    drawing_revision_id: uuid.UUID,
) -> tuple[RevisionEntitySnapshot, ...]:
    with session.no_autoflush:
        result = await session.execute(
            select(RevisionEntity)
            .where(
                RevisionEntity.project_id == project_id,
                RevisionEntity.drawing_revision_id == drawing_revision_id,
            )
            .order_by(RevisionEntity.sequence_index.asc(), RevisionEntity.id.asc())
        )
    return tuple(_revision_entity_snapshot_from_model(entity) for entity in result.scalars())


def _confidence_score_from_json(confidence_json: object) -> float | None:
    """Best-effort per-entity confidence score from the canonical confidence payload."""
    if isinstance(confidence_json, dict):
        score = confidence_json.get("score")
        if isinstance(score, int | float) and not isinstance(score, bool):
            return float(score)
    return None


def _revision_entity_snapshot_from_model(entity: RevisionEntity) -> RevisionEntitySnapshot:
    return RevisionEntitySnapshot(
        id=entity.id,
        sequence_index=entity.sequence_index,
        entity_id=entity.entity_id,
        entity_type=entity.entity_type,
        entity_schema_version=entity.entity_schema_version,
        confidence_score=_confidence_score_from_json(entity.confidence_json),
        confidence_json=copy.deepcopy(entity.confidence_json),
        geometry_json=copy.deepcopy(entity.geometry_json),
        properties_json=copy.deepcopy(entity.properties_json),
        provenance_json=copy.deepcopy(entity.provenance_json),
        parent_entity_ref=entity.parent_entity_ref,
        canonical_entity_json=copy.deepcopy(entity.canonical_entity_json),
        layout_ref=entity.layout_ref,
        layer_ref=entity.layer_ref,
        block_ref=entity.block_ref,
        source_identity=entity.source_identity,
        source_hash=entity.source_hash,
    )


def _normalize_status(value: str) -> str:
    return value.strip().lower()


__all__ = [
    "ChangeSetApplyLoadError",
    "LoadedChangeSetApplyInput",
    "load_and_apply_change_set",
    "load_change_set_apply_input",
]
