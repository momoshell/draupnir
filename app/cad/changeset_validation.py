"""Transaction-neutral CAD changeset validation service."""

from __future__ import annotations

import uuid
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from math import isfinite
from numbers import Real
from typing import Any, Literal, cast

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.cad.changesets import (
    CadChangeSetValidationResultCreate,
    append_change_set_validation_result,
    list_change_set_operations,
    update_change_set_status,
)
from app.ingestion.validation import _has_valid_polygon_area_geometry
from app.models.cad_changeset import (
    CadChangeOperation,
    CadChangeSet,
    CadChangeSetValidationResult,
)

_ALLOWED_START_STATUSES = frozenset({"proposed", "validation_requested", "validation_failed"})
_VALIDATED_CHANGE_SET_STATUS = "validated"
_VALIDATION_FAILED_CHANGE_SET_STATUS = "validation_failed"
_VALIDATOR_NAME = "changeset_validation_service"
_VALIDATOR_VERSION = "1"
_RESULT_SCHEMA_VERSION = "changeset-validation-result-v1"
_UPDATE_PROPERTY_WHITELIST = frozenset(
    {
        "properties.description",
        "properties.label",
        "properties.mark",
        "properties.notes",
        "properties.review_status",
        "properties.metadata",
    }
)

ValidationSeverity = Literal["error", "warning", "review"]
ValidationStatus = Literal["valid", "valid_with_warnings", "invalid", "needs_review"]


class ChangeSetValidationErrorCode(StrEnum):
    CHANGE_SET_NOT_FOUND = "change_set_not_found"
    BASE_REVISION_NOT_FOUND = "base_revision_not_found"
    MATERIALIZATION_MANIFEST_NOT_FOUND = "materialization_manifest_not_found"
    INVALID_CHANGE_SET_STATUS = "invalid_change_set_status"


class ChangeSetValidationServiceError(RuntimeError):
    def __init__(
        self,
        code: ChangeSetValidationErrorCode,
        message: str,
    ) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class _ValidationFinding:
    severity: ValidationSeverity
    code: str
    message: str
    operation_id: uuid.UUID
    sequence_index: int
    details: Mapping[str, Any]

    def as_json(self) -> dict[str, Any]:
        return {
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "operation_id": str(self.operation_id),
            "sequence_index": self.sequence_index,
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class _ValidationContext:
    change_set: CadChangeSet
    operations: tuple[CadChangeOperation, ...]


@dataclass(frozen=True, slots=True)
class _ResolvedRevisionEntity:
    id: uuid.UUID
    entity_id: str
    source_identity: str | None
    source_hash: str | None
    layer_ref: str | None
    layout_ref: str | None
    block_ref: str | None


@dataclass(slots=True)
class _ValidationState:
    removed_entity_ids: set[uuid.UUID]


async def validate_change_set(
    db: AsyncSession,
    change_set_id: uuid.UUID,
) -> CadChangeSetValidationResult:
    context = await _load_validation_context(db, change_set_id)
    change_set = context.change_set

    if change_set.status not in _ALLOWED_START_STATUSES:
        allowed_statuses = ", ".join(sorted(_ALLOWED_START_STATUSES))
        raise ChangeSetValidationServiceError(
            ChangeSetValidationErrorCode.INVALID_CHANGE_SET_STATUS,
            "Changeset status is not eligible for validation: "
            f"{change_set.status!r}. Expected one of {allowed_statuses}.",
        )

    if not await _base_revision_exists(db, change_set.project_id, change_set.base_revision_id):
        raise ChangeSetValidationServiceError(
            ChangeSetValidationErrorCode.BASE_REVISION_NOT_FOUND,
            f"Base revision {change_set.base_revision_id} was not found.",
        )

    if not await _materialization_manifest_exists(
        db,
        change_set.project_id,
        change_set.base_revision_id,
    ):
        raise ChangeSetValidationServiceError(
            ChangeSetValidationErrorCode.MATERIALIZATION_MANIFEST_NOT_FOUND,
            f"Revision {change_set.base_revision_id} has no materialization manifest.",
        )

    findings = await _validate_operations(db, context)
    validation_status = _derive_validation_status(findings)
    result_json = _build_result_json(change_set, validation_status, findings)
    validation_result = await append_change_set_validation_result(
        db,
        project_id=change_set.project_id,
        change_set_id=change_set.id,
        validation_result=CadChangeSetValidationResultCreate(
            validation_status=validation_status,
            result_json=result_json,
            validator_name=_VALIDATOR_NAME,
            validator_version=_VALIDATOR_VERSION,
        ),
    )
    await update_change_set_status(
        db,
        project_id=change_set.project_id,
        change_set_id=change_set.id,
        status=(
            _VALIDATED_CHANGE_SET_STATUS
            if validation_status in {"valid", "valid_with_warnings"}
            else _VALIDATION_FAILED_CHANGE_SET_STATUS
        ),
    )
    return validation_result


async def _load_validation_context(
    db: AsyncSession,
    change_set_id: uuid.UUID,
) -> _ValidationContext:
    result = await db.execute(select(CadChangeSet).where(CadChangeSet.id == change_set_id))
    change_set = result.scalar_one_or_none()
    if change_set is None:
        raise ChangeSetValidationServiceError(
            ChangeSetValidationErrorCode.CHANGE_SET_NOT_FOUND,
            f"Changeset {change_set_id} was not found.",
        )
    operations = await list_change_set_operations(
        db,
        project_id=change_set.project_id,
        change_set_id=change_set.id,
    )
    return _ValidationContext(change_set=change_set, operations=operations)


async def _validate_operations(
    db: AsyncSession,
    context: _ValidationContext,
) -> list[_ValidationFinding]:
    findings: list[_ValidationFinding] = []
    state = _ValidationState(removed_entity_ids=set())
    for operation in context.operations:
        findings.extend(
            await _validate_operation(
                db,
                context.change_set,
                operation,
                state,
            )
        )
    return findings


async def _validate_operation(
    db: AsyncSession,
    change_set: CadChangeSet,
    operation: CadChangeOperation,
    state: _ValidationState,
) -> list[_ValidationFinding]:
    findings = _validate_operation_envelope(operation)
    if findings:
        return findings

    payload = cast(Mapping[str, Any], operation.operation_json["payload"])
    operation_type = operation.operation_type

    if operation_type == "annotate_entity":
        target_entity, target_findings = await _resolve_target_revision_entity(
            db,
            change_set,
            operation,
            operation_type,
            state,
        )
        findings.extend(target_findings)
        annotation = payload.get("annotation")
        if annotation is None or not isinstance(annotation, (str, Mapping)):
            findings.append(
                _finding(
                    operation,
                    severity="error",
                    code="invalid_annotation_payload",
                    message="annotate_entity requires a string or object annotation payload.",
                    details={"payload_key": "annotation"},
                )
            )
        findings.append(
            _finding(
                operation,
                severity="warning",
                code="metadata_only_operation",
                message="annotate_entity is metadata-only and should be reviewed before apply.",
                details={"operation_type": operation_type},
            )
        )
        return findings

    if operation_type == "change_layer":
        _, target_findings = await _resolve_target_revision_entity(
            db,
            change_set,
            operation,
            operation_type,
            state,
        )
        findings.extend(target_findings)
        layer_ref = _extract_text_reference(
            payload,
            keys=("layer_ref", "layer", "layer_name", "new_layer"),
        )
        if layer_ref is None:
            findings.append(
                _finding(
                    operation,
                    severity="error",
                    code="invalid_layer_ref",
                    message="change_layer requires a non-empty payload.layer_ref string.",
                    details={"payload_key": "layer_ref"},
                )
            )
            return findings
        if not await _layer_exists(
            db,
            change_set.project_id,
            change_set.base_revision_id,
            layer_ref,
        ):
            findings.append(
                _finding(
                    operation,
                    severity="error",
                    code="layer_not_found",
                    message=(
                        "change_layer references a layer that does not exist on the base revision."
                    ),
                    details={"layer_ref": layer_ref},
                )
            )
        return findings

    if operation_type == "add_entity":
        entity = _extract_add_entity_candidate(payload)
        geometry = _extract_geometry_candidate(payload, entity)
        raw_geometry = payload.get("geometry")
        if entity is None and geometry is None:
            if raw_geometry is not None:
                findings.append(
                    _finding(
                        operation,
                        severity="error",
                        code="invalid_geometry_payload",
                        message="add_entity requires geometry to be a JSON object.",
                        details={"payload_key": "geometry"},
                    )
                )
                return findings
            findings.append(
                _finding(
                    operation,
                    severity="error",
                    code="invalid_entity_payload",
                    message=(
                        "add_entity requires payload.entity, payload.canonical_entity, or "
                        "payload.geometry."
                    ),
                    details={"payload_keys": ["entity", "canonical_entity", "geometry"]},
                )
            )
            return findings

        if geometry is None or not isinstance(geometry, Mapping):
            findings.append(
                _finding(
                    operation,
                    severity="error",
                    code="invalid_geometry_payload",
                    message="add_entity requires geometry to be a JSON object.",
                    details={"payload_key": "geometry"},
                )
            )

        if geometry is not None and not _geometry_coordinate_leaves_are_valid(geometry):
            findings.append(
                _finding(
                    operation,
                    severity="error",
                    code="nonfinite_geometry_coordinate",
                    message="add_entity geometry must contain only finite numeric coordinates.",
                    details={},
                )
            )

        polygon_candidate = entity if entity is not None else geometry
        if polygon_candidate is not None and _looks_like_polygon_area_candidate(polygon_candidate):
            polygon_rings = _extract_polygon_area_rings(polygon_candidate, geometry)
            if polygon_rings is None or not all(
                _has_valid_polygon_area_geometry(ring) for ring in polygon_rings
            ):
                findings.append(
                    _finding(
                        operation,
                        severity="error",
                        code="invalid_polygon_area_geometry",
                        message="add_entity polygon area geometry is invalid.",
                        details={"entity_type": _polygon_candidate_entity_type(polygon_candidate)},
                    )
                )

        for field_name, reference_value, exists_fn in (
            (
                "layer_ref",
                _extract_text_reference(entity or payload, keys=("layer_ref",)),
                _layer_exists,
            ),
            (
                "layout_ref",
                _extract_text_reference(entity or payload, keys=("layout_ref",)),
                _layout_exists,
            ),
            (
                "block_ref",
                _extract_text_reference(entity or payload, keys=("block_ref",)),
                _block_exists,
            ),
        ):
            if reference_value is None:
                continue
            if not await exists_fn(
                db,
                change_set.project_id,
                change_set.base_revision_id,
                reference_value,
            ):
                findings.append(
                    _finding(
                        operation,
                        severity="error",
                        code=f"{field_name}_not_found",
                        message=(
                            f"add_entity references {field_name} {reference_value!r} that does "
                            "not exist on the base revision."
                        ),
                        details={field_name: reference_value},
                    )
                )

        return findings

    if operation_type == "remove_entity":
        target_entity, target_findings = await _resolve_target_revision_entity(
            db,
            change_set,
            operation,
            operation_type,
            state,
        )
        findings.extend(target_findings)
        if target_entity is not None and not _has_error_findings(target_findings):
            state.removed_entity_ids.add(target_entity.id)
        return findings

    if operation_type == "replace_block":
        target_entity, target_findings = await _resolve_target_revision_entity(
            db,
            change_set,
            operation,
            operation_type,
            state,
            required=False,
        )
        findings.extend(target_findings)

        if target_entity is not None and target_entity.block_ref is None:
            findings.append(
                _finding(
                    operation,
                    severity="error",
                    code="target_entity_has_no_block_ref",
                    message=(
                        "replace_block target entity is not associated with a base-revision block."
                    ),
                    details={"target_revision_entity_id": str(target_entity.id)},
                )
            )

        block_ref = _extract_block_reference(payload)
        if block_ref is None:
            findings.append(
                _finding(
                    operation,
                    severity="error",
                    code="invalid_block_ref",
                    message="replace_block requires a non-empty payload.block_ref string.",
                    details={"payload_key": "block_ref"},
                )
            )
        elif not await _block_exists(
            db,
            change_set.project_id,
            change_set.base_revision_id,
            block_ref,
        ):
            findings.append(
                _finding(
                    operation,
                    severity="error",
                    code="block_not_found",
                    message=(
                        "replace_block references a block that does not exist on the base revision."
                    ),
                    details={"block_ref": block_ref},
                )
            )
        else:
            findings.append(
                _finding(
                    operation,
                    severity="review",
                    code="operation_requires_review",
                    message="replace_block requires manual review before apply.",
                    details={"operation_type": operation_type},
                )
            )
        return findings

    if operation_type == "replace_profile_material_candidate":
        _, target_findings = await _resolve_target_revision_entity(
            db,
            change_set,
            operation,
            operation_type,
            state,
            required=False,
        )
        findings.extend(target_findings)
        candidate = payload.get("candidate")
        if not isinstance(candidate, Mapping) or not candidate:
            findings.append(
                _finding(
                    operation,
                    severity="error",
                    code="invalid_candidate_payload",
                    message=(
                        "replace_profile_material_candidate requires a non-empty "
                        "payload.candidate object."
                    ),
                    details={"payload_key": "candidate"},
                )
            )
        else:
            findings.append(
                _finding(
                    operation,
                    severity="review",
                    code="profile_material_review_required",
                    message=(
                        "replace_profile_material_candidate requires manual review before apply."
                    ),
                    details={"operation_type": operation_type},
                )
            )
        return findings

    if operation_type == "update_property":
        _, target_findings = await _resolve_target_revision_entity(
            db,
            change_set,
            operation,
            operation_type,
            state,
            required=False,
        )
        findings.extend(target_findings)
        property_path = _extract_text_reference(
            payload,
            keys=("property", "property_name", "property_key", "path", "name"),
        )
        if not isinstance(property_path, str) or not property_path.strip():
            findings.append(
                _finding(
                    operation,
                    severity="error",
                    code="invalid_property_path",
                    message="update_property requires a non-empty payload.path string.",
                    details={"payload_key": "path"},
                )
            )
            return findings
        if property_path not in _UPDATE_PROPERTY_WHITELIST:
            findings.append(
                _finding(
                    operation,
                    severity="error",
                    code="disallowed_property_path",
                    message="update_property path is not allowed.",
                    details={"path": property_path},
                )
            )
            return findings
        if "value" not in payload and "new_value" not in payload:
            findings.append(
                _finding(
                    operation,
                    severity="error",
                    code="missing_property_value",
                    message="update_property requires payload.value or payload.new_value.",
                    details={"payload_keys": ["value", "new_value"]},
                )
            )
        return findings

    if operation_type == "flag_for_review":
        _, target_findings = await _resolve_target_revision_entity(
            db,
            change_set,
            operation,
            operation_type,
            state,
            required=False,
        )
        findings.extend(target_findings)
        review_reason = payload.get("reason")
        if not isinstance(review_reason, str) or not review_reason.strip():
            findings.append(
                _finding(
                    operation,
                    severity="error",
                    code="invalid_review_reason",
                    message="flag_for_review requires a non-empty payload.reason string.",
                    details={"payload_key": "reason"},
                )
            )
        else:
            findings.append(
                _finding(
                    operation,
                    severity="review",
                    code="manual_review_flagged",
                    message="flag_for_review marks the changeset for manual review.",
                    details={"operation_type": operation_type},
                )
            )
        return findings

    findings.append(
        _finding(
            operation,
            severity="error",
            code="unsupported_operation_type",
            message=f"Unsupported changeset operation type {operation_type!r}.",
            details={"operation_type": operation_type},
        )
    )
    return findings


def _validate_operation_envelope(operation: CadChangeOperation) -> list[_ValidationFinding]:
    findings: list[_ValidationFinding] = []
    operation_json = operation.operation_json
    if not isinstance(operation_json, Mapping):
        return [
            _finding(
                operation,
                severity="error",
                code="invalid_operation_envelope",
                message="Operation payload must be a JSON object.",
                details={"field": "operation_json"},
            )
        ]

    payload_version = operation_json.get("payload_version")
    if payload_version != 1:
        findings.append(
            _finding(
                operation,
                severity="error",
                code="unsupported_payload_version",
                message="Changeset operation payload_version must be 1.",
                details={"payload_version": payload_version},
            )
        )

    target = operation_json.get("target")
    if target is not None and not isinstance(target, Mapping):
        findings.append(
            _finding(
                operation,
                severity="error",
                code="invalid_operation_envelope",
                message="Changeset operation target must be a JSON object.",
                details={"field": "target"},
            )
        )

    if "payload" not in operation_json or not isinstance(operation_json.get("payload"), Mapping):
        findings.append(
            _finding(
                operation,
                severity="error",
                code="invalid_operation_envelope",
                message="Changeset operation payload must be a JSON object.",
                details={"field": "payload"},
            )
        )

    reason = operation_json.get("reason")
    if reason is not None and not isinstance(reason, str):
        findings.append(
            _finding(
                operation,
                severity="error",
                code="invalid_operation_envelope",
                message="Changeset operation reason must be a string when present.",
                details={"field": "reason"},
            )
        )

    provenance = operation_json.get("provenance")
    if provenance is not None and not isinstance(provenance, Mapping):
        findings.append(
            _finding(
                operation,
                severity="error",
                code="invalid_operation_envelope",
                message="Changeset operation provenance must be an object when present.",
                details={"field": "provenance"},
            )
        )

    return findings


async def _resolve_target_revision_entity(
    db: AsyncSession,
    change_set: CadChangeSet,
    operation: CadChangeOperation,
    operation_type: str,
    state: _ValidationState,
    *,
    required: bool = True,
) -> tuple[_ResolvedRevisionEntity | None, list[_ValidationFinding]]:
    if operation.target_revision_entity_id is None:
        if not required:
            return None, []
        return None, [
            _finding(
                operation,
                severity="error",
                code="missing_target_revision_entity",
                message=f"{operation_type} requires target_revision_entity_id.",
                details={"operation_type": operation_type},
            )
        ]

    entity = await _load_revision_entity(
        db,
        change_set.project_id,
        change_set.base_revision_id,
        operation.target_revision_entity_id,
    )
    if entity is None:
        return None, [
            _finding(
                operation,
                severity="error",
                code="target_revision_entity_not_found",
                message="target_revision_entity_id was not found on the base revision.",
                details={"target_revision_entity_id": str(operation.target_revision_entity_id)},
            )
        ]

    findings: list[_ValidationFinding] = []
    if entity.id in state.removed_entity_ids:
        findings.append(
            _finding(
                operation,
                severity="error",
                code="target_removed_by_prior_operation",
                message="target_revision_entity_id was removed by an earlier operation.",
                details={"target_revision_entity_id": str(entity.id)},
            )
        )

    if (
        operation.expected_source_identity is not None
        and operation.expected_source_identity != entity.source_identity
    ):
        findings.append(
            _finding(
                operation,
                severity="error",
                code="expected_source_identity_mismatch",
                message="expected_source_identity does not match the base revision target entity.",
                details={
                    "expected_source_identity": operation.expected_source_identity,
                    "actual_source_identity": entity.source_identity,
                },
            )
        )

    if (
        operation.expected_source_hash is not None
        and operation.expected_source_hash != entity.source_hash
    ):
        findings.append(
            _finding(
                operation,
                severity="error",
                code="expected_source_hash_mismatch",
                message="expected_source_hash does not match the base revision target entity.",
                details={
                    "expected_source_hash": operation.expected_source_hash,
                    "actual_source_hash": entity.source_hash,
                },
            )
        )

    return entity, findings


def _derive_validation_status(findings: Sequence[_ValidationFinding]) -> ValidationStatus:
    severity_counts = Counter(finding.severity for finding in findings)
    if severity_counts["error"]:
        return "invalid"
    if severity_counts["review"]:
        return "needs_review"
    if severity_counts["warning"]:
        return "valid_with_warnings"
    return "valid"


def _build_result_json(
    change_set: CadChangeSet,
    validation_status: ValidationStatus,
    findings: Sequence[_ValidationFinding],
) -> dict[str, Any]:
    severity_counts = Counter(finding.severity for finding in findings)
    return {
        "schema_version": _RESULT_SCHEMA_VERSION,
        "change_set_id": str(change_set.id),
        "base_revision_id": str(change_set.base_revision_id),
        "status": validation_status,
        "findings": [finding.as_json() for finding in findings],
        "summary": {
            "total_findings": len(findings),
            "error_count": severity_counts["error"],
            "warning_count": severity_counts["warning"],
            "review_count": severity_counts["review"],
        },
    }


def _finding(
    operation: CadChangeOperation,
    *,
    severity: ValidationSeverity,
    code: str,
    message: str,
    details: Mapping[str, Any],
) -> _ValidationFinding:
    return _ValidationFinding(
        severity=severity,
        code=code,
        message=message,
        operation_id=operation.id,
        sequence_index=operation.sequence_index,
        details=dict(details),
    )


async def _base_revision_exists(
    db: AsyncSession,
    project_id: uuid.UUID,
    base_revision_id: uuid.UUID,
) -> bool:
    result = await db.execute(
        text(
            """
            SELECT 1
            FROM drawing_revisions
            WHERE project_id = :project_id
              AND id = :base_revision_id
            """
        ),
        {"project_id": project_id, "base_revision_id": base_revision_id},
    )
    return result.scalar_one_or_none() == 1


async def _materialization_manifest_exists(
    db: AsyncSession,
    project_id: uuid.UUID,
    drawing_revision_id: uuid.UUID,
) -> bool:
    result = await db.execute(
        text(
            """
            SELECT 1
            FROM revision_entity_manifests
            WHERE project_id = :project_id
              AND drawing_revision_id = :drawing_revision_id
            """
        ),
        {"project_id": project_id, "drawing_revision_id": drawing_revision_id},
    )
    return result.scalar_one_or_none() == 1


async def _revision_entity_exists(
    db: AsyncSession,
    project_id: uuid.UUID,
    drawing_revision_id: uuid.UUID,
    revision_entity_id: uuid.UUID,
) -> bool:
    result = await db.execute(
        text(
            """
            SELECT 1
            FROM revision_entities
            WHERE project_id = :project_id
              AND drawing_revision_id = :drawing_revision_id
              AND id = :revision_entity_id
            """
        ),
        {
            "project_id": project_id,
            "drawing_revision_id": drawing_revision_id,
            "revision_entity_id": revision_entity_id,
        },
    )
    return result.scalar_one_or_none() == 1


async def _load_revision_entity(
    db: AsyncSession,
    project_id: uuid.UUID,
    drawing_revision_id: uuid.UUID,
    revision_entity_id: uuid.UUID,
) -> _ResolvedRevisionEntity | None:
    result = await db.execute(
        text(
            """
            SELECT id, entity_id, source_identity, source_hash, layer_ref, layout_ref, block_ref
            FROM revision_entities
            WHERE project_id = :project_id
              AND drawing_revision_id = :drawing_revision_id
              AND id = :revision_entity_id
            """
        ),
        {
            "project_id": project_id,
            "drawing_revision_id": drawing_revision_id,
            "revision_entity_id": revision_entity_id,
        },
    )
    row = result.mappings().one_or_none()
    if row is None:
        return None
    return _ResolvedRevisionEntity(
        id=row["id"],
        entity_id=row["entity_id"],
        source_identity=row["source_identity"],
        source_hash=row["source_hash"],
        layer_ref=row["layer_ref"],
        layout_ref=row["layout_ref"],
        block_ref=row["block_ref"],
    )


async def _layer_exists(
    db: AsyncSession,
    project_id: uuid.UUID,
    drawing_revision_id: uuid.UUID,
    layer_ref: str,
) -> bool:
    result = await db.execute(
        text(
            """
            SELECT 1
            FROM revision_layers
            WHERE project_id = :project_id
              AND drawing_revision_id = :drawing_revision_id
              AND layer_ref = :layer_ref
            """
        ),
        {
            "project_id": project_id,
            "drawing_revision_id": drawing_revision_id,
            "layer_ref": layer_ref,
        },
    )
    return result.scalar_one_or_none() == 1


async def _layout_exists(
    db: AsyncSession,
    project_id: uuid.UUID,
    drawing_revision_id: uuid.UUID,
    layout_ref: str,
) -> bool:
    result = await db.execute(
        text(
            """
            SELECT 1
            FROM revision_layouts
            WHERE project_id = :project_id
              AND drawing_revision_id = :drawing_revision_id
              AND layout_ref = :layout_ref
            """
        ),
        {
            "project_id": project_id,
            "drawing_revision_id": drawing_revision_id,
            "layout_ref": layout_ref,
        },
    )
    return result.scalar_one_or_none() == 1


async def _block_exists(
    db: AsyncSession,
    project_id: uuid.UUID,
    drawing_revision_id: uuid.UUID,
    block_ref: str,
) -> bool:
    result = await db.execute(
        text(
            """
            SELECT 1
            FROM revision_blocks
            WHERE project_id = :project_id
              AND drawing_revision_id = :drawing_revision_id
              AND block_ref = :block_ref
            """
        ),
        {
            "project_id": project_id,
            "drawing_revision_id": drawing_revision_id,
            "block_ref": block_ref,
        },
    )
    return result.scalar_one_or_none() == 1


def _extract_text_reference(
    payload: Mapping[str, Any],
    *,
    keys: Sequence[str],
) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str):
            normalized = value.strip()
            if normalized:
                return normalized
    return None


def _extract_block_reference(payload: Mapping[str, Any]) -> str | None:
    for key in ("block_ref", "block_name", "new_block_name"):
        value = _extract_text_reference(payload, keys=(key,))
        if value is not None:
            return value
    for key in ("block", "replacement"):
        value = payload.get(key)
        if isinstance(value, str):
            normalized = value.strip()
            if normalized:
                return normalized
        if isinstance(value, Mapping):
            nested = _extract_text_reference(
                value,
                keys=("block_ref", "block_name", "name", "ref"),
            )
            if nested is not None:
                return nested
    return None


def _extract_add_entity_candidate(payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
    for key in ("entity", "canonical_entity"):
        value = payload.get(key)
        if isinstance(value, Mapping):
            return value
    return None


def _extract_geometry_candidate(
    payload: Mapping[str, Any],
    entity: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    if entity is not None and isinstance(entity.get("geometry"), Mapping):
        return cast(Mapping[str, Any], entity["geometry"])
    geometry = payload.get("geometry")
    if isinstance(geometry, Mapping):
        return geometry
    return None


def _geometry_coordinate_leaves_are_valid(value: Any, *, coordinate_context: bool = False) -> bool:
    if isinstance(value, Mapping):
        if any(axis_key in value for axis_key in ("x", "y", "z")):
            for axis_key in ("x", "y", "z"):
                if axis_key in value and not _is_valid_coordinate_leaf(value[axis_key]):
                    return False
        for key, item in value.items():
            next_coordinate_context = coordinate_context or key in {
                "coordinates",
                "points",
                "vertices",
                "start",
                "end",
                "center",
                "midpoint",
                "origin",
                "rings",
                "outer",
                "inner",
            }
            if not _geometry_coordinate_leaves_are_valid(
                item,
                coordinate_context=next_coordinate_context,
            ):
                return False
        return True
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return all(
            _geometry_coordinate_leaves_are_valid(item, coordinate_context=coordinate_context)
            for item in value
        )
    if coordinate_context:
        return _is_valid_coordinate_leaf(value)
    return True


def _is_valid_coordinate_leaf(value: Any) -> bool:
    return isinstance(value, Real) and not isinstance(value, bool) and isfinite(float(value))


def _has_error_findings(findings: Sequence[_ValidationFinding]) -> bool:
    return any(finding.severity == "error" for finding in findings)


def _looks_like_polygon_area_candidate(candidate: Mapping[str, Any]) -> bool:
    entity_type = candidate.get("entity_type")
    candidate_type = candidate.get("type")
    geometry = candidate.get("geometry")
    geometry_type = geometry.get("type") if isinstance(geometry, Mapping) else None
    return (
        entity_type == "polygon_area"
        or candidate_type == "polygon_area"
        or geometry_type == "polygon_area"
    )


def _extract_polygon_area_rings(
    candidate: Mapping[str, Any] | None,
    geometry: Mapping[str, Any] | None,
) -> tuple[Sequence[Any], ...] | None:
    if candidate is None or not _looks_like_polygon_area_candidate(candidate):
        return None
    source = geometry if geometry is not None else candidate.get("geometry")
    if not isinstance(source, Mapping):
        return None
    rings = source.get("rings")
    if rings is not None:
        if not isinstance(rings, Sequence) or isinstance(rings, (str, bytes, bytearray)):
            return None
        extracted_rings = tuple(rings)
        if not extracted_rings:
            return None
        if not all(_is_polygon_area_ring(ring) for ring in extracted_rings):
            return None
        return cast(tuple[Sequence[Any], ...], extracted_rings)
    for key in ("points", "vertices", "outer"):
        if key not in source:
            continue
        ring = source.get(key)
        if _is_polygon_area_ring(ring):
            return (cast(Sequence[Any], ring),)
        return None
    return None


def _is_polygon_area_ring(value: Any) -> bool:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return False
    ring = tuple(value)
    if not ring:
        return False
    return all(_is_polygon_area_point(point) for point in ring)


def _is_polygon_area_point(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    if "x" not in value or "y" not in value:
        return False
    return _is_valid_coordinate_leaf(value["x"]) and _is_valid_coordinate_leaf(value["y"])


def _polygon_candidate_entity_type(candidate: Mapping[str, Any] | None) -> Any:
    if candidate is None:
        return None
    entity_type = candidate.get("entity_type")
    if entity_type is not None:
        return entity_type
    candidate_type = candidate.get("type")
    if candidate_type is not None:
        return candidate_type
    geometry = candidate.get("geometry")
    if isinstance(geometry, Mapping):
        return geometry.get("type")
    return None


__all__ = [
    "ChangeSetValidationErrorCode",
    "ChangeSetValidationServiceError",
    "validate_change_set",
]
