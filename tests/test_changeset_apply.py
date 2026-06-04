from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.cad.changeset import (
    AppliedEntity,
    ChangeSetApplyConflict,
    ChangeSetApplyConflictDetails,
    ChangeSetApplyConflictTarget,
    ChangeSetApplyError,
    ChangeSetApplyLoadError,
    ChangeSetApplySuccess,
    ChangeSetOperation,
    ChangeSetOperationTarget,
    LoadedChangeSetApplyInput,
    RevisionEntitySnapshot,
    RevisionRef,
    apply_change_set,
    build_stale_base_conflict,
    build_stale_base_conflict_details,
    load_and_apply_change_set,
    load_change_set_apply_input,
)
from app.cad.changeset.apply import ALLOWED_LAYER_KEYS
from app.core.errors import ErrorCode
from app.jobs import worker as worker_module
from app.models.adapter_run_output import AdapterRunOutput
from app.models.cad_changeset import (
    CadChangeOperation,
    CadChangeSet,
    CadChangeSetValidationResult,
)
from app.models.changeset_apply_job_input import ChangeSetApplyJobInput
from app.models.drawing_revision import DrawingRevision
from app.models.extraction_profile import ExtractionProfile
from app.models.file import File
from app.models.job import Job, JobType
from app.models.project import Project
from app.models.revision_materialization import (
    RevisionBlock,
    RevisionEntity,
    RevisionEntityManifest,
    RevisionLayer,
    RevisionLayout,
)
from app.models.validation_report import ValidationReport
from tests.conftest import requires_database

_DEFAULT_CHANGESET_APPLY_JOB_ID = uuid.UUID("90000000-0000-0000-0000-000000000100")


def test_build_stale_base_conflict_details_returns_none_for_matching_current_revision() -> None:
    revision_id = uuid.UUID("11111111-1111-1111-1111-111111111111")
    base_revision = RevisionRef(revision_id=revision_id, revision_sequence=7)
    current_revision = RevisionRef(revision_id=revision_id, revision_sequence=7)

    details = build_stale_base_conflict_details(
        change_set_id=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        base_revision=base_revision,
        current_revision=current_revision,
        conflicting_targets=(
            ChangeSetApplyConflictTarget(
                operation_id=uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
                sequence_index=1,
                operation_type="change_layer",
                entity_id="entity-1",
            ),
        ),
    )

    assert details is None


def test_build_stale_base_conflict_details_returns_expected_payload_for_stale_base() -> None:
    change_set_id = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    base_revision = RevisionRef(
        revision_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        revision_sequence=7,
    )
    current_revision = RevisionRef(
        revision_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        revision_sequence=9,
    )
    conflict_target = ChangeSetApplyConflictTarget(
        operation_id=uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        sequence_index=2,
        operation_type="update_property",
        target_revision_entity_id=uuid.UUID("33333333-3333-3333-3333-333333333333"),
        entity_id="entity-7",
        expected_source_identity="wall:entity-7",
        expected_source_hash="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    )

    details = build_stale_base_conflict_details(
        change_set_id=change_set_id,
        base_revision=base_revision,
        current_revision=current_revision,
        conflicting_targets=[conflict_target],
    )
    conflict = build_stale_base_conflict(
        change_set_id=change_set_id,
        base_revision=base_revision,
        current_revision=current_revision,
        conflicting_targets=[conflict_target],
    )

    assert details == ChangeSetApplyConflictDetails(
        base_revision_id=base_revision.revision_id,
        base_revision_sequence=base_revision.revision_sequence,
        current_revision_id=current_revision.revision_id,
        current_revision_sequence=current_revision.revision_sequence,
        change_set_id=change_set_id,
        conflicting_targets=(conflict_target,),
    )
    assert conflict == ChangeSetApplyConflict(details=details)


def test_apply_change_set_returns_copy_on_write_entities_and_effects() -> None:
    change_set_id = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    base_revision = RevisionRef(
        revision_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        revision_sequence=7,
    )
    current_revision = RevisionRef(
        revision_id=base_revision.revision_id,
        revision_sequence=base_revision.revision_sequence,
    )
    wall = _entity_snapshot(
        revision_entity_id=uuid.UUID("10000000-0000-0000-0000-000000000001"),
        sequence_index=1,
        entity_id="wall-1",
        layer_ref="A-WALL",
        properties_json={
            "description": "Wall",
            "layer_name": "A-WALL",
            "metadata": {"tags": ["base"]},
        },
        canonical_entity_json={
            "layer_name": "A-WALL",
            "properties": {"description": "Wall", "layer": "A-WALL"},
        },
        source_identity="wall:1",
        source_hash="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    )
    door = _entity_snapshot(
        revision_entity_id=uuid.UUID("10000000-0000-0000-0000-000000000002"),
        sequence_index=2,
        entity_id="door-1",
        layer_ref="A-DOOR",
        properties_json={"label": "Door A"},
        canonical_entity_json={"properties": {"label": "Door A", "notes": "stale"}},
        source_identity="door:1",
        source_hash="fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
    )
    tag = _entity_snapshot(
        revision_entity_id=uuid.UUID("10000000-0000-0000-0000-000000000003"),
        sequence_index=3,
        entity_id="tag-1",
        layer_ref="A-TAG",
        properties_json={"notes": "keep"},
    )

    operations = (
        ChangeSetOperation(
            operation_id=uuid.UUID("20000000-0000-0000-0000-000000000001"),
            sequence_index=1,
            operation_type="change_layer",
            operation_json={"new_layer": "A-WALL-NEW"},
            target=ChangeSetOperationTarget(entity_id="wall-1"),
        ),
        ChangeSetOperation(
            operation_id=uuid.UUID("20000000-0000-0000-0000-000000000002"),
            sequence_index=2,
            operation_type="update_property",
            operation_json={"property_path": "properties.notes", "value": "inspect"},
            target=ChangeSetOperationTarget(target_revision_entity_id=door.id),
        ),
        ChangeSetOperation(
            operation_id=uuid.UUID("20000000-0000-0000-0000-000000000003"),
            sequence_index=3,
            operation_type="annotate_entity",
            operation_json={"annotation": {"code": "verify", "message": "check swing"}},
            target=ChangeSetOperationTarget(entity_id="door-1"),
        ),
        ChangeSetOperation(
            operation_id=uuid.UUID("20000000-0000-0000-0000-000000000004"),
            sequence_index=4,
            operation_type="remove_entity",
            operation_json={},
            target=ChangeSetOperationTarget(entity_id="tag-1"),
        ),
        ChangeSetOperation(
            operation_id=uuid.UUID("20000000-0000-0000-0000-000000000005"),
            sequence_index=5,
            operation_type="add_entity",
            operation_json={
                "entity": {
                    "entity_type": "text",
                    "entity_schema_version": "1",
                    "geometry_json": {"kind": "point", "x": 2.0, "y": 3.0},
                    "properties_json": {"label": "N-1"},
                    "provenance_json": {"origin": "user_created"},
                    "layer": "A-NOTE",
                }
            },
        ),
        ChangeSetOperation(
            operation_id=uuid.UUID("20000000-0000-0000-0000-000000000006"),
            sequence_index=6,
            operation_type="flag_for_review",
            operation_json={"review_flag": {"reason": "manual_check", "severity": "warning"}},
            target=ChangeSetOperationTarget(entity_id="door-1"),
        ),
    )

    result = apply_change_set(
        change_set_id=change_set_id,
        base_revision=base_revision,
        current_revision=current_revision,
        operations=operations,
        entities=(wall, door, tag),
    )

    assert isinstance(result, ChangeSetApplySuccess)
    assert [entity.entity_id for entity in result.entities] == [
        "wall-1",
        "door-1",
        _added_entity_id(change_set_id, operations[4].operation_id),
    ]
    assert [entity.sequence_index for entity in result.entities] == [0, 1, 2]
    assert wall.layer_ref == "A-WALL"
    assert door.properties_json == {"label": "Door A"}
    assert tag.sequence_index == 3

    changed_wall = result.entities[0]
    metadata_door = result.entities[1]
    added_note = result.entities[2]

    assert changed_wall.layer_ref == "A-WALL-NEW"
    assert changed_wall.properties_json == {
        "description": "Wall",
        "layer_name": "A-WALL-NEW",
        "metadata": {"tags": ["base"]},
    }
    assert changed_wall.canonical_entity_json == {
        "layer_name": "A-WALL-NEW",
        "properties": {"description": "Wall", "layer": "A-WALL-NEW"},
    }
    assert metadata_door.properties_json == {
        "label": "Door A",
        "notes": "inspect",
        "metadata": {
            "annotations": [{"code": "verify", "message": "check swing"}],
            "review_flags": [{"reason": "manual_check", "severity": "warning"}],
        },
    }
    assert metadata_door.canonical_entity_json == {
        "properties": {"label": "Door A", "notes": "inspect"}
    }
    assert added_note.layer_ref == "A-NOTE"
    assert added_note.id == _added_revision_entity_id(change_set_id, operations[4].operation_id)

    assert result.effects.changed_entities == (
        AppliedEntity(
            operation_id=operations[0].operation_id,
            effect="changed",
            entity=changed_wall,
        ),
    )
    assert result.effects.metadata_only_entities == (
        AppliedEntity(
            operation_id=operations[1].operation_id,
            effect="metadata_only",
            entity=metadata_door,
        ),
    )
    assert result.effects.added_entities == (
        AppliedEntity(
            operation_id=operations[4].operation_id,
            effect="added",
            entity=added_note,
        ),
    )
    assert result.effects.removed_entities == (
        AppliedEntity(
            operation_id=operations[3].operation_id,
            effect="removed",
            entity=tag,
        ),
    )
    assert result.effects.unchanged_entities == ()


def test_apply_change_set_marks_no_op_update_as_unchanged() -> None:
    revision = RevisionRef(
        revision_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        revision_sequence=7,
    )
    entity = _entity_snapshot(
        revision_entity_id=uuid.UUID("10000000-0000-0000-0000-000000000001"),
        sequence_index=1,
        entity_id="wall-1",
        properties_json={"notes": "keep"},
    )
    operation = ChangeSetOperation(
        operation_id=uuid.UUID("20000000-0000-0000-0000-000000000010"),
        sequence_index=1,
        operation_type="update_property",
        operation_json={"property": "properties.notes", "value": "keep"},
        target=ChangeSetOperationTarget(entity_id="wall-1"),
    )

    result = apply_change_set(
        change_set_id=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        base_revision=revision,
        current_revision=revision,
        operations=(operation,),
        entities=(entity,),
    )

    assert isinstance(result, ChangeSetApplySuccess)
    assert result.entities[0].sequence_index == 0
    assert result.entities[0].entity_id == entity.entity_id
    assert result.entities[0].properties_json == entity.properties_json
    assert result.effects.unchanged_entities == (
        AppliedEntity(
            operation_id=operation.operation_id,
            effect="unchanged",
            entity=result.entities[0],
        ),
    )


@pytest.mark.parametrize("layer_key", ALLOWED_LAYER_KEYS)
def test_apply_change_set_accepts_add_entity_alias_payloads(layer_key: str) -> None:
    change_set_id = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    revision = RevisionRef(
        revision_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        revision_sequence=7,
    )
    operation = ChangeSetOperation(
        operation_id=uuid.UUID("20000000-0000-0000-0000-000000000021"),
        sequence_index=1,
        operation_type="add_entity",
        operation_json={
            "entity": {
                "entity_type": "text",
                "geometry": {"kind": "point", "x": 2.0, "y": 3.0},
                "canonical_entity": {
                    "geometry": {"kind": "point", "x": 2.0, "y": 3.0},
                    "properties": {"label": "N-1"},
                },
                "properties_json": {"label": "N-1"},
                layer_key: "A-NOTE",
            }
        },
    )

    result = apply_change_set(
        change_set_id=change_set_id,
        base_revision=revision,
        current_revision=revision,
        operations=(operation,),
        entities=(),
    )

    assert isinstance(result, ChangeSetApplySuccess)
    added_entity = result.entities[0]
    assert added_entity.geometry_json == {"kind": "point", "x": 2.0, "y": 3.0}
    assert added_entity.canonical_entity_json == {
        "geometry": {"kind": "point", "x": 2.0, "y": 3.0},
        "properties": {"label": "N-1"},
    }
    assert added_entity.layer_ref == "A-NOTE"


def test_apply_change_set_accepts_top_level_canonical_entity_payload() -> None:
    change_set_id = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    revision = RevisionRef(
        revision_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        revision_sequence=7,
    )
    operation = ChangeSetOperation(
        operation_id=uuid.UUID("20000000-0000-0000-0000-000000000025"),
        sequence_index=1,
        operation_type="add_entity",
        operation_json={
            "canonical_entity": {
                "entity_type": "text",
                "geometry": {"kind": "point", "x": 2.0, "y": 3.0},
                "properties": {"label": "N-1"},
                "layer_name": "A-NOTE",
            }
        },
    )

    result = apply_change_set(
        change_set_id=change_set_id,
        base_revision=revision,
        current_revision=revision,
        operations=(operation,),
        entities=(),
    )

    assert isinstance(result, ChangeSetApplySuccess)
    added_entity = result.entities[0]
    assert added_entity.entity_type == "text"
    assert added_entity.geometry_json == {"kind": "point", "x": 2.0, "y": 3.0}
    assert added_entity.canonical_entity_json == {
        "entity_type": "text",
        "geometry": {"kind": "point", "x": 2.0, "y": 3.0},
        "properties": {"label": "N-1"},
        "layer_name": "A-NOTE",
    }
    assert added_entity.layer_ref == "A-NOTE"


def test_apply_change_set_accepts_top_level_geometry_alias_for_canonical_entity_payload() -> None:
    change_set_id = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    revision = RevisionRef(
        revision_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        revision_sequence=7,
    )
    operation = ChangeSetOperation(
        operation_id=uuid.UUID("20000000-0000-0000-0000-000000000026"),
        sequence_index=1,
        operation_type="add_entity",
        operation_json={
            "canonical_entity": {
                "entity_type": "text",
                "layer_ref": "A-NOTE",
            },
            "geometry": {"kind": "point", "x": 2.0, "y": 3.0},
        },
    )

    result = apply_change_set(
        change_set_id=change_set_id,
        base_revision=revision,
        current_revision=revision,
        operations=(operation,),
        entities=(),
    )

    assert isinstance(result, ChangeSetApplySuccess)
    added_entity = result.entities[0]
    assert added_entity.geometry_json == {"kind": "point", "x": 2.0, "y": 3.0}
    assert added_entity.canonical_entity_json == {
        "entity_type": "text",
        "layer_ref": "A-NOTE",
    }
    assert added_entity.layer_ref == "A-NOTE"


@pytest.mark.parametrize("property_alias", ["property_name", "property_key", "name"])
def test_apply_change_set_accepts_update_property_aliases(property_alias: str) -> None:
    revision = RevisionRef(
        revision_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        revision_sequence=7,
    )
    entity = _entity_snapshot(
        revision_entity_id=uuid.UUID("10000000-0000-0000-0000-000000000001"),
        sequence_index=1,
        entity_id="wall-1",
        properties_json={"notes": "keep"},
        canonical_entity_json={"properties": {"notes": "keep"}},
    )
    operation = ChangeSetOperation(
        operation_id=uuid.UUID("20000000-0000-0000-0000-000000000022"),
        sequence_index=1,
        operation_type="update_property",
        operation_json={property_alias: "notes", "new_value": "review"},
        target=ChangeSetOperationTarget(entity_id="wall-1"),
    )

    result = apply_change_set(
        change_set_id=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        base_revision=revision,
        current_revision=revision,
        operations=(operation,),
        entities=(entity,),
    )

    assert isinstance(result, ChangeSetApplySuccess)
    assert result.entities[0].properties_json == {"notes": "review"}
    assert result.entities[0].canonical_entity_json == {"properties": {"notes": "review"}}


def test_apply_change_set_derives_provenance_for_added_entity_without_input_provenance() -> None:
    change_set_id = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    operation_id = uuid.UUID("20000000-0000-0000-0000-000000000023")
    revision = RevisionRef(
        revision_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        revision_sequence=7,
    )
    operation = ChangeSetOperation(
        operation_id=operation_id,
        sequence_index=1,
        operation_type="add_entity",
        operation_json={
            "entity_type": "text",
            "geometry_json": {"kind": "point", "x": 2.0, "y": 3.0},
        },
    )

    result = apply_change_set(
        change_set_id=change_set_id,
        base_revision=revision,
        current_revision=revision,
        operations=(operation,),
        entities=(),
    )

    assert isinstance(result, ChangeSetApplySuccess)
    added_entity = result.entities[0]
    expected_source_identity = _added_entity_id(change_set_id, operation_id)
    expected_source_hash = hashlib.sha256(expected_source_identity.encode("utf-8")).hexdigest()
    assert added_entity.source_identity == expected_source_identity
    assert added_entity.source_hash == expected_source_hash
    assert added_entity.provenance_json == {
        "origin": "user_created",
        "adapter": "changeset",
        "source_identity": expected_source_identity,
        "source_hash": expected_source_hash,
    }


def test_apply_change_set_prevents_provenance_spoofing_for_added_entity() -> None:
    change_set_id = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    operation_id = uuid.UUID("20000000-0000-0000-0000-000000000024")
    revision = RevisionRef(
        revision_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        revision_sequence=7,
    )
    operation = ChangeSetOperation(
        operation_id=operation_id,
        sequence_index=1,
        operation_type="add_entity",
        operation_json={
            "entity_type": "text",
            "geometry_json": {"kind": "point", "x": 2.0, "y": 3.0},
            "source_identity": "spoofed:source",
            "source_hash": "f" * 64,
            "provenance_json": {
                "origin": "source_direct",
                "adapter": "spoofed_adapter",
                "source_identity": "spoofed:provenance",
                "source_hash": "e" * 64,
            },
        },
    )

    result = apply_change_set(
        change_set_id=change_set_id,
        base_revision=revision,
        current_revision=revision,
        operations=(operation,),
        entities=(),
    )

    assert isinstance(result, ChangeSetApplySuccess)
    added_entity = result.entities[0]
    expected_source_identity = _added_entity_id(change_set_id, operation_id)
    expected_source_hash = hashlib.sha256(expected_source_identity.encode("utf-8")).hexdigest()
    assert added_entity.source_identity == expected_source_identity
    assert added_entity.source_hash == expected_source_hash
    assert added_entity.provenance_json == {
        "origin": "user_created",
        "adapter": "changeset",
        "source_identity": expected_source_identity,
        "source_hash": expected_source_hash,
    }


def test_apply_change_set_returns_explicit_error_for_invalid_property_path() -> None:
    revision = RevisionRef(
        revision_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        revision_sequence=7,
    )
    entity = _entity_snapshot(
        revision_entity_id=uuid.UUID("10000000-0000-0000-0000-000000000001"),
        sequence_index=1,
        entity_id="wall-1",
    )
    operation = ChangeSetOperation(
        operation_id=uuid.UUID("20000000-0000-0000-0000-000000000011"),
        sequence_index=1,
        operation_type="update_property",
        operation_json={"property_path": "properties.height", "value": 42},
        target=ChangeSetOperationTarget(entity_id="wall-1"),
    )

    result = apply_change_set(
        change_set_id=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        base_revision=revision,
        current_revision=revision,
        operations=(operation,),
        entities=(entity,),
    )

    assert result == ChangeSetApplyError(
        change_set_id=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        error_code="INVALID_OPERATION",
        message="Unsupported property path 'properties.height'.",
        details={
            "operation_id": str(operation.operation_id),
            "sequence_index": 1,
            "operation_type": "update_property",
            "allowed_property_paths": (
                "properties.description",
                "properties.label",
                "properties.mark",
                "properties.metadata",
                "properties.notes",
                "properties.review_status",
            ),
        },
    )


def test_apply_change_set_returns_explicit_error_for_missing_target() -> None:
    revision = RevisionRef(
        revision_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        revision_sequence=7,
    )
    operation = ChangeSetOperation(
        operation_id=uuid.UUID("20000000-0000-0000-0000-000000000012"),
        sequence_index=1,
        operation_type="remove_entity",
        operation_json={},
    )

    result = apply_change_set(
        change_set_id=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        base_revision=revision,
        current_revision=revision,
        operations=(operation,),
        entities=(),
    )

    assert result == ChangeSetApplyError(
        change_set_id=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        error_code="INVALID_OPERATION",
        message="Entity-targeted changeset operations require a target selector.",
        details={
            "operation_id": str(operation.operation_id),
            "sequence_index": 1,
            "operation_type": "remove_entity",
        },
    )


def test_apply_change_set_returns_explicit_error_for_unsupported_operation() -> None:
    revision = RevisionRef(
        revision_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        revision_sequence=7,
    )
    operation = ChangeSetOperation(
        operation_id=uuid.UUID("20000000-0000-0000-0000-000000000014"),
        sequence_index=1,
        operation_type="explode_block",
        operation_json={},
    )

    result = apply_change_set(
        change_set_id=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        base_revision=revision,
        current_revision=revision,
        operations=(operation,),
        entities=(),
    )

    assert result == ChangeSetApplyError(
        change_set_id=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        error_code="UNSUPPORTED_OPERATION",
        message="Unsupported changeset operation 'explode_block'.",
        details={
            "operation_id": str(operation.operation_id),
            "sequence_index": 1,
            "operation_type": "explode_block",
        },
    )


def test_apply_change_set_returns_conflict_for_stale_current_revision() -> None:
    change_set_id = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    base_revision = RevisionRef(
        revision_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        revision_sequence=7,
    )
    current_revision = RevisionRef(
        revision_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        revision_sequence=8,
    )
    operation = ChangeSetOperation(
        operation_id=uuid.UUID("20000000-0000-0000-0000-000000000013"),
        sequence_index=1,
        operation_type="change_layer",
        operation_json={"layer_ref": "A-NEW"},
        target=ChangeSetOperationTarget(entity_id="wall-1"),
    )

    result = apply_change_set(
        change_set_id=change_set_id,
        base_revision=base_revision,
        current_revision=current_revision,
        operations=(operation,),
        entities=(),
    )

    assert result == build_stale_base_conflict(
        change_set_id=change_set_id,
        base_revision=base_revision,
        current_revision=current_revision,
        conflicting_targets=(
            ChangeSetApplyConflictTarget(
                operation_id=operation.operation_id,
                sequence_index=1,
                operation_type="change_layer",
                entity_id="wall-1",
            ),
        ),
    )


def test_build_changeset_revision_materialization_rows_copies_base_rows_and_remaps_ids() -> None:
    project_id = uuid.UUID("30000000-0000-0000-0000-000000000001")
    file_id = uuid.UUID("30000000-0000-0000-0000-000000000002")
    profile_id = uuid.UUID("30000000-0000-0000-0000-000000000003")
    job_id = uuid.UUID("30000000-0000-0000-0000-000000000004")
    revision_id = uuid.UUID("30000000-0000-0000-0000-000000000005")
    adapter_id = uuid.UUID("30000000-0000-0000-0000-000000000006")
    change_set_id = uuid.UUID("30000000-0000-0000-0000-000000000007")

    base_manifest = RevisionEntityManifest(
        id=uuid.UUID("30000000-0000-0000-0000-000000000008"),
        project_id=project_id,
        source_file_id=file_id,
        extraction_profile_id=profile_id,
        source_job_id=job_id,
        drawing_revision_id=revision_id,
        adapter_run_output_id=adapter_id,
        canonical_entity_schema_version="1",
        counts_json={"layouts": 99, "layers": 99, "blocks": 99, "entities": 2},
    )
    base_layout = RevisionLayout(
        id=uuid.UUID("30000000-0000-0000-0000-000000000009"),
        project_id=project_id,
        source_file_id=file_id,
        extraction_profile_id=profile_id,
        source_job_id=job_id,
        drawing_revision_id=revision_id,
        adapter_run_output_id=adapter_id,
        canonical_entity_schema_version="1",
        sequence_index=0,
        payload_json={"name": "Model"},
        layout_ref="model",
    )
    wall_layer = RevisionLayer(
        id=uuid.UUID("30000000-0000-0000-0000-000000000010"),
        project_id=project_id,
        source_file_id=file_id,
        extraction_profile_id=profile_id,
        source_job_id=job_id,
        drawing_revision_id=revision_id,
        adapter_run_output_id=adapter_id,
        canonical_entity_schema_version="1",
        sequence_index=0,
        payload_json={"name": "A-WALL"},
        layer_ref="A-WALL",
    )
    updated_wall_layer = RevisionLayer(
        id=uuid.UUID("30000000-0000-0000-0000-000000000011"),
        project_id=project_id,
        source_file_id=file_id,
        extraction_profile_id=profile_id,
        source_job_id=job_id,
        drawing_revision_id=revision_id,
        adapter_run_output_id=adapter_id,
        canonical_entity_schema_version="1",
        sequence_index=1,
        payload_json={"name": "A-WALL-UPDATED"},
        layer_ref="A-WALL-UPDATED",
    )
    door_layer = RevisionLayer(
        id=uuid.UUID("30000000-0000-0000-0000-000000000012"),
        project_id=project_id,
        source_file_id=file_id,
        extraction_profile_id=profile_id,
        source_job_id=job_id,
        drawing_revision_id=revision_id,
        adapter_run_output_id=adapter_id,
        canonical_entity_schema_version="1",
        sequence_index=2,
        payload_json={"name": "A-DOOR"},
        layer_ref="A-DOOR",
    )
    note_layer = RevisionLayer(
        id=uuid.UUID("30000000-0000-0000-0000-000000000013"),
        project_id=project_id,
        source_file_id=file_id,
        extraction_profile_id=profile_id,
        source_job_id=job_id,
        drawing_revision_id=revision_id,
        adapter_run_output_id=adapter_id,
        canonical_entity_schema_version="1",
        sequence_index=3,
        payload_json={"name": "A-NOTE"},
        layer_ref="A-NOTE",
    )
    base_block = RevisionBlock(
        id=uuid.UUID("30000000-0000-0000-0000-000000000014"),
        project_id=project_id,
        source_file_id=file_id,
        extraction_profile_id=profile_id,
        source_job_id=job_id,
        drawing_revision_id=revision_id,
        adapter_run_output_id=adapter_id,
        canonical_entity_schema_version="1",
        sequence_index=0,
        payload_json={"name": "door-block"},
        block_ref="door-block",
    )

    apply_result = ChangeSetApplySuccess(
        change_set_id=change_set_id,
        base_revision=RevisionRef(revision_id=revision_id, revision_sequence=1),
        current_revision=RevisionRef(revision_id=revision_id, revision_sequence=1),
        operations=(),
        entities=(
            _entity_snapshot(
                revision_entity_id=uuid.UUID("30000000-0000-0000-0000-000000000015"),
                sequence_index=0,
                entity_id="wall-1",
                layout_ref="model",
                layer_ref="A-WALL-UPDATED",
                properties_json={"description": "Wall"},
                canonical_entity_json={"properties": {"description": "Wall"}},
                source_identity="wall:1",
                source_hash="a" * 64,
            ),
            _entity_snapshot(
                revision_entity_id=uuid.UUID("30000000-0000-0000-0000-000000000016"),
                sequence_index=1,
                entity_id="door-1",
                layout_ref="model",
                layer_ref="A-DOOR",
                block_ref="door-block",
                parent_entity_ref="wall-1",
                properties_json={"label": "Door"},
                canonical_entity_json={"properties": {"label": "Door"}},
                source_identity="door:1",
                source_hash="b" * 64,
            ),
            _entity_snapshot(
                revision_entity_id=uuid.UUID("30000000-0000-0000-0000-000000000017"),
                sequence_index=2,
                entity_id="note-1",
                layout_ref="model",
                layer_ref="A-NOTE",
                properties_json={"label": "N-1"},
                canonical_entity_json={"properties": {"label": "N-1"}},
                source_identity="note:1",
                source_hash="c" * 64,
            ),
        ),
    )

    rows = worker_module._build_changeset_revision_materialization_rows(
        apply_result,
        base_manifest=base_manifest,
        base_layouts=(base_layout,),
        base_layers=(wall_layer, updated_wall_layer, door_layer, note_layer),
        base_blocks=(base_block,),
    )

    assert rows.counts_json == {"layouts": 1, "layers": 4, "blocks": 1, "entities": 3}
    assert len(rows.layouts) == 1
    assert len(rows.layers) == 4
    assert len(rows.blocks) == 1
    assert len(rows.entities) == 3

    copied_layout = rows.layouts[0]
    copied_block = rows.blocks[0]
    layer_ids_by_ref = {row["layer_ref"]: row["id"] for row in rows.layers}
    entity_rows_by_entity_id = {row["entity_id"]: row for row in rows.entities}

    assert copied_layout["id"] != base_layout.id
    assert copied_layout["sequence_index"] == base_layout.sequence_index
    assert copied_layout["payload_json"] == base_layout.payload_json
    assert copied_layout["layout_ref"] == base_layout.layout_ref

    assert copied_block["id"] != base_block.id
    assert copied_block["sequence_index"] == base_block.sequence_index
    assert copied_block["payload_json"] == base_block.payload_json
    assert copied_block["block_ref"] == base_block.block_ref

    assert layer_ids_by_ref["A-WALL-UPDATED"] != updated_wall_layer.id
    assert layer_ids_by_ref["A-DOOR"] != door_layer.id
    assert layer_ids_by_ref["A-NOTE"] != note_layer.id

    wall_row = entity_rows_by_entity_id["wall-1"]
    door_row = entity_rows_by_entity_id["door-1"]
    note_row = entity_rows_by_entity_id["note-1"]

    assert wall_row["sequence_index"] == 0
    assert wall_row["layout_ref"] == "model"
    assert wall_row["layout_id"] == copied_layout["id"]
    assert wall_row["layer_ref"] == "A-WALL-UPDATED"
    assert wall_row["layer_id"] == layer_ids_by_ref["A-WALL-UPDATED"]
    assert wall_row["block_id"] is None
    assert wall_row["parent_entity_row_id"] is None
    assert wall_row["canonical_entity_json"] == {"properties": {"description": "Wall"}}

    assert door_row["sequence_index"] == 1
    assert door_row["layout_id"] == copied_layout["id"]
    assert door_row["layer_id"] == layer_ids_by_ref["A-DOOR"]
    assert door_row["block_ref"] == "door-block"
    assert door_row["block_id"] == copied_block["id"]
    assert door_row["parent_entity_ref"] == "wall-1"
    assert door_row["parent_entity_row_id"] == wall_row["id"]
    assert door_row["canonical_entity_json"] == {"properties": {"label": "Door"}}

    assert note_row["sequence_index"] == 2
    assert note_row["layout_id"] == copied_layout["id"]
    assert note_row["layer_id"] == layer_ids_by_ref["A-NOTE"]
    assert note_row["block_id"] is None
    assert note_row["parent_entity_row_id"] is None
    assert note_row["canonical_entity_json"] == {"properties": {"label": "N-1"}}

    leaked_origin_keys = {
        "project_id",
        "source_file_id",
        "source_job_id",
        "drawing_revision_id",
        "extraction_profile_id",
        "adapter_run_output_id",
        "canonical_entity_schema_version",
    }
    for row in [*rows.layouts, *rows.layers, *rows.blocks, *rows.entities]:
        assert leaked_origin_keys.isdisjoint(row)


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_changeset_apply_job_input_persists_valid_lineage() -> None:
    session, seeded = await _seed_loader_case()
    apply_job = _build_changeset_apply_job(seeded=seeded)
    apply_input = _build_changeset_apply_job_input(seeded=seeded, source_job_id=apply_job.id)
    try:
        session.add(apply_job)
        await session.flush()

        session.add(apply_input)
        await session.flush()
        session.expunge_all()

        persisted = await session.get(ChangeSetApplyJobInput, apply_job.id)

        assert persisted is not None
        assert persisted.source_job_id == apply_job.id
        assert persisted.source_job_type == JobType.CHANGESET_APPLY.value
        assert persisted.drawing_revision_id == seeded.base_revision.id
        assert persisted.change_set_id == seeded.change_set.id
        assert persisted.latest_validation_result_id == seeded.latest_validation.id
        assert persisted.latest_validation_status == seeded.latest_validation.validation_status
    finally:
        await session.rollback()
        await session.close()


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_changeset_apply_job_input_rejects_duplicate_changeset_intent() -> None:
    session, seeded = await _seed_loader_case()
    first_job = _build_changeset_apply_job(seeded=seeded)
    second_job = _build_changeset_apply_job(
        seeded=seeded,
        job_id=uuid.UUID("90000000-0000-0000-0000-000000000105"),
    )
    try:
        session.add_all([first_job, second_job])
        await session.flush()

        session.add(
            _build_changeset_apply_job_input(
                seeded=seeded,
                source_job_id=first_job.id,
            )
        )
        await session.flush()

        session.add(
            _build_changeset_apply_job_input(
                seeded=seeded,
                source_job_id=second_job.id,
            )
        )

        with pytest.raises(
            IntegrityError,
            match="uq_changeset_apply_job_inputs_project_id_change_set_id",
        ):
            await session.flush()
    finally:
        await session.rollback()
        await session.close()


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_changeset_apply_job_requires_base_revision_id() -> None:
    session, seeded = await _seed_loader_case()
    try:
        session.add(
            Job(
                id=uuid.UUID("90000000-0000-0000-0000-000000000104"),
                project_id=seeded.project.id,
                file_id=seeded.base_revision.source_file_id,
                job_type=JobType.CHANGESET_APPLY.value,
                status="pending",
            )
        )

        with pytest.raises(
            IntegrityError,
            match="ck_jobs_revision_scoped_base_revision_required",
        ):
            await session.flush()
    finally:
        await session.rollback()
        await session.close()


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_changeset_apply_job_forbids_extraction_profile_id() -> None:
    session, seeded = await _seed_loader_case()
    try:
        session.add(
            _build_changeset_apply_job(
                seeded=seeded,
                extraction_profile_id=seeded.base_revision.extraction_profile_id,
            )
        )

        with pytest.raises(
            IntegrityError,
            match="ck_jobs_revision_scoped_extraction_profile_forbidden",
        ):
            await session.flush()
    finally:
        await session.rollback()
        await session.close()


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_changeset_apply_job_input_rejects_wrong_job_type() -> None:
    session, seeded = await _seed_loader_case()
    wrong_job = Job(
        id=uuid.UUID("90000000-0000-0000-0000-000000000101"),
        project_id=seeded.project.id,
        file_id=seeded.base_revision.source_file_id,
        extraction_profile_id=seeded.base_revision.extraction_profile_id,
        base_revision_id=seeded.base_revision.id,
        job_type=JobType.REPROCESS.value,
        status="pending",
    )
    try:
        session.add(wrong_job)
        await session.flush()

        session.add(
            _build_changeset_apply_job_input(
                seeded=seeded,
                source_job_id=wrong_job.id,
                source_job_type=JobType.REPROCESS.value,
            )
        )

        with pytest.raises(
            IntegrityError,
            match="ck_changeset_apply_job_inputs_source_job_type",
        ):
            await session.flush()
    finally:
        await session.rollback()
        await session.close()


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_changeset_apply_job_input_rejects_wrong_changeset_base_revision() -> None:
    session, seeded = await _seed_loader_case(include_current_revision=True)
    apply_job = _build_changeset_apply_job(
        seeded=seeded,
        job_id=uuid.UUID("90000000-0000-0000-0000-000000000102"),
        base_revision_id=seeded.current_revision.id,
    )
    try:
        session.add(apply_job)
        await session.flush()

        session.add(
            _build_changeset_apply_job_input(
                seeded=seeded,
                source_job_id=apply_job.id,
                drawing_revision_id=seeded.current_revision.id,
            )
        )

        with pytest.raises(
            IntegrityError,
            match="fk_changeset_apply_job_inputs_base_changeset",
        ):
            await session.flush()
    finally:
        await session.rollback()
        await session.close()


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_changeset_apply_job_input_rejects_mismatched_latest_validation_status() -> None:
    session, seeded = await _seed_loader_case(latest_validation_status="valid_with_warnings")
    apply_job = _build_changeset_apply_job(
        seeded=seeded,
        job_id=uuid.UUID("90000000-0000-0000-0000-000000000103"),
    )
    try:
        session.add(apply_job)
        await session.flush()

        session.add(
            _build_changeset_apply_job_input(
                seeded=seeded,
                source_job_id=apply_job.id,
                latest_validation_status="valid",
            )
        )

        with pytest.raises(
            IntegrityError,
            match="fk_changeset_apply_job_inputs_validation_result",
        ):
            await session.flush()
    finally:
        await session.rollback()
        await session.close()


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_load_change_set_apply_input_orders_operations_and_entities() -> None:
    session, seeded = await _seed_loader_case()
    try:
        loaded = await load_change_set_apply_input(
            session,
            project_id=seeded.project.id,
            change_set_id=seeded.change_set.id,
        )
        applied = await load_and_apply_change_set(
            session,
            project_id=seeded.project.id,
            change_set_id=seeded.change_set.id,
        )

        assert isinstance(loaded, LoadedChangeSetApplyInput)
        assert loaded.base_revision == RevisionRef(
            revision_id=seeded.base_revision.id,
            revision_sequence=seeded.base_revision.revision_sequence,
        )
        assert loaded.current_revision == loaded.base_revision
        assert [operation.sequence_index for operation in loaded.operations] == [1, 2]
        assert [operation.operation_id for operation in loaded.operations] == [
            seeded.first_operation.id,
            seeded.second_operation.id,
        ]
        assert [operation.operation_json for operation in loaded.operations] == [
            {"new_layer": "A-WALL-UPDATED"},
            {"property_path": "properties.notes", "value": "review"},
        ]
        assert [operation.target.entity_id for operation in loaded.operations] == [
            seeded.first_entity.entity_id,
            seeded.second_entity.entity_id,
        ]
        assert [entity.sequence_index for entity in loaded.entities] == [1, 4]
        assert [entity.id for entity in loaded.entities] == [
            seeded.first_entity.id,
            seeded.second_entity.id,
        ]
        assert isinstance(applied, ChangeSetApplySuccess)
        assert [entity.sequence_index for entity in applied.entities] == [0, 1]
        assert applied.entities[0].layer_ref == "A-WALL-UPDATED"
        assert applied.entities[1].properties_json == {"label": "Door", "notes": "review"}
    finally:
        await session.close()


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_load_change_set_apply_input_requires_latest_valid_validation() -> None:
    session, seeded = await _seed_loader_case(
        latest_validation_status="invalid",
        older_validation_status="valid",
    )
    try:
        with pytest.raises(
            ChangeSetApplyLoadError,
            match="latest validation must be valid or valid_with_warnings",
        ):
            await load_change_set_apply_input(
                session,
                project_id=seeded.project.id,
                change_set_id=seeded.change_set.id,
            )
    finally:
        await session.close()


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_load_change_set_apply_input_selects_do_not_autoflush_pending_changes() -> None:
    session, seeded = await _seed_loader_case()
    pending_project = Project(id=uuid.UUID("90000000-0000-0000-0000-000000000099"))
    try:
        session.add(pending_project)

        loaded = await load_change_set_apply_input(
            session,
            project_id=seeded.project.id,
            change_set_id=seeded.change_set.id,
        )

        assert isinstance(loaded, LoadedChangeSetApplyInput)
        assert pending_project in session.new
    finally:
        await session.rollback()
        await session.close()


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_load_and_apply_change_set_short_circuits_stale_current_before_entity_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, seeded = await _seed_loader_case(include_current_revision=True)
    try:
        import app.cad.changeset.loading as loading_module

        def _fail_if_called(_: RevisionEntity) -> RevisionEntitySnapshot:
            raise AssertionError("entity mapping should not run for stale current revisions")

        monkeypatch.setattr(
            loading_module,
            "_revision_entity_snapshot_from_model",
            _fail_if_called,
        )
        result = await load_and_apply_change_set(
            session,
            project_id=seeded.project.id,
            change_set_id=seeded.change_set.id,
        )
    finally:
        await session.close()

    assert result == build_stale_base_conflict(
        change_set_id=seeded.change_set.id,
        base_revision=RevisionRef(
            revision_id=seeded.base_revision.id,
            revision_sequence=seeded.base_revision.revision_sequence,
        ),
        current_revision=RevisionRef(
            revision_id=seeded.current_revision.id,
            revision_sequence=seeded.current_revision.revision_sequence,
        ),
        conflicting_targets=(
            ChangeSetApplyConflictTarget(
                operation_id=seeded.first_operation.id,
                sequence_index=1,
                operation_type="change_layer",
                target_revision_entity_id=seeded.first_entity.id,
                entity_id=seeded.first_entity.entity_id,
                expected_source_identity=seeded.first_entity.source_identity,
                expected_source_hash=seeded.first_entity.source_hash,
            ),
            ChangeSetApplyConflictTarget(
                operation_id=seeded.second_operation.id,
                sequence_index=2,
                operation_type="update_property",
                target_revision_entity_id=seeded.second_entity.id,
                entity_id=seeded.second_entity.entity_id,
                expected_source_identity=seeded.second_entity.source_identity,
                expected_source_hash=seeded.second_entity.source_hash,
            ),
        ),
    )


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_execute_changeset_apply_job_attempt_returns_success_for_finalization() -> None:
    session, seeded = await _seed_loader_case()
    attempt_token = uuid.UUID("90000000-0000-0000-0000-000000000120")
    job = _build_changeset_apply_job(
        seeded=seeded,
        job_id=uuid.UUID("90000000-0000-0000-0000-000000000121"),
    )
    job.status = "running"
    job.attempts = 1
    job.attempt_token = attempt_token
    job.started_at = datetime(2026, 1, 1, tzinfo=UTC)
    try:
        session.add(job)
        await session.flush()
        session.add(_build_changeset_apply_job_input(seeded=seeded, source_job_id=job.id))
        await session.commit()

        result = await worker_module._execute_changeset_apply_job_attempt(
            job.id,
            attempt_token=attempt_token,
        )
    finally:
        await session.close()

    assert isinstance(result, worker_module._RegisteredJobAttemptResult)
    apply_result = result.finalize_kwargs["apply_result"]
    assert isinstance(apply_result, ChangeSetApplySuccess)
    assert apply_result.change_set_id == seeded.change_set.id
    assert apply_result.base_revision == RevisionRef(
        revision_id=seeded.base_revision.id,
        revision_sequence=seeded.base_revision.revision_sequence,
    )


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_process_changeset_apply_job_missing_input_fails_deterministically() -> None:
    session, seeded = await _seed_loader_case()
    job = _build_changeset_apply_job(
        seeded=seeded,
        job_id=uuid.UUID("90000000-0000-0000-0000-000000000122"),
    )
    try:
        session.add(job)
        await session.commit()
    finally:
        await session.close()

    await worker_module.process_changeset_apply_job(job.id)

    persisted = await _get_job(job.id)
    assert persisted is not None
    assert persisted.status == "failed"
    assert persisted.error_code == ErrorCode.NOT_FOUND.value
    assert persisted.error_message == "Changeset apply job input is missing."


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_process_changeset_apply_job_multiple_inputs_fail_deterministically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, seeded = await _seed_loader_case()
    job = _build_changeset_apply_job(
        seeded=seeded,
        job_id=uuid.UUID("90000000-0000-0000-0000-000000000123"),
    )
    apply_input = _build_changeset_apply_job_input(seeded=seeded, source_job_id=job.id)
    try:
        session.add(job)
        await session.flush()
        session.add(apply_input)
        await session.commit()
    finally:
        await session.close()

    async def _duplicate_inputs(*args: object, **kwargs: object) -> list[ChangeSetApplyJobInput]:
        _ = (args, kwargs)
        return [apply_input, apply_input]

    monkeypatch.setattr(worker_module, "_query_changeset_apply_job_inputs", _duplicate_inputs)

    await worker_module.process_changeset_apply_job(job.id)

    persisted = await _get_job(job.id)
    assert persisted is not None
    assert persisted.status == "failed"
    assert persisted.error_code == ErrorCode.INPUT_INVALID.value
    assert persisted.error_message == "Changeset apply job has multiple immutable inputs."


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_process_changeset_apply_job_input_mismatch_fails_deterministically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, seeded = await _seed_loader_case()
    job = _build_changeset_apply_job(
        seeded=seeded,
        job_id=uuid.UUID("90000000-0000-0000-0000-000000000124"),
    )
    apply_input = _build_changeset_apply_job_input(seeded=seeded, source_job_id=job.id)
    mismatched_input = _build_changeset_apply_job_input(seeded=seeded, source_job_id=job.id)
    mismatched_input.drawing_revision_id = uuid.UUID("90000000-0000-0000-0000-000000000199")
    try:
        session.add(job)
        await session.flush()
        session.add(apply_input)
        await session.commit()
    finally:
        await session.close()

    async def _mismatched_inputs(*args: object, **kwargs: object) -> list[ChangeSetApplyJobInput]:
        _ = (args, kwargs)
        return [mismatched_input]

    monkeypatch.setattr(worker_module, "_query_changeset_apply_job_inputs", _mismatched_inputs)

    await worker_module.process_changeset_apply_job(job.id)

    persisted = await _get_job(job.id)
    assert persisted is not None
    assert persisted.status == "failed"
    assert persisted.error_code == ErrorCode.INPUT_INVALID.value
    assert (
        persisted.error_message
        == "Changeset apply job input lineage does not match the persisted job."
    )


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_process_changeset_apply_job_stale_base_maps_to_revision_conflict() -> None:
    session, seeded = await _seed_loader_case(include_current_revision=True)
    job = _build_changeset_apply_job(
        seeded=seeded,
        job_id=uuid.UUID("90000000-0000-0000-0000-000000000125"),
    )
    try:
        session.add(job)
        await session.flush()
        session.add(_build_changeset_apply_job_input(seeded=seeded, source_job_id=job.id))
        await session.commit()
    finally:
        await session.close()

    await worker_module.process_changeset_apply_job(job.id)

    persisted = await _get_job(job.id)
    assert persisted is not None
    assert persisted.status == "failed"
    assert persisted.error_code == ErrorCode.REVISION_CONFLICT.value
    assert (
        persisted.error_message
        == "Changeset apply base revision is stale relative to the current revision."
    )
    persisted_change_set = await _get_change_set(seeded.change_set.id)
    assert persisted_change_set is not None
    assert persisted_change_set.status == "revision_conflict"


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_process_changeset_apply_job_apply_error_fails_deterministically() -> None:
    session, seeded = await _seed_loader_case()
    job = _build_changeset_apply_job(
        seeded=seeded,
        job_id=uuid.UUID("90000000-0000-0000-0000-000000000126"),
    )
    try:
        session.add(job)
        await session.flush()
        session.add(_build_changeset_apply_job_input(seeded=seeded, source_job_id=job.id))
        await session.commit()
    finally:
        await session.close()

    async def _return_apply_error(*args: object, **kwargs: object) -> ChangeSetApplyError:
        _ = (args, kwargs)
        return ChangeSetApplyError(
            change_set_id=seeded.change_set.id,
            error_code="UNSUPPORTED_OPERATION",
            message="Unsupported changeset operation 'explode_block'.",
        )

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(worker_module, "load_and_apply_change_set", _return_apply_error)
    try:
        await worker_module.process_changeset_apply_job(job.id)
    finally:
        monkeypatch.undo()

    persisted = await _get_job(job.id)
    assert persisted is not None
    assert persisted.status == "failed"
    assert persisted.error_code == ErrorCode.INPUT_INVALID.value
    assert persisted.error_message == "Unsupported changeset operation 'explode_block'."
    persisted_change_set = await _get_change_set(seeded.change_set.id)
    assert persisted_change_set is not None
    assert persisted_change_set.status == "apply_failed"


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_process_changeset_apply_job_persists_changeset_revision_and_validation_report() -> (
    None
):
    session, seeded = await _seed_loader_case()
    attempt_token = uuid.UUID("90000000-0000-0000-0000-000000000127")
    job = _build_changeset_apply_job(
        seeded=seeded,
        job_id=uuid.UUID("90000000-0000-0000-0000-000000000128"),
    )
    job.status = "running"
    job.attempts = 1
    job.attempt_token = attempt_token
    job.started_at = datetime(2026, 1, 1, tzinfo=UTC)
    try:
        session.add(job)
        await session.flush()
        session.add(_build_changeset_apply_job_input(seeded=seeded, source_job_id=job.id))
        await session.commit()
    finally:
        await session.close()

    await worker_module.process_changeset_apply_job(job.id)

    persisted_job = await _get_job(job.id)
    assert persisted_job is not None
    assert persisted_job.status == "succeeded"
    assert persisted_job.error_code is None
    assert persisted_job.error_message is None

    persisted_revision = await _get_drawing_revision_by_source_job(job.id)
    assert persisted_revision is not None
    assert persisted_revision.project_id == seeded.project.id
    assert persisted_revision.source_file_id == seeded.base_revision.source_file_id
    assert persisted_revision.source_job_id == job.id
    assert persisted_revision.revision_kind == "changeset"
    assert persisted_revision.predecessor_revision_id == seeded.base_revision.id
    assert persisted_revision.changeset_id == seeded.change_set.id
    assert persisted_revision.revision_sequence == seeded.base_revision.revision_sequence + 1
    assert persisted_revision.extraction_profile_id is None
    assert persisted_revision.adapter_run_output_id is None

    persisted_report = await _get_validation_report_by_source_job(job.id)
    assert persisted_report is not None
    assert persisted_report.drawing_revision_id == persisted_revision.id
    assert persisted_report.project_id == seeded.project.id
    assert persisted_report.validation_status == "valid"
    assert persisted_report.review_state == "approved"
    assert persisted_report.quantity_gate == "allowed"
    assert persisted_report.report_json["change_set_id"] == str(seeded.change_set.id)
    assert persisted_report.report_json["predecessor_revision_id"] == str(seeded.base_revision.id)
    assert persisted_report.report_json["provenance"]["pinned_validation_result_id"] == str(
        seeded.latest_validation.id
    )
    assert (
        persisted_report.report_json["provenance"]["pinned_validation_status"]
        == seeded.latest_validation.validation_status
    )

    persisted_entities = await _get_revision_entities_for_revision(persisted_revision.id)
    assert [entity.entity_id for entity in persisted_entities] == ["wall-1", "door-1"]
    assert persisted_entities[0].layer_ref == "A-WALL-UPDATED"
    assert persisted_entities[0].source_job_id == job.id
    assert persisted_entities[0].extraction_profile_id is None
    assert persisted_entities[0].adapter_run_output_id is None
    assert persisted_entities[1].properties_json == {"label": "Door", "notes": "review"}

    persisted_manifest = await _get_revision_entity_manifest_for_revision(persisted_revision.id)
    assert persisted_manifest is not None
    assert persisted_manifest.source_job_id == job.id
    assert persisted_manifest.extraction_profile_id is None
    assert persisted_manifest.adapter_run_output_id is None
    assert persisted_manifest.counts_json["entities"] == 2

    persisted_change_set = await _get_change_set(seeded.change_set.id)
    assert persisted_change_set is not None
    assert persisted_change_set.status == "applied"


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_finalize_changeset_apply_job_revision_conflict() -> None:
    session, seeded = await _seed_loader_case()
    attempt_token = uuid.UUID("90000000-0000-0000-0000-000000000129")
    job = _build_changeset_apply_job(
        seeded=seeded,
        job_id=uuid.UUID("90000000-0000-0000-0000-000000000130"),
    )
    job.status = "running"
    job.attempts = 1
    job.attempt_token = attempt_token
    job.started_at = datetime(2026, 1, 1, tzinfo=UTC)
    try:
        session.add(job)
        await session.flush()
        session.add(_build_changeset_apply_job_input(seeded=seeded, source_job_id=job.id))
        await session.commit()

        result = await worker_module._execute_changeset_apply_job_attempt(
            job.id,
            attempt_token=attempt_token,
        )
    finally:
        await session.close()

    assert result is not None
    apply_result = result.finalize_kwargs["apply_result"]
    assert isinstance(apply_result, ChangeSetApplySuccess)

    await _append_competing_revision(seeded=seeded)

    with pytest.raises(
        worker_module._RevisionConflictError,
        match=r"Changeset apply base revision became stale before finalization\.",
    ):
        await worker_module._finalize_changeset_apply_job(
            job.id,
            attempt_token=attempt_token,
            apply_result=apply_result,
        )

    persisted_revision = await _get_drawing_revision_by_source_job(job.id)
    assert persisted_revision is None
    persisted_report = await _get_validation_report_by_source_job(job.id)
    assert persisted_report is None

    persisted_change_set = await _get_change_set(seeded.change_set.id)
    assert persisted_change_set is not None
    assert persisted_change_set.status == "approved"
    persisted_job = await _get_job(job.id)
    assert persisted_job is not None
    assert persisted_job.status == "running"


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_process_changeset_apply_job_finalization_conflict() -> None:
    session, seeded = await _seed_loader_case()
    job = _build_changeset_apply_job(
        seeded=seeded,
        job_id=uuid.UUID("90000000-0000-0000-0000-000000000136"),
    )
    try:
        session.add(job)
        await session.flush()
        session.add(_build_changeset_apply_job_input(seeded=seeded, source_job_id=job.id))
        await session.commit()
    finally:
        await session.close()

    real_execute = worker_module._execute_changeset_apply_job_attempt

    async def _execute_with_competing_revision(
        job_id: uuid.UUID,
        *,
        attempt_token: uuid.UUID,
    ) -> worker_module._RegisteredJobAttemptResult | None:
        result = await real_execute(job_id, attempt_token=attempt_token)
        assert result is not None
        await _append_competing_revision(seeded=seeded)
        return result

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        worker_module,
        "_execute_changeset_apply_job_attempt",
        _execute_with_competing_revision,
    )
    try:
        await worker_module.process_changeset_apply_job(job.id)
    finally:
        monkeypatch.undo()

    persisted_job = await _get_job(job.id)
    assert persisted_job is not None
    assert persisted_job.status == "failed"
    assert persisted_job.error_code == ErrorCode.REVISION_CONFLICT.value
    assert (
        persisted_job.error_message
        == "Changeset apply base revision became stale before finalization."
    )

    persisted_revision = await _get_drawing_revision_by_source_job(job.id)
    assert persisted_revision is None
    persisted_report = await _get_validation_report_by_source_job(job.id)
    assert persisted_report is None

    persisted_change_set = await _get_change_set(seeded.change_set.id)
    assert persisted_change_set is not None
    assert persisted_change_set.status == "revision_conflict"


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_finalize_changeset_apply_job_cancellation_before_output_creates_no_revision() -> (
    None
):
    session, seeded = await _seed_loader_case()
    attempt_token = uuid.UUID("90000000-0000-0000-0000-000000000134")
    job = _build_changeset_apply_job(
        seeded=seeded,
        job_id=uuid.UUID("90000000-0000-0000-0000-000000000135"),
    )
    job.status = "running"
    job.attempts = 1
    job.attempt_token = attempt_token
    job.started_at = datetime(2026, 1, 1, tzinfo=UTC)
    try:
        session.add(job)
        await session.flush()
        session.add(_build_changeset_apply_job_input(seeded=seeded, source_job_id=job.id))
        await session.commit()

        result = await worker_module._execute_changeset_apply_job_attempt(
            job.id,
            attempt_token=attempt_token,
        )
    finally:
        await session.close()

    assert result is not None
    apply_result = result.finalize_kwargs["apply_result"]
    assert isinstance(apply_result, ChangeSetApplySuccess)

    import app.db.session as session_module

    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")
    async with session_maker() as update_session:
        persisted_job = await update_session.get(Job, job.id)
        assert persisted_job is not None
        persisted_job.cancel_requested = True
        await update_session.commit()

    finalized = await worker_module._finalize_changeset_apply_job(
        job.id,
        attempt_token=attempt_token,
        apply_result=apply_result,
    )

    assert finalized is False
    persisted_job = await _get_job(job.id)
    assert persisted_job is not None
    assert persisted_job.status == "cancelled"
    assert persisted_job.finished_at is not None
    assert await _get_drawing_revision_by_source_job(job.id) is None
    assert await _get_validation_report_by_source_job(job.id) is None

    persisted_change_set = await _get_change_set(seeded.change_set.id)
    assert persisted_change_set is not None
    assert persisted_change_set.status == "approved"


@requires_database
@pytest.mark.usefixtures("cleanup_projects")
@pytest.mark.asyncio
async def test_finalize_changeset_apply_job_stale_attempt_creates_no_revision() -> None:
    session, seeded = await _seed_loader_case()
    stale_attempt_token = uuid.UUID("90000000-0000-0000-0000-000000000136")
    current_attempt_token = uuid.UUID("90000000-0000-0000-0000-000000000137")
    job = _build_changeset_apply_job(
        seeded=seeded,
        job_id=uuid.UUID("90000000-0000-0000-0000-000000000138"),
    )
    job.status = "running"
    job.attempts = 1
    job.attempt_token = stale_attempt_token
    job.started_at = datetime(2026, 1, 1, tzinfo=UTC)
    try:
        session.add(job)
        await session.flush()
        session.add(_build_changeset_apply_job_input(seeded=seeded, source_job_id=job.id))
        await session.commit()

        result = await worker_module._execute_changeset_apply_job_attempt(
            job.id,
            attempt_token=stale_attempt_token,
        )
    finally:
        await session.close()

    assert result is not None
    apply_result = result.finalize_kwargs["apply_result"]
    assert isinstance(apply_result, ChangeSetApplySuccess)

    import app.db.session as session_module

    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")
    async with session_maker() as update_session:
        persisted_job = await update_session.get(Job, job.id)
        assert persisted_job is not None
        persisted_job.attempt_token = current_attempt_token
        persisted_job.attempts = 2
        await update_session.commit()

    finalized = await worker_module._finalize_changeset_apply_job(
        job.id,
        attempt_token=stale_attempt_token,
        apply_result=apply_result,
    )

    assert finalized is False
    persisted_job = await _get_job(job.id)
    assert persisted_job is not None
    assert persisted_job.status == "running"
    assert persisted_job.attempt_token == current_attempt_token
    assert persisted_job.finished_at is None
    assert await _get_drawing_revision_by_source_job(job.id) is None
    assert await _get_validation_report_by_source_job(job.id) is None

    persisted_change_set = await _get_change_set(seeded.change_set.id)
    assert persisted_change_set is not None
    assert persisted_change_set.status == "approved"


async def _get_job(job_id: uuid.UUID) -> Job | None:
    import app.db.session as session_module

    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        return await session.get(Job, job_id)


async def _get_change_set(change_set_id: uuid.UUID) -> CadChangeSet | None:
    import app.db.session as session_module

    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        return await session.get(CadChangeSet, change_set_id)


async def _get_drawing_revision_by_source_job(source_job_id: uuid.UUID) -> DrawingRevision | None:
    import app.db.session as session_module

    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        return cast(
            DrawingRevision | None,
            await session.scalar(
                select(DrawingRevision).where(DrawingRevision.source_job_id == source_job_id)
            ),
        )


async def _get_validation_report_by_source_job(source_job_id: uuid.UUID) -> ValidationReport | None:
    import app.db.session as session_module

    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        return cast(
            ValidationReport | None,
            await session.scalar(
                select(ValidationReport).where(ValidationReport.source_job_id == source_job_id)
            ),
        )


async def _get_revision_entities_for_revision(
    drawing_revision_id: uuid.UUID,
) -> list[RevisionEntity]:
    import app.db.session as session_module

    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        result = await session.execute(
            select(RevisionEntity)
            .where(RevisionEntity.drawing_revision_id == drawing_revision_id)
            .order_by(RevisionEntity.sequence_index.asc(), RevisionEntity.id.asc())
        )
        return list(result.scalars().all())


async def _get_revision_entity_manifest_for_revision(
    drawing_revision_id: uuid.UUID,
) -> RevisionEntityManifest | None:
    import app.db.session as session_module

    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        return cast(
            RevisionEntityManifest | None,
            await session.scalar(
                select(RevisionEntityManifest).where(
                    RevisionEntityManifest.drawing_revision_id == drawing_revision_id
                )
            ),
        )


async def _append_competing_revision(*, seeded: _SeededLoaderCase) -> None:
    import app.db.session as session_module

    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        conflict_job_id = uuid.UUID("90000000-0000-0000-0000-000000000131")
        conflict_adapter_id = uuid.UUID("90000000-0000-0000-0000-000000000132")
        conflict_revision_id = uuid.UUID("90000000-0000-0000-0000-000000000133")
        session.add(
            Job(
                id=conflict_job_id,
                project_id=seeded.project.id,
                file_id=seeded.base_revision.source_file_id,
                extraction_profile_id=uuid.UUID("90000000-0000-0000-0000-000000000003"),
                base_revision_id=seeded.base_revision.id,
                job_type=JobType.REPROCESS.value,
                status="succeeded",
            )
        )
        await session.flush()
        session.add(
            AdapterRunOutput(
                id=conflict_adapter_id,
                project_id=seeded.project.id,
                source_file_id=seeded.base_revision.source_file_id,
                extraction_profile_id=uuid.UUID("90000000-0000-0000-0000-000000000003"),
                source_job_id=conflict_job_id,
                adapter_key="test_adapter",
                adapter_version="1",
                input_family="dxf",
                canonical_entity_schema_version="1",
                canonical_json={"entities": []},
                provenance_json={"adapter": "test_adapter"},
                confidence_json={},
                confidence_score=1.0,
                warnings_json=[],
                diagnostics_json=[],
                result_checksum_sha256="8" * 64,
            )
        )
        await session.flush()
        session.add(
            DrawingRevision(
                id=conflict_revision_id,
                project_id=seeded.project.id,
                source_file_id=seeded.base_revision.source_file_id,
                extraction_profile_id=uuid.UUID("90000000-0000-0000-0000-000000000003"),
                source_job_id=conflict_job_id,
                adapter_run_output_id=conflict_adapter_id,
                predecessor_revision_id=seeded.base_revision.id,
                revision_sequence=seeded.base_revision.revision_sequence + 1,
                revision_kind="reprocess",
                review_state="approved",
                canonical_entity_schema_version="1",
                confidence_score=1.0,
            )
        )
        await session.commit()


def _entity_snapshot(
    *,
    revision_entity_id: uuid.UUID,
    sequence_index: int,
    entity_id: str,
    entity_type: str = "polyline",
    parent_entity_ref: str | None = None,
    layout_ref: str | None = None,
    layer_ref: str | None = None,
    block_ref: str | None = None,
    geometry_json: dict[str, object] | None = None,
    properties_json: dict[str, object] | None = None,
    canonical_entity_json: dict[str, object] | None = None,
    source_identity: str | None = None,
    source_hash: str | None = None,
) -> RevisionEntitySnapshot:
    return RevisionEntitySnapshot(
        id=revision_entity_id,
        sequence_index=sequence_index,
        entity_id=entity_id,
        entity_type=entity_type,
        entity_schema_version="1",
        confidence_score=1.0,
        confidence_json={},
        geometry_json=geometry_json or {"kind": "polyline", "vertices": [{"x": 0.0, "y": 0.0}]},
        properties_json=properties_json or {},
        provenance_json={"origin": "source_direct"},
        parent_entity_ref=parent_entity_ref,
        canonical_entity_json=canonical_entity_json,
        layout_ref=layout_ref,
        layer_ref=layer_ref,
        block_ref=block_ref,
        source_identity=source_identity,
        source_hash=source_hash,
    )


def _added_entity_id(change_set_id: uuid.UUID, operation_id: uuid.UUID) -> str:
    return f"changeset:{change_set_id}:operation:{operation_id}"


def _added_revision_entity_id(change_set_id: uuid.UUID, operation_id: uuid.UUID) -> uuid.UUID:
    return uuid.uuid5(
        uuid.UUID("1e9ec7ab-0355-44a5-b69f-c34790709a44"),
        f"changeset:{change_set_id}:operation:{operation_id}:revision-entity",
    )


def _persisted_operation_envelope(
    *,
    entity_id: str,
    payload: dict[str, object],
    expected_source_identity: str | None = None,
    expected_source_hash: str | None = None,
) -> dict[str, object]:
    target: dict[str, object] = {"entity_id": entity_id}
    if expected_source_identity is not None:
        target["expected_source_identity"] = expected_source_identity
    if expected_source_hash is not None:
        target["expected_source_hash"] = expected_source_hash
    return {
        "payload_version": 1,
        "target": target,
        "payload": payload,
        "reason": "seeded-loader-test",
        "provenance": {"origin": "user_created"},
    }


class _SeededLoaderCase:
    def __init__(
        self,
        *,
        project: Project,
        change_set: CadChangeSet,
        base_revision: DrawingRevision,
        current_revision: DrawingRevision,
        latest_validation: CadChangeSetValidationResult,
        first_operation: CadChangeOperation,
        second_operation: CadChangeOperation,
        first_entity: RevisionEntity,
        second_entity: RevisionEntity,
    ) -> None:
        self.project = project
        self.change_set = change_set
        self.base_revision = base_revision
        self.current_revision = current_revision
        self.latest_validation = latest_validation
        self.first_operation = first_operation
        self.second_operation = second_operation
        self.first_entity = first_entity
        self.second_entity = second_entity


def _build_changeset_apply_job(
    *,
    seeded: _SeededLoaderCase,
    job_id: uuid.UUID = _DEFAULT_CHANGESET_APPLY_JOB_ID,
    base_revision_id: uuid.UUID | None = None,
    extraction_profile_id: uuid.UUID | None = None,
) -> Job:
    return Job(
        id=job_id,
        project_id=seeded.project.id,
        file_id=seeded.base_revision.source_file_id,
        extraction_profile_id=extraction_profile_id,
        base_revision_id=base_revision_id or seeded.base_revision.id,
        job_type=JobType.CHANGESET_APPLY.value,
        status="pending",
    )


def _build_changeset_apply_job_input(
    *,
    seeded: _SeededLoaderCase,
    source_job_id: uuid.UUID,
    drawing_revision_id: uuid.UUID | None = None,
    change_set_id: uuid.UUID | None = None,
    source_job_type: str = JobType.CHANGESET_APPLY.value,
    latest_validation_result_id: uuid.UUID | None = None,
    latest_validation_status: str | None = None,
) -> ChangeSetApplyJobInput:
    return ChangeSetApplyJobInput(
        source_job_id=source_job_id,
        project_id=seeded.project.id,
        source_file_id=seeded.base_revision.source_file_id,
        drawing_revision_id=drawing_revision_id or seeded.base_revision.id,
        change_set_id=change_set_id or seeded.change_set.id,
        source_job_type=source_job_type,
        latest_validation_result_id=latest_validation_result_id or seeded.latest_validation.id,
        latest_validation_status=latest_validation_status
        or seeded.latest_validation.validation_status,
    )


async def _seed_loader_case(
    *,
    latest_validation_status: str = "valid_with_warnings",
    older_validation_status: str | None = None,
    include_current_revision: bool = False,
) -> tuple[AsyncSession, _SeededLoaderCase]:
    import app.db.session as session_module

    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    session = session_maker()
    project_id = uuid.UUID("90000000-0000-0000-0000-000000000001")
    file_id = uuid.UUID("90000000-0000-0000-0000-000000000002")
    profile_id = uuid.UUID("90000000-0000-0000-0000-000000000003")
    ingest_job_id = uuid.UUID("90000000-0000-0000-0000-000000000004")
    reprocess_job_id = uuid.UUID("90000000-0000-0000-0000-000000000005")
    base_adapter_id = uuid.UUID("90000000-0000-0000-0000-000000000006")
    current_adapter_id = uuid.UUID("90000000-0000-0000-0000-000000000007")
    base_revision_id = uuid.UUID("90000000-0000-0000-0000-000000000008")
    current_revision_id = uuid.UUID("90000000-0000-0000-0000-000000000009")
    change_set_id = uuid.UUID("90000000-0000-0000-0000-000000000010")
    first_entity_id = uuid.UUID("90000000-0000-0000-0000-000000000011")
    second_entity_id = uuid.UUID("90000000-0000-0000-0000-000000000012")
    first_operation_id = uuid.UUID("90000000-0000-0000-0000-000000000013")
    second_operation_id = uuid.UUID("90000000-0000-0000-0000-000000000014")

    project = Project(id=project_id, name="Loader project")
    file = File(
        id=file_id,
        project_id=project_id,
        original_filename="loader-test.dxf",
        media_type="image/vnd.dxf",
        detected_format="dxf",
        storage_uri="storage://loader-test.dxf",
        size_bytes=128,
        checksum_sha256="1" * 64,
    )
    profile = ExtractionProfile(
        id=profile_id,
        project_id=project_id,
        profile_version="1",
        layout_mode="modelspace",
        xref_handling="ignore",
        block_handling="expand",
    )
    ingest_job = Job(
        id=ingest_job_id,
        project_id=project_id,
        file_id=file_id,
        extraction_profile_id=profile_id,
        job_type="ingest",
        status="succeeded",
    )
    reprocess_job = Job(
        id=reprocess_job_id,
        project_id=project_id,
        file_id=file_id,
        extraction_profile_id=profile_id,
        base_revision_id=base_revision_id,
        job_type="reprocess",
        status="succeeded",
    )
    base_adapter = AdapterRunOutput(
        id=base_adapter_id,
        project_id=project_id,
        source_file_id=file_id,
        extraction_profile_id=profile_id,
        source_job_id=ingest_job_id,
        adapter_key="test_adapter",
        adapter_version="1",
        input_family="dxf",
        canonical_entity_schema_version="1",
        canonical_json={"entities": []},
        provenance_json={"adapter": "test_adapter"},
        confidence_json={},
        confidence_score=1.0,
        warnings_json=[],
        diagnostics_json=[],
        result_checksum_sha256="2" * 64,
    )
    current_adapter = AdapterRunOutput(
        id=current_adapter_id,
        project_id=project_id,
        source_file_id=file_id,
        extraction_profile_id=profile_id,
        source_job_id=reprocess_job_id,
        adapter_key="test_adapter",
        adapter_version="1",
        input_family="dxf",
        canonical_entity_schema_version="1",
        canonical_json={"entities": []},
        provenance_json={"adapter": "test_adapter"},
        confidence_json={},
        confidence_score=1.0,
        warnings_json=[],
        diagnostics_json=[],
        result_checksum_sha256="3" * 64,
    )
    base_revision = DrawingRevision(
        id=base_revision_id,
        project_id=project_id,
        source_file_id=file_id,
        extraction_profile_id=profile_id,
        source_job_id=ingest_job_id,
        adapter_run_output_id=base_adapter_id,
        revision_sequence=1,
        revision_kind="ingest",
        review_state="approved",
        canonical_entity_schema_version="1",
        confidence_score=1.0,
    )
    current_revision = base_revision
    if include_current_revision:
        current_revision = DrawingRevision(
            id=current_revision_id,
            project_id=project_id,
            source_file_id=file_id,
            extraction_profile_id=profile_id,
            source_job_id=reprocess_job_id,
            adapter_run_output_id=current_adapter_id,
            predecessor_revision_id=base_revision_id,
            revision_sequence=2,
            revision_kind="reprocess",
            review_state="approved",
            canonical_entity_schema_version="1",
            confidence_score=1.0,
        )

    change_set = CadChangeSet(
        id=change_set_id,
        project_id=project_id,
        base_revision_id=base_revision_id,
        status="approved",
        created_by="tester",
    )
    first_entity = RevisionEntity(
        id=first_entity_id,
        project_id=project_id,
        source_file_id=file_id,
        extraction_profile_id=profile_id,
        source_job_id=base_revision.source_job_id,
        drawing_revision_id=base_revision_id,
        adapter_run_output_id=base_adapter_id,
        canonical_entity_schema_version="1",
        sequence_index=1,
        entity_id="wall-1",
        entity_type="polyline",
        entity_schema_version="1",
        confidence_score=1.0,
        confidence_json={},
        geometry_json={"kind": "polyline", "vertices": [{"x": 0.0, "y": 0.0}]},
        properties_json={"description": "Wall"},
        provenance_json={"origin": "source_direct"},
        canonical_entity_json={"properties": {"description": "Wall"}},
        layer_ref="A-WALL",
        source_identity="wall:1",
        source_hash="4" * 64,
    )
    second_entity = RevisionEntity(
        id=second_entity_id,
        project_id=project_id,
        source_file_id=file_id,
        extraction_profile_id=profile_id,
        source_job_id=base_revision.source_job_id,
        drawing_revision_id=base_revision_id,
        adapter_run_output_id=base_adapter_id,
        canonical_entity_schema_version="1",
        sequence_index=4,
        entity_id="door-1",
        entity_type="text",
        entity_schema_version="1",
        confidence_score=1.0,
        confidence_json={},
        geometry_json={"kind": "point", "x": 1.0, "y": 2.0},
        properties_json={"label": "Door"},
        provenance_json={"origin": "source_direct"},
        canonical_entity_json={"properties": {"label": "Door"}},
        layer_ref="A-DOOR",
        source_identity="door:1",
        source_hash="5" * 64,
    )
    first_operation = CadChangeOperation(
        id=first_operation_id,
        project_id=project_id,
        change_set_id=change_set_id,
        sequence_index=1,
        operation_type="change_layer",
        target_revision_entity_id=first_entity_id,
        expected_source_identity=first_entity.source_identity,
        expected_source_hash=first_entity.source_hash,
        operation_json=_persisted_operation_envelope(
            entity_id=first_entity.entity_id,
            payload={"new_layer": "A-WALL-UPDATED"},
            expected_source_identity=first_entity.source_identity,
            expected_source_hash=first_entity.source_hash,
        ),
    )
    second_operation = CadChangeOperation(
        id=second_operation_id,
        project_id=project_id,
        change_set_id=change_set_id,
        sequence_index=2,
        operation_type="update_property",
        target_revision_entity_id=second_entity_id,
        expected_source_identity=second_entity.source_identity,
        expected_source_hash=second_entity.source_hash,
        operation_json=_persisted_operation_envelope(
            entity_id=second_entity.entity_id,
            payload={"property_path": "properties.notes", "value": "review"},
            expected_source_identity=second_entity.source_identity,
            expected_source_hash=second_entity.source_hash,
        ),
    )

    first_validation_at = datetime(2026, 1, 1, tzinfo=UTC)
    validations: list[CadChangeSetValidationResult] = []
    if older_validation_status is not None:
        validations.append(
            CadChangeSetValidationResult(
                id=uuid.UUID("90000000-0000-0000-0000-000000000015"),
                project_id=project_id,
                change_set_id=change_set_id,
                validation_status=older_validation_status,
                validator_name="changeset_validation_service",
                validator_version="1",
                result_json={"outcome": older_validation_status},
                created_at=first_validation_at,
            )
        )
    validations.append(
        CadChangeSetValidationResult(
            id=uuid.UUID("90000000-0000-0000-0000-000000000016"),
            project_id=project_id,
            change_set_id=change_set_id,
            validation_status=latest_validation_status,
            validator_name="changeset_validation_service",
            validator_version="1",
            result_json={"outcome": latest_validation_status},
            created_at=first_validation_at + timedelta(minutes=1),
        )
    )

    base_manifest = RevisionEntityManifest(
        id=uuid.UUID("90000000-0000-0000-0000-000000000017"),
        project_id=project_id,
        source_file_id=file_id,
        extraction_profile_id=profile_id,
        source_job_id=ingest_job_id,
        drawing_revision_id=base_revision_id,
        adapter_run_output_id=base_adapter_id,
        canonical_entity_schema_version="1",
        counts_json={"entities": 2},
    )
    base_validation_report = ValidationReport(
        id=uuid.UUID("90000000-0000-0000-0000-000000000019"),
        project_id=project_id,
        drawing_revision_id=base_revision_id,
        source_job_id=ingest_job_id,
        validation_report_schema_version="0.1",
        canonical_entity_schema_version="1",
        validation_status="valid",
        review_state="approved",
        quantity_gate="allowed",
        effective_confidence=1.0,
        validator_name="test_validation_service",
        validator_version="1",
        report_json={
            "summary": {
                "validation_status": "valid",
                "review_state": "approved",
                "quantity_gate": "allowed",
            },
            "checks": [
                {
                    "code": "seeded_validation",
                    "status": "passed",
                    "message": "Seeded base validation report.",
                }
            ],
        },
        generated_at=first_validation_at,
    )

    try:
        session.add(project)
        await session.flush()

        session.add_all([file, profile])
        await session.flush()

        session.add(ingest_job)
        await session.flush()

        session.add(base_adapter)
        await session.flush()

        session.add_all([base_revision, base_manifest, base_validation_report])
        await session.flush()

        session.add_all([first_entity, second_entity])
        await session.flush()

        if include_current_revision:
            current_manifest = RevisionEntityManifest(
                id=uuid.UUID("90000000-0000-0000-0000-000000000018"),
                project_id=project_id,
                source_file_id=file_id,
                extraction_profile_id=profile_id,
                source_job_id=reprocess_job_id,
                drawing_revision_id=current_revision_id,
                adapter_run_output_id=current_adapter_id,
                canonical_entity_schema_version="1",
                counts_json={"entities": 0},
            )
            session.add(reprocess_job)
            await session.flush()

            session.add(current_adapter)
            await session.flush()

            session.add_all([current_revision, current_manifest])
            await session.flush()

        session.add(change_set)
        await session.flush()

        session.add_all([second_operation, first_operation, *validations])
        await session.flush()
        return session, _SeededLoaderCase(
            project=project,
            change_set=change_set,
            base_revision=base_revision,
            current_revision=current_revision,
            latest_validation=validations[-1],
            first_operation=first_operation,
            second_operation=second_operation,
            first_entity=first_entity,
            second_entity=second_entity,
        )
    except Exception:
        await session.rollback()
        await session.close()
        raise
