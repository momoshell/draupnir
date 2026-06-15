"""Focused DB tests for changeset validation service."""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator, Callable, Mapping
from dataclasses import dataclass
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from sqlalchemy import MetaData, Table
from sqlalchemy.ext.asyncio import AsyncSession

import app.db.session as session_module
from app.cad.changeset.apply import ALLOWED_LAYER_KEYS
from app.cad.changeset_validation import (
    ChangeSetValidationErrorCode,
    ChangeSetValidationServiceError,
    validate_change_set,
)
from app.cad.changesets import (
    CadChangeOperationCreate,
    create_change_set,
    get_change_set,
    list_change_set_validation_results,
)
from tests.conftest import requires_database

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.usefixtures("cleanup_projects"),
    requires_database,
]


@dataclass(frozen=True, slots=True)
class SeededRevision:
    project_id: uuid.UUID
    file_id: uuid.UUID
    extraction_profile_id: uuid.UUID
    source_job_id: uuid.UUID
    adapter_run_output_id: uuid.UUID
    revision_id: uuid.UUID
    manifest_id: uuid.UUID
    layout_id: uuid.UUID
    layout_ref: str
    layer_id: uuid.UUID
    layer_ref: str
    block_id: uuid.UUID
    block_ref: str
    revision_entity_row_id: uuid.UUID
    revision_entity_business_id: str


@pytest_asyncio.fixture
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        yield session


async def _reflect_tables(session: AsyncSession) -> dict[str, Table]:
    metadata = MetaData()
    connection = await session.connection()

    table_names = (
        "projects",
        "files",
        "extraction_profiles",
        "jobs",
        "adapter_run_outputs",
        "drawing_revisions",
        "revision_entity_manifests",
        "revision_layouts",
        "revision_layers",
        "revision_blocks",
        "revision_entities",
    )

    await connection.run_sync(
        lambda sync_connection: [
            Table(table_name, metadata, autoload_with=sync_connection) for table_name in table_names
        ]
    )
    return {table_name: metadata.tables[table_name] for table_name in table_names}


async def _seed_revision(
    session: AsyncSession,
    *,
    with_manifest: bool = True,
    with_layer: bool = True,
    with_entity: bool = True,
) -> SeededRevision:
    tables = await _reflect_tables(session)
    project_id = uuid.uuid4()
    file_id = uuid.uuid4()
    extraction_profile_id = uuid.uuid4()
    source_job_id = uuid.uuid4()
    adapter_run_output_id = uuid.uuid4()
    revision_id = uuid.uuid4()
    manifest_id = uuid.uuid4()
    layout_id = uuid.uuid4()
    layout_ref = "Model"
    layer_id = uuid.uuid4()
    layer_ref = "A-WALL"
    block_id = uuid.uuid4()
    block_ref = "door_block_v1"
    revision_entity_row_id = uuid.uuid4()
    revision_entity_business_id = "entity-1"

    await session.execute(
        tables["projects"].insert().values(id=project_id, name="Validation Project")
    )
    await session.execute(
        tables["files"]
        .insert()
        .values(
            id=file_id,
            project_id=project_id,
            original_filename="source.dxf",
            media_type="application/dxf",
            storage_uri=f"originals/{file_id}/{'0' * 64}",
            size_bytes=128,
            checksum_sha256="0" * 64,
            immutable=True,
        )
    )
    await session.execute(
        tables["extraction_profiles"]
        .insert()
        .values(
            id=extraction_profile_id,
            project_id=project_id,
            profile_version="1",
            layout_mode="modelspace_only",
            xref_handling="skip",
            block_handling="explode",
            text_extraction=True,
            dimension_extraction=True,
            confidence_threshold=0.6,
        )
    )
    await session.execute(
        tables["jobs"]
        .insert()
        .values(
            id=source_job_id,
            project_id=project_id,
            file_id=file_id,
            job_type="ingest",
            status="succeeded",
            attempts=0,
            max_attempts=3,
            cancel_requested=False,
            extraction_profile_id=extraction_profile_id,
            enqueue_status="published",
            enqueue_attempts=1,
        )
    )
    await session.execute(
        tables["adapter_run_outputs"]
        .insert()
        .values(
            id=adapter_run_output_id,
            project_id=project_id,
            source_file_id=file_id,
            extraction_profile_id=extraction_profile_id,
            source_job_id=source_job_id,
            adapter_key="test_adapter",
            adapter_version="1",
            input_family="dxf",
            canonical_entity_schema_version="1",
            canonical_json={},
            provenance_json={},
            confidence_json={},
            confidence_score=1.0,
            warnings_json=[],
            diagnostics_json={},
            result_checksum_sha256="1" * 64,
        )
    )
    await session.execute(
        tables["drawing_revisions"]
        .insert()
        .values(
            id=revision_id,
            project_id=project_id,
            source_file_id=file_id,
            extraction_profile_id=extraction_profile_id,
            source_job_id=source_job_id,
            adapter_run_output_id=adapter_run_output_id,
            revision_sequence=1,
            revision_kind="ingest",
            review_state="approved",
            canonical_entity_schema_version="1",
            confidence_score=1.0,
        )
    )

    if with_manifest:
        await session.execute(
            tables["revision_entity_manifests"]
            .insert()
            .values(
                id=manifest_id,
                project_id=project_id,
                source_file_id=file_id,
                extraction_profile_id=extraction_profile_id,
                source_job_id=source_job_id,
                drawing_revision_id=revision_id,
                adapter_run_output_id=adapter_run_output_id,
                canonical_entity_schema_version="1",
                counts_json={"entities": 1 if with_entity else 0, "layers": 1 if with_layer else 0},
            )
        )

    if with_layer:
        await session.execute(
            tables["revision_layouts"]
            .insert()
            .values(
                id=layout_id,
                project_id=project_id,
                source_file_id=file_id,
                extraction_profile_id=extraction_profile_id,
                source_job_id=source_job_id,
                drawing_revision_id=revision_id,
                adapter_run_output_id=adapter_run_output_id,
                canonical_entity_schema_version="1",
                sequence_index=1,
                payload_json={"name": layout_ref},
                layout_ref=layout_ref,
            )
        )
        await session.execute(
            tables["revision_layers"]
            .insert()
            .values(
                id=layer_id,
                project_id=project_id,
                source_file_id=file_id,
                extraction_profile_id=extraction_profile_id,
                source_job_id=source_job_id,
                drawing_revision_id=revision_id,
                adapter_run_output_id=adapter_run_output_id,
                canonical_entity_schema_version="1",
                sequence_index=1,
                payload_json={"name": layer_ref},
                layer_ref=layer_ref,
            )
        )
        await session.execute(
            tables["revision_blocks"]
            .insert()
            .values(
                id=block_id,
                project_id=project_id,
                source_file_id=file_id,
                extraction_profile_id=extraction_profile_id,
                source_job_id=source_job_id,
                drawing_revision_id=revision_id,
                adapter_run_output_id=adapter_run_output_id,
                canonical_entity_schema_version="1",
                sequence_index=1,
                payload_json={"name": block_ref},
                block_ref=block_ref,
            )
        )

    if with_entity:
        await session.execute(
            tables["revision_entities"]
            .insert()
            .values(
                id=revision_entity_row_id,
                project_id=project_id,
                source_file_id=file_id,
                extraction_profile_id=extraction_profile_id,
                source_job_id=source_job_id,
                drawing_revision_id=revision_id,
                adapter_run_output_id=adapter_run_output_id,
                canonical_entity_schema_version="1",
                sequence_index=1,
                entity_id=revision_entity_business_id,
                entity_type="line",
                entity_schema_version="1",
                confidence_score=1.0,
                confidence_json={},
                geometry_json={
                    "type": "line",
                    "start": {"x": 0.0, "y": 0.0},
                    "end": {"x": 10.0, "y": 0.0},
                },
                properties_json={"label": "Existing entity"},
                provenance_json={"origin": "source_direct"},
                canonical_entity_json={
                    "entity_type": "line",
                    "geometry": {
                        "type": "line",
                        "start": {"x": 0.0, "y": 0.0},
                        "end": {"x": 10.0, "y": 0.0},
                    },
                },
                layout_ref=layout_ref if with_layer else None,
                block_ref=block_ref if with_layer else None,
                layer_ref=layer_ref if with_layer else None,
                layout_id=layout_id if with_layer else None,
                layer_id=layer_id if with_layer else None,
                block_id=block_id if with_layer else None,
                source_identity="entity-identity-1",
                source_hash="2" * 64,
            )
        )

    await session.flush()
    return SeededRevision(
        project_id=project_id,
        file_id=file_id,
        extraction_profile_id=extraction_profile_id,
        source_job_id=source_job_id,
        adapter_run_output_id=adapter_run_output_id,
        revision_id=revision_id,
        manifest_id=manifest_id,
        layout_id=layout_id,
        layout_ref=layout_ref,
        layer_id=layer_id,
        layer_ref=layer_ref,
        block_id=block_id,
        block_ref=block_ref,
        revision_entity_row_id=revision_entity_row_id,
        revision_entity_business_id=revision_entity_business_id,
    )


def _operation_envelope(
    *,
    target: Mapping[str, Any] | None,
    payload: Mapping[str, Any],
    payload_version: int = 1,
    include_target: bool = True,
) -> dict[str, Any]:
    operation_json: dict[str, Any] = {
        "payload_version": payload_version,
        "payload": dict(payload),
        "reason": "test change",
        "provenance": {"origin": "user_created"},
    }
    if include_target:
        operation_json["target"] = dict(target) if isinstance(target, Mapping) else target
    return operation_json


def _annotate_operation(seed: SeededRevision) -> CadChangeOperationCreate:
    return CadChangeOperationCreate(
        operation_type="annotate_entity",
        target_revision_entity_id=seed.revision_entity_row_id,
        operation_json=_operation_envelope(
            target={"entity_id": seed.revision_entity_business_id},
            payload={"annotation": "Mark for coordination"},
        ),
    )


def _change_layer_operation(seed: SeededRevision) -> CadChangeOperationCreate:
    return CadChangeOperationCreate(
        operation_type="change_layer",
        target_revision_entity_id=seed.revision_entity_row_id,
        operation_json=_operation_envelope(
            target={"entity_id": seed.revision_entity_business_id},
            payload={"new_layer": seed.layer_ref},
        ),
    )


def _add_entity_operation(_: SeededRevision) -> CadChangeOperationCreate:
    return CadChangeOperationCreate(
        operation_type="add_entity",
        operation_json=_operation_envelope(
            target={"drawing_revision_scope": "base"},
            payload={
                "entity": {
                    "entity_type": "line",
                    "geometry": {
                        "type": "line",
                        "start": {"x": 1.0, "y": 1.0},
                        "end": {"x": 2.0, "y": 2.0},
                    },
                    "properties": {"label": "Added entity"},
                }
            },
        ),
    )


def _remove_entity_operation(seed: SeededRevision) -> CadChangeOperationCreate:
    return CadChangeOperationCreate(
        operation_type="remove_entity",
        target_revision_entity_id=seed.revision_entity_row_id,
        operation_json=_operation_envelope(
            target={"entity_id": seed.revision_entity_business_id},
            payload={"mode": "tombstone"},
        ),
    )


def _replace_block_operation(seed: SeededRevision) -> CadChangeOperationCreate:
    return CadChangeOperationCreate(
        operation_type="replace_block",
        target_revision_entity_id=seed.revision_entity_row_id,
        operation_json=_operation_envelope(
            target={"entity_id": seed.revision_entity_business_id},
            payload={"block_name": seed.block_ref},
        ),
    )


def _replace_profile_material_candidate_operation(seed: SeededRevision) -> CadChangeOperationCreate:
    return CadChangeOperationCreate(
        operation_type="replace_profile_material_candidate",
        target_revision_entity_id=seed.revision_entity_row_id,
        operation_json=_operation_envelope(
            target={"entity_id": seed.revision_entity_business_id},
            payload={"candidate": {"profile": "C1", "material": "steel"}},
        ),
    )


def _update_property_operation(seed: SeededRevision) -> CadChangeOperationCreate:
    return CadChangeOperationCreate(
        operation_type="update_property",
        target_revision_entity_id=seed.revision_entity_row_id,
        operation_json=_operation_envelope(
            target={"entity_id": seed.revision_entity_business_id},
            payload={"path": "properties.label", "value": "Updated label"},
        ),
    )


def _flag_for_review_operation(seed: SeededRevision) -> CadChangeOperationCreate:
    return CadChangeOperationCreate(
        operation_type="flag_for_review",
        target_revision_entity_id=seed.revision_entity_row_id,
        operation_json=_operation_envelope(
            target={"entity_id": seed.revision_entity_business_id},
            payload={"reason": "Needs manual verification"},
        ),
    )


def _result_codes(result_json: Mapping[str, Any]) -> list[str]:
    return [finding["code"] for finding in result_json["findings"]]


@pytest.mark.parametrize(
    ("builder", "expected_validation_status", "expected_change_set_status", "expected_code"),
    [
        (_annotate_operation, "valid_with_warnings", "validated", "metadata_only_operation"),
        (_change_layer_operation, "valid", "validated", None),
        (_add_entity_operation, "valid", "validated", None),
        (_remove_entity_operation, "valid", "validated", None),
        (
            _replace_block_operation,
            "needs_review",
            "validation_failed",
            "operation_requires_review",
        ),
        (
            _replace_profile_material_candidate_operation,
            "needs_review",
            "validation_failed",
            "profile_material_review_required",
        ),
        (_update_property_operation, "valid", "validated", None),
        (_flag_for_review_operation, "needs_review", "validation_failed", "manual_review_flagged"),
    ],
)
async def test_validate_change_set_covers_all_operation_types(
    db_session: AsyncSession,
    builder: Callable[[SeededRevision], CadChangeOperationCreate],
    expected_validation_status: str,
    expected_change_set_status: str,
    expected_code: str | None,
) -> None:
    seed = await _seed_revision(db_session)
    change_set = await create_change_set(
        db_session,
        project_id=seed.project_id,
        base_revision_id=seed.revision_id,
        status="proposed",
        created_by="tester",
        operations=[builder(seed)],
    )

    validation_result = await validate_change_set(db_session, change_set.id)
    refreshed_change_set = await get_change_set(
        db_session,
        project_id=seed.project_id,
        change_set_id=change_set.id,
    )

    assert refreshed_change_set is not None
    assert validation_result.validation_status == expected_validation_status
    assert validation_result.validator_name == "changeset_validation_service"
    assert validation_result.validator_version == "1"
    assert validation_result.result_json["schema_version"] == "changeset-validation-result-v1"
    assert validation_result.result_json["change_set_id"] == str(change_set.id)
    assert validation_result.result_json["base_revision_id"] == str(seed.revision_id)
    assert validation_result.result_json["status"] == expected_validation_status
    assert refreshed_change_set.status == expected_change_set_status

    findings = validation_result.result_json["findings"]
    if expected_code is None:
        assert findings == []
        assert validation_result.result_json["summary"] == {
            "total_findings": 0,
            "error_count": 0,
            "warning_count": 0,
            "review_count": 0,
        }
    else:
        assert [finding["code"] for finding in findings] == [expected_code]
        assert findings[0]["sequence_index"] == 1
        assert findings[0]["operation_id"]


@pytest.mark.parametrize(
    "payload",
    [
        {"annotation": "Mark for coordination"},
        {"note": "Mark for coordination"},
        {"label": "Coordination"},
        {"text": "See RFI 12"},
        {"metadata": {"rfi": 12}},
    ],
)
async def test_validate_change_set_accepts_annotation_under_any_supported_key(
    db_session: AsyncSession,
    payload: dict[str, Any],
) -> None:
    """annotate_entity validates under any key the create contract accepts (#410)."""
    seed = await _seed_revision(db_session)
    operation = CadChangeOperationCreate(
        operation_type="annotate_entity",
        target_revision_entity_id=seed.revision_entity_row_id,
        operation_json=_operation_envelope(
            target={"entity_id": seed.revision_entity_business_id},
            payload=payload,
        ),
    )
    change_set = await create_change_set(
        db_session,
        project_id=seed.project_id,
        base_revision_id=seed.revision_id,
        status="proposed",
        created_by="tester",
        operations=[operation],
    )

    validation_result = await validate_change_set(db_session, change_set.id)

    codes = _result_codes(validation_result.result_json)
    assert "invalid_annotation_payload" not in codes
    assert validation_result.validation_status == "valid_with_warnings"


@pytest.mark.parametrize(
    ("operation_json", "expected_code"),
    [
        (
            {
                "payload_version": 2,
                "target": {"entity_id": "entity-1"},
                "payload": {"path": "properties.label", "value": "Updated"},
                "reason": "test change",
                "provenance": {"origin": "user_created"},
            },
            "unsupported_payload_version",
        ),
        (
            {
                "payload_version": 1,
                "target": {"entity_id": "entity-1"},
                "payload": ["not", "an", "object"],
                "reason": "test change",
                "provenance": {"origin": "user_created"},
            },
            "invalid_operation_envelope",
        ),
    ],
)
async def test_validate_change_set_marks_malformed_persisted_operations_invalid(
    db_session: AsyncSession,
    operation_json: dict[str, Any],
    expected_code: str,
) -> None:
    seed = await _seed_revision(db_session)
    change_set = await create_change_set(
        db_session,
        project_id=seed.project_id,
        base_revision_id=seed.revision_id,
        status="proposed",
        created_by="tester",
        operations=[
            CadChangeOperationCreate(
                operation_type="update_property",
                target_revision_entity_id=seed.revision_entity_row_id,
                operation_json=operation_json,
            )
        ],
    )

    validation_result = await validate_change_set(db_session, change_set.id)
    refreshed_change_set = await get_change_set(
        db_session,
        project_id=seed.project_id,
        change_set_id=change_set.id,
    )

    assert refreshed_change_set is not None
    assert validation_result.validation_status == "invalid"
    assert refreshed_change_set.status == "validation_failed"
    assert expected_code in {
        finding["code"] for finding in validation_result.result_json["findings"]
    }
    assert validation_result.result_json["summary"]["error_count"] >= 1


async def test_validate_change_set_rejects_target_guard_mismatches(
    db_session: AsyncSession,
) -> None:
    seed = await _seed_revision(db_session)
    change_set = await create_change_set(
        db_session,
        project_id=seed.project_id,
        base_revision_id=seed.revision_id,
        status="proposed",
        created_by="tester",
        operations=[
            CadChangeOperationCreate(
                operation_type="update_property",
                target_revision_entity_id=seed.revision_entity_row_id,
                expected_source_identity="wrong-identity",
                expected_source_hash="3" * 64,
                operation_json=_operation_envelope(
                    target={"entity_id": seed.revision_entity_business_id},
                    payload={"path": "properties.label", "value": "Updated label"},
                ),
            )
        ],
    )

    validation_result = await validate_change_set(db_session, change_set.id)

    assert validation_result.validation_status == "invalid"
    assert _result_codes(validation_result.result_json) == [
        "expected_source_identity_mismatch",
        "expected_source_hash_mismatch",
    ]


@pytest.mark.parametrize(
    ("operation", "expected_status", "expected_code"),
    [
        (
            lambda _seed: CadChangeOperationCreate(
                operation_type="replace_profile_material_candidate",
                operation_json=_operation_envelope(
                    target=None,
                    payload={"candidate": {"profile": "C1", "material": "steel"}},
                ),
            ),
            "needs_review",
            "profile_material_review_required",
        ),
        (
            lambda seed: CadChangeOperationCreate(
                operation_type="replace_block",
                operation_json=_operation_envelope(
                    target=None,
                    payload={"block_name": seed.block_ref},
                ),
            ),
            "needs_review",
            "operation_requires_review",
        ),
        (
            lambda _seed: CadChangeOperationCreate(
                operation_type="flag_for_review",
                operation_json=_operation_envelope(
                    target=None,
                    payload={"reason": "Needs manual verification"},
                ),
            ),
            "needs_review",
            "manual_review_flagged",
        ),
    ],
)
async def test_validate_change_set_allows_non_entity_operations_without_entity_target(
    db_session: AsyncSession,
    operation: Callable[[SeededRevision], CadChangeOperationCreate],
    expected_status: str,
    expected_code: str,
) -> None:
    seed = await _seed_revision(db_session)
    change_set = await create_change_set(
        db_session,
        project_id=seed.project_id,
        base_revision_id=seed.revision_id,
        status="proposed",
        created_by="tester",
        operations=[operation(seed)],
    )

    validation_result = await validate_change_set(db_session, change_set.id)

    assert validation_result.validation_status == expected_status
    assert _result_codes(validation_result.result_json) == [expected_code]


async def test_validate_change_set_accepts_update_property_aliases_without_entity_target(
    db_session: AsyncSession,
) -> None:
    seed = await _seed_revision(db_session)
    change_set = await create_change_set(
        db_session,
        project_id=seed.project_id,
        base_revision_id=seed.revision_id,
        status="proposed",
        created_by="tester",
        operations=[
            CadChangeOperationCreate(
                operation_type="update_property",
                operation_json=_operation_envelope(
                    target=None,
                    payload={
                        "property_name": "properties.label",
                        "new_value": "Updated label",
                    },
                ),
            )
        ],
    )

    validation_result = await validate_change_set(db_session, change_set.id)

    assert validation_result.validation_status == "valid"
    assert validation_result.result_json["findings"] == []


async def test_validate_change_set_accepts_add_entity_without_target_and_direct_geometry(
    db_session: AsyncSession,
) -> None:
    seed = await _seed_revision(db_session)
    change_set = await create_change_set(
        db_session,
        project_id=seed.project_id,
        base_revision_id=seed.revision_id,
        status="proposed",
        created_by="tester",
        operations=[
            CadChangeOperationCreate(
                operation_type="add_entity",
                operation_json=_operation_envelope(
                    target=None,
                    payload={
                        "geometry": {
                            "type": "line",
                            "start": {"x": 1.0, "y": 1.0},
                            "end": {"x": 2.0, "y": 2.0},
                        }
                    },
                    include_target=False,
                ),
            )
        ],
    )

    validation_result = await validate_change_set(db_session, change_set.id)

    assert validation_result.validation_status == "valid"
    assert validation_result.result_json["findings"] == []


@pytest.mark.parametrize(
    ("payload", "expected_code"),
    [
        (
            {
                "geometry": "not-an-object",
            },
            "invalid_geometry_payload",
        ),
        (
            {
                "geometry": {
                    "type": "line",
                    "start": {"x": True, "y": 1.0},
                    "end": {"x": 2.0, "y": 2.0},
                }
            },
            "nonfinite_geometry_coordinate",
        ),
        (
            {
                "entity": {
                    "entity_type": "line",
                    "geometry": {
                        "type": "line",
                        "start": {"x": 1.0, "y": 1.0},
                        "end": {"x": 2.0, "y": 2.0},
                    },
                    "layer_ref": "missing-layer",
                }
            },
            "layer_ref_not_found",
        ),
        (
            {
                "entity": {
                    "entity_type": "line",
                    "geometry": {
                        "type": "line",
                        "start": {"x": 1.0, "y": 1.0},
                        "end": {"x": 2.0, "y": 2.0},
                    },
                    "layout_ref": "missing-layout",
                }
            },
            "layout_ref_not_found",
        ),
        (
            {
                "entity": {
                    "entity_type": "line",
                    "geometry": {
                        "type": "line",
                        "start": {"x": 1.0, "y": 1.0},
                        "end": {"x": 2.0, "y": 2.0},
                    },
                    "block_ref": "missing-block",
                }
            },
            "block_ref_not_found",
        ),
        (
            {
                "geometry": {
                    "type": "polygon_area",
                }
            },
            "invalid_polygon_area_geometry",
        ),
        (
            {
                "geometry": {
                    "type": "polygon_area",
                    "rings": [
                        [
                            {"x": 0.0, "y": 0.0},
                            {"x": 1.0, "y": 0.0},
                            {"x": 1.0, "y": 1.0},
                            {"x": 0.0, "y": 0.0},
                        ],
                        "not-a-ring",
                    ],
                }
            },
            "invalid_polygon_area_geometry",
        ),
        (
            {
                "geometry": {
                    "type": "polygon_area",
                    "rings": [
                        [
                            {"x": 0.0, "y": 0.0},
                            {"x": 1.0, "y": 0.0},
                            {"x": 1.0, "y": 1.0},
                            {},
                        ]
                    ],
                }
            },
            "invalid_polygon_area_geometry",
        ),
        (
            {
                "geometry": {
                    "type": "polygon_area",
                    "rings": [
                        [
                            {"x": 0.0, "y": 0.0},
                            {"x": 1.0, "y": 0.0},
                            {"x": 1.0, "y": 1.0},
                            {"z": 1.0},
                        ]
                    ],
                }
            },
            "invalid_polygon_area_geometry",
        ),
        (
            {
                "geometry": {
                    "type": "polygon_area",
                    "rings": [[{"x": 0.0, "y": 0.0}, {"x": 1.0, "y": 0.0}]],
                }
            },
            "invalid_polygon_area_geometry",
        ),
    ],
)
async def test_validate_change_set_rejects_invalid_add_entity_geometry_and_references(
    db_session: AsyncSession,
    payload: dict[str, Any],
    expected_code: str,
) -> None:
    seed = await _seed_revision(db_session)
    change_set = await create_change_set(
        db_session,
        project_id=seed.project_id,
        base_revision_id=seed.revision_id,
        status="proposed",
        created_by="tester",
        operations=[
            CadChangeOperationCreate(
                operation_type="add_entity",
                operation_json=_operation_envelope(
                    target={"drawing_revision_scope": "base"},
                    payload=payload,
                ),
            )
        ],
    )

    validation_result = await validate_change_set(db_session, change_set.id)

    assert validation_result.validation_status == "invalid"
    assert expected_code in _result_codes(validation_result.result_json)


@pytest.mark.parametrize("layer_key", ALLOWED_LAYER_KEYS)
async def test_validate_change_set_rejects_missing_add_entity_layer_aliases(
    db_session: AsyncSession,
    layer_key: str,
) -> None:
    seed = await _seed_revision(db_session)
    change_set = await create_change_set(
        db_session,
        project_id=seed.project_id,
        base_revision_id=seed.revision_id,
        status="proposed",
        created_by="tester",
        operations=[
            CadChangeOperationCreate(
                operation_type="add_entity",
                operation_json=_operation_envelope(
                    target={"drawing_revision_scope": "base"},
                    payload={
                        "entity": {
                            "entity_type": "line",
                            "geometry": {
                                "type": "line",
                                "start": {"x": 1.0, "y": 1.0},
                                "end": {"x": 2.0, "y": 2.0},
                            },
                            layer_key: "missing-layer",
                        }
                    },
                ),
            )
        ],
    )

    validation_result = await validate_change_set(db_session, change_set.id)

    assert validation_result.validation_status == "invalid"
    assert f"{layer_key}_not_found" in _result_codes(validation_result.result_json)


async def test_validate_change_set_rejects_invalid_replace_block_reference(
    db_session: AsyncSession,
) -> None:
    seed = await _seed_revision(db_session)
    change_set = await create_change_set(
        db_session,
        project_id=seed.project_id,
        base_revision_id=seed.revision_id,
        status="proposed",
        created_by="tester",
        operations=[
            CadChangeOperationCreate(
                operation_type="replace_block",
                target_revision_entity_id=seed.revision_entity_row_id,
                operation_json=_operation_envelope(
                    target={"entity_id": seed.revision_entity_business_id},
                    payload={"replacement": {"block_ref": "missing-block"}},
                ),
            )
        ],
    )

    validation_result = await validate_change_set(db_session, change_set.id)

    assert validation_result.validation_status == "invalid"
    assert _result_codes(validation_result.result_json) == ["block_not_found"]


async def test_validate_change_set_rejects_targets_removed_by_prior_operations(
    db_session: AsyncSession,
) -> None:
    seed = await _seed_revision(db_session)
    change_set = await create_change_set(
        db_session,
        project_id=seed.project_id,
        base_revision_id=seed.revision_id,
        status="proposed",
        created_by="tester",
        operations=[
            _remove_entity_operation(seed),
            _update_property_operation(seed),
        ],
    )

    validation_result = await validate_change_set(db_session, change_set.id)

    assert validation_result.validation_status == "invalid"
    assert _result_codes(validation_result.result_json) == ["target_removed_by_prior_operation"]


async def test_validate_change_set_revalidation_appends_results_without_overwrite(
    db_session: AsyncSession,
) -> None:
    seed = await _seed_revision(db_session)
    change_set = await create_change_set(
        db_session,
        project_id=seed.project_id,
        base_revision_id=seed.revision_id,
        status="validation_failed",
        created_by="tester",
        operations=[_flag_for_review_operation(seed)],
    )

    first_result = await validate_change_set(db_session, change_set.id)
    second_result = await validate_change_set(db_session, change_set.id)
    all_results = await list_change_set_validation_results(
        db_session,
        project_id=seed.project_id,
        change_set_id=change_set.id,
    )
    refreshed_change_set = await get_change_set(
        db_session,
        project_id=seed.project_id,
        change_set_id=change_set.id,
    )

    assert refreshed_change_set is not None
    assert refreshed_change_set.status == "validation_failed"
    assert first_result.id != second_result.id
    assert [result.validation_status for result in all_results] == ["needs_review", "needs_review"]
    assert [result.validator_name for result in all_results] == [
        "changeset_validation_service",
        "changeset_validation_service",
    ]


async def test_validate_change_set_raises_for_disallowed_start_status(
    db_session: AsyncSession,
) -> None:
    seed = await _seed_revision(db_session)
    change_set = await create_change_set(
        db_session,
        project_id=seed.project_id,
        base_revision_id=seed.revision_id,
        status="validated",
        created_by="tester",
        operations=[_change_layer_operation(seed)],
    )

    with pytest.raises(ChangeSetValidationServiceError) as exc_info:
        await validate_change_set(db_session, change_set.id)

    all_results = await list_change_set_validation_results(
        db_session,
        project_id=seed.project_id,
        change_set_id=change_set.id,
    )

    assert exc_info.value.code == ChangeSetValidationErrorCode.INVALID_CHANGE_SET_STATUS
    assert all_results == ()


async def test_validate_change_set_raises_when_manifest_missing(
    db_session: AsyncSession,
) -> None:
    seed = await _seed_revision(db_session, with_manifest=False)
    change_set = await create_change_set(
        db_session,
        project_id=seed.project_id,
        base_revision_id=seed.revision_id,
        status="proposed",
        created_by="tester",
        operations=[_change_layer_operation(seed)],
    )

    with pytest.raises(ChangeSetValidationServiceError) as exc_info:
        await validate_change_set(db_session, change_set.id)

    all_results = await list_change_set_validation_results(
        db_session,
        project_id=seed.project_id,
        change_set_id=change_set.id,
    )

    assert exc_info.value.code == ChangeSetValidationErrorCode.MATERIALIZATION_MANIFEST_NOT_FOUND
    assert all_results == ()


async def test_validate_change_set_raises_when_change_set_missing(
    db_session: AsyncSession,
) -> None:
    with pytest.raises(ChangeSetValidationServiceError) as exc_info:
        await validate_change_set(db_session, uuid.uuid4())

    assert exc_info.value.code == ChangeSetValidationErrorCode.CHANGE_SET_NOT_FOUND


async def test_validate_change_set_raises_when_base_revision_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    change_set_id = uuid.uuid4()
    fake_change_set = type(
        "FakeChangeSet",
        (),
        {
            "id": change_set_id,
            "project_id": uuid.uuid4(),
            "base_revision_id": uuid.uuid4(),
            "status": "proposed",
        },
    )()

    monkeypatch.setattr(
        "app.cad.changeset_validation._load_validation_context",
        AsyncMock(
            return_value=type(
                "Context",
                (),
                {"change_set": fake_change_set, "operations": ()},
            )()
        ),
    )
    monkeypatch.setattr(
        "app.cad.changeset_validation._base_revision_exists",
        AsyncMock(return_value=False),
    )

    with pytest.raises(ChangeSetValidationServiceError) as exc_info:
        await validate_change_set(cast(AsyncSession, object()), change_set_id)

    assert exc_info.value.code == ChangeSetValidationErrorCode.BASE_REVISION_NOT_FOUND
