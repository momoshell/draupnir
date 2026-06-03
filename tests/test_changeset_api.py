from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select

import app.db.session as session_module
from app.api.idempotency import hash_idempotency_key
from app.models.adapter_run_output import AdapterRunOutput
from app.models.cad_changeset import CadChangeOperation, CadChangeSet
from app.models.drawing_revision import DrawingRevision
from app.models.extraction_profile import ExtractionProfile
from app.models.file import File
from app.models.idempotency_key import IdempotencyKey
from app.models.job import Job, JobType
from app.models.project import Project
from tests.conftest import requires_database

pytestmark = requires_database

_VALID_SOURCE_HASH = "a" * 64


@dataclass(frozen=True, slots=True)
class ChangesetLineage:
    project_id: UUID
    file_id: UUID
    revision_id: UUID
    source_job_id: UUID


async def _seed_changeset_lineage() -> ChangesetLineage:
    assert session_module.AsyncSessionLocal is not None

    async with session_module.AsyncSessionLocal() as session:
        project = Project(name=f"Changeset API Project {uuid4()}")
        session.add(project)
        await session.flush()

        extraction_profile = ExtractionProfile(
            project_id=project.id,
            profile_version="1.0",
            layout_mode="model_space",
            xref_handling="bind",
            block_handling="expand",
        )
        session.add(extraction_profile)
        await session.flush()

        source_file = File(
            project_id=project.id,
            original_filename="changeset-plan.dxf",
            media_type="image/vnd.dxf",
            detected_format="dxf",
            storage_uri=f"file:///tmp/originals/{uuid4()}.dxf",
            size_bytes=128,
            checksum_sha256=uuid4().hex + uuid4().hex,
            immutable=True,
            initial_extraction_profile_id=extraction_profile.id,
        )
        session.add(source_file)
        await session.flush()

        revision_source_job = Job(
            project_id=project.id,
            file_id=source_file.id,
            extraction_profile_id=extraction_profile.id,
            base_revision_id=None,
            parent_job_id=None,
            job_type=JobType.INGEST.value,
            status="succeeded",
            attempts=1,
            max_attempts=3,
            enqueue_status="pending",
            enqueue_attempts=0,
            cancel_requested=False,
        )
        session.add(revision_source_job)
        await session.flush()

        adapter_run_output = AdapterRunOutput(
            project_id=project.id,
            source_file_id=source_file.id,
            extraction_profile_id=extraction_profile.id,
            source_job_id=revision_source_job.id,
            adapter_key="fake-dxf",
            adapter_version="1.0",
            input_family="dxf",
            canonical_entity_schema_version="1.0",
            canonical_json={"entities": []},
            provenance_json={"origin": "test"},
            confidence_json={"entities": []},
            confidence_score=0.99,
            warnings_json=[],
            diagnostics_json={},
            result_checksum_sha256=uuid4().hex + uuid4().hex,
        )
        session.add(adapter_run_output)
        await session.flush()

        revision = DrawingRevision(
            project_id=project.id,
            source_file_id=source_file.id,
            extraction_profile_id=extraction_profile.id,
            source_job_id=revision_source_job.id,
            adapter_run_output_id=adapter_run_output.id,
            predecessor_revision_id=None,
            revision_sequence=1,
            revision_kind="ingest",
            review_state="approved",
            canonical_entity_schema_version="1.0",
            confidence_score=0.99,
        )
        session.add(revision)
        await session.flush()

        lineage = ChangesetLineage(
            project_id=project.id,
            file_id=source_file.id,
            revision_id=revision.id,
            source_job_id=revision_source_job.id,
        )
        await session.commit()

    return lineage


def _route_path(lineage: ChangesetLineage) -> str:
    return f"/v1/revisions/{lineage.revision_id}/changesets"


def _valid_create_body(*, created_by: str = " api-user ") -> dict[str, object]:
    target_entity_id = uuid4()
    return {
        "created_by": created_by,
        "operations": [
            {
                "payload_version": 1,
                "sequence_index": 1,
                "operation_type": "change_layer",
                "target": {"revision_entity_id": str(target_entity_id)},
                "expected_source_identity": " source-001 ",
                "expected_source_hash": _VALID_SOURCE_HASH.upper(),
                "payload": {"new_layer": "A-ANNO"},
                "reason": " clarify annotation layer ",
                "provenance": {"origin": "test"},
            },
            {
                "payload_version": 1,
                "sequence_index": 2,
                "operation_type": "flag_for_review",
                "payload": {"reason": "manual review", "subject": "changeset"},
            },
        ],
    }


async def _count_changeset_rows(lineage: ChangesetLineage) -> tuple[int, int]:
    assert session_module.AsyncSessionLocal is not None

    async with session_module.AsyncSessionLocal() as session:
        change_set_count = await session.scalar(
            select(func.count())
            .select_from(CadChangeSet)
            .where(CadChangeSet.base_revision_id == lineage.revision_id)
        )
        operation_count = await session.scalar(
            select(func.count())
            .select_from(CadChangeOperation)
            .join(
                CadChangeSet,
                CadChangeSet.id == CadChangeOperation.change_set_id,
            )
            .where(CadChangeSet.base_revision_id == lineage.revision_id)
        )

    assert change_set_count is not None
    assert operation_count is not None
    return change_set_count, operation_count


async def _count_idempotency_rows(idempotency_key: str) -> int:
    assert session_module.AsyncSessionLocal is not None

    async with session_module.AsyncSessionLocal() as session:
        row_count = await session.scalar(
            select(func.count())
            .select_from(IdempotencyKey)
            .where(IdempotencyKey.key_hash == hash_idempotency_key(idempotency_key))
        )

    assert row_count is not None
    return row_count


async def _set_source_file_deleted(lineage: ChangesetLineage) -> None:
    assert session_module.AsyncSessionLocal is not None

    async with session_module.AsyncSessionLocal() as session:
        source_file = await session.get(File, lineage.file_id)
        assert source_file is not None
        source_file.deleted_at = datetime.now(UTC)
        await session.commit()


async def test_create_changeset_persists_proposed_changeset_and_operations(
    async_client: AsyncClient,
) -> None:
    lineage = await _seed_changeset_lineage()

    response = await async_client.post(_route_path(lineage), json=_valid_create_body())

    assert response.status_code == 201
    body = response.json()
    assert body["project_id"] == str(lineage.project_id)
    assert body["base_revision_id"] == str(lineage.revision_id)
    assert body["status"] == "proposed"
    assert body["created_by"] == "api-user"

    operations = body["operations"]
    assert [operation["sequence_index"] for operation in operations] == [1, 2]
    assert [operation["operation_type"] for operation in operations] == [
        "change_layer",
        "flag_for_review",
    ]
    assert all(operation["payload_version"] == 1 for operation in operations)
    assert UUID(operations[0]["operation_id"])
    assert operations[0]["target"]["revision_entity_id"]
    assert operations[0]["expected_source_identity"] == "source-001"
    assert operations[0]["expected_source_hash"] == _VALID_SOURCE_HASH
    assert operations[0]["payload"] == {"new_layer": "A-ANNO"}
    assert operations[0]["reason"] == "clarify annotation layer"
    assert operations[0]["provenance"] == {"origin": "test"}
    assert operations[1]["payload"] == {
        "reason": "manual review",
        "subject": "changeset",
    }
    assert await _count_changeset_rows(lineage) == (1, 2)


async def test_read_and_list_changesets_return_ordered_operations(
    async_client: AsyncClient,
) -> None:
    lineage = await _seed_changeset_lineage()

    first_response = await async_client.post(
        _route_path(lineage),
        json=_valid_create_body(created_by="first"),
    )
    second_response = await async_client.post(
        _route_path(lineage),
        json=_valid_create_body(created_by="second"),
    )
    assert first_response.status_code == 201
    assert second_response.status_code == 201

    first_body = first_response.json()
    get_response = await async_client.get(f"{_route_path(lineage)}/{first_body['id']}")
    assert get_response.status_code == 200
    assert get_response.json() == first_body

    page_one_response = await async_client.get(_route_path(lineage), params={"limit": 1})
    assert page_one_response.status_code == 200
    page_one = page_one_response.json()
    assert len(page_one["items"]) == 1
    assert page_one["next_cursor"] is not None
    page_one_operation_indexes = [
        operation["sequence_index"] for operation in page_one["items"][0]["operations"]
    ]
    assert page_one_operation_indexes == [1, 2]

    page_two_response = await async_client.get(
        _route_path(lineage),
        params={"limit": 1, "cursor": page_one["next_cursor"]},
    )
    assert page_two_response.status_code == 200
    page_two = page_two_response.json()
    assert len(page_two["items"]) == 1
    assert page_two["next_cursor"] is None

    assert {page_one["items"][0]["id"], page_two["items"][0]["id"]} == {
        first_body["id"],
        second_response.json()["id"],
    }


@pytest.mark.parametrize(
    "invalid_operation",
    [
        {
            "payload_version": 2,
            "operation_type": "flag_for_review",
            "payload": {"reason": "manual review", "subject": "changeset"},
        },
        {
            "payload_version": 1,
            "sequence_index": 2,
            "operation_type": "flag_for_review",
            "payload": {"reason": "manual review", "subject": "changeset"},
        },
        {
            "operation_id": str(uuid4()),
            "payload_version": 1,
            "operation_type": "flag_for_review",
            "payload": {"reason": "manual review", "subject": "changeset"},
        },
        {
            "payload_version": 1,
            "operation_type": "unsupported_operation",
            "payload": {"reason": "manual review", "subject": "changeset"},
        },
        {
            "payload_version": 1,
            "operation_type": "change_layer",
            "target": {},
            "payload": {"new_layer": "A-ANNO"},
        },
        {
            "payload_version": 1,
            "operation_type": "update_property",
            "target": {"scope": "entity"},
            "payload": {"property": "fire_rating"},
        },
        {
            "payload_version": 1,
            "operation_type": "change_layer",
            "target": {"revision_entity_id": str(uuid4())},
            "expected_source_identity": "x" * 256,
            "payload": {"new_layer": "A-ANNO"},
        },
        {
            "payload_version": 1,
            "operation_type": "remove_entity",
            "target": {"revision_entity_id": str(uuid4())},
            "payload": {},
        },
    ],
    ids=[
        "unsupported-payload-version",
        "mismatched-sequence-index",
        "client-operation-id",
        "unsupported-operation-type",
        "missing-required-target",
        "missing-required-payload-value",
        "overlong-expected-source-identity",
        "missing-remove-entity-payload",
    ],
)
async def test_create_changeset_rejects_invalid_operation_contract_before_claim(
    async_client: AsyncClient,
    invalid_operation: dict[str, object],
) -> None:
    lineage = await _seed_changeset_lineage()
    idempotency_key = f"changeset-invalid-{uuid4().hex}"

    response = await async_client.post(
        _route_path(lineage),
        json={"operations": [invalid_operation]},
        headers={"Idempotency-Key": idempotency_key},
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INPUT_INVALID"
    assert await _count_changeset_rows(lineage) == (0, 0)
    assert await _count_idempotency_rows(idempotency_key) == 0


async def test_create_changeset_rejects_overlong_created_by_before_claim(
    async_client: AsyncClient,
) -> None:
    lineage = await _seed_changeset_lineage()
    idempotency_key = f"changeset-created-by-too-long-{uuid4().hex}"
    body = _valid_create_body(created_by="x" * 129)

    response = await async_client.post(
        _route_path(lineage),
        json=body,
        headers={"Idempotency-Key": idempotency_key},
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INPUT_INVALID"
    assert await _count_changeset_rows(lineage) == (0, 0)
    assert await _count_idempotency_rows(idempotency_key) == 0


async def test_list_changesets_rejects_invalid_cursor(async_client: AsyncClient) -> None:
    lineage = await _seed_changeset_lineage()

    response = await async_client.get(_route_path(lineage), params={"cursor": "not-a-cursor"})

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_CURSOR"


async def test_changeset_routes_hide_deleted_lineage(async_client: AsyncClient) -> None:
    lineage = await _seed_changeset_lineage()
    create_response = await async_client.post(_route_path(lineage), json=_valid_create_body())
    assert create_response.status_code == 201
    change_set_id = create_response.json()["id"]

    await _set_source_file_deleted(lineage)

    create_after_delete = await async_client.post(
        _route_path(lineage),
        json=_valid_create_body(created_by="after-delete"),
    )
    list_after_delete = await async_client.get(_route_path(lineage))
    get_after_delete = await async_client.get(f"{_route_path(lineage)}/{change_set_id}")

    assert create_after_delete.status_code == 404
    assert list_after_delete.status_code == 404
    assert get_after_delete.status_code == 404
    assert await _count_changeset_rows(lineage) == (1, 2)


async def test_create_changeset_replays_idempotent_request(
    async_client: AsyncClient,
) -> None:
    lineage = await _seed_changeset_lineage()
    idempotency_key = f"changeset-create-replay-{uuid4().hex}"
    headers = {"Idempotency-Key": idempotency_key}
    body = _valid_create_body()

    first_response = await async_client.post(_route_path(lineage), json=body, headers=headers)
    assert first_response.status_code == 201

    await _set_source_file_deleted(lineage)

    second_response = await async_client.post(_route_path(lineage), json=body, headers=headers)

    assert second_response.status_code == 201
    assert second_response.json() == first_response.json()
    assert await _count_changeset_rows(lineage) == (1, 2)
    assert await _count_idempotency_rows(idempotency_key) == 1


async def test_create_changeset_rejects_same_idempotency_key_with_different_payload(
    async_client: AsyncClient,
) -> None:
    lineage = await _seed_changeset_lineage()
    idempotency_key = f"changeset-create-conflict-{uuid4().hex}"
    headers = {"Idempotency-Key": idempotency_key}

    first_response = await async_client.post(
        _route_path(lineage),
        json=_valid_create_body(created_by="first"),
        headers=headers,
    )
    conflicting_response = await async_client.post(
        _route_path(lineage),
        json=_valid_create_body(created_by="second"),
        headers=headers,
    )

    assert first_response.status_code == 201
    assert conflicting_response.status_code == 409
    assert conflicting_response.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert await _count_changeset_rows(lineage) == (1, 2)
    assert await _count_idempotency_rows(idempotency_key) == 1
