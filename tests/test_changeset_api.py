from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

import app.db.session as session_module
from app.api.idempotency import hash_idempotency_key
from app.api.v1.revision_routes import changesets as changeset_routes
from app.cad.changeset.conflicts import build_stale_base_conflict
from app.cad.changeset.contracts import RevisionRef
from app.cad.changeset.loading import ChangeSetApplyLoadError
from app.core.exceptions import raise_not_found
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
            canonical_entity_schema_version="1.0",
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


def _action_route_path(lineage: ChangesetLineage, change_set_id: UUID, action: str) -> str:
    return f"{_route_path(lineage)}/{change_set_id}/{action}"


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


async def _approve_changeset_with_valid_validation_result(
    lineage: ChangesetLineage,
    change_set_id: UUID,
) -> UUID:
    assert session_module.AsyncSessionLocal is not None

    async with session_module.AsyncSessionLocal() as session:
        change_set = await session.get(CadChangeSet, change_set_id)
        assert change_set is not None
        change_set.status = "approved"
        validation_result = CadChangeSetValidationResult(
            project_id=lineage.project_id,
            change_set_id=change_set_id,
            validation_status="valid",
            validator_name="api-test",
            validator_version="1",
            result_json={"status": "valid"},
        )
        session.add(validation_result)
        await session.flush()
        validation_result_id = validation_result.id
        await session.commit()

    return validation_result_id


async def _get_changeset_apply_job_input(job_id: UUID) -> ChangeSetApplyJobInput | None:
    assert session_module.AsyncSessionLocal is not None

    async with session_module.AsyncSessionLocal() as session:
        return await session.get(ChangeSetApplyJobInput, job_id)


async def _count_changeset_apply_contract_rows(
    lineage: ChangesetLineage,
    change_set_id: UUID,
) -> tuple[int, int]:
    assert session_module.AsyncSessionLocal is not None

    async with session_module.AsyncSessionLocal() as session:
        job_count = await session.scalar(
            select(func.count())
            .select_from(Job)
            .where(
                Job.project_id == lineage.project_id,
                Job.file_id == lineage.file_id,
                Job.base_revision_id == lineage.revision_id,
                Job.job_type == JobType.CHANGESET_APPLY.value,
            )
        )
        input_count = await session.scalar(
            select(func.count())
            .select_from(ChangeSetApplyJobInput)
            .where(
                ChangeSetApplyJobInput.project_id == lineage.project_id,
                ChangeSetApplyJobInput.source_file_id == lineage.file_id,
                ChangeSetApplyJobInput.drawing_revision_id == lineage.revision_id,
                ChangeSetApplyJobInput.change_set_id == change_set_id,
            )
        )

    assert job_count is not None
    assert input_count is not None
    return job_count, input_count


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


async def test_validate_changeset_returns_change_set_and_validation_result(
    async_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lineage = await _seed_changeset_lineage()
    create_response = await async_client.post(_route_path(lineage), json=_valid_create_body())
    assert create_response.status_code == 201
    change_set_id = UUID(create_response.json()["id"])

    async def fake_validate_change_set(
        db: AsyncSession,
        validated_change_set_id: UUID,
    ) -> CadChangeSetValidationResult:
        validation_result = CadChangeSetValidationResult(
            project_id=lineage.project_id,
            change_set_id=validated_change_set_id,
            validation_status="valid",
            validator_name="api-test",
            validator_version="1",
            result_json={"status": "valid"},
        )
        db.add(validation_result)
        await db.flush()
        return validation_result

    monkeypatch.setattr(changeset_routes, "_validate_change_set", fake_validate_change_set)

    response = await async_client.post(_action_route_path(lineage, change_set_id, "validate"))

    assert response.status_code == 200
    body = response.json()
    assert body["change_set"]["id"] == str(change_set_id)
    assert body["change_set"]["base_revision_id"] == str(lineage.revision_id)
    assert body["validation_result"]["change_set_id"] == str(change_set_id)
    assert body["validation_result"]["validation_status"] == "valid"
    assert body["validation_result"]["result_json"] == {"status": "valid"}


async def test_apply_changeset_creates_pending_job_and_immutable_input(
    async_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lineage = await _seed_changeset_lineage()
    create_response = await async_client.post(_route_path(lineage), json=_valid_create_body())
    assert create_response.status_code == 201
    change_set_id = UUID(create_response.json()["id"])
    validation_result_id = await _approve_changeset_with_valid_validation_result(
        lineage,
        change_set_id,
    )
    load_calls: list[tuple[UUID, UUID]] = []

    async def fake_load_change_set_apply_input(
        _db: AsyncSession,
        *,
        project_id: UUID,
        change_set_id: UUID,
    ) -> object:
        load_calls.append((project_id, change_set_id))
        return object()

    monkeypatch.setattr(
        changeset_routes,
        "_load_change_set_apply_input",
        fake_load_change_set_apply_input,
    )
    published_job_ids: list[UUID] = []

    async def fake_publish_job_enqueue_intent(
        job_id: UUID,
        *,
        publisher: object | None = None,
        suppress_exceptions: bool = False,
    ) -> bool:
        published_job_ids.append(job_id)
        assert publisher is changeset_routes._enqueue_changeset_apply_job
        assert suppress_exceptions is True
        assert session_module.AsyncSessionLocal is not None
        async with session_module.AsyncSessionLocal() as session:
            persisted_job = await session.get(Job, job_id)
            assert persisted_job is not None
        return True

    monkeypatch.setattr(
        changeset_routes,
        "_publish_job_enqueue_intent",
        fake_publish_job_enqueue_intent,
    )

    response = await async_client.post(_action_route_path(lineage, change_set_id, "apply"))

    assert response.status_code == 202
    body = response.json()
    assert body["project_id"] == str(lineage.project_id)
    assert body["file_id"] == str(lineage.file_id)
    assert body["base_revision_id"] == str(lineage.revision_id)
    assert body["job_type"] == JobType.CHANGESET_APPLY.value
    assert body["status"] == "pending"
    assert load_calls == [(lineage.project_id, change_set_id)]

    job_id = UUID(body["id"])
    apply_input = await _get_changeset_apply_job_input(job_id)
    assert apply_input is not None
    assert apply_input.project_id == lineage.project_id
    assert apply_input.source_file_id == lineage.file_id
    assert apply_input.drawing_revision_id == lineage.revision_id
    assert apply_input.change_set_id == change_set_id
    assert apply_input.latest_validation_result_id == validation_result_id
    assert apply_input.latest_validation_status == "valid"

    assert session_module.AsyncSessionLocal is not None
    async with session_module.AsyncSessionLocal() as session:
        job = await session.get(Job, job_id)
    assert job is not None
    assert job.enqueue_status == "pending"
    assert job.enqueue_attempts == 0
    assert job.enqueue_published_at is None
    assert published_job_ids == [job_id]


async def test_apply_changeset_reuses_existing_pending_job_without_duplicate_input(
    async_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lineage = await _seed_changeset_lineage()
    create_response = await async_client.post(_route_path(lineage), json=_valid_create_body())
    assert create_response.status_code == 201
    change_set_id = UUID(create_response.json()["id"])
    await _approve_changeset_with_valid_validation_result(lineage, change_set_id)
    load_calls: list[tuple[UUID, UUID]] = []

    async def fake_load_change_set_apply_input(
        _db: AsyncSession,
        *,
        project_id: UUID,
        change_set_id: UUID,
    ) -> object:
        load_calls.append((project_id, change_set_id))
        return object()

    monkeypatch.setattr(
        changeset_routes,
        "_load_change_set_apply_input",
        fake_load_change_set_apply_input,
    )

    first_response = await async_client.post(_action_route_path(lineage, change_set_id, "apply"))
    second_response = await async_client.post(
        _action_route_path(lineage, change_set_id, "apply"),
        headers={"Idempotency-Key": f"changeset-apply-second-{uuid4().hex}"},
    )
    third_response = await async_client.post(_action_route_path(lineage, change_set_id, "apply"))

    assert first_response.status_code == 202
    assert second_response.status_code == 202
    assert third_response.status_code == 202
    assert second_response.json() == first_response.json()
    assert third_response.json() == first_response.json()
    assert load_calls == [(lineage.project_id, change_set_id)]
    assert await _count_changeset_apply_contract_rows(lineage, change_set_id) == (1, 1)


async def test_apply_changeset_stale_base_returns_replayable_revision_conflict(
    async_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lineage = await _seed_changeset_lineage()
    create_response = await async_client.post(_route_path(lineage), json=_valid_create_body())
    assert create_response.status_code == 201
    change_set_id = UUID(create_response.json()["id"])
    await _approve_changeset_with_valid_validation_result(lineage, change_set_id)
    current_revision_id = uuid4()
    load_calls: list[tuple[UUID, UUID]] = []

    async def fake_load_change_set_apply_input(
        _db: AsyncSession,
        *,
        project_id: UUID,
        change_set_id: UUID,
    ) -> object:
        load_calls.append((project_id, change_set_id))
        conflict = build_stale_base_conflict(
            change_set_id=change_set_id,
            base_revision=RevisionRef(
                revision_id=lineage.revision_id,
                revision_sequence=1,
            ),
            current_revision=RevisionRef(
                revision_id=current_revision_id,
                revision_sequence=2,
            ),
        )
        assert conflict is not None
        return conflict

    monkeypatch.setattr(
        changeset_routes,
        "_load_change_set_apply_input",
        fake_load_change_set_apply_input,
    )
    headers = {"Idempotency-Key": f"changeset-apply-stale-{uuid4().hex}"}

    first_response = await async_client.post(
        _action_route_path(lineage, change_set_id, "apply"),
        headers=headers,
    )
    second_response = await async_client.post(
        _action_route_path(lineage, change_set_id, "apply"),
        headers=headers,
    )

    assert first_response.status_code == 409
    assert second_response.status_code == 409
    assert second_response.json() == first_response.json()
    error = first_response.json()["error"]
    assert error["code"] == "REVISION_CONFLICT"
    assert error["details"] == {
        "base_revision_id": str(lineage.revision_id),
        "base_revision_sequence": 1,
        "current_revision_id": str(current_revision_id),
        "current_revision_sequence": 2,
        "change_set_id": str(change_set_id),
    }
    assert load_calls == [(lineage.project_id, change_set_id)]
    assert await _count_changeset_apply_contract_rows(lineage, change_set_id) == (0, 0)


async def test_apply_changeset_load_error_replays_input_invalid_without_job(
    async_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lineage = await _seed_changeset_lineage()
    create_response = await async_client.post(_route_path(lineage), json=_valid_create_body())
    assert create_response.status_code == 201
    change_set_id = UUID(create_response.json()["id"])
    await _approve_changeset_with_valid_validation_result(lineage, change_set_id)
    load_calls: list[tuple[UUID, UUID]] = []

    async def fake_load_change_set_apply_input(
        _db: AsyncSession,
        *,
        project_id: UUID,
        change_set_id: UUID,
    ) -> object:
        load_calls.append((project_id, change_set_id))
        raise ChangeSetApplyLoadError("not approved")

    monkeypatch.setattr(
        changeset_routes,
        "_load_change_set_apply_input",
        fake_load_change_set_apply_input,
    )
    headers = {"Idempotency-Key": f"changeset-apply-load-error-{uuid4().hex}"}

    first_response = await async_client.post(
        _action_route_path(lineage, change_set_id, "apply"),
        headers=headers,
    )
    second_response = await async_client.post(
        _action_route_path(lineage, change_set_id, "apply"),
        headers=headers,
    )

    assert first_response.status_code == 400
    assert second_response.status_code == 400
    assert second_response.json() == first_response.json()
    error = first_response.json()["error"]
    assert error["code"] == "INPUT_INVALID"
    assert error["details"] == {"code": "ChangeSetApplyLoadError"}
    assert load_calls == [(lineage.project_id, change_set_id)]
    assert await _count_changeset_apply_contract_rows(lineage, change_set_id) == (0, 0)


@pytest.mark.parametrize("action", ["validate", "apply"])
async def test_changeset_action_postclaim_not_found_replays_with_same_idempotency_key(
    async_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    action: str,
) -> None:
    lineage = await _seed_changeset_lineage()
    create_response = await async_client.post(_route_path(lineage), json=_valid_create_body())
    assert create_response.status_code == 201
    change_set_id = UUID(create_response.json()["id"])
    if action == "apply":
        await _approve_changeset_with_valid_validation_result(lineage, change_set_id)

    require_calls: list[tuple[UUID, UUID, bool]] = []

    async def fake_require_active_revision_changeset(
        db: AsyncSession,
        revision_id: UUID,
        patched_change_set_id: UUID,
        *,
        for_update: bool = False,
    ) -> tuple[DrawingRevision, CadChangeSet]:
        require_calls.append((revision_id, patched_change_set_id, for_update))
        if for_update:
            raise_not_found("change_set", str(patched_change_set_id))

        revision = await db.get(DrawingRevision, revision_id)
        change_set = await db.get(CadChangeSet, patched_change_set_id)
        assert revision is not None
        assert change_set is not None
        return revision, change_set

    monkeypatch.setattr(
        changeset_routes,
        "_require_active_revision_changeset",
        fake_require_active_revision_changeset,
    )

    if action == "validate":

        async def unexpected_validate_change_set(
            _db: AsyncSession,
            _change_set_id: UUID,
        ) -> CadChangeSetValidationResult:
            raise AssertionError("validate should not run after postclaim not found")

        monkeypatch.setattr(
            changeset_routes,
            "_validate_change_set",
            unexpected_validate_change_set,
        )
    else:

        async def unexpected_load_change_set_apply_input(
            _db: AsyncSession,
            *,
            project_id: UUID,
            change_set_id: UUID,
        ) -> object:
            raise AssertionError("apply load should not run after postclaim not found")

        monkeypatch.setattr(
            changeset_routes,
            "_load_change_set_apply_input",
            unexpected_load_change_set_apply_input,
        )

    headers = {"Idempotency-Key": f"changeset-{action}-postclaim-not-found-{uuid4().hex}"}
    route_path = _action_route_path(lineage, change_set_id, action)

    first_response = await async_client.post(route_path, headers=headers)
    second_response = await async_client.post(route_path, headers=headers)

    assert first_response.status_code == 404
    assert second_response.status_code == 404
    assert second_response.json() == first_response.json()
    assert first_response.json()["error"]["code"] == "NOT_FOUND"
    assert require_calls == [
        (lineage.revision_id, change_set_id, False),
        (lineage.revision_id, change_set_id, True),
    ]
    assert await _count_idempotency_rows(headers["Idempotency-Key"]) == 1


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
