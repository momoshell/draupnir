"""Integration tests for endpoint-level Idempotency-Key behavior."""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from typing import Any, cast

import httpx
import pytest
from fastapi import HTTPException
from sqlalchemy import func, select

import app.api.v1.files as files_api
import app.api.v1.jobs as jobs_api
import app.db.session as session_module
from app.api.idempotency import (
    IdempotencyReservation,
    build_idempotency_fingerprint,
    get_idempotency_key,
    hash_idempotency_key,
    mark_idempotency_completed,
    replay_idempotency,
)
from app.core.config import settings
from app.models.idempotency_key import IdempotencyKey, IdempotencyStatus
from app.models.job import Job
from app.models.project import Project
from tests.conftest import requires_database


def _headers(idempotency_key: str | None) -> dict[str, str]:
    """Build optional request headers for idempotent requests."""

    if idempotency_key is None:
        return {}
    return {"Idempotency-Key": idempotency_key}


def test_build_idempotency_fingerprint_is_order_independent() -> None:
    """Canonical JSON fingerprinting should not depend on input key order."""

    first = build_idempotency_fingerprint("projects.create", {"name": "A", "description": "B"})
    second = build_idempotency_fingerprint(
        "projects.create",
        {"description": "B", "name": "A"},
    )

    assert first == second


def test_hash_idempotency_key_uses_configured_hmac_secret() -> None:
    """Raw keys should hash to a stable HMAC digest instead of being stored directly."""

    first = hash_idempotency_key("raw-key-1")
    second = hash_idempotency_key("raw-key-1")

    assert first == second
    assert first != "raw-key-1"
    assert len(first) == 64


@pytest.mark.asyncio
async def test_replay_idempotency_returns_retry_after_for_in_progress_conflict() -> None:
    """In-progress idempotency conflicts should short-circuit with Retry-After."""

    key = "unit-in-progress-1"
    payload = {"name": "Blocked Project"}
    fingerprint = build_idempotency_fingerprint("projects.create", payload)
    record = IdempotencyKey(
        key_hash=hash_idempotency_key(key),
        request_fingerprint=fingerprint,
        request_method="POST",
        request_path="/projects",
        status=IdempotencyStatus.IN_PROGRESS.value,
    )

    class _FakeResult:
        def scalar_one_or_none(self) -> IdempotencyKey:
            return record

    class _FakeAsyncSession:
        async def execute(self, _: Any) -> _FakeResult:
            return _FakeResult()

    replay = await replay_idempotency(
        cast(Any, _FakeAsyncSession()),
        key=key,
        fingerprint=fingerprint,
    )

    assert replay is not None
    assert replay.response.status_code == 409
    assert replay.response.headers["Retry-After"] == "1"
    assert json.loads(bytes(replay.response.body)) == {
        "error": {
            "code": "IDEMPOTENCY_CONFLICT",
            "message": "A request with this Idempotency-Key is already in progress.",
            "details": {"reason": "in_progress"},
        }
    }


@pytest.mark.asyncio
async def test_get_idempotency_key_rejects_invalid_rfc9110_token() -> None:
    """Header validation should reject keys outside the RFC 9110 token grammar."""

    with pytest.raises(HTTPException) as exc_info:
        await get_idempotency_key("contains space")

    assert exc_info.value.status_code == 400


@pytest.fixture(autouse=True)
def _set_idempotency_hash_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    """Use a deterministic test secret for idempotency key hashing."""

    monkeypatch.setattr(settings, "idempotency_key_hash_secret", "test-idempotency-secret")


async def _create_project(
    async_client: httpx.AsyncClient,
    *,
    payload: Mapping[str, Any] | None = None,
    idempotency_key: str | None = None,
) -> httpx.Response:
    """Create a project with optional idempotency headers."""

    return await async_client.post(
        "/v1/projects",
        json=dict(payload or {"name": "Idempotency Test Project"}),
        headers=_headers(idempotency_key),
    )


async def _upload_pdf(
    async_client: httpx.AsyncClient,
    *,
    project_id: str,
    filename: str = "plan.pdf",
    content: bytes = b"%PDF-1.7\nidempotency\n",
    idempotency_key: str | None = None,
) -> httpx.Response:
    """Upload a supported PDF file with optional idempotency headers."""

    return await async_client.post(
        f"/v1/projects/{project_id}/files",
        files={"file": (filename, content, "application/pdf")},
        headers=_headers(idempotency_key),
    )


def _get_async_client_app(async_client: httpx.AsyncClient) -> Any:
    """Return the FastAPI app behind the shared async test client."""

    app = getattr(async_client, "app", None)
    if app is not None:
        return app
    return cast(Any, async_client)._transport.app


async def _project_count() -> int:
    """Return the total persisted project row count."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    async with session_maker() as session:
        return int((await session.execute(select(func.count()).select_from(Project))).scalar_one())


async def _job_count_for_file(file_id: str) -> int:
    """Return the number of persisted jobs for a file."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    async with session_maker() as session:
        statement = select(func.count()).select_from(Job).where(Job.file_id == uuid.UUID(file_id))
        return int((await session.execute(statement)).scalar_one())


async def _get_job_for_file(file_id: str) -> Job:
    """Return the persisted job for a file upload."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    async with session_maker() as session:
        return (
            await session.execute(select(Job).where(Job.file_id == uuid.UUID(file_id)))
        ).scalar_one()


async def _update_job_status(job_id: str, *, status_value: str) -> None:
    """Mutate a persisted job status for replay assertions."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    async with session_maker() as session:
        job = await session.get(Job, uuid.UUID(job_id))
        assert job is not None
        job.status = status_value
        await session.commit()


async def _mark_job_failed(job_id: str) -> None:
    """Place a persisted job into the failed state for retry tests."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    async with session_maker() as session:
        job = await session.get(Job, uuid.UUID(job_id))
        assert job is not None
        job.status = "failed"
        job.error_message = "failed"
        await session.commit()


async def _get_idempotency_record(raw_key: str) -> IdempotencyKey:
    """Return a persisted idempotency record by raw key."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    async with session_maker() as session:
        return (
            await session.execute(
                select(IdempotencyKey).where(
                    IdempotencyKey.key_hash == hash_idempotency_key(raw_key)
                )
            )
        ).scalar_one()


async def _get_idempotency_record_or_none(raw_key: str) -> IdempotencyKey | None:
    """Return a persisted idempotency record by raw key when present."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    async with session_maker() as session:
        return (
            await session.execute(
                select(IdempotencyKey).where(
                    IdempotencyKey.key_hash == hash_idempotency_key(raw_key)
                )
            )
        ).scalar_one_or_none()


async def _insert_in_progress_record(
    *,
    raw_key: str,
    fingerprint: str,
    method: str,
    path: str,
) -> None:
    """Insert an in-progress reservation to drive conflict behavior."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    async with session_maker() as session:
        session.add(
            IdempotencyKey(
                key_hash=hash_idempotency_key(raw_key),
                request_fingerprint=fingerprint,
                request_method=method,
                request_path=path,
                status=IdempotencyStatus.IN_PROGRESS.value,
            )
        )
        await session.commit()


@requires_database
class TestEndpointIdempotency:
    """Mutating endpoint replays and conflicts should match the TRD contract."""

    async def test_create_project_replays_completed_response(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """POST /projects should replay the original 201 response for the same key."""

        _ = self
        _ = cleanup_projects
        key = "project-create-1"
        payload = {"name": "Replay Project", "description": "first body"}

        first = await _create_project(async_client, payload=payload, idempotency_key=key)
        second = await _create_project(async_client, payload=payload, idempotency_key=key)

        assert first.status_code == 201
        assert second.status_code == 201
        assert second.json() == first.json()
        assert await _project_count() == 1

    async def test_update_project_replays_completed_response(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """PATCH /projects/{id} should replay the original 200 response for the same key."""

        _ = self
        _ = cleanup_projects
        created = cast(dict[str, Any], (await _create_project(async_client)).json())
        key = "project-update-1"
        payload = {"description": "updated once"}

        first = await async_client.patch(
            f"/v1/projects/{created['id']}",
            json=payload,
            headers=_headers(key),
        )
        second = await async_client.patch(
            f"/v1/projects/{created['id']}",
            json=payload,
            headers=_headers(key),
        )

        assert first.status_code == 200
        assert second.status_code == 200
        assert second.json() == first.json()

    async def test_delete_project_replays_completed_empty_response(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """DELETE /projects/{id} should replay the original 204 response for the same key."""

        _ = self
        _ = cleanup_projects
        created = cast(dict[str, Any], (await _create_project(async_client)).json())
        key = "project-delete-1"

        first = await async_client.delete(
            f"/v1/projects/{created['id']}",
            headers=_headers(key),
        )
        second = await async_client.delete(
            f"/v1/projects/{created['id']}",
            headers=_headers(key),
        )

        assert first.status_code == 204
        assert second.status_code == 204
        assert first.content == b""
        assert second.content == b""

    async def test_invalid_idempotency_key_is_rejected(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """Invalid header tokens should fail RFC 9110 token validation."""

        _ = self
        _ = cleanup_projects

        response = await _create_project(
            async_client,
            payload={"name": "Bad Key"},
            idempotency_key="contains space",
        )

        assert response.status_code == 400
        assert response.json() == {
            "error": {
                "code": "INPUT_INVALID",
                "message": "Idempotency-Key must be a valid RFC 9110 token.",
                "details": None,
            }
        }

    async def test_matching_in_progress_reservation_returns_conflict_with_retry_after(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """A matching in-progress reservation should return the shared 409 conflict shape."""

        _ = self
        _ = cleanup_projects
        key = "project-in-progress-1"
        payload = {"name": "Blocked Project"}
        fingerprint = build_idempotency_fingerprint("projects.create", payload)
        await _insert_in_progress_record(
            raw_key=key,
            fingerprint=fingerprint,
            method="POST",
            path="/projects",
        )

        response = await _create_project(async_client, payload=payload, idempotency_key=key)

        assert response.status_code == 409
        assert response.headers["Retry-After"] == "1"
        assert response.json() == {
            "error": {
                "code": "IDEMPOTENCY_CONFLICT",
                "message": "A request with this Idempotency-Key is already in progress.",
                "details": {"reason": "in_progress"},
            }
        }

    async def test_update_matching_in_progress_reservation_returns_conflict_with_retry_after(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """PATCH /projects/{id} should include Retry-After on matching in-progress conflicts."""

        _ = self
        _ = cleanup_projects
        created = cast(dict[str, Any], (await _create_project(async_client)).json())
        key = "project-update-in-progress-1"
        payload = {"description": "Blocked update"}
        fingerprint = build_idempotency_fingerprint(
            f"projects.update:{created['id']}",
            payload,
        )
        await _insert_in_progress_record(
            raw_key=key,
            fingerprint=fingerprint,
            method="PATCH",
            path=f"/projects/{created['id']}",
        )

        response = await async_client.patch(
            f"/v1/projects/{created['id']}",
            json=payload,
            headers=_headers(key),
        )

        assert response.status_code == 409
        assert response.headers["Retry-After"] == "1"
        assert response.json() == {
            "error": {
                "code": "IDEMPOTENCY_CONFLICT",
                "message": "A request with this Idempotency-Key is already in progress.",
                "details": {"reason": "in_progress"},
            }
        }

    async def test_upload_replays_completed_response_without_duplicate_rows(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """POST /files should replay the original 201 response and avoid duplicate rows."""

        _ = self
        _ = cleanup_projects
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        key = "file-upload-1"

        first = await _upload_pdf(async_client, project_id=project["id"], idempotency_key=key)
        second = await _upload_pdf(async_client, project_id=project["id"], idempotency_key=key)

        assert first.status_code == 201
        assert second.status_code == 201
        assert second.json() == first.json()
        assert len(enqueued_job_ids) == 1
        assert await _job_count_for_file(first.json()["id"]) == 1

    async def test_upload_same_key_different_request_returns_request_mismatch(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Reusing the same upload key for a different fingerprint should fail with 409."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        key = "file-upload-mismatch-1"

        first = await _upload_pdf(async_client, project_id=project["id"], idempotency_key=key)
        second = await _upload_pdf(
            async_client,
            project_id=project["id"],
            filename="different.pdf",
            content=b"%PDF-1.7\ndifferent\n",
            idempotency_key=key,
        )

        assert first.status_code == 201
        assert second.status_code == 409
        assert second.json() == {
            "error": {
                "code": "IDEMPOTENCY_CONFLICT",
                "message": (
                    "This Idempotency-Key is already associated with a different request."
                ),
                "details": {"reason": "request_mismatch"},
            }
        }

    async def test_upload_enqueue_failure_replays_sanitized_error_snapshot(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Enqueue failures must replay the original sanitized 500 after durable writes."""

        _ = self
        _ = cleanup_projects
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        key = "file-upload-enqueue-failure-1"

        def _fail_enqueue(_: uuid.UUID) -> None:
            raise RuntimeError("broker unavailable")

        monkeypatch.setattr(files_api, "enqueue_ingest_job", _fail_enqueue)

        first = await _upload_pdf(async_client, project_id=project["id"], idempotency_key=key)
        second = await _upload_pdf(async_client, project_id=project["id"], idempotency_key=key)
        record = await _get_idempotency_record(key)

        assert first.status_code == 500
        assert first.json()["error"]["message"] == "Failed to enqueue ingest job"
        assert second.status_code == 500
        assert second.json() == first.json()
        assert record.status == IdempotencyStatus.COMPLETED.value
        assert record.response_status_code == 500
        assert record.response_body_json == first.json()

    async def test_upload_storage_failure_replays_sanitized_error_snapshot(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Storage failures after claim must finalize and replay the sanitized 500."""

        _ = self
        _ = cleanup_projects
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        key = "file-upload-storage-failure-1"

        class _FailingStorage:
            async def put(self, *_: Any, **__: Any) -> Any:
                raise OSError("disk full")

        app = _get_async_client_app(async_client)
        get_storage_dependency = cast(Any, files_api).get_storage
        app.dependency_overrides[get_storage_dependency] = lambda: _FailingStorage()
        try:
            first = await _upload_pdf(async_client, project_id=project["id"], idempotency_key=key)
            second = await _upload_pdf(async_client, project_id=project["id"], idempotency_key=key)
        finally:
            app.dependency_overrides.pop(get_storage_dependency, None)

        record = await _get_idempotency_record(key)

        assert first.status_code == 500
        assert first.json() == {
            "error": {
                "code": "STORAGE_FAILED",
                "message": "Failed to persist uploaded file.",
                "details": None,
            }
        }
        assert second.status_code == 500
        assert second.json() == first.json()
        assert record.status == IdempotencyStatus.COMPLETED.value
        assert record.response_status_code == 500
        assert record.response_body_json == first.json()
        assert enqueued_job_ids == []

    async def test_mark_idempotency_completed_does_not_overwrite_completed_snapshot(
        self,
        cleanup_projects: None,
    ) -> None:
        """Completed snapshots must remain immutable once a response is finalized."""

        _ = self
        _ = cleanup_projects
        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        raw_key = "idempotency-completed-once-1"

        async with session_maker() as session:
            record = IdempotencyKey(
                key_hash=hash_idempotency_key(raw_key),
                request_fingerprint=build_idempotency_fingerprint(
                    "jobs.retry:test",
                    {"job_id": "job-1"},
                ),
                request_method="POST",
                request_path="/jobs/job-1/retry",
                status=IdempotencyStatus.IN_PROGRESS.value,
            )
            session.add(record)
            await session.commit()
            await session.refresh(record)

            reservation = IdempotencyReservation(record_id=record.id)
            first_body = {"job_id": "job-1", "status": "pending"}
            await mark_idempotency_completed(
                session,
                reservation,
                status_code=202,
                response_body=first_body,
            )
            await session.commit()
            await session.refresh(record)
            first_completed_at = record.completed_at

            await mark_idempotency_completed(
                session,
                reservation,
                status_code=500,
                response_body={
                    "error": {
                        "code": "INTERNAL_ERROR",
                        "message": "Failed to enqueue ingest job",
                        "details": None,
                    }
                },
            )
            await session.commit()
            await session.refresh(record)

            assert record.status == IdempotencyStatus.COMPLETED.value
            assert record.response_status_code == 202
            assert record.response_body_json == first_body
            assert record.completed_at == first_completed_at

    async def test_reprocess_replays_original_job_snapshot(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """POST /reprocess should replay the original 202 job snapshot for the same key."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        uploaded = cast(
            dict[str, Any],
            (await _upload_pdf(async_client, project_id=project["id"])).json(),
        )
        key = "file-reprocess-1"
        payload = {"extraction_profile": {"profile_version": "v0.1"}}

        first = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json=payload,
            headers=_headers(key),
        )
        assert first.status_code == 202
        await _update_job_status(first.json()["id"], status_value="running")

        second = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json=payload,
            headers=_headers(key),
        )

        assert second.status_code == 202
        assert second.json() == first.json()
        assert await _job_count_for_file(uploaded["id"]) == 2

    async def test_reprocess_invalid_extraction_profile_replays_original_not_found(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Precondition failures must not leave reprocess keys stuck in progress."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        uploaded = cast(
            dict[str, Any],
            (await _upload_pdf(async_client, project_id=project["id"])).json(),
        )
        key = "file-reprocess-missing-profile-1"
        payload = {"extraction_profile_id": str(uuid.uuid4())}

        first = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json=payload,
            headers=_headers(key),
        )
        second = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json=payload,
            headers=_headers(key),
        )

        assert first.status_code == 404
        assert second.status_code == 404
        assert second.json() == first.json()
        assert await _get_idempotency_record_or_none(key) is None

    async def test_reprocess_enqueue_failure_replays_sanitized_error_snapshot(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Reprocess enqueue failures must finalize and replay the sanitized 500."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        uploaded = cast(
            dict[str, Any],
            (await _upload_pdf(async_client, project_id=project["id"])).json(),
        )
        key = "file-reprocess-enqueue-failure-1"
        payload = {"extraction_profile": {"profile_version": "v0.1"}}

        def _fail_enqueue(_: uuid.UUID) -> None:
            raise RuntimeError("broker unavailable")

        monkeypatch.setattr(files_api, "enqueue_ingest_job", _fail_enqueue)

        first = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json=payload,
            headers=_headers(key),
        )
        second = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json=payload,
            headers=_headers(key),
        )
        record = await _get_idempotency_record(key)

        assert first.status_code == 500
        assert first.json()["error"]["message"] == "Failed to enqueue ingest job"
        assert second.status_code == 500
        assert second.json() == first.json()
        assert record.status == IdempotencyStatus.COMPLETED.value
        assert record.response_status_code == 500
        assert record.response_body_json == first.json()

    async def test_cancel_replays_original_job_snapshot(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """POST /cancel should replay the original 202 job snapshot for the same key."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        uploaded = cast(
            dict[str, Any],
            (await _upload_pdf(async_client, project_id=project["id"])).json(),
        )
        job = await _get_job_for_file(uploaded["id"])
        key = "job-cancel-1"

        first = await async_client.post(f"/v1/jobs/{job.id}/cancel", headers=_headers(key))
        assert first.status_code == 202
        await _update_job_status(first.json()["id"], status_value="cancelled")

        second = await async_client.post(f"/v1/jobs/{job.id}/cancel", headers=_headers(key))

        assert second.status_code == 202
        assert second.json() == first.json()

    async def test_retry_replays_original_job_snapshot(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """POST /retry should replay the original 202 job snapshot for the same key."""

        _ = self
        _ = cleanup_projects
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        uploaded = cast(
            dict[str, Any],
            (await _upload_pdf(async_client, project_id=project["id"])).json(),
        )

        def _record_retry_enqueue(job_id: uuid.UUID) -> None:
            enqueued_job_ids.append(str(job_id))

        monkeypatch.setattr(jobs_api, "enqueue_ingest_job", _record_retry_enqueue)

        job = await _get_job_for_file(uploaded["id"])
        await _mark_job_failed(str(job.id))
        key = "job-retry-1"
        enqueued_before_retry = len(enqueued_job_ids)

        first = await async_client.post(f"/v1/jobs/{job.id}/retry", headers=_headers(key))
        assert first.status_code == 202
        assert len(enqueued_job_ids) == enqueued_before_retry + 1
        await _update_job_status(first.json()["id"], status_value="running")

        second = await async_client.post(f"/v1/jobs/{job.id}/retry", headers=_headers(key))

        assert second.status_code == 202
        assert second.json() == first.json()
        assert len(enqueued_job_ids) == enqueued_before_retry + 1

    async def test_retry_enqueue_failure_replays_sanitized_error_snapshot(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry enqueue failures must finalize and replay the sanitized 500."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        uploaded = cast(
            dict[str, Any],
            (await _upload_pdf(async_client, project_id=project["id"])).json(),
        )
        job = await _get_job_for_file(uploaded["id"])
        await _mark_job_failed(str(job.id))
        key = "job-retry-enqueue-failure-1"

        def _fail_enqueue(_: uuid.UUID) -> None:
            raise RuntimeError("broker unavailable")

        monkeypatch.setattr(jobs_api, "enqueue_ingest_job", _fail_enqueue)

        first = await async_client.post(f"/v1/jobs/{job.id}/retry", headers=_headers(key))
        second = await async_client.post(f"/v1/jobs/{job.id}/retry", headers=_headers(key))
        record = await _get_idempotency_record(key)

        assert first.status_code == 500
        assert first.json()["error"]["message"] == "Failed to enqueue ingest job"
        assert second.status_code == 500
        assert second.json() == first.json()
        assert record.status == IdempotencyStatus.COMPLETED.value
        assert record.response_status_code == 500
        assert record.response_body_json == first.json()
