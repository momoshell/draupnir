"""Integration tests for public jobs API behaviors."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

import app.api.v1.files as files_api
import app.db.session as session_module
import app.jobs.worker as worker_module
from app.models.job_event import JobEvent
from tests.conftest import requires_database
from tests.jobs_test_helpers import (
    _TEST_UPLOAD_BODY,
    _create_job_event,
    _create_project,
    _get_job_for_file,
    _upload_file,
    fake_ingestion_runner,
)


@pytest.mark.usefixtures(fake_ingestion_runner.__name__)
@requires_database
class TestJobs:
    """Tests for upload enqueueing and jobs read/events API endpoints."""

    async def test_upload_file_enqueues_ingest_job(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Uploading a file should enqueue the persisted ingest job."""
        _ = self
        _ = cleanup_projects

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        assert enqueued_job_ids == [str(job.id)]
        assert job.status == "pending"
        assert job.attempts == 0
        assert job.enqueue_status == "published"

    async def test_upload_file_succeeds_when_publish_is_deferred(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Uploading should succeed once durable enqueue intent commits."""
        _ = self
        _ = cleanup_projects

        async def _skip_publish(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (job_id, publisher, suppress_exceptions, kwargs)
            return False

        monkeypatch.setattr(files_api, "publish_job_enqueue_intent", _skip_publish)

        project = await _create_project(async_client)
        response = await async_client.post(
            f"/v1/projects/{project['id']}/files",
            files={"file": ("plan.pdf", _TEST_UPLOAD_BODY, "application/pdf")},
        )

        assert response.status_code == 201
        payload = response.json()
        job = await _get_job_for_file(payload["id"])

        assert job.status == "pending"
        assert job.attempts == 0
        assert job.error_code is None
        assert job.error_message is None
        assert job.started_at is None
        assert job.finished_at is None
        assert job.enqueue_status == "pending"
        assert job.enqueue_attempts == 0

    async def test_upload_file_replays_success_when_publish_is_deferred_after_commit(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Upload idempotency should snapshot success before best-effort publish."""
        _ = self
        _ = cleanup_projects

        async def _skip_publish(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (job_id, publisher, suppress_exceptions, kwargs)
            return False

        monkeypatch.setattr(files_api, "publish_job_enqueue_intent", _skip_publish)

        project = await _create_project(async_client)
        headers = {"Idempotency-Key": "upload-outbox-replay"}

        first = await async_client.post(
            f"/v1/projects/{project['id']}/files",
            files={"file": ("plan.pdf", _TEST_UPLOAD_BODY, "application/pdf")},
            headers=headers,
        )
        second = await async_client.post(
            f"/v1/projects/{project['id']}/files",
            files={"file": ("plan.pdf", _TEST_UPLOAD_BODY, "application/pdf")},
            headers=headers,
        )

        assert first.status_code == 201
        assert second.status_code == 201
        assert second.json() == first.json()

        job = await _get_job_for_file(first.json()["id"])
        assert job.status == "pending"
        assert job.enqueue_status == "pending"

    async def test_upload_file_succeeds_when_enqueue_claim_raises_after_commit(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Upload should not fail if post-commit enqueue bookkeeping raises before claim."""
        _ = self
        _ = cleanup_projects

        async def _fail_claim(_: uuid.UUID) -> worker_module._EnqueueIntentLease | None:
            raise RuntimeError("transient enqueue claim failure")

        monkeypatch.setattr(worker_module, "_claim_job_enqueue_intent", _fail_claim)

        project = await _create_project(async_client)
        response = await async_client.post(
            f"/v1/projects/{project['id']}/files",
            files={"file": ("plan.pdf", _TEST_UPLOAD_BODY, "application/pdf")},
        )

        assert response.status_code == 201
        job = await _get_job_for_file(response.json()["id"])
        assert job.status == "pending"
        assert job.enqueue_status == "pending"

    async def test_get_job_returns_persisted_state(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """GET should return the persisted job state for a known job."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        response = await async_client.get(f"/v1/jobs/{job.id}")
        assert response.status_code == 200

        data = response.json()
        assert data["id"] == str(job.id)
        assert data["project_id"] == project["id"]
        assert data["file_id"] == uploaded["id"]
        assert data["extraction_profile_id"] == str(job.extraction_profile_id)
        assert data["job_type"] == "ingest"
        assert data["status"] == "pending"
        assert data["attempts"] == 0
        assert data["max_attempts"] == 3
        assert data["cancel_requested"] is False
        assert data["error_code"] is None
        assert data["error_message"] is None
        assert data["started_at"] is None
        assert data["finished_at"] is None
        assert data["created_at"] is not None

    async def test_get_job_not_found(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """GET should return the standard Job 404 envelope for unknown ids."""
        _ = self
        _ = cleanup_projects

        missing_job_id = uuid.uuid4()
        response = await async_client.get(f"/v1/jobs/{missing_job_id}")
        assert response.status_code == 404
        assert response.json() == {
            "error": {
                "code": "NOT_FOUND",
                "message": f"Job with identifier '{missing_job_id}' not found",
                "details": None,
            }
        }

    async def test_list_job_events_returns_404_for_unknown_job(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """GET events should return the standard Job 404 envelope for unknown ids."""
        _ = self
        _ = cleanup_projects

        missing_job_id = uuid.uuid4()
        response = await async_client.get(f"/v1/jobs/{missing_job_id}/events")

        assert response.status_code == 404
        assert response.json() == {
            "error": {
                "code": "NOT_FOUND",
                "message": f"Job with identifier '{missing_job_id}' not found",
                "details": None,
            }
        }

    async def test_list_job_events_returns_chronological_order(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """GET events should return ascending created-at ordered results."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        base = datetime.now(UTC).replace(microsecond=0)
        await _create_job_event(
            job.id,
            level="info",
            message="third",
            created_at=base + timedelta(seconds=2),
        )
        await _create_job_event(
            job.id,
            level="info",
            message="first",
            created_at=base,
        )
        await _create_job_event(
            job.id,
            level="info",
            message="second",
            created_at=base + timedelta(seconds=1),
        )

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200

        data = response.json()
        assert [event["message"] for event in data["items"]] == [
            "first",
            "second",
            "third",
        ]
        assert data["next_cursor"] is None

    async def test_list_job_events_supports_cursor_pagination(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """GET events should support opaque cursor pagination."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        base = datetime.now(UTC).replace(microsecond=0)
        await _create_job_event(job.id, level="info", message="first", created_at=base)
        await _create_job_event(
            job.id,
            level="info",
            message="second",
            created_at=base + timedelta(seconds=1),
        )
        await _create_job_event(
            job.id,
            level="info",
            message="third",
            created_at=base + timedelta(seconds=2),
        )

        first_response = await async_client.get(f"/v1/jobs/{job.id}/events?limit=2")
        assert first_response.status_code == 200
        first_data = first_response.json()
        assert [event["message"] for event in first_data["items"]] == ["first", "second"]
        assert first_data["next_cursor"] is not None

        second_response = await async_client.get(
            f"/v1/jobs/{job.id}/events?limit=2&cursor={first_data['next_cursor']}"
        )
        assert second_response.status_code == 200
        second_data = second_response.json()
        assert [event["message"] for event in second_data["items"]] == ["third"]
        assert second_data["next_cursor"] is None

    async def test_list_job_events_preserves_same_transaction_emission_order_on_ties(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """GET events should preserve insertion order for same-timestamp events."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        tied_created_at = datetime.now(UTC).replace(microsecond=0)
        first_id = uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
        second_id = uuid.UUID("00000000-0000-0000-0000-000000000001")

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            session.add(
                JobEvent(
                    id=first_id,
                    job_id=job.id,
                    level="info",
                    message="first emitted",
                    created_at=tied_created_at,
                )
            )
            session.add(
                JobEvent(
                    id=second_id,
                    job_id=job.id,
                    level="info",
                    message="second emitted",
                    created_at=tied_created_at,
                )
            )
            await session.commit()

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200

        data = response.json()
        assert [event["message"] for event in data["items"]] == [
            "first emitted",
            "second emitted",
        ]
        assert data["next_cursor"] is None

    async def test_list_job_events_invalid_cursor_returns_error_envelope(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """GET events should return standard envelope for invalid cursor values."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        response = await async_client.get(f"/v1/jobs/{job.id}/events?cursor=not-base64")

        assert response.status_code == 400
        assert response.json() == {
            "error": {
                "code": "INVALID_CURSOR",
                "message": "Invalid cursor format",
                "details": None,
            }
        }

    async def test_list_job_events_includes_worker_lifecycle_events(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """GET events should expose worker-emitted lifecycle events."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await worker_module.process_ingest_job(job.id)

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200
        data = response.json()

        assert [event["message"] for event in data["items"]] == [
            "Job started",
            "Job succeeded",
        ]
        assert [event["data_json"]["status"] for event in data["items"]] == [
            "running",
            "succeeded",
        ]
        assert data["next_cursor"] is None
