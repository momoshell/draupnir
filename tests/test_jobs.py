"""Integration tests for persisted job status and worker transitions."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import httpx
import pytest
from sqlalchemy import select

import app.api.v1.files as files_api
import app.api.v1.jobs as jobs_api
import app.db.session as session_module
import app.jobs.worker as worker_module
from app.jobs.worker import process_ingest_job, recover_incomplete_ingest_jobs
from app.models.job import Job
from tests.conftest import requires_database


async def _create_project(async_client: httpx.AsyncClient) -> dict[str, Any]:
    """Create a project and return its payload."""
    response = await async_client.post(
        "/v1/projects",
        json={
            "name": "Jobs Test Project",
            "description": "A project for job tests",
        },
    )
    assert response.status_code == 201
    return cast(dict[str, Any], response.json())


async def _upload_file(
    async_client: httpx.AsyncClient,
    project_id: str,
) -> dict[str, Any]:
    """Upload a supported file and return its payload."""
    response = await async_client.post(
        f"/v1/projects/{project_id}/files",
        files={"file": ("plan.pdf", b"%PDF-1.7\njob-test\n", "application/pdf")},
    )
    assert response.status_code == 201
    return cast(dict[str, Any], response.json())


async def _get_job_for_file(file_id: str) -> Job:
    """Load the ingest job associated with a file id."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        result = await session.execute(select(Job).where(Job.file_id == uuid.UUID(file_id)))
        job = result.scalar_one_or_none()

    assert job is not None
    return job


async def _get_job(job_id: uuid.UUID) -> Job:
    """Load a job by id."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        job = await session.get(Job, job_id)

    assert job is not None
    return job


async def _update_job(
    job_id: uuid.UUID,
    *,
    status: str | None = None,
    attempts: int | None = None,
    max_attempts: int | None = None,
    cancel_requested: bool | None = None,
    error_message: str | None = None,
) -> Job:
    """Update and return a persisted job for test setup."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        job = await session.get(Job, job_id)
        assert job is not None

        if status is not None:
            job.status = status
        if attempts is not None:
            job.attempts = attempts
        if max_attempts is not None:
            job.max_attempts = max_attempts
        if cancel_requested is not None:
            job.cancel_requested = cancel_requested

        if error_message is not None:
            job.error_message = error_message

        await session.commit()

    return await _get_job(job_id)


@pytest.fixture
def enqueued_job_ids(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Capture enqueue calls without requiring a live broker."""
    recorded_job_ids: list[str] = []

    def _fake_enqueue(job_id: uuid.UUID) -> None:
        recorded_job_ids.append(str(job_id))

    monkeypatch.setattr(files_api, "enqueue_ingest_job", _fake_enqueue)
    return recorded_job_ids


@requires_database
class TestJobs:
    """Tests for job status retrieval and worker state transitions."""

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

    async def test_upload_file_marks_job_failed_when_enqueue_publish_fails(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Uploading should leave a visible failed job when broker publish fails."""
        _ = self
        _ = cleanup_projects

        def _fail_enqueue(_: uuid.UUID) -> None:
            raise RuntimeError("broker unavailable")

        monkeypatch.setattr(files_api, "enqueue_ingest_job", _fail_enqueue)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        assert job.status == "failed"
        assert job.attempts == 0
        assert job.error_code is None
        assert job.error_message == "Failed to enqueue ingest job: broker unavailable"
        assert job.started_at is None
        assert job.finished_at is not None

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

    async def test_process_ingest_job_transitions_to_succeeded(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Worker processing should persist running and succeeded state transitions."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(job.id)

        updated_job = await _get_job_for_file(str(uploaded["id"]))
        assert updated_job.status == "succeeded"
        assert updated_job.attempts == 1
        assert updated_job.started_at is not None
        assert updated_job.finished_at is not None
        assert updated_job.finished_at >= updated_job.started_at
        assert updated_job.error_code is None
        assert updated_job.error_message is None

    async def test_process_ingest_job_continues_redelivered_running_job(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Redelivery should complete an already-running ingest attempt."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = datetime.now(UTC)
            await session.commit()

        await process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "succeeded"
        assert updated_job.attempts == 1
        assert updated_job.started_at is not None
        assert updated_job.finished_at is not None

    async def test_recover_incomplete_ingest_jobs_requeues_pending_jobs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker startup recovery should requeue pending ingest jobs."""
        _ = self
        _ = cleanup_projects

        recovered_job_ids: list[str] = []

        def _fake_recovery_enqueue(job_id: uuid.UUID) -> None:
            recovered_job_ids.append(str(job_id))

        monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fake_recovery_enqueue)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        enqueued_job_ids.clear()

        requeued = await recover_incomplete_ingest_jobs()

        assert recovered_job_ids == [str(job.id)]
        assert requeued == [job.id]
        updated_job = await _get_job(job.id)
        assert updated_job.status == "pending"
        assert updated_job.attempts == 0

    async def test_recover_incomplete_ingest_jobs_requeues_orphaned_running_jobs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker startup recovery should requeue stale running ingest jobs."""
        _ = self
        _ = cleanup_projects

        recovered_job_ids: list[str] = []

        def _fake_recovery_enqueue(job_id: uuid.UUID) -> None:
            recovered_job_ids.append(str(job_id))

        monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fake_recovery_enqueue)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        enqueued_job_ids.clear()

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = (
                datetime.now(UTC)
                - worker_module._RUNNING_JOB_STALE_AFTER
                - timedelta(seconds=1)
            )
            await session.commit()

        requeued = await recover_incomplete_ingest_jobs()

        assert recovered_job_ids == [str(job.id)]
        assert requeued == [job.id]

        recovered_job = await _get_job(job.id)
        assert recovered_job.status == "pending"
        assert recovered_job.attempts == 1
        assert recovered_job.started_at is None
        assert recovered_job.finished_at is None

        await process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "succeeded"
        assert updated_job.attempts == 2

    async def test_recover_incomplete_ingest_jobs_skips_fresh_running_jobs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker startup recovery should not requeue fresh running ingest jobs."""
        _ = self
        _ = cleanup_projects

        recovered_job_ids: list[str] = []

        def _fake_recovery_enqueue(job_id: uuid.UUID) -> None:
            recovered_job_ids.append(str(job_id))

        monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fake_recovery_enqueue)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        enqueued_job_ids.clear()

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = datetime.now(UTC)
            await session.commit()

        requeued = await recover_incomplete_ingest_jobs()

        assert recovered_job_ids == []
        assert requeued == []

        unchanged_job = await _get_job(job.id)
        assert unchanged_job.status == "running"
        assert unchanged_job.attempts == 1
        assert unchanged_job.started_at is not None

    async def test_process_ingest_job_ignores_duplicate_delivery_after_success(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Duplicate delivery should not mutate terminal succeeded jobs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(job.id)
        first_completion = await _get_job(job.id)
        assert first_completion.status == "succeeded"
        assert first_completion.attempts == 1

        await process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "succeeded"
        assert updated_job.attempts == 1

    async def test_cancel_job_marks_pending_job_cancel_requested(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Cancel should mark a pending job for cancellation."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        response = await async_client.post(f"/v1/jobs/{job.id}/cancel")

        assert response.status_code == 202
        updated_job = await _get_job(job.id)
        assert updated_job.cancel_requested is True
        assert updated_job.status in {"pending", "cancelled"}

    async def test_cancel_job_returns_404_for_unknown_job(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """Cancel should return 404 for unknown jobs."""
        _ = self
        _ = cleanup_projects

        missing_job_id = uuid.uuid4()

        response = await async_client.post(f"/v1/jobs/{missing_job_id}/cancel")

        assert response.status_code == 404
        assert response.json() == {
            "error": {
                "code": "NOT_FOUND",
                "message": f"Job with identifier '{missing_job_id}' not found",
                "details": None,
            }
        }

    async def test_cancel_job_is_terminal_no_op_for_succeeded_job(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Cancel should not mutate terminal succeeded jobs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(job.id)
        completed = await _get_job(job.id)
        assert completed.status == "succeeded"
        assert completed.cancel_requested is False

        response = await async_client.post(f"/v1/jobs/{job.id}/cancel")

        assert response.status_code == 202
        unchanged = await _get_job(job.id)
        assert unchanged.status == "succeeded"
        assert unchanged.cancel_requested is False
        assert unchanged.attempts == 1

    async def test_retry_job_requeues_failed_job_below_max_attempts(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry should requeue failed jobs that still have capacity."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        retried_job_ids: list[str] = []

        def _fake_retry_enqueue(job_id: uuid.UUID) -> None:
            retried_job_ids.append(str(job_id))

        monkeypatch.setattr(
            jobs_api,
            "enqueue_ingest_job",
            _fake_retry_enqueue,
            raising=False,
        )

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await _update_job(
            job.id,
            status="failed",
            attempts=1,
            max_attempts=3,
            error_message="previous failure",
        )

        response = await async_client.post(f"/v1/jobs/{job.id}/retry")

        assert response.status_code == 202
        assert retried_job_ids == [str(job.id)]
        updated = await _get_job(job.id)
        assert updated.status == "pending"

    async def test_retry_job_noops_when_attempt_limit_reached(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry should no-op when attempts already reached max_attempts."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        retried_job_ids: list[str] = []

        def _fake_retry_enqueue(job_id: uuid.UUID) -> None:
            retried_job_ids.append(str(job_id))

        monkeypatch.setattr(
            jobs_api,
            "enqueue_ingest_job",
            _fake_retry_enqueue,
            raising=False,
        )

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await _update_job(
            job.id,
            status="failed",
            attempts=3,
            max_attempts=3,
            error_message="maxed out",
        )

        response = await async_client.post(f"/v1/jobs/{job.id}/retry")

        assert response.status_code == 202
        assert retried_job_ids == []
        unchanged = await _get_job(job.id)
        assert unchanged.status == "failed"
        assert unchanged.attempts == 3
        assert unchanged.max_attempts == 3

    async def test_retry_job_is_terminal_no_op_for_cancelled_job(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry should no-op for terminal cancelled jobs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        retried_job_ids: list[str] = []

        def _fake_retry_enqueue(job_id: uuid.UUID) -> None:
            retried_job_ids.append(str(job_id))

        monkeypatch.setattr(
            jobs_api,
            "enqueue_ingest_job",
            _fake_retry_enqueue,
            raising=False,
        )

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await _update_job(job.id, status="cancelled", attempts=1, max_attempts=3)

        response = await async_client.post(f"/v1/jobs/{job.id}/retry")

        assert response.status_code == 202
        assert retried_job_ids == []
        unchanged = await _get_job(job.id)
        assert unchanged.status == "cancelled"
        assert unchanged.attempts == 1

    async def test_retry_job_returns_404_for_unknown_job(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """Retry should return 404 for unknown jobs."""
        _ = self
        _ = cleanup_projects

        missing_job_id = uuid.uuid4()

        response = await async_client.post(f"/v1/jobs/{missing_job_id}/retry")

        assert response.status_code == 404
        assert response.json() == {
            "error": {
                "code": "NOT_FOUND",
                "message": f"Job with identifier '{missing_job_id}' not found",
                "details": None,
            }
        }

    async def test_process_ingest_job_finalizes_cancel_requested_job_as_cancelled(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Worker should finalize cancel-requested jobs to cancelled."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await _update_job(job.id, status="pending", cancel_requested=True)

        await process_ingest_job(job.id)

        updated = await _get_job(job.id)
        assert updated.status == "cancelled"
        assert updated.cancel_requested is True
        assert updated.finished_at is not None
        assert updated.error_message is None

    async def test_process_ingest_job_finalizes_cancelled_when_requested_during_completion_race(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker should persist cancelled if cancellation commits before completion."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _cancel_during_work(_: float) -> None:
            await _update_job(job.id, cancel_requested=True)

        monkeypatch.setattr(asyncio, "sleep", _cancel_during_work)

        await process_ingest_job(job.id)

        updated = await _get_job(job.id)
        assert updated.status == "cancelled"
        assert updated.attempts == 1
        assert updated.cancel_requested is True
        assert updated.finished_at is not None
        assert updated.error_code == worker_module._JOB_CANCELLED_ERROR_CODE

    async def test_process_ingest_job_ignores_duplicate_delivery_after_cancelled(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Duplicate delivery should not mutate terminal cancelled jobs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await _update_job(job.id, status="cancelled", attempts=1, cancel_requested=True)

        before = await _get_job(job.id)
        before_finished_at = before.finished_at

        await process_ingest_job(job.id)

        after = await _get_job(job.id)
        assert after.status == "cancelled"
        assert after.attempts == 1
        assert after.cancel_requested is True
        assert after.finished_at == before_finished_at
