"""Lifecycle worker tests extracted from tests/test_jobs.py."""

import asyncio
import hashlib
import types
import uuid
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from sqlalchemy import select

import app.api.v1.files as files_api
import app.db.session as session_module
import app.jobs.worker as worker_module
from app.core.errors import ErrorCode
from app.ingestion.contracts import CancellationHandle
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunRequest
from app.models.job import Job, JobType
from app.models.project import Project
from tests.conftest import requires_database
from tests.jobs_test_helpers import (
    _TEST_UPLOAD_BODY,
    _build_fake_ingest_payload,
    _create_project,
    _get_generated_artifacts_for_job,
    _get_job,
    _get_job_for_file,
    _mark_source_deleted,
    _update_job,
    _upload_file,
    fake_ingestion_runner,
)


@pytest.mark.usefixtures(fake_ingestion_runner.__name__)
@requires_database
class TestJobsWorkerLifecycle:
    """Ingest worker lifecycle tests."""

    async def test_recover_incomplete_ingest_jobs_requeues_stranded_pending_jobs(
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
        monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fake_recovery_enqueue)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        enqueued_job_ids.clear()

        requeued = await worker_module.recover_incomplete_ingest_jobs()

        assert recovered_job_ids == [str(job.id)]
        assert requeued == [job.id]
        updated_job = await _get_job(job.id)
        assert updated_job.status == "pending"
        assert updated_job.attempts == 0
        assert updated_job.enqueue_status == "published"
        assert updated_job.enqueue_attempts == 1

    async def test_recover_incomplete_ingest_jobs_does_not_duplicate_requeue_after_publish(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Recovery should not requeue the same pending job twice once publish state is durable."""
        _ = self
        _ = cleanup_projects

        recovered_job_ids: list[str] = []

        def _fake_recovery_enqueue(job_id: uuid.UUID) -> None:
            recovered_job_ids.append(str(job_id))

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
        monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fake_recovery_enqueue)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        enqueued_job_ids.clear()

        first_requeued = await worker_module.recover_incomplete_ingest_jobs()
        second_requeued = await worker_module.recover_incomplete_ingest_jobs()

        assert recovered_job_ids == [str(job.id)]
        assert first_requeued == [job.id]
        assert second_requeued == []

        updated_job = await _get_job(job.id)
        assert updated_job.status == "pending"
        assert updated_job.enqueue_status == "published"
        assert updated_job.enqueue_attempts == 1

    async def test_recover_incomplete_ingest_jobs_requeues_stranded_pending_reprocess_jobs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker startup recovery should requeue pending reprocess jobs."""
        _ = self
        _ = cleanup_projects

        recovered_job_ids: list[str] = []

        def _fake_recovery_enqueue(job_id: uuid.UUID) -> None:
            recovered_job_ids.append(str(job_id))

        monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fake_recovery_enqueue)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        initial_job = await _get_job_for_file(str(uploaded["id"]))
        await worker_module.process_ingest_job(initial_job.id)

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
        reprocess_response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(initial_job.extraction_profile_id)},
        )
        assert reprocess_response.status_code == 202
        job = await _get_job(uuid.UUID(reprocess_response.json()["id"]))
        enqueued_job_ids.clear()

        requeued = await worker_module.recover_incomplete_ingest_jobs()

        assert recovered_job_ids == [str(job.id)]
        assert requeued == [job.id]

        await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.job_type == "reprocess"
        assert updated_job.status == "succeeded"
        assert updated_job.attempts == 1

    async def test_process_ingest_job_cancels_mid_run_when_project_is_deleted(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Project deletion during execution should cancel the active runner."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        runner_started = asyncio.Event()

        async def _wait_for_project_delete_cancel(
            _: IngestionRunRequest,
            *,
            cancellation: CancellationHandle | None = None,
            **__: object,
        ) -> IngestFinalizationPayload:
            assert cancellation is not None
            runner_started.set()
            cancellation_deadline = asyncio.get_running_loop().time() + 1
            while not cancellation.is_cancelled():
                if asyncio.get_running_loop().time() >= cancellation_deadline:
                    raise AssertionError("Expected project delete to cancel runner within 1 second")
                await asyncio.sleep(0.01)

            await asyncio.sleep(1)
            raise AssertionError("Worker cancellation should interrupt the runner task")

        monkeypatch.setattr(worker_module, "run_ingestion", _wait_for_project_delete_cancel)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        process_task = asyncio.create_task(worker_module.process_ingest_job(job.id))
        await asyncio.wait_for(runner_started.wait(), timeout=2)

        delete_response = await async_client.delete(f"/v1/projects/{project['id']}")

        assert delete_response.status_code == 204
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(process_task, timeout=2)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "cancelled"
        assert updated_job.cancel_requested is True
        assert updated_job.error_code == ErrorCode.JOB_CANCELLED.value
        assert updated_job.finished_at is not None

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

        await worker_module.process_ingest_job(job.id)

        updated = await _get_job(job.id)
        assert updated.status == "cancelled"
        assert updated.cancel_requested is True
        assert updated.error_code == ErrorCode.JOB_CANCELLED.value
        assert updated.finished_at is not None
        assert updated.error_message is None

    async def test_process_ingest_job_cancels_soft_deleted_source_before_runner(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker should not start ingestion when the source project is already soft-deleted."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        runner_calls: list[IngestionRunRequest] = []

        async def _unexpected_run_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            runner_calls.append(request)
            return _build_fake_ingest_payload(request)

        monkeypatch.setattr(worker_module, "run_ingestion", _unexpected_run_ingestion)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await _mark_source_deleted(
            uuid.UUID(project["id"]),
            uuid.UUID(uploaded["id"]),
            delete_project=True,
            delete_file=False,
        )

        await worker_module.process_ingest_job(job.id)

        updated = await _get_job(job.id)
        assert runner_calls == []
        assert updated.status == "cancelled"
        assert updated.cancel_requested is True
        assert updated.error_code == ErrorCode.JOB_CANCELLED.value
        assert updated.finished_at is not None

    async def test_process_ingest_job_skips_finalization_storage_for_soft_deleted_source(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker should cancel before storage writes if the source is deleted mid-run."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        class _UnexpectedStorage:
            async def put(self, *_: Any, **__: Any) -> Any:
                raise AssertionError("storage.put should not be called for deleted sources")

        async def _delete_source_during_run(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            await _mark_source_deleted(
                uuid.UUID(project["id"]),
                uuid.UUID(uploaded["id"]),
                delete_project=False,
                delete_file=True,
            )
            return _build_fake_ingest_payload(request)

        monkeypatch.setattr(worker_module, "get_storage", lambda: _UnexpectedStorage())
        monkeypatch.setattr(worker_module, "run_ingestion", _delete_source_during_run)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await worker_module.process_ingest_job(job.id)

        updated = await _get_job(job.id)
        assert updated.status == "cancelled"
        assert updated.cancel_requested is True
        assert updated.error_code == ErrorCode.JOB_CANCELLED.value
        assert updated.finished_at is not None

    async def test_process_ingest_job_begin_race_delete_wins_cancels_before_runner(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Delete vs worker begin lock contention should cancel without deadlock."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        runner_calls: list[IngestionRunRequest] = []

        async def _unexpected_run_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            runner_calls.append(request)
            return _build_fake_ingest_payload(request)

        monkeypatch.setattr(worker_module, "run_ingestion", _unexpected_run_ingestion)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            await session.execute(
                select(Project)
                .where(Project.id == uuid.UUID(project["id"]))
                .with_for_update(of=Project)
            )

            delete_task = asyncio.create_task(async_client.delete(f"/v1/projects/{project['id']}"))
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(delete_task), timeout=0.2)

            process_task = asyncio.create_task(worker_module.process_ingest_job(job.id))
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(process_task), timeout=0.2)

        delete_response = await asyncio.wait_for(delete_task, timeout=2)
        await asyncio.wait_for(process_task, timeout=2)

        assert delete_response.status_code == 204
        updated = await _get_job(job.id)
        assert runner_calls == []
        assert updated.status == "cancelled"
        assert updated.cancel_requested is True
        assert updated.error_code == ErrorCode.JOB_CANCELLED.value
        assert updated.finished_at is not None

        artifacts = await _get_generated_artifacts_for_job(job.id)
        assert artifacts == []

    async def test_mark_job_failed_delete_race_delete_wins_without_failed_overwrite(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Delete should win over a concurrent worker failure terminal write."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        lease = await worker_module._begin_or_resume_ingest_job(job.id)

        assert lease is not None
        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            await session.execute(
                select(Project)
                .where(Project.id == uuid.UUID(project["id"]))
                .with_for_update(of=Project)
            )

            delete_task = asyncio.create_task(async_client.delete(f"/v1/projects/{project['id']}"))
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(delete_task), timeout=0.2)

            fail_task = asyncio.create_task(
                worker_module._mark_job_failed(
                    job.id,
                    error_message="Ingest job failed unexpectedly.",
                    attempt_token=lease.token,
                )
            )
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(fail_task), timeout=0.2)

        delete_response = await asyncio.wait_for(delete_task, timeout=2)
        failed = await asyncio.wait_for(fail_task, timeout=2)

        assert delete_response.status_code == 204
        assert failed is False

        updated_job = await _get_job(job.id)
        assert updated_job.status == "cancelled"
        assert updated_job.cancel_requested is True
        assert updated_job.error_code == ErrorCode.JOB_CANCELLED.value
        assert updated_job.error_message is None
        assert updated_job.finished_at is not None

    async def test_finalize_ingest_job_delete_race_finalize_wins_soft_deletes_artifact(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Finalize before delete should keep job terminal while delete soft-deletes outputs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        storage_put_started = asyncio.Event()
        allow_storage_put = asyncio.Event()

        class _BlockingStorage:
            async def put(self, key: str, payload: bytes, *, immutable: bool = True) -> Any:
                _ = immutable
                storage_put_started.set()
                await allow_storage_put.wait()
                return types.SimpleNamespace(
                    key=key,
                    storage_uri=f"file://tests/{key}",
                    size_bytes=len(payload),
                    checksum_sha256=hashlib.sha256(payload).hexdigest(),
                )

        monkeypatch.setattr(worker_module, "get_storage", lambda: _BlockingStorage())

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        lease = await worker_module._begin_or_resume_ingest_job(job.id)
        assert lease is not None

        finalize_request = IngestionRunRequest(
            job_id=job.id,
            file_id=job.file_id,
            checksum_sha256=hashlib.sha256(_TEST_UPLOAD_BODY).hexdigest(),
            detected_format="pdf",
            media_type="application/pdf",
            original_name="plan.pdf",
            extraction_profile_id=job.extraction_profile_id,
            initial_job_id=job.id,
        )
        payload = _build_fake_ingest_payload(finalize_request)

        finalize_task = asyncio.create_task(
            worker_module._finalize_ingest_job(
                job.id,
                attempt_token=lease.token,
                payload=payload,
            )
        )

        await asyncio.wait_for(storage_put_started.wait(), timeout=2)

        delete_task = asyncio.create_task(async_client.delete(f"/v1/projects/{project['id']}"))
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(delete_task), timeout=0.2)

        allow_storage_put.set()

        finalized = await asyncio.wait_for(finalize_task, timeout=3)
        delete_response = await asyncio.wait_for(delete_task, timeout=3)

        assert finalized is True
        assert delete_response.status_code == 204

        updated = await _get_job(job.id)
        assert updated.status == "succeeded"
        assert updated.cancel_requested is False
        assert updated.finished_at is not None

        artifacts = await _get_generated_artifacts_for_job(job.id)
        assert len(artifacts) == 1
        assert artifacts[0].deleted_at is not None

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

        async def _cancel_during_work(request: IngestionRunRequest) -> IngestFinalizationPayload:
            await _update_job(job.id, cancel_requested=True)
            return _build_fake_ingest_payload(request)

        monkeypatch.setattr(worker_module, "run_ingestion", _cancel_during_work)

        with suppress(asyncio.CancelledError):
            await worker_module.process_ingest_job(job.id)

        updated = await _get_job(job.id)
        assert updated.status == "cancelled"
        assert updated.attempts == 1
        assert updated.cancel_requested is True
        assert updated.finished_at is not None
        assert updated.error_code == ErrorCode.JOB_CANCELLED.value

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

        await worker_module.process_ingest_job(job.id)

        after = await _get_job(job.id)
        assert after.status == "cancelled"
        assert after.attempts == 1
        assert after.cancel_requested is True
        assert after.finished_at == before_finished_at

    async def test_process_ingest_job_skips_fresh_redelivered_running_job(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Fresh redelivery should not execute or mutate an already-running attempt."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        runner_calls: list[IngestionRunRequest] = []

        async def _unexpected_run_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            runner_calls.append(request)
            return _build_fake_ingest_payload(request)

        monkeypatch.setattr(worker_module, "run_ingestion", _unexpected_run_ingestion)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        attempt_token = uuid.uuid4()
        lease_expires_at = datetime.now(UTC) + worker_module._RUNNING_JOB_STALE_AFTER

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = datetime.now(UTC)
            persisted_job.attempt_token = attempt_token
            persisted_job.attempt_lease_expires_at = lease_expires_at
            await session.commit()

        await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert runner_calls == []
        assert updated_job.status == "running"
        assert updated_job.attempts == 1
        assert updated_job.started_at is not None
        assert updated_job.finished_at is None
        assert updated_job.attempt_token == attempt_token
        assert updated_job.attempt_lease_expires_at == lease_expires_at

    async def test_begin_or_resume_ingest_job_reclaims_stale_running_attempt_with_new_token(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Stale running attempts should be reclaimed with a fresh ownership token."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        old_attempt_token = uuid.uuid4()

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = (
                datetime.now(UTC) - worker_module._RUNNING_JOB_STALE_AFTER - timedelta(seconds=1)
            )
            persisted_job.attempt_token = old_attempt_token
            persisted_job.attempt_lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            await session.commit()

        lease = await worker_module._begin_or_resume_ingest_job(job.id)

        assert lease is not None
        assert lease.token != old_attempt_token
        reclaimed_job = await _get_job(job.id)
        assert reclaimed_job.status == "running"
        assert reclaimed_job.attempts == 2
        assert reclaimed_job.attempt_token == lease.token
        assert reclaimed_job.attempt_lease_expires_at == lease.lease_expires_at
        assert reclaimed_job.attempt_lease_expires_at is not None
        assert reclaimed_job.attempt_lease_expires_at > datetime.now(UTC)

    async def test_stale_attempt_writes_noop_after_reclaim(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Stale attempt events and terminal writes should no-op after reclaim."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        old_attempt_token = uuid.uuid4()

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = (
                datetime.now(UTC) - worker_module._RUNNING_JOB_STALE_AFTER - timedelta(seconds=1)
            )
            persisted_job.attempt_token = old_attempt_token
            persisted_job.attempt_lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            await session.commit()

        new_lease = await worker_module._begin_or_resume_ingest_job(job.id)

        assert new_lease is not None
        event_written = await worker_module.emit_job_event(
            job.id,
            level="info",
            message="Stale progress",
            data_json={"status": "running", "event": "progress", "stage": "stale"},
            attempt_token=old_attempt_token,
        )
        failed = await worker_module._mark_job_failed(
            job.id,
            error_message="stale failure",
            attempt_token=old_attempt_token,
        )
        cancelled = await worker_module._mark_job_cancelled(
            job.id,
            attempt_token=old_attempt_token,
        )
        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None
            inactive_source_cancelled = await worker_module._cancel_job_for_inactive_source(
                session,
                persisted_job,
                reason="source_deleted",
                attempt_token=old_attempt_token,
            )

        finalize_request = IngestionRunRequest(
            job_id=job.id,
            file_id=job.file_id,
            checksum_sha256=hashlib.sha256(_TEST_UPLOAD_BODY).hexdigest(),
            detected_format="pdf",
            media_type="application/pdf",
            original_name="plan.pdf",
            extraction_profile_id=job.extraction_profile_id,
            initial_job_id=job.id,
        )
        finalized = await worker_module._finalize_ingest_job(
            job.id,
            attempt_token=old_attempt_token,
            payload=_build_fake_ingest_payload(finalize_request),
        )

        assert event_written is False
        assert failed is False
        assert cancelled is False
        assert inactive_source_cancelled is False
        assert finalized is False

        updated_job = await _get_job(job.id)
        assert updated_job.status == "running"
        assert updated_job.attempts == 2
        assert updated_job.attempt_token == new_lease.token
        assert updated_job.finished_at is None

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200
        data = response.json()
        assert [event["message"] for event in data["items"]] == ["Job started"]

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
        old_attempt_token = uuid.uuid4()
        enqueued_job_ids.clear()

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = (
                datetime.now(UTC) - worker_module._RUNNING_JOB_STALE_AFTER - timedelta(seconds=1)
            )
            persisted_job.attempt_token = old_attempt_token
            persisted_job.attempt_lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            await session.commit()

        requeued = await worker_module.recover_incomplete_ingest_jobs()

        assert recovered_job_ids == [str(job.id)]
        assert requeued == [job.id]

        recovered_job = await _get_job(job.id)
        assert recovered_job.status == "pending"
        assert recovered_job.attempts == 1
        assert recovered_job.started_at is None
        assert recovered_job.finished_at is None
        assert recovered_job.attempt_token is None
        assert recovered_job.attempt_lease_expires_at is None

        await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "succeeded"
        assert updated_job.attempts == 2

    async def test_recover_incomplete_ingest_jobs_skips_locked_reclaimed_jobs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker startup recovery should not clobber a concurrently reclaimed live attempt."""
        _ = self
        _ = cleanup_projects

        recovered_job_ids: list[str] = []

        def _fake_recovery_enqueue(job_id: uuid.UUID) -> None:
            recovered_job_ids.append(str(job_id))

        monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fake_recovery_enqueue)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        stale_attempt_token = uuid.uuid4()
        fresh_attempt_token = uuid.uuid4()
        enqueued_job_ids.clear()

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = (
                datetime.now(UTC) - worker_module._RUNNING_JOB_STALE_AFTER - timedelta(seconds=1)
            )
            persisted_job.attempt_token = stale_attempt_token
            persisted_job.attempt_lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            await session.commit()

        fresh_lease_expires_at = datetime.now(UTC) + worker_module._RUNNING_JOB_STALE_AFTER

        async with session_maker() as reclaim_session:
            reclaimed_job = await worker_module._get_job_for_update(reclaim_session, job.id)
            assert reclaimed_job is not None
            reclaimed_job.status = "running"
            reclaimed_job.attempts = 2
            reclaimed_job.started_at = datetime.now(UTC)
            reclaimed_job.finished_at = None
            reclaimed_job.error_code = None
            reclaimed_job.error_message = None
            reclaimed_job.attempt_token = fresh_attempt_token
            reclaimed_job.attempt_lease_expires_at = fresh_lease_expires_at
            await reclaim_session.flush()

            requeued = await asyncio.wait_for(
                worker_module.recover_incomplete_ingest_jobs(),
                timeout=2,
            )

            assert requeued == []
            assert recovered_job_ids == []

            await reclaim_session.commit()

        updated_job = await _get_job(job.id)
        assert updated_job.status == "running"
        assert updated_job.attempts == 2
        assert updated_job.attempt_token == fresh_attempt_token
        assert updated_job.attempt_lease_expires_at == fresh_lease_expires_at

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

        requeued = await worker_module.recover_incomplete_ingest_jobs()

        assert recovered_job_ids == []
        assert requeued == []

        unchanged_job = await _get_job(job.id)
        assert unchanged_job.status == "running"
        assert unchanged_job.attempts == 1
        assert unchanged_job.started_at is not None

    async def test_recover_incomplete_ingest_jobs_sanitizes_enqueue_failure_details(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker startup recovery should not persist raw enqueue exception text."""
        _ = self
        _ = cleanup_projects

        secret_broker_text = "amqp://user:super-secret-password@broker/vhost timed out"
        logger_error_calls: list[tuple[str, dict[str, Any]]] = []

        def _fail_recovery_enqueue(_: uuid.UUID) -> None:
            raise RuntimeError(secret_broker_text)

        def _capture_logger_error(event: str, **kwargs: Any) -> None:
            logger_error_calls.append((event, kwargs))

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
        monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fail_recovery_enqueue)
        monkeypatch.setattr(worker_module.logger, "error", _capture_logger_error)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        enqueued_job_ids.clear()

        requeued = await worker_module.recover_incomplete_ingest_jobs()

        assert requeued == []

        failed_job = await _get_job(job.id)
        assert failed_job.status == "failed"
        assert failed_job.error_code == ErrorCode.INTERNAL_ERROR.value
        assert failed_job.error_message == "Failed to enqueue ingest job"
        assert secret_broker_text not in failed_job.error_message
        assert len(failed_job.error_message) <= 255

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200
        data = response.json()
        assert [event["message"] for event in data["items"]] == ["Job failed"]
        assert data["items"][0]["data_json"] == {
            "status": "failed",
            "error_code": ErrorCode.INTERNAL_ERROR.value,
            "error_message": "Failed to enqueue ingest job",
        }
        assert secret_broker_text not in str(data["items"][0]["data_json"])
        assert data["next_cursor"] is None

        assert logger_error_calls == [
            (
                "job_recovery_enqueue_failed",
                {
                    "job_id": str(job.id),
                    "job_type": JobType.INGEST.value,
                    "error_code": ErrorCode.INTERNAL_ERROR.value,
                    "recovery_action": "mark_failed",
                },
            )
        ]

    async def test_mark_recovery_enqueue_failed_skips_owned_running_jobs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Recovery enqueue failure should not fail a job reclaimed by a live attempt."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        attempt_token = uuid.uuid4()
        lease_expires_at = datetime.now(UTC) + worker_module._RUNNING_JOB_STALE_AFTER

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None
            persisted_job.status = "running"
            persisted_job.attempts = 1
            persisted_job.started_at = datetime.now(UTC)
            persisted_job.finished_at = None
            persisted_job.error_code = None
            persisted_job.error_message = None
            persisted_job.attempt_token = attempt_token
            persisted_job.attempt_lease_expires_at = lease_expires_at
            await session.commit()

        marked_failed = await worker_module._mark_recovery_enqueue_failed(
            job.id,
            job_type=JobType.INGEST.value,
        )

        assert marked_failed is False
        updated_job = await _get_job(job.id)
        assert updated_job.status == "running"
        assert updated_job.error_code is None
        assert updated_job.error_message is None
        assert updated_job.finished_at is None
        assert updated_job.attempt_token == attempt_token
        assert updated_job.attempt_lease_expires_at == lease_expires_at

    async def test_mark_recovery_enqueue_failed_skips_terminal_jobs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Recovery enqueue failure should not rewrite a terminalized job."""
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
            persisted_job.status = "succeeded"
            persisted_job.finished_at = datetime.now(UTC)
            await session.commit()

        marked_failed = await worker_module._mark_recovery_enqueue_failed(
            job.id,
            job_type=JobType.INGEST.value,
        )

        assert marked_failed is False
        updated_job = await _get_job(job.id)
        assert updated_job.status == "succeeded"
        assert updated_job.error_code is None
        assert updated_job.error_message is None

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

        await worker_module.process_ingest_job(job.id)
        first_completion = await _get_job(job.id)
        assert first_completion.status == "succeeded"
        assert first_completion.attempts == 1

        await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "succeeded"
        assert updated_job.attempts == 1

    async def test_finalize_ingest_job_clears_owned_attempt_lease_on_success(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Owned terminal success should clear persisted attempt lease state."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        lease = await worker_module._begin_or_resume_ingest_job(job.id)

        assert lease is not None
        finalize_request = IngestionRunRequest(
            job_id=job.id,
            file_id=job.file_id,
            checksum_sha256=hashlib.sha256(_TEST_UPLOAD_BODY).hexdigest(),
            detected_format="pdf",
            media_type="application/pdf",
            original_name="plan.pdf",
            extraction_profile_id=job.extraction_profile_id,
            initial_job_id=job.id,
        )

        finalized = await worker_module._finalize_ingest_job(
            job.id,
            attempt_token=lease.token,
            payload=_build_fake_ingest_payload(finalize_request),
        )

        assert finalized is True
        updated_job = await _get_job(job.id)
        assert updated_job.status == "succeeded"
        assert updated_job.attempt_token is None
        assert updated_job.attempt_lease_expires_at is None
