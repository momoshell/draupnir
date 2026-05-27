"""Ingest worker tests extracted from tests/test_jobs.py."""

import asyncio
import hashlib
import types
import uuid
from collections.abc import Callable
from typing import Any

import httpx
import pytest

import app.jobs.worker as worker_module
from app.core.errors import ErrorCode
from app.ingestion.contracts import (
    AdapterFailureKind,
    CancellationHandle,
    ProgressCallback,
    ProgressUpdate,
)
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import (
    IngestionRunnerError,
    IngestionRunRequest,
)
from app.ingestion.runner import run_ingestion as real_run_ingestion
from tests.conftest import requires_database
from tests.jobs_test_helpers import (
    _TEST_UPLOAD_BODY,
    _build_fake_ingest_payload,
    _create_project,
    _get_job,
    _get_job_for_file,
    _remove_source_file_bytes,
    _update_job,
    _upload_file,
    fake_ingestion_runner,
)


class _AdapterModule(types.ModuleType):
    create_adapter: Callable[[], object]

    def __init__(self, name: str, create_adapter: Callable[[], object]) -> None:
        super().__init__(name)
        self.create_adapter = create_adapter


@pytest.mark.usefixtures(fake_ingestion_runner.__name__)
@requires_database
class TestJobsWorkerIngest:
    """Ingest worker transition and sanitization tests."""

    async def test_process_ingest_job_marks_internal_error_code_on_failure(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker failures should persist INTERNAL_ERROR on the job row."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _fail_run_ingestion(_: IngestionRunRequest) -> IngestFinalizationPayload:
            raise RuntimeError("adapter exploded")

        monkeypatch.setattr(worker_module, "run_ingestion", _fail_run_ingestion)

        with pytest.raises(RuntimeError, match="adapter exploded"):
            await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.INTERNAL_ERROR.value
        assert updated_job.error_message == "Ingest job failed unexpectedly."
        assert "adapter exploded" not in updated_job.error_message
        assert updated_job.finished_at is not None

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200
        data = response.json()
        assert data["items"][-1]["data_json"] == {
            "status": "failed",
            "error_code": ErrorCode.INTERNAL_ERROR.value,
            "error_message": "Ingest job failed unexpectedly.",
        }
        assert "adapter exploded" not in str(data["items"][-1]["data_json"])

    async def test_process_ingest_job_marks_expected_runner_failure_with_sanitized_code(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Expected runner failures should persist their sanitized code and message."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        logger_error_calls: list[tuple[str, dict[str, Any]]] = []

        def _capture_logger_error(event: str, **kwargs: Any) -> None:
            logger_error_calls.append((event, kwargs))

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _fail_run_ingestion(_: IngestionRunRequest) -> IngestFinalizationPayload:
            raise IngestionRunnerError(
                error_code=ErrorCode.ADAPTER_UNAVAILABLE,
                failure_kind=AdapterFailureKind.UNAVAILABLE,
                message="Adapter could not be loaded.",
                details={"stderr": "super-secret adapter detail"},
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _fail_run_ingestion)
        monkeypatch.setattr(worker_module.logger, "error", _capture_logger_error)

        with pytest.raises(IngestionRunnerError, match="Adapter could not be loaded"):
            await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.ADAPTER_UNAVAILABLE.value
        assert updated_job.error_message == "Adapter could not be loaded."
        assert "super-secret adapter detail" not in updated_job.error_message
        assert updated_job.finished_at is not None
        assert logger_error_calls == [
            (
                "ingest_job_failed",
                {
                    "job_id": str(job.id),
                    "error_code": ErrorCode.ADAPTER_UNAVAILABLE.value,
                    "failure_kind": AdapterFailureKind.UNAVAILABLE.value,
                    "error_message": "Adapter could not be loaded.",
                },
            )
        ]

    async def test_process_ingest_job_persists_sanitized_real_runner_storage_failure(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Real runner storage failures should persist and log only sanitized fields."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        logger_error_calls: list[tuple[str, dict[str, Any]]] = []

        def _capture_logger_error(event: str, **kwargs: Any) -> None:
            logger_error_calls.append((event, kwargs))

        module = _AdapterModule("available_vector_adapter_module", lambda: object())

        def fake_import_module(module_name: str) -> types.ModuleType:
            if module_name == "app.ingestion.adapters.pymupdf":
                return module
            raise AssertionError(f"Unexpected module import: {module_name}")

        monkeypatch.setattr(worker_module, "run_ingestion", real_run_ingestion)
        monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)
        monkeypatch.setattr(worker_module.logger, "error", _capture_logger_error)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        secret_storage_uri = await _remove_source_file_bytes(uuid.UUID(uploaded["id"]))

        with pytest.raises(
            IngestionRunnerError,
            match="Failed to read original source from storage",
        ):
            await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.STORAGE_FAILED.value
        assert updated_job.error_message == "Failed to read original source from storage."
        assert secret_storage_uri not in updated_job.error_message

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200
        data = response.json()
        assert data["items"][-1]["data_json"] == {
            "status": "failed",
            "error_code": ErrorCode.STORAGE_FAILED.value,
            "error_message": "Failed to read original source from storage.",
        }
        assert secret_storage_uri not in str(data["items"][-1]["data_json"])
        assert secret_storage_uri not in str(logger_error_calls)
        assert logger_error_calls == [
            (
                "ingest_job_failed",
                {
                    "job_id": str(job.id),
                    "error_code": ErrorCode.STORAGE_FAILED.value,
                    "failure_kind": AdapterFailureKind.FAILED.value,
                    "error_message": "Failed to read original source from storage.",
                    "adapter_key": "pymupdf",
                    "input_family": "pdf_vector",
                    "reason": "not_found",
                },
            )
        ]

    async def test_process_ingest_job_persists_sanitized_real_runner_staging_failure(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Real runner staging failures should not leak temp paths or exception text."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        secret_temp_marker = "temp-path-leak"
        logger_error_calls: list[tuple[str, dict[str, Any]]] = []

        def _capture_logger_error(event: str, **kwargs: Any) -> None:
            logger_error_calls.append((event, kwargs))

        async def _fail_copy_to_path(*args: Any, **kwargs: Any) -> Any:
            raise OSError(f"{secret_temp_marker}: staging")

        module = _AdapterModule("available_stage_vector_adapter_module", lambda: object())

        def fake_import_module(module_name: str) -> types.ModuleType:
            if module_name == "app.ingestion.adapters.pymupdf":
                return module
            raise AssertionError(f"Unexpected module import: {module_name}")

        monkeypatch.setattr(worker_module, "run_ingestion", real_run_ingestion)
        monkeypatch.setattr("app.ingestion.loader.importlib.import_module", fake_import_module)
        monkeypatch.setattr(worker_module.logger, "error", _capture_logger_error)

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        monkeypatch.setattr("app.storage.memory.MemoryStorage.copy_to_path", _fail_copy_to_path)
        monkeypatch.setattr(
            "app.storage.local.LocalFilesystemStorage.copy_to_path",
            _fail_copy_to_path,
        )

        with pytest.raises(IngestionRunnerError, match="Failed to stage original source"):
            await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.STORAGE_FAILED.value
        assert updated_job.error_message == "Failed to stage original source."
        assert secret_temp_marker not in updated_job.error_message

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200
        data = response.json()
        assert data["items"][-1]["data_json"] == {
            "status": "failed",
            "error_code": ErrorCode.STORAGE_FAILED.value,
            "error_message": "Failed to stage original source.",
        }
        assert secret_temp_marker not in str(data["items"][-1]["data_json"])
        assert secret_temp_marker not in str(logger_error_calls)
        assert logger_error_calls == [
            (
                "ingest_job_failed",
                {
                    "job_id": str(job.id),
                    "error_code": ErrorCode.STORAGE_FAILED.value,
                    "failure_kind": AdapterFailureKind.FAILED.value,
                    "error_message": "Failed to stage original source.",
                    "adapter_key": "pymupdf",
                    "input_family": "pdf_vector",
                    "reason": "stage_failed",
                },
            )
        ]

    async def test_process_ingest_job_flushes_progress_events_before_success(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker should persist queued progress updates before terminal success."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _run_with_progress(
            request: IngestionRunRequest,
            *,
            on_progress: ProgressCallback | None = None,
            **_: Any,
        ) -> IngestFinalizationPayload:
            assert on_progress is not None
            on_progress(
                ProgressUpdate(
                    stage="source",
                    message="Staged original",
                    completed=1,
                    total=3,
                    percent=1 / 3,
                )
            )
            on_progress(
                ProgressUpdate(
                    stage="extract",
                    message="Extracted entities",
                    completed=2,
                    total=3,
                    percent=2 / 3,
                )
            )
            return _build_fake_ingest_payload(request)

        monkeypatch.setattr(worker_module, "run_ingestion", _run_with_progress)

        await worker_module.process_ingest_job(job.id)

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200
        data = response.json()
        assert [event["message"] for event in data["items"]] == [
            "Job started",
            "Staged original",
            "Extracted entities",
            "Job succeeded",
        ]
        assert [event["data_json"]["status"] for event in data["items"]] == [
            "running",
            "running",
            "running",
            "succeeded",
        ]
        assert data["items"][1]["data_json"] == {
            "status": "running",
            "event": "progress",
            "stage": "source",
            "detail": "Staged original",
            "completed": 1,
            "total": 3,
            "percent": 1 / 3,
        }
        assert data["items"][2]["data_json"] == {
            "status": "running",
            "event": "progress",
            "stage": "extract",
            "detail": "Extracted entities",
            "completed": 2,
            "total": 3,
            "percent": 2 / 3,
        }
        assert data["next_cursor"] is None

    async def test_process_ingest_job_transitions_to_succeeded(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        fake_ingestion_runner: list[IngestionRunRequest],
    ) -> None:
        """Worker processing should persist running and succeeded state transitions."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job_for_file(str(uploaded["id"]))
        assert len(fake_ingestion_runner) == 1
        request = fake_ingestion_runner[0]
        assert updated_job.status == "succeeded"
        assert updated_job.attempts == 1
        assert updated_job.started_at is not None
        assert updated_job.finished_at is not None
        assert updated_job.finished_at >= updated_job.started_at
        assert updated_job.error_code is None
        assert updated_job.error_message is None
        assert request.job_id == job.id
        assert request.file_id == job.file_id
        assert request.checksum_sha256 == hashlib.sha256(_TEST_UPLOAD_BODY).hexdigest()
        assert request.detected_format == "pdf"
        assert request.media_type == "application/pdf"
        assert request.original_name == "plan.pdf"
        assert request.extraction_profile_id == job.extraction_profile_id
        assert request.initial_job_id == job.id

    async def test_process_ingest_job_flushes_progress_events_before_failure(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker should persist queued progress updates before terminal failure."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _run_with_progress_then_fail(
            _: IngestionRunRequest,
            *,
            on_progress: ProgressCallback | None = None,
            **__: Any,
        ) -> IngestFinalizationPayload:
            assert on_progress is not None
            on_progress(
                ProgressUpdate(
                    stage="source",
                    message="Staged original",
                    completed=1,
                    total=3,
                    percent=1 / 3,
                )
            )
            on_progress(
                ProgressUpdate(
                    stage="extract",
                    message="Extracted entities",
                    completed=2,
                    total=3,
                    percent=2 / 3,
                )
            )
            raise IngestionRunnerError(
                error_code=ErrorCode.ADAPTER_FAILED,
                failure_kind=AdapterFailureKind.FAILED,
                message="Adapter execution failed.",
                details={"stderr": "super-secret adapter detail"},
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_with_progress_then_fail)

        with pytest.raises(IngestionRunnerError, match="Adapter execution failed"):
            await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.ADAPTER_FAILED.value
        assert updated_job.error_message == "Adapter execution failed."
        assert "super-secret adapter detail" not in updated_job.error_message

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200
        data = response.json()
        assert [event["message"] for event in data["items"]] == [
            "Job started",
            "Staged original",
            "Extracted entities",
            "Job failed",
        ]
        assert [event["data_json"]["status"] for event in data["items"]] == [
            "running",
            "running",
            "running",
            "failed",
        ]
        assert data["items"][1]["data_json"] == {
            "status": "running",
            "event": "progress",
            "stage": "source",
            "detail": "Staged original",
            "completed": 1,
            "total": 3,
            "percent": 1 / 3,
        }
        assert data["items"][2]["data_json"] == {
            "status": "running",
            "event": "progress",
            "stage": "extract",
            "detail": "Extracted entities",
            "completed": 2,
            "total": 3,
            "percent": 2 / 3,
        }
        assert data["items"][3]["data_json"] == {
            "status": "failed",
            "error_code": ErrorCode.ADAPTER_FAILED.value,
            "error_message": "Adapter execution failed.",
        }
        assert "super-secret adapter detail" not in str(data["items"][3]["data_json"])
        assert data["next_cursor"] is None

    async def test_process_ingest_job_marks_runner_cancellation_as_cancelled(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Runner cancellation should flush progress and persist a cancelled terminal state."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _cancel_run(
            _: IngestionRunRequest,
            *,
            cancellation: CancellationHandle | None = None,
            on_progress: ProgressCallback | None = None,
            **__: Any,
        ) -> IngestFinalizationPayload:
            assert cancellation is not None
            assert on_progress is not None
            on_progress(ProgressUpdate(stage="source", message="Staged original"))
            await _update_job(job.id, cancel_requested=True)
            cancellation_deadline = asyncio.get_running_loop().time() + 1
            while not cancellation.is_cancelled():
                if asyncio.get_running_loop().time() >= cancellation_deadline:
                    raise AssertionError("Expected cancellation flag within 1 second")
                await asyncio.sleep(0.01)

            await asyncio.sleep(1)
            raise AssertionError("Worker cancellation should interrupt the runner task")

        monkeypatch.setattr(worker_module, "run_ingestion", _cancel_run)

        with pytest.raises(asyncio.CancelledError):
            await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "cancelled"
        assert updated_job.cancel_requested is True
        assert updated_job.error_code == ErrorCode.JOB_CANCELLED.value
        assert updated_job.error_message is None
        assert updated_job.finished_at is not None

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200
        data = response.json()
        assert [event["message"] for event in data["items"]] == [
            "Job started",
            "Staged original",
            "Job cancelled",
        ]
        assert [event["data_json"]["status"] for event in data["items"]] == [
            "running",
            "running",
            "cancelled",
        ]
