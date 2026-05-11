"""Integration tests for persisted job status and worker transitions."""

import asyncio
import hashlib
import types
import uuid
from collections.abc import Callable
from contextlib import suppress
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

import app.api.v1.files as files_api
import app.api.v1.jobs as jobs_api
import app.db.session as session_module
import app.jobs.worker as worker_module
from app.core.errors import ErrorCode
from app.ingestion.contracts import (
    AdapterFailureKind,
    CancellationHandle,
    ProgressCallback,
    ProgressUpdate,
)
from app.ingestion.finalization import IngestFinalizationPayload, compute_adapter_result_checksum
from app.ingestion.runner import IngestionRunnerError, IngestionRunRequest
from app.ingestion.runner import (
    run_ingestion as real_run_ingestion,
)
from app.models.file import File
from app.models.job import Job
from app.models.job_event import JobEvent
from app.models.project import Project
from tests.conftest import requires_database

_FAKE_RUNNER_ADAPTER_KEY = "tests.fake_ingestion_runner"
_FAKE_RUNNER_ADAPTER_VERSION = "1.0"
_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION = "0.1"
_FAKE_RUNNER_VALIDATION_REPORT_SCHEMA_VERSION = "0.1"
_FAKE_RUNNER_CONFIDENCE_SCORE = 0.75
_FAKE_RUNNER_REVIEW_STATE = "provisional"
_FAKE_RUNNER_VALIDATION_STATUS = "valid"
_FAKE_RUNNER_QUANTITY_GATE = "allowed_provisional"
_FAKE_RUNNER_VALIDATOR_NAME = "tests.fake_ingestion_runner"
_FAKE_RUNNER_VALIDATOR_VERSION = "1.0"
_TEST_UPLOAD_BODY = b"%PDF-1.7\njob-test\n"
_UNSET = object()


class _AdapterModule(types.ModuleType):
    create_adapter: Callable[[], object]

    def __init__(self, name: str, create_adapter: Callable[[], object]) -> None:
        super().__init__(name)
        self.create_adapter = create_adapter


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
        files={"file": ("plan.pdf", _TEST_UPLOAD_BODY, "application/pdf")},
    )
    assert response.status_code == 201
    return cast(dict[str, Any], response.json())


def _resolve_fake_revision_kind(request: IngestionRunRequest) -> str:
    """Match ingest vs reprocess semantics for fake runner payloads."""
    if request.initial_job_id == request.job_id:
        return "ingest"

    return "reprocess"


def _resolve_fake_input_family(request: IngestionRunRequest) -> str:
    """Return a stable runner-like input family for tests."""
    if request.detected_format == "pdf" and request.media_type == "application/pdf":
        return "pdf_vector"

    if request.detected_format is not None:
        return request.detected_format

    return "unknown"


def _build_fake_ingest_payload(
    request: IngestionRunRequest,
    *,
    generated_at: datetime | None = None,
) -> IngestFinalizationPayload:
    """Build a deterministic fake finalization payload for worker tests."""
    normalized_generated_at = generated_at or datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    revision_kind = _resolve_fake_revision_kind(request)
    input_family = _resolve_fake_input_family(request)
    entity_counts = {
        "layouts": 1,
        "layers": 1,
        "blocks": 0,
        "entities": 1,
    }
    canonical_json = {
        "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "layouts": [{"name": "Model"}],
        "layers": [{"name": "A-WALL"}],
        "blocks": [],
        "entities": [{"kind": "line", "layer": "A-WALL"}],
        "entity_counts": entity_counts,
    }
    provenance_json = {
        "schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "bridge": "tests.fake_ingestion_runner",
        "adapter": {
            "key": _FAKE_RUNNER_ADAPTER_KEY,
            "version": _FAKE_RUNNER_ADAPTER_VERSION,
        },
        "source": {
            "file_id": str(request.file_id),
            "job_id": str(request.job_id),
            "extraction_profile_id": (
                str(request.extraction_profile_id)
                if request.extraction_profile_id is not None
                else None
            ),
            "input_family": input_family,
            "revision_kind": revision_kind,
            "original_name": request.original_name,
        },
        "generated_at": normalized_generated_at.isoformat(),
    }
    confidence_json = {
        "score": _FAKE_RUNNER_CONFIDENCE_SCORE,
        "effective_confidence": _FAKE_RUNNER_CONFIDENCE_SCORE,
        "review_state": _FAKE_RUNNER_REVIEW_STATE,
    }
    warnings_json: list[Any] = []
    diagnostics_json = {
        "runner": "tests.fake_ingestion_runner",
        "detected_format": request.detected_format,
    }
    report_json = {
        "validation_report_schema_version": _FAKE_RUNNER_VALIDATION_REPORT_SCHEMA_VERSION,
        "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "validator": {
            "name": _FAKE_RUNNER_VALIDATOR_NAME,
            "version": _FAKE_RUNNER_VALIDATOR_VERSION,
        },
        "summary": {
            "validation_status": _FAKE_RUNNER_VALIDATION_STATUS,
            "review_state": _FAKE_RUNNER_REVIEW_STATE,
            "quantity_gate": _FAKE_RUNNER_QUANTITY_GATE,
            "effective_confidence": _FAKE_RUNNER_CONFIDENCE_SCORE,
            "entity_counts": entity_counts,
        },
        "checks": [],
        "findings": [],
        "adapter_warnings": warnings_json,
        "provenance": provenance_json,
    }
    result_envelope = {
        "adapter_key": _FAKE_RUNNER_ADAPTER_KEY,
        "adapter_version": _FAKE_RUNNER_ADAPTER_VERSION,
        "input_family": input_family,
        "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "canonical_json": canonical_json,
        "provenance_json": provenance_json,
        "confidence_json": confidence_json,
        "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
        "warnings_json": warnings_json,
        "diagnostics_json": diagnostics_json,
    }

    return IngestFinalizationPayload(
        revision_kind=revision_kind,
        adapter_key=_FAKE_RUNNER_ADAPTER_KEY,
        adapter_version=_FAKE_RUNNER_ADAPTER_VERSION,
        input_family=input_family,
        canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        canonical_json=canonical_json,
        provenance_json=provenance_json,
        confidence_json=confidence_json,
        confidence_score=_FAKE_RUNNER_CONFIDENCE_SCORE,
        warnings_json=warnings_json,
        diagnostics_json=diagnostics_json,
        result_checksum_sha256=compute_adapter_result_checksum(result_envelope),
        validation_report_schema_version=_FAKE_RUNNER_VALIDATION_REPORT_SCHEMA_VERSION,
        validation_status=_FAKE_RUNNER_VALIDATION_STATUS,
        review_state=_FAKE_RUNNER_REVIEW_STATE,
        quantity_gate=_FAKE_RUNNER_QUANTITY_GATE,
        effective_confidence=_FAKE_RUNNER_CONFIDENCE_SCORE,
        validator_name=_FAKE_RUNNER_VALIDATOR_NAME,
        validator_version=_FAKE_RUNNER_VALIDATOR_VERSION,
        report_json=report_json,
        generated_at=normalized_generated_at,
    )


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
    error_code: str | None = None,
    error_message: str | None = None,
    enqueue_status: str | None = None,
    enqueue_attempts: int | None = None,
    enqueue_owner_token: uuid.UUID | object = _UNSET,
    enqueue_lease_expires_at: datetime | object = _UNSET,
    enqueue_last_attempted_at: datetime | object = _UNSET,
    enqueue_published_at: datetime | object = _UNSET,
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
        if error_code is not None:
            job.error_code = error_code

        if error_message is not None:
            job.error_message = error_message
        if enqueue_status is not None:
            job.enqueue_status = enqueue_status
        if enqueue_attempts is not None:
            job.enqueue_attempts = enqueue_attempts
        if enqueue_owner_token is not _UNSET:
            job.enqueue_owner_token = cast(uuid.UUID | None, enqueue_owner_token)
        if enqueue_lease_expires_at is not _UNSET:
            job.enqueue_lease_expires_at = cast(datetime | None, enqueue_lease_expires_at)
        if enqueue_last_attempted_at is not _UNSET:
            job.enqueue_last_attempted_at = cast(datetime | None, enqueue_last_attempted_at)
        if enqueue_published_at is not _UNSET:
            job.enqueue_published_at = cast(datetime | None, enqueue_published_at)

        await session.commit()

    return await _get_job(job_id)


async def _update_source_file(
    file_id: uuid.UUID,
    *,
    checksum_sha256: str | None = None,
    original_filename: str | None = None,
) -> File:
    """Update and return a persisted source file for test setup."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        source_file = await session.get(File, file_id)
        assert source_file is not None

        if checksum_sha256 is not None:
            source_file.checksum_sha256 = checksum_sha256
        if original_filename is not None:
            source_file.original_filename = original_filename

        await session.commit()

    return source_file


async def _mark_source_deleted(
    project_id: uuid.UUID,
    file_id: uuid.UUID,
    *,
    delete_project: bool,
    delete_file: bool,
) -> None:
    """Soft-delete the source project and/or file for worker visibility tests."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        if delete_project:
            project = await session.get(Project, project_id)
            assert project is not None
            project.deleted_at = datetime.now(UTC)
        if delete_file:
            source_file = await session.get(File, file_id)
            assert source_file is not None
            source_file.deleted_at = datetime.now(UTC)
        await session.commit()


async def _create_job_event(
    job_id: uuid.UUID,
    *,
    level: str,
    message: str,
    data_json: dict[str, Any] | None = None,
    created_at: datetime | None = None,
) -> JobEvent:
    """Persist and return a job event for test setup."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        event = JobEvent(
            job_id=job_id,
            level=level,
            message=message,
            data_json=data_json,
        )
        if created_at is not None:
            event.created_at = created_at
        session.add(event)
        await session.commit()
        await session.refresh(event)

    return event


@pytest.fixture(autouse=True)
def fake_ingestion_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> list[IngestionRunRequest]:
    """Patch worker ingestion with a deterministic fake runner payload."""
    recorded_requests: list[IngestionRunRequest] = []

    async def _fake_run_ingestion(request: IngestionRunRequest) -> IngestFinalizationPayload:
        recorded_requests.append(request)
        return _build_fake_ingest_payload(request)

    monkeypatch.setattr(worker_module, "run_ingestion", _fake_run_ingestion)
    return recorded_requests


async def test_mark_recovery_enqueue_failed_logs_only_safe_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery enqueue failure logging should exclude exception text and traceback."""
    job_id = uuid.uuid4()
    logger_error_calls: list[tuple[str, dict[str, Any]]] = []
    marked_failed_job_ids: list[uuid.UUID] = []

    async def _fake_mark_job_failed_if_recovery_safe(
        failed_job_id: uuid.UUID,
        *,
        error_message: str,
        error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
    ) -> bool:
        marked_failed_job_ids.append(failed_job_id)
        assert error_message == "Failed to enqueue ingest job"
        assert error_code == ErrorCode.INTERNAL_ERROR
        return True

    def _capture_logger_error(event: str, **kwargs: Any) -> None:
        logger_error_calls.append((event, kwargs))

    monkeypatch.setattr(
        worker_module,
        "_mark_job_failed_if_recovery_safe",
        _fake_mark_job_failed_if_recovery_safe,
    )
    monkeypatch.setattr(worker_module.logger, "error", _capture_logger_error)

    marked_failed = await worker_module._mark_recovery_enqueue_failed(job_id)

    assert marked_failed is True
    assert marked_failed_job_ids == [job_id]
    assert logger_error_calls == [
        (
            "ingest_job_recovery_enqueue_failed",
            {
                "job_id": str(job_id),
                "error_code": ErrorCode.INTERNAL_ERROR.value,
                "recovery_action": "mark_failed",
            },
        )
    ]


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

        async def _skip_publish(_: uuid.UUID) -> bool:
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

        async def _skip_publish(_: uuid.UUID) -> bool:
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

        secret_checksum = "f" * 64
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
        await _update_source_file(uuid.UUID(uploaded["id"]), checksum_sha256=secret_checksum)

        with pytest.raises(
            IngestionRunnerError,
            match="Failed to read original source from storage",
        ):
            await worker_module.process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.STORAGE_FAILED.value
        assert updated_job.error_message == "Failed to read original source from storage."
        assert secret_checksum not in updated_job.error_message

        response = await async_client.get(f"/v1/jobs/{job.id}/events")
        assert response.status_code == 200
        data = response.json()
        assert data["items"][-1]["data_json"] == {
            "status": "failed",
            "error_code": ErrorCode.STORAGE_FAILED.value,
            "error_message": "Failed to read original source from storage.",
        }
        assert secret_checksum not in str(data["items"][-1]["data_json"])
        assert secret_checksum not in str(logger_error_calls)
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
        await _update_source_file(uuid.UUID(uploaded["id"]), original_filename="..")
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
                datetime.now(UTC)
                - worker_module._RUNNING_JOB_STALE_AFTER
                - timedelta(seconds=1)
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
                datetime.now(UTC)
                - worker_module._RUNNING_JOB_STALE_AFTER
                - timedelta(seconds=1)
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

        async def _skip_publish(_: uuid.UUID) -> bool:
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

        async def _skip_publish(_: uuid.UUID) -> bool:
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

        async def _skip_publish(_: uuid.UUID) -> bool:
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

    async def test_reprocess_job_persists_base_revision_snapshot(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Reprocess creation should persist job_type and the latest finalized base revision."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        initial_job = await _get_job_for_file(str(uploaded["id"]))
        await worker_module.process_ingest_job(initial_job.id)

        response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(initial_job.extraction_profile_id)},
        )

        assert response.status_code == 202
        assert response.json()["job_type"] == "reprocess"
        assert response.json()["base_revision_id"] is not None

        reprocess_job = await _get_job(uuid.UUID(response.json()["id"]))
        assert reprocess_job.job_type == "reprocess"
        assert reprocess_job.base_revision_id == uuid.UUID(response.json()["base_revision_id"])

    async def test_reprocess_job_replays_success_when_publish_is_deferred_after_commit(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Reprocess idempotency should snapshot success before best-effort publish."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        initial_job = await _get_job_for_file(str(uploaded["id"]))
        await worker_module.process_ingest_job(initial_job.id)

        async def _skip_publish(_: uuid.UUID) -> bool:
            return False

        monkeypatch.setattr(files_api, "publish_job_enqueue_intent", _skip_publish)

        headers = {"Idempotency-Key": "reprocess-outbox-replay"}
        first = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(initial_job.extraction_profile_id)},
            headers=headers,
        )
        second = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(initial_job.extraction_profile_id)},
            headers=headers,
        )

        assert first.status_code == 202
        assert second.status_code == 202
        assert second.json() == first.json()

        reprocess_job = await _get_job(uuid.UUID(first.json()["id"]))
        assert reprocess_job.status == "pending"
        assert reprocess_job.enqueue_status == "pending"

    async def test_reprocess_job_succeeds_when_enqueue_finalize_raises_after_commit(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Reprocess should not fail if post-commit enqueue bookkeeping raises after publish."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        initial_job = await _get_job_for_file(str(uploaded["id"]))
        await worker_module.process_ingest_job(initial_job.id)

        async def _fail_finalize(_: uuid.UUID, *, lease_token: uuid.UUID) -> bool:
            _ = lease_token
            raise RuntimeError("transient enqueue finalize failure")

        monkeypatch.setattr(worker_module, "_mark_job_enqueue_published", _fail_finalize)

        response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(initial_job.extraction_profile_id)},
        )

        assert response.status_code == 202
        job = await _get_job(uuid.UUID(response.json()["id"]))
        assert job.status == "pending"
        assert job.enqueue_status == "publishing"

    async def test_process_reprocess_job_rejects_payload_revision_kind_mismatch(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Worker finalization should fail reprocess jobs whose payload kind drifts."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        initial_job = await _get_job_for_file(str(uploaded["id"]))
        await worker_module.process_ingest_job(initial_job.id)

        response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(initial_job.extraction_profile_id)},
        )
        assert response.status_code == 202
        reprocess_job = await _get_job(uuid.UUID(response.json()["id"]))

        async def _run_ingestion_with_wrong_kind(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            return replace(_build_fake_ingest_payload(request), revision_kind="ingest")

        monkeypatch.setattr(worker_module, "run_ingestion", _run_ingestion_with_wrong_kind)

        with pytest.raises(
            worker_module._RevisionConflictError,
            match=r"Ingest job revision kind changed before finalization\.",
        ):
            await worker_module.process_ingest_job(reprocess_job.id)

        failed_job = await _get_job(reprocess_job.id)
        assert failed_job.status == "failed"
        assert failed_job.error_code == ErrorCode.REVISION_CONFLICT.value
        assert failed_job.error_message == "Ingest job revision kind changed before finalization."

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
                datetime.now(UTC)
                - worker_module._RUNNING_JOB_STALE_AFTER
                - timedelta(seconds=1)
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
                datetime.now(UTC)
                - worker_module._RUNNING_JOB_STALE_AFTER
                - timedelta(seconds=1)
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

        async def _skip_publish(_: uuid.UUID) -> bool:
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
                "ingest_job_recovery_enqueue_failed",
                {
                    "job_id": str(job.id),
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

        marked_failed = await worker_module._mark_recovery_enqueue_failed(job.id)

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

        marked_failed = await worker_module._mark_recovery_enqueue_failed(job.id)

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

        await worker_module.process_ingest_job(job.id)
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

        async def _fake_retry_publish(job_id: uuid.UUID) -> bool:
            retried_job_ids.append(str(job_id))
            return True

        monkeypatch.setattr(
            jobs_api,
            "publish_job_enqueue_intent",
            _fake_retry_publish,
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
        assert updated.error_code is None
        assert updated.error_message is None

    async def test_retry_job_succeeds_when_publish_is_deferred(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry should succeed once durable enqueue intent commits."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        async def _skip_retry_publish(_: uuid.UUID) -> bool:
            return False

        monkeypatch.setattr(
            jobs_api,
            "publish_job_enqueue_intent",
            _skip_retry_publish,
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

        updated = await _get_job(job.id)
        assert updated.status == "pending"
        assert updated.error_code is None
        assert updated.error_message is None
        assert updated.finished_at is None
        assert updated.enqueue_status == "pending"

    async def test_retry_job_replays_success_when_publish_is_deferred_after_commit(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry idempotency should snapshot success before best-effort publish."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        async def _skip_retry_publish(_: uuid.UUID) -> bool:
            return False

        monkeypatch.setattr(
            jobs_api,
            "publish_job_enqueue_intent",
            _skip_retry_publish,
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

        headers = {"Idempotency-Key": "retry-outbox-replay"}
        first = await async_client.post(f"/v1/jobs/{job.id}/retry", headers=headers)
        second = await async_client.post(f"/v1/jobs/{job.id}/retry", headers=headers)

        assert first.status_code == 202
        assert second.status_code == 202
        assert second.json() == first.json()

        updated = await _get_job(job.id)
        assert updated.status == "pending"
        assert updated.enqueue_status == "pending"

    async def test_retry_job_succeeds_when_enqueue_claim_raises_after_commit(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry should not fail if post-commit enqueue bookkeeping raises before claim."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        async def _fail_claim(_: uuid.UUID) -> worker_module._EnqueueIntentLease | None:
            raise RuntimeError("transient enqueue claim failure")

        monkeypatch.setattr(worker_module, "_claim_job_enqueue_intent", _fail_claim)

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
        updated = await _get_job(job.id)
        assert updated.status == "pending"
        assert updated.enqueue_status == "pending"

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

        async def _fake_retry_publish(job_id: uuid.UUID) -> bool:
            retried_job_ids.append(str(job_id))
            return True

        monkeypatch.setattr(
            jobs_api,
            "publish_job_enqueue_intent",
            _fake_retry_publish,
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

    async def test_retry_job_noops_for_revision_conflict_failure(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry should not requeue jobs that already failed with REVISION_CONFLICT."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        retried_job_ids: list[str] = []

        async def _fake_retry_publish(job_id: uuid.UUID) -> bool:
            retried_job_ids.append(str(job_id))
            return True

        monkeypatch.setattr(
            jobs_api,
            "publish_job_enqueue_intent",
            _fake_retry_publish,
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
            error_code=ErrorCode.REVISION_CONFLICT.value,
            error_message="Reprocess base revision became stale before finalization.",
        )

        response = await async_client.post(f"/v1/jobs/{job.id}/retry")

        assert response.status_code == 202
        assert retried_job_ids == []
        unchanged = await _get_job(job.id)
        assert unchanged.status == "failed"
        assert unchanged.error_code == ErrorCode.REVISION_CONFLICT.value
        assert (
            unchanged.error_message
            == "Reprocess base revision became stale before finalization."
        )

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

        async def _fake_retry_publish(job_id: uuid.UUID) -> bool:
            retried_job_ids.append(str(job_id))
            return True

        monkeypatch.setattr(
            jobs_api,
            "publish_job_enqueue_intent",
            _fake_retry_publish,
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

    @pytest.mark.parametrize(
        ("delete_project", "delete_file"),
        [(True, False), (False, True)],
    )
    async def test_retry_job_returns_404_for_soft_deleted_source(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
        delete_project: bool,
        delete_file: bool,
    ) -> None:
        """Retry should hide jobs whose backing project or file is soft-deleted."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        retried_job_ids: list[str] = []

        async def _fake_retry_publish(job_id: uuid.UUID) -> bool:
            retried_job_ids.append(str(job_id))
            return True

        monkeypatch.setattr(
            jobs_api,
            "publish_job_enqueue_intent",
            _fake_retry_publish,
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
        await _mark_source_deleted(
            uuid.UUID(project["id"]),
            uuid.UUID(uploaded["id"]),
            delete_project=delete_project,
            delete_file=delete_file,
        )

        response = await async_client.post(f"/v1/jobs/{job.id}/retry")

        assert response.status_code == 404
        assert response.json() == {
            "error": {
                "code": "NOT_FOUND",
                "message": f"Job with identifier '{job.id}' not found",
                "details": None,
            }
        }
        assert retried_job_ids == []
        unchanged = await _get_job(job.id)
        assert unchanged.status == "failed"

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

    async def test_job_constraints_accept_valid_type_status_error_writes(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Valid constrained job writes should still commit."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        initial_job = await _get_job_for_file(str(uploaded["id"]))
        await worker_module.process_ingest_job(initial_job.id)
        reprocess_response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(initial_job.extraction_profile_id)},
        )
        assert reprocess_response.status_code == 202
        job = await _get_job(uuid.UUID(reprocess_response.json()["id"]))

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            persisted_job = await session.get(Job, job.id)
            assert persisted_job is not None

            persisted_job.job_type = "reprocess"
            persisted_job.status = "failed"
            persisted_job.error_code = ErrorCode.ADAPTER_FAILED.value
            persisted_job.error_message = "Adapter execution failed."
            persisted_job.enqueue_status = "published"

            await session.commit()

        updated = await _get_job(job.id)
        assert updated.job_type == "reprocess"
        assert updated.status == "failed"
        assert updated.error_code == ErrorCode.ADAPTER_FAILED.value

    @pytest.mark.parametrize(
        ("field_name", "invalid_value"),
        [
            ("job_type", "not-a-job-type"),
            ("status", "queued"),
            ("error_code", "NOT_A_REAL_ERROR_CODE"),
            ("enqueue_status", "queued"),
        ],
    )
    async def test_job_constraints_reject_invalid_string_writes(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        field_name: str,
        invalid_value: str,
    ) -> None:
        """Invalid constrained string writes should fail at the database."""
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

            setattr(persisted_job, field_name, invalid_value)

            with pytest.raises(IntegrityError):
                await session.commit()

            await session.rollback()

    @pytest.mark.parametrize("job_type", ["ingest", "reprocess"])
    async def test_job_constraints_reject_profile_required_job_without_extraction_profile(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        job_type: str,
    ) -> None:
        """Persisted ingest/reprocess jobs must retain an extraction profile identifier."""
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

            persisted_job.job_type = job_type
            persisted_job.extraction_profile_id = None

            with pytest.raises(IntegrityError):
                await session.commit()

            await session.rollback()
