"""Shared helpers for job integration tests."""

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
from sqlalchemy import select

import app.db.session as session_module
import app.jobs.worker as worker_module
from app.ingestion.finalization import IngestFinalizationPayload, compute_adapter_result_checksum
from app.ingestion.runner import IngestionRunRequest
from app.models.file import File
from app.models.generated_artifact import GeneratedArtifact
from app.models.job import Job
from app.models.job_event import JobEvent
from app.models.project import Project

_TEST_UPLOAD_BODY = b"%PDF-1.7\njob-test\n"
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
_UNSET = object()


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
    *,
    filename: str = "plan.pdf",
    content: bytes = _TEST_UPLOAD_BODY,
    media_type: str = "application/pdf",
) -> dict[str, Any]:
    """Upload a supported file and return its payload."""
    response = await async_client.post(
        f"/v1/projects/{project_id}/files",
        files={"file": (filename, content, media_type)},
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


async def _get_generated_artifacts_for_job(job_id: uuid.UUID) -> list[GeneratedArtifact]:
    """Load generated artifacts for a job id."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        artifacts = (
            (
                await session.execute(
                    select(GeneratedArtifact)
                    .where(GeneratedArtifact.job_id == job_id)
                    .order_by(GeneratedArtifact.created_at.asc(), GeneratedArtifact.id.asc())
                )
            )
            .scalars()
            .all()
        )

    return list(artifacts)


async def _remove_source_file_bytes(file_id: uuid.UUID) -> str:
    """Delete stored source bytes without mutating append-only file metadata."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        source_file = await session.get(File, file_id)
        assert source_file is not None
        storage_uri = source_file.storage_uri

    assert storage_uri.startswith("file://")
    storage_path = Path(storage_uri.removeprefix("file://"))
    assert storage_path.exists()
    storage_path.unlink()
    return storage_uri


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
        warnings_json=warnings_json,
        diagnostics_json=diagnostics_json,
        result_checksum_sha256=compute_adapter_result_checksum(result_envelope),
        validation_report_schema_version=_FAKE_RUNNER_VALIDATION_REPORT_SCHEMA_VERSION,
        validation_status=_FAKE_RUNNER_VALIDATION_STATUS,
        validator_name=_FAKE_RUNNER_VALIDATOR_NAME,
        validator_version=_FAKE_RUNNER_VALIDATOR_VERSION,
        report_json=report_json,
        generated_at=normalized_generated_at,
    )


@pytest.fixture
def fake_ingestion_runner(monkeypatch: pytest.MonkeyPatch) -> list[IngestionRunRequest]:
    """Patch worker ingestion with a deterministic fake runner payload."""
    recorded_requests: list[IngestionRunRequest] = []

    async def _fake_run_ingestion(request: IngestionRunRequest) -> IngestFinalizationPayload:
        recorded_requests.append(request)
        return _build_fake_ingest_payload(request)

    monkeypatch.setattr(worker_module, "run_ingestion", _fake_run_ingestion)
    return recorded_requests
