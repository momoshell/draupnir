"""Smoke tests for Draupnir."""

import io
import json
import logging
import os
import subprocess
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import UUID

import httpx
import pytest
from sqlalchemy import select

import app.api.v1.system as system_endpoints
from app import __version__
from app.core.config import settings
from app.core.middleware import REQUEST_ID_PATTERN
from app.db.session import AsyncSessionLocal
from app.ingestion.finalization import IngestFinalizationPayload
from app.jobs.worker import _INITIAL_INGEST_REVISION_KIND, process_ingest_job
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.validation_report import ValidationReport
from app.schemas.system import (
    AdapterHealthCheck,
    DependencyHealthCheck,
    SystemCheckStatus,
)
from tests.conftest import requires_database


def test_version() -> None:
    """Test that version is a non-empty string."""
    assert isinstance(__version__, str)
    assert len(__version__) > 0


def _fake_ingest_payload(*, generated_at: datetime) -> IngestFinalizationPayload:
    return IngestFinalizationPayload(
        revision_kind=_INITIAL_INGEST_REVISION_KIND,
        adapter_key="smoke-test-adapter",
        adapter_version="0.0.1",
        input_family="vector_pdf",
        canonical_entity_schema_version="v1",
        canonical_json={"entities": []},
        provenance_json={"source": "smoke-test"},
        confidence_json={"entities": []},
        confidence_score=0.99,
        warnings_json=[],
        diagnostics_json={},
        result_checksum_sha256="a" * 64,
        validation_report_schema_version="0.1",
        validation_status="valid_with_warnings",
        review_state="approved",
        quantity_gate="allowed",
        effective_confidence=0.99,
        validator_name="smoke-validator",
        validator_version="0.0.1",
        report_json={
            "summary": {"status": "ok", "issues": 0},
            "checks": [],
            "findings": [],
            "adapter_warnings": [],
            "provenance": {},
            "validator": {"name": "smoke-validator", "version": "0.0.1"},
        },
        generated_at=generated_at,
    )

class TestHealthEndpoint:
    """Smoke tests for the health check endpoint."""

    async def test_health_endpoint_returns_200_and_json_shape(
        self,
        async_client: httpx.AsyncClient,
    ) -> None:
        """Test that GET /v1/health returns 200 with correct JSON shape.

        Success case: status is "ok", version field is present and non-null
        when settings.expose_version_in_health is True, otherwise null.
        """
        response = await async_client.get("/v1/health")

        # Assert status code
        assert response.status_code == 200

        # Parse JSON response
        data = response.json()

        # Assert required status field
        assert data["status"] == "ok"

        # Assert exact keys in response
        assert set(data.keys()) == {"status", "version"}

        # Assert version field based on expose_version_in_health setting
        if settings.expose_version_in_health:
            assert isinstance(data["version"], str)
            assert len(data["version"]) > 0
        else:
            assert data["version"] is None

    async def test_health_endpoint_returns_request_id(
        self,
        async_client: httpx.AsyncClient,
    ) -> None:
        """Test that GET /v1/health returns a valid X-Request-Id header.

        Success case: X-Request-Id header exists and matches the expected pattern
        (alphanumeric, hyphens, underscores, max 64 chars).
        """
        response = await async_client.get("/v1/health")

        # Assert status code
        assert response.status_code == 200

        # Assert X-Request-Id header exists
        assert "X-Request-Id" in response.headers

        request_id = response.headers["X-Request-Id"]

        # Assert request ID matches the expected pattern
        assert REQUEST_ID_PATTERN.match(request_id) is not None, (
            f"Request ID '{request_id}' does not match pattern "
            f"{REQUEST_ID_PATTERN.pattern}"
        )

        # Assert request ID length is within bounds (max 64 chars)
        assert len(request_id) <= 64, (
            f"Request ID '{request_id}' exceeds maximum length of 64 characters"
        )

        # Assert request ID is not empty
        assert len(request_id) > 0, "Request ID should not be empty"

    async def test_health_endpoint_logs_request_id(
        self,
        async_client: httpx.AsyncClient,
    ) -> None:
        """Test that request_id appears in structlog JSON output.

        Success case: The same request_id from the X-Request-Id response header
        appears in the log output as the `request_id` field in a JSON log line.

        structlog writes JSON to stdout via a StreamHandler. We temporarily
        replace the handler's stream with a StringIO to capture the output.
        """
        # Get the root logger and its handler
        root_logger = logging.getLogger()
        handler = root_logger.handlers[0] if root_logger.handlers else None

        if handler is None or not isinstance(handler, logging.StreamHandler):
            pytest.skip("No StreamHandler configured")

        # Save original stream and replace with StringIO
        original_stream = handler.stream
        captured_output = io.StringIO()
        handler.stream = captured_output

        try:
            response = await async_client.get("/v1/health")

            # Assert status code
            assert response.status_code == 200

            # Get the request_id from the response header
            assert "X-Request-Id" in response.headers
            request_id = response.headers["X-Request-Id"]
        finally:
            # Restore original stream
            handler.stream = original_stream

        # Get captured output where structlog writes JSON lines
        stdout_output = captured_output.getvalue()

        # Parse each line as JSON and look for request_id field
        found_request_id_in_logs = False
        for line in stdout_output.strip().split("\n"):
            if not line.strip():
                continue
            try:
                log_entry = json.loads(line)
                if log_entry.get("request_id") == request_id:
                    found_request_id_in_logs = True
                    break
            except json.JSONDecodeError:
                # Skip non-JSON lines
                continue

        assert found_request_id_in_logs, (
            f"Request ID '{request_id}' from response header not found "
            f"in structlog JSON output. Captured stdout: {stdout_output[:500]}"
        )


class TestSystemHealthSmoke:
    """Smoke tests for degraded system health behavior."""

    async def test_system_health_degraded_while_shallow_health_stays_ok(
        self,
        async_client: httpx.AsyncClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """System health should degrade independently from shallow health."""

        async def _probe_database_ok() -> DependencyHealthCheck:
            return DependencyHealthCheck(
                status=SystemCheckStatus.OK,
                latency_ms=1.0,
                details={"reachable": True},
            )

        async def _probe_storage_ok() -> DependencyHealthCheck:
            return DependencyHealthCheck(
                status=SystemCheckStatus.OK,
                latency_ms=1.0,
                details=None,
            )

        async def _probe_broker_degraded() -> DependencyHealthCheck:
            return DependencyHealthCheck(
                status=SystemCheckStatus.DEGRADED,
                latency_ms=1.0,
                details={"reachable": True, "warning": "simulated"},
            )

        async def _probe_adapters_ok() -> tuple[
            list[AdapterHealthCheck],
            SystemCheckStatus,
        ]:
            return ([], SystemCheckStatus.OK)

        monkeypatch.setattr(system_endpoints, "_probe_database_check", _probe_database_ok)
        monkeypatch.setattr(system_endpoints, "_probe_storage_check", _probe_storage_ok)
        monkeypatch.setattr(system_endpoints, "_probe_broker_check", _probe_broker_degraded)
        monkeypatch.setattr(system_endpoints, "_build_adapter_health_checks", _probe_adapters_ok)

        system_response = await async_client.get("/v1/system/health")

        assert system_response.status_code == 503
        system_data = system_response.json()
        assert system_data["status"] == "degraded"
        assert system_data["checks"]["broker"]["status"] == "degraded"

        shallow_response = await async_client.get("/v1/health")

        assert shallow_response.status_code == 200
        assert shallow_response.json()["status"] == "ok"


@requires_database
@pytest.mark.usefixtures("init_database_resources")
class TestIngestWorkflowSmoke:
    """Smoke tests for upload -> ingest -> persisted output visibility."""

    async def test_upload_ingest_job_persists_outputs_and_exposes_validation(
        self,
        async_client: httpx.AsyncClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Run one ingest happy path and verify persisted output visibility."""
        generated_at = datetime(2026, 1, 1, tzinfo=UTC)
        payload = _fake_ingest_payload(generated_at=generated_at)
        enqueued_job_ids: list[UUID] = []

        async def _fake_run_ingestion(
            *args: object, **kwargs: object
        ) -> IngestFinalizationPayload:
            del args, kwargs
            return payload

        def _fake_enqueue_ingest_job(job_id: UUID) -> None:
            enqueued_job_ids.append(job_id)

        monkeypatch.setattr("app.jobs.worker.run_ingestion", _fake_run_ingestion)
        monkeypatch.setattr("app.api.v1.files.enqueue_ingest_job", _fake_enqueue_ingest_job)

        project_response = await async_client.post(
            "/v1/projects",
            json={"name": "smoke-project"},
        )
        assert project_response.status_code == 201
        project_id = project_response.json()["id"]

        upload_response = await async_client.post(
            f"/v1/projects/{project_id}/files",
            files={"file": ("smoke.pdf", b"%PDF-1.4\n%%EOF\n", "application/pdf")},
        )

        assert upload_response.status_code == 201
        upload_data = upload_response.json()
        assert upload_data["initial_job_id"] is not None
        assert upload_data["initial_extraction_profile_id"] is not None

        job_id = UUID(upload_data["initial_job_id"])
        assert enqueued_job_ids == [job_id]

        await process_ingest_job(job_id)

        job_response = await async_client.get(f"/v1/jobs/{job_id}")
        assert job_response.status_code == 200
        job_data = job_response.json()
        assert job_data["status"] == "succeeded"
        assert job_data["finished_at"] is not None

        assert AsyncSessionLocal is not None
        async with AsyncSessionLocal() as db_session:
            adapter_output = await db_session.scalar(
                select(AdapterRunOutput).where(AdapterRunOutput.source_job_id == job_id)
            )
            assert adapter_output is not None

            drawing_revision = await db_session.scalar(
                select(DrawingRevision).where(DrawingRevision.source_job_id == job_id)
            )
            assert drawing_revision is not None

            validation_report = await db_session.scalar(
                select(ValidationReport).where(ValidationReport.source_job_id == job_id)
            )
            assert validation_report is not None

        validation_response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/validation-report"
        )
        assert validation_response.status_code == 200
        validation_data = validation_response.json()
        assert validation_data["drawing_revision_id"] == str(drawing_revision.id)
        assert validation_data["source_job_id"] == str(job_id)

    async def test_upload_rejects_missing_file(
        self,
        async_client: httpx.AsyncClient,
    ) -> None:
        """Upload endpoint rejects requests without multipart file content."""
        project_response = await async_client.post(
            "/v1/projects",
            json={"name": "smoke-project-missing-file"},
        )
        assert project_response.status_code == 201
        project_id = project_response.json()["id"]

        upload_response = await async_client.post(f"/v1/projects/{project_id}/files")

        assert upload_response.status_code == 422


# Skip unless compose smoke is explicitly enabled.
COMPOSE_SMOKE = os.environ.get("COMPOSE_SMOKE") == "1"
SMOKE_BASE_URL = os.environ.get("SMOKE_BASE_URL", "")
REPO_ROOT = Path(__file__).resolve().parents[1]
WORKER_STORAGE_READ_SCRIPT = """
import asyncio
import hashlib
import inspect
import json
import os

from app.storage.dependencies import get_storage


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value

async def main():
    storage = get_storage()
    storage_key = os.environ["STORAGE_KEY"]
    expected_sha256 = os.environ["EXPECTED_SHA256"]

    metadata = await _maybe_await(
        storage.stat(storage_key, expected_checksum_sha256=expected_sha256)
    )
    stored_object = await _maybe_await(
        storage.get(storage_key, expected_checksum_sha256=expected_sha256)
    )
    content = stored_object.body
    if hasattr(content, "read"):
        content = await _maybe_await(content.read())

    digest = hashlib.sha256(content).hexdigest()
    if digest != expected_sha256:
        raise AssertionError(f"sha256 mismatch: {digest} != {expected_sha256}")

    print(
        json.dumps(
            {
                "path": storage_key,
                "sha256": digest,
                "size": metadata.size_bytes,
            }
        )
    )


asyncio.run(main())
"""


def _read_original_from_worker_storage(
    *, storage_key: str, expected_sha256: str
) -> dict[str, object]:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "-e",
            f"STORAGE_KEY={storage_key}",
            "-e",
            f"EXPECTED_SHA256={expected_sha256}",
            "worker",
            "python",
            "-c",
            WORKER_STORAGE_READ_SCRIPT,
        ],
        check=False,
        capture_output=True,
        cwd=REPO_ROOT,
        text=True,
    )

    assert result.returncode == 0, result.stderr or result.stdout

    stdout_lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert stdout_lines, "worker storage probe produced no stdout"

    payload: object = json.loads(stdout_lines[-1])
    assert isinstance(payload, dict), "worker storage probe returned non-object JSON"
    assert all(
        isinstance(key, str) for key in payload
    ), "worker storage probe returned non-string keys"

    return cast(dict[str, object], payload)


@pytest.mark.skipif(
    not (COMPOSE_SMOKE and SMOKE_BASE_URL),
    reason="COMPOSE_SMOKE=1 and SMOKE_BASE_URL must be set for compose smoke",
)
class TestHealthEndpointRealServer:
    """Smoke tests against a real running server (compose stack)."""

    @pytest.fixture
    async def real_async_client(self) -> AsyncGenerator[httpx.AsyncClient, None]:
        """Provide an async HTTP client for testing against real server."""
        base_url = SMOKE_BASE_URL or "http://localhost:8000"
        async with httpx.AsyncClient(base_url=base_url) as client:
            yield client

    async def test_health_endpoint_against_real_server(
        self,
        real_async_client: httpx.AsyncClient,
    ) -> None:
        """Test that GET /v1/health returns 200 against real server.

        Success case: Real server responds with 200 and correct JSON shape.
        This test only runs when compose smoke is explicitly enabled.
        """
        response = await real_async_client.get("/v1/health")

        # Assert status code
        assert response.status_code == 200

        # Parse JSON response
        data = response.json()

        # Assert required status field
        assert data["status"] == "ok"

        # Assert X-Request-Id header exists
        assert "X-Request-Id" in response.headers
        request_id = response.headers["X-Request-Id"]

        # Assert request ID matches the expected pattern
        assert REQUEST_ID_PATTERN.match(request_id) is not None

        # Assert request ID length is within bounds
        assert len(request_id) <= 64
        assert len(request_id) > 0

    async def test_upload_original_is_readable_from_worker_storage(
        self,
        real_async_client: httpx.AsyncClient,
    ) -> None:
        """Compose stack shares immutable upload storage between API and worker."""
        pdf_bytes = b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\ntrailer\n<<>>\n%%EOF\n"

        project_response = await real_async_client.post(
            "/v1/projects",
            json={"name": "compose-smoke-project"},
        )
        assert project_response.status_code == 201
        project_id = project_response.json()["id"]

        upload_response = await real_async_client.post(
            f"/v1/projects/{project_id}/files",
            files={"file": ("compose-smoke.pdf", pdf_bytes, "application/pdf")},
        )
        assert upload_response.status_code == 201

        upload_data = upload_response.json()
        file_id = upload_data["id"]
        UUID(file_id)
        checksum_sha256 = upload_data["checksum_sha256"]
        storage_key = f"originals/{file_id}/{checksum_sha256}"

        worker_data = _read_original_from_worker_storage(
            storage_key=storage_key,
            expected_sha256=checksum_sha256,
        )

        assert worker_data == {
            "path": storage_key,
            "sha256": checksum_sha256,
            "size": len(pdf_bytes),
        }
