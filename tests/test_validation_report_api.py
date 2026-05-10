"""Integration tests for validation report API responses."""

import uuid
from datetime import UTC, datetime

import httpx
import pytest

import app.db.session as session_module
import app.jobs.worker as worker_module
from app.core.errors import ErrorCode
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunRequest
from app.jobs.worker import process_ingest_job
from app.models.validation_report import ValidationReport
from tests.conftest import requires_database
from tests.test_ingest_output_persistence import (
    _assert_validation_report_json_matches_columns,
    _load_project_outputs,
)
from tests.test_jobs import (
    _build_fake_ingest_payload,
    _create_project,
    _get_job_for_file,
    _upload_file,
)


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


def _parse_timestamp(value: str) -> datetime:
    """Parse API timestamps while accepting UTC Z suffix serialization."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


@requires_database
class TestValidationReportApi:
    """Tests for revision validation report retrieval."""

    async def test_get_validation_report_returns_canonical_response(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """The API should return the persisted canonical validation report."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(job.id)

        _adapter_outputs, drawing_revisions, validation_reports, _generated_artifacts = (
            await _load_project_outputs(project["id"])
        )
        drawing_revision = drawing_revisions[0]
        validation_report = validation_reports[0]

        response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/validation-report"
        )

        assert response.status_code == 200

        body = response.json()
        _assert_validation_report_json_matches_columns(validation_report)
        assert body["validation_report_id"] == str(validation_report.id)
        assert body["drawing_revision_id"] == str(drawing_revision.id)
        assert body["source_job_id"] == str(validation_report.source_job_id)
        assert (
            body["validation_report_schema_version"]
            == validation_report.validation_report_schema_version
        )
        assert (
            body["canonical_entity_schema_version"]
            == validation_report.canonical_entity_schema_version
        )
        assert body["validation_status"] == validation_report.validation_status
        assert body["review_state"] == validation_report.review_state
        assert body["quantity_gate"] == validation_report.quantity_gate
        assert body["effective_confidence"] == validation_report.effective_confidence
        assert body["validator"] == {
            "name": validation_report.validator_name,
            "version": validation_report.validator_version,
        }
        assert body["confidence"]["effective_confidence"] == validation_report.effective_confidence
        assert body["confidence"]["review_state"] == validation_report.review_state
        assert body["confidence"]["review_required"] == (
            validation_report.review_state == "review_required"
        )
        assert _parse_timestamp(body["generated_at"]) == (
            validation_report.generated_at.astimezone(UTC)
        )
        assert body["summary"]["validation_status"] == validation_report.validation_status
        assert body["summary"]["review_state"] == validation_report.review_state
        assert body["summary"]["quantity_gate"] == validation_report.quantity_gate
        assert (
            body["summary"]["effective_confidence"]
            == validation_report.effective_confidence
        )
        assert body["checks"]
        assert body["findings"] == validation_report.report_json["findings"]
        assert (
            body["adapter_warnings"] == validation_report.report_json["adapter_warnings"]
        )
        assert body["provenance"] == validation_report.report_json["provenance"]

    async def test_get_validation_report_rewrites_nested_confidence_from_db_columns(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """API output should prefer persisted columns over stale nested confidence JSON."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(job.id)

        _adapter_outputs, drawing_revisions, validation_reports, _generated_artifacts = (
            await _load_project_outputs(project["id"])
        )
        drawing_revision = drawing_revisions[0]
        validation_report = validation_reports[0]

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            persisted_report = await session.get(ValidationReport, validation_report.id)
            assert persisted_report is not None
            persisted_report.validation_status = "needs_review"
            persisted_report.review_state = "review_required"
            persisted_report.quantity_gate = "review_gated"
            persisted_report.effective_confidence = 0.59
            persisted_report.report_json = {
                **persisted_report.report_json,
                "confidence": {
                    "score": 0.95,
                    "effective_confidence": 0.95,
                    "review_state": "approved",
                    "review_required": False,
                    "basis": "stale",
                },
                "summary": {
                    **persisted_report.report_json["summary"],
                    "validation_status": "valid",
                    "review_state": "approved",
                    "quantity_gate": "allowed",
                    "effective_confidence": 0.95,
                },
            }
            await session.commit()

        response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/validation-report"
        )

        assert response.status_code == 200
        body = response.json()
        assert body["validation_status"] == "needs_review"
        assert body["review_state"] == "review_required"
        assert body["quantity_gate"] == "review_gated"
        assert body["effective_confidence"] == 0.59
        assert body["summary"]["validation_status"] == "needs_review"
        assert body["summary"]["review_state"] == "review_required"
        assert body["summary"]["quantity_gate"] == "review_gated"
        assert body["summary"]["effective_confidence"] == 0.59
        assert body["confidence"] == {
            "score": 0.95,
            "effective_confidence": 0.59,
            "review_state": "review_required",
            "review_required": True,
            "basis": "stale",
        }

    async def test_get_validation_report_returns_404_for_missing_revision(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Unknown revisions should return the standardized not found error."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(job.id)

        _adapter_outputs, _drawing_revisions, validation_reports, _generated_artifacts = (
            await _load_project_outputs(project["id"])
        )
        assert len(validation_reports) == 1

        revision_id = uuid.uuid4()
        response = await async_client.get(f"/v1/revisions/{revision_id}/validation-report")

        assert response.status_code == 404
        assert response.json() == {
            "error": {
                "code": ErrorCode.NOT_FOUND.value,
                "message": f"Drawing revision with identifier '{revision_id}' not found",
                "details": None,
            }
        }

        (
            _adapter_outputs,
            _drawing_revisions,
            validation_reports_after,
            _generated_artifacts,
        ) = await _load_project_outputs(project["id"])
        assert len(validation_reports_after) == 1

    async def test_get_validation_report_returns_404_when_report_missing(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Existing revisions without persisted reports should return 404."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(job.id)

        _adapter_outputs, drawing_revisions, validation_reports, _generated_artifacts = (
            await _load_project_outputs(project["id"])
        )
        drawing_revision = drawing_revisions[0]
        validation_report = validation_reports[0]

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            persisted_report = await session.get(ValidationReport, validation_report.id)
            assert persisted_report is not None
            await session.delete(persisted_report)
            await session.commit()

        response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/validation-report"
        )

        assert response.status_code == 404
        assert response.json() == {
            "error": {
                "code": ErrorCode.NOT_FOUND.value,
                "message": (
                    f"Validation report with identifier '{drawing_revision.id}' not found"
                ),
                "details": None,
            }
        }

        (
            _adapter_outputs,
            _drawing_revisions,
            validation_reports_after,
            _generated_artifacts,
        ) = await _load_project_outputs(project["id"])
        assert validation_reports_after == []

    async def test_get_validation_report_returns_404_for_soft_deleted_project_data(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Validation report reads should hide revisions under soft-deleted project/file data."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(job.id)

        _adapter_outputs, drawing_revisions, _validation_reports, _generated_artifacts = (
            await _load_project_outputs(project["id"])
        )
        drawing_revision = drawing_revisions[0]

        delete_response = await async_client.delete(f"/v1/projects/{project['id']}")
        assert delete_response.status_code == 204

        response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/validation-report"
        )

        assert response.status_code == 404
        assert response.json() == {
            "error": {
                "code": ErrorCode.NOT_FOUND.value,
                "message": f"Drawing revision with identifier '{drawing_revision.id}' not found",
                "details": None,
            }
        }
