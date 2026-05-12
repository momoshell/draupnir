"""Integration tests for validation report API responses."""

import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

import app.db.session as session_module
import app.jobs.worker as worker_module
from app.core.errors import ErrorCode
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunRequest
from app.jobs.worker import process_ingest_job
from app.models.generated_artifact import GeneratedArtifact
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


def _artifact_ids(items: list[dict[str, Any]]) -> list[str]:
    """Return artifact identifiers from a JSON list response."""

    return [item["id"] for item in items]


def _clone_model(instance: Any, **overrides: Any) -> Any:
    """Clone a persisted SQLAlchemy model instance with selected overrides."""

    data = {column.name: getattr(instance, column.name) for column in instance.__table__.columns}
    data.update(overrides)
    return instance.__class__(**data)


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
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """API output should prefer persisted columns over stale nested confidence JSON."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _run_ingestion_with_stale_confidence(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            return replace(
                payload,
                validation_status="needs_review",
                review_state="review_required",
                quantity_gate="review_gated",
                effective_confidence=0.59,
                confidence_json={
                    "score": 0.95,
                    "effective_confidence": 0.95,
                    "review_state": "approved",
                    "basis": "stale",
                },
                report_json={
                    **payload.report_json,
                    "confidence": {
                        "score": 0.95,
                        "effective_confidence": 0.95,
                        "review_state": "approved",
                        "review_required": False,
                        "basis": "stale",
                    },
                    "summary": {
                        **payload.report_json["summary"],
                        "validation_status": "valid",
                        "review_state": "approved",
                        "quantity_gate": "allowed",
                        "effective_confidence": 0.95,
                    },
                },
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_ingestion_with_stale_confidence)

        await process_ingest_job(job.id)

        _adapter_outputs, drawing_revisions, _validation_reports, _generated_artifacts = (
            await _load_project_outputs(project["id"])
        )
        drawing_revision = drawing_revisions[0]

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

        adapter_outputs, drawing_revisions, validation_reports, _generated_artifacts = (
            await _load_project_outputs(project["id"])
        )
        drawing_revision = drawing_revisions[0]
        adapter_output = adapter_outputs[0]
        validation_report = validation_reports[0]

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        next_created_at = drawing_revision.created_at + timedelta(seconds=1)
        next_job = _clone_model(
            job,
            id=uuid.uuid4(),
            base_revision_id=drawing_revision.id,
            job_type="reprocess",
            status="succeeded",
            started_at=next_created_at,
            finished_at=next_created_at,
            created_at=next_created_at,
        )
        next_adapter_output = _clone_model(
            adapter_output,
            id=uuid.uuid4(),
            source_job_id=next_job.id,
            result_checksum_sha256=uuid.uuid4().hex * 2,
            created_at=next_created_at,
        )
        next_revision = _clone_model(
            drawing_revision,
            id=uuid.uuid4(),
            source_job_id=next_job.id,
            adapter_run_output_id=next_adapter_output.id,
            predecessor_revision_id=drawing_revision.id,
            revision_sequence=drawing_revision.revision_sequence + 1,
            created_at=next_created_at,
        )

        async with session_maker() as session:
            session.add(next_job)
            await session.commit()

        async with session_maker() as session:
            session.add(next_adapter_output)
            session.add(next_revision)
            await session.commit()

        response = await async_client.get(
            f"/v1/revisions/{next_revision.id}/validation-report"
        )

        assert response.status_code == 404
        assert response.json() == {
            "error": {
                "code": ErrorCode.NOT_FOUND.value,
                "message": (
                    f"Validation report with identifier '{next_revision.id}' not found"
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
        assert [report.id for report in validation_reports_after] == [validation_report.id]

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

    async def test_list_file_revisions_returns_revision_metadata(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """File revision listing should expose active revision metadata."""
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

        response = await async_client.get(f"/v1/files/{uploaded['id']}/revisions")

        assert response.status_code == 200
        body = response.json()
        assert [item["id"] for item in body["items"]] == [str(drawing_revision.id)]
        assert body["next_cursor"] is None

        item = body["items"][0]
        assert item["project_id"] == str(drawing_revision.project_id)
        assert item["source_file_id"] == str(drawing_revision.source_file_id)
        assert item["extraction_profile_id"] == str(drawing_revision.extraction_profile_id)
        assert item["source_job_id"] == str(drawing_revision.source_job_id)
        assert item["adapter_run_output_id"] == str(drawing_revision.adapter_run_output_id)
        assert item["predecessor_revision_id"] is None
        assert item["revision_sequence"] == drawing_revision.revision_sequence
        assert item["revision_kind"] == drawing_revision.revision_kind
        assert item["review_state"] == drawing_revision.review_state
        assert (
            item["canonical_entity_schema_version"]
            == drawing_revision.canonical_entity_schema_version
        )
        assert item["confidence_score"] == drawing_revision.confidence_score
        assert _parse_timestamp(item["created_at"]) == drawing_revision.created_at.astimezone(UTC)

    async def test_list_file_revisions_supports_limit_cursor_and_invalid_cursor(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """File revision listing should provide bounded cursor pagination."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(job.id)

        adapter_outputs, drawing_revisions, _validation_reports, _generated_artifacts = (
            await _load_project_outputs(project["id"])
        )
        adapter_output = adapter_outputs[0]
        drawing_revision = drawing_revisions[0]

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        next_created_at = drawing_revision.created_at + timedelta(seconds=1)
        next_job = _clone_model(
            job,
            id=uuid.uuid4(),
            base_revision_id=drawing_revision.id,
            job_type="reprocess",
            status="succeeded",
            started_at=next_created_at,
            finished_at=next_created_at,
            created_at=next_created_at,
        )
        next_adapter_output = _clone_model(
            adapter_output,
            id=uuid.uuid4(),
            source_job_id=next_job.id,
            result_checksum_sha256=uuid.uuid4().hex * 2,
            created_at=next_created_at,
        )
        next_revision = _clone_model(
            drawing_revision,
            id=uuid.uuid4(),
            source_job_id=next_job.id,
            adapter_run_output_id=next_adapter_output.id,
            predecessor_revision_id=drawing_revision.id,
            revision_sequence=drawing_revision.revision_sequence + 1,
            created_at=next_created_at,
        )

        async with session_maker() as session:
            session.add(next_job)
            await session.commit()

        async with session_maker() as session:
            session.add(next_adapter_output)
            session.add(next_revision)
            await session.commit()

        first_page_response = await async_client.get(
            f"/v1/files/{uploaded['id']}/revisions",
            params={"limit": 1},
        )

        assert first_page_response.status_code == 200
        first_page = first_page_response.json()
        assert [item["id"] for item in first_page["items"]] == [str(drawing_revision.id)]
        assert first_page["next_cursor"] is not None

        second_page_response = await async_client.get(
            f"/v1/files/{uploaded['id']}/revisions",
            params={"limit": 1, "cursor": first_page["next_cursor"]},
        )

        assert second_page_response.status_code == 200
        second_page = second_page_response.json()
        assert [item["id"] for item in second_page["items"]] == [str(next_revision.id)]
        assert second_page["next_cursor"] is None

        invalid_cursor_response = await async_client.get(
            f"/v1/files/{uploaded['id']}/revisions",
            params={"cursor": "not-a-valid-cursor"},
        )

        assert invalid_cursor_response.status_code == 422
        assert invalid_cursor_response.json() == {
            "error": {
                "code": ErrorCode.VALIDATION_ERROR.value,
                "message": "Invalid cursor",
                "details": None,
            }
        }

    async def test_get_adapter_output_returns_safe_metadata_for_revision_and_output(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Adapter output discoverability should expose metadata without raw payloads."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(job.id)

        adapter_outputs, drawing_revisions, _validation_reports, _generated_artifacts = (
            await _load_project_outputs(project["id"])
        )
        adapter_output = adapter_outputs[0]
        drawing_revision = drawing_revisions[0]

        by_revision_response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/adapter-output"
        )
        by_id_response = await async_client.get(f"/v1/adapter-outputs/{adapter_output.id}")

        assert by_revision_response.status_code == 200
        assert by_id_response.status_code == 200

        by_revision_body = by_revision_response.json()
        by_id_body = by_id_response.json()
        assert by_revision_body == by_id_body
        assert by_revision_body["id"] == str(adapter_output.id)
        assert by_revision_body["project_id"] == str(adapter_output.project_id)
        assert by_revision_body["source_file_id"] == str(adapter_output.source_file_id)
        assert (
            by_revision_body["extraction_profile_id"]
            == str(adapter_output.extraction_profile_id)
        )
        assert by_revision_body["source_job_id"] == str(adapter_output.source_job_id)
        assert by_revision_body["adapter_key"] == adapter_output.adapter_key
        assert by_revision_body["adapter_version"] == adapter_output.adapter_version
        assert by_revision_body["input_family"] == adapter_output.input_family
        assert (
            by_revision_body["canonical_entity_schema_version"]
            == adapter_output.canonical_entity_schema_version
        )
        assert by_revision_body["confidence_score"] == adapter_output.confidence_score
        assert (
            by_revision_body["result_checksum_sha256"]
            == adapter_output.result_checksum_sha256
        )
        assert _parse_timestamp(by_revision_body["created_at"]) == (
            adapter_output.created_at.astimezone(UTC)
        )
        assert "canonical_json" not in by_revision_body
        assert "provenance_json" not in by_revision_body
        assert "confidence_json" not in by_revision_body
        assert "warnings_json" not in by_revision_body
        assert "diagnostics_json" not in by_revision_body

    async def test_generated_artifact_lists_filter_deleted_rows_and_hide_storage_fields(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Generated artifact discoverability should exclude deleted rows and storage paths."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(job.id)

        _adapter_outputs, drawing_revisions, _validation_reports, generated_artifacts = (
            await _load_project_outputs(project["id"])
        )
        drawing_revision = drawing_revisions[0]

        file_response = await async_client.get(f"/v1/files/{uploaded['id']}/generated-artifacts")
        revision_response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/generated-artifacts"
        )

        assert file_response.status_code == 200
        assert revision_response.status_code == 200

        file_body = file_response.json()
        revision_body = revision_response.json()
        expected_revision_artifacts = [
            artifact
            for artifact in generated_artifacts
            if artifact.drawing_revision_id == drawing_revision.id
        ]
        assert _artifact_ids(file_body["items"]) == [
            str(artifact.id) for artifact in generated_artifacts
        ]
        assert _artifact_ids(revision_body["items"]) == [
            str(artifact.id) for artifact in expected_revision_artifacts
        ]
        assert file_body["next_cursor"] is None
        assert revision_body["next_cursor"] is None

        first_item = file_body["items"][0]
        first_artifact = generated_artifacts[0]
        assert first_item["project_id"] == str(first_artifact.project_id)
        assert first_item["source_file_id"] == str(first_artifact.source_file_id)
        assert first_item["job_id"] == str(first_artifact.job_id)
        assert first_item["drawing_revision_id"] == str(first_artifact.drawing_revision_id)
        assert (
            first_item["adapter_run_output_id"]
            == str(first_artifact.adapter_run_output_id)
        )
        assert first_item["artifact_kind"] == first_artifact.artifact_kind
        assert first_item["name"] == first_artifact.name
        assert first_item["format"] == first_artifact.format
        assert first_item["media_type"] == first_artifact.media_type
        assert first_item["size_bytes"] == first_artifact.size_bytes
        assert first_item["checksum_sha256"] == first_artifact.checksum_sha256
        assert first_item["generator_name"] == first_artifact.generator_name
        assert first_item["generator_version"] == first_artifact.generator_version
        assert first_item["predecessor_artifact_id"] is None
        assert _parse_timestamp(first_item["created_at"]) == first_artifact.created_at.astimezone(
            UTC
        )
        assert "storage_key" not in first_item
        assert "storage_uri" not in first_item
        assert "generator_config_json" not in first_item
        assert "lineage_json" not in first_item

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        deleted_artifact_id = generated_artifacts[0].id
        async with session_maker() as session:
            persisted_artifact = await session.get(
                GeneratedArtifact,
                deleted_artifact_id,
            )
            assert persisted_artifact is not None
            persisted_artifact.deleted_at = datetime.now(UTC)
            await session.commit()

        filtered_file_response = await async_client.get(
            f"/v1/files/{uploaded['id']}/generated-artifacts"
        )
        filtered_revision_response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/generated-artifacts"
        )

        assert filtered_file_response.status_code == 200
        assert filtered_revision_response.status_code == 200
        assert str(deleted_artifact_id) not in _artifact_ids(filtered_file_response.json()["items"])
        assert str(deleted_artifact_id) not in _artifact_ids(
            filtered_revision_response.json()["items"]
        )

    async def test_generated_artifact_lists_support_limit_and_cursor(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Generated artifact listing should provide bounded cursor pagination."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(job.id)

        _adapter_outputs, drawing_revisions, _validation_reports, generated_artifacts = (
            await _load_project_outputs(project["id"])
        )
        drawing_revision = drawing_revisions[0]

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        next_created_at = generated_artifacts[0].created_at + timedelta(seconds=1)
        next_artifact = _clone_model(
            generated_artifacts[0],
            id=uuid.uuid4(),
            name=f"{generated_artifacts[0].name}-page-2",
            checksum_sha256=uuid.uuid4().hex * 2,
            storage_key=f"generated/{uuid.uuid4()}",
            storage_uri=f"file:///tmp/{uuid.uuid4()}",
            predecessor_artifact_id=generated_artifacts[0].id,
            created_at=next_created_at,
        )

        async with session_maker() as session:
            session.add(next_artifact)
            await session.commit()

        generated_artifacts = [generated_artifacts[0], next_artifact]

        file_first_page_response = await async_client.get(
            f"/v1/files/{uploaded['id']}/generated-artifacts",
            params={"limit": 1},
        )
        assert file_first_page_response.status_code == 200
        file_first_page = file_first_page_response.json()
        assert _artifact_ids(file_first_page["items"]) == [str(generated_artifacts[0].id)]
        assert file_first_page["next_cursor"] is not None

        file_second_page_response = await async_client.get(
            f"/v1/files/{uploaded['id']}/generated-artifacts",
            params={"limit": 1, "cursor": file_first_page["next_cursor"]},
        )
        assert file_second_page_response.status_code == 200
        file_second_page = file_second_page_response.json()
        assert _artifact_ids(file_second_page["items"]) == [str(generated_artifacts[1].id)]

        revision_artifacts = [
            artifact
            for artifact in generated_artifacts
            if artifact.drawing_revision_id == drawing_revision.id
        ]
        assert len(revision_artifacts) >= 2

        revision_first_page_response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/generated-artifacts",
            params={"limit": 1},
        )
        assert revision_first_page_response.status_code == 200
        revision_first_page = revision_first_page_response.json()
        assert _artifact_ids(revision_first_page["items"]) == [str(revision_artifacts[0].id)]
        assert revision_first_page["next_cursor"] is not None

        revision_second_page_response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/generated-artifacts",
            params={"limit": 1, "cursor": revision_first_page["next_cursor"]},
        )
        assert revision_second_page_response.status_code == 200
        revision_second_page = revision_second_page_response.json()
        assert _artifact_ids(revision_second_page["items"]) == [str(revision_artifacts[1].id)]

    async def test_discoverability_endpoints_hide_soft_deleted_project_data(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Revision and adapter output reads should hide soft-deleted project/file data."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(job.id)

        adapter_outputs, drawing_revisions, _validation_reports, _generated_artifacts = (
            await _load_project_outputs(project["id"])
        )
        drawing_revision = drawing_revisions[0]
        adapter_output = adapter_outputs[0]

        delete_response = await async_client.delete(f"/v1/projects/{project['id']}")
        assert delete_response.status_code == 204

        revisions_response = await async_client.get(f"/v1/files/{uploaded['id']}/revisions")
        adapter_output_response = await async_client.get(
            f"/v1/adapter-outputs/{adapter_output.id}"
        )

        assert revisions_response.status_code == 404
        assert revisions_response.json() == {
            "error": {
                "code": ErrorCode.NOT_FOUND.value,
                "message": f"File with identifier '{uploaded['id']}' not found",
                "details": None,
            }
        }
        assert adapter_output_response.status_code == 404
        assert adapter_output_response.json() == {
            "error": {
                "code": ErrorCode.NOT_FOUND.value,
                "message": f"Adapter run output with identifier '{adapter_output.id}' not found",
                "details": None,
            }
        }

        revision_artifacts_response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/generated-artifacts"
        )
        assert revision_artifacts_response.status_code == 404
        assert revision_artifacts_response.json() == {
            "error": {
                "code": ErrorCode.NOT_FOUND.value,
                "message": f"Drawing revision with identifier '{drawing_revision.id}' not found",
                "details": None,
            }
        }
