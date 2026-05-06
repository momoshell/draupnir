"""Integration tests for persisted ingest output rows."""

import asyncio
import uuid

import httpx
import pytest
from sqlalchemy import select

import app.db.session as session_module
import app.jobs.worker as worker_module
from app.core.errors import ErrorCode
from app.jobs.worker import process_ingest_job
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.generated_artifact import GeneratedArtifact
from app.models.job import Job
from app.models.job_event import JobEvent
from app.models.validation_report import ValidationReport
from tests.conftest import requires_database
from tests.test_jobs import (
    _FAKE_RUNNER_ADAPTER_KEY,
    _FAKE_RUNNER_ADAPTER_VERSION,
    _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
    _FAKE_RUNNER_CONFIDENCE_SCORE,
    _FAKE_RUNNER_QUANTITY_GATE,
    _FAKE_RUNNER_REVIEW_STATE,
    _FAKE_RUNNER_VALIDATION_REPORT_SCHEMA_VERSION,
    _FAKE_RUNNER_VALIDATION_STATUS,
    _FAKE_RUNNER_VALIDATOR_NAME,
    _FAKE_RUNNER_VALIDATOR_VERSION,
    _build_fake_ingest_payload,
    _create_project,
    _get_job,
    _get_job_for_file,
    _update_job,
    _upload_file,
)

pytest_plugins = ("tests.test_jobs",)


def _as_uuid(value: str | uuid.UUID) -> uuid.UUID:
    """Normalize string UUID inputs for direct model comparisons."""
    if isinstance(value, uuid.UUID):
        return value

    return uuid.UUID(value)


async def _load_project_outputs(
    project_id: str | uuid.UUID,
) -> tuple[
    list[AdapterRunOutput],
    list[DrawingRevision],
    list[ValidationReport],
    list[GeneratedArtifact],
]:
    """Load persisted ingest outputs for a single isolated project."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    normalized_project_id = _as_uuid(project_id)

    async with session_maker() as session:
        adapter_outputs = list(
            (
                await session.execute(
                    select(AdapterRunOutput)
                    .where(AdapterRunOutput.project_id == normalized_project_id)
                    .order_by(AdapterRunOutput.created_at, AdapterRunOutput.id)
                )
            ).scalars()
        )
        drawing_revisions = list(
            (
                await session.execute(
                    select(DrawingRevision)
                    .where(DrawingRevision.project_id == normalized_project_id)
                    .order_by(DrawingRevision.revision_sequence, DrawingRevision.id)
                )
            ).scalars()
        )
        validation_reports = list(
            (
                await session.execute(
                    select(ValidationReport)
                    .where(ValidationReport.project_id == normalized_project_id)
                    .order_by(ValidationReport.created_at, ValidationReport.id)
                )
            ).scalars()
        )
        generated_artifacts = list(
            (
                await session.execute(
                    select(GeneratedArtifact)
                    .where(GeneratedArtifact.project_id == normalized_project_id)
                    .order_by(GeneratedArtifact.created_at, GeneratedArtifact.id)
                )
            ).scalars()
        )

    return adapter_outputs, drawing_revisions, validation_reports, generated_artifacts


async def _load_job_events(job_id: uuid.UUID) -> list[JobEvent]:
    """Load persisted job events in chronological order."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        return list(
            (
                await session.execute(
                    select(JobEvent)
                    .where(JobEvent.job_id == job_id)
                    .order_by(JobEvent.created_at, JobEvent.id)
                )
            ).scalars()
        )


async def _set_job_extraction_profile_id(
    job_id: uuid.UUID,
    extraction_profile_id: uuid.UUID | None,
) -> Job:
    """Update a job extraction profile for failure-path setup."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        job = await session.get(Job, job_id)
        assert job is not None
        job.extraction_profile_id = extraction_profile_id
        await session.commit()

    return await _get_job(job_id)


@requires_database
class TestIngestOutputPersistence:
    """Tests for durable ingest output persistence and finalization guards."""

    async def test_process_ingest_job_persists_single_runner_output_set(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Successful ingest should atomically persist one runner output set."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(job.id)

        adapter_outputs, drawing_revisions, validation_reports, generated_artifacts = (
            await _load_project_outputs(project["id"])
        )

        assert len(adapter_outputs) == 1
        assert len(drawing_revisions) == 1
        assert len(validation_reports) == 1
        assert generated_artifacts == []

        adapter_output = adapter_outputs[0]
        drawing_revision = drawing_revisions[0]
        validation_report = validation_reports[0]

        assert adapter_output.project_id == job.project_id
        assert adapter_output.source_file_id == job.file_id
        assert adapter_output.source_job_id == job.id
        assert adapter_output.extraction_profile_id == job.extraction_profile_id
        assert adapter_output.adapter_key == _FAKE_RUNNER_ADAPTER_KEY
        assert adapter_output.adapter_version == _FAKE_RUNNER_ADAPTER_VERSION
        assert adapter_output.input_family == "pdf_vector"
        assert (
            adapter_output.canonical_entity_schema_version
            == _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION
        )
        assert adapter_output.confidence_score == _FAKE_RUNNER_CONFIDENCE_SCORE
        assert adapter_output.canonical_json == {
            "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            "schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            "layouts": [{"name": "Model"}],
            "layers": [{"name": "A-WALL"}],
            "blocks": [],
            "entities": [{"kind": "line", "layer": "A-WALL"}],
            "entity_counts": {
                "layouts": 1,
                "layers": 1,
                "blocks": 0,
                "entities": 1,
            },
        }
        assert adapter_output.provenance_json == {
            "schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            "bridge": "tests.fake_ingestion_runner",
            "adapter": {
                "key": _FAKE_RUNNER_ADAPTER_KEY,
                "version": _FAKE_RUNNER_ADAPTER_VERSION,
            },
            "source": {
                "file_id": str(job.file_id),
                "job_id": str(job.id),
                "extraction_profile_id": str(job.extraction_profile_id),
                "input_family": "pdf_vector",
                "revision_kind": "ingest",
                "original_name": "plan.pdf",
            },
            "generated_at": adapter_output.provenance_json["generated_at"],
        }

        assert drawing_revision.project_id == job.project_id
        assert drawing_revision.source_file_id == job.file_id
        assert drawing_revision.source_job_id == job.id
        assert drawing_revision.extraction_profile_id == job.extraction_profile_id
        assert drawing_revision.adapter_run_output_id == adapter_output.id
        assert drawing_revision.predecessor_revision_id is None
        assert drawing_revision.revision_sequence == 1
        assert drawing_revision.revision_kind == "ingest"
        assert drawing_revision.review_state == _FAKE_RUNNER_REVIEW_STATE
        assert (
            drawing_revision.canonical_entity_schema_version
            == _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION
        )
        assert drawing_revision.confidence_score == _FAKE_RUNNER_CONFIDENCE_SCORE

        assert validation_report.project_id == job.project_id
        assert validation_report.drawing_revision_id == drawing_revision.id
        assert validation_report.source_job_id == job.id
        assert (
            validation_report.validation_report_schema_version
            == _FAKE_RUNNER_VALIDATION_REPORT_SCHEMA_VERSION
        )
        assert (
            validation_report.canonical_entity_schema_version
            == _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION
        )
        assert validation_report.validation_status == _FAKE_RUNNER_VALIDATION_STATUS
        assert validation_report.review_state == _FAKE_RUNNER_REVIEW_STATE
        assert validation_report.quantity_gate == _FAKE_RUNNER_QUANTITY_GATE
        assert validation_report.effective_confidence == _FAKE_RUNNER_CONFIDENCE_SCORE
        assert validation_report.report_json == {
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
                "entity_counts": {
                    "layouts": 1,
                    "layers": 1,
                    "blocks": 0,
                    "entities": 1,
                },
            },
            "checks": [],
            "findings": [],
            "adapter_warnings": [],
            "provenance": adapter_output.provenance_json,
        }

    async def test_reprocess_creates_second_revision_with_predecessor(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Reprocess should append a second output set linked to the prior revision."""
        _ = self
        _ = cleanup_projects

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        first_job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(first_job.id)

        reprocess_response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(first_job.extraction_profile_id)},
        )

        assert reprocess_response.status_code == 202

        second_job_id = _as_uuid(reprocess_response.json()["id"])
        second_job = await _get_job(second_job_id)
        assert enqueued_job_ids == [str(first_job.id), str(second_job.id)]

        await process_ingest_job(second_job.id)

        adapter_outputs, drawing_revisions, validation_reports, generated_artifacts = (
            await _load_project_outputs(project["id"])
        )

        assert len(adapter_outputs) == 2
        assert len(drawing_revisions) == 2
        assert len(validation_reports) == 2
        assert generated_artifacts == []

        first_revision, second_revision = drawing_revisions
        first_adapter_output = next(
            output for output in adapter_outputs if output.source_job_id == first_job.id
        )
        second_adapter_output = next(
            output for output in adapter_outputs if output.source_job_id == second_job.id
        )
        first_validation_report = next(
            report for report in validation_reports if report.source_job_id == first_job.id
        )
        second_validation_report = next(
            report for report in validation_reports if report.source_job_id == second_job.id
        )

        assert first_revision.revision_sequence == 1
        assert first_revision.revision_kind == "ingest"
        assert first_revision.predecessor_revision_id is None
        assert first_revision.adapter_run_output_id == first_adapter_output.id
        assert first_validation_report.drawing_revision_id == first_revision.id

        assert second_adapter_output.project_id == first_job.project_id
        assert second_adapter_output.source_file_id == first_job.file_id
        assert second_adapter_output.source_job_id == second_job.id
        assert second_adapter_output.extraction_profile_id == second_job.extraction_profile_id
        assert second_adapter_output.adapter_key == _FAKE_RUNNER_ADAPTER_KEY

        assert second_revision.project_id == first_job.project_id
        assert second_revision.source_file_id == first_job.file_id
        assert second_revision.source_job_id == second_job.id
        assert second_revision.extraction_profile_id == second_job.extraction_profile_id
        assert second_revision.adapter_run_output_id == second_adapter_output.id
        assert second_revision.revision_sequence == 2
        assert second_revision.revision_kind == "reprocess"
        assert second_revision.predecessor_revision_id == first_revision.id

        assert second_validation_report.project_id == first_job.project_id
        assert second_validation_report.source_job_id == second_job.id
        assert second_validation_report.drawing_revision_id == second_revision.id
        assert second_validation_report.validation_status == _FAKE_RUNNER_VALIDATION_STATUS
        assert second_validation_report.review_state == _FAKE_RUNNER_REVIEW_STATE
        assert second_validation_report.quantity_gate == _FAKE_RUNNER_QUANTITY_GATE
        assert second_validation_report.effective_confidence == _FAKE_RUNNER_CONFIDENCE_SCORE

    async def test_validation_report_allows_trd_status_and_gate_values(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Persisted validation reports should accept TRD v0.1 status and gate values."""
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
        validation_report = validation_reports[0]

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            persisted_validation_report = await session.get(ValidationReport, validation_report.id)
            assert persisted_validation_report is not None
            persisted_validation_report.validation_status = "valid_with_warnings"
            persisted_validation_report.quantity_gate = "allowed_provisional"
            await session.commit()

        _adapter_outputs, _drawing_revisions, updated_validation_reports, _generated_artifacts = (
            await _load_project_outputs(project["id"])
        )
        updated_validation_report = updated_validation_reports[0]

        assert updated_validation_report.validation_status == "valid_with_warnings"
        assert updated_validation_report.quantity_gate == "allowed_provisional"

    async def test_concurrent_reprocess_creates_linear_three_revision_chain(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Concurrent reprocess runs should still persist a linear revision chain."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        first_job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(first_job.id)

        second_reprocess_response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(first_job.extraction_profile_id)},
        )
        assert second_reprocess_response.status_code == 202

        third_reprocess_response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(first_job.extraction_profile_id)},
        )
        assert third_reprocess_response.status_code == 202

        second_job = await _get_job(_as_uuid(second_reprocess_response.json()["id"]))
        third_job = await _get_job(_as_uuid(third_reprocess_response.json()["id"]))

        await asyncio.gather(
            process_ingest_job(second_job.id),
            process_ingest_job(third_job.id),
        )

        adapter_outputs, drawing_revisions, validation_reports, generated_artifacts = (
            await _load_project_outputs(project["id"])
        )

        assert len(adapter_outputs) == 3
        assert len(drawing_revisions) == 3
        assert len(validation_reports) == 3
        assert generated_artifacts == []

        first_revision, second_revision, _third_revision = drawing_revisions
        assert [revision.revision_sequence for revision in drawing_revisions] == [1, 2, 3]
        assert [revision.predecessor_revision_id for revision in drawing_revisions] == [
            None,
            first_revision.id,
            second_revision.id,
        ]
        assert [revision.revision_kind for revision in drawing_revisions] == [
            "ingest",
            "reprocess",
            "reprocess",
        ]

        expected_job_ids = {first_job.id, second_job.id, third_job.id}
        assert {output.source_job_id for output in adapter_outputs} == expected_job_ids
        assert {report.source_job_id for report in validation_reports} == expected_job_ids

    async def test_reprocess_before_initial_finalization_fails_without_outputs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """A reprocess cannot finalize before the initial revision exists."""
        _ = self
        _ = cleanup_projects

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        initial_job = await _get_job_for_file(str(uploaded["id"]))

        first_reprocess_response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(initial_job.extraction_profile_id)},
        )
        assert first_reprocess_response.status_code == 202

        second_reprocess_response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(initial_job.extraction_profile_id)},
        )
        assert second_reprocess_response.status_code == 202

        blocked_reprocess_job = await _get_job(_as_uuid(first_reprocess_response.json()["id"]))
        _unused_reprocess_job = await _get_job(_as_uuid(second_reprocess_response.json()["id"]))
        assert enqueued_job_ids == [
            str(initial_job.id),
            str(blocked_reprocess_job.id),
            str(_unused_reprocess_job.id),
        ]

        with pytest.raises(
            ValueError,
            match="Reprocess ingest job cannot finalize before a predecessor revision exists",
        ):
            await process_ingest_job(blocked_reprocess_job.id)

        failed_reprocess_job = await _get_job(blocked_reprocess_job.id)
        assert failed_reprocess_job.status == "failed"
        assert failed_reprocess_job.error_code == ErrorCode.INTERNAL_ERROR.value
        assert (
            failed_reprocess_job.error_message
            == worker_module._FINALIZE_INGEST_JOB_ERROR_MESSAGE
        )

        adapter_outputs, drawing_revisions, validation_reports, generated_artifacts = (
            await _load_project_outputs(project["id"])
        )
        assert adapter_outputs == []
        assert drawing_revisions == []
        assert validation_reports == []
        assert generated_artifacts == []

        await process_ingest_job(initial_job.id)

        adapter_outputs, drawing_revisions, validation_reports, generated_artifacts = (
            await _load_project_outputs(project["id"])
        )
        assert len(adapter_outputs) == 1
        assert len(drawing_revisions) == 1
        assert len(validation_reports) == 1
        assert generated_artifacts == []
        assert adapter_outputs[0].source_job_id == initial_job.id
        assert drawing_revisions[0].source_job_id == initial_job.id
        assert drawing_revisions[0].revision_sequence == 1
        assert drawing_revisions[0].predecessor_revision_id is None
        assert drawing_revisions[0].revision_kind == "ingest"

    async def test_process_ingest_job_cancelled_before_finalization_creates_no_outputs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Cancellation before finalization should publish no output rows."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _cancel_during_work(request) -> object:
            await _update_job(job.id, cancel_requested=True)
            return _build_fake_ingest_payload(request)

        monkeypatch.setattr(worker_module, "run_ingestion", _cancel_during_work)

        await process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "cancelled"
        assert updated_job.error_code == ErrorCode.JOB_CANCELLED.value

        adapter_outputs, drawing_revisions, validation_reports, generated_artifacts = (
            await _load_project_outputs(project["id"])
        )

        assert adapter_outputs == []
        assert drawing_revisions == []
        assert validation_reports == []
        assert generated_artifacts == []

    async def test_process_ingest_job_duplicate_delivery_after_success_keeps_single_output_set(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Duplicate success delivery should not append extra persisted outputs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(job.id)
        first_outputs = await _load_project_outputs(project["id"])

        await process_ingest_job(job.id)
        second_outputs = await _load_project_outputs(project["id"])

        assert len(second_outputs[0]) == 1
        assert len(second_outputs[1]) == 1
        assert len(second_outputs[2]) == 1
        assert second_outputs[3] == []
        assert [row.id for row in second_outputs[0]] == [row.id for row in first_outputs[0]]
        assert [row.id for row in second_outputs[1]] == [row.id for row in first_outputs[1]]
        assert [row.id for row in second_outputs[2]] == [row.id for row in first_outputs[2]]

    async def test_process_ingest_job_missing_profile_fails_without_outputs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Missing extraction profile should fail finalization without partial outputs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await _set_job_extraction_profile_id(job.id, None)

        with pytest.raises(ValueError, match="Ingest job missing extraction profile"):
            await process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.INTERNAL_ERROR.value
        assert updated_job.error_message == worker_module._FINALIZE_INGEST_JOB_ERROR_MESSAGE

        events = await _load_job_events(job.id)
        assert events[-1].level == "error"
        assert events[-1].message == "Job failed"
        assert events[-1].data_json == {
            "status": "failed",
            "error_code": ErrorCode.INTERNAL_ERROR.value,
            "error_message": worker_module._FINALIZE_INGEST_JOB_ERROR_MESSAGE,
        }

        adapter_outputs, drawing_revisions, validation_reports, generated_artifacts = (
            await _load_project_outputs(project["id"])
        )

        assert adapter_outputs == []
        assert drawing_revisions == []
        assert validation_reports == []
        assert generated_artifacts == []
