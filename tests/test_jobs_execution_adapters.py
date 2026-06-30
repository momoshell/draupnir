"""Unit tests for worker execution adapter glue."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.ingestion.contracts import InputFamily
from app.jobs import execution_adapters
from app.models.estimate_version import EstimateVersion
from app.models.export_job_input import ExportJobInput
from app.models.job import Job
from app.models.quantity_takeoff import QuantityTakeoff

PROJECT_ID = uuid.uuid4()
FILE_ID = uuid.uuid4()
JOB_ID = uuid.uuid4()
REVISION_ID = uuid.uuid4()
TAKEOFF_ID = uuid.uuid4()
ESTIMATE_ID = uuid.uuid4()
TOKEN = uuid.uuid4()


class _InactiveSourceError(Exception):
    pass


class _StaleAttemptError(Exception):
    pass


class _FakeSession:
    def __init__(self, rows: dict[tuple[type[Any], uuid.UUID], Any] | None = None) -> None:
        self.rows = rows or {}
        self.get_calls: list[tuple[type[Any], uuid.UUID]] = []

    async def get(self, model_type: type[Any], row_id: uuid.UUID) -> Any:
        self.get_calls.append((model_type, row_id))
        return self.rows.get((model_type, row_id))


class _FakeSessionContext:
    def __init__(self, session: _FakeSession) -> None:
        self.session = session

    async def __aenter__(self) -> _FakeSession:
        return self.session

    async def __aexit__(self, *_exc_info: object) -> None:
        return None


class _FakeSessionMaker:
    def __init__(self, session: _FakeSession) -> None:
        self.session = session

    def __call__(self) -> _FakeSessionContext:
        return _FakeSessionContext(self.session)


def _job(**overrides: Any) -> Any:
    row = {
        "id": JOB_ID,
        "project_id": PROJECT_ID,
        "file_id": FILE_ID,
        "extraction_profile_id": None,
    }
    row.update(overrides)
    return SimpleNamespace(**row)


def _source_file(**overrides: Any) -> Any:
    row = {
        "id": FILE_ID,
        "checksum_sha256": "a" * 64,
        "detected_format": "pdf",
        "media_type": "application/pdf",
        "original_filename": "drawing.pdf",
        "initial_job_id": JOB_ID,
        "deleted_at": None,
    }
    row.update(overrides)
    return SimpleNamespace(**row)


async def test_requested_input_family_from_pdf_input_mode_maps_explicit_modes() -> None:
    assert (
        execution_adapters.requested_input_family_from_pdf_input_mode("vector")
        == InputFamily.PDF_VECTOR
    )
    assert (
        execution_adapters.requested_input_family_from_pdf_input_mode("raster")
        == InputFamily.PDF_RASTER
    )
    assert execution_adapters.requested_input_family_from_pdf_input_mode("auto") is None
    assert execution_adapters.requested_input_family_from_pdf_input_mode(None) is None


async def test_build_ingestion_run_request_builds_runner_request_from_persisted_rows() -> None:
    extraction_profile_id = uuid.uuid4()
    session = _FakeSession()
    job = _job(extraction_profile_id=extraction_profile_id)
    source_file = _source_file()
    asserted_jobs: list[Any] = []

    async def get_job_lock_bootstrap(_session: Any, job_id: uuid.UUID) -> Any:
        assert job_id == JOB_ID
        return SimpleNamespace(project_id=PROJECT_ID, file_id=FILE_ID)

    async def get_project(_session: Any, project_id: uuid.UUID, *, for_update: bool) -> Any:
        assert project_id == PROJECT_ID
        assert for_update is True
        return SimpleNamespace(id=project_id, deleted_at=None)

    async def get_job_for_update_with_metadata(
        _session: Any,
        job_id: uuid.UUID,
        *,
        expected_project_id: uuid.UUID,
        expected_file_id: uuid.UUID,
    ) -> Any:
        assert job_id == JOB_ID
        assert expected_project_id == PROJECT_ID
        assert expected_file_id == FILE_ID
        return job

    def job_attempt_is_current(loaded_job: Any, *, attempt_token: uuid.UUID) -> bool:
        assert loaded_job is job
        assert attempt_token == TOKEN
        return True

    def assert_job_base_revision_invariants(loaded_job: Job) -> None:
        asserted_jobs.append(loaded_job)

    async def cancel_job_for_inactive_source(*_args: Any, **_kwargs: Any) -> bool:
        raise AssertionError("active source should not be cancelled")

    async def get_source_file(
        _session: Any,
        *,
        project_id: uuid.UUID,
        file_id: uuid.UUID,
        for_update: bool,
    ) -> Any:
        assert project_id == PROJECT_ID
        assert file_id == FILE_ID
        assert for_update is True
        return source_file

    async def get_extraction_profile(_session: Any, *, extraction_profile_id: uuid.UUID) -> Any:
        assert extraction_profile_id == job.extraction_profile_id
        return SimpleNamespace(pdf_input_mode="raster")

    request = await execution_adapters.build_ingestion_run_request(
        JOB_ID,
        attempt_token=TOKEN,
        session_maker_factory=lambda: _FakeSessionMaker(session),
        get_job_lock_bootstrap=get_job_lock_bootstrap,
        get_project=get_project,
        get_job_for_update_with_metadata=get_job_for_update_with_metadata,
        job_attempt_is_current=job_attempt_is_current,
        assert_job_base_revision_invariants=assert_job_base_revision_invariants,
        cancel_job_for_inactive_source=cancel_job_for_inactive_source,
        get_source_file=get_source_file,
        get_extraction_profile=get_extraction_profile,
        inactive_source_error_type=_InactiveSourceError,
        stale_job_attempt_error_type=_StaleAttemptError,
    )

    assert asserted_jobs == [job]
    assert request.job_id == JOB_ID
    assert request.file_id == FILE_ID
    assert request.checksum_sha256 == source_file.checksum_sha256
    assert request.detected_format == "pdf"
    assert request.media_type == "application/pdf"
    assert request.original_name == "drawing.pdf"
    assert request.extraction_profile_id == extraction_profile_id
    assert request.initial_job_id == JOB_ID
    assert request.requested_input_family == InputFamily.PDF_RASTER


async def test_build_ingestion_run_request_cancels_inactive_project() -> None:
    session = _FakeSession()
    job = _job()
    cancelled: list[tuple[Any, str, uuid.UUID]] = []

    async def get_job_lock_bootstrap(_session: Any, _job_id: uuid.UUID) -> Any:
        return SimpleNamespace(project_id=PROJECT_ID, file_id=FILE_ID)

    async def get_project(_session: Any, _project_id: uuid.UUID, *, for_update: bool) -> Any:
        return SimpleNamespace(id=PROJECT_ID, deleted_at=datetime.now(UTC))

    async def get_job_for_update_with_metadata(_session: Any, *_args: Any, **_kwargs: Any) -> Any:
        return job

    async def cancel_job_for_inactive_source(
        _session: Any,
        loaded_job: Any,
        *,
        reason: str,
        attempt_token: uuid.UUID,
    ) -> bool:
        cancelled.append((loaded_job, reason, attempt_token))
        return True

    with pytest.raises(_InactiveSourceError):
        await execution_adapters.build_ingestion_run_request(
            JOB_ID,
            attempt_token=TOKEN,
            session_maker_factory=lambda: _FakeSessionMaker(session),
            get_job_lock_bootstrap=get_job_lock_bootstrap,
            get_project=get_project,
            get_job_for_update_with_metadata=get_job_for_update_with_metadata,
            job_attempt_is_current=lambda *_args, **_kwargs: True,
            assert_job_base_revision_invariants=lambda _job: None,
            cancel_job_for_inactive_source=cancel_job_for_inactive_source,
            get_source_file=lambda *_args, **_kwargs: None,
            get_extraction_profile=lambda *_args, **_kwargs: None,
            inactive_source_error_type=_InactiveSourceError,
            stale_job_attempt_error_type=_StaleAttemptError,
        )

    assert cancelled == [(job, "source_deleted", TOKEN)]


async def test_session_export_row_loader_delegates_to_session_and_revision_lookup() -> None:
    job = object()
    export_input = object()
    takeoff = object()
    estimate = object()
    revision = object()
    session = _FakeSession(
        {
            (Job, JOB_ID): job,
            (ExportJobInput, JOB_ID): export_input,
            (QuantityTakeoff, TAKEOFF_ID): takeoff,
            (EstimateVersion, ESTIMATE_ID): estimate,
        }
    )
    revision_calls: list[tuple[Any, uuid.UUID]] = []

    async def get_drawing_revision(session_arg: Any, *, revision_id: uuid.UUID) -> Any:
        revision_calls.append((session_arg, revision_id))
        return revision

    loader = execution_adapters.SessionExportRowLoader(
        cast(AsyncSession, session),
        get_drawing_revision=get_drawing_revision,
    )

    assert await loader.get_job(JOB_ID) is job
    assert await loader.get_export_job_input(JOB_ID) is export_input
    assert await loader.get_drawing_revision(REVISION_ID) is revision
    assert await loader.get_quantity_takeoff(TAKEOFF_ID) is takeoff
    assert await loader.get_estimate_version(ESTIMATE_ID) is estimate
    assert revision_calls == [(session, REVISION_ID)]
    assert session.get_calls == [
        (Job, JOB_ID),
        (ExportJobInput, JOB_ID),
        (QuantityTakeoff, TAKEOFF_ID),
        (EstimateVersion, ESTIMATE_ID),
    ]


async def test_session_quantity_row_loader_delegates_to_session_and_revision_queries() -> None:
    job = object()
    revision = object()
    report = object()
    manifest = object()
    entities = [object()]
    session = _FakeSession({(Job, JOB_ID): job})
    calls: list[tuple[str, Any]] = []

    async def get_drawing_revision(session_arg: Any, *, revision_id: uuid.UUID) -> Any:
        calls.append(("revision", (session_arg, revision_id)))
        return revision

    async def get_validation_report_for_revision(
        session_arg: Any,
        *,
        project_id: uuid.UUID,
        drawing_revision_id: uuid.UUID,
    ) -> Any:
        calls.append(("report", (session_arg, project_id, drawing_revision_id)))
        return report

    async def get_revision_entity_manifest_for_revision(
        session_arg: Any,
        *,
        project_id: uuid.UUID,
        source_file_id: uuid.UUID,
        drawing_revision_id: uuid.UUID,
    ) -> Any:
        calls.append(("manifest", (session_arg, project_id, source_file_id, drawing_revision_id)))
        return manifest

    async def get_revision_entities_for_revision(
        session_arg: Any,
        *,
        project_id: uuid.UUID,
        source_file_id: uuid.UUID,
        drawing_revision_id: uuid.UUID,
    ) -> Any:
        calls.append(("entities", (session_arg, project_id, source_file_id, drawing_revision_id)))
        return entities

    loader = execution_adapters.SessionQuantityRowLoader(
        cast(AsyncSession, session),
        get_drawing_revision=get_drawing_revision,
        get_validation_report_for_revision=get_validation_report_for_revision,
        get_revision_entity_manifest_for_revision=get_revision_entity_manifest_for_revision,
        get_revision_entities_for_revision=get_revision_entities_for_revision,
    )

    assert await loader.get_job(JOB_ID) is job
    assert await loader.get_drawing_revision(REVISION_ID) is revision
    assert (
        await loader.get_validation_report(
            project_id=PROJECT_ID,
            drawing_revision_id=REVISION_ID,
        )
        is report
    )
    assert (
        await loader.get_entity_manifest(
            project_id=PROJECT_ID,
            source_file_id=FILE_ID,
            drawing_revision_id=REVISION_ID,
        )
        is manifest
    )
    assert (
        await loader.get_revision_entities(
            project_id=PROJECT_ID,
            source_file_id=FILE_ID,
            drawing_revision_id=REVISION_ID,
        )
        is entities
    )
    assert session.get_calls == [(Job, JOB_ID)]
    assert calls == [
        ("revision", (session, REVISION_ID)),
        ("report", (session, PROJECT_ID, REVISION_ID)),
        ("manifest", (session, PROJECT_ID, FILE_ID, REVISION_ID)),
        ("entities", (session, PROJECT_ID, FILE_ID, REVISION_ID)),
    ]


async def test_export_and_quantity_builders_open_session_and_delegate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _FakeSession()
    export_loader = object()
    quantity_loader = object()
    export_execution = object()
    quantity_execution = object()
    export_calls: list[tuple[Any, ...]] = []
    quantity_calls: list[tuple[Any, ...]] = []

    def row_loader_factory(session_arg: Any) -> Any:
        assert session_arg is session
        return export_loader

    def quantity_row_loader_factory(session_arg: Any) -> Any:
        assert session_arg is session
        return quantity_loader

    def resolve_export_spec(export_kind: str) -> Any:
        _ = export_kind
        raise AssertionError("validator should receive this callback without invoking it")

    def build_artifact_name(
        *,
        export_kind: str,
        export_format: str,
        drawing_revision_id: uuid.UUID,
        changeset_id: uuid.UUID | None = None,
        quantity_takeoff_id: uuid.UUID | None = None,
        estimate_version_id: uuid.UUID | None = None,
    ) -> str:
        _ = (
            export_kind,
            export_format,
            drawing_revision_id,
            changeset_id,
            quantity_takeoff_id,
            estimate_version_id,
        )
        raise AssertionError("validator should receive this callback without invoking it")

    async def validate_export_execution_input(
        job_id: uuid.UUID,
        *,
        attempt_token: uuid.UUID,
        loader: Any,
        resolve_export_spec: Any,
        build_artifact_name: Any,
    ) -> Any:
        export_calls.append(
            (job_id, attempt_token, loader, resolve_export_spec, build_artifact_name)
        )
        return export_execution

    async def validate_quantity_takeoff_execution_input(
        job_id: uuid.UUID,
        *,
        attempt_token: uuid.UUID,
        loader: Any,
    ) -> Any:
        quantity_calls.append((job_id, attempt_token, loader))
        return quantity_execution

    monkeypatch.setattr(
        execution_adapters,
        "validate_export_execution_input",
        validate_export_execution_input,
    )
    monkeypatch.setattr(
        execution_adapters,
        "validate_quantity_takeoff_execution_input",
        validate_quantity_takeoff_execution_input,
    )

    built_export = await execution_adapters.build_export_execution_input(
        JOB_ID,
        attempt_token=TOKEN,
        session_maker_factory=lambda: _FakeSessionMaker(session),
        row_loader_factory=row_loader_factory,
        resolve_export_spec=resolve_export_spec,
        build_artifact_name=build_artifact_name,
    )
    built_quantity = await execution_adapters.build_quantity_takeoff_execution_input(
        JOB_ID,
        attempt_token=TOKEN,
        session_maker_factory=lambda: _FakeSessionMaker(session),
        row_loader_factory=quantity_row_loader_factory,
    )

    assert built_export is export_execution
    assert built_quantity is quantity_execution
    assert export_calls == [
        (JOB_ID, TOKEN, export_loader, resolve_export_spec, build_artifact_name)
    ]
    assert quantity_calls == [(JOB_ID, TOKEN, quantity_loader)]
