"""Integration tests for restrictive lineage foreign keys."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError, IntegrityError

import app.db.session as session_module
import app.jobs.worker as worker_module
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunRequest
from app.jobs.worker import process_ingest_job
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.extraction_profile import ExtractionProfile
from app.models.file import File as FileModel
from app.models.generated_artifact import GeneratedArtifact
from app.models.job import Job
from app.models.project import Project
from app.models.validation_report import ValidationReport
from tests.conftest import requires_database
from tests.test_ingest_output_persistence import _as_uuid, _load_project_outputs
from tests.test_jobs import (
    _build_fake_ingest_payload,
    _create_project,
    _get_job_for_file,
    _upload_file,
)


@dataclass
class _LineageSnapshot:
    project: Project
    source_file: FileModel
    extraction_profile: ExtractionProfile
    job: Job
    adapter_output: AdapterRunOutput
    drawing_revision: DrawingRevision
    validation_report: ValidationReport
    generated_artifact: GeneratedArtifact


@dataclass(frozen=True)
class _ForeignKeyDeleteRule:
    table_name: str
    constrained_columns: tuple[str, ...]
    referred_table: str
    referred_columns: tuple[str, ...]
    ondelete: str


_EXPECTED_LINEAGE_FOREIGN_KEYS: tuple[_ForeignKeyDeleteRule, ...] = (
    _ForeignKeyDeleteRule("files", ("project_id",), "projects", ("id",), "RESTRICT"),
    _ForeignKeyDeleteRule(
        "extraction_profiles",
        ("project_id",),
        "projects",
        ("id",),
        "RESTRICT",
    ),
    _ForeignKeyDeleteRule(
        "jobs",
        ("file_id", "project_id"),
        "files",
        ("id", "project_id"),
        "RESTRICT",
    ),
    _ForeignKeyDeleteRule(
        "jobs",
        ("extraction_profile_id", "project_id"),
        "extraction_profiles",
        ("id", "project_id"),
        "RESTRICT",
    ),
    _ForeignKeyDeleteRule("jobs", ("project_id",), "projects", ("id",), "RESTRICT"),
    _ForeignKeyDeleteRule("job_events", ("job_id",), "jobs", ("id",), "RESTRICT"),
    _ForeignKeyDeleteRule(
        "adapter_run_outputs",
        ("source_file_id", "project_id"),
        "files",
        ("id", "project_id"),
        "RESTRICT",
    ),
    _ForeignKeyDeleteRule(
        "adapter_run_outputs",
        ("extraction_profile_id", "project_id"),
        "extraction_profiles",
        ("id", "project_id"),
        "RESTRICT",
    ),
    _ForeignKeyDeleteRule(
        "adapter_run_outputs",
        ("project_id",),
        "projects",
        ("id",),
        "RESTRICT",
    ),
    _ForeignKeyDeleteRule(
        "adapter_run_outputs",
        ("source_job_id",),
        "jobs",
        ("id",),
        "RESTRICT",
    ),
    _ForeignKeyDeleteRule(
        "drawing_revisions",
        ("source_file_id", "project_id"),
        "files",
        ("id", "project_id"),
        "RESTRICT",
    ),
    _ForeignKeyDeleteRule(
        "drawing_revisions",
        ("extraction_profile_id", "project_id"),
        "extraction_profiles",
        ("id", "project_id"),
        "RESTRICT",
    ),
    _ForeignKeyDeleteRule(
        "drawing_revisions",
        ("adapter_run_output_id", "project_id"),
        "adapter_run_outputs",
        ("id", "project_id"),
        "RESTRICT",
    ),
    _ForeignKeyDeleteRule(
        "drawing_revisions",
        ("project_id",),
        "projects",
        ("id",),
        "RESTRICT",
    ),
    _ForeignKeyDeleteRule(
        "drawing_revisions",
        ("source_job_id",),
        "jobs",
        ("id",),
        "RESTRICT",
    ),
    _ForeignKeyDeleteRule(
        "validation_reports",
        ("drawing_revision_id", "project_id"),
        "drawing_revisions",
        ("id", "project_id"),
        "RESTRICT",
    ),
    _ForeignKeyDeleteRule(
        "validation_reports",
        ("project_id",),
        "projects",
        ("id",),
        "RESTRICT",
    ),
    _ForeignKeyDeleteRule(
        "validation_reports",
        ("source_job_id",),
        "jobs",
        ("id",),
        "RESTRICT",
    ),
    _ForeignKeyDeleteRule(
        "generated_artifacts",
        ("source_file_id", "project_id"),
        "files",
        ("id", "project_id"),
        "RESTRICT",
    ),
    _ForeignKeyDeleteRule(
        "generated_artifacts",
        ("drawing_revision_id", "project_id"),
        "drawing_revisions",
        ("id", "project_id"),
        "RESTRICT",
    ),
    _ForeignKeyDeleteRule(
        "generated_artifacts",
        ("adapter_run_output_id", "project_id"),
        "adapter_run_outputs",
        ("id", "project_id"),
        "RESTRICT",
    ),
    _ForeignKeyDeleteRule(
        "generated_artifacts",
        ("project_id",),
        "projects",
        ("id",),
        "RESTRICT",
    ),
    _ForeignKeyDeleteRule(
        "generated_artifacts",
        ("job_id",),
        "jobs",
        ("id",),
        "RESTRICT",
    ),
)


def _normalize_ondelete(raw_foreign_key: Mapping[str, Any]) -> str:
    """Extract a normalized delete action from inspector output."""

    options = raw_foreign_key.get("options")
    if isinstance(options, dict):
        return str(options.get("ondelete") or "").upper()
    return ""


def _inspect_lineage_foreign_keys(
    sync_connection: sa.Connection,
) -> dict[tuple[str, tuple[str, ...], str, tuple[str, ...]], str]:
    """Load installed delete actions for lineage foreign keys from PostgreSQL."""

    inspector = sa.inspect(sync_connection)
    lineage_tables = sorted({rule.table_name for rule in _EXPECTED_LINEAGE_FOREIGN_KEYS})
    installed_rules: dict[tuple[str, tuple[str, ...], str, tuple[str, ...]], str] = {}

    for table_name in lineage_tables:
        for foreign_key in inspector.get_foreign_keys(table_name):
            installed_rules[
                (
                    table_name,
                    tuple(foreign_key["constrained_columns"]),
                    str(foreign_key["referred_table"]),
                    tuple(foreign_key["referred_columns"]),
                )
            ] = _normalize_ondelete(foreign_key)

    return installed_rules


async def _load_installed_lineage_foreign_keys(
) -> dict[tuple[str, tuple[str, ...], str, tuple[str, ...]], str]:
    """Inspect the migrated database instead of ORM metadata."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        connection = await session.connection()
        return await connection.run_sync(_inspect_lineage_foreign_keys)


@pytest.fixture(autouse=True)
def fake_ingestion_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> list[IngestionRunRequest]:
    """Patch worker ingestion with deterministic persisted outputs."""

    recorded_requests: list[IngestionRunRequest] = []

    async def _fake_run_ingestion(request: IngestionRunRequest) -> IngestFinalizationPayload:
        recorded_requests.append(request)
        return _build_fake_ingest_payload(request)

    monkeypatch.setattr(worker_module, "run_ingestion", _fake_run_ingestion)
    return recorded_requests


async def _load_lineage_snapshot(
    project_id: str,
    file_id: str,
    job_id: uuid.UUID,
) -> _LineageSnapshot:
    """Load a fully persisted ingest lineage for delete-policy assertions."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    adapter_outputs, drawing_revisions, validation_reports, generated_artifacts = (
        await _load_project_outputs(project_id)
    )
    assert len(adapter_outputs) == 1
    assert len(drawing_revisions) == 1
    assert len(validation_reports) == 1
    assert len(generated_artifacts) == 1

    async with session_maker() as session:
        project = await session.get(Project, _as_uuid(project_id))
        source_file = await session.get(FileModel, _as_uuid(file_id))
        job = await session.get(Job, job_id)

        assert project is not None
        assert source_file is not None
        assert job is not None
        assert job.extraction_profile_id is not None

        extraction_profile = await session.get(ExtractionProfile, job.extraction_profile_id)
        assert extraction_profile is not None

    return _LineageSnapshot(
        project=project,
        source_file=source_file,
        extraction_profile=extraction_profile,
        job=job,
        adapter_output=adapter_outputs[0],
        drawing_revision=drawing_revisions[0],
        validation_report=validation_reports[0],
        generated_artifact=generated_artifacts[0],
    )


async def _assert_hard_delete_fails(model: type[Any], object_id: uuid.UUID) -> None:
    """Assert a physical delete is rejected and the row remains."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        row = await session.get(model, object_id)
        assert row is not None

        await session.delete(row)

        with pytest.raises((DBAPIError, IntegrityError)) as exc_info:
            await session.commit()

        if isinstance(exc_info.value, DBAPIError):
            sqlstate = getattr(exc_info.value.orig, "sqlstate", None)
            if sqlstate == "55000":
                assert "append-only trigger blocked DELETE on" in str(exc_info.value)
            else:
                assert sqlstate == "23503"

        await session.rollback()

    async with session_maker() as session:
        assert await session.get(model, object_id) is not None


@requires_database
class TestLineageDeleteRestrictions:
    """Tests for non-destructive foreign key delete policies."""

    async def test_lineage_foreign_keys_are_restrict_in_upgraded_database(self) -> None:
        """Lineage FKs should be restrictive in the migrated PostgreSQL schema."""
        _ = self

        installed_rules = await _load_installed_lineage_foreign_keys()

        assert all(ondelete != "CASCADE" for ondelete in installed_rules.values())

        for expected_rule in _EXPECTED_LINEAGE_FOREIGN_KEYS:
            edge = (
                expected_rule.table_name,
                expected_rule.constrained_columns,
                expected_rule.referred_table,
                expected_rule.referred_columns,
            )
            assert edge in installed_rules
            assert installed_rules[edge] == expected_rule.ondelete

    @pytest.mark.parametrize(
        "model_name",
        [
            "project",
            "source_file",
            "extraction_profile",
            "job",
            "adapter_output",
            "drawing_revision",
        ],
    )
    async def test_hard_delete_of_lineage_parent_fails(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        model_name: str,
    ) -> None:
        """Physical deletes should fail instead of cascading through lineage."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(job.id)

        snapshot = await _load_lineage_snapshot(
            project["id"],
            str(uploaded["id"]),
            job.id,
        )
        row_by_model_name: dict[str, tuple[type[Any], uuid.UUID]] = {
            "project": (Project, snapshot.project.id),
            "source_file": (FileModel, snapshot.source_file.id),
            "extraction_profile": (ExtractionProfile, snapshot.extraction_profile.id),
            "job": (Job, snapshot.job.id),
            "adapter_output": (AdapterRunOutput, snapshot.adapter_output.id),
            "drawing_revision": (DrawingRevision, snapshot.drawing_revision.id),
        }

        model, object_id = row_by_model_name[model_name]
        await _assert_hard_delete_fails(model, object_id)

    async def test_project_soft_delete_retains_lineage_rows(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Soft-delete flows should keep lineage rows while marking retained resources."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(job.id)

        delete_response = await async_client.delete(f"/v1/projects/{project['id']}")
        assert delete_response.status_code == 204

        snapshot = await _load_lineage_snapshot(project["id"], str(uploaded["id"]), job.id)

        assert snapshot.project.deleted_at is not None
        assert snapshot.source_file.deleted_at is not None
        assert snapshot.generated_artifact.deleted_at is not None
        assert snapshot.job.project_id == snapshot.project.id
        assert snapshot.job.file_id == snapshot.source_file.id
        assert snapshot.job.extraction_profile_id == snapshot.extraction_profile.id
        assert snapshot.adapter_output.source_file_id == snapshot.source_file.id
        assert snapshot.adapter_output.source_job_id == snapshot.job.id
        assert snapshot.drawing_revision.adapter_run_output_id == snapshot.adapter_output.id
        assert snapshot.validation_report.drawing_revision_id == snapshot.drawing_revision.id
        assert snapshot.generated_artifact.job_id == snapshot.job.id
