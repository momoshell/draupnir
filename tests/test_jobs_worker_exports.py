"""Integration tests for export worker jobs."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import app.db.session as session_module
import app.jobs.worker as worker_module
from app.core.errors import ErrorCode
from app.exports._base import ExportArtifact
from app.exports.csv import CsvExportResult, render_estimate_csv_export, render_quantity_csv_export
from app.exports.estimate_pdf import EstimatePdfExportResult, render_estimate_pdf_export
from app.exports.revised_dxf import RevisedDxfExportError
from app.exports.revision_json import RevisionJsonExportResult, render_revision_json_export
from app.models.cad_changeset import CadChangeSet
from app.models.drawing_revision import DrawingRevision
from app.models.estimate_version import EstimateItem, EstimateSnapshotEntry, EstimateVersion
from app.models.export_job_input import ExportJobInput
from app.models.generated_artifact import GeneratedArtifact
from app.models.job import Job, JobType
from app.models.job_event import JobEvent
from app.models.quantity_takeoff import QuantityItem, QuantityItemKind, QuantityTakeoff
from app.schemas.export import ExportKind
from app.storage import get_storage
from tests.conftest import requires_database
from tests.jobs_test_helpers import (
    _create_project,
    _get_generated_artifacts_for_job,
    _get_job,
    _get_job_for_file,
    _update_job,
    _upload_file,
    fake_ingestion_runner,
)

pytestmark = [requires_database, pytest.mark.usefixtures(fake_ingestion_runner.__name__)]


@dataclass(frozen=True, slots=True)
class _ExportTestLineage:
    project_id: uuid.UUID
    file_id: uuid.UUID
    drawing_revision_id: uuid.UUID
    quantity_takeoff_id: uuid.UUID
    estimate_version_id: uuid.UUID
    changeset_id: uuid.UUID | None = None


class _StorageSpy:
    def __init__(self, wrapped: Any) -> None:
        self._wrapped = wrapped
        self.written_paths: list[Path] = []
        self.cleaned_paths: list[Path] = []

    async def put(self, key: str, content_bytes: bytes, *, immutable: bool) -> Any:
        stored_object = await self._wrapped.put(key, content_bytes, immutable=immutable)
        self.written_paths.append(Path(stored_object.storage_uri.removeprefix("file://")))
        return stored_object

    async def delete_failed_put(self, key: str, *, storage_uri: str) -> None:
        self.cleaned_paths.append(Path(storage_uri.removeprefix("file://")))
        await self._wrapped.delete_failed_put(key, storage_uri=storage_uri)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._wrapped, name)


def _get_session_maker() -> async_sessionmaker[AsyncSession]:
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    return session_maker


async def _get_latest_revision(file_id: uuid.UUID) -> DrawingRevision:
    session_maker = _get_session_maker()

    async with session_maker() as session:
        result = await session.execute(
            select(DrawingRevision)
            .where(DrawingRevision.source_file_id == file_id)
            .order_by(DrawingRevision.revision_sequence.desc(), DrawingRevision.id.desc())
            .limit(1)
        )
        revision = result.scalar_one_or_none()

    assert revision is not None
    return revision


async def _get_job_events(job_id: uuid.UUID) -> list[JobEvent]:
    session_maker = _get_session_maker()

    async with session_maker() as session:
        result = await session.execute(
            select(JobEvent)
            .where(JobEvent.job_id == job_id)
            .order_by(JobEvent.created_at.asc(), JobEvent.id.asc())
        )
        events = result.scalars().all()

    return list(events)


async def _create_completed_job(
    *,
    project_id: uuid.UUID,
    file_id: uuid.UUID,
    base_revision_id: uuid.UUID,
    job_type: JobType,
) -> Job:
    session_maker = _get_session_maker()
    now = datetime.now(UTC)

    async with session_maker() as session:
        job = Job(
            id=uuid.uuid4(),
            project_id=project_id,
            file_id=file_id,
            base_revision_id=base_revision_id,
            job_type=job_type.value,
            status="succeeded",
            attempts=1,
            started_at=now,
            finished_at=now,
        )
        session.add(job)
        await session.commit()
        await session.refresh(job)

    return job


async def _persist_quantity_takeoff(
    *,
    project_id: uuid.UUID,
    file_id: uuid.UUID,
    drawing_revision_id: uuid.UUID,
) -> QuantityTakeoff:
    session_maker = _get_session_maker()
    source_job = await _create_completed_job(
        project_id=project_id,
        file_id=file_id,
        base_revision_id=drawing_revision_id,
        job_type=JobType.QUANTITY_TAKEOFF,
    )

    async with session_maker() as session:
        takeoff = QuantityTakeoff(
            id=uuid.uuid4(),
            project_id=project_id,
            source_file_id=file_id,
            drawing_revision_id=drawing_revision_id,
            source_job_id=source_job.id,
            source_job_type=JobType.QUANTITY_TAKEOFF.value,
            review_state="approved",
            validation_status="valid",
            quantity_gate="allowed",
            trusted_totals=True,
        )
        item = QuantityItem(
            id=uuid.uuid4(),
            quantity_takeoff_id=takeoff.id,
            project_id=project_id,
            drawing_revision_id=drawing_revision_id,
            item_kind=QuantityItemKind.AGGREGATE.value,
            quantity_type="length",
            value=12.5,
            unit="m",
            review_state="approved",
            validation_status="valid",
            quantity_gate="allowed",
            source_entity_id=None,
            excluded_source_entity_ids_json=[],
        )
        session.add(takeoff)
        await session.flush()
        session.add(item)
        await session.commit()
        await session.refresh(takeoff)

    return takeoff


async def _persist_estimate_version(
    *,
    project_id: uuid.UUID,
    file_id: uuid.UUID,
    drawing_revision_id: uuid.UUID,
    quantity_takeoff_id: uuid.UUID,
) -> EstimateVersion:
    session_maker = _get_session_maker()
    source_job = await _create_completed_job(
        project_id=project_id,
        file_id=file_id,
        base_revision_id=drawing_revision_id,
        job_type=JobType.ESTIMATE,
    )

    async with session_maker() as session:
        estimate_version = EstimateVersion(
            id=uuid.uuid4(),
            project_id=project_id,
            source_file_id=file_id,
            drawing_revision_id=drawing_revision_id,
            quantity_takeoff_id=quantity_takeoff_id,
            source_job_id=source_job.id,
            quantity_gate="allowed",
            trusted_totals=True,
            currency="GBP",
            subtotal_amount=Decimal("40.00"),
            tax_amount=Decimal("0.00"),
            total_amount=Decimal("40.00"),
        )
        snapshot_entry = EstimateSnapshotEntry(
            id=uuid.uuid4(),
            estimate_version_id=estimate_version.id,
            project_id=project_id,
            drawing_revision_id=drawing_revision_id,
            entry_type="assumption",
            entry_key="assumption:1",
            entry_label="Allowance",
            sort_order=1,
            source_payload_json={"kind": "assumption", "note": "manual allowance"},
        )
        item = EstimateItem(
            id=uuid.uuid4(),
            estimate_version_id=estimate_version.id,
            project_id=project_id,
            drawing_revision_id=drawing_revision_id,
            line_type="assumption",
            line_number=1,
            line_key="line-1",
            description="Linear wall allowance",
            currency="GBP",
            subtotal_amount=Decimal("40.00"),
            tax_amount=Decimal("0.00"),
            total_amount=Decimal("40.00"),
            rounding_json={"mode": "HALF_UP", "scale": 2},
            assumption_snapshot_entry_id=snapshot_entry.id,
        )
        session.add(estimate_version)
        await session.flush()
        session.add(snapshot_entry)
        await session.flush()
        session.add(item)
        await session.commit()
        await session.refresh(estimate_version)

    return estimate_version


async def _create_export_test_lineage(async_client: httpx.AsyncClient) -> _ExportTestLineage:
    project = await _create_project(async_client)
    uploaded_file = await _upload_file(async_client, project["id"])
    ingest_job = await _get_job_for_file(uploaded_file["id"])
    await worker_module.process_ingest_job(ingest_job.id)

    file_id = uuid.UUID(uploaded_file["id"])
    project_id = uuid.UUID(project["id"])
    revision = await _get_latest_revision(file_id)
    quantity_takeoff = await _persist_quantity_takeoff(
        project_id=project_id,
        file_id=file_id,
        drawing_revision_id=revision.id,
    )
    estimate_version = await _persist_estimate_version(
        project_id=project_id,
        file_id=file_id,
        drawing_revision_id=revision.id,
        quantity_takeoff_id=quantity_takeoff.id,
    )
    return _ExportTestLineage(
        project_id=project_id,
        file_id=file_id,
        drawing_revision_id=revision.id,
        quantity_takeoff_id=quantity_takeoff.id,
        estimate_version_id=estimate_version.id,
    )


async def _create_revised_dxf_export_test_lineage(
    async_client: httpx.AsyncClient,
) -> _ExportTestLineage:
    lineage = await _create_export_test_lineage(async_client)
    base_revision = await _get_latest_revision(lineage.file_id)
    changeset_id = uuid.uuid4()
    changeset_revision_id = uuid.uuid4()
    changeset_job = await _create_completed_job(
        project_id=lineage.project_id,
        file_id=lineage.file_id,
        base_revision_id=base_revision.id,
        job_type=JobType.CHANGESET_APPLY,
    )
    session_maker = _get_session_maker()

    async with session_maker() as session:
        session.add(
            CadChangeSet(
                id=changeset_id,
                project_id=lineage.project_id,
                base_revision_id=base_revision.id,
                status="applied",
                created_by="test",
            )
        )
        session.add(
            DrawingRevision(
                id=changeset_revision_id,
                project_id=lineage.project_id,
                source_file_id=lineage.file_id,
                extraction_profile_id=None,
                source_job_id=changeset_job.id,
                adapter_run_output_id=None,
                changeset_id=changeset_id,
                predecessor_revision_id=base_revision.id,
                revision_sequence=base_revision.revision_sequence + 1,
                revision_kind="changeset",
                review_state=base_revision.review_state,
                canonical_entity_schema_version=base_revision.canonical_entity_schema_version,
                confidence_score=base_revision.confidence_score,
            )
        )
        await session.commit()

    return replace(
        lineage,
        drawing_revision_id=changeset_revision_id,
        changeset_id=changeset_id,
    )


async def _create_export_job(
    *,
    lineage: _ExportTestLineage,
    export_kind: str,
    export_format: str,
    media_type: str,
    options_json: dict[str, object],
    quantity_takeoff_id: uuid.UUID | None = None,
    estimate_version_id: uuid.UUID | None = None,
    quantity_gate: str | None = None,
    trusted_totals: bool | None = None,
) -> uuid.UUID:
    session_maker = _get_session_maker()
    export_job_id = uuid.uuid4()

    async with session_maker() as session:
        session.add(
            Job(
                id=export_job_id,
                project_id=lineage.project_id,
                file_id=lineage.file_id,
                base_revision_id=lineage.drawing_revision_id,
                job_type=JobType.EXPORT.value,
                status="pending",
            )
        )
        await session.flush()
        session.add(
            ExportJobInput(
                source_job_id=export_job_id,
                project_id=lineage.project_id,
                source_file_id=lineage.file_id,
                drawing_revision_id=lineage.drawing_revision_id,
                source_job_type=JobType.EXPORT.value,
                export_kind=export_kind,
                export_format=export_format,
                media_type=media_type,
                options_json=options_json,
                quantity_takeoff_id=quantity_takeoff_id,
                quantity_gate=quantity_gate,
                trusted_totals=trusted_totals,
                estimate_version_id=estimate_version_id,
            )
        )
        await session.commit()

    return export_job_id


async def _expected_export_bytes(
    *,
    lineage: _ExportTestLineage,
    export_kind: str,
    options_json: dict[str, object],
) -> tuple[bytes, str, str]:
    session_maker = _get_session_maker()

    async with session_maker() as session:
        result: RevisionJsonExportResult | CsvExportResult | EstimatePdfExportResult
        if export_kind == "revision_json":
            result = await render_revision_json_export(
                session,
                lineage.drawing_revision_id,
                options=options_json,
            )
        elif export_kind == "quantity_csv":
            result = await render_quantity_csv_export(session, lineage.quantity_takeoff_id)
        elif export_kind == "estimate_csv":
            result = await render_estimate_csv_export(session, lineage.estimate_version_id)
        elif export_kind == "estimate_pdf":
            result = await render_estimate_pdf_export(
                session,
                lineage.estimate_version_id,
                options=options_json,
            )
        else:
            raise AssertionError(f"Unexpected export kind {export_kind}")

    return result.content_bytes, result.generator_name, result.generator_version


def _build_export_execution_input(
    *,
    lineage: _ExportTestLineage,
    export_kind: str,
    export_format: str,
    media_type: str,
    options_json: dict[str, object],
) -> worker_module._ExportExecutionInput:
    quantity_takeoff_id = (
        lineage.quantity_takeoff_id
        if export_kind in {"quantity_csv", "estimate_csv", "estimate_pdf"}
        else None
    )
    estimate_version_id = (
        lineage.estimate_version_id if export_kind in {"estimate_csv", "estimate_pdf"} else None
    )
    return worker_module._ExportExecutionInput(
        drawing_revision_id=lineage.drawing_revision_id,
        export_kind=export_kind,
        export_format=export_format,
        media_type=media_type,
        artifact_name=worker_module._build_export_artifact_name(
            export_kind=export_kind,
            export_format=export_format,
            drawing_revision_id=lineage.drawing_revision_id,
            quantity_takeoff_id=quantity_takeoff_id,
            estimate_version_id=estimate_version_id,
        ),
        options_json=options_json,
        quantity_takeoff_id=quantity_takeoff_id,
        estimate_version_id=estimate_version_id,
    )


def _build_revised_dxf_execution_input(
    *,
    lineage: _ExportTestLineage,
    changeset_id: uuid.UUID | None = None,
    options_json: dict[str, object] | None = None,
) -> worker_module._ExportExecutionInput:
    resolved_changeset_id = changeset_id or lineage.changeset_id or uuid.uuid4()
    resolved_options = {} if options_json is None else options_json
    return worker_module._ExportExecutionInput(
        drawing_revision_id=lineage.drawing_revision_id,
        export_kind="revised_dxf",
        export_format="dxf",
        media_type="application/dxf",
        artifact_name=worker_module._build_export_artifact_name(
            export_kind="revised_dxf",
            export_format="dxf",
            drawing_revision_id=lineage.drawing_revision_id,
            changeset_id=resolved_changeset_id,
        ),
        options_json=resolved_options,
        changeset_id=resolved_changeset_id,
    )


@pytest.mark.parametrize(
    ("export_kind", "export_format", "media_type", "options_json"),
    [
        ("revision_json", "json", "application/json", {"include_manifest": True}),
        ("quantity_csv", "csv", "text/csv", {}),
        ("estimate_csv", "csv", "text/csv", {}),
        ("estimate_pdf", "pdf", "application/pdf", {}),
    ],
)
async def test_process_export_job_supported_kinds_persist_artifact(
    async_client: httpx.AsyncClient,
    export_kind: str,
    export_format: str,
    media_type: str,
    options_json: dict[str, object],
) -> None:
    lineage = await _create_export_test_lineage(async_client)
    export_job_id = await _create_export_job(
        lineage=lineage,
        export_kind=export_kind,
        export_format=export_format,
        media_type=media_type,
        options_json=options_json,
        quantity_takeoff_id=(
            lineage.quantity_takeoff_id
            if export_kind in {"quantity_csv", "estimate_csv", "estimate_pdf"}
            else None
        ),
        estimate_version_id=(
            lineage.estimate_version_id if export_kind in {"estimate_csv", "estimate_pdf"} else None
        ),
        quantity_gate=(
            "allowed" if export_kind in {"quantity_csv", "estimate_csv", "estimate_pdf"} else None
        ),
        trusted_totals=(
            True if export_kind in {"quantity_csv", "estimate_csv", "estimate_pdf"} else None
        ),
    )

    await worker_module.process_export_job(export_job_id)

    job = await _get_job(export_job_id)
    assert job.status == "succeeded"
    assert job.error_code is None
    assert job.error_message is None

    artifacts = await _get_generated_artifacts_for_job(export_job_id)
    assert len(artifacts) == 1
    artifact = artifacts[0]
    assert artifact.artifact_kind == export_kind
    assert artifact.format == export_format
    assert artifact.media_type == media_type
    assert artifact.project_id == lineage.project_id
    assert artifact.source_file_id == lineage.file_id
    assert artifact.drawing_revision_id == lineage.drawing_revision_id
    assert artifact.quantity_takeoff_id == (
        lineage.quantity_takeoff_id
        if export_kind in {"quantity_csv", "estimate_csv", "estimate_pdf"}
        else None
    )
    assert artifact.estimate_version_id == (
        lineage.estimate_version_id if export_kind in {"estimate_csv", "estimate_pdf"} else None
    )
    assert artifact.generator_config_json == options_json
    lineage_json = cast(dict[str, Any], artifact.lineage_json)
    assert lineage_json["drawing_revision"] == {"id": str(lineage.drawing_revision_id)}
    assert "changeset_id" not in cast(dict[str, Any], lineage_json["export"])

    (
        expected_bytes,
        expected_generator_name,
        expected_generator_version,
    ) = await _expected_export_bytes(
        lineage=lineage,
        export_kind=export_kind,
        options_json=options_json,
    )
    storage_path = Path(artifact.storage_uri.removeprefix("file://"))
    assert storage_path.exists()
    stored_bytes = storage_path.read_bytes()
    assert stored_bytes == expected_bytes
    assert hashlib.sha256(stored_bytes).hexdigest() == artifact.checksum_sha256
    assert len(stored_bytes) == artifact.size_bytes
    assert artifact.generator_name == expected_generator_name
    assert artifact.generator_version == expected_generator_version

    success_events = [
        event for event in await _get_job_events(export_job_id) if event.message == "Job succeeded"
    ]
    assert len(success_events) == 1
    assert success_events[0].data_json == {
        "status": "succeeded",
        "attempts": 1,
        "generated_artifact_id": str(artifact.id),
        "export_kind": export_kind,
    }


def test_export_kind_spec_revised_dxf_is_registered_with_changeset_anchor() -> None:
    spec = worker_module._get_export_kind_spec("revised_dxf")

    assert spec.format == "dxf"
    assert spec.media_type == "application/dxf"
    assert spec.lineage_anchor == worker_module._EXPORT_LINEAGE_ANCHOR_CHANGESET


@pytest.mark.parametrize(
    ("export_kind", "export_format", "media_type", "options_json"),
    [
        ("revision_json", "json", "application/json", {"include_manifest": True}),
        ("quantity_csv", "csv", "text/csv", {}),
        ("estimate_csv", "csv", "text/csv", {}),
        ("estimate_pdf", "pdf", "application/pdf", {}),
    ],
)
async def test_render_export_artifact_supported_kinds_return_shared_export_artifact(
    async_client: httpx.AsyncClient,
    export_kind: str,
    export_format: str,
    media_type: str,
    options_json: dict[str, object],
) -> None:
    lineage = await _create_export_test_lineage(async_client)
    execution = _build_export_execution_input(
        lineage=lineage,
        export_kind=export_kind,
        export_format=export_format,
        media_type=media_type,
        options_json=options_json,
    )
    session_maker = _get_session_maker()

    async with session_maker() as session:
        rendered = await worker_module._render_export_artifact(session, execution)

    assert isinstance(rendered, ExportArtifact)
    assert rendered.media_type == media_type


async def test_process_export_job_revised_dxf_requires_changeset_origin_revision(
    async_client: httpx.AsyncClient,
) -> None:
    lineage = await _create_export_test_lineage(async_client)
    export_job_id = await _create_export_job(
        lineage=lineage,
        export_kind="revised_dxf",
        export_format="dxf",
        media_type="application/dxf",
        options_json={},
    )

    await worker_module.process_export_job(export_job_id)

    job = await _get_job(export_job_id)
    assert job.status == "failed"
    assert job.error_code == ErrorCode.INPUT_INVALID.value
    assert job.error_message == "Revised DXF export requires a changeset-origin drawing revision."
    assert await _get_generated_artifacts_for_job(export_job_id) == []


async def test_process_export_job_revised_dxf_persists_typed_changeset_artifact_and_redelivery_noop(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lineage = await _create_revised_dxf_export_test_lineage(async_client)
    export_job_id = await _create_export_job(
        lineage=lineage,
        export_kind="revised_dxf",
        export_format="dxf",
        media_type="application/dxf",
        options_json={"profile": "default"},
    )

    async def _render_revised_dxf(
        db: AsyncSession,
        revision_id: uuid.UUID,
        options: dict[str, object] | None = None,
    ) -> ExportArtifact:
        _ = db
        assert revision_id == lineage.drawing_revision_id
        assert options == {"profile": "default"}
        content_bytes = b"0\nSECTION\n2\nHEADER\n0\nENDSEC\n0\nEOF\n"
        return ExportArtifact(
            content_bytes=content_bytes,
            checksum_sha256=hashlib.sha256(content_bytes).hexdigest(),
            size_bytes=len(content_bytes),
            media_type="application/dxf",
            generator_name="revised_dxf_export",
            generator_version="1",
        )

    monkeypatch.setattr(worker_module, "render_revised_dxf_export", _render_revised_dxf)

    await worker_module.process_export_job(export_job_id)
    await worker_module.process_export_job(export_job_id)

    job = await _get_job(export_job_id)
    assert job.status == "succeeded"
    assert job.error_code is None
    artifacts = await _get_generated_artifacts_for_job(export_job_id)
    assert len(artifacts) == 1
    artifact = artifacts[0]
    lineage_json = cast(dict[str, Any], artifact.lineage_json)
    assert artifact.artifact_kind == "revised_dxf"
    assert artifact.format == "dxf"
    assert artifact.media_type == "application/dxf"
    assert artifact.name == f"changeset-{lineage.changeset_id}.dxf"
    assert artifact.changeset_id == lineage.changeset_id
    assert lineage_json["drawing_revision"] == {
        "id": str(lineage.drawing_revision_id),
        "changeset_id": str(lineage.changeset_id),
    }
    export_lineage = cast(dict[str, Any], lineage_json["export"])
    assert export_lineage["changeset_id"] == str(lineage.changeset_id)

    success_events = [
        event for event in await _get_job_events(export_job_id) if event.message == "Job succeeded"
    ]
    assert len(success_events) == 1


@pytest.mark.parametrize(
    ("renderer_code", "expected_error_code"),
    [
        ("UNSUPPORTED_ENTITY_TYPE", ErrorCode.INPUT_INVALID.value),
        ("ADAPTER_UNAVAILABLE", ErrorCode.ADAPTER_UNAVAILABLE.value),
        ("ADAPTER_LOAD_FAILED", ErrorCode.ADAPTER_UNAVAILABLE.value),
        ("ADAPTER_FAILED", ErrorCode.ADAPTER_FAILED.value),
        ("EXPORT_FAILED", ErrorCode.ADAPTER_FAILED.value),
        ("WRITER_CONTRACT_BROKEN", ErrorCode.ADAPTER_FAILED.value),
    ],
)
async def test_process_export_job_revised_dxf_renderer_errors_map_without_artifact(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    renderer_code: str,
    expected_error_code: str,
) -> None:
    lineage = await _create_revised_dxf_export_test_lineage(async_client)
    export_job_id = await _create_export_job(
        lineage=lineage,
        export_kind="revised_dxf",
        export_format="dxf",
        media_type="application/dxf",
        options_json={},
    )

    async def _raise_revised_dxf_error(
        db: AsyncSession,
        revision_id: uuid.UUID,
        options: dict[str, object] | None = None,
    ) -> ExportArtifact:
        _ = (db, revision_id, options)
        raise RevisedDxfExportError(
            code=renderer_code,
            message=f"renderer failed with {renderer_code}",
            details={"entity_type": "ARC"},
        )

    monkeypatch.setattr(worker_module, "render_revised_dxf_export", _raise_revised_dxf_error)

    await worker_module.process_export_job(export_job_id)

    job = await _get_job(export_job_id)
    assert job.status == "failed"
    assert job.error_code == expected_error_code
    assert job.error_message is not None
    assert await _get_generated_artifacts_for_job(export_job_id) == []


async def test_process_export_job_revised_dxf_unexpected_renderer_failure_is_internal_error(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lineage = await _create_revised_dxf_export_test_lineage(async_client)
    export_job_id = await _create_export_job(
        lineage=lineage,
        export_kind="revised_dxf",
        export_format="dxf",
        media_type="application/dxf",
        options_json={},
    )

    async def _raise_runtime_error(
        db: AsyncSession,
        revision_id: uuid.UUID,
        options: dict[str, object] | None = None,
    ) -> ExportArtifact:
        _ = (db, revision_id, options)
        raise RuntimeError("unexpected revised dxf failure")

    monkeypatch.setattr(worker_module, "render_revised_dxf_export", _raise_runtime_error)

    with pytest.raises(RuntimeError, match="unexpected revised dxf failure"):
        await worker_module.process_export_job(export_job_id)

    job = await _get_job(export_job_id)
    assert job.status == "failed"
    assert job.error_code == ErrorCode.INTERNAL_ERROR.value
    assert job.error_message == worker_module._PROCESS_EXPORT_JOB_ERROR_MESSAGE
    assert await _get_generated_artifacts_for_job(export_job_id) == []


async def test_process_export_job_duplicate_terminal_redelivery_is_noop(
    async_client: httpx.AsyncClient,
) -> None:
    lineage = await _create_export_test_lineage(async_client)
    export_job_id = await _create_export_job(
        lineage=lineage,
        export_kind="revision_json",
        export_format="json",
        media_type="application/json",
        options_json={"include_manifest": True},
    )

    await worker_module.process_export_job(export_job_id)
    await worker_module.process_export_job(export_job_id)

    artifacts = await _get_generated_artifacts_for_job(export_job_id)
    assert len(artifacts) == 1

    success_events = [
        event for event in await _get_job_events(export_job_id) if event.message == "Job succeeded"
    ]
    assert len(success_events) == 1


async def test_process_export_job_metadata_mismatch_fails_without_artifact(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lineage = await _create_export_test_lineage(async_client)
    export_job_id = await _create_export_job(
        lineage=lineage,
        export_kind="revision_json",
        export_format="json",
        media_type="application/json",
        options_json={"include_manifest": True},
    )
    monkeypatch.setitem(
        worker_module._EXPORT_KIND_SPECS,
        "revision_json",
        replace(
            worker_module._EXPORT_KIND_SPECS["revision_json"],
            format="csv",
            media_type="text/csv",
        ),
    )

    await worker_module.process_export_job(export_job_id)

    job = await _get_job(export_job_id)
    assert job.status == "failed"
    assert job.error_code == ErrorCode.INPUT_INVALID.value
    assert job.error_message == "Export job metadata does not match the supported export kind."
    assert await _get_generated_artifacts_for_job(export_job_id) == []


async def test_process_export_job_unsupported_kind_fails_without_artifact(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lineage = await _create_export_test_lineage(async_client)
    export_job_id = await _create_export_job(
        lineage=lineage,
        export_kind="revision_json",
        export_format="json",
        media_type="application/json",
        options_json={"include_manifest": True},
    )
    monkeypatch.delitem(worker_module._EXPORT_KIND_SPECS, "revision_json")

    await worker_module.process_export_job(export_job_id)

    job = await _get_job(export_job_id)
    assert job.status == "failed"
    assert job.error_code == ErrorCode.INPUT_INVALID.value
    assert job.error_message == "Export job kind is not supported by the worker."
    assert await _get_generated_artifacts_for_job(export_job_id) == []


async def test_process_export_job_honors_cancel_before_finalization(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lineage = await _create_export_test_lineage(async_client)
    export_job_id = await _create_export_job(
        lineage=lineage,
        export_kind="revision_json",
        export_format="json",
        media_type="application/json",
        options_json={"include_manifest": True},
    )
    original_build_execution_input = worker_module._build_export_execution_input

    async def _cancel_after_input_load(
        job_id: uuid.UUID,
        *,
        attempt_token: uuid.UUID,
    ) -> worker_module._ExportExecutionInput:
        execution = await original_build_execution_input(job_id, attempt_token=attempt_token)
        await _update_job(job_id, cancel_requested=True)
        return execution

    monkeypatch.setattr(worker_module, "_build_export_execution_input", _cancel_after_input_load)

    await worker_module.process_export_job(export_job_id)

    job = await _get_job(export_job_id)
    assert job.status == "cancelled"
    assert job.error_code == ErrorCode.JOB_CANCELLED.value
    assert job.error_message is None
    assert await _get_generated_artifacts_for_job(export_job_id) == []
    assert (await _get_job_events(export_job_id))[-1].data_json == {"status": "cancelled"}


async def test_process_export_job_honors_cancel_requested_before_compute(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancel requested after the attempt is claimed short-circuits before render."""
    lineage = await _create_export_test_lineage(async_client)
    export_job_id = await _create_export_job(
        lineage=lineage,
        export_kind="revision_json",
        export_format="json",
        media_type="application/json",
        options_json={"include_manifest": True},
    )

    original_begin = worker_module._begin_or_resume_registered_job

    async def _begin_then_request_cancel(
        job_id: uuid.UUID,
        *,
        process_name: str,
    ) -> Any:
        lease = await original_begin(job_id, process_name=process_name)
        if lease is not None:
            await _update_job(job_id, cancel_requested=True)
        return lease

    async def _fail_if_compute_starts(
        job_id: uuid.UUID,
        *,
        attempt_token: uuid.UUID,
    ) -> Any:
        raise AssertionError("export compute must not start once cancel is requested")

    monkeypatch.setattr(
        worker_module, "_begin_or_resume_registered_job", _begin_then_request_cancel
    )
    monkeypatch.setattr(worker_module, "_build_export_execution_input", _fail_if_compute_starts)

    await worker_module.process_export_job(export_job_id)

    job = await _get_job(export_job_id)
    assert job.status == "cancelled"
    assert job.error_code == ErrorCode.JOB_CANCELLED.value
    assert await _get_generated_artifacts_for_job(export_job_id) == []
    assert (await _get_job_events(export_job_id))[-1].data_json == {"status": "cancelled"}


async def test_process_export_job_revision_drift_fails_without_artifact(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lineage = await _create_export_test_lineage(async_client)
    export_job_id = await _create_export_job(
        lineage=lineage,
        export_kind="revision_json",
        export_format="json",
        media_type="application/json",
        options_json={"include_manifest": True},
    )
    original_build_execution_input = worker_module._build_export_execution_input

    async def _load_stale_execution_input(
        job_id: uuid.UUID,
        *,
        attempt_token: uuid.UUID,
    ) -> worker_module._ExportExecutionInput:
        execution = await original_build_execution_input(job_id, attempt_token=attempt_token)
        return replace(execution, drawing_revision_id=uuid.uuid4())

    async def _render_without_revision_lookup(
        session: AsyncSession,
        execution: worker_module._ExportExecutionInput,
    ) -> ExportArtifact:
        _ = (session, execution)
        content_bytes = b"stale-rendered-export"
        return ExportArtifact(
            content_bytes=content_bytes,
            checksum_sha256=hashlib.sha256(content_bytes).hexdigest(),
            size_bytes=len(content_bytes),
            media_type="application/json",
            generator_name="test_export_renderer",
            generator_version="1",
        )

    monkeypatch.setattr(worker_module, "_build_export_execution_input", _load_stale_execution_input)
    monkeypatch.setattr(worker_module, "_render_export_artifact", _render_without_revision_lookup)

    await worker_module.process_export_job(export_job_id)

    job = await _get_job(export_job_id)
    assert job.status == "failed"
    assert job.error_code == ErrorCode.REVISION_CONFLICT.value
    assert await _get_generated_artifacts_for_job(export_job_id) == []


async def test_process_export_job_invalid_trust_gate_fails_without_artifact(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lineage = await _create_export_test_lineage(async_client)
    export_job_id = await _create_export_job(
        lineage=lineage,
        export_kind="quantity_csv",
        export_format="csv",
        media_type="text/csv",
        options_json={},
        quantity_takeoff_id=lineage.quantity_takeoff_id,
        quantity_gate="allowed",
        trusted_totals=True,
    )

    async def _raise_invalid_trust_gate(
        job_id: uuid.UUID,
        *,
        attempt_token: uuid.UUID,
    ) -> worker_module._ExportExecutionInput:
        _ = (job_id, attempt_token)
        raise worker_module._build_export_job_input_error(
            "Export job input requires a trusted quantity takeoff with allowed gate.",
            details={
                "quantity_takeoff_id": str(lineage.quantity_takeoff_id),
                "quantity_gate": "review_gated",
                "trusted_totals": False,
            },
        )

    monkeypatch.setattr(worker_module, "_build_export_execution_input", _raise_invalid_trust_gate)

    await worker_module.process_export_job(export_job_id)

    job = await _get_job(export_job_id)
    assert job.status == "failed"
    assert job.error_code == ErrorCode.INPUT_INVALID.value
    assert (
        job.error_message
        == "Export job input requires a trusted quantity takeoff with allowed gate."
    )
    assert await _get_generated_artifacts_for_job(export_job_id) == []


async def test_process_export_job_flush_failure_cleans_up_storage_without_artifact(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lineage = await _create_export_test_lineage(async_client)
    export_job_id = await _create_export_job(
        lineage=lineage,
        export_kind="revision_json",
        export_format="json",
        media_type="application/json",
        options_json={"include_manifest": True},
    )
    storage_spy = _StorageSpy(get_storage())
    original_flush = AsyncSession.flush

    async def _fail_flush_after_storage_write(
        self: AsyncSession,
        objects: Sequence[Any] | None = None,
    ) -> None:
        if any(isinstance(instance, GeneratedArtifact) for instance in self.sync_session.new):
            raise RuntimeError("flush failed after storage write")
        await original_flush(self, objects)

    monkeypatch.setattr(worker_module, "get_storage", lambda: storage_spy)
    monkeypatch.setattr(AsyncSession, "flush", _fail_flush_after_storage_write)

    with pytest.raises(RuntimeError, match="flush failed after storage write"):
        await worker_module.process_export_job(export_job_id)

    job = await _get_job(export_job_id)
    assert job.status == "failed"
    assert job.error_code == ErrorCode.INTERNAL_ERROR.value
    assert job.error_message == worker_module._FINALIZE_EXPORT_JOB_ERROR_MESSAGE
    assert await _get_generated_artifacts_for_job(export_job_id) == []

    success_events = [
        event for event in await _get_job_events(export_job_id) if event.message == "Job succeeded"
    ]
    assert success_events == []

    assert len(storage_spy.written_paths) == 1
    assert storage_spy.cleaned_paths == storage_spy.written_paths
    assert not storage_spy.written_paths[0].exists()


async def test_publish_export_job_recovery_failure_marks_export_specific_message(
    async_client: httpx.AsyncClient,
) -> None:
    lineage = await _create_export_test_lineage(async_client)
    export_job_id = await _create_export_job(
        lineage=lineage,
        export_kind="revision_json",
        export_format="json",
        media_type="application/json",
        options_json={"include_manifest": True},
    )

    def _raise_publish(job_id: uuid.UUID) -> None:
        _ = job_id
        raise RuntimeError("broker unavailable")

    published = await worker_module.publish_job_enqueue_intent(
        export_job_id,
        recovery=True,
        publisher=_raise_publish,
    )

    assert published is False

    job = await _get_job(export_job_id)
    assert job.status == "failed"
    assert job.error_code == ErrorCode.INTERNAL_ERROR.value
    assert job.error_message == worker_module._ENQUEUE_EXPORT_JOB_ERROR_MESSAGE


@dataclass(frozen=True, slots=True)
class _ExportE2ECase:
    name: str
    export_kind: ExportKind
    path: str
    body: dict[str, object]


def _export_e2e_cases(lineage: _ExportTestLineage) -> list[_ExportE2ECase]:
    return [
        _ExportE2ECase(
            name="revision_json",
            export_kind=ExportKind.REVISION_JSON,
            path=f"/v1/revisions/{lineage.drawing_revision_id}/exports/revision-json",
            body={"options": {"include_manifest": True}},
        ),
        _ExportE2ECase(
            name="quantity_csv",
            export_kind=ExportKind.QUANTITY_CSV,
            path=(
                f"/v1/revisions/{lineage.drawing_revision_id}"
                f"/quantity-takeoffs/{lineage.quantity_takeoff_id}/exports/quantity-csv"
            ),
            body={"options": {"dialect": "excel", "include_units": True}},
        ),
        _ExportE2ECase(
            name="estimate_pdf",
            export_kind=ExportKind.ESTIMATE_PDF,
            path=(
                f"/v1/revisions/{lineage.drawing_revision_id}"
                f"/quantity-takeoffs/{lineage.quantity_takeoff_id}"
                f"/estimates/{lineage.estimate_version_id}/exports"
            ),
            body={"export_kind": ExportKind.ESTIMATE_PDF.value, "options": {}},
        ),
    ]


def _assert_export_payload_bytes(case: _ExportE2ECase, payload_bytes: bytes) -> None:
    if case.export_kind == ExportKind.REVISION_JSON:
        body = json.loads(payload_bytes.decode("utf-8"))
        assert body["schema_version"] == "revision-json-export-v1"
        return

    if case.export_kind == ExportKind.QUANTITY_CSV:
        csv_text = payload_bytes.decode("utf-8")
        assert "quantity_item_id" in csv_text
        assert "quantity_takeoff_id" in csv_text
        return

    assert case.export_kind == ExportKind.ESTIMATE_PDF
    assert payload_bytes.startswith(b"%PDF")


@pytest.mark.parametrize(
    "case_name",
    ["revision_json", "quantity_csv", "estimate_pdf"],
)
async def test_export_create_to_worker_to_artifact_list_storage_readback(
    async_client: httpx.AsyncClient,
    case_name: str,
) -> None:
    lineage = await _create_export_test_lineage(async_client)
    case = next(case for case in _export_e2e_cases(lineage) if case.name == case_name)

    create_response = await async_client.post(case.path, json=case.body)

    assert create_response.status_code == 202
    export_job_id = uuid.UUID(create_response.json()["id"])

    await worker_module.process_export_job(export_job_id)

    export_job = await _get_job(export_job_id)
    assert export_job.status == "succeeded"

    artifacts_for_job = await _get_generated_artifacts_for_job(export_job_id)
    assert len(artifacts_for_job) == 1
    artifact = artifacts_for_job[0]

    list_response = await async_client.get(
        f"/v1/revisions/{lineage.drawing_revision_id}/generated-artifacts"
    )

    assert list_response.status_code == 200
    payload = list_response.json()
    listed_artifacts = payload["items"]
    assert len(listed_artifacts) >= 1
    listed_ids = {item["id"] for item in listed_artifacts}
    assert str(artifact.id) in listed_ids

    storage_path = Path(artifact.storage_uri.removeprefix("file://"))
    assert storage_path.exists()
    artifact_bytes = storage_path.read_bytes()
    _assert_export_payload_bytes(case, artifact_bytes)
