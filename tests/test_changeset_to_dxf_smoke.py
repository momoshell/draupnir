"""End-to-end smoke coverage for changeset apply through revised-DXF export."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

import app.db.session as session_module
import app.jobs.worker as worker_module
from app.api.v1.revision_routes import changesets as changeset_routes_module
from app.api.v1.revision_routes import exports as exports_routes_module
from app.models.adapter_run_output import AdapterRunOutput
from app.models.cad_changeset import CadChangeSet, CadChangeSetValidationResult
from app.models.drawing_revision import DrawingRevision
from app.models.extraction_profile import ExtractionProfile
from app.models.file import File
from app.models.generated_artifact import GeneratedArtifact
from app.models.job import Job, JobType
from app.models.project import Project
from app.models.revision_materialization import (
    RevisionEntity,
    RevisionEntityManifest,
    RevisionLayer,
    RevisionLayout,
)
from app.models.validation_report import ValidationReport
from app.schemas.export import ExportKind
from tests.conftest import requires_database, truncate_projects_cascade_for_cleanup

pytestmark = [pytest.mark.anyio, requires_database]

_SOURCE_HASH = "1" * 64


@dataclass(frozen=True, slots=True)
class _SeededChangesetSmokeLineage:
    project_id: UUID
    file_id: UUID
    extraction_profile_id: UUID
    base_revision_id: UUID
    source_job_id: UUID
    revision_entity_id: UUID


async def _reset_database_pool() -> None:
    """Drop DB connections opened by direct helpers before ASGI API calls."""

    await session_module.close_db()
    session_module.engine, session_module.AsyncSessionLocal = session_module._init_db_resources()


async def _truncate_and_reset_database_pool() -> None:
    await truncate_projects_cascade_for_cleanup()
    await _reset_database_pool()


async def _seed_changeset_smoke_lineage() -> _SeededChangesetSmokeLineage:
    assert session_module.AsyncSessionLocal is not None

    async with session_module.AsyncSessionLocal() as session:
        project = Project(name=f"Changeset to DXF smoke {uuid4()}")
        session.add(project)
        await session.flush()

        profile = ExtractionProfile(
            project_id=project.id,
            profile_version="1",
            layout_mode="modelspace",
            xref_handling="ignore",
            block_handling="expand",
        )
        session.add(profile)
        await session.flush()

        source_file = File(
            project_id=project.id,
            original_filename="changeset-smoke.dxf",
            media_type="application/dxf",
            detected_format="dxf",
            storage_uri=f"file:///tmp/changeset-smoke-{uuid4()}.dxf",
            size_bytes=128,
            checksum_sha256=uuid4().hex + uuid4().hex,
            immutable=True,
            initial_extraction_profile_id=profile.id,
        )
        session.add(source_file)
        await session.flush()

        source_job = Job(
            project_id=project.id,
            file_id=source_file.id,
            extraction_profile_id=profile.id,
            job_type=JobType.INGEST.value,
            status="succeeded",
            attempts=1,
            max_attempts=3,
            enqueue_status="published",
            enqueue_attempts=1,
        )
        session.add(source_job)
        await session.flush()

        adapter_output = AdapterRunOutput(
            project_id=project.id,
            source_file_id=source_file.id,
            extraction_profile_id=profile.id,
            source_job_id=source_job.id,
            adapter_key="test_adapter",
            adapter_version="1",
            input_family="dxf",
            canonical_entity_schema_version="1",
            canonical_json={"entities": []},
            provenance_json={"adapter": "test_adapter"},
            confidence_json={},
            confidence_score=1.0,
            warnings_json=[],
            diagnostics_json=[],
            result_checksum_sha256=uuid4().hex + uuid4().hex,
        )
        session.add(adapter_output)
        await session.flush()

        base_revision = DrawingRevision(
            project_id=project.id,
            source_file_id=source_file.id,
            extraction_profile_id=profile.id,
            source_job_id=source_job.id,
            adapter_run_output_id=adapter_output.id,
            revision_sequence=1,
            revision_kind="ingest",
            review_state="approved",
            canonical_entity_schema_version="1",
            confidence_score=1.0,
        )
        session.add(base_revision)
        await session.flush()

        layout = RevisionLayout(
            project_id=project.id,
            source_file_id=source_file.id,
            extraction_profile_id=profile.id,
            source_job_id=source_job.id,
            drawing_revision_id=base_revision.id,
            adapter_run_output_id=adapter_output.id,
            canonical_entity_schema_version="1",
            sequence_index=0,
            layout_ref="Model",
            payload_json={"name": "Model"},
        )
        layer = RevisionLayer(
            project_id=project.id,
            source_file_id=source_file.id,
            extraction_profile_id=profile.id,
            source_job_id=source_job.id,
            drawing_revision_id=base_revision.id,
            adapter_run_output_id=adapter_output.id,
            canonical_entity_schema_version="1",
            sequence_index=0,
            layer_ref="A-WALL",
            payload_json={"name": "A-WALL"},
        )
        session.add_all([layout, layer])
        await session.flush()

        entity = RevisionEntity(
            project_id=project.id,
            source_file_id=source_file.id,
            extraction_profile_id=profile.id,
            source_job_id=source_job.id,
            drawing_revision_id=base_revision.id,
            adapter_run_output_id=adapter_output.id,
            canonical_entity_schema_version="1",
            sequence_index=0,
            entity_id="line-1",
            entity_type="line",
            entity_schema_version="1",
            confidence_score=1.0,
            confidence_json={},
            geometry_json={
                "start": {"x": 0.0, "y": 0.0},
                "end": {"x": 1.0, "y": 0.0},
                "units": {"normalized": "m"},
            },
            properties_json={"label": "Base line"},
            provenance_json={"origin": "source_direct"},
            canonical_entity_json={
                "entity_type": "line",
                "geometry": {
                    "start": {"x": 0.0, "y": 0.0},
                    "end": {"x": 1.0, "y": 0.0},
                    "units": {"normalized": "m"},
                },
                "properties": {"label": "Base line"},
            },
            layout_ref=layout.layout_ref,
            layer_ref=layer.layer_ref,
            source_identity="line:1",
            source_hash=_SOURCE_HASH,
            layout_id=layout.id,
            layer_id=layer.id,
        )
        manifest = RevisionEntityManifest(
            project_id=project.id,
            source_file_id=source_file.id,
            extraction_profile_id=profile.id,
            source_job_id=source_job.id,
            drawing_revision_id=base_revision.id,
            adapter_run_output_id=adapter_output.id,
            canonical_entity_schema_version="1",
            counts_json={"layouts": 1, "layers": 1, "blocks": 0, "entities": 1},
        )
        validation_report = ValidationReport(
            project_id=project.id,
            drawing_revision_id=base_revision.id,
            source_job_id=source_job.id,
            validation_report_schema_version="0.1",
            canonical_entity_schema_version="1",
            validation_status="valid",
            review_state="approved",
            quantity_gate="allowed",
            effective_confidence=1.0,
            validator_name="test_validation_service",
            validator_version="1",
            report_json={"summary": {"validation_status": "valid"}},
            generated_at=datetime.now(UTC),
        )
        session.add_all([entity, manifest, validation_report])
        await session.commit()

        return _SeededChangesetSmokeLineage(
            project_id=project.id,
            file_id=source_file.id,
            extraction_profile_id=profile.id,
            base_revision_id=base_revision.id,
            source_job_id=source_job.id,
            revision_entity_id=entity.id,
        )


def _changeset_route(lineage: _SeededChangesetSmokeLineage) -> str:
    return f"/v1/revisions/{lineage.base_revision_id}/changesets"


def _changeset_action_route(
    lineage: _SeededChangesetSmokeLineage,
    change_set_id: UUID,
    action: str,
) -> str:
    return f"{_changeset_route(lineage)}/{change_set_id}/{action}"


def _valid_changeset_body(lineage: _SeededChangesetSmokeLineage) -> dict[str, object]:
    return {
        "created_by": "changeset-to-dxf-smoke",
        "operations": [
            {
                "payload_version": 1,
                "sequence_index": 1,
                "operation_type": "update_property",
                "target": {"revision_entity_id": str(lineage.revision_entity_id)},
                "expected_source_identity": "line:1",
                "expected_source_hash": _SOURCE_HASH,
                "payload": {"path": "properties.label", "value": "Revised line"},
                "reason": "smoke revised DXF export",
                "provenance": {"origin": "test"},
            }
        ],
    }


async def _mark_change_set_approved(change_set_id: UUID) -> None:
    assert session_module.AsyncSessionLocal is not None

    async with session_module.AsyncSessionLocal() as session:
        change_set = await session.get(CadChangeSet, change_set_id)
        assert change_set is not None
        change_set.status = "approved"
        await session.commit()


async def _latest_changeset_revision(change_set_id: UUID) -> DrawingRevision:
    assert session_module.AsyncSessionLocal is not None

    async with session_module.AsyncSessionLocal() as session:
        revision = await session.scalar(
            select(DrawingRevision).where(DrawingRevision.changeset_id == change_set_id)
        )

    assert revision is not None
    return revision


async def _artifact_count_for_change_set(change_set_id: UUID) -> int:
    assert session_module.AsyncSessionLocal is not None

    async with session_module.AsyncSessionLocal() as session:
        count = await session.scalar(
            select(func.count())
            .select_from(GeneratedArtifact)
            .where(GeneratedArtifact.changeset_id == change_set_id)
        )

    assert count is not None
    return count


async def _generated_artifacts_for_job(job_id: UUID) -> list[GeneratedArtifact]:
    assert session_module.AsyncSessionLocal is not None

    async with session_module.AsyncSessionLocal() as session:
        return list(
            await session.scalars(
                select(GeneratedArtifact).where(GeneratedArtifact.job_id == job_id)
            )
        )


async def test_changeset_apply_to_revised_dxf_export_smoke(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _truncate_and_reset_database_pool()
    lineage = await _seed_changeset_smoke_lineage()
    await _reset_database_pool()
    published_apply_job_ids: list[UUID] = []
    published_export_job_ids: list[UUID] = []

    async def _capture_changeset_validation(
        db: AsyncSession,
        change_set_id: UUID,
    ) -> CadChangeSetValidationResult:
        validation_result = CadChangeSetValidationResult(
            project_id=lineage.project_id,
            change_set_id=change_set_id,
            validation_status="valid",
            validator_name="changeset-to-dxf-smoke",
            validator_version="1",
            result_json={"status": "valid"},
        )
        db.add(validation_result)
        await db.flush()
        return validation_result

    async def _capture_apply_enqueue(job_id: UUID, **kwargs: Any) -> bool:
        assert kwargs["publisher"] is changeset_routes_module._enqueue_changeset_apply_job
        assert kwargs["suppress_exceptions"] is True
        published_apply_job_ids.append(job_id)
        return True

    async def _capture_export_enqueue(job_id: UUID, **kwargs: Any) -> bool:
        assert kwargs["publisher"] is exports_routes_module._enqueue_export_job
        assert kwargs["suppress_exceptions"] is True
        published_export_job_ids.append(job_id)
        return True

    monkeypatch.setattr(
        changeset_routes_module,
        "_validate_change_set",
        _capture_changeset_validation,
    )
    monkeypatch.setattr(
        changeset_routes_module,
        "_publish_job_enqueue_intent",
        _capture_apply_enqueue,
    )
    monkeypatch.setattr(
        exports_routes_module,
        "_publish_job_enqueue_intent",
        _capture_export_enqueue,
    )

    create_response = await async_client.post(
        _changeset_route(lineage),
        json=_valid_changeset_body(lineage),
    )
    assert create_response.status_code == 201
    change_set_id = UUID(create_response.json()["id"])

    validate_response = await async_client.post(
        _changeset_action_route(lineage, change_set_id, "validate")
    )
    assert validate_response.status_code == 200
    assert validate_response.json()["validation_result"]["validation_status"] == "valid"
    await _mark_change_set_approved(change_set_id)
    await _reset_database_pool()

    apply_response = await async_client.post(
        _changeset_action_route(lineage, change_set_id, "apply")
    )
    assert apply_response.status_code == 202
    apply_job_id = UUID(apply_response.json()["id"])
    assert apply_response.json()["job_type"] == JobType.CHANGESET_APPLY.value
    assert apply_response.json()["status"] == "pending"
    assert published_apply_job_ids == [apply_job_id]

    await worker_module.process_changeset_apply_job(apply_job_id)
    changeset_revision = await _latest_changeset_revision(change_set_id)
    assert changeset_revision.revision_kind == "changeset"
    assert changeset_revision.predecessor_revision_id == lineage.base_revision_id
    assert changeset_revision.changeset_id == change_set_id
    await _reset_database_pool()

    export_response = await async_client.post(
        f"/v1/revisions/{changeset_revision.id}/exports/revised-dxf",
        json={"options": {"target_version": "R2010"}},
    )
    assert export_response.status_code == 202
    export_job_id = UUID(export_response.json()["id"])
    assert export_response.json()["job_type"] == JobType.EXPORT.value
    assert export_response.json()["status"] == "pending"
    assert published_export_job_ids == [export_job_id]

    await worker_module.process_export_job(export_job_id)
    artifacts = await _generated_artifacts_for_job(export_job_id)
    assert len(artifacts) == 1
    artifact = artifacts[0]
    assert artifact.artifact_kind == ExportKind.REVISED_DXF.value
    assert artifact.format == "dxf"
    assert artifact.media_type == "application/dxf"
    assert artifact.changeset_id == change_set_id
    assert artifact.drawing_revision_id == changeset_revision.id
    assert artifact.size_bytes > 0
    storage_path = Path(artifact.storage_uri.removeprefix("file://"))
    content_bytes = storage_path.read_bytes()
    assert content_bytes.startswith(b"  0\nSECTION")
    assert b"EOF" in content_bytes
    assert hashlib.sha256(content_bytes).hexdigest() == artifact.checksum_sha256


async def test_changeset_apply_stale_base_conflict_leaves_no_revised_dxf_artifact(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _truncate_and_reset_database_pool()
    lineage = await _seed_changeset_smoke_lineage()
    await _reset_database_pool()

    async def _capture_changeset_validation(
        db: AsyncSession,
        change_set_id: UUID,
    ) -> CadChangeSetValidationResult:
        validation_result = CadChangeSetValidationResult(
            project_id=lineage.project_id,
            change_set_id=change_set_id,
            validation_status="valid",
            validator_name="changeset-to-dxf-smoke",
            validator_version="1",
            result_json={"status": "valid"},
        )
        db.add(validation_result)
        await db.flush()
        return validation_result

    monkeypatch.setattr(
        changeset_routes_module,
        "_validate_change_set",
        _capture_changeset_validation,
    )

    create_response = await async_client.post(
        _changeset_route(lineage),
        json=_valid_changeset_body(lineage),
    )
    assert create_response.status_code == 201
    change_set_id = UUID(create_response.json()["id"])
    validate_response = await async_client.post(
        _changeset_action_route(lineage, change_set_id, "validate")
    )
    assert validate_response.status_code == 200
    await _mark_change_set_approved(change_set_id)

    assert session_module.AsyncSessionLocal is not None
    async with session_module.AsyncSessionLocal() as session:
        reprocess_job = Job(
            project_id=lineage.project_id,
            file_id=lineage.file_id,
            extraction_profile_id=lineage.extraction_profile_id,
            base_revision_id=lineage.base_revision_id,
            job_type=JobType.REPROCESS.value,
            status="succeeded",
            attempts=1,
            max_attempts=3,
            enqueue_status="published",
            enqueue_attempts=1,
        )
        session.add(reprocess_job)
        await session.flush()

        adapter_output = AdapterRunOutput(
            project_id=lineage.project_id,
            source_file_id=lineage.file_id,
            extraction_profile_id=lineage.extraction_profile_id,
            source_job_id=reprocess_job.id,
            adapter_key="test_adapter",
            adapter_version="1",
            input_family="dxf",
            canonical_entity_schema_version="1",
            canonical_json={"entities": []},
            provenance_json={"adapter": "test_adapter"},
            confidence_json={},
            confidence_score=1.0,
            warnings_json=[],
            diagnostics_json=[],
            result_checksum_sha256=uuid4().hex + uuid4().hex,
        )
        session.add(adapter_output)
        await session.flush()

        stale_revision = DrawingRevision(
            project_id=lineage.project_id,
            source_file_id=lineage.file_id,
            extraction_profile_id=lineage.extraction_profile_id,
            source_job_id=reprocess_job.id,
            adapter_run_output_id=adapter_output.id,
            predecessor_revision_id=lineage.base_revision_id,
            revision_sequence=2,
            revision_kind="reprocess",
            review_state="approved",
            canonical_entity_schema_version="1",
            confidence_score=1.0,
        )
        session.add(stale_revision)
        await session.commit()
    await _reset_database_pool()

    apply_response = await async_client.post(
        _changeset_action_route(lineage, change_set_id, "apply"),
        headers={"Idempotency-Key": f"stale-base-{uuid4().hex}"},
    )

    assert apply_response.status_code == 409
    assert apply_response.json()["error"]["code"] == "REVISION_CONFLICT"
    assert await _artifact_count_for_change_set(change_set_id) == 0
