"""Integration tests for persisted ingest output rows."""

import asyncio
import uuid
from contextlib import suppress
from copy import deepcopy
from dataclasses import dataclass, replace
from typing import Any, cast

import httpx
import pytest
from sqlalchemy import select

import app.db.session as session_module
import app.jobs.worker as worker_module
from app.core.errors import ErrorCode
from app.ingestion.finalization import IngestFinalizationPayload, compute_adapter_result_checksum
from app.ingestion.runner import IngestionRunRequest
from app.jobs.worker import process_ingest_job
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.file import File as FileModel
from app.models.generated_artifact import GeneratedArtifact
from app.models.job import Job
from app.models.job_event import JobEvent
from app.models.project import Project
from app.models.revision_materialization import (
    RevisionBlock,
    RevisionEntity,
    RevisionEntityManifest,
    RevisionLayer,
    RevisionLayout,
)
from app.models.validation_report import ValidationReport
from app.storage.keys import build_generated_artifact_storage_key
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
    _build_fake_ingest_payload,
    _create_project,
    _get_job,
    _get_job_for_file,
    _update_job,
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
        return _replace_fake_canonical_payload(_build_fake_ingest_payload(request))

    monkeypatch.setattr(worker_module, "run_ingestion", _fake_run_ingestion)
    return recorded_requests


def _as_uuid(value: str | uuid.UUID) -> uuid.UUID:
    """Normalize string UUID inputs for direct model comparisons."""
    if isinstance(value, uuid.UUID):
        return value

    return uuid.UUID(value)


def _build_contract_entity(
    *,
    entity_id: str,
    entity_type: str,
    layer_ref: str,
    layout_ref: str = "Model",
    block_ref: str | None = None,
    parent_entity_ref: str | None = None,
    source_id: str,
    source_hash: str | None = None,
    confidence_score: float = _FAKE_RUNNER_CONFIDENCE_SCORE,
) -> dict[str, Any]:
    """Build a canonical contract-shaped entity payload for materialization tests."""
    provenance_json: dict[str, Any] = {
        "origin": "adapter_normalized",
        "adapter": {},
        "source_ref": None,
        "source_identity": source_id,
        "source_hash": source_hash,
        "extraction_path": [],
        "notes": [],
    }

    entity: dict[str, Any] = {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "layout_ref": layout_ref,
        "layer_ref": layer_ref,
        "confidence_score": confidence_score,
        "confidence_json": {"score": confidence_score, "basis": "adapter"},
        "geometry_json": {"type": entity_type, "coordinates": [[0.0, 0.0], [1.0, 1.0]]},
        "properties_json": {"layer": layer_ref},
        "provenance_json": provenance_json,
    }
    if block_ref is not None:
        entity["block_ref"] = block_ref
    if parent_entity_ref is not None:
        entity["parent_entity_ref"] = parent_entity_ref

    return entity


def _replace_fake_canonical_payload(
    payload: IngestFinalizationPayload,
    *,
    layouts: list[dict[str, Any]] | None = None,
    layers: list[dict[str, Any]] | None = None,
    blocks: list[dict[str, Any]] | None = None,
    entities: list[dict[str, Any]] | None = None,
) -> IngestFinalizationPayload:
    """Replace fake runner canonical payload sections and refresh derived checksum/report counts."""
    next_layouts = deepcopy(
        layouts if layouts is not None else [{"layout_ref": "Model", "name": "Model"}]
    )
    next_layers = deepcopy(
        layers if layers is not None else [{"layer_ref": "A-WALL", "name": "A-WALL"}]
    )
    next_blocks = deepcopy(blocks if blocks is not None else [])
    next_entities = deepcopy(
        entities
        if entities is not None
        else [
            _build_contract_entity(
                entity_id="entity-001",
                entity_type="line",
                layer_ref="A-WALL",
                source_id="entity-source-001",
            )
        ]
    )
    entity_counts = {
        "layouts": len(next_layouts),
        "layers": len(next_layers),
        "blocks": len(next_blocks),
        "entities": len(next_entities),
    }
    canonical_json = {
        **payload.canonical_json,
        "canonical_entity_schema_version": payload.canonical_entity_schema_version,
        "schema_version": payload.canonical_entity_schema_version,
        "layouts": next_layouts,
        "layers": next_layers,
        "blocks": next_blocks,
        "entities": next_entities,
        "entity_counts": entity_counts,
    }
    report_json = {
        **payload.report_json,
        "summary": {
            **payload.report_json["summary"],
            "entity_counts": entity_counts,
        },
    }
    result_envelope = {
        "adapter_key": payload.adapter_key,
        "adapter_version": payload.adapter_version,
        "input_family": payload.input_family,
        "canonical_entity_schema_version": payload.canonical_entity_schema_version,
        "canonical_json": canonical_json,
        "provenance_json": payload.provenance_json,
        "confidence_json": payload.confidence_json,
        "confidence_score": payload.confidence_score,
        "warnings_json": payload.warnings_json,
        "diagnostics_json": payload.diagnostics_json,
    }
    return replace(
        payload,
        canonical_json=canonical_json,
        report_json=report_json,
        result_checksum_sha256=compute_adapter_result_checksum(result_envelope),
    )


def _assert_validation_report_json_matches_columns(report: ValidationReport) -> None:
    """Assert canonical report JSON matches authoritative validation columns."""
    report_json = report.report_json

    assert report_json["validation_report_id"] == str(report.id)
    assert report_json["drawing_revision_id"] == str(report.drawing_revision_id)
    assert report_json["source_job_id"] == str(report.source_job_id)
    assert (
        report_json["validation_report_schema_version"] == report.validation_report_schema_version
    )
    assert report_json["canonical_entity_schema_version"] == report.canonical_entity_schema_version
    assert report_json["validation_status"] == report.validation_status
    assert report_json["review_state"] == report.review_state
    assert report_json["quantity_gate"] == report.quantity_gate
    assert report_json["effective_confidence"] == report.effective_confidence
    assert report_json["validator"] == {
        "name": report.validator_name,
        "version": report.validator_version,
    }
    assert report_json["confidence"]["effective_confidence"] == report.effective_confidence
    assert report_json["confidence"]["review_state"] == report.review_state
    assert report_json["confidence"]["review_required"] == (
        report.review_state == "review_required"
    )
    assert report_json["generated_at"] == report.generated_at.isoformat()
    assert report_json["summary"]["validation_status"] == report.validation_status
    assert report_json["summary"]["review_state"] == report.review_state
    assert report_json["summary"]["quantity_gate"] == report.quantity_gate
    assert report_json["summary"]["effective_confidence"] == report.effective_confidence
    assert report_json["checks"]


def _assert_debug_overlay_artifact(
    artifact: GeneratedArtifact,
    *,
    job: Job,
    drawing_revision: DrawingRevision,
    adapter_output: AdapterRunOutput,
    predecessor_artifact_id: uuid.UUID | None,
) -> None:
    """Assert persisted SVG debug overlay artifact metadata and lineage."""
    assert artifact.project_id == job.project_id
    assert artifact.source_file_id == job.file_id
    assert artifact.job_id == job.id
    assert artifact.drawing_revision_id == drawing_revision.id
    assert artifact.adapter_run_output_id == adapter_output.id
    assert artifact.artifact_kind == "debug_overlay"
    assert artifact.name == "debug-overlay.svg"
    assert artifact.format == "svg"
    assert artifact.media_type == "image/svg+xml"
    assert artifact.size_bytes > 0
    assert len(artifact.checksum_sha256) == 64
    assert artifact.generator_name == "app.ingestion.debug_overlay"
    assert artifact.generator_version == "1"
    assert artifact.generator_config_json == {
        "title": f"plan.pdf revision {drawing_revision.revision_sequence}",
        "source_label": "plan.pdf",
        "review_state": drawing_revision.review_state,
        "confidence_score": drawing_revision.confidence_score,
    }
    assert artifact.storage_key == build_generated_artifact_storage_key(artifact.id, artifact.name)
    assert artifact.storage_uri
    assert artifact.predecessor_artifact_id == predecessor_artifact_id

    lineage = cast(dict[str, Any], artifact.lineage_json)
    source_file_lineage = cast(dict[str, Any], lineage["source_file"])
    assert source_file_lineage["id"] == str(job.file_id)
    assert source_file_lineage["original_filename"] == "plan.pdf"
    assert source_file_lineage["detected_format"] == "pdf"
    assert source_file_lineage["media_type"] == "application/pdf"
    assert source_file_lineage["checksum_sha256"]
    assert lineage["job"] == {
        "id": str(job.id),
        "extraction_profile_id": str(job.extraction_profile_id),
        "attempts": job.attempts,
    }
    assert lineage["drawing_revision"] == {
        "id": str(drawing_revision.id),
        "revision_sequence": drawing_revision.revision_sequence,
        "revision_kind": drawing_revision.revision_kind,
        "predecessor_revision_id": (
            str(drawing_revision.predecessor_revision_id)
            if drawing_revision.predecessor_revision_id is not None
            else None
        ),
    }
    assert lineage["adapter"] == {
        "id": str(adapter_output.id),
        "key": adapter_output.adapter_key,
        "version": adapter_output.adapter_version,
        "input_family": adapter_output.input_family,
        "result_checksum_sha256": adapter_output.result_checksum_sha256,
    }
    assert lineage["entities"] == {
        "schema_version": adapter_output.canonical_entity_schema_version,
        "counts": adapter_output.canonical_json["entity_counts"],
        "total": len(adapter_output.canonical_json["entities"]),
    }
    assert lineage["options"] == {}


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


async def _load_project_materialization(
    project_id: str | uuid.UUID,
) -> tuple[
    list[RevisionEntityManifest],
    list[RevisionLayout],
    list[RevisionLayer],
    list[RevisionBlock],
    list[RevisionEntity],
]:
    """Load revision-scoped normalized materialization rows for one project."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    normalized_project_id = _as_uuid(project_id)

    async with session_maker() as session:
        manifests = list(
            (
                await session.execute(
                    select(RevisionEntityManifest)
                    .where(RevisionEntityManifest.project_id == normalized_project_id)
                    .order_by(RevisionEntityManifest.created_at, RevisionEntityManifest.id)
                )
            ).scalars()
        )
        layouts = list(
            (
                await session.execute(
                    select(RevisionLayout)
                    .where(RevisionLayout.project_id == normalized_project_id)
                    .order_by(RevisionLayout.drawing_revision_id, RevisionLayout.sequence_index)
                )
            ).scalars()
        )
        layers = list(
            (
                await session.execute(
                    select(RevisionLayer)
                    .where(RevisionLayer.project_id == normalized_project_id)
                    .order_by(RevisionLayer.drawing_revision_id, RevisionLayer.sequence_index)
                )
            ).scalars()
        )
        blocks = list(
            (
                await session.execute(
                    select(RevisionBlock)
                    .where(RevisionBlock.project_id == normalized_project_id)
                    .order_by(RevisionBlock.drawing_revision_id, RevisionBlock.sequence_index)
                )
            ).scalars()
        )
        entities = list(
            (
                await session.execute(
                    select(RevisionEntity)
                    .where(RevisionEntity.project_id == normalized_project_id)
                    .order_by(RevisionEntity.drawing_revision_id, RevisionEntity.sequence_index)
                )
            ).scalars()
        )

    return manifests, layouts, layers, blocks, entities


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
        job = await _get_job(job.id)

        (
            adapter_outputs,
            drawing_revisions,
            validation_reports,
            generated_artifacts,
        ) = await _load_project_outputs(project["id"])

        assert len(adapter_outputs) == 1
        assert len(drawing_revisions) == 1
        assert len(validation_reports) == 1
        assert len(generated_artifacts) == 1

        adapter_output = adapter_outputs[0]
        drawing_revision = drawing_revisions[0]
        validation_report = validation_reports[0]
        generated_artifact = generated_artifacts[0]
        manifests, layouts, layers, blocks, entities = await _load_project_materialization(
            project["id"]
        )
        manifest = manifests[0]
        layout = layouts[0]
        layer = layers[0]
        entity = entities[0]

        assert adapter_output.project_id == job.project_id
        assert adapter_output.source_file_id == job.file_id
        assert adapter_output.source_job_id == job.id
        assert adapter_output.extraction_profile_id == job.extraction_profile_id
        assert adapter_output.adapter_key == _FAKE_RUNNER_ADAPTER_KEY
        assert adapter_output.adapter_version == _FAKE_RUNNER_ADAPTER_VERSION
        assert adapter_output.input_family == "pdf_vector"
        assert (
            adapter_output.canonical_entity_schema_version == _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION
        )
        assert adapter_output.confidence_score == _FAKE_RUNNER_CONFIDENCE_SCORE
        assert adapter_output.canonical_json == {
            "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            "schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            "layouts": [{"layout_ref": "Model", "name": "Model"}],
            "layers": [{"layer_ref": "A-WALL", "name": "A-WALL"}],
            "blocks": [],
            "entities": [
                {
                    "entity_id": "entity-001",
                    "entity_type": "line",
                    "entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                    "layout_ref": "Model",
                    "layer_ref": "A-WALL",
                    "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                    "confidence_json": {
                        "score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                        "basis": "adapter",
                    },
                    "geometry_json": {
                        "type": "line",
                        "coordinates": [[0.0, 0.0], [1.0, 1.0]],
                    },
                    "properties_json": {"layer": "A-WALL"},
                    "provenance_json": {
                        "origin": "adapter_normalized",
                        "adapter": {},
                        "source_ref": None,
                        "source_identity": "entity-source-001",
                        "source_hash": None,
                        "extraction_path": [],
                        "notes": [],
                    },
                }
            ],
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
        _assert_validation_report_json_matches_columns(validation_report)
        assert validation_report.report_json["summary"]["entity_counts"] == {
            "layouts": 1,
            "layers": 1,
            "blocks": 0,
            "entities": 1,
        }
        assert validation_report.report_json["findings"] == []
        assert validation_report.report_json["adapter_warnings"] == []
        assert validation_report.report_json["provenance"] == adapter_output.provenance_json
        _assert_debug_overlay_artifact(
            generated_artifact,
            job=job,
            drawing_revision=drawing_revision,
            adapter_output=adapter_output,
            predecessor_artifact_id=None,
        )
        assert len(manifests) == 1
        assert len(layouts) == 1
        assert len(layers) == 1
        assert blocks == []
        assert len(entities) == 1

        assert manifest.project_id == job.project_id
        assert manifest.source_file_id == job.file_id
        assert manifest.extraction_profile_id == job.extraction_profile_id
        assert manifest.source_job_id == job.id
        assert manifest.drawing_revision_id == drawing_revision.id
        assert manifest.adapter_run_output_id == adapter_output.id
        assert manifest.canonical_entity_schema_version == _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION
        assert manifest.counts_json == {"layouts": 1, "layers": 1, "blocks": 0, "entities": 1}

        assert layout.project_id == job.project_id
        assert layout.source_file_id == job.file_id
        assert layout.extraction_profile_id == job.extraction_profile_id
        assert layout.source_job_id == job.id
        assert layout.drawing_revision_id == drawing_revision.id
        assert layout.adapter_run_output_id == adapter_output.id
        assert layout.canonical_entity_schema_version == _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION
        assert layout.sequence_index == 0
        assert layout.layout_ref == "Model"
        assert layout.payload_json == {"layout_ref": "Model", "name": "Model"}

        assert layer.project_id == job.project_id
        assert layer.source_file_id == job.file_id
        assert layer.extraction_profile_id == job.extraction_profile_id
        assert layer.source_job_id == job.id
        assert layer.drawing_revision_id == drawing_revision.id
        assert layer.adapter_run_output_id == adapter_output.id
        assert layer.canonical_entity_schema_version == _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION
        assert layer.sequence_index == 0
        assert layer.layer_ref == "A-WALL"
        assert layer.payload_json == {"layer_ref": "A-WALL", "name": "A-WALL"}

        assert entity.project_id == job.project_id
        assert entity.source_file_id == job.file_id
        assert entity.extraction_profile_id == job.extraction_profile_id
        assert entity.source_job_id == job.id
        assert entity.drawing_revision_id == drawing_revision.id
        assert entity.adapter_run_output_id == adapter_output.id
        assert entity.canonical_entity_schema_version == _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION
        assert entity.sequence_index == 0
        assert entity.entity_id == "entity-001"
        assert entity.entity_type == "line"
        assert entity.entity_schema_version == _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION
        assert entity.parent_entity_ref is None
        assert entity.confidence_score == _FAKE_RUNNER_CONFIDENCE_SCORE
        assert entity.confidence_json == {
            "score": _FAKE_RUNNER_CONFIDENCE_SCORE,
            "basis": "adapter",
        }
        assert entity.geometry_json == {
            "type": "line",
            "coordinates": [[0.0, 0.0], [1.0, 1.0]],
        }
        assert entity.properties_json == {"layer": "A-WALL"}
        assert entity.provenance_json == {
            "origin": "adapter_normalized",
            "adapter": {},
            "source_ref": None,
            "source_identity": "entity-source-001",
            "source_hash": None,
            "extraction_path": [],
            "notes": [],
        }
        assert entity.layout_ref == "Model"
        assert entity.layer_ref == "A-WALL"
        assert entity.block_ref is None
        assert entity.source_identity == "entity-source-001"
        assert entity.source_hash is None
        assert entity.layout_id == layout.id
        assert entity.layer_id == layer.id
        assert entity.block_id is None
        assert entity.parent_entity_row_id is None
        assert entity.canonical_entity_json == {
            "entity_id": "entity-001",
            "entity_type": "line",
            "entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            "layout_ref": "Model",
            "layer_ref": "A-WALL",
            "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
            "confidence_json": {
                "score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                "basis": "adapter",
            },
            "geometry_json": {
                "type": "line",
                "coordinates": [[0.0, 0.0], [1.0, 1.0]],
            },
            "properties_json": {"layer": "A-WALL"},
            "provenance_json": {
                "origin": "adapter_normalized",
                "adapter": {},
                "source_ref": None,
                "source_identity": "entity-source-001",
                "source_hash": None,
                "extraction_path": [],
                "notes": [],
            },
        }

    async def test_revision_materialization_canonicalizes_legacy_entity_provenance(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Legacy entity provenance payloads should persist in canonical contract shape."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _run_legacy_provenance_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            return _replace_fake_canonical_payload(
                payload,
                entities=[
                    {
                        "entity_id": "entity-dxf-legacy",
                        "entity_type": "line",
                        "entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                        "layout_ref": "Model",
                        "layer_ref": "A-WALL",
                        "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                        "confidence_json": {
                            "score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                            "basis": "adapter",
                        },
                        "geometry_json": {
                            "type": "line",
                            "coordinates": [[0.0, 0.0], [1.0, 1.0]],
                        },
                        "properties_json": {"layer": "A-WALL"},
                        "provenance_json": {
                            "source_handle": "A1",
                            "source_entity_ref": "doc:entities/A1",
                            "normalized_source_hash": "b" * 64,
                            "adapter": {"family": "dxf"},
                        },
                    },
                    {
                        "entity_id": "entity-ifc-legacy",
                        "entity_type": "polyline",
                        "entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                        "layout_ref": "Model",
                        "layer_ref": "A-WALL",
                        "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                        "confidence_json": {
                            "score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                            "basis": "adapter",
                        },
                        "geometry_json": {
                            "type": "polyline",
                            "coordinates": [[0.0, 0.0], [1.0, 1.0]],
                        },
                        "properties_json": {"layer": "A-WALL"},
                        "provenance": {
                            "origin": "source_direct",
                            "source_identity": "ifc-guid-001",
                            "normalized_source_hash": "c" * 64,
                            "extraction_path": ("IfcProject", "IfcWall", "Body"),
                            "notes": ("ifc", "semantic"),
                        },
                    },
                ],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_legacy_provenance_ingestion)

        await process_ingest_job(job.id)

        _, _, _, _, entities = await _load_project_materialization(project["id"])

        legacy_dxf_entity, legacy_ifc_entity = entities
        assert legacy_dxf_entity.provenance_json == {
            "origin": "adapter_normalized",
            "adapter": {"family": "dxf"},
            "source_ref": "doc:entities/A1",
            "source_identity": "A1",
            "source_hash": "b" * 64,
            "extraction_path": [],
            "notes": [],
        }
        assert legacy_dxf_entity.source_identity == "A1"
        assert legacy_dxf_entity.source_hash == "b" * 64
        assert legacy_dxf_entity.canonical_entity_json is not None
        assert legacy_dxf_entity.canonical_entity_json["provenance_json"] == {
            "source_handle": "A1",
            "source_entity_ref": "doc:entities/A1",
            "normalized_source_hash": "b" * 64,
            "adapter": {"family": "dxf"},
        }

        assert legacy_ifc_entity.provenance_json == {
            "origin": "source_direct",
            "adapter": {},
            "source_ref": None,
            "source_identity": "ifc-guid-001",
            "source_hash": "c" * 64,
            "extraction_path": ["IfcProject", "IfcWall", "Body"],
            "notes": ["ifc", "semantic"],
        }
        assert legacy_ifc_entity.source_identity == "ifc-guid-001"
        assert legacy_ifc_entity.source_hash == "c" * 64
        assert legacy_ifc_entity.canonical_entity_json is not None
        assert legacy_ifc_entity.canonical_entity_json["provenance"] == {
            "origin": "source_direct",
            "source_identity": "ifc-guid-001",
            "normalized_source_hash": "c" * 64,
            "extraction_path": ["IfcProject", "IfcWall", "Body"],
            "notes": ["ifc", "semantic"],
        }

    async def test_revision_materialization_rejects_invalid_nonempty_entity_origin(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Invalid non-empty provenance origins should fail before persisting outputs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _run_invalid_origin_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            return _replace_fake_canonical_payload(
                payload,
                entities=[
                    {
                        "entity_id": "entity-invalid-origin",
                        "entity_type": "line",
                        "entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                        "layout_ref": "Model",
                        "layer_ref": "A-WALL",
                        "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                        "confidence_json": {
                            "score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                            "basis": "adapter",
                        },
                        "geometry_json": {
                            "type": "line",
                            "coordinates": [[0.0, 0.0], [1.0, 1.0]],
                        },
                        "properties_json": {"layer": "A-WALL"},
                        "provenance_json": {
                            "origin": "legacy_adapter",
                            "source_id": "entity-source-001",
                        },
                    }
                ],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_invalid_origin_ingestion)

        with pytest.raises(ValueError, match="Invalid entity provenance origin 'legacy_adapter'"):
            await process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.INTERNAL_ERROR.value
        assert updated_job.error_message == worker_module._FINALIZE_INGEST_JOB_ERROR_MESSAGE

        (
            adapter_outputs,
            drawing_revisions,
            validation_reports,
            generated_artifacts,
        ) = await _load_project_outputs(project["id"])
        assert adapter_outputs == []
        assert drawing_revisions == []
        assert validation_reports == []
        assert generated_artifacts == []

        manifests, layouts, layers, blocks, entities = await _load_project_materialization(
            project["id"]
        )
        assert manifests == []
        assert layouts == []
        assert layers == []
        assert blocks == []
        assert entities == []

    async def test_revision_materialization_wraps_scalar_adapter_provenance(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Scalar legacy adapter provenance should be preserved in object form."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _run_scalar_adapter_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            return _replace_fake_canonical_payload(
                payload,
                entities=[
                    {
                        "entity_id": "entity-dwg-scalar-adapter",
                        "entity_type": "line",
                        "entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                        "layout_ref": "Model",
                        "layer_ref": "A-WALL",
                        "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                        "confidence_json": {
                            "score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                            "basis": "adapter",
                        },
                        "geometry_json": {
                            "type": "line",
                            "coordinates": [[0.0, 0.0], [1.0, 1.0]],
                        },
                        "properties_json": {"layer": "A-WALL"},
                        "provenance_json": {
                            "source_handle": "A1",
                            "source_entity_ref": "doc:entities/A1",
                            "normalized_source_hash": "b" * 64,
                            "adapter": "libredwg",
                        },
                    }
                ],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_scalar_adapter_ingestion)

        await process_ingest_job(job.id)

        _, _, _, _, entities = await _load_project_materialization(project["id"])

        assert len(entities) == 1
        entity = entities[0]
        assert entity.provenance_json == {
            "origin": "adapter_normalized",
            "adapter": {"value": "libredwg"},
            "source_ref": "doc:entities/A1",
            "source_identity": "A1",
            "source_hash": "b" * 64,
            "extraction_path": [],
            "notes": [],
        }
        assert entity.canonical_entity_json is not None
        assert entity.canonical_entity_json["provenance_json"] == {
            "source_handle": "A1",
            "source_entity_ref": "doc:entities/A1",
            "normalized_source_hash": "b" * 64,
            "adapter": "libredwg",
        }

    async def test_revision_materialization_resolves_entity_relationship_columns(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Materialization should resolve best-effort layout/layer/block/parent foreign keys."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _run_relationship_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            return _replace_fake_canonical_payload(
                payload,
                blocks=[{"block_ref": "DOOR-1", "name": "DOOR-1"}],
                entities=[
                    _build_contract_entity(
                        entity_id="entity-parent",
                        entity_type="insert",
                        layer_ref="A-WALL",
                        block_ref="DOOR-1",
                        source_id="entity-source-parent",
                    ),
                    _build_contract_entity(
                        entity_id="entity-child",
                        entity_type="line",
                        layer_ref="A-WALL",
                        parent_entity_ref="entity-parent",
                        source_id="entity-source-child",
                        source_hash="a" * 64,
                    ),
                ],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_relationship_ingestion)

        await process_ingest_job(job.id)

        manifests, layouts, layers, blocks, entities = await _load_project_materialization(
            project["id"]
        )

        assert manifests[0].counts_json == {"layouts": 1, "layers": 1, "blocks": 1, "entities": 2}
        assert len(layouts) == 1
        assert len(layers) == 1
        assert len(blocks) == 1
        assert len(entities) == 2

        parent_entity, child_entity = entities
        layout = layouts[0]
        layer = layers[0]
        block = blocks[0]

        assert parent_entity.entity_id == "entity-parent"
        assert parent_entity.block_ref == "DOOR-1"
        assert parent_entity.layout_id == layout.id
        assert parent_entity.layer_id == layer.id
        assert parent_entity.block_id == block.id
        assert parent_entity.parent_entity_row_id is None

        assert child_entity.entity_id == "entity-child"
        assert child_entity.parent_entity_ref == "entity-parent"
        assert child_entity.layout_id == layout.id
        assert child_entity.layer_id == layer.id
        assert child_entity.block_id is None
        assert child_entity.parent_entity_row_id == parent_entity.id
        assert child_entity.source_hash == "a" * 64

    async def test_revision_materialization_accepts_numeric_legacy_confidence(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """DXF-style numeric confidence should materialize as the entity score and JSON."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _run_numeric_confidence_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            entity = _build_contract_entity(
                entity_id="entity-dxf-001",
                entity_type="line",
                layer_ref="A-WALL",
                source_id="entity-source-dxf-001",
            )
            entity.pop("confidence_score")
            entity.pop("confidence_json")
            entity["confidence"] = 0.0
            return _replace_fake_canonical_payload(payload, entities=[entity])

        monkeypatch.setattr(worker_module, "run_ingestion", _run_numeric_confidence_ingestion)

        await process_ingest_job(job.id)

        manifests, layouts, layers, blocks, entities = await _load_project_materialization(
            project["id"]
        )

        assert len(manifests) == 1
        assert len(layouts) == 1
        assert len(layers) == 1
        assert blocks == []
        assert len(entities) == 1
        assert entities[0].confidence_score == 0.0
        assert entities[0].confidence_json == {"score": 0.0}
        assert entities[0].canonical_entity_json is not None
        assert entities[0].canonical_entity_json["confidence"] == 0.0

    async def test_revision_materialization_avoids_fallback_ref_collisions(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Fallback refs should suffix collisions instead of reusing existing explicit refs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _run_collision_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            return _replace_fake_canonical_payload(
                payload,
                layouts=[{"layout_ref": "layout-000001", "name": "Primary"}, {}],
                layers=[{"layer_ref": "layer-000001", "name": "Walls"}, {}],
                blocks=[{"block_ref": "block-000001", "name": "Door"}, {}],
                entities=[
                    _build_contract_entity(
                        entity_id="entity-000001",
                        entity_type="insert",
                        layer_ref="layer-000001",
                        layout_ref="layout-000001",
                        block_ref="block-000001",
                        source_id="entity-source-001",
                    ),
                    {
                        "entity_type": "line",
                        "entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                        "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                        "confidence_json": {
                            "score": _FAKE_RUNNER_CONFIDENCE_SCORE,
                            "basis": "adapter",
                        },
                        "geometry_json": {
                            "type": "line",
                            "coordinates": [[0.0, 0.0], [1.0, 1.0]],
                        },
                        "properties_json": {},
                        "provenance_json": {},
                    },
                ],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_collision_ingestion)

        await process_ingest_job(job.id)

        _, layouts, layers, blocks, entities = await _load_project_materialization(project["id"])

        assert [layout.layout_ref for layout in layouts] == ["layout-000001", "layout-000001-1"]
        assert [layer.layer_ref for layer in layers] == ["layer-000001", "layer-000001-1"]
        assert [block.block_ref for block in blocks] == ["block-000001", "block-000001-1"]
        assert [entity.entity_id for entity in entities] == ["entity-000001", "entity-000001-1"]

    async def test_revision_materialization_inserts_parent_before_children_across_chunks(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Entity inserts should satisfy immediate parent self-FKs even across chunk boundaries."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _run_large_hierarchy_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            child_entities = [
                _build_contract_entity(
                    entity_id=f"entity-child-{index:03d}",
                    entity_type="line",
                    layer_ref="A-WALL",
                    parent_entity_ref="entity-parent",
                    source_id=f"entity-source-child-{index:03d}",
                )
                for index in range(501)
            ]
            parent_entity = _build_contract_entity(
                entity_id="entity-parent",
                entity_type="insert",
                layer_ref="A-WALL",
                source_id="entity-source-parent",
            )
            return _replace_fake_canonical_payload(
                payload,
                entities=[*child_entities, parent_entity],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_large_hierarchy_ingestion)

        await process_ingest_job(job.id)

        manifests, layouts, layers, blocks, entities = await _load_project_materialization(
            project["id"]
        )

        assert len(manifests) == 1
        assert len(layouts) == 1
        assert len(layers) == 1
        assert blocks == []
        assert len(entities) == 502

        parent_entity = next(entity for entity in entities if entity.entity_id == "entity-parent")
        child_entities = [entity for entity in entities if entity.entity_id != "entity-parent"]

        assert parent_entity.sequence_index == 501
        assert all(entity.parent_entity_row_id == parent_entity.id for entity in child_entities)

    async def test_revision_materialization_manifest_distinguishes_empty_from_legacy_missing(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Valid empty revisions should persist a zero-count manifest while legacy rows do not."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        empty_project = await _create_project(async_client)
        empty_uploaded = await _upload_file(async_client, empty_project["id"])
        empty_job = await _get_job_for_file(str(empty_uploaded["id"]))

        async def _run_empty_ingestion(request: IngestionRunRequest) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            return _replace_fake_canonical_payload(
                payload,
                layouts=[],
                layers=[],
                blocks=[],
                entities=[],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_empty_ingestion)

        await process_ingest_job(empty_job.id)

        empty_outputs = await _load_project_outputs(empty_project["id"])
        empty_materialization = await _load_project_materialization(empty_project["id"])

        assert len(empty_outputs[1]) == 1
        assert len(empty_materialization[0]) == 1
        assert empty_materialization[0][0].drawing_revision_id == empty_outputs[1][0].id
        assert empty_materialization[0][0].counts_json == {
            "layouts": 0,
            "layers": 0,
            "blocks": 0,
            "entities": 0,
        }
        assert empty_materialization[1] == []
        assert empty_materialization[2] == []
        assert empty_materialization[3] == []
        assert empty_materialization[4] == []

        legacy_project = await _create_project(async_client)
        legacy_uploaded = await _upload_file(async_client, legacy_project["id"])
        legacy_job = await _get_job_for_file(str(legacy_uploaded["id"]))
        legacy_adapter_output_id = uuid.uuid4()
        legacy_revision_id = uuid.uuid4()
        assert legacy_job.extraction_profile_id is not None

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            session.add(
                AdapterRunOutput(
                    id=legacy_adapter_output_id,
                    project_id=legacy_job.project_id,
                    source_file_id=legacy_job.file_id,
                    extraction_profile_id=legacy_job.extraction_profile_id,
                    source_job_id=legacy_job.id,
                    adapter_key=_FAKE_RUNNER_ADAPTER_KEY,
                    adapter_version=_FAKE_RUNNER_ADAPTER_VERSION,
                    input_family="pdf_vector",
                    canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                    canonical_json={
                        "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                        "schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                        "layouts": [],
                        "layers": [],
                        "blocks": [],
                        "entities": [],
                        "entity_counts": {
                            "layouts": 0,
                            "layers": 0,
                            "blocks": 0,
                            "entities": 0,
                        },
                    },
                    provenance_json={
                        "schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                        "adapter": {
                            "key": _FAKE_RUNNER_ADAPTER_KEY,
                            "version": _FAKE_RUNNER_ADAPTER_VERSION,
                        },
                        "source": {
                            "file_id": str(legacy_job.file_id),
                            "job_id": str(legacy_job.id),
                            "extraction_profile_id": str(legacy_job.extraction_profile_id),
                            "input_family": "pdf_vector",
                            "revision_kind": "ingest",
                        },
                        "records": [],
                        "generated_at": "2026-01-02T03:04:05+00:00",
                    },
                    confidence_json={"score": _FAKE_RUNNER_CONFIDENCE_SCORE},
                    confidence_score=_FAKE_RUNNER_CONFIDENCE_SCORE,
                    warnings_json=[],
                    diagnostics_json={"adapter": _FAKE_RUNNER_ADAPTER_KEY, "diagnostics": []},
                    result_checksum_sha256="0" * 64,
                )
            )
            session.add(
                DrawingRevision(
                    id=legacy_revision_id,
                    project_id=legacy_job.project_id,
                    source_file_id=legacy_job.file_id,
                    extraction_profile_id=legacy_job.extraction_profile_id,
                    source_job_id=legacy_job.id,
                    adapter_run_output_id=legacy_adapter_output_id,
                    predecessor_revision_id=None,
                    revision_sequence=1,
                    revision_kind="ingest",
                    review_state=_FAKE_RUNNER_REVIEW_STATE,
                    canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                    confidence_score=_FAKE_RUNNER_CONFIDENCE_SCORE,
                )
            )
            await session.commit()

        legacy_outputs = await _load_project_outputs(legacy_project["id"])
        legacy_materialization = await _load_project_materialization(legacy_project["id"])

        assert len(legacy_outputs[1]) == 1
        assert legacy_outputs[1][0].id == legacy_revision_id
        assert legacy_materialization[0] == []
        assert legacy_materialization[1] == []
        assert legacy_materialization[2] == []
        assert legacy_materialization[3] == []
        assert legacy_materialization[4] == []

    async def test_delete_project_soft_deletes_generated_artifacts_without_removing_rows(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Project deletion should retain rows while marking files/artifacts deleted."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(job.id)

        delete_response = await async_client.delete(f"/v1/projects/{project['id']}")
        assert delete_response.status_code == 204

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            project_row = await session.get(Project, _as_uuid(project["id"]))
            file_row = await session.get(FileModel, _as_uuid(uploaded["id"]))
            artifact_rows = list(
                (
                    await session.execute(
                        select(GeneratedArtifact)
                        .where(GeneratedArtifact.project_id == _as_uuid(project["id"]))
                        .order_by(GeneratedArtifact.created_at, GeneratedArtifact.id)
                    )
                ).scalars()
            )

        assert project_row is not None
        assert project_row.deleted_at is not None
        assert file_row is not None
        assert file_row.deleted_at is not None
        assert len(artifact_rows) == 1
        assert artifact_rows[0].deleted_at is not None

    async def test_process_ingest_job_persists_payload_provenance_and_confidence_precedence(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Finalization should persist payload provenance and authoritative confidence fields."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        captured_payloads: list[IngestFinalizationPayload] = []

        async def _run_ingestion_with_stale_report(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            payload = _replace_fake_canonical_payload(_build_fake_ingest_payload(request))
            payload.report_json.pop("provenance", None)
            payload.report_json["confidence"] = {
                "score": payload.confidence_score,
                "effective_confidence": 0.01,
                "review_state": "approved",
                "review_required": False,
                "basis": "stale",
            }
            captured_payloads.append(payload)
            return payload

        monkeypatch.setattr(worker_module, "run_ingestion", _run_ingestion_with_stale_report)

        await process_ingest_job(job.id)

        (
            adapter_outputs,
            _drawing_revisions,
            validation_reports,
            _generated_artifacts,
        ) = await _load_project_outputs(project["id"])
        adapter_output = adapter_outputs[0]
        validation_report = validation_reports[0]

        assert captured_payloads[0].provenance_json == adapter_output.provenance_json
        assert validation_report.report_json["provenance"] == adapter_output.provenance_json
        assert validation_report.report_json["confidence"]["score"] == captured_payloads[
            0
        ].confidence_json.get("score")
        assert (
            validation_report.report_json["confidence"]["effective_confidence"]
            == validation_report.effective_confidence
        )
        assert (
            validation_report.report_json["confidence"]["review_state"]
            == validation_report.review_state
        )
        assert validation_report.report_json["confidence"]["review_required"] == (
            validation_report.review_state == "review_required"
        )
        assert validation_report.report_json["confidence"].get("basis") == captured_payloads[
            0
        ].confidence_json.get("basis")

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
        first_job = await _get_job(first_job.id)

        reprocess_response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(first_job.extraction_profile_id)},
        )

        assert reprocess_response.status_code == 202

        second_job_id = _as_uuid(reprocess_response.json()["id"])
        second_job = await _get_job(second_job_id)
        assert enqueued_job_ids == [str(first_job.id), str(second_job.id)]

        await process_ingest_job(second_job.id)
        second_job = await _get_job(second_job.id)

        (
            adapter_outputs,
            drawing_revisions,
            validation_reports,
            generated_artifacts,
        ) = await _load_project_outputs(project["id"])

        assert len(adapter_outputs) == 2
        assert len(drawing_revisions) == 2
        assert len(validation_reports) == 2
        assert len(generated_artifacts) == 2

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
        first_generated_artifact = next(
            artifact for artifact in generated_artifacts if artifact.job_id == first_job.id
        )
        second_generated_artifact = next(
            artifact for artifact in generated_artifacts if artifact.job_id == second_job.id
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
        _assert_debug_overlay_artifact(
            first_generated_artifact,
            job=first_job,
            drawing_revision=first_revision,
            adapter_output=first_adapter_output,
            predecessor_artifact_id=None,
        )
        _assert_debug_overlay_artifact(
            second_generated_artifact,
            job=second_job,
            drawing_revision=second_revision,
            adapter_output=second_adapter_output,
            predecessor_artifact_id=first_generated_artifact.id,
        )

    async def test_validation_report_allows_trd_status_and_gate_values(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Persisted validation reports should accept TRD v0.1 status and gate values."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _run_ingestion_with_trd_values(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            payload = _replace_fake_canonical_payload(_build_fake_ingest_payload(request))
            validation_status = "valid_with_warnings"
            quantity_gate = "allowed_provisional"
            return replace(
                payload,
                validation_status=validation_status,
                quantity_gate=quantity_gate,
                report_json={
                    **payload.report_json,
                    "summary": {
                        **payload.report_json["summary"],
                        "validation_status": validation_status,
                        "quantity_gate": quantity_gate,
                    },
                },
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_ingestion_with_trd_values)

        await process_ingest_job(job.id)

        (
            _adapter_outputs,
            _drawing_revisions,
            validation_reports,
            _generated_artifacts,
        ) = await _load_project_outputs(project["id"])

        validation_report = validation_reports[0]
        assert validation_report.validation_status == "valid_with_warnings"
        assert validation_report.quantity_gate == "allowed_provisional"

    async def test_concurrent_reprocess_creates_linear_three_revision_chain(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Concurrent reprocess runs should leave one success and one stale conflict."""
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

        results = await asyncio.gather(
            process_ingest_job(second_job.id),
            process_ingest_job(third_job.id),
            return_exceptions=True,
        )

        assert sum(result is None for result in results) == 1
        assert (
            sum(
                isinstance(result, worker_module._RevisionConflictError)
                for result in results
            )
            == 1
        )

        second_job = await _get_job(second_job.id)
        third_job = await _get_job(third_job.id)
        succeeded_reprocess_jobs = [
            job for job in (second_job, third_job) if job.status == "succeeded"
        ]
        failed_reprocess_jobs = [job for job in (second_job, third_job) if job.status == "failed"]

        assert len(succeeded_reprocess_jobs) == 1
        assert len(failed_reprocess_jobs) == 1
        assert failed_reprocess_jobs[0].error_code == ErrorCode.REVISION_CONFLICT.value
        assert (
            failed_reprocess_jobs[0].error_message
            == "Reprocess base revision became stale before finalization."
        )

        (
            adapter_outputs,
            drawing_revisions,
            validation_reports,
            generated_artifacts,
        ) = await _load_project_outputs(project["id"])

        assert len(adapter_outputs) == 2
        assert len(drawing_revisions) == 2
        assert len(validation_reports) == 2
        assert len(generated_artifacts) == 2

        first_revision, second_revision = drawing_revisions
        artifacts_by_revision_id = {
            artifact.drawing_revision_id: artifact for artifact in generated_artifacts
        }
        first_artifact = artifacts_by_revision_id[first_revision.id]
        second_artifact = artifacts_by_revision_id[second_revision.id]
        assert [revision.revision_sequence for revision in drawing_revisions] == [1, 2]
        assert [revision.predecessor_revision_id for revision in drawing_revisions] == [
            None,
            first_revision.id,
        ]
        assert [revision.revision_kind for revision in drawing_revisions] == [
            "ingest",
            "reprocess",
        ]

        expected_job_ids = {first_job.id, succeeded_reprocess_jobs[0].id}
        assert {output.source_job_id for output in adapter_outputs} == expected_job_ids
        assert {report.source_job_id for report in validation_reports} == expected_job_ids
        assert {artifact.job_id for artifact in generated_artifacts} == expected_job_ids
        assert failed_reprocess_jobs[0].id not in {
            output.source_job_id for output in adapter_outputs
        }
        assert first_artifact.predecessor_artifact_id is None
        assert second_artifact.predecessor_artifact_id == first_artifact.id

    async def test_reprocess_without_finalized_base_returns_conflict_without_job(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Reprocess creation should fail before enqueue when no finalized base exists."""
        _ = self
        _ = cleanup_projects

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        initial_job = await _get_job_for_file(str(uploaded["id"]))

        reprocess_response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(initial_job.extraction_profile_id)},
        )
        assert reprocess_response.status_code == 409
        assert reprocess_response.json() == {
            "error": {
                "code": "REVISION_CONFLICT",
                "message": "Reprocess requires a finalized base revision.",
                "details": None,
            }
        }
        assert enqueued_job_ids == [str(initial_job.id)]

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            persisted_jobs = list(
                (
                    await session.execute(
                        select(Job)
                        .where(Job.file_id == initial_job.file_id)
                        .order_by(Job.created_at, Job.id)
                    )
                ).scalars()
            )
        assert [job.id for job in persisted_jobs] == [initial_job.id]

        (
            adapter_outputs,
            drawing_revisions,
            validation_reports,
            generated_artifacts,
        ) = await _load_project_outputs(project["id"])
        assert adapter_outputs == []
        assert drawing_revisions == []
        assert validation_reports == []
        assert generated_artifacts == []

    async def test_stale_reprocess_finalization_fails_without_storage_or_extra_outputs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A stale reprocess base should fail before storage writes and append no outputs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        @dataclass(frozen=True, slots=True)
        class _StoredOverlay:
            key: str
            storage_uri: str
            size_bytes: int
            checksum_sha256: str

        class _RecordingStorage:
            def __init__(self) -> None:
                self.put_calls: list[str] = []
                self.delete_calls: list[tuple[str, str]] = []

            async def put(self, key: str, payload: bytes, *, immutable: bool) -> _StoredOverlay:
                assert immutable is True
                self.put_calls.append(key)
                return _StoredOverlay(
                    key=key,
                    storage_uri=f"memory://{key}",
                    size_bytes=len(payload),
                    checksum_sha256="0" * 64,
                )

            async def delete_failed_put(self, key: str, *, storage_uri: str) -> None:
                self.delete_calls.append((key, storage_uri))

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        initial_job = await _get_job_for_file(str(uploaded["id"]))
        storage = _RecordingStorage()
        monkeypatch.setattr(worker_module, "get_storage", lambda: storage)

        await process_ingest_job(initial_job.id)

        (
            adapter_outputs,
            drawing_revisions,
            _validation_reports,
            _generated_artifacts,
        ) = await _load_project_outputs(project["id"])
        base_revision = drawing_revisions[0]
        assert len(adapter_outputs) == 1

        first_reprocess_response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(initial_job.extraction_profile_id)},
        )
        second_reprocess_response = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile_id": str(initial_job.extraction_profile_id)},
        )
        assert first_reprocess_response.status_code == 202
        assert second_reprocess_response.status_code == 202

        first_reprocess_job = await _get_job(_as_uuid(first_reprocess_response.json()["id"]))
        second_reprocess_job = await _get_job(_as_uuid(second_reprocess_response.json()["id"]))
        assert first_reprocess_job.job_type == "reprocess"
        assert second_reprocess_job.job_type == "reprocess"
        assert first_reprocess_job.base_revision_id == base_revision.id
        assert second_reprocess_job.base_revision_id == base_revision.id

        await process_ingest_job(first_reprocess_job.id)

        (
            adapter_outputs_before_stale,
            drawing_revisions_before_stale,
            validation_reports_before_stale,
            generated_artifacts_before_stale,
        ) = await _load_project_outputs(project["id"])
        storage_put_calls_before_stale = list(storage.put_calls)

        with pytest.raises(
            worker_module._RevisionConflictError,
            match=r"Reprocess base revision became stale before finalization\.",
        ):
            await process_ingest_job(second_reprocess_job.id)

        (
            adapter_outputs,
            drawing_revisions,
            validation_reports,
            generated_artifacts,
        ) = await _load_project_outputs(project["id"])
        current_revision = next(
            revision
            for revision in drawing_revisions
            if revision.source_job_id == first_reprocess_job.id
        )
        assert [row.id for row in adapter_outputs] == [
            row.id for row in adapter_outputs_before_stale
        ]
        assert [row.id for row in drawing_revisions] == [
            row.id for row in drawing_revisions_before_stale
        ]
        assert [row.id for row in validation_reports] == [
            row.id for row in validation_reports_before_stale
        ]
        assert [row.id for row in generated_artifacts] == [
            row.id for row in generated_artifacts_before_stale
        ]
        assert second_reprocess_job.id not in {output.source_job_id for output in adapter_outputs}
        assert second_reprocess_job.id not in {
            revision.source_job_id for revision in drawing_revisions
        }
        assert second_reprocess_job.id not in {
            report.source_job_id for report in validation_reports
        }
        assert second_reprocess_job.id not in {artifact.job_id for artifact in generated_artifacts}
        failed_reprocess_job = await _get_job(second_reprocess_job.id)
        assert failed_reprocess_job.status == "failed"
        assert failed_reprocess_job.error_code == ErrorCode.REVISION_CONFLICT.value
        assert (
            failed_reprocess_job.error_message
            == "Reprocess base revision became stale before finalization."
        )
        assert failed_reprocess_job.attempt_token is None
        assert failed_reprocess_job.attempt_lease_expires_at is None

        job_events = await _load_job_events(second_reprocess_job.id)
        assert job_events[-1].data_json == {
            "status": "failed",
            "error_code": ErrorCode.REVISION_CONFLICT.value,
            "error_message": "Reprocess base revision became stale before finalization.",
            "details": {
                "base_revision_id": str(base_revision.id),
                "base_revision_sequence": 1,
                "current_revision_id": str(current_revision.id),
                "current_revision_sequence": 2,
            },
        }

        assert len(adapter_outputs) == 2
        assert len(drawing_revisions) == 2
        assert len(validation_reports) == 2
        assert len(generated_artifacts) == 2
        assert storage.put_calls == storage_put_calls_before_stale
        assert storage.delete_calls == []

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

        async def _cancel_during_work(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            await _update_job(job.id, cancel_requested=True)
            return _replace_fake_canonical_payload(_build_fake_ingest_payload(request))

        monkeypatch.setattr(worker_module, "run_ingestion", _cancel_during_work)

        with suppress(asyncio.CancelledError):
            await process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "cancelled"
        assert updated_job.error_code == ErrorCode.JOB_CANCELLED.value

        (
            adapter_outputs,
            drawing_revisions,
            validation_reports,
            generated_artifacts,
        ) = await _load_project_outputs(project["id"])

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
        assert len(second_outputs[3]) == 1
        assert [row.id for row in second_outputs[0]] == [row.id for row in first_outputs[0]]
        assert [row.id for row in second_outputs[1]] == [row.id for row in first_outputs[1]]
        assert [row.id for row in second_outputs[2]] == [row.id for row in first_outputs[2]]
        assert [row.id for row in second_outputs[3]] == [row.id for row in first_outputs[3]]

    async def test_process_ingest_job_precommit_overlay_failure_cleans_storage_without_outputs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A pre-commit overlay failure should clean storage and persist no outputs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        @dataclass(frozen=True, slots=True)
        class _StoredOverlay:
            key: str
            storage_uri: str
            size_bytes: int
            checksum_sha256: str

        class _RecordingStorage:
            def __init__(self) -> None:
                self.put_calls: list[str] = []
                self.delete_calls: list[tuple[str, str]] = []

            async def put(self, key: str, payload: bytes, *, immutable: bool) -> _StoredOverlay:
                assert immutable is True
                self.put_calls.append(key)
                return _StoredOverlay(
                    key=key,
                    storage_uri=f"memory://{key}",
                    size_bytes=len(payload),
                    checksum_sha256="0" * 64,
                )

            async def delete_failed_put(self, key: str, *, storage_uri: str) -> None:
                self.delete_calls.append((key, storage_uri))

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        storage = _RecordingStorage()
        original_emit_job_event = worker_module.emit_job_event

        async def _run_ingestion(request: IngestionRunRequest) -> IngestFinalizationPayload:
            return _replace_fake_canonical_payload(_build_fake_ingest_payload(request))

        async def _fail_success_event(*args: Any, **kwargs: Any) -> None:
            if kwargs.get("message") == "Job succeeded":
                raise RuntimeError("forced overlay finalization failure")
            await original_emit_job_event(*args, **kwargs)

        monkeypatch.setattr(worker_module, "run_ingestion", _run_ingestion)
        monkeypatch.setattr(worker_module, "get_storage", lambda: storage)
        monkeypatch.setattr(worker_module, "emit_job_event", _fail_success_event)

        with pytest.raises(RuntimeError, match="forced overlay finalization failure"):
            await process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "failed"
        assert updated_job.error_code == ErrorCode.INTERNAL_ERROR.value
        assert updated_job.error_message == worker_module._FINALIZE_INGEST_JOB_ERROR_MESSAGE
        assert len(storage.put_calls) == 1
        assert storage.delete_calls == [
            (storage.put_calls[0], f"memory://{storage.put_calls[0]}")
        ]

        (
            adapter_outputs,
            drawing_revisions,
            validation_reports,
            generated_artifacts,
        ) = await _load_project_outputs(project["id"])

        assert adapter_outputs == []
        assert drawing_revisions == []
        assert validation_reports == []
        assert generated_artifacts == []

        (
            manifests,
            layouts,
            layers,
            blocks,
            entities,
        ) = await _load_project_materialization(project["id"])

        assert manifests == []
        assert layouts == []
        assert layers == []
        assert blocks == []
        assert entities == []

    async def test_process_ingest_job_precommit_cancellation_cleans_storage_without_outputs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A pre-commit cancellation should clean storage and persist no outputs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        @dataclass(frozen=True, slots=True)
        class _StoredOverlay:
            key: str
            storage_uri: str
            size_bytes: int
            checksum_sha256: str

        class _RecordingStorage:
            def __init__(self) -> None:
                self.put_calls: list[str] = []
                self.delete_calls: list[tuple[str, str]] = []

            async def put(self, key: str, payload: bytes, *, immutable: bool) -> _StoredOverlay:
                assert immutable is True
                self.put_calls.append(key)
                return _StoredOverlay(
                    key=key,
                    storage_uri=f"memory://{key}",
                    size_bytes=len(payload),
                    checksum_sha256="0" * 64,
                )

            async def delete_failed_put(self, key: str, *, storage_uri: str) -> None:
                self.delete_calls.append((key, storage_uri))

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        storage = _RecordingStorage()
        original_emit_job_event = worker_module.emit_job_event

        async def _run_ingestion(request: IngestionRunRequest) -> IngestFinalizationPayload:
            return _replace_fake_canonical_payload(_build_fake_ingest_payload(request))

        async def _cancel_success_event(*args: Any, **kwargs: Any) -> None:
            if kwargs.get("message") == "Job succeeded":
                raise asyncio.CancelledError()
            await original_emit_job_event(*args, **kwargs)

        monkeypatch.setattr(worker_module, "run_ingestion", _run_ingestion)
        monkeypatch.setattr(worker_module, "get_storage", lambda: storage)
        monkeypatch.setattr(worker_module, "emit_job_event", _cancel_success_event)

        with suppress(asyncio.CancelledError):
            await process_ingest_job(job.id)

        updated_job = await _get_job(job.id)
        assert updated_job.status == "cancelled"
        assert updated_job.error_code == ErrorCode.JOB_CANCELLED.value
        assert updated_job.error_message is None
        assert len(storage.put_calls) == 1
        assert storage.delete_calls == [
            (storage.put_calls[0], f"memory://{storage.put_calls[0]}")
        ]

        (
            adapter_outputs,
            drawing_revisions,
            validation_reports,
            generated_artifacts,
        ) = await _load_project_outputs(project["id"])

        assert adapter_outputs == []
        assert drawing_revisions == []
        assert validation_reports == []
        assert generated_artifacts == []

        (
            manifests,
            layouts,
            layers,
            blocks,
            entities,
        ) = await _load_project_materialization(project["id"])

        assert manifests == []
        assert layouts == []
        assert layers == []
        assert blocks == []
        assert entities == []
