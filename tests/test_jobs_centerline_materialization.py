"""Tests for centerline materialization producer dispatch and DB-backed materialization.

Lane: db_worker (matches the ingest+materialization harness).
"""

from __future__ import annotations

import uuid
from copy import deepcopy
from dataclasses import replace
from typing import Any
from uuid import UUID

import httpx
import pytest
from sqlalchemy import select

import app.api.v1.revision_routes.service_takeoff as service_takeoff_route
import app.db.session as session_module
import app.jobs.worker as worker_module
from app.api.v1.revision_routes.service_takeoff import _enqueue_centerline_materialization
from app.ingestion.centerline_contract import CURRENT_ALGO_VERSION
from app.ingestion.centerline_dwg import dwg_centerlines
from app.ingestion.centerline_passthrough import passthrough_centerlines
from app.ingestion.centerline_pdf import pdf_centerlines
from app.ingestion.finalization import IngestFinalizationPayload, compute_adapter_result_checksum
from app.ingestion.runner import IngestionRunRequest
from app.ingestion.validation.reconciliation import build_reconciliation
from app.interpretation.service_takeoff_loaders import load_measured_lengths
from app.jobs.centerline_materialization import select_centerline_producer
from app.jobs.worker import process_centerline_job, process_ingest_job
from app.models.drawing_revision import DrawingRevision
from app.models.job import Job, JobType
from app.models.revision_routed_length import RevisionRoutedLength
from tests.conftest import requires_database, truncate_projects_cascade_for_cleanup
from tests.jobs_test_helpers import _create_project, _get_job_for_file, _upload_file
from tests.test_ingest_output_persistence import _load_project_outputs
from tests.test_jobs import (
    _FAKE_RUNNER_ADAPTER_KEY,
    _FAKE_RUNNER_ADAPTER_VERSION,
    _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
    _FAKE_RUNNER_CONFIDENCE_SCORE,
    _FAKE_RUNNER_VALIDATION_REPORT_SCHEMA_VERSION,
    _FAKE_RUNNER_VALIDATION_STATUS,
    _FAKE_RUNNER_VALIDATOR_NAME,
    _FAKE_RUNNER_VALIDATOR_VERSION,
    _build_fake_ingest_payload,
)

# ---------------------------------------------------------------------------
# Pure unit tests — no DB required
# ---------------------------------------------------------------------------


def test_select_centerline_producer_dwg_returns_dwg_centerlines() -> None:
    assert select_centerline_producer("dwg") is dwg_centerlines


def test_select_centerline_producer_dxf_returns_dwg_centerlines() -> None:
    assert select_centerline_producer("dxf") is dwg_centerlines


def test_select_centerline_producer_pdf_vector_returns_pdf_centerlines() -> None:
    assert select_centerline_producer("pdf_vector") is pdf_centerlines


def test_select_centerline_producer_pdf_raster_returns_pdf_centerlines() -> None:
    assert select_centerline_producer("pdf_raster") is pdf_centerlines


def test_select_centerline_producer_none_returns_passthrough() -> None:
    assert select_centerline_producer(None) is passthrough_centerlines


def test_select_centerline_producer_unknown_returns_passthrough() -> None:
    assert select_centerline_producer("other") is passthrough_centerlines


# ---------------------------------------------------------------------------
# Helpers shared by DB-backed tests
# ---------------------------------------------------------------------------


def _make_entity(
    entity_id: str,
    entity_type: str,
    layer_ref: str,
    geometry_json: dict[str, Any],
    *,
    style: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base_props: dict[str, Any] = {"layer": layer_ref}
    payload: dict[str, Any] = {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "layout_ref": "Model",
        "layer_ref": layer_ref,
        "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
        "confidence_json": {"score": _FAKE_RUNNER_CONFIDENCE_SCORE, "basis": "adapter"},
        "geometry_json": geometry_json,
        "properties_json": base_props,
        "provenance_json": {
            "origin": "adapter_normalized",
            "adapter": {},
            "source_ref": None,
            "source_identity": entity_id,
            "source_hash": None,
            "extraction_path": [],
            "notes": [],
        },
    }
    if style is not None:
        payload["style"] = style
    return payload


def _build_payload_with(
    request: IngestionRunRequest,
    *,
    entities: list[dict[str, Any]],
    input_family: str = "dxf",
) -> IngestFinalizationPayload:
    entity_counts = {
        "layouts": 1,
        "layers": 1,
        "blocks": 0,
        "entities": len(entities),
    }
    canonical_json: dict[str, Any] = {
        "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "layouts": [{"layout_ref": "Model", "name": "Model"}],
        "layers": [{"layer_ref": "Pipes", "name": "Pipes"}],
        "blocks": [],
        "entities": deepcopy(entities),
        "entity_counts": entity_counts,
    }
    report_json: dict[str, Any] = {
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
        "coverage": {
            "schema_version": "0.1",
            "entities": {
                "total": len(entities),
                "mapped": len(entities),
                "unmapped": 0,
                "mapped_ratio": 1.0,
                "by_type": {},
            },
            "unmapped_by_reason": {},
            "layers": {"count": 1, "entities_with_layer_ref": 0, "source": None},
            "blocks": {"count": 0, "child_geometry_count": 0},
            "review_flagged_entities": 0,
        },
        "reconciliation": build_reconciliation(canonical_json),
        "checks": [],
        "findings": [],
        "adapter_warnings": [],
        "provenance": {},
    }
    result_envelope = {
        "adapter_key": _FAKE_RUNNER_ADAPTER_KEY,
        "adapter_version": _FAKE_RUNNER_ADAPTER_VERSION,
        "input_family": input_family,
        "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "canonical_json": canonical_json,
        "provenance_json": {},
        "confidence_json": {"score": _FAKE_RUNNER_CONFIDENCE_SCORE},
        "warnings_json": [],
        "diagnostics_json": {},
    }
    base = _build_fake_ingest_payload(request)
    return replace(
        base,
        input_family=input_family,
        canonical_json=canonical_json,
        report_json=report_json,
        result_checksum_sha256=compute_adapter_result_checksum(result_envelope),
    )


class _IngestSeed:
    """Lightweight seed result from a fake ingest."""

    def __init__(self, revision: DrawingRevision) -> None:
        self.drawing_revision_id: UUID = revision.id
        self.project_id: UUID = revision.project_id
        self.source_file_id: UUID = revision.source_file_id


async def _ingest_and_seed(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    *,
    entities: list[dict[str, Any]],
    input_family: str = "dxf",
) -> _IngestSeed:
    """Ingest a fake revision and return a seed with lineage IDs."""

    async def _fake_run(request: IngestionRunRequest) -> IngestFinalizationPayload:
        return _build_payload_with(request, entities=entities, input_family=input_family)

    monkeypatch.setattr(worker_module, "run_ingestion", _fake_run)

    project = await _create_project(async_client)
    uploaded = await _upload_file(async_client, project["id"])
    job = await _get_job_for_file(str(uploaded["id"]))
    await process_ingest_job(job.id)

    _, drawing_revisions, _, _ = await _load_project_outputs(project["id"])
    assert len(drawing_revisions) == 1
    return _IngestSeed(drawing_revisions[0])


def _get_session() -> Any:
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None, "DATABASE_URL not configured"
    return session_maker()


async def _get_centerline_job_for_revision(revision_id: UUID) -> Job | None:
    """Return the CENTERLINE job for a revision, or None."""
    async with _get_session() as session:
        row: Job | None = await session.scalar(
            select(Job).where(
                Job.base_revision_id == revision_id,
                Job.job_type == JobType.CENTERLINE.value,
            )
        )
    return row


async def _enqueue_and_run_centerline(seed: _IngestSeed) -> UUID:
    """Enqueue then immediately process the CENTERLINE job for a seeded revision."""
    await _enqueue_centerline_materialization(
        revision_id=seed.drawing_revision_id,
        project_id=seed.project_id,
        source_file_id=seed.source_file_id,
    )
    job = await _get_centerline_job_for_revision(seed.drawing_revision_id)
    assert job is not None, f"No CENTERLINE job found for revision {seed.drawing_revision_id}"
    await process_centerline_job(job.id)
    return job.id


# ---------------------------------------------------------------------------
# DB-backed tests
# ---------------------------------------------------------------------------


@pytest.fixture
async def cleanup_db() -> None:  # type: ignore[misc]
    await truncate_projects_cascade_for_cleanup()
    yield
    await truncate_projects_cascade_for_cleanup()


@pytest.fixture(autouse=True)
def stub_centerline_publish(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prevent real Celery broker calls when enqueueing centerline jobs."""

    async def _noop_publish(*_args: Any, **_kwargs: Any) -> None:
        pass

    def _noop_enqueue(_job_id: Any) -> None:
        pass

    monkeypatch.setattr(service_takeoff_route, "_publish_job_enqueue_intent", _noop_publish)
    monkeypatch.setattr(service_takeoff_route, "_enqueue_centerline_job", _noop_enqueue)


_COLOR = {"index": 1, "rgb": "#ff0000", "by_layer": False, "by_block": False}

# A simple parallel-wall pair suitable for DWG derived-path testing.
_DWG_ENTITIES = [
    _make_entity(
        "wall-a",
        "line",
        "Pipes",
        {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
        style={"color": _COLOR},
    ),
    _make_entity(
        "wall-b",
        "line",
        "Pipes",
        {"start": [0.0, 20.0, 0.0], "end": [1000.0, 20.0, 0.0]},
        style={"color": _COLOR},
    ),
]

_PDF_ENTITIES = [
    _make_entity(
        "pdf-line-001",
        "line",
        "Pipes",
        {"start": [0.0, 0.0, 0.0], "end": [500.0, 0.0, 0.0]},
        style={"color": _COLOR},
    ),
]


@requires_database
async def test_dwg_materialization_producer_kind(
    async_client: httpx.AsyncClient,
    cleanup_db: Any,
    enqueued_job_ids: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Materializing a DWG revision produces rows with dwg_authored/dwg_derived producer_kind."""
    seed = await _ingest_and_seed(
        async_client, monkeypatch, entities=_DWG_ENTITIES, input_family="dwg"
    )
    await _enqueue_and_run_centerline(seed)

    async with _get_session() as session:
        rows = list(
            (
                await session.execute(
                    select(RevisionRoutedLength).where(
                        RevisionRoutedLength.drawing_revision_id == seed.drawing_revision_id
                    )
                )
            )
            .scalars()
            .all()
        )

    assert rows, "Expected at least one RevisionRoutedLength row for the DWG revision"
    allowed = {"dwg_authored", "dwg_derived"}
    for row in rows:
        assert row.producer_kind in allowed, (
            f"Expected producer_kind in {allowed!r}, got {row.producer_kind!r}"
        )
        assert row.algo_version == CURRENT_ALGO_VERSION, (
            f"Expected algo_version={CURRENT_ALGO_VERSION!r}, got {row.algo_version!r}"
        )


@requires_database
async def test_dxf_materialization_producer_kind(
    async_client: httpx.AsyncClient,
    cleanup_db: Any,
    enqueued_job_ids: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Materializing a DXF revision also uses dwg_centerlines producer."""
    seed = await _ingest_and_seed(
        async_client, monkeypatch, entities=_DWG_ENTITIES, input_family="dxf"
    )
    await _enqueue_and_run_centerline(seed)

    async with _get_session() as session:
        rows = list(
            (
                await session.execute(
                    select(RevisionRoutedLength).where(
                        RevisionRoutedLength.drawing_revision_id == seed.drawing_revision_id
                    )
                )
            )
            .scalars()
            .all()
        )

    assert rows, "Expected at least one RevisionRoutedLength row for the DXF revision"
    allowed = {"dwg_authored", "dwg_derived"}
    for row in rows:
        assert row.producer_kind in allowed, (
            f"Expected producer_kind in {allowed!r}, got {row.producer_kind!r}"
        )


@requires_database
async def test_pdf_vector_materialization_producer_kind(
    async_client: httpx.AsyncClient,
    cleanup_db: Any,
    enqueued_job_ids: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Materializing a pdf_vector revision uses the pdf_raster producer."""
    pytest.importorskip("cv2")
    seed = await _ingest_and_seed(
        async_client, monkeypatch, entities=_PDF_ENTITIES, input_family="pdf_vector"
    )
    await _enqueue_and_run_centerline(seed)

    async with _get_session() as session:
        rows = list(
            (
                await session.execute(
                    select(RevisionRoutedLength).where(
                        RevisionRoutedLength.drawing_revision_id == seed.drawing_revision_id
                    )
                )
            )
            .scalars()
            .all()
        )

    assert rows, "Expected at least one RevisionRoutedLength row for the pdf_vector revision"
    for row in rows:
        assert row.producer_kind == "pdf_raster", (
            f"Expected producer_kind='pdf_raster', got {row.producer_kind!r}"
        )
        assert row.algo_version == CURRENT_ALGO_VERSION, (
            f"Expected algo_version={CURRENT_ALGO_VERSION!r}, got {row.algo_version!r}"
        )


@requires_database
async def test_version_gate_divergence(
    async_client: httpx.AsyncClient,
    cleanup_db: Any,
    enqueued_job_ids: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """load_measured_lengths with a stale version string returns empty results.

    A revision materialized at CURRENT_ALGO_VERSION (c2-pdf-1) must not be
    visible when queried at the old c0-passthrough-1 version.
    """
    seed = await _ingest_and_seed(
        async_client, monkeypatch, entities=_DWG_ENTITIES, input_family="dwg"
    )
    await _enqueue_and_run_centerline(seed)

    async with _get_session() as session:
        # Default version (c2-pdf-1) should find rows.
        _mapping_current, present_current = await load_measured_lengths(
            session, seed.drawing_revision_id
        )
        # Stale version (c0-passthrough-1) should be empty.
        _mapping_stale, present_stale = await load_measured_lengths(
            session, seed.drawing_revision_id, algo_version="c0-passthrough-1"
        )

    assert present_current, "Expected non-empty results at current algo version"
    assert not present_stale, (
        "Expected empty results when querying at the stale c0-passthrough-1 version"
    )


@requires_database
async def test_dwg_materialization_idempotency(
    async_client: httpx.AsyncClient,
    cleanup_db: Any,
    enqueued_job_ids: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Materializing the same DWG revision twice inserts each group exactly once."""
    from app.jobs.centerline_materialization import materialize_centerline_lengths

    seed = await _ingest_and_seed(
        async_client, monkeypatch, entities=_DWG_ENTITIES, input_family="dwg"
    )
    # Run the job once to materialize.
    await _enqueue_and_run_centerline(seed)

    async with _get_session() as session:
        rows_first = list(
            (
                await session.execute(
                    select(RevisionRoutedLength).where(
                        RevisionRoutedLength.drawing_revision_id == seed.drawing_revision_id
                    )
                )
            )
            .scalars()
            .all()
        )

    assert rows_first, "Expected rows after first materialization"
    row = rows_first[0]

    # Run materialization a second time directly to exercise ON CONFLICT DO NOTHING.
    async with _get_session() as session:
        await materialize_centerline_lengths(
            session,
            job_id=uuid.uuid4(),
            project_id=row.project_id,
            source_file_id=row.source_file_id,
            drawing_revision_id=seed.drawing_revision_id,
            adapter_run_output_id=row.adapter_run_output_id,
            canonical_entity_schema_version=row.canonical_entity_schema_version,
        )

    async with _get_session() as session:
        rows_second = list(
            (
                await session.execute(
                    select(RevisionRoutedLength).where(
                        RevisionRoutedLength.drawing_revision_id == seed.drawing_revision_id
                    )
                )
            )
            .scalars()
            .all()
        )

    assert len(rows_first) == len(rows_second), (
        f"Expected idempotent insert: first={len(rows_first)}, second={len(rows_second)}"
    )
