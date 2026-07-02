"""Tests for room materialization (#831).

Lane: db_worker (matches the ingest+materialization harness used by the
centerline materializer tests).
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any
from uuid import UUID

import httpx
import pytest
from sqlalchemy import select

import app.api.v1.revision_routes.service_takeoff as service_takeoff_route
import app.db.session as session_module
import app.jobs.worker as worker_module
from app.api.v1.revision_routes.rooms import _resolve_rooms_with_family
from app.api.v1.revision_routes.service_takeoff import _enqueue_rooms_materialization
from app.ingestion.finalization import IngestFinalizationPayload, compute_adapter_result_checksum
from app.ingestion.runner import IngestionRunRequest
from app.ingestion.validation.reconciliation import build_reconciliation
from app.interpretation.label_rooms import has_genuine_room_identity
from app.interpretation.room_loaders import load_rooms
from app.jobs.room_materialization import ROOM_ALGO_VERSION, materialize_rooms
from app.jobs.worker import process_ingest_job, process_rooms_job
from app.models.drawing_revision import DrawingRevision
from app.models.job import Job, JobType
from app.models.revision_room import RevisionRoom
from app.models.revision_room_summary import RevisionRoomSummary
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
# Synthetic entities: one closed room polygon (A-ROOM) + a room-number label.
# ---------------------------------------------------------------------------

_ROOM_SQUARE = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]


def _closed_polyline_entity(entity_id: str, layer_ref: str) -> dict[str, Any]:
    closed = [*_ROOM_SQUARE, _ROOM_SQUARE[0]]
    geometry_json: dict[str, Any] = {
        "kind": "polyline",
        "closed": True,
        "vertices": [{"x": x, "y": y, "z": 0.0} for x, y in closed],
    }
    return {
        "entity_id": entity_id,
        "entity_type": "polyline",
        "entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "layout_ref": "Model",
        "layer_ref": layer_ref,
        "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
        "confidence_json": {"score": _FAKE_RUNNER_CONFIDENCE_SCORE, "basis": "adapter"},
        "geometry_json": geometry_json,
        "properties_json": {"layer": layer_ref},
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


def _text_entity(entity_id: str, layer_ref: str, text: str, x: float, y: float) -> dict[str, Any]:
    geometry_json: dict[str, Any] = {"kind": "text", "text": text, "insertion": {"x": x, "y": y}}
    return {
        "entity_id": entity_id,
        "entity_type": "text",
        "entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "layout_ref": "Model",
        "layer_ref": layer_ref,
        "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
        "confidence_json": {"score": _FAKE_RUNNER_CONFIDENCE_SCORE, "basis": "adapter"},
        "geometry_json": geometry_json,
        "properties_json": {"layer": layer_ref},
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


_ROOM_ENTITIES: list[dict[str, Any]] = [
    _closed_polyline_entity("room-1", "A-ROOM"),
    _text_entity("room-1-label", "A-ANNO", "Kitchen", 5.0, 5.0),
]


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
        "layers": [
            {"layer_ref": "A-ROOM", "name": "A-ROOM"},
            {"layer_ref": "A-ANNO", "name": "A-ANNO"},
        ],
        "blocks": [],
        "entities": entities,
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
            "layers": {"count": 2, "entities_with_layer_ref": 0, "source": None},
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


async def _get_rooms_job_for_revision(revision_id: UUID) -> Job | None:
    """Return the ROOMS job for a revision, or None."""
    async with _get_session() as session:
        row: Job | None = await session.scalar(
            select(Job).where(
                Job.base_revision_id == revision_id,
                Job.job_type == JobType.ROOMS.value,
            )
        )
    return row


async def _enqueue_and_run_rooms(seed: _IngestSeed) -> UUID:
    """Enqueue then immediately process the ROOMS job for a seeded revision."""
    await _enqueue_rooms_materialization(
        revision_id=seed.drawing_revision_id,
        project_id=seed.project_id,
        source_file_id=seed.source_file_id,
    )
    job = await _get_rooms_job_for_revision(seed.drawing_revision_id)
    assert job is not None, f"No ROOMS job found for revision {seed.drawing_revision_id}"
    await process_rooms_job(job.id)
    return job.id


async def _load_room_rows(revision_id: UUID) -> list[RevisionRoom]:
    async with _get_session() as session:
        return list(
            (
                await session.execute(
                    select(RevisionRoom).where(RevisionRoom.drawing_revision_id == revision_id)
                )
            )
            .scalars()
            .all()
        )


async def _load_summary_rows(revision_id: UUID) -> list[RevisionRoomSummary]:
    async with _get_session() as session:
        return list(
            (
                await session.execute(
                    select(RevisionRoomSummary).where(
                        RevisionRoomSummary.drawing_revision_id == revision_id
                    )
                )
            )
            .scalars()
            .all()
        )


# ---------------------------------------------------------------------------
# DB-backed tests
# ---------------------------------------------------------------------------


@pytest.fixture
async def cleanup_db() -> None:  # type: ignore[misc]
    await truncate_projects_cascade_for_cleanup()
    yield
    await truncate_projects_cascade_for_cleanup()


@pytest.fixture(autouse=True)
def stub_rooms_publish(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prevent real Celery broker calls when enqueueing rooms jobs."""

    async def _noop_publish(*_args: Any, **_kwargs: Any) -> None:
        pass

    def _noop_enqueue(_job_id: Any) -> None:
        pass

    monkeypatch.setattr(service_takeoff_route, "_publish_job_enqueue_intent", _noop_publish)
    monkeypatch.setattr(service_takeoff_route, "_enqueue_rooms_job", _noop_enqueue)


@requires_database
async def test_room_materialization_rows_match_interpret_rooms_registry(
    async_client: httpx.AsyncClient,
    cleanup_db: Any,
    enqueued_job_ids: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persisted RevisionRoom rows reproduce the GENUINE (surfaced) interpret_rooms set."""
    seed = await _ingest_and_seed(
        async_client, monkeypatch, entities=_ROOM_ENTITIES, input_family="dxf"
    )
    await _enqueue_and_run_rooms(seed)

    rows = await _load_room_rows(seed.drawing_revision_id)
    assert rows, "Expected at least one RevisionRoom row"

    async with _get_session() as session:
        result, input_family = await _resolve_rooms_with_family(session, seed.drawing_revision_id)
    expected_rooms = {
        room.id: room
        for room in result.rooms
        if has_genuine_room_identity(room, input_family=input_family)
    }

    assert {row.room_key for row in rows} == set(expected_rooms.keys())

    for row in rows:
        room = expected_rooms[row.room_key]
        assert row.name == room.name
        assert row.number == room.number
        assert row.source == room.source
        assert row.needs_review == room.needs_review
        if room.bounds is not None:
            assert row.bounds_min_x == room.bounds[0]
            assert row.bounds_min_y == room.bounds[1]
            assert row.bounds_max_x == room.bounds[2]
            assert row.bounds_max_y == room.bounds[3]
        else:
            assert row.bounds_min_x is None
            assert row.bounds_max_x is None
        assert row.algo_version == ROOM_ALGO_VERSION
        expected_device_ids = [
            a.device_id for a in result.device_assignments if a.room_id == room.id
        ]
        assert row.assigned_device_ids_json == expected_device_ids

    summary_rows = await _load_summary_rows(seed.drawing_revision_id)
    assert len(summary_rows) == 1
    assert summary_rows[0].algo_version == ROOM_ALGO_VERSION
    assert summary_rows[0].full_registry_size == len(result.rooms)


@requires_database
async def test_load_rooms_reconstructs_the_live_genuine_set(
    async_client: httpx.AsyncClient,
    cleanup_db: Any,
    enqueued_job_ids: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#831 Phase 2 gate 9: load_rooms reconstructs Room objects matching the live genuine
    set on every RoomRead-relevant field (not just the persisted-row spot checks above).
    """
    seed = await _ingest_and_seed(
        async_client, monkeypatch, entities=_ROOM_ENTITIES, input_family="dxf"
    )
    await _enqueue_and_run_rooms(seed)

    async with _get_session() as session:
        result, input_family = await _resolve_rooms_with_family(session, seed.drawing_revision_id)
    expected_rooms = {
        room.id: room
        for room in result.rooms
        if has_genuine_room_identity(room, input_family=input_family)
    }

    async with _get_session() as session:
        loaded_rooms, loaded_assignments = await load_rooms(session, seed.drawing_revision_id)

    assert {room.id for room in loaded_rooms} == set(expected_rooms.keys())
    for loaded_room in loaded_rooms:
        live_room = expected_rooms[loaded_room.id]
        assert loaded_room.name == live_room.name
        assert loaded_room.number == live_room.number
        assert loaded_room.source == live_room.source
        assert loaded_room.needs_review == live_room.needs_review
        assert loaded_room.confidence == live_room.confidence
        assert loaded_room.bounds == live_room.bounds
        assert loaded_room.anchors == live_room.anchors
        assert loaded_room.location == live_room.location
        if live_room.area is not None:
            assert loaded_room.area == pytest.approx(live_room.area)
        else:
            assert loaded_room.area is None

    # Assignment SET equality: per-room expansion groups assignments by room while the
    # live pipeline groups by phase, so order is not a contract -- membership is (this
    # still proves the M1 no-extra/no-missing fix).
    expected_pairs = [
        (a.device_id, a.room_id) for a in result.device_assignments if a.room_id in expected_rooms
    ]
    loaded_pairs = [(a.device_id, a.room_id) for a in loaded_assignments]
    assert set(loaded_pairs) == set(expected_pairs)
    loaded_device_ids = [pair[0] for pair in loaded_pairs]
    assert len(loaded_device_ids) == len(set(loaded_device_ids))


@requires_database
async def test_room_materialization_idempotency(
    async_client: httpx.AsyncClient,
    cleanup_db: Any,
    enqueued_job_ids: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Materializing the same revision twice inserts each room exactly once."""
    seed = await _ingest_and_seed(
        async_client, monkeypatch, entities=_ROOM_ENTITIES, input_family="dxf"
    )
    await _enqueue_and_run_rooms(seed)

    rows_first = await _load_room_rows(seed.drawing_revision_id)
    assert rows_first, "Expected rows after first materialization"
    row = rows_first[0]

    summary_rows_first = await _load_summary_rows(seed.drawing_revision_id)
    assert len(summary_rows_first) == 1

    # Run materialization a second time directly to exercise ON CONFLICT DO NOTHING.
    # Reuse the original ROOMS job id (source_job_id has an FK to jobs).
    async with _get_session() as session:
        await materialize_rooms(
            session,
            job_id=row.source_job_id,
            project_id=row.project_id,
            source_file_id=row.source_file_id,
            drawing_revision_id=seed.drawing_revision_id,
            adapter_run_output_id=row.adapter_run_output_id,
            canonical_entity_schema_version=row.canonical_entity_schema_version,
        )
        await session.commit()

    rows_second = await _load_room_rows(seed.drawing_revision_id)
    assert len(rows_first) == len(rows_second), (
        f"Expected idempotent insert: first={len(rows_first)}, second={len(rows_second)}"
    )

    summary_rows_second = await _load_summary_rows(seed.drawing_revision_id)
    assert len(summary_rows_second) == 1, "Expected idempotent summary insert (ON CONFLICT)"
    assert summary_rows_second[0].id == summary_rows_first[0].id


@requires_database
async def test_room_materialization_version_bump_coexists(
    async_client: httpx.AsyncClient,
    cleanup_db: Any,
    enqueued_job_ids: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A different ROOM_ALGO_VERSION produces new rows alongside the old ones (append-only)."""
    seed = await _ingest_and_seed(
        async_client, monkeypatch, entities=_ROOM_ENTITIES, input_family="dxf"
    )
    await _enqueue_and_run_rooms(seed)

    rows_v1 = await _load_room_rows(seed.drawing_revision_id)
    assert rows_v1, "Expected rows after first materialization"
    row = rows_v1[0]
    assert row.algo_version == ROOM_ALGO_VERSION

    monkeypatch.setattr("app.jobs.room_materialization.ROOM_ALGO_VERSION", "999-test")

    # Reuse the original ROOMS job id (source_job_id has an FK to jobs).
    async with _get_session() as session:
        await materialize_rooms(
            session,
            job_id=row.source_job_id,
            project_id=row.project_id,
            source_file_id=row.source_file_id,
            drawing_revision_id=seed.drawing_revision_id,
            adapter_run_output_id=row.adapter_run_output_id,
            canonical_entity_schema_version=row.canonical_entity_schema_version,
        )
        await session.commit()

    rows_after = await _load_room_rows(seed.drawing_revision_id)
    algo_versions = {r.algo_version for r in rows_after}
    assert ROOM_ALGO_VERSION in algo_versions
    assert "999-test" in algo_versions
    assert len(rows_after) == 2 * len(rows_v1), (
        "New algo_version rows should coexist with the old ones, not replace them"
    )
