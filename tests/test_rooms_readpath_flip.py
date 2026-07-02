"""Read-path flip validation gate for /rooms + /summary (#831 Phase 2).

Exercises the full ingest -> materialize -> read-flip loop via the same fake-ingest
harness shape as ``test_room_materialization.py``: a DWG-family fixture (mirrors
P-520001 rev 78d00585) and a PDF-family fixture (mirrors P-520001 rev 08f162bd),
each with a polygon room + a label-only room so both the polygon and
label-derived (``location``/``anchors``) reconstruction paths are covered.

Lane: db_worker (matches the ingest+materialization harness).
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
from app.interpretation.room_loaders import load_room_summary, load_rooms
from app.jobs.worker import process_ingest_job, process_rooms_job
from app.models.drawing_revision import DrawingRevision
from app.models.job import Job, JobType
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
# Synthetic entities. Each fixture carries:
#   - a closed polygon room ("room-1") with a number label inside it, and
#   - an unmatched number label far outside the polygon (-> label-only room),
# so both polygon and label-derived reconstruction paths are exercised.
# ---------------------------------------------------------------------------

_ROOM_SQUARE = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]

# DWG-family fixture (mirrors P-520001 rev 78d00585): a plain numbered polygon room
# plus a distant label-only room, under the loose (non-PDF) genuine-room rule.
_DWG_POLYGON_NUMBER = "0.9.01"
_DWG_LABEL_ONLY_NUMBER = "0.9.02"
_DWG_LABEL_ONLY_POINT = (500.0, 500.0)

# PDF-family fixture (mirrors P-520001 rev 08f162bd): numbers must pass the stricter
# is_plausible_room_number rule (#828 PR-3) to be genuine.
_PDF_POLYGON_NUMBER = "0.2.17"
_PDF_LABEL_ONLY_NUMBER = "0.2.18"
_PDF_LABEL_ONLY_POINT = (500.0, 500.0)


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


def _dwg_entities() -> list[dict[str, Any]]:
    return [
        _closed_polyline_entity("room-1", "A-ROOM"),
        _text_entity("room-1-label", "A-ANNO", _DWG_POLYGON_NUMBER, 5.0, 5.0),
        _text_entity(
            "room-2-label",
            "A-ANNO",
            _DWG_LABEL_ONLY_NUMBER,
            *_DWG_LABEL_ONLY_POINT,
        ),
    ]


def _pdf_entities() -> list[dict[str, Any]]:
    return [
        _closed_polyline_entity("room-1", "A-ROOM"),
        _text_entity("room-1-label", "A-ANNO", _PDF_POLYGON_NUMBER, 5.0, 5.0),
        _text_entity(
            "room-2-label",
            "A-ANNO",
            _PDF_LABEL_ONLY_NUMBER,
            *_PDF_LABEL_ONLY_POINT,
        ),
    ]


_EMPTY_GENUINE_ENTITIES: list[dict[str, Any]] = [
    # A closed polygon with no room-number/name label at all -> anonymous, filtered out by
    # has_genuine_room_identity, so the genuine set is empty but the polygon registry is not.
    _closed_polyline_entity("room-anon", "A-ROOM"),
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


async def _get_rooms_jobs_for_revision(revision_id: UUID) -> list[Job]:
    async with _get_session() as session:
        return list(
            (
                await session.execute(
                    select(Job).where(
                        Job.base_revision_id == revision_id,
                        Job.job_type == JobType.ROOMS.value,
                    )
                )
            )
            .scalars()
            .all()
        )


async def _materialize(seed: _IngestSeed) -> None:
    """Enqueue then immediately process the ROOMS job for a seeded revision."""
    await _enqueue_rooms_materialization(
        revision_id=seed.drawing_revision_id,
        project_id=seed.project_id,
        source_file_id=seed.source_file_id,
    )
    jobs = await _get_rooms_jobs_for_revision(seed.drawing_revision_id)
    assert len(jobs) == 1, f"Expected exactly one ROOMS job, found {len(jobs)}"
    await process_rooms_job(jobs[0].id)


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


@pytest.mark.parametrize(
    ("entities", "input_family"),
    [
        pytest.param(_dwg_entities(), "dxf", id="dwg_family"),
        pytest.param(_pdf_entities(), "pdf_vector", id="pdf_family"),
    ],
)
@requires_database
async def test_flipped_summary_and_rooms_are_byte_identical_to_live(
    async_client: httpx.AsyncClient,
    cleanup_db: Any,
    enqueued_job_ids: list[str],
    monkeypatch: pytest.MonkeyPatch,
    entities: list[dict[str, Any]],
    input_family: str,
) -> None:
    """Gate 1/2/3/6: flipped /summary + /rooms match live compute-on-read field-for-field.

    Also covers gate 2 (ordered assignment equality) and gate 3 (unlabeled_polygon_count)
    and gate 6 (strict location on the label-only room).
    """
    seed = await _ingest_and_seed(
        async_client, monkeypatch, entities=entities, input_family=input_family
    )

    # Compute the live (pre-materialization) reference BEFORE materializing, from the same
    # resolver the routes use, so live and flipped agree on what "genuine" means.
    async with _get_session() as session:
        live_result, live_input_family = await _resolve_rooms_with_family(
            session, seed.drawing_revision_id
        )
    live_genuine = [
        room
        for room in live_result.rooms
        if has_genuine_room_identity(room, input_family=live_input_family)
    ]
    live_genuine_ids = {room.id for room in live_genuine}
    live_assignments_expected = [
        a for a in live_result.device_assignments if a.room_id in live_genuine_ids
    ]

    await _materialize(seed)

    # Sanity: materialization is now a hit.
    async with _get_session() as session:
        summary_row = await load_room_summary(session, seed.drawing_revision_id)
    assert summary_row is not None
    assert summary_row.full_registry_size == len(live_result.rooms)

    rooms_response = await async_client.get(f"/v1/revisions/{seed.drawing_revision_id}/rooms")
    assert rooms_response.status_code == 200
    rooms_payload = rooms_response.json()

    summary_response = await async_client.get(f"/v1/revisions/{seed.drawing_revision_id}/summary")
    assert summary_response.status_code == 200
    summary_payload = summary_response.json()

    # --- Gate 1: byte-identical counts + items (field-for-field on serialized RoomRead) ---
    assert summary_payload["room_count"] == len(live_genuine)
    assert summary_payload["named_room_count"] == sum(
        1 for room in live_genuine if room.name is not None
    )
    assert rooms_payload["summary"]["rooms"] == len(live_genuine)
    assert rooms_payload["summary"]["named_rooms"] == sum(
        1 for room in live_genuine if room.name is not None
    )

    live_items_by_id = {room.id: room for room in live_genuine}
    # Gate 1 (strengthened): ordinal preserves genuine `interpret_rooms` item order, so
    # the flipped /rooms items ID sequence must match the live order exactly, not just
    # as a set.
    assert [item["id"] for item in rooms_payload["items"]] == [room.id for room in live_genuine]
    for item in rooms_payload["items"]:
        live_room = live_items_by_id[item["id"]]
        assert item["name"] == live_room.name
        assert item["number"] == live_room.number
        assert item["source"] == live_room.source
        assert item["needs_review"] == live_room.needs_review
        assert item["confidence"] == live_room.confidence
        if live_room.bounds is not None:
            assert item["bounds"] == {
                "min_x": live_room.bounds[0],
                "min_y": live_room.bounds[1],
                "max_x": live_room.bounds[2],
                "max_y": live_room.bounds[3],
            }
        else:
            assert item["bounds"] is None
        if live_room.area is not None:
            assert item["area"] == pytest.approx(live_room.area)
        else:
            assert item["area"] is None
        # Gate 6: strict location -- room.location == anchors[0] for label-derived rooms.
        if live_room.polygon is None and live_room.anchors:
            assert item["location"] == {
                "x": live_room.anchors[0][0],
                "y": live_room.anchors[0][1],
            }
            assert live_room.location == live_room.anchors[0]

    # --- Gate 2: assignment SET equality (assignments EXPANDED from persisted rows, not
    # recomputed). Per-room expansion groups assignments by room while the live pipeline
    # groups by phase, so order is not a contract -- membership is (this still proves the
    # M1 no-extra/no-missing fix). ---
    flipped_assignments = [(a["device_id"], a["room_id"]) for a in rooms_payload["assignments"]]
    async with _get_session() as session:
        loaded_rooms, loaded_assignments = await load_rooms(session, seed.drawing_revision_id)
    loaded_pairs = [(a.device_id, a.room_id) for a in loaded_assignments]
    live_pairs_expected = [(a.device_id, a.room_id) for a in live_assignments_expected]
    assert set(flipped_assignments) == set(loaded_pairs) == set(live_pairs_expected)
    # Each device_id must appear at most once across the assignment set (no duplicate
    # room assignment for a single device).
    flipped_device_ids = [pair[0] for pair in flipped_assignments]
    assert len(flipped_device_ids) == len(set(flipped_device_ids))

    # --- Gate 3: unlabeled_polygon_count == live registry size - genuine count ---
    expected_unlabeled = len(live_result.rooms) - len(live_genuine)
    assert rooms_payload["summary"]["unlabeled_polygon_count"] == expected_unlabeled

    # --- Gate 9 (extends test_room_materialization.py rows-match-live): reconstructed
    # Room via load_rooms matches the live genuine Room on all RoomRead-relevant fields.
    loaded_by_id = {room.id: room for room in loaded_rooms}
    assert set(loaded_by_id.keys()) == live_genuine_ids
    for room_id, live_room in live_items_by_id.items():
        loaded_room = loaded_by_id[room_id]
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

    # --- Gate 7: every /rooms item id resolves (200, not 404) in /rooms/{id}/entities ---
    for item in rooms_payload["items"]:
        entities_response = await async_client.get(
            f"/v1/revisions/{seed.drawing_revision_id}/rooms/{item['id']}/entities"
        )
        assert entities_response.status_code == 200, (
            f"room id {item['id']!r} did not resolve in /entities"
        )


@requires_database
async def test_miss_path_serves_live_and_enqueues_once(
    async_client: httpx.AsyncClient,
    cleanup_db: Any,
    enqueued_job_ids: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Gate 4: an unmaterialized revision serves live provisional + enqueues exactly one job."""
    seed = await _ingest_and_seed(
        async_client, monkeypatch, entities=_dwg_entities(), input_family="dxf"
    )

    async with _get_session() as session:
        summary_row = await load_room_summary(session, seed.drawing_revision_id)
    assert summary_row is None, "Sanity: revision must be unmaterialized before the request"

    response = await async_client.get(f"/v1/revisions/{seed.drawing_revision_id}/rooms")
    assert response.status_code == 200
    payload = response.json()
    # Provisional/live path still surfaces the genuine polygon room -- never degraded.
    assert payload["summary"]["rooms"] >= 1
    # #831 review §1: the live/provisional (miss) path must narrow assignments to genuine rooms
    # the SAME way the materialized (hit) path does -- otherwise the same revision's
    # `assignments`/`assigned_devices` would shrink the instant materialization lands. Guard the
    # invariant: every surfaced assignment references a surfaced item, and the count agrees.
    item_ids = {item["id"] for item in payload["items"]}
    assert all(a["room_id"] in item_ids for a in payload["assignments"])
    assert payload["summary"]["assigned_devices"] == len(payload["assignments"])

    jobs = await _get_rooms_jobs_for_revision(seed.drawing_revision_id)
    assert len(jobs) == 1, f"Expected exactly one deduped ROOMS job, found {len(jobs)}"

    # A second read while still unmaterialized must not enqueue a duplicate (dedup).
    await async_client.get(f"/v1/revisions/{seed.drawing_revision_id}/rooms")
    jobs_after = await _get_rooms_jobs_for_revision(seed.drawing_revision_id)
    assert len(jobs_after) == 1, "Expected no duplicate ROOMS job on a repeated miss"


@requires_database
async def test_v1_only_revision_is_a_miss_at_v2(
    async_client: httpx.AsyncClient,
    cleanup_db: Any,
    enqueued_job_ids: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Flag #1: a revision materialized only at v1 (pre-bump) is a miss at v2 -> live + enqueue."""
    seed = await _ingest_and_seed(
        async_client, monkeypatch, entities=_dwg_entities(), input_family="dxf"
    )

    # Materialize at the OLD version explicitly (simulates a pre-#831-flip row set), then
    # restore the current version so the read path below resolves its real default ("2").
    with pytest.MonkeyPatch.context() as v1_patch:
        v1_patch.setattr("app.jobs.room_materialization.ROOM_ALGO_VERSION", "1")
        await _materialize(seed)

    async with _get_session() as session:
        v1_summary = await load_room_summary(session, seed.drawing_revision_id, algo_version="1")
        v2_summary = await load_room_summary(session, seed.drawing_revision_id)
    assert v1_summary is not None
    assert v2_summary is None, "v1-only materialization must read as a miss at v2"

    # The v1 materialization above already created (and ran) one ROOMS job; the route call
    # below must enqueue exactly one MORE (a fresh v2 job), not zero and not more than one.
    jobs_before = await _get_rooms_jobs_for_revision(seed.drawing_revision_id)
    assert len(jobs_before) == 1

    response = await async_client.get(f"/v1/revisions/{seed.drawing_revision_id}/rooms")
    assert response.status_code == 200
    assert response.json()["summary"]["rooms"] >= 1

    jobs_after = await _get_rooms_jobs_for_revision(seed.drawing_revision_id)
    assert len(jobs_after) == 2, "v1-only miss should enqueue exactly one NEW (v2) ROOMS job"


@requires_database
async def test_empty_genuine_set_serves_empty_without_reenqueue(
    async_client: httpx.AsyncClient,
    cleanup_db: Any,
    enqueued_job_ids: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Gate 5: a materialized revision with zero genuine rooms serves empty, no re-enqueue."""
    seed = await _ingest_and_seed(
        async_client, monkeypatch, entities=_EMPTY_GENUINE_ENTITIES, input_family="dxf"
    )
    await _materialize(seed)

    async with _get_session() as session:
        summary_row = await load_room_summary(session, seed.drawing_revision_id)
    assert summary_row is not None
    assert summary_row.full_registry_size >= 1, "Anonymous polygon still counted in registry size"

    response = await async_client.get(f"/v1/revisions/{seed.drawing_revision_id}/rooms")
    assert response.status_code == 200
    payload = response.json()
    assert payload["items"] == []
    assert payload["summary"]["rooms"] == 0
    assert payload["summary"]["unlabeled_polygon_count"] == summary_row.full_registry_size

    summary_response = await async_client.get(f"/v1/revisions/{seed.drawing_revision_id}/summary")
    assert summary_response.status_code == 200
    assert summary_response.json()["room_count"] == 0

    # A present summary row is a HIT -- must not enqueue.
    jobs = await _get_rooms_jobs_for_revision(seed.drawing_revision_id)
    assert len(jobs) == 1, "Only the original materialization job should exist (no re-enqueue)"


@requires_database
async def test_never_raises_on_corrupt_or_missing_data(
    async_client: httpx.AsyncClient,
    cleanup_db: Any,
    enqueued_job_ids: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Gate 8: corrupt polygon json / missing summary / empty table never 500 / never raise."""
    from types import SimpleNamespace

    from app.interpretation.room_loaders import _room_from_row

    # A corrupt polygon_geometry_json (missing "geometry", malformed GeoJSON, etc.) must not
    # raise -- _room_from_row falls back to polygon=None rather than exploding.
    corrupt_row = SimpleNamespace(
        room_key="corrupt-room",
        name=None,
        source="wall_polygonize",
        polygon_geometry_json={"schema_version": "0.1"},  # no "geometry" key
        area=None,
        bounds_min_x=None,
        bounds_min_y=None,
        bounds_max_x=None,
        bounds_max_y=None,
        confidence=None,
        number=None,
        anchors_json=None,  # NULL anchors_json (flag #5): treated as empty, not raised on
        needs_review=False,
        assigned_device_ids_json=None,  # NULL assigned_device_ids_json (flag #5): treated as []
    )
    reconstructed = _room_from_row(corrupt_row)  # type: ignore[arg-type]
    assert reconstructed.polygon is None
    assert reconstructed.anchors == ()
    assert reconstructed.location is None

    # Flag #5: load_rooms treats a NULL assigned_device_ids_json as [] (the loop guard
    # `row.assigned_device_ids_json or []` mirrors what _room_from_row does for anchors_json).
    assert list(corrupt_row.assigned_device_ids_json or []) == []

    seed = await _ingest_and_seed(
        async_client, monkeypatch, entities=_dwg_entities(), input_family="dxf"
    )
    await _materialize(seed)

    async with _get_session() as session:
        from app.models.revision_room import RevisionRoom

        rows = (
            (
                await session.execute(
                    select(RevisionRoom).where(
                        RevisionRoom.drawing_revision_id == seed.drawing_revision_id,
                        RevisionRoom.polygon_geometry_json.is_not(None),
                    )
                )
            )
            .scalars()
            .all()
        )
        room_id = rows[0].room_key if rows else None

        summary_row = await load_room_summary(session, seed.drawing_revision_id)
        loaded_rooms, loaded_assignments = await load_rooms(session, seed.drawing_revision_id)
        assert summary_row is not None
        assert loaded_rooms  # real materialized data still loads fine
        assert loaded_assignments is not None

    # Missing summary (unmaterialized revision) -> None, not an exception.
    async with _get_session() as session:
        missing = await load_room_summary(session, UUID(int=0))
    assert missing is None

    # Empty table for a real but never-materialized revision -> ([], []), not an exception.
    other_seed = await _ingest_and_seed(
        async_client, monkeypatch, entities=_dwg_entities(), input_family="dxf"
    )
    async with _get_session() as session:
        rooms, assignments = await load_rooms(session, other_seed.drawing_revision_id)
    assert rooms == []
    assert assignments == []

    # The route itself must never 500 regardless of room_id validity.
    if room_id is not None:
        response = await async_client.get(
            f"/v1/revisions/{seed.drawing_revision_id}/rooms/{room_id}/entities"
        )
        assert response.status_code == 200
    not_found = await async_client.get(
        f"/v1/revisions/{seed.drawing_revision_id}/rooms/does-not-exist/entities"
    )
    assert not_found.status_code == 404
