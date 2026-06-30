"""Integration tests for the per-revision orientation summary API.

The summary is a pure aggregation; these tests assert each aggregated field
matches the default response of its dedicated underlying resource.
"""

import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from httpx import ASGITransport

import app.api.v1.revision_routes.summary as summary_route
import app.jobs.worker as worker_module
from app.core.errors import ErrorCode
from app.db.session import get_db
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunRequest
from app.interpretation.geometry import build_polygon
from app.interpretation.room_pipeline import RoomInterpretation
from app.interpretation.rooms import room_from_polygon
from app.jobs.worker import process_ingest_job
from app.schemas.revision import RevisionScaleRead
from tests.conftest import requires_database
from tests.jobs_test_helpers import _create_project, _get_job_for_file, _upload_file
from tests.test_ingest_output_persistence import (
    _build_contract_entity,
    _load_project_outputs,
    _replace_fake_canonical_payload,
)
from tests.test_jobs import _build_fake_ingest_payload

_REVISION_ID = uuid.uuid4()
_SQUARE = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]


def _manifest() -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        source_file_id=uuid.uuid4(),
        extraction_profile_id=None,
        source_job_id=uuid.uuid4(),
        drawing_revision_id=_REVISION_ID,
        adapter_run_output_id=None,
        canonical_entity_schema_version="1",
        counts_json={"layouts": 1, "layers": 0, "blocks": 0, "entities": 0},
        created_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
    )


_DXF_UNITS = {
    "normalized": "millimeter",
    "source": "$INSUNITS",
    "source_value": 4,
    "conversion_target": "meter",
    "conversion_factor": 0.001,
}


def _materialized_runner() -> Callable[[IngestionRunRequest], Awaitable[IngestFinalizationPayload]]:
    """Fake ``run_ingestion`` producing a small multi-layer revision with units."""

    async def _run(request: IngestionRunRequest) -> IngestFinalizationPayload:
        payload = _build_fake_ingest_payload(request)
        payload = _replace_fake_canonical_payload(
            payload,
            layouts=[{"layout_ref": "Model", "name": "Model"}],
            layers=[
                {"layer_ref": "A-WALL", "name": "A-WALL"},
                {"layer_ref": "A-DOOR", "name": "A-DOOR"},
                {"layer_ref": "M-DUCT", "name": "M-DUCT"},
            ],
            blocks=[{"block_ref": "DOOR-1", "name": "DOOR-1"}],
            entities=[
                _build_contract_entity(
                    entity_id="device-1",
                    entity_type="insert",
                    layer_ref="A-DOOR",
                    block_ref="DOOR-1",
                    source_id="src-device-1",
                ),
                _build_contract_entity(
                    entity_id="wall-1",
                    entity_type="line",
                    layer_ref="A-WALL",
                    source_id="src-wall-1",
                    source_hash="a" * 64,
                ),
            ],
        )
        canonical = {**payload.canonical_json, "units": _DXF_UNITS}
        return replace(payload, canonical_json=canonical)

    return _run


@requires_database
class TestRevisionSummaryApi:
    """Tests for ``GET /revisions/{revision_id}/summary``."""

    async def _ingest(self, async_client: httpx.AsyncClient) -> Any:
        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await process_ingest_job(job.id)
        _outputs, drawing_revisions, _reports, _artifacts = await _load_project_outputs(
            project["id"]
        )
        return drawing_revisions[0]

    async def test_summary_aggregates_and_matches_underlying_resources(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """One call returns counts/scale/coverage matching the dedicated endpoints."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        monkeypatch.setattr(worker_module, "run_ingestion", _materialized_runner())

        revision = await self._ingest(async_client)
        rid = revision.id

        summary = (await async_client.get(f"/v1/revisions/{rid}/summary")).json()
        entities = (await async_client.get(f"/v1/revisions/{rid}/entities")).json()
        layer_roles = (await async_client.get(f"/v1/revisions/{rid}/layer-roles")).json()
        devices = (await async_client.get(f"/v1/revisions/{rid}/devices")).json()
        rooms = (await async_client.get(f"/v1/revisions/{rid}/rooms")).json()
        scale = (await async_client.get(f"/v1/revisions/{rid}/scale")).json()
        report = (await async_client.get(f"/v1/revisions/{rid}/validation-report")).json()

        assert summary["revision_id"] == str(rid)

        # Entity counts mirror the materialization manifest.
        assert summary["entity_counts"] == entities["manifest"]["counts"]
        assert summary["entity_counts"]["layers"] == 3
        assert summary["entity_counts"]["entities"] == 2

        # Layers + roles mirror /layer-roles.
        assert summary["layer_count"] == len(layer_roles["items"])
        assert summary["layer_roles"] == layer_roles["summary"]["counts"]

        # Devices mirror /devices schedule total.
        assert summary["device_count"] == devices["association"]["total_devices"]

        # Rooms mirror /rooms summary.
        assert summary["room_count"] == rooms["summary"]["rooms"]
        assert summary["named_room_count"] == rooms["summary"]["named_rooms"]

        # Scale mirrors /scale.
        assert summary["scale"] == scale
        assert summary["scale"]["units"]["normalized"] == "millimeter"

        # Coverage mirrors /validation-report; by_type echoes coverage entities.
        assert summary["coverage"] == report.get("coverage")
        expected_by_type = (report.get("coverage") or {}).get("entities", {}).get("by_type", {})
        assert summary["entities_by_type"] == expected_by_type

    async def test_summary_room_counts_match_rooms_when_labels_off_tag_layer(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Regression for #584: /summary and /rooms must agree even when room labels sit on a
        non-device-tag layer. The old /summary path built labels from the device-tag loader and
        missed them, reporting a different room count than /rooms (which uses all text)."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        def _text(entity_id: str, layer: str, text: str, x: float, y: float) -> dict[str, Any]:
            entity = _build_contract_entity(
                entity_id=entity_id,
                entity_type="text",
                layer_ref=layer,
                source_id=f"s-{entity_id}",
            )
            entity["geometry_json"] = {"type": "text", "text": text, "insertion": {"x": x, "y": y}}
            return entity

        async def _run(request: IngestionRunRequest) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            return _replace_fake_canonical_payload(
                payload,
                layouts=[{"layout_ref": "Model", "name": "Model"}],
                layers=[
                    {"layer_ref": "A-IDEN", "name": "A-IDEN"},  # room-label layer (no tag token)
                    {"layer_ref": "Device Tags", "name": "Device Tags"},  # matches a tag token
                ],
                blocks=[],
                entities=[
                    _text("rname", "A-IDEN", "PH Plantroom", 5.0, 5.4),
                    _text("rnum", "A-IDEN", "0.9.01", 5.0, 5.0),
                    _text("dtag", "Device Tags", "H", 50.0, 50.0),
                ],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run)
        revision = await self._ingest(async_client)
        rid = revision.id

        summary = (await async_client.get(f"/v1/revisions/{rid}/summary")).json()
        rooms = (await async_client.get(f"/v1/revisions/{rid}/rooms")).json()

        # Meaningful (not 0==0): the fixture yields a real named, numbered label room.
        assert rooms["summary"]["named_rooms"] >= 1
        assert summary["room_count"] == rooms["summary"]["rooms"]
        assert summary["named_room_count"] == rooms["summary"]["named_rooms"]

    async def test_missing_revision_returns_not_found(
        self,
        async_client: httpx.AsyncClient,
    ) -> None:
        """An unknown revision id returns the standard not-found error envelope."""
        _ = self

        response = await async_client.get(f"/v1/revisions/{uuid.uuid4()}/summary")

        assert response.status_code == 404
        assert response.json()["error"]["code"] == ErrorCode.NOT_FOUND.value


# ---------------------------------------------------------------------------
# No-DB fixture tests: summary genuine-room filter (#792)
# ---------------------------------------------------------------------------
# These tests patch _resolve_rooms and all other async dependencies directly
# so they run without a database, mirroring the test_rooms_api.py pattern.
# ---------------------------------------------------------------------------


class _FakeScalarResult:
    """Minimal stand-in for the SQLAlchemy scalar result returned by db.scalars()."""

    def all(self) -> list[Any]:
        return []


class _FakeDb:
    """Minimal async DB stub that returns no layers (empty scalar query)."""

    async def scalars(self, _query: Any) -> _FakeScalarResult:
        return _FakeScalarResult()


def _polygon_room_genuine(room_id: str, name: str) -> Any:
    """Polygon room with a real mixed-case name (passes the genuine filter)."""
    polygon = build_polygon(_SQUARE)
    assert polygon is not None
    return room_from_polygon(room_id, polygon, source="test", name=name)


def _polygon_room_anon(room_id: str) -> Any:
    """Anonymous polygon room (name=None, number=None — fails the genuine filter)."""
    polygon = build_polygon(_SQUARE)
    assert polygon is not None
    return room_from_polygon(room_id, polygon, source="test")


def _polygon_room_prose(room_id: str, name: str) -> Any:
    """Polygon room stamped with spec-prose text (fails the genuine filter)."""
    polygon = build_polygon(_SQUARE)
    assert polygon is not None
    return room_from_polygon(room_id, polygon, source="test", name=name)


def _summary_app_with_rooms(
    app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
    rooms: list[Any],
) -> FastAPI:
    """Patch the summary route to return the given room list, with no DB needed."""

    async def _no_db() -> AsyncGenerator[Any, None]:
        yield _FakeDb()

    app.dependency_overrides[get_db] = _no_db

    async def _fake_manifest(revision_id: uuid.UUID, db: Any) -> SimpleNamespace:
        return _manifest()

    async def _fake_resolve_rooms(db: Any, revision_id: uuid.UUID, **_: Any) -> RoomInterpretation:
        return RoomInterpretation(
            rooms=rooms,
            device_assignments=[],
            strategy="explicit_layer",
            source_layers=(),
        )

    async def _fake_devices(db: Any, revision_id: uuid.UUID, **_: Any) -> list[Any]:
        return []

    async def _fake_scale(revision_id: uuid.UUID, db: Any) -> RevisionScaleRead:
        return RevisionScaleRead(
            units={"normalized": "unknown"},
            units_confidence="unknown",
            real_world_dimensions_available=False,
            units_contradicted=False,
            pdf_scale=None,
            source_input_family=None,
        )

    async def _fake_validation_report(revision_id: uuid.UUID, db: Any) -> None:
        return None

    monkeypatch.setattr(summary_route, "_get_active_revision_manifest_or_409", _fake_manifest)
    monkeypatch.setattr(summary_route, "_resolve_rooms", _fake_resolve_rooms)
    monkeypatch.setattr(summary_route, "enumerate_devices", _fake_devices)
    monkeypatch.setattr(summary_route, "resolve_revision_scale", _fake_scale)
    monkeypatch.setattr(summary_route, "_get_active_validation_report", _fake_validation_report)
    return app


async def test_summary_room_count_filters_out_junk_rooms(
    app: FastAPI, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Summary room_count mirrors /rooms genuine-room filter: junk rooms excluded (#792).

    Raw rooms_result contains 3 genuine rooms and 2 junk entries (1 anonymous, 1
    spec-prose-named). room_count must equal 3 and named_room_count must equal 3
    (all genuine rooms have real names here).
    """
    raw_rooms = [
        _polygon_room_genuine("r1", "Cooling Plantroom"),
        _polygon_room_genuine("r2", "Server Room"),
        _polygon_room_genuine("r3", "Heating & Hot Water"),
        _polygon_room_anon("anon-1"),  # anonymous — must be excluded
        _polygon_room_prose("prose-1", "ALL PIPEWORK SHALL BE PR"),  # spec-prose — excluded
    ]
    patched_app = _summary_app_with_rooms(app, monkeypatch, raw_rooms)
    try:
        transport = ASGITransport(app=patched_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(f"/v1/revisions/{_REVISION_ID}/summary")
        assert response.status_code == 200
        body = response.json()
        assert body["room_count"] == 3, f"Expected 3 genuine rooms, got {body['room_count']}"
        assert body["named_room_count"] == 3
    finally:
        app.dependency_overrides.clear()


async def test_summary_named_room_count_uses_genuine_rooms_only(
    app: FastAPI, monkeypatch: pytest.MonkeyPatch
) -> None:
    """named_room_count counts only genuine rooms that have a name — not junk rooms (#792).

    Mix: 2 genuine named, 1 anonymous, 1 spec-prose. named_room_count must be 2.
    """
    raw_rooms = [
        _polygon_room_genuine("r1", "Plantroom"),
        _polygon_room_genuine("r2", "WC"),
        _polygon_room_anon("anon-1"),
        _polygon_room_prose("prose-1", "ACCORDANCE WITH BUILDING"),
    ]
    patched_app = _summary_app_with_rooms(app, monkeypatch, raw_rooms)
    try:
        transport = ASGITransport(app=patched_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(f"/v1/revisions/{_REVISION_ID}/summary")
        assert response.status_code == 200
        body = response.json()
        assert body["room_count"] == 2
        assert body["named_room_count"] == 2
    finally:
        app.dependency_overrides.clear()


async def test_summary_room_count_all_genuine_is_noop(
    app: FastAPI, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DWG-style drawings: when all rooms are genuine the filter is a no-op (#792).

    All 4 rooms have real names; room_count and named_room_count both equal 4.
    """
    raw_rooms = [
        _polygon_room_genuine("r1", "Kitchen"),
        _polygon_room_genuine("r2", "Office"),
        _polygon_room_genuine("r3", "Lobby"),
        _polygon_room_genuine("r4", "Plant Room"),
    ]
    patched_app = _summary_app_with_rooms(app, monkeypatch, raw_rooms)
    try:
        transport = ASGITransport(app=patched_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(f"/v1/revisions/{_REVISION_ID}/summary")
        assert response.status_code == 200
        body = response.json()
        assert body["room_count"] == 4
        assert body["named_room_count"] == 4
    finally:
        app.dependency_overrides.clear()
