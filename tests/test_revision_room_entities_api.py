"""Integration tests for the entities-in-room query.

Builds a revision with an explicit room layer carrying a big outer room and a
small nested inner room, plus point entities placed inside each. Asserts the
centroid-in-polygon, smallest-containing semantics end to end (real bbox SQL
prefilter + room interpretation), including the nested-room case.
"""

import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
import pytest

import app.jobs.worker as worker_module
from app.core.errors import ErrorCode
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunRequest
from app.jobs.worker import process_ingest_job
from tests.conftest import requires_database
from tests.jobs_test_helpers import _create_project, _get_job_for_file, _upload_file
from tests.test_ingest_output_persistence import (
    _build_contract_entity,
    _load_project_outputs,
    _replace_fake_canonical_payload,
)
from tests.test_jobs import _build_fake_ingest_payload


def _square(x0: float, y0: float, x1: float, y1: float) -> dict[str, Any]:
    return {
        "kind": "polyline",
        "closed": True,
        "vertices": [
            {"x": x0, "y": y0, "z": 0.0},
            {"x": x1, "y": y0, "z": 0.0},
            {"x": x1, "y": y1, "z": 0.0},
            {"x": x0, "y": y1, "z": 0.0},
        ],
    }


def _point(x: float, y: float) -> dict[str, Any]:
    return {"kind": "point", "position": {"x": x, "y": y}}


def _entity(
    entity_id: str, entity_type: str, layer: str, geometry: dict[str, Any]
) -> dict[str, Any]:
    payload = _build_contract_entity(
        entity_id=entity_id,
        entity_type=entity_type,
        layer_ref=layer,
        source_id=f"src-{entity_id}",
    )
    payload["geometry_json"] = geometry
    return payload


def _nested_rooms_runner() -> Callable[[IngestionRunRequest], Awaitable[IngestFinalizationPayload]]:
    """Fake runner: an outer room (0,0)-(100,100) with a nested inner room (10,10)-(30,30)."""

    async def _run(request: IngestionRunRequest) -> IngestFinalizationPayload:
        payload = _build_fake_ingest_payload(request)
        return _replace_fake_canonical_payload(
            payload,
            layouts=[{"layout_ref": "Model", "name": "Model"}],
            layers=[
                {"layer_ref": "A-ROOM", "name": "A-ROOM"},
                {"layer_ref": "E-DEVICE", "name": "E-DEVICE"},
            ],
            blocks=[],
            entities=[
                _entity("room-outer", "polyline", "A-ROOM", _square(0, 0, 100, 100)),
                _entity("room-inner", "polyline", "A-ROOM", _square(10, 10, 30, 30)),
                _entity("ent-inner", "point", "E-DEVICE", _point(20, 20)),
                _entity("ent-outer", "point", "E-DEVICE", _point(60, 60)),
                _entity("ent-outside", "point", "E-DEVICE", _point(200, 200)),
            ],
        )

    return _run


def _ids(body: dict[str, Any]) -> list[str]:
    return [item["entity_id"] for item in body["items"]]


@requires_database
class TestRevisionRoomEntitiesApi:
    """Tests for ``GET /revisions/{revision_id}/rooms/{room_id}/entities``."""

    async def _ingest(self, async_client: httpx.AsyncClient) -> Any:
        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await process_ingest_job(job.id)
        _outputs, drawing_revisions, _reports, _artifacts = await _load_project_outputs(
            project["id"]
        )
        return drawing_revisions[0]

    async def test_nested_rooms_use_smallest_containing(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An entity inside a nested room is attributed to the inner room, not the outer."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        monkeypatch.setattr(worker_module, "run_ingestion", _nested_rooms_runner())

        rid = (await self._ingest(async_client)).id
        base = f"/v1/revisions/{rid}/rooms"

        inner = (await async_client.get(f"{base}/room-inner/entities?layer_ref=E-DEVICE")).json()
        outer = (await async_client.get(f"{base}/room-outer/entities?layer_ref=E-DEVICE")).json()

        # ent-inner falls inside both rooms but is attributed to the smaller inner room.
        assert _ids(inner) == ["ent-inner"]
        assert inner["total"] == 1
        assert inner["room"]["id"] == "room-inner"

        # The outer room sees only the device whose smallest containing room is the outer.
        assert _ids(outer) == ["ent-outer"]
        assert "ent-inner" not in _ids(outer)
        assert outer["room"]["id"] == "room-outer"

        # ent-outside is in no room.
        assert "ent-outside" not in _ids(inner) + _ids(outer)

    async def test_default_is_inclusive_with_projection_and_pagination(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Unfiltered listing includes the boundary polyline; fields/pagination compose."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        monkeypatch.setattr(worker_module, "run_ingestion", _nested_rooms_runner())

        rid = (await self._ingest(async_client)).id
        base = f"/v1/revisions/{rid}/rooms/room-outer/entities"

        full = (await async_client.get(base)).json()
        # Outer room contains its own boundary polyline (centroid 50,50) + ent-outer,
        # but not the nested inner room's geometry/device.
        assert set(_ids(full)) == {"room-outer", "ent-outer"}
        assert full["total"] == 2
        assert "geometry" not in full["items"][0] or full["items"][0]["geometry"] is None

        # Heavy-block projection opts geometry in.
        projected = (await async_client.get(f"{base}?fields=geometry")).json()
        assert all(item["geometry"] is not None for item in projected["items"])

        # Offset pagination over the computed set.
        page1 = (await async_client.get(f"{base}?limit=1")).json()
        assert len(page1["items"]) == 1
        assert page1["total"] == 2
        assert page1["next_cursor"] is not None
        page2 = (await async_client.get(f"{base}?limit=1&cursor={page1['next_cursor']}")).json()
        assert len(page2["items"]) == 1
        assert set(_ids(page1) + _ids(page2)) == {"room-outer", "ent-outer"}

    async def test_unknown_room_returns_not_found(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An unknown room id (for a materialized revision) returns a 404 envelope."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        monkeypatch.setattr(worker_module, "run_ingestion", _nested_rooms_runner())

        rid = (await self._ingest(async_client)).id
        response = await async_client.get(f"/v1/revisions/{rid}/rooms/room-nope/entities")

        assert response.status_code == 404
        assert response.json()["error"]["code"] == ErrorCode.NOT_FOUND.value

    async def test_missing_revision_returns_not_found(
        self,
        async_client: httpx.AsyncClient,
    ) -> None:
        """An unknown revision id returns the standard not-found error envelope."""
        _ = self

        response = await async_client.get(f"/v1/revisions/{uuid.uuid4()}/rooms/room-1/entities")

        assert response.status_code == 404
        assert response.json()["error"]["code"] == ErrorCode.NOT_FOUND.value
