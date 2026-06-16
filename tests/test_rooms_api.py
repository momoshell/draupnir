"""Route-level tests for the /rooms endpoint (issue #479).

These exercise the full request → response path (routing, loaders seam, the
interpretation pipeline, and schema serialization) with the DB loaders overridden
by in-memory fixtures, so they run without a database.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator, Iterator, Sequence
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from httpx import ASGITransport

import app.api.v1.revision_routes.rooms as rooms_route
from app.db.session import get_db
from app.interpretation.devices import Device, _TagCandidate

REVISION_ID = uuid.uuid4()


def _manifest() -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        source_file_id=uuid.uuid4(),
        extraction_profile_id=None,
        source_job_id=uuid.uuid4(),
        drawing_revision_id=REVISION_ID,
        adapter_run_output_id=None,
        canonical_entity_schema_version="1",
        counts_json={"layouts": 1, "layers": 2, "blocks": 0, "entities": 5},
        created_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
    )


def _entity(entity_id: str, layer: str, geometry: dict[str, Any]) -> SimpleNamespace:
    return SimpleNamespace(
        entity_id=entity_id,
        layer_ref=layer,
        geometry_json=geometry,
        sequence_index=0,
        block_ref=None,
    )


def _closed_polyline(vertices: list[tuple[float, float]]) -> dict[str, Any]:
    return {
        "kind": "polyline",
        "closed": True,
        "vertices": [{"x": x, "y": y, "z": 0.0} for x, y in vertices],
    }


SQUARE = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]


@pytest.fixture
def rooms_app(app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> Iterator[FastAPI]:
    """App with the room loaders patched to in-memory fixtures (no DB)."""

    async def _no_db() -> AsyncGenerator[None, None]:
        yield None

    app.dependency_overrides[get_db] = _no_db

    async def _fake_manifest(revision_id: uuid.UUID, db: Any) -> SimpleNamespace:
        return _manifest()

    async def _fake_entities(
        db: Any, revision_id: uuid.UUID, entity_types: Sequence[str], **_: Any
    ) -> list[SimpleNamespace]:
        return [_entity("room-1", "A-ROOM", _closed_polyline(SQUARE))]

    async def _fake_devices(db: Any, revision_id: uuid.UUID, **_: Any) -> list[Device]:
        return [
            Device(
                entity_id="dev-1",
                sequence_index=0,
                depth=0,
                block_ref=None,
                layer_ref="E-DEVICE",
                position={"x": 5.0, "y": 5.0},
                tag=None,
            ),
            Device(
                entity_id="dev-out",
                sequence_index=1,
                depth=0,
                block_ref=None,
                layer_ref="E-DEVICE",
                position={"x": 99.0, "y": 99.0},
                tag=None,
            ),
        ]

    async def _fake_tags(db: Any, revision_id: uuid.UUID, **_: Any) -> list[_TagCandidate]:
        return [_TagCandidate(entity_id="t1", text="Kitchen", layer_ref="A-ANNO", x=5.0, y=5.0)]

    monkeypatch.setattr(rooms_route, "_get_active_revision_manifest_or_409", _fake_manifest)
    monkeypatch.setattr(rooms_route, "load_revision_entities_by_type", _fake_entities)
    monkeypatch.setattr(rooms_route, "enumerate_devices", _fake_devices)
    monkeypatch.setattr(rooms_route, "load_tag_candidates", _fake_tags)

    yield app
    app.dependency_overrides.clear()


async def _get_rooms(rooms_app: FastAPI, query: str = "") -> httpx.Response:
    transport = ASGITransport(app=rooms_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(f"/v1/revisions/{REVISION_ID}/rooms{query}")


async def test_rooms_endpoint_returns_named_rooms_and_assignments(rooms_app: FastAPI) -> None:
    response = await _get_rooms(rooms_app)
    assert response.status_code == 200
    body = response.json()

    assert body["strategy"] == "explicit_layer"
    assert body["source_layers"] == ["A-ROOM"]
    assert len(body["items"]) == 1
    room = body["items"][0]
    assert room["id"] == "room-1"
    assert room["name"] == "Kitchen"
    assert room["source"] == "explicit_layer"
    assert room["area"] == 100.0
    assert room["bounds"] == {"min_x": 0.0, "min_y": 0.0, "max_x": 10.0, "max_y": 10.0}
    assert room["confidence"] is None  # explicit layer is taken at face value

    # Inside device assigned; outside device omitted.
    assert body["assignments"] == [{"device_id": "dev-1", "room_id": "room-1"}]
    assert body["summary"] == {"rooms": 1, "named_rooms": 1, "assigned_devices": 1}


async def test_rooms_endpoint_forced_wall_strategy_finds_nothing_for_room_layer(
    rooms_app: FastAPI,
) -> None:
    # The fixture only has an A-ROOM polyline; forcing wall_polygonize yields no rooms.
    response = await _get_rooms(rooms_app, "?strategy=wall_polygonize")
    assert response.status_code == 200
    body = response.json()
    assert body["strategy"] == "wall_polygonize"
    assert body["items"] == []
    assert body["assignments"] == []


async def test_rooms_endpoint_rejects_out_of_range_min_area(rooms_app: FastAPI) -> None:
    response = await _get_rooms(rooms_app, "?min_area=-1")
    assert response.status_code == 422
