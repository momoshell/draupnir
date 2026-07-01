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


async def _fake_input_family(db: Any, revision_id: uuid.UUID) -> str | None:
    """No-DB stand-in for ``_resolve_input_family`` — unknown origin (DWG/DXF path)."""
    return None


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
    # Room labels now come from all text (#549); device-tag source kept for the override path.
    monkeypatch.setattr(rooms_route, "load_text_candidates", _fake_tags)
    monkeypatch.setattr(rooms_route, "load_tag_candidates", _fake_tags)
    monkeypatch.setattr(rooms_route, "_resolve_input_family", _fake_input_family)

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
    assert body["summary"] == {
        "rooms": 1,
        "named_rooms": 1,
        "assigned_devices": 1,
        "unlabeled_polygon_count": 0,
    }


async def test_rooms_endpoint_forced_wall_strategy_polygonizes_closed_boundary(
    rooms_app: FastAPI,
) -> None:
    # The fixture's A-ROOM polyline is a closed boundary. Wall-polygonization now selects
    # face-forming layers by polygonization yield (#554) — not only "wall"-named layers — so
    # forcing wall_polygonize recovers the closed face. (AUTO still prefers the explicit layer.)
    response = await _get_rooms(rooms_app, "?strategy=wall_polygonize")
    assert response.status_code == 200
    body = response.json()
    assert body["strategy"] == "wall_polygonize"
    assert len(body["items"]) == 1
    assert body["items"][0]["area"] == 100.0


async def test_rooms_endpoint_rejects_out_of_range_min_area(rooms_app: FastAPI) -> None:
    response = await _get_rooms(rooms_app, "?min_area=-1")
    assert response.status_code == 422


@pytest.fixture
def rooms_app_capture_scope(
    app: FastAPI, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[FastAPI, dict[str, Any]]]:
    """Like ``rooms_app`` but records the ``exclude_off_sheet`` kwarg each loader receives,
    so the #583 ``scope`` route param wiring can be asserted without a DB."""
    seen: dict[str, Any] = {}

    async def _no_db() -> AsyncGenerator[None, None]:
        yield None

    app.dependency_overrides[get_db] = _no_db

    async def _fake_manifest(revision_id: uuid.UUID, db: Any) -> SimpleNamespace:
        return _manifest()

    async def _fake_entities(
        db: Any, revision_id: uuid.UUID, entity_types: Sequence[str], **kw: Any
    ) -> list[SimpleNamespace]:
        seen["entities"] = kw.get("exclude_off_sheet")
        return [_entity("room-1", "A-ROOM", _closed_polyline(SQUARE))]

    async def _fake_devices(db: Any, revision_id: uuid.UUID, **_: Any) -> list[Device]:
        return []

    async def _fake_text(db: Any, revision_id: uuid.UUID, **kw: Any) -> list[_TagCandidate]:
        seen["labels"] = kw.get("exclude_off_sheet")
        return [_TagCandidate(entity_id="t1", text="Kitchen", layer_ref="A-ANNO", x=5.0, y=5.0)]

    monkeypatch.setattr(rooms_route, "_get_active_revision_manifest_or_409", _fake_manifest)
    monkeypatch.setattr(rooms_route, "load_revision_entities_by_type", _fake_entities)
    monkeypatch.setattr(rooms_route, "enumerate_devices", _fake_devices)
    monkeypatch.setattr(rooms_route, "load_text_candidates", _fake_text)
    monkeypatch.setattr(rooms_route, "_resolve_input_family", _fake_input_family)
    yield app, seen
    app.dependency_overrides.clear()


async def test_rooms_scope_defaults_to_printed_sheet(
    rooms_app_capture_scope: tuple[FastAPI, dict[str, Any]],
) -> None:
    """Default scope (#583) excludes off-sheet entities for BOTH geometry and labels."""
    application, seen = rooms_app_capture_scope
    transport = ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(f"/v1/revisions/{REVISION_ID}/rooms")
    assert response.status_code == 200
    assert seen["entities"] is True
    assert seen["labels"] is True


async def test_rooms_scope_modelspace_keeps_full_extent(
    rooms_app_capture_scope: tuple[FastAPI, dict[str, Any]],
) -> None:
    application, seen = rooms_app_capture_scope
    transport = ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(f"/v1/revisions/{REVISION_ID}/rooms?scope=modelspace")
    assert response.status_code == 200
    assert seen["entities"] is False
    assert seen["labels"] is False


async def test_rooms_scope_invalid_value_is_422(rooms_app: FastAPI) -> None:
    response = await _get_rooms(rooms_app, "?scope=bogus")
    assert response.status_code == 422


# --- Presentation filter tests (#778) ---

# Four closed polylines all on the same wall layer so wall_polygonize picks them all up.
# Two are labeled (one named-only, one name+number via "101 Plantroom" style); two are
# anonymous (name=None AND number=None) — the internal wall-polygon cells the registry
# retains for Phase-R conservation.
_WALL_LABELED_A = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]
_WALL_ANON_A = [(20.0, 0.0), (30.0, 0.0), (30.0, 10.0), (20.0, 10.0)]
_WALL_ANON_B = [(40.0, 0.0), (50.0, 0.0), (50.0, 10.0), (40.0, 10.0)]
_WALL_LABELED_B = [(60.0, 0.0), (70.0, 0.0), (70.0, 10.0), (60.0, 10.0)]


@pytest.fixture
def rooms_app_mixed(app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> Iterator[FastAPI]:
    """App fixture yielding 4 wall_polygonize rooms: 2 labeled, 2 anonymous.

    All polylines are on A-WALL so the auto strategy falls back to wall_polygonize (no
    explicit room layer).  Labels land inside the first and last polylines only.
    """

    async def _no_db() -> AsyncGenerator[None, None]:
        yield None

    app.dependency_overrides[get_db] = _no_db

    async def _fake_manifest(revision_id: uuid.UUID, db: Any) -> SimpleNamespace:
        return _manifest()

    async def _fake_entities(
        db: Any, revision_id: uuid.UUID, entity_types: Sequence[str], **_: Any
    ) -> list[SimpleNamespace]:
        return [
            _entity("w1", "A-WALL", _closed_polyline(_WALL_LABELED_A)),
            _entity("w2", "A-WALL", _closed_polyline(_WALL_ANON_A)),
            _entity("w3", "A-WALL", _closed_polyline(_WALL_ANON_B)),
            _entity("w4", "A-WALL", _closed_polyline(_WALL_LABELED_B)),
        ]

    async def _fake_devices(db: Any, revision_id: uuid.UUID, **_: Any) -> list[Device]:
        return []

    async def _fake_tags(db: Any, revision_id: uuid.UUID, **_: Any) -> list[_TagCandidate]:
        # "Plantroom" lands inside _WALL_LABELED_A (centroid ~5,5).
        # "Server Room" (name-only, no number) lands inside _WALL_LABELED_B (centroid ~65,5).
        # No label inside _WALL_ANON_A or _WALL_ANON_B → those remain anonymous.
        return [
            _TagCandidate(entity_id="t1", text="Plantroom", layer_ref="A-ANNO", x=5.0, y=5.0),
            _TagCandidate(entity_id="t2", text="Server Room", layer_ref="A-ANNO", x=65.0, y=5.0),
        ]

    monkeypatch.setattr(rooms_route, "_get_active_revision_manifest_or_409", _fake_manifest)
    monkeypatch.setattr(rooms_route, "load_revision_entities_by_type", _fake_entities)
    monkeypatch.setattr(rooms_route, "enumerate_devices", _fake_devices)
    monkeypatch.setattr(rooms_route, "load_text_candidates", _fake_tags)
    monkeypatch.setattr(rooms_route, "load_tag_candidates", _fake_tags)
    monkeypatch.setattr(rooms_route, "_resolve_input_family", _fake_input_family)

    yield app
    app.dependency_overrides.clear()


async def test_rooms_filter_excludes_anonymous_wall_polygon_cells(
    rooms_app_mixed: FastAPI,
) -> None:
    """Anonymous wall-polygon rooms (name=None AND number=None) are excluded from /rooms.

    Named-only and numbered rooms are retained. The suppressed count is reported in
    summary['unlabeled_polygon_count'] for transparency (#778).

    The wall_polygonize strategy is forced so all 4 polylines form rooms; only 2 are
    labeled — the other 2 are the anonymous internal cells.
    """
    transport = ASGITransport(app=rooms_app_mixed)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(f"/v1/revisions/{REVISION_ID}/rooms?strategy=wall_polygonize")

    assert response.status_code == 200
    body = response.json()
    assert body["strategy"] == "wall_polygonize"

    # Only labeled rooms appear in items; all items must have a name or number.
    for item in body["items"]:
        assert item["name"] is not None or item["number"] is not None, (
            f"Anonymous room leaked into response: {item}"
        )

    assert len(body["items"]) == 2

    # Transparency count: 2 anonymous cells were dropped.
    assert body["summary"]["unlabeled_polygon_count"] == 2
    assert body["summary"]["rooms"] == 2


async def test_rooms_filter_named_only_room_retained(rooms_app_mixed: FastAPI) -> None:
    """A room that has a name but no number is NOT anonymous — it must appear in /rooms."""
    transport = ASGITransport(app=rooms_app_mixed)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(f"/v1/revisions/{REVISION_ID}/rooms?strategy=wall_polygonize")

    body = response.json()
    item_names = {item["name"] for item in body["items"]}
    assert "Server Room" in item_names  # name-only room is retained
    # Verify no item has name=None AND number=None.
    for item in body["items"]:
        assert item["name"] is not None or item["number"] is not None


# Four closed polylines all on A-WALL (wall_polygonize); two are labeled with spec-prose
# note text ("ALL PIPEWORK SHALL BE PR", "ACCORDANCE WITH BUILDING") that the polygonizer
# stamps on the cell — they are NOT real room names. One has a real name ("Server Room").
# One is anonymous.
_PROSE_SQUARE_A = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]
_PROSE_SQUARE_B = [(20.0, 0.0), (30.0, 0.0), (30.0, 10.0), (20.0, 10.0)]
_PROSE_REAL_SQUARE = [(40.0, 0.0), (50.0, 0.0), (50.0, 10.0), (40.0, 10.0)]
_PROSE_ANON_SQUARE = [(60.0, 0.0), (70.0, 0.0), (70.0, 10.0), (60.0, 10.0)]


@pytest.fixture
def rooms_app_prose(app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> Iterator[FastAPI]:
    """App fixture with spec-prose-named and anonymous wall-polygon rooms.

    Two polylines are labeled with ALL-CAPS note text (spec-prose, not real names).
    One has a real name ("Server Room"). One is anonymous. The prose-named and anonymous
    cells must be suppressed by the route presentation filter (#778).
    """

    async def _no_db() -> AsyncGenerator[None, None]:
        yield None

    app.dependency_overrides[get_db] = _no_db

    async def _fake_manifest(revision_id: uuid.UUID, db: Any) -> SimpleNamespace:
        return _manifest()

    async def _fake_entities(
        db: Any, revision_id: uuid.UUID, entity_types: Sequence[str], **_: Any
    ) -> list[SimpleNamespace]:
        return [
            _entity("p1", "A-WALL", _closed_polyline(_PROSE_SQUARE_A)),
            _entity("p2", "A-WALL", _closed_polyline(_PROSE_SQUARE_B)),
            _entity("p3", "A-WALL", _closed_polyline(_PROSE_REAL_SQUARE)),
            _entity("p4", "A-WALL", _closed_polyline(_PROSE_ANON_SQUARE)),
        ]

    async def _fake_devices(db: Any, revision_id: uuid.UUID, **_: Any) -> list[Device]:
        return []

    async def _fake_tags(db: Any, revision_id: uuid.UUID, **_: Any) -> list[_TagCandidate]:
        # Spec-prose notes inside first two polygons; real name inside third; fourth unlabeled.
        return [
            _TagCandidate(
                entity_id="t1",
                text="ALL PIPEWORK SHALL BE PR",
                layer_ref="A-ANNO",
                x=5.0,
                y=5.0,
            ),
            _TagCandidate(
                entity_id="t2",
                text="ACCORDANCE WITH BUILDING",
                layer_ref="A-ANNO",
                x=25.0,
                y=5.0,
            ),
            _TagCandidate(entity_id="t3", text="Server Room", layer_ref="A-ANNO", x=45.0, y=5.0),
        ]

    monkeypatch.setattr(rooms_route, "_get_active_revision_manifest_or_409", _fake_manifest)
    monkeypatch.setattr(rooms_route, "load_revision_entities_by_type", _fake_entities)
    monkeypatch.setattr(rooms_route, "enumerate_devices", _fake_devices)
    monkeypatch.setattr(rooms_route, "load_text_candidates", _fake_tags)
    monkeypatch.setattr(rooms_route, "load_tag_candidates", _fake_tags)
    monkeypatch.setattr(rooms_route, "_resolve_input_family", _fake_input_family)

    yield app
    app.dependency_overrides.clear()


async def test_rooms_filter_excludes_spec_prose_named_cells(
    rooms_app_prose: FastAPI,
) -> None:
    """Spec-prose-named wall-polygon rooms are excluded from /rooms even though name != None.

    The M-560103 drawing stamps building-regulation note text ("ALL PIPEWORK SHALL BE PR",
    "ACCORDANCE WITH BUILDING") onto anonymous polygon cells — these are not real rooms.
    The extended _looks_like_room_name gate rejects multi-word all-caps strings (#778).
    """
    transport = ASGITransport(app=rooms_app_prose)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(f"/v1/revisions/{REVISION_ID}/rooms?strategy=wall_polygonize")

    assert response.status_code == 200
    body = response.json()

    item_names = [item["name"] for item in body["items"]]
    # Spec-prose must not appear in items.
    assert "ALL PIPEWORK SHALL BE PR" not in item_names
    assert "ACCORDANCE WITH BUILDING" not in item_names
    # Real name-only room must be retained.
    assert "Server Room" in item_names
    assert len(body["items"]) == 1
    # All 3 non-room entries (2 prose-named + 1 anonymous) suppressed.
    assert body["summary"]["unlabeled_polygon_count"] == 3
