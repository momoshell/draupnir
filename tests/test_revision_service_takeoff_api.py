"""Route-level tests for GET /revisions/{id}/service-takeoff (issue #606, P3 / be-p3-03).

Mirrors test_rooms_api.py: the DB loaders are monkeypatched with in-memory fixtures
so these run without a database. Three scenarios:

1. A revision with routed pipes + tags + confirmed scale -> 200, item with positive
   real_length_m, basis='real_world', unscaled=False.
2. A revision with no adapter run (no scale) -> 200, unscaled=True or empty items,
   real_length_m null.
3. ADR-005 regression: the new route module does NOT import app.estimating.quantities.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator, Iterator
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from httpx import ASGITransport

import app.api.v1.revision_routes.service_takeoff as service_takeoff_route
from app.db.session import get_db
from app.interpretation.measurement import ScaleContext
from app.interpretation.rooms import Room
from app.interpretation.service_takeoff_loaders import ServiceTakeoffInputs

REVISION_ID = uuid.uuid4()

# A confirmed metric scale: 1 drawing unit = 0.001 m (mm -> m).
_CONFIRMED_SCALE = ScaleContext(
    conversion_factor=0.001,
    real_world_available=True,
    contradicted=False,
    units_confidence="confirmed",
)

_NO_SCALE = ScaleContext(
    conversion_factor=None,
    real_world_available=False,
    contradicted=False,
    units_confidence="unknown",
)


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


def _line_entity_geometry(x0: float, y0: float, x1: float, y1: float) -> dict[str, Any]:
    """Geometry dict for a line entity usable by the measurement module."""
    return {
        "start": [x0, y0, 0.0],
        "end": [x1, y1, 0.0],
    }


# ---------------------------------------------------------------------------
# Fixture helpers: minimal ServiceTakeoffInputs bundles
# ---------------------------------------------------------------------------


def _inputs_with_pipe_and_tag(scale: ScaleContext) -> ServiceTakeoffInputs:
    """A minimal inputs bundle: one 1000-unit pipe line on 'PIPE' layer, red colour,
    one tag '50' on layer 'PIPE TAG' near the pipe midpoint.

    With confirmed scale (0.001 m/unit) the line is 1.0 m.
    """
    from app.interpretation.routed_runs import RoutedEntity
    from app.interpretation.run_service_identity import TagPlacement
    from app.interpretation.service_legend import ServiceLegend

    entity_id = "pipe-1"
    color: dict[str, Any] = {"index": 1, "rgb": "#ff0000"}
    geometry = _line_entity_geometry(0.0, 0.0, 1000.0, 0.0)

    routed_entities = [
        RoutedEntity(
            entity_id=entity_id,
            entity_type="line",
            layer_ref="PIPE",
            color=color,
            geometry=geometry,
        )
    ]

    # Legend: colour index 1 (colour_key "#ff0000") -> "VAC" (vacuum).
    # colour_key for rgb="#ff0000" is "#ff0000" (lowercased).
    from app.interpretation.service_legend import ServiceEntry

    entry = ServiceEntry(
        colour_key="#ff0000",
        colour_index=1,
        colour_rgb="#ff0000",
        discipline="medical-gas",
        abbreviation="VAC",
        description="Vacuum",
        sources=("swatch",),
        confidence=None,
        competing_disciplines=(),
    )
    legend = ServiceLegend(entries=(entry,))

    # Tag near the pipe midpoint (500, 0). Pattern: "NN SERVICE" (round, no mm).
    tag_placements = [
        TagPlacement(text="50 VAC", point=(500.0, 0.0), layer_ref="PIPE TAG"),
    ]

    return ServiceTakeoffInputs(
        routed_entities=routed_entities,
        legend=legend,
        tag_placements=tag_placements,
        geometry_by_entity_id={entity_id: geometry},
        scale=scale,
        rise_entities=[],
        drop_entities=[],
    )


def _inputs_empty(scale: ScaleContext) -> ServiceTakeoffInputs:
    """Empty inputs: no routed entities, no legend, no tags."""
    from app.interpretation.service_legend import ServiceLegend

    return ServiceTakeoffInputs(
        routed_entities=[],
        legend=ServiceLegend(entries=()),
        tag_placements=[],
        geometry_by_entity_id={},
        scale=scale,
        rise_entities=[],
        drop_entities=[],
    )


# ---------------------------------------------------------------------------
# Common no-room room-result (pipe runs will be unassigned)
# ---------------------------------------------------------------------------


class _FakeRoomInterpretation(SimpleNamespace):
    """Minimal duck-type for RoomInterpretation returned by _resolve_rooms."""

    rooms: list[Room]
    strategy: str
    source_layers: list[str]
    device_assignments: list[Any]


def _no_rooms() -> _FakeRoomInterpretation:
    ns = _FakeRoomInterpretation()
    ns.rooms = []
    ns.strategy = "explicit_layer"
    ns.source_layers = []
    ns.device_assignments = []
    return ns


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def takeoff_app_confirmed_scale(app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> Iterator[FastAPI]:
    """App with loaders patched for a revision that has a 1000-unit pipe line and
    confirmed scale (1 drawing unit = 0.001 m).
    """

    async def _no_db() -> AsyncGenerator[None, None]:
        yield None

    app.dependency_overrides[get_db] = _no_db

    async def _fake_manifest(revision_id: uuid.UUID, db: Any) -> SimpleNamespace:
        return _manifest()

    async def _fake_inputs(
        db: Any,
        revision_id: uuid.UUID,
        *,
        layer_refs: Any = None,
        tag_layers: Any = None,
        legend_layers: Any = None,
        exclude_off_sheet: bool = True,
    ) -> ServiceTakeoffInputs:
        return _inputs_with_pipe_and_tag(_CONFIRMED_SCALE)

    async def _fake_rooms(db: Any, revision_id: uuid.UUID, **_: Any) -> _FakeRoomInterpretation:
        return _no_rooms()

    monkeypatch.setattr(
        service_takeoff_route, "_get_active_revision_manifest_or_409", _fake_manifest
    )
    monkeypatch.setattr(service_takeoff_route, "load_service_takeoff_inputs", _fake_inputs)
    monkeypatch.setattr(service_takeoff_route, "_resolve_rooms", _fake_rooms)

    yield app
    app.dependency_overrides.clear()


@pytest.fixture
def takeoff_app_no_scale(app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> Iterator[FastAPI]:
    """App with loaders patched for a revision with no adapter run (unknown scale)."""

    async def _no_db() -> AsyncGenerator[None, None]:
        yield None

    app.dependency_overrides[get_db] = _no_db

    async def _fake_manifest(revision_id: uuid.UUID, db: Any) -> SimpleNamespace:
        return _manifest()

    async def _fake_inputs(
        db: Any,
        revision_id: uuid.UUID,
        *,
        layer_refs: Any = None,
        tag_layers: Any = None,
        legend_layers: Any = None,
        exclude_off_sheet: bool = True,
    ) -> ServiceTakeoffInputs:
        return _inputs_empty(_NO_SCALE)

    async def _fake_rooms(db: Any, revision_id: uuid.UUID, **_: Any) -> _FakeRoomInterpretation:
        return _no_rooms()

    monkeypatch.setattr(
        service_takeoff_route, "_get_active_revision_manifest_or_409", _fake_manifest
    )
    monkeypatch.setattr(service_takeoff_route, "load_service_takeoff_inputs", _fake_inputs)
    monkeypatch.setattr(service_takeoff_route, "_resolve_rooms", _fake_rooms)

    yield app
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _get_takeoff(application: FastAPI, query: str = "") -> httpx.Response:
    transport = ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(f"/v1/revisions/{REVISION_ID}/service-takeoff{query}")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_service_takeoff_confirmed_scale_returns_real_length(
    takeoff_app_confirmed_scale: FastAPI,
) -> None:
    """A revision with a 1000-unit pipe and confirmed scale (0.001 m/unit) ->
    200, one item, real_length_m=1.0, basis='real_world', unscaled=False.
    """
    response = await _get_takeoff(takeoff_app_confirmed_scale)
    assert response.status_code == 200
    body = response.json()

    assert body["unscaled"] is False

    scale = body["scale"]
    assert scale["units_confidence"] == "confirmed"
    assert scale["real_world_available"] is True
    assert scale["contradicted"] is False
    assert scale["conversion_factor"] == pytest.approx(0.001)

    items = body["items"]
    assert len(items) >= 1

    # The VAC line should appear (with or without size from tag parsing).
    item = next((i for i in items if i["service"] == "VAC"), None)
    assert item is not None, f"Expected VAC item in {[i['service'] for i in items]}"

    assert item["basis"] == "real_world"
    assert item["real_length_m"] == pytest.approx(1.0)
    assert item["drawing_length"] == pytest.approx(1000.0)
    assert item["units_confidence"] == "confirmed"
    assert item["run_count"] >= 1


async def test_service_takeoff_response_has_required_top_level_keys(
    takeoff_app_confirmed_scale: FastAPI,
) -> None:
    """Response body includes exactly the expected top-level keys (extra='forbid' in model)."""
    response = await _get_takeoff(takeoff_app_confirmed_scale)
    assert response.status_code == 200
    body = response.json()
    # extra='forbid' on ServiceTakeoffResponse -- assert all expected keys are present.
    expected_keys = {"manifest", "items", "summary", "scale", "unscaled"}
    assert expected_keys == set(body.keys())


async def test_service_takeoff_no_scale_returns_200_unscaled(
    takeoff_app_no_scale: FastAPI,
) -> None:
    """A revision with no adapter run -> 200, unscaled=True (or empty items),
    real_length_m null on any present line.
    """
    response = await _get_takeoff(takeoff_app_no_scale)
    assert response.status_code == 200
    body = response.json()

    # Empty items: unscaled is False by the coordinator convention (no lines -> not unscaled).
    # But the scale must reflect unknown confidence.
    assert body["scale"]["units_confidence"] == "unknown"
    assert body["scale"]["real_world_available"] is False

    # All items (if any) must be drawing_units_only with null real_length_m.
    for item in body["items"]:
        assert item["basis"] == "drawing_units_only"
        assert item["real_length_m"] is None


async def test_service_takeoff_invalid_scope_is_422(
    takeoff_app_confirmed_scale: FastAPI,
) -> None:
    response = await _get_takeoff(takeoff_app_confirmed_scale, "?scope=bogus")
    assert response.status_code == 422


async def test_service_takeoff_invalid_radius_is_422(
    takeoff_app_confirmed_scale: FastAPI,
) -> None:
    """radius must be > 0."""
    response = await _get_takeoff(takeoff_app_confirmed_scale, "?radius=0")
    assert response.status_code == 422


async def test_service_takeoff_items_sorted_deterministically(
    takeoff_app_confirmed_scale: FastAPI,
) -> None:
    """Items are deterministically sorted (same request, same order)."""
    r1 = await _get_takeoff(takeoff_app_confirmed_scale)
    r2 = await _get_takeoff(takeoff_app_confirmed_scale)
    assert r1.json()["items"] == r2.json()["items"]


# ---------------------------------------------------------------------------
# ADR-005 regression: the new route module must NOT import app.estimating.quantities
# ---------------------------------------------------------------------------


def test_service_takeoff_route_does_not_import_estimating_quantities() -> None:
    """ADR-005 guard: scan source text of the three takeoff modules and assert that none
    contain an import statement referencing the forbidden estimating/quantities surface.

    This is a static text check (reads the .py file directly), not a runtime attribute
    walk, so it cannot be fooled by lazy imports or conditional import paths.
    """
    import ast
    import importlib
    import pathlib

    # The three modules whose source must be clean.
    module_names = [
        "app.api.v1.revision_routes.service_takeoff",
        "app.interpretation.service_takeoff",
        "app.interpretation.service_takeoff_loaders",
    ]
    # Forbidden tokens -- any import statement (import or from ... import) that references
    # one of these is a violation of ADR-005.
    forbidden_tokens = (
        "app.estimating.quantities",
        "app.jobs.result_builders",
        "app.jobs.estimate_execution_input",
        "app.jobs.estimate_assembly",
        "QuantityTakeoff",
        "QuantityItem",
    )

    for module_name in module_names:
        mod = importlib.import_module(module_name)
        src_path = pathlib.Path(mod.__file__)  # type: ignore[arg-type]
        source = src_path.read_text(encoding="utf-8")

        # 1. Fast substring check on import lines (catches most cases, fast).
        for line in source.splitlines():
            stripped = line.strip()
            if stripped.startswith(("import ", "from ")):
                for token in forbidden_tokens:
                    assert token not in stripped, (
                        f"ADR-005 violation in {module_name}: import line contains {token!r}:\n"
                        f"  {stripped}"
                    )

        # 2. AST walk -- catches any import node regardless of indentation or aliasing.
        tree = ast.parse(source, filename=str(src_path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    for token in forbidden_tokens:
                        assert token not in alias.name, (
                            f"ADR-005 violation in {module_name}: `import {alias.name}` "
                            f"contains {token!r}"
                        )
            elif isinstance(node, ast.ImportFrom):
                from_module = node.module or ""
                for token in forbidden_tokens:
                    assert token not in from_module, (
                        f"ADR-005 violation in {module_name}: `from {from_module} import ...` "
                        f"contains {token!r}"
                    )
                for alias in node.names:
                    for token in forbidden_tokens:
                        assert token not in alias.name, (
                            f"ADR-005 violation in {module_name}: "
                            f"`from {from_module} import {alias.name}` contains {token!r}"
                        )


# ---------------------------------------------------------------------------
# P0 route tests: empty-revision summary + multi-item fixture
# ---------------------------------------------------------------------------


async def test_service_takeoff_empty_revision_summary_all_zeros(
    takeoff_app_no_scale: FastAPI,
) -> None:
    """Empty revision -> 200 with items=[] AND all summary counts zero."""
    response = await _get_takeoff(takeoff_app_no_scale)
    assert response.status_code == 200
    body = response.json()

    assert body["items"] == []

    summary = body["summary"]
    assert summary["services"] == 0
    assert summary["sizes"] == 0
    assert summary["rooms"] == 0
    assert summary["lines"] == 0
    assert summary["unassigned_runs"] == 0
    assert summary["unknown_service_runs"] == 0


def _inputs_two_services_two_rooms(scale: ScaleContext) -> ServiceTakeoffInputs:
    """Two VAC pipes in two separate rooms: yields 2 items, 1 service, 2 rooms, 2 lines."""
    from app.interpretation.routed_runs import RoutedEntity
    from app.interpretation.run_service_identity import TagPlacement
    from app.interpretation.service_legend import ServiceEntry, ServiceLegend

    # Two entities on separate layers / colours so they form two separate run groups.
    color_red: dict[str, Any] = {"index": 1, "rgb": "#ff0000"}
    color_blue: dict[str, Any] = {"index": 5, "rgb": "#0000ff"}

    geom_a = _line_entity_geometry(100.0, 100.0, 400.0, 100.0)  # length 300, anchor (250, 100)
    geom_b = _line_entity_geometry(5100.0, 100.0, 5400.0, 100.0)  # length 300, anchor (5250, 100)

    routed_entities = [
        RoutedEntity(
            entity_id="pipe-a",
            entity_type="line",
            layer_ref="PIPE",
            color=color_red,
            geometry=geom_a,
        ),
        RoutedEntity(
            entity_id="pipe-b",
            entity_type="line",
            layer_ref="PIPE",
            color=color_blue,
            geometry=geom_b,
        ),
    ]

    entry_red = ServiceEntry(
        colour_key="#ff0000",
        colour_index=1,
        colour_rgb="#ff0000",
        discipline="medical-gas",
        abbreviation="VAC",
        description="Vacuum",
        sources=("swatch",),
        confidence=None,
        competing_disciplines=(),
    )
    entry_blue = ServiceEntry(
        colour_key="#0000ff",
        colour_index=5,
        colour_rgb="#0000ff",
        discipline="medical-gas",
        abbreviation="VAC",
        description="Vacuum",
        sources=("swatch",),
        confidence=None,
        competing_disciplines=(),
    )
    legend = ServiceLegend(entries=(entry_red, entry_blue))

    tag_placements = [
        TagPlacement(text="50 VAC", point=(250.0, 100.0), layer_ref="PIPE TAG"),
        TagPlacement(text="50 VAC", point=(5250.0, 100.0), layer_ref="PIPE TAG"),
    ]

    geometry_by_entity_id: dict[str, Any] = {
        "pipe-a": geom_a,
        "pipe-b": geom_b,
    }

    return ServiceTakeoffInputs(
        routed_entities=routed_entities,
        legend=legend,
        tag_placements=tag_placements,
        geometry_by_entity_id=geometry_by_entity_id,
        scale=scale,
        rise_entities=[],
        drop_entities=[],
    )


class _FakeRoomInterpretationWithRooms(_FakeRoomInterpretation):
    pass


def _two_rooms() -> _FakeRoomInterpretationWithRooms:
    """Two non-overlapping square rooms covering the two pipe anchors."""
    from shapely.geometry import Polygon

    poly_a = Polygon([(0, 0), (1000, 0), (1000, 1000), (0, 1000)])
    poly_b = Polygon([(5000, 0), (6000, 0), (6000, 1000), (5000, 1000)])

    room_a = Room(
        id="room-a",
        name="Room A",
        source="test",
        polygon=poly_a,
        area=1_000_000.0,
        bounds=(0.0, 0.0, 1000.0, 1000.0),
        number="1.01",
    )
    room_b = Room(
        id="room-b",
        name="Room B",
        source="test",
        polygon=poly_b,
        area=1_000_000.0,
        bounds=(5000.0, 0.0, 6000.0, 1000.0),
        number="1.02",
    )

    ns = _FakeRoomInterpretationWithRooms()
    ns.rooms = [room_a, room_b]
    ns.strategy = "explicit_layer"
    ns.source_layers = []
    ns.device_assignments = []
    return ns


@pytest.fixture
def takeoff_app_two_services_two_rooms(
    app: FastAPI, monkeypatch: pytest.MonkeyPatch
) -> Iterator[FastAPI]:
    """App patched with two VAC pipes in two separate rooms (confirmed scale)."""

    async def _no_db() -> AsyncGenerator[None, None]:
        yield None

    app.dependency_overrides[get_db] = _no_db

    async def _fake_manifest(revision_id: uuid.UUID, db: Any) -> SimpleNamespace:
        return _manifest()

    async def _fake_inputs(
        db: Any,
        revision_id: uuid.UUID,
        *,
        layer_refs: Any = None,
        tag_layers: Any = None,
        legend_layers: Any = None,
        exclude_off_sheet: bool = True,
    ) -> ServiceTakeoffInputs:
        return _inputs_two_services_two_rooms(_CONFIRMED_SCALE)

    async def _fake_rooms(
        db: Any, revision_id: uuid.UUID, **_: Any
    ) -> _FakeRoomInterpretationWithRooms:
        return _two_rooms()

    monkeypatch.setattr(
        service_takeoff_route, "_get_active_revision_manifest_or_409", _fake_manifest
    )
    monkeypatch.setattr(service_takeoff_route, "load_service_takeoff_inputs", _fake_inputs)
    monkeypatch.setattr(service_takeoff_route, "_resolve_rooms", _fake_rooms)

    yield app
    app.dependency_overrides.clear()


async def test_service_takeoff_multi_item_summary_matches_items(
    takeoff_app_two_services_two_rooms: FastAPI,
) -> None:
    """Multi-item fixture: summary.services/sizes/rooms/lines match the actual items,
    and items are sorted by (service, size_raw or '', room_number or '', room_id).
    """
    response = await _get_takeoff(takeoff_app_two_services_two_rooms)
    assert response.status_code == 200
    body = response.json()

    items = body["items"]
    summary = body["summary"]

    # Must have at least 2 items (one per room; same service VAC).
    assert len(items) >= 2, f"Expected >=2 items, got {len(items)}: {items}"

    # Summary counts must match the items.
    distinct_services = len({i["service"] for i in items})
    distinct_sizes = len({(i["service"], i["size_raw"]) for i in items})
    distinct_rooms = len({i["room_id"] for i in items})

    assert summary["services"] == distinct_services
    assert summary["sizes"] == distinct_sizes
    assert summary["rooms"] == distinct_rooms
    assert summary["lines"] == len(items)

    # Items must be sorted by (service, size_raw or '', room_number or '', room_id).
    def _sort_key(i: dict[str, Any]) -> tuple[str, str, str, str]:
        return (i["service"], i["size_raw"] or "", i["room_number"] or "", i["room_id"])

    assert items == sorted(items, key=_sort_key), (
        f"Items not in documented sort order: {[(i['service'], i['room_id']) for i in items]}"
    )


# ---------------------------------------------------------------------------
# Spec test 7: API surfaces riser_count/drop_count/total_risers/total_drops +
# extra="forbid" still holds
# ---------------------------------------------------------------------------


async def test_service_takeoff_items_have_riser_and_drop_count_fields(
    takeoff_app_confirmed_scale: FastAPI,
) -> None:
    """Every item in the response has riser_count and drop_count fields (>=0 ints)."""
    response = await _get_takeoff(takeoff_app_confirmed_scale)
    assert response.status_code == 200
    body = response.json()

    items = body["items"]
    assert len(items) >= 1, "Expected at least one item from the confirmed-scale fixture"

    for item in items:
        assert "riser_count" in item, f"Missing riser_count on item: {item}"
        assert "drop_count" in item, f"Missing drop_count on item: {item}"
        assert isinstance(item["riser_count"], int)
        assert isinstance(item["drop_count"], int)
        assert item["riser_count"] >= 0
        assert item["drop_count"] >= 0


async def test_service_takeoff_summary_has_total_risers_and_drops(
    takeoff_app_confirmed_scale: FastAPI,
) -> None:
    """Summary has total_risers and total_drops fields."""
    response = await _get_takeoff(takeoff_app_confirmed_scale)
    assert response.status_code == 200
    body = response.json()

    summary = body["summary"]
    assert "total_risers" in summary, f"Missing total_risers in summary: {summary}"
    assert "total_drops" in summary, f"Missing total_drops in summary: {summary}"
    assert isinstance(summary["total_risers"], int)
    assert isinstance(summary["total_drops"], int)
    assert summary["total_risers"] >= 0
    assert summary["total_drops"] >= 0


async def test_service_takeoff_empty_revision_total_risers_drops_zero(
    takeoff_app_no_scale: FastAPI,
) -> None:
    """Empty revision -> total_risers=0, total_drops=0 in summary."""
    response = await _get_takeoff(takeoff_app_no_scale)
    assert response.status_code == 200
    body = response.json()

    summary = body["summary"]
    assert summary["total_risers"] == 0
    assert summary["total_drops"] == 0


async def test_service_takeoff_extra_field_rejected(
    takeoff_app_confirmed_scale: FastAPI,
) -> None:
    """extra='forbid' on ServiceTakeoffResponse -- response does not contain unexpected keys.

    ServiceTakeoffResponse already has extra='forbid'; this test confirms the model_config
    is still in place after adding riser/drop fields (a stray extra field would 422 the
    Pydantic serialiser, causing a 500, not a 200).
    """
    response = await _get_takeoff(takeoff_app_confirmed_scale)
    assert response.status_code == 200
    body = response.json()
    expected_keys = {"manifest", "items", "summary", "scale", "unscaled"}
    assert expected_keys == set(body.keys()), (
        f"Unexpected top-level keys: {set(body.keys()) - expected_keys}"
    )
