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

    # Tag near the pipe midpoint (500, 0).
    # Real service tags carry a size unit / Ø glyph; the bare form is gated under strict_content
    # (validated against the real med-gas oracle), so the realistic fixture uses "mm".
    tag_placements = [
        TagPlacement(text="50mm VAC", point=(500.0, 0.0), layer_ref="PIPE TAG"),
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
    expected_keys = {
        "manifest",
        "items",
        "summary",
        "scale",
        "fill_attribution",
        "tag_service_attribution",
        "segment_label_attribution",
        "unscaled",
        "length_provisional",
    }
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

    # Real service tags carry a size unit / Ø glyph; the bare form is gated under strict_content
    # (validated against the real med-gas oracle), so the realistic fixture uses "mm".
    tag_placements = [
        TagPlacement(text="50mm VAC", point=(250.0, 100.0), layer_ref="PIPE TAG"),
        TagPlacement(text="50mm VAC", point=(5250.0, 100.0), layer_ref="PIPE TAG"),
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
    expected_keys = {
        "manifest",
        "items",
        "summary",
        "scale",
        "fill_attribution",
        "tag_service_attribution",
        "segment_label_attribution",
        "unscaled",
        "length_provisional",
    }
    assert expected_keys == set(body.keys()), (
        f"Unexpected top-level keys: {set(body.keys()) - expected_keys}"
    )


# ---------------------------------------------------------------------------
# PDF-specific response tests: length_provisional + SERVICE_UNKNOWN suppression
# ---------------------------------------------------------------------------


def _inputs_pdf_with_unknown_and_resolved(scale: ScaleContext) -> ServiceTakeoffInputs:
    """PDF inputs: one resolved VAC run + one unknown-service run.

    Both have confirmed scale lengths so the coordinator produces real_length_m.
    The route must suppress lengths only on the SERVICE_UNKNOWN line.
    """
    from app.interpretation.routed_runs import RoutedEntity
    from app.interpretation.run_service_identity import TagPlacement
    from app.interpretation.service_legend import ServiceEntry, ServiceLegend

    color_red: dict[str, Any] = {"index": 1, "rgb": "#ff0000"}
    color_grey: dict[str, Any] = {"index": None, "rgb": "#888888"}

    geom_vac = _line_entity_geometry(0.0, 0.0, 1000.0, 0.0)  # length 1000 -> 1.0 m
    geom_unk = _line_entity_geometry(0.0, 500.0, 1000.0, 500.0)  # length 1000 -> 1.0 m (suppress)

    routed_entities = [
        RoutedEntity(
            entity_id="pdf-vac",
            entity_type="line",
            layer_ref=None,  # PDF: layer_ref normalised to None
            color=color_red,
            geometry=geom_vac,
        ),
        RoutedEntity(
            entity_id="pdf-unk",
            entity_type="line",
            layer_ref=None,
            color=color_grey,
            geometry=geom_unk,
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
    legend = ServiceLegend(entries=(entry_red,))

    tag_placements = [
        TagPlacement(text="50 mm VAC", point=(500.0, 0.0), layer_ref=None),
    ]

    geometry_by_entity_id: dict[str, Any] = {
        "pdf-vac": geom_vac,
        "pdf-unk": geom_unk,
    }

    return ServiceTakeoffInputs(
        routed_entities=routed_entities,
        legend=legend,
        tag_placements=tag_placements,
        geometry_by_entity_id=geometry_by_entity_id,
        scale=scale,
        rise_entities=[],
        drop_entities=[],
        input_family="pdf_vector",
    )


def _inputs_dwg_with_unknown(scale: ScaleContext) -> ServiceTakeoffInputs:
    """DWG inputs: one unknown-service run with confirmed scale.

    The route must NOT suppress lengths for DWG.
    """
    from app.interpretation.routed_runs import RoutedEntity
    from app.interpretation.service_legend import ServiceLegend

    color: dict[str, Any] = {"index": 3, "rgb": "#00ff00"}
    geom = _line_entity_geometry(0.0, 0.0, 1000.0, 0.0)

    routed_entities = [
        RoutedEntity(
            entity_id="dwg-unk",
            entity_type="line",
            layer_ref="Pipes",
            color=color,
            geometry=geom,
        ),
    ]

    return ServiceTakeoffInputs(
        routed_entities=routed_entities,
        legend=ServiceLegend(entries=()),
        tag_placements=[],
        geometry_by_entity_id={"dwg-unk": geom},
        scale=scale,
        rise_entities=[],
        drop_entities=[],
        input_family="dwg",
    )


@pytest.fixture
def takeoff_app_pdf(app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> Iterator[FastAPI]:
    """App patched with a PDF-vector revision: one VAC run + one unknown run,
    confirmed scale."""

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
        return _inputs_pdf_with_unknown_and_resolved(_CONFIRMED_SCALE)

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
def takeoff_app_dwg_unknown(app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> Iterator[FastAPI]:
    """App patched with a DWG revision: one unknown-service run, confirmed scale."""

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
        return _inputs_dwg_with_unknown(_CONFIRMED_SCALE)

    async def _fake_rooms(db: Any, revision_id: uuid.UUID, **_: Any) -> _FakeRoomInterpretation:
        return _no_rooms()

    monkeypatch.setattr(
        service_takeoff_route, "_get_active_revision_manifest_or_409", _fake_manifest
    )
    monkeypatch.setattr(service_takeoff_route, "load_service_takeoff_inputs", _fake_inputs)
    monkeypatch.setattr(service_takeoff_route, "_resolve_rooms", _fake_rooms)

    yield app
    app.dependency_overrides.clear()


async def test_pdf_response_length_provisional_true(
    takeoff_app_pdf: FastAPI,
) -> None:
    """PDF-vector revision -> length_provisional=True in response."""
    response = await _get_takeoff(takeoff_app_pdf)
    assert response.status_code == 200
    body = response.json()
    assert body["length_provisional"] is True


async def test_pdf_unknown_service_length_suppressed(
    takeoff_app_pdf: FastAPI,
) -> None:
    """PDF SERVICE_UNKNOWN line has real_length_m=None and drawing_length=0.0;
    run_count and identity_status are preserved."""
    response = await _get_takeoff(takeoff_app_pdf)
    assert response.status_code == 200
    body = response.json()

    items = body["items"]
    unknown_items = [i for i in items if i["service"] == "unknown"]
    assert len(unknown_items) >= 1, f"Expected at least one unknown item; got: {items}"

    for item in unknown_items:
        assert item["real_length_m"] is None, (
            f"PDF SERVICE_UNKNOWN real_length_m must be None; got {item['real_length_m']}"
        )
        assert item["drawing_length"] == pytest.approx(0.0), (
            f"PDF SERVICE_UNKNOWN drawing_length must be 0.0; got {item['drawing_length']}"
        )
        assert item["run_count"] >= 1, "run_count must be preserved even when length suppressed"
        assert "identity_status" in item


async def test_pdf_resolved_service_length_preserved(
    takeoff_app_pdf: FastAPI,
) -> None:
    """PDF resolved-service (VAC) line keeps its real_length_m."""
    response = await _get_takeoff(takeoff_app_pdf)
    assert response.status_code == 200
    body = response.json()

    items = body["items"]
    vac_items = [i for i in items if i["service"] == "VAC"]
    assert len(vac_items) >= 1, f"Expected at least one VAC item; got: {items}"

    for item in vac_items:
        assert item["real_length_m"] is not None, (
            f"PDF resolved-service real_length_m must not be None; got {item}"
        )
        assert item["real_length_m"] > 0.0


async def test_dwg_response_length_provisional_false(
    takeoff_app_dwg_unknown: FastAPI,
) -> None:
    """DWG revision -> length_provisional=False."""
    response = await _get_takeoff(takeoff_app_dwg_unknown)
    assert response.status_code == 200
    body = response.json()
    assert body["length_provisional"] is False


async def test_dwg_unknown_service_length_not_suppressed(
    takeoff_app_dwg_unknown: FastAPI,
) -> None:
    """DWG SERVICE_UNKNOWN line retains its real_length_m (suppression is PDF-only)."""
    response = await _get_takeoff(takeoff_app_dwg_unknown)
    assert response.status_code == 200
    body = response.json()

    items = body["items"]
    unknown_items = [i for i in items if i["service"] == "unknown"]
    assert len(unknown_items) >= 1, f"Expected at least one unknown item; got: {items}"

    for item in unknown_items:
        assert item["real_length_m"] is not None, (
            f"DWG SERVICE_UNKNOWN real_length_m must be preserved; got {item}"
        )
        assert item["real_length_m"] > 0.0


async def test_extra_forbid_still_holds_with_length_provisional(
    takeoff_app_pdf: FastAPI,
) -> None:
    """extra='forbid' still holds after adding length_provisional: no unexpected keys."""
    response = await _get_takeoff(takeoff_app_pdf)
    assert response.status_code == 200
    body = response.json()
    expected_keys = {
        "manifest",
        "items",
        "summary",
        "scale",
        "fill_attribution",
        "tag_service_attribution",
        "segment_label_attribution",
        "unscaled",
        "length_provisional",
    }
    assert expected_keys == set(body.keys()), (
        f"Unexpected top-level keys: {set(body.keys()) - expected_keys}"
    )


# ---------------------------------------------------------------------------
# tag_service_attribution field tests (#674 Phase 3)
# ---------------------------------------------------------------------------


async def test_tag_service_attribution_present_in_dwg_response(
    takeoff_app_confirmed_scale: FastAPI,
) -> None:
    """DWG revision: tag_service_attribution is present in the response (may be null
    when no tag stacks are found, but the field itself must exist)."""
    response = await _get_takeoff(takeoff_app_confirmed_scale)
    assert response.status_code == 200
    body = response.json()
    assert "tag_service_attribution" in body


async def test_tag_service_attribution_null_for_pdf(
    takeoff_app_pdf: FastAPI,
) -> None:
    """PDF-vector revision: tag_service_attribution must be null (honest-absent)."""
    response = await _get_takeoff(takeoff_app_pdf)
    assert response.status_code == 200
    body = response.json()
    assert body["tag_service_attribution"] is None


async def test_tag_service_attribution_shape_when_present(
    takeoff_app_confirmed_scale: FastAPI,
) -> None:
    """When tag_service_attribution is not null it must have the correct schema shape."""
    response = await _get_takeoff(takeoff_app_confirmed_scale)
    assert response.status_code == 200
    body = response.json()
    tsa = body["tag_service_attribution"]
    if tsa is None:
        # No bundle bands in the monkeypatched fixture — degenerate but valid.
        return
    assert isinstance(tsa["per_colour"], list)
    assert isinstance(tsa["unmatched_colour_keys"], list)
    assert isinstance(tsa["matched_stack_count"], int)
    assert tsa["matched_stack_count"] >= 0
    assert isinstance(tsa["ambiguous"], bool)
    for entry in tsa["per_colour"]:
        assert isinstance(entry["colour_key"], str)
        assert isinstance(entry["service"], str)
        assert isinstance(entry["sizes"], list)


async def test_tag_service_attribution_degenerate_dwg_returns_empty_per_colour(
    takeoff_app_dwg_unknown: FastAPI,
) -> None:
    """DWG revision with no bundle bands: tag_service_attribution.per_colour=[] and
    unmatched_colour_keys=[] (no bands → no colours to classify), matched_stack_count=0.
    """
    response = await _get_takeoff(takeoff_app_dwg_unknown)
    assert response.status_code == 200
    body = response.json()
    tsa = body["tag_service_attribution"]
    # The monkeypatched DWG fixture has no HATCH bands so assign_services_by_tag_stack
    # returns the empty result.
    assert tsa is not None
    assert tsa["per_colour"] == []


# ---------------------------------------------------------------------------
# D3b (#796): reassemble_tag_fragments pre-pass is wired into the route
# ---------------------------------------------------------------------------


async def test_reassemble_tag_fragments_prepass_is_invoked(
    app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The tag-fragment reassembly pre-pass (D3a) runs BEFORE fuse_run_service_identities
    and the Step-5e segment-label loop.

    Strategy: monkeypatch ``reassemble_tag_fragments`` on the route module so we can
    record what it was called with, then confirm the call happened with the placements
    from ``inputs.tag_placements`` and that the route still returns 200.
    """
    from collections.abc import Sequence

    from app.interpretation.run_service_identity import TagPlacement
    from app.interpretation.tag_reassembly import ReassemblyResult

    inputs_bundle = _inputs_with_pipe_and_tag(_CONFIRMED_SCALE)

    called_with: list[Sequence[TagPlacement]] = []

    def _spy_reassemble(
        placements: Sequence[TagPlacement],
        *,
        cluster_radius_m: float = 0.5,
        legend_abbreviations: frozenset[str] | None = None,
    ) -> ReassemblyResult:
        called_with.append(placements)
        # Pass through unchanged so the rest of the pipeline is unaffected.
        return ReassemblyResult(
            placements=tuple(placements),
            reassembled_count=0,
            rejected_cluster_count=0,
        )

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
        return inputs_bundle

    async def _fake_rooms(db: Any, revision_id: uuid.UUID, **_: Any) -> _FakeRoomInterpretation:
        return _no_rooms()

    monkeypatch.setattr(
        service_takeoff_route, "_get_active_revision_manifest_or_409", _fake_manifest
    )
    monkeypatch.setattr(service_takeoff_route, "load_service_takeoff_inputs", _fake_inputs)
    monkeypatch.setattr(service_takeoff_route, "_resolve_rooms", _fake_rooms)
    monkeypatch.setattr(service_takeoff_route, "reassemble_tag_fragments", _spy_reassemble)

    response = await _get_takeoff(app)
    assert response.status_code == 200

    # The pre-pass must have been called exactly once.
    assert len(called_with) == 1, (
        f"Expected 1 call to reassemble_tag_fragments, got {len(called_with)}"
    )

    # It must have received the tag_placements from the inputs bundle.
    assert list(called_with[0]) == inputs_bundle.tag_placements, (
        "reassemble_tag_fragments was not called with inputs.tag_placements"
    )

    app.dependency_overrides.clear()
