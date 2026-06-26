"""Unit tests for room_fusion_loaders (issue #717, Phase R-C).

All tests mock ``_resolve_rooms`` — no real DB required. The isolation-import
test (subprocess) guards the #705 cycle invariant: ``room_fusion_loaders`` must
import without pulling ``app.api`` at module load time.
"""

from __future__ import annotations

import subprocess
import sys
import uuid
from dataclasses import dataclass
from unittest.mock import AsyncMock, patch

import pytest
from shapely.geometry import Polygon

from app.interpretation.room_fusion import DEFAULT_POLYGON_VORONOI_CONFIDENCE, RoomRegistry
from app.interpretation.rooms import room_from_label, room_from_polygon

# ---------------------------------------------------------------------------
# Minimal stand-ins
# ---------------------------------------------------------------------------


def _make_polygon_room(room_id: str, number: str) -> object:
    poly = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    return room_from_polygon(room_id, poly, source="explicit_layer", number=number)


def _make_label_room(room_id: str, x: float, y: float, number: str) -> object:
    return room_from_label(room_id, source="label_cluster", location=(x, y), number=number)


@dataclass
class _FakeRoomInterpretation:
    rooms: list[object]
    device_assignments: list[object]
    strategy: str
    source_layers: tuple[str, ...]


# ---------------------------------------------------------------------------
# Test: load_room_registry returns a RoomRegistry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_room_registry_returns_registry() -> None:
    """load_room_registry calls _resolve_rooms and wraps the result in RoomRegistry."""
    from app.interpretation.room_fusion_loaders import load_room_registry

    fake_rooms = [
        _make_polygon_room("p1", "1.01"),
        _make_label_room("l1", 20.0, 20.0, "2.01"),
    ]
    fake_result = _FakeRoomInterpretation(
        rooms=fake_rooms,
        device_assignments=[],
        strategy="explicit_layer",
        source_layers=("ROOM-BOUNDARY",),
    )

    mock_resolve = AsyncMock(return_value=fake_result)
    mock_load_text = AsyncMock(return_value=[])
    revision_id = uuid.uuid4()

    with (
        patch("app.api.v1.revision_routes.rooms._resolve_rooms", mock_resolve),
        patch("app.interpretation.devices.load_text_candidates", mock_load_text),
    ):
        db = AsyncMock()
        registry = await load_room_registry(db, revision_id)

    assert isinstance(registry, RoomRegistry)
    mock_resolve.assert_awaited_once()


@pytest.mark.asyncio
async def test_load_room_registry_passes_scope_as_exclude_off_sheet() -> None:
    """scope='sheet' maps to exclude_off_sheet=True; 'modelspace' → False."""
    from app.interpretation.room_fusion_loaders import load_room_registry

    fake_result = _FakeRoomInterpretation(
        rooms=[],
        device_assignments=[],
        strategy="auto",
        source_layers=(),
    )
    mock_resolve = AsyncMock(return_value=fake_result)
    mock_load_text = AsyncMock(return_value=[])
    revision_id = uuid.uuid4()
    db = AsyncMock()

    with (
        patch("app.api.v1.revision_routes.rooms._resolve_rooms", mock_resolve),
        patch("app.interpretation.devices.load_text_candidates", mock_load_text),
    ):
        await load_room_registry(db, revision_id, scope="sheet")

    call_args = mock_resolve.await_args
    assert call_args is not None
    assert call_args.kwargs["exclude_off_sheet"] is True

    mock_resolve.reset_mock()
    mock_resolve.return_value = fake_result

    with (
        patch("app.api.v1.revision_routes.rooms._resolve_rooms", mock_resolve),
        patch("app.interpretation.devices.load_text_candidates", mock_load_text),
    ):
        await load_room_registry(db, revision_id, scope="modelspace")

    call_args = mock_resolve.await_args
    assert call_args is not None
    assert call_args.kwargs["exclude_off_sheet"] is False


@pytest.mark.asyncio
async def test_load_room_registry_voronoi_fallback_propagated() -> None:
    """voronoi_fallback=False is baked into the returned registry."""
    from app.interpretation.room_fusion_loaders import load_room_registry

    fake_result = _FakeRoomInterpretation(
        rooms=[_make_label_room("l1", 5.0, 5.0, "1.01")],
        device_assignments=[],
        strategy="auto",
        source_layers=(),
    )
    mock_resolve = AsyncMock(return_value=fake_result)
    mock_load_text = AsyncMock(return_value=[])
    revision_id = uuid.uuid4()
    db = AsyncMock()

    with (
        patch("app.api.v1.revision_routes.rooms._resolve_rooms", mock_resolve),
        patch("app.interpretation.devices.load_text_candidates", mock_load_text),
    ):
        registry = await load_room_registry(db, revision_id, voronoi_fallback=False)

    assert registry._voronoi_fallback is False


# ---------------------------------------------------------------------------
# Isolation-import test: room_fusion_loaders must not pull app.api at module level
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Welbeck-shaped multi-tag polygon fixture tests (issue #733)
# ---------------------------------------------------------------------------


@dataclass
class _FakeTagCandidate:
    """Stand-in for _TagCandidate (text + layer + coords)."""

    entity_id: str
    text: str
    layer_ref: str | None
    x: float
    y: float


def _make_big_polygon_room(room_id: str) -> object:
    """A polygon that spans x∈[0,20], y∈[0,10] — big enough to swallow 3 tags."""
    poly = Polygon([(0, 0), (20, 0), (20, 10), (0, 10)])
    return room_from_polygon(room_id, poly, source="wall_polygonize", number="0.9.04")


def _make_sliver_room(room_id: str) -> object:
    """A tiny polygon with no tags inside."""
    poly = Polygon([(100, 100), (101, 100), (101, 101), (100, 101)])
    return room_from_polygon(room_id, poly, source="wall_polygonize", number="sliver")


def _make_single_tag_polygon_room(room_id: str) -> object:
    """A polygon at x∈[50,70], y∈[0,10] with exactly one tag inside."""
    poly = Polygon([(50, 0), (70, 0), (70, 10), (50, 10)])
    return room_from_polygon(room_id, poly, source="wall_polygonize", number="1.01")


@pytest.mark.asyncio
async def test_multi_tag_polygon_map_has_three_tags() -> None:
    """Welbeck fixture: one big polygon containing 3 numbered tags → map entry with 3 tags."""
    from app.interpretation.room_fusion_loaders import load_room_registry

    big_poly_room = _make_big_polygon_room("big_poly")
    sliver_room = _make_sliver_room("sliver")

    fake_result = _FakeRoomInterpretation(
        rooms=[big_poly_room, sliver_room],
        device_assignments=[],
        strategy="wall_polygonize",
        source_layers=(),
    )

    # Three numbered tags inside the big polygon, none inside the sliver.
    label_candidates = [
        _FakeTagCandidate("t1", "0.9.04", "ROOM-TAG", 3.0, 5.0),
        _FakeTagCandidate("t2", "0.9.05", "ROOM-TAG", 10.0, 5.0),
        _FakeTagCandidate("t3", "0.9.06", "ROOM-TAG", 17.0, 5.0),
    ]

    mock_resolve = AsyncMock(return_value=fake_result)
    mock_load_text = AsyncMock(return_value=label_candidates)
    revision_id = uuid.uuid4()
    db = AsyncMock()

    with (
        patch("app.api.v1.revision_routes.rooms._resolve_rooms", mock_resolve),
        patch("app.interpretation.devices.load_text_candidates", mock_load_text),
    ):
        registry = await load_room_registry(db, revision_id)

    # Big polygon has 3 distinct tags → entry in map.
    assert "big_poly" in registry._polygon_tags_by_room_id
    tags = registry._polygon_tags_by_room_id["big_poly"]
    assert len(tags) == 3
    tag_numbers = {tag.number for tag in tags}
    assert tag_numbers == {"0.9.04", "0.9.05", "0.9.06"}

    # Sliver has 0 tags → no entry.
    assert "sliver" not in registry._polygon_tags_by_room_id


@pytest.mark.asyncio
async def test_multi_tag_polygon_classifies_near_each_tag() -> None:
    """Registry classifies near each tag to that tag's number with polygon_voronoi basis."""
    from app.interpretation.room_fusion_loaders import load_room_registry

    big_poly_room = _make_big_polygon_room("big_poly")

    fake_result = _FakeRoomInterpretation(
        rooms=[big_poly_room],
        device_assignments=[],
        strategy="wall_polygonize",
        source_layers=(),
    )

    label_candidates = [
        _FakeTagCandidate("t1", "0.9.04", "ROOM-TAG", 3.0, 5.0),
        _FakeTagCandidate("t2", "0.9.05", "ROOM-TAG", 10.0, 5.0),
        _FakeTagCandidate("t3", "0.9.06", "ROOM-TAG", 17.0, 5.0),
    ]

    mock_resolve = AsyncMock(return_value=fake_result)
    mock_load_text = AsyncMock(return_value=label_candidates)
    revision_id = uuid.uuid4()
    db = AsyncMock()

    with (
        patch("app.api.v1.revision_routes.rooms._resolve_rooms", mock_resolve),
        patch("app.interpretation.devices.load_text_candidates", mock_load_text),
    ):
        registry = await load_room_registry(db, revision_id)

    # Point near first tag → 0.9.04.
    result = registry.classify((2.0, 5.0))
    assert result.room_number == "0.9.04"
    assert result.boundary_basis == "polygon_voronoi"
    assert result.confidence == pytest.approx(DEFAULT_POLYGON_VORONOI_CONFIDENCE)
    assert result.needs_review is True

    # Point near second tag → 0.9.05.
    result = registry.classify((10.0, 5.0))
    assert result.room_number == "0.9.05"
    assert result.boundary_basis == "polygon_voronoi"

    # Point near third tag → 0.9.06.
    result = registry.classify((18.0, 5.0))
    assert result.room_number == "0.9.06"
    assert result.boundary_basis == "polygon_voronoi"


@pytest.mark.asyncio
async def test_single_tag_polygon_stays_polygon_basis() -> None:
    """A polygon with exactly one numbered tag → NOT in subdivision map → plain polygon."""
    from app.interpretation.room_fusion_loaders import load_room_registry

    single_tag_room = _make_single_tag_polygon_room("single_poly")

    fake_result = _FakeRoomInterpretation(
        rooms=[single_tag_room],
        device_assignments=[],
        strategy="wall_polygonize",
        source_layers=(),
    )

    # One tag inside the polygon.
    label_candidates = [
        _FakeTagCandidate("t1", "1.01", "ROOM-TAG", 60.0, 5.0),
    ]

    mock_resolve = AsyncMock(return_value=fake_result)
    mock_load_text = AsyncMock(return_value=label_candidates)
    revision_id = uuid.uuid4()
    db = AsyncMock()

    with (
        patch("app.api.v1.revision_routes.rooms._resolve_rooms", mock_resolve),
        patch("app.interpretation.devices.load_text_candidates", mock_load_text),
    ):
        registry = await load_room_registry(db, revision_id)

    # Single-tag polygon → no entry in subdivision map.
    assert "single_poly" not in registry._polygon_tags_by_room_id

    # Classification → plain polygon.
    result = registry.classify((60.0, 5.0))
    assert result.boundary_basis == "polygon"
    assert result.room_number == "1.01"


@pytest.mark.asyncio
async def test_sliver_polygon_zero_tags_no_error() -> None:
    """A sliver polygon with 0 contained tags → no entry in map, no error raised."""
    from app.interpretation.room_fusion_loaders import load_room_registry

    sliver_room = _make_sliver_room("sliver")

    fake_result = _FakeRoomInterpretation(
        rooms=[sliver_room],
        device_assignments=[],
        strategy="wall_polygonize",
        source_layers=(),
    )

    # No tags at all.
    mock_resolve = AsyncMock(return_value=fake_result)
    mock_load_text = AsyncMock(return_value=[])
    revision_id = uuid.uuid4()
    db = AsyncMock()

    with (
        patch("app.api.v1.revision_routes.rooms._resolve_rooms", mock_resolve),
        patch("app.interpretation.devices.load_text_candidates", mock_load_text),
    ):
        registry = await load_room_registry(db, revision_id)

    assert "sliver" not in registry._polygon_tags_by_room_id
    assert isinstance(registry, RoomRegistry)


# ---------------------------------------------------------------------------
# Isolation-import test: room_fusion_loaders must not pull app.api at module level
# ---------------------------------------------------------------------------


def test_room_fusion_loaders_imports_in_isolation_no_api_cycle() -> None:
    """app.interpretation.room_fusion_loaders must import without loading app.api (#705 class).

    The lazy import of _resolve_rooms inside load_room_registry is what prevents
    the interpretation→api cycle. This subprocess test proves the lazy guard holds
    at module load time: importing the loader alone does not drag in app.api.

    Arrange: script imports only room_fusion_loaders (no app.main bootstrap).
    Act:     run in fresh subprocess.
    Assert:  exit 0, and app.api not in sys.modules.
    """
    script = """\
import sys
import app.interpretation.room_fusion_loaders
loaded_api = [k for k in sys.modules if k.startswith("app.api")]
if loaded_api:
    print("CYCLE DETECTED — app.api loaded at import time:", loaded_api, file=sys.stderr)
    sys.exit(1)
print("ok")
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (
        "room_fusion_loaders pulled app.api at module load time (#705 cycle).\n"
        f"stderr: {result.stderr}\nstdout: {result.stdout}"
    )
