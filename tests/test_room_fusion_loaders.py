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

from app.interpretation.room_fusion import RoomRegistry
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
    revision_id = uuid.uuid4()

    with patch(
        "app.api.v1.revision_routes.rooms._resolve_rooms",
        mock_resolve,
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
    revision_id = uuid.uuid4()
    db = AsyncMock()

    with patch("app.api.v1.revision_routes.rooms._resolve_rooms", mock_resolve):
        await load_room_registry(db, revision_id, scope="sheet")

    call_args = mock_resolve.await_args
    assert call_args is not None
    assert call_args.kwargs["exclude_off_sheet"] is True

    mock_resolve.reset_mock()
    mock_resolve.return_value = fake_result

    with patch("app.api.v1.revision_routes.rooms._resolve_rooms", mock_resolve):
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
    revision_id = uuid.uuid4()
    db = AsyncMock()

    with patch("app.api.v1.revision_routes.rooms._resolve_rooms", mock_resolve):
        registry = await load_room_registry(db, revision_id, voronoi_fallback=False)

    assert registry._voronoi_fallback is False


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
