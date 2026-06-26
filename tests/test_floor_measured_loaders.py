"""Tests for floor_measured_loaders.py — mocked DB seam (issue #718, Phase R-D).

All tests mock ``load_measured_geometry`` and ``load_service_takeoff_inputs``
(non-DB). No real database is accessed. An env-guard smoke test lives in
``test_floor_measured_realdata.py``.
"""

from __future__ import annotations

import subprocess
import sys
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from shapely.geometry import Polygon

from app.interpretation.floor_registration import FloorRegistration, RegisteredMember
from app.interpretation.grid_registration import GridTransform
from app.interpretation.measurement import ScaleContext
from app.interpretation.room_fusion import build_room_registry
from app.interpretation.rooms import room_from_polygon

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

_POLY_A = Polygon([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)])
_POLY_B = Polygon([(10.0, 0.0), (20.0, 0.0), (20.0, 10.0), (10.0, 10.0)])

_REV_ID_A = UUID("aaaa0000-0000-0000-0000-000000000001")
_REV_ID_B = UUID("bbbb0000-0000-0000-0000-000000000002")
_REF_ID = UUID("cccc0000-0000-0000-0000-000000000003")

_GroupKey = tuple[str | None, str | None]
_Polylines = tuple[tuple[tuple[float, float], ...], ...]


def _identity_transform() -> GridTransform:
    return GridTransform(
        dx=0.0,
        dy=0.0,
        rotation_rad=0.0,
        scale=1.0,
        matched_labels=(),
        matched_count=0,
        max_residual_m=0.0,
        median_residual_m=0.0,
        quality="good",
    )


def _shift_transform(dx: float, dy: float) -> GridTransform:
    return GridTransform(
        dx=dx,
        dy=dy,
        rotation_rad=0.0,
        scale=1.0,
        matched_labels=(),
        matched_count=3,
        max_residual_m=0.0,
        median_residual_m=0.0,
        quality="good",
    )


def _registered_member(rev_id: UUID, role: str, transform: GridTransform) -> RegisteredMember:
    return RegisteredMember(
        revision_id=rev_id,
        role=role,
        transform=transform,
        audit={},
    )


def _simple_registration(members: list[RegisteredMember]) -> FloorRegistration:
    return FloorRegistration(
        reference_revision_id=_REF_ID,
        reference_role="reference",
        members_ok=tuple(members),
        members_failed=(),
    )


def _scale_confirmed() -> ScaleContext:
    return ScaleContext(
        conversion_factor=1.0,
        real_world_available=True,
        contradicted=False,
        units_confidence="confirmed",
    )


def _mock_inputs(scale: ScaleContext | None = None) -> Any:
    """Build a minimal ServiceTakeoffInputs mock."""
    if scale is None:
        scale = _scale_confirmed()
    mock = MagicMock()
    mock.routed_entities = []
    mock.legend = MagicMock()
    mock.tag_placements = []
    mock.geometry_by_entity_id = {}
    mock.scale = scale
    return mock


# ---------------------------------------------------------------------------
# Test: empty member skipped (no geometry)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_member_skipped() -> None:
    """A member revision with no persisted geometry is silently skipped.

    Arrange: one member; load_measured_geometry returns {} for it.
    Act:     load_fused_measured.
    Assert:  result has no items, no unassigned.
    """
    from app.interpretation.floor_measured_loaders import load_fused_measured

    room_a = room_from_polygon("room-a", _POLY_A, source="explicit", number="1.01")
    registry = build_room_registry([room_a], voronoi_fallback=False)

    member = _registered_member(_REV_ID_A, "containment", _identity_transform())
    registration = _simple_registration([member])

    with (
        patch(
            "app.interpretation.floor_measured_loaders.load_measured_geometry",
            new=AsyncMock(return_value={}),
        ),
        patch(
            "app.interpretation.floor_measured_loaders.load_service_takeoff_inputs",
            new=AsyncMock(return_value=_mock_inputs()),
        ),
        patch(
            "app.interpretation.floor_measured_loaders.identify_routed_runs",
            return_value=MagicMock(groups=[]),
        ),
        patch(
            "app.interpretation.floor_measured_loaders.fuse_run_service_identities",
            return_value=MagicMock(identities=[]),
        ),
    ):
        result = await load_fused_measured(
            MagicMock(),  # db stub
            registration=registration,
            registry=registry,
        )

    assert result.items == ()
    assert result.unassigned == ()


# ---------------------------------------------------------------------------
# Test: transform applied (member dx=30.2 → polyline lands in shifted room)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_transform_applied_shifts_polyline_into_room() -> None:
    """A member with dx=30.2 shifts polylines so they land in a different room.

    Arrange:
    - Room at x∈[30,40], y∈[0,10] (shifted room).
    - Raw polyline at x∈[0,9] (before transform, outside the shifted room).
    - Member transform: dx=30.2, dy=0.0.
    Act:     load_fused_measured.
    Assert:  item assigned to the shifted room (transformation applied).
    """
    from app.interpretation.floor_measured_loaders import load_fused_measured

    poly_shifted = Polygon([(30.0, 0.0), (40.0, 0.0), (40.0, 10.0), (30.0, 10.0)])
    room_shifted = room_from_polygon("room-shifted", poly_shifted, source="explicit", number="2.01")
    registry = build_room_registry([room_shifted], voronoi_fallback=False)

    # The raw polyline is at x∈[0,9], y=5 — outside the shifted room.
    # After transform (dx=30.2): x∈[30.2, 39.2] — inside the shifted room.
    raw_polyline: tuple[tuple[float, float], ...] = ((0.0, 5.0), (9.0, 5.0))
    geom_return: dict[_GroupKey, _Polylines] = {(None, "ff0000"): (raw_polyline,)}

    member = _registered_member(_REV_ID_A, "power", _shift_transform(30.2, 0.0))
    registration = _simple_registration([member])

    with (
        patch(
            "app.interpretation.floor_measured_loaders.load_measured_geometry",
            new=AsyncMock(return_value=geom_return),
        ),
        patch(
            "app.interpretation.floor_measured_loaders.load_service_takeoff_inputs",
            new=AsyncMock(return_value=_mock_inputs()),
        ),
        patch(
            "app.interpretation.floor_measured_loaders.identify_routed_runs",
            return_value=MagicMock(groups=[]),
        ),
        patch(
            "app.interpretation.floor_measured_loaders.fuse_run_service_identities",
            return_value=MagicMock(identities=[]),
        ),
    ):
        result = await load_fused_measured(
            MagicMock(),
            registration=registration,
            registry=registry,
        )

    assert len(result.items) == 1, f"Expected 1 item, got {result.items}"
    assert result.items[0].room_number == "2.01"
    assert result.items[0].boundary_basis == "polygon"


# ---------------------------------------------------------------------------
# Test: per-member conservation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_per_member_conservation() -> None:
    """Total drawing_length in result equals input polyline geometric length."""
    from shapely.geometry import LineString

    from app.interpretation.floor_measured_loaders import load_fused_measured

    room_a = room_from_polygon("room-a", _POLY_A, source="explicit", number="1.01")
    room_b = room_from_polygon("room-b", _POLY_B, source="explicit", number="1.02")
    registry = build_room_registry([room_a, room_b], voronoi_fallback=False)

    # Polyline spanning both rooms: x∈[0,20], length=20.
    raw_polyline: tuple[tuple[float, float], ...] = ((0.0, 5.0), (20.0, 5.0))
    geom_return: dict[_GroupKey, _Polylines] = {(None, "ff0000"): (raw_polyline,)}
    expected_total = float(LineString(raw_polyline).length)

    member = _registered_member(_REV_ID_A, "containment", _identity_transform())
    registration = _simple_registration([member])

    with (
        patch(
            "app.interpretation.floor_measured_loaders.load_measured_geometry",
            new=AsyncMock(return_value=geom_return),
        ),
        patch(
            "app.interpretation.floor_measured_loaders.load_service_takeoff_inputs",
            new=AsyncMock(return_value=_mock_inputs()),
        ),
        patch(
            "app.interpretation.floor_measured_loaders.identify_routed_runs",
            return_value=MagicMock(groups=[]),
        ),
        patch(
            "app.interpretation.floor_measured_loaders.fuse_run_service_identities",
            return_value=MagicMock(identities=[]),
        ),
    ):
        result = await load_fused_measured(
            MagicMock(),
            registration=registration,
            registry=registry,
        )

    actual_total = sum(i.drawing_length for i in result.items) + sum(
        i.drawing_length for i in result.unassigned
    )
    rel_err = abs(actual_total - expected_total) / max(expected_total, 1e-12)
    assert rel_err <= 1e-6, (
        f"Per-member conservation failed: expected {expected_total}, got {actual_total}"
    )


# ---------------------------------------------------------------------------
# Test: per-member isolation — one loader raises → others fuse
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_per_member_isolation_one_raises_others_fuse() -> None:
    """If one member's geometry load raises, other members are still processed.

    Arrange: two members; first member's load_measured_geometry raises; second returns data.
    Act:     load_fused_measured.
    Assert:  result contains items from the second member; no exception propagated.
    """
    from app.interpretation.floor_measured_loaders import load_fused_measured

    room_a = room_from_polygon("room-a", _POLY_A, source="explicit", number="1.01")
    registry = build_room_registry([room_a], voronoi_fallback=False)

    raw_polyline: tuple[tuple[float, float], ...] = ((1.0, 5.0), (9.0, 5.0))

    # Member A raises on geometry load; Member B succeeds.
    member_a = _registered_member(_REV_ID_A, "lighting", _identity_transform())
    member_b = _registered_member(_REV_ID_B, "containment", _identity_transform())
    registration = _simple_registration([member_a, member_b])

    async def _side_effect_geom(
        db: object,
        revision_id: UUID,
        **kwargs: object,
    ) -> dict[_GroupKey, _Polylines]:
        if revision_id == _REV_ID_A:
            raise RuntimeError("simulated DB error for member A")
        return {(None, "ff0000"): (raw_polyline,)}

    with (
        patch(
            "app.interpretation.floor_measured_loaders.load_measured_geometry",
            new=AsyncMock(side_effect=_side_effect_geom),
        ),
        patch(
            "app.interpretation.floor_measured_loaders.load_service_takeoff_inputs",
            new=AsyncMock(return_value=_mock_inputs()),
        ),
        patch(
            "app.interpretation.floor_measured_loaders.identify_routed_runs",
            return_value=MagicMock(groups=[]),
        ),
        patch(
            "app.interpretation.floor_measured_loaders.fuse_run_service_identities",
            return_value=MagicMock(identities=[]),
        ),
    ):
        result = await load_fused_measured(
            MagicMock(),
            registration=registration,
            registry=registry,
        )

    # Member B contributes; Member A was skipped.
    assert len(result.items) + len(result.unassigned) > 0, (
        "Expected items from member B, got nothing"
    )
    all_member_ids = {i.member_revision_id for i in (*result.items, *result.unassigned)}
    # Member A's ID must NOT appear (it was skipped due to exception).
    assert str(_REV_ID_A) not in all_member_ids
    assert str(_REV_ID_B) in all_member_ids


# ---------------------------------------------------------------------------
# Test: isolation-import (no module-level app.api import)
# ---------------------------------------------------------------------------


def test_floor_measured_loaders_imports_in_isolation_no_cycle() -> None:
    """floor_measured_loaders must import standalone without pulling app.api at module level.

    Mirrors the pattern guarding service_takeoff_loaders in test_centerline_import_boundary.

    Arrange: script imports only floor_measured_loaders (no app.main bootstrap).
    Act:     run in fresh subprocess.
    Assert:  exit 0 (no circular ImportError, no module-level api import).
    """
    script = "import app.interpretation.floor_measured_loaders\nprint('ok')\n"
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (
        "floor_measured_loaders import cycle or error.\n"
        f"stderr: {result.stderr}\nstdout: {result.stdout}"
    )


def test_floor_measured_loaders_does_not_import_api_at_module_level() -> None:
    """Module-level import of floor_measured_loaders must not pull app.api into sys.modules.

    Arrange: script imports only floor_measured_loaders, then checks sys.modules for app.api.
    Act:     run in fresh subprocess.
    Assert:  exit 0 (app.api not in sys.modules).
    """
    script = """\
import sys
import app.interpretation.floor_measured_loaders
api_loaded = any(k == "app.api" or k.startswith("app.api.") for k in sys.modules)
if api_loaded:
    print("FORBIDDEN: app.api loaded at module level by floor_measured_loaders", file=sys.stderr)
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
        "app.api imported at module level by floor_measured_loaders (cycle risk).\n"
        f"stderr: {result.stderr}\nstdout: {result.stdout}"
    )
