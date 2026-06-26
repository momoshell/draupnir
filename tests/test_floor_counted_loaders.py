"""Mocked (non-DB) tests for floor_counted_loaders (issue #719, Phase R-E).

All tests mock ``load_typed_devices`` so no real database is needed.  The test DB
isolation lane (conftest) is NOT used here — these run in the standard pytest process.

Coverage targets:
- Two members, one empty → no raise; result contains devices from non-empty member.
- Transform is applied (routing through bucket_counted_devices).
- Per-member isolation: one member raises → others continue; result still succeeds.
- Isolation-import test: importing floor_counted_loaders must not pull any app.api module.
"""

from __future__ import annotations

import subprocess
import sys
from typing import Any
from unittest.mock import patch
from uuid import UUID

import pytest

from app.interpretation.device_identity import KIND_DEVICE
from app.interpretation.devices import TypedDevice
from app.interpretation.floor_registration import FloorRegistration, RegisteredMember
from app.interpretation.grid_registration import GridTransform
from app.interpretation.room_fusion import RoomRegistry, build_room_registry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REVISION_A = UUID("aaaaaaaa-0000-0000-0000-000000000001")
_REVISION_B = UUID("bbbbbbbb-0000-0000-0000-000000000001")

_IDENTITY = GridTransform(
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


def _typed(device_id: str, type_name: str, *, x: float = 5.0, y: float = 5.0) -> TypedDevice:
    return TypedDevice(
        device_id=device_id,
        type_name=type_name,
        kind=KIND_DEVICE,
        position={"x": x, "y": y},
    )


def _member(revision_id: UUID) -> RegisteredMember:
    return RegisteredMember(
        revision_id=revision_id,
        role="test",
        transform=_IDENTITY,
        audit={},
    )


def _registration(*revision_ids: UUID) -> FloorRegistration:
    members = tuple(_member(r) for r in revision_ids)
    return FloorRegistration(
        reference_revision_id=revision_ids[0] if revision_ids else _REVISION_A,
        reference_role="test",
        members_ok=members,
        members_failed=(),
    )


def _empty_registry() -> RoomRegistry:
    return build_room_registry([])


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_two_members_one_empty_no_raise() -> None:
    """Two members where one returns empty typed_devices: no exception, others fused."""
    reg = _empty_registry()
    reg_obj = _registration(_REVISION_A, _REVISION_B)

    # REVISION_A returns one device; REVISION_B returns empty list.
    async def _fake_load(db: Any, revision_id: UUID, **_: Any) -> list[TypedDevice]:
        if revision_id == _REVISION_A:
            return [_typed("a1", "SD")]
        return []

    with patch(
        "app.interpretation.floor_counted_loaders.load_typed_devices",
        side_effect=_fake_load,
    ):
        from app.interpretation.floor_counted_loaders import load_counted_fusion

        result = await load_counted_fusion(None, reg_obj, reg)  # type: ignore[arg-type]

    # One assignment from revision A.
    assert len(result.assignments) == 1
    assert result.assignments[0].device_id == "a1"


@pytest.mark.asyncio
async def test_transform_applied_via_loader() -> None:
    """load_counted_fusion passes the member's transform to bucket_counted_devices."""
    from shapely.geometry import Polygon as ShapelyPolygon

    from app.interpretation.rooms import Room

    # Room only reachable at x ≥ 100 (after dx=100 translate).
    poly = ShapelyPolygon([(100.0, 0.0), (200.0, 0.0), (200.0, 10.0), (100.0, 10.0)])
    shifted_room = Room(
        id="r1",
        name=None,
        source="test",
        polygon=poly,
        area=1000.0,
        bounds=None,
        number="1.01",
    )
    reg = build_room_registry([shifted_room])

    translate = GridTransform(
        dx=100.0,
        dy=0.0,
        rotation_rad=0.0,
        scale=1.0,
        matched_labels=(),
        matched_count=0,
        max_residual_m=0.0,
        median_residual_m=0.0,
        quality="good",
    )
    member = RegisteredMember(revision_id=_REVISION_A, role="test", transform=translate, audit={})
    registration = FloorRegistration(
        reference_revision_id=_REVISION_A,
        reference_role="test",
        members_ok=(member,),
        members_failed=(),
    )

    async def _fake_load(db: Any, revision_id: UUID, **_: Any) -> list[TypedDevice]:
        return [_typed("d1", "SD", x=5.0, y=5.0)]  # x=5 + dx=100 → 105, inside room

    with patch(
        "app.interpretation.floor_counted_loaders.load_typed_devices",
        side_effect=_fake_load,
    ):
        from app.interpretation.floor_counted_loaders import load_counted_fusion

        result = await load_counted_fusion(None, registration, reg)  # type: ignore[arg-type]

    assert result.assignments[0].room_number == "1.01"


@pytest.mark.asyncio
async def test_per_member_isolation_one_raises() -> None:
    """If one member's load_typed_devices raises, the other member is still fused."""
    reg = _empty_registry()
    reg_obj = _registration(_REVISION_A, _REVISION_B)

    async def _fake_load(db: Any, revision_id: UUID, **_: Any) -> list[TypedDevice]:
        if revision_id == _REVISION_A:
            raise RuntimeError("simulated load failure")
        return [_typed("b1", "SD")]

    with patch(
        "app.interpretation.floor_counted_loaders.load_typed_devices",
        side_effect=_fake_load,
    ):
        from app.interpretation.floor_counted_loaders import load_counted_fusion

        # Must not raise.
        result = await load_counted_fusion(None, reg_obj, reg)  # type: ignore[arg-type]

    assert len(result.assignments) == 1
    assert result.assignments[0].device_id == "b1"


@pytest.mark.asyncio
async def test_all_members_fail_returns_empty() -> None:
    """If all members fail, return an empty CountedFusionResult without raising."""
    reg = _empty_registry()
    reg_obj = _registration(_REVISION_A)

    async def _always_fail(db: object, revision_id: UUID, **_: object) -> list[TypedDevice]:
        raise RuntimeError("always fail")

    with patch(
        "app.interpretation.floor_counted_loaders.load_typed_devices",
        side_effect=_always_fail,
    ):
        from app.interpretation.floor_counted_loaders import load_counted_fusion

        result = await load_counted_fusion(None, reg_obj, reg)  # type: ignore[arg-type]

    assert result.assignments == ()
    assert result.counts_by_room == {}


# ---------------------------------------------------------------------------
# Isolation-import test: floor_counted_loaders must not import app.api at module level
# ---------------------------------------------------------------------------


def test_floor_counted_loaders_no_module_level_app_api_import() -> None:
    """Importing floor_counted_loaders in isolation must not pull any app.api module.

    The lazy-import pattern (#705) must be honoured: any app.api use must be inside
    function bodies, not at module scope.

    Arrange: fresh subprocess imports only floor_counted_loaders.
    Act:     run in fresh subprocess.
    Assert:  exit 0 and 'app.api' not in sys.modules output.
    """
    script = (
        "import sys\n"
        "import app.interpretation.floor_counted_loaders\n"
        "api_mods = [k for k in sys.modules if k.startswith('app.api')]\n"
        "if api_mods:\n"
        "    print('FORBIDDEN:', api_mods, file=sys.stderr)\n"
        "    sys.exit(1)\n"
        "print('ok')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (
        "floor_counted_loaders imported app.api at module level.\n"
        f"stderr: {result.stderr}\nstdout: {result.stdout}"
    )
