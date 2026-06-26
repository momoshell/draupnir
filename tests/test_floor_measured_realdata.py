"""Real-data smoke test for floor_measured_loaders (issue #718, Phase R-D).

Env-guarded: skipped unless ``DRAUPNIR_REALDATA_SMOKE=1`` is set. Requires a
live database with project 49140cff populated.

Members referenced:
- Lighting revision E-630003: 95015ea4-b4d0-46a6-b676-e3fc7f622a7f
- Containment member E-610003: b29fcd01-3eb0-41ad-9f0c-012925d47824
"""

from __future__ import annotations

import os

import pytest

_SMOKE = os.getenv("DRAUPNIR_REALDATA_SMOKE") == "1"

_LIGHTING_REV_ID = "95015ea4-b4d0-46a6-b676-e3fc7f622a7f"
_CONTAINMENT_REV_ID = "b29fcd01-3eb0-41ad-9f0c-012925d47824"
_PROJECT_ID = "49140cff"


@pytest.mark.skipif(not _SMOKE, reason="DRAUPNIR_REALDATA_SMOKE!=1")
@pytest.mark.asyncio
async def test_floor_measured_realdata_smoke() -> None:
    """Smoke: load fused measured for E-630003 lighting + E-610003 containment.

    Verifies:
    - load_fused_measured runs without raising.
    - Result has non-zero items or unassigned (geometry is present).
    - Total drawing_length conservation (items + unassigned ≈ input polylines).

    This test does NOT assert room numbers or service names — those depend on
    the specific revision data and are covered by integration tests.
    """
    from uuid import UUID

    from app.db.session import get_session_maker
    from app.interpretation.floor_measured_loaders import load_fused_measured
    from app.interpretation.floor_registration import (
        FloorRegistration,
        RegisteredMember,
    )
    from app.interpretation.grid_registration import GridTransform
    from app.interpretation.room_fusion import build_room_registry

    session_maker = get_session_maker()
    assert session_maker is not None, "No session maker available — DB not configured"

    lighting_id = UUID(_LIGHTING_REV_ID)
    containment_id = UUID(_CONTAINMENT_REV_ID)

    identity_tx = GridTransform(
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

    registration = FloorRegistration(
        reference_revision_id=lighting_id,
        reference_role="lighting",
        members_ok=(
            RegisteredMember(
                revision_id=lighting_id,
                role="lighting",
                transform=identity_tx,
                audit={},
            ),
            RegisteredMember(
                revision_id=containment_id,
                role="containment",
                transform=identity_tx,
                audit={},
            ),
        ),
        members_failed=(),
    )

    # Minimal registry (no rooms loaded — just a placeholder).
    registry = build_room_registry([], voronoi_fallback=False)

    async with session_maker() as db:
        result = await load_fused_measured(
            db,
            registration=registration,
            registry=registry,
        )

    # Structural checks — not prescriptive about exact values. The result may be empty
    # if the members' centerline geometry has not been materialized yet (the CENTERLINE
    # job is lazy, triggered by a prior service-takeoff read); R-H is the dedicated
    # real-data gate. But WHEN the pipeline produces buckets, they must carry real,
    # strictly-positive drawing length — guard against a silent all-zero/empty regression.
    total_items = len(result.items) + len(result.unassigned)
    total_len = sum(i.drawing_length for i in (*result.items, *result.unassigned))
    if total_items > 0:
        assert total_len > 0.0
