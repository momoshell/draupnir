"""Env-guarded real-data smoke test for floor_estimated_loaders (issue #720, Phase R-F).

Skipped unless ``DRAUPNIR_REALDATA_SMOKE=1`` is set in the environment.

Uses the canonical Phase R test revision:
  - Project: 49140cff-...
  - Lighting: 95015ea4-b4d0-46a6-b676-e3fc7f622a7f
  - Containment: b29fcd01-3eb0-41ad-9f0c-012925d47824

Expected approximate totals (from the cable-estimate route):
  - total_device_drop_m ≈ 43.6
  - total_home_run_m ≈ 8.3
  - total_in_plan_length_m ≈ 259.2

Conservation invariants must hold (RuntimeError from the pure function would propagate here).
"""

from __future__ import annotations

import math
import os

import pytest

_SKIP_UNLESS_REALDATA = pytest.mark.skipif(
    os.environ.get("DRAUPNIR_REALDATA_SMOKE") != "1",
    reason="Set DRAUPNIR_REALDATA_SMOKE=1 to run real-data smoke tests",
)

_LIGHTING_REVISION_ID = "95015ea4-b4d0-46a6-b676-e3fc7f622a7f"
_CONTAINMENT_REVISION_ID = "b29fcd01-3eb0-41ad-9f0c-012925d47824"

# Tolerances — approximate expected totals from the cable-estimate route.
_EXPECTED_DEVICE_DROP = 43.6
_EXPECTED_HOME_RUN = 8.3
_EXPECTED_IN_PLAN = 259.2
_ABS_TOL = 5.0  # ±5 m tolerance for real-data variance


@_SKIP_UNLESS_REALDATA
@pytest.mark.asyncio
async def test_floor_estimated_realdata_conservation() -> None:
    """Real-data smoke: load_estimated_fusion conserves all three quantities.

    Arrangement: load from the canonical Phase R test project with both lighting and
    containment revisions. Act: call load_estimated_fusion. Assert: conservation holds
    (the pure function raises RuntimeError if violated), and totals are near expected.
    """
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from app.interpretation.floor_estimated_loaders import load_estimated_fusion
    from app.interpretation.floor_registration import FloorRegistration
    from app.interpretation.room_fusion import build_room_registry

    database_url = os.environ.get("DRAUPNIR_TEST_DB_URL")
    if not database_url:
        pytest.skip("DRAUPNIR_TEST_DB_URL not set")

    engine = create_async_engine(database_url, echo=False)
    async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    lighting_rev = UUID(_LIGHTING_REVISION_ID)
    containment_rev = UUID(_CONTAINMENT_REVISION_ID)

    async with async_session() as db:
        # Minimal registration: no registered members (no room classification needed
        # to pass conservation; rooms may be all-unassigned in this smoke test).
        registration = FloorRegistration(
            reference_revision_id=lighting_rev,
            reference_role="lighting",
            members_ok=(),
            members_failed=(),
        )
        registry = build_room_registry([])

        result = await load_estimated_fusion(
            db,
            registration,
            registry,
            lighting_revision_id=lighting_rev,
            containment_revision_id=containment_rev,
        )

    # Conservation: if the pure function didn't raise RuntimeError, conservation holds.
    total_drop = sum(r.device_drop_m for r in result.per_room) + result.unassigned.device_drop_m
    total_hr = sum(r.home_run_m for r in result.per_room) + result.unassigned.home_run_m
    total_ip = sum(c.in_plan_length_m for c in result.estimated_circuits)

    # Sanity check against known approximate totals.
    assert math.isclose(total_drop, _EXPECTED_DEVICE_DROP, abs_tol=_ABS_TOL), (
        f"device_drop total {total_drop:.2f} is far from expected {_EXPECTED_DEVICE_DROP:.2f}"
    )
    assert math.isclose(total_hr, _EXPECTED_HOME_RUN, abs_tol=_ABS_TOL), (
        f"home_run total {total_hr:.2f} is far from expected {_EXPECTED_HOME_RUN:.2f}"
    )
    assert math.isclose(total_ip, _EXPECTED_IN_PLAN, abs_tol=_ABS_TOL), (
        f"in_plan total {total_ip:.2f} is far from expected {_EXPECTED_IN_PLAN:.2f}"
    )

    # Metadata integrity.
    assert result.quantity_kind == "estimated"
    assert result.source["lighting_revision_id"] == _LIGHTING_REVISION_ID
    assert result.source["containment_revision_id"] == _CONTAINMENT_REVISION_ID

    await engine.dispose()
