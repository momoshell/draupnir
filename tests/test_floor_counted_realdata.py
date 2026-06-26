"""Real-data smoke test for the COUNTED (device) floor fusion (issue #719, Phase R-E).

Guarded by ``DRAUPNIR_REALDATA_SMOKE=1``.  Uses project 49140cff, reference E-630003
lighting revision 95015ea4-b4d0-46a6-b676-e3fc7f622a7f, and power member E-620003
bd2ac5ee-7de4-448c-b24e-cf0d294aaef0.

This test is intentionally thin — it exercises the full load+fuse pipeline and asserts
basic shape (non-empty results, no exception).  Detailed count assertions live in the
pure unit tests.
"""

from __future__ import annotations

import os
from typing import Any
from uuid import UUID

import pytest

_SKIP_REASON = "Set DRAUPNIR_REALDATA_SMOKE=1 to run real-data smoke tests"

pytestmark = pytest.mark.skipif(
    os.getenv("DRAUPNIR_REALDATA_SMOKE") != "1",
    reason=_SKIP_REASON,
)

_PROJECT_ID = UUID("49140cff-0000-0000-0000-000000000000")
_REFERENCE_REVISION_ID = UUID("95015ea4-b4d0-46a6-b676-e3fc7f622a7f")
_POWER_MEMBER_REVISION_ID = UUID("bd2ac5ee-7de4-448c-b24e-cf0d294aaef0")


@pytest.mark.asyncio
async def test_counted_fusion_realdata_smoke(db: Any) -> None:
    """Smoke: load+fuse counted devices for the reference + one power member.

    Asserts:
    - No exception raised.
    - At least one assignment produced.
    - excluded_kinds is a dict (may be empty if no architecture on this floor).
    - counts_by_room is a dict.
    """
    from app.interpretation.floor_counted_loaders import load_counted_fusion
    from app.interpretation.floor_registration import FloorMember
    from app.interpretation.floor_registration_loaders import load_floor_registration
    from app.interpretation.room_fusion_loaders import load_room_registry

    registry = await load_room_registry(db, _REFERENCE_REVISION_ID)

    registration = await load_floor_registration(
        db,
        reference_revision_id=_REFERENCE_REVISION_ID,
        reference_role="lighting",
        members=[
            FloorMember(revision_id=_POWER_MEMBER_REVISION_ID, role="power"),
        ],
    )

    result = await load_counted_fusion(db, registration, registry)

    assert isinstance(result.excluded_kinds, dict)
    assert isinstance(result.counts_by_room, dict)
    # The reference revision must contribute at least one counted device on a real drawing.
    assert len(result.assignments) >= 1
