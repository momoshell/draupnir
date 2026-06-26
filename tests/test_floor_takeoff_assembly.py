"""Mocked (non-DB) tests for floor_takeoff_loaders (issue #721, Phase R-G).

All tests mock the 5 floor loaders so no real database is needed.  The test DB
isolation lane (conftest) is NOT used here — these run in the standard pytest process.

Coverage targets:
- ``parse_member``: happy path, no-colon error, invalid UUID, empty role.
- ``assemble_floor_takeoff``: happy-path mocked run returns FloorTakeoffResponse.
- All three kind sub-blocks flow into rooms and unassigned correctly.
- ``summary`` fields are computed (rooms, named_rooms, etc.).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest

from app.interpretation.floor_counted import CountedDeviceAssignment, CountedFusionResult
from app.interpretation.floor_estimated import (
    EstimatedCircuitContribution,
    FusedEstimatedResult,
    RoomEstimatedContribution,
)
from app.interpretation.floor_measured import FusedMeasuredItem, FusedMeasuredResult
from app.interpretation.floor_registration import (
    FailedMember,
    FloorRegistration,
    RegisteredMember,
)
from app.interpretation.floor_takeoff_loaders import assemble_floor_takeoff, parse_member
from app.interpretation.grid_registration import GridTransform
from app.interpretation.room_fusion import RoomRegistry, build_room_registry
from app.schemas.floor_takeoff import FloorTakeoffResponse

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_REF_REV = UUID("aaaa0000-0000-0000-0000-000000000001")
_MEM_REV = UUID("bbbb0000-0000-0000-0000-000000000002")

_IDENTITY = GridTransform(
    dx=0.0,
    dy=0.0,
    rotation_rad=0.0,
    scale=1.0,
    matched_labels=(),
    matched_count=3,
    max_residual_m=0.001,
    median_residual_m=0.001,
    quality="good",
)

# ---------------------------------------------------------------------------
# parse_member unit tests
# ---------------------------------------------------------------------------


def test_parse_member_happy_path() -> None:
    rid, role = parse_member(f"{_REF_REV}:lighting")
    assert rid == _REF_REV
    assert role == "lighting"


def test_parse_member_role_with_colon() -> None:
    """Everything after the first colon is the role (colons in role are allowed)."""
    rid, role = parse_member(f"{_REF_REV}:multi:part:role")
    assert rid == _REF_REV
    assert role == "multi:part:role"


def test_parse_member_no_colon_raises() -> None:
    with pytest.raises(ValueError, match="colon-separated"):
        parse_member("not-a-valid-token")


def test_parse_member_invalid_uuid_raises() -> None:
    with pytest.raises(ValueError, match="not a valid UUID"):
        parse_member("not-a-uuid:role")


def test_parse_member_empty_role_raises() -> None:
    with pytest.raises(ValueError, match="role is empty"):
        parse_member(f"{_REF_REV}:")


# ---------------------------------------------------------------------------
# Helpers for mocked assembly tests
# ---------------------------------------------------------------------------


def _good_audit(revision_id: UUID, role: str) -> dict[str, object]:
    return {
        "dx": 0.0,
        "dy": 0.0,
        "rotation_rad": 0.0,
        "scale": 1.0,
        "matched_count": 3,
        "matched_labels": [],
        "median_residual_m": 0.001,
        "max_residual_m": 0.002,
        "quality": "good",
        "revision_id": str(revision_id),
        "role": role,
        "reference_revision_id": str(_REF_REV),
    }


def _empty_registration() -> FloorRegistration:
    ref_member = RegisteredMember(
        revision_id=_REF_REV,
        role="lighting",
        transform=_IDENTITY,
        audit=_good_audit(_REF_REV, "lighting"),
    )
    mem_member = RegisteredMember(
        revision_id=_MEM_REV,
        role="containment",
        transform=_IDENTITY,
        audit=_good_audit(_MEM_REV, "containment"),
    )
    return FloorRegistration(
        reference_revision_id=_REF_REV,
        reference_role="lighting",
        members_ok=(ref_member, mem_member),
        members_failed=(),
    )


def _empty_registry() -> RoomRegistry:
    return build_room_registry([])


def _empty_measured() -> FusedMeasuredResult:
    return FusedMeasuredResult(items=(), unassigned=(), unscaled=False)


def _measured_with_one_room() -> FusedMeasuredResult:
    item = FusedMeasuredItem(
        member_revision_id=str(_REF_REV),
        role="lighting",
        service="cable",
        size_raw=None,
        size_kind=None,
        room_id="room-1",
        room_number="1.01",
        room_name="Office",
        boundary_basis="polygon",
        confidence=0.9,
        needs_review=False,
        drawing_length=100.0,
        real_length_m=10.0,
        basis="real_world",
        length_provisional=False,
    )
    return FusedMeasuredResult(items=(item,), unassigned=(), unscaled=False)


def _empty_counted() -> CountedFusionResult:
    return CountedFusionResult(assignments=(), counts_by_room={}, excluded_kinds={})


def _counted_with_one_room() -> CountedFusionResult:
    return CountedFusionResult(
        assignments=(),
        counts_by_room={"1.01": {"spot_light": 3}},
        excluded_kinds={},
    )


def _counted_only_room_with_metadata() -> CountedFusionResult:
    """A counted result where the room exists ONLY in counted (no measured/estimated).

    The assignment carries room metadata so assemble_floor_takeoff can fill room_id etc.
    """
    assignment = CountedDeviceAssignment(
        device_id="dev-99",
        type_name="downlight",
        source_revision_id=str(_MEM_REV),
        room_number="2.01",
        room_id="room-2",
        boundary_basis="polygon",
        confidence=0.85,
        needs_review=False,
    )
    return CountedFusionResult(
        assignments=(assignment,),
        counts_by_room={"2.01": {"downlight": 2}},
        excluded_kinds={},
    )


def _empty_estimated() -> FusedEstimatedResult:
    unassigned = RoomEstimatedContribution(
        room_number=None,
        room_id=None,
        boundary_basis=None,
        confidence=None,
        needs_review=False,
        device_drop_m=0.0,
        home_run_m=0.0,
        circuit_ids=(),
    )
    return FusedEstimatedResult(
        per_room=(),
        unassigned=unassigned,
        estimated_circuits=(),
        quantity_kind="estimated",
        reliability={
            "device_drop_m": "reliable",
            "in_plan_length_m": "schematic_provisional",
            "note": "",
        },
        params_stamp={"spare_fraction": 0.1},
        source={"lighting_revision_id": str(_REF_REV), "containment_revision_id": None},
    )


def _estimated_with_one_room() -> FusedEstimatedResult:
    room_contrib = RoomEstimatedContribution(
        room_number="1.01",
        room_id="room-1",
        boundary_basis="polygon",
        confidence=0.9,
        needs_review=False,
        device_drop_m=2.5,
        home_run_m=5.0,
        circuit_ids=(1,),
    )
    unassigned = RoomEstimatedContribution(
        room_number=None,
        room_id=None,
        boundary_basis=None,
        confidence=None,
        needs_review=False,
        device_drop_m=0.0,
        home_run_m=0.0,
        circuit_ids=(),
    )
    circuit_contrib = EstimatedCircuitContribution(
        circuit_id=1,
        in_plan_length_m=12.0,
        rooms_touched=("1.01",),
    )
    return FusedEstimatedResult(
        per_room=(room_contrib,),
        unassigned=unassigned,
        estimated_circuits=(circuit_contrib,),
        quantity_kind="estimated",
        reliability={
            "device_drop_m": "reliable",
            "in_plan_length_m": "schematic_provisional",
            "note": "",
        },
        params_stamp={"spare_fraction": 0.1},
        source={"lighting_revision_id": str(_REF_REV), "containment_revision_id": None},
    )


# ---------------------------------------------------------------------------
# Assembly tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_assemble_floor_takeoff_empty_returns_200() -> None:
    """assemble_floor_takeoff with all-empty loaders returns a valid FloorTakeoffResponse."""
    # The lazy import inside assemble_floor_takeoff resolves room_fusion_loaders at call
    # time, so patch that module's function rather than a not-yet-existing module attribute.
    with (
        patch(
            "app.interpretation.floor_takeoff_loaders.load_floor_registration",
            new=AsyncMock(return_value=_empty_registration()),
        ),
        patch(
            "app.interpretation.floor_takeoff_loaders.load_fused_measured",
            new=AsyncMock(return_value=_empty_measured()),
        ),
        patch(
            "app.interpretation.floor_takeoff_loaders.load_counted_fusion",
            new=AsyncMock(return_value=_empty_counted()),
        ),
        patch(
            "app.interpretation.floor_takeoff_loaders.load_estimated_fusion",
            new=AsyncMock(return_value=_empty_estimated()),
        ),
        patch(
            "app.interpretation.room_fusion_loaders.load_room_registry",
            new=AsyncMock(return_value=_empty_registry()),
        ),
    ):
        response = await assemble_floor_takeoff(
            AsyncMock(),
            reference_revision_id=_REF_REV,
            reference_role="lighting",
            members=[(_MEM_REV, "containment")],
            containment_revision_id=None,
            scope="sheet",
            strategy="auto",
            snap_tolerance=0.0,
            min_area=0.0,
            voronoi_fallback=True,
        )

    assert isinstance(response, FloorTakeoffResponse)
    assert response.reference_revision_id == _REF_REV
    assert response.reference_role == "lighting"
    assert response.rooms == []
    assert response.summary.rooms == 0
    assert response.summary.members_registered == 2
    assert response.summary.members_failed == 0
    assert response.summary.unscaled is False
    assert response.estimated_meta is not None
    assert response.estimated_meta.quantity_kind == "estimated"


@pytest.mark.asyncio
async def test_assemble_floor_takeoff_with_one_room() -> None:
    """assemble_floor_takeoff populates a RoomBlock for room 1.01."""
    with (
        patch(
            "app.interpretation.floor_takeoff_loaders.load_floor_registration",
            new=AsyncMock(return_value=_empty_registration()),
        ),
        patch(
            "app.interpretation.floor_takeoff_loaders.load_fused_measured",
            new=AsyncMock(return_value=_measured_with_one_room()),
        ),
        patch(
            "app.interpretation.floor_takeoff_loaders.load_counted_fusion",
            new=AsyncMock(return_value=_counted_with_one_room()),
        ),
        patch(
            "app.interpretation.floor_takeoff_loaders.load_estimated_fusion",
            new=AsyncMock(return_value=_estimated_with_one_room()),
        ),
        patch(
            "app.interpretation.room_fusion_loaders.load_room_registry",
            new=AsyncMock(return_value=_empty_registry()),
        ),
    ):
        response = await assemble_floor_takeoff(
            AsyncMock(),
            reference_revision_id=_REF_REV,
            reference_role="lighting",
            members=[(_MEM_REV, "containment")],
            containment_revision_id=None,
            scope="sheet",
            strategy="auto",
            snap_tolerance=0.0,
            min_area=0.0,
            voronoi_fallback=True,
        )

    assert len(response.rooms) == 1
    room = response.rooms[0]
    assert room.room_number == "1.01"
    assert room.room_name == "Office"
    assert len(room.measured) == 1
    assert room.measured[0].service == "cable"
    assert room.measured[0].real_length_m == pytest.approx(10.0)
    assert len(room.counted) == 1
    assert room.counted[0].type_name == "spot_light"
    assert room.counted[0].count == 3
    assert room.estimated is not None
    assert room.estimated.device_drop_m == pytest.approx(2.5)
    assert room.estimated.home_run_m == pytest.approx(5.0)
    assert room.estimated.circuit_ids == [1]

    # Floor-level circuits
    assert len(response.estimated_circuits) == 1
    assert response.estimated_circuits[0].circuit_id == 1
    assert response.estimated_circuits[0].in_plan_length_m == pytest.approx(12.0)

    # Summary
    assert response.summary.rooms == 1
    assert response.summary.named_rooms == 1
    assert response.summary.total_measured_m_by_discipline == {"lighting": pytest.approx(10.0)}
    assert response.summary.no_anchor_fraction == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_assemble_floor_takeoff_failed_members() -> None:
    """assemble_floor_takeoff surfaces members_failed from registration."""
    failed_reg = FloorRegistration(
        reference_revision_id=_REF_REV,
        reference_role="lighting",
        members_ok=(
            RegisteredMember(
                revision_id=_REF_REV,
                role="lighting",
                transform=_IDENTITY,
                audit=_good_audit(_REF_REV, "lighting"),
            ),
        ),
        members_failed=(
            FailedMember(
                revision_id=_MEM_REV,
                role="containment",
                reason="no_fiducials",
            ),
        ),
    )
    with (
        patch(
            "app.interpretation.floor_takeoff_loaders.load_floor_registration",
            new=AsyncMock(return_value=failed_reg),
        ),
        patch(
            "app.interpretation.floor_takeoff_loaders.load_fused_measured",
            new=AsyncMock(return_value=_empty_measured()),
        ),
        patch(
            "app.interpretation.floor_takeoff_loaders.load_counted_fusion",
            new=AsyncMock(return_value=_empty_counted()),
        ),
        patch(
            "app.interpretation.floor_takeoff_loaders.load_estimated_fusion",
            new=AsyncMock(return_value=_empty_estimated()),
        ),
        patch(
            "app.interpretation.room_fusion_loaders.load_room_registry",
            new=AsyncMock(return_value=_empty_registry()),
        ),
    ):
        response = await assemble_floor_takeoff(
            AsyncMock(),
            reference_revision_id=_REF_REV,
            reference_role="lighting",
            members=[(_MEM_REV, "containment")],
            containment_revision_id=None,
            scope="sheet",
            strategy="auto",
            snap_tolerance=0.0,
            min_area=0.0,
            voronoi_fallback=True,
        )

    assert response.summary.members_registered == 1
    assert response.summary.members_failed == 1
    assert len(response.members_failed) == 1
    assert response.members_failed[0].revision_id == _MEM_REV
    assert response.members_failed[0].reason == "no_fiducials"


@pytest.mark.asyncio
async def test_assemble_floor_takeoff_counted_only_room_gets_metadata() -> None:
    """A room present only in counted (no measured/estimated) gets non-null metadata.

    CountedFusionResult.assignments carry room_id/boundary_basis/confidence; the
    assembler must fill room metadata from assignments for counted-only rooms.
    """
    with (
        patch(
            "app.interpretation.floor_takeoff_loaders.load_floor_registration",
            new=AsyncMock(return_value=_empty_registration()),
        ),
        patch(
            "app.interpretation.floor_takeoff_loaders.load_fused_measured",
            new=AsyncMock(return_value=_empty_measured()),
        ),
        patch(
            "app.interpretation.floor_takeoff_loaders.load_counted_fusion",
            new=AsyncMock(return_value=_counted_only_room_with_metadata()),
        ),
        patch(
            "app.interpretation.floor_takeoff_loaders.load_estimated_fusion",
            new=AsyncMock(return_value=_empty_estimated()),
        ),
        patch(
            "app.interpretation.room_fusion_loaders.load_room_registry",
            new=AsyncMock(return_value=_empty_registry()),
        ),
    ):
        response = await assemble_floor_takeoff(
            AsyncMock(),
            reference_revision_id=_REF_REV,
            reference_role="lighting",
            members=[(_MEM_REV, "containment")],
            containment_revision_id=None,
            scope="sheet",
            strategy="auto",
            snap_tolerance=0.0,
            min_area=0.0,
            voronoi_fallback=True,
        )

    assert len(response.rooms) == 1
    room = response.rooms[0]
    assert room.room_number == "2.01"
    # Metadata must be non-null (populated from assignments, not all-None).
    assert room.room_id == "room-2"
    assert room.boundary_basis == "polygon"
    assert room.confidence == pytest.approx(0.85)
    assert room.needs_review is False
    # Counted items present.
    assert len(room.counted) == 1
    assert room.counted[0].type_name == "downlight"
    assert room.counted[0].count == 2
    # Measured and estimated absent.
    assert room.measured == []
    assert room.estimated is None
