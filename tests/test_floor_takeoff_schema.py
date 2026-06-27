"""Tests for the FloorTakeoffResponse schema (issue #717, Phase R-C / R-G update).

All tests use only Pydantic model construction — no DB, no FastAPI.

Phase R-G replaced the R-C skeleton schema with the fully populated schema.
Tests are updated to match the new field names.
"""

from __future__ import annotations

import uuid

import pytest
from pydantic import ValidationError

from app.schemas.floor_takeoff import (
    EstimatedCircuitItem,
    FailedMemberRead,
    FloorTakeoffResponse,
    FloorTakeoffSummary,
    MemberRegistrationRead,
    RoomBlock,
    RoomCountedItem,
    RoomEstimatedBlock,
    RoomMeasuredItem,
    UnassignedBlock,
)

# ---------------------------------------------------------------------------
# Leaf models
# ---------------------------------------------------------------------------


def test_member_registration_read_constructs() -> None:
    """MemberRegistrationRead accepts valid fields and rejects extras."""
    rev_id = uuid.uuid4()
    m = MemberRegistrationRead(
        revision_id=rev_id,
        role="lighting",
        quality="good",
        dx=0.0,
        dy=0.0,
        rotation_rad=0.0,
        scale=1.0,
        matched_count=3,
        median_residual_m=0.001,
        max_residual_m=0.002,
    )
    assert m.revision_id == rev_id
    assert m.role == "lighting"
    assert m.quality == "good"


def test_member_registration_read_forbids_extra() -> None:
    """MemberRegistrationRead has extra='forbid' — raises ValidationError on unknown fields."""
    rev_id = uuid.uuid4()
    with pytest.raises(ValidationError):
        MemberRegistrationRead.model_validate(
            {
                "revision_id": str(rev_id),
                "role": "lighting",
                "quality": "good",
                "dx": 0.0,
                "dy": 0.0,
                "rotation_rad": 0.0,
                "scale": 1.0,
                "matched_count": 3,
                "median_residual_m": None,
                "max_residual_m": None,
                "unexpected_field": "x",
            }
        )


def test_failed_member_read_constructs() -> None:
    """FailedMemberRead accepts minimal fields."""
    rev_id = uuid.uuid4()
    f = FailedMemberRead(revision_id=rev_id, role="containment", reason="no_fiducials")
    assert f.revision_id == rev_id
    assert f.reason == "no_fiducials"


def test_failed_member_read_forbids_extra() -> None:
    """FailedMemberRead has extra='forbid'."""
    rev_id = uuid.uuid4()
    with pytest.raises(ValidationError):
        FailedMemberRead.model_validate(
            {"revision_id": str(rev_id), "role": "x", "reason": "y", "rogue": "z"}
        )


# ---------------------------------------------------------------------------
# Kind sub-block models
# ---------------------------------------------------------------------------


def test_room_measured_item_constructs() -> None:
    rev_id = str(uuid.uuid4())
    item = RoomMeasuredItem(
        member_revision_id=rev_id,
        role="lighting",
        service="cable",
        size_raw=None,
        size_kind=None,
        boundary_basis="polygon",
        confidence=0.9,
        needs_review=False,
        drawing_length=100.0,
        real_length_m=10.0,
        basis="real_world",
        length_provisional=False,
    )
    assert item.real_length_m == pytest.approx(10.0)
    assert item.basis == "real_world"


def test_room_measured_item_rejects_negative_drawing_length() -> None:
    rev_id = str(uuid.uuid4())
    with pytest.raises(ValidationError):
        RoomMeasuredItem(
            member_revision_id=rev_id,
            role="lighting",
            service="cable",
            size_raw=None,
            size_kind=None,
            boundary_basis="polygon",
            confidence=None,
            needs_review=False,
            drawing_length=-1.0,
            real_length_m=None,
            basis="drawing_units_only",
            length_provisional=False,
        )


def test_room_counted_item_constructs() -> None:
    item = RoomCountedItem(type_name="spot_light", count=4, source_revision_id=None)
    assert item.count == 4
    assert item.source_revision_id is None


def test_room_counted_item_rejects_negative_count() -> None:
    with pytest.raises(ValidationError):
        RoomCountedItem(type_name="fitting", count=-1, source_revision_id=None)


def test_room_estimated_block_defaults() -> None:
    block = RoomEstimatedBlock(
        quantity_kind="estimated",
        device_drop_m=0.0,
        home_run_m=0.0,
    )
    assert block.circuit_ids == []
    assert block.quantity_kind == "estimated"


def test_room_estimated_block_with_ids() -> None:
    block = RoomEstimatedBlock(
        quantity_kind="estimated",
        device_drop_m=2.5,
        home_run_m=5.0,
        circuit_ids=[1, 2, 3],
    )
    assert block.circuit_ids == [1, 2, 3]
    assert block.device_drop_m == pytest.approx(2.5)


# ---------------------------------------------------------------------------
# RoomBlock
# ---------------------------------------------------------------------------


def test_room_block_minimal_defaults() -> None:
    """RoomBlock constructs with minimal fields; kind sub-blocks default to empty."""
    block = RoomBlock.model_validate({"room_number": "1.01"})
    assert block.room_id is None
    assert block.boundary_basis is None
    assert block.measured == []
    assert block.counted == []
    assert block.estimated is None
    assert block.needs_review is False


def test_room_block_full_fields() -> None:
    """RoomBlock accepts all optional fields."""
    rev_id = str(uuid.uuid4())
    block = RoomBlock(
        room_id="r-abc",
        room_number="2.01",
        room_name="Meeting Room",
        boundary_basis="polygon",
        confidence=None,
        needs_review=False,
        measured=[
            RoomMeasuredItem(
                member_revision_id=rev_id,
                role="lighting",
                service="cable",
                size_raw=None,
                size_kind=None,
                boundary_basis="polygon",
                confidence=None,
                needs_review=False,
                drawing_length=50.0,
                real_length_m=5.0,
                basis="real_world",
                length_provisional=False,
            )
        ],
        counted=[RoomCountedItem(type_name="fitting", count=2, source_revision_id=None)],
        estimated=RoomEstimatedBlock(
            quantity_kind="estimated",
            device_drop_m=1.0,
            home_run_m=2.0,
            circuit_ids=[7],
        ),
    )
    assert block.room_number == "2.01"
    assert len(block.measured) == 1
    assert block.estimated is not None
    assert block.estimated.circuit_ids == [7]


def test_room_block_accepts_unknown_fields_via_model_validate() -> None:
    """RoomBlock does NOT have extra='forbid' — model_validate silently ignores extras.

    R-D/E/F added fields incrementally; extra='forbid' would break forward compat.
    """
    block = RoomBlock.model_validate({"room_number": "3.01", "future_field": "some-value"})
    assert block.room_number == "3.01"


# ---------------------------------------------------------------------------
# UnassignedBlock
# ---------------------------------------------------------------------------


def test_unassigned_block_defaults() -> None:
    u = UnassignedBlock(estimated=None)
    assert u.measured == []
    assert u.counted == []
    assert u.estimated is None


def test_unassigned_block_with_items() -> None:
    rev_id = str(uuid.uuid4())
    u = UnassignedBlock(
        measured=[
            RoomMeasuredItem(
                member_revision_id=rev_id,
                role="lighting",
                service="pipe",
                size_raw=None,
                size_kind=None,
                boundary_basis=None,
                confidence=None,
                needs_review=False,
                drawing_length=30.0,
                real_length_m=3.0,
                basis="real_world",
                length_provisional=False,
            )
        ],
        estimated=RoomEstimatedBlock(
            quantity_kind="estimated",
            device_drop_m=0.0,
            home_run_m=1.5,
            circuit_ids=[],
        ),
    )
    assert len(u.measured) == 1
    assert u.estimated is not None
    assert u.estimated.home_run_m == pytest.approx(1.5)


# ---------------------------------------------------------------------------
# EstimatedCircuitItem
# ---------------------------------------------------------------------------


def test_estimated_circuit_item_constructs() -> None:
    item = EstimatedCircuitItem(
        circuit_id=42,
        in_plan_length_m=15.0,
        rooms_touched=["1.01", "1.02"],
    )
    assert item.circuit_id == 42
    assert item.in_plan_length_m == pytest.approx(15.0)
    assert item.rooms_touched == ["1.01", "1.02"]


# ---------------------------------------------------------------------------
# FloorTakeoffSummary
# ---------------------------------------------------------------------------


def test_floor_takeoff_summary_constructs() -> None:
    s = FloorTakeoffSummary(
        rooms=5,
        named_rooms=4,
        members_registered=2,
        members_failed=0,
        voronoi_fallback_rooms=1,
        unscaled=False,
        no_anchor_fraction=0.0,
        no_anchor_status="ok",
        total_measured_m_by_discipline={"lighting": 10.0},
    )
    assert s.rooms == 5
    assert s.no_anchor_fraction == pytest.approx(0.0)
    assert s.no_anchor_status == "ok"
    assert s.total_measured_m_by_discipline["lighting"] == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# FloorTakeoffResponse
# ---------------------------------------------------------------------------


def test_floor_takeoff_response_minimal() -> None:
    """FloorTakeoffResponse constructs with required fields; optional fields default."""
    ref_id = uuid.uuid4()
    response = FloorTakeoffResponse(
        reference_revision_id=ref_id,
        reference_role="lighting",
        room_registry_strategy="auto",
        voronoi_fallback_enabled=True,
        estimated_meta=None,
        summary=FloorTakeoffSummary(
            rooms=0,
            named_rooms=0,
            members_registered=0,
            members_failed=0,
            voronoi_fallback_rooms=0,
            unscaled=False,
            no_anchor_fraction=0.0,
            no_anchor_status="ok",
            total_measured_m_by_discipline={},
        ),
    )
    assert response.reference_revision_id == ref_id
    assert response.reference_role == "lighting"
    assert response.rooms == []
    assert response.unassigned.measured == []
    assert response.voronoi_fallback_enabled is True
    assert response.estimated_meta is None
    assert response.estimated_circuits == []


def test_floor_takeoff_response_three_kinds_are_separate_fields() -> None:
    """The three kind sub-blocks are separate fields on RoomBlock — not summed."""
    rev_id = str(uuid.uuid4())
    block = RoomBlock.model_validate(
        {
            "room_number": "5.01",
            "measured": [
                {
                    "member_revision_id": rev_id,
                    "role": "lighting",
                    "service": "cable",
                    "size_raw": None,
                    "size_kind": None,
                    "boundary_basis": "polygon",
                    "confidence": None,
                    "needs_review": False,
                    "drawing_length": 100.0,
                    "real_length_m": 10.0,
                    "basis": "real_world",
                    "length_provisional": False,
                }
            ],
            "counted": [{"type_name": "device", "count": 3}],
            "estimated": {
                "quantity_kind": "estimated",
                "device_drop_m": 1.0,
                "home_run_m": 2.0,
                "circuit_ids": [11],
            },
        }
    )
    # Verify all three coexist without collision.
    assert len(block.measured) == 1
    assert len(block.counted) == 1
    assert block.estimated is not None
    assert block.estimated.circuit_ids == [11]
