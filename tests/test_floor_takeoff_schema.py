"""Tests for the FloorTakeoffResponse schema skeleton (issue #717, Phase R-C).

All tests use only Pydantic model construction — no DB, no FastAPI.
"""

from __future__ import annotations

import uuid

import pytest
from pydantic import ValidationError

from app.schemas.floor_takeoff import (
    EstimatedCircuitItem,
    FloorMemberRead,
    FloorTakeoffResponse,
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
    m = MemberRegistrationRead(member_type="cable", display_name="Cables", unit="m")
    assert m.member_type == "cable"
    assert m.unit == "m"


def test_member_registration_read_forbids_extra() -> None:
    """MemberRegistrationRead has extra='forbid' — raises ValidationError on unknown fields."""
    with pytest.raises(ValidationError):
        MemberRegistrationRead.model_validate(
            {"member_type": "pipe", "display_name": "Pipes", "unit": "m", "unexpected_field": "x"}
        )


def test_floor_member_read_constructs() -> None:
    """FloorMemberRead accepts minimal fields; display_name defaults to None."""
    f = FloorMemberRead(member_id="abc123", member_type="duct", display_name=None)
    assert f.member_id == "abc123"
    assert f.display_name is None


def test_floor_member_read_forbids_extra() -> None:
    """FloorMemberRead has extra='forbid' — raises ValidationError on unknown fields."""
    with pytest.raises(ValidationError):
        FloorMemberRead.model_validate({"member_id": "x", "member_type": "duct", "rogue": "y"})


# ---------------------------------------------------------------------------
# Kind sub-block models
# ---------------------------------------------------------------------------


def test_room_measured_item_constructs() -> None:
    item = RoomMeasuredItem(member_type="cable", length_m=12.5, reliability="reliable")
    assert item.length_m == pytest.approx(12.5)
    assert item.reliability == "reliable"


def test_room_measured_item_rejects_negative_length() -> None:
    with pytest.raises(ValidationError):
        RoomMeasuredItem(member_type="cable", length_m=-1.0, reliability="reliable")


def test_room_counted_item_constructs() -> None:
    item = RoomCountedItem(member_type="fitting", count=4)
    assert item.count == 4


def test_room_counted_item_rejects_negative_count() -> None:
    with pytest.raises(ValidationError):
        RoomCountedItem(member_type="fitting", count=-1)


def test_room_estimated_block_defaults() -> None:
    block = RoomEstimatedBlock()
    assert block.circuit_ids == []


def test_room_estimated_block_with_ids() -> None:
    block = RoomEstimatedBlock(circuit_ids=[1, 2, 3])
    assert block.circuit_ids == [1, 2, 3]


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
    block = RoomBlock(
        room_id="r-abc",
        room_number="2.01",
        room_name="Meeting Room",
        boundary_basis="polygon",
        confidence=None,
        needs_review=False,
        measured=[RoomMeasuredItem(member_type="cable", length_m=5.0, reliability="reliable")],
        counted=[RoomCountedItem(member_type="fitting", count=2)],
        estimated=RoomEstimatedBlock(circuit_ids=[7]),
    )
    assert block.room_number == "2.01"
    assert len(block.measured) == 1
    assert block.estimated is not None
    assert block.estimated.circuit_ids == [7]


def test_room_block_accepts_unknown_fields_via_model_validate() -> None:
    """RoomBlock does NOT have extra='forbid' — model_validate silently ignores extras.

    R-D/E/F will add fields incrementally; extra='forbid' would break forward compat.
    """
    # Unknown field should be silently ignored (not raise ValidationError).
    block = RoomBlock.model_validate({"room_number": "3.01", "future_field": "some-value"})
    assert block.room_number == "3.01"


# ---------------------------------------------------------------------------
# UnassignedBlock
# ---------------------------------------------------------------------------


def test_unassigned_block_defaults() -> None:
    u = UnassignedBlock()
    assert u.measured == []
    assert u.counted == []
    assert u.estimated_circuits == []


def test_unassigned_block_with_items() -> None:
    u = UnassignedBlock(
        measured=[RoomMeasuredItem(member_type="pipe", length_m=3.0, reliability="reliable")],
        estimated_circuits=[
            EstimatedCircuitItem(circuit_id=1, length_m=2.5, reliability="estimated_routed")
        ],
    )
    assert len(u.measured) == 1
    assert len(u.estimated_circuits) == 1
    assert u.estimated_circuits[0].circuit_id == 1


# ---------------------------------------------------------------------------
# FloorTakeoffResponse
# ---------------------------------------------------------------------------


def test_floor_takeoff_response_minimal() -> None:
    """FloorTakeoffResponse constructs with required fields; optional fields default."""
    rev_id = uuid.uuid4()
    response = FloorTakeoffResponse.model_validate(
        {
            "revision_id": str(rev_id),
            "room_registry_strategy": "explicit_layer",
        }
    )
    assert response.revision_id == rev_id
    assert response.floor_id is None
    assert response.rooms == []
    assert response.unassigned.measured == []
    assert response.voronoi_fallback_enabled is True


def test_floor_takeoff_response_with_rooms() -> None:
    """FloorTakeoffResponse accepts a populated rooms list."""
    rev_id = uuid.uuid4()
    response = FloorTakeoffResponse.model_validate(
        {
            "revision_id": str(rev_id),
            "floor_id": "GF",
            "rooms": [
                {"room_number": "1.01", "boundary_basis": "polygon"},
                {"room_number": "1.02", "boundary_basis": "voronoi", "needs_review": True},
            ],
            "room_registry_strategy": "auto",
            "voronoi_fallback_enabled": True,
        }
    )
    assert len(response.rooms) == 2
    assert response.rooms[1].needs_review is True
    assert response.floor_id == "GF"


def test_floor_takeoff_response_three_kinds_are_separate_fields() -> None:
    """The three kind sub-blocks are separate fields on RoomBlock — not summed."""
    block = RoomBlock.model_validate(
        {
            "room_number": "5.01",
            "measured": [{"member_type": "cable", "length_m": 10.0, "reliability": "reliable"}],
            "counted": [{"member_type": "device", "count": 3}],
            "estimated": {"circuit_ids": [11]},
        }
    )
    # Verify all three coexist without collision.
    assert len(block.measured) == 1
    assert len(block.counted) == 1
    assert block.estimated is not None
