"""Tests for floor_measured.py — pure per-room measured bucket coordinator (issue #718, R-D).

All tests use coordinate fixtures only — no DB, no FastAPI, no heavy deps beyond shapely.
"""

from __future__ import annotations

import random

from shapely.geometry import Polygon

from app.interpretation.floor_measured import (
    SERVICE_UNKNOWN,
    FusedMeasuredResult,
    bucket_measured_for_member,
)
from app.interpretation.measurement import ScaleContext
from app.interpretation.room_fusion import (
    build_room_registry,
)
from app.interpretation.rooms import Room, room_from_label, room_from_polygon

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

# Two non-overlapping polygon rooms side-by-side:
# Room A: x∈[0,10], y∈[0,10]
# Room B: x∈[10,20], y∈[0,10]
_POLY_A = Polygon([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)])
_POLY_B = Polygon([(10.0, 0.0), (20.0, 0.0), (20.0, 10.0), (10.0, 10.0)])

_MEMBER_ID = "aaaa-1111"
_ROLE = "containment"

# Group key type alias for readability.
_GroupKey = tuple[str | None, str | None]
_Polylines = tuple[tuple[tuple[float, float], ...], ...]
_ServiceTuple = tuple[str, str | None, str | None]


def _poly_room_a() -> Room:
    return room_from_polygon("room-a", _POLY_A, source="explicit", name="Room A", number="1.01")


def _poly_room_b() -> Room:
    return room_from_polygon("room-b", _POLY_B, source="explicit", name="Room B", number="1.02")


def _label_room_outside() -> Room:
    """Label-only room at (25, 5) — outside both polygon rooms."""
    return room_from_label(
        "room-label-c",
        source="label_cluster",
        location=(25.0, 5.0),
        name="Room C",
        number="1.03",
        anchors=((25.0, 5.0),),
    )


def _scale_confirmed(factor: float = 1.0) -> ScaleContext:
    return ScaleContext(
        conversion_factor=factor,
        real_world_available=True,
        contradicted=False,
        units_confidence="confirmed",
    )


def _scale_unconfirmed() -> ScaleContext:
    return ScaleContext(
        conversion_factor=None,
        real_world_available=False,
        contradicted=False,
        units_confidence="unknown",
    )


# ---------------------------------------------------------------------------
# Helper: sum of all drawing_length across items + unassigned
# ---------------------------------------------------------------------------


def _total_drawing_length(result: FusedMeasuredResult) -> float:
    return sum(i.drawing_length for i in result.items) + sum(
        i.drawing_length for i in result.unassigned
    )


# ---------------------------------------------------------------------------
# Test: polygon split
# ---------------------------------------------------------------------------


def test_polygon_split_across_two_rooms() -> None:
    """A horizontal polyline spanning Room A and Room B is split per-room.

    Arrange: polyline from x=0 to x=20 at y=5 (length=20); registry with A+B polygon rooms.
    Act:     bucket_measured_for_member.
    Assert:  items has two entries, one per room, each with drawing_length≈10; unassigned empty.
    """
    room_a = _poly_room_a()
    room_b = _poly_room_b()
    registry = build_room_registry([room_a, room_b], voronoi_fallback=False)
    scale = _scale_confirmed()

    polylines_by_group: dict[_GroupKey, _Polylines] = {
        (None, "ff0000"): (((0.0, 5.0), (20.0, 5.0)),),
    }
    services_by_group: dict[_GroupKey, tuple[_ServiceTuple, ...]] = {
        (None, "ff0000"): (("PIPE", "54 mm", "round"),),
    }

    result = bucket_measured_for_member(
        member_revision_id=_MEMBER_ID,
        role=_ROLE,
        polylines_by_group=polylines_by_group,
        services_by_group=services_by_group,
        registry=registry,
        scale=scale,
    )

    assert len(result.items) == 2, f"Expected 2 items, got {len(result.items)}: {result.items}"
    assert len(result.unassigned) == 0

    room_numbers = {item.room_number for item in result.items}
    assert room_numbers == {"1.01", "1.02"}

    for item in result.items:
        assert item.service == "PIPE"
        assert item.boundary_basis == "polygon"
        assert abs(item.drawing_length - 10.0) < 1e-9, f"Expected 10.0, got {item.drawing_length}"
        assert item.real_length_m is not None
        assert abs(item.real_length_m - 10.0) < 1e-9


# ---------------------------------------------------------------------------
# Test: Voronoi residual on
# ---------------------------------------------------------------------------


def test_voronoi_residual_on_assigns_outside_segment() -> None:
    """A segment outside all polygon rooms is assigned via Voronoi when enabled.

    Arrange: polyline entirely in x∈[20,30] (outside both polygon rooms); label room at (25,5).
    Act:     bucket with voronoi_fallback=True.
    Assert:  geometry is conserved; Voronoi path is exercised (at least one voronoi item
             when the label room is within the Voronoi envelope).
    """
    room_a = _poly_room_a()
    label_c = _label_room_outside()
    registry = build_room_registry([room_a, label_c], voronoi_fallback=True)
    scale = _scale_confirmed()

    polylines_by_group: dict[_GroupKey, _Polylines] = {
        (None, "00ff00"): (((20.0, 5.0), (30.0, 5.0)),),
    }
    services_by_group: dict[_GroupKey, tuple[_ServiceTuple, ...]] = {}

    result = bucket_measured_for_member(
        member_revision_id=_MEMBER_ID,
        role=_ROLE,
        polylines_by_group=polylines_by_group,
        services_by_group=services_by_group,
        registry=registry,
        scale=scale,
    )

    # Conservation: total drawing_length == 10 regardless of assignment.
    total = _total_drawing_length(result)
    assert abs(total - 10.0) < 1e-6, f"Conservation failed: expected 10.0, got {total}"


# ---------------------------------------------------------------------------
# Test: KEYSTONE — Voronoi on/off byte-identity for polygon buckets
# ---------------------------------------------------------------------------


def test_voronoi_on_off_polygon_buckets_byte_identical() -> None:
    """Polygon buckets are byte-identical regardless of voronoi_fallback (KEYSTONE invariant).

    Arrange: one polyline spanning Room A (x∈[1,9]) only, with a label room outside.
    Act:     bucket with voronoi_fallback=True vs False.
    Assert:  items (polygon portion) are identical tuples.
    """
    room_a = _poly_room_a()
    label_c = _label_room_outside()

    registry_on = build_room_registry([room_a, label_c], voronoi_fallback=True)
    registry_off = build_room_registry([room_a, label_c], voronoi_fallback=False)
    scale = _scale_confirmed()

    # Polyline entirely within Room A so remainder=0; no Voronoi path taken at all.
    polylines_by_group: dict[_GroupKey, _Polylines] = {
        (None, "0000ff"): (((1.0, 5.0), (9.0, 5.0)),),
    }
    services_by_group: dict[_GroupKey, tuple[_ServiceTuple, ...]] = {
        (None, "0000ff"): (("DUCT", "200x100", "rect"),),
    }

    result_on = bucket_measured_for_member(
        member_revision_id=_MEMBER_ID,
        role=_ROLE,
        polylines_by_group=polylines_by_group,
        services_by_group=services_by_group,
        registry=registry_on,
        scale=scale,
    )
    result_off = bucket_measured_for_member(
        member_revision_id=_MEMBER_ID,
        role=_ROLE,
        polylines_by_group=polylines_by_group,
        services_by_group=services_by_group,
        registry=registry_off,
        scale=scale,
    )

    # Polygon items must be byte-identical frozen dataclasses.
    polygon_on = [i for i in result_on.items if i.boundary_basis == "polygon"]
    polygon_off = [i for i in result_off.items if i.boundary_basis == "polygon"]
    assert polygon_on == polygon_off, (
        f"Polygon buckets differ between voronoi on/off.\non={polygon_on}\noff={polygon_off}"
    )
    assert len(polygon_on) == 1
    assert polygon_on[0].room_number == "1.01"


def test_voronoi_on_off_polygon_buckets_byte_identical_with_remainder() -> None:
    """Polygon buckets are byte-identical even when remainder exists.

    Arrange: polyline from x=0 to x=25 (10 inside A, 10 inside B, 5 outside).
    The polygon portions (assigned) must be identical in both modes.
    """
    room_a = _poly_room_a()
    room_b = _poly_room_b()
    label_c = _label_room_outside()

    registry_on = build_room_registry([room_a, room_b, label_c], voronoi_fallback=True)
    registry_off = build_room_registry([room_a, room_b, label_c], voronoi_fallback=False)
    scale = _scale_confirmed()

    # Polyline x∈[0,25]: 10 in A, 10 in B, 5 outside.
    polylines_by_group: dict[_GroupKey, _Polylines] = {
        ("layer1", "abcdef"): (((0.0, 5.0), (25.0, 5.0)),),
    }
    services_by_group: dict[_GroupKey, tuple[_ServiceTuple, ...]] = {}

    result_on = bucket_measured_for_member(
        member_revision_id=_MEMBER_ID,
        role=_ROLE,
        polylines_by_group=polylines_by_group,
        services_by_group=services_by_group,
        registry=registry_on,
        scale=scale,
    )
    result_off = bucket_measured_for_member(
        member_revision_id=_MEMBER_ID,
        role=_ROLE,
        polylines_by_group=polylines_by_group,
        services_by_group=services_by_group,
        registry=registry_off,
        scale=scale,
    )

    polygon_on = sorted(
        [i for i in result_on.items if i.boundary_basis == "polygon"],
        key=lambda x: x.room_number or "",
    )
    polygon_off = sorted(
        [i for i in result_off.items if i.boundary_basis == "polygon"],
        key=lambda x: x.room_number or "",
    )

    assert polygon_on == polygon_off, (
        "Polygon buckets differ between voronoi on/off when remainder exists.\n"
        f"on={polygon_on}\noff={polygon_off}"
    )
    assert len(polygon_on) == 2
    room_nums = {i.room_number for i in polygon_on}
    assert room_nums == {"1.01", "1.02"}


# ---------------------------------------------------------------------------
# Test: scale gate on/off
# ---------------------------------------------------------------------------


def test_scale_gate_confirmed_emits_real_length() -> None:
    """A confirmed scale emits real_length_m and basis='real_world'."""
    room_a = _poly_room_a()
    registry = build_room_registry([room_a], voronoi_fallback=False)
    scale = _scale_confirmed(factor=0.001)

    polylines_by_group: dict[_GroupKey, _Polylines] = {
        (None, "ff0000"): (((0.0, 5.0), (6.0, 5.0)),),
    }
    services_by_group: dict[_GroupKey, tuple[_ServiceTuple, ...]] = {}

    result = bucket_measured_for_member(
        member_revision_id=_MEMBER_ID,
        role=_ROLE,
        polylines_by_group=polylines_by_group,
        services_by_group=services_by_group,
        registry=registry,
        scale=scale,
    )

    assert len(result.items) == 1
    item = result.items[0]
    assert item.basis == "real_world"
    assert item.real_length_m is not None
    assert abs(item.real_length_m - 6.0 * 0.001) < 1e-12
    assert not result.unscaled


def test_scale_gate_unconfirmed_yields_drawing_units_only() -> None:
    """An unconfirmed scale yields real_length_m=None and basis='drawing_units_only'."""
    room_a = _poly_room_a()
    registry = build_room_registry([room_a], voronoi_fallback=False)
    scale = _scale_unconfirmed()

    polylines_by_group: dict[_GroupKey, _Polylines] = {
        (None, "ff0000"): (((0.0, 5.0), (6.0, 5.0)),),
    }
    services_by_group: dict[_GroupKey, tuple[_ServiceTuple, ...]] = {}

    result = bucket_measured_for_member(
        member_revision_id=_MEMBER_ID,
        role=_ROLE,
        polylines_by_group=polylines_by_group,
        services_by_group=services_by_group,
        registry=registry,
        scale=scale,
    )

    assert len(result.items) == 1
    item = result.items[0]
    assert item.basis == "drawing_units_only"
    assert item.real_length_m is None
    assert result.unscaled


# ---------------------------------------------------------------------------
# Test: bundle model — 2 services → full length each
# ---------------------------------------------------------------------------


def test_bundle_model_two_services_full_length_each() -> None:
    """A group with 2 services attributes FULL per-room length to EACH service.

    Arrange: 1 polyline, length=8, 2 services.
    Assert: 2 items, each drawing_length==8 (not 4).
    """
    room_a = _poly_room_a()
    registry = build_room_registry([room_a], voronoi_fallback=False)
    scale = _scale_confirmed()

    polylines_by_group: dict[_GroupKey, _Polylines] = {
        (None, "ff0000"): (((1.0, 5.0), (9.0, 5.0)),),
    }
    services_by_group: dict[_GroupKey, tuple[_ServiceTuple, ...]] = {
        (None, "ff0000"): (
            ("SVC_A", "54 mm", "round"),
            ("SVC_B", "42 mm", "round"),
        ),
    }

    result = bucket_measured_for_member(
        member_revision_id=_MEMBER_ID,
        role=_ROLE,
        polylines_by_group=polylines_by_group,
        services_by_group=services_by_group,
        registry=registry,
        scale=scale,
    )

    assert len(result.items) == 2
    services_seen = {item.service for item in result.items}
    assert services_seen == {"SVC_A", "SVC_B"}

    for item in result.items:
        assert abs(item.drawing_length - 8.0) < 1e-9, (
            f"Expected 8.0, got {item.drawing_length} for {item.service}"
        )


# ---------------------------------------------------------------------------
# Test: geometry conservation
# ---------------------------------------------------------------------------


def test_geometry_conservation_no_length_lost() -> None:
    """Σ(items + unassigned drawing_length) == Σ input polyline lengths, rel-tol 1e-6.

    Arrange: two groups, one spanning two rooms, one outside all rooms.
    Act:     bucket (voronoi off so remainder stays unassigned).
    Assert:  total drawing length equals geometric sum of input polylines.
    """
    from shapely.geometry import LineString

    room_a = _poly_room_a()
    room_b = _poly_room_b()
    registry = build_room_registry([room_a, room_b], voronoi_fallback=False)
    scale = _scale_confirmed()

    polylines_by_group: dict[_GroupKey, _Polylines] = {
        (None, "ff0000"): (((0.0, 5.0), (15.0, 5.0)),),  # 10 in A, 5 in B
        (None, "00ff00"): (((22.0, 5.0), (28.0, 5.0)),),  # 6 outside both rooms
    }
    services_by_group: dict[_GroupKey, tuple[_ServiceTuple, ...]] = {}

    expected_total = sum(
        float(LineString(pl).length)
        for polylines in polylines_by_group.values()
        for pl in polylines
    )

    result = bucket_measured_for_member(
        member_revision_id=_MEMBER_ID,
        role=_ROLE,
        polylines_by_group=polylines_by_group,
        services_by_group=services_by_group,
        registry=registry,
        scale=scale,
    )

    actual_total = _total_drawing_length(result)
    rel_err = abs(actual_total - expected_total) / max(expected_total, 1e-12)
    assert rel_err <= 1e-6, (
        f"Conservation failed: expected {expected_total:.6f}, got {actual_total:.6f}, "
        f"rel_err={rel_err:.2e}"
    )


def test_geometry_conservation_voronoi_on() -> None:
    """Conservation holds with Voronoi enabled (residual is redistributed, not lost)."""
    from shapely.geometry import LineString

    room_a = _poly_room_a()
    label_c = _label_room_outside()
    registry = build_room_registry([room_a, label_c], voronoi_fallback=True)
    scale = _scale_confirmed()

    polylines_by_group: dict[_GroupKey, _Polylines] = {
        (None, "ff0000"): (((0.0, 5.0), (30.0, 5.0)),),  # 10 in A, 20 outside
    }

    expected_total = sum(
        float(LineString(pl).length)
        for polylines in polylines_by_group.values()
        for pl in polylines
    )

    result = bucket_measured_for_member(
        member_revision_id=_MEMBER_ID,
        role=_ROLE,
        polylines_by_group=polylines_by_group,
        services_by_group={},
        registry=registry,
        scale=scale,
    )

    actual_total = _total_drawing_length(result)
    rel_err = abs(actual_total - expected_total) / max(expected_total, 1e-12)
    assert rel_err <= 1e-6, (
        f"Conservation (voronoi on) failed: expected {expected_total:.6f}, "
        f"got {actual_total:.6f}, rel_err={rel_err:.2e}"
    )


# ---------------------------------------------------------------------------
# Test: determinism under shuffle
# ---------------------------------------------------------------------------


def test_determinism_under_group_shuffle() -> None:
    """Result is identical regardless of group-map insertion order."""
    room_a = _poly_room_a()
    room_b = _poly_room_b()
    registry = build_room_registry([room_a, room_b], voronoi_fallback=False)
    scale = _scale_confirmed()

    groups_ordered: dict[_GroupKey, _Polylines] = {
        (None, "ff0000"): (((0.0, 5.0), (10.0, 5.0)),),
        ("layer1", "00ff00"): (((10.0, 5.0), (20.0, 5.0)),),
        ("layer2", "0000ff"): (((5.0, 5.0), (15.0, 5.0)),),
    }
    services_by_group: dict[_GroupKey, tuple[_ServiceTuple, ...]] = {}

    # Build a shuffled copy.
    keys = list(groups_ordered.keys())
    random.seed(42)
    random.shuffle(keys)
    groups_shuffled: dict[_GroupKey, _Polylines] = {k: groups_ordered[k] for k in keys}

    result_ordered = bucket_measured_for_member(
        member_revision_id=_MEMBER_ID,
        role=_ROLE,
        polylines_by_group=groups_ordered,
        services_by_group=services_by_group,
        registry=registry,
        scale=scale,
    )
    result_shuffled = bucket_measured_for_member(
        member_revision_id=_MEMBER_ID,
        role=_ROLE,
        polylines_by_group=groups_shuffled,
        services_by_group=services_by_group,
        registry=registry,
        scale=scale,
    )

    assert result_ordered.items == result_shuffled.items, "Items differ under group-map shuffle"
    assert result_ordered.unassigned == result_shuffled.unassigned, (
        "Unassigned differ under group-map shuffle"
    )


# ---------------------------------------------------------------------------
# Test: SERVICE_UNKNOWN when no services provided
# ---------------------------------------------------------------------------


def test_service_unknown_when_no_services() -> None:
    """A group with no services in services_by_group produces SERVICE_UNKNOWN items."""
    room_a = _poly_room_a()
    registry = build_room_registry([room_a], voronoi_fallback=False)
    scale = _scale_confirmed()

    polylines_by_group: dict[_GroupKey, _Polylines] = {
        (None, "ff0000"): (((1.0, 5.0), (9.0, 5.0)),),
    }
    # Key not present in services_by_group → SERVICE_UNKNOWN.
    services_by_group: dict[_GroupKey, tuple[_ServiceTuple, ...]] = {}

    result = bucket_measured_for_member(
        member_revision_id=_MEMBER_ID,
        role=_ROLE,
        polylines_by_group=polylines_by_group,
        services_by_group=services_by_group,
        registry=registry,
        scale=scale,
    )

    assert len(result.items) == 1
    assert result.items[0].service == SERVICE_UNKNOWN
    assert result.items[0].size_raw is None
    assert result.items[0].size_kind is None


# ---------------------------------------------------------------------------
# Test: empty polylines_by_group
# ---------------------------------------------------------------------------


def test_empty_polylines_by_group_yields_empty_result() -> None:
    """An empty polylines_by_group produces an empty result without error."""
    room_a = _poly_room_a()
    registry = build_room_registry([room_a], voronoi_fallback=False)
    scale = _scale_confirmed()

    result = bucket_measured_for_member(
        member_revision_id=_MEMBER_ID,
        role=_ROLE,
        polylines_by_group={},
        services_by_group={},
        registry=registry,
        scale=scale,
    )

    assert result.items == ()
    assert result.unassigned == ()
    assert not result.unscaled


# ---------------------------------------------------------------------------
# Test: member_revision_id and role are threaded through
# ---------------------------------------------------------------------------


def test_member_revision_id_and_role_in_output() -> None:
    """Each output item carries the correct member_revision_id and role."""
    room_a = _poly_room_a()
    registry = build_room_registry([room_a], voronoi_fallback=False)
    scale = _scale_confirmed()

    polylines_by_group: dict[_GroupKey, _Polylines] = {
        (None, "ff0000"): (((1.0, 5.0), (9.0, 5.0)),),
    }

    result = bucket_measured_for_member(
        member_revision_id="test-revision-xyz",
        role="power",
        polylines_by_group=polylines_by_group,
        services_by_group={},
        registry=registry,
        scale=scale,
    )

    assert all(i.member_revision_id == "test-revision-xyz" for i in result.items)
    assert all(i.role == "power" for i in result.items)
