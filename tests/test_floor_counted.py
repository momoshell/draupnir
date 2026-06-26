"""Pure-logic tests for the COUNTED (device) floor fusion (issue #719, Phase R-E).

All tests are non-DB and non-network.  Registry and TypedDevice objects are constructed
directly from public dataclasses.

Coverage targets:
- Per-(type, room) counts
- Architecture / annotation / legend_exemplar excluded → excluded_kinds, NOT counted
- RoomRegistry polygon path and Voronoi-rescue path
- Unassigned: registry returns room_number=None AND position=None → needs_review
- device→room map: one entry per counted device + by_device_id lookup
- Determinism under member-order shuffle
- Per-member COUNTED conservation: Σ(rooms + unassigned) == counted typed_devices for each type
"""

from __future__ import annotations

import random

from shapely.geometry import Polygon as ShapelyPolygon

from app.interpretation.device_identity import (
    KIND_ANNOTATION,
    KIND_ARCHITECTURE,
    KIND_DEVICE,
    KIND_LEGEND_EXEMPLAR,
)
from app.interpretation.devices import TypedDevice
from app.interpretation.floor_counted import bucket_counted_devices
from app.interpretation.grid_registration import GridTransform
from app.interpretation.room_fusion import RoomRegistry, build_room_registry
from app.interpretation.rooms import Room, room_from_label

# ---------------------------------------------------------------------------
# Helpers — build test objects
# ---------------------------------------------------------------------------

_IDENTITY_TRANSFORM = GridTransform(
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

_TRANSLATE_TRANSFORM = GridTransform(
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


def _typed(
    device_id: str,
    type_name: str,
    *,
    kind: str = KIND_DEVICE,
    x: float | None = 5.0,
    y: float | None = 5.0,
) -> TypedDevice:
    pos = {"x": x, "y": y} if x is not None and y is not None else None
    return TypedDevice(device_id=device_id, type_name=type_name, kind=kind, position=pos)


def _polygon_room(
    room_id: str,
    number: str,
    *,
    x0: float = 0.0,
    y0: float = 0.0,
    x1: float = 10.0,
    y1: float = 10.0,
) -> Room:
    poly = ShapelyPolygon([(x0, y0), (x1, y0), (x1, y1), (x0, y1)])
    return Room(
        id=room_id,
        name=None,
        source="test",
        polygon=poly,
        area=(x1 - x0) * (y1 - y0),
        bounds=None,
        number=number,
    )


def _label_room(room_id: str, number: str, *, x: float, y: float) -> Room:
    return room_from_label(room_id, source="test", location=(x, y), number=number)


def _registry(rooms: list[Room], *, voronoi_fallback: bool = True) -> RoomRegistry:
    return build_room_registry(rooms, voronoi_fallback=voronoi_fallback)


# ---------------------------------------------------------------------------
# Per-(type, room) counts
# ---------------------------------------------------------------------------


def test_counts_by_room_single_member_single_type() -> None:
    """Devices of the same type in the same room are aggregated."""
    reg = _registry([_polygon_room("r1", "1.01")])
    devices = [
        _typed("d1", "SD", x=5.0, y=5.0),
        _typed("d2", "SD", x=5.0, y=5.0),
        _typed("d3", "SD", x=5.0, y=5.0),
    ]
    result = bucket_counted_devices([("rev-A", _IDENTITY_TRANSFORM, devices)], reg)

    assert result.counts_by_room["1.01"]["SD"] == 3
    assert len(result.assignments) == 3


def test_counts_by_room_two_types() -> None:
    """Different type_names land in separate buckets under the same room."""
    reg = _registry([_polygon_room("r1", "1.01")])
    devices = [
        _typed("d1", "SD", x=5.0, y=5.0),
        _typed("d2", "FAP", x=5.0, y=5.0),
        _typed("d3", "SD", x=5.0, y=5.0),
    ]
    result = bucket_counted_devices([("rev-A", _IDENTITY_TRANSFORM, devices)], reg)

    assert result.counts_by_room["1.01"]["SD"] == 2
    assert result.counts_by_room["1.01"]["FAP"] == 1


def test_counts_by_room_two_rooms() -> None:
    """Devices in different polygon rooms are separated."""
    reg = _registry(
        [
            _polygon_room("r1", "1.01", x0=0.0, x1=10.0),
            _polygon_room("r2", "1.02", x0=20.0, x1=30.0, y0=0.0, y1=10.0),
        ]
    )
    devices = [
        _typed("d1", "SD", x=5.0, y=5.0),  # room 1.01
        _typed("d2", "SD", x=25.0, y=5.0),  # room 1.02
    ]
    result = bucket_counted_devices([("rev-A", _IDENTITY_TRANSFORM, devices)], reg)

    assert result.counts_by_room["1.01"]["SD"] == 1
    assert result.counts_by_room["1.02"]["SD"] == 1


# ---------------------------------------------------------------------------
# Exclusion of non-counting kinds
# ---------------------------------------------------------------------------


def test_architecture_excluded_from_counts_but_tallied() -> None:
    """KIND_ARCHITECTURE is excluded from assignments/counts and tallied in excluded_kinds."""
    reg = _registry([_polygon_room("r1", "1.01")])
    devices = [
        _typed("arch-1", "architecture", kind=KIND_ARCHITECTURE, x=5.0, y=5.0),
        _typed("arch-2", "architecture", kind=KIND_ARCHITECTURE, x=5.0, y=5.0),
        _typed("sd-1", "SD", x=5.0, y=5.0),
    ]
    result = bucket_counted_devices([("rev-A", _IDENTITY_TRANSFORM, devices)], reg)

    # Only sd-1 is counted.
    assert len(result.assignments) == 1
    assert result.assignments[0].device_id == "sd-1"
    # architecture appears in excluded_kinds.
    assert result.excluded_kinds.get("architecture", 0) == 2
    # Counts only contain SD.
    assert result.counts_by_room.get("1.01", {}).get("SD") == 1
    assert "architecture" not in result.counts_by_room.get("1.01", {})


def test_annotation_excluded() -> None:
    """KIND_ANNOTATION is excluded from assignments and tallied."""
    reg = _registry([_polygon_room("r1", "1.01")])
    devices = [
        _typed("ann-1", "unresolved", kind=KIND_ANNOTATION, x=5.0, y=5.0),
    ]
    result = bucket_counted_devices([("rev-A", _IDENTITY_TRANSFORM, devices)], reg)

    assert len(result.assignments) == 0
    assert result.excluded_kinds.get("annotation", 0) == 1


def test_legend_exemplar_excluded() -> None:
    """KIND_LEGEND_EXEMPLAR is excluded from assignments and tallied."""
    reg = _registry([_polygon_room("r1", "1.01")])
    devices = [
        _typed("ex-1", "SD", kind=KIND_LEGEND_EXEMPLAR, x=5.0, y=5.0),
    ]
    result = bucket_counted_devices([("rev-A", _IDENTITY_TRANSFORM, devices)], reg)

    assert len(result.assignments) == 0
    assert result.excluded_kinds.get("legend_exemplar", 0) == 1


def test_unresolved_device_is_counted_not_excluded() -> None:
    """KIND_DEVICE with type_name='unresolved' is still counted (not excluded)."""
    reg = _registry([_polygon_room("r1", "1.01")])
    devices = [_typed("u1", "unresolved", kind=KIND_DEVICE, x=5.0, y=5.0)]
    result = bucket_counted_devices([("rev-A", _IDENTITY_TRANSFORM, devices)], reg)

    assert len(result.assignments) == 1
    assert result.counts_by_room["1.01"]["unresolved"] == 1
    assert not result.excluded_kinds  # nothing excluded


# ---------------------------------------------------------------------------
# Registry paths: polygon + Voronoi rescue
# ---------------------------------------------------------------------------


def test_polygon_assignment() -> None:
    """Device inside a polygon room → boundary_basis='polygon'."""
    reg = _registry([_polygon_room("r1", "1.01")])
    result = bucket_counted_devices(
        [("rev-A", _IDENTITY_TRANSFORM, [_typed("d1", "SD", x=5.0, y=5.0)])],
        reg,
    )
    a = result.assignments[0]
    assert a.room_number == "1.01"
    assert a.boundary_basis == "polygon"
    assert a.room_id == "r1"


def test_voronoi_fallback_assignment() -> None:
    """Device outside polygons but within Voronoi envelope → boundary_basis='voronoi'."""
    # Only a label-only room — Voronoi is the only path.
    reg = _registry([_label_room("r2", "2.01", x=50.0, y=50.0)])
    result = bucket_counted_devices(
        [("rev-A", _IDENTITY_TRANSFORM, [_typed("d1", "SD", x=50.0, y=50.0)])],
        reg,
    )
    a = result.assignments[0]
    assert a.room_number == "2.01"
    assert a.boundary_basis == "voronoi"


def test_unassigned_device_no_room() -> None:
    """Device outside every room → room_number=None, boundary_basis=None, needs_review=False."""
    # Empty registry: no rooms at all.
    reg = _registry([])
    result = bucket_counted_devices(
        [("rev-A", _IDENTITY_TRANSFORM, [_typed("d1", "SD", x=999.0, y=999.0)])],
        reg,
    )
    a = result.assignments[0]
    assert a.room_number is None
    assert a.boundary_basis is None
    assert not a.needs_review
    # Unassigned devices land in counts_by_room[None].
    assert result.counts_by_room[None]["SD"] == 1


def test_no_position_needs_review() -> None:
    """Device with position=None → room_number=None, needs_review=True (honest, not dropped)."""
    reg = _registry([_polygon_room("r1", "1.01")])
    devices = [_typed("d-nopos", "SD", x=None, y=None)]  # position=None
    result = bucket_counted_devices([("rev-A", _IDENTITY_TRANSFORM, devices)], reg)

    assert len(result.assignments) == 1
    a = result.assignments[0]
    assert a.room_number is None
    assert a.needs_review is True
    assert result.counts_by_room[None]["SD"] == 1


# ---------------------------------------------------------------------------
# device→room map and by_device_id
# ---------------------------------------------------------------------------


def test_assignments_one_entry_per_counted_device() -> None:
    """assignments has exactly one entry per non-excluded device."""
    reg = _registry([_polygon_room("r1", "1.01")])
    devices = [
        _typed("d1", "SD"),
        _typed("d2", "FAP"),
        _typed("arch", "architecture", kind=KIND_ARCHITECTURE),  # excluded
    ]
    result = bucket_counted_devices([("rev-A", _IDENTITY_TRANSFORM, devices)], reg)

    assert len(result.assignments) == 2
    ids = {a.device_id for a in result.assignments}
    assert ids == {"d1", "d2"}


def test_by_device_id_lookup() -> None:
    """by_device_id returns a dict keyed by device_id."""
    reg = _registry([_polygon_room("r1", "1.01")])
    devices = [_typed("d1", "SD"), _typed("d2", "FAP")]
    result = bucket_counted_devices([("rev-A", _IDENTITY_TRANSFORM, devices)], reg)

    lookup = result.by_device_id()
    assert isinstance(lookup, dict)
    assert set(lookup.keys()) == {"d1", "d2"}
    assert lookup["d1"].type_name == "SD"
    assert lookup["d2"].type_name == "FAP"


# ---------------------------------------------------------------------------
# Determinism under member-order shuffle
# ---------------------------------------------------------------------------


def test_determinism_under_member_shuffle() -> None:
    """Shuffling the member list order produces byte-identical CountedFusionResult."""
    reg = _registry([_polygon_room("r1", "1.01")])
    members = [
        ("rev-Z", _IDENTITY_TRANSFORM, [_typed("dZ", "SD")]),
        ("rev-A", _IDENTITY_TRANSFORM, [_typed("dA", "FAP")]),
        ("rev-M", _IDENTITY_TRANSFORM, [_typed("dM", "SD")]),
    ]
    canonical = bucket_counted_devices(members, reg)

    for _ in range(5):
        shuffled = list(members)
        random.shuffle(shuffled)
        result = bucket_counted_devices(shuffled, reg)

        # assignments must be in the same order (sorted by revision_id then device order).
        assert [a.device_id for a in result.assignments] == [
            a.device_id for a in canonical.assignments
        ]
        assert result.counts_by_room == canonical.counts_by_room


# ---------------------------------------------------------------------------
# Per-member COUNTED conservation
# ---------------------------------------------------------------------------


def test_per_member_counted_conservation() -> None:
    """For each type, Σ counts across rooms (incl. None) equals the counted input total."""
    reg = _registry([_polygon_room("r1", "1.01"), _polygon_room("r2", "1.02", x0=20.0, x1=30.0)])
    devices_a = [
        _typed("a1", "SD", x=5.0, y=5.0),
        _typed("a2", "SD", x=25.0, y=5.0),
        _typed("a3", "FAP", x=5.0, y=5.0),
        _typed("a4", "FAP", x=None, y=None),  # no-position → None room
    ]
    result = bucket_counted_devices([("rev-A", _IDENTITY_TRANSFORM, devices_a)], reg)

    # SD: 2 devices (1 in 1.01, 1 in 1.02).
    sd_total = sum(counts.get("SD", 0) for counts in result.counts_by_room.values())
    assert sd_total == 2

    # FAP: 2 devices (1 in 1.01, 1 with no position → None).
    fap_total = sum(counts.get("FAP", 0) for counts in result.counts_by_room.values())
    assert fap_total == 2


# ---------------------------------------------------------------------------
# Multi-member fusion
# ---------------------------------------------------------------------------


def test_multi_member_devices_fused() -> None:
    """Devices from two members are fused into a single result."""
    reg = _registry([_polygon_room("r1", "1.01")])
    members = [
        ("rev-A", _IDENTITY_TRANSFORM, [_typed("a1", "SD"), _typed("a2", "SD")]),
        ("rev-B", _IDENTITY_TRANSFORM, [_typed("b1", "FAP")]),
    ]
    result = bucket_counted_devices(members, reg)

    assert len(result.assignments) == 3
    assert result.counts_by_room["1.01"]["SD"] == 2
    assert result.counts_by_room["1.01"]["FAP"] == 1


def test_transform_applied_to_device_position() -> None:
    """Transform.apply is called: device at x=5 with dx=100 lands at x=105."""
    # Room covers x=100..120, y=0..10 — only reachable after the translate.
    reg = _registry([_polygon_room("r1", "1.01", x0=100.0, x1=120.0)])
    devices = [_typed("d1", "SD", x=5.0, y=5.0)]

    result = bucket_counted_devices([("rev-A", _TRANSLATE_TRANSFORM, devices)], reg)

    assert result.assignments[0].room_number == "1.01"
    assert result.counts_by_room["1.01"]["SD"] == 1


def test_source_revision_id_in_assignment() -> None:
    """Each assignment carries the correct source_revision_id."""
    reg = _registry([_polygon_room("r1", "1.01")])
    members = [
        ("rev-A", _IDENTITY_TRANSFORM, [_typed("a1", "SD")]),
        ("rev-B", _IDENTITY_TRANSFORM, [_typed("b1", "SD")]),
    ]
    result = bucket_counted_devices(members, reg)

    by_id = result.by_device_id()
    assert by_id["a1"].source_revision_id == "rev-A"
    assert by_id["b1"].source_revision_id == "rev-B"


def test_empty_members_returns_empty_result() -> None:
    """No members → empty CountedFusionResult without raising."""
    reg = _registry([])
    result = bucket_counted_devices([], reg)

    assert result.assignments == ()
    assert result.counts_by_room == {}
    assert result.excluded_kinds == {}
