"""Tests for app/interpretation/service_takeoff.py (pure, fakes, no DB).

M-540003-like fixture: HYDRAULIC bundle of LINE segments + a RunServiceIdentity with
services {VAC@54, AGSS@42} + confirmed-mm ScaleContext + one containing room.
"""

from __future__ import annotations

import random
from typing import Any

import pytest
from shapely.geometry import Polygon

from app.interpretation.measurement import ScaleContext
from app.interpretation.rise_drop import (
    KIND_DROP,
    KIND_RISE,
    STATUS_RESOLVED,
    RiseDropSymbol,
)
from app.interpretation.rooms import Room, room_from_label
from app.interpretation.routed_runs import RunGroup
from app.interpretation.run_service_identity import (
    IDENTITY_PARTIAL,
    IDENTITY_RESOLVED,
    IDENTITY_UNKNOWN,
    RunServiceIdentity,
    ServiceSize,
)
from app.interpretation.run_tags import BASIS_TAG_TEXT, PipeSize
from app.interpretation.segment_label_takeoff import SegmentLabel
from app.interpretation.service_takeoff import (
    ROOM_UNASSIGNED_ID,
    SERVICE_UNKNOWN,
    ServiceTakeoffLine,
    ServiceTakeoffResult,
    _collapse_services_per_service,
    _dominant_service,
    _is_achromatic_colour_key,
    _nearest_label_room,
    compute_service_takeoff,
)

# ---------------------------------------------------------------------------
# Helpers / factories
# ---------------------------------------------------------------------------

_BASIS_LEGEND = "legend_colour"
_BASIS_LEGEND_TAG = "legend_colour+tag_text"


def _pipe_size(service: str, diameter: int) -> ServiceSize:
    return ServiceSize(
        service=service,
        size=PipeSize(kind="round", diameter=diameter, width=None, height=None, raw=str(diameter)),
        source_tag_text=f"{diameter} mm {service}",
        basis=BASIS_TAG_TEXT,
    )


def _make_run(
    *,
    layer_ref: str | None = "Pipes",
    colour_key: str | None = "idx150",
    discipline: str | None = "HYDRAULIC EQUIPMENT",
    entity_ids: tuple[str, ...] = ("e1",),
    competing_disciplines: tuple[str, ...] = (),
    confidence: float | None = None,
) -> RunGroup:
    return RunGroup(
        layer_ref=layer_ref,
        colour_key=colour_key,
        colour_index=150,
        colour_rgb=None,
        status="resolved",
        discipline=discipline,
        basis=_BASIS_LEGEND,
        source_layers=(layer_ref,) if layer_ref else (),
        confidence=confidence,
        competing_disciplines=competing_disciplines,
        entity_ids=entity_ids,
    )


def _make_identity(
    *,
    layer_ref: str | None = "Pipes",
    colour_key: str | None = "idx150",
    discipline: str | None = "HYDRAULIC EQUIPMENT",
    services: tuple[ServiceSize, ...] = (),
    status: str = IDENTITY_RESOLVED,
    entity_ids: tuple[str, ...] = ("e1",),
    competing_disciplines: tuple[str, ...] = (),
    confidence: float | None = None,
) -> RunServiceIdentity:
    return RunServiceIdentity(
        layer_ref=layer_ref,
        colour_key=colour_key,
        discipline=discipline,
        services=services,
        status=status,
        basis=_BASIS_LEGEND_TAG if services else _BASIS_LEGEND,
        source_layers=(layer_ref,) if layer_ref else (),
        confidence=confidence,
        competing_disciplines=competing_disciplines,
        entity_ids=entity_ids,
    )


def _line_geom(x1: float, y1: float, x2: float, y2: float) -> dict[str, list[float]]:
    return {"start": [x1, y1, 0.0], "end": [x2, y2, 0.0]}


def _arc_geom() -> dict[str, Any]:
    """Geometry dict that looks like an arc (no start/end, no vertices)."""
    return {"center": [0.0, 0.0], "radius": 100.0, "start_angle": 0.0, "end_angle": 90.0}


def _square_room(
    x: float = 0.0, y: float = 0.0, size: float = 10000.0, room_id: str = "room-1"
) -> Room:
    """Axis-aligned square polygon room."""
    poly = Polygon([(x, y), (x + size, y), (x + size, y + size), (x, y + size)])
    return Room(
        id=room_id,
        name="Test Room",
        source="test",
        polygon=poly,
        area=size * size,
        bounds=(x, y, x + size, y + size),
        number="1.01",
    )


def _confirmed_mm_scale() -> ScaleContext:
    """Confirmed mm-unit scale: conversion_factor=0.001, real_world_available=True."""
    return ScaleContext(
        conversion_factor=0.001,
        real_world_available=True,
        contradicted=False,
        units_confidence="confirmed",
    )


def _unknown_scale() -> ScaleContext:
    """Unknown scale: no conversion possible."""
    return ScaleContext(
        conversion_factor=None,
        real_world_available=False,
        contradicted=False,
        units_confidence="unknown",
    )


# ---------------------------------------------------------------------------
# M-540003-like: dual-service HYDRAULIC bundle, confirmed scale, one room
# ---------------------------------------------------------------------------


def test_m540003_dual_service_bundle_real_world() -> None:
    """VAC@54 + AGSS@42 in one HYDRAULIC run, confirmed mm scale, inside a room.

    Verifies:
    - One ServiceTakeoffLine per service (VAC and AGSS).
    - Each line's real_length_m == full summed drawn length * 0.001 (parallel, not halved).
    - basis == "real_world", unscaled False.
    - room_id, room_name, room_number populated from the containing room.
    """
    # Two LINE entities end-to-end: e1=(0,0)->(3000,0), e2=(3000,0)->(7000,0).
    # Total drawn length = 3000 + 4000 = 7000 drawing units.
    entity_ids = ("e1", "e2")
    geometry = {
        "e1": _line_geom(0.0, 0.0, 3000.0, 0.0),
        "e2": _line_geom(3000.0, 0.0, 7000.0, 0.0),
    }
    expected_drawn = 7000.0
    expected_real_m = 7000.0 * 0.001  # = 7.0

    services = (
        _pipe_size("VAC", 54),
        _pipe_size("AGSS", 42),
    )
    run = _make_run(entity_ids=entity_ids)
    identity = _make_identity(entity_ids=entity_ids, services=services, status=IDENTITY_RESOLVED)

    # Room covers (0,0) to (10000,10000); anchor of run is midpoint avg = (3500, 0) -- inside.
    room = _square_room()

    scale = _confirmed_mm_scale()
    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[room],
        scale=scale,
    )

    assert isinstance(result, ServiceTakeoffResult)
    assert len(result.lines) == 2, f"Expected 2 lines, got {len(result.lines)}"
    assert not result.unscaled
    assert result.unassigned_run_count == 0
    assert result.unknown_service_run_count == 0

    by_service = {ln.service: ln for ln in result.lines}
    assert set(by_service.keys()) == {"VAC", "AGSS"}

    for svc in ("VAC", "AGSS"):
        ln = by_service[svc]
        assert ln.drawing_length == expected_drawn, (
            f"{svc}: drawing_length {ln.drawing_length} != {expected_drawn}"
        )
        assert ln.real_length_m is not None
        assert abs(ln.real_length_m - expected_real_m) < 1e-9, (
            f"{svc}: real_length_m {ln.real_length_m} != {expected_real_m}"
        )
        assert ln.basis == "real_world"
        assert ln.units_confidence == "confirmed"
        assert ln.room_id == room.id
        assert ln.room_name == room.name
        assert ln.room_number == room.number
        assert ln.identity_status == IDENTITY_RESOLVED


# ---------------------------------------------------------------------------
# Explicit test: L not L/2 per service in a 2-service bundle
# ---------------------------------------------------------------------------


def test_bundle_length_not_halved_for_two_services() -> None:
    """Explicit assertion: 2-service bundle of length L contributes L (not L/2) to each.

    A 1000-unit segment with services [SVC_A, SVC_B] must produce
    SVC_A.drawing_length == 1000 AND SVC_B.drawing_length == 1000 (parallel runs).
    """
    entity_ids = ("seg1",)
    geometry = {"seg1": _line_geom(0.0, 0.0, 1000.0, 0.0)}
    segment_length = 1000.0

    services = (_pipe_size("SVC_A", 50), _pipe_size("SVC_B", 32))
    identity = _make_identity(entity_ids=entity_ids, services=services)
    run = _make_run(entity_ids=entity_ids)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_unknown_scale(),
    )

    assert len(result.lines) == 2
    by_service = {ln.service: ln for ln in result.lines}
    assert by_service["SVC_A"].drawing_length == segment_length
    assert by_service["SVC_B"].drawing_length == segment_length
    # Explicitly not halved:
    assert by_service["SVC_A"].drawing_length != segment_length / 2
    assert by_service["SVC_B"].drawing_length != segment_length / 2
    # #655: a multi-service run is flagged as a bundle (full length to each, not a split).
    assert by_service["SVC_A"].bundle is True
    assert by_service["SVC_B"].bundle is True


def test_single_service_run_is_not_flagged_bundle() -> None:
    """A single-service run is not a bundle (its length is individually resolved)."""
    identity = _make_identity(services=(_pipe_size("VAC", 54),), entity_ids=("e1",))
    result = compute_service_takeoff(
        runs=[_make_run()],
        identities=[identity],
        geometry_by_entity_id={"e1": _line_geom(0.0, 0.0, 1000.0, 0.0)},
        rooms=[],
        scale=_unknown_scale(),
    )
    assert len(result.lines) == 1
    assert result.lines[0].bundle is False


# ---------------------------------------------------------------------------
# Unknown scale -> drawing_units_only
# ---------------------------------------------------------------------------


def test_unknown_scale_yields_drawing_units_only() -> None:
    """Unknown ScaleContext -> real_length_m None, basis drawing_units_only, unscaled True."""
    entity_ids = ("e1",)
    geometry = {"e1": _line_geom(0.0, 0.0, 500.0, 0.0)}
    services = (_pipe_size("VAC", 54),)
    identity = _make_identity(entity_ids=entity_ids, services=services)
    run = _make_run(entity_ids=entity_ids)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_unknown_scale(),
    )

    assert len(result.lines) == 1
    ln = result.lines[0]
    assert ln.real_length_m is None
    assert ln.basis == "drawing_units_only"
    assert result.unscaled is True


# ---------------------------------------------------------------------------
# Anchor outside all rooms -> ROOM_UNASSIGNED_ID
# ---------------------------------------------------------------------------


def test_anchor_outside_rooms_yields_unassigned() -> None:
    """Run anchor outside all rooms -> ROOM_UNASSIGNED_ID, unassigned_run_count 1."""
    # Room covers (0,0)..(1000,1000); entity lives at (5000,5000)..(6000,5000) -- outside.
    entity_ids = ("e1",)
    geometry = {"e1": _line_geom(5000.0, 5000.0, 6000.0, 5000.0)}
    services = (_pipe_size("VAC", 54),)
    identity = _make_identity(entity_ids=entity_ids, services=services)
    run = _make_run(entity_ids=entity_ids)
    room = _square_room(x=0.0, y=0.0, size=1000.0)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[room],
        scale=_confirmed_mm_scale(),
    )

    assert len(result.lines) == 1
    ln = result.lines[0]
    assert ln.room_id == ROOM_UNASSIGNED_ID
    assert ln.room_name is None
    assert ln.room_number is None
    assert result.unassigned_run_count == 1


# ---------------------------------------------------------------------------
# Empty services -> SERVICE_UNKNOWN bucket
# ---------------------------------------------------------------------------


def test_empty_services_yields_service_unknown() -> None:
    """Run with no services -> SERVICE_UNKNOWN line, unknown_service_run_count 1."""
    entity_ids = ("e1",)
    geometry = {"e1": _line_geom(0.0, 0.0, 200.0, 0.0)}
    identity = _make_identity(
        entity_ids=entity_ids,
        services=(),
        status=IDENTITY_PARTIAL,
    )
    run = _make_run(entity_ids=entity_ids)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_unknown_scale(),
    )

    assert result.unknown_service_run_count == 1
    assert len(result.lines) == 1
    ln = result.lines[0]
    assert ln.service == SERVICE_UNKNOWN
    assert ln.size_raw is None
    assert ln.drawing_length == 200.0


# ---------------------------------------------------------------------------
# Arc-only run -> 0 drawn length, no crash
# ---------------------------------------------------------------------------


def test_arc_only_run_zero_length_no_crash() -> None:
    """Arc geometry contributes 0 drawn length (P4 gap); run is still accounted for."""
    entity_ids = ("arc1",)
    geometry = {"arc1": _arc_geom()}
    services = (_pipe_size("VAC", 54),)
    identity = _make_identity(entity_ids=entity_ids, services=services)
    run = _make_run(entity_ids=entity_ids)

    # Should not raise.
    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_confirmed_mm_scale(),
    )

    assert len(result.lines) == 1
    ln = result.lines[0]
    assert ln.drawing_length == 0.0
    # 0 * factor = 0 metres; basis is real_world (scale valid, length just happens to be 0).
    assert ln.real_length_m == 0.0
    assert ln.basis == "real_world"


# ---------------------------------------------------------------------------
# Permutation-invariance
# ---------------------------------------------------------------------------


def test_permutation_invariance() -> None:
    """Shuffling runs/identities/rooms does not change the result."""
    # Three runs, two rooms.
    e_ids_1 = ("e1", "e2")
    e_ids_2 = ("e3",)
    e_ids_3 = ("e4",)
    geometry: dict[str, Any] = {
        "e1": _line_geom(0.0, 0.0, 1000.0, 0.0),
        "e2": _line_geom(1000.0, 0.0, 2000.0, 0.0),
        "e3": _line_geom(500.0, 500.0, 800.0, 500.0),
        "e4": _line_geom(5500.0, 5500.0, 6000.0, 5500.0),
    }

    svc_vac = (_pipe_size("VAC", 54), _pipe_size("AGSS", 42))
    svc_ma = (_pipe_size("MA", 25),)

    run1 = _make_run(layer_ref="P1", colour_key="ck1", entity_ids=e_ids_1)
    run2 = _make_run(layer_ref="P2", colour_key="ck2", entity_ids=e_ids_2)
    run3 = _make_run(layer_ref="P3", colour_key="ck3", entity_ids=e_ids_3)

    id1 = _make_identity(layer_ref="P1", colour_key="ck1", entity_ids=e_ids_1, services=svc_vac)
    id2 = _make_identity(layer_ref="P2", colour_key="ck2", entity_ids=e_ids_2, services=svc_ma)
    id3 = _make_identity(
        layer_ref="P3",
        colour_key="ck3",
        entity_ids=e_ids_3,
        services=(_pipe_size("EA", 100),),
    )

    room_a = _square_room(x=0.0, y=0.0, size=3000.0, room_id="room-a")
    room_b = _square_room(x=4000.0, y=4000.0, size=3000.0, room_id="room-b")

    scale = _confirmed_mm_scale()

    canonical = compute_service_takeoff(
        runs=[run1, run2, run3],
        identities=[id1, id2, id3],
        geometry_by_entity_id=geometry,
        rooms=[room_a, room_b],
        scale=scale,
    )

    rng = random.Random(42)
    for _ in range(8):
        runs_shuffled = [run1, run2, run3]
        ids_shuffled = [id1, id2, id3]
        rooms_shuffled = [room_a, room_b]
        rng.shuffle(runs_shuffled)
        rng.shuffle(ids_shuffled)
        rng.shuffle(rooms_shuffled)

        result = compute_service_takeoff(
            runs=runs_shuffled,
            identities=ids_shuffled,
            geometry_by_entity_id=geometry,
            rooms=rooms_shuffled,
            scale=scale,
        )

        assert result.lines == canonical.lines, (
            f"Permutation produced different lines:\n{result.lines}\nvs\n{canonical.lines}"
        )
        assert result.unscaled == canonical.unscaled
        assert result.unassigned_run_count == canonical.unassigned_run_count
        assert result.unknown_service_run_count == canonical.unknown_service_run_count


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_inputs_returns_empty_result() -> None:
    """No identities -> empty result, no crash."""
    result = compute_service_takeoff(
        runs=[],
        identities=[],
        geometry_by_entity_id={},
        rooms=[],
        scale=_confirmed_mm_scale(),
    )
    assert result.lines == ()
    assert not result.unscaled
    assert result.unassigned_run_count == 0
    assert result.unknown_service_run_count == 0


def test_identity_status_worst_across_runs() -> None:
    """Two runs rolled up to the same bucket: worst status (UNKNOWN < PARTIAL < RESOLVED) wins."""
    # Both runs: same service/size/room (no rooms -> ROOM_UNASSIGNED_ID).
    e_ids_1 = ("e1",)
    e_ids_2 = ("e2",)
    geometry = {
        "e1": _line_geom(0.0, 0.0, 100.0, 0.0),
        "e2": _line_geom(0.0, 0.0, 200.0, 0.0),
    }
    svc = (_pipe_size("VAC", 54),)

    # Use same layer_ref/colour_key so they map to the same bucket.
    id1 = _make_identity(
        layer_ref="P", colour_key="ck", entity_ids=e_ids_1, services=svc, status=IDENTITY_RESOLVED
    )
    id2 = _make_identity(
        layer_ref="P", colour_key="ck2", entity_ids=e_ids_2, services=svc, status=IDENTITY_UNKNOWN
    )
    run1 = _make_run(layer_ref="P", colour_key="ck", entity_ids=e_ids_1)
    run2 = _make_run(layer_ref="P", colour_key="ck2", entity_ids=e_ids_2)

    result = compute_service_takeoff(
        runs=[run1, run2],
        identities=[id1, id2],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_unknown_scale(),
    )

    # Both map to VAC / "54" / ROOM_UNASSIGNED_ID -> one line.
    assert len(result.lines) == 1
    ln = result.lines[0]
    assert ln.identity_status == IDENTITY_UNKNOWN  # worst wins
    assert ln.run_count == 2
    assert ln.drawing_length == 300.0


def test_entity_ids_are_sorted_union() -> None:
    """entity_ids on the output line is the sorted union across contributing runs."""
    e_ids_1 = ("e3", "e1")
    e_ids_2 = ("e2",)
    geometry = {
        "e1": _line_geom(0.0, 0.0, 50.0, 0.0),
        "e2": _line_geom(0.0, 0.0, 50.0, 0.0),
        "e3": _line_geom(0.0, 0.0, 50.0, 0.0),
    }
    svc = (_pipe_size("VAC", 54),)
    id1 = _make_identity(layer_ref="P", colour_key="ck", entity_ids=e_ids_1, services=svc)
    id2 = _make_identity(layer_ref="P", colour_key="ck2", entity_ids=e_ids_2, services=svc)
    run1 = _make_run(layer_ref="P", colour_key="ck", entity_ids=e_ids_1)
    run2 = _make_run(layer_ref="P", colour_key="ck2", entity_ids=e_ids_2)

    result = compute_service_takeoff(
        runs=[run1, run2],
        identities=[id1, id2],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_unknown_scale(),
    )

    assert len(result.lines) == 1
    assert result.lines[0].entity_ids == ("e1", "e2", "e3")


# ---------------------------------------------------------------------------
# P0 coverage -- boundary, nesting, multi-room split, status rollup,
# discipline determinism, unassigned_run_count per RUN
# ---------------------------------------------------------------------------


def test_anchor_on_room_boundary_is_inside() -> None:
    """An anchor exactly ON a room polygon boundary is classified inside that room.

    point_in_polygon is boundary-inclusive (Shapely contains_properly is NOT used;
    the geometry helper uses the standard ``polygon.contains`` which is True on boundary).
    """
    # Room: (0,0) to (1000,1000). Entity: a 100-unit line whose midpoint lands exactly at
    # x=500, y=1000 -- the top edge of the room.
    entity_ids = ("e1",)
    geometry = {"e1": _line_geom(450.0, 1000.0, 550.0, 1000.0)}
    services = (_pipe_size("VAC", 54),)
    identity = _make_identity(entity_ids=entity_ids, services=services)
    run = _make_run(entity_ids=entity_ids)
    room = _square_room(x=0.0, y=0.0, size=1000.0)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[room],
        scale=_unknown_scale(),
    )

    assert len(result.lines) == 1
    ln = result.lines[0]
    assert ln.room_id == room.id, (
        f"Boundary anchor should be inside room; got room_id={ln.room_id!r}"
    )
    assert result.unassigned_run_count == 0


def test_nested_rooms_smallest_area_wins() -> None:
    """A run anchor inside both an outer and an inner room -> inner (smaller) room wins."""
    # Outer room: (0,0) to (5000,5000) area=25_000_000.
    # Inner room: (1000,1000) to (3000,3000) area=4_000_000. Both contain (2000, 2000).
    entity_ids = ("e1",)
    # Anchor = midpoint of line = (2000, 2000) -- inside both rooms.
    geometry = {"e1": _line_geom(1900.0, 2000.0, 2100.0, 2000.0)}
    services = (_pipe_size("MA", 25),)
    identity = _make_identity(entity_ids=entity_ids, services=services)
    run = _make_run(entity_ids=entity_ids)

    outer = _square_room(x=0.0, y=0.0, size=5000.0, room_id="outer")
    inner = _square_room(x=1000.0, y=1000.0, size=2000.0, room_id="inner")

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[outer, inner],
        scale=_unknown_scale(),
    )

    assert len(result.lines) == 1
    ln = result.lines[0]
    assert ln.room_id == "inner", f"Expected inner room; got {ln.room_id!r}"


def test_same_service_size_different_rooms_yields_two_lines() -> None:
    """Two runs with the same (service, size) but different room_ids -> two separate lines.

    The bucket key includes room_id, so they must NOT be merged.
    """
    # Room A: (0,0)-(1000,1000). Room B: (5000,0)-(6000,1000).
    # Run 1 anchor at (500, 500) -> Room A.  Run 2 anchor at (5500, 500) -> Room B.
    geometry = {
        "e1": _line_geom(450.0, 500.0, 550.0, 500.0),
        "e2": _line_geom(5450.0, 500.0, 5550.0, 500.0),
    }
    svc = (_pipe_size("HWS", 50),)
    id1 = _make_identity(layer_ref="P1", colour_key="ck1", entity_ids=("e1",), services=svc)
    id2 = _make_identity(layer_ref="P2", colour_key="ck2", entity_ids=("e2",), services=svc)
    run1 = _make_run(layer_ref="P1", colour_key="ck1", entity_ids=("e1",))
    run2 = _make_run(layer_ref="P2", colour_key="ck2", entity_ids=("e2",))

    room_a = _square_room(x=0.0, y=0.0, size=1000.0, room_id="room-a")
    room_b = _square_room(x=5000.0, y=0.0, size=1000.0, room_id="room-b")

    result = compute_service_takeoff(
        runs=[run1, run2],
        identities=[id1, id2],
        geometry_by_entity_id=geometry,
        rooms=[room_a, room_b],
        scale=_unknown_scale(),
    )

    assert len(result.lines) == 2, (
        f"Same service in two rooms should yield 2 lines; got {len(result.lines)}"
    )
    room_ids = {ln.room_id for ln in result.lines}
    assert room_ids == {"room-a", "room-b"}


def test_worst_status_partial_beats_resolved() -> None:
    """PARTIAL + RESOLVED in one bucket -> bucket status is PARTIAL (worse)."""
    geometry = {
        "e1": _line_geom(0.0, 0.0, 100.0, 0.0),
        "e2": _line_geom(0.0, 0.0, 200.0, 0.0),
    }
    svc = (_pipe_size("VAC", 54),)
    id1 = _make_identity(
        layer_ref="P",
        colour_key="ck1",
        entity_ids=("e1",),
        services=svc,
        status=IDENTITY_PARTIAL,
    )
    id2 = _make_identity(
        layer_ref="P",
        colour_key="ck2",
        entity_ids=("e2",),
        services=svc,
        status=IDENTITY_RESOLVED,
    )
    run1 = _make_run(layer_ref="P", colour_key="ck1", entity_ids=("e1",))
    run2 = _make_run(layer_ref="P", colour_key="ck2", entity_ids=("e2",))

    result = compute_service_takeoff(
        runs=[run1, run2],
        identities=[id1, id2],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_unknown_scale(),
    )

    assert len(result.lines) == 1
    assert result.lines[0].identity_status == IDENTITY_PARTIAL


def test_discipline_determinism_under_shuffle() -> None:
    """SERVICE_UNKNOWN bucket with two runs carrying DIFFERENT disciplines is deterministic.

    The surviving discipline must be the alphabetical min() across contributors,
    independent of input permutation order.
    """
    geometry = {
        "e1": _line_geom(0.0, 0.0, 100.0, 0.0),
        "e2": _line_geom(0.0, 0.0, 200.0, 0.0),
    }
    # Both have empty services -> SERVICE_UNKNOWN bucket. Different disciplines.
    id1 = _make_identity(
        layer_ref="P1",
        colour_key="ck1",
        entity_ids=("e1",),
        services=(),
        discipline="HYDRAULIC EQUIPMENT",
        status=IDENTITY_PARTIAL,
    )
    id2 = _make_identity(
        layer_ref="P2",
        colour_key="ck2",
        entity_ids=("e2",),
        services=(),
        discipline="MECHANICAL",
        status=IDENTITY_PARTIAL,
    )
    run1 = _make_run(layer_ref="P1", colour_key="ck1", entity_ids=("e1",))
    run2 = _make_run(layer_ref="P2", colour_key="ck2", entity_ids=("e2",))

    expected_discipline = min("HYDRAULIC EQUIPMENT", "MECHANICAL")  # alphabetical

    rng = random.Random(7)
    for _ in range(10):
        ids_shuffled = [id1, id2]
        runs_shuffled = [run1, run2]
        rng.shuffle(ids_shuffled)
        rng.shuffle(runs_shuffled)

        result = compute_service_takeoff(
            runs=runs_shuffled,
            identities=ids_shuffled,
            geometry_by_entity_id=geometry,
            rooms=[],
            scale=_unknown_scale(),
        )

        assert len(result.lines) == 1
        assert result.lines[0].discipline == expected_discipline, (
            f"Expected {expected_discipline!r}, got {result.lines[0].discipline!r}"
        )


def test_unassigned_run_count_per_run_not_per_service() -> None:
    """A dual-service bundle outside all rooms increments unassigned_run_count by 1, not 2.

    unassigned_run_count tracks RUNS (identities), not individual services within a run.
    """
    # Single entity outside an empty rooms list; two services in the bundle.
    entity_ids = ("e1",)
    geometry = {"e1": _line_geom(0.0, 0.0, 500.0, 0.0)}
    services = (_pipe_size("VAC", 54), _pipe_size("AGSS", 42))
    identity = _make_identity(entity_ids=entity_ids, services=services)
    run = _make_run(entity_ids=entity_ids)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_unknown_scale(),
    )

    # Two service lines (bundle), but only ONE run was unassigned.
    assert len(result.lines) == 2
    assert result.unassigned_run_count == 1, (
        f"Dual-service bundle outside rooms: expected unassigned_run_count=1, "
        f"got {result.unassigned_run_count}"
    )


# ---------------------------------------------------------------------------
# Rise/drop symbol count tests (spec tests 3-6)
# ---------------------------------------------------------------------------


def _make_symbol(
    *,
    kind: str = KIND_RISE,
    anchor: tuple[float, float] = (500.0, 500.0),
    discipline: str | None = "medical-gas",
    entity_ids: tuple[str, ...] = ("arc-1",),
    status: str = STATUS_RESOLVED,
) -> RiseDropSymbol:
    return RiseDropSymbol(
        kind=kind,
        anchor_point=anchor,
        discipline=discipline,
        colour_key=None,
        entity_ids=entity_ids,
        status=status,
    )


def test_rise_symbol_inside_room_counted_on_service_unknown_line() -> None:
    """Spec test 3a: symbol anchor inside room R -> riser_count on SERVICE_UNKNOWN line for R."""
    room = _square_room()  # (0,0) to (10000,10000)
    sym = _make_symbol(anchor=(500.0, 500.0))  # inside

    result = compute_service_takeoff(
        runs=[],
        identities=[],
        geometry_by_entity_id={},
        rooms=[room],
        scale=_unknown_scale(),
        rise_symbols=[sym],
    )

    assert len(result.lines) == 1
    ln = result.lines[0]
    assert ln.service == SERVICE_UNKNOWN
    assert ln.room_id == room.id
    assert ln.riser_count == 1
    assert ln.drop_count == 0
    assert result.total_risers == 1
    assert result.total_drops == 0


def test_drop_symbol_outside_rooms_counted_on_unassigned_line() -> None:
    """Spec test 3b: symbol anchor outside all rooms -> ROOM_UNASSIGNED_ID bucket."""
    room = _square_room(x=0.0, y=0.0, size=100.0)  # small room
    sym = _make_symbol(kind=KIND_DROP, anchor=(5000.0, 5000.0))  # far outside

    result = compute_service_takeoff(
        runs=[],
        identities=[],
        geometry_by_entity_id={},
        rooms=[room],
        scale=_unknown_scale(),
        drop_symbols=[sym],
    )

    assert len(result.lines) == 1
    ln = result.lines[0]
    assert ln.service == SERVICE_UNKNOWN
    assert ln.room_id == ROOM_UNASSIGNED_ID
    assert ln.drop_count == 1
    assert ln.riser_count == 0
    assert result.total_drops == 1


def test_riser_counted_once_across_multiple_service_lines() -> None:
    """Spec test 4: one riser in a discipline+room counted ONCE even with multiple resolved
    service lines for that discipline+room.  The count lands on the SERVICE_UNKNOWN bucket,
    NOT spread across VAC/AGSS lines.
    """
    room = _square_room()  # (0,0) to (10000,10000)

    # Two service lines for VAC and AGSS in the same room.
    identity = _make_identity(
        entity_ids=("e1",),
        services=(
            _pipe_size("VAC", 54),
            _pipe_size("AGSS", 42),
        ),
        status=IDENTITY_RESOLVED,
    )
    run = _make_run(entity_ids=("e1",))
    geometry = {"e1": _line_geom(500.0, 500.0, 1000.0, 500.0)}

    sym = _make_symbol(anchor=(500.0, 500.0))  # inside the room

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[room],
        scale=_unknown_scale(),
        rise_symbols=[sym],
    )

    # There should be 3 lines: VAC, AGSS (from the run), + SERVICE_UNKNOWN (from the riser).
    services = {ln.service for ln in result.lines}
    assert SERVICE_UNKNOWN in services
    assert "VAC" in services
    assert "AGSS" in services

    # The riser appears ONLY in the SERVICE_UNKNOWN line for room-1.
    unknown_lines = [ln for ln in result.lines if ln.service == SERVICE_UNKNOWN]
    assert len(unknown_lines) == 1
    assert unknown_lines[0].riser_count == 1

    # VAC and AGSS lines must have riser_count == 0.
    for ln in result.lines:
        if ln.service != SERVICE_UNKNOWN:
            assert ln.riser_count == 0, (
                f"Non-unknown service {ln.service} should have riser_count=0"
            )

    # Total is the distinct symbol count, not the sum of per-line counts.
    assert result.total_risers == 1


def test_total_counts_equal_distinct_symbol_counts() -> None:
    """Spec test 5: total_risers/total_drops == len of input symbol sequences."""
    rise_syms = [
        _make_symbol(anchor=(100.0, 100.0), entity_ids=("a1",)),
        _make_symbol(anchor=(200.0, 200.0), entity_ids=("a2",)),
        _make_symbol(anchor=(300.0, 300.0), entity_ids=("a3",)),
    ]
    drop_syms = [
        _make_symbol(kind=KIND_DROP, anchor=(400.0, 400.0), entity_ids=("b1",)),
        _make_symbol(kind=KIND_DROP, anchor=(500.0, 500.0), entity_ids=("b2",)),
    ]

    result = compute_service_takeoff(
        runs=[],
        identities=[],
        geometry_by_entity_id={},
        rooms=[],
        scale=_unknown_scale(),
        rise_symbols=rise_syms,
        drop_symbols=drop_syms,
    )

    assert result.total_risers == 3
    assert result.total_drops == 2
    # Sum of per-line counts also equals the totals (no duplication when all go to
    # ROOM_UNASSIGNED_ID since there are no rooms).
    total_line_risers = sum(ln.riser_count for ln in result.lines)
    total_line_drops = sum(ln.drop_count for ln in result.lines)
    assert total_line_risers == 3
    assert total_line_drops == 2


def test_no_rise_drop_args_regression() -> None:
    """Spec test 6: compute_service_takeoff with no rise/drop args -> identical to before
    (riser_count=0, drop_count=0 on all lines; total_risers/total_drops=0).
    """
    entity_ids = ("e1",)
    geometry = {"e1": _line_geom(0.0, 0.0, 1000.0, 0.0)}
    identity = _make_identity(entity_ids=entity_ids, services=(_pipe_size("VAC", 54),))
    run = _make_run(entity_ids=entity_ids)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_unknown_scale(),
        # No rise_symbols / drop_symbols -- uses defaults.
    )

    assert len(result.lines) == 1
    ln = result.lines[0]
    assert ln.riser_count == 0
    assert ln.drop_count == 0
    assert result.total_risers == 0
    assert result.total_drops == 0


# ---------------------------------------------------------------------------
# measured_length_by_group override tests (C0 / be-639b)
# ---------------------------------------------------------------------------


def test_measured_length_by_group_overrides_naive_sum_for_matching_key() -> None:
    """A matching (layer_ref, colour_key) in measured_length_by_group replaces naive sum.

    The entity geometry gives 3000 drawing units; the injected measured length
    is 9999.0.  The line's drawing_length must use the measured value.
    """
    entity_ids = ("m1",)
    geometry = {"m1": _line_geom(0.0, 0.0, 3000.0, 0.0)}

    run = _make_run(layer_ref="PIPE", colour_key="red", entity_ids=entity_ids)
    identity = _make_identity(
        layer_ref="PIPE",
        colour_key="red",
        entity_ids=entity_ids,
        services=(_pipe_size("VAC", 54),),
        status=IDENTITY_RESOLVED,
    )

    measured: dict[tuple[str | None, str | None], float] = {("PIPE", "red"): 9999.0}

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_unknown_scale(),
        measured_length_by_group=measured,
    )

    assert len(result.lines) == 1
    assert result.lines[0].drawing_length == 9999.0, (
        f"Expected 9999.0 (measured), got {result.lines[0].drawing_length}"
    )


def test_measured_length_by_group_pdf_case_none_layer_ref() -> None:
    """PDF revisions have layer_ref=None; (None, colour_key) must match correctly."""
    entity_ids = ("p1",)
    geometry = {"p1": _line_geom(0.0, 0.0, 1000.0, 0.0)}

    run = _make_run(layer_ref=None, colour_key="#ff0000", entity_ids=entity_ids)
    identity = _make_identity(
        layer_ref=None,
        colour_key="#ff0000",
        entity_ids=entity_ids,
        services=(_pipe_size("VAC", 54),),
        status=IDENTITY_RESOLVED,
    )

    measured: dict[tuple[str | None, str | None], float] = {(None, "#ff0000"): 5500.0}

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_unknown_scale(),
        measured_length_by_group=measured,
    )

    assert len(result.lines) == 1
    assert result.lines[0].drawing_length == 5500.0, (
        f"Expected 5500.0 (measured), got {result.lines[0].drawing_length}"
    )


def test_measured_length_by_group_non_matching_key_falls_back_to_naive() -> None:
    """A key absent from measured_length_by_group uses the naive entity-sum."""
    entity_ids = ("n1",)
    geometry = {"n1": _line_geom(0.0, 0.0, 2000.0, 0.0)}

    run = _make_run(layer_ref="PIPE", colour_key="blue", entity_ids=entity_ids)
    identity = _make_identity(
        layer_ref="PIPE",
        colour_key="blue",
        entity_ids=entity_ids,
        services=(_pipe_size("MA", 42),),
        status=IDENTITY_RESOLVED,
    )

    # Only a different key is in the mapping -- "blue" must fall back.
    measured: dict[tuple[str | None, str | None], float] = {("PIPE", "red"): 9999.0}

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_unknown_scale(),
        measured_length_by_group=measured,
    )

    assert len(result.lines) == 1
    assert result.lines[0].drawing_length == 2000.0, (
        f"Expected 2000.0 (naive), got {result.lines[0].drawing_length}"
    )


def test_measured_length_by_group_none_reproduces_current_results() -> None:
    """measured_length_by_group=None must be byte-identical to the old default.

    Regression guard: the None path must match compute_service_takeoff without
    the kwarg (which also defaults to None).
    """
    entity_ids = ("r1", "r2")
    geometry = {
        "r1": _line_geom(0.0, 0.0, 1500.0, 0.0),
        "r2": _line_geom(1500.0, 0.0, 3000.0, 0.0),
    }
    run = _make_run(entity_ids=entity_ids)
    identity = _make_identity(
        entity_ids=entity_ids,
        services=(_pipe_size("VAC", 54),),
        status=IDENTITY_RESOLVED,
    )

    shared_kwargs: dict[str, Any] = {
        "runs": [run],
        "identities": [identity],
        "geometry_by_entity_id": geometry,
        "rooms": [],
        "scale": _unknown_scale(),
    }

    result_default = compute_service_takeoff(**shared_kwargs)
    result_explicit_none = compute_service_takeoff(**shared_kwargs, measured_length_by_group=None)

    assert result_default.lines[0].drawing_length == result_explicit_none.lines[0].drawing_length
    assert result_default.lines[0].drawing_length == 3000.0


# ---------------------------------------------------------------------------
# LP2 (#654): per-room centerline clipping
# ---------------------------------------------------------------------------


def test_dict_coord_geometry_lengths_and_anchors() -> None:
    """Real materialized geometry uses dict coords {"x","y"}; length + room anchor must work.

    Regression for the KeyError on real M-540003 DWG (synthetic tests used list coords, masking
    it). A line entity with dict-shaped start/end is measured and anchored into its room.
    """
    room = _square_room(x=0.0, room_id="room-a")  # 10000 x 10000 du square at origin
    identity = _make_identity(services=(_pipe_size("VAC", 54),), entity_ids=("e1",))
    # dict coords, no measured_geometry -> exercises _entity_drawn_length + _run_anchor fallback
    geom = {
        "e1": {
            "start": {"x": 1000.0, "y": 5000.0, "z": 0.0},
            "end": {"x": 4000.0, "y": 5000.0, "z": 0.0},
        }
    }
    result = compute_service_takeoff(
        runs=[_make_run()],
        identities=[identity],
        geometry_by_entity_id=geom,
        rooms=[room],
        scale=_confirmed_mm_scale(),
    )
    assert len(result.lines) == 1
    ln = result.lines[0]
    assert ln.room_id == "room-a"  # anchored into the room (dict-coord anchor worked)
    assert ln.drawing_length == pytest.approx(3000.0)  # |4000-1000| du (dict-coord length worked)
    assert ln.real_length_m == pytest.approx(3.0)  # x 0.001


def test_lp2_clip_splits_length_across_rooms() -> None:
    """A measured run spanning two rooms has its length distributed per room, not all-to-one.

    Two adjacent 10000-du square rooms; a horizontal centerline crossing both (10000 du in each).
    Pre-LP2 the whole 20000 du landed in the single anchor room; LP2 clips per room.
    """
    room_a = _square_room(x=0.0, room_id="room-a")
    room_b = _square_room(x=10000.0, room_id="room-b")
    identity = _make_identity(services=(_pipe_size("VAC", 54),), entity_ids=("e1",))
    polylines = (((0.0, 5000.0), (20000.0, 5000.0)),)

    result = compute_service_takeoff(
        runs=[_make_run()],
        identities=[identity],
        geometry_by_entity_id={"e1": _line_geom(0.0, 5000.0, 20000.0, 5000.0)},
        rooms=[room_a, room_b],
        scale=_confirmed_mm_scale(),
        measured_length_by_group={("Pipes", "idx150"): 20000.0},
        measured_geometry_by_group={("Pipes", "idx150"): polylines},
    )

    by_room = {ln.room_id: ln for ln in result.lines}
    assert set(by_room) == {"room-a", "room-b"}
    assert by_room["room-a"].drawing_length == pytest.approx(10000.0, rel=1e-3)
    assert by_room["room-b"].drawing_length == pytest.approx(10000.0, rel=1e-3)
    assert by_room["room-a"].real_length_m == pytest.approx(10.0, rel=1e-3)
    assert by_room["room-b"].real_length_m == pytest.approx(10.0, rel=1e-3)
    # run_count is NOT inflated: the run's per-room partitions land in DISTINCT buckets, so each
    # bucket counts the run exactly once (regression guard against per-partition double-counting).
    assert by_room["room-a"].run_count == 1
    assert by_room["room-b"].run_count == 1


def test_lp2_falls_back_to_anchor_without_geometry() -> None:
    """No persisted geometry -> pre-LP2 behaviour: whole length to the single anchor room."""
    room_a = _square_room(x=0.0, room_id="room-a")
    room_b = _square_room(x=10000.0, room_id="room-b")
    identity = _make_identity(services=(_pipe_size("VAC", 54),), entity_ids=("e1",))

    result = compute_service_takeoff(
        runs=[_make_run()],
        identities=[identity],
        # Anchor (midpoint ~ (5000, 5000)) falls in room-a.
        geometry_by_entity_id={"e1": _line_geom(1000.0, 5000.0, 9000.0, 5000.0)},
        rooms=[room_a, room_b],
        scale=_confirmed_mm_scale(),
        measured_length_by_group={("Pipes", "idx150"): 8000.0},
        measured_geometry_by_group=None,
    )

    by_room = {ln.room_id: ln for ln in result.lines}
    assert set(by_room) == {"room-a"}
    assert by_room["room-a"].drawing_length == pytest.approx(8000.0, rel=1e-3)


def test_lp2_length_outside_all_rooms_is_unassigned() -> None:
    """Centerline length that falls outside every room polygon lands in the unassigned bucket."""
    room_a = _square_room(x=0.0, size=10000.0, room_id="room-a")
    identity = _make_identity(services=(_pipe_size("VAC", 54),), entity_ids=("e1",))
    # First half inside room-a (5000 du), second half outside any room (5000 du).
    polylines = (((0.0, 5000.0), (15000.0, 5000.0)),)

    result = compute_service_takeoff(
        runs=[_make_run()],
        identities=[identity],
        geometry_by_entity_id={"e1": _line_geom(0.0, 5000.0, 15000.0, 5000.0)},
        rooms=[room_a],
        scale=_confirmed_mm_scale(),
        measured_length_by_group={("Pipes", "idx150"): 15000.0},
        measured_geometry_by_group={("Pipes", "idx150"): polylines},
    )

    by_room = {ln.room_id: ln for ln in result.lines}
    assert by_room["room-a"].drawing_length == pytest.approx(10000.0, rel=1e-3)
    assert ROOM_UNASSIGNED_ID in by_room
    assert by_room[ROOM_UNASSIGNED_ID].drawing_length == pytest.approx(5000.0, rel=1e-3)
    # A run with any length outside all rooms is counted once as unassigned.
    assert result.unassigned_run_count == 1


# ---------------------------------------------------------------------------
# Label-proximity fallback tests (#662)
# ---------------------------------------------------------------------------


def _label_room(
    x: float,
    y: float,
    room_id: str = "label-room-1",
    name: str = "PH Plantroom",
    number: str = "0.9.01",
) -> Room:
    return room_from_label(
        room_id, source="test", location=(x, y), name=name, number=number, confidence=0.7
    )


def test_anchor_label_room_in_range_attributed() -> None:
    """Run anchor outside polygon rooms but within 8m of a label room -> attributed to that room.

    mm fixture: radius = 8.0 / 0.001 = 8000 du. Label room 1000 du away -> within range.
    """
    entity_ids = ("e1",)
    # Anchor midpoint = (5500, 5000) -- outside the polygon room (0,0)-(1000,1000).
    geometry = {"e1": _line_geom(5000.0, 5000.0, 6000.0, 5000.0)}
    services = (_pipe_size("VAC", 54),)
    identity = _make_identity(entity_ids=entity_ids, services=services)
    run = _make_run(entity_ids=entity_ids)
    poly_room = _square_room(x=0.0, y=0.0, size=1000.0)
    # Label room 1000 du from anchor (5500 + 1000 = 6500, same y).
    label = _label_room(6500.0, 5000.0)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[poly_room, label],
        scale=_confirmed_mm_scale(),
    )

    assert len(result.lines) == 1
    ln = result.lines[0]
    assert ln.room_id == "label-room-1"
    assert ln.room_name == "PH Plantroom"
    assert ln.room_number == "0.9.01"
    assert result.unassigned_run_count == 0


def test_anchor_label_room_out_of_range_unassigned() -> None:
    """Label room 20000 du away (>> 8000 du radius) -> run stays unassigned."""
    entity_ids = ("e1",)
    geometry = {"e1": _line_geom(5000.0, 5000.0, 6000.0, 5000.0)}
    services = (_pipe_size("VAC", 54),)
    identity = _make_identity(entity_ids=entity_ids, services=services)
    run = _make_run(entity_ids=entity_ids)
    poly_room = _square_room(x=0.0, y=0.0, size=1000.0)
    # Label room 20000 du away -- beyond 8000 du radius.
    label = _label_room(25500.0, 5000.0)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[poly_room, label],
        scale=_confirmed_mm_scale(),
    )

    assert len(result.lines) == 1
    assert result.lines[0].room_id == ROOM_UNASSIGNED_ID
    assert result.unassigned_run_count == 1


def test_polygon_containment_precedence_over_label() -> None:
    """Anchor inside a polygon room AND within range of a label room -> polygon room wins."""
    entity_ids = ("e1",)
    # Anchor midpoint = (500, 500) -- inside poly_room (0,0)-(10000,10000).
    geometry = {"e1": _line_geom(0.0, 500.0, 1000.0, 500.0)}
    services = (_pipe_size("VAC", 54),)
    identity = _make_identity(entity_ids=entity_ids, services=services)
    run = _make_run(entity_ids=entity_ids)
    poly_room = _square_room(x=0.0, y=0.0, size=10000.0, room_id="poly-room-1")
    # Label room very close (100 du away) -- should NOT override polygon containment.
    label = _label_room(600.0, 500.0)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[poly_room, label],
        scale=_confirmed_mm_scale(),
    )

    assert len(result.lines) == 1
    assert result.lines[0].room_id == "poly-room-1"
    assert result.unassigned_run_count == 0


def test_lp2_remainder_attributed_to_nearest_label_room() -> None:
    """LP2: centerline remainder outside polygon rooms -> attributed to nearby label room.

    Polyline: (0,5000) to (15000,5000). Room-a covers (0,0)-(10000,10000) -> 10000 du inside.
    Remainder: (10000,5000)-(15000,5000) = 5000 du outside. Label room near (12500, 5000).
    With mm scale, radius = 8000 du. 12500 - 12500 = 0 du to label room -> within range.
    """
    room_a = _square_room(x=0.0, size=10000.0, room_id="room-a")
    identity = _make_identity(services=(_pipe_size("VAC", 54),), entity_ids=("e1",))
    polylines = (((0.0, 5000.0), (15000.0, 5000.0)),)
    # representative_point of (10000,5000)-(15000,5000) is near x=12500, y=5000.
    label = _label_room(12500.0, 5000.0)

    result = compute_service_takeoff(
        runs=[_make_run()],
        identities=[identity],
        geometry_by_entity_id={"e1": _line_geom(0.0, 5000.0, 15000.0, 5000.0)},
        rooms=[room_a, label],
        scale=_confirmed_mm_scale(),
        measured_length_by_group={("Pipes", "idx150"): 15000.0},
        measured_geometry_by_group={("Pipes", "idx150"): polylines},
    )

    by_room = {ln.room_id: ln for ln in result.lines}
    assert "room-a" in by_room
    assert by_room["room-a"].drawing_length == pytest.approx(10000.0, rel=1e-3)
    assert "label-room-1" in by_room
    assert by_room["label-room-1"].drawing_length == pytest.approx(5000.0, rel=1e-3)
    assert ROOM_UNASSIGNED_ID not in by_room
    assert result.unassigned_run_count == 0


def test_lp2_remainder_label_room_out_of_range_unassigned() -> None:
    """LP2: label room far from the remainder -> remainder stays unassigned."""
    room_a = _square_room(x=0.0, size=10000.0, room_id="room-a")
    identity = _make_identity(services=(_pipe_size("VAC", 54),), entity_ids=("e1",))
    polylines = (((0.0, 5000.0), (15000.0, 5000.0)),)
    # Label room far from the remainder's representative_point (~12500, 5000).
    label = _label_room(100000.0, 5000.0)

    result = compute_service_takeoff(
        runs=[_make_run()],
        identities=[identity],
        geometry_by_entity_id={"e1": _line_geom(0.0, 5000.0, 15000.0, 5000.0)},
        rooms=[room_a, label],
        scale=_confirmed_mm_scale(),
        measured_length_by_group={("Pipes", "idx150"): 15000.0},
        measured_geometry_by_group={("Pipes", "idx150"): polylines},
    )

    by_room = {ln.room_id: ln for ln in result.lines}
    assert "room-a" in by_room
    assert ROOM_UNASSIGNED_ID in by_room
    assert by_room[ROOM_UNASSIGNED_ID].drawing_length == pytest.approx(5000.0, rel=1e-3)
    assert result.unassigned_run_count == 1


def test_label_fallback_skipped_without_scale_factor() -> None:
    """Unknown scale -> conversion_factor is None -> label_radius is None -> fallback skipped."""
    entity_ids = ("e1",)
    # Anchor at (5500, 5000) -- outside poly_room (0,0)-(1000,1000).
    geometry = {"e1": _line_geom(5000.0, 5000.0, 6000.0, 5000.0)}
    services = (_pipe_size("VAC", 54),)
    identity = _make_identity(entity_ids=entity_ids, services=services)
    run = _make_run(entity_ids=entity_ids)
    poly_room = _square_room(x=0.0, y=0.0, size=1000.0)
    # Label room right next to the anchor (1 du away) -- would be in range if scale were known.
    label = _label_room(5501.0, 5000.0)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[poly_room, label],
        scale=_unknown_scale(),  # conversion_factor=None
    )

    assert len(result.lines) == 1
    assert result.lines[0].room_id == ROOM_UNASSIGNED_ID
    assert result.unassigned_run_count == 1


def test_lp2_remainder_distributed_across_two_label_rooms() -> None:
    """LP2 per-segment: a line with no polygon rooms splits between two label rooms by segment.

    Horizontal line (0,5000)-(20000,5000), no polygon rooms.
    Label room A near (3000,5000), label room B near (17000,5000).
    With mm scale, radius = 8000 du (= 8.0 / 0.001).

    The single segment midpoint is (10000,5000) — equidistant from both labels (7000 du each).
    To force a clear split we use a two-segment polyline: (0,5000)-(10000,5000)-(20000,5000)
    so segment 1 midpoint=(5000,5000) nearest A (2000 du), segment 2 midpoint=(15000,5000)
    nearest B (2000 du). Each segment is 10000 du -> each label room gets ~10000 du.
    """
    identity = _make_identity(services=(_pipe_size("VAC", 54),), entity_ids=("e1",))
    # Two-segment polyline that straddles both label rooms.
    polylines = (((0.0, 5000.0), (10000.0, 5000.0), (20000.0, 5000.0)),)
    label_a = _label_room(3000.0, 5000.0, room_id="label-a", name="Room A", number="1.01")
    label_b = _label_room(17000.0, 5000.0, room_id="label-b", name="Room B", number="1.02")

    result = compute_service_takeoff(
        runs=[_make_run()],
        identities=[identity],
        geometry_by_entity_id={"e1": _line_geom(0.0, 5000.0, 20000.0, 5000.0)},
        rooms=[label_a, label_b],
        scale=_confirmed_mm_scale(),
        measured_length_by_group={("Pipes", "idx150"): 20000.0},
        measured_geometry_by_group={("Pipes", "idx150"): polylines},
    )

    by_room = {ln.room_id: ln for ln in result.lines}
    # Both label rooms must receive length.
    assert "label-a" in by_room, f"label-a missing; got rooms: {set(by_room)}"
    assert "label-b" in by_room, f"label-b missing; got rooms: {set(by_room)}"
    assert ROOM_UNASSIGNED_ID not in by_room
    # Each segment is 10000 du, so each label room gets approx 10000 du.
    assert by_room["label-a"].drawing_length == pytest.approx(10000.0, rel=1e-3)
    assert by_room["label-b"].drawing_length == pytest.approx(10000.0, rel=1e-3)
    assert result.unassigned_run_count == 0


def test_lp2_per_segment_segments_out_of_range_unassigned() -> None:
    """LP2 per-segment: segments far from all label rooms land unassigned.

    A line entirely 50000 du from the only label room (radius 8000 du) -> all unassigned.
    """
    identity = _make_identity(services=(_pipe_size("VAC", 54),), entity_ids=("e1",))
    polylines = (((0.0, 5000.0), (10000.0, 5000.0)),)
    # Label room 50000 du away -- well beyond 8000 du radius.
    label = _label_room(60000.0, 5000.0, room_id="far-label")

    result = compute_service_takeoff(
        runs=[_make_run()],
        identities=[identity],
        geometry_by_entity_id={"e1": _line_geom(0.0, 5000.0, 10000.0, 5000.0)},
        rooms=[label],
        scale=_confirmed_mm_scale(),
        measured_length_by_group={("Pipes", "idx150"): 10000.0},
        measured_geometry_by_group={("Pipes", "idx150"): polylines},
    )

    by_room = {ln.room_id: ln for ln in result.lines}
    assert ROOM_UNASSIGNED_ID in by_room, f"Expected unassigned; got rooms: {set(by_room)}"
    assert "far-label" not in by_room
    assert by_room[ROOM_UNASSIGNED_ID].drawing_length == pytest.approx(10000.0, rel=1e-3)
    assert result.unassigned_run_count == 1


def test_nearest_label_room_deterministic_tie_break() -> None:
    """_nearest_label_room: two equidistant label rooms -> lower room.id wins regardless of
    order (permutation-invariant tie-break)."""
    # Both rooms at equal distance (500 du) from origin.
    room_a = _label_room(500.0, 0.0, room_id="aaa-room")
    room_b = _label_room(-500.0, 0.0, room_id="zzz-room")
    point = (0.0, 0.0)
    radius = 1000.0

    result_forward = _nearest_label_room(point, [room_a, room_b], radius=radius)
    result_reversed = _nearest_label_room(point, [room_b, room_a], radius=radius)

    assert result_forward is not None
    assert result_reversed is not None
    assert result_forward.id == "aaa-room"
    assert result_reversed.id == "aaa-room"


# ---------------------------------------------------------------------------
# D3c (#798): dominant-service collapse for over-tagged runs
# ---------------------------------------------------------------------------


def test_d3c_dominant_service_collapses_duplicate_tags_to_once() -> None:
    """D3c: a run with services=[SVP/100, SVP/100, SVP/75] contributes length ONCE to SVP/100.

    Pre-D3c the loop added length for each entry (3x the run length).  SVP/100 is dominant
    (2 occurrences vs 1 for SVP/75).  After collapse: one line for SVP/100, none for SVP/75.
    run_count in the SVP/100 bucket must be 1, not 3.
    """
    entity_ids = ("e1",)
    run_length = 1500.0
    geometry = {"e1": _line_geom(0.0, 0.0, run_length, 0.0)}

    svp_100a = _pipe_size("SVP", 100)
    svp_100b = ServiceSize(
        service="SVP",
        size=svp_100a.size,
        source_tag_text="100 mm SVP tag2",
        basis=BASIS_TAG_TEXT,
    )
    svp_75 = _pipe_size("SVP", 75)

    # Simulate fuse_run_service_identities output: all 3 nearby tags assigned.
    # Sorted by (service, size.raw, source_tag_text):
    #   SVP/100/"100 mm SVP", SVP/100/"100 mm SVP tag2", SVP/75/"75 mm SVP"
    services = (svp_100a, svp_100b, svp_75)
    identity = _make_identity(entity_ids=entity_ids, services=services, status=IDENTITY_RESOLVED)
    run = _make_run(entity_ids=entity_ids)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_unknown_scale(),
    )

    # Only SVP/100 line; SVP/75 must NOT appear.
    assert len(result.lines) == 1, (
        f"Expected 1 line (dominant SVP/100 only), got {len(result.lines)}: "
        f"{[(ln.service, ln.size_raw) for ln in result.lines]}"
    )
    ln = result.lines[0]
    assert ln.service == "SVP"
    assert ln.size_raw == "100"
    # Length attributed once, not 3x.
    assert ln.drawing_length == run_length, (
        f"drawing_length should be {run_length} (once), got {ln.drawing_length}"
    )
    # run_count is 1 (the run, not the number of tags).
    assert ln.run_count == 1, f"run_count should be 1, got {ln.run_count}"
    # Collapsed run is not a bundle.
    assert ln.bundle is False


def test_d3c_single_service_run_unchanged() -> None:
    """D3c: a run with a single service entry is unaffected (pre-D3b byte-identical path)."""
    entity_ids = ("e1",)
    run_length = 2000.0
    geometry = {"e1": _line_geom(0.0, 0.0, run_length, 0.0)}
    services = (_pipe_size("HWS", 50),)
    identity = _make_identity(entity_ids=entity_ids, services=services, status=IDENTITY_RESOLVED)
    run = _make_run(entity_ids=entity_ids)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_unknown_scale(),
    )

    assert len(result.lines) == 1
    ln = result.lines[0]
    assert ln.service == "HWS"
    assert ln.drawing_length == run_length
    assert ln.run_count == 1
    assert ln.bundle is False


def test_d3c_true_bundle_distinct_services_preserved() -> None:
    """D3c: a run with all-unique (service, size.raw) pairs keeps the bundle-length model.

    A run with services=[VAC/54, AGSS/42] (distinct pairs) must still produce two lines,
    each with the full run length.  Dominant collapse must NOT fire when there are no
    duplicate (service, size.raw) pairs.
    """
    entity_ids = ("e1",)
    run_length = 3000.0
    geometry = {"e1": _line_geom(0.0, 0.0, run_length, 0.0)}
    services = (_pipe_size("VAC", 54), _pipe_size("AGSS", 42))
    identity = _make_identity(entity_ids=entity_ids, services=services, status=IDENTITY_RESOLVED)
    run = _make_run(entity_ids=entity_ids)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_unknown_scale(),
    )

    # Both services must appear; each gets the full length.
    assert len(result.lines) == 2, f"Expected 2 lines (bundle), got {len(result.lines)}"
    by_svc = {ln.service: ln for ln in result.lines}
    assert "VAC" in by_svc
    assert "AGSS" in by_svc
    assert by_svc["VAC"].drawing_length == run_length
    assert by_svc["AGSS"].drawing_length == run_length
    assert by_svc["VAC"].bundle is True
    assert by_svc["AGSS"].bundle is True


def test_d3c_dominant_service_helper_direct() -> None:
    """Unit test for _dominant_service directly.

    - Empty / single entry -> None (caller uses bundle model / unchanged).
    - All-unique pairs -> None (true bundle, no collapse).
    - Duplicate pairs -> returns representative ServiceSize for the dominant pair.
    - Tie among duplicates -> deterministic (lowest (service, size.raw, source_tag_text)).
    """
    svp_100a = _pipe_size("SVP", 100)
    svp_100b = ServiceSize(
        service="SVP",
        size=svp_100a.size,
        source_tag_text="100 mm SVP tag2",
        basis=BASIS_TAG_TEXT,
    )
    svp_75 = _pipe_size("SVP", 75)
    vac_54 = _pipe_size("VAC", 54)
    agss_42 = _pipe_size("AGSS", 42)

    # Empty tuple.
    assert _dominant_service(()) is None

    # Single entry.
    assert _dominant_service((svp_100a,)) is None

    # All-unique -> None (true bundle).
    assert _dominant_service((vac_54, agss_42)) is None

    # [SVP/100, SVP/100, SVP/75] -> dominant=SVP/100; first entry in sorted order wins.
    winner = _dominant_service((svp_100a, svp_100b, svp_75))
    assert winner is not None
    assert winner.service == "SVP"
    assert winner.size.raw == "100"

    # Order should not matter (input already sorted by fuse_run_service_identities).
    winner2 = _dominant_service((svp_75, svp_100a, svp_100b))
    assert winner2 is not None
    assert winner2.service == "SVP"
    assert winner2.size.raw == "100"


def test_d3c_dense_tag_inflation_conservation() -> None:
    """D3c regression: 10 identical SVP/100 tags on one run must not inflate total length.

    Simulates the real-data case where D3b tag reassembly produces dense nearby tags.
    Pre-fix: run_count=10, drawing_length=10x baseline.
    Post-fix: run_count=1, drawing_length=baseline.
    """
    entity_ids = ("e1",)
    run_length = 184.0  # drawing units (mirrors the real-data baseline)
    geometry = {"e1": _line_geom(0.0, 0.0, run_length, 0.0)}

    # 10 SVP/100 tags, all with different source_tag_text (as reassembled tags would have).
    services = tuple(
        ServiceSize(
            service="SVP",
            size=PipeSize(kind="round", diameter=100, width=None, height=None, raw="100"),
            source_tag_text=f"SVP100-tag-{i}",
            basis=BASIS_TAG_TEXT,
        )
        for i in range(10)
    )
    identity = _make_identity(entity_ids=entity_ids, services=services, status=IDENTITY_RESOLVED)
    run = _make_run(entity_ids=entity_ids)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_unknown_scale(),
    )

    assert len(result.lines) == 1
    ln = result.lines[0]
    assert ln.drawing_length == run_length, (
        f"Expected drawing_length={run_length} (conserved), got {ln.drawing_length} (inflated)"
    )
    assert ln.run_count == 1, f"Expected run_count=1, got {ln.run_count}"


def test_d3c_med_gas_bundle_all_services_survive() -> None:
    """D3c round-2: med-gas multi-service bundle must not lose services after per-service collapse.

    Real-data regression: M-540003 had [VAC/54, MA/42, OXY/42, AGSS/42, AGSS/42].
    The v1 single-dominant collapse saw AGSS/42 as the most-frequent (service,size) pair
    and emitted only AGSS, dropping VAC/MA/OXY and collapsing 3,660 m to 244 m.

    The per-service fix must:
    - Emit 4 distinct services (VAC, MA, OXY, AGSS), each with the full run length.
    - Deduplicate AGSS/42 (appears twice) to a single AGSS/42 entry.
    - Mark every line as bundle=True.
    """
    entity_ids = ("e1",)
    run_length = 3660.0  # mirrors real-data scale
    geometry = {"e1": _line_geom(0.0, 0.0, run_length, 0.0)}

    vac_54 = _pipe_size("VAC", 54)
    ma_42 = _pipe_size("MA", 42)
    oxy_42 = _pipe_size("OXY", 42)
    agss_42a = _pipe_size("AGSS", 42)
    agss_42b = ServiceSize(
        service="AGSS",
        size=agss_42a.size,
        source_tag_text="AGSS-42-tag2",
        basis=BASIS_TAG_TEXT,
    )

    # Simulates the reassembled tag output: VAC/54, MA/42, OXY/42, AGSS/42 (x2).
    services = (vac_54, ma_42, oxy_42, agss_42a, agss_42b)
    identity = _make_identity(entity_ids=entity_ids, services=services, status=IDENTITY_RESOLVED)
    run = _make_run(entity_ids=entity_ids)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_unknown_scale(),
    )

    # All 4 distinct services must appear.
    assert len(result.lines) == 4, (
        f"Expected 4 lines (VAC,MA,OXY,AGSS), got {len(result.lines)}: "
        f"{[(ln.service, ln.size_raw) for ln in result.lines]}"
    )
    by_svc = {ln.service: ln for ln in result.lines}
    assert set(by_svc.keys()) == {"VAC", "MA", "OXY", "AGSS"}
    # Each service gets the FULL run length.
    for svc, ln in by_svc.items():
        assert ln.drawing_length == run_length, (
            f"{svc}: expected drawing_length={run_length}, got {ln.drawing_length}"
        )
    # All lines are in the bundle.
    for ln in result.lines:
        assert ln.bundle is True, f"{ln.service} should be bundle=True"
    # AGSS deduped to size 42.
    assert by_svc["AGSS"].size_raw == "42"


def test_d3c_collapse_services_per_service_helper_direct() -> None:
    """Unit tests for _collapse_services_per_service.

    - Empty tuple -> empty tuple.
    - Single entry -> unchanged.
    - All-unique service names -> unchanged (true bundle, no collapse).
    - Single-service with duplicate sizes -> deduplicated to dominant size.
    - Multi-service where one service has duplicates -> that service deduped, others kept.
    """
    svp_100a = _pipe_size("SVP", 100)
    svp_100b = ServiceSize(
        service="SVP",
        size=svp_100a.size,
        source_tag_text="100 mm SVP tag2",
        basis=BASIS_TAG_TEXT,
    )
    svp_75 = _pipe_size("SVP", 75)
    vac_54 = _pipe_size("VAC", 54)
    ma_42 = _pipe_size("MA", 42)
    oxy_42 = _pipe_size("OXY", 42)
    agss_42a = _pipe_size("AGSS", 42)
    agss_42b = ServiceSize(
        service="AGSS",
        size=agss_42a.size,
        source_tag_text="AGSS-42-tag2",
        basis=BASIS_TAG_TEXT,
    )

    # Empty.
    assert _collapse_services_per_service(()) == ()

    # Single entry unchanged.
    result_single = _collapse_services_per_service((svp_100a,))
    assert len(result_single) == 1
    assert result_single[0].service == "SVP"

    # All-unique service names -> all preserved.
    result_bundle = _collapse_services_per_service((vac_54, ma_42))
    assert len(result_bundle) == 2
    assert {ss.service for ss in result_bundle} == {"VAC", "MA"}

    # [SVP/100, SVP/100, SVP/75] -> single-service dedup -> [SVP/100].
    result_svp = _collapse_services_per_service((svp_100a, svp_100b, svp_75))
    assert len(result_svp) == 1
    assert result_svp[0].service == "SVP"
    assert result_svp[0].size.raw == "100"

    # Med-gas: [VAC/54, MA/42, OXY/42, AGSS/42, AGSS/42] -> 4 distinct services.
    result_med = _collapse_services_per_service((vac_54, ma_42, oxy_42, agss_42a, agss_42b))
    assert len(result_med) == 4
    services_in = {ss.service for ss in result_med}
    assert services_in == {"VAC", "MA", "OXY", "AGSS"}
    agss_entry = next(ss for ss in result_med if ss.service == "AGSS")
    assert agss_entry.size.raw == "42"


# ---------------------------------------------------------------------------
# D3c Part B (#798): colour_differentiated gate + achromatic apportionment
# ---------------------------------------------------------------------------


def _seg_label(x: float, y: float, service: str, size_raw: str | None = "100") -> SegmentLabel:
    return SegmentLabel(point=(x, y), service=service, size_raw=size_raw, size_kind="round")


def test_is_achromatic_colour_key_black_six_char() -> None:
    """000000 (black) is achromatic."""
    assert _is_achromatic_colour_key("000000") is True


def test_is_achromatic_colour_key_white_six_char() -> None:
    """ffffff (white) is achromatic."""
    assert _is_achromatic_colour_key("ffffff") is True


def test_is_achromatic_colour_key_grey_six_char() -> None:
    """808080 (mid grey, spread=0) is achromatic."""
    assert _is_achromatic_colour_key("808080") is True


def test_is_achromatic_colour_key_bylayer_eight_char() -> None:
    """c3000007 (8-char with alpha, stripped to 000007 ≈ black) is achromatic."""
    assert _is_achromatic_colour_key("c3000007") is True


def test_is_achromatic_colour_key_chromatic() -> None:
    """ff0000 (pure red, spread=255) is chromatic."""
    assert _is_achromatic_colour_key("ff0000") is False


def test_is_achromatic_colour_key_idx_is_chromatic() -> None:
    """idx:150 (index colour, no parseable RGB) is treated as chromatic (safe default)."""
    assert _is_achromatic_colour_key("idx:150") is False


def test_achromatic_conservation_with_labels() -> None:
    """Achromatic mode: Σ per-service + unknown == centerline total (±0.1 m).

    Two-segment polyline (100 du + 100 du = 200 du total).
    Label SVP@50,0 near segment 1 midpoint (25,0); label CDP@150,0 near segment 2 midpoint (150,0).
    Each segment attributed to its nearest label.
    """
    # Single run with two segments in its geometry.
    entity_ids = ("e1",)
    run_length_du = 200.0
    # Polyline: (0,0) -> (100,0) -> (200,0)  [two 100-du segments]
    polylines = (((0.0, 0.0), (100.0, 0.0), (200.0, 0.0)),)

    identity = _make_identity(
        entity_ids=entity_ids,
        services=(
            _pipe_size("SVP", 100),
            _pipe_size("CDP", 100),
        ),
        status=IDENTITY_RESOLVED,
    )
    run = _make_run(entity_ids=entity_ids)
    geom = {"e1": _line_geom(0.0, 0.0, run_length_du, 0.0)}

    # Two labels: SVP near midpoint of segment 1 (50, 0); CDP near midpoint of segment 2 (150, 0).
    labels = [
        _seg_label(50.0, 0.0, "SVP"),
        _seg_label(150.0, 0.0, "CDP"),
    ]

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geom,
        rooms=[],
        scale=_confirmed_mm_scale(),
        measured_geometry_by_group={("Pipes", "idx150"): polylines},
        colour_differentiated=False,
        segment_labels=labels,
        segment_label_max_m=5.0,
    )

    # Conservation: Σ all lines == total run length.
    total = sum(ln.drawing_length for ln in result.lines)
    assert abs(total - run_length_du) < 0.1, (
        f"Conservation failed: total={total}, expected {run_length_du}"
    )

    by_svc = {ln.service: ln for ln in result.lines}
    # Both services attributed; no unknown (every segment has a label nearby).
    assert "SVP" in by_svc, f"SVP missing; got {set(by_svc)}"
    assert "CDP" in by_svc, f"CDP missing; got {set(by_svc)}"
    if SERVICE_UNKNOWN in by_svc:
        raise AssertionError(f"Unexpected unknown length: {by_svc[SERVICE_UNKNOWN].drawing_length}")

    # Each segment 100 du → each service gets 100 du.
    assert by_svc["SVP"].drawing_length == pytest.approx(100.0, abs=0.1)
    assert by_svc["CDP"].drawing_length == pytest.approx(100.0, abs=0.1)


def test_achromatic_honest_unknown_for_orphan_segments() -> None:
    """Achromatic mode: segments with no label within radius land in SERVICE_UNKNOWN.

    A 200-du run with no labels at all → all length in unknown; nothing forced onto
    a service.
    """
    entity_ids = ("e1",)
    polylines = (((0.0, 0.0), (100.0, 0.0), (200.0, 0.0)),)
    identity = _make_identity(
        entity_ids=entity_ids,
        services=(_pipe_size("SVP", 100),),
        status=IDENTITY_RESOLVED,
    )
    run = _make_run(entity_ids=entity_ids)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id={"e1": _line_geom(0.0, 0.0, 200.0, 0.0)},
        rooms=[],
        scale=_confirmed_mm_scale(),
        measured_geometry_by_group={("Pipes", "idx150"): polylines},
        colour_differentiated=False,
        segment_labels=[],  # no labels → all orphan
        segment_label_max_m=5.0,
    )

    by_svc = {ln.service: ln for ln in result.lines}
    assert SERVICE_UNKNOWN in by_svc, "Orphan segments must land in SERVICE_UNKNOWN"
    assert "SVP" not in by_svc, "SVP must not appear when no tag is nearby"
    # Conservation: unknown length == total.
    assert by_svc[SERVICE_UNKNOWN].drawing_length == pytest.approx(200.0, abs=0.1)


def test_achromatic_conservation_with_mixed_attributed_and_orphan() -> None:
    """Achromatic: partial attribution conserves — attributed + unknown == total."""
    entity_ids = ("e1",)
    # Three segments: (0,0)→(100,0)→(200,0)→(300,0); labels near first two midpoints only.
    polylines = (((0.0, 0.0), (100.0, 0.0), (200.0, 0.0), (300.0, 0.0)),)
    total_du = 300.0

    identity = _make_identity(
        entity_ids=entity_ids,
        services=(_pipe_size("SVP", 100),),
        status=IDENTITY_RESOLVED,
    )
    run = _make_run(entity_ids=entity_ids)
    labels = [
        _seg_label(50.0, 0.0, "SVP"),  # near segment 1 midpoint
        _seg_label(150.0, 0.0, "CDP"),  # near segment 2 midpoint
        # segment 3 midpoint (250,0) has no nearby label → orphan
    ]

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id={"e1": _line_geom(0.0, 0.0, total_du, 0.0)},
        rooms=[],
        scale=_confirmed_mm_scale(),
        measured_geometry_by_group={("Pipes", "idx150"): polylines},
        colour_differentiated=False,
        segment_labels=labels,
        segment_label_max_m=5.0,
    )

    total = sum(ln.drawing_length for ln in result.lines)
    assert abs(total - total_du) < 0.1, f"Conservation: {total} != {total_du}"

    by_svc = {ln.service: ln for ln in result.lines}
    assert "SVP" in by_svc
    assert "CDP" in by_svc
    assert SERVICE_UNKNOWN in by_svc  # third segment is orphan
    assert by_svc[SERVICE_UNKNOWN].drawing_length == pytest.approx(100.0, abs=0.1)


def test_bundle_mode_unchanged_with_colour_differentiated_true() -> None:
    """colour_differentiated=True (default) produces the same result as omitting the flag.

    Regression guard: the new flag must not change the bundle path for any existing caller.
    """
    entity_ids = ("e1",)
    run_length = 3000.0
    geometry = {"e1": _line_geom(0.0, 0.0, run_length, 0.0)}
    services = (_pipe_size("VAC", 54), _pipe_size("AGSS", 42))
    identity = _make_identity(entity_ids=entity_ids, services=services, status=IDENTITY_RESOLVED)
    run = _make_run(entity_ids=entity_ids)
    scale = _confirmed_mm_scale()

    result_default = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=scale,
    )
    result_explicit_true = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=scale,
        colour_differentiated=True,
    )

    assert result_default.lines == result_explicit_true.lines, (
        "colour_differentiated=True must be byte-identical to default (no flag)"
    )
    # Both services get full run length.
    by_svc = {ln.service: ln for ln in result_default.lines}
    assert by_svc["VAC"].drawing_length == run_length
    assert by_svc["AGSS"].drawing_length == run_length


def test_achromatic_no_geometry_falls_back_to_anchor() -> None:
    """Achromatic without measured_geometry falls back to anchor attribution (honest degradation).

    When measured_geometry_by_group is None or the key is absent, the apportionment falls
    back to the whole-length-to-anchor-room path.  Conservation still holds.
    """
    entity_ids = ("e1",)
    run_length = 500.0
    geometry = {"e1": _line_geom(0.0, 0.0, run_length, 0.0)}
    identity = _make_identity(
        entity_ids=entity_ids,
        services=(_pipe_size("SVP", 100),),
        status=IDENTITY_RESOLVED,
    )
    run = _make_run(entity_ids=entity_ids)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_unknown_scale(),
        measured_geometry_by_group=None,  # no geometry → anchor fallback
        colour_differentiated=False,
        segment_labels=[],
        segment_label_max_m=5.0,
    )

    # Anchor fallback: one line for SVP (from identity.services), whole length.
    total = sum(ln.drawing_length for ln in result.lines)
    assert abs(total - run_length) < 0.1, f"Fallback conservation: {total} != {run_length}"


def test_achromatic_no_geometry_multi_service_emits_once_to_unknown() -> None:
    """D3c Part B: a multi-service run with no geometry must emit Σ == drawn, NOT N x drawn.

    The bug: the no-geometry fallback iterated over all services and appended one row per
    service, inflating the total by N.  Fix: when >1 distinct service and no geometry, emit
    the full length once to SERVICE_UNKNOWN (honest — no per-segment evidence to split).
    """
    entity_ids = ("e1",)
    run_length = 600.0
    geometry = {"e1": _line_geom(0.0, 0.0, run_length, 0.0)}
    identity = _make_identity(
        entity_ids=entity_ids,
        services=(_pipe_size("VAC", 54), _pipe_size("MA", 42), _pipe_size("OXY", 42)),
        status=IDENTITY_RESOLVED,
    )
    run = _make_run(entity_ids=entity_ids)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_unknown_scale(),
        measured_geometry_by_group=None,  # no geometry → anchor fallback
        colour_differentiated=False,
        segment_labels=[],
        segment_label_max_m=5.0,
    )

    total = sum(ln.drawing_length for ln in result.lines)
    assert abs(total - run_length) < 0.1, (
        f"Multi-service no-geometry conservation: Σ={total} != drawn={run_length}"
    )
    # Must be attributed to SERVICE_UNKNOWN (no per-segment evidence)
    services_emitted = {ln.service for ln in result.lines}
    assert services_emitted == {SERVICE_UNKNOWN}, (
        f"Expected only SERVICE_UNKNOWN, got {services_emitted}"
    )


def test_achromatic_determinism_under_label_shuffle() -> None:
    """Achromatic mode is deterministic regardless of label list order."""
    import random as _random

    entity_ids = ("e1",)
    polylines = (((0.0, 0.0), (100.0, 0.0), (200.0, 0.0), (300.0, 0.0)),)
    identity = _make_identity(
        entity_ids=entity_ids,
        services=(_pipe_size("SVP", 100),),
        status=IDENTITY_RESOLVED,
    )
    run = _make_run(entity_ids=entity_ids)
    labels = [
        _seg_label(50.0, 0.0, "SVP"),
        _seg_label(150.0, 0.0, "CDP"),
        _seg_label(250.0, 0.0, "SVP"),
    ]
    geom = {"e1": _line_geom(0.0, 0.0, 300.0, 0.0)}
    scale = _confirmed_mm_scale()

    canonical = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geom,
        rooms=[],
        scale=scale,
        measured_geometry_by_group={("Pipes", "idx150"): polylines},
        colour_differentiated=False,
        segment_labels=labels,
        segment_label_max_m=5.0,
    )

    rng = _random.Random(99)
    for _ in range(8):
        shuffled = labels[:]
        rng.shuffle(shuffled)
        result = compute_service_takeoff(
            runs=[run],
            identities=[identity],
            geometry_by_entity_id=geom,
            rooms=[],
            scale=scale,
            measured_geometry_by_group={("Pipes", "idx150"): polylines},
            colour_differentiated=False,
            segment_labels=shuffled,
            segment_label_max_m=5.0,
        )
        assert result.lines == canonical.lines, (
            "Non-deterministic: shuffled labels produced different result"
        )


# ---------------------------------------------------------------------------
# #813 PR-2: per-run bundle-decision gate (bundle_service_sets)
# ---------------------------------------------------------------------------


def _polylines_for_key(
    length: float,
) -> tuple[tuple[tuple[float, float], ...], ...]:
    """Return a single-segment polyline of the given length along x-axis."""
    return (((0.0, 0.0), (length, 0.0)),)


def test_bundle_gate_drainage_per_segment_conserved() -> None:
    """Drainage shape: multi-service run NOT overlapping bundle_service_sets by >=2.

    One corridor, 2 services (SVP + CDP).  bundle_service_sets = frozenset({"MA","VAC","AGSS"})
    — no overlap with {SVP, CDP} by >=2.  The run must use per-segment apportionment,
    NOT be multiplied.  Total == geometry (NOT 2x).  Lines have bundle=False and
    bundle_evidence_absent=True.
    """
    run_length = 184.0
    entity_ids = ("e1",)
    geometry = {"e1": _line_geom(0.0, 0.0, run_length, 0.0)}
    polylines = _polylines_for_key(run_length)

    # Two labels: one per 92 du half-segment midpoint
    labels = [
        _seg_label(46.0, 0.0, "SVP"),  # near midpoint of first half
        _seg_label(138.0, 0.0, "CDP"),  # near midpoint of second half
    ]
    services = (_pipe_size("SVP", 100), _pipe_size("CDP", 100))
    identity = _make_identity(entity_ids=entity_ids, services=services, status=IDENTITY_RESOLVED)
    run = _make_run(entity_ids=entity_ids)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_confirmed_mm_scale(),
        measured_geometry_by_group={("Pipes", "idx150"): polylines},
        segment_labels=labels,
        segment_label_max_m=50.0,
        bundle_service_sets=(frozenset({"MA", "VAC", "AGSS"}),),
    )

    # Conservation: total == geometry, NOT 2x.
    total = sum(ln.drawing_length for ln in result.lines)
    assert abs(total - run_length) < 0.1, (
        f"Drainage conservation failed: total={total}, expected {run_length} (not {2 * run_length})"
    )

    # Lines must have bundle=False and bundle_evidence_absent=True.
    for ln in result.lines:
        if ln.service != SERVICE_UNKNOWN:
            assert ln.bundle is False, (
                f"{ln.service}: expected bundle=False on per-segment drainage run"
            )
            assert ln.bundle_evidence_absent is True, (
                f"{ln.service}: expected bundle_evidence_absent=True for multi-service "
                f"run without overlapping evidence"
            )


def test_bundle_gate_med_gas_confirmed_bundle_multiplied() -> None:
    """Med-gas shape: multi-service run overlapping bundle_service_sets by >=2 -> bundle=True.

    Run with services {MA, VAC, AGSS}; bundle_service_sets=(frozenset({"MA","VAC","AGSS"}),).
    All 3 services overlap the evidence set by 3 >= 2. Each service gets the FULL corridor length.
    bundle=True, bundle_evidence_absent=False on all lines.
    """
    run_length = 290.0
    entity_ids = ("e1",)
    geometry = {"e1": _line_geom(0.0, 0.0, run_length, 0.0)}
    polylines = _polylines_for_key(run_length)

    services = (_pipe_size("MA", 42), _pipe_size("VAC", 54), _pipe_size("AGSS", 42))
    identity = _make_identity(entity_ids=entity_ids, services=services, status=IDENTITY_RESOLVED)
    run = _make_run(entity_ids=entity_ids)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_confirmed_mm_scale(),
        measured_geometry_by_group={("Pipes", "idx150"): polylines},
        segment_labels=[],
        segment_label_max_m=5.0,
        bundle_service_sets=(frozenset({"MA", "VAC", "AGSS"}),),
    )

    assert len(result.lines) == 3, f"Expected 3 lines (MA, VAC, AGSS), got {len(result.lines)}"
    by_svc = {ln.service: ln for ln in result.lines}
    assert set(by_svc.keys()) == {"MA", "VAC", "AGSS"}

    # Each service gets the FULL run length.
    for svc, ln in by_svc.items():
        assert ln.drawing_length == run_length, (
            f"{svc}: expected drawing_length={run_length} (bundle), got {ln.drawing_length}"
        )
        assert ln.bundle is True, f"{svc}: expected bundle=True on confirmed bundle run"
        assert ln.bundle_evidence_absent is False, (
            f"{svc}: bundle_evidence_absent must be False on confirmed bundle"
        )


def test_bundle_gate_single_service_run_unchanged() -> None:
    """Single-service run with bundle_service_sets provided: per-segment, bundle=False,
    bundle_evidence_absent=False (it is not multi-service so no architect flag needed).

    A label is placed near the segment midpoint so the service is attributed (not unknown).
    """
    run_length = 150.0
    entity_ids = ("e1",)
    geometry = {"e1": _line_geom(0.0, 0.0, run_length, 0.0)}
    polylines = _polylines_for_key(run_length)
    # Label near the single segment midpoint (75,0) so VAC is attributed.
    labels = [_seg_label(75.0, 0.0, "VAC", size_raw="54")]

    services = (_pipe_size("VAC", 54),)
    identity = _make_identity(entity_ids=entity_ids, services=services, status=IDENTITY_RESOLVED)
    run = _make_run(entity_ids=entity_ids)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_confirmed_mm_scale(),
        measured_geometry_by_group={("Pipes", "idx150"): polylines},
        segment_labels=labels,
        segment_label_max_m=80.0,
        bundle_service_sets=(frozenset({"MA", "VAC", "AGSS"}),),
    )

    assert len(result.lines) == 1
    ln = result.lines[0]
    assert ln.service == "VAC"
    assert ln.drawing_length == pytest.approx(run_length, abs=0.1)
    assert ln.bundle is False
    # Not multi-service: bundle_evidence_absent must be False.
    assert ln.bundle_evidence_absent is False


def test_bundle_gate_legacy_path_byte_identical() -> None:
    """Legacy path (bundle_service_sets=None, colour_differentiated=True) is byte-identical
    to the existing behaviour. Regression guard for PR-2 back-compat constraint.
    """
    run_length = 3000.0
    entity_ids = ("e1",)
    geometry = {"e1": _line_geom(0.0, 0.0, run_length, 0.0)}
    services = (_pipe_size("VAC", 54), _pipe_size("AGSS", 42))
    identity = _make_identity(entity_ids=entity_ids, services=services, status=IDENTITY_RESOLVED)
    run = _make_run(entity_ids=entity_ids)
    scale = _confirmed_mm_scale()

    shared: dict[str, Any] = {
        "runs": [run],
        "identities": [identity],
        "geometry_by_entity_id": geometry,
        "rooms": [],
        "scale": scale,
    }

    # Existing default (no bundle_service_sets).
    result_legacy = compute_service_takeoff(**shared)
    # Explicit None — must be identical.
    result_explicit_none = compute_service_takeoff(**shared, bundle_service_sets=None)

    assert result_legacy.lines == result_explicit_none.lines, (
        "bundle_service_sets=None must be byte-identical to the default (legacy path)"
    )
    # Both lines are bundles (legacy behaviour).
    by_svc = {ln.service: ln for ln in result_legacy.lines}
    assert by_svc["VAC"].bundle is True
    assert by_svc["AGSS"].bundle is True
    # bundle_evidence_absent is always False on the legacy path.
    assert by_svc["VAC"].bundle_evidence_absent is False
    assert by_svc["AGSS"].bundle_evidence_absent is False


def test_bundle_gate_per_segment_conservation() -> None:
    """Per-segment path (new gate, non-bundle run): Sigma == geometry (±0.1 du).

    Three-segment polyline (300 du total); multi-service run {SVP, CDP} with no overlap
    against the provided bundle_service_sets. Labels near each segment midpoint ensure
    all segments are attributed. Total must equal geometry.
    """
    run_length = 300.0
    entity_ids = ("e1",)
    geometry = {"e1": _line_geom(0.0, 0.0, run_length, 0.0)}
    polylines = (((0.0, 0.0), (100.0, 0.0), (200.0, 0.0), (300.0, 0.0)),)

    labels = [
        _seg_label(50.0, 0.0, "SVP"),
        _seg_label(150.0, 0.0, "CDP"),
        _seg_label(250.0, 0.0, "SVP"),
    ]
    services = (_pipe_size("SVP", 100), _pipe_size("CDP", 100))
    identity = _make_identity(entity_ids=entity_ids, services=services, status=IDENTITY_RESOLVED)
    run = _make_run(entity_ids=entity_ids)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_confirmed_mm_scale(),
        measured_geometry_by_group={("Pipes", "idx150"): polylines},
        segment_labels=labels,
        segment_label_max_m=55.0,
        bundle_service_sets=(frozenset({"MA", "VAC", "AGSS"}),),
    )

    total = sum(ln.drawing_length for ln in result.lines)
    assert abs(total - run_length) < 0.1, (
        f"Conservation failed on per-segment path: Sigma={total}, expected {run_length}"
    )
    # Confirm all lines are per-segment, not bundle.
    for ln in result.lines:
        assert ln.bundle is False, f"{ln.service}: bundle must be False on per-segment path"


def test_bundle_gate_partial_overlap_not_enough() -> None:
    """A run with services {SVP, MA} overlaps bundle_set {"MA","VAC","AGSS"} by only 1
    (MA only). That is < 2, so it must NOT be confirmed as a bundle.
    bundle_evidence_absent=True on the resulting per-segment lines.
    """
    run_length = 200.0
    entity_ids = ("e1",)
    geometry = {"e1": _line_geom(0.0, 0.0, run_length, 0.0)}
    polylines = _polylines_for_key(run_length)

    labels = [
        _seg_label(50.0, 0.0, "SVP"),
        _seg_label(150.0, 0.0, "MA"),
    ]
    services = (_pipe_size("SVP", 100), _pipe_size("MA", 42))
    identity = _make_identity(entity_ids=entity_ids, services=services, status=IDENTITY_RESOLVED)
    run = _make_run(entity_ids=entity_ids)

    result = compute_service_takeoff(
        runs=[run],
        identities=[identity],
        geometry_by_entity_id=geometry,
        rooms=[],
        scale=_confirmed_mm_scale(),
        measured_geometry_by_group={("Pipes", "idx150"): polylines},
        segment_labels=labels,
        segment_label_max_m=55.0,
        bundle_service_sets=(frozenset({"MA", "VAC", "AGSS"}),),
    )

    # Per-segment: total conserved, no bundle.
    total = sum(ln.drawing_length for ln in result.lines)
    assert abs(total - run_length) < 0.1, (
        f"Partial-overlap conservation: Sigma={total}, expected {run_length}"
    )
    for ln in result.lines:
        assert ln.bundle is False
        assert ln.bundle_evidence_absent is True, (
            f"{ln.service}: expected bundle_evidence_absent=True for partial-overlap run"
        )


def test_bundle_evidence_absent_default_false_on_legacy_dataclass() -> None:
    """ServiceTakeoffLine.bundle_evidence_absent defaults to False (additive field).

    Construct a line directly without the new field to verify the default works,
    and confirm no existing code is broken by the new field.
    """
    # The field has a default so existing construction patterns stay valid.
    line = ServiceTakeoffLine(
        service="VAC",
        size_raw="54",
        size_kind="round",
        discipline="HYDRAULIC",
        room_id="room-1",
        room_name="Room 1",
        room_number="1.01",
        drawing_length=100.0,
        real_length_m=0.1,
        basis="real_world",
        units_confidence="confirmed",
        run_count=1,
        entity_ids=("e1",),
        identity_status="resolved",
        confidence=None,
        riser_count=0,
        drop_count=0,
        bundle=False,
    )
    assert line.bundle_evidence_absent is False
