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
from app.interpretation.rooms import Room
from app.interpretation.routed_runs import RunGroup
from app.interpretation.run_service_identity import (
    IDENTITY_PARTIAL,
    IDENTITY_RESOLVED,
    IDENTITY_UNKNOWN,
    RunServiceIdentity,
    ServiceSize,
)
from app.interpretation.run_tags import BASIS_TAG_TEXT, PipeSize
from app.interpretation.service_takeoff import (
    ROOM_UNASSIGNED_ID,
    SERVICE_UNKNOWN,
    ServiceTakeoffResult,
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
