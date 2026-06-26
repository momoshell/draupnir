"""Pure (non-DB) tests for floor_estimated.bucket_estimated_cable (issue #720, Phase R-F).

All inputs are hand-crafted in-memory objects — no database, no mocks.

Coverage targets:
- device_drop → device's room (first-claim attribution).
- home_run → anchor device's room (ok status only).
- Non-ok home_run status (no_anchor, disconnected_tray, etc.) → contributes nothing.
- Anchor with no room assignment → unassigned home_run bucket.
- in_plan_length_m → floor-level only (EstimatedCircuitContribution), not per-room.
- Per-circuit replay guard + floor conservation: corrupt input → RuntimeError.
- Distinct-device first-claim: device in 2 circuits counted once.
- Never summed with MEASURED (quantity_kind check).
- Determinism under shuffle of device_footprints and circuits.
- Devices in no circuit → unassigned bucket via unattributed path.
"""

from __future__ import annotations

import math
import random

import pytest

from app.interpretation.cable_circuits import CableCircuit, CableCircuitSet
from app.interpretation.cable_estimate_assembly import CableAssembly, CircuitCableEstimate
from app.interpretation.cable_estimate_params import (
    CATEGORY_LUMINAIRE,
    CATEGORY_SWITCH,
    CableEstimateParams,
    DropParam,
)
from app.interpretation.cable_topology import DeviceFootprint
from app.interpretation.floor_counted import CountedDeviceAssignment
from app.interpretation.floor_estimated import (
    FusedEstimatedResult,
    bucket_estimated_cable,
)

# ---------------------------------------------------------------------------
# Test-fixture helpers
# ---------------------------------------------------------------------------

_LUMINAIRE_BLOCK = "Pr_Luminaire Standard"
_SWITCH_BLOCK = "Switch Plate"
_PANEL_BLOCK = "Distribution Board DB-1"


def _params_simple() -> CableEstimateParams:
    """Params with luminaire=2.0, switch=1.2, distribution_board=0.0, spare=0.0."""
    drops = (
        DropParam(category=CATEGORY_LUMINAIRE, drop_m=2.0, source="test", basis="test"),
        DropParam(category=CATEGORY_SWITCH, drop_m=1.2, source="test", basis="test"),
        DropParam(category="distribution_board", drop_m=0.0, source="test", basis="test"),
    )
    return CableEstimateParams(
        vertical_drops=tuple(sorted(drops, key=lambda d: d.category)),
        spare_fraction=0.0,
        spare_source="test",
        spare_basis="test",
    )


def _footprint(entity_id: str, block_ref: str | None) -> DeviceFootprint:
    return DeviceFootprint(
        entity_id=entity_id,
        block_ref=block_ref,
        kind="device",
        position=None,
        bbox=None,
    )


def _assignment(
    device_id: str,
    *,
    room_number: str | None,
    room_id: str | None = None,
    boundary_basis: str | None = "polygon",
    confidence: float | None = 1.0,
    needs_review: bool = False,
) -> CountedDeviceAssignment:
    return CountedDeviceAssignment(
        device_id=device_id,
        type_name="SD",
        source_revision_id="rev-a",
        room_number=room_number,
        room_id=room_id,
        boundary_basis=boundary_basis,
        confidence=confidence,
        needs_review=needs_review,
    )


def _circuit(
    circuit_id: int,
    device_entity_ids: tuple[str, ...],
    spline_entity_ids: tuple[str, ...] = (),
    anchor_device_entity_id: str | None = None,
) -> CableCircuit:
    return CableCircuit(
        circuit_id=circuit_id,
        node_ids=(),
        spline_entity_ids=spline_entity_ids,
        device_entity_ids=device_entity_ids,
        anchor_device_entity_id=anchor_device_entity_id,
        edge_count=len(spline_entity_ids),
        node_count=0,
        device_count=len(device_entity_ids),
    )


def _circuit_set(*circuits: CableCircuit) -> CableCircuitSet:
    edge_count = sum(c.edge_count for c in circuits)
    all_devices: set[str] = set()
    for c in circuits:
        all_devices.update(c.device_entity_ids)
    return CableCircuitSet(
        circuits=tuple(circuits),
        circuit_count=len(circuits),
        edge_count=edge_count,
        assigned_edge_count=edge_count,
        device_count=len(all_devices),
    )


def _circuit_ce(
    circuit_id: int,
    *,
    device_drop_m: float,
    in_plan_length_m: float,
    home_run_m: float | None = None,
    home_run_status: str = "no_containment_sheet",
) -> CircuitCableEstimate:
    base = device_drop_m + in_plan_length_m + (home_run_m or 0.0)
    return CircuitCableEstimate(
        circuit_id=circuit_id,
        device_drop_m=device_drop_m,
        in_plan_length_m=in_plan_length_m,
        home_run_m=home_run_m,
        home_run_status=home_run_status,
        base_total_m=base,
        total_with_spare_m=base,
    )


def _assembly(
    per_circuit: tuple[CircuitCableEstimate, ...],
    *,
    unattributed_device_drop_m: float = 0.0,
    params: CableEstimateParams | None = None,
    home_run_suppressed_counts: dict[str, int] | None = None,
) -> CableAssembly:
    if params is None:
        params = _params_simple()
    total_drop = sum(c.device_drop_m for c in per_circuit) + unattributed_device_drop_m
    total_in_plan = sum(c.in_plan_length_m for c in per_circuit)
    total_hr = sum(c.home_run_m for c in per_circuit if c.home_run_m is not None)
    grand_base = sum(c.base_total_m for c in per_circuit) + unattributed_device_drop_m
    suppressed = home_run_suppressed_counts or {
        "no_anchor": 0,
        "unreachable_tray": 0,
        "disconnected_tray": 0,
        "bad_registration": 0,
        "no_containment_sheet": len(per_circuit),
    }
    return CableAssembly(
        per_circuit=per_circuit,
        total_device_drop_m=total_drop,
        total_in_plan_length_m=total_in_plan,
        total_home_run_m=total_hr,
        unattributed_device_drop_m=unattributed_device_drop_m,
        grand_base_m=grand_base,
        grand_with_spare_m=grand_base,
        spare_fraction=params.spare_fraction,
        quantity_kind="estimated",
        reliability={
            "device_drop_m": "reliable",
            "in_plan_length_m": "schematic_provisional",
            "home_run_m": "estimated_routed",
            "combined": "mixed_reliability",
        },
        params_stamp=params.as_stamp(),
        home_run_suppressed_counts=suppressed,
    )


# ---------------------------------------------------------------------------
# Tests: device_drop → device's room
# ---------------------------------------------------------------------------


def test_device_drop_attributed_to_device_room() -> None:
    """A luminaire in room '101' gets its 2.0 m drop attributed to '101'."""
    params = _params_simple()
    fp = _footprint("d1", _LUMINAIRE_BLOCK)
    circuit = _circuit(0, ("d1",))
    circuits = _circuit_set(circuit)
    ce = _circuit_ce(0, device_drop_m=2.0, in_plan_length_m=0.0)
    asm = _assembly((ce,), params=params)
    drm = {"d1": _assignment("d1", room_number="101", room_id="r1")}
    src: dict[str, str | None] = {"lighting_revision_id": "rev-a", "containment_revision_id": None}

    result = bucket_estimated_cable(
        assembly=asm,
        circuits=circuits,
        device_footprints=[fp],
        params=params,
        device_room_map=drm,
        source=src,
    )

    assert len(result.per_room) == 1
    room = result.per_room[0]
    assert room.room_number == "101"
    assert room.room_id == "r1"
    assert math.isclose(room.device_drop_m, 2.0, abs_tol=1e-9)
    assert math.isclose(result.unassigned.device_drop_m, 0.0, abs_tol=1e-9)


def test_device_drop_unassigned_when_no_room_map_entry() -> None:
    """A device with no room assignment goes to the unassigned bucket."""
    params = _params_simple()
    fp = _footprint("d1", _LUMINAIRE_BLOCK)
    circuit = _circuit(0, ("d1",))
    circuits = _circuit_set(circuit)
    ce = _circuit_ce(0, device_drop_m=2.0, in_plan_length_m=0.0)
    asm = _assembly((ce,), params=params)
    # Empty device_room_map — device has no room.
    src: dict[str, str | None] = {"lighting_revision_id": "rev-a", "containment_revision_id": None}

    result = bucket_estimated_cable(
        assembly=asm,
        circuits=circuits,
        device_footprints=[fp],
        params=params,
        device_room_map={},
        source=src,
    )

    assert len(result.per_room) == 0
    assert math.isclose(result.unassigned.device_drop_m, 2.0, abs_tol=1e-9)


# ---------------------------------------------------------------------------
# Tests: home_run → anchor device's room
# ---------------------------------------------------------------------------


def test_home_run_attributed_to_anchor_room() -> None:
    """ok home_run is attributed to the anchor device's room."""
    params = _params_simple()
    fp_anchor = _footprint("panel", _PANEL_BLOCK)
    fp_lum = _footprint("lum", _LUMINAIRE_BLOCK)
    circuit = _circuit(0, ("lum", "panel"), anchor_device_entity_id="panel")
    circuits = _circuit_set(circuit)
    # Anchor drop = 0.0 (distribution_board), luminaire = 2.0.
    ce = _circuit_ce(
        0, device_drop_m=2.0, in_plan_length_m=5.0, home_run_m=10.0, home_run_status="ok"
    )
    suppressed = {
        "no_anchor": 0,
        "unreachable_tray": 0,
        "disconnected_tray": 0,
        "bad_registration": 0,
        "no_containment_sheet": 0,
    }
    asm = _assembly((ce,), params=params, home_run_suppressed_counts=suppressed)
    drm = {
        "panel": _assignment("panel", room_number="102", room_id="r2"),
        "lum": _assignment("lum", room_number="101", room_id="r1"),
    }
    src: dict[str, str | None] = {
        "lighting_revision_id": "rev-a",
        "containment_revision_id": "rev-b",
    }

    result = bucket_estimated_cable(
        assembly=asm,
        circuits=circuits,
        device_footprints=[fp_anchor, fp_lum],
        params=params,
        device_room_map=drm,
        source=src,
    )

    # Anchor is in room '102' → home_run_m 10.0 goes there.
    rooms = {r.room_number: r for r in result.per_room}
    assert math.isclose(rooms["102"].home_run_m, 10.0, abs_tol=1e-9)
    # Luminaire in room '101' → 2.0 m drop.
    assert math.isclose(rooms["101"].device_drop_m, 2.0, abs_tol=1e-9)
    assert math.isclose(result.unassigned.home_run_m, 0.0, abs_tol=1e-9)


def test_home_run_non_ok_status_contributes_nothing() -> None:
    """Non-ok home_run statuses contribute 0 to every bucket (honest absence)."""
    params = _params_simple()
    fp = _footprint("d1", _LUMINAIRE_BLOCK)
    circuit = _circuit(0, ("d1",), anchor_device_entity_id="d1")
    circuits = _circuit_set(circuit)

    for bad_status in (
        "no_anchor",
        "unreachable_tray",
        "disconnected_tray",
        "bad_registration",
        "no_containment_sheet",
    ):
        ce = _circuit_ce(
            0,
            device_drop_m=2.0,
            in_plan_length_m=0.0,
            home_run_m=None,
            home_run_status=bad_status,
        )
        suppressed: dict[str, int] = {
            "no_anchor": 0,
            "unreachable_tray": 0,
            "disconnected_tray": 0,
            "bad_registration": 0,
            "no_containment_sheet": 0,
        }
        suppressed[bad_status] = 1
        asm = _assembly((ce,), params=params, home_run_suppressed_counts=suppressed)
        drm = {"d1": _assignment("d1", room_number="101", room_id="r1")}
        src: dict[str, str | None] = {
            "lighting_revision_id": "rev-a",
            "containment_revision_id": None,
        }

        result = bucket_estimated_cable(
            assembly=asm,
            circuits=circuits,
            device_footprints=[fp],
            params=params,
            device_room_map=drm,
            source=src,
        )

        total_hr = sum(r.home_run_m for r in result.per_room) + result.unassigned.home_run_m
        assert math.isclose(total_hr, 0.0, abs_tol=1e-9), (
            f"status={bad_status!r} left {total_hr!r} home_run"
        )


def test_anchor_unassigned_home_run_goes_to_unassigned_bucket() -> None:
    """An ok home_run whose anchor has no room assignment goes to the unassigned bucket."""
    params = _params_simple()
    fp = _footprint("panel", _PANEL_BLOCK)
    circuit = _circuit(0, ("panel",), anchor_device_entity_id="panel")
    circuits = _circuit_set(circuit)
    ce = _circuit_ce(
        0, device_drop_m=0.0, in_plan_length_m=0.0, home_run_m=8.0, home_run_status="ok"
    )
    suppressed = {
        "no_anchor": 0,
        "unreachable_tray": 0,
        "disconnected_tray": 0,
        "bad_registration": 0,
        "no_containment_sheet": 0,
    }
    asm = _assembly((ce,), params=params, home_run_suppressed_counts=suppressed)
    # Anchor has no room assignment.
    src: dict[str, str | None] = {
        "lighting_revision_id": "rev-a",
        "containment_revision_id": None,
    }

    result = bucket_estimated_cable(
        assembly=asm,
        circuits=circuits,
        device_footprints=[fp],
        params=params,
        device_room_map={},
        source=src,
    )

    assert len(result.per_room) == 0
    assert math.isclose(result.unassigned.home_run_m, 8.0, abs_tol=1e-9)


# ---------------------------------------------------------------------------
# Tests: in_plan → floor-level only
# ---------------------------------------------------------------------------


def test_in_plan_floor_level_only() -> None:
    """in_plan_length_m lives on EstimatedCircuitContribution, NOT on rooms."""
    params = _params_simple()
    fp = _footprint("d1", _LUMINAIRE_BLOCK)
    circuit = _circuit(0, ("d1",))
    circuits = _circuit_set(circuit)
    ce = _circuit_ce(0, device_drop_m=2.0, in_plan_length_m=50.0)
    asm = _assembly((ce,), params=params)
    drm = {"d1": _assignment("d1", room_number="101")}
    src: dict[str, str | None] = {"lighting_revision_id": "rev-a", "containment_revision_id": None}

    result = bucket_estimated_cable(
        assembly=asm,
        circuits=circuits,
        device_footprints=[fp],
        params=params,
        device_room_map=drm,
        source=src,
    )

    # RoomEstimatedContribution has NO in_plan_length_m field.
    for r in result.per_room:
        assert not hasattr(r, "in_plan_length_m"), "per-room should not have in_plan_length_m"

    # EstimatedCircuitContribution carries the in-plan term.
    assert len(result.estimated_circuits) == 1
    assert math.isclose(result.estimated_circuits[0].in_plan_length_m, 50.0, abs_tol=1e-9)


def test_rooms_touched_in_circuit_contribution() -> None:
    """rooms_touched lists the room_numbers of the circuit's devices."""
    params = _params_simple()
    fp1 = _footprint("d1", _LUMINAIRE_BLOCK)
    fp2 = _footprint("d2", _SWITCH_BLOCK)
    circuit = _circuit(0, ("d1", "d2"))
    circuits = _circuit_set(circuit)
    ce = _circuit_ce(0, device_drop_m=3.2, in_plan_length_m=10.0)
    asm = _assembly((ce,), params=params)
    drm = {
        "d1": _assignment("d1", room_number="101"),
        "d2": _assignment("d2", room_number="102"),
    }
    src: dict[str, str | None] = {"lighting_revision_id": "rev-a", "containment_revision_id": None}

    result = bucket_estimated_cable(
        assembly=asm,
        circuits=circuits,
        device_footprints=[fp1, fp2],
        params=params,
        device_room_map=drm,
        source=src,
    )

    ec = result.estimated_circuits[0]
    assert set(ec.rooms_touched) == {"101", "102"}


# ---------------------------------------------------------------------------
# Tests: first-claim (device in 2 circuits counted once)
# ---------------------------------------------------------------------------


def test_first_claim_device_in_two_circuits_counted_once() -> None:
    """A device appearing in two circuits is claimed by the lower circuit_id only."""
    params = _params_simple()
    # "d1" appears in both circuit 0 and circuit 1.
    fp = _footprint("d1", _LUMINAIRE_BLOCK)
    c0 = _circuit(0, ("d1",))
    c1 = _circuit(1, ("d1",))
    circuits = _circuit_set(c0, c1)
    # circuit 0 claims d1 (2.0 m drop), circuit 1 gets 0 from d1.
    ce0 = _circuit_ce(0, device_drop_m=2.0, in_plan_length_m=0.0)
    ce1 = _circuit_ce(1, device_drop_m=0.0, in_plan_length_m=0.0)
    asm = _assembly((ce0, ce1), params=params)
    drm = {"d1": _assignment("d1", room_number="101")}
    src: dict[str, str | None] = {"lighting_revision_id": "rev-a", "containment_revision_id": None}

    result = bucket_estimated_cable(
        assembly=asm,
        circuits=circuits,
        device_footprints=[fp],
        params=params,
        device_room_map=drm,
        source=src,
    )

    # Total drop must equal exactly one luminaire drop (2.0 m).
    total_drop = sum(r.device_drop_m for r in result.per_room) + result.unassigned.device_drop_m
    assert math.isclose(total_drop, 2.0, abs_tol=1e-9)


# ---------------------------------------------------------------------------
# Tests: floor-level conservation
# ---------------------------------------------------------------------------


def test_floor_conservation_holds_multi_room() -> None:
    """Σ per_room + unassigned == assembly totals for drop, home_run, in_plan."""
    params = _params_simple()
    fp1 = _footprint("d1", _LUMINAIRE_BLOCK)
    fp2 = _footprint("d2", _LUMINAIRE_BLOCK)
    fp3 = _footprint("panel", _PANEL_BLOCK)
    c0 = _circuit(0, ("d1", "panel"), anchor_device_entity_id="panel")
    c1 = _circuit(1, ("d2",))
    circuits = _circuit_set(c0, c1)
    # Both luminaires = 2.0 m; panel = 0.0; home_run 5.0 on circuit 0.
    ce0 = _circuit_ce(
        0, device_drop_m=2.0, in_plan_length_m=20.0, home_run_m=5.0, home_run_status="ok"
    )
    ce1 = _circuit_ce(1, device_drop_m=2.0, in_plan_length_m=15.0)
    suppressed = {
        "no_anchor": 0,
        "unreachable_tray": 0,
        "disconnected_tray": 0,
        "bad_registration": 0,
        "no_containment_sheet": 1,
    }
    asm = _assembly((ce0, ce1), params=params, home_run_suppressed_counts=suppressed)
    drm = {
        "d1": _assignment("d1", room_number="101"),
        "d2": _assignment("d2", room_number="102"),
        "panel": _assignment("panel", room_number="101"),
    }
    src: dict[str, str | None] = {
        "lighting_revision_id": "rev-a",
        "containment_revision_id": None,
    }

    result = bucket_estimated_cable(
        assembly=asm,
        circuits=circuits,
        device_footprints=[fp1, fp2, fp3],
        params=params,
        device_room_map=drm,
        source=src,
    )

    total_drop = sum(r.device_drop_m for r in result.per_room) + result.unassigned.device_drop_m
    total_hr = sum(r.home_run_m for r in result.per_room) + result.unassigned.home_run_m
    total_ip = sum(c.in_plan_length_m for c in result.estimated_circuits)

    assert math.isclose(total_drop, asm.total_device_drop_m, abs_tol=1e-6)
    assert math.isclose(total_hr, asm.total_home_run_m, abs_tol=1e-6)
    assert math.isclose(total_ip, asm.total_in_plan_length_m, abs_tol=1e-6)


# ---------------------------------------------------------------------------
# Tests: replay guard fires RuntimeError on corrupt input
# ---------------------------------------------------------------------------


def test_replay_guard_fires_on_corrupt_assembly_drop() -> None:
    """Corrupt assembly device_drop_m → replay guard raises RuntimeError."""
    params = _params_simple()
    fp = _footprint("d1", _LUMINAIRE_BLOCK)
    circuit = _circuit(0, ("d1",))
    circuits = _circuit_set(circuit)
    # Claim 3.0 m drop but real drop for a luminaire is 2.0 m → mismatch.
    ce = _circuit_ce(0, device_drop_m=3.0, in_plan_length_m=0.0)
    asm = _assembly((ce,), params=params)
    drm = {"d1": _assignment("d1", room_number="101")}
    src: dict[str, str | None] = {"lighting_revision_id": "rev-a", "containment_revision_id": None}

    with pytest.raises(RuntimeError, match="replay guard"):
        bucket_estimated_cable(
            assembly=asm,
            circuits=circuits,
            device_footprints=[fp],
            params=params,
            device_room_map=drm,
            source=src,
        )


def test_replay_guard_fires_when_circuit_absent_from_assembly() -> None:
    """Circuit present in circuits but absent from assembly.per_circuit → RuntimeError.

    This guards against mismatched partitions: if circuits and assembly come from
    different partition runs, circuit_ids may not align. The guard must fire
    immediately (not silently zero the in-plan term).
    """
    params = _params_simple()
    fp = _footprint("d1", _LUMINAIRE_BLOCK)
    # circuits has circuit_id=0 and circuit_id=1.
    c0 = _circuit(0, ("d1",))
    c1 = _circuit(1, ())
    circuits = _circuit_set(c0, c1)
    # assembly only has circuit_id=0 — circuit 1 is missing.
    ce0 = _circuit_ce(0, device_drop_m=2.0, in_plan_length_m=0.0)
    asm = _assembly((ce0,), params=params)
    drm = {"d1": _assignment("d1", room_number="101")}
    src: dict[str, str | None] = {"lighting_revision_id": "rev-a", "containment_revision_id": None}

    with pytest.raises(RuntimeError, match=r"not in assembly\.per_circuit"):
        bucket_estimated_cable(
            assembly=asm,
            circuits=circuits,
            device_footprints=[fp],
            params=params,
            device_room_map=drm,
            source=src,
        )


def test_conservation_fires_on_corrupt_total_device_drop() -> None:
    """A CableAssembly whose total_device_drop_m doesn't add up → RuntimeError."""
    params = _params_simple()
    fp = _footprint("d1", _LUMINAIRE_BLOCK)
    circuit = _circuit(0, ("d1",))
    circuits = _circuit_set(circuit)
    # Correctly replayed per-circuit (2.0), but assembly total is wrong (99.0).
    ce = _circuit_ce(0, device_drop_m=2.0, in_plan_length_m=5.0)
    asm_ok = _assembly((ce,), params=params)
    # Forge a corrupt assembly with wrong total.
    asm_bad = CableAssembly(
        per_circuit=asm_ok.per_circuit,
        total_device_drop_m=99.0,  # wrong
        total_in_plan_length_m=asm_ok.total_in_plan_length_m,
        total_home_run_m=asm_ok.total_home_run_m,
        unattributed_device_drop_m=asm_ok.unattributed_device_drop_m,
        grand_base_m=asm_ok.grand_base_m,
        grand_with_spare_m=asm_ok.grand_with_spare_m,
        spare_fraction=asm_ok.spare_fraction,
        quantity_kind=asm_ok.quantity_kind,
        reliability=asm_ok.reliability,
        params_stamp=asm_ok.params_stamp,
        home_run_suppressed_counts=asm_ok.home_run_suppressed_counts,
    )
    drm = {"d1": _assignment("d1", room_number="101")}
    src: dict[str, str | None] = {"lighting_revision_id": "rev-a", "containment_revision_id": None}

    with pytest.raises(RuntimeError, match="conservation"):
        bucket_estimated_cable(
            assembly=asm_bad,
            circuits=circuits,
            device_footprints=[fp],
            params=params,
            device_room_map=drm,
            source=src,
        )


# ---------------------------------------------------------------------------
# Tests: quantity_kind — never summed with MEASURED
# ---------------------------------------------------------------------------


def test_quantity_kind_is_estimated() -> None:
    """quantity_kind is always 'estimated'; must not equal 'measured'."""
    params = _params_simple()
    fp = _footprint("d1", _LUMINAIRE_BLOCK)
    circuit = _circuit(0, ("d1",))
    circuits = _circuit_set(circuit)
    ce = _circuit_ce(0, device_drop_m=2.0, in_plan_length_m=0.0)
    asm = _assembly((ce,), params=params)
    drm = {"d1": _assignment("d1", room_number="101")}
    src: dict[str, str | None] = {"lighting_revision_id": "rev-a", "containment_revision_id": None}

    result = bucket_estimated_cable(
        assembly=asm,
        circuits=circuits,
        device_footprints=[fp],
        params=params,
        device_room_map=drm,
        source=src,
    )

    assert result.quantity_kind == "estimated"
    assert result.quantity_kind != "measured"


# ---------------------------------------------------------------------------
# Tests: devices in no circuit → unassigned bucket
# ---------------------------------------------------------------------------


def test_device_in_no_circuit_goes_to_unassigned() -> None:
    """A device that appears in no circuit goes to the unassigned drop bucket."""
    params = _params_simple()
    # d1 is in circuit 0; d2 is NOT in any circuit.
    fp1 = _footprint("d1", _LUMINAIRE_BLOCK)
    fp2 = _footprint("d2", _LUMINAIRE_BLOCK)
    circuit = _circuit(0, ("d1",))
    circuits = _circuit_set(circuit)
    ce = _circuit_ce(0, device_drop_m=2.0, in_plan_length_m=0.0)
    # Unattributed device drop = 2.0 (d2 is a luminaire, drop 2.0 m).
    asm = _assembly((ce,), unattributed_device_drop_m=2.0, params=params)
    drm = {
        "d1": _assignment("d1", room_number="101"),
        "d2": _assignment("d2", room_number="102"),
    }
    src: dict[str, str | None] = {"lighting_revision_id": "rev-a", "containment_revision_id": None}

    result = bucket_estimated_cable(
        assembly=asm,
        circuits=circuits,
        device_footprints=[fp1, fp2],
        params=params,
        device_room_map=drm,
        source=src,
    )

    # d2 is unattributed (in no circuit) → its drop goes to unassigned.
    assert math.isclose(result.unassigned.device_drop_m, 2.0, abs_tol=1e-9)
    # Conservation must hold.
    total_drop = sum(r.device_drop_m for r in result.per_room) + result.unassigned.device_drop_m
    assert math.isclose(total_drop, asm.total_device_drop_m, abs_tol=1e-6)


# ---------------------------------------------------------------------------
# Tests: determinism under shuffle
# ---------------------------------------------------------------------------


def test_determinism_under_shuffle() -> None:
    """Result is identical regardless of device_footprints list ordering."""
    params = _params_simple()
    fps = [
        _footprint("d1", _LUMINAIRE_BLOCK),
        _footprint("d2", _LUMINAIRE_BLOCK),
        _footprint("d3", _SWITCH_BLOCK),
    ]
    c0 = _circuit(0, ("d1", "d3"))
    c1 = _circuit(1, ("d2",))
    circuits = _circuit_set(c0, c1)
    ce0 = _circuit_ce(0, device_drop_m=3.2, in_plan_length_m=10.0)
    ce1 = _circuit_ce(1, device_drop_m=2.0, in_plan_length_m=5.0)
    asm = _assembly((ce0, ce1), params=params)
    drm = {
        "d1": _assignment("d1", room_number="101"),
        "d2": _assignment("d2", room_number="102"),
        "d3": _assignment("d3", room_number="101"),
    }
    src: dict[str, str | None] = {"lighting_revision_id": "rev-a", "containment_revision_id": None}

    def _run(fp_list: list[DeviceFootprint]) -> FusedEstimatedResult:
        return bucket_estimated_cable(
            assembly=asm,
            circuits=circuits,
            device_footprints=fp_list,
            params=params,
            device_room_map=drm,
            source=src,
        )

    base = _run(fps)
    rng = random.Random(42)
    for _ in range(5):
        shuffled = fps[:]
        rng.shuffle(shuffled)
        shuffled_result = _run(shuffled)
        assert shuffled_result.per_room == base.per_room
        assert shuffled_result.unassigned == base.unassigned
        assert shuffled_result.estimated_circuits == base.estimated_circuits


# ---------------------------------------------------------------------------
# Tests: per_room sorted by room_number; unassigned bucket always present
# ---------------------------------------------------------------------------


def test_per_room_sorted_by_room_number() -> None:
    """per_room is sorted by room_number ascending."""
    params = _params_simple()
    fp1 = _footprint("d1", _LUMINAIRE_BLOCK)
    fp2 = _footprint("d2", _LUMINAIRE_BLOCK)
    c0 = _circuit(0, ("d1",))
    c1 = _circuit(1, ("d2",))
    circuits = _circuit_set(c0, c1)
    ce0 = _circuit_ce(0, device_drop_m=2.0, in_plan_length_m=0.0)
    ce1 = _circuit_ce(1, device_drop_m=2.0, in_plan_length_m=0.0)
    asm = _assembly((ce0, ce1), params=params)
    drm = {
        "d1": _assignment("d1", room_number="201"),
        "d2": _assignment("d2", room_number="102"),
    }
    src: dict[str, str | None] = {"lighting_revision_id": "rev-a", "containment_revision_id": None}

    result = bucket_estimated_cable(
        assembly=asm,
        circuits=circuits,
        device_footprints=[fp1, fp2],
        params=params,
        device_room_map=drm,
        source=src,
    )

    room_numbers = [r.room_number for r in result.per_room]
    assert room_numbers == sorted(room_numbers, key=lambda r: (r is None, r or ""))


def test_unassigned_bucket_always_present_even_when_zero() -> None:
    """Unassigned bucket exists with zero drops when every device has a room."""
    params = _params_simple()
    fp = _footprint("d1", _LUMINAIRE_BLOCK)
    circuit = _circuit(0, ("d1",))
    circuits = _circuit_set(circuit)
    ce = _circuit_ce(0, device_drop_m=2.0, in_plan_length_m=0.0)
    asm = _assembly((ce,), params=params)
    drm = {"d1": _assignment("d1", room_number="101")}
    src: dict[str, str | None] = {"lighting_revision_id": "rev-a", "containment_revision_id": None}

    result = bucket_estimated_cable(
        assembly=asm,
        circuits=circuits,
        device_footprints=[fp],
        params=params,
        device_room_map=drm,
        source=src,
    )

    # unassigned bucket must always exist.
    assert result.unassigned is not None
    assert result.unassigned.room_number is None
    assert math.isclose(result.unassigned.device_drop_m, 0.0, abs_tol=1e-9)
    assert math.isclose(result.unassigned.home_run_m, 0.0, abs_tol=1e-9)


# ---------------------------------------------------------------------------
# Tests: source + reliability + params_stamp forwarded from assembly
# ---------------------------------------------------------------------------


def test_result_carries_assembly_metadata() -> None:
    """reliability, params_stamp, quantity_kind, source are forwarded from assembly."""
    params = _params_simple()
    fp = _footprint("d1", _LUMINAIRE_BLOCK)
    circuit = _circuit(0, ("d1",))
    circuits = _circuit_set(circuit)
    ce = _circuit_ce(0, device_drop_m=2.0, in_plan_length_m=0.0)
    asm = _assembly((ce,), params=params)
    drm = {"d1": _assignment("d1", room_number="101")}
    src: dict[str, str | None] = {
        "lighting_revision_id": "rev-lights",
        "containment_revision_id": "rev-containment",
    }

    result = bucket_estimated_cable(
        assembly=asm,
        circuits=circuits,
        device_footprints=[fp],
        params=params,
        device_room_map=drm,
        source=src,
    )

    assert result.quantity_kind == "estimated"
    assert result.reliability == asm.reliability
    assert result.params_stamp == asm.params_stamp
    assert result.source == src


# ---------------------------------------------------------------------------
# Tests: empty floor (no devices, no circuits)
# ---------------------------------------------------------------------------


def test_empty_floor_returns_valid_result() -> None:
    """An empty floor (no devices, no circuits) produces a valid zeroed result."""
    params = _params_simple()
    circuits = _circuit_set()
    asm = _assembly((), params=params)
    src: dict[str, str | None] = {"lighting_revision_id": "rev-a", "containment_revision_id": None}

    result = bucket_estimated_cable(
        assembly=asm,
        circuits=circuits,
        device_footprints=[],
        params=params,
        device_room_map={},
        source=src,
    )

    assert result.per_room == ()
    assert result.estimated_circuits == ()
    assert result.unassigned.device_drop_m == 0.0
    assert result.unassigned.home_run_m == 0.0
