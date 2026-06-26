"""Per-room ESTIMATED (cable) contribution for fused floor takeoff (issue #720, Phase R-F).

Pure module — stdlib + app.interpretation.{cable_estimate, cable_estimate_assembly,
cable_circuits, cable_topology, cable_estimate_params, floor_counted} ONLY.
No DB, ORM, FastAPI, cv2, skimage, or shapely imports.

Algorithm
---------
``bucket_estimated_cable`` distributes a ``CableAssembly`` across rooms by replaying
the cable-estimate's first-claim per-device-drop logic:

1. **Device drops** (``device_drop_m``): For each circuit (ascending circuit_id),
   replay the first-claim logic over its ``device_entity_ids``.  Each device is claimed
   at most once.  The claimed drop is attributed to the device's room via the
   ``device_room_map`` (from R-E's ``CountedFusionResult.by_device_id()``).
   Unclaimed devices in any circuit (already claimed by an earlier circuit) are skipped.
   Devices in no circuit go to the unassigned bucket.

2. **Home runs** (``home_run_m``): For each circuit with ``home_run_status == "ok"``,
   the home run is attributed to the anchor device's room.  Non-"ok" status → contributes
   nothing (honest absence, no fabricated bucket).  Circuits with no anchor room assignment
   → unassigned home_run bucket.

3. **In-plan lengths** (``in_plan_length_m``): NOT per-room; one
   ``EstimatedCircuitContribution`` per circuit at floor level, with ``rooms_touched``
   (context only — the set of room_numbers touched by the circuit's devices).

Conservation invariants (RuntimeError, fires under -O):
   Σ per_room.device_drop_m + unassigned.device_drop_m == assembly.total_device_drop_m
   Σ per_room.home_run_m   + unassigned.home_run_m    == assembly.total_home_run_m
   Σ estimated_circuits.in_plan_length_m               == assembly.total_in_plan_length_m

Replay guard (per circuit, RuntimeError):
   Σ(per-device drops first-claimed by circuit C) == assembly.per_circuit[C].device_drop_m

``quantity_kind`` is always ``"estimated"`` — NEVER summed with MEASURED.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from app.interpretation.cable_circuits import CableCircuit, CableCircuitSet
from app.interpretation.cable_estimate import categorize_device
from app.interpretation.cable_estimate_assembly import CableAssembly
from app.interpretation.cable_estimate_params import CableEstimateParams
from app.interpretation.cable_topology import DeviceFootprint
from app.interpretation.floor_counted import CountedDeviceAssignment

# ---------------------------------------------------------------------------
# Result dataclasses (locked — R-G assembles them)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RoomEstimatedContribution:
    """Per-room ESTIMATED cable contribution — device drops + home runs.

    ``room_number``, ``room_id``, ``boundary_basis``, and ``confidence`` propagate
    from the underlying R-E ``CountedDeviceAssignment``.  The special unassigned bucket
    uses ``room_number=None``.

    ``circuit_ids`` is the sorted tuple of circuit_ids that contribute drops to this room.
    """

    room_number: str | None
    room_id: str | None
    boundary_basis: str | None
    confidence: float | None
    needs_review: bool
    device_drop_m: float
    home_run_m: float
    circuit_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class EstimatedCircuitContribution:
    """Floor-level in-plan length contribution for one circuit.

    ``in_plan_length_m`` is NOT per-room — it is a floor-level quantity.
    ``rooms_touched`` is a context-only tuple of room_numbers for the circuit's devices
    (may include None for unassigned devices).
    """

    circuit_id: int
    in_plan_length_m: float
    rooms_touched: tuple[str | None, ...]


@dataclass(frozen=True, slots=True)
class FusedEstimatedResult:
    """Complete per-room estimated cable result for one floor.

    ``per_room`` excludes the unassigned bucket and is sorted by ``room_number``
    (None values sort last because room_number is always a string when assigned).
    ``unassigned`` is the single bucket for device drops from devices with no room
    assignment and home runs from circuits with no-room anchor.
    ``estimated_circuits`` is sorted by ``circuit_id``.

    ``quantity_kind`` is always ``"estimated"``; NEVER folded into MEASURED.
    ``reliability`` and ``params_stamp`` are carried from the ``CableAssembly``.
    ``source`` carries the revision IDs used to produce this result.
    """

    per_room: tuple[RoomEstimatedContribution, ...]
    unassigned: RoomEstimatedContribution
    estimated_circuits: tuple[EstimatedCircuitContribution, ...]
    quantity_kind: str
    reliability: dict[str, str]
    params_stamp: dict[str, object]
    source: dict[str, str | None]


# ---------------------------------------------------------------------------
# Public bucketing function
# ---------------------------------------------------------------------------


def bucket_estimated_cable(
    *,
    assembly: CableAssembly,
    circuits: CableCircuitSet,
    device_footprints: Sequence[DeviceFootprint],
    params: CableEstimateParams,
    device_room_map: Mapping[str, CountedDeviceAssignment],
    source: dict[str, str | None],
) -> FusedEstimatedResult:
    """Distribute a ``CableAssembly`` across rooms using the R-E device→room map.

    Parameters
    ----------
    assembly:
        Fully assembled cable estimate from ``assemble_cable_estimate``.
    circuits:
        Partitioned circuit set from ``partition_circuits``.  Must be the same
        partition used to produce ``assembly`` (circuit_ids must align).
    device_footprints:
        All ``DeviceFootprint`` objects for the lighting revision.
    params:
        ``CableEstimateParams`` used to produce ``assembly``; provides
        ``vertical_drops`` for the per-device replay.
    device_room_map:
        ``device_entity_id → CountedDeviceAssignment`` from
        ``CountedFusionResult.by_device_id()``.  Devices absent from this map are
        treated as unassigned (room_number=None).
    source:
        Provenance dict forwarded into ``FusedEstimatedResult.source``.

    Returns
    -------
    FusedEstimatedResult
        Per-room and floor-level estimated contributions with conservation enforced.

    Raises
    ------
    RuntimeError
        If the per-circuit replay guard fails (reconstructed drops ≠ assembly drops)
        or if any of the three floor-level conservation checks fail.
    """
    # -----------------------------------------------------------------------
    # Step 0: build lookup structures
    # -----------------------------------------------------------------------
    drop_by_category: dict[str, float] = {dp.category: dp.drop_m for dp in params.vertical_drops}

    block_ref_by_device_id: dict[str, str | None] = {
        fp.entity_id: fp.block_ref for fp in device_footprints
    }

    # Index assembly per_circuit by circuit_id for O(1) lookup.
    assembly_by_circuit_id = {c.circuit_id: c for c in assembly.per_circuit}

    # Index CableCircuit by circuit_id for O(1) home-run anchor lookup in Step 3.
    circuit_by_id: dict[int, CableCircuit] = {c.circuit_id: c for c in circuits.circuits}

    # Sorted circuit list (ascending circuit_id) — deterministic first-claim order.
    sorted_circuits = sorted(circuits.circuits, key=lambda c: c.circuit_id)

    # -----------------------------------------------------------------------
    # Step 1: Replay per-device drops with first-claim attribution → room buckets.
    #
    # Accumulator: room_number → (room_id, boundary_basis, confidence, needs_review,
    #                              device_drop_m, home_run_m, circuit_ids_set)
    # We use room_number as the key because it is the stable room identity used
    # throughout Phase R; room_id is carried for completeness.
    # -----------------------------------------------------------------------

    room_drop: dict[str | None, float] = {}  # room_number → Σ device_drop_m
    room_homerun: dict[str | None, float] = {}  # room_number → Σ home_run_m
    room_meta: dict[
        str | None,
        tuple[str | None, str | None, float | None, bool],
    ] = {}  # room_number → (room_id, boundary_basis, confidence, needs_review)
    room_circuits: dict[str | None, set[int]] = {}  # room_number → circuit_ids

    # Devices that touched a circuit (union of all circuit device_entity_ids).
    circuit_device_universe: set[str] = set()
    for circuit in circuits.circuits:
        circuit_device_universe.update(circuit.device_entity_ids)

    claimed: set[str] = set()

    estimated_circuit_list: list[EstimatedCircuitContribution] = []

    for circuit in sorted_circuits:
        circuit_id = circuit.circuit_id
        assembly_circuit = assembly_by_circuit_id.get(circuit_id)

        # Replay per-device drops for this circuit (first-claim).
        replayed_drop = 0.0
        rooms_touched: set[str | None] = set()

        for dev_eid in circuit.device_entity_ids:  # already sorted
            if dev_eid in claimed:
                continue  # claimed by an earlier circuit
            claimed.add(dev_eid)

            block_ref = block_ref_by_device_id.get(dev_eid)
            category = categorize_device(block_ref)
            drop = drop_by_category.get(category, 0.0) if category is not None else 0.0
            replayed_drop += drop

            # Room attribution via R-E device_room_map.
            assignment: CountedDeviceAssignment | None = device_room_map.get(dev_eid)
            rn = assignment.room_number if assignment is not None else None
            rooms_touched.add(rn)

            if drop > 0.0:
                # Accumulate drop into the device's room bucket.
                room_drop[rn] = room_drop.get(rn, 0.0) + drop
                room_circuits.setdefault(rn, set()).add(circuit_id)
                if rn not in room_meta:
                    if assignment is not None:
                        room_meta[rn] = (
                            assignment.room_id,
                            assignment.boundary_basis,
                            assignment.confidence,
                            assignment.needs_review,
                        )
                    else:
                        room_meta[rn] = (None, None, None, False)

        # -----------------------------------------------------------------------
        # Replay guard: per-circuit reconstructed drops must match assembly.
        # A missing circuit_id means circuits and assembly come from different
        # partitions — fail immediately rather than silently zeroing in-plan.
        # -----------------------------------------------------------------------
        if assembly_circuit is None:
            raise RuntimeError(
                f"Circuit {circuit_id} exists in circuits but not in assembly.per_circuit"
                " — circuits and assembly must come from the same partition."
            )
        if not math.isclose(replayed_drop, assembly_circuit.device_drop_m, abs_tol=1e-6):
            raise RuntimeError(
                f"Per-circuit drop replay guard failed for circuit {circuit_id}: "
                f"replayed={replayed_drop!r} != assembly={assembly_circuit.device_drop_m!r}. "
                "Circuit partition or DeviceFootprints do not match the CableAssembly."
            )

        # Floor-level in-plan contribution (not per-room).
        in_plan_m = assembly_circuit.in_plan_length_m

        # rooms_touched: sorted, deterministic (None maps to "" for sort key).
        sorted_rooms_touched = tuple(sorted(rooms_touched, key=lambda r: (r is None, r or "")))
        estimated_circuit_list.append(
            EstimatedCircuitContribution(
                circuit_id=circuit_id,
                in_plan_length_m=in_plan_m,
                rooms_touched=sorted_rooms_touched,
            )
        )

    # -----------------------------------------------------------------------
    # Step 2: Unattributed device drops (devices in no circuit at all).
    #
    # A device with no circuit membership is treated as unassigned even when its
    # room is known via device_room_map — without circuit context, attributing its
    # drop to a room would be speculative (it could belong to an out-of-scope circuit
    # whose total is not tracked in the assembly).  This mirrors cable_estimate.py's
    # unattributed bucket logic exactly.
    # -----------------------------------------------------------------------
    for fp in sorted(device_footprints, key=lambda f: f.entity_id):
        if fp.entity_id in circuit_device_universe:
            continue
        category = categorize_device(fp.block_ref)
        drop = drop_by_category.get(category, 0.0) if category is not None else 0.0
        if drop > 0.0:
            # Unassigned bucket regardless of whether device_room_map has an entry.
            room_drop[None] = room_drop.get(None, 0.0) + drop
            if None not in room_meta:
                room_meta[None] = (None, None, None, False)

    # -----------------------------------------------------------------------
    # Step 3: Home-run attribution to anchor device's room.
    #
    # Only circuits with home_run_status == "ok" contribute.
    # The anchor device's room is looked up via device_room_map.
    # -----------------------------------------------------------------------
    for assembly_circuit in assembly.per_circuit:
        if assembly_circuit.home_run_status != "ok" or assembly_circuit.home_run_m is None:
            continue  # non-ok status → contributes nothing (honest)

        circuit_match: CableCircuit | None = circuit_by_id.get(assembly_circuit.circuit_id)
        if circuit_match is None or circuit_match.anchor_device_entity_id is None:
            # No anchor → unassigned home_run bucket.
            room_homerun[None] = room_homerun.get(None, 0.0) + assembly_circuit.home_run_m
            if None not in room_meta:
                room_meta[None] = (None, None, None, False)
            continue

        anchor_id = circuit_match.anchor_device_entity_id
        anchor_assignment: CountedDeviceAssignment | None = device_room_map.get(anchor_id)
        anchor_rn = anchor_assignment.room_number if anchor_assignment is not None else None

        room_homerun[anchor_rn] = room_homerun.get(anchor_rn, 0.0) + assembly_circuit.home_run_m
        room_circuits.setdefault(anchor_rn, set()).add(assembly_circuit.circuit_id)
        if anchor_rn not in room_meta:
            if anchor_assignment is not None:
                room_meta[anchor_rn] = (
                    anchor_assignment.room_id,
                    anchor_assignment.boundary_basis,
                    anchor_assignment.confidence,
                    anchor_assignment.needs_review,
                )
            else:
                room_meta[anchor_rn] = (None, None, None, False)

    # -----------------------------------------------------------------------
    # Step 4: Assemble RoomEstimatedContribution objects.
    # -----------------------------------------------------------------------
    # Collect all room_numbers that have any contribution.
    all_room_numbers: set[str | None] = set(room_drop.keys()) | set(room_homerun.keys())

    per_room_list: list[RoomEstimatedContribution] = []
    unassigned_contrib: RoomEstimatedContribution | None = None

    for rn in sorted(
        all_room_numbers,
        key=lambda r: (r is None, r or ""),
    ):
        meta = room_meta.get(rn, (None, None, None, False))
        room_id, boundary_basis, confidence, needs_review = meta
        d_drop = room_drop.get(rn, 0.0)
        h_run = room_homerun.get(rn, 0.0)
        c_ids = tuple(sorted(room_circuits.get(rn, set())))

        contrib = RoomEstimatedContribution(
            room_number=rn,
            room_id=room_id,
            boundary_basis=boundary_basis,
            confidence=confidence,
            needs_review=needs_review,
            device_drop_m=d_drop,
            home_run_m=h_run,
            circuit_ids=c_ids,
        )

        if rn is None:
            unassigned_contrib = contrib
        else:
            per_room_list.append(contrib)

    # Ensure the unassigned bucket always exists (even if zero).
    if unassigned_contrib is None:
        unassigned_contrib = RoomEstimatedContribution(
            room_number=None,
            room_id=None,
            boundary_basis=None,
            confidence=None,
            needs_review=False,
            device_drop_m=0.0,
            home_run_m=0.0,
            circuit_ids=(),
        )

    # Sort per_room by room_number ascending.
    per_room_list.sort(key=lambda r: r.room_number or "")

    # Sort estimated_circuits by circuit_id.
    estimated_circuit_list.sort(key=lambda c: c.circuit_id)

    # -----------------------------------------------------------------------
    # Step 5: Floor-level conservation checks (RuntimeError, fires under -O).
    # -----------------------------------------------------------------------
    total_device_drop_check = (
        sum(r.device_drop_m for r in per_room_list) + unassigned_contrib.device_drop_m
    )
    if not math.isclose(total_device_drop_check, assembly.total_device_drop_m, abs_tol=1e-6):
        raise RuntimeError(
            f"device_drop conservation violated: "
            f"Σ per_room.device_drop_m + unassigned={total_device_drop_check!r} "
            f"!= assembly.total_device_drop_m={assembly.total_device_drop_m!r}"
        )

    total_home_run_check = sum(r.home_run_m for r in per_room_list) + unassigned_contrib.home_run_m
    if not math.isclose(total_home_run_check, assembly.total_home_run_m, abs_tol=1e-6):
        raise RuntimeError(
            f"home_run conservation violated: "
            f"Σ per_room.home_run_m + unassigned={total_home_run_check!r} "
            f"!= assembly.total_home_run_m={assembly.total_home_run_m!r}"
        )

    total_in_plan_check = sum(c.in_plan_length_m for c in estimated_circuit_list)
    if not math.isclose(total_in_plan_check, assembly.total_in_plan_length_m, abs_tol=1e-6):
        raise RuntimeError(
            f"in_plan_length conservation violated: "
            f"Σ estimated_circuits.in_plan_length_m={total_in_plan_check!r} "
            f"!= assembly.total_in_plan_length_m={assembly.total_in_plan_length_m!r}"
        )

    return FusedEstimatedResult(
        per_room=tuple(per_room_list),
        unassigned=unassigned_contrib,
        estimated_circuits=tuple(estimated_circuit_list),
        quantity_kind=assembly.quantity_kind,
        reliability=dict(assembly.reliability),
        params_stamp=dict(assembly.params_stamp),
        source=source,
    )
