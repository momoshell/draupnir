"""Synthetic unit tests for app.interpretation.cable_estimate (issue #696).

All geometry is in metres.  No DB, ORM, or FastAPI.

Tests use the real pipeline:
  SplineInput + DeviceFootprint → build_cable_graph → partition_circuits
  → estimate_cable_in_plan

Internal dataclasses are never hand-constructed.
"""

from __future__ import annotations

import json
import random

import pytest

from app.interpretation.cable_circuits import partition_circuits
from app.interpretation.cable_estimate import (
    CableEstimateResult,
    _polyline_length,
    categorize_device,
    estimate_cable_in_plan,
)
from app.interpretation.cable_estimate_params import (
    CATEGORY_DISTRIBUTION_BOARD,
    CATEGORY_LUMINAIRE,
    CATEGORY_SOCKET,
    CATEGORY_SWITCH,
    default_estimate_params,
)
from app.interpretation.cable_topology import (
    DeviceFootprint,
    SplineInput,
    build_cable_graph,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _spline(
    entity_id: str,
    *pts: tuple[float, float],
    closed: bool = False,
) -> SplineInput:
    return SplineInput(entity_id=entity_id, vertices=tuple(pts), closed=closed)


def _device(
    entity_id: str,
    block_ref: str | None,
    position: tuple[float, float],
    bbox_half: float = 0.3,
) -> DeviceFootprint:
    x, y = position
    return DeviceFootprint(
        entity_id=entity_id,
        block_ref=block_ref,
        kind="device",
        bbox=(x - bbox_half, y - bbox_half, x + bbox_half, y + bbox_half),
        position=position,
    )


def _device_wide(
    entity_id: str,
    block_ref: str | None,
    bbox: tuple[float, float, float, float],
) -> DeviceFootprint:
    """Device with an explicit large bbox (for cross-circuit overlap tests)."""
    cx = (bbox[0] + bbox[2]) / 2
    cy = (bbox[1] + bbox[3]) / 2
    return DeviceFootprint(
        entity_id=entity_id,
        block_ref=block_ref,
        kind="device",
        bbox=bbox,
        position=(cx, cy),
    )


def _run_pipeline(
    splines: list[SplineInput],
    devices: list[DeviceFootprint],
) -> CableEstimateResult:
    """End-to-end pipeline helper."""
    params = default_estimate_params()
    graph = build_cable_graph(splines=splines, devices=devices)
    circuit_set = partition_circuits(graph)
    return estimate_cable_in_plan(
        circuits=circuit_set,
        splines=splines,
        devices=devices,
        params=params,
    )


# ---------------------------------------------------------------------------
# _polyline_length
# ---------------------------------------------------------------------------


class TestPolylineLength:
    def test_zero_vertices(self) -> None:
        assert _polyline_length(()) == pytest.approx(0.0)

    def test_one_vertex(self) -> None:
        assert _polyline_length(((1.0, 2.0),)) == pytest.approx(0.0)

    def test_straight_horizontal(self) -> None:
        # 5 m horizontal segment
        assert _polyline_length(((0.0, 0.0), (5.0, 0.0))) == pytest.approx(5.0)

    def test_straight_vertical(self) -> None:
        assert _polyline_length(((0.0, 0.0), (0.0, 3.0))) == pytest.approx(3.0)

    def test_right_triangle(self) -> None:
        # 3-4-5 triangle
        verts = ((0.0, 0.0), (3.0, 0.0), (3.0, 4.0))
        assert _polyline_length(verts) == pytest.approx(7.0)

    def test_known_diagonal(self) -> None:
        import math

        # unit diagonal: sqrt(2)
        assert _polyline_length(((0.0, 0.0), (1.0, 1.0))) == pytest.approx(math.sqrt(2))


# ---------------------------------------------------------------------------
# categorize_device
# ---------------------------------------------------------------------------


class TestCategorizeDevice:
    def test_none_block_ref(self) -> None:
        assert categorize_device(None) is None

    def test_unknown_block_ref(self) -> None:
        assert categorize_device("MysteryBlock") is None

    def test_whitecroft_luminaire(self) -> None:
        assert categorize_device("Whitecroft LED Panel 600x600") == CATEGORY_LUMINAIRE

    def test_plate_switch(self) -> None:
        assert categorize_device("Plate Switches - One Way Switch") == CATEGORY_SWITCH

    def test_distribution_board_uniclass(self) -> None:
        # Must map to distribution_board, NOT luminaire/switch via any embedded token.
        assert (
            categorize_device("Pr_60_70_22_85_PB800 - PANEL BOARD 800A")
            == CATEGORY_DISTRIBUTION_BOARD
        )

    def test_distribution_board_not_luminaire_or_switch(self) -> None:
        # Paranoid: confirm the DB result is not confused with luminaire/switch categories.
        result = categorize_device("Pr_60_70_22_22_DB-TST1 - Standard Distribution Board")
        assert result == CATEGORY_DISTRIBUTION_BOARD
        assert result != CATEGORY_LUMINAIRE
        assert result != CATEGORY_SWITCH

    def test_socket(self) -> None:
        assert categorize_device("SSO - Twin Socket Outlet") == CATEGORY_SOCKET

    def test_outlet(self) -> None:
        assert categorize_device("Power Point Double GPO") == CATEGORY_SOCKET

    def test_dimmer(self) -> None:
        assert categorize_device("Dimmer Switch 400W") == CATEGORY_SWITCH

    def test_downlight(self) -> None:
        assert categorize_device("Downlight Recessed 10W") == CATEGORY_LUMINAIRE

    def test_case_insensitive(self) -> None:
        assert categorize_device("WHITECROFT SURFACE MOUNT") == CATEGORY_LUMINAIRE
        assert categorize_device("plate switch single") == CATEGORY_SWITCH

    def test_distribution_checked_before_generic(self) -> None:
        # A block_ref that contains BOTH "Distribution Board" (→ distribution_board)
        # AND "Switch" as a substring — distribution_board must win because it's first.
        result = categorize_device("Distribution Board with Switch Gear")
        assert result == CATEGORY_DISTRIBUTION_BOARD


# ---------------------------------------------------------------------------
# Core estimate: 2 luminaires + 1 switch in one T-shaped circuit
# ---------------------------------------------------------------------------


class TestCircuitEstimate:
    """
    Topology: a T-shaped graph forming one connected circuit.

    Splines:
      S1: (0,0) → (5,0)    length = 5.0 m  (trunk)
      S2: (5,0) → (10,0)   length = 5.0 m  (right arm)
      S3: (5,0) → (5,5)    length = 5.0 m  (branch)

    Nodes:
      (0,0)  — degree 1, terminal → D_L1 (luminaire)
      (10,0) — degree 1, terminal → D_L2 (luminaire)
      (5,5)  — degree 1, terminal → D_SW (switch)
      (5,0)  — degree 3, interior (no terminal association)

    All four nodes form one component (3 splines).

    Default params: luminaire drop = 2.0, switch drop = 1.2
    Expected total_device_drop_m = 2.0 + 2.0 + 1.2 = 5.2
    Expected in_plan_length_m = 5.0 + 5.0 + 5.0 = 15.0
    """

    def _build(self) -> CableEstimateResult:
        splines = [
            _spline("S1", (0.0, 0.0), (5.0, 0.0)),
            _spline("S2", (5.0, 0.0), (10.0, 0.0)),
            _spline("S3", (5.0, 0.0), (5.0, 5.0)),
        ]
        devices = [
            _device("D_L1", "Whitecroft LED 60x60", (0.0, 0.0)),
            _device("D_L2", "Whitecroft LED 60x60", (10.0, 0.0)),
            _device("D_SW", "Plate Switches - One Way Switch", (5.0, 5.0)),
        ]
        return _run_pipeline(splines, devices)

    def test_device_drop_total(self) -> None:
        result = self._build()
        assert result.total_device_drop_m == pytest.approx(5.2)

    def test_breakdown_correct(self) -> None:
        result = self._build()
        # Flatten attributed breakdowns across circuits.
        all_breakdown: dict[str, tuple[int, float]] = {}
        for circuit in result.circuits:
            for bd in circuit.drop_breakdown:
                prev_count, prev_sub = all_breakdown.get(bd.category, (0, 0.0))
                all_breakdown[bd.category] = (
                    prev_count + bd.device_count,
                    prev_sub + bd.subtotal_m,
                )
        assert CATEGORY_LUMINAIRE in all_breakdown
        lum_count, lum_sub = all_breakdown[CATEGORY_LUMINAIRE]
        assert lum_count == 2
        assert lum_sub == pytest.approx(4.0)

        assert CATEGORY_SWITCH in all_breakdown
        sw_count, sw_sub = all_breakdown[CATEGORY_SWITCH]
        assert sw_count == 1
        assert sw_sub == pytest.approx(1.2)

    def test_in_plan_length(self) -> None:
        result = self._build()
        # Three splines each 5.0 m → 15.0 m total.
        assert result.total_in_plan_length_m == pytest.approx(15.0)

    def test_quantity_kind(self) -> None:
        assert self._build().quantity_kind == "estimated"

    def test_reliability_labels(self) -> None:
        result = self._build()
        assert result.reliability["device_drop_m"] == "reliable"
        assert result.reliability["in_plan_length_m"] == "schematic_provisional"
        assert "note" in result.reliability

    def test_params_stamp_present_and_json_serializable(self) -> None:
        result = self._build()
        stamp = result.params_stamp
        assert isinstance(stamp, dict)
        # Must be fully JSON-serialisable (no datetime, UUID, dataclass, etc.)
        serialised = json.dumps(stamp)
        roundtrip = json.loads(serialised)
        assert "vertical_drops" in roundtrip
        assert "spare" in roundtrip

    def test_spare_fraction_carried(self) -> None:
        result = self._build()
        assert result.spare_fraction == pytest.approx(0.10)

    def test_no_unattributed_when_all_devices_in_circuit(self) -> None:
        result = self._build()
        assert result.unattributed_device_count == 0
        assert result.unattributed_device_drop_m == pytest.approx(0.0)

    def test_conservation_invariant(self) -> None:
        result = self._build()
        circuit_sum = sum(c.device_drop_m for c in result.circuits)
        assert circuit_sum + result.unattributed_device_drop_m == pytest.approx(
            result.total_device_drop_m
        )


# ---------------------------------------------------------------------------
# Uncategorized device: no drop, counted honestly
# ---------------------------------------------------------------------------


class TestUncategorizedDevice:
    def test_mystery_block_no_crash_no_drop(self) -> None:
        # One spline, one uncategorized device at the terminal.
        splines = [_spline("SU1", (0.0, 0.0), (5.0, 0.0))]
        devices = [_device("D_MYSTERY", "MysteryBlock", (0.0, 0.0))]
        result = _run_pipeline(splines, devices)

        assert result.total_device_drop_m == pytest.approx(0.0)
        assert result.total_uncategorized_devices >= 1
        # Uncategorized must not appear in any breakdown category.
        for circuit in result.circuits:
            cats = {bd.category for bd in circuit.drop_breakdown}
            assert CATEGORY_LUMINAIRE not in cats
            assert CATEGORY_SWITCH not in cats
            assert CATEGORY_SOCKET not in cats
            assert CATEGORY_DISTRIBUTION_BOARD not in cats


# ---------------------------------------------------------------------------
# Cross-circuit device: shared device counted exactly once
# ---------------------------------------------------------------------------


class TestCrossCircuitDevice:
    """
    Two disjoint circuits; one luminaire device has a large bbox that spans
    the terminal of BOTH circuits so it appears in both circuits'
    device_entity_ids.  Its drop must be counted exactly ONCE in
    total_device_drop_m, and attributed to the lower circuit_id only.

    Layout:
      Circuit A: SA1 (0,0)→(1,0)  — terminal at (0,0), device D_BIG there
      Circuit B: SB1 (10,0)→(11,0) — terminal at (11,0), unrelated device D_LB

    D_BIG has a large bbox that also covers (10,0) so it will be in BOTH
    circuits' device_entity_ids.

    Expected:
      total_device_drop_m = 1 luminaire (D_BIG, 2.0) + 1 luminaire (D_LB, 2.0) = 4.0
      D_BIG attributed to circuit A (lower id) ONLY.
      conservation: sum per-circuit + unattributed == 4.0
    """

    def _build(self) -> CableEstimateResult:
        # D_BIG: bbox spans from x=-1 to x=12, covering terminals of BOTH circuits,
        # so partition_circuits places it in both circuits' device_entity_ids.
        # D_LB: at (11,0) — contained inside D_BIG's bbox, loses the entity_id
        # tie-break ("D_BIG" < "D_LB"), so (11,0) terminal associates to D_BIG;
        # D_LB ends up in no circuit → unattributed bucket.
        d_big = _device_wide("D_BIG", "Whitecroft LED Panel", (-1.0, -1.0, 12.0, 1.0))
        d_lb = _device("D_LB", "Downlight 10W", (11.0, 0.0))
        splines = [
            _spline("SA1", (0.0, 0.0), (1.0, 0.0)),
            _spline("SB1", (10.0, 0.0), (11.0, 0.0)),
        ]
        devices = [d_big, d_lb]
        return _run_pipeline(splines, devices)

    def test_total_device_drop_no_double_count(self) -> None:
        result = self._build()
        # D_BIG counted once (2.0) + D_LB once (2.0) = 4.0, not 6.0.
        assert result.total_device_drop_m == pytest.approx(4.0)

    def test_d_big_attributed_to_lower_circuit_only(self) -> None:
        result = self._build()
        assert len(result.circuits) == 2
        circuit_a = result.circuits[0]  # lower circuit_id (contains SA1)
        circuit_b = result.circuits[1]  # higher circuit_id (contains SB1)

        # D_BIG must be attributed to circuit_a (lowest circuit_id that lists it).
        # circuit_a carries D_BIG's luminaire drop (2.0); circuit_b carries nothing
        # for it — if first-claim attribution regressed, circuit_b would have 2.0 too.
        assert circuit_a.device_drop_m == pytest.approx(2.0), (
            "circuit_a should carry D_BIG's 2.0 m drop"
        )
        assert circuit_b.device_drop_m == pytest.approx(0.0), (
            "circuit_b must NOT double-count D_BIG; it was already claimed by circuit_a"
        )

        # D_LB is unattributed (lost the terminal tie-break to D_BIG).
        assert result.unattributed_device_count == 1
        assert result.unattributed_device_drop_m == pytest.approx(2.0)

    def test_conservation_invariant(self) -> None:
        result = self._build()
        circuit_sum = sum(c.device_drop_m for c in result.circuits)
        assert circuit_sum + result.unattributed_device_drop_m == pytest.approx(
            result.total_device_drop_m
        )


# ---------------------------------------------------------------------------
# Unattributed device: categorized device in no circuit
# ---------------------------------------------------------------------------


class TestUnattributedDevice:
    """
    One circuit + one luminaire that is too far from any terminal to be
    associated with any circuit.

    The unattributed luminaire still contributes to total_device_drop_m
    but lands in the unattributed bucket rather than any circuit.
    """

    def _build(self) -> CableEstimateResult:
        splines = [_spline("S1", (0.0, 0.0), (5.0, 0.0))]
        # D_NEAR: at (0,0) — will be associated to the circuit terminal.
        # D_FAR: at (100,100) — well beyond terminal_assoc_m; no circuit will list it.
        devices = [
            _device("D_NEAR", "Whitecroft LED", (0.0, 0.0)),
            _device("D_FAR", "Downlight 10W", (100.0, 100.0)),
        ]
        return _run_pipeline(splines, devices)

    def test_total_includes_unattributed(self) -> None:
        result = self._build()
        # Both devices are luminaires (2.0 each) → total = 4.0.
        assert result.total_device_drop_m == pytest.approx(4.0)

    def test_unattributed_bucket_populated(self) -> None:
        result = self._build()
        assert result.unattributed_device_count == 1
        assert result.unattributed_device_drop_m == pytest.approx(2.0)

    def test_conservation_invariant(self) -> None:
        result = self._build()
        circuit_sum = sum(c.device_drop_m for c in result.circuits)
        assert circuit_sum + result.unattributed_device_drop_m == pytest.approx(
            result.total_device_drop_m
        )


# ---------------------------------------------------------------------------
# Determinism: shuffle inputs → identical result
# ---------------------------------------------------------------------------


class TestDeterminism:
    def _build_with_order(
        self,
        spline_order: list[int],
        device_order: list[int],
    ) -> CableEstimateResult:
        # T-shaped topology — all 3 device terminals are degree-1 nodes so they
        # receive terminal associations deterministically.
        splines_base = [
            _spline("S1", (0.0, 0.0), (5.0, 0.0)),
            _spline("S2", (5.0, 0.0), (10.0, 0.0)),
            _spline("S3", (5.0, 0.0), (5.0, 5.0)),
        ]
        devices_base = [
            _device("D_L1", "Whitecroft LED", (0.0, 0.0)),
            _device("D_L2", "Downlight 10W", (10.0, 0.0)),
            _device("D_SW", "Plate Switches - One Way Switch", (5.0, 5.0)),
        ]
        splines = [splines_base[i] for i in spline_order]
        devices = [devices_base[i] for i in device_order]
        return _run_pipeline(splines, devices)

    def test_shuffle_gives_identical_result(self) -> None:
        rng = random.Random(42)
        base = self._build_with_order([0, 1, 2], [0, 1, 2])

        for _ in range(5):
            so = [0, 1, 2]
            do = [0, 1, 2]
            rng.shuffle(so)
            rng.shuffle(do)
            other = self._build_with_order(so, do)
            assert other == base, f"Mismatch for spline_order={so}, device_order={do}"


# ---------------------------------------------------------------------------
# Two disjoint circuits — each device cleanly in one circuit
# ---------------------------------------------------------------------------


class TestTwoDisjointCircuits:
    """
    Two isolated chains, no shared nodes, each device clearly in one circuit.

    Circuit A: SA1 (0,0)→(1,0), luminaire at (0,0)
    Circuit B: SB1 (10,0)→(11,0), switch at (11,0)
    """

    def _build(self) -> CableEstimateResult:
        splines = [
            _spline("SA1", (0.0, 0.0), (1.0, 0.0)),
            _spline("SB1", (10.0, 0.0), (11.0, 0.0)),
        ]
        devices = [
            _device("D_LA", "Whitecroft LED", (0.0, 0.0)),
            _device("D_SWB", "Plate Switches - One Way Switch", (11.0, 0.0)),
        ]
        return _run_pipeline(splines, devices)

    def test_total_device_drop(self) -> None:
        result = self._build()
        # 1 luminaire x 2.0 + 1 switch x 1.2 = 3.2
        assert result.total_device_drop_m == pytest.approx(3.2)

    def test_circuit_count(self) -> None:
        result = self._build()
        assert len(result.circuits) == 2

    def test_in_plan_length_total(self) -> None:
        result = self._build()
        # Two 1.0 m splines → 2.0 m
        assert result.total_in_plan_length_m == pytest.approx(2.0)

    def test_no_unattributed(self) -> None:
        result = self._build()
        assert result.unattributed_device_count == 0
        assert result.unattributed_device_drop_m == pytest.approx(0.0)

    def test_conservation_invariant(self) -> None:
        result = self._build()
        circuit_sum = sum(c.device_drop_m for c in result.circuits)
        assert circuit_sum + result.unattributed_device_drop_m == pytest.approx(
            result.total_device_drop_m
        )


# ---------------------------------------------------------------------------
# Import purity: no DB/ORM/FastAPI in source
# ---------------------------------------------------------------------------


def test_import_purity() -> None:
    """cable_estimate.py must not contain any DB/ORM/FastAPI/cv2/shapely import."""
    import ast
    import pathlib
    import sys

    mod = sys.modules.get("app.interpretation.cable_estimate")
    assert mod is not None, "module not loaded"

    forbidden = {"sqlalchemy", "fastapi", "asyncpg", "cv2", "shapely"}
    src = pathlib.Path(mod.__file__).read_text()  # type: ignore[arg-type]
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                for f in forbidden:
                    assert not alias.name.startswith(f), f"forbidden import: {alias.name}"
        elif isinstance(node, ast.ImportFrom) and node.module:
            for f in forbidden:
                assert not node.module.startswith(f), f"forbidden from-import: {node.module}"
