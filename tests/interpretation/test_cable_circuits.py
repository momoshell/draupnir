"""Synthetic unit tests for app.interpretation.cable_circuits (issue #694).

All geometry is in metres.  No DB, no ORM, no FastAPI.
Tests cover: import purity, two disjoint chains, shared junction, determinism,
anchor detection, conservation, and the edge-count invariant.

CableGraph instances are built via build_cable_graph (public API) — internal
CableGraph fields are never hand-constructed.
"""

from __future__ import annotations

import random

from app.interpretation.cable_circuits import (
    partition_circuits,
)
from app.interpretation.cable_topology import (
    DeviceFootprint,
    SplineInput,
    build_cable_graph,
)

# ---------------------------------------------------------------------------
# Helpers — mirrors test_cable_topology.py conventions
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
    bbox: tuple[float, float, float, float] | None = None,
    position: tuple[float, float] | None = None,
) -> DeviceFootprint:
    return DeviceFootprint(
        entity_id=entity_id,
        block_ref=block_ref,
        kind="device",
        bbox=bbox,
        position=position,
    )


# ---------------------------------------------------------------------------
# (1) Import purity
# ---------------------------------------------------------------------------


def test_cable_circuits_import_pure() -> None:
    """cable_circuits.py must not import any DB/ORM/FastAPI/cv2/shapely module."""
    import ast
    import pathlib
    import sys

    mod = sys.modules.get("app.interpretation.cable_circuits")
    assert mod is not None, "module not loaded"

    forbidden = {"sqlalchemy", "fastapi", "cv2", "shapely"}
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


# ---------------------------------------------------------------------------
# (2) Two disjoint edge chains → 2 circuits, deterministic circuit_ids, partition
# ---------------------------------------------------------------------------


def test_two_disjoint_chains_produce_two_circuits() -> None:
    """Two disjoint linear spline chains become two distinct circuits.

    Chain A: s1 (0,0)→(5,0),  s2 (5,0)→(10,0)   — nodes share (5,0) junction
    Chain B: s3 (20,0)→(25,0) — isolated, no junction

    Expect:
    - circuit_count == 2
    - Each circuit contains only its own splines (no cross-contamination)
    - assigned_edge_count == graph.edge_count == 3
    - Conservation holds
    """
    splines = [
        _spline("s1", (0.0, 0.0), (5.0, 0.0)),
        _spline("s2", (5.0, 0.0), (10.0, 0.0)),
        _spline("s3", (20.0, 0.0), (25.0, 0.0)),
    ]
    graph = build_cable_graph(splines=splines, devices=[])
    result = partition_circuits(graph)

    assert result.circuit_count == 2, f"expected 2 circuits, got {result.circuit_count}"
    assert result.assigned_edge_count == 3
    assert result.edge_count == 3

    # Collect per-circuit spline sets.
    spline_sets = [set(c.spline_entity_ids) for c in result.circuits]
    assert {"s1", "s2"} in spline_sets or {"s2", "s1"} in spline_sets
    assert {"s3"} in spline_sets

    # No overlap between circuits.
    all_splines: list[str] = []
    for c in result.circuits:
        all_splines.extend(c.spline_entity_ids)
    assert len(all_splines) == len(set(all_splines)), "duplicate splines across circuits"


def test_two_disjoint_chains_circuit_ids_are_deterministic() -> None:
    """circuit_id assignment follows min node_id sort order — predictable, not arbitrary."""
    splines = [
        _spline("s1", (0.0, 0.0), (5.0, 0.0)),
        _spline("s3", (20.0, 0.0), (25.0, 0.0)),
    ]
    graph = build_cable_graph(splines=splines, devices=[])
    result = partition_circuits(graph)

    assert result.circuit_count == 2
    # circuit_id 0 is the component with the smallest minimum node_id.
    circuit_0 = result.circuits[0]
    circuit_1 = result.circuits[1]
    assert circuit_0.circuit_id == 0
    assert circuit_1.circuit_id == 1
    # The chain starting near (0,0) has a smaller node_id than the one near (20,0).
    assert min(circuit_0.node_ids) < min(circuit_1.node_ids)


# ---------------------------------------------------------------------------
# (3) Shared junction → 1 circuit, both terminal devices collected
# ---------------------------------------------------------------------------


def test_shared_junction_produces_one_circuit() -> None:
    """Two splines meeting at a junction form a single circuit containing both terminals.

    Arrange:
    - s1: (0,0) → (5,0)  — terminal at (0,0) in device d1 bbox
    - s2: (5,0) → (10,0) — terminal at (10,0) in device d2 bbox
    Shared junction at (5,0) ties them into one component.
    """
    splines = [
        _spline("s1", (0.0, 0.0), (5.0, 0.0)),
        _spline("s2", (5.0, 0.0), (10.0, 0.0)),
    ]
    devices = [
        _device("d1", "Luminaire_A", bbox=(-0.5, -0.5, 0.5, 0.5)),
        _device("d2", "Luminaire_B", bbox=(9.5, -0.5, 10.5, 0.5)),
    ]
    graph = build_cable_graph(splines=splines, devices=devices)
    result = partition_circuits(graph)

    assert result.circuit_count == 1, f"expected 1 circuit, got {result.circuit_count}"
    circuit = result.circuits[0]
    assert set(circuit.spline_entity_ids) == {"s1", "s2"}
    assert set(circuit.device_entity_ids) == {"d1", "d2"}
    assert circuit.edge_count == 2
    assert circuit.node_count == 3  # (0,0), (5,0), (10,0)
    assert circuit.device_count == 2


# ---------------------------------------------------------------------------
# (4) Determinism — shuffled inputs → identical CableCircuitSet
# ---------------------------------------------------------------------------


def test_determinism_shuffled_inputs() -> None:
    """partition_circuits is order-independent: shuffled graph inputs yield equal output."""
    splines = [
        _spline("s1", (0.0, 0.0), (5.0, 0.0)),
        _spline("s2", (5.0, 0.0), (10.0, 0.0)),
        _spline("s3", (20.0, 0.0), (25.0, 0.0)),
    ]
    devices = [
        _device("d1", "Luminaire_A", bbox=(-0.5, -0.5, 0.5, 0.5)),
        _device("d2", "Luminaire_B", bbox=(24.5, -0.5, 25.5, 0.5)),
    ]

    graph_a = build_cable_graph(splines=splines, devices=devices)
    result_a = partition_circuits(graph_a)

    rng = random.Random(42)
    shuffled_splines = list(splines)
    shuffled_devices = list(devices)
    rng.shuffle(shuffled_splines)
    rng.shuffle(shuffled_devices)

    graph_b = build_cable_graph(splines=shuffled_splines, devices=shuffled_devices)
    result_b = partition_circuits(graph_b)

    assert result_a == result_b, "CableCircuitSet differs after input shuffle"


def test_determinism_many_shuffles() -> None:
    """Ten independent shuffles all produce the same CableCircuitSet."""
    splines = [
        _spline("sa", (0.0, 0.0), (3.0, 0.0)),
        _spline("sb", (3.0, 0.0), (6.0, 0.0)),
        _spline("sc", (6.0, 0.0), (9.0, 0.0)),
        _spline("sd", (50.0, 0.0), (55.0, 0.0)),  # disjoint chain
    ]
    devices = [
        _device("d1", "Lum", bbox=(-0.5, -0.5, 0.5, 0.5)),
    ]

    reference = partition_circuits(build_cable_graph(splines=splines, devices=devices))

    rng = random.Random(99)
    for _ in range(10):
        s_copy = list(splines)
        d_copy = list(devices)
        rng.shuffle(s_copy)
        rng.shuffle(d_copy)
        candidate = partition_circuits(build_cable_graph(splines=s_copy, devices=d_copy))
        assert candidate == reference, "CableCircuitSet differs on a shuffle"


# ---------------------------------------------------------------------------
# (5) Anchor device detection
# ---------------------------------------------------------------------------


def test_anchor_panel_board_detected() -> None:
    """A terminal associated to a 'Pr_60_70_22_85_PB800 - PANEL BOARD' block becomes anchor.

    Arrange: one circuit with two terminal devices — a luminaire and a panel board.
    Expect: anchor_device_entity_id points to the panel-board device.
    """
    splines = [_spline("s1", (0.0, 0.0), (5.0, 0.0))]
    devices = [
        _device(
            "d_lum",
            "Luminaire_LED_Downlight",
            bbox=(-0.5, -0.5, 0.5, 0.5),
        ),
        _device(
            "d_panel",
            "Pr_60_70_22_85_PB800 - PANEL BOARD 800A",
            bbox=(4.5, -0.5, 5.5, 0.5),
        ),
    ]
    graph = build_cable_graph(splines=splines, devices=devices)
    result = partition_circuits(graph)

    assert result.circuit_count == 1
    circuit = result.circuits[0]
    assert circuit.anchor_device_entity_id == "d_panel"


def test_anchor_distribution_board_detected() -> None:
    """A 'Standard Distribution Board' block ref triggers anchor detection."""
    splines = [_spline("s1", (0.0, 0.0), (5.0, 0.0))]
    devices = [
        _device(
            "d_db",
            "Pr_60_70_22_22_DB-TST1 - Standard Distribution Board",
            bbox=(4.5, -0.5, 5.5, 0.5),
        ),
        _device("d_lum", "Luminaire_LED", bbox=(-0.5, -0.5, 0.5, 0.5)),
    ]
    graph = build_cable_graph(splines=splines, devices=devices)
    result = partition_circuits(graph)

    assert result.circuits[0].anchor_device_entity_id == "d_db"


def test_anchor_none_for_luminaires_only() -> None:
    """A circuit with only luminaires (no distribution/panel device) → anchor is None."""
    splines = [_spline("s1", (0.0, 0.0), (5.0, 0.0))]
    devices = [
        _device("d1", "Luminaire_LED_Downlight", bbox=(-0.5, -0.5, 0.5, 0.5)),
        _device("d2", "Luminaire_LED_Strip", bbox=(4.5, -0.5, 5.5, 0.5)),
    ]
    graph = build_cable_graph(splines=splines, devices=devices)
    result = partition_circuits(graph)

    assert result.circuits[0].anchor_device_entity_id is None


def test_anchor_multiple_candidates_lowest_entity_id_wins() -> None:
    """When multiple distribution devices qualify, lowest entity_id wins."""
    splines = [
        _spline("s1", (0.0, 0.0), (5.0, 0.0)),
        _spline("s2", (5.0, 0.0), (10.0, 0.0)),
    ]
    devices = [
        _device(
            "panel_zzz",
            "Pr_60_70_22_85_PB800 - PANEL BOARD",
            bbox=(-0.5, -0.5, 0.5, 0.5),
        ),
        _device(
            "panel_aaa",
            "Distribution Board Type A",
            bbox=(9.5, -0.5, 10.5, 0.5),
        ),
    ]
    graph = build_cable_graph(splines=splines, devices=devices)
    result = partition_circuits(graph)

    # Both qualify; "panel_aaa" < "panel_zzz" lexicographically.
    assert result.circuits[0].anchor_device_entity_id == "panel_aaa"


def test_anchor_case_insensitive_match() -> None:
    """Pattern matching on block_ref is case-insensitive."""
    splines = [_spline("s1", (0.0, 0.0), (5.0, 0.0))]
    devices = [
        _device("d1", "SWITCHBOARD MAIN", bbox=(4.5, -0.5, 5.5, 0.5)),
        _device("d2", "Luminaire", bbox=(-0.5, -0.5, 0.5, 0.5)),
    ]
    graph = build_cable_graph(splines=splines, devices=devices)
    result = partition_circuits(graph)

    assert result.circuits[0].anchor_device_entity_id == "d1"


# ---------------------------------------------------------------------------
# (6) Conservation — assigned_edge_count == graph.edge_count
# ---------------------------------------------------------------------------


def test_conservation_assigned_equals_graph_edge_count() -> None:
    """assigned_edge_count == graph.edge_count for any topology."""
    splines = [
        _spline("e1", (0.0, 0.0), (1.0, 0.0)),
        _spline("e2", (1.0, 0.0), (2.0, 0.0)),
        _spline("e3", (10.0, 0.0), (11.0, 0.0)),
        _spline("e4", (20.0, 0.0), (21.0, 0.0)),
        _spline("e5", (21.0, 0.0), (22.0, 0.0)),
        _spline("e6", (22.0, 0.0), (23.0, 0.0)),
    ]
    graph = build_cable_graph(splines=splines, devices=[])
    result = partition_circuits(graph)

    assert result.assigned_edge_count == graph.edge_count
    assert result.edge_count == graph.edge_count
    assert sum(c.edge_count for c in result.circuits) == graph.edge_count


def test_conservation_sum_of_per_circuit_edge_counts() -> None:
    """sum(c.edge_count) == graph.edge_count for an arbitrary mixed topology."""
    splines = [
        _spline("a1", (0.0, 0.0), (5.0, 0.0)),
        _spline("a2", (5.0, 0.0), (10.0, 0.0)),
        _spline("b1", (100.0, 0.0), (105.0, 0.0)),
    ]
    graph = build_cable_graph(splines=splines, devices=[])
    result = partition_circuits(graph)

    total = sum(c.edge_count for c in result.circuits)
    assert total == graph.edge_count, (
        f"sum of per-circuit edge_count ({total}) != graph.edge_count ({graph.edge_count})"
    )


def test_conservation_single_spline() -> None:
    """Single spline → 1 circuit, 1 edge, conservation trivially holds."""
    graph = build_cable_graph(
        splines=[_spline("only", (0.0, 0.0), (1.0, 0.0))],
        devices=[],
    )
    result = partition_circuits(graph)

    assert result.circuit_count == 1
    assert result.assigned_edge_count == 1
    assert result.edge_count == 1
    assert result.circuits[0].edge_count == 1


def test_conservation_empty_graph() -> None:
    """Empty graph (no splines) → 0 circuits, 0 edges, conservation trivially holds."""
    graph = build_cable_graph(splines=[], devices=[])
    result = partition_circuits(graph)

    assert result.circuit_count == 0
    assert result.edge_count == 0
    assert result.assigned_edge_count == 0
    assert result.device_count == 0


# ---------------------------------------------------------------------------
# (7) Closed spline — self-loop edge stays in its own circuit
# ---------------------------------------------------------------------------


def test_closed_spline_self_loop_forms_singleton_circuit() -> None:
    """A closed spline (node_a == node_b possible) must appear in exactly one circuit."""
    splines = [
        _spline("closed1", (0.0, 0.0), (1.0, 0.0), (1.0, 1.0), closed=True),
        _spline("open1", (50.0, 0.0), (55.0, 0.0)),  # disjoint open chain
    ]
    graph = build_cable_graph(splines=splines, devices=[])
    result = partition_circuits(graph)

    # Both splines must be in exactly one circuit each (no cross-assignment).
    all_splines: list[str] = []
    for c in result.circuits:
        all_splines.extend(c.spline_entity_ids)
    assert sorted(all_splines) == ["closed1", "open1"]
    assert result.assigned_edge_count == 2


# ---------------------------------------------------------------------------
# (8) Star topology — multiple splines meeting at one junction
# ---------------------------------------------------------------------------


def test_star_topology_single_circuit() -> None:
    """Four splines radiating from a common hub → one circuit containing all four."""
    hub = (5.0, 5.0)
    splines = [
        _spline("r1", hub, (10.0, 5.0)),
        _spline("r2", hub, (5.0, 10.0)),
        _spline("r3", hub, (0.0, 5.0)),
        _spline("r4", hub, (5.0, 0.0)),
    ]
    graph = build_cable_graph(splines=splines, devices=[])
    result = partition_circuits(graph)

    assert result.circuit_count == 1
    circuit = result.circuits[0]
    assert set(circuit.spline_entity_ids) == {"r1", "r2", "r3", "r4"}
    assert circuit.edge_count == 4
    assert result.assigned_edge_count == 4


# ---------------------------------------------------------------------------
# (9) device_count in CableCircuitSet — distinct across all circuits
# ---------------------------------------------------------------------------


def test_device_count_across_circuits() -> None:
    """device_count in CableCircuitSet sums distinct devices from all circuits."""
    splines = [
        _spline("s1", (0.0, 0.0), (5.0, 0.0)),  # circuit A
        _spline("s2", (20.0, 0.0), (25.0, 0.0)),  # circuit B
    ]
    devices = [
        _device("d1", "Lum", bbox=(-0.5, -0.5, 0.5, 0.5)),
        _device("d2", "Lum", bbox=(4.5, -0.5, 5.5, 0.5)),
        _device("d3", "Lum", bbox=(24.5, -0.5, 25.5, 0.5)),
    ]
    graph = build_cable_graph(splines=splines, devices=devices)
    result = partition_circuits(graph)

    assert result.device_count == 3  # d1, d2 in one circuit; d3 in the other


# ---------------------------------------------------------------------------
# (10) CableCircuit inner collections are sorted
# ---------------------------------------------------------------------------


def test_inner_tuples_are_sorted() -> None:
    """node_ids, spline_entity_ids, device_entity_ids must all be sorted."""
    splines = [
        _spline("z_spline", (0.0, 0.0), (5.0, 0.0)),
        _spline("a_spline", (5.0, 0.0), (10.0, 0.0)),
    ]
    devices = [
        _device("z_dev", "Lum", bbox=(9.5, -0.5, 10.5, 0.5)),
        _device("a_dev", "Lum", bbox=(-0.5, -0.5, 0.5, 0.5)),
    ]
    graph = build_cable_graph(splines=splines, devices=devices)
    result = partition_circuits(graph)

    assert result.circuit_count == 1
    circuit = result.circuits[0]

    assert list(circuit.spline_entity_ids) == sorted(circuit.spline_entity_ids)
    assert list(circuit.node_ids) == sorted(circuit.node_ids)
    assert list(circuit.device_entity_ids) == sorted(circuit.device_entity_ids)
    # Devices should be sorted: "a_dev" before "z_dev"
    assert circuit.device_entity_ids[0] == "a_dev"
    assert circuit.device_entity_ids[1] == "z_dev"
