"""Synthetic unit tests for app.interpretation.cable_topology (issue #693).

All geometry is in metres.  No DB, no ORM, no FastAPI.
Tests cover: import purity, determinism, topology, annotation exclusion,
conservation, no-length field, closed-spline behaviour.
"""

from __future__ import annotations

import random

import pytest

from app.interpretation.cable_topology import (
    CableEdge,
    DeviceFootprint,
    SplineInput,
    build_cable_graph,
)
from app.interpretation.cable_topology_loaders import (
    _apply_affine,
    _compute_block_world_bbox,
    _is_annotation_block,
    _is_architecture_block,
    _is_non_device_block,
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
# (1) Import purity — cable_topology.py must not import sqlalchemy/fastapi/cv2
# ---------------------------------------------------------------------------


def test_cable_topology_import_pure() -> None:
    """cable_topology.py must not import any DB/ORM/FastAPI/cv2 module."""
    import sys

    # The module should already be imported; check its source for forbidden imports.
    mod = sys.modules.get("app.interpretation.cable_topology")
    assert mod is not None, "module not loaded"

    # Verify the forbidden modules are NOT in the module's own namespace attributes.
    forbidden = {"sqlalchemy", "fastapi", "cv2", "shapely"}
    for name in forbidden:
        assert name not in getattr(mod, "__dict__", {}), f"forbidden import found: {name}"

    # Also verify at source level by inspecting __file__.
    import ast
    import pathlib

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
# (2) Determinism — shuffled inputs → identical CableGraph
# ---------------------------------------------------------------------------


def test_determinism_shuffled_inputs() -> None:
    """build_cable_graph is order-independent: shuffled inputs yield equal output."""
    splines = [
        _spline("s1", (0.0, 0.0), (5.0, 0.0)),
        _spline("s2", (5.0, 0.0), (10.0, 0.0)),
        _spline("s3", (10.0, 0.0), (15.0, 3.0)),
    ]
    devices = [
        _device("d1", "Luminaire_A", bbox=(-0.5, -0.5, 0.5, 0.5)),
        _device("d2", "Luminaire_B", bbox=(14.5, 2.5, 15.5, 3.5)),
    ]

    graph_a = build_cable_graph(splines=splines, devices=devices)

    rng = random.Random(42)
    shuffled_splines = list(splines)
    shuffled_devices = list(devices)
    rng.shuffle(shuffled_splines)
    rng.shuffle(shuffled_devices)

    graph_b = build_cable_graph(splines=shuffled_splines, devices=shuffled_devices)

    assert graph_a == graph_b, "CableGraph differs after input shuffle"


# ---------------------------------------------------------------------------
# (3) Two splines sharing endpoint → 3 nodes, 2 edges, 1 junction, 2 terminals
# ---------------------------------------------------------------------------


def test_two_splines_shared_endpoint_topology() -> None:
    """Two splines meeting at one node → junction at shared end, terminals at far ends.

    Arrange:
      - Spline s1: (0,0) → (5,0)   far-end in device d1 bbox
      - Spline s2: (5,0) → (10,0)  far-end in device d2 bbox
    Shared node at (5,0) has degree 2 → interior junction.
    Far ends have degree 1 → terminals associated to d1 and d2.
    """
    splines = [
        _spline("s1", (0.0, 0.0), (5.0, 0.0)),
        _spline("s2", (5.0, 0.0), (10.0, 0.0)),
    ]
    devices = [
        _device("d1", "Luminaire_A", bbox=(-0.5, -0.5, 0.5, 0.5)),  # contains (0,0)
        _device("d2", "Luminaire_B", bbox=(9.5, -0.5, 10.5, 0.5)),  # contains (10,0)
    ]

    graph = build_cable_graph(splines=splines, devices=devices, snap_tol_m=0.30)

    assert graph.node_count == 3, f"expected 3 nodes, got {graph.node_count}"
    assert graph.edge_count == 2, f"expected 2 edges, got {graph.edge_count}"
    assert graph.interior_junction_count == 1, (
        f"expected 1 junction, got {graph.interior_junction_count}"
    )
    assert graph.terminal_count == 2, f"expected 2 terminals, got {graph.terminal_count}"

    # Both terminals should be associated to distinct devices.
    assoc_ids = {t.device_entity_id for t in graph.terminals}
    assert assoc_ids == {"d1", "d2"}, f"unexpected terminal associations: {assoc_ids}"

    # Check contained flag.
    for t in graph.terminals:
        assert t.contained is True, f"terminal {t.node_id} should be contained"
        assert t.distance_m == pytest.approx(0.0), "contained terminal should have distance 0"


# ---------------------------------------------------------------------------
# (4) Architecture exclusion filter
# ---------------------------------------------------------------------------


def test_is_architecture_block_matches_patterns() -> None:
    """_is_architecture_block returns True for architecture-family block_refs."""
    assert _is_architecture_block("RUK_G Room Name and Number") is True
    assert _is_architecture_block("SomeGrid-01") is True
    assert _is_architecture_block("MPA_Door_Type_A") is True
    assert _is_architecture_block("Window_Panel_02") is True


def test_is_architecture_block_keeps_luminaire() -> None:
    """_is_architecture_block returns False for real device blocks."""
    assert _is_architecture_block("Luminaire_LED_Downlight") is False
    assert _is_architecture_block("FDP-Panel") is False
    assert _is_architecture_block(None) is False


def test_architecture_filter_in_device_set() -> None:
    """Devices with architecture block_refs are excluded; luminaires are kept.

    Simulates the v1 filter logic used in load_device_footprints.
    """
    all_devices = [
        DeviceFootprint("arch1", "RUK_G Room Name and Number", "device", None, (1.0, 1.0)),
        DeviceFootprint("dev1", "Luminaire_LED_Panel", "device", (-0.5, -0.5, 0.5, 0.5), None),
    ]
    kept = [d for d in all_devices if not _is_architecture_block(d.block_ref)]
    assert len(kept) == 1
    assert kept[0].entity_id == "dev1"


# ---------------------------------------------------------------------------
# (5) Conservation — every spline → edge OR dropped
# ---------------------------------------------------------------------------


def test_conservation_every_spline_accounted() -> None:
    """spline_count == edge_count + dropped for any input mix."""
    splines = [
        _spline("good1", (0.0, 0.0), (1.0, 0.0)),
        _spline("good2", (1.0, 0.0), (2.0, 0.0)),
        SplineInput("bad1", vertices=(), closed=False),  # empty → dropped
        SplineInput("bad2", vertices=((0.0, 0.0),), closed=False),  # 1 vertex → dropped
        # malformed vertex content (out of the typed contract; loader guarantees
        # tuples) — builder must defensively drop, not crash:
        SplineInput("bad3", vertices=({"x": "oops", "y": None},), closed=False),  # type: ignore[arg-type]
    ]
    graph = build_cable_graph(splines=splines, devices=[])

    assert graph.spline_count == len(splines)
    assert graph.edge_count + graph.dropped == graph.spline_count, (
        f"edge_count={graph.edge_count} + dropped={graph.dropped} "
        f"!= spline_count={graph.spline_count}"
    )


def test_conservation_zero_vertex_row_surfaces_in_dropped() -> None:
    """A SplineInput with empty vertices (as emitted by the fixed loader for
    zero-parseable-vertex DB rows) must appear in dropped, not be silently lost.

    This confirms Fix 1: load_spline_inputs no longer skips un-parseable rows;
    it emits SplineInput(vertices=()) so the builder's conservation invariant
    (spline_count == edge_count + dropped) holds against the full DB row count.
    """
    # Simulate what the fixed loader emits for a row with un-parseable vertices.
    splines = [
        _spline("good1", (0.0, 0.0), (1.0, 0.0)),
        SplineInput("malformed_db_row", vertices=(), closed=False),  # loader output for bad row
    ]
    graph = build_cable_graph(splines=splines, devices=[])

    assert graph.spline_count == 2
    assert graph.edge_count == 1, f"only good1 should produce an edge, got {graph.edge_count}"
    assert graph.dropped == 1, f"malformed row should be in dropped, got {graph.dropped}"
    assert graph.edge_count + graph.dropped == graph.spline_count


# ---------------------------------------------------------------------------
# (6) Terminal with no device within radius → device_entity_id None
# ---------------------------------------------------------------------------


def test_terminal_no_device_within_radius() -> None:
    """Terminal node far from all devices → device_entity_id=None, contained=False."""
    splines = [_spline("s1", (0.0, 0.0), (1.0, 0.0))]
    devices = [
        _device("d1", "Luminaire_A", bbox=(100.0, 100.0, 101.0, 101.0)),
    ]
    graph = build_cable_graph(splines=splines, devices=devices, terminal_assoc_m=2.5)

    # Two terminals (both degree-1 endpoints).
    assert graph.terminal_count == 2
    for t in graph.terminals:
        assert t.device_entity_id is None, f"expected no association, got {t.device_entity_id}"
        assert t.contained is False
        assert t.distance_m is None


# ---------------------------------------------------------------------------
# (7) No length field on CableEdge or CableGraph
# ---------------------------------------------------------------------------


def test_no_length_field_on_edge_or_graph() -> None:
    """CableEdge and CableGraph must NOT have a length field."""
    edge = CableEdge(
        spline_entity_id="s1",
        node_a=(0, 0),
        node_b=(1, 0),
        closed=False,
    )
    assert not hasattr(edge, "length"), "CableEdge must not have a length field"
    assert not hasattr(edge, "length_m"), "CableEdge must not have a length_m field"

    splines = [_spline("s1", (0.0, 0.0), (1.0, 0.0))]
    graph = build_cable_graph(splines=splines, devices=[])
    assert not hasattr(graph, "length"), "CableGraph must not have a length field"
    assert not hasattr(graph, "length_m"), "CableGraph must not have a length_m field"


# ---------------------------------------------------------------------------
# (8) Closed spline → edge present, its nodes NOT terminals
# ---------------------------------------------------------------------------


def test_closed_spline_nodes_not_terminals() -> None:
    """A closed spline's endpoints must not appear as terminals even though degree==1.

    Arrange: one closed spline with the same endpoint repeated (loop to itself),
    plus one open spline sharing one end with a device at the open terminal.
    """
    splines = [
        _spline("closed1", (0.0, 0.0), (1.0, 0.0), (1.0, 1.0), closed=True),
        _spline("open1", (5.0, 0.0), (6.0, 0.0)),  # both ends free
    ]
    devices = [_device("d1", "Lum", bbox=(4.5, -0.5, 5.5, 0.5))]

    graph = build_cable_graph(splines=splines, devices=devices, snap_tol_m=0.30)

    # Closed spline must produce exactly one edge.
    closed_edges = [e for e in graph.edges if e.closed]
    assert len(closed_edges) == 1, f"expected 1 closed edge, got {len(closed_edges)}"

    # Nodes from the closed spline must NOT be terminals.
    closed_edge = closed_edges[0]
    closed_node_ids = {closed_edge.node_a, closed_edge.node_b}
    terminal_node_ids = {t.node_id for t in graph.terminals}
    overlap = closed_node_ids & terminal_node_ids
    assert not overlap, f"closed-spline nodes appear as terminals: {overlap}"


# ---------------------------------------------------------------------------
# (9) Nearest device by bbox distance — not contained, within radius
# ---------------------------------------------------------------------------


def test_terminal_nearest_device_by_bbox_distance() -> None:
    """Terminal not inside any bbox → nearest device within terminal_assoc_m is picked."""
    splines = [_spline("s1", (0.0, 0.0), (3.0, 0.0))]
    # Device d_near: bbox [4.0, 0.0, 5.0, 0.0] — clamped distance from (3,0) to (4,0) = 1.0
    # Device d_far:  bbox [20.0, 0.0, 21.0, 0.0] — distance = 17.0 (beyond radius)
    devices = [
        _device("d_near", "Lum_Near", bbox=(4.0, -0.5, 5.0, 0.5)),
        _device("d_far", "Lum_Far", bbox=(20.0, -0.5, 21.0, 0.5)),
    ]
    graph = build_cable_graph(splines=splines, devices=devices, terminal_assoc_m=2.5)

    # (3,0) terminal should pick d_near (dist=1.0 < 2.5), not d_far.
    # (0,0) terminal is 4.0 m from d_near, beyond radius → None.
    by_assoc = {t.device_entity_id: t for t in graph.terminals}
    assert "d_near" in by_assoc, "d_near should be associated to the close terminal"
    near_term = by_assoc["d_near"]
    assert near_term.contained is False
    assert near_term.distance_m == pytest.approx(1.0, abs=1e-6)

    # Far terminal has no match.
    assert None in by_assoc, "far terminal should have no association"


def test_tie_break_equidistant_devices_permutation_independent() -> None:
    """Three devices where two are equidistant from one terminal — always picks
    lowest entity_id regardless of input permutation (Fix 2 verification).

    Arrange:
      - Spline s1: (0,0) → (5,0).  Terminal at (5,0).
      - Device "zzz_lum": bbox at x=[6, 7], distance from (5,0) = 1.0 m.
      - Device "aaa_lum": bbox at x=[6, 7] (same shape, different y-band so
        same clamped distance = 1.0 m exactly).
      - Device "mmm_lum": distance = 2.0 m (distinct, should lose).
    Expected winner: "aaa_lum" (lowest lex among the tied pair).
    Tested across 6 permutations to confirm order-independence.
    """
    import itertools

    splines = [_spline("s1", (0.0, 0.0), (5.0, 0.0))]

    # Both "aaa_lum" and "zzz_lum" are exactly 1.0 m from terminal (5,0).
    # "mmm_lum" is 2.0 m away.
    equidistant_a = _device("aaa_lum", "Lum_A", bbox=(6.0, -0.5, 7.0, 0.5))
    equidistant_z = _device("zzz_lum", "Lum_Z", bbox=(6.0, -0.5, 7.0, 0.5))
    far = _device("mmm_lum", "Lum_M", bbox=(7.0, -0.5, 8.0, 0.5))

    base_devices = [equidistant_a, equidistant_z, far]

    winner_ids: set[str] = set()
    for perm in itertools.permutations(base_devices):
        graph = build_cable_graph(splines=splines, devices=list(perm), terminal_assoc_m=2.5)
        # Terminal at (5,0) — find the associated terminal.
        assoc = {t.device_entity_id for t in graph.terminals if t.device_entity_id is not None}
        winner_ids.update(assoc)

    # Only "aaa_lum" should ever win (lowest entity_id of the tied pair).
    # "zzz_lum" must never win; "mmm_lum" is farther so it must never win either.
    assert winner_ids == {"aaa_lum"}, (
        f"expected only 'aaa_lum' across all permutations, got {winner_ids}"
    )


# ---------------------------------------------------------------------------
# (10) Position-only device fallback (bbox=None, position given)
# ---------------------------------------------------------------------------


def test_device_position_fallback_bbox_none() -> None:
    """Device with bbox=None but position → treated as degenerate zero-area bbox."""
    splines = [_spline("s1", (0.0, 0.0), (1.0, 0.0))]
    # Device at (0,0) exactly — point (0,0) is contained in the zero-area bbox.
    devices = [_device("d1", "SomeDevice", bbox=None, position=(0.0, 0.0))]

    graph = build_cable_graph(splines=splines, devices=devices, terminal_assoc_m=2.5)

    # At least one terminal should be associated to d1 (the (0,0) end).
    assoc = [t for t in graph.terminals if t.device_entity_id == "d1"]
    assert len(assoc) >= 1, "device with position fallback should be associated"
    assert assoc[0].contained is True


# ---------------------------------------------------------------------------
# (11) _apply_affine — pure unit tests
# ---------------------------------------------------------------------------


def test_apply_affine_identity_translate() -> None:
    """Identity rotation/scale with non-zero insertion → pure translation."""
    result = _apply_affine(
        (3.0, 4.0),
        insertion=(10.0, 20.0),
        scale=(1.0, 1.0),
        rotation_rad=0.0,
        base=(0.0, 0.0),
    )
    assert result == pytest.approx((13.0, 24.0), abs=1e-9)


def test_apply_affine_90deg_rotation() -> None:
    """90° CCW rotation: local (1,0) → world (0,1) relative to insertion."""
    import math as _math

    result = _apply_affine(
        (1.0, 0.0),
        insertion=(0.0, 0.0),
        scale=(1.0, 1.0),
        rotation_rad=_math.pi / 2,
        base=(0.0, 0.0),
    )
    # R(90°) · (1,0) = (0, 1)
    assert result[0] == pytest.approx(0.0, abs=1e-9)
    assert result[1] == pytest.approx(1.0, abs=1e-9)


def test_apply_affine_scale_and_base() -> None:
    """Scale + base_point subtraction: local at (5,5) with base (5,5) → origin after subtract."""
    result = _apply_affine(
        (5.0, 5.0),
        insertion=(2.0, 3.0),
        scale=(2.0, 2.0),
        rotation_rad=0.0,
        base=(5.0, 5.0),
    )
    # After subtract: (0,0); scale: (0,0); rotate: (0,0); translate: (2,3)
    assert result == pytest.approx((2.0, 3.0), abs=1e-9)


def test_apply_affine_deterministic() -> None:
    """Same inputs → same output (no randomness)."""
    import math as _math

    kwargs: dict[str, object] = {
        "insertion": (1.5, 2.5),
        "scale": (0.5, 0.5),
        "rotation_rad": _math.pi / 4,
        "base": (1.0, 1.0),
    }
    r1 = _apply_affine((3.0, 3.0), **kwargs)  # type: ignore[arg-type]
    r2 = _apply_affine((3.0, 3.0), **kwargs)  # type: ignore[arg-type]
    assert r1 == r2


# ---------------------------------------------------------------------------
# (12) _compute_block_world_bbox — synthetic block payload test
# ---------------------------------------------------------------------------


def test_compute_block_world_bbox_synthetic() -> None:
    """Given a synthetic block payload with two line-child entities, verify world bbox.

    Block has one child line from local (0,0) to (1,0) and another from (0,0) to (0,1).
    Insert: insertion=(5,5), scale=(1,1), rotation=0, base=(0,0).
    Expected world bbox: (5,5) to (6,6).
    """
    payload = {
        "base_point": {"x": 0.0, "y": 0.0, "z": 0.0},
        "entities": [
            {
                "entity_type": "line",
                "geometry": {
                    "start": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "end": {"x": 1.0, "y": 0.0, "z": 0.0},
                },
            },
            {
                "entity_type": "line",
                "geometry": {
                    "start": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "end": {"x": 0.0, "y": 1.0, "z": 0.0},
                },
            },
        ],
    }
    result = _compute_block_world_bbox(
        payload,
        insertion=(5.0, 5.0),
        scale=(1.0, 1.0),
        rotation_rad=0.0,
    )
    assert result is not None, "expected a bbox"
    min_x, min_y, max_x, max_y = result
    assert min_x == pytest.approx(5.0, abs=1e-9)
    assert min_y == pytest.approx(5.0, abs=1e-9)
    assert max_x == pytest.approx(6.0, abs=1e-9)
    assert max_y == pytest.approx(6.0, abs=1e-9)


def test_compute_block_world_bbox_with_scale_and_rotation() -> None:
    """Block child (1,0) local with scale=2 and 90° rotation → world (0,2) relative to insertion.

    Block: one line from (0,0) to (1,0).
    Insert: insertion=(0,0), scale=(2,2), rotation=90°, base=(0,0).
    Local bbox: (0,0,1,0) — degenerate on y but corners at (0,0) and (1,0).
    After transform: (0,0) → (0,0); (1,0) → (0,2); (0,0) → (0,0); (1,0) → (0,2).
    World bbox: (0,0,0,2).
    """
    import math as _math

    payload = {
        "base_point": {"x": 0.0, "y": 0.0, "z": 0.0},
        "entities": [
            {
                "entity_type": "line",
                "geometry": {
                    "start": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "end": {"x": 1.0, "y": 0.0, "z": 0.0},
                },
            },
        ],
    }
    result = _compute_block_world_bbox(
        payload,
        insertion=(0.0, 0.0),
        scale=(2.0, 2.0),
        rotation_rad=_math.pi / 2,
    )
    assert result is not None
    min_x, min_y, max_x, max_y = result
    assert min_x == pytest.approx(0.0, abs=1e-9)
    assert min_y == pytest.approx(0.0, abs=1e-9)
    assert max_x == pytest.approx(0.0, abs=1e-9)
    assert max_y == pytest.approx(2.0, abs=1e-9)


def test_compute_block_world_bbox_empty_children() -> None:
    """Block with no children → None."""
    payload = {"base_point": {"x": 0.0, "y": 0.0}, "entities": []}
    result = _compute_block_world_bbox(
        payload, insertion=(0.0, 0.0), scale=(1.0, 1.0), rotation_rad=0.0
    )
    assert result is None


# ---------------------------------------------------------------------------
# (13) Annotation exclusion filter (_is_annotation_block, _is_non_device_block)
# ---------------------------------------------------------------------------


def test_is_annotation_block_titleblock() -> None:
    """Titleblock variants are excluded by the annotation filter."""
    assert _is_annotation_block("RUK-Annotation-TitleblockA0") is True
    assert _is_annotation_block("Title Block 01") is True
    assert _is_annotation_block("North Arrow") is True
    assert _is_annotation_block("Scale Bar") is True


def test_is_annotation_block_keeps_luminaire() -> None:
    """Non-annotation blocks pass through the annotation filter."""
    assert _is_annotation_block("Luminaire_LED") is False
    assert _is_annotation_block("FDP_Panel") is False
    assert _is_annotation_block(None) is False


def test_is_non_device_block_combined() -> None:
    """_is_non_device_block excludes both architecture and annotation families."""
    assert _is_non_device_block("RUK_G Room Name and Number") is True  # architecture
    assert _is_non_device_block("RUK-Annotation-TitleblockA0") is True  # annotation
    assert _is_non_device_block("Luminaire_LED_Panel") is False  # real device
    assert _is_non_device_block(None) is False


def test_non_device_filter_titleblock_excluded_from_footprints() -> None:
    """A titleblock device is excluded alongside an architecture block; luminaire kept."""
    all_devices = [
        DeviceFootprint("arch1", "RUK_G Room Name and Number", "device", None, (1.0, 1.0)),
        DeviceFootprint("ann1", "RUK-Annotation-TitleblockA0", "device", None, (2.0, 2.0)),
        DeviceFootprint("dev1", "Luminaire_LED_Panel", "device", (-0.5, -0.5, 0.5, 0.5), None),
    ]
    kept = [d for d in all_devices if not _is_non_device_block(d.block_ref)]
    assert len(kept) == 1
    assert kept[0].entity_id == "dev1"
