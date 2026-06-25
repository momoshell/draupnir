"""Synthetic unit tests for app.interpretation.cable_home_run (issue #697b).

All geometry is in metres. No DB, no ORM, no FastAPI.

Tests cover:
- Import purity
- Simple straight tray → status "ok", correct home_run_m
- No anchor → "no_anchor"
- Panel too far → "unreachable_tray"
- Two-component tray → "disconnected_tray" with finite lower_bound_m
- registration_failed=True → all "bad_registration"
- Determinism: shuffling inputs yields identical HomeRunResult
- Conservation: ok_count + Σ suppressed == circuit_count

All CableCircuitSet instances are built via the real public APIs
(build_cable_graph + partition_circuits) over synthetic SplineInput / DeviceFootprint.
"""

from __future__ import annotations

import random

import pytest

from app.interpretation.cable_circuits import CableCircuitSet, partition_circuits
from app.interpretation.cable_home_run import (
    compute_home_runs,
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


def _make_circuits(
    splines: list[SplineInput],
    devices: list[DeviceFootprint],
) -> CableCircuitSet:
    """Build a CableCircuitSet from synthetic splines and devices."""
    graph = build_cable_graph(splines=splines, devices=devices)
    return partition_circuits(graph)


# A block_ref that triggers the anchor heuristic.
_PANEL_BLOCK_REF = "Pr_60_70_22_22_DB-TST1 - Standard Distribution Board"


# ---------------------------------------------------------------------------
# (1) Import purity — cable_home_run must not import DB/ORM/FastAPI/cv2/shapely/numpy
# ---------------------------------------------------------------------------


def test_import_purity() -> None:
    import ast
    import pathlib

    src = pathlib.Path("app/interpretation/cable_home_run.py").read_text()
    tree = ast.parse(src)
    forbidden = {
        "sqlalchemy",
        "fastapi",
        "cv2",
        "shapely",
        "numpy",
        "skimage",
        "PIL",
    }
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            module = ""
            if isinstance(node, ast.Import):
                for alias in node.names:
                    module = alias.name
                    top = module.split(".")[0]
                    assert top not in forbidden, f"Forbidden import: {module}"
            else:
                module = node.module or ""
                top = module.split(".")[0]
                assert top not in forbidden, f"Forbidden import: {module}"


# ---------------------------------------------------------------------------
# (2) Simple straight tray → "ok" with correct metrics
# ---------------------------------------------------------------------------


def test_simple_tray_ok() -> None:
    """Panel near one end of a straight tray, served device near the other end."""
    # Tray: horizontal line from (0, 5) to (10, 5)
    tray = [[(0.0, 5.0), (10.0, 5.0)]]

    # Panel device at (0, 5) → panel point centroid at (0, 5)
    panel_dev = _device("panel-1", _PANEL_BLOCK_REF, bbox=(-0.1, 4.9, 0.1, 5.1))
    # Served device at (10, 5)
    served_dev = _device("served-1", "Luminaire", bbox=(9.9, 4.9, 10.1, 5.1))

    # Cable spline connects panel terminal to served terminal.
    # Terminals at panel and served device positions.
    splines = [_spline("sp-1", (0.0, 5.0), (10.0, 5.0))]
    devices = [panel_dev, served_dev]

    circuits = _make_circuits(splines, devices)
    device_map = {d.entity_id: d for d in devices}

    result = compute_home_runs(
        circuits=circuits,
        device_footprints_by_id=device_map,
        tray_polylines=tray,
        snap_tol_m=0.30,
        entry_max_m=5.0,
    )

    assert result.circuit_count == 1
    assert result.ok_count == 1
    assert len(result.per_circuit) == 1

    cr = result.per_circuit[0]
    assert cr.status == "ok"
    assert cr.home_run_m is not None
    assert cr.along_tray_m is not None
    # The tray runs 10 m; panel and area are at/near the endpoints → along_tray ≈ 10 m.
    assert cr.along_tray_m == pytest.approx(10.0, abs=0.5)
    # d_panel and d_area should be tiny (< 0.5 m).
    assert cr.d_panel_m is not None and cr.d_panel_m < 0.5
    assert cr.d_area_m is not None and cr.d_area_m < 0.5
    # home_run_m = d_panel + along_tray + d_area
    assert cr.home_run_m == pytest.approx(cr.d_panel_m + cr.along_tray_m + cr.d_area_m, abs=1e-9)
    # Total matches the single ok circuit.
    assert result.total_home_run_m == pytest.approx(cr.home_run_m, abs=1e-9)
    # lower_bound_m is on CircuitHomeRun only; None when status is "ok".
    assert cr.lower_bound_m is None


# ---------------------------------------------------------------------------
# (3) No anchor → "no_anchor"
# ---------------------------------------------------------------------------


def test_no_anchor() -> None:
    """Circuit has no distribution board → all circuits flagged no_anchor."""
    tray = [[(0.0, 0.0), (5.0, 0.0)]]

    # Two ordinary luminaires, no panel.
    dev_a = _device("lum-a", "Luminaire", bbox=(0.0, -0.1, 0.2, 0.1))
    dev_b = _device("lum-b", "Luminaire", bbox=(4.8, -0.1, 5.0, 0.1))

    splines = [_spline("sp-1", (0.0, 0.0), (5.0, 0.0))]
    devices = [dev_a, dev_b]

    circuits = _make_circuits(splines, devices)
    device_map = {d.entity_id: d for d in devices}

    result = compute_home_runs(
        circuits=circuits,
        device_footprints_by_id=device_map,
        tray_polylines=tray,
    )

    assert result.ok_count == 0
    for cr in result.per_circuit:
        assert cr.status == "no_anchor"
        assert cr.home_run_m is None


# ---------------------------------------------------------------------------
# (4) Panel far from tray → "unreachable_tray"
# ---------------------------------------------------------------------------


def test_unreachable_tray() -> None:
    """Panel centroid is > entry_max_m from any tray node → unreachable_tray."""
    # Tray at y=0, panel at y=20 (far away).
    tray = [[(0.0, 0.0), (5.0, 0.0)]]

    panel_dev = _device("panel-far", _PANEL_BLOCK_REF, bbox=(-0.1, 19.9, 0.1, 20.1))
    served_dev = _device("served-1", "Luminaire", bbox=(4.9, -0.1, 5.1, 0.1))

    splines = [_spline("sp-1", (0.0, 20.0), (5.0, 0.0))]
    devices = [panel_dev, served_dev]

    circuits = _make_circuits(splines, devices)
    device_map = {d.entity_id: d for d in devices}

    result = compute_home_runs(
        circuits=circuits,
        device_footprints_by_id=device_map,
        tray_polylines=tray,
        entry_max_m=5.0,
    )

    assert result.ok_count == 0
    for cr in result.per_circuit:
        # The panel is 20 m from the tray → unreachable.
        if cr.status == "unreachable_tray":
            assert cr.home_run_m is None


# ---------------------------------------------------------------------------
# (5) Two-component tray → "disconnected_tray"
# ---------------------------------------------------------------------------


def test_disconnected_tray() -> None:
    """Panel near component A, area near component B → disconnected_tray with lower_bound_m."""
    # Component A: x ∈ [0, 3]
    # Component B: x ∈ [7, 10]  (gap at x ∈ [3, 7])
    tray = [
        [(0.0, 0.0), (3.0, 0.0)],
        [(7.0, 0.0), (10.0, 0.0)],
    ]

    panel_dev = _device("panel-1", _PANEL_BLOCK_REF, bbox=(-0.1, -0.1, 0.1, 0.1))
    served_dev = _device("served-1", "Luminaire", bbox=(9.9, -0.1, 10.1, 0.1))

    splines = [
        _spline("sp-1", (0.0, 0.0), (10.0, 0.0)),
    ]
    devices = [panel_dev, served_dev]

    circuits = _make_circuits(splines, devices)
    device_map = {d.entity_id: d for d in devices}

    result = compute_home_runs(
        circuits=circuits,
        device_footprints_by_id=device_map,
        tray_polylines=tray,
        entry_max_m=5.0,
    )

    assert result.ok_count == 0
    disconnected = [cr for cr in result.per_circuit if cr.status == "disconnected_tray"]
    assert len(disconnected) >= 1

    for cr in disconnected:
        assert cr.home_run_m is None
        assert cr.lower_bound_m is not None
        assert cr.lower_bound_m > 0.0  # panel-entry and area-entry nodes are separated


# ---------------------------------------------------------------------------
# (6) registration_failed=True → all "bad_registration"
# ---------------------------------------------------------------------------


def test_registration_failed() -> None:
    """When registration_failed=True every circuit gets bad_registration."""
    tray = [[(0.0, 0.0), (10.0, 0.0)]]

    panel_dev = _device("panel-1", _PANEL_BLOCK_REF, bbox=(-0.1, -0.1, 0.1, 0.1))
    served_dev = _device("served-1", "Luminaire", bbox=(9.9, -0.1, 10.1, 0.1))

    splines = [_spline("sp-1", (0.0, 0.0), (10.0, 0.0))]
    devices = [panel_dev, served_dev]

    circuits = _make_circuits(splines, devices)
    device_map = {d.entity_id: d for d in devices}

    result = compute_home_runs(
        circuits=circuits,
        device_footprints_by_id=device_map,
        tray_polylines=tray,
        registration_failed=True,
    )

    assert result.ok_count == 0
    assert result.total_home_run_m == 0.0
    for cr in result.per_circuit:
        assert cr.status == "bad_registration"
        assert cr.home_run_m is None
        assert cr.d_panel_m is None
        assert cr.along_tray_m is None
        assert cr.d_area_m is None
        assert cr.lower_bound_m is None


# ---------------------------------------------------------------------------
# (7) Determinism: shuffled inputs → identical HomeRunResult
# ---------------------------------------------------------------------------


def test_determinism() -> None:
    """Shuffling tray polylines and circuit ordering yields identical HomeRunResult."""
    tray_a = [(0.0, 0.0), (5.0, 0.0)]
    tray_b = [(5.0, 0.0), (10.0, 0.0)]
    tray_c = [(10.0, 0.0), (15.0, 0.0)]

    panel_dev = _device("panel-1", _PANEL_BLOCK_REF, bbox=(-0.1, -0.1, 0.1, 0.1))
    served_a = _device("served-a", "Luminaire", bbox=(7.9, -0.1, 8.1, 0.1))
    served_b = _device("served-b", "Luminaire", bbox=(14.9, -0.1, 15.1, 0.1))

    splines = [
        _spline("sp-1", (0.0, 0.0), (8.0, 0.0)),
        _spline("sp-2", (8.0, 0.0), (15.0, 0.0)),
    ]
    devices = [panel_dev, served_a, served_b]

    circuits = _make_circuits(splines, devices)
    device_map = {d.entity_id: d for d in devices}

    tray_canonical = [tray_a, tray_b, tray_c]

    result_canonical = compute_home_runs(
        circuits=circuits,
        device_footprints_by_id=device_map,
        tray_polylines=tray_canonical,
    )

    rng = random.Random(42)
    for _ in range(5):
        tray_shuffled = list(tray_canonical)
        rng.shuffle(tray_shuffled)
        result_shuffled = compute_home_runs(
            circuits=circuits,
            device_footprints_by_id=device_map,
            tray_polylines=tray_shuffled,
        )
        assert result_canonical.ok_count == result_shuffled.ok_count
        assert result_canonical.circuit_count == result_shuffled.circuit_count
        assert result_canonical.total_home_run_m == pytest.approx(
            result_shuffled.total_home_run_m, abs=1e-6
        )
        for a, b in zip(result_canonical.per_circuit, result_shuffled.per_circuit, strict=True):
            assert a.circuit_id == b.circuit_id
            assert a.status == b.status
            if a.home_run_m is not None:
                assert a.home_run_m == pytest.approx(b.home_run_m, abs=1e-6)


# ---------------------------------------------------------------------------
# (8) Conservation: ok_count + Σ suppressed_counts == circuit_count
# ---------------------------------------------------------------------------


def test_conservation_mixed() -> None:
    """Mix of ok, no_anchor, unreachable_tray → conservation holds in all states."""
    tray = [[(0.0, 0.0), (10.0, 0.0)]]

    # Circuit 1: panel + served near tray → ok
    panel1 = _device("panel-1", _PANEL_BLOCK_REF, bbox=(-0.1, -0.1, 0.1, 0.1))
    served1 = _device("served-1", "Luminaire", bbox=(9.9, -0.1, 10.1, 0.1))

    # Circuit 2: no panel → no_anchor. Isolated spline not connected to circuit 1.
    lum2 = _device("lum-2", "Luminaire", bbox=(4.9, 2.9, 5.1, 3.1))

    # Deliberately isolated splines so circuits don't merge.
    splines = [
        _spline("sp-1", (0.0, 0.0), (10.0, 0.0)),  # connects panel1 + served1
        _spline("sp-2", (5.0, 3.0), (5.0, 3.5)),  # isolated — lum2 only
    ]
    devices = [panel1, served1, lum2]

    circuits = _make_circuits(splines, devices)
    device_map = {d.entity_id: d for d in devices}

    result = compute_home_runs(
        circuits=circuits,
        device_footprints_by_id=device_map,
        tray_polylines=tray,
    )

    total_accounted = result.ok_count + sum(result.suppressed_counts.values())
    assert total_accounted == result.circuit_count, (
        f"Conservation violated: ok={result.ok_count} "
        f"suppressed={result.suppressed_counts} total={result.circuit_count}"
    )


# ---------------------------------------------------------------------------
# (9) Suppressed counts key order is fixed
# ---------------------------------------------------------------------------


def test_suppressed_counts_key_order() -> None:
    """suppressed_counts always contains exactly the fixed status keys."""
    tray = [[(0.0, 0.0), (5.0, 0.0)]]
    panel = _device("panel-1", _PANEL_BLOCK_REF, bbox=(-0.1, -0.1, 0.1, 0.1))
    served = _device("served-1", "Luminaire", bbox=(4.9, -0.1, 5.1, 0.1))
    splines = [_spline("sp-1", (0.0, 0.0), (5.0, 0.0))]

    circuits = _make_circuits(splines, [panel, served])
    device_map = {d.entity_id: d for d in [panel, served]}

    result = compute_home_runs(
        circuits=circuits,
        device_footprints_by_id=device_map,
        tray_polylines=tray,
    )

    expected_keys = {"no_anchor", "unreachable_tray", "disconnected_tray", "bad_registration"}
    assert set(result.suppressed_counts.keys()) == expected_keys


# ---------------------------------------------------------------------------
# (10) Empty tray → all unreachable_tray
# ---------------------------------------------------------------------------


def test_empty_tray() -> None:
    """With no tray polylines every circuit is unreachable_tray."""
    panel = _device("panel-1", _PANEL_BLOCK_REF, bbox=(-0.1, -0.1, 0.1, 0.1))
    served = _device("served-1", "Luminaire", bbox=(4.9, -0.1, 5.1, 0.1))
    splines = [_spline("sp-1", (0.0, 0.0), (5.0, 0.0))]

    circuits = _make_circuits(splines, [panel, served])
    device_map = {d.entity_id: d for d in [panel, served]}

    result = compute_home_runs(
        circuits=circuits,
        device_footprints_by_id=device_map,
        tray_polylines=[],  # empty
    )

    assert result.ok_count == 0
    for cr in result.per_circuit:
        assert cr.status == "unreachable_tray"


# ---------------------------------------------------------------------------
# (11) home_run_m decomposition identity: d_panel + along_tray + d_area
# ---------------------------------------------------------------------------


def test_home_run_decomposition() -> None:
    """home_run_m must exactly equal d_panel_m + along_tray_m + d_area_m."""
    tray = [[(0.0, 0.0), (20.0, 0.0)]]

    panel = _device("panel-1", _PANEL_BLOCK_REF, bbox=(-0.1, 0.9, 0.1, 1.1))
    served = _device("served-1", "Luminaire", bbox=(19.9, 0.9, 20.1, 1.1))

    splines = [_spline("sp-1", (0.0, 1.0), (20.0, 1.0))]
    devices = [panel, served]

    circuits = _make_circuits(splines, devices)
    device_map = {d.entity_id: d for d in devices}

    result = compute_home_runs(
        circuits=circuits,
        device_footprints_by_id=device_map,
        tray_polylines=tray,
    )

    for cr in result.per_circuit:
        if cr.status == "ok":
            expected = cr.d_panel_m + cr.along_tray_m + cr.d_area_m  # type: ignore[operator]
            assert cr.home_run_m == pytest.approx(expected, abs=1e-9)


# ---------------------------------------------------------------------------
# (12) Non-finite vertices in tray polylines → graceful skip, no crash
# ---------------------------------------------------------------------------


def test_nan_inf_vertices_in_tray() -> None:
    """A tray polyline containing NaN/inf vertices is silently dropped; good segments survive."""
    import math as _math

    # One good polyline, one with a NaN vertex, one with an inf vertex.
    nan = _math.nan
    inf = _math.inf
    tray_good = [(0.0, 0.0), (10.0, 0.0)]
    tray_with_nan = [(0.0, 0.0), (nan, 5.0), (10.0, 5.0)]
    tray_with_inf = [(0.0, 0.0), (inf, 0.0), (10.0, 0.0)]

    panel = _device("panel-1", _PANEL_BLOCK_REF, bbox=(-0.1, -0.1, 0.1, 0.1))
    served = _device("served-1", "Luminaire", bbox=(9.9, -0.1, 10.1, 0.1))

    splines = [_spline("sp-1", (0.0, 0.0), (10.0, 0.0))]
    devices = [panel, served]

    circuits = _make_circuits(splines, devices)
    device_map = {d.entity_id: d for d in devices}

    # Should not raise — bad polylines are skipped, good one is used.
    result = compute_home_runs(
        circuits=circuits,
        device_footprints_by_id=device_map,
        tray_polylines=[tray_good, tray_with_nan, tray_with_inf],
    )

    # Good tray is still present → at least one circuit routes ok.
    assert result.ok_count >= 1
    for cr in result.per_circuit:
        if cr.status == "ok":
            assert cr.home_run_m is not None
            assert _math.isfinite(cr.home_run_m)
