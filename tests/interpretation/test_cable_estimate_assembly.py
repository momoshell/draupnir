"""Synthetic unit tests for app.interpretation.cable_estimate_assembly (issue #698a).

All geometry is in metres. No DB, ORM, or FastAPI.

Tests build CableEstimateResult and HomeRunResult via the real pipelines
(SplineInput / DeviceFootprint → build_cable_graph → partition_circuits
→ estimate_cable_in_plan + compute_home_runs) then assemble.

Where the circuit structure is simpler the inputs are constructed directly
from CableEstimateResult/HomeRunResult without invoking the full pipeline —
this keeps the tests focused on the assembler logic, not the upstream stages.
"""

from __future__ import annotations

import json
import math
import random

import pytest

from app.interpretation.cable_circuits import partition_circuits
from app.interpretation.cable_estimate import (
    CableEstimateResult,
    CircuitEstimate,
    estimate_cable_in_plan,
)
from app.interpretation.cable_estimate_assembly import (
    assemble_cable_estimate,
)
from app.interpretation.cable_estimate_params import (
    CATEGORY_LUMINAIRE,
    SOURCE_ASSUMPTION,
    CableEstimateParams,
    DropParam,
    default_estimate_params,
)
from app.interpretation.cable_home_run import (
    CircuitHomeRun,
    HomeRunResult,
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


def _params(spare: float = 0.10) -> CableEstimateParams:
    return CableEstimateParams(
        vertical_drops=(
            DropParam(
                category=CATEGORY_LUMINAIRE,
                drop_m=2.0,
                source=SOURCE_ASSUMPTION,
                basis="test",
            ),
        ),
        spare_fraction=spare,
        spare_source=SOURCE_ASSUMPTION,
        spare_basis="test spare",
    )


def _make_in_plan_result(
    *,
    circuits: tuple[CircuitEstimate, ...],
    total_device_drop_m: float,
    total_in_plan_length_m: float,
    unattributed_device_drop_m: float = 0.0,
    unattributed_device_count: int = 0,
    total_uncategorized_devices: int = 0,
    spare_fraction: float = 0.10,
) -> CableEstimateResult:
    """Build a CableEstimateResult from components without running the full pipeline."""
    params = _params(spare_fraction)
    return CableEstimateResult(
        circuits=circuits,
        total_device_drop_m=total_device_drop_m,
        total_in_plan_length_m=total_in_plan_length_m,
        total_uncategorized_devices=total_uncategorized_devices,
        unattributed_device_count=unattributed_device_count,
        unattributed_device_drop_m=unattributed_device_drop_m,
        spare_fraction=spare_fraction,
        quantity_kind="estimated",
        params_stamp=params.as_stamp(),
        reliability={
            "device_drop_m": "reliable",
            "in_plan_length_m": "schematic_provisional",
            "note": "test",
        },
    )


def _circuit_estimate(
    circuit_id: int,
    device_drop_m: float,
    in_plan_length_m: float,
    device_count: int = 1,
) -> CircuitEstimate:
    return CircuitEstimate(
        circuit_id=circuit_id,
        device_count=device_count,
        uncategorized_device_count=0,
        device_drop_m=device_drop_m,
        drop_breakdown=(),
        in_plan_length_m=in_plan_length_m,
    )


def _make_home_run_result(
    *,
    circuits: tuple[CircuitHomeRun, ...],
) -> HomeRunResult:
    """Build a HomeRunResult from a tuple of CircuitHomeRun objects."""
    ok_count = sum(1 for c in circuits if c.status == "ok")
    total_home_run_m = sum(c.home_run_m for c in circuits if c.status == "ok")  # type: ignore[misc]
    statuses = ("no_anchor", "unreachable_tray", "disconnected_tray", "bad_registration")
    suppressed: dict[str, int] = dict.fromkeys(statuses, 0)
    for c in circuits:
        if c.status != "ok":
            suppressed[c.status] = suppressed.get(c.status, 0) + 1
    return HomeRunResult(
        per_circuit=circuits,
        total_home_run_m=total_home_run_m,
        ok_count=ok_count,
        suppressed_counts=suppressed,
        circuit_count=len(circuits),
    )


def _circuit_home_run_ok(circuit_id: int, home_run_m: float) -> CircuitHomeRun:
    return CircuitHomeRun(
        circuit_id=circuit_id,
        status="ok",
        home_run_m=home_run_m,
        d_panel_m=home_run_m / 3,
        along_tray_m=home_run_m / 3,
        d_area_m=home_run_m / 3,
        lower_bound_m=None,
    )


def _circuit_home_run_suppressed(circuit_id: int, status: str = "no_anchor") -> CircuitHomeRun:
    return CircuitHomeRun(
        circuit_id=circuit_id,
        status=status,
        home_run_m=None,
        d_panel_m=None,
        along_tray_m=None,
        d_area_m=None,
        lower_bound_m=None,
    )


# ---------------------------------------------------------------------------
# Import purity
# ---------------------------------------------------------------------------


def test_import_purity() -> None:
    """The assembly module must not import DB/ORM/FastAPI/shapely/cv2/numpy."""
    import importlib.util
    from pathlib import Path

    mod_name = "app.interpretation.cable_estimate_assembly"
    spec = importlib.util.find_spec(mod_name)
    assert spec is not None

    # Check for forbidden *import statements* (not docstring mentions).
    forbidden = {"sqlalchemy", "fastapi", "shapely", "cv2", "numpy", "asyncpg"}
    source_path = spec.origin
    assert source_path is not None
    lines = Path(source_path).read_text().splitlines()

    import_lines = [ln for ln in lines if ln.lstrip().startswith(("import ", "from "))]
    import_text = "\n".join(import_lines)
    for name in forbidden:
        assert name not in import_text, f"Forbidden import found in import statement: {name!r}"


# ---------------------------------------------------------------------------
# Core arithmetic: drop + in_plan + home_run with spare
# ---------------------------------------------------------------------------


def test_base_and_spare_arithmetic() -> None:
    """drop=2.0, in_plan=3.0, home_run=5.0, spare=0.10 → base=10.0, with_spare=11.0."""
    spare = 0.10
    params = _params(spare)

    in_plan = _make_in_plan_result(
        circuits=(_circuit_estimate(1, device_drop_m=2.0, in_plan_length_m=3.0),),
        total_device_drop_m=2.0,
        total_in_plan_length_m=3.0,
        spare_fraction=spare,
    )
    hr = _make_home_run_result(
        circuits=(_circuit_home_run_ok(1, home_run_m=5.0),),
    )

    result = assemble_cable_estimate(in_plan=in_plan, home_run=hr, params=params)

    assert len(result.per_circuit) == 1
    ce = result.per_circuit[0]
    assert ce.circuit_id == 1
    assert math.isclose(ce.device_drop_m, 2.0)
    assert math.isclose(ce.in_plan_length_m, 3.0)
    assert ce.home_run_m is not None
    assert math.isclose(ce.home_run_m, 5.0)
    assert ce.home_run_status == "ok"
    assert math.isclose(ce.base_total_m, 10.0)
    assert math.isclose(ce.total_with_spare_m, 11.0)

    assert math.isclose(result.grand_base_m, 10.0)
    assert math.isclose(result.grand_with_spare_m, 11.0)


# ---------------------------------------------------------------------------
# Suppressed home-run: base uses drop+in_plan only
# ---------------------------------------------------------------------------


def test_suppressed_home_run_excluded_from_base() -> None:
    """A circuit with home_run_m=None: base = drop + in_plan only; status surfaced."""
    spare = 0.10
    params = _params(spare)

    in_plan = _make_in_plan_result(
        circuits=(_circuit_estimate(1, device_drop_m=2.0, in_plan_length_m=3.0),),
        total_device_drop_m=2.0,
        total_in_plan_length_m=3.0,
        spare_fraction=spare,
    )
    hr = _make_home_run_result(
        circuits=(_circuit_home_run_suppressed(1, status="no_anchor"),),
    )

    result = assemble_cable_estimate(in_plan=in_plan, home_run=hr, params=params)

    ce = result.per_circuit[0]
    assert ce.home_run_m is None
    assert ce.home_run_status == "no_anchor"
    assert math.isclose(ce.base_total_m, 5.0)  # 2.0 + 3.0 + 0.0
    assert math.isclose(ce.total_with_spare_m, 5.5)


def test_unreachable_tray_status_surfaced() -> None:
    """Status "unreachable_tray" is carried through in home_run_status."""
    spare = 0.0
    params = _params(spare)

    in_plan = _make_in_plan_result(
        circuits=(_circuit_estimate(2, device_drop_m=1.0, in_plan_length_m=1.0),),
        total_device_drop_m=1.0,
        total_in_plan_length_m=1.0,
        spare_fraction=spare,
    )
    hr = _make_home_run_result(
        circuits=(_circuit_home_run_suppressed(2, status="unreachable_tray"),),
    )

    result = assemble_cable_estimate(in_plan=in_plan, home_run=hr, params=params)

    assert result.per_circuit[0].home_run_status == "unreachable_tray"
    assert result.per_circuit[0].home_run_m is None


# ---------------------------------------------------------------------------
# unattributed_device_drop_m flows into grand_base_m
# ---------------------------------------------------------------------------


def test_unattributed_drops_in_grand_base() -> None:
    """Unattributed device drops must be included in grand_base_m."""
    spare = 0.10
    params = _params(spare)

    # One circuit: drop=2, in_plan=3, home_run=5 → base=10
    # unattributed_device_drop_m=4.0
    # grand_base_m = 10 + 4 = 14; grand_with_spare = 14 * 1.1 = 15.4
    in_plan = _make_in_plan_result(
        circuits=(_circuit_estimate(1, device_drop_m=2.0, in_plan_length_m=3.0),),
        total_device_drop_m=6.0,  # 2.0 attributed + 4.0 unattributed
        total_in_plan_length_m=3.0,
        unattributed_device_drop_m=4.0,
        unattributed_device_count=2,
        spare_fraction=spare,
    )
    hr = _make_home_run_result(
        circuits=(_circuit_home_run_ok(1, home_run_m=5.0),),
    )

    result = assemble_cable_estimate(in_plan=in_plan, home_run=hr, params=params)

    assert math.isclose(result.unattributed_device_drop_m, 4.0)
    assert math.isclose(result.grand_base_m, 14.0)
    assert math.isclose(result.grand_with_spare_m, 15.4)


# ---------------------------------------------------------------------------
# Multi-circuit case
# ---------------------------------------------------------------------------


def test_multi_circuit_grand_totals() -> None:
    """Two circuits; grand totals = Σ per-circuit + unattributed."""
    spare = 0.0  # zero spare simplifies arithmetic
    params = _params(spare)

    # Circuit 1: drop=1, in_plan=2, home_run=3 → base=6
    # Circuit 2: drop=4, in_plan=5, home_run=None → base=9
    in_plan = _make_in_plan_result(
        circuits=(
            _circuit_estimate(1, device_drop_m=1.0, in_plan_length_m=2.0),
            _circuit_estimate(2, device_drop_m=4.0, in_plan_length_m=5.0),
        ),
        total_device_drop_m=5.0,
        total_in_plan_length_m=7.0,
        unattributed_device_drop_m=0.0,
        spare_fraction=spare,
    )
    hr = _make_home_run_result(
        circuits=(
            _circuit_home_run_ok(1, home_run_m=3.0),
            _circuit_home_run_suppressed(2, status="no_anchor"),
        ),
    )

    result = assemble_cable_estimate(in_plan=in_plan, home_run=hr, params=params)

    assert len(result.per_circuit) == 2
    c1 = result.per_circuit[0]
    c2 = result.per_circuit[1]
    assert math.isclose(c1.base_total_m, 6.0)
    assert math.isclose(c2.base_total_m, 9.0)
    assert math.isclose(result.grand_base_m, 15.0)
    assert math.isclose(result.grand_with_spare_m, 15.0)
    assert math.isclose(result.total_home_run_m, 3.0)


# ---------------------------------------------------------------------------
# Determinism: shuffled input order → identical CableAssembly
# ---------------------------------------------------------------------------


def test_determinism_shuffle_order() -> None:
    """Shuffling circuit order in the inputs must yield identical CableAssembly."""
    spare = 0.10
    params = _params(spare)

    circuit_ids = [1, 2, 3, 4, 5]
    circuits_ip = tuple(
        _circuit_estimate(cid, device_drop_m=float(cid), in_plan_length_m=float(cid) * 0.5)
        for cid in circuit_ids
    )
    circuits_hr = tuple(
        _circuit_home_run_ok(cid, home_run_m=float(cid) * 2.0) for cid in circuit_ids
    )

    in_plan = _make_in_plan_result(
        circuits=circuits_ip,
        total_device_drop_m=sum(c.device_drop_m for c in circuits_ip),
        total_in_plan_length_m=sum(c.in_plan_length_m for c in circuits_ip),
        spare_fraction=spare,
    )
    hr = _make_home_run_result(circuits=circuits_hr)

    # Canonical result.
    canonical = assemble_cable_estimate(in_plan=in_plan, home_run=hr, params=params)

    # Shuffle circuits (rebuild in_plan and hr with shuffled tuples).
    rng = random.Random(42)
    shuffled_ip = list(circuits_ip)
    shuffled_hr_list = list(circuits_hr)
    rng.shuffle(shuffled_ip)
    rng.shuffle(shuffled_hr_list)

    in_plan_shuffled = _make_in_plan_result(
        circuits=tuple(shuffled_ip),
        total_device_drop_m=in_plan.total_device_drop_m,
        total_in_plan_length_m=in_plan.total_in_plan_length_m,
        spare_fraction=spare,
    )
    hr_shuffled = _make_home_run_result(circuits=tuple(shuffled_hr_list))

    shuffled_result = assemble_cable_estimate(
        in_plan=in_plan_shuffled, home_run=hr_shuffled, params=params
    )

    assert canonical.per_circuit == shuffled_result.per_circuit
    assert math.isclose(canonical.grand_base_m, shuffled_result.grand_base_m)
    assert math.isclose(canonical.grand_with_spare_m, shuffled_result.grand_with_spare_m)


# ---------------------------------------------------------------------------
# Defensive: circuit missing from home_run treated as no_anchor
# ---------------------------------------------------------------------------


def test_circuit_missing_from_home_run_treated_as_no_anchor() -> None:
    """Circuit present in in_plan but absent from home_run → status 'no_anchor', home_run_m=None."""
    spare = 0.0
    params = _params(spare)

    in_plan = _make_in_plan_result(
        circuits=(_circuit_estimate(99, device_drop_m=3.0, in_plan_length_m=2.0),),
        total_device_drop_m=3.0,
        total_in_plan_length_m=2.0,
        spare_fraction=spare,
    )
    # home_run has no circuit 99.
    hr = _make_home_run_result(circuits=())

    result = assemble_cable_estimate(in_plan=in_plan, home_run=hr, params=params)

    ce = result.per_circuit[0]
    assert ce.home_run_m is None
    assert ce.home_run_status == "no_anchor"
    assert math.isclose(ce.base_total_m, 5.0)


# ---------------------------------------------------------------------------
# Conservation invariant
# ---------------------------------------------------------------------------


def test_conservation_invariant_holds() -> None:
    """grand_base_m == Σ per-circuit base_total_m + unattributed_device_drop_m."""
    spare = 0.15
    params = _params(spare)

    in_plan = _make_in_plan_result(
        circuits=(
            _circuit_estimate(1, device_drop_m=1.5, in_plan_length_m=2.5),
            _circuit_estimate(2, device_drop_m=0.5, in_plan_length_m=1.0),
        ),
        total_device_drop_m=4.0,
        total_in_plan_length_m=3.5,
        unattributed_device_drop_m=2.0,
        unattributed_device_count=1,
        spare_fraction=spare,
    )
    hr = _make_home_run_result(
        circuits=(
            _circuit_home_run_ok(1, home_run_m=4.0),
            _circuit_home_run_ok(2, home_run_m=2.0),
        ),
    )

    result = assemble_cable_estimate(in_plan=in_plan, home_run=hr, params=params)

    expected_circuit_sum = sum(c.base_total_m for c in result.per_circuit)
    expected_grand = expected_circuit_sum + result.unattributed_device_drop_m
    assert math.isclose(result.grand_base_m, expected_grand, abs_tol=1e-9)


# ---------------------------------------------------------------------------
# Reliability map
# ---------------------------------------------------------------------------


def test_reliability_map_keys_and_values() -> None:
    """Reliability map must have all four required keys, combined=mixed_reliability."""
    params = _params()
    in_plan = _make_in_plan_result(
        circuits=(_circuit_estimate(1, 1.0, 1.0),),
        total_device_drop_m=1.0,
        total_in_plan_length_m=1.0,
    )
    hr = _make_home_run_result(circuits=(_circuit_home_run_ok(1, 2.0),))

    result = assemble_cable_estimate(in_plan=in_plan, home_run=hr, params=params)

    rel = result.reliability
    assert "device_drop_m" in rel
    assert "in_plan_length_m" in rel
    assert "home_run_m" in rel
    assert "combined" in rel
    assert rel["device_drop_m"] == "reliable"
    assert rel["in_plan_length_m"] == "schematic_provisional"
    assert rel["home_run_m"] == "estimated_routed"
    assert rel["combined"] == "mixed_reliability"


# ---------------------------------------------------------------------------
# quantity_kind
# ---------------------------------------------------------------------------


def test_quantity_kind_is_estimated() -> None:
    params = _params()
    in_plan = _make_in_plan_result(
        circuits=(_circuit_estimate(1, 1.0, 1.0),),
        total_device_drop_m=1.0,
        total_in_plan_length_m=1.0,
    )
    hr = _make_home_run_result(circuits=(_circuit_home_run_ok(1, 1.0),))

    result = assemble_cable_estimate(in_plan=in_plan, home_run=hr, params=params)

    assert result.quantity_kind == "estimated"


# ---------------------------------------------------------------------------
# params_stamp and registration_audit are JSON-serialisable
# ---------------------------------------------------------------------------


def test_params_stamp_json_serialisable() -> None:
    params = _params()
    in_plan = _make_in_plan_result(
        circuits=(_circuit_estimate(1, 1.0, 1.0),),
        total_device_drop_m=1.0,
        total_in_plan_length_m=1.0,
    )
    hr = _make_home_run_result(circuits=(_circuit_home_run_ok(1, 1.0),))

    result = assemble_cable_estimate(in_plan=in_plan, home_run=hr, params=params)

    # Must not raise.
    json.dumps(result.params_stamp)


def test_registration_audit_passed_through() -> None:
    """registration_audit dict is carried through unchanged."""
    params = _params()
    in_plan = _make_in_plan_result(
        circuits=(_circuit_estimate(1, 1.0, 1.0),),
        total_device_drop_m=1.0,
        total_in_plan_length_m=1.0,
    )
    hr = _make_home_run_result(circuits=(_circuit_home_run_ok(1, 1.0),))
    audit = {"transform_type": "affine", "rmse_m": 0.05}

    result = assemble_cable_estimate(
        in_plan=in_plan, home_run=hr, params=params, registration_audit=audit
    )

    assert result.registration_audit == audit
    # JSON-serialisable.
    json.dumps(result.registration_audit)


def test_registration_audit_none_when_omitted() -> None:
    params = _params()
    in_plan = _make_in_plan_result(
        circuits=(_circuit_estimate(1, 1.0, 1.0),),
        total_device_drop_m=1.0,
        total_in_plan_length_m=1.0,
    )
    hr = _make_home_run_result(circuits=(_circuit_home_run_ok(1, 1.0),))

    result = assemble_cable_estimate(in_plan=in_plan, home_run=hr, params=params)

    assert result.registration_audit is None


# ---------------------------------------------------------------------------
# spare_fraction mismatch → RuntimeError
# ---------------------------------------------------------------------------


def test_spare_fraction_mismatch_raises() -> None:
    """params.spare_fraction != in_plan.spare_fraction must raise RuntimeError."""
    params = _params(spare=0.10)
    in_plan = _make_in_plan_result(
        circuits=(_circuit_estimate(1, 1.0, 1.0),),
        total_device_drop_m=1.0,
        total_in_plan_length_m=1.0,
        spare_fraction=0.20,  # deliberate mismatch
    )
    hr = _make_home_run_result(circuits=(_circuit_home_run_ok(1, 1.0),))

    with pytest.raises(RuntimeError, match="spare_fraction mismatch"):
        assemble_cable_estimate(in_plan=in_plan, home_run=hr, params=params)


# ---------------------------------------------------------------------------
# home_run_suppressed_counts carried from HomeRunResult
# ---------------------------------------------------------------------------


def test_home_run_suppressed_counts_carried() -> None:
    """suppressed_counts from HomeRunResult are surfaced on CableAssembly."""
    spare = 0.0
    params = _params(spare)

    in_plan = _make_in_plan_result(
        circuits=(
            _circuit_estimate(1, 1.0, 1.0),
            _circuit_estimate(2, 1.0, 1.0),
            _circuit_estimate(3, 1.0, 1.0),
        ),
        total_device_drop_m=3.0,
        total_in_plan_length_m=3.0,
        spare_fraction=spare,
    )
    hr = _make_home_run_result(
        circuits=(
            _circuit_home_run_ok(1, 5.0),
            _circuit_home_run_suppressed(2, "no_anchor"),
            _circuit_home_run_suppressed(3, "unreachable_tray"),
        ),
    )

    result = assemble_cable_estimate(in_plan=in_plan, home_run=hr, params=params)

    sc = result.home_run_suppressed_counts
    assert sc["no_anchor"] == 1
    assert sc["unreachable_tray"] == 1


# ---------------------------------------------------------------------------
# Integration: real pipeline (SplineInput → build_cable_graph → ... → assemble)
# ---------------------------------------------------------------------------


def test_integration_real_pipeline() -> None:
    """End-to-end via real pipeline: partition → estimate → home_run → assemble.

    One-circuit topology: panel at origin, tray along x-axis to (10,0),
    one luminaire at (10, 1).
    """
    params = default_estimate_params()

    # Panel (distribution board) at origin.
    panel = DeviceFootprint(
        entity_id="panel_0",
        block_ref="Distribution Board A",
        kind="device",
        bbox=(-0.3, -0.3, 0.3, 0.3),
        position=(0.0, 0.0),
    )
    # Luminaire at (10, 1).
    lum = DeviceFootprint(
        entity_id="lum_1",
        block_ref="Luminaire Type A",
        kind="device",
        bbox=(9.7, 0.7, 10.3, 1.3),
        position=(10.0, 1.0),
    )
    # Spline connecting panel → luminaire.
    spline = SplineInput(
        entity_id="sp_0",
        vertices=((0.0, 0.0), (10.0, 1.0)),
        closed=False,
    )

    graph = build_cable_graph(splines=[spline], devices=[panel, lum])
    circuits = partition_circuits(graph)

    in_plan = estimate_cable_in_plan(
        circuits=circuits,
        splines=[spline],
        devices=[panel, lum],
        params=params,
    )

    # Tray: horizontal segment along x-axis.
    tray_poly = [(0.0, 0.0), (10.0, 0.0)]
    hr = compute_home_runs(
        circuits=circuits,
        device_footprints_by_id={d.entity_id: d for d in [panel, lum]},
        tray_polylines=[tray_poly],
        snap_tol_m=0.30,
        entry_max_m=5.0,
    )

    result = assemble_cable_estimate(in_plan=in_plan, home_run=hr, params=params)

    # Structural checks.
    assert result.quantity_kind == "estimated"
    assert result.reliability["combined"] == "mixed_reliability"
    assert result.spare_fraction == params.spare_fraction

    # grand_base_m conservation.
    circuit_sum = sum(c.base_total_m for c in result.per_circuit)
    assert math.isclose(
        result.grand_base_m,
        circuit_sum + result.unattributed_device_drop_m,
        abs_tol=1e-6,
    )

    # grand_with_spare_m = grand_base_m * (1 + spare).
    assert math.isclose(
        result.grand_with_spare_m,
        result.grand_base_m * (1.0 + params.spare_fraction),
        abs_tol=1e-9,
    )

    # params_stamp is JSON-serialisable.
    json.dumps(result.params_stamp)
