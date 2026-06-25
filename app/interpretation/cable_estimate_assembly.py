"""Cable estimate assembler — combines device drops, in-plan lengths, and home runs (issue #698a).

Takes three separately-produced terms and combines them into a single :class:`CableAssembly`:

``device_drop_m``
    Per-circuit (and unattributed) vertical drops from
    :func:`~app.interpretation.cable_estimate.estimate_cable_in_plan`.
    Labelled "reliable".

``in_plan_length_m``
    Per-circuit spline/polyline chord lengths from the same source.
    Labelled "schematic_provisional".

``home_run_m``
    Panel→area routing along containment tray from
    :func:`~app.interpretation.cable_home_run.compute_home_runs`.
    Labelled "estimated_routed". None for suppressed circuits.

The combined ``base_total_m`` / ``grand_base_m`` mixes all three reliability
classes → labelled ``combined: "mixed_reliability"`` in the reliability map.
quantity_kind is always ``"estimated"``; NEVER folded into MEASURED.

Spare fraction comes from ``params.spare_fraction`` (authoritative source).  If
``in_plan.spare_fraction`` differs from ``params.spare_fraction`` a RuntimeError
is raised — the caller is expected to pass a consistent params object.

Conservation invariant (always asserted, fires even under python -O):
    ``grand_base_m == Σ per_circuit.base_total_m + unattributed_device_drop_m``

Pure module — NO DB, ORM, FastAPI, SQLAlchemy, cv2, shapely, or numpy imports.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from app.interpretation.cable_estimate import CableEstimateResult
from app.interpretation.cable_estimate_params import CableEstimateParams
from app.interpretation.cable_home_run import HomeRunResult

# ---------------------------------------------------------------------------
# Output dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CircuitCableEstimate:
    """Combined cable estimate for a single circuit.

    ``home_run_m`` is None when the home-run computation was suppressed for this
    circuit (any non-"ok" status).  In that case ``base_total_m`` uses 0.0 for
    the home-run term — the suppression reason is surfaced in ``home_run_status``.
    """

    circuit_id: int
    device_drop_m: float  # reliable
    in_plan_length_m: float  # schematic_provisional
    home_run_m: float | None  # estimated_routed; None when suppressed
    home_run_status: str  # from CircuitHomeRun.status (or "no_anchor" when missing from home_run)
    base_total_m: float  # device_drop_m + in_plan_length_m + (home_run_m or 0.0)
    total_with_spare_m: float  # base_total_m * (1 + spare_fraction)


@dataclass(frozen=True, slots=True)
class CableAssembly:
    """Complete assembled cable estimate across all circuits.

    Conservation invariant (always asserted):
        ``grand_base_m == Σ per_circuit.base_total_m + unattributed_device_drop_m``

    ``quantity_kind`` is always ``"estimated"`` — never folded into MEASURED.
    ``reliability["combined"]`` is always ``"mixed_reliability"`` — the grand
    totals blend reliable, schematic, and routed terms.
    """

    per_circuit: tuple[CircuitCableEstimate, ...]  # sorted by circuit_id
    total_device_drop_m: float  # carried from CableEstimateResult
    total_in_plan_length_m: float  # carried from CableEstimateResult
    total_home_run_m: float  # sum of ok home-runs; from HomeRunResult
    unattributed_device_drop_m: float  # carried from CableEstimateResult (#696)
    grand_base_m: float  # Σ per-circuit base_total_m + unattributed_device_drop_m
    grand_with_spare_m: float  # grand_base_m * (1 + spare_fraction)
    spare_fraction: float
    quantity_kind: str  # always "estimated"
    reliability: dict[str, str]  # fixed key order; combined="mixed_reliability"
    params_stamp: dict[str, object]  # params.as_stamp()
    home_run_suppressed_counts: dict[str, int]  # from HomeRunResult.suppressed_counts
    registration_audit: dict[str, object] | None = field(default=None)
    """Passed through from the coordinator (#698b); not inspected by the assembler."""


# ---------------------------------------------------------------------------
# Fixed reliability map
# ---------------------------------------------------------------------------

_RELIABILITY: dict[str, str] = {
    "device_drop_m": "reliable",
    "in_plan_length_m": "schematic_provisional",
    "home_run_m": "estimated_routed",
    "combined": "mixed_reliability",
}


# ---------------------------------------------------------------------------
# Public assembler
# ---------------------------------------------------------------------------


def assemble_cable_estimate(
    *,
    in_plan: CableEstimateResult,
    home_run: HomeRunResult,
    params: CableEstimateParams,
    registration_audit: dict[str, object] | None = None,
) -> CableAssembly:
    """Combine in-plan estimates and home-run lengths into a :class:`CableAssembly`.

    Parameters
    ----------
    in_plan:
        Per-circuit device drops and spline lengths from ``estimate_cable_in_plan``.
        This is the authoritative circuit set — ``in_plan.circuits`` drives iteration.
    home_run:
        Home-run routing results from ``compute_home_runs``.  Circuits missing from
        ``home_run`` are treated defensively as status "no_anchor" with home_run_m=None.
    params:
        Parameter set whose ``spare_fraction`` is used as the authoritative spare.
        Must match ``in_plan.spare_fraction``; a mismatch raises ``RuntimeError``.
    registration_audit:
        Optional audit dict supplied by the coordinator (#698b).  The assembler
        carries it through unchanged; it is not inspected here.

    Returns
    -------
    CableAssembly
        Fully deterministic.  ``per_circuit`` is sorted by circuit_id.

    Raises
    ------
    RuntimeError
        If ``params.spare_fraction != in_plan.spare_fraction`` (mismatch guard), or
        if the grand_base_m conservation invariant is violated.
    """
    # Guard: spare_fraction must be consistent.
    if not math.isclose(params.spare_fraction, in_plan.spare_fraction, rel_tol=1e-9, abs_tol=1e-12):
        raise RuntimeError(
            f"spare_fraction mismatch: params={params.spare_fraction!r} "
            f"!= in_plan={in_plan.spare_fraction!r}. "
            f"Pass the same CableEstimateParams to both "
            f"estimate_cable_in_plan and assemble_cable_estimate."
        )

    spare = params.spare_fraction

    # Index home_run by circuit_id for O(1) lookup.
    home_run_index = {chr.circuit_id: chr for chr in home_run.per_circuit}

    # Build per-circuit assembly (in_plan.circuits is already sorted by circuit_id).
    per_circuit_list: list[CircuitCableEstimate] = []
    for ce in in_plan.circuits:
        chr_ = home_run_index.get(ce.circuit_id)

        if chr_ is not None:
            hr_m = chr_.home_run_m  # None when suppressed
            hr_status = chr_.status
        else:
            # Defensive: circuit not present in home_run output.
            hr_m = None
            hr_status = "no_anchor"

        base_total = ce.device_drop_m + ce.in_plan_length_m + (hr_m or 0.0)
        with_spare = base_total * (1.0 + spare)

        per_circuit_list.append(
            CircuitCableEstimate(
                circuit_id=ce.circuit_id,
                device_drop_m=ce.device_drop_m,
                in_plan_length_m=ce.in_plan_length_m,
                home_run_m=hr_m,
                home_run_status=hr_status,
                base_total_m=base_total,
                total_with_spare_m=with_spare,
            )
        )

    # Sort by circuit_id for determinism (in_plan is already sorted; defensive sort ensures it).
    per_circuit_list.sort(key=lambda x: x.circuit_id)
    per_circuit = tuple(per_circuit_list)

    # Grand totals.
    circuit_base_sum = sum(c.base_total_m for c in per_circuit)
    grand_base_m = circuit_base_sum + in_plan.unattributed_device_drop_m
    grand_with_spare_m = grand_base_m * (1.0 + spare)

    # Conservation invariant — explicit RuntimeError, fires even under python -O.
    expected_grand_base = circuit_base_sum + in_plan.unattributed_device_drop_m
    if not math.isclose(grand_base_m, expected_grand_base, rel_tol=1e-9, abs_tol=1e-6):
        raise RuntimeError(
            f"grand_base_m conservation violated: "
            f"circuit_sum={circuit_base_sum!r} + "
            f"unattributed={in_plan.unattributed_device_drop_m!r} = {expected_grand_base!r} "
            f"!= grand_base_m={grand_base_m!r}"
        )

    return CableAssembly(
        per_circuit=per_circuit,
        total_device_drop_m=in_plan.total_device_drop_m,
        total_in_plan_length_m=in_plan.total_in_plan_length_m,
        total_home_run_m=home_run.total_home_run_m,
        unattributed_device_drop_m=in_plan.unattributed_device_drop_m,
        grand_base_m=grand_base_m,
        grand_with_spare_m=grand_with_spare_m,
        spare_fraction=spare,
        quantity_kind="estimated",
        reliability=dict(_RELIABILITY),  # copy — stable key order, JSON-serialisable
        params_stamp=params.as_stamp(),
        home_run_suppressed_counts=dict(home_run.suppressed_counts),  # copy for isolation
        registration_audit=registration_audit,
    )
