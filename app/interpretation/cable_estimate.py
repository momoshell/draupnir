"""In-plan cable length estimate — per-circuit device drops + spline lengths (issue #696).

Consumes:
- :class:`~app.interpretation.cable_circuits.CableCircuitSet` (#694)
- :class:`~app.interpretation.cable_topology.SplineInput` and
  :class:`~app.interpretation.cable_topology.DeviceFootprint` (#693)
- :class:`~app.interpretation.cable_estimate_params.CableEstimateParams` (#695)

Produces :class:`CableEstimateResult` with two SEPARATELY LABELED terms:

``device_drop_m``
    Σ per-device vertical drops (REAL — physically grounded in spec notes or
    standard-practice assumptions).

``in_plan_length_m``
    Σ spline/polyline lengths on the drawing (SCHEMATIC / PROVISIONAL — on
    E-630003 device positions are notional/ceiling-rose fed / no through-wiring,
    so the spline length is not real cable length on that sheet; it IS real cable
    on through-wired sheets).  A reliability label makes this explicit.

These terms are NEVER summed.  The reliability map in :class:`CableEstimateResult`
labels each term.  Spare fraction is CARRIED but NOT applied here — the final
assembly (#698) applies it.

``quantity_kind`` is always ``"estimated"`` — never folded into MEASURED.

**Device-drop counting rule (distinct devices, no double-count):**
A device whose footprint bbox spans multiple circuit-fragments may appear in
MULTIPLE circuits' ``device_entity_ids`` (documented in #694).  A device's
vertical drop is a per-device PHYSICAL quantity — it must be counted EXACTLY ONCE
regardless of how many circuit-fragments it touches.

``total_device_drop_m`` = Σ over ALL DISTINCT categorized devices in the ``devices``
input, one drop per entity_id.

Per-circuit attribution: each distinct associated device is attributed to the
LOWEST circuit_id circuit that claims it (first-claim-wins, circuits iterated in
ascending circuit_id order).  Distinct devices that appear in NO circuit go into
the unattributed bucket (``unattributed_device_count`` /
``unattributed_device_drop_m`` on :class:`CableEstimateResult`).

Conservation invariant (always asserted):
    ``Σ circuit.device_drop_m + unattributed_device_drop_m == total_device_drop_m``

Pure module — NO DB, ORM, FastAPI, SQLAlchemy, cv2, or shapely imports.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

from app.interpretation.cable_circuits import (
    _DEFAULT_DISTRIBUTION_PATTERNS,
    CableCircuitSet,
)
from app.interpretation.cable_estimate_params import (
    CATEGORY_DISTRIBUTION_BOARD,
    CATEGORY_LUMINAIRE,
    CATEGORY_SOCKET,
    CATEGORY_SWITCH,
    CableEstimateParams,
)
from app.interpretation.cable_topology import (
    DeviceFootprint,
    SplineInput,
)

# ---------------------------------------------------------------------------
# Device → category heuristic (v1)
#
# Case-insensitive substring match; first match wins in the explicit order below.
# DISTRIBUTION_BOARD is checked before all others so that a panel block_ref
# containing a generic token (e.g. "Board" inside "Distribution Board") is never
# misclassified as another category.
# ---------------------------------------------------------------------------

# Patterns per category — each tuple is checked in the listed order.
_DISTRIBUTION_PATTERNS: tuple[str, ...] = _DEFAULT_DISTRIBUTION_PATTERNS  # re-use from #694

_LUMINAIRE_PATTERNS: tuple[str, ...] = (
    "Luminaire",
    "Whitecroft",
    "Downlight",
    "Light Fitting",
    "Lighting Unit",
)

_SWITCH_PATTERNS: tuple[str, ...] = (
    "Switch",
    "Dimmer",
    "Plate Switch",
)

_SOCKET_PATTERNS: tuple[str, ...] = (
    "Socket",
    "Outlet",
    "SSO",
    "Power Point",
)

# Category evaluation order: distribution board MUST come first.
_CATEGORY_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (CATEGORY_DISTRIBUTION_BOARD, _DISTRIBUTION_PATTERNS),
    (CATEGORY_LUMINAIRE, _LUMINAIRE_PATTERNS),
    (CATEGORY_SWITCH, _SWITCH_PATTERNS),
    (CATEGORY_SOCKET, _SOCKET_PATTERNS),
)


def categorize_device(block_ref: str | None) -> str | None:
    """Categorize a device by its ``block_ref`` using v1 substring heuristics.

    Performs case-insensitive substring matching against known patterns for each
    category, in a fixed deterministic order.  ``DISTRIBUTION_BOARD`` is checked
    before all other categories so that a panel block_ref is never mistakenly
    captured by a generic token such as "Switch" or "Lighting Unit".

    Returns the first matching category name (a ``CATEGORY_*`` constant), or
    ``None`` when no pattern matches (the device is UNCATEGORIZED).
    """
    if block_ref is None:
        return None
    lower = block_ref.lower()
    for category, patterns in _CATEGORY_RULES:
        if any(pat.lower() in lower for pat in patterns):
            return category
    return None


# ---------------------------------------------------------------------------
# Length helper
# ---------------------------------------------------------------------------


def _polyline_length(vertices: tuple[tuple[float, float], ...]) -> float:
    """Σ chord lengths between consecutive vertices (control-polygon approximation).

    Acceptable for the schematic/provisional in-plan term.  Returns 0.0 for
    fewer than 2 vertices.
    """
    if len(vertices) < 2:
        return 0.0
    total = 0.0
    for i in range(len(vertices) - 1):
        x0, y0 = vertices[i]
        x1, y1 = vertices[i + 1]
        total += math.hypot(x1 - x0, y1 - y0)
    return total


# ---------------------------------------------------------------------------
# Output dataclasses (frozen + slots for determinism-by-value)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DeviceDropBreakdown:
    """Per-category device-drop subtotal within a single circuit."""

    category: str
    device_count: int
    per_device_drop_m: float
    subtotal_m: float


@dataclass(frozen=True, slots=True)
class CircuitEstimate:
    """Cable length estimate for one circuit.

    ``device_count`` is the count of devices attributed to THIS circuit (i.e. where
    this circuit has the lowest circuit_id among all circuits claiming that device).
    ``uncategorized_device_count`` counts attributed-but-uncategorized devices only;
    the unattributed bucket at the result level handles devices in no circuit.
    """

    circuit_id: int
    device_count: int
    uncategorized_device_count: int
    device_drop_m: float
    drop_breakdown: tuple[DeviceDropBreakdown, ...]  # sorted by category
    in_plan_length_m: float


@dataclass(frozen=True, slots=True)
class CableEstimateResult:
    """Complete in-plan cable estimate across all circuits.

    ``device_drop_m`` and ``in_plan_length_m`` are ALWAYS separate.
    ``spare_fraction`` is CARRIED but NOT applied here; #698 applies it.
    ``quantity_kind`` is always ``"estimated"``; never folded into MEASURED.
    ``reliability`` labels the nature of each term (see field-level notes).
    ``params_stamp`` is a fully JSON-serialisable parameter audit trail.

    Conservation (always asserted):
        ``Σ circuit.device_drop_m + unattributed_device_drop_m == total_device_drop_m``
    """

    circuits: tuple[CircuitEstimate, ...]  # sorted by circuit_id
    total_device_drop_m: float  # Σ over ALL DISTINCT categorized devices (no double-count)
    total_in_plan_length_m: float
    total_uncategorized_devices: int  # distinct devices with no matching category
    unattributed_device_count: int  # categorized devices associated to no circuit
    unattributed_device_drop_m: float  # drop total for unattributed devices
    spare_fraction: float  # carried, not applied
    quantity_kind: str  # always "estimated"
    params_stamp: dict[str, object]  # params.as_stamp()
    reliability: dict[str, str]  # {"device_drop_m": ..., "in_plan_length_m": ..., "note": ...}


# ---------------------------------------------------------------------------
# Public estimator
# ---------------------------------------------------------------------------


def estimate_cable_in_plan(
    *,
    circuits: CableCircuitSet,
    splines: Sequence[SplineInput],
    devices: Sequence[DeviceFootprint],
    params: CableEstimateParams,
) -> CableEstimateResult:
    """Compute per-circuit cable length estimates from topology + params.

    Parameters
    ----------
    circuits:
        Partitioned cable graph from :func:`~app.interpretation.cable_circuits.partition_circuits`.
    splines:
        All :class:`~app.interpretation.cable_topology.SplineInput` objects for the drawing.
    devices:
        All :class:`~app.interpretation.cable_topology.DeviceFootprint` objects for the drawing.
    params:
        Vertical-drop parameters and spare fraction from
        :func:`~app.interpretation.cable_estimate_params.default_estimate_params` or overrides.

    Returns
    -------
    CableEstimateResult
        Fully deterministic; output is identical regardless of input list ordering.
    """
    # Build lookup indices once — O(n) then O(1) per lookup.
    spline_index: dict[str, SplineInput] = {s.entity_id: s for s in splines}
    device_index: dict[str, DeviceFootprint] = {d.entity_id: d for d in devices}

    # Pre-build category → drop_m dict to avoid linear scan per device.
    drop_by_category: dict[str, float] = {}
    for drop_param in params.vertical_drops:
        drop_by_category[drop_param.category] = drop_param.drop_m

    # -----------------------------------------------------------------------
    # Step 1: Compute total_device_drop_m over ALL DISTINCT devices (no double-count).
    #
    # Each device's drop is a physical per-device quantity — counted once regardless
    # of how many circuit-fragments list it in device_entity_ids.  Use the `devices`
    # input (the drawing's full device population) as the authoritative distinct set.
    # -----------------------------------------------------------------------
    total_device_drop_m = 0.0
    total_uncategorized_devices = 0
    # Sorted for determinism in all downstream operations.
    all_device_ids_sorted = sorted(device_index.keys())

    for dev_eid in all_device_ids_sorted:
        device = device_index[dev_eid]
        category = categorize_device(device.block_ref)
        if category is not None and category in drop_by_category:
            total_device_drop_m += drop_by_category[category]
        else:
            total_uncategorized_devices += 1

    # -----------------------------------------------------------------------
    # Step 2: Build per-circuit estimates with first-claim attribution.
    #
    # Iterate circuits in ascending circuit_id order. For each circuit, only
    # devices not yet claimed by a lower-id circuit contribute to that circuit's
    # device_drop_m and breakdown. Each distinct device is attributed to exactly
    # one circuit — the one with the lowest circuit_id that lists it.
    # -----------------------------------------------------------------------
    claimed: set[str] = set()  # entity_ids attributed to a circuit already

    sorted_circuits = sorted(circuits.circuits, key=lambda c: c.circuit_id)

    circuit_estimates: list[CircuitEstimate] = []

    for circuit in sorted_circuits:
        # --- In-plan length: sum spline polyline lengths ---
        # Splines partition cleanly across circuits (#694 conservation), so no
        # deduplication needed here.
        in_plan_length_m = 0.0
        for eid in circuit.spline_entity_ids:
            spline = spline_index.get(eid)
            if spline is not None:
                in_plan_length_m += _polyline_length(spline.vertices)

        # --- Device drops: first-claim attribution ---
        cat_counts: dict[str, int] = {}
        uncategorized_device_count = 0
        circuit_device_drop_m = 0.0
        circuit_device_count = 0

        # device_entity_ids is already sorted (from partition_circuits).
        for dev_eid in circuit.device_entity_ids:
            if dev_eid in claimed:
                # Already attributed to an earlier (lower circuit_id) circuit — skip.
                continue

            claimed.add(dev_eid)
            circuit_device_count += 1

            found_device: DeviceFootprint | None = device_index.get(dev_eid)
            block_ref = found_device.block_ref if found_device is not None else None
            category = categorize_device(block_ref)

            if category is not None and category in drop_by_category:
                drop_m = drop_by_category[category]
                cat_counts[category] = cat_counts.get(category, 0) + 1
                circuit_device_drop_m += drop_m
            else:
                # UNCATEGORIZED: no drop applied; counted honestly.
                uncategorized_device_count += 1

        # Build breakdown sorted by category for determinism.
        breakdown: list[DeviceDropBreakdown] = []
        for cat in sorted(cat_counts.keys()):
            count = cat_counts[cat]
            per_drop = drop_by_category[cat]
            breakdown.append(
                DeviceDropBreakdown(
                    category=cat,
                    device_count=count,
                    per_device_drop_m=per_drop,
                    subtotal_m=per_drop * count,
                )
            )

        circuit_estimates.append(
            CircuitEstimate(
                circuit_id=circuit.circuit_id,
                device_count=circuit_device_count,
                uncategorized_device_count=uncategorized_device_count,
                device_drop_m=circuit_device_drop_m,
                drop_breakdown=tuple(breakdown),
                in_plan_length_m=in_plan_length_m,
            )
        )

    # -----------------------------------------------------------------------
    # Step 3: Unattributed bucket — categorized devices that appear in NO circuit.
    # -----------------------------------------------------------------------
    # The set of all entity_ids that appear in any circuit's device_entity_ids.
    circuit_device_universe: set[str] = set()
    for circuit in circuits.circuits:
        circuit_device_universe.update(circuit.device_entity_ids)

    unattributed_device_count = 0
    unattributed_device_drop_m = 0.0

    for dev_eid in all_device_ids_sorted:
        if dev_eid in circuit_device_universe:
            # This device appeared in at least one circuit — it was either claimed
            # by the first-claim logic or was a duplicate that got skipped. Either
            # way it is not "unattributed" (it was circuit-associated).
            continue
        device = device_index[dev_eid]
        category = categorize_device(device.block_ref)
        if category is not None and category in drop_by_category:
            unattributed_device_count += 1
            unattributed_device_drop_m += drop_by_category[category]
        # Uncategorized devices in no circuit contribute 0 and are already in
        # total_uncategorized_devices — not double-counted here.

    # -----------------------------------------------------------------------
    # Conservation invariant — explicit RuntimeError, fires under python -O too.
    # -----------------------------------------------------------------------
    circuit_drop_sum = sum(c.device_drop_m for c in circuit_estimates)
    conservation_total = circuit_drop_sum + unattributed_device_drop_m
    if abs(conservation_total - total_device_drop_m) > 1e-9:
        raise RuntimeError(
            f"device-drop conservation violated: "
            f"circuit_sum={circuit_drop_sum!r} + "
            f"unattributed={unattributed_device_drop_m!r} = {conservation_total!r} "
            f"!= total={total_device_drop_m!r}"
        )

    grand_in_plan_length_m = sum(c.in_plan_length_m for c in circuit_estimates)

    reliability: dict[str, str] = {
        "device_drop_m": "reliable",
        "in_plan_length_m": "schematic_provisional",
        "note": (
            "in_plan_length_m is the sum of on-drawing spline/polyline chord lengths. "
            "On E-630003 device positions are notional (ceiling-rose fed, no through-wiring), "
            "so spline length is not real installed cable length on that sheet; "
            "it IS real on through-wired sheets. "
            "device_drop_m is grounded in spec notes (luminaire 2 m) and standard-practice "
            "assumptions; labelled 'reliable' relative to the schematic term."
        ),
    }

    return CableEstimateResult(
        circuits=tuple(circuit_estimates),  # sorted by circuit_id
        total_device_drop_m=total_device_drop_m,
        total_in_plan_length_m=grand_in_plan_length_m,
        total_uncategorized_devices=total_uncategorized_devices,
        unattributed_device_count=unattributed_device_count,
        unattributed_device_drop_m=unattributed_device_drop_m,
        spare_fraction=params.spare_fraction,
        quantity_kind="estimated",
        params_stamp=params.as_stamp(),
        reliability=reliability,
    )
