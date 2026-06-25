"""Cable home-run length estimator (issue #697b).

Routes each circuit's panel → served area ALONG the containment (cable-tray) centerline.

A "home run" is the end-to-end cable path from the distribution panel/board to the
last served device in a circuit:

    home_run_m = d_panel + along_tray_m + d_area

where:
- d_panel      : straight-line distance from panel centroid to nearest tray entry node
- along_tray_m : Dijkstra shortest path along tray graph between entry nodes
- d_area       : straight-line distance from area centroid to nearest tray entry node

This module is PURE — NO DB, ORM, FastAPI, SQLAlchemy, cv2, shapely, or numpy imports.
All geometry is pre-scaled to METRES (conversion_factor=1.0). Do NOT re-scale.
"""

from __future__ import annotations

import heapq
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from app.interpretation.cable_circuits import CableCircuitSet
from app.interpretation.cable_topology import DeviceFootprint

# ---------------------------------------------------------------------------
# Output dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CircuitHomeRun:
    """Home-run routing result for a single circuit."""

    circuit_id: int
    # "ok" | "no_anchor" | "unreachable_tray" | "disconnected_tray" | "bad_registration"
    status: str
    home_run_m: float | None  # None unless status=="ok"
    d_panel_m: float | None  # straight-line panel->tray entry
    along_tray_m: float | None  # Dijkstra path along tray
    d_area_m: float | None  # straight-line tray->area entry
    lower_bound_m: float | None  # advisory lower bound for "disconnected_tray" only


@dataclass(frozen=True, slots=True)
class HomeRunResult:
    """Aggregate home-run result for a full circuit set."""

    per_circuit: tuple[CircuitHomeRun, ...]  # sorted by circuit_id
    total_home_run_m: float  # sum over status=="ok"
    ok_count: int
    suppressed_counts: dict[str, int]  # by status, fixed key order
    circuit_count: int


# ---------------------------------------------------------------------------
# Internal: tray graph primitives
# ---------------------------------------------------------------------------

# Fixed status key order for suppressed_counts determinism.
# "no_containment_sheet" is the synthetic status used when no containment revision is supplied
# (the caller chose not to provide tray geometry — honest absence, not a routing failure).
_STATUS_ORDER: tuple[str, ...] = (
    "no_anchor",
    "unreachable_tray",
    "disconnected_tray",
    "bad_registration",
    "no_containment_sheet",
)

# Multiplier on entry_max_m beyond which a reported d_panel/d_area distance is suppressed
# (None) in an "unreachable_tray" result — avoids surfacing absurd noise distances from
# near-empty or corrupt geometry to callers.
_UNREACHABLE_REPORT_MULTIPLIER: float = 10.0


def _snap_node(coord: tuple[float, float], tol: float) -> tuple[int, int]:
    """Map a world coordinate to a grid cell index for node identity."""
    return (round(coord[0] / tol), round(coord[1] / tol))


def _node_world(node_id: tuple[int, int], tol: float) -> tuple[float, float]:
    """Recover the grid-centre world coordinate for a node_id."""
    return (node_id[0] * tol, node_id[1] * tol)


def _build_tray_graph(
    tray_polylines: Sequence[Sequence[tuple[float, float]]],
    snap_tol_m: float,
) -> tuple[
    # adjacency: node -> [(neighbour, weight_m)]
    dict[tuple[int, int], list[tuple[tuple[int, int], float]]],
    dict[tuple[int, int], int],  # component_id per node
]:
    """Build a snapped-endpoint adjacency graph from tray polylines.

    Nodes are snapped on a ``snap_tol_m`` grid; edge weights are Euclidean segment lengths.
    Returns (adjacency, component_id_per_node).

    Connected-component IDs are assigned by BFS so component membership is stable
    regardless of polyline ordering.
    """
    # adjacency[node_id] = list of (neighbour_node_id, edge_length_m)
    adjacency: dict[tuple[int, int], list[tuple[tuple[int, int], float]]] = {}

    def _ensure(nid: tuple[int, int]) -> None:
        if nid not in adjacency:
            adjacency[nid] = []

    for polyline in tray_polylines:
        # Drop non-finite vertices before processing — NaN/inf coordinates make
        # round() raise in _snap_node and propagate NaN through hypot.
        pts = [p for p in polyline if math.isfinite(p[0]) and math.isfinite(p[1])]
        if len(pts) < 2:
            continue
        for i in range(len(pts) - 1):
            p0 = pts[i]
            p1 = pts[i + 1]
            nid0 = _snap_node(p0, snap_tol_m)
            nid1 = _snap_node(p1, snap_tol_m)
            _ensure(nid0)
            _ensure(nid1)
            seg_len = math.hypot(p1[0] - p0[0], p1[1] - p0[1])
            if nid0 == nid1:
                # Zero-length segment after snapping — skip edge but keep both nodes.
                continue
            adjacency[nid0].append((nid1, seg_len))
            adjacency[nid1].append((nid0, seg_len))

    # BFS connected-component labelling (deterministic: nodes sorted, so BFS order is stable).
    component_id: dict[tuple[int, int], int] = {}
    comp = 0
    for start in sorted(adjacency.keys()):
        if start in component_id:
            continue
        queue = [start]
        component_id[start] = comp
        head = 0
        while head < len(queue):
            current = queue[head]
            head += 1
            for neighbour, _ in adjacency[current]:
                if neighbour not in component_id:
                    component_id[neighbour] = comp
                    queue.append(neighbour)
        comp += 1

    return adjacency, component_id


def _nearest_tray_node(
    point: tuple[float, float],
    adjacency: dict[tuple[int, int], list[tuple[tuple[int, int], float]]],
    snap_tol_m: float,
) -> tuple[tuple[int, int], float]:
    """Return the nearest tray node to ``point`` and its distance in metres.

    Iterates all nodes; for small tray graphs (typical single-floor plans) this is fine.
    Tie-break: lexicographic node_id.
    """
    best_nid: tuple[int, int] | None = None
    best_dist = math.inf
    px, py = point
    for nid in adjacency:
        wx, wy = _node_world(nid, snap_tol_m)
        d = math.hypot(wx - px, wy - py)
        if d < best_dist or (d == best_dist and (best_nid is None or nid < best_nid)):
            best_dist = d
            best_nid = nid
    if best_nid is None:
        raise ValueError("tray graph is empty — no nodes to snap to")
    return best_nid, best_dist


def _dijkstra(
    adjacency: dict[tuple[int, int], list[tuple[tuple[int, int], float]]],
    source: tuple[int, int],
    target: tuple[int, int],
) -> float | None:
    """Dijkstra shortest path from source to target.

    Returns the path length in metres, or None if target is unreachable.
    Tie-break on equal tentative distance: lexicographic node_id (deterministic).
    """
    if source == target:
        return 0.0

    # heap: (distance, node_id)
    dist: dict[tuple[int, int], float] = {source: 0.0}
    heap: list[tuple[float, tuple[int, int]]] = [(0.0, source)]

    while heap:
        d_u, u = heapq.heappop(heap)
        if d_u > dist.get(u, math.inf):
            continue  # stale entry
        if u == target:
            return d_u
        for v, w in sorted(adjacency.get(u, []), key=lambda x: x[0]):  # sorted for determinism
            d_v = d_u + w
            if d_v < dist.get(v, math.inf):
                dist[v] = d_v
                heapq.heappush(heap, (d_v, v))

    return None


# ---------------------------------------------------------------------------
# Internal: footprint centroid helpers
# ---------------------------------------------------------------------------


def _device_centroid(device: DeviceFootprint) -> tuple[float, float] | None:
    """Return the centroid of a device's effective bounding box.

    Prefers bbox centre; falls back to position.
    """
    if device.bbox is not None:
        min_x, min_y, max_x, max_y = device.bbox
        return ((min_x + max_x) / 2.0, (min_y + max_y) / 2.0)
    if device.position is not None:
        return device.position
    return None


def _mean_point(points: list[tuple[float, float]]) -> tuple[float, float]:
    """Arithmetic mean of a list of (x, y) points."""
    n = len(points)
    return (sum(p[0] for p in points) / n, sum(p[1] for p in points) / n)


# ---------------------------------------------------------------------------
# Public builder
# ---------------------------------------------------------------------------


def compute_home_runs(
    *,
    circuits: CableCircuitSet,
    device_footprints_by_id: Mapping[str, DeviceFootprint],
    tray_polylines: Sequence[Sequence[tuple[float, float]]],
    registration_failed: bool = False,
    snap_tol_m: float = 0.30,
    entry_max_m: float = 5.0,
) -> HomeRunResult:
    """Compute home-run lengths for every circuit in *circuits*.

    Parameters
    ----------
    circuits:
        Partitioned cable circuits (from ``partition_circuits``).
    device_footprints_by_id:
        Mapping entity_id → DeviceFootprint in the LIGHTING frame.
    tray_polylines:
        Containment centerline polylines ALREADY registered into the lighting frame.
        Each polyline is a sequence of (x, y) metre-scale vertices.
    registration_failed:
        When True (set by the coordinator when the grid transform is unusable), every
        circuit receives status "bad_registration" and all metric terms are None.
    snap_tol_m:
        Grid cell size for snapping tray endpoints to nodes (metres).
    entry_max_m:
        Maximum straight-line distance from a circuit point (panel or area centroid)
        to the nearest tray node before the circuit is flagged "unreachable_tray".

    Returns
    -------
    HomeRunResult
        Sorted by circuit_id; conservation invariant enforced.

    Notes
    -----
    All geometry is assumed to be in metres (conversion_factor=1.0 on ingestion).
    A full per-revision units re-resolution is out of scope for v1 — callers must
    ensure both revisions share the same metre scale.

    **Node-snap approximation (v1):** panel and area entry points attach to the
    NEAREST tray NODE (polyline vertex), not the nearest point on a segment
    interior. This is an acceptable v1 approximation but can inflate d_panel/d_area
    and occasionally over-trip ``unreachable_tray`` when tray segments are long with
    sparse vertices. Segment-projection snap is a planned follow-up.
    """
    circuit_count = circuits.circuit_count

    # --- Step 1: fail-fast when registration is unusable ---
    if registration_failed:
        per_circuit = tuple(
            CircuitHomeRun(
                circuit_id=c.circuit_id,
                status="bad_registration",
                home_run_m=None,
                d_panel_m=None,
                along_tray_m=None,
                d_area_m=None,
                lower_bound_m=None,
            )
            for c in sorted(circuits.circuits, key=lambda c: c.circuit_id)
        )
        suppressed: dict[str, int] = dict.fromkeys(_STATUS_ORDER, 0)
        suppressed["bad_registration"] = circuit_count
        return HomeRunResult(
            per_circuit=per_circuit,
            total_home_run_m=0.0,
            ok_count=0,
            suppressed_counts=suppressed,
            circuit_count=circuit_count,
        )

    # --- Step 2: build tray graph ---
    adjacency, component_id = _build_tray_graph(tray_polylines, snap_tol_m)

    # --- Step 3: per-circuit routing ---
    per_circuit_list: list[CircuitHomeRun] = []

    for circuit in sorted(circuits.circuits, key=lambda c: c.circuit_id):
        cid = circuit.circuit_id

        # No anchor → cannot route.
        if circuit.anchor_device_entity_id is None:
            per_circuit_list.append(
                CircuitHomeRun(
                    circuit_id=cid,
                    status="no_anchor",
                    home_run_m=None,
                    d_panel_m=None,
                    along_tray_m=None,
                    d_area_m=None,
                    lower_bound_m=None,
                )
            )
            continue

        anchor_id = circuit.anchor_device_entity_id
        anchor_device = device_footprints_by_id.get(anchor_id)
        panel_point = _device_centroid(anchor_device) if anchor_device is not None else None

        if panel_point is None:
            # Anchor device has no usable geometry — treat as no_anchor.
            per_circuit_list.append(
                CircuitHomeRun(
                    circuit_id=cid,
                    status="no_anchor",
                    home_run_m=None,
                    d_panel_m=None,
                    along_tray_m=None,
                    d_area_m=None,
                    lower_bound_m=None,
                )
            )
            continue

        # Area centroid: mean of NON-anchor device centroids.
        non_anchor_centroids: list[tuple[float, float]] = []
        for dev_id in sorted(circuit.device_entity_ids):  # sorted for determinism
            if dev_id == anchor_id:
                continue
            dev = device_footprints_by_id.get(dev_id)
            if dev is None:
                continue
            c_pt = _device_centroid(dev)
            if c_pt is not None:
                non_anchor_centroids.append(c_pt)

        # Degenerate fallback: no served devices → route to self (home_run_m ≈ 0).
        area_point: tuple[float, float] = (
            _mean_point(non_anchor_centroids) if non_anchor_centroids else panel_point
        )

        # No tray nodes → unreachable.
        if not adjacency:
            per_circuit_list.append(
                CircuitHomeRun(
                    circuit_id=cid,
                    status="unreachable_tray",
                    home_run_m=None,
                    d_panel_m=None,
                    along_tray_m=None,
                    d_area_m=None,
                    lower_bound_m=None,
                )
            )
            continue

        # Snap panel and area to nearest tray node.
        panel_node, d_panel = _nearest_tray_node(panel_point, adjacency, snap_tol_m)
        area_node, d_area = _nearest_tray_node(area_point, adjacency, snap_tol_m)

        if d_panel > entry_max_m or d_area > entry_max_m:
            per_circuit_list.append(
                CircuitHomeRun(
                    circuit_id=cid,
                    status="unreachable_tray",
                    home_run_m=None,
                    d_panel_m=(
                        d_panel if d_panel <= entry_max_m * _UNREACHABLE_REPORT_MULTIPLIER else None
                    ),
                    along_tray_m=None,
                    d_area_m=(
                        d_area if d_area <= entry_max_m * _UNREACHABLE_REPORT_MULTIPLIER else None
                    ),
                    lower_bound_m=None,
                )
            )
            continue

        # Check connectivity — same component?
        panel_comp = component_id.get(panel_node)
        area_comp = component_id.get(area_node)

        if panel_comp != area_comp or panel_comp is None:
            # Disconnected tray: advisory lower bound is straight-line between entry nodes.
            pnx, pny = _node_world(panel_node, snap_tol_m)
            anx, any_ = _node_world(area_node, snap_tol_m)
            lower_bound = math.hypot(anx - pnx, any_ - pny)
            per_circuit_list.append(
                CircuitHomeRun(
                    circuit_id=cid,
                    status="disconnected_tray",
                    home_run_m=None,
                    d_panel_m=d_panel,
                    along_tray_m=None,
                    d_area_m=d_area,
                    lower_bound_m=lower_bound,
                )
            )
            continue

        # Dijkstra along tray.
        along_tray = _dijkstra(adjacency, panel_node, area_node)

        if along_tray is None:
            # Should not happen when components agree, but defensive.
            pnx, pny = _node_world(panel_node, snap_tol_m)
            anx, any_ = _node_world(area_node, snap_tol_m)
            per_circuit_list.append(
                CircuitHomeRun(
                    circuit_id=cid,
                    status="disconnected_tray",
                    home_run_m=None,
                    d_panel_m=d_panel,
                    along_tray_m=None,
                    d_area_m=d_area,
                    lower_bound_m=math.hypot(anx - pnx, any_ - pny),
                )
            )
            continue

        home_run_m = d_panel + along_tray + d_area
        per_circuit_list.append(
            CircuitHomeRun(
                circuit_id=cid,
                status="ok",
                home_run_m=home_run_m,
                d_panel_m=d_panel,
                along_tray_m=along_tray,
                d_area_m=d_area,
                lower_bound_m=None,
            )
        )

    # --- Step 4: totals and conservation ---
    per_circuit = tuple(per_circuit_list)
    ok_count = sum(1 for r in per_circuit if r.status == "ok")
    total_home_run_m = sum(r.home_run_m for r in per_circuit if r.status == "ok")  # type: ignore[misc]

    suppressed_counts: dict[str, int] = dict.fromkeys(_STATUS_ORDER, 0)
    for r in per_circuit:
        if r.status != "ok":
            suppressed_counts[r.status] = suppressed_counts.get(r.status, 0) + 1

    suppressed_total = sum(suppressed_counts.values())
    # Conservation invariant — explicit RuntimeError (fires even under python -O).
    if ok_count + suppressed_total != circuit_count:
        raise RuntimeError(
            f"home-run conservation violated: ok={ok_count} "
            f"suppressed={suppressed_total} circuit_count={circuit_count}"
        )

    return HomeRunResult(
        per_circuit=per_circuit,
        total_home_run_m=total_home_run_m,
        ok_count=ok_count,
        suppressed_counts=suppressed_counts,
        circuit_count=circuit_count,
    )


# ---------------------------------------------------------------------------
# No-containment factory (#698b)
# ---------------------------------------------------------------------------


def no_containment_home_run_result(circuit_ids: Sequence[int]) -> HomeRunResult:
    """Build a :class:`HomeRunResult` for the case where no containment revision is supplied.

    Every circuit receives status ``"no_containment_sheet"`` with all metric terms set to None.
    The aggregate totals are honest: ``total_home_run_m=0.0``, ``ok_count=0``.

    This is NOT a routing failure — the caller explicitly chose not to provide a containment
    revision. The status propagates through the assembler as a None home-run term, which is
    treated identically to any other non-"ok" suppression (home_run_m=None → 0.0 in base_total).

    Parameters
    ----------
    circuit_ids:
        Ordered sequence of circuit IDs from the lighting partition.

    Returns
    -------
    HomeRunResult
        Sorted by circuit_id; conservation invariant holds.
    """
    ids = sorted(circuit_ids)
    per_circuit = tuple(
        CircuitHomeRun(
            circuit_id=cid,
            status="no_containment_sheet",
            home_run_m=None,
            d_panel_m=None,
            along_tray_m=None,
            d_area_m=None,
            lower_bound_m=None,
        )
        for cid in ids
    )
    circuit_count = len(ids)
    suppressed_counts: dict[str, int] = dict.fromkeys(_STATUS_ORDER, 0)
    suppressed_counts["no_containment_sheet"] = circuit_count
    return HomeRunResult(
        per_circuit=per_circuit,
        total_home_run_m=0.0,
        ok_count=0,
        suppressed_counts=suppressed_counts,
        circuit_count=circuit_count,
    )
