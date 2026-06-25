"""Cable connection-topology graph builder (issue #693).

Converts spline inputs (cable runs) and device footprints into a node/edge graph
suitable for downstream circuit analysis.

**Algorithm:**

1. Per spline: extract endpoint vertices via ``_xy``. Skip degenerate (<2 finite
   vertices) → increment ``dropped``.
2. Snap each endpoint to a ``snap_tol_m`` grid. One CableEdge per spline.
   Node ``point`` = mean of all endpoints that share a node_id.
3. ``degree`` = count of incident edge-endpoints. ``terminal`` = degree==1 AND
   not on a closed spline.
4. Terminal→device association: prefer bbox-containing device (contained=True,
   distance_m=0.0); else nearest within ``terminal_assoc_m`` by point-to-bbox
   clamped Euclidean distance; tie → lowest device_entity_id lexicographically.
   No match → device_entity_id=None, distance_m=None, contained=False.

**v1 architecture filter:** devices are pre-filtered by the caller via
``ARCHITECTURE_FAMILY_PATTERNS`` substring match on block_ref (see loaders).
A follow-up can swap to full ``classify_instance_kind``/``KIND_DEVICE`` once a
legend is wired.

Pure module — NO DB, ORM, FastAPI, SQLAlchemy, cv2, or shapely imports.
All geometry is pre-scaled to METRES (conversion_factor=1.0). Do NOT re-scale.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

from app.ingestion.centerline_contract import _xy

# ---------------------------------------------------------------------------
# Tolerance constants — all in metres
# ---------------------------------------------------------------------------

# PROVISIONAL (single-building E-630003): snap grid cell size (node identity).
_DEFAULT_SNAP_TOL_M: float = 0.30

# PROVISIONAL (single-building E-630003): max device search radius for terminal association.
_DEFAULT_TERMINAL_ASSOC_M: float = 2.5

# Epsilon for float distance comparisons — tie-break within this many metres treated as equal.
_EQ_EPSILON: float = 1e-9


# ---------------------------------------------------------------------------
# Input dataclasses (loader → builder)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SplineInput:
    """One spline entity from the DB, pre-converted to metre-scale vertices."""

    entity_id: str
    vertices: tuple[tuple[float, float], ...]
    closed: bool


@dataclass(frozen=True, slots=True)
class DeviceFootprint:
    """One device entity with its axis-aligned bounding box (or degenerate position fallback)."""

    entity_id: str
    block_ref: str | None
    kind: str
    bbox: tuple[float, float, float, float] | None  # (min_x, min_y, max_x, max_y)
    position: tuple[float, float] | None


# ---------------------------------------------------------------------------
# Output dataclasses (builder → consumers)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CableNode:
    """A topology node at a snapped grid position."""

    node_id: tuple[int, int]
    degree: int
    point: tuple[float, float]  # mean world coordinate of contributing endpoints


@dataclass(frozen=True, slots=True)
class CableEdge:
    """One cable segment (spline) connecting two topology nodes."""

    spline_entity_id: str
    node_a: tuple[int, int]
    node_b: tuple[int, int]
    closed: bool


@dataclass(frozen=True, slots=True)
class TerminalAssociation:
    """Association between a terminal node (degree==1) and the nearest device."""

    node_id: tuple[int, int]
    device_entity_id: str | None
    device_block_ref: str | None
    distance_m: float | None
    contained: bool


@dataclass(frozen=True, slots=True)
class CableGraph:
    """Complete topology graph for one drawing revision."""

    nodes: tuple[CableNode, ...]  # sorted by node_id
    edges: tuple[CableEdge, ...]  # sorted by spline_entity_id
    terminals: tuple[TerminalAssociation, ...]  # sorted by node_id
    spline_count: int
    node_count: int
    edge_count: int
    terminal_count: int
    interior_junction_count: int  # nodes with degree >= 2
    associated_terminal_count: int  # terminals with device_entity_id != None
    dropped: int  # splines skipped due to <2 finite vertices


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _snap(coord: tuple[float, float], tol: float) -> tuple[int, int]:
    """Map a world coordinate to a grid cell index for node identity."""
    return (round(coord[0] / tol), round(coord[1] / tol))


def _point_to_bbox_distance(px: float, py: float, bbox: tuple[float, float, float, float]) -> float:
    """Euclidean distance from point (px, py) to the nearest point on an AABB."""
    min_x, min_y, max_x, max_y = bbox
    # Clamp point to box, then measure.
    cx = max(min_x, min(px, max_x))
    cy = max(min_y, min(py, max_y))
    return math.hypot(px - cx, py - cy)


def _device_bbox(device: DeviceFootprint) -> tuple[float, float, float, float] | None:
    """Return the effective bbox for a device, using position as a degenerate fallback."""
    if device.bbox is not None:
        return device.bbox
    if device.position is not None:
        x, y = device.position
        return (x, y, x, y)
    return None


def _contains(bbox: tuple[float, float, float, float], px: float, py: float) -> bool:
    min_x, min_y, max_x, max_y = bbox
    return min_x <= px <= max_x and min_y <= py <= max_y


def _associate_terminal(
    px: float,
    py: float,
    devices: Sequence[DeviceFootprint],
    terminal_assoc_m: float,
) -> TerminalAssociation | None:
    """Return the best TerminalAssociation for a terminal node at (px, py).

    Returns None when no device is usable (caller wraps into no-match result).

    Precedence: contained (point inside bbox, distance_m=0.0) > nearest within
    terminal_assoc_m.

    **Tie-break is permutation-independent (two-pass):**
    - Contained: collect all containers, pick lowest entity_id.
    - Nearest: (a) find the global minimum distance; (b) collect all devices
      whose distance is within _EQ_EPSILON of that minimum; (c) pick lowest
      entity_id among them.  This is equivalent to sorting by (distance, entity_id)
      but guarantees the epsilon window is anchored on the true global minimum, not
      on whichever device happened to be processed first.
    """
    # Pass 1: partition candidates into contained vs. near.
    containers: list[DeviceFootprint] = []
    near_candidates: list[tuple[float, DeviceFootprint]] = []  # (distance, device)

    for device in devices:
        effective_bbox = _device_bbox(device)
        if effective_bbox is None:
            continue
        if _contains(effective_bbox, px, py):
            containers.append(device)
        else:
            dist = _point_to_bbox_distance(px, py, effective_bbox)
            if dist <= terminal_assoc_m:
                near_candidates.append((dist, device))

    # Contained wins over nearest; among containers pick lowest entity_id.
    if containers:
        best_contained = min(containers, key=lambda d: d.entity_id)
        return TerminalAssociation(
            node_id=(0, 0),  # placeholder; caller fills node_id
            device_entity_id=best_contained.entity_id,
            device_block_ref=best_contained.block_ref,
            distance_m=0.0,
            contained=True,
        )

    if not near_candidates:
        return None

    # Pass 2: find global minimum distance, then collect all devices within
    # _EQ_EPSILON of it, then pick lowest entity_id (permutation-independent).
    min_dist = min(d for d, _ in near_candidates)
    tied = [dev for d, dev in near_candidates if abs(d - min_dist) <= _EQ_EPSILON]
    best_near = min(tied, key=lambda dev: dev.entity_id)

    return TerminalAssociation(
        node_id=(0, 0),  # placeholder; caller fills node_id
        device_entity_id=best_near.entity_id,
        device_block_ref=best_near.block_ref,
        distance_m=min_dist,
        contained=False,
    )


# ---------------------------------------------------------------------------
# Public builder
# ---------------------------------------------------------------------------


def build_cable_graph(
    *,
    splines: Sequence[SplineInput],
    devices: Sequence[DeviceFootprint],
    snap_tol_m: float = _DEFAULT_SNAP_TOL_M,
    terminal_assoc_m: float = _DEFAULT_TERMINAL_ASSOC_M,
) -> CableGraph:
    """Build the cable connection-topology graph from spline and device inputs.

    All geometry must be pre-scaled to metres. Do NOT re-scale inside this function.

    ``snap_tol_m``: grid cell size for endpoint snapping (node identity).
    ``terminal_assoc_m``: max search radius for terminal→device association.

    Returns a fully frozen :class:`CableGraph`. Deterministic: output is identical
    regardless of input list ordering (all sort keys are fully ordered).
    """
    dropped = 0

    # --- Step 1 & 2: build edges and accumulate node endpoint sums ---

    # node_id → list of (world_x, world_y) endpoint coords snapped to it
    node_endpoints: dict[tuple[int, int], list[tuple[float, float]]] = {}
    # node_id → count of incident edge-endpoints (degree)
    node_degree: dict[tuple[int, int], int] = {}
    # node_ids that belong to closed-spline endpoints (excluded from terminal consideration)
    closed_node_ids: set[tuple[int, int]] = set()

    raw_edges: list[CableEdge] = []

    for spline in splines:
        # Extract finite (x,y) vertices via _xy.
        finite_verts: list[tuple[float, float]] = []
        for v in spline.vertices:
            # vertices are already (float,float) tuples from the loader;
            # but _xy handles dict-coords too — call it for safety on dict-shaped inputs.
            if isinstance(v, tuple) and len(v) >= 2:
                xy = v[:2]
                x, y = xy[0], xy[1]
                if math.isfinite(x) and math.isfinite(y):
                    finite_verts.append((float(x), float(y)))
            else:
                # dict-shaped coord (unlikely from SplineInput but tolerate)
                parsed = _xy(v)
                if parsed is not None:
                    finite_verts.append(parsed)

        if len(finite_verts) < 2:
            dropped += 1
            continue

        ep_a = finite_verts[0]
        ep_b = finite_verts[-1]
        nid_a = _snap(ep_a, snap_tol_m)
        nid_b = _snap(ep_b, snap_tol_m)

        node_endpoints.setdefault(nid_a, []).append(ep_a)
        node_endpoints.setdefault(nid_b, []).append(ep_b)
        node_degree[nid_a] = node_degree.get(nid_a, 0) + 1
        node_degree[nid_b] = node_degree.get(nid_b, 0) + 1

        if spline.closed:
            closed_node_ids.add(nid_a)
            closed_node_ids.add(nid_b)

        raw_edges.append(
            CableEdge(
                spline_entity_id=spline.entity_id,
                node_a=nid_a,
                node_b=nid_b,
                closed=spline.closed,
            )
        )

    # --- Step 3: build CableNode objects ---

    nodes: list[CableNode] = []
    for node_id in sorted(node_endpoints.keys()):
        coords = node_endpoints[node_id]
        mean_x = sum(c[0] for c in coords) / len(coords)
        mean_y = sum(c[1] for c in coords) / len(coords)
        degree = node_degree.get(node_id, 0)
        nodes.append(
            CableNode(
                node_id=node_id,
                degree=degree,
                point=(mean_x, mean_y),
            )
        )

    # Sort edges by spline_entity_id for determinism.
    edges = tuple(sorted(raw_edges, key=lambda e: e.spline_entity_id))

    # --- Step 4: terminal→device association ---

    terminals: list[TerminalAssociation] = []
    for node in sorted(nodes, key=lambda n: n.node_id):
        if node.degree != 1 or node.node_id in closed_node_ids:
            continue
        px, py = node.point
        assoc = _associate_terminal(px, py, devices, terminal_assoc_m)
        if assoc is not None:
            terminals.append(
                TerminalAssociation(
                    node_id=node.node_id,
                    device_entity_id=assoc.device_entity_id,
                    device_block_ref=assoc.device_block_ref,
                    distance_m=assoc.distance_m,
                    contained=assoc.contained,
                )
            )
        else:
            terminals.append(
                TerminalAssociation(
                    node_id=node.node_id,
                    device_entity_id=None,
                    device_block_ref=None,
                    distance_m=None,
                    contained=False,
                )
            )

    # --- Counts ---

    node_count = len(nodes)
    edge_count = len(edges)
    terminal_count = len(terminals)
    interior_junction_count = sum(1 for n in nodes if n.degree >= 2)
    associated_terminal_count = sum(1 for t in terminals if t.device_entity_id is not None)

    return CableGraph(
        nodes=tuple(nodes),
        edges=edges,
        terminals=tuple(terminals),
        spline_count=len(splines),
        node_count=node_count,
        edge_count=edge_count,
        terminal_count=terminal_count,
        interior_junction_count=interior_junction_count,
        associated_terminal_count=associated_terminal_count,
        dropped=dropped,
    )
