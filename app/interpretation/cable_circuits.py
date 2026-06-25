"""Cable circuit partitioner — connected-component decomposition of a CableGraph (issue #694).

Partitions a :class:`~app.interpretation.cable_topology.CableGraph` into connected
components over its edges, yielding one :class:`CableCircuit` per component.

**Algorithm:**

1. Union-find (with path compression) over ``graph.edges``: union(node_a, node_b) per edge.
   Self-loops (node_a == node_b, closed splines) are no-ops — the node belongs to its own
   singleton component.
2. Each connected component collects its node_ids (all nodes that share a root) and its
   edge's spline_entity_ids.
3. Device associations are collected from ``graph.terminals`` whose node_id is in the
   component and whose device_entity_id is not None (distinct, sorted).
4. The **anchor** device is the component member whose ``device_block_ref`` matches any
   pattern in ``distribution_block_patterns`` (case-insensitive substring search). If
   multiple qualify, the lowest ``device_entity_id`` wins. If none match, anchor is None.

**Determinism:** after finding components, they are sorted by their minimum node_id
(tuples sort lexicographically), then circuit_id = 0, 1, 2, … is assigned in that order.
This makes circuit_id independent of input ordering, mirroring #693's design bar.

**Conservation invariant:**  ``sum(c.edge_count for c in circuits) == graph.edge_count``.
This is asserted on every call.

Pure module — NO DB, ORM, FastAPI, SQLAlchemy, cv2, or shapely imports.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.interpretation.cable_topology import CableGraph

# ---------------------------------------------------------------------------
# Anchor heuristic patterns — v1 heuristic, replaceable by legend/device-identity
# classification in a future iteration.
#
# These case-insensitive substring patterns identify LV distribution boards and
# panel boards, based on E-630003 evidence:
#   Pr_60_70_22_22_DB-TST1 - Standard Distribution Board
#   Pr_60_70_22_85_PB800 - PANEL BOARD 800A
# The Uniclass code prefix "Pr_60_70_22" covers LV distribution/panel products.
# Bare "DB"/"PB" are intentionally avoided — too many false positives as substrings.
# ---------------------------------------------------------------------------

_DEFAULT_DISTRIBUTION_PATTERNS: tuple[str, ...] = (
    "Distribution Board",
    "Panel Board",
    "Switchboard",
    "Consumer Unit",
    "Pr_60_70_22",
)


# ---------------------------------------------------------------------------
# Output dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CableCircuit:
    """One connected component of the cable topology graph — a candidate circuit."""

    circuit_id: int
    node_ids: tuple[tuple[int, int], ...]  # sorted
    spline_entity_ids: tuple[str, ...]  # sorted
    device_entity_ids: tuple[str, ...]  # sorted, distinct, non-None terminals in this component
    anchor_device_entity_id: str | None  # distribution board / panel in this circuit, else None
    edge_count: int
    node_count: int
    device_count: int


@dataclass(frozen=True, slots=True)
class CableCircuitSet:
    """Complete partitioning of a CableGraph into circuits."""

    circuits: tuple[CableCircuit, ...]  # sorted by circuit_id
    circuit_count: int
    edge_count: int  # == graph.edge_count (conservation)
    assigned_edge_count: int  # edges placed into a circuit (== edge_count by invariant)
    device_count: int  # distinct devices across all circuits


# ---------------------------------------------------------------------------
# Union-find (path compression, no rank — input sizes are small)
# ---------------------------------------------------------------------------


class _UnionFind:
    """Minimal union-find with path compression over tuple[int,int] node_ids."""

    def __init__(self) -> None:
        self._parent: dict[tuple[int, int], tuple[int, int]] = {}

    def ensure(self, x: tuple[int, int]) -> None:
        if x not in self._parent:
            self._parent[x] = x

    def find(self, x: tuple[int, int]) -> tuple[int, int]:
        self.ensure(x)
        root = x
        while self._parent[root] != root:
            root = self._parent[root]
        # Path compression — iterative to avoid recursion depth on large graphs.
        node = x
        while self._parent[node] != root:
            nxt = self._parent[node]
            self._parent[node] = root
            node = nxt
        return root

    def union(self, a: tuple[int, int], b: tuple[int, int]) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        # Deterministic merge: always attach the larger root to the smaller root so
        # the canonical root is the lexicographically smaller of the two roots.
        if rb < ra:
            ra, rb = rb, ra
        self._parent[rb] = ra


# ---------------------------------------------------------------------------
# Public partitioner
# ---------------------------------------------------------------------------


def partition_circuits(
    graph: CableGraph,
    *,
    distribution_block_patterns: tuple[str, ...] = _DEFAULT_DISTRIBUTION_PATTERNS,
) -> CableCircuitSet:
    """Partition *graph* into connected-component circuits.

    Each edge defines an adjacency between its two endpoint nodes.  A circuit is
    a maximal set of nodes (and all edges incident to them) reachable from each
    other.  Isolated nodes (nodes that appear only as self-loop endpoints, i.e.
    single closed splines) form their own singleton circuits.

    Parameters
    ----------
    graph:
        A :class:`~app.interpretation.cable_topology.CableGraph` produced by
        :func:`~app.interpretation.cable_topology.build_cable_graph`.
    distribution_block_patterns:
        Case-insensitive substrings identifying distribution/panel devices.
        Matched against ``device_block_ref`` of each terminal association.

    Returns
    -------
    CableCircuitSet
        All circuits with the conservation invariant
        ``assigned_edge_count == graph.edge_count`` asserted.
    """
    uf = _UnionFind()

    # Ensure every node in the graph is registered, even those that appear only
    # in self-loops so they form their own singleton components.
    for node in graph.nodes:
        uf.ensure(node.node_id)

    # Union endpoints for every edge.  Self-loops (node_a == node_b) are no-ops
    # because find(x) == find(x), so union returns immediately — the node stays
    # in its own component.
    for edge in graph.edges:
        uf.union(edge.node_a, edge.node_b)

    # Build a lookup from node_id → terminal associations (with a device).
    terminal_device: dict[tuple[int, int], list[tuple[str, str | None]]] = {}
    for term in graph.terminals:
        if term.device_entity_id is not None:
            terminal_device.setdefault(term.node_id, []).append(
                (term.device_entity_id, term.device_block_ref)
            )

    # Group nodes by root — sort roots deterministically.
    root_to_nodes: dict[tuple[int, int], list[tuple[int, int]]] = {}
    for node in graph.nodes:
        root = uf.find(node.node_id)
        root_to_nodes.setdefault(root, []).append(node.node_id)

    # Group edges by root of either endpoint (they share a root by construction).
    root_to_edges: dict[tuple[int, int], list[str]] = {}
    for edge in graph.edges:
        root = uf.find(edge.node_a)
        root_to_edges.setdefault(root, []).append(edge.spline_entity_id)

    # Sort components by their minimum node_id for deterministic circuit_id assignment.
    sorted_roots = sorted(
        root_to_nodes.keys(),
        key=lambda r: min(root_to_nodes[r]),
    )

    lower_patterns = tuple(p.lower() for p in distribution_block_patterns)

    circuits: list[CableCircuit] = []
    for circuit_id, root in enumerate(sorted_roots):
        node_ids = tuple(sorted(root_to_nodes[root]))
        spline_ids = tuple(sorted(root_to_edges.get(root, [])))

        # Collect distinct device_entity_ids from terminals in this component.
        device_ids_set: set[str] = set()
        device_block_map: dict[str, str | None] = {}
        for nid in node_ids:
            for dev_id, block_ref in terminal_device.get(nid, []):
                device_ids_set.add(dev_id)
                device_block_map[dev_id] = block_ref

        device_entity_ids = tuple(sorted(device_ids_set))

        # Anchor: pick the lowest entity_id among devices whose block_ref matches
        # any distribution pattern (case-insensitive substring).
        anchor: str | None = None
        anchor_candidates: list[str] = []
        for dev_id in device_entity_ids:
            block_ref = device_block_map.get(dev_id)
            if block_ref is not None:
                block_lower = block_ref.lower()
                if any(pat in block_lower for pat in lower_patterns):
                    anchor_candidates.append(dev_id)
        if anchor_candidates:
            anchor = min(anchor_candidates)

        circuits.append(
            CableCircuit(
                circuit_id=circuit_id,
                node_ids=node_ids,
                spline_entity_ids=spline_ids,
                device_entity_ids=device_entity_ids,
                anchor_device_entity_id=anchor,
                edge_count=len(spline_ids),
                node_count=len(node_ids),
                device_count=len(device_entity_ids),
            )
        )

    assigned_edge_count = sum(c.edge_count for c in circuits)
    # Conservation invariant — every edge must be in exactly one circuit.
    assert assigned_edge_count == graph.edge_count, (
        f"Edge conservation violated: assigned {assigned_edge_count} "
        f"but graph has {graph.edge_count}"
    )

    # Distinct devices across all circuits (a device cannot belong to two circuits
    # because a node belongs to exactly one component).
    all_device_ids: set[str] = set()
    for c in circuits:
        all_device_ids.update(c.device_entity_ids)

    return CableCircuitSet(
        circuits=tuple(circuits),
        circuit_count=len(circuits),
        edge_count=graph.edge_count,
        assigned_edge_count=assigned_edge_count,
        device_count=len(all_device_ids),
    )
