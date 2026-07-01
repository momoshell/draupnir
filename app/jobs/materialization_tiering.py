"""Entity-reduction materialization-tier predicate (#831 PR-2).

Stamps low-value hatch-fill sliver LINE entities as ``fill_noise`` at ingest time so
storage can eventually be reclaimed (ADR-011: tier, don't drop — every entity is still
persisted, just excluded from loaders' default query scope). The predicate is
conservative-by-default: any ambiguity resolves to ``primary``.

Two pieces:

1. :func:`compute_line_component_sizes` — a near-linear union-find over a revision's
   LINE entities (by shared endpoint, within a snap tolerance) producing each line's
   connected-component size. Dense hatch-fill sliver linework forms large components
   (hundreds+ of tiny mutually-touching segments); a single stray sub-cm line on a
   load-bearing layer does not.
2. :func:`classify_materialization_tier` — the pure 4-way conjunction predicate that
   consumes the precomputed component size plus the entity's own type/length/layer.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

# ---------------------------------------------------------------------------
# Thresholds (module constants; #831 PR-0 scout-validated on P-520001)
# ---------------------------------------------------------------------------

#: Segment length (metres) below which a LINE is a candidate sliver. DWG coords are
#: already pre-scaled to metres (see app/jobs/revision_materialization.py) — never
#: re-apply conversion_factor here.
SLIVER_LEN_M: float = 0.01

#: Minimum connected-component size (shared-endpoint LINE graph) for a sliver's
#: component to be considered dense hatch-fill noise rather than incidental geometry.
FILL_COMPONENT_MIN: int = 50

#: Endpoint snap tolerance (metres) for the shared-endpoint connectivity graph.
ENDPOINT_SNAP_TOLERANCE_M: float = 1e-6

# ---------------------------------------------------------------------------
# Consumed-layer tokens (layer gate, predicate arm 4)
# ---------------------------------------------------------------------------
#
# Every ilike/substring token any downstream interpretation loader currently uses to
# select a layer by naming convention (rooms, walls, routed services, containment,
# devices, grid, legend). A layer matching ANY of these tokens is NEVER tiered
# fill_noise, regardless of how dense/sliver-like its LINE population is — this is the
# condition that keeps a named architecture/service layer (which can legitimately carry
# a dense sub-cm component) at "primary". Sourced from (as of #831 PR-2):
#   - app.interpretation.explicit_rooms.ROOM_BOUNDARY_LAYER_KEYWORDS
#   - app.interpretation.wall_rooms.WALL_LAYER_KEYWORDS
#   - app.interpretation.service_takeoff_loaders._DEFAULT_CONTAINER_LAYER_TOKENS
#   - app.interpretation.service_takeoff_loaders._DEFAULT_CENTERLINE_LAYER_TOKENS
#   - app.interpretation.service_takeoff_loaders._DEFAULT_TAG_LAYER_TOKENS
#   - app.interpretation.service_takeoff_loaders._DEFAULT_RISE_LAYER_TOKENS
#   - app.interpretation.service_takeoff_loaders._DEFAULT_DROP_LAYER_TOKENS
#   - app.interpretation.devices._DEFAULT_TAG_TOKENS
#   - app.interpretation.grid_registration_loaders._DEFAULT_GRID_LAYER_TOKENS
#   - legend/key layer ilike literals in build_service_legend / build_containment_legend_db /
#     build_mech_service_legend_db / load_legend_text_candidates
CONSUMED_LAYER_TOKENS: tuple[str, ...] = (
    "wall",
    "room",
    "space",
    "rmsep",
    "pipe",
    "tray",
    "ladder",
    "trunking",
    "basket",
    "conduit",
    "duct",
    "center line",
    "centre line",
    "centerline",
    "centreline",
    "tag",
    "device",
    "rise",
    "riser",
    "drop",
    "z030g",
    "grid",
    "legend",
    "key",
)


def is_consumed_layer(
    layer_ref: str | None, *, tokens: Sequence[str] = CONSUMED_LAYER_TOKENS
) -> bool:
    """Return whether *layer_ref* matches any downstream-consumer naming token.

    Case-insensitive substring match (mirrors the ``ilike("%token%")`` pattern every
    caller above uses). ``None``/empty layer refs never match (conservative-by-default:
    an entity with no layer identity cannot be proven safe to tier, but predicate arm 4
    only needs to prove it is NOT a consumed layer — an unnamed layer legitimately
    passes this arm; the component-size and length arms still gate it).
    """
    if not layer_ref:
        return False
    name = layer_ref.lower()
    return any(token in name for token in tokens)


# ---------------------------------------------------------------------------
# Connectivity precompute (shared-endpoint union-find over LINE entities)
# ---------------------------------------------------------------------------


def _round_point(point: tuple[float, float], *, tolerance: float) -> tuple[float, float]:
    """Snap a point to a tolerance-sized grid so near-coincident endpoints unify."""
    if tolerance <= 0.0:
        return point
    scale = 1.0 / tolerance
    return (round(point[0] * scale), round(point[1] * scale))


def _line_endpoints(
    geometry_json: Mapping[str, Any] | None,
) -> tuple[tuple[float, float], tuple[float, float]] | None:
    """Extract a LINE entity's (start, end) endpoints from its geometry payload."""
    if not isinstance(geometry_json, Mapping):
        return None
    start = _point(geometry_json.get("start"))
    end = _point(geometry_json.get("end"))
    if start is None or end is None:
        return None
    return start, end


def _point(value: Any) -> tuple[float, float] | None:
    if isinstance(value, Mapping):
        x, y = value.get("x"), value.get("y")
    elif isinstance(value, (list, tuple)) and len(value) >= 2:
        x, y = value[0], value[1]
    else:
        return None
    if isinstance(x, bool) or isinstance(y, bool):
        return None
    if isinstance(x, (int, float)) and isinstance(y, (int, float)):
        return (float(x), float(y))
    return None


class _UnionFind:
    """Minimal path-compressed, union-by-size disjoint-set forest."""

    __slots__ = ("_parent", "_size")

    def __init__(self) -> None:
        self._parent: dict[Any, Any] = {}
        self._size: dict[Any, int] = {}

    def find(self, key: Any) -> Any:
        self._parent.setdefault(key, key)
        self._size.setdefault(key, 1)
        root = key
        while self._parent[root] != root:
            root = self._parent[root]
        # Path compression.
        while self._parent[key] != root:
            self._parent[key], key = root, self._parent[key]
        return root

    def union(self, a: Any, b: Any) -> None:
        root_a, root_b = self.find(a), self.find(b)
        if root_a == root_b:
            return
        if self._size[root_a] < self._size[root_b]:
            root_a, root_b = root_b, root_a
        self._parent[root_b] = root_a
        self._size[root_a] += self._size[root_b]


def compute_line_component_sizes(
    line_entities: Sequence[tuple[str, Mapping[str, Any] | None]],
    *,
    snap_tolerance: float = ENDPOINT_SNAP_TOLERANCE_M,
) -> dict[str, int]:
    """Return each LINE entity's connected-component size (shared-endpoint graph).

    ``line_entities`` is a sequence of ``(entity_key, geometry_json)`` pairs for the
    revision's LINE entities only (caller filters by ``entity_type``). Endpoints are
    snapped to a tolerance-sized grid before union — this is a near-linear (union-find,
    effectively O(n) amortized) precompute safe on ~200k-line revisions.

    Entities with unreadable/missing endpoints get their own singleton component (size 1)
    — conservative: they can never satisfy ``FILL_COMPONENT_MIN`` and so never tier
    fill_noise via the component-size arm.
    """
    uf = _UnionFind()
    entity_endpoint_keys: dict[str, tuple[Any, Any]] = {}

    for entity_key, geometry_json in line_entities:
        endpoints = _line_endpoints(geometry_json)
        if endpoints is None:
            # Give it a private, never-shared node so it forms its own singleton.
            uf.find(("__isolated__", entity_key))
            entity_endpoint_keys[entity_key] = (
                ("__isolated__", entity_key),
                ("__isolated__", entity_key),
            )
            continue
        start, end = endpoints
        start_key = _round_point(start, tolerance=snap_tolerance)
        end_key = _round_point(end, tolerance=snap_tolerance)
        uf.union(start_key, end_key)
        entity_endpoint_keys[entity_key] = (start_key, end_key)

    # Component membership: every entity's component id is its (post-union) start-key root.
    component_members: dict[Any, set[str]] = {}
    for entity_key, (start_key, _end_key) in entity_endpoint_keys.items():
        root = uf.find(start_key)
        component_members.setdefault(root, set()).add(entity_key)

    component_size_by_root = {root: len(members) for root, members in component_members.items()}
    return {
        entity_key: component_size_by_root[uf.find(start_key)]
        for entity_key, (start_key, _end_key) in entity_endpoint_keys.items()
    }


def _segment_length(geometry_json: Mapping[str, Any] | None) -> float | None:
    """Euclidean length of a LINE entity's start/end segment, or None if unreadable."""
    endpoints = _line_endpoints(geometry_json)
    if endpoints is None:
        return None
    (sx, sy), (ex, ey) = endpoints
    return float(((ex - sx) ** 2 + (ey - sy) ** 2) ** 0.5)


# ---------------------------------------------------------------------------
# Predicate
# ---------------------------------------------------------------------------

TIER_PRIMARY = "primary"
TIER_FILL_NOISE = "fill_noise"


def classify_materialization_tier(
    *,
    entity_type: str,
    geometry_json: Mapping[str, Any] | None,
    layer_ref: str | None,
    component_size: int,
    consumed_layer_tokens: Sequence[str] = CONSUMED_LAYER_TOKENS,
    sliver_len_m: float = SLIVER_LEN_M,
    fill_component_min: int = FILL_COMPONENT_MIN,
) -> str:
    """Classify a single entity's materialization tier (pure; conservative-by-default).

    Returns ``"fill_noise"`` iff ALL of:

    1. ``entity_type == "line"`` (never polyline/arc/hatch/insert/text — narrow scope).
    2. Its segment length is readable and ``< sliver_len_m``.
    3. Its precomputed connected-component size is ``>= fill_component_min``.
    4. Its layer is NOT matched by any token in ``consumed_layer_tokens`` (load-bearing:
       excludes named architecture/service layers even when their sub-cm component
       happens to be dense).

    Any missing/unreadable input (e.g. no geometry, unparseable endpoints) fails the
    corresponding arm and the entity stays ``"primary"`` — never guess toward
    fill_noise.
    """
    if entity_type != "line":
        return TIER_PRIMARY

    length = _segment_length(geometry_json)
    if length is None or not (length < sliver_len_m):
        return TIER_PRIMARY

    if component_size < fill_component_min:
        return TIER_PRIMARY

    if is_consumed_layer(layer_ref, tokens=consumed_layer_tokens):
        return TIER_PRIMARY

    return TIER_FILL_NOISE
