"""Tests for app.interpretation.rise_drop (issue #619)."""

from __future__ import annotations

import random

from app.interpretation.rise_drop import (
    KIND_DROP,
    KIND_RISE,
    STATUS_RESOLVED,
    STATUS_UNKNOWN,
    RiseDropEntity,
    RiseDropResult,
    cluster_rise_drop_symbols,
)
from app.interpretation.service_legend import (
    ServiceLegend,
    SwatchInput,
    from_colour_swatches,
    fuse,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_arc(
    entity_id: str, cx: float, cy: float, color: dict[str, object] | None = None
) -> RiseDropEntity:
    return RiseDropEntity(
        entity_id=entity_id,
        entity_type="arc",
        layer_ref="RISE",
        color=color,
        geometry={"center": {"x": cx, "y": cy, "z": 0.0}},
    )


def _make_hatch(
    entity_id: str,
    vertices: list[tuple[float, float]],
    color: dict[str, object] | None = None,
    *,
    use_bbox: bool = False,
) -> RiseDropEntity:
    if use_bbox:
        # Geometry with no vertices -- only bbox.
        xs = [v[0] for v in vertices]
        ys = [v[1] for v in vertices]
        geom: dict[str, object] = {
            "bbox": {
                "min": {"x": min(xs), "y": min(ys), "z": 0.0},
                "max": {"x": max(xs), "y": max(ys), "z": 0.0},
            }
        }
    else:
        geom = {"vertices": [{"x": x, "y": y, "z": 0.0} for x, y in vertices]}
    return RiseDropEntity(
        entity_id=entity_id,
        entity_type="hatch",
        layer_ref="RISE",
        color=color,
        geometry=geom,
    )


def _color_idx(n: int) -> dict[str, object]:
    return {"index": n}


def _color_rgb(hex_str: str) -> dict[str, object]:
    return {"rgb": hex_str}


def _legend_from_swatches(pairs: list[tuple[dict[str, object], str]]) -> ServiceLegend:
    """Build a ServiceLegend from (color_dict, discipline) pairs."""
    swatches = [SwatchInput(color=c, text=disc) for c, disc in pairs]
    return fuse(from_colour_swatches(swatches))


_EMPTY_LEGEND = ServiceLegend(entries=())

# ---------------------------------------------------------------------------
# Test 1: 3 concentric arcs at one center -> 1 symbol
# ---------------------------------------------------------------------------


def test_three_concentric_arcs_one_symbol() -> None:
    cx, cy = 100.0, 200.0
    color = _color_idx(3)
    entities = [
        _make_arc("a1", cx, cy, color),  # radius 7.5 (not encoded in geometry -- just anchor)
        _make_arc("a2", cx, cy, color),  # radius 15
        _make_arc("a3", cx, cy, color),  # radius 27
    ]
    result = cluster_rise_drop_symbols(entities, _EMPTY_LEGEND, kind=KIND_RISE)
    assert isinstance(result, RiseDropResult)
    assert len(result.symbols) == 1
    sym = result.symbols[0]
    assert sym.entity_ids == ("a1", "a2", "a3")
    assert sym.kind == KIND_RISE


# ---------------------------------------------------------------------------
# Test 2: Concentric arcs + hatch at same center -> 1 symbol
# ---------------------------------------------------------------------------


def test_arcs_and_hatch_fold_into_one_symbol() -> None:
    cx, cy = 50.0, 75.0
    # Hatch vertices: a square around (cx, cy).
    vertices = [(cx - 5, cy - 5), (cx + 5, cy - 5), (cx + 5, cy + 5), (cx - 5, cy + 5)]
    color = _color_idx(5)
    entities = [
        _make_arc("arc1", cx, cy, color),
        _make_arc("arc2", cx, cy, color),
        _make_hatch("hatch1", vertices, color),
    ]
    result = cluster_rise_drop_symbols(entities, _EMPTY_LEGEND, kind=KIND_RISE)
    assert len(result.symbols) == 1
    sym = result.symbols[0]
    assert set(sym.entity_ids) == {"arc1", "arc2", "hatch1"}
    # Anchor should be approximately (cx, cy).
    assert abs(sym.anchor_point[0] - cx) < 1.0
    assert abs(sym.anchor_point[1] - cy) < 1.0


# ---------------------------------------------------------------------------
# Test 3: Two centers >epsilon apart -> 2 symbols
# ---------------------------------------------------------------------------


def test_two_centers_two_symbols() -> None:
    color = _color_idx(1)
    entities = [
        _make_arc("a1", 0.0, 0.0, color),
        _make_arc("a2", 5.0, 0.0, color),  # 5 units apart; epsilon=1.0 by default
    ]
    result = cluster_rise_drop_symbols(entities, _EMPTY_LEGEND, kind=KIND_RISE)
    assert len(result.symbols) == 2


# ---------------------------------------------------------------------------
# Test 4: 27 rise centers + 7 drop centers (synthetic)
# ---------------------------------------------------------------------------


def test_27_rise_7_drop_symbols() -> None:
    color = _color_idx(2)
    rise_entities = [_make_arc(f"rise_{i}", float(i * 10), 0.0, color) for i in range(27)]
    drop_entities = [_make_arc(f"drop_{i}", float(i * 10), 100.0, color) for i in range(7)]
    rise_result = cluster_rise_drop_symbols(rise_entities, _EMPTY_LEGEND, kind=KIND_RISE)
    drop_result = cluster_rise_drop_symbols(drop_entities, _EMPTY_LEGEND, kind=KIND_DROP)
    assert len(rise_result.symbols) == 27
    assert len(drop_result.symbols) == 7


# ---------------------------------------------------------------------------
# Test 5: idx150 -> RESOLVED; unknown colour -> UNKNOWN + discipline None
# ---------------------------------------------------------------------------


def test_legend_resolved_and_unknown() -> None:
    legend = _legend_from_swatches([(_color_idx(150), "SPRINKLER")])
    resolved_ent = _make_arc("r1", 0.0, 0.0, _color_idx(150))
    unknown_ent = _make_arc("u1", 50.0, 50.0, _color_idx(999))

    result = cluster_rise_drop_symbols([resolved_ent, unknown_ent], legend, kind=KIND_RISE)
    assert len(result.symbols) == 2

    sym_by_id = {s.entity_ids[0]: s for s in result.symbols}

    resolved = sym_by_id["r1"]
    assert resolved.status == STATUS_RESOLVED
    assert resolved.discipline == "SPRINKLER"
    assert resolved.colour_key == "idx:150"

    unknown = sym_by_id["u1"]
    assert unknown.status == STATUS_UNKNOWN
    assert unknown.discipline is None
    assert unknown.colour_key == "idx:999"


# ---------------------------------------------------------------------------
# Test 6: Entity with no usable geometry excluded; rest of cluster intact
# ---------------------------------------------------------------------------


def test_degenerate_entity_excluded() -> None:
    color = _color_idx(1)
    entities = [
        _make_arc("good1", 10.0, 10.0, color),
        _make_arc("good2", 10.0, 10.0, color),
        # No geometry at all.
        RiseDropEntity(
            entity_id="bad1",
            entity_type="arc",
            layer_ref=None,
            color=color,
            geometry=None,
        ),
        # Empty geometry dict.
        RiseDropEntity(
            entity_id="bad2",
            entity_type="arc",
            layer_ref=None,
            color=color,
            geometry={},
        ),
    ]
    result = cluster_rise_drop_symbols(entities, _EMPTY_LEGEND, kind=KIND_RISE)
    assert len(result.symbols) == 1
    sym = result.symbols[0]
    assert "bad1" not in sym.entity_ids
    assert "bad2" not in sym.entity_ids
    assert set(sym.entity_ids) == {"good1", "good2"}


# ---------------------------------------------------------------------------
# Test 7: Permutation invariance
# ---------------------------------------------------------------------------


def test_permutation_invariant() -> None:
    color = _color_idx(7)
    entities = [
        _make_arc(f"e{i}", float((i % 5) * 10), float((i // 5) * 10), color) for i in range(15)
    ]
    reference = cluster_rise_drop_symbols(entities, _EMPTY_LEGEND, kind=KIND_RISE)

    rng = random.Random(42)
    for _ in range(10):
        shuffled = list(entities)
        rng.shuffle(shuffled)
        result = cluster_rise_drop_symbols(shuffled, _EMPTY_LEGEND, kind=KIND_RISE)
        assert result == reference, "Output changed under permutation"


# ---------------------------------------------------------------------------
# Test 8: kind propagated correctly
# ---------------------------------------------------------------------------


def test_kind_propagated() -> None:
    color = _color_idx(1)
    rise_entities = [_make_arc("r1", 0.0, 0.0, color)]
    drop_entities = [_make_arc("d1", 0.0, 0.0, color)]

    rise_result = cluster_rise_drop_symbols(rise_entities, _EMPTY_LEGEND, kind=KIND_RISE)
    drop_result = cluster_rise_drop_symbols(drop_entities, _EMPTY_LEGEND, kind=KIND_DROP)

    assert rise_result.symbols[0].kind == KIND_RISE
    assert drop_result.symbols[0].kind == KIND_DROP


# ---------------------------------------------------------------------------
# Test: hatch bbox fallback when vertices absent
# ---------------------------------------------------------------------------


def test_hatch_bbox_fallback() -> None:
    cx, cy = 30.0, 40.0
    vertices = [(cx - 5, cy - 5), (cx + 5, cy + 5)]  # used only to compute bbox
    hatch = _make_hatch("h1", vertices, _color_idx(1), use_bbox=True)
    result = cluster_rise_drop_symbols([hatch], _EMPTY_LEGEND, kind=KIND_RISE)
    assert len(result.symbols) == 1
    sym = result.symbols[0]
    assert abs(sym.anchor_point[0] - cx) < 0.01
    assert abs(sym.anchor_point[1] - cy) < 0.01
