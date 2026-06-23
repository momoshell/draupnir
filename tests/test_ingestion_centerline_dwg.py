"""Unit tests for app.ingestion.centerline_dwg.

All tests are pure-algorithm: no DB, no DWG files read.

Oracle regression grounding
---------------------------
Real M-540003 drawing: Pipes layer = 477,525 du (double-line walls,
pipe diameter Ø42/Ø54).  Authored Center Line layer = 244,010 du.
The authored centerline IS the per-pair midline — mean of the two wall
lengths per pair.

Synthetic fixture strategy: build N antiparallel wall pairs so that the
sum of midline lengths equals 244,010 du exactly.  Each pair is a
horizontal wall of length L_i at y=0 and a reversed wall at y=D_i
(offset within the accepted range [2, 80]).  The midline length equals
the wall length (start-averaged endpoints collapse to the same length
for equal-length parallel segments).

We use 10 pairs with lengths summing to 244,010 du.  The pairing
algorithm recovers all 10 pairs -> derived length == 244,010 du.
Assert within [0.95, 1.10] * 244,010 (allows for floating-point noise;
the exact fixture should hit 1.0x).
"""

from __future__ import annotations

import math
import random

from app.ingestion.centerline_contract import entity_group_drawn_length
from app.ingestion.centerline_dwg import (
    _DEFAULT_CENTERLINE_LAYER_TOKENS,
    _UNPAIRED_LENGTH_FACTOR,
    dwg_centerlines,
)
from app.interpretation.routed_runs import RunGroup

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_group(
    entity_ids: tuple[str, ...],
    layer_ref: str | None = None,
    colour_key: str | None = None,
) -> RunGroup:
    return RunGroup(
        layer_ref=layer_ref,
        colour_key=colour_key,
        colour_index=None,
        colour_rgb=None,
        status="unknown",
        discipline=None,
        basis="unresolved",
        source_layers=(),
        confidence=None,
        competing_disciplines=(),
        entity_ids=entity_ids,
    )


def _line_geom(sx: float, sy: float, ex: float, ey: float) -> dict[str, list[float]]:
    return {"start": [sx, sy], "end": [ex, ey]}


def _polyline_geom(pts: list[tuple[float, float]]) -> dict[str, list[list[float]]]:
    return {"vertices": [[x, y] for x, y in pts]}


# ---------------------------------------------------------------------------
# Test: authored path
# ---------------------------------------------------------------------------


def test_authored_path_producer_kind_and_length() -> None:
    """A group whose layer_ref contains a centerline token -> dwg_authored.

    The length_du must equal entity_group_drawn_length of the same entities.

    Arrange: two line entities on a "Center Line" layer.
    Act:     dwg_centerlines.
    Assert:  producer_kind="dwg_authored", length_du matches helper.
    """
    layer = "M-PIPE Center Line"
    geom: dict[str, dict[str, list[float]]] = {
        "e1": _line_geom(0, 0, 100, 0),
        "e2": _line_geom(100, 0, 250, 0),
    }
    entity_ids: tuple[str, ...] = ("e1", "e2")
    group = _make_group(entity_ids, layer_ref=layer)

    results = dwg_centerlines([group], geom)
    assert len(results) == 1
    cl = results[0]

    assert cl.producer_kind == "dwg_authored"
    expected = entity_group_drawn_length(entity_ids, geom)
    assert math.isclose(cl.geometry.length_du, expected, rel_tol=1e-9)


def test_authored_path_all_tokens_recognised() -> None:
    """Each token in _DEFAULT_CENTERLINE_LAYER_TOKENS triggers dwg_authored."""
    geom = {"e1": _line_geom(0, 0, 50, 0)}
    entity_ids: tuple[str, ...] = ("e1",)
    for token in _DEFAULT_CENTERLINE_LAYER_TOKENS:
        group = _make_group(entity_ids, layer_ref=f"prefix {token.upper()} suffix")
        results = dwg_centerlines([group], geom)
        assert results[0].producer_kind == "dwg_authored", (
            f"Token '{token}' did not trigger dwg_authored"
        )


# ---------------------------------------------------------------------------
# Test: derive clean pair
# ---------------------------------------------------------------------------


def test_derive_clean_pair_geometry() -> None:
    """Two antiparallel equal-length segments offset by D=42 -> one midline.

    Arrange:
        Segment i: (0,0) -> (100,0)  length=100, direction=(1,0)
        Segment j: (100,42) -> (0,42) length=100, direction=(-1,0) [antiparallel]

    Expected:
        ONE midline polyline at y=21, length=100.
        producer_kind="dwg_derived".
        length_du == 100.0.

    Grounding: midpoint-averaged endpoints:
        start = avg((0,0),(100,42)) = (50,21)
        end   = avg((100,0),(0,42)) = (50,21)  -- same point if naive average

    Wait -- the midline_polyline aligns j first.  After flipping j:
        s_j=(0,42), e_j=(100,42)
        mid_s = avg((0,0),(0,42)) = (0,21)
        mid_e = avg((100,0),(100,42)) = (100,21)
        length = 100.
    """
    geom = {
        "wall_a": _line_geom(0, 0, 100, 0),
        "wall_b": _line_geom(100, 42, 0, 42),  # antiparallel
    }
    entity_ids: tuple[str, ...] = ("wall_a", "wall_b")
    group = _make_group(entity_ids, layer_ref="M-PIPE Pipes")  # no token -> derived

    results = dwg_centerlines([group], geom)
    cl = results[0]

    assert cl.producer_kind == "dwg_derived"
    assert len(cl.geometry.polylines) == 1
    midline = cl.geometry.polylines[0]
    assert len(midline) == 2
    mid_s, mid_e = midline
    # Midline should be at y=21
    assert math.isclose(mid_s[1], 21.0, abs_tol=1e-9)
    assert math.isclose(mid_e[1], 21.0, abs_tol=1e-9)
    assert math.isclose(cl.geometry.length_du, 100.0, rel_tol=1e-9)


# ---------------------------------------------------------------------------
# Test: oracle regression (synthetic fixture ~ 244,010 du)
# ---------------------------------------------------------------------------

# Build 10 pairs whose individual lengths sum to 244,010 du.
# Each pair is a horizontal line of length L at y=0 and its antiparallel twin
# at y=D (D chosen within [2,80]).
#
# With equal-length pairs and aligned endpoints, the midline length equals L
# (the average of L and L).  So derived_length == sum(L_i) == 244,010.
_TARGET_LENGTH = 244_010.0
_PAIR_LENGTHS = [
    30_000.0,
    28_000.0,
    26_000.0,
    25_000.0,
    24_010.0,
    24_000.0,
    23_000.0,
    22_000.0,
    21_000.0,
    21_000.0,
]
assert abs(sum(_PAIR_LENGTHS) - _TARGET_LENGTH) < 1e-6, "Fixture sums to target"

_PAIR_OFFSETS = [42.0, 54.0, 30.0, 20.0, 10.0, 50.0, 35.0, 60.0, 25.0, 45.0]
_PAIR_Y_BASES = [i * 200.0 for i in range(10)]  # spread so pairs don't cross


def _build_oracle_fixture() -> tuple[tuple[str, ...], dict[str, dict[str, list[float]]]]:
    """Construct entity_ids and geometry_by_entity_id for the oracle fixture.

    Each pair k:
        wall A: (x_base, y_base) -> (x_base + L_k, y_base)         direction=(1,0)
        wall B: (x_base + L_k, y_base + D_k) -> (x_base, y_base + D_k)  direction=(-1,0)
    """
    entity_ids: list[str] = []
    geom: dict[str, dict[str, list[float]]] = {}
    x_base = 0.0
    for k in range(10):
        seg_len = _PAIR_LENGTHS[k]
        offset = _PAIR_OFFSETS[k]
        y0 = _PAIR_Y_BASES[k]
        a_id = f"wall_a_{k}"
        b_id = f"wall_b_{k}"
        geom[a_id] = _line_geom(x_base, y0, x_base + seg_len, y0)
        geom[b_id] = _line_geom(x_base + seg_len, y0 + offset, x_base, y0 + offset)
        entity_ids.extend([a_id, b_id])
        x_base += seg_len + 100.0  # gap between pairs to avoid cross-pair matching
    return tuple(entity_ids), geom


def test_oracle_regression_derived_length() -> None:
    """Synthetic double-line fixture -> derived length within [0.95, 1.10] * 244,010.

    Fixture construction:
        10 antiparallel wall pairs, lengths summing to 244,010 du (mirroring the
        real M-540003 authored centerline measurement).  Each pair is a horizontal
        segment of length L_k at y=y_base and its antiparallel twin at y=y_base+D_k
        (D_k in {10..60} du, all within [2,80]).

    Expected: all 10 pairs matched -> derived_length == 244,010 du exactly (or
    very close due to floating-point).  Assert [0.95, 1.10] allows for geometry
    measurement differences in a real drawing context.
    """
    entity_ids, geom = _build_oracle_fixture()
    group = _make_group(entity_ids, layer_ref="M-PIPE Pipes")  # derived path

    results = dwg_centerlines([group], geom)
    cl = results[0]

    lo = 0.95 * _TARGET_LENGTH
    hi = 1.10 * _TARGET_LENGTH
    assert lo <= cl.geometry.length_du <= hi, (
        f"Derived length {cl.geometry.length_du:.1f} outside [{lo:.0f}, {hi:.0f}]"
    )


# ---------------------------------------------------------------------------
# Test: unpaired honesty
# ---------------------------------------------------------------------------


def test_unpaired_segment_half_length() -> None:
    """A lone unpaired segment contributes exactly 0.5 x its drawn length.

    Arrange: single horizontal line entity of length 200, no pairable neighbour.
    Act:     dwg_centerlines on a group with just that entity.
    Assert:  length_du == 200 * 0.5 == 100.
    """
    geom = {"lone": _line_geom(0, 0, 200, 0)}
    entity_ids: tuple[str, ...] = ("lone",)
    group = _make_group(entity_ids, layer_ref="M-PIPE Pipes")

    results = dwg_centerlines([group], geom)
    cl = results[0]

    expected = 200.0 * _UNPAIRED_LENGTH_FACTOR
    assert math.isclose(cl.geometry.length_du, expected, rel_tol=1e-9)
    assert cl.producer_kind == "dwg_derived"


# ---------------------------------------------------------------------------
# Test: determinism under input permutation
# ---------------------------------------------------------------------------


def test_determinism_under_shuffle() -> None:
    """Shuffling entity_ids order does not change length_du or polylines.

    Arrange: 4 horizontal wall pairs (8 entities) for the derived path.
    Act:     shuffle entity_ids 10 times and run dwg_centerlines each time.
    Assert:  length_du and polylines are identical across all runs.
    """
    geom: dict[str, dict[str, list[float]]] = {}
    entity_ids_list: list[str] = []
    for k in range(4):
        seg_len = 100.0 + k * 50.0
        offset = 30.0 + k * 5.0
        y0 = k * 200.0
        a_id = f"da_{k}"
        b_id = f"db_{k}"
        geom[a_id] = _line_geom(0, y0, seg_len, y0)
        geom[b_id] = _line_geom(seg_len, y0 + offset, 0, y0 + offset)
        entity_ids_list.extend([a_id, b_id])

    original_ids = tuple(entity_ids_list)
    group0 = _make_group(original_ids, layer_ref="M-PIPE Pipes")
    base = dwg_centerlines([group0], geom)[0]

    rng = random.Random(42)
    for _ in range(10):
        shuffled = list(original_ids)
        rng.shuffle(shuffled)
        group = _make_group(tuple(shuffled), layer_ref="M-PIPE Pipes")
        cl = dwg_centerlines([group], geom)[0]
        assert math.isclose(cl.geometry.length_du, base.geometry.length_du, rel_tol=1e-9), (
            f"length_du changed after shuffle: {cl.geometry.length_du} vs {base.geometry.length_du}"
        )
        assert cl.geometry.polylines == base.geometry.polylines, "polylines changed after shuffle"


# ---------------------------------------------------------------------------
# Test: degenerate inputs
# ---------------------------------------------------------------------------


def test_empty_group_no_raise() -> None:
    """An empty group (no entity_ids) returns length_du=0.0 without raising."""
    group = _make_group((), layer_ref="M-PIPE Pipes")
    results = dwg_centerlines([group], {})
    cl = results[0]
    assert cl.geometry.length_du == 0.0
    assert cl.geometry.polylines == ()


def test_missing_geometry_no_raise() -> None:
    """Entity IDs with no geometry entries return length_du=0.0."""
    group = _make_group(("ghost_1", "ghost_2"), layer_ref="M-PIPE Pipes")
    results = dwg_centerlines([group], {})
    cl = results[0]
    assert cl.geometry.length_du == 0.0


def test_empty_run_groups_returns_empty_list() -> None:
    """dwg_centerlines([]) -> []."""
    results = dwg_centerlines([], {})
    assert results == []


def test_zero_length_line_no_raise() -> None:
    """A degenerate zero-length line is dropped silently; length_du=0.0."""
    geom = {"zero": _line_geom(5, 5, 5, 5)}
    group = _make_group(("zero",), layer_ref="M-PIPE Pipes")
    results = dwg_centerlines([group], geom)
    assert results[0].geometry.length_du == 0.0


# ---------------------------------------------------------------------------
# Test: raster_params_hash is deterministic
# ---------------------------------------------------------------------------


def test_raster_params_hash_stable() -> None:
    """The raster_params_hash is identical across two independent calls."""
    geom = {"e1": _line_geom(0, 0, 100, 0)}
    group = _make_group(("e1",), layer_ref="M-PIPE Pipes")
    r1 = dwg_centerlines([group], geom)[0]
    r2 = dwg_centerlines([group], geom)[0]
    assert r1.raster_params_hash == r2.raster_params_hash
    assert len(r1.raster_params_hash) == 64  # SHA-256 hex digest


def test_algo_version_matches_contract() -> None:
    """algo_version must equal CURRENT_ALGO_VERSION from the contract."""
    from app.ingestion.centerline_contract import CURRENT_ALGO_VERSION

    geom = {"e1": _line_geom(0, 0, 100, 0)}
    group = _make_group(("e1",), layer_ref="M-PIPE Pipes")
    cl = dwg_centerlines([group], geom)[0]
    assert cl.algo_version == CURRENT_ALGO_VERSION
