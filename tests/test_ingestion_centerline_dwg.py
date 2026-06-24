"""Unit tests for app.ingestion.centerline_dwg.

All tests are pure-algorithm: no DB, no DWG files read.

Oracle regression grounding
---------------------------
Real M-540003 drawing: authored Center Line layer = 244,010 du.
The authored path sums entity drawn lengths directly.

Derived path (#681): width-agnostic rail-pair synthesis at metre scale.
Synthetic fixtures use metre-scale geometry (geometry is pre-scaled to metres
before the producer receives it).

Key metre-scale pairing bounds:
  _PERP_OFFSET_MIN_M = 0.01 m  (1 cm)
  _PERP_OFFSET_MAX_M = 2.0 m   (2 m)
  _RUNG_MAX_LEN_M    = 0.7 m   (above widest tray rung 650 mm; plateau [0.7, 0.9] m on E-610003)
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


def _line_geom_list(sx: float, sy: float, ex: float, ey: float) -> dict[str, list[float]]:
    """Line geometry with list coords (legacy/fixture shape)."""
    return {"start": [sx, sy], "end": [ex, ey]}


def _line_geom_dict(sx: float, sy: float, ex: float, ey: float) -> dict[str, dict[str, float]]:
    """Line geometry with dict coords (canonical materialized shape)."""
    return {"start": {"x": sx, "y": sy, "z": 0.0}, "end": {"x": ex, "y": ey, "z": 0.0}}


def _line_geom(sx: float, sy: float, ex: float, ey: float) -> dict[str, list[float]]:
    """Default alias — list coords (matches existing helper usage)."""
    return _line_geom_list(sx, sy, ex, ey)


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
# Test: derive clean pair geometry (metre-scale fixture)
# ---------------------------------------------------------------------------


def test_derive_clean_pair_geometry() -> None:
    """Two antiparallel equal-length rails offset by 0.42 m -> one midline.

    Metre-scale fixture: geometry is pre-scaled to metres before the producer
    receives it.

    Arrange (list-coord shape):
        Rail i: (0, 0) -> (5.0, 0)    length=5.0 m, direction=(1,0)
        Rail j: (5.0, 0.42) -> (0, 0.42)  length=5.0 m, antiparallel

    Expected:
        ONE midline polyline at y=0.21 m, length=5.0 m.
        producer_kind="dwg_derived".
        length_du == 5.0.

    After aligning j: s_j=(0, 0.42), e_j=(5.0, 0.42)
        mid_s = avg((0,0),(0,0.42)) = (0, 0.21)
        mid_e = avg((5,0),(5,0.42)) = (5, 0.21)
        length = 5.0 m.
    """
    geom = {
        "rail_a": _line_geom_list(0.0, 0.0, 5.0, 0.0),
        "rail_b": _line_geom_list(5.0, 0.42, 0.0, 0.42),  # antiparallel
    }
    entity_ids: tuple[str, ...] = ("rail_a", "rail_b")
    group = _make_group(entity_ids, layer_ref="E-TRAY Cable Tray")  # no token -> derived

    results = dwg_centerlines([group], geom)
    cl = results[0]

    assert cl.producer_kind == "dwg_derived"
    assert len(cl.geometry.polylines) == 1
    midline = cl.geometry.polylines[0]
    assert len(midline) == 2
    mid_s, mid_e = midline
    assert math.isclose(mid_s[1], 0.21, abs_tol=1e-9)
    assert math.isclose(mid_e[1], 0.21, abs_tol=1e-9)
    assert math.isclose(cl.geometry.length_du, 5.0, rel_tol=1e-9)


def test_derive_clean_pair_geometry_dict_coords() -> None:
    """Same as above but with dict-coord geometry (canonical materialized shape).

    Validates the #660 lesson: dict-coord fixtures must be tested alongside
    list-coord fixtures.
    """
    geom = {
        "rail_a": _line_geom_dict(0.0, 0.0, 5.0, 0.0),
        "rail_b": _line_geom_dict(5.0, 0.42, 0.0, 0.42),
    }
    entity_ids: tuple[str, ...] = ("rail_a", "rail_b")
    group = _make_group(entity_ids, layer_ref="E-TRAY Cable Tray")

    results = dwg_centerlines([group], geom)
    cl = results[0]

    assert cl.producer_kind == "dwg_derived"
    assert len(cl.geometry.polylines) == 1
    mid_s, mid_e = cl.geometry.polylines[0]
    assert math.isclose(mid_s[1], 0.21, abs_tol=1e-9)
    assert math.isclose(mid_e[1], 0.21, abs_tol=1e-9)
    assert math.isclose(cl.geometry.length_du, 5.0, rel_tol=1e-9)


# ---------------------------------------------------------------------------
# Test: rung exclusion — rails + perpendicular crossbars
# ---------------------------------------------------------------------------


def test_rungs_excluded_from_length_and_polylines() -> None:
    """Two parallel rails + perpendicular rungs -> one midline; rungs excluded.

    Metre-scale fixture (list coords) exercising real-world tray geometry:
        Rail A: (0, 0) -> (3.0, 0)              horizontal, 3.0 m  (> _RUNG_MAX_LEN_M=0.7 m)
        Rail B: (3.0, 0.65) -> (0, 0.65)        horizontal, 3.0 m, antiparallel, offset=0.65 m
        Rung 1: (0, 0) -> (0, 0.65)             vertical, 0.65 m  (< _RUNG_MAX_LEN_M -> filtered)
        Rung 2: (1.5, 0) -> (1.5, 0.65)         vertical, 0.65 m  (< _RUNG_MAX_LEN_M -> filtered)
        Rung 3: (3.0, 0) -> (3.0, 0.65)         vertical, 0.65 m  (< _RUNG_MAX_LEN_M -> filtered)

    Rung lengths (0.65 m) represent the widest containment width on the evidence
    sheet (E-610003).  All rungs are below _RUNG_MAX_LEN_M=0.7 m so they are
    excluded from rail_indices entirely — they contribute neither length nor polylines.

    Expected:
        - Exactly ONE midline polyline (from the rail pair).
        - length_du == 3.0 m (midline of two equal 3.0 m rails).
        - Rungs contribute 0 length and 0 polylines.
    """
    geom = {
        "rail_a": _line_geom_list(0.0, 0.0, 3.0, 0.0),
        "rail_b": _line_geom_list(3.0, 0.65, 0.0, 0.65),
        "rung_1": _line_geom_list(0.0, 0.0, 0.0, 0.65),
        "rung_2": _line_geom_list(1.5, 0.0, 1.5, 0.65),
        "rung_3": _line_geom_list(3.0, 0.0, 3.0, 0.65),
    }
    entity_ids: tuple[str, ...] = ("rail_a", "rail_b", "rung_1", "rung_2", "rung_3")
    group = _make_group(entity_ids, layer_ref="E-TRAY Cable Tray")

    results = dwg_centerlines([group], geom)
    cl = results[0]

    assert cl.producer_kind == "dwg_derived"
    assert len(cl.geometry.polylines) == 1, (
        f"Expected 1 midline polyline, got {len(cl.geometry.polylines)}"
    )
    assert math.isclose(cl.geometry.length_du, 3.0, rel_tol=1e-9), (
        f"Expected length_du=3.0, got {cl.geometry.length_du}"
    )


def test_rungs_excluded_dict_coords() -> None:
    """Same rung-exclusion test with dict-coord geometry (canonical materialized shape)."""
    geom = {
        "rail_a": _line_geom_dict(0.0, 0.0, 3.0, 0.0),
        "rail_b": _line_geom_dict(3.0, 0.65, 0.0, 0.65),
        "rung_1": _line_geom_dict(0.0, 0.0, 0.0, 0.65),
        "rung_2": _line_geom_dict(1.5, 0.0, 1.5, 0.65),
        "rung_3": _line_geom_dict(3.0, 0.0, 3.0, 0.65),
    }
    entity_ids: tuple[str, ...] = ("rail_a", "rail_b", "rung_1", "rung_2", "rung_3")
    group = _make_group(entity_ids, layer_ref="E-TRAY Cable Tray")

    results = dwg_centerlines([group], geom)
    cl = results[0]

    assert len(cl.geometry.polylines) == 1
    assert math.isclose(cl.geometry.length_du, 3.0, rel_tol=1e-9)


# ---------------------------------------------------------------------------
# Test: conservation invariant (no rail silently zeroed)
# ---------------------------------------------------------------------------


def test_conservation_coincident_midpoints() -> None:
    """Every rail contributes either a midline or the 0.5x unpaired residual.

    Regression for the de-dup bug: if a de-dup guard marks rails as matched but
    skips emitting their midline, the rails contribute zero to total_length (not
    even the 0.5x residual).  This test asserts the conservation invariant holds
    even when two distinct rail-pairs produce midlines that coincide within 1 mm.

    Fixture:
        Pair 1 rails at y=0 / y=0.4 m,  x=[0, 3.0]  -> midline at y=0.20, length=3.0 m
        Pair 2 rails at y=10 / y=10.4 m, x=[0, 3.0]  -> midline at y=10.20, length=3.0 m
        Pair 3 rails at y=0 / y=0.4 m,  x=[20, 23.0] -> midline at y=0.20, length=3.0 m
                (same y-band as pair 1 but no x-overlap -> distinct physical run)

    All three pairs are valid and distinct; pair 1 and pair 3 produce midlines
    with the SAME y-coordinate (0.20 m) and the same length (3.0 m) but at
    different x positions — a de-dup by endpoint would drop pair 3.

    Conservation assertion:
        total_length == 3 * 3.0 == 9.0 m  (all three pairs fully emitted)
        polylines == 3
    """
    geom = {
        # Pair 1
        "p1_a": _line_geom_list(0.0, 0.0, 3.0, 0.0),
        "p1_b": _line_geom_list(3.0, 0.4, 0.0, 0.4),
        # Pair 2
        "p2_a": _line_geom_list(0.0, 10.0, 3.0, 10.0),
        "p2_b": _line_geom_list(3.0, 10.4, 0.0, 10.4),
        # Pair 3 — same y-band as pair 1, different x range (no overlap with pair 1)
        "p3_a": _line_geom_list(20.0, 0.0, 23.0, 0.0),
        "p3_b": _line_geom_list(23.0, 0.4, 20.0, 0.4),
    }
    entity_ids: tuple[str, ...] = ("p1_a", "p1_b", "p2_a", "p2_b", "p3_a", "p3_b")
    group = _make_group(entity_ids, layer_ref="E-TRAY Cable Tray")

    results = dwg_centerlines([group], geom)
    cl = results[0]

    # All 3 pairs must be emitted; no rail silently zeroed.
    assert len(cl.geometry.polylines) == 3, (
        f"Expected 3 midline polylines (conservation), got {len(cl.geometry.polylines)}"
    )
    assert math.isclose(cl.geometry.length_du, 9.0, rel_tol=1e-9), (
        f"Expected total 9.0 m (3 x 3.0 m), got {cl.geometry.length_du}"
    )


def test_conservation_unpaired_not_silenced() -> None:
    """Unpaired rails always reach the 0.5x residual — never silently zero.

    Three rails: pairs (A,B) match; C is unpaired.
    total = midline(A,B) + 0.5 * len(C).
    """
    geom = {
        "ra": _line_geom_list(0.0, 0.0, 4.0, 0.0),
        "rb": _line_geom_list(4.0, 0.3, 0.0, 0.3),
        "rc": _line_geom_list(10.0, 5.0, 14.0, 5.0),  # lone rail, no partner
    }
    entity_ids: tuple[str, ...] = ("ra", "rb", "rc")
    group = _make_group(entity_ids, layer_ref="E-TRAY Cable Tray")

    results = dwg_centerlines([group], geom)
    cl = results[0]

    # 1 midline (ra+rb) + 1 unpaired polyline (rc)
    assert len(cl.geometry.polylines) == 2, (
        f"Expected 2 polylines (1 midline + 1 unpaired), got {len(cl.geometry.polylines)}"
    )
    expected = 4.0 + 4.0 * _UNPAIRED_LENGTH_FACTOR  # midline=4.0 + 0.5*4.0=2.0
    assert math.isclose(cl.geometry.length_du, expected, rel_tol=1e-9), (
        f"Expected {expected} m, got {cl.geometry.length_du}"
    )


# ---------------------------------------------------------------------------
# Test: width-agnostic pairing
# ---------------------------------------------------------------------------


def test_width_agnostic_narrow_offset() -> None:
    """Rails separated by 0.021 m (narrow cable tray) pair correctly.

    Verifies the lower bound _PERP_OFFSET_MIN_M=0.01 m is not too aggressive.
    """
    offset = 0.021  # narrow: just above 0.01 m minimum
    geom = {
        "rail_a": _line_geom_list(0.0, 0.0, 4.0, 0.0),
        "rail_b": _line_geom_list(4.0, offset, 0.0, offset),
    }
    entity_ids: tuple[str, ...] = ("rail_a", "rail_b")
    group = _make_group(entity_ids, layer_ref="E-TRAY Cable Tray")

    results = dwg_centerlines([group], geom)
    cl = results[0]

    assert len(cl.geometry.polylines) == 1, "Narrow offset should pair"
    assert math.isclose(cl.geometry.length_du, 4.0, rel_tol=1e-9)


def test_width_agnostic_wide_offset() -> None:
    """Rails separated by 0.65 m (wide duct) also pair correctly.

    Verifies the algorithm is truly width-agnostic between 0.01 m and 2.0 m.
    """
    offset = 0.65
    geom = {
        "rail_a": _line_geom_list(0.0, 0.0, 4.0, 0.0),
        "rail_b": _line_geom_list(4.0, offset, 0.0, offset),
    }
    entity_ids: tuple[str, ...] = ("rail_a", "rail_b")
    group = _make_group(entity_ids, layer_ref="E-DUCT Ductwork")

    results = dwg_centerlines([group], geom)
    cl = results[0]

    assert len(cl.geometry.polylines) == 1, "Wide offset should pair"
    assert math.isclose(cl.geometry.length_du, 4.0, rel_tol=1e-9)


def test_non_overlapping_parallels_not_paired() -> None:
    """Two parallel rails with no projection overlap are NOT paired.

    Rail A: (0, 0) -> (2.0, 0)
    Rail B: (5.0, 0.3) -> (7.0, 0.3)   — no x-range overlap with A

    Expected: both unpaired -> total = (2.0 + 2.0) * 0.5 = 2.0 m, 2 polylines.
    """
    geom = {
        "rail_a": _line_geom_list(0.0, 0.0, 2.0, 0.0),
        "rail_b": _line_geom_list(5.0, 0.3, 7.0, 0.3),
    }
    entity_ids: tuple[str, ...] = ("rail_a", "rail_b")
    group = _make_group(entity_ids, layer_ref="E-TRAY Cable Tray")

    results = dwg_centerlines([group], geom)
    cl = results[0]

    assert len(cl.geometry.polylines) == 2, "Non-overlapping parallels should NOT pair"
    expected = (2.0 + 2.0) * _UNPAIRED_LENGTH_FACTOR
    assert math.isclose(cl.geometry.length_du, expected, rel_tol=1e-9)


def test_near_collinear_not_paired() -> None:
    """Two co-linear rails (offset < _PERP_OFFSET_MIN_M) are NOT paired.

    Rail A: (0, 0) -> (3.0, 0)
    Rail B: (0, 0.005) -> (3.0, 0.005)  — offset=0.005 m < 0.01 m minimum

    Expected: both unpaired -> total = (3.0 + 3.0) * 0.5 = 3.0 m.
    """
    geom = {
        "rail_a": _line_geom_list(0.0, 0.0, 3.0, 0.0),
        "rail_b": _line_geom_list(0.0, 0.005, 3.0, 0.005),
    }
    entity_ids: tuple[str, ...] = ("rail_a", "rail_b")
    group = _make_group(entity_ids, layer_ref="E-TRAY Cable Tray")

    results = dwg_centerlines([group], geom)
    cl = results[0]

    assert len(cl.geometry.polylines) == 2, "Near-collinear rails should NOT pair"
    expected = (3.0 + 3.0) * _UNPAIRED_LENGTH_FACTOR
    assert math.isclose(cl.geometry.length_du, expected, rel_tol=1e-9)


# ---------------------------------------------------------------------------
# Test: oracle regression (metre-scale synthetic fixture)
# ---------------------------------------------------------------------------

# 10 rail pairs at metre scale; lengths and offsets all in metres.
# Each pair: rail A horizontal at y=y_base, rail B antiparallel at y=y_base+D.
# All offsets in (_PERP_OFFSET_MIN_M=0.01, _PERP_OFFSET_MAX_M=2.0).
_TARGET_LENGTH_M = 299.5  # approximate oracle target from E-610003 real-data gate
_PAIR_LENGTHS_M = [
    40.0,
    35.0,
    32.0,
    30.0,
    28.5,
    28.0,
    27.0,
    26.0,
    24.5,
    28.5,
]
assert abs(sum(_PAIR_LENGTHS_M) - 299.5) < 1e-6, "Fixture sums to 299.5 m"

_PAIR_OFFSETS_M = [0.42, 0.54, 0.30, 0.20, 0.15, 0.50, 0.35, 0.60, 0.25, 0.45]
_PAIR_Y_BASES_M = [i * 5.0 for i in range(10)]  # 5 m gap between pairs


def _build_oracle_fixture_m() -> tuple[tuple[str, ...], dict[str, dict[str, list[float]]]]:
    """Construct entity_ids and geometry for the metre-scale oracle fixture.

    Each pair k:
        Rail A: (x_base, y_base) -> (x_base + L_k, y_base)         direction=(1,0)
        Rail B: (x_base + L_k, y_base + D_k) -> (x_base, y_base + D_k)  antiparallel
    """
    entity_ids: list[str] = []
    geom: dict[str, dict[str, list[float]]] = {}
    x_base = 0.0
    for k in range(10):
        seg_len = _PAIR_LENGTHS_M[k]
        offset = _PAIR_OFFSETS_M[k]
        y0 = _PAIR_Y_BASES_M[k]
        a_id = f"rail_a_{k}"
        b_id = f"rail_b_{k}"
        geom[a_id] = _line_geom(x_base, y0, x_base + seg_len, y0)
        geom[b_id] = _line_geom(x_base + seg_len, y0 + offset, x_base, y0 + offset)
        entity_ids.extend([a_id, b_id])
        x_base += seg_len + 2.0  # 2 m gap between pairs to avoid cross-pair matching
    return tuple(entity_ids), geom


def test_oracle_regression_derived_length() -> None:
    """Synthetic double-line metre-scale fixture -> derived length ~299.5 m.

    10 antiparallel rail pairs summing to 299.5 m; all offsets within
    [0.15, 0.60] m, well within the width-agnostic bounds.
    Expects all 10 pairs matched -> derived_length == 299.5 m.
    """
    entity_ids, geom = _build_oracle_fixture_m()
    group = _make_group(entity_ids, layer_ref="E-TRAY Cable Tray")

    results = dwg_centerlines([group], geom)
    cl = results[0]

    lo = 0.95 * _TARGET_LENGTH_M
    hi = 1.10 * _TARGET_LENGTH_M
    assert lo <= cl.geometry.length_du <= hi, (
        f"Derived length {cl.geometry.length_du:.3f} m outside [{lo:.1f}, {hi:.1f}]"
    )


# ---------------------------------------------------------------------------
# Test: unpaired honesty
# ---------------------------------------------------------------------------


def test_unpaired_segment_half_length() -> None:
    """A lone unpaired rail contributes exactly 0.5 x its drawn length.

    Arrange: single horizontal line entity of length 5.0 m, no pairable neighbour.
    Act:     dwg_centerlines on a group with just that entity.
    Assert:  length_du == 5.0 * 0.5 == 2.5 m.
    """
    geom = {"lone": _line_geom(0.0, 0.0, 5.0, 0.0)}
    entity_ids: tuple[str, ...] = ("lone",)
    group = _make_group(entity_ids, layer_ref="E-TRAY Cable Tray")

    results = dwg_centerlines([group], geom)
    cl = results[0]

    expected = 5.0 * _UNPAIRED_LENGTH_FACTOR
    assert math.isclose(cl.geometry.length_du, expected, rel_tol=1e-9)
    assert cl.producer_kind == "dwg_derived"


# ---------------------------------------------------------------------------
# Test: determinism under input permutation (metre-scale)
# ---------------------------------------------------------------------------


def test_determinism_under_shuffle() -> None:
    """Shuffling entity_ids order does not change length_du or polylines.

    Metre-scale fixture: 4 horizontal rail pairs (8 entities) for the derived path.
    """
    geom: dict[str, dict[str, list[float]]] = {}
    entity_ids_list: list[str] = []
    for k in range(4):
        seg_len = 3.0 + k * 1.5  # 3.0, 4.5, 6.0, 7.5 m
        offset = 0.30 + k * 0.08  # 0.30, 0.38, 0.46, 0.54 m
        y0 = k * 3.0
        a_id = f"da_{k}"
        b_id = f"db_{k}"
        geom[a_id] = _line_geom(0.0, y0, seg_len, y0)
        geom[b_id] = _line_geom(seg_len, y0 + offset, 0.0, y0 + offset)
        entity_ids_list.extend([a_id, b_id])

    original_ids = tuple(entity_ids_list)
    group0 = _make_group(original_ids, layer_ref="E-TRAY Cable Tray")
    base = dwg_centerlines([group0], geom)[0]

    rng = random.Random(42)
    for _ in range(10):
        shuffled = list(original_ids)
        rng.shuffle(shuffled)
        group = _make_group(tuple(shuffled), layer_ref="E-TRAY Cable Tray")
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
    group = _make_group((), layer_ref="E-TRAY Cable Tray")
    results = dwg_centerlines([group], {})
    cl = results[0]
    assert cl.geometry.length_du == 0.0
    assert cl.geometry.polylines == ()


def test_missing_geometry_no_raise() -> None:
    """Entity IDs with no geometry entries return length_du=0.0."""
    group = _make_group(("ghost_1", "ghost_2"), layer_ref="E-TRAY Cable Tray")
    results = dwg_centerlines([group], {})
    cl = results[0]
    assert cl.geometry.length_du == 0.0


def test_empty_run_groups_returns_empty_list() -> None:
    """dwg_centerlines([]) -> []."""
    results = dwg_centerlines([], {})
    assert results == []


def test_zero_length_line_no_raise() -> None:
    """A degenerate zero-length line is dropped silently; length_du=0.0."""
    geom = {"zero": _line_geom(5.0, 5.0, 5.0, 5.0)}
    group = _make_group(("zero",), layer_ref="E-TRAY Cable Tray")
    results = dwg_centerlines([group], geom)
    assert results[0].geometry.length_du == 0.0


# ---------------------------------------------------------------------------
# Test: raster_params_hash is deterministic
# ---------------------------------------------------------------------------


def test_raster_params_hash_stable() -> None:
    """The raster_params_hash is identical across two independent calls."""
    geom = {"e1": _line_geom(0.0, 0.0, 1.0, 0.0)}
    group = _make_group(("e1",), layer_ref="E-TRAY Cable Tray")
    r1 = dwg_centerlines([group], geom)[0]
    r2 = dwg_centerlines([group], geom)[0]
    assert r1.raster_params_hash == r2.raster_params_hash
    assert len(r1.raster_params_hash) == 64  # SHA-256 hex digest


def test_algo_version_matches_contract() -> None:
    """algo_version must equal CURRENT_ALGO_VERSION from the contract."""
    from app.ingestion.centerline_contract import CURRENT_ALGO_VERSION

    geom = {"e1": _line_geom(0.0, 0.0, 1.0, 0.0)}
    group = _make_group(("e1",), layer_ref="E-TRAY Cable Tray")
    cl = dwg_centerlines([group], geom)[0]
    assert cl.algo_version == CURRENT_ALGO_VERSION
