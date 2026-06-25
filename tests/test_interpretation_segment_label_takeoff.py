"""Synthetic unit tests for app.interpretation.segment_label_takeoff (issue #687).

All geometry is in metres; polylines are Sequence[Sequence[tuple[float,float]]].
No DB, no ORM, no FastAPI.
"""

from __future__ import annotations

import pytest

from app.interpretation.segment_label_takeoff import (
    SegmentLabel,
    compute_segment_label_lengths,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _label(
    x: float,
    y: float,
    service: str,
    size_raw: str | None = "54",
    size_kind: str | None = "round",
) -> SegmentLabel:
    return SegmentLabel(point=(x, y), service=service, size_raw=size_raw, size_kind=size_kind)


def _polyline(*pts: tuple[float, float]) -> tuple[tuple[float, float], ...]:
    return pts


# ---------------------------------------------------------------------------
# (1) All segments near one label → all length attributed to that (service, size)
# ---------------------------------------------------------------------------


def test_all_segments_near_one_label() -> None:
    """All segments within nearest_max_m of a single label → full length attributed to it.

    Arrange: three 1 m segments near a VAC/54 label at (0.5, 0).
    Act:     compute with nearest_max_m=5.0.
    Assert:  per_service=[VAC: 3.0 m]; per_size=[(VAC,54,round): 3.0 m]; unknown=0.
    """
    label = _label(0.5, 0.0, "VAC", "54", "round")
    polylines = [
        _polyline((0.0, 0.0), (1.0, 0.0)),  # mid=(0.5,0) dist=0
        _polyline((0.0, 1.0), (1.0, 1.0)),  # mid=(0.5,1) dist=1
        _polyline((0.0, 2.0), (1.0, 2.0)),  # mid=(0.5,2) dist=2
    ]
    result = compute_segment_label_lengths(
        centerline_polylines=polylines, labels=[label], nearest_max_m=5.0
    )
    assert len(result.per_service) == 1
    assert result.per_service[0].service == "VAC"
    assert result.per_service[0].length_m == pytest.approx(3.0, abs=1e-6)
    assert len(result.per_size) == 1
    assert result.per_size[0].service == "VAC"
    assert result.per_size[0].size_raw == "54"
    assert result.per_size[0].size_kind == "round"
    assert result.per_size[0].length_m == pytest.approx(3.0, abs=1e-6)
    assert result.unknown_length_m == pytest.approx(0.0, abs=1e-6)
    assert result.total_length_m == pytest.approx(3.0, abs=1e-6)
    assert result.segment_count == 3


# ---------------------------------------------------------------------------
# (2) Two labels → segments split by nearest midpoint
# ---------------------------------------------------------------------------


def test_two_labels_segments_split_by_nearest() -> None:
    """Two labels at opposite ends — segments attribute to their nearest label.

    Arrange:
      - VAC/54 label at x=0; OXY/40 label at x=10.
      - Segment A: (0,0)→(1,0), mid=(0.5,0) → dist to VAC=0.5, dist to OXY=9.5 → VAC.
      - Segment B: (9,0)→(11,0), mid=(10,0) → dist to OXY=0, dist to VAC=10 → OXY.
    """
    vac = _label(0.0, 0.0, "VAC", "54", "round")
    oxy = _label(10.0, 0.0, "OXY", "40", "round")
    polylines = [
        _polyline((0.0, 0.0), (1.0, 0.0)),  # → VAC
        _polyline((9.0, 0.0), (11.0, 0.0)),  # → OXY
    ]
    result = compute_segment_label_lengths(
        centerline_polylines=polylines, labels=[vac, oxy], nearest_max_m=5.0
    )
    svc_map = {s.service: s.length_m for s in result.per_service}
    assert svc_map["VAC"] == pytest.approx(1.0, abs=1e-6)
    assert svc_map["OXY"] == pytest.approx(2.0, abs=1e-6)
    assert result.unknown_length_m == pytest.approx(0.0, abs=1e-6)
    assert result.total_length_m == pytest.approx(3.0, abs=1e-6)


# ---------------------------------------------------------------------------
# (3) Segment midpoint > nearest_max_m from all labels → UNKNOWN
# ---------------------------------------------------------------------------


def test_segment_beyond_max_radius_goes_to_unknown() -> None:
    """A segment whose midpoint is farther than nearest_max_m from all labels → unknown.

    Arrange: label at (0,0); segment from (10,0) to (11,0) → mid=(10.5,0), dist=10.5 > 5.0.
    """
    label = _label(0.0, 0.0, "VAC", "54", "round")
    polylines = [_polyline((10.0, 0.0), (11.0, 0.0))]
    result = compute_segment_label_lengths(
        centerline_polylines=polylines, labels=[label], nearest_max_m=5.0
    )
    assert len(result.per_service) == 0
    assert len(result.per_size) == 0
    assert result.unknown_length_m == pytest.approx(1.0, abs=1e-6)
    assert result.total_length_m == pytest.approx(1.0, abs=1e-6)


# ---------------------------------------------------------------------------
# (4) Empty polylines → empty result, no raise
# ---------------------------------------------------------------------------


def test_empty_polylines_returns_empty_result() -> None:
    """Empty centerline_polylines → empty result without raising."""
    label = _label(0.0, 0.0, "VAC")
    result = compute_segment_label_lengths(
        centerline_polylines=[], labels=[label], nearest_max_m=5.0
    )
    assert result.per_service == ()
    assert result.per_size == ()
    assert result.unknown_length_m == 0.0
    assert result.total_length_m == 0.0
    assert result.segment_count == 0


def test_empty_polylines_and_empty_labels_no_raise() -> None:
    """Both inputs empty → no exception; result is all-zero."""
    result = compute_segment_label_lengths(centerline_polylines=[], labels=[], nearest_max_m=5.0)
    assert result.per_service == ()
    assert result.per_size == ()
    assert result.unknown_length_m == 0.0
    assert result.total_length_m == 0.0
    assert result.segment_count == 0


# ---------------------------------------------------------------------------
# (5) Empty labels → all length goes to UNKNOWN
# ---------------------------------------------------------------------------


def test_empty_labels_all_unknown() -> None:
    """No labels provided → every segment goes to unknown_length_m."""
    polylines = [_polyline((0.0, 0.0), (1.0, 0.0)), _polyline((2.0, 0.0), (3.0, 0.0))]
    result = compute_segment_label_lengths(
        centerline_polylines=polylines, labels=[], nearest_max_m=5.0
    )
    assert result.per_service == ()
    assert result.per_size == ()
    assert result.unknown_length_m == pytest.approx(2.0, abs=1e-6)
    assert result.total_length_m == pytest.approx(2.0, abs=1e-6)


# ---------------------------------------------------------------------------
# (6) Sum invariants: Σper_service + unknown == total; per_size sums to per_service
# ---------------------------------------------------------------------------


def test_sum_invariant_per_service_plus_unknown_equals_total() -> None:
    """Σ(per_service lengths) + unknown_length_m == total_length_m within ±0.1 m."""
    vac = _label(0.5, 0.0, "VAC", "54", "round")
    oxy = _label(5.5, 0.0, "OXY", "40", "round")
    polylines = [
        _polyline((0.0, 0.0), (1.0, 0.0)),  # → VAC (dist=0)
        _polyline((5.0, 0.0), (6.0, 0.0)),  # → OXY (dist=0)
        _polyline((20.0, 0.0), (21.0, 0.0)),  # → unknown (dist > 5.0)
    ]
    result = compute_segment_label_lengths(
        centerline_polylines=polylines, labels=[vac, oxy], nearest_max_m=5.0
    )
    per_service_sum = sum(s.length_m for s in result.per_service)
    total_check = per_service_sum + result.unknown_length_m
    assert total_check == pytest.approx(result.total_length_m, abs=0.1)
    assert result.total_length_m == pytest.approx(3.0, abs=1e-6)


def test_sum_invariant_per_size_sums_to_per_service() -> None:
    """Σ(per_size lengths for a service) == per_service length for that service."""
    vac_54 = _label(0.5, 0.0, "VAC", "54", "round")
    vac_40 = _label(2.5, 0.0, "VAC", "40", "round")
    oxy = _label(6.0, 0.0, "OXY", "32", "round")
    polylines = [
        _polyline((0.0, 0.0), (1.0, 0.0)),  # mid=0.5 → VAC/54
        _polyline((2.0, 0.0), (3.0, 0.0)),  # mid=2.5 → VAC/40
        _polyline((5.5, 0.0), (6.5, 0.0)),  # mid=6.0 → OXY/32
    ]
    result = compute_segment_label_lengths(
        centerline_polylines=polylines, labels=[vac_54, vac_40, oxy], nearest_max_m=5.0
    )
    # Σ per_size for VAC == per_service VAC
    vac_per_service = next(s.length_m for s in result.per_service if s.service == "VAC")
    vac_per_size_sum = sum(s.length_m for s in result.per_size if s.service == "VAC")
    assert vac_per_size_sum == pytest.approx(vac_per_service, abs=1e-6)

    # Global invariant
    per_service_sum = sum(s.length_m for s in result.per_service)
    total_check = per_service_sum + result.unknown_length_m
    assert total_check == pytest.approx(result.total_length_m, abs=0.1)


# ---------------------------------------------------------------------------
# (7) Deterministic tie-break
# ---------------------------------------------------------------------------


def test_deterministic_tie_break_on_equal_distance() -> None:
    """Two labels equidistant from a segment midpoint → lexicographically smallest wins.

    Arrange: segment from (-1,0) to (1,0), mid=(0,0).
      Label A: OXY at (0,1) → dist=1.
      Label B: VAC at (0,-1) → dist=1.
    Tie: OXY < VAC lexicographically → OXY wins.
    """
    oxy = _label(0.0, 1.0, "OXY", "40", "round")
    vac = _label(0.0, -1.0, "VAC", "54", "round")
    polylines = [_polyline((-1.0, 0.0), (1.0, 0.0))]
    result = compute_segment_label_lengths(
        centerline_polylines=polylines, labels=[oxy, vac], nearest_max_m=5.0
    )
    assert len(result.per_service) == 1
    assert result.per_service[0].service == "OXY"
    assert result.unknown_length_m == pytest.approx(0.0, abs=1e-6)


def test_deterministic_tie_break_consistent_regardless_of_input_order() -> None:
    """Result is identical regardless of label input order (deterministic)."""
    oxy = _label(0.0, 1.0, "OXY", "40", "round")
    vac = _label(0.0, -1.0, "VAC", "54", "round")
    polylines = [_polyline((-1.0, 0.0), (1.0, 0.0))]

    result_a = compute_segment_label_lengths(
        centerline_polylines=polylines, labels=[oxy, vac], nearest_max_m=5.0
    )
    result_b = compute_segment_label_lengths(
        centerline_polylines=polylines, labels=[vac, oxy], nearest_max_m=5.0
    )
    assert result_a.per_service == result_b.per_service
    assert result_a.per_size == result_b.per_size
    assert result_a.unknown_length_m == result_b.unknown_length_m
    assert result_a.total_length_m == result_b.total_length_m


# ---------------------------------------------------------------------------
# (8) Multi-point polyline decomposes into consecutive segments
# ---------------------------------------------------------------------------


def test_multipoint_polyline_decomposes_correctly() -> None:
    """A polyline with 3 points produces 2 segments, each attributed independently."""
    # L-shaped: (0,0)→(1,0)→(1,1). Two 1 m segments.
    # Label near (0.5,0): captures first segment; label near (1,0.5) captures second.
    label_a = _label(0.5, 0.0, "VAC", "54", "round")
    label_b = _label(1.0, 0.5, "OXY", "40", "round")
    polylines = [_polyline((0.0, 0.0), (1.0, 0.0), (1.0, 1.0))]
    result = compute_segment_label_lengths(
        centerline_polylines=polylines, labels=[label_a, label_b], nearest_max_m=5.0
    )
    assert result.segment_count == 2
    assert result.total_length_m == pytest.approx(2.0, abs=1e-6)
    svc_map = {s.service: s.length_m for s in result.per_service}
    assert svc_map.get("VAC", 0.0) == pytest.approx(1.0, abs=1e-6)
    assert svc_map.get("OXY", 0.0) == pytest.approx(1.0, abs=1e-6)


# ---------------------------------------------------------------------------
# (9) per_service sorted by service; per_size sorted by (service, size_raw)
# ---------------------------------------------------------------------------


def test_output_sorted_correctly() -> None:
    """per_service sorted by service; per_size sorted by (service, size_raw)."""
    labels = [
        _label(0.5, 0.0, "VAC", "54", "round"),
        _label(2.5, 0.0, "OXY", "40", "round"),
        _label(4.5, 0.0, "AIR", "32", "round"),
    ]
    polylines = [
        _polyline((0.0, 0.0), (1.0, 0.0)),
        _polyline((2.0, 0.0), (3.0, 0.0)),
        _polyline((4.0, 0.0), (5.0, 0.0)),
    ]
    result = compute_segment_label_lengths(
        centerline_polylines=polylines, labels=labels, nearest_max_m=5.0
    )
    services = [s.service for s in result.per_service]
    assert services == sorted(services)
    size_keys = [(s.service, s.size_raw or "") for s in result.per_size]
    assert size_keys == sorted(size_keys)


# ---------------------------------------------------------------------------
# (10) size_raw=None labels do not cause errors
# ---------------------------------------------------------------------------


def test_none_size_raw_handled_gracefully() -> None:
    """Labels with size_raw=None are accepted and attributed correctly."""
    label = SegmentLabel(point=(0.5, 0.0), service="EA", size_raw=None, size_kind=None)
    polylines = [_polyline((0.0, 0.0), (1.0, 0.0))]
    result = compute_segment_label_lengths(
        centerline_polylines=polylines, labels=[label], nearest_max_m=5.0
    )
    assert len(result.per_service) == 1
    assert result.per_service[0].service == "EA"
    assert len(result.per_size) == 1
    assert result.per_size[0].size_raw is None
    assert result.total_length_m == pytest.approx(1.0, abs=1e-6)
    assert result.unknown_length_m == pytest.approx(0.0, abs=1e-6)
