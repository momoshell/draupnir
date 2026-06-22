"""Pure unit tests for the measurement primitive (issue #608, P0).

All tests are self-contained (no DB, no filesystem, no adapters).

Test categories:
- path_length: basic segments, polylines, degenerate inputs, non-finite coords
- measure_length gating matrix (confirmed/inferred/contradicted/unavailable)
- drawing_length coercion (negative, NaN, inf)
- units_confidence and conversion_factor echoing
- Determinism
"""

from __future__ import annotations

import math

import pytest

from app.interpretation.measurement import (
    ScaleContext,
    measure_length,
    path_length,
)

# ---------------------------------------------------------------------------
# path_length
# ---------------------------------------------------------------------------


class TestPathLength:
    def test_single_horizontal_segment(self) -> None:
        pts = [(0.0, 0.0), (3.0, 0.0)]
        assert path_length(pts) == pytest.approx(3.0)

    def test_single_diagonal_segment(self) -> None:
        pts = [(0.0, 0.0), (3.0, 4.0)]
        assert path_length(pts) == pytest.approx(5.0)

    def test_three_segment_polyline(self) -> None:
        # L-shaped path: right 3, up 4, right 3 -> 3 + 4 + 3 = 10
        pts = [(0.0, 0.0), (3.0, 0.0), (3.0, 4.0), (6.0, 4.0)]
        assert path_length(pts) == pytest.approx(10.0)

    def test_zero_points(self) -> None:
        assert path_length([]) == 0.0

    def test_one_point(self) -> None:
        assert path_length([(1.0, 2.0)]) == 0.0

    def test_nan_x_coord_segment_contributes_zero(self) -> None:
        pts = [(0.0, 0.0), (math.nan, 0.0), (3.0, 0.0)]
        # segment 0->1 has NaN -> 0; segment 1->2 has NaN -> 0; total = 0
        result = path_length(pts)
        assert result == pytest.approx(0.0)

    def test_nan_y_coord_segment_contributes_zero(self) -> None:
        pts = [(0.0, math.nan), (3.0, 4.0)]
        result = path_length(pts)
        assert result == pytest.approx(0.0)

    def test_inf_coord_segment_contributes_zero(self) -> None:
        pts = [(0.0, 0.0), (math.inf, 0.0)]
        result = path_length(pts)
        assert result == pytest.approx(0.0)

    def test_mixed_valid_and_nan_segments(self) -> None:
        # segment (0,0)->(3,4) is valid (length 5); segment (3,4)->(nan,0) is bad (0)
        pts = [(0.0, 0.0), (3.0, 4.0), (math.nan, 0.0)]
        result = path_length(pts)
        assert result == pytest.approx(5.0)

    def test_never_raises_on_nan_input(self) -> None:
        pts = [(math.nan, math.nan), (math.inf, -math.inf)]
        result = path_length(pts)
        assert math.isfinite(result)

    def test_zero_length_segment(self) -> None:
        pts = [(1.0, 1.0), (1.0, 1.0)]
        assert path_length(pts) == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# measure_length -- gating matrix
# ---------------------------------------------------------------------------


class TestMeasureLengthGating:
    def test_confirmed_available_real_world(self) -> None:
        scale = ScaleContext(
            conversion_factor=0.001,
            real_world_available=True,
            contradicted=False,
            units_confidence="confirmed",
        )
        m = measure_length(1000.0, scale)
        assert m.basis == "real_world"
        assert m.real_length_m == pytest.approx(1.0)
        assert m.drawing_length == pytest.approx(1000.0)
        assert m.units_confidence == "confirmed"
        assert m.conversion_factor == pytest.approx(0.001)

    def test_inferred_available_real_world(self) -> None:
        scale = ScaleContext(
            conversion_factor=0.001,
            real_world_available=True,
            contradicted=False,
            units_confidence="inferred",
        )
        m = measure_length(2000.0, scale)
        assert m.basis == "real_world"
        assert m.real_length_m == pytest.approx(2.0)
        assert m.units_confidence == "inferred"

    def test_contradicted_true_yields_drawing_units_only(self) -> None:
        scale = ScaleContext(
            conversion_factor=0.001,
            real_world_available=True,
            contradicted=True,
            units_confidence="inferred",
        )
        m = measure_length(1000.0, scale)
        assert m.basis == "drawing_units_only"
        assert m.real_length_m is None
        assert m.drawing_length == pytest.approx(1000.0)

    def test_real_world_available_false_yields_drawing_units_only(self) -> None:
        scale = ScaleContext(
            conversion_factor=0.001,
            real_world_available=False,
            contradicted=False,
            units_confidence="inferred",
        )
        m = measure_length(1000.0, scale)
        assert m.basis == "drawing_units_only"
        assert m.real_length_m is None

    def test_conversion_factor_none_yields_drawing_units_only(self) -> None:
        scale = ScaleContext(
            conversion_factor=None,
            real_world_available=True,
            contradicted=False,
            units_confidence="unknown",
        )
        m = measure_length(500.0, scale)
        assert m.basis == "drawing_units_only"
        assert m.real_length_m is None
        assert m.conversion_factor is None

    def test_conversion_factor_zero_yields_drawing_units_only(self) -> None:
        scale = ScaleContext(
            conversion_factor=0.0,
            real_world_available=True,
            contradicted=False,
            units_confidence="inferred",
        )
        m = measure_length(500.0, scale)
        assert m.basis == "drawing_units_only"
        assert m.real_length_m is None

    def test_conversion_factor_negative_yields_drawing_units_only(self) -> None:
        scale = ScaleContext(
            conversion_factor=-0.001,
            real_world_available=True,
            contradicted=False,
            units_confidence="inferred",
        )
        m = measure_length(500.0, scale)
        assert m.basis == "drawing_units_only"
        assert m.real_length_m is None

    def test_conversion_factor_nan_yields_drawing_units_only(self) -> None:
        scale = ScaleContext(
            conversion_factor=math.nan,
            real_world_available=True,
            contradicted=False,
            units_confidence="inferred",
        )
        m = measure_length(500.0, scale)
        assert m.basis == "drawing_units_only"
        assert m.real_length_m is None

    def test_conversion_factor_inf_yields_drawing_units_only(self) -> None:
        scale = ScaleContext(
            conversion_factor=math.inf,
            real_world_available=True,
            contradicted=False,
            units_confidence="inferred",
        )
        m = measure_length(500.0, scale)
        assert m.basis == "drawing_units_only"
        assert m.real_length_m is None

    def test_units_confidence_echoed_in_drawing_units_only(self) -> None:
        scale = ScaleContext(
            conversion_factor=None,
            real_world_available=False,
            units_confidence="unknown",
        )
        m = measure_length(100.0, scale)
        assert m.units_confidence == "unknown"

    def test_conversion_factor_echoed_in_drawing_units_only(self) -> None:
        # Even when unusable, the raw factor is echoed for diagnostics.
        scale = ScaleContext(
            conversion_factor=-5.0,
            real_world_available=True,
            contradicted=False,
            units_confidence="inferred",
        )
        m = measure_length(100.0, scale)
        assert m.conversion_factor == pytest.approx(-5.0)


# ---------------------------------------------------------------------------
# drawing_length coercion
# ---------------------------------------------------------------------------


class TestDrawingLengthCoercion:
    def test_negative_coerced_to_zero(self) -> None:
        scale = ScaleContext(
            conversion_factor=0.001,
            real_world_available=True,
            contradicted=False,
            units_confidence="inferred",
        )
        m = measure_length(-500.0, scale)
        assert m.drawing_length == pytest.approx(0.0)
        assert m.real_length_m == pytest.approx(0.0)

    def test_nan_coerced_to_zero(self) -> None:
        scale = ScaleContext(
            conversion_factor=0.001,
            real_world_available=True,
            contradicted=False,
            units_confidence="inferred",
        )
        m = measure_length(math.nan, scale)
        assert m.drawing_length == pytest.approx(0.0)
        assert m.real_length_m == pytest.approx(0.0)

    def test_inf_coerced_to_zero(self) -> None:
        scale = ScaleContext(
            conversion_factor=0.001,
            real_world_available=True,
            contradicted=False,
            units_confidence="inferred",
        )
        m = measure_length(math.inf, scale)
        assert m.drawing_length == pytest.approx(0.0)
        assert m.real_length_m == pytest.approx(0.0)

    def test_negative_inf_coerced_to_zero_no_raise(self) -> None:
        scale = ScaleContext(
            conversion_factor=None,
            real_world_available=False,
            units_confidence="unknown",
        )
        m = measure_length(-math.inf, scale)
        assert m.drawing_length == pytest.approx(0.0)
        assert m.real_length_m is None


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_identical_inputs_produce_identical_measurement(self) -> None:
        scale = ScaleContext(
            conversion_factor=0.01,
            real_world_available=True,
            contradicted=False,
            units_confidence="inferred",
        )
        results = [measure_length(3600.0, scale) for _ in range(5)]
        assert all(r == results[0] for r in results)

    def test_drawing_units_only_deterministic(self) -> None:
        scale = ScaleContext(
            conversion_factor=None,
            real_world_available=False,
            units_confidence="unknown",
        )
        results = [measure_length(100.0, scale) for _ in range(5)]
        assert all(r == results[0] for r in results)


# ---------------------------------------------------------------------------
# Measurement is a frozen dataclass (structural check)
# ---------------------------------------------------------------------------


class TestMeasurementStructure:
    def test_measurement_is_frozen(self) -> None:
        scale = ScaleContext(
            conversion_factor=0.001,
            real_world_available=True,
            units_confidence="confirmed",
        )
        m = measure_length(1000.0, scale)
        with pytest.raises((AttributeError, TypeError)):
            m.drawing_length = 9999.0  # type: ignore[misc]

    def test_drawing_length_always_nonnegative(self) -> None:
        for dl in (0.0, 100.0, 1e6):
            scale = ScaleContext(
                conversion_factor=0.001,
                real_world_available=True,
                units_confidence="inferred",
            )
            m = measure_length(dl, scale)
            assert m.drawing_length >= 0.0
