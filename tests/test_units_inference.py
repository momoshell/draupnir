"""Pure unit tests for the units inference engine (issue #558, P1).

All tests are self-contained (no DB, no filesystem, no adapters beyond the
shared units table imported transitively through units_inference).

Test categories:
- Building-scale extents → millimeter inference
- Meter-scale extents → meter inference
- Contradiction detection
- Determinism under permutation of unused seam parameters
- Never-confirmed invariant
- Degenerate inputs (None, NaN, inf, zero, negative, exotic declared_code)
- Unused seam parameters do not affect output
"""

from __future__ import annotations

import math
from itertools import permutations
from typing import Any

import pytest

from app.ingestion.adapters._units import METER_SCALE
from app.ingestion.units_inference import (
    Extent,
    UnitCandidate,  # noqa: F401 — imported to confirm it is exportable
    UnitInferenceResult,
    extent_from_bboxes,
    infer_units,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _infer(
    *,
    declared_code: int | None = None,
    width: float = 0.0,
    height: float = 0.0,
    text_height_samples: tuple[float, ...] = (),
    dimension_observations: tuple[object, ...] = (),
) -> UnitInferenceResult:
    return infer_units(
        declared_code=declared_code,
        extent=Extent(width=width, height=height),
        text_height_samples=text_height_samples,
        dimension_observations=dimension_observations,
    )


# ---------------------------------------------------------------------------
# Building-scale (mm) extents
# ---------------------------------------------------------------------------


class TestMillimeterInference:
    """Representative size 1e4-1e5 raw units -> infers millimeter."""

    def test_typical_floor_plan_mm(self) -> None:
        # A 50 m x 30 m building drawn in mm: 50 000 x 30 000 raw units.
        result = _infer(declared_code=None, width=50_000.0, height=30_000.0)
        assert result.normalized == "millimeter"
        assert result.conversion_factor == METER_SCALE[4]
        assert result.conversion_factor == 0.001
        assert result.confidence == "inferred"
        assert "millimeter" in result.basis

    def test_smaller_floor_plan_mm(self) -> None:
        # 10 m x 8 m in mm: 10 000 x 8 000.
        result = _infer(declared_code=None, width=10_000.0, height=8_000.0)
        assert result.normalized == "millimeter"
        assert result.conversion_factor == 0.001

    def test_basis_names_band(self) -> None:
        result = _infer(width=25_000.0, height=15_000.0)
        assert result.basis.startswith("extent_magnitude_band:")
        assert "millimeter" in result.basis

    def test_candidates_nonempty_for_mm_extent(self) -> None:
        result = _infer(width=20_000.0, height=10_000.0)
        assert len(result.candidates) >= 1
        assert result.candidates[0].normalized == "millimeter"

    def test_candidates_deterministic_order(self) -> None:
        r1 = _infer(width=20_000.0, height=10_000.0)
        r2 = _infer(width=20_000.0, height=10_000.0)
        assert r1.candidates == r2.candidates


# ---------------------------------------------------------------------------
# Meter-scale extents
# ---------------------------------------------------------------------------


class TestMeterInference:
    """Representative size ~1e1-1e2 raw units -> infers meter."""

    def test_meter_scale_small(self) -> None:
        # 80 m x 50 m in metres: 80 x 50 raw units.
        result = _infer(declared_code=None, width=80.0, height=50.0)
        assert result.normalized == "meter"
        assert result.conversion_factor == 1.0
        assert result.confidence == "inferred"

    def test_meter_scale_mid(self) -> None:
        # 30 m x 20 m -- well inside the meter band and below the centimeter overlap floor.
        result = _infer(width=30.0, height=20.0)
        assert result.normalized == "meter"
        assert result.conversion_factor == 1.0

    def test_meter_basis_names_band(self) -> None:
        result = _infer(width=100.0, height=60.0)
        assert "meter" in result.basis


# ---------------------------------------------------------------------------
# Contradiction detection
# ---------------------------------------------------------------------------


class TestContradiction:
    """Declared meter (code 6) + mm-scale extent → contradicts_declared is True."""

    def test_declared_meter_but_mm_extent(self) -> None:
        # declared: meter (factor 1.0), inferred: millimeter (factor 0.001).
        # ratio = 0.001 / 1.0 = 0.001, which is ≤ 0.1 → contradiction.
        result = _infer(declared_code=6, width=50_000.0, height=30_000.0)
        assert result.contradicts_declared is True
        assert result.declared_normalized == "meter"
        assert result.normalized == "millimeter"  # winner is NOT overridden

    def test_declared_mm_agrees_with_mm_extent(self) -> None:
        # declared: millimeter (code 4, factor 0.001), inferred: millimeter.
        result = _infer(declared_code=4, width=50_000.0, height=30_000.0)
        assert result.contradicts_declared is False
        assert result.declared_normalized == "millimeter"
        assert result.normalized == "millimeter"

    def test_declared_meter_agrees_with_meter_extent(self) -> None:
        result = _infer(declared_code=6, width=80.0, height=50.0)
        assert result.contradicts_declared is False
        assert result.declared_normalized == "meter"

    def test_candidates_contain_inferred_unit_even_when_contradiction(self) -> None:
        result = _infer(declared_code=6, width=50_000.0, height=30_000.0)
        normalized_names = [c.normalized for c in result.candidates]
        assert "millimeter" in normalized_names

    def test_declared_uncurated_code_no_contradiction(self) -> None:
        # Code 0 (unitless) is not in METER_SCALE → no contradiction possible.
        result = _infer(declared_code=0, width=50_000.0, height=30_000.0)
        assert result.contradicts_declared is False
        assert result.declared_normalized is None

    def test_declared_none_no_contradiction(self) -> None:
        result = _infer(declared_code=None, width=50_000.0, height=30_000.0)
        assert result.contradicts_declared is False
        assert result.declared_normalized is None

    def test_centimeter_declared_vs_meter_extent_contradiction(self) -> None:
        # centimeter (0.01) vs meter (1.0): ratio = 1.0 / 0.01 = 100 → contradiction.
        result = _infer(declared_code=5, width=80.0, height=50.0)
        assert result.contradicts_declared is True
        assert result.declared_normalized == "centimeter"


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    """Identical inputs always produce identical outputs; unused params are no-ops."""

    def test_repeated_calls_identical(self) -> None:
        for _ in range(5):
            r = _infer(declared_code=4, width=20_000.0, height=15_000.0)
            assert r.normalized == "millimeter"
            assert r.confidence == "inferred"

    def test_text_height_samples_permutation_invariant(self) -> None:
        samples_base: tuple[float, ...] = (1.5, 2.0, 3.0, 0.5)
        base = _infer(
            declared_code=None,
            width=20_000.0,
            height=15_000.0,
            text_height_samples=samples_base,
        )
        for perm in permutations(samples_base):
            result = _infer(
                declared_code=None,
                width=20_000.0,
                height=15_000.0,
                text_height_samples=perm,
            )
            assert result == base, f"Differed on permutation {perm!r}"

    def test_dimension_observations_permutation_invariant(self) -> None:
        obs_base: tuple[object, ...] = ("a", "b", "c")
        base = _infer(
            declared_code=None,
            width=20_000.0,
            height=15_000.0,
            dimension_observations=obs_base,
        )
        for perm in permutations(obs_base):
            result = _infer(
                declared_code=None,
                width=20_000.0,
                height=15_000.0,
                dimension_observations=tuple(perm),
            )
            assert result == base, f"Differed on permutation {perm!r}"

    def test_candidates_order_stable(self) -> None:
        r1 = _infer(width=20_000.0, height=10_000.0)
        r2 = _infer(width=20_000.0, height=10_000.0)
        assert r1.candidates == r2.candidates


# ---------------------------------------------------------------------------
# Never-confirmed invariant
# ---------------------------------------------------------------------------


class TestNeverConfirmed:
    """confidence must never equal 'confirmed' for any input."""

    @pytest.mark.parametrize(
        "declared_code,width,height",
        [
            (None, 50_000.0, 30_000.0),
            (4, 50_000.0, 30_000.0),
            (6, 80.0, 50.0),
            (6, 50_000.0, 30_000.0),  # contradiction case
            (0, 50_000.0, 30_000.0),
            (999, 50_000.0, 30_000.0),
            (None, 80.0, 50.0),
            (5, 8_000.0, 5_000.0),
        ],
    )
    def test_never_confirmed(self, declared_code: int | None, width: float, height: float) -> None:
        result = _infer(declared_code=declared_code, width=width, height=height)
        assert result.confidence in ("inferred", "unknown")


# ---------------------------------------------------------------------------
# Degenerate inputs — must never raise; must return well-formed unknown
# ---------------------------------------------------------------------------


class TestDegenerateInputs:
    def test_none_extent(self) -> None:
        result = infer_units(declared_code=None, extent=None)
        assert result.confidence == "unknown"
        assert result.normalized == "unknown"
        assert result.conversion_factor is None
        assert result.candidates == ()

    def test_nan_width(self) -> None:
        result = _infer(width=math.nan, height=10_000.0)
        assert result.confidence == "unknown"

    def test_nan_height(self) -> None:
        # Both orderings of NaN must degrade — check width=nan explicitly.
        result = _infer(width=math.nan, height=10_000.0)
        assert result.confidence == "unknown"

    def test_nan_width_second_arg(self) -> None:
        # Python max(10000, nan) == 10000 (nan is not propagated when second arg),
        # but the guard checks each dimension individually so this must also degrade.
        result = _infer(width=10_000.0, height=math.nan)
        assert result.confidence == "unknown"

    def test_inf_width(self) -> None:
        result = _infer(width=math.inf, height=10_000.0)
        assert result.confidence == "unknown"

    def test_negative_inf(self) -> None:
        # -inf is not finite → guard must catch it regardless of max() behaviour.
        result = _infer(width=-math.inf, height=10_000.0)
        assert result.confidence == "unknown"

    def test_zero_width_height(self) -> None:
        result = _infer(width=0.0, height=0.0)
        assert result.confidence == "unknown"

    def test_negative_dims(self) -> None:
        # max(-50 000, -30 000) = -30 000 <= 0 -> unknown.
        result = _infer(width=-50_000.0, height=-30_000.0)
        assert result.confidence == "unknown"

    def test_size_out_of_range_tiny(self) -> None:
        # 0.001 raw units → log10(0.001) = -3, outside all bands.
        result = _infer(width=0.001, height=0.0005)
        assert result.confidence == "unknown"
        assert result.basis == "extent_out_of_range"

    def test_size_out_of_range_huge(self) -> None:
        # 1e10 raw units → log10(1e10) = 10, outside all bands.
        result = _infer(width=1e10, height=5e9)
        assert result.confidence == "unknown"
        assert result.basis == "extent_out_of_range"

    def test_exotic_declared_code_no_raise(self) -> None:
        result = _infer(declared_code=9999, width=50_000.0, height=30_000.0)
        assert result.confidence == "inferred"
        assert result.declared_normalized is None
        assert result.contradicts_declared is False

    def test_none_extent_with_curated_declared(self) -> None:
        result = infer_units(declared_code=6, extent=None)
        assert result.confidence == "unknown"
        assert result.declared_normalized == "meter"  # still resolved from code

    def test_none_extent_basis_is_no_extent(self) -> None:
        result = infer_units(declared_code=None, extent=None)
        assert result.basis == "no_extent"


# ---------------------------------------------------------------------------
# Unused seam parameters do not change output
# ---------------------------------------------------------------------------


class TestUnusedSeamParameters:
    """Non-empty seam params must produce the same result as empty."""

    def test_text_height_samples_nonempty_vs_empty(self) -> None:
        empty = _infer(declared_code=None, width=20_000.0, height=15_000.0)
        with_samples = _infer(
            declared_code=None,
            width=20_000.0,
            height=15_000.0,
            text_height_samples=(1.0, 2.0, 3.0),
        )
        assert empty == with_samples

    def test_dimension_observations_nonempty_vs_empty(self) -> None:
        empty = _infer(declared_code=None, width=20_000.0, height=15_000.0)
        with_obs = _infer(
            declared_code=None,
            width=20_000.0,
            height=15_000.0,
            dimension_observations=(object(), object()),
        )
        assert empty == with_obs

    def test_both_seam_params_nonempty_vs_empty(self) -> None:
        empty = _infer(declared_code=4, width=50_000.0, height=30_000.0)
        with_both = _infer(
            declared_code=4,
            width=50_000.0,
            height=30_000.0,
            text_height_samples=(0.5, 1.0),
            dimension_observations=("x",),
        )
        assert empty == with_both


# ---------------------------------------------------------------------------
# Result shape invariants
# ---------------------------------------------------------------------------


class TestResultShapeInvariants:
    """Structural guarantees on every result."""

    def _all_results(self) -> list[UnitInferenceResult]:
        inputs: list[dict[str, Any]] = [
            {"declared_code": None, "width": 50_000.0, "height": 30_000.0},
            {"declared_code": 4, "width": 50_000.0, "height": 30_000.0},
            {"declared_code": 6, "width": 80.0, "height": 50.0},
            {"declared_code": 6, "width": 50_000.0, "height": 30_000.0},
            {"declared_code": 0, "width": 50_000.0, "height": 30_000.0},
            {"declared_code": None, "width": 0.001, "height": 0.0005},
            {"declared_code": None, "width": 1e10, "height": 5e9},
        ]
        results = [_infer(**kw) for kw in inputs]
        results.append(infer_units(declared_code=None, extent=None))
        return results

    def test_confidence_is_inferred_or_unknown(self) -> None:
        for r in self._all_results():
            assert r.confidence in ("inferred", "unknown")

    def test_unknown_has_none_conversion_factor(self) -> None:
        for r in self._all_results():
            if r.confidence == "unknown":
                assert r.conversion_factor is None

    def test_inferred_has_nonnone_conversion_factor(self) -> None:
        for r in self._all_results():
            if r.confidence == "inferred":
                assert r.conversion_factor is not None

    def test_unknown_has_empty_candidates(self) -> None:
        for r in self._all_results():
            if r.normalized == "unknown":
                # unknown result from no_extent or out_of_range → empty candidates
                assert r.candidates == ()

    def test_candidates_are_tuple(self) -> None:
        for r in self._all_results():
            assert isinstance(r.candidates, tuple)

    def test_winner_matches_first_candidate_when_inferred(self) -> None:
        for r in self._all_results():
            if r.confidence == "inferred" and r.candidates:
                assert r.candidates[0].normalized == r.normalized
                assert r.candidates[0].conversion_factor == r.conversion_factor


# ---------------------------------------------------------------------------
# extent_from_bboxes helper
# ---------------------------------------------------------------------------


class TestExtentFromBboxes:
    """Pure unit tests for the shared extent aggregation helper."""

    def _bbox(self, x0: float, y0: float, x1: float, y1: float) -> dict[str, object]:
        return {
            "min": {"x": x0, "y": y0, "z": 0.0},
            "max": {"x": x1, "y": y1, "z": 0.0},
        }

    def test_none_on_empty_list(self) -> None:
        assert extent_from_bboxes([]) is None

    def test_none_when_all_bboxes_non_dict(self) -> None:
        assert extent_from_bboxes([None, "foo", 42]) is None  # type: ignore[list-item]

    def test_none_when_all_coords_nonfinite(self) -> None:
        bbox: dict[str, object] = {
            "min": {"x": float("nan"), "y": 0.0},
            "max": {"x": float("inf"), "y": 0.0},
        }
        assert extent_from_bboxes([bbox]) is None

    def test_single_bbox_produces_correct_extent(self) -> None:
        result = extent_from_bboxes([self._bbox(0.0, 0.0, 40_000.0, 20_000.0)])
        assert result is not None
        assert result.width == pytest.approx(40_000.0)
        assert result.height == pytest.approx(20_000.0)

    def test_multiple_bboxes_aggregated_correctly(self) -> None:
        bboxes = [
            self._bbox(0.0, 0.0, 10_000.0, 5_000.0),
            self._bbox(5_000.0, 2_000.0, 40_000.0, 20_000.0),
        ]
        result = extent_from_bboxes(bboxes)
        assert result is not None
        # Overall: x range [0, 40000], y range [0, 20000]
        assert result.width == pytest.approx(40_000.0)
        assert result.height == pytest.approx(20_000.0)

    def test_collinear_entities_give_zero_height(self) -> None:
        # All points on y=0 → height=0, width > 0
        result = extent_from_bboxes([self._bbox(0.0, 0.0, 40_000.0, 0.0)])
        assert result is not None
        assert result.width == pytest.approx(40_000.0)
        assert result.height == pytest.approx(0.0)

    def test_mixed_finite_and_nonfinite_skips_nonfinite(self) -> None:
        bad_bbox: dict[str, object] = {
            "min": {"x": float("inf"), "y": 0.0},
            "max": {"x": 0.0, "y": float("nan")},
        }
        good_bbox = self._bbox(0.0, 0.0, 40_000.0, 20_000.0)
        # Non-finite coords are skipped; finite ones still produce a result.
        result = extent_from_bboxes([bad_bbox, good_bbox])
        assert result is not None
        assert result.width == pytest.approx(40_000.0)
        assert result.height == pytest.approx(20_000.0)

    def test_skips_non_dict_entries_in_mixed_list(self) -> None:
        good_bbox = self._bbox(0.0, 0.0, 40_000.0, 20_000.0)
        result = extent_from_bboxes([None, good_bbox, "invalid"])  # type: ignore[list-item]
        assert result is not None
        assert result.width == pytest.approx(40_000.0)


# ---------------------------------------------------------------------------
# P1 additions — focused cases required by issue #600
# ---------------------------------------------------------------------------


class TestCentimeterOnlyZone:
    """Representative size in the cm-exclusive window (100-999 raw) -> centimeter."""

    def test_centimeter_only_window_infers_centimeter(self) -> None:
        # 600 raw units: log10(600) ≈ 2.78.
        # cm band: [log10(100), log10(30 000)] = [2.0, 4.477] → in-band.
        # mm band: [log10(1 000), log10(300 000)] = [3.0, 5.477] → out-of-band.
        # Therefore centimeter is the sole candidate.
        result = _infer(declared_code=None, width=600.0, height=400.0)

        assert result.normalized == "centimeter"
        assert result.conversion_factor == pytest.approx(0.01)
        assert result.confidence == "inferred"
        assert len(result.candidates) == 1
        assert result.candidates[0].normalized == "centimeter"


class TestOverlapZoneTwoCandidateOrdering:
    """Size 5 000 hits both cm and mm bands; cm wins on score; mm is second candidate."""

    def test_overlap_zone_winner_and_candidate_order(self) -> None:
        # log10(5 000) ≈ 3.70.
        # cm score ≈ 0.778 > mm score ≈ 0.699 → cm wins despite mm having higher
        # tiebreak priority (_TIEBREAK: mm=0, cm=1 — tiebreak only used on equal scores).
        # Both bands are in-range so two candidates are returned.
        result = _infer(declared_code=None, width=5_000.0, height=3_000.0)

        assert result.normalized == "centimeter"
        assert result.conversion_factor == pytest.approx(0.01)
        assert len(result.candidates) == 2
        assert result.candidates[0].normalized == "centimeter"
        assert result.candidates[1].normalized == "millimeter"


class TestContradictionThresholdExactBoundaries:
    """Exact ≥/≤ boundary checks for the contradiction gate (threshold = 10.0)."""

    def test_ratio_exactly_at_threshold_contradicts(self) -> None:
        # declared cm (code 5, factor 0.01) + inferred mm (factor 0.001).
        # ratio = 0.001 / 0.01 = 0.1, which is exactly 1/threshold → contradicts (≤).
        result = _infer(declared_code=5, width=40_000.0, height=20_000.0)

        assert result.contradicts_declared is True
        assert result.declared_normalized == "centimeter"
        assert result.normalized == "millimeter"

    def test_ratio_inside_threshold_does_not_contradict(self) -> None:
        # declared foot (code 2, factor 0.3048) + inferred meter (factor 1.0).
        # ratio = 1.0 / 0.3048 ≈ 3.28, which is strictly inside [0.1, 10] → no contradiction.
        result = _infer(declared_code=2, width=80.0, height=50.0)

        assert result.contradicts_declared is False
        assert result.declared_normalized == "foot"
        assert result.normalized == "meter"


class TestCollinearExtentThroughEngine:
    """A degenerate / 1-D extent (one dimension zero) is NOT a scale-inferable 2-D drawing,
    so inference stays honestly unknown rather than over-confidently guessing (#600 review)."""

    def test_collinear_extent_is_unknown_not_inferred(self) -> None:
        # Extent(40000, 0) is collinear (e.g. a single line / all-collinear geometry).
        # declared_code=0 (unitless, not in METER_SCALE) → declared_normalized=None.
        result = infer_units(declared_code=0, extent=Extent(40_000.0, 0.0))

        assert result.normalized == "unknown"
        assert result.confidence == "unknown"
        assert result.conversion_factor is None
        assert result.basis == "degenerate_extent"
        assert result.contradicts_declared is False


class TestDeclaredCuratedOutOfRangeExtent:
    """declared_code resolves to a curated unit even when extent is out of range."""

    def test_declared_code_resolves_despite_out_of_range_extent(self) -> None:
        # Extent 1e10 x 5e9 → out of all bands → basis="extent_out_of_range", confidence="unknown".
        # declared_code=6 (meter) is in METER_SCALE → declared_normalized is still resolved.
        result = infer_units(declared_code=6, extent=Extent(1e10, 5e9))

        assert result.confidence == "unknown"
        assert result.basis == "extent_out_of_range"
        assert result.declared_normalized == "meter"
