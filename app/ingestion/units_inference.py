"""Pure unit-inference engine — Tier-2 geometry plausibility (issue #558, P1).

Implements magnitude-band inference from drawing extent only.  No DB, ORM,
FastAPI, or SQLAlchemy imports.  Domain knowledge (unit names and meter-scale
factors) is imported exclusively from :mod:`app.ingestion.adapters._units`;
this module never duplicates that table.

Architecture constraints (ADR-004 / interpretation purity seam):
- Inferred values are never "confirmed" — confidence is only "inferred" or
  "unknown".
- Identical inputs always produce identical outputs (determinism invariant).
- Never raises on degenerate input (None extent, NaN/inf, exotic codes).

Tier-1 (dimension-observation) logic belongs to #601/#602 and is represented
here only as declared-but-unused seam parameters so the P2/P3 consumers can
call the same signature without change.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal

from app.ingestion.adapters._units import METER_SCALE, UNIT_NAMES

# ---------------------------------------------------------------------------
# Public dataclasses — frozen P1 contract consumed by P2/P3
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Extent:
    """Raw drawing extent in the file's native unit (before any scaling)."""

    width: float
    height: float


@dataclass(frozen=True, slots=True)
class UnitCandidate:
    """One plausible unit from the magnitude-band analysis.

    ``band_label`` names the magnitude band that placed this candidate, giving
    the caller transparent provenance without needing to reverse-engineer the
    score.
    """

    normalized: str  # UNIT_NAMES value, e.g. "millimeter","centimeter","meter"
    conversion_factor: float  # METER_SCALE value for this unit
    band_label: str  # which extent band placed it


@dataclass(frozen=True, slots=True)
class UnitInferenceResult:
    """Outcome of :func:`infer_units` for a single drawing extent observation.

    ``confidence`` is never ``"confirmed"`` — that designation is reserved for
    adapter-level declared units that have been reviewed against METER_SCALE.
    """

    normalized: str  # winning unit name, or "unknown"
    conversion_factor: float | None  # winner's meters factor; None when unknown
    confidence: Literal["inferred", "unknown"]  # NEVER "confirmed"
    basis: str  # names the winning signal
    contradicts_declared: bool
    declared_normalized: str | None  # resolved from declared_code when curated
    candidates: tuple[UnitCandidate, ...]  # deterministic order


# ---------------------------------------------------------------------------
# Magnitude bands — module-level named constants
#
# Each band maps the *representative raw size* (max of width/height in native
# units) to the unit that would produce building-scale geometry (~1-300 m for
# a typical architectural plan).
#
# Bands intentionally overlap.  Scores are computed as how "centred" the size
# is in log-space within each band; the best score wins.  Ties are broken by
# a fixed unit priority: millimeter > centimeter > meter — construction DWGs
# almost universally default to millimeters, so when a size sits equally well
# in two bands, the coarser-sounding unit should not win by accident.
# ---------------------------------------------------------------------------

# (lower_inclusive, upper_inclusive) for log10 of the representative size.
# meter:       10^0  .. 10^2.477  (~1 .. ~300)
# centimeter:  10^2  .. 10^4.477  (~100 .. ~30 000)
# millimeter:  10^3  .. 10^5.477  (~1 000 .. ~300 000)
_BAND_LOG_METER = (0.0, math.log10(3e2))
_BAND_LOG_CENTIMETER = (math.log10(1e2), math.log10(3e4))
_BAND_LOG_MILLIMETER = (math.log10(1e3), math.log10(3e5))

# INSUNITS codes for the three building-scale units; must exist in METER_SCALE.
_CODE_METER = 6
_CODE_CENTIMETER = 5
_CODE_MILLIMETER = 4

# Tiebreak priority index (lower = higher priority).
_TIEBREAK: dict[int, int] = {
    _CODE_MILLIMETER: 0,
    _CODE_CENTIMETER: 1,
    _CODE_METER: 2,
}

_BANDS: tuple[tuple[tuple[float, float], int, str], ...] = (
    (_BAND_LOG_MILLIMETER, _CODE_MILLIMETER, "millimeter"),
    (_BAND_LOG_CENTIMETER, _CODE_CENTIMETER, "centimeter"),
    (_BAND_LOG_METER, _CODE_METER, "meter"),
)

# Contradiction threshold: one order of magnitude (factor ≥ 10 or ≤ 0.1).
_CONTRADICTION_RATIO_THRESHOLD = 10.0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _representative_size(extent: Extent) -> float:
    """Return the single scalar that represents the drawing's overall size.

    Uses ``max(width, height)`` — the dominant dimension — rather than area,
    because buildings may be elongated and we want a single magnitude signal
    that maps cleanly to one of the calibrated bands.
    """
    return max(extent.width, extent.height)


def _band_score(log_size: float, band_log: tuple[float, float]) -> float | None:
    """Score how well ``log_size`` sits inside ``band_log``.

    Returns a non-negative score (higher = better fit) when the size is within
    the band, or ``None`` when it falls outside.  The score is the half-width
    of the band minus the distance to the band midpoint, so the band centre
    scores highest.  Boundary hits score exactly 0.0 and are still "in band".
    """
    lo, hi = band_log
    if log_size < lo or log_size > hi:
        return None
    mid = (lo + hi) / 2.0
    half_width = (hi - lo) / 2.0
    return half_width - abs(log_size - mid)


def _unknown_result(
    *, declared_normalized: str | None, basis: str = "no_extent"
) -> UnitInferenceResult:
    return UnitInferenceResult(
        normalized="unknown",
        conversion_factor=None,
        confidence="unknown",
        basis=basis,
        contradicts_declared=False,
        declared_normalized=declared_normalized,
        candidates=(),
    )


def _resolve_declared(declared_code: int | None) -> str | None:
    """Return the curated unit name for ``declared_code``, or None if not curated."""
    if declared_code is not None and declared_code in METER_SCALE:
        return UNIT_NAMES[declared_code]
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extent_from_bboxes(bboxes: list[dict[str, object]]) -> Extent | None:
    """Aggregate per-entity bboxes into a single drawing :class:`Extent`.

    Each bbox must be a dict with ``"min"`` and ``"max"`` sub-dicts that each
    carry ``"x"`` and ``"y"`` numeric values.  Bboxes that are ``None``, not
    dicts, or that contain non-finite coordinates are silently skipped.

    Returns ``None`` when no usable bbox is found.
    """
    xs: list[float] = []
    ys: list[float] = []
    for bbox in bboxes:
        if not isinstance(bbox, dict):
            continue
        mn = bbox.get("min")
        mx = bbox.get("max")
        if not isinstance(mn, dict) or not isinstance(mx, dict):
            continue
        for sub in (mn, mx):
            x_raw = sub.get("x")
            y_raw = sub.get("y")
            if (
                isinstance(x_raw, (int, float))
                and not isinstance(x_raw, bool)
                and math.isfinite(float(x_raw))
            ):
                xs.append(float(x_raw))
            if (
                isinstance(y_raw, (int, float))
                and not isinstance(y_raw, bool)
                and math.isfinite(float(y_raw))
            ):
                ys.append(float(y_raw))
    if not xs or not ys:
        return None
    width = max(xs) - min(xs)
    height = max(ys) - min(ys)
    if not math.isfinite(width) or not math.isfinite(height):
        return None
    # A degenerate extent (all points collinear) gives 0 in one dimension.
    # This is still usable — representative_size takes the max of both, so as
    # long as the other dimension is non-zero the inference fires normally.
    return Extent(width=width, height=height)


def infer_units(
    *,
    declared_code: int | None,
    extent: Extent | None,
    text_height_samples: tuple[float, ...] = (),  # UNUSED in P1 — declared seam
    dimension_observations: tuple[object, ...] = (),  # UNUSED in P1 — declared seam
) -> UnitInferenceResult:
    """Infer the drawing's native unit from extent magnitude only (P1).

    Parameters
    ----------
    declared_code:
        Raw INSUNITS code from the drawing header; may be None or exotic.
    extent:
        Raw bounding box in native units; None when unavailable.
    text_height_samples:
        Reserved for Tier-1 text-height inference (P3); ignored in P1.
    dimension_observations:
        Reserved for Tier-1 dimension-observation inference (P2); ignored in P1.

    Returns
    -------
    :class:`UnitInferenceResult`
        ``confidence`` is ``"inferred"`` when a band match is found, otherwise
        ``"unknown"``.  Never raises; always returns a well-formed result.
    """
    # Prevent any accidental use of the seam parameters from affecting output.
    # The arguments are received but intentionally not read beyond this point.
    _ = text_height_samples
    _ = dimension_observations

    declared_normalized = _resolve_declared(declared_code)

    # Guard: missing or degenerate extent → unknown.
    if extent is None:
        return _unknown_result(declared_normalized=declared_normalized)

    # Check both dimensions individually before computing the representative size.
    # Python's max() is not symmetric with NaN (max(10, nan) == 10 but
    # max(nan, 10) == nan), so we must validate each dimension first.
    if not math.isfinite(extent.width) or not math.isfinite(extent.height):
        return _unknown_result(declared_normalized=declared_normalized)

    # A degenerate / 1-D extent (one dimension is zero — e.g. a single line or all-collinear
    # geometry) is NOT a scale-inferable 2-D drawing. Inferring a unit from it would be
    # over-confident (the unitless single-line case), so stay honestly unknown (#600 review).
    if extent.width <= 0.0 or extent.height <= 0.0:
        return _unknown_result(declared_normalized=declared_normalized, basis="degenerate_extent")

    size = _representative_size(extent)

    log_size = math.log10(size)

    # Score every band; collect candidates that are in-band.
    scored: list[tuple[float, int, str]] = []  # (score, code, band_label)
    for band_log, code, band_label in _BANDS:
        score = _band_score(log_size, band_log)
        if score is not None:
            scored.append((score, code, band_label))

    if not scored:
        # Size is outside all calibrated bands.
        return UnitInferenceResult(
            normalized="unknown",
            conversion_factor=None,
            confidence="unknown",
            basis="extent_out_of_range",
            contradicts_declared=False,
            declared_normalized=declared_normalized,
            candidates=(),
        )

    # Sort descending by score; tiebreak by unit priority (lower index = higher priority).
    scored.sort(key=lambda t: (-t[0], _TIEBREAK[t[1]]))

    candidates = tuple(
        UnitCandidate(
            normalized=UNIT_NAMES[code],
            conversion_factor=METER_SCALE[code],
            band_label=band_label,
        )
        for _, code, band_label in scored
    )

    # Winner is the first candidate after deterministic sort.
    winner_code = scored[0][1]
    winner_name = UNIT_NAMES[winner_code]
    winner_factor = METER_SCALE[winner_code]
    winner_band = scored[0][2]

    # Contradiction check: only when declared_code resolves to a curated unit
    # AND the conversion factors differ by ≥ one order of magnitude.
    contradicts_declared = False
    if declared_code is not None and declared_code in METER_SCALE:
        declared_factor = METER_SCALE[declared_code]
        ratio = winner_factor / declared_factor
        inverse_threshold = 1.0 / _CONTRADICTION_RATIO_THRESHOLD
        if ratio >= _CONTRADICTION_RATIO_THRESHOLD or ratio <= inverse_threshold:
            contradicts_declared = True

    return UnitInferenceResult(
        normalized=winner_name,
        conversion_factor=winner_factor,
        confidence="inferred",
        basis=f"extent_magnitude_band:{winner_band}",
        contradicts_declared=contradicts_declared,
        declared_normalized=declared_normalized,
        candidates=candidates,
    )
