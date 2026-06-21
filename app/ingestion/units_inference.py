"""Pure unit-inference engine -- Tier-1 dimension-text + Tier-2 geometry plausibility (#558 P2).

Implements dimension-text-override inference (Tier-1) and magnitude-band
inference from drawing extent (Tier-2).  No DB, ORM, FastAPI, or SQLAlchemy
imports.  Domain knowledge (unit names and meter-scale factors) is imported
exclusively from :mod:`app.ingestion.adapters._units`; this module never
duplicates that table.

Architecture constraints (ADR-004 / interpretation purity seam):
- Inferred values are never "confirmed" -- confidence is only "inferred" or
  "unknown".
- Identical inputs always produce identical outputs (determinism invariant).
- Never raises on degenerate input (None extent, NaN/inf, exotic codes).

Tier-1 (dimension-observation) runs first and outranks Tier-2 (extent) when
at least one usable, in-band DimensionObservation is present.
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
class DimensionObservation:
    """One dimension entity's measurement data for Tier-1 unit inference.

    Adapter-agnostic: no ezdxf types.  Reused by #602 without change.

    Attributes
    ----------
    raw_span:
        Euclidean distance between defpoint2 and defpoint3 in raw drawing
        units (the physical extent the dimension annotates).
    stated_override:
        Drafter's NUMERIC typed override text value.  None means the label
        is measured-only or non-numeric and this observation is UNUSABLE.
    dimlfac:
        DIMLFAC display linear scale factor; default 1.0.
    is_linear:
        False for angular/radial/etc. dimensions that are skipped.
    """

    raw_span: float
    stated_override: float | None
    dimlfac: float = 1.0
    is_linear: bool = True


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
# Tier-1 helper -- dimension-text override inference
# ---------------------------------------------------------------------------


def _infer_from_dimensions(
    observations: tuple[DimensionObservation, ...],
    *,
    declared_code: int | None,
    declared_normalized: str | None,
) -> UnitInferenceResult | None:
    """Infer unit from drafter dimension-text overrides (Tier-1).

    Returns a UnitInferenceResult when at least one usable observation casts a
    vote, or None to signal fall-through to Tier-2 (extent).

    Never raises.  Non-DimensionObservation entries are silently skipped.

    Algorithm
    ---------
    1. Filter to usable observations: DimensionObservation instances with
       is_linear=True, finite+positive stated_override, finite+non-negative raw_span
       (zero is allowed -- raw_span is kept for audit/P3 but not used in the vote),
       finite+positive dimlfac.
    2. For each usable observation compute display_value = stated_override / dimlfac
       and find ALL in-band _BANDS entries.  Pick the one with the highest
       construction priority (_TIEBREAK: mm=0 > cm=1 > m=2) as the vote for that
       observation.  When multiple bands match, millimeter always wins over centimeter
       (deliberate construction-CAD bias -- see comment in the loop).
    3. Aggregate all votes via median: map to _TIEBREAK priority, sort, pick middle
       (even count -> lower _TIEBREAK index of two middles = higher priority).
    4. Build result with basis "dimension_text:single" or "dimension_text:median".
    """
    # --- usable filter ---
    usable: list[DimensionObservation] = []
    for obs in observations:
        if not isinstance(obs, DimensionObservation):
            continue  # backward-compat: skip non-DimensionObservation entries
        if not obs.is_linear:
            continue
        if obs.stated_override is None:
            continue
        if not (math.isfinite(obs.stated_override) and obs.stated_override > 0):
            continue
        if not (math.isfinite(obs.raw_span) and obs.raw_span >= 0):
            continue
        if not (math.isfinite(obs.dimlfac) and obs.dimlfac > 0):
            continue
        usable.append(obs)

    if not usable:
        return None

    # --- per-observation vote ---
    votes: list[int] = []  # one winning code per in-band usable observation
    for obs in usable:
        display_value = obs.stated_override / obs.dimlfac  # type: ignore[operator]
        if display_value <= 0 or not math.isfinite(display_value):
            continue
        log_dv = math.log10(display_value)
        # Collect ALL in-band codes, then pick the one with the highest construction
        # priority (lowest _TIEBREAK index: mm=0 > cm=1 > m=2).  This encodes a
        # deliberate bias: in the mm/cm magnitude overlap, prefer millimeter, because
        # construction CAD drawings overwhelmingly default to mm and genuine cm drawings
        # are rare.  Do NOT use the band score to choose -- score-based selection
        # mis-maps common mm values (e.g. 3600, 2400) to centimeter.
        in_band_codes: list[int] = []
        for band_log, code, _band_label in _BANDS:
            if _band_score(log_dv, band_log) is not None:
                in_band_codes.append(code)
        if in_band_codes:
            # Lower _TIEBREAK index = higher priority (mm wins over cm wins over m).
            best_code = min(in_band_codes, key=lambda c: _TIEBREAK[c])
            votes.append(best_code)

    if not votes:
        return None

    # --- median aggregate (permutation-invariant) ---
    # Sort votes by _TIEBREAK priority index so that ties resolve deterministically.
    sorted_votes = sorted(votes, key=lambda c: _TIEBREAK[c])
    n = len(sorted_votes)
    # Even count: pick the higher-priority (lower _TIEBREAK index) of the two middles.
    winner_code = sorted_votes[n // 2] if n % 2 == 1 else sorted_votes[n // 2 - 1]

    winner_name = UNIT_NAMES[winner_code]
    winner_factor = METER_SCALE[winner_code]
    basis = "dimension_text:single" if n == 1 else "dimension_text:median"

    # Distinct voted units as candidates in _TIEBREAK order.
    seen: set[int] = set()
    candidate_codes: list[int] = []
    for code in sorted_votes:
        if code not in seen:
            seen.add(code)
            candidate_codes.append(code)
    candidates = tuple(
        UnitCandidate(
            normalized=UNIT_NAMES[code],
            conversion_factor=METER_SCALE[code],
            band_label="dimension_override",
        )
        for code in candidate_codes
    )

    # Contradiction check (same logic as Tier-2 path).
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
        basis=basis,
        contradicts_declared=contradicts_declared,
        declared_normalized=declared_normalized,
        candidates=candidates,
    )


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
    text_height_samples: tuple[float, ...] = (),  # UNUSED in P2 -- declared seam for P3
    dimension_observations: tuple[DimensionObservation, ...] = (),  # Tier-1 (P2)
) -> UnitInferenceResult:
    """Infer the drawing's native unit from dimension overrides (Tier-1) or extent (Tier-2).

    Parameters
    ----------
    declared_code:
        Raw INSUNITS code from the drawing header; may be None or exotic.
    extent:
        Raw bounding box in native units; None when unavailable.
    text_height_samples:
        Reserved for Tier-1 text-height inference (P3); ignored in P2.
    dimension_observations:
        DimensionObservation entries for Tier-1 inference.  Non-DimensionObservation
        entries are silently skipped for backward compatibility with P1 seam callers.

    Returns
    -------
    :class:`UnitInferenceResult`
        ``confidence`` is ``"inferred"`` when a band match is found, otherwise
        ``"unknown"``.  Never raises; always returns a well-formed result.
    """
    _ = text_height_samples  # seam -- P3 will use this

    declared_normalized = _resolve_declared(declared_code)

    # Tier-1: dimension-text override inference.  Runs first and outranks Tier-2
    # when at least one usable, in-band observation is present.
    tier1 = _infer_from_dimensions(
        dimension_observations,
        declared_code=declared_code,
        declared_normalized=declared_normalized,
    )
    if tier1 is not None:
        return tier1

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
