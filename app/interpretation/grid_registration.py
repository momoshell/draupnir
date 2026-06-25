"""Grid-bubble fiducial registration: compute a rigid transform between two drawing revisions.

Pure (stdlib + math only) — no DB/ORM/FastAPI/numpy/cv2/shapely imports.

A structural grid layer (e.g. Z030G) carries INSERT entities whose nearest text labels are
identical fiducials on every sheet of the same floor.  Matching those labels yields a set of
point-pair correspondences from which a translation (and, as quality-gate data, rotation + scale)
can be estimated robustly.

ADR-003: layer name tokens are CONFIGURABLE DEFAULTS, never hardcoded literals.  The default token
"z030g" and secondary "grid" live in the loader; this module is token-agnostic.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GridFiducial:
    """A named grid-bubble with a world-space position in one revision's coordinate frame."""

    label: str  # normalized: stripped + upper-cased
    point: tuple[float, float]  # (x, y) in metres


@dataclass(frozen=True, slots=True)
class GridTransform:
    """Rigid transform estimated from matched grid fiducials.

    ``apply()`` applies TRANSLATION ONLY in v1.  ``rotation_rad`` and ``scale`` are estimated
    and reported so callers can quality-gate, but they are NOT composed into ``apply()``.
    """

    dx: float
    dy: float
    rotation_rad: float  # estimated; ~0 expected on aligned sheets
    scale: float  # estimated; ~1 expected
    matched_labels: tuple[str, ...]  # sorted, deterministic
    matched_count: int
    max_residual_m: float
    median_residual_m: float
    quality: Literal["good", "degraded", "failed"]

    def apply(self, point: tuple[float, float]) -> tuple[float, float]:
        """Translate *point* from source to target frame (v1: translation only, no rotation)."""
        return (point[0] + self.dx, point[1] + self.dy)


# ---------------------------------------------------------------------------
# Label normalization
# ---------------------------------------------------------------------------


def _normalize_label(raw: str) -> str:
    """Strip whitespace and upper-case so "b", "B ", " B" all map to "B"."""
    return raw.strip().upper()


# ---------------------------------------------------------------------------
# Robust median helpers
# ---------------------------------------------------------------------------


def _median(values: list[float]) -> float:
    """Deterministic median: even count → average of the two central values."""
    n = len(values)
    if n == 0:
        raise ValueError("median of empty sequence")
    s = sorted(values)
    mid = n // 2
    if n % 2 == 1:
        return s[mid]
    return (s[mid - 1] + s[mid]) / 2.0


# ---------------------------------------------------------------------------
# Rotation / scale estimation (quality gates only — not applied in v1)
# ---------------------------------------------------------------------------


def _estimate_rotation_scale(
    pairs: list[tuple[tuple[float, float], tuple[float, float]]],
) -> tuple[float, float]:
    """Estimate rotation (rad) and scale from the two most-separated matched pairs.

    Uses the vector between the two most-separated source points: compares its length and
    angle to the corresponding target vector.  Returns (0.0, 1.0) when fewer than 2 pairs.
    """
    if len(pairs) < 2:
        return 0.0, 1.0

    # Find the pair of pairs whose source points are furthest apart.
    best_dist = -1.0
    best_i, best_j = 0, 1
    for i in range(len(pairs)):
        for j in range(i + 1, len(pairs)):
            d = math.hypot(
                pairs[j][0][0] - pairs[i][0][0],
                pairs[j][0][1] - pairs[i][0][1],
            )
            if d > best_dist:
                best_dist = d
                best_i, best_j = i, j

    if best_dist < 1e-9:
        # Source points are coincident — can't estimate.
        return 0.0, 1.0

    src_a, src_b = pairs[best_i][0], pairs[best_j][0]
    tgt_a, tgt_b = pairs[best_i][1], pairs[best_j][1]

    src_vec = (src_b[0] - src_a[0], src_b[1] - src_a[1])
    tgt_vec = (tgt_b[0] - tgt_a[0], tgt_b[1] - tgt_a[1])

    src_len = math.hypot(*src_vec)
    tgt_len = math.hypot(*tgt_vec)

    scale = tgt_len / src_len if src_len > 1e-9 else 1.0
    src_angle = math.atan2(src_vec[1], src_vec[0])
    tgt_angle = math.atan2(tgt_vec[1], tgt_vec[0])
    rotation_rad = tgt_angle - src_angle
    # Wrap to (-π, π].
    while rotation_rad > math.pi:
        rotation_rad -= 2 * math.pi
    while rotation_rad <= -math.pi:
        rotation_rad += 2 * math.pi

    return rotation_rad, scale


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------


def estimate_grid_transform(
    source: Sequence[GridFiducial],
    target: Sequence[GridFiducial],
    *,
    residual_warn_m: float = 1.0,
    residual_fail_m: float = 3.0,
    rot_tol_rad: float = 0.02,
    scale_tol: float = 0.02,
) -> GridTransform:
    """Estimate the rigid transform from *source* revision to *target* revision.

    Algorithm
    ---------
    1. Match fiducials by exact normalized label (present in both sets).
       If a label appears more than once on either side it is ambiguous — skip it.
       (Multiple inserts with the same bubble label indicate a duplicate/corrupt entity.)
    2. Translation = componentwise MEDIAN of per-pair deltas (robust to one bad bubble).
    3. Rotation + scale are ESTIMATED from the widest baseline (quality gates only).
    4. Residuals: apply translation to source points; compute per-pair distance to target.
    5. Quality: "good" / "degraded" / "failed" per spec thresholds.
    """

    # Build label → points index, detecting duplicates on each side.
    def _index(fiducials: Sequence[GridFiducial]) -> dict[str, tuple[float, float] | None]:
        idx: dict[str, tuple[float, float] | None] = {}
        for f in fiducials:
            lbl = _normalize_label(f.label)
            if lbl in idx:
                idx[lbl] = None  # mark ambiguous
            else:
                idx[lbl] = f.point
        return idx

    src_idx = _index(source)
    tgt_idx = _index(target)

    # Pairs: label present (and unambiguous) on both sides, in sorted label order.
    matched_labels: list[str] = sorted(
        lbl
        for lbl in src_idx
        if lbl in tgt_idx and src_idx[lbl] is not None and tgt_idx[lbl] is not None
    )

    pairs: list[tuple[tuple[float, float], tuple[float, float]]] = [
        (src_idx[lbl], tgt_idx[lbl])  # type: ignore[misc]  # both non-None by filter above
        for lbl in matched_labels
    ]

    n = len(pairs)

    # Fewer than 2 matches → can't even estimate.
    if n < 2:
        return GridTransform(
            dx=0.0,
            dy=0.0,
            rotation_rad=0.0,
            scale=1.0,
            matched_labels=tuple(matched_labels),
            matched_count=n,
            max_residual_m=0.0,
            median_residual_m=0.0,
            quality="failed",
        )

    # Step 2: robust median translation.
    dx_samples = [tgt[0] - src[0] for src, tgt in pairs]
    dy_samples = [tgt[1] - src[1] for src, tgt in pairs]
    dx = _median(dx_samples)
    dy = _median(dy_samples)

    # Step 3: rotation + scale estimate (quality gates only).
    rotation_rad, scale = _estimate_rotation_scale(pairs)

    # Step 4: residuals after applying translation.
    residuals = [math.hypot((src[0] + dx) - tgt[0], (src[1] + dy) - tgt[1]) for src, tgt in pairs]
    max_residual = max(residuals)
    median_residual = _median(residuals)

    # Step 5: quality classification.
    rot_ok = abs(rotation_rad) <= rot_tol_rad
    scale_ok = abs(scale - 1.0) <= scale_tol

    if n < 2 or median_residual > residual_fail_m:
        quality: Literal["good", "degraded", "failed"] = "failed"
    elif n >= 3 and median_residual <= residual_warn_m and rot_ok and scale_ok:
        quality = "good"
    else:
        quality = "degraded"

    return GridTransform(
        dx=dx,
        dy=dy,
        rotation_rad=rotation_rad,
        scale=scale,
        matched_labels=tuple(matched_labels),
        matched_count=n,
        max_residual_m=max_residual,
        median_residual_m=median_residual,
        quality=quality,
    )
