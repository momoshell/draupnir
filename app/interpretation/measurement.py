"""Real-world length measurement primitive (issue #608, P0).

Converts a drawn geometric length (drawing units) to a real-world length in
metres using the scale conversion_factor, confidence-gated per ADR-004:
real metres ONLY when the scale supports it, otherwise the drawn value with an
explicit flag.

Pure module: no I/O, no DB, no ORM, no adapter imports.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

Point = tuple[float, float]


@dataclass(frozen=True, slots=True)
class ScaleContext:
    """The scale signals needed to decide if a drawing length converts to real metres."""

    conversion_factor: float | None  # drawing-units -> metres (e.g. 0.001 for mm)
    real_world_available: bool  # from the scale surface (#557)
    contradicted: bool = False  # units_contradicted (#558)
    units_confidence: str = "unknown"  # declared|confirmed|inferred|unknown (echoed through)


@dataclass(frozen=True, slots=True)
class Measurement:
    drawing_length: float  # raw drawing-unit length (always present, >=0)
    real_length_m: float | None  # metres when scale supports it, else None
    basis: Literal["real_world", "drawing_units_only"]
    units_confidence: str  # echoed from ScaleContext
    conversion_factor: float | None  # echoed (the factor used, or None)


def _positive_finite(x: object) -> bool:
    """Return True only for a real, positive, finite number (not bool)."""
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x) and x > 0


def path_length(points: Sequence[Point]) -> float:
    """Sum of euclidean segment lengths over consecutive points.

    Returns 0.0 for fewer than 2 points. Non-finite coordinates in any
    segment cause that segment to contribute 0 rather than raising.
    """
    if len(points) < 2:
        return 0.0

    total = 0.0
    for i in range(len(points) - 1):
        x0, y0 = points[i]
        x1, y1 = points[i + 1]
        dx = x1 - x0
        dy = y1 - y0
        sq = dx * dx + dy * dy
        if math.isfinite(sq):
            total += math.sqrt(sq)
        # non-finite segment contributes 0 -- silent, never raises
    return total


def measure_length(drawing_length: float, scale: ScaleContext) -> Measurement:
    """Convert a drawing-unit length to a Measurement using the given ScaleContext.

    Gate: real metres are emitted ONLY when real_world_available is True,
    contradicted is False, and conversion_factor is a positive finite number.
    Otherwise drawing_units_only is returned to avoid advertising a metres
    value that could be 10-1000x wrong (ADR-004).

    drawing_length is coerced to a finite >=0 float; non-finite or negative
    values are treated as 0.0. Never raises.
    """
    # Coerce drawing_length to a safe value.
    if not math.isfinite(drawing_length) or drawing_length < 0:
        drawing_length = 0.0

    available = (
        scale.real_world_available
        and not scale.contradicted
        and _positive_finite(scale.conversion_factor)
    )

    if available:
        factor = scale.conversion_factor  # known positive-finite by gate above
        return Measurement(
            drawing_length=drawing_length,
            real_length_m=drawing_length * factor,  # type: ignore[operator]
            basis="real_world",
            units_confidence=scale.units_confidence,
            conversion_factor=factor,
        )

    return Measurement(
        drawing_length=drawing_length,
        real_length_m=None,
        basis="drawing_units_only",
        units_confidence=scale.units_confidence,
        conversion_factor=scale.conversion_factor,
    )
