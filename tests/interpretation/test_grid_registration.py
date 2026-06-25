"""Synthetic unit tests for grid_registration (issue #703).

All geometry is in metres.  No DB, no ORM, no FastAPI imports.
Covers: import purity, estimate_grid_transform correctness, robustness, edge cases,
determinism, label normalisation, apply().
"""

from __future__ import annotations

import importlib
import math
import random
from pathlib import Path

from app.interpretation.grid_registration import (
    GridFiducial,
    GridTransform,
    estimate_grid_transform,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _f(label: str, x: float, y: float) -> GridFiducial:
    return GridFiducial(label=label, point=(x, y))


def _shifted(fiducials: list[GridFiducial], dx: float, dy: float) -> list[GridFiducial]:
    # GridFiducial.point is a plain tuple[float, float]; unpack by index.
    return [
        GridFiducial(label=f.label, point=(f.point[0] + dx, f.point[1] + dy)) for f in fiducials
    ]


# ---------------------------------------------------------------------------
# (0) Import purity — grid_registration.py must not import heavy libs
# ---------------------------------------------------------------------------


def test_import_purity_grid_registration() -> None:
    mod = importlib.import_module("app.interpretation.grid_registration")
    source_file = getattr(mod, "__file__", "") or ""
    # Scan only lines that are actual import statements, not docstring prose.
    import_lines = [
        line
        for line in Path(source_file).read_text().splitlines()
        if line.lstrip().startswith(("import ", "from "))
    ]
    src = "\n".join(import_lines)
    forbidden = ("sqlalchemy", "fastapi", "numpy", "cv2", "shapely")
    for pkg in forbidden:
        assert pkg not in src, f"grid_registration.py must not import {pkg}"


# ---------------------------------------------------------------------------
# (1) Basic: known offset recovered
# ---------------------------------------------------------------------------


def test_known_offset_recovered() -> None:
    src = [
        _f("4", 10.0, 5.0),
        _f("5", 20.0, 5.0),
        _f("6", 30.0, 5.0),
        _f("B", 10.0, 15.0),
        _f("C", 20.0, 15.0),
    ]
    expected_dx, expected_dy = 27.75, 0.05
    tgt = _shifted(src, expected_dx, expected_dy)

    result = estimate_grid_transform(src, tgt)

    assert result.matched_count == 5
    assert result.matched_labels == ("4", "5", "6", "B", "C")
    assert abs(result.dx - expected_dx) < 1e-9
    assert abs(result.dy - expected_dy) < 1e-9
    assert result.max_residual_m < 1e-6
    assert result.median_residual_m < 1e-6
    assert abs(result.rotation_rad) < 0.001
    assert abs(result.scale - 1.0) < 0.001
    assert result.quality == "good"


# ---------------------------------------------------------------------------
# (2) Robustness: one mislabelled/outlier pair among many
# ---------------------------------------------------------------------------


def test_outlier_pair_median_robust() -> None:
    """Median translation is correct even when one pair has a large outlier delta."""
    dx_true, dy_true = 28.0, 0.0
    # 6 correctly shifted pairs.
    src = [
        _f("4", 10.0, 5.0),
        _f("5", 20.0, 5.0),
        _f("6", 30.0, 5.0),
        _f("7", 40.0, 5.0),
        _f("B", 10.0, 15.0),
        _f("C", 20.0, 15.0),
    ]
    tgt = _shifted(src, dx_true, dy_true)
    # Replace one target point with a wildly wrong value (simulates a mislabelled bubble).
    tgt_mutated = list(tgt)
    tgt_mutated[2] = GridFiducial(
        label="6", point=(tgt_mutated[2].point[0] + 50.0, tgt_mutated[2].point[1])
    )

    result = estimate_grid_transform(src, tgt_mutated)

    # Median over 6 dx-samples: [28, 28, 78, 28, 28, 28] → sorted [28,28,28,28,28,78] → median 28.
    assert abs(result.dx - dx_true) < 1e-9
    assert abs(result.dy - dy_true) < 1e-9
    assert result.matched_count == 6


# ---------------------------------------------------------------------------
# (3) Failed: fewer than 2 matched labels
# ---------------------------------------------------------------------------


def test_no_matches_failed() -> None:
    src = [_f("A", 0.0, 0.0)]
    tgt = [_f("B", 10.0, 10.0)]  # no label overlap

    result = estimate_grid_transform(src, tgt)

    assert result.quality == "failed"
    assert result.matched_count == 0


def test_one_match_failed() -> None:
    src = [_f("4", 0.0, 0.0), _f("5", 5.0, 0.0)]
    tgt = [_f("4", 28.0, 0.0), _f("X", 100.0, 0.0)]  # only "4" matches

    result = estimate_grid_transform(src, tgt)

    assert result.quality == "failed"
    assert result.matched_count == 1


# ---------------------------------------------------------------------------
# (4) Rotated/scaled set beyond tolerance → quality not "good", apply() still translation-only
# ---------------------------------------------------------------------------


def test_rotation_beyond_tol_degraded() -> None:
    """When the matched pairs imply a large rotation, quality must be 'degraded' (not 'good')."""
    # Two source points along the x-axis; target rotated 10° (>> 0.02 rad tolerance).
    angle = math.radians(10)
    src = [
        _f("4", 0.0, 0.0),
        _f("5", 10.0, 0.0),
        _f("6", 20.0, 0.0),
    ]
    dx_base = 28.0

    # Apply a rotation about the first target point to manufacture a realistic-looking target.
    def _rotate(x: float, y: float) -> tuple[float, float]:
        return (
            x * math.cos(angle) - y * math.sin(angle) + dx_base,
            x * math.sin(angle) + y * math.cos(angle),
        )

    tgt = [GridFiducial(label=f.label, point=_rotate(*f.point)) for f in src]

    result = estimate_grid_transform(src, tgt, rot_tol_rad=0.02)

    assert result.quality != "good"
    assert abs(result.rotation_rad) > 0.02

    # apply() must still be translation-only (not composing rotation).
    pt = (0.0, 0.0)
    applied = result.apply(pt)
    assert applied == (pt[0] + result.dx, pt[1] + result.dy)


def test_scale_beyond_tol_degraded() -> None:
    """When matched pairs imply scale != 1, quality is at most 'degraded'."""
    src = [
        _f("4", 0.0, 0.0),
        _f("5", 10.0, 0.0),
        _f("6", 20.0, 0.0),
    ]
    # Scale target by 1.1 (10 % > scale_tol=0.02).
    scale = 1.1
    tgt = [
        GridFiducial(label=f.label, point=(f.point[0] * scale + 28.0, f.point[1] * scale))
        for f in src
    ]

    result = estimate_grid_transform(src, tgt, scale_tol=0.02)

    assert result.quality != "good"
    assert abs(result.scale - 1.0) > 0.02


# ---------------------------------------------------------------------------
# (5) Determinism: shuffling source/target order produces identical GridTransform
# ---------------------------------------------------------------------------


def test_determinism_shuffle() -> None:
    labels = ["4", "5", "6", "7", "8", "B", "C", "D"]
    src = [_f(lbl, float(i * 5), 0.0) for i, lbl in enumerate(labels)]
    tgt = _shifted(src, 28.3, 0.2)

    def _run(seed: int) -> GridTransform:
        s = list(src)
        t = list(tgt)
        rng2 = random.Random(seed)
        rng2.shuffle(s)
        rng2.shuffle(t)
        return estimate_grid_transform(s, t)

    r1 = _run(1)
    r2 = _run(2)
    r3 = _run(99)

    assert r1 == r2 == r3


# ---------------------------------------------------------------------------
# (6) apply() translates correctly
# ---------------------------------------------------------------------------


def test_apply_translates() -> None:
    src = [_f("4", 0.0, 0.0), _f("5", 10.0, 0.0), _f("6", 20.0, 0.0)]
    tgt = _shifted(src, 28.0, 1.5)

    result = estimate_grid_transform(src, tgt)

    pt = (5.0, 3.0)
    applied = result.apply(pt)
    assert abs(applied[0] - (5.0 + result.dx)) < 1e-9
    assert abs(applied[1] - (3.0 + result.dy)) < 1e-9


# ---------------------------------------------------------------------------
# (7) Label normalization: "b" matches "B ", " b" etc.
# ---------------------------------------------------------------------------


def test_label_normalization_case_whitespace() -> None:
    """Labels are normalized (stripped + upper-cased) before matching."""
    src = [
        GridFiducial(label="b", point=(0.0, 0.0)),  # lower-case
        GridFiducial(label="C ", point=(10.0, 0.0)),  # trailing space
        GridFiducial(label=" D", point=(20.0, 0.0)),  # leading space
    ]
    # Target uses different case/spacing — should still match.
    tgt = [
        GridFiducial(label="B", point=(28.0, 0.0)),
        GridFiducial(label="c", point=(38.0, 0.0)),
        GridFiducial(label="d ", point=(48.0, 0.0)),
    ]
    result = estimate_grid_transform(src, tgt)

    assert result.matched_count == 3
    assert set(result.matched_labels) == {"B", "C", "D"}
    assert abs(result.dx - 28.0) < 1e-9


# ---------------------------------------------------------------------------
# (8) Ambiguous (duplicate) labels on one side are skipped
# ---------------------------------------------------------------------------


def test_ambiguous_label_skipped() -> None:
    """A label that appears twice on the source side is ambiguous and must not be matched."""
    src = [
        _f("4", 0.0, 0.0),
        _f("4", 1.0, 0.0),  # duplicate — ambiguous
        _f("5", 10.0, 0.0),
        _f("6", 20.0, 0.0),
    ]
    tgt = [
        _f("4", 28.0, 0.0),
        _f("5", 38.0, 0.0),
        _f("6", 48.0, 0.0),
    ]
    result = estimate_grid_transform(src, tgt)

    # "4" is ambiguous on source → only "5" and "6" are matched.
    assert "4" not in result.matched_labels
    assert result.matched_count == 2


# ---------------------------------------------------------------------------
# (9) Two matched pairs → quality not "good" (need ≥3 for "good")
# ---------------------------------------------------------------------------


def test_two_matches_degraded_not_good() -> None:
    src = [_f("4", 0.0, 0.0), _f("5", 10.0, 0.0)]
    tgt = _shifted(src, 28.0, 0.0)

    result = estimate_grid_transform(src, tgt)

    assert result.matched_count == 2
    # matched_count < 3 so quality must be "degraded" (residual ~0 so not "failed").
    assert result.quality == "degraded"


# ---------------------------------------------------------------------------
# (10) Large residual → "failed"
# ---------------------------------------------------------------------------


def test_large_residual_failed() -> None:
    """When median residual exceeds residual_fail_m, quality is 'failed'."""
    src = [_f("4", 0.0, 0.0), _f("5", 10.0, 0.0), _f("6", 20.0, 0.0)]
    # Target points scattered at wildly different offsets per label.
    tgt = [
        _f("4", 1000.0, 500.0),
        _f("5", 0.0, 0.0),
        _f("6", -500.0, 200.0),
    ]
    result = estimate_grid_transform(src, tgt, residual_fail_m=3.0)

    assert result.quality == "failed"
