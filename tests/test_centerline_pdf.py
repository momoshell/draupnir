"""Unit tests for app.ingestion.centerline_pdf.

All tests are pure-algorithm: no DB, no real PDF files read.

Synthetic double-line oracle
----------------------------
Two parallel horizontal segments of length L offset by D (pipe diameter proxy)
should collapse after CLOSE + skeletonize to ONE midline of length ~L.  The
tolerance band (0.85, 1.15) * L is conservative enough to accommodate pixel
discretization but tight enough to detect a doubled-length regression.
"""

from __future__ import annotations

import builtins
import sys

import pytest

# The producer lazy-imports cv2/skimage inside pdf_centerlines (ADR-008 read-path
# boundary). These unit tests CALL it, so they need the ingestion extra present;
# skip the module honestly where it is absent rather than erroring at call time.
pytest.importorskip("cv2")
pytest.importorskip("skimage")

from app.ingestion.centerline_contract import CURRENT_ALGO_VERSION
from app.ingestion.centerline_pdf import (
    _PDF_RASTER_PARAMS_HASH,
    pdf_centerlines,
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
# Test 1: synthetic double-line oracle
# ---------------------------------------------------------------------------


def test_double_line_collapses_to_single_midline() -> None:
    """Two parallel segments offset by D=3 pt collapse to one midline of ~L.

    Arrange: two horizontal lines of length L=500 pt at y=0 and y=3 pt.
    The offset of 3 pt matches _DEFAULT_DIAMETER_DU (PDF point-scale, 1:50
    sheet wall gap) so the CLOSE kernel bridges this gap.
    Act:     pdf_centerlines.
    Assert:  length_du within (0.85, 1.15) * L (NOT 2*L).

    This verifies the CLOSE + skeletonize collapse — the core correctness
    property of the raster producer.
    """
    seg_length = 500.0
    offset = 3.0  # PDF-realistic: wall gap ~3 pt at 1:50 (matches _DEFAULT_DIAMETER_DU)
    geom = {
        "top": _line_geom(0.0, 0.0, seg_length, 0.0),
        "bot": _line_geom(0.0, offset, seg_length, offset),
    }
    entity_ids: tuple[str, ...] = ("top", "bot")
    group = _make_group(entity_ids, layer_ref="M-PIPE", colour_key="red")

    results = pdf_centerlines([group], geom)
    assert len(results) == 1
    cl = results[0]

    assert cl.producer_kind == "pdf_raster"
    lo = 0.85 * seg_length
    hi = 1.15 * seg_length
    assert lo <= cl.geometry.length_du <= hi, (
        f"length_du={cl.geometry.length_du:.1f} outside [{lo:.0f}, {hi:.0f}] "
        f"(expected ~{seg_length}; ~2*seg_length would be a regression)"
    )
    # Must have at least one polyline (skeleton exists).
    assert len(cl.geometry.polylines) >= 1


# ---------------------------------------------------------------------------
# Test 2: determinism
# ---------------------------------------------------------------------------


def test_determinism_same_input() -> None:
    """Two calls with identical input produce identical length_du and polylines."""
    seg_length = 300.0
    offset = 3.0  # PDF-realistic wall gap in points
    geom = {
        "a": _line_geom(0.0, 0.0, seg_length, 0.0),
        "b": _line_geom(0.0, offset, seg_length, offset),
    }
    entity_ids: tuple[str, ...] = ("a", "b")
    group = _make_group(entity_ids, layer_ref="PIPE", colour_key="blue")

    r1 = pdf_centerlines([group], geom)
    r2 = pdf_centerlines([group], geom)

    assert r1[0].geometry.length_du == r2[0].geometry.length_du
    assert r1[0].geometry.polylines == r2[0].geometry.polylines


# ---------------------------------------------------------------------------
# Test 3: per-group isolation
# ---------------------------------------------------------------------------


def test_multiple_groups_one_centerline_each() -> None:
    """Multiple groups each produce exactly one Centerline in input order.

    Each Centerline carries its own layer_ref and colour_key.
    """
    seg_length = 200.0
    offset = 3.0  # PDF-realistic wall gap in points
    geom = {
        "g1_a": _line_geom(0.0, 0.0, seg_length, 0.0),
        "g1_b": _line_geom(0.0, offset, seg_length, offset),
        "g2_a": _line_geom(1000.0, 0.0, 1000.0 + seg_length, 0.0),
        "g2_b": _line_geom(1000.0, offset, 1000.0 + seg_length, offset),
    }
    groups = [
        _make_group(("g1_a", "g1_b"), layer_ref="LAYER_A", colour_key="red"),
        _make_group(("g2_a", "g2_b"), layer_ref="LAYER_B", colour_key="green"),
    ]

    results = pdf_centerlines(groups, geom)

    assert len(results) == 2
    assert results[0].layer_ref == "LAYER_A"
    assert results[0].colour_key == "red"
    assert results[1].layer_ref == "LAYER_B"
    assert results[1].colour_key == "green"
    # Both should have non-zero length (paired double lines).
    assert results[0].geometry.length_du > 0.0
    assert results[1].geometry.length_du > 0.0


def test_group_order_preserved() -> None:
    """Output order matches input run_groups order."""
    geom = {
        "e1": _line_geom(0.0, 0.0, 100.0, 0.0),
        "e2": _line_geom(500.0, 0.0, 600.0, 0.0),
    }
    groups = [
        _make_group(("e1",), layer_ref="FIRST"),
        _make_group(("e2",), layer_ref="SECOND"),
    ]
    results = pdf_centerlines(groups, geom)
    assert results[0].layer_ref == "FIRST"
    assert results[1].layer_ref == "SECOND"


# ---------------------------------------------------------------------------
# Test 4: degenerate inputs
# ---------------------------------------------------------------------------


def test_empty_group_returns_zero_length_no_raise() -> None:
    """Empty entity_ids -> length_du=0.0, polylines=(), no exception."""
    group = _make_group((), layer_ref="EMPTY")
    results = pdf_centerlines([group], {})
    assert len(results) == 1
    cl = results[0]
    assert cl.geometry.length_du == 0.0
    assert cl.geometry.polylines == ()
    assert cl.producer_kind == "pdf_raster"


def test_missing_geometry_returns_zero_length_no_raise() -> None:
    """Entity IDs with no geometry entries -> length_du=0.0, no exception."""
    group = _make_group(("ghost_1", "ghost_2"), layer_ref="GHOST")
    results = pdf_centerlines([group], {})
    cl = results[0]
    assert cl.geometry.length_du == 0.0
    assert cl.geometry.polylines == ()


def test_zero_length_line_returns_zero_no_raise() -> None:
    """A degenerate zero-length line is silently skipped; length_du=0.0."""
    geom = {"zero": _line_geom(5.0, 5.0, 5.0, 5.0)}
    group = _make_group(("zero",), layer_ref="DEGENERATE")
    results = pdf_centerlines([group], geom)
    assert results[0].geometry.length_du == 0.0
    assert results[0].geometry.polylines == ()


def test_empty_run_groups_returns_empty_list() -> None:
    """pdf_centerlines([]) -> []."""
    results = pdf_centerlines([], {})
    assert results == []


# ---------------------------------------------------------------------------
# Test 5: hash stability
# ---------------------------------------------------------------------------


def test_raster_params_hash_is_sha256_hex() -> None:
    """_PDF_RASTER_PARAMS_HASH is a 64-char SHA-256 hex digest."""
    assert len(_PDF_RASTER_PARAMS_HASH) == 64
    # Must be valid hex.
    int(_PDF_RASTER_PARAMS_HASH, 16)


def test_raster_params_hash_stable_across_calls() -> None:
    """Two pdf_centerlines calls emit the same raster_params_hash."""
    geom = {"e1": _line_geom(0.0, 0.0, 100.0, 0.0)}
    group = _make_group(("e1",), layer_ref="PIPE")
    r1 = pdf_centerlines([group], geom)
    r2 = pdf_centerlines([group], geom)
    assert r1[0].raster_params_hash == r2[0].raster_params_hash
    assert r1[0].raster_params_hash == _PDF_RASTER_PARAMS_HASH


def test_algo_version_matches_contract() -> None:
    """algo_version must equal CURRENT_ALGO_VERSION from the contract."""
    geom = {"e1": _line_geom(0.0, 0.0, 100.0, 0.0)}
    group = _make_group(("e1",), layer_ref="PIPE")
    cl = pdf_centerlines([group], geom)[0]
    assert cl.algo_version == CURRENT_ALGO_VERSION


# ---------------------------------------------------------------------------
# Test 6: honest-degrade — cv2 absent raises, does NOT return zero rows
# ---------------------------------------------------------------------------


def test_honest_degrade_raises_when_cv2_absent() -> None:
    """pdf_centerlines raises ImportError/RuntimeError when cv2 is unavailable.

    Simulates a missing cv2 by inserting a sentinel into sys.modules, then
    calling pdf_centerlines.  The call must raise — a silent zero-row return
    would write bogus "materialised" rows.

    The test restores sys.modules after the check.
    """
    geom = {
        "top": _line_geom(0.0, 0.0, 100.0, 0.0),
        "bot": _line_geom(0.0, 3.0, 100.0, 3.0),
    }
    group = _make_group(("top", "bot"), layer_ref="PIPE")

    real_import = builtins.__import__

    def _mock_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "cv2" or name.startswith("cv2."):
            raise ImportError("cv2 intentionally absent")
        return real_import(name, *args, **kwargs)  # type: ignore[arg-type]

    builtins.__import__ = _mock_import  # type: ignore[assignment]
    # Also remove cv2 from sys.modules so the lazy import path hits our mock.
    cv2_backup = sys.modules.pop("cv2", None)
    try:
        with pytest.raises((ImportError, RuntimeError)):
            pdf_centerlines([group], geom)
    finally:
        builtins.__import__ = real_import
        if cv2_backup is not None:
            sys.modules["cv2"] = cv2_backup
