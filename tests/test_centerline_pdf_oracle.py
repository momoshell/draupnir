"""Real-PDF oracle regression for the PDF raster centerline producer.

Loads page 0 of M-540003.pdf (a 1:50 mechanical plantroom PDF), extracts all
``2d71ff``-coloured line segments via pymupdf, calls ``pdf_centerlines``, and
asserts that the resulting skeleton length converted to metres falls within the
[0.85, 0.98] band of the 244 m oracle measurement.

The test is SKIPPED if the PDF is not present on the local filesystem.  It is
the ground-truth tuning loop: the producer constants must keep this test green.

Scale context: 1 PDF point = (1/72 inch) * 0.0254 m * 50 (sheet scale) = 0.017638889 m.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_PDF_PATH = Path(
    "/Users/x/Documents/Welbeck Oxford/Electircal/Plantroom/14784-RAM-WO-00-DR-M-540003.pdf"
)
_TARGET_COLOUR = "2d71ff"
_ORACLE_M = 244.0
_M_PER_PT_AT_1_50: float = 0.017638889  # 1pt * (1/72 in) * 0.0254 m/in * 50

# [0.85, 0.98] x 244 m
_LO_M = 0.85 * _ORACLE_M  # 207.4 m
_HI_M = 0.98 * _ORACLE_M  # 239.1 m

# Equivalent DU (point) band
_LO_DU = _LO_M / _M_PER_PT_AT_1_50
_HI_DU = _HI_M / _M_PER_PT_AT_1_50

pytestmark = pytest.mark.skipif(
    not _PDF_PATH.exists(),
    reason=f"Real PDF not present at {_PDF_PATH}",
)


def _extract_segments(
    pdf_path: Path,
    colour_hex: str,
) -> tuple[dict[str, dict[str, list[float]]], list[str]]:
    """Return (geometry_by_entity_id, entity_ids) for line segments of the given colour.

    Iterates page 0 drawings via pymupdf.  Each ``"l"`` item whose parent
    drawing has the matching stroke colour is registered as a separate entity.
    Colour is matched by converting the (R, G, B) float triple to a 6-char hex
    string using round-to-nearest (not int-truncation) to avoid float drift.
    """
    import fitz  # pymupdf

    doc = fitz.open(str(pdf_path))
    page = doc[0]

    geometry: dict[str, dict[str, list[float]]] = {}
    entity_ids: list[str] = []

    for draw_idx, drawing in enumerate(page.get_drawings()):
        color = drawing.get("color")
        if color is None:
            continue
        r, g, b = color
        hex_col = f"{round(r * 255):02x}{round(g * 255):02x}{round(b * 255):02x}"
        if hex_col != colour_hex:
            continue
        for item_idx, item in enumerate(drawing.get("items", [])):
            if item[0] != "l":
                continue
            p1, p2 = item[1], item[2]
            eid = f"d{draw_idx}_i{item_idx}"
            geometry[eid] = {
                "start": [float(p1.x), float(p1.y)],
                "end": [float(p2.x), float(p2.y)],
            }
            entity_ids.append(eid)

    return geometry, entity_ids


@pytest.fixture(scope="module")
def real_pdf_segments() -> tuple[dict[str, dict[str, list[float]]], list[str]]:
    """Load and cache the real PDF segment extraction for the module."""
    return _extract_segments(_PDF_PATH, _TARGET_COLOUR)


def test_oracle_segment_count(
    real_pdf_segments: tuple[dict[str, dict[str, list[float]]], list[str]],
) -> None:
    """Sanity: the real PDF must yield at least 50 000 line segments for 2d71ff."""
    _, entity_ids = real_pdf_segments
    assert len(entity_ids) >= 50_000, (
        f"Expected >=50000 segments for colour {_TARGET_COLOUR}, got {len(entity_ids)}"
    )


def test_oracle_skeleton_length_in_band(
    real_pdf_segments: tuple[dict[str, dict[str, list[float]]], list[str]],
) -> None:
    """Real M-540003 2d71ff group: skeleton_length_du in [_LO_DU, _HI_DU].

    Arrange: extract all 2d71ff line segments from page 0 as entity geometry.
    Act:     call pdf_centerlines with a single RunGroup containing all entity IDs.
    Assert:
      - length_du is in the DU band corresponding to [207, 239] m at 1:50.
      - length_du converted to metres is in [207.0, 239.0] m.

    Tolerance band is [0.85, 0.98] x 244 m oracle — the ~0.93x systematic
    undercount of the raster-skeleton algo is inside this band.
    """
    from app.ingestion.centerline_pdf import pdf_centerlines
    from app.interpretation.routed_runs import RunGroup

    geometry_by_entity_id, entity_ids = real_pdf_segments
    assert entity_ids, f"No segments found for colour {_TARGET_COLOUR} in {_PDF_PATH}"

    group = RunGroup(
        layer_ref=None,
        colour_key=_TARGET_COLOUR,
        colour_index=None,
        colour_rgb=None,
        status="unknown",
        discipline=None,
        basis="unresolved",
        source_layers=(),
        confidence=None,
        competing_disciplines=(),
        entity_ids=tuple(entity_ids),
    )

    results = pdf_centerlines([group], geometry_by_entity_id)
    assert len(results) == 1
    cl = results[0]

    length_du = cl.geometry.length_du
    length_m = length_du * _M_PER_PT_AT_1_50
    ratio = length_m / _ORACLE_M

    assert _LO_DU <= length_du <= _HI_DU, (
        f"length_du={length_du:.0f} pt outside [{_LO_DU:.0f}, {_HI_DU:.0f}] pt "
        f"({length_m:.1f} m, ratio={ratio:.3f}x; oracle={_ORACLE_M} m)"
    )
    assert _LO_M <= length_m <= _HI_M, (
        f"length_m={length_m:.1f} m outside [{_LO_M:.1f}, {_HI_M:.1f}] m "
        f"(ratio={ratio:.3f}x; oracle={_ORACLE_M} m)"
    )


def test_oracle_produces_polylines(
    real_pdf_segments: tuple[dict[str, dict[str, list[float]]], list[str]],
) -> None:
    """pdf_centerlines on real data yields at least one polyline in the geometry."""
    from app.ingestion.centerline_pdf import pdf_centerlines
    from app.interpretation.routed_runs import RunGroup

    geometry_by_entity_id, entity_ids = real_pdf_segments
    group = RunGroup(
        layer_ref=None,
        colour_key=_TARGET_COLOUR,
        colour_index=None,
        colour_rgb=None,
        status="unknown",
        discipline=None,
        basis="unresolved",
        source_layers=(),
        confidence=None,
        competing_disciplines=(),
        entity_ids=tuple(entity_ids),
    )

    results = pdf_centerlines([group], geometry_by_entity_id)
    cl = results[0]

    assert len(cl.geometry.polylines) >= 1, (
        "Expected at least one polyline in the centerline output"
    )
    # Each polyline must have at least 2 points (a segment).
    for pl in cl.geometry.polylines:
        assert len(pl) >= 2, f"Polyline with <2 points: {pl}"


def test_oracle_determinism(
    real_pdf_segments: tuple[dict[str, dict[str, list[float]]], list[str]],
) -> None:
    """Two calls on real data produce identical length_du (determinism guard)."""
    from app.ingestion.centerline_pdf import pdf_centerlines
    from app.interpretation.routed_runs import RunGroup

    geometry_by_entity_id, entity_ids = real_pdf_segments
    group = RunGroup(
        layer_ref=None,
        colour_key=_TARGET_COLOUR,
        colour_index=None,
        colour_rgb=None,
        status="unknown",
        discipline=None,
        basis="unresolved",
        source_layers=(),
        confidence=None,
        competing_disciplines=(),
        entity_ids=tuple(entity_ids),
    )

    r1 = pdf_centerlines([group], geometry_by_entity_id)
    r2 = pdf_centerlines([group], geometry_by_entity_id)
    assert r1[0].geometry.length_du == r2[0].geometry.length_du
