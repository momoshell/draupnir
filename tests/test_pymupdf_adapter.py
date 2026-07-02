"""Regression tests for PyMuPDF adapter payload robustness (real-PDF edge cases)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from app.core.config import settings
from app.ingestion.adapters import pymupdf as pdf_adapter
from app.ingestion.adapters._process_limits import ParserProcessLimits
from app.ingestion.contracts import (
    AdapterExecutionOptions,
    AdapterSource,
    AdapterWarning,
    InputFamily,
    JSONValue,
    UploadFormat,
)


class _FakePoint:
    def __init__(self, x: float, y: float) -> None:
        self.x = x
        self.y = y


class _FakeQuad:
    """Mimics a PyMuPDF Quad (four corner points), as emitted for 'qu' draw items."""

    def __init__(self) -> None:
        self.ul = _FakePoint(0.0, 0.0)
        self.ur = _FakePoint(2.0, 0.0)
        self.lr = _FakePoint(2.0, 1.0)
        self.ll = _FakePoint(0.0, 1.0)


def test_entity_properties_tolerates_present_but_none_fields() -> None:
    # Fill-only paths carry width=None; some drawings carry lineCap/seqno=None. dict.get(k, default)
    # returns the stored None, so these must not crash float()/int()/iteration.
    drawing = {
        "type": None,
        "width": None,
        "color": None,
        "fill": None,
        "lineCap": None,
        "lineJoin": None,
        "seqno": None,
        "dashes": None,
    }
    props = pdf_adapter._entity_properties(drawing, closed=True, rect_like=False)
    assert props["stroke_width"] == 0.0
    assert props["line_cap"] == ()
    assert props["sequence_number"] == 0
    assert props["path_type"] == "unknown"
    assert props["dashes"] == ""


def test_normalize_source_value_handles_quad() -> None:
    normalized, representable = pdf_adapter._normalize_source_value(_FakeQuad())
    assert representable is True
    assert isinstance(normalized, dict)
    assert set(normalized.keys()) == {"ul", "ur", "lr", "ll"}
    assert normalized["ur"] == {"x": 2.0, "y": 0.0}
    # And a payload containing a Quad is now hashable (previously raised).
    assert pdf_adapter._normalized_source_hash({"quad": _FakeQuad()}) is not None


def test_bbox_points_from_quad() -> None:
    points = pdf_adapter._bbox_points_from_value(_FakeQuad())
    assert (0.0, 0.0) in points
    assert (2.0, 1.0) in points


def _ok_envelope() -> dict[str, object]:
    return {
        "status": "ok",
        "canonical": {"entities": [], "metadata": {}},
        "warnings": [{"code": "x", "message": "y"}],
    }


async def test_extract_in_current_process_prepends_isolation_warning(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr(pdf_adapter, "_extract_in_child_process", lambda _request: _ok_envelope())
    request = pdf_adapter._ProcessExtractionRequest(
        file_path="/tmp/x.pdf",
        upload_format=UploadFormat.PDF.value,
        timeout_seconds=None,
        limits=pdf_adapter._ExtractionLimits.from_settings(),
    )
    _canonical, warnings = await pdf_adapter._extract_in_current_process(request, reason="daemonic")
    assert warnings[0].code == "pymupdf_subprocess_isolation_unavailable"
    assert warnings[0].details == {"reason": "daemonic"}
    assert warnings[1].code == "x"  # original adapter warnings preserved after the caveat


async def test_extract_with_process_fails_closed_when_spawn_is_blocked(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    def _blocked(_request: object) -> object:
        raise AssertionError("daemonic processes are not allowed to have children")

    monkeypatch.setattr(pdf_adapter, "_start_extraction_process", _blocked)
    monkeypatch.setattr(pdf_adapter, "_extract_in_child_process", lambda _request: _ok_envelope())
    source = AdapterSource(
        file_path=Path("/tmp/x.pdf"),
        upload_format=UploadFormat.PDF,
        input_family=InputFamily.PDF_VECTOR,
    )
    with pytest.raises(pdf_adapter.PyMuPDFIsolationUnavailableError) as exc_info:
        await pdf_adapter._extract_with_process(source, AdapterExecutionOptions(timeout=None))
    assert str(exc_info.value) == (
        "PyMuPDF extraction requires subprocess isolation for untrusted PDF input."
    )
    assert exc_info.value.failure_reason == "isolation_unavailable"


def test_pen_signature_layer_name_is_deterministic_and_distinct() -> None:
    # Same pen -> same name; different colour or width -> different name. Content-derived,
    # so naming is stable regardless of encounter order. (Issue #413.)
    black_thin = pdf_adapter._pen_signature_layer_name({"color": (0.0, 0.0, 0.0), "width": 0.51})
    black_thin_again = pdf_adapter._pen_signature_layer_name(
        {"color": (0.0, 0.0, 0.0), "width": 0.51}
    )
    grey_thin = pdf_adapter._pen_signature_layer_name({"color": (0.92, 0.92, 0.92), "width": 0.51})
    black_thick = pdf_adapter._pen_signature_layer_name({"color": (0.0, 0.0, 0.0), "width": 1.0})

    assert black_thin == "pen-000000-w0.51"
    assert black_thin == black_thin_again
    assert grey_thin == "pen-ebebeb-w0.51"
    assert black_thick == "pen-000000-w1"
    assert len({black_thin, grey_thin, black_thick}) == 3


def test_pen_signature_layer_name_fill_and_colourless_fallback() -> None:
    # Fill-only paths (no stroke colour) key off the fill colour; fully colourless paths
    # fall back to the default layer name.
    assert (
        pdf_adapter._pen_signature_layer_name(
            {"color": None, "fill": (0.0, 1.0, 0.0), "width": 0.7}
        )
        == "pen-00ff00-w0.7"
    )
    assert (
        pdf_adapter._pen_signature_layer_name({"color": None, "fill": None, "width": 0.0})
        == "default-w0"
    )


def test_resolve_drawing_layer_honors_explicit_layer_field() -> None:
    # An explicit (OCG-style) layer name wins over the pen signature.
    assert (
        pdf_adapter._resolve_drawing_layer(
            {"layer": "E-LITE", "color": (0.0, 0.0, 0.0), "width": 0.5}
        )
        == "E-LITE"
    )


def test_populate_page_entities_assigns_pen_signature_layers() -> None:
    def line(color: object, width: float) -> dict[str, object]:
        return {
            "color": color,
            "width": width,
            "closePath": False,
            "items": [("l", _FakePoint(0.0, 0.0), _FakePoint(1.0, 1.0))],
        }

    drawings = [
        line((0.0, 0.0, 0.0), 0.5),  # black thin
        line((0.0, 0.0, 0.0), 0.5),  # same pen -> same layer
        line((0.92, 0.92, 0.92), 0.5),  # grey -> different layer
        line((0.0, 0.0, 0.0), 1.0),  # black thick -> different layer
    ]
    entities: list[dict[str, JSONValue]] = []
    warnings: list[AdapterWarning] = []
    layer_names: list[str] = []
    budget = pdf_adapter._ExtractionBudget(started_at=0.0, timeout_seconds=None)

    pdf_adapter._populate_page_entities(
        drawings,
        entities=entities,
        warnings=warnings,
        layer_names=layer_names,
        page_number=1,
        layout_name="page-1",
        options=AdapterExecutionOptions(timeout=None),
        budget=budget,
        entity_limit=100,
    )

    assert set(layer_names) == {"pen-000000-w0.5", "pen-ebebeb-w0.5", "pen-000000-w1"}
    assert entities, "expected at least one entity"
    assert all(entity["layer_ref"] in set(layer_names) for entity in entities)


def test_points_to_mm_conversion() -> None:
    # 72 pt = 1 inch = 25.4 mm
    assert pdf_adapter._points_to_mm(72.0) == 25.4
    assert pdf_adapter._points_to_mm(0.0) == 0.0


def test_match_sheet_size_is_orientation_agnostic_and_tolerant() -> None:
    # A0 = 841 x 1189 mm, matched regardless of orientation, within tolerance.
    assert pdf_adapter._match_sheet_size(1189.0, 841.0) == "A0"
    assert pdf_adapter._match_sheet_size(840.5, 1188.5) == "A0"  # within 3mm tolerance
    assert pdf_adapter._match_sheet_size(215.9, 279.4) == "ANSI A (Letter)"
    assert pdf_adapter._match_sheet_size(210.0, 350.0) is None  # near nothing standard


def test_paper_size_from_a0_mediabox_points() -> None:
    # 680003.pdf MediaBox: 3370.32 x 2383.92 pt == ISO A0.
    paper = pdf_adapter._paper_size(width_pt=3370.32, height_pt=2383.92, page_number=1)
    assert paper["name"] == "A0"
    assert paper["page_number"] == 1
    assert paper["width_mm"] == 1188.974
    assert paper["height_mm"] == 840.994


def _block(text: str, bbox: dict[str, float] | None = None) -> dict[str, object]:
    return {"text": text, "bbox": bbox or {"x_min": 0.0, "y_min": 0.0, "x_max": 1.0, "y_max": 1.0}}


def test_parse_scale_ratio_prefers_architectural_and_ignores_timestamps() -> None:
    blocks = [
        _block(
            "Project No:\nScale (@ A0):\nDrawn:",
            {"x_min": 3058, "y_min": 2262, "x_max": 3285, "y_max": 2273},
        ),
        _block(
            "1620014784\n28/01/25\nHK\n1:50",
            {"x_min": 3065, "y_min": 2272, "x_max": 3295, "y_max": 2290},
        ),
        _block("28/03/2025 17:52:20"),  # timestamp must not be read as a scale
    ]
    result = pdf_adapter._parse_scale_ratio(blocks)
    assert result is not None
    numerator, denominator, _source, confidence = result
    assert (numerator, denominator) == (1, 50)
    assert confidence == "high"  # a "Scale" label is present


def test_parse_scale_ratio_none_when_only_timestamp() -> None:
    assert pdf_adapter._parse_scale_ratio([_block("28/03/2025 17:52:20")]) is None
    assert pdf_adapter._parse_scale_ratio([_block("no ratios here")]) is None


def test_parse_scale_ratio_medium_confidence_without_label() -> None:
    result = pdf_adapter._parse_scale_ratio([_block("drawn at 1:100 nominal")])
    assert result is not None
    assert (result[0], result[1]) == (1, 100)
    assert result[3] == "medium"


def test_parse_real_world_unit_anchors_on_dimensions_not_levels() -> None:
    assert pdf_adapter._parse_real_world_unit(
        [_block("ALL DIMENSIONS ARE IN MILLIMETRES U.N.O.")]
    ) == ("millimeter", "ALL DIMENSIONS ARE IN MILLIMETRES U.N.O.")
    # "LEVELS ARE IN METRES" (datum) must not be taken as the plan unit.
    assert (
        pdf_adapter._parse_real_world_unit([_block("ALL LEVELS ARE IN METRES ABOVE DATUM")]) is None
    )
    assert pdf_adapter._parse_real_world_unit([_block("nothing relevant")]) is None


def test_derive_pdf_scale_full_title_block() -> None:
    blocks = [
        _block("DO NOT SCALE FROM THIS DRAWING."),
        _block("ALL DIMENSIONS ARE IN MILLIMETRES U.N.O."),
        _block("Scale (@ A0):", {"x_min": 3058, "y_min": 2262, "x_max": 3285, "y_max": 2273}),
        _block("HK\n1:50", {"x_min": 3065, "y_min": 2272, "x_max": 3295, "y_max": 2290}),
    ]
    scale = pdf_adapter._derive_pdf_scale(blocks)
    assert scale["status"] == "derived_from_text"
    assert scale["real_world_units"] is True
    assert scale["real_world_unit"] == "millimeter"
    assert scale["scale_ratio"] == {"numerator": 1, "denominator": 50, "text": "1:50"}
    assert scale["points_to_real"] == 17.638889  # 25.4/72 * 50
    assert scale["caveats"] == ("do_not_scale",)


def test_derive_pdf_scale_unconfirmed_when_no_scale_text() -> None:
    scale = pdf_adapter._derive_pdf_scale([_block("just a label")])
    assert scale["status"] == "unconfirmed"
    assert scale["real_world_units"] is False
    assert "scale_ratio" not in scale


def test_distinct_scale_ratios_dedupes_and_filters() -> None:
    # Same ratio in two blocks -> one distinct entry; a non-architectural ratio (3:4) is excluded.
    blocks = [
        _block("Scale 1:50"),
        _block("detail 1 : 50"),
        _block("aspect 3:4"),
        _block("28/03/2025 17:52:20"),
    ]
    assert pdf_adapter._distinct_scale_ratios(blocks) == [(1, 50)]


def test_derive_pdf_scale_ambiguous_when_multiple_distinct_ratios() -> None:
    """A genuinely multi-scale 'As indicated' sheet (1:50 + 1:100) must NOT pick one scale.

    Per ADR-004 (#636): a confident sheet-wide factor would measure every 1:100 view at 2x the
    true length. The detector degrades to an honest unknown instead — recording the candidates
    but emitting no points_to_real and never flipping real_world_units true.
    """
    blocks = [
        _block("Scale (@ A0):", {"x_min": 3058, "y_min": 2262, "x_max": 3285, "y_max": 2273}),
        _block("As indicated", {"x_min": 3065, "y_min": 2272, "x_max": 3295, "y_max": 2290}),
        _block("ALL DIMENSIONS ARE IN MILLIMETRES U.N.O."),
        _block("GA PLAN\n1 : 50", {"x_min": 400, "y_min": 1800, "x_max": 600, "y_max": 1820}),
        _block("DETAIL\n1 : 100", {"x_min": 1800, "y_min": 1800, "x_max": 2000, "y_max": 1820}),
    ]
    scale = pdf_adapter._derive_pdf_scale(blocks)
    assert scale["status"] == "ambiguous_multi_scale"
    assert scale["scale_ratio_candidates"] == ("1:50", "1:100")
    assert scale["confidence"] == "low"
    assert scale["real_world_units"] is False
    assert "points_to_real" not in scale
    assert "scale_ratio" not in scale
    # The real-world unit is still recorded (it is independent of the ambiguous ratio).
    assert scale["real_world_unit"] == "millimeter"


def test_derive_pdf_scale_single_ratio_unaffected_by_guard() -> None:
    """The exactly-one-ratio path is unchanged: a lone 1:50 still resolves confidently.

    Covers the single-ratio 'As indicated' sheet (a 1:50 outside the title-block cell), which
    #559 already handles and the multi-scale guard must not regress.
    """
    blocks = [
        _block("Scale (@ A0):", {"x_min": 3058, "y_min": 2262, "x_max": 3285, "y_max": 2273}),
        _block("As indicated", {"x_min": 3065, "y_min": 2272, "x_max": 3295, "y_max": 2290}),
        _block("ALL DIMENSIONS ARE IN MILLIMETRES U.N.O."),
        _block("GA PLAN\n1 : 50", {"x_min": 400, "y_min": 1800, "x_max": 600, "y_max": 1820}),
    ]
    scale = pdf_adapter._derive_pdf_scale(blocks)
    assert scale["status"] == "derived_from_text"
    assert scale["scale_ratio"] == {"numerator": 1, "denominator": 50, "text": "1:50"}
    assert scale["points_to_real"] == 17.638889
    assert scale["real_world_units"] is True


def test_layer_source_reports_ocg_vs_pen() -> None:
    assert pdf_adapter._layer_source(["pen-000000-w0.5", "default"]) == "pen_signature"
    assert pdf_adapter._layer_source(["default-w0"]) == "pen_signature"
    # A non-pen, non-default layer name can only come from a native OCG layer.
    assert pdf_adapter._layer_source(["Architecture", "pen-000000-w0.5"]) == "ocg"


def test_populate_page_entities_honors_native_ocg_layer() -> None:
    drawings: list[dict[str, object]] = [
        {
            "color": (0.0, 0.0, 0.0),
            "width": 0.5,
            "closePath": False,
            "items": [("l", _FakePoint(0.0, 0.0), _FakePoint(1.0, 1.0))],
            "layer": "Architecture",  # native OCG layer set by PyMuPDF get_drawings
        }
    ]
    entities: list[dict[str, JSONValue]] = []
    warnings: list[AdapterWarning] = []
    layer_names: list[str] = []
    budget = pdf_adapter._ExtractionBudget(started_at=0.0, timeout_seconds=None)

    pdf_adapter._populate_page_entities(
        drawings,
        entities=entities,
        warnings=warnings,
        layer_names=layer_names,
        page_number=1,
        layout_name="page-1",
        options=AdapterExecutionOptions(timeout=None),
        budget=budget,
        entity_limit=100,
    )

    assert layer_names == ["Architecture"]
    assert entities and all(entity["layer_ref"] == "Architecture" for entity in entities)
    assert pdf_adapter._layer_source(layer_names) == "ocg"


def _pdf_source() -> AdapterSource:
    # file_path is never read in these tests — the subprocess runner is faked.
    return AdapterSource(
        file_path=Path("/tmp/unused.pdf"),
        upload_format=UploadFormat.PDF,
        input_family=InputFamily.PDF_VECTOR,
    )


async def test_extract_with_process_decodes_success_envelope_from_runner() -> None:
    # The SubprocessRunner seam: a fake runner returns an envelope; no real process is spawned.
    async def _runner(request: Any, options: Any) -> dict[str, Any]:
        return {
            "status": "ok",
            "canonical": {"entities": [], "metadata": {}},
            "warnings": [{"code": "w", "message": "m"}],
        }

    canonical, warnings = await pdf_adapter._extract_with_process(
        _pdf_source(), AdapterExecutionOptions(timeout=None), runner=_runner
    )
    assert canonical == {"entities": [], "metadata": {}}
    assert [w.code for w in warnings] == ["w"]


async def test_extract_with_process_propagates_error_envelope() -> None:
    async def _runner(request: Any, options: Any) -> dict[str, Any]:
        return {"status": "error", "kind": "timeout", "message": "timed out"}

    with pytest.raises(TimeoutError):
        await pdf_adapter._extract_with_process(
            _pdf_source(), AdapterExecutionOptions(timeout=None), runner=_runner
        )


async def test_extract_with_process_fails_closed_when_runner_cannot_spawn() -> None:
    # A runner that cannot spawn (daemonic parent) must not route untrusted input in-process.
    async def _runner(request: Any, options: Any) -> dict[str, Any]:
        raise AssertionError("daemonic processes are not allowed to have children")

    with pytest.raises(pdf_adapter.PyMuPDFIsolationUnavailableError):
        await pdf_adapter._extract_with_process(
            _pdf_source(), AdapterExecutionOptions(timeout=None), runner=_runner
        )


def test_configure_child_process_limits_clamps_cpu_to_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_limits: list[ParserProcessLimits] = []
    monkeypatch.setattr(settings, "parser_subprocess_max_memory_mb", 384)
    monkeypatch.setattr(settings, "parser_subprocess_cpu_seconds", 300)
    monkeypatch.setattr(
        pdf_adapter,
        "apply_parser_process_limits",
        lambda limits: observed_limits.append(limits),
    )

    pdf_adapter._configure_child_process_limits(timeout_seconds=12.5)

    assert observed_limits == [
        ParserProcessLimits(
            max_address_space_bytes=384 * 1024 * 1024,
            max_cpu_seconds=12.5,
        )
    ]


# ---------------------------------------------------------------------------
# _build_pdf_style / style block on lineish entities
# ---------------------------------------------------------------------------


def _make_drawing(
    *,
    color: object = None,
    width: object = None,
    dashes: object = None,
) -> dict[str, Any]:
    """Minimal drawing dict for style/entity tests."""
    return {
        "color": color,
        "width": width,
        "dashes": dashes,
        "type": "s",
        "fill": None,
        "stroke_opacity": None,
        "fill_opacity": None,
        "lineCap": None,
        "lineJoin": None,
        "seqno": None,
        "closePath": False,
        "items": [],
    }


def test_build_pdf_style_colour_present() -> None:
    # (0.176, 0.443, 1.0) -> 0x2d 0x71 0xff -> "2d71ff"
    drawing = _make_drawing(color=(0.176, 0.443, 1.0), width=0.5)
    style = pdf_adapter._build_pdf_style(drawing)
    color = style["color"]
    assert isinstance(color, dict)
    assert color["rgb"] == "2d71ff"
    assert color["index"] is None
    assert color["by_layer"] is False
    assert color["by_block"] is False


def test_build_pdf_style_no_colour_rgb_is_none() -> None:
    drawing = _make_drawing(color=None, width=0.5)
    style = pdf_adapter._build_pdf_style(drawing)
    color = style["color"]
    assert isinstance(color, dict)
    assert color["rgb"] is None


def test_build_pdf_style_dashed_vs_solid() -> None:
    dashed_drawing = _make_drawing(dashes="[3] 0")
    solid_drawing_bracket = _make_drawing(dashes="[] 0")
    solid_drawing_empty = _make_drawing(dashes="")
    solid_drawing_none = _make_drawing(dashes=None)

    dashed_style = pdf_adapter._build_pdf_style(dashed_drawing)
    assert isinstance(dashed_style["linetype"], dict)
    assert dashed_style["linetype"]["dashed"] is True

    for drawing in (solid_drawing_bracket, solid_drawing_empty, solid_drawing_none):
        style = pdf_adapter._build_pdf_style(drawing)
        assert isinstance(style["linetype"], dict)
        raw = drawing.get("dashes")
        assert style["linetype"]["dashed"] is False, f"expected dashed=False for dashes={raw!r}"


def _lineish_entity_for(drawing: dict[str, Any], *, polyline: bool = False) -> dict[str, JSONValue]:
    """Build a lineish entity from a drawing dict via the real builder."""
    if polyline:
        points: list[tuple[float, float]] = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)]
    else:
        points = [(0.0, 0.0), (1.0, 1.0)]
    entity = pdf_adapter._build_lineish_entity(
        points=points,
        page_number=1,
        layout_name="page-1",
        layer_name="pen-000000-w0.5",
        drawing_index=0,
        entity_index=0,
        item_indices=(0,),
        drawing=drawing,
        closed=False,
        rect_like=False,
    )
    assert entity is not None
    return entity


def test_style_on_line_entity() -> None:
    drawing = _make_drawing(color=(0.176, 0.443, 1.0), width=0.5, dashes="[] 0")
    entity = _lineish_entity_for(drawing, polyline=False)
    assert entity["entity_type"] == "line"
    style = entity["style"]
    assert isinstance(style, dict)
    color = style["color"]
    assert isinstance(color, dict)
    assert color["rgb"] == "2d71ff"
    linetype = style["linetype"]
    assert isinstance(linetype, dict)
    assert linetype["dashed"] is False
    lineweight = style["lineweight"]
    assert isinstance(lineweight, dict)
    # PDF stroke width is in points, not mm; mm is honestly None (width stays in properties).
    assert lineweight["mm"] is None
    properties = entity["properties"]
    assert isinstance(properties, dict)
    assert properties["stroke_width"] == 0.5


def test_style_on_polyline_entity() -> None:
    drawing = _make_drawing(color=(0.0, 0.0, 0.0), width=1.0, dashes="[3 1] 0")
    entity = _lineish_entity_for(drawing, polyline=True)
    assert entity["entity_type"] == "polyline"
    style = entity["style"]
    assert isinstance(style, dict)
    color = style["color"]
    assert isinstance(color, dict)
    assert color["rgb"] == "000000"
    linetype = style["linetype"]
    assert isinstance(linetype, dict)
    assert linetype["dashed"] is True
    lineweight = style["lineweight"]
    assert isinstance(lineweight, dict)
    # PDF stroke width is in points, not mm; mm is honestly None (width stays in properties).
    assert lineweight["mm"] is None
    properties = entity["properties"]
    assert isinstance(properties, dict)
    assert properties["stroke_width"] == 1.0


# ---------------------------------------------------------------------------
# PDF text entity emission (#790)
# ---------------------------------------------------------------------------


def _fake_line(text: str, bbox: tuple[float, float, float, float]) -> dict[str, Any]:
    """Minimal PyMuPDF ``get_text("dict")`` line dict."""
    return {
        "bbox": bbox,
        "spans": [{"text": text}],
    }


def _fake_block(
    block_number: int,
    lines: list[dict[str, Any]],
    bbox: tuple[float, float, float, float] = (0.0, 0.0, 100.0, 20.0),
) -> dict[str, Any]:
    return {
        "type": 0,
        "number": block_number,
        "bbox": bbox,
        "lines": lines,
    }


class _FakePage:
    """Minimal page stub that returns a ``get_text("dict")`` payload."""

    def __init__(self, blocks: list[dict[str, Any]]) -> None:
        self._blocks = blocks

    def get_text(self, mode: str) -> dict[str, Any]:
        assert mode == "dict"
        return {"blocks": self._blocks}


def _run_extract_text_blocks(
    page: _FakePage,
    *,
    page_number: int = 1,
    layout_name: str = "page-1",
) -> tuple[
    list[dict[str, JSONValue]],
    list[AdapterWarning],
    int,
    list[dict[str, JSONValue]],
]:
    budget = pdf_adapter._ExtractionBudget(started_at=0.0, timeout_seconds=None)
    return pdf_adapter._extract_text_blocks(
        page,
        page_number=page_number,
        layout_name=layout_name,
        options=AdapterExecutionOptions(timeout=None),
        budget=budget,
        text_block_limit=pdf_adapter._MAX_TEXT_BLOCKS,
        text_byte_limit=pdf_adapter._MAX_TEXT_BYTES,
    )


def test_extract_text_blocks_emits_canonical_text_entities_for_each_non_empty_line() -> None:
    """Each non-empty text line becomes a canonical ``text`` entity (#790)."""
    blocks = [
        _fake_block(
            0,
            [
                _fake_line("0.2.17", (10.0, 20.0, 50.0, 30.0)),  # room number
                _fake_line("100∅SVP", (10.0, 30.0, 70.0, 40.0)),  # pipe tag
            ],
        ),
        _fake_block(
            1,
            [
                _fake_line("Server Room", (100.0, 50.0, 200.0, 60.0)),  # room name
            ],
        ),
    ]
    page = _FakePage(blocks)
    _text_blocks, _warnings, _text_bytes, text_entities = _run_extract_text_blocks(page)

    # Three non-empty lines → three text entities.
    assert len(text_entities) == 3
    texts = [str(e["text"]) for e in text_entities]
    assert "0.2.17" in texts
    assert "100∅SVP" in texts
    assert "Server Room" in texts

    # Each entity must satisfy the canonical contract.
    for entity in text_entities:
        assert entity["entity_type"] == "text"
        assert entity["kind"] == "text"
        assert entity["layout_ref"] == "page-1"
        assert entity["layer_ref"] == "default"
        assert entity["block_ref"] is None
        assert entity["drawing_revision_id"] is None
        # geometry must carry both "text" and "insertion" with x/y.
        geometry = entity["geometry"]
        assert isinstance(geometry, dict)
        assert isinstance(geometry["text"], str)
        insertion = geometry["insertion"]
        assert isinstance(insertion, dict)
        assert "x" in insertion and "y" in insertion
        # Confidence must be the high embedded-font score, not the vector score.
        confidence = entity["confidence"]
        assert isinstance(confidence, dict)
        assert confidence["basis"] == "embedded_font_text"
        assert float(confidence["score"]) == pdf_adapter._EMBEDDED_TEXT_CONFIDENCE_SCORE


def test_extract_text_blocks_insertion_is_bbox_centre() -> None:
    """Insertion point must be the centre of the line bbox."""
    line = _fake_line("0.2.17", (10.0, 20.0, 50.0, 30.0))
    block = _fake_block(0, [line])
    page = _FakePage([block])
    _, _, _, text_entities = _run_extract_text_blocks(page)

    assert len(text_entities) == 1
    geometry = text_entities[0]["geometry"]
    assert isinstance(geometry, dict)
    insertion = geometry["insertion"]
    assert isinstance(insertion, dict)
    # cx = (10 + 50) / 2 = 30, cy = (20 + 30) / 2 = 25
    assert float(insertion["x"]) == pytest.approx(30.0)
    assert float(insertion["y"]) == pytest.approx(25.0)


def test_extract_text_blocks_metadata_blocks_still_populated() -> None:
    """metadata.text_blocks output is unaffected by text entity emission."""
    blocks = [
        _fake_block(0, [_fake_line("0.2.17", (0.0, 0.0, 50.0, 10.0))]),
        _fake_block(1, [_fake_line("Server Room", (0.0, 10.0, 100.0, 20.0))]),
    ]
    page = _FakePage(blocks)
    text_blocks, _warnings, text_bytes, _text_entities = _run_extract_text_blocks(page)

    assert len(text_blocks) == 2
    assert text_blocks[0]["text"] == "0.2.17"
    assert text_blocks[1]["text"] == "Server Room"
    # text_bytes must reflect actual content.
    assert text_bytes > 0


def test_extract_text_blocks_skips_empty_lines_as_entities() -> None:
    """Blank / whitespace-only lines do NOT produce text entities."""
    blocks = [
        _fake_block(
            0,
            [
                _fake_line("", (0.0, 0.0, 50.0, 10.0)),
                _fake_line("   ", (0.0, 10.0, 50.0, 20.0)),
                _fake_line("0.2.17", (0.0, 20.0, 50.0, 30.0)),
            ],
        )
    ]
    page = _FakePage(blocks)
    _, _, _, text_entities = _run_extract_text_blocks(page)

    assert len(text_entities) == 1
    assert text_entities[0]["text"] == "0.2.17"


def test_extract_text_blocks_drawings_only_page_yields_no_text_entities() -> None:
    """A page with no text blocks (drawings only) must return an empty text entity list."""
    page = _FakePage([])
    _, warnings, _, text_entities = _run_extract_text_blocks(page)
    assert text_entities == []
    assert warnings == []


def test_extract_text_blocks_non_text_blocks_ignored() -> None:
    """Image blocks (type != 0) must not produce text entities."""
    blocks = [
        {"type": 1, "number": 0, "bbox": (0.0, 0.0, 100.0, 100.0)},  # image block
        _fake_block(0, [_fake_line("Label", (0.0, 0.0, 50.0, 10.0))]),
    ]
    page = _FakePage(blocks)
    _, _, _, text_entities = _run_extract_text_blocks(page)
    assert len(text_entities) == 1
    assert text_entities[0]["text"] == "Label"


def test_extract_text_blocks_entities_do_not_affect_line_entity_count() -> None:
    """Drawing-path entities produced by ``_populate_page_entities`` are unaffected."""
    drawings = [
        {
            "color": (0.0, 0.0, 0.0),
            "width": 0.5,
            "closePath": False,
            "items": [("l", _FakePoint(0.0, 0.0), _FakePoint(1.0, 1.0))],
        },
        {
            "color": (0.0, 0.0, 0.0),
            "width": 0.5,
            "closePath": False,
            "items": [("l", _FakePoint(2.0, 0.0), _FakePoint(3.0, 1.0))],
        },
    ]
    entities: list[dict[str, JSONValue]] = []
    warnings: list[AdapterWarning] = []
    layer_names: list[str] = []
    budget = pdf_adapter._ExtractionBudget(started_at=0.0, timeout_seconds=None)

    pdf_adapter._populate_page_entities(
        drawings,
        entities=entities,
        warnings=warnings,
        layer_names=layer_names,
        page_number=1,
        layout_name="page-1",
        options=AdapterExecutionOptions(timeout=None),
        budget=budget,
        entity_limit=100,
    )

    assert len(entities) == 2
    assert all(e["entity_type"] == "line" for e in entities)


def test_build_pdf_text_entity_contract() -> None:
    """Direct unit-test of the builder: verify every load-bearing field is present."""
    line = _fake_line("Plantroom", (100.0, 200.0, 300.0, 220.0))
    entity = pdf_adapter._build_pdf_text_entity(
        line=line,
        line_text="Plantroom",
        page_number=2,
        layout_name="page-2",
        block_number=3,
        line_index=7,
    )
    assert entity["entity_type"] == "text"
    assert entity["entity_id"] == "page-2:text-block-3:line-7"
    assert entity["text"] == "Plantroom"
    assert entity["text_length"] == len("Plantroom")
    assert entity["layout_ref"] == "page-2"
    assert entity["layer_ref"] == "default"
    geometry = entity["geometry"]
    assert isinstance(geometry, dict)
    assert geometry["text"] == "Plantroom"
    insertion = geometry["insertion"]
    assert isinstance(insertion, dict)
    # cx = (100+300)/2 = 200, cy = (200+220)/2 = 210
    assert float(insertion["x"]) == pytest.approx(200.0)
    assert float(insertion["y"]) == pytest.approx(210.0)
    confidence = entity["confidence"]
    assert isinstance(confidence, dict)
    assert confidence["basis"] == "embedded_font_text"
    assert float(confidence["score"]) == pdf_adapter._EMBEDDED_TEXT_CONFIDENCE_SCORE
    # Provenance must not be absent.
    assert "provenance" in entity
