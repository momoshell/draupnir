"""Regression tests for PyMuPDF adapter payload robustness (real-PDF edge cases)."""

from __future__ import annotations

from pathlib import Path

from app.ingestion.adapters import pymupdf as pdf_adapter
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


async def test_extract_with_process_falls_back_when_spawn_is_blocked(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    def _blocked(_request: object) -> object:
        raise AssertionError("daemonic processes are not allowed to have children")

    monkeypatch.setattr(pdf_adapter, "_start_extraction_process", _blocked)
    monkeypatch.setattr(pdf_adapter, "_extract_in_child_process", lambda _request: _ok_envelope())
    source = AdapterSource(
        file_path=Path("/tmp/x.pdf"),
        upload_format=UploadFormat.PDF,
        input_family=InputFamily.PDF_VECTOR,
    )
    canonical, warnings = await pdf_adapter._extract_with_process(
        source, AdapterExecutionOptions(timeout=None)
    )
    assert canonical == {"entities": [], "metadata": {}}
    assert warnings[0].code == "pymupdf_subprocess_isolation_unavailable"


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
