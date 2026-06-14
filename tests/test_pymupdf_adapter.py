"""Regression tests for PyMuPDF adapter payload robustness (real-PDF edge cases)."""

from __future__ import annotations

from pathlib import Path

from app.ingestion.adapters import pymupdf as pdf_adapter
from app.ingestion.contracts import (
    AdapterExecutionOptions,
    AdapterSource,
    InputFamily,
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
