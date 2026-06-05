"""Focused tests for debug overlay generation primitives."""

from __future__ import annotations

import re
import uuid

from app.ingestion.contracts import JSONValue
from app.ingestion.debug_overlay import (
    SVG_DEBUG_OVERLAY_FILENAME,
    SVG_DEBUG_OVERLAY_MEDIA_TYPE,
    plan_svg_debug_overlay,
)
from app.storage.keys import build_generated_artifact_storage_key


def test_build_generated_artifact_storage_key_uses_artifact_namespace() -> None:
    """Generated artifact keys should always be anchored under the artifact id."""
    artifact_id = uuid.UUID("11111111-1111-1111-1111-111111111111")

    key = build_generated_artifact_storage_key(artifact_id, "debug-overlay.svg")

    assert key == "artifacts/11111111-1111-1111-1111-111111111111/debug-overlay.svg"


def test_build_generated_artifact_storage_key_rejects_path_segments() -> None:
    """Generated artifact keys should reject nested or traversal-like filenames."""
    artifact_id = uuid.UUID("11111111-1111-1111-1111-111111111111")

    try:
        build_generated_artifact_storage_key(artifact_id, "nested/debug-overlay.svg")
    except ValueError as exc:
        assert str(exc) == "Artifact filename must not contain path segments."
    else:
        raise AssertionError("Expected path segment rejection for generated artifact name.")


def test_plan_svg_debug_overlay_emits_deterministic_svg_with_geometry_and_placeholders() -> None:
    """Overlay plans should render deterministic SVG bytes for mixed canonical geometry."""
    canonical: dict[str, JSONValue] = {
        "layouts": (
            {
                "name": "page-1",
                "page_number": 1,
                "bbox": {
                    "x_min": 0.0,
                    "y_min": 0.0,
                    "x_max": 100.0,
                    "y_max": 100.0,
                },
            },
            {"name": "Model"},
        ),
        "entities": (
            {
                "entity_id": "page-1:line-1",
                "kind": "line",
                "layout": "page-1",
                "start": {"x": 10.0, "y": 10.0},
                "end": {"x": 90.0, "y": 90.0},
                "bbox": {
                    "x_min": 10.0,
                    "y_min": 10.0,
                    "x_max": 90.0,
                    "y_max": 90.0,
                },
                "confidence_score": 0.97,
            },
            {
                "kind": "polyline",
                "layout": "page-1",
                "points": (
                    {"x": 10.0, "y": 90.0},
                    {"x": 50.0, "y": 55.0},
                    {"x": 90.0, "y": 10.0},
                ),
                "bbox": {
                    "x_min": 10.0,
                    "y_min": 10.0,
                    "x_max": 90.0,
                    "y_max": 90.0,
                },
                "confidence": {"score": 0.75},
                "review_state": "provisional",
            },
            {
                "entity_id": "ifc:wall-1",
                "kind": "ifc_wall",
                "layout": "Model",
                "confidence": 0.31,
            },
        ),
    }

    first_plan = plan_svg_debug_overlay(
        canonical,
        title="Debug Overlay Sample",
        source_label="originals/plan.pdf",
        review_state="review_required",
        confidence_score=0.42,
    )
    second_plan = plan_svg_debug_overlay(
        canonical,
        title="Debug Overlay Sample",
        source_label="originals/plan.pdf",
        review_state="review_required",
        confidence_score=0.42,
    )

    assert first_plan.filename == SVG_DEBUG_OVERLAY_FILENAME
    assert first_plan.media_type == SVG_DEBUG_OVERLAY_MEDIA_TYPE
    assert first_plan.payload == second_plan.payload

    payload = first_plan.payload.decode("utf-8")
    assert "<!-- Debug overlay only; not source of truth. -->" in payload
    assert "Title: Debug Overlay Sample" in payload
    assert "Source: originals/plan.pdf" in payload
    assert "NOT SOURCE OF TRUTH" in payload
    assert "Layout: page-1 | Page: 1" in payload
    assert "Layout: Model" in payload
    assert "page-1:line-1 | line | review_required | 0.97" in payload
    assert "page-1:entity-0002 | polyline | provisional | 0.75" in payload
    assert "ifc:wall-1 | ifc_wall | review_required | 0.31" in payload
    assert "ifc:wall-1 | No canonical geometry" in payload
    assert "cue-review-required" in payload
    assert "cue-provisional" in payload
    assert '<line class="geometry cue-review-required"' in payload
    assert '<polyline class="geometry cue-provisional"' in payload


def test_plan_svg_debug_overlay_parses_layout_aliases_and_nested_bbox_for_line_geometry() -> None:
    """LibreDWG-shaped entities should parse layout aliases and nested bbox."""
    canonical: dict[str, JSONValue] = {
        "layouts": (
            {
                "name": "Model",
                "page_number": 1,
                "bbox": {
                    "x_min": 0.0,
                    "y_min": 0.0,
                    "x_max": 10.0,
                    "y_max": 10.0,
                },
            },
            {
                "name": "Layout-B",
                "page_number": 2,
                "bbox": {
                    "x_min": 0.0,
                    "y_min": 0.0,
                    "x_max": 12.0,
                    "y_max": 12.0,
                },
            },
        ),
        "entities": (
            {
                "entity_id": "model-line",
                "kind": "line",
                "layout_name": "Model",
                "bbox": {
                    "min": {"x": 1.0, "y": 2.0},
                    "max": {"x": 9.0, "y": 8.0},
                },
                "confidence_score": 0.37,
            },
            {
                "entity_id": "alt-line",
                "kind": "line",
                "layoutName": "Layout-B",
                "bbox": {
                    "min": {"x": 1.0, "y": 2.0},
                    "max": {"x": 3.0, "y": 4.0},
                },
                "confidence": {"score": 0.42},
            },
        ),
    }

    payload = plan_svg_debug_overlay(
        canonical,
        title="LibreDWG Entity Sample",
        source_label="originals/example.dwg",
        review_state="review_required",
        confidence_score=0.93,
    ).payload.decode("utf-8")

    assert "Layout: __document__" not in payload
    assert "Layout: document" not in payload
    assert payload.count('<rect class="panel"') == 2
    assert payload.count('class="bbox cue-review-required"') == 2
    assert "Layout: Model | Page: 1" in payload
    assert "Layout: Layout-B | Page: 2" in payload
    assert "model-line | line | review_required | 0.37" in payload
    assert "alt-line | line | review_required | 0.42" in payload
    assert "No canonical geometry" not in payload


def test_plan_svg_debug_overlay_parses_layout_name_and_nested_bbox_for_line_geometry() -> None:
    """LibreDWG-shaped entities should render with layout_name and nested bbox."""
    canonical: dict[str, JSONValue] = {
        "entities": (
            {
                "entity_id": "model-line",
                "kind": "line",
                "layout_name": "Model",
                "start": {"x": 1.0, "y": 2.0},
                "end": {"x": 9.0, "y": 8.0},
                "bbox": {
                    "min": {"x": 1.0, "y": 2.0},
                    "max": {"x": 9.0, "y": 8.0},
                },
                "confidence": {
                    "score": 0.37,
                },
            },
        ),
    }

    payload = plan_svg_debug_overlay(
        canonical,
        title="LibreDWG Entity Sample",
        source_label="originals/example.dwg",
        review_state="review_required",
        confidence_score=0.93,
    ).payload.decode("utf-8")

    assert "Layout: __document__" not in payload
    assert "Layout: document" not in payload
    assert payload.count('<rect class="panel"') == 1

    model_panel_match = re.search(
        (
            r'<rect class="panel"[^/]*/>\s*'
            r'<text class="label"[^>]*>Layout: Model</text>\s*'
            r'<text class="meta"[^>]*>Entities: 1</text>(?:(?!<rect class="panel").)*'
        ),
        payload,
        re.S,
    )
    assert model_panel_match is not None
    assert "model-line | line | review_required | 0.37" in model_panel_match.group(0)

    assert '<line class="geometry cue-review-required"' in payload
    assert "No canonical geometry" not in payload


def test_plan_svg_debug_overlay_escapes_title_source_layout_and_entity_metadata() -> None:
    """Overlay SVG should HTML-escape user-provided metadata fields."""
    canonical: dict[str, JSONValue] = {
        "layouts": (
            {
                "name": 'Model <A>&"B"',
                "page_number": 2,
                "bbox": {
                    "x_min": 0.0,
                    "y_min": 0.0,
                    "x_max": 10.0,
                    "y_max": 10.0,
                },
            },
        ),
        "entities": (
            {
                "entity_id": 'entity<1>&"2"',
                "kind": 'line<kind>&"shape"',
                "layout": 'Model <A>&"B"',
                "review_state": 'review<state>&"needed"',
                "confidence_score": 0.51,
                "start": {"x": 1.0, "y": 1.0},
                "end": {"x": 9.0, "y": 9.0},
            },
        ),
    }

    payload = plan_svg_debug_overlay(
        canonical,
        title='Debug <Overlay> "Phase" & Check',
        source_label='originals/<plan>&"sheet".pdf',
        review_state="review_required",
        confidence_score=0.51,
    ).payload.decode("utf-8")

    assert (
        '<title id="overlay-title">Debug &lt;Overlay&gt; &quot;Phase&quot; &amp; Check '
        "debug overlay</title>"
    ) in payload
    assert (
        "Diagnostic overlay for originals/&lt;plan&gt;&amp;&quot;sheet&quot;.pdf. "
        "Not source of truth."
    ) in payload
    assert "Title: Debug &lt;Overlay&gt; &quot;Phase&quot; &amp; Check" in payload
    assert "Source: originals/&lt;plan&gt;&amp;&quot;sheet&quot;.pdf" in payload
    assert "Layout: Model &lt;A&gt;&amp;&quot;B&quot; | Page: 2" in payload
    assert (
        "entity&lt;1&gt;&amp;&quot;2&quot; | line&lt;kind&gt;&amp;&quot;shape&quot; | "
        "review&lt;state&gt;&amp;&quot;needed&quot; | 0.51"
    ) in payload
    assert 'Debug <Overlay> "Phase" & Check debug overlay' not in payload
    assert 'Layout: Model <A>&"B" | Page: 2' not in payload
    assert 'entity<1>&"2" | line<kind>&"shape" | review<state>&"needed" | 0.51' not in payload


def test_plan_svg_debug_overlay_degrades_invalid_geometry_to_placeholder_without_nan_or_inf() -> (
    None
):
    """Invalid or non-finite geometry should render as safe diagnostic placeholders."""
    canonical: dict[str, JSONValue] = {
        "layouts": ({"name": "page-1"},),
        "entities": (
            {
                "entity_id": "broken-entity",
                "kind": "polyline",
                "layout": "page-1",
                "bbox": {
                    "x_min": 0.0,
                    "y_min": 0.0,
                    "x_max": float("inf"),
                    "y_max": 10.0,
                },
                "points": (
                    {"x": 1.0, "y": 1.0},
                    {"x": float("nan"), "y": 3.0},
                ),
                "start": {"x": float("inf"), "y": 0.0},
                "end": {"x": 5.0, "y": float("nan")},
            },
        ),
    }

    payload = plan_svg_debug_overlay(
        canonical,
        title="Debug Overlay Sample",
        source_label="originals/plan.pdf",
        review_state="review_required",
        confidence_score=0.42,
    ).payload.decode("utf-8")

    assert "broken-entity | No canonical geometry" in payload
    assert '<polyline class="geometry' not in payload
    assert '<line class="geometry' not in payload
    assert re.search(r'(?:x|y|x1|y1|x2|y2|width|height|points)="[^"]*(?:nan|inf)', payload) is None


def test_plan_svg_debug_overlay_degrades_finite_overflow_geometry_to_placeholder() -> None:
    """Finite-but-overflowing geometry spans should not serialize invalid SVG coordinates."""
    canonical: dict[str, JSONValue] = {
        "layouts": (
            {
                "name": "page-1",
                "bbox": {
                    "x_min": -1.7976931348623157e308,
                    "y_min": 0.0,
                    "x_max": 1.7976931348623157e308,
                    "y_max": 10.0,
                },
            },
        ),
        "entities": (
            {
                "entity_id": "overflow-entity",
                "kind": "polyline",
                "layout": "page-1",
                "points": (
                    {"x": -1.7976931348623157e308, "y": 1.0},
                    {"x": 1.7976931348623157e308, "y": 9.0},
                ),
            },
        ),
    }

    payload = plan_svg_debug_overlay(
        canonical,
        title="Debug Overlay Sample",
        source_label="originals/plan.pdf",
        review_state="review_required",
        confidence_score=0.42,
    ).payload.decode("utf-8")

    assert "overflow-entity | No canonical geometry" in payload
    assert '<polyline class="geometry' not in payload
    assert '<line class="geometry' not in payload
    assert '<rect class="layout-box"' not in payload
    assert re.search(r'(?:x|y|x1|y1|x2|y2|width|height|points)="[^"]*(?:nan|inf)', payload) is None
