"""Focused tests for debug overlay generation primitives."""

from __future__ import annotations

import re
import uuid
from collections.abc import Iterator

from app.ingestion.contracts import JSONValue
from app.ingestion.debug_overlay import (
    SVG_DEBUG_OVERLAY_FILENAME,
    SVG_DEBUG_OVERLAY_MEDIA_TYPE,
    _BBox,
    _CueStyle,
    _normalize_text_snippet,
    _OverlayEntity,
    _OverlayLayout,
    _render_layout_panel,
    plan_svg_debug_overlay,
)
from app.storage.keys import build_generated_artifact_storage_key


class _GuardedLongText(str):
    __slots__ = ("iterated", "max_iterated")

    max_iterated: int
    iterated: int

    def __new__(cls, value: str, *, max_iterated: int) -> _GuardedLongText:
        instance = super().__new__(cls, value)
        instance.max_iterated = max_iterated
        instance.iterated = 0
        return instance

    def __iter__(self) -> Iterator[str]:
        for character in str.__iter__(self):
            self.iterated += 1
            if self.iterated > self.max_iterated:
                raise AssertionError("text normalization iterated past the guarded limit")
            yield character


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


def test_plan_svg_debug_overlay_adds_sanitized_text_snippets_only_for_text_entities() -> None:
    """Canonical text entities should expose bounded normalized snippets in summaries."""
    canonical: dict[str, JSONValue] = {
        "layouts": ({"name": "Model"},),
        "entities": (
            {
                "entity_id": "text-top-level",
                "kind": "text",
                "layout": "Model",
                "text": "Panel\tA\nZone\x00North",
                "bbox": {
                    "x_min": 1.0,
                    "y_min": 1.0,
                    "x_max": 2.0,
                    "y_max": 2.0,
                },
                "review_state": "approved",
                "confidence_score": 0.96,
            },
            {
                "entity_id": "text-geometry",
                "kind": "text",
                "layout": "Model",
                "geometry": {"content": "Door\nTag"},
                "bbox": {
                    "x_min": 3.0,
                    "y_min": 3.0,
                    "x_max": 4.0,
                    "y_max": 4.0,
                },
                "review_state": "provisional",
                "confidence_score": 0.75,
            },
            {
                "entity_id": "text-properties",
                "kind": "text",
                "layout": "Model",
                "properties": {"value": "Window 01"},
                "bbox": {
                    "x_min": 5.0,
                    "y_min": 5.0,
                    "x_max": 6.0,
                    "y_max": 6.0,
                },
                "review_state": "provisional",
                "confidence_score": 0.61,
            },
            {
                "entity_id": "text-escaped",
                "kind": "text",
                "layout": "Model",
                "text": "<tag>\nAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "bbox": {
                    "x_min": 7.0,
                    "y_min": 7.0,
                    "x_max": 8.0,
                    "y_max": 8.0,
                },
                "review_state": "review_required",
                "confidence_score": 0.55,
            },
            {
                "entity_id": "line-1",
                "kind": "line",
                "layout": "Model",
                "start": {"x": 0.0, "y": 0.0},
                "end": {"x": 9.0, "y": 9.0},
                "review_state": "approved",
                "confidence_score": 0.97,
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

    assert "text-top-level | text | approved | 0.96 | text=Panel A Zone North" in payload
    assert "text-geometry | text | provisional | 0.75 | text=Door Tag" in payload
    assert "text-properties | text | provisional | 0.61 | text=Window 01" in payload
    assert (
        "text-escaped | text | review_required | 0.55 | text=&lt;tag&gt; AAAAAAAAAAAAAAAAAAAAAAA..."
    ) in payload
    assert "text=Panel\tA\nZone" not in payload
    assert "line-1 | line | approved | 0.97 | text=" not in payload


def test_normalize_text_snippet_stops_after_snippet_budget_for_large_text() -> None:
    """Large text snippets should normalize lazily once truncation is known."""
    guarded = _GuardedLongText(
        ("A" * 29) + "\n\t" + ("B" * 4096),
        max_iterated=80,
    )

    assert _normalize_text_snippet(guarded) == f"{'A' * 29}..."
    assert guarded.iterated <= 40


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


def _make_cue(review_state: str = "review_required") -> _CueStyle:
    return _CueStyle(class_name=f"cue-{review_state}", review_state=review_state)


def _make_geometric_entity(stable_id: str, layout_key: str = "Model") -> _OverlayEntity:
    """Entity whose geometry_bounds() returns a valid bbox."""
    return _OverlayEntity(
        stable_id=stable_id,
        kind="line",
        layout_key=layout_key,
        bbox=_BBox(x_min=0.0, y_min=0.0, x_max=10.0, y_max=10.0),
        points=(),
        start=None,
        end=None,
        cue=_make_cue(),
        confidence_score=0.9,
    )


def _make_placeholder_entity(stable_id: str, layout_key: str = "Model") -> _OverlayEntity:
    """Entity whose geometry_bounds() returns None (no bbox, no points, no start/end)."""
    return _OverlayEntity(
        stable_id=stable_id,
        kind="ifc_wall",
        layout_key=layout_key,
        bbox=None,
        points=(),
        start=None,
        end=None,
        cue=_make_cue(),
        confidence_score=None,
    )


def test_render_layout_panel_partitions_by_identity_not_value_equality() -> None:
    """Geometric/placeholder partition must use object identity, not value equality.

    Regression: the former `entity not in geometric_entities` list-scan used __eq__ on
    frozen dataclasses, producing O(n²) behaviour. The fix uses an identity set. This
    test validates the partition is exhaustive and mutually exclusive: every entity in
    `entities` appears in exactly one of geometric or placeholder, with no silent drops.

    The identity distinction matters when value-equal duplicate objects exist in the
    entities list — each must be accounted for independently rather than treated as one
    logical entity by value equality. Here both copies of the value-twin have geometry
    so both land in geometric_entities; the placeholder entity lands in placeholders.
    The entity-count in the SVG summary proves no entity was silently dropped.
    """
    layout = _OverlayLayout(
        key="Model",
        label="Model",
        page_number=None,
        bbox=_BBox(x_min=0.0, y_min=0.0, x_max=100.0, y_max=100.0),
    )

    # Two geometrically identical objects (equal-by-value) that are distinct Python objects.
    geo_entity = _make_geometric_entity("shared-id")
    value_twin = _make_geometric_entity("shared-id")  # same field values, different identity
    assert geo_entity == value_twin, "precondition: objects must be equal-by-value"
    assert geo_entity is not value_twin, "precondition: objects must be distinct identities"

    non_geo_entity = _make_placeholder_entity("placeholder-1")

    # entities has 3 distinct objects; the SVG summary emits one row per entity (by identity).
    entities = [geo_entity, non_geo_entity, value_twin]

    lines = _render_layout_panel(layout, entities, x=0.0, y=0.0, width=800.0, height=600.0)
    svg = "\n".join(lines)

    # Total entity count row is driven by len(entities) — proves no silent drops.
    assert "Entities: 3" in svg
    # Both geo entities render (both have bbox → geometry_bounds() is not None).
    # The summary sidebar emits one row per entity; "shared-id | line" appears twice.
    assert svg.count("shared-id | line") == 2
    # placeholder entity renders as a placeholder.
    assert "placeholder-1 | No canonical geometry" in svg


def test_render_layout_panel_all_entities_become_placeholders_when_bounds_is_none() -> None:
    """When no entity has geometry and layout has no bbox, bounds is None.

    In that case `bounds is not None` is False so geometric_entities is empty and ALL
    entities must degrade to placeholders — the identity-set fix must preserve this guard.
    """
    layout = _OverlayLayout(
        key="Model",
        label="Model",
        page_number=None,
        bbox=None,  # no layout bbox; combined with no-geometry entities → bounds=None
    )

    # All entities have no geometry so _panel_bounds returns None.
    no_geo_a = _make_placeholder_entity("no-geo-a")
    no_geo_b = _make_placeholder_entity("no-geo-b")
    no_geo_c = _make_placeholder_entity("no-geo-c")
    entities = [no_geo_a, no_geo_b, no_geo_c]

    lines = _render_layout_panel(layout, entities, x=0.0, y=0.0, width=800.0, height=600.0)
    svg = "\n".join(lines)

    # With no valid bounds, ALL entities must degrade to placeholders.
    assert svg.count("No canonical geometry") == 3


def test_render_layout_panel_preserves_input_ordering_in_both_lists() -> None:
    """Geometric and placeholder sub-lists must appear in their original entities order."""
    layout = _OverlayLayout(
        key="Model",
        label="Model",
        page_number=None,
        bbox=_BBox(x_min=0.0, y_min=0.0, x_max=100.0, y_max=100.0),
    )

    # Interleave geometric and placeholder entities so ordering is observable.
    g1 = _make_geometric_entity("g1")
    p1 = _make_placeholder_entity("p1")
    g2 = _make_geometric_entity("g2")
    p2 = _make_placeholder_entity("p2")
    g3 = _make_geometric_entity("g3")
    entities = [g1, p1, g2, p2, g3]

    lines = _render_layout_panel(layout, entities, x=0.0, y=0.0, width=800.0, height=600.0)
    svg = "\n".join(lines)

    # Geometric entities render as <line>/<rect> geometry elements; their ids appear
    # in cue-labelled geometry blocks in input order (g1 before g2 before g3).
    g1_pos = svg.index("g1 | line")
    g2_pos = svg.index("g2 | line")
    g3_pos = svg.index("g3 | line")
    assert g1_pos < g2_pos < g3_pos, "geometric entities must appear in input order"

    # Placeholder entities appear in input order (p1 before p2).
    p1_pos = svg.index("p1 | No canonical geometry")
    p2_pos = svg.index("p2 | No canonical geometry")
    assert p1_pos < p2_pos, "placeholder entities must appear in input order"


def test_plan_svg_debug_overlay_identity_partition_renders_without_error_on_large_layout() -> None:
    """Overlay must render cleanly on a realistic multi-entity layout (partition + counts)."""
    n_geometric = 50
    n_placeholder = 10
    entities: list[dict[str, JSONValue]] = []
    for i in range(n_geometric):
        entities.append(
            {
                "entity_id": f"pipe-{i}",
                "kind": "line",
                "layout": "page-1",
                "start": {"x": float(i), "y": 0.0},
                "end": {"x": float(i) + 1.0, "y": 1.0},
                "bbox": {
                    "x_min": float(i),
                    "y_min": 0.0,
                    "x_max": float(i) + 1.0,
                    "y_max": 1.0,
                },
                "confidence_score": 0.9,
            }
        )
    for j in range(n_placeholder):
        entities.append(
            {
                "entity_id": f"ifc-wall-{j}",
                "kind": "ifc_wall",
                "layout": "page-1",
                "confidence_score": 0.5,
            }
        )

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
        ),
        "entities": tuple(entities),
    }

    payload = plan_svg_debug_overlay(
        canonical,
        title="Large Layout Test",
        source_label="originals/plan.pdf",
        review_state="review_required",
        confidence_score=0.8,
    ).payload.decode("utf-8")

    # All geometric entities render as geometry elements.
    assert payload.count('<line class="geometry') == n_geometric
    # All placeholder entities render as placeholder summaries.
    assert payload.count("No canonical geometry") == n_placeholder
    # Verify total entity count label.
    assert f"Entities: {n_geometric + n_placeholder}" in payload
