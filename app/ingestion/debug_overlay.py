"""Deterministic SVG debug overlay primitives for canonical outputs."""

from __future__ import annotations

import html
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final

from app.ingestion.contracts import JSONValue

SVG_DEBUG_OVERLAY_FILENAME: Final[str] = "debug-overlay.svg"
SVG_DEBUG_OVERLAY_MEDIA_TYPE: Final[str] = "image/svg+xml"

_DEFAULT_LAYOUT_KEY: Final[str] = "__document__"
_CANVAS_WIDTH: Final[float] = 1200.0
_CANVAS_MARGIN: Final[float] = 24.0
_HEADER_HEIGHT: Final[float] = 128.0
_PANEL_WIDTH: Final[float] = _CANVAS_WIDTH - (_CANVAS_MARGIN * 2.0)
_PANEL_MIN_HEIGHT: Final[float] = 240.0
_PANEL_ROW_HEIGHT: Final[float] = 16.0
_PANEL_GAP: Final[float] = 20.0
_VIEWPORT_WIDTH: Final[float] = 400.0
_VIEWPORT_PADDING: Final[float] = 12.0
_CONFIDENCE_REVIEW_REQUIRED: Final[float] = 0.60
_CONFIDENCE_PROVISIONAL: Final[float] = 0.95


@dataclass(frozen=True, slots=True)
class DebugOverlayArtifactPlan:
    """Ready-to-store debug overlay artifact payload."""

    filename: str
    media_type: str
    payload: bytes


@dataclass(frozen=True, slots=True)
class _Point:
    x: float
    y: float


@dataclass(frozen=True, slots=True)
class _BBox:
    x_min: float
    y_min: float
    x_max: float
    y_max: float

    @property
    def width(self) -> float:
        span = self.x_max - self.x_min
        if not math.isfinite(span):
            return 1.0
        return max(span, 1.0)

    @property
    def height(self) -> float:
        span = self.y_max - self.y_min
        if not math.isfinite(span):
            return 1.0
        return max(span, 1.0)

    @property
    def center(self) -> _Point:
        return _Point(
            x=self.x_min + ((self.x_max - self.x_min) / 2.0),
            y=self.y_min + ((self.y_max - self.y_min) / 2.0),
        )

    def padded(self) -> _BBox | None:
        span_x = self.x_max - self.x_min
        span_y = self.y_max - self.y_min
        if not math.isfinite(span_x) or not math.isfinite(span_y):
            return None

        padding = max(max(span_x, 1.0), max(span_y, 1.0)) * 0.05
        if padding <= 0:
            padding = 1.0
        x_min = self.x_min - padding
        y_min = self.y_min - padding
        x_max = self.x_max + padding
        y_max = self.y_max + padding
        if not all(math.isfinite(value) for value in (x_min, y_min, x_max, y_max)):
            return None
        return _BBox(x_min=x_min, y_min=y_min, x_max=x_max, y_max=y_max)


@dataclass(frozen=True, slots=True)
class _CueStyle:
    class_name: str
    review_state: str


@dataclass(frozen=True, slots=True)
class _OverlayLayout:
    key: str
    label: str
    page_number: int | None
    bbox: _BBox | None

    @property
    def heading(self) -> str:
        if self.page_number is None:
            return f"Layout: {self.label}"
        return f"Layout: {self.label} | Page: {self.page_number}"


@dataclass(frozen=True, slots=True)
class _OverlayEntity:
    stable_id: str
    kind: str
    layout_key: str
    bbox: _BBox | None
    points: tuple[_Point, ...]
    start: _Point | None
    end: _Point | None
    cue: _CueStyle
    confidence_score: float | None

    @property
    def has_geometry(self) -> bool:
        return self.geometry_bounds() is not None

    @property
    def summary(self) -> str:
        confidence = f"{self.confidence_score:.2f}" if self.confidence_score is not None else "n/a"
        return f"{self.stable_id} | {self.kind} | {self.cue.review_state} | {confidence}"

    def geometry_bounds(self) -> _BBox | None:
        if self.bbox is not None:
            return _validated_bbox(self.bbox)

        points: list[_Point] = []
        if len(self.points) >= 2:
            points.extend(self.points)
        if self.start is not None and self.end is not None:
            points.extend((self.start, self.end))
        if not points:
            return None

        x_values = [point.x for point in points]
        y_values = [point.y for point in points]
        return _validated_bbox(
            _BBox(
                x_min=min(x_values),
                y_min=min(y_values),
                x_max=max(x_values),
                y_max=max(y_values),
            )
        )


def plan_svg_debug_overlay(
    canonical: Mapping[str, JSONValue],
    *,
    title: str,
    source_label: str,
    review_state: str | None = None,
    confidence_score: float | None = None,
) -> DebugOverlayArtifactPlan:
    """Plan a deterministic SVG debug overlay artifact for later finalization."""
    return DebugOverlayArtifactPlan(
        filename=SVG_DEBUG_OVERLAY_FILENAME,
        media_type=SVG_DEBUG_OVERLAY_MEDIA_TYPE,
        payload=generate_svg_debug_overlay(
            canonical,
            title=title,
            source_label=source_label,
            review_state=review_state,
            confidence_score=confidence_score,
        ),
    )


def generate_svg_debug_overlay(
    canonical: Mapping[str, JSONValue],
    *,
    title: str,
    source_label: str,
    review_state: str | None = None,
    confidence_score: float | None = None,
) -> bytes:
    """Render a deterministic SVG debug overlay for canonical entities."""
    layouts = _parse_layouts(canonical.get("layouts"))
    entities = _parse_entities(
        canonical.get("entities"),
        fallback_review_state=review_state,
        fallback_confidence_score=confidence_score,
    )
    grouped_entities = _group_entities_by_layout(layouts, entities)
    ordered_layouts = _ordered_layouts(layouts, grouped_entities)
    overall_review_state = review_state or "unknown"

    panel_heights = {
        layout.key: _panel_height(grouped_entities.get(layout.key, ()))
        for layout in ordered_layouts
    }
    total_height = (
        _HEADER_HEIGHT
        + sum(panel_heights.values())
        + (_PANEL_GAP * max(len(ordered_layouts) - 1, 0))
        + _CANVAS_MARGIN
    )

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        "<!-- Debug overlay only; not source of truth. -->",
        (
            '<svg xmlns="http://www.w3.org/2000/svg" '
            f'width="{_format_number(_CANVAS_WIDTH)}" '
            f'height="{_format_number(total_height)}" '
            f'viewBox="0 0 {_format_number(_CANVAS_WIDTH)} {_format_number(total_height)}" '
            'role="img" aria-labelledby="overlay-title overlay-desc">'
        ),
        f'  <title id="overlay-title">{_escape(title)} debug overlay</title>',
        (
            '  <desc id="overlay-desc">'
            f"Diagnostic overlay for {_escape(source_label)}. Not source of truth."
            "</desc>"
        ),
        "  <defs>",
        "    <style>",
        "      .bg { fill: #0f172a; }",
        ("      .panel { fill: #111827; stroke: #334155; stroke-width: 1; rx: 12; ry: 12; }"),
        ("      .viewport { fill: #020617; stroke: #475569; stroke-width: 1; rx: 8; ry: 8; }"),
        (
            "      .layout-box { fill: none; stroke: #64748b; stroke-width: 1; "
            "stroke-dasharray: 4 4; }"
        ),
        "      .label { fill: #e2e8f0; font-family: monospace; font-size: 12px; }",
        "      .meta { fill: #94a3b8; font-family: monospace; font-size: 12px; }",
        (
            "      .marker { fill: #f87171; font-family: monospace; font-size: 14px; "
            "font-weight: 700; }"
        ),
        "      .entity-text { fill: #cbd5e1; font-family: monospace; font-size: 11px; }",
        (
            "      .placeholder { fill: #1e293b; stroke: #64748b; stroke-width: 1; "
            "stroke-dasharray: 6 4; }"
        ),
        "      .cue-approved { stroke: #16a34a; fill: rgba(22, 163, 74, 0.08); }",
        "      .cue-provisional { stroke: #d97706; fill: rgba(217, 119, 6, 0.08); }",
        "      .cue-review-required { stroke: #dc2626; fill: rgba(220, 38, 38, 0.08); }",
        "      .cue-rejected { stroke: #991b1b; fill: rgba(153, 27, 27, 0.08); }",
        "      .cue-superseded { stroke: #475569; fill: rgba(71, 85, 105, 0.08); }",
        "      .geometry { fill: none; stroke-width: 2; }",
        "      .bbox { fill: none; stroke-width: 1; stroke-dasharray: 4 3; opacity: 0.8; }",
        "    </style>",
        "  </defs>",
        _svg_rect(
            class_name="bg",
            x=0.0,
            y=0.0,
            width=_CANVAS_WIDTH,
            height=total_height,
        ),
        _svg_text(
            class_name="label",
            x=_CANVAS_MARGIN,
            y=40.0,
            content=f"Title: {_escape(title)}",
        ),
        _svg_text(
            class_name="meta",
            x=_CANVAS_MARGIN,
            y=62.0,
            content=f"Source: {_escape(source_label)}",
        ),
        _svg_text(
            class_name="meta",
            x=_CANVAS_MARGIN,
            y=84.0,
            content=(
                f"Review: {_escape(overall_review_state)} | "
                f"Confidence: {_format_confidence(confidence_score)}"
            ),
        ),
        _svg_text(
            class_name="marker",
            x=_CANVAS_MARGIN,
            y=108.0,
            content="NOT SOURCE OF TRUTH — diagnostic overlay only",
        ),
    ]

    panel_y = _CANVAS_MARGIN + _HEADER_HEIGHT
    for layout in ordered_layouts:
        layout_entities = grouped_entities.get(layout.key, ())
        panel_lines = _render_layout_panel(
            layout,
            layout_entities,
            x=_CANVAS_MARGIN,
            y=panel_y,
            width=_PANEL_WIDTH,
            height=panel_heights[layout.key],
        )
        lines.extend(panel_lines)
        panel_y += panel_heights[layout.key] + _PANEL_GAP

    lines.append("</svg>")
    return ("\n".join(lines) + "\n").encode("utf-8")


def _parse_layouts(value: object) -> tuple[_OverlayLayout, ...]:
    layouts: list[_OverlayLayout] = []
    for index, item in enumerate(_sequence(value), start=1):
        if not isinstance(item, Mapping):
            continue
        name = _string(item.get("name")) or f"layout-{index}"
        layouts.append(
            _OverlayLayout(
                key=name,
                label=name,
                page_number=_int(item.get("page_number")),
                bbox=_bbox(item.get("bbox")),
            )
        )
    if layouts:
        return tuple(layouts)
    return (
        _OverlayLayout(
            key=_DEFAULT_LAYOUT_KEY,
            label="document",
            page_number=None,
            bbox=None,
        ),
    )


def _parse_entities(
    value: object,
    *,
    fallback_review_state: str | None,
    fallback_confidence_score: float | None,
) -> tuple[_OverlayEntity, ...]:
    entities: list[_OverlayEntity] = []
    for index, item in enumerate(_sequence(value), start=1):
        if not isinstance(item, Mapping):
            continue

        layout_key = _string(item.get("layout")) or _DEFAULT_LAYOUT_KEY
        stable_id = (
            _string(item.get("entity_id"))
            or _string(item.get("id"))
            or f"{layout_key}:entity-{index:04d}"
        )
        kind = _string(item.get("kind")) or _string(item.get("entity_type")) or "entity"
        entity_review_state = (
            _string(item.get("review_state")) or fallback_review_state or "unknown"
        )
        entity_confidence_score = _entity_confidence_score(item, fallback_confidence_score)
        entities.append(
            _OverlayEntity(
                stable_id=stable_id,
                kind=kind,
                layout_key=layout_key,
                bbox=_bbox(item.get("bbox")),
                points=_points(item.get("points")),
                start=_point(item.get("start")),
                end=_point(item.get("end")),
                cue=_cue_style(entity_review_state, entity_confidence_score),
                confidence_score=entity_confidence_score,
            )
        )
    return tuple(entities)


def _group_entities_by_layout(
    layouts: tuple[_OverlayLayout, ...],
    entities: tuple[_OverlayEntity, ...],
) -> dict[str, tuple[_OverlayEntity, ...]]:
    grouped: dict[str, list[_OverlayEntity]] = {layout.key: [] for layout in layouts}
    for entity in entities:
        grouped.setdefault(entity.layout_key, []).append(entity)
    return {key: tuple(items) for key, items in grouped.items()}


def _ordered_layouts(
    layouts: tuple[_OverlayLayout, ...],
    grouped_entities: Mapping[str, tuple[_OverlayEntity, ...]],
) -> tuple[_OverlayLayout, ...]:
    ordered: list[_OverlayLayout] = list(layouts)
    known = {layout.key for layout in layouts}
    for key in grouped_entities:
        if key in known:
            continue
        ordered.append(_OverlayLayout(key=key, label=key, page_number=None, bbox=None))
    return tuple(ordered)


def _panel_height(entities: Sequence[_OverlayEntity]) -> float:
    row_count = max(len(entities), 1)
    return max(_PANEL_MIN_HEIGHT, 132.0 + (row_count * _PANEL_ROW_HEIGHT))


def _render_layout_panel(
    layout: _OverlayLayout,
    entities: Sequence[_OverlayEntity],
    *,
    x: float,
    y: float,
    width: float,
    height: float,
) -> list[str]:
    lines = [
        _svg_rect(class_name="panel", x=x, y=y, width=width, height=height),
        _svg_text(
            class_name="label",
            x=x + 20.0,
            y=y + 28.0,
            content=_escape(layout.heading),
        ),
        _svg_text(
            class_name="meta",
            x=x + 20.0,
            y=y + 48.0,
            content=f"Entities: {len(entities)}",
        ),
    ]

    viewport_x = x + 20.0
    viewport_y = y + 62.0
    viewport_height = height - 82.0
    summary_x = viewport_x + _VIEWPORT_WIDTH + 28.0
    summary_y = viewport_y + 14.0
    lines.append(
        _svg_rect(
            class_name="viewport",
            x=viewport_x,
            y=viewport_y,
            width=_VIEWPORT_WIDTH,
            height=viewport_height,
        )
    )

    bounds = _panel_bounds(layout, entities)
    geometric_entities = [
        entity for entity in entities if bounds is not None and entity.geometry_bounds() is not None
    ]
    placeholder_entities = [entity for entity in entities if entity not in geometric_entities]
    layout_bbox = _validated_bbox(layout.bbox)

    if layout_bbox is not None and bounds is not None:
        lines.append(
            _render_bbox(
                layout_bbox,
                bounds,
                viewport_x,
                viewport_y,
                _VIEWPORT_WIDTH,
                viewport_height,
                "layout-box",
            )
        )

    for entity in geometric_entities:
        lines.extend(
            _render_entity_geometry(
                entity,
                bounds,
                viewport_x,
                viewport_y,
                _VIEWPORT_WIDTH,
                viewport_height,
            )
        )

    if placeholder_entities:
        lines.extend(
            _render_placeholder_entities(
                placeholder_entities,
                viewport_x,
                viewport_y,
                _VIEWPORT_WIDTH,
                viewport_height,
            )
        )
    elif not geometric_entities:
        lines.append(
            _svg_text(
                class_name="entity-text",
                x=viewport_x + 20.0,
                y=viewport_y + 28.0,
                content="No canonical geometry entities",
            )
        )

    if entities:
        for index, entity in enumerate(entities):
            lines.append(
                _svg_text(
                    class_name="entity-text",
                    x=summary_x,
                    y=summary_y + (index * _PANEL_ROW_HEIGHT),
                    content=_escape(entity.summary),
                )
            )
    else:
        lines.append(
            _svg_text(
                class_name="entity-text",
                x=summary_x,
                y=summary_y,
                content="No canonical entities available for this layout",
            )
        )

    return lines


def _panel_bounds(
    layout: _OverlayLayout,
    entities: Sequence[_OverlayEntity],
) -> _BBox | None:
    boxes = [entity.geometry_bounds() for entity in entities]
    finite_boxes = [box for box in boxes if box is not None]
    layout_bbox = _validated_bbox(layout.bbox)
    if layout_bbox is not None:
        finite_boxes.insert(0, layout_bbox)
    if not finite_boxes:
        return None

    x_min = min(box.x_min for box in finite_boxes)
    y_min = min(box.y_min for box in finite_boxes)
    x_max = max(box.x_max for box in finite_boxes)
    y_max = max(box.y_max for box in finite_boxes)
    return _BBox(x_min=x_min, y_min=y_min, x_max=x_max, y_max=y_max).padded()


def _render_entity_geometry(
    entity: _OverlayEntity,
    bounds: _BBox | None,
    viewport_x: float,
    viewport_y: float,
    viewport_width: float,
    viewport_height: float,
) -> list[str]:
    if bounds is None:
        return []

    lines: list[str] = []
    cue_class = entity.cue.class_name
    if entity.bbox is not None:
        lines.append(
            _render_bbox(
                entity.bbox,
                bounds,
                viewport_x,
                viewport_y,
                viewport_width,
                viewport_height,
                f"bbox {cue_class}",
            )
        )

    if len(entity.points) >= 2:
        points = [
            _project_point(point, bounds, viewport_x, viewport_y, viewport_width, viewport_height)
            for point in entity.points
        ]
        polyline_points = " ".join(
            f"{_format_number(point.x)},{_format_number(point.y)}" for point in points
        )
        lines.append(_svg_polyline(class_name=f"geometry {cue_class}", points=polyline_points))
        label_point = points[0]
    elif entity.start is not None and entity.end is not None:
        start = _project_point(
            entity.start,
            bounds,
            viewport_x,
            viewport_y,
            viewport_width,
            viewport_height,
        )
        end = _project_point(
            entity.end,
            bounds,
            viewport_x,
            viewport_y,
            viewport_width,
            viewport_height,
        )
        lines.append(
            _svg_line(
                class_name=f"geometry {cue_class}",
                x1=start.x,
                y1=start.y,
                x2=end.x,
                y2=end.y,
            )
        )
        label_point = start
    elif entity.bbox is not None:
        label_point = _project_point(
            entity.bbox.center,
            bounds,
            viewport_x,
            viewport_y,
            viewport_width,
            viewport_height,
        )
    else:
        return lines

    lines.append(
        _svg_text(
            class_name="entity-text",
            x=label_point.x + 6.0,
            y=label_point.y - 6.0,
            content=_escape(entity.stable_id),
        )
    )
    return lines


def _render_placeholder_entities(
    entities: Sequence[_OverlayEntity],
    viewport_x: float,
    viewport_y: float,
    viewport_width: float,
    viewport_height: float,
) -> list[str]:
    lines: list[str] = []
    placeholder_width = max(viewport_width - 24.0, 120.0)
    placeholder_height = 26.0
    for index, entity in enumerate(entities):
        y_position = viewport_y + 12.0 + (index * 34.0)
        if y_position + placeholder_height > viewport_y + viewport_height:
            break
        lines.append(
            _svg_rect(
                class_name=f"placeholder {entity.cue.class_name}",
                x=viewport_x + 12.0,
                y=y_position,
                width=placeholder_width,
                height=placeholder_height,
            )
        )
        lines.append(
            _svg_text(
                class_name="entity-text",
                x=viewport_x + 20.0,
                y=y_position + 17.0,
                content=f"{_escape(entity.stable_id)} | No canonical geometry",
            )
        )
    return lines


def _render_bbox(
    bbox: _BBox,
    bounds: _BBox,
    viewport_x: float,
    viewport_y: float,
    viewport_width: float,
    viewport_height: float,
    class_name: str,
) -> str:
    top_left = _project_point(
        _Point(x=bbox.x_min, y=bbox.y_max),
        bounds,
        viewport_x,
        viewport_y,
        viewport_width,
        viewport_height,
    )
    bottom_right = _project_point(
        _Point(x=bbox.x_max, y=bbox.y_min),
        bounds,
        viewport_x,
        viewport_y,
        viewport_width,
        viewport_height,
    )
    width = max(bottom_right.x - top_left.x, 1.0)
    height = max(bottom_right.y - top_left.y, 1.0)
    return _svg_rect(
        class_name=class_name,
        x=top_left.x,
        y=top_left.y,
        width=width,
        height=height,
    )


def _project_point(
    point: _Point,
    bounds: _BBox,
    viewport_x: float,
    viewport_y: float,
    viewport_width: float,
    viewport_height: float,
) -> _Point:
    drawable_width = max(viewport_width - (_VIEWPORT_PADDING * 2.0), 1.0)
    drawable_height = max(viewport_height - (_VIEWPORT_PADDING * 2.0), 1.0)
    scale = min(drawable_width / bounds.width, drawable_height / bounds.height)
    scaled_width = bounds.width * scale
    scaled_height = bounds.height * scale
    offset_x = viewport_x + ((viewport_width - scaled_width) / 2.0)
    offset_y = viewport_y + ((viewport_height - scaled_height) / 2.0)
    x_position = offset_x + ((point.x - bounds.x_min) * scale)
    y_position = offset_y + ((bounds.y_max - point.y) * scale)
    return _Point(x=x_position, y=y_position)


def _cue_style(review_state: str, confidence_score: float | None) -> _CueStyle:
    normalized_review_state = review_state.strip().lower().replace("_", "-") or "unknown"
    review_classes = {
        "approved": "cue-approved",
        "provisional": "cue-provisional",
        "review-required": "cue-review-required",
        "review_required": "cue-review-required",
        "rejected": "cue-rejected",
        "superseded": "cue-superseded",
    }
    class_name = review_classes.get(normalized_review_state)
    if class_name is None:
        class_name = _confidence_class(confidence_score)
    display_review_state = normalized_review_state.replace("-", "_")
    return _CueStyle(class_name=class_name, review_state=display_review_state)


def _confidence_class(confidence_score: float | None) -> str:
    if confidence_score is None or confidence_score < _CONFIDENCE_REVIEW_REQUIRED:
        return "cue-review-required"
    if confidence_score < _CONFIDENCE_PROVISIONAL:
        return "cue-provisional"
    return "cue-approved"


def _entity_confidence_score(
    entity: Mapping[str, object],
    fallback_confidence_score: float | None,
) -> float | None:
    direct = _float(entity.get("confidence_score"))
    if direct is not None:
        return direct

    confidence = entity.get("confidence")
    if isinstance(confidence, Mapping):
        nested_score = _float(confidence.get("score"))
        if nested_score is not None:
            return nested_score
    else:
        nested_score = _float(confidence)
        if nested_score is not None:
            return nested_score

    return fallback_confidence_score


def _sequence(value: object) -> tuple[object, ...]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(value)
    return ()


def _point(value: object) -> _Point | None:
    if not isinstance(value, Mapping):
        return None
    x = _float(value.get("x"))
    y = _float(value.get("y"))
    if x is None or y is None:
        return None
    return _Point(x=x, y=y)


def _points(value: object) -> tuple[_Point, ...]:
    points: list[_Point] = []
    for item in _sequence(value):
        point = _point(item)
        if point is not None:
            points.append(point)
    return tuple(points)


def _bbox(value: object) -> _BBox | None:
    if not isinstance(value, Mapping):
        return None
    x_min = _float(value.get("x_min"))
    y_min = _float(value.get("y_min"))
    x_max = _float(value.get("x_max"))
    y_max = _float(value.get("y_max"))
    if x_min is None or y_min is None or x_max is None or y_max is None:
        return None
    minimum_x = min(x_min, x_max)
    maximum_x = max(x_min, x_max)
    minimum_y = min(y_min, y_max)
    maximum_y = max(y_min, y_max)
    return _BBox(x_min=minimum_x, y_min=minimum_y, x_max=maximum_x, y_max=maximum_y)


def _string(value: object) -> str | None:
    if isinstance(value, str):
        candidate = value.strip()
        return candidate or None
    return None


def _int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer() and math.isfinite(value):
        return int(value)
    return None


def _validated_bbox(bbox: _BBox | None) -> _BBox | None:
    if bbox is None:
        return None

    span_x = bbox.x_max - bbox.x_min
    span_y = bbox.y_max - bbox.y_min
    if not math.isfinite(span_x) or not math.isfinite(span_y):
        return None

    return bbox


def _float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        candidate = float(value)
        if math.isfinite(candidate):
            return candidate
    return None


def _format_number(value: float) -> str:
    if not math.isfinite(value):
        return "0"
    normalized = 0.0 if value == 0 else value
    return f"{normalized:.6f}".rstrip("0").rstrip(".") or "0"


def _format_confidence(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def _svg_rect(
    *,
    class_name: str,
    x: float,
    y: float,
    width: float,
    height: float,
) -> str:
    return (
        f'  <rect class="{class_name}" x="{_format_number(x)}" y="{_format_number(y)}" '
        f'width="{_format_number(width)}" height="{_format_number(height)}" />'
    )


def _svg_text(*, class_name: str, x: float, y: float, content: str) -> str:
    return (
        f'  <text class="{class_name}" x="{_format_number(x)}" y="{_format_number(y)}">'
        f"{content}</text>"
    )


def _svg_line(
    *,
    class_name: str,
    x1: float,
    y1: float,
    x2: float,
    y2: float,
) -> str:
    return (
        f'  <line class="{class_name}" x1="{_format_number(x1)}" y1="{_format_number(y1)}" '
        f'x2="{_format_number(x2)}" y2="{_format_number(y2)}" />'
    )


def _svg_polyline(*, class_name: str, points: str) -> str:
    return f'  <polyline class="{class_name}" points="{points}" />'


def _escape(value: str) -> str:
    return html.escape(value, quote=True)
