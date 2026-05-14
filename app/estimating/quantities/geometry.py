from __future__ import annotations

import json
import math
from dataclasses import dataclass
from itertools import pairwise
from typing import TypedDict

from app.estimating.quantities.contracts import (
    BoundingBoxSummary,
    ContributorMethod,
    JSONValue,
    QuantityExclusion,
    QuantityType,
    RevisionEntityInput,
)

type Point = tuple[float, ...]


class UnitMap(TypedDict):
    length: str
    area: str
    context: str | None


@dataclass(frozen=True, slots=True)
class GeometryQuantity:
    quantity_type: QuantityType
    value: float
    unit: str
    context: str | None
    method: ContributorMethod


@dataclass(frozen=True, slots=True)
class GeometryExtraction:
    quantities: tuple[GeometryQuantity, ...]
    exclusions: tuple[QuantityExclusion, ...]
    bbox: BoundingBoxSummary | None
    fingerprint: str


def extract_geometry_quantities(entity: RevisionEntityInput) -> GeometryExtraction:
    geometry = entity.geometry_json
    if not isinstance(geometry, dict):
        return GeometryExtraction(
            quantities=(),
            exclusions=(
                QuantityExclusion(
                    entity_id=entity.entity_id,
                    quantity_type=None,
                    reason="unsupported_geometry",
                    details={"detail": "geometry_json must be an object"},
                ),
            ),
            bbox=None,
            fingerprint=_fingerprint(None),
        )

    start = _extract_point(geometry.get("start"))
    end = _extract_point(geometry.get("end"))
    points = _extract_points(geometry.get("points") or geometry.get("vertices"))
    kind = _infer_kind(geometry, start, end, points)
    raw_units = geometry.get("units") if "units" in geometry else entity.properties_json
    unit_map = _normalize_units(raw_units)

    if kind == "line":
        if start is None or end is None:
            return _invalid_geometry(
                entity.entity_id,
                kind,
                "line requires finite start/end points",
            )
        length = _segment_length(start, end)
        bbox = _bbox([start, end])
        payload_line: JSONValue = {
            "kind": kind,
            "start": list(start),
            "end": list(end),
            "units": _unit_payload(unit_map),
        }
        return GeometryExtraction(
            quantities=(
                GeometryQuantity(
                    quantity_type="length",
                    value=length,
                    unit=unit_map["length"],
                    context=unit_map["context"],
                    method="geometry",
                ),
            ),
            exclusions=(),
            bbox=bbox,
            fingerprint=_fingerprint(payload_line),
        )

    if kind in {"polyline", "lwpolyline"}:
        if len(points) < 2:
            return _invalid_geometry(
                entity.entity_id,
                kind,
                "polyline requires at least two finite points",
            )
        is_closed = _is_closed_polyline(geometry, points)
        segment_total = math.fsum(
            _segment_length(points[index], points[index + 1])
            for index in range(len(points) - 1)
        )
        exclusions: list[QuantityExclusion] = []
        quantities: list[GeometryQuantity] = []
        bbox = _bbox(points)
        if is_closed:
            perimeter = segment_total
            if points[0] != points[-1]:
                perimeter += _segment_length(points[-1], points[0])
            quantities.append(
                GeometryQuantity(
                    quantity_type="perimeter",
                    value=perimeter,
                    unit=unit_map["length"],
                    context=unit_map["context"],
                    method="geometry",
                )
            )
            area = _polygon_area(points)
            if area is None:
                exclusions.append(
                    QuantityExclusion(
                        entity_id=entity.entity_id,
                        quantity_type="area",
                        reason="ineligible_area",
                        details={"detail": "closed polyline area must be finite and non-zero"},
                    )
                )
            else:
                quantities.append(
                    GeometryQuantity(
                        quantity_type="area",
                        value=area,
                        unit=unit_map["area"],
                        context=unit_map["context"],
                        method="geometry",
                    )
                )
        else:
            quantities.append(
                GeometryQuantity(
                    quantity_type="length",
                    value=segment_total,
                    unit=unit_map["length"],
                    context=unit_map["context"],
                    method="geometry",
                )
            )
        payload_polyline: JSONValue = {
            "kind": kind,
            "closed": is_closed,
            "points": [list(point) for point in points],
            "units": _unit_payload(unit_map),
        }
        return GeometryExtraction(
            quantities=tuple(quantities),
            exclusions=tuple(exclusions),
            bbox=bbox,
            fingerprint=_fingerprint(payload_polyline),
        )

    summary_bbox = _summary_bbox(geometry.get("geometry_summary"))
    return GeometryExtraction(
        quantities=(),
        exclusions=(
            QuantityExclusion(
                entity_id=entity.entity_id,
                quantity_type=None,
                reason="unsupported_geometry",
                details={"detail": f"unsupported geometry type: {kind or 'unknown'}"},
            ),
        ),
        bbox=summary_bbox,
        fingerprint=_fingerprint({"kind": kind, "summary_bbox": _bbox_payload(summary_bbox)}),
    )


def _invalid_geometry(entity_id: str, kind: str | None, detail: str) -> GeometryExtraction:
    return GeometryExtraction(
        quantities=(),
        exclusions=(
            QuantityExclusion(
                entity_id=entity_id,
                quantity_type=None,
                reason="unsupported_geometry",
                details={"geometry_type": kind or "unknown", "detail": detail},
            ),
        ),
        bbox=None,
        fingerprint=_fingerprint({"kind": kind}),
    )


def _normalize_units(raw_units: JSONValue) -> UnitMap:
    default_unit = "unitless"
    context: str | None = None

    if isinstance(raw_units, dict):
        if "units" in raw_units:
            return _normalize_units(raw_units["units"])
        length = _normalize_string(raw_units.get("length"))
        if length is None:
            length = _normalize_string(raw_units.get("unit"))
        if length is None:
            length = _normalize_string(raw_units.get("normalized"))
        area = _normalize_string(raw_units.get("area")) or length
        context = _normalize_string(raw_units.get("context"))
        return {
            "length": length or default_unit,
            "area": area or length or default_unit,
            "context": context,
        }

    scalar_unit = _normalize_string(raw_units)
    return {
        "length": scalar_unit or default_unit,
        "area": scalar_unit or default_unit,
        "context": context,
    }


def _normalize_string(value: JSONValue) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    return normalized or None


def _infer_kind(
    geometry: dict[str, JSONValue],
    start: Point | None,
    end: Point | None,
    points: list[Point],
) -> str | None:
    direct_kind = _normalize_string(geometry.get("type") or geometry.get("kind"))
    if direct_kind is not None:
        return direct_kind

    if start is not None and end is not None:
        return "line"

    if len(points) >= 2:
        return "polyline"

    summary = geometry.get("geometry_summary")
    if isinstance(summary, dict):
        summary_kind = _normalize_string(summary.get("kind") or summary.get("type"))
        if summary_kind is not None:
            return summary_kind

    return None


def _extract_points(raw_points: JSONValue) -> list[Point]:
    if not isinstance(raw_points, list):
        return []
    points: list[Point] = []
    for raw_point in raw_points:
        point = _extract_point(raw_point)
        if point is None:
            return []
        points.append(point)
    return points


def _extract_point(raw_point: JSONValue) -> Point | None:
    if isinstance(raw_point, dict):
        coords: list[float] = []
        for axis in ("x", "y", "z"):
            raw_axis = raw_point.get(axis)
            if raw_axis is None:
                continue
            if not isinstance(raw_axis, int | float) or not math.isfinite(float(raw_axis)):
                return None
            coords.append(float(raw_axis))
        if len(coords) < 2:
            return None
        return tuple(coords)

    if isinstance(raw_point, list):
        coords = []
        for value in raw_point:
            if not isinstance(value, int | float) or not math.isfinite(float(value)):
                return None
            coords.append(float(value))
        if len(coords) < 2:
            return None
        return tuple(coords)

    return None


def _segment_length(start: Point, end: Point) -> float:
    dimensions = min(len(start), len(end))
    return math.dist(start[:dimensions], end[:dimensions])


def _bbox(points: list[Point]) -> BoundingBoxSummary | None:
    if not points:
        return None
    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    return BoundingBoxSummary(
        min_x=min(xs),
        min_y=min(ys),
        max_x=max(xs),
        max_y=max(ys),
        point_count=len(points),
    )


def _summary_bbox(summary: JSONValue) -> BoundingBoxSummary | None:
    if not isinstance(summary, dict):
        return None
    raw_bbox = summary.get("bbox")
    if not isinstance(raw_bbox, dict):
        return None
    keys = ("min_x", "min_y", "max_x", "max_y")
    values: dict[str, float] = {}
    for key in keys:
        raw_value = raw_bbox.get(key)
        if not isinstance(raw_value, int | float) or not math.isfinite(float(raw_value)):
            return None
        values[key] = float(raw_value)
    raw_points = raw_bbox.get("point_count", 0)
    if not isinstance(raw_points, int):
        return None
    return BoundingBoxSummary(point_count=raw_points, **values)


def _is_closed_polyline(geometry: dict[str, JSONValue], points: list[Point]) -> bool:
    raw_closed = geometry.get("closed")
    if isinstance(raw_closed, bool):
        return raw_closed
    return points[0] == points[-1]


def _polygon_area(points: list[Point]) -> float | None:
    unique_points = points[:-1] if points[0] == points[-1] else points
    if len(unique_points) < 3:
        return None

    total = 0.0
    loop_points = [*unique_points, unique_points[0]]
    for start, end in pairwise(loop_points):
        total += (start[0] * end[1]) - (end[0] * start[1])
    area = abs(total) / 2.0
    if not math.isfinite(area) or area <= 0.0:
        return None
    return area


def _fingerprint(payload: JSONValue | None) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _unit_payload(unit_map: UnitMap) -> dict[str, JSONValue]:
    return {
        "length": unit_map["length"],
        "area": unit_map["area"],
        "context": unit_map["context"],
    }


def _bbox_payload(bbox: BoundingBoxSummary | None) -> JSONValue:
    if bbox is None:
        return None
    return {
        "min_x": bbox.min_x,
        "min_y": bbox.min_y,
        "max_x": bbox.max_x,
        "max_y": bbox.max_y,
        "point_count": bbox.point_count,
    }
