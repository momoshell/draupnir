"""Geometry and status helpers for ingest validation policy."""

from __future__ import annotations

from collections.abc import Mapping
from math import isfinite
from typing import Any

from ._constants import (
    _BLOCK_REFERENCE_ENTITY_KINDS,
    _CENTER_RADIUS_ENTITY_KINDS,
    _FAIL_STATUS_VALUES,
    _LINE_ENTITY_KINDS,
    _PASS_STATUS_VALUES,
    _POINT_ENTITY_KINDS,
    _POLYGON_ENTITY_KINDS,
)


def _normalize_status_hint(*candidates: Any) -> bool | None:
    for candidate in candidates:
        if candidate is None:
            continue
        normalized = _normalize_status_value(candidate)
        if normalized is not None:
            return normalized
        if isinstance(candidate, Mapping):
            for key in (
                "status",
                "state",
                "validation_status",
                "resolution_status",
                "calibration_status",
                "review_state",
                "supported",
                "valid",
                "complete",
                "captured",
                "normalized",
                "resolved",
                "confirmed",
            ):
                nested = _normalize_status_value(candidate.get(key))
                if nested is not None:
                    return nested

    return None


def _normalize_status_value(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return None
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _PASS_STATUS_VALUES:
            return True
        if normalized in _FAIL_STATUS_VALUES:
            return False

    return None


def _sequence_mappings(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, (list, tuple)):
        return []

    return [item for item in value if isinstance(item, Mapping)]


def _entity_mappings(canonical_json: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return _sequence_mappings(canonical_json.get("entities"))


def _entity_has_valid_geometry(entity: Mapping[str, Any]) -> bool | None:
    kind = str(entity.get("kind", "")).strip().lower()
    if kind in _LINE_ENTITY_KINDS:
        return (
            (
                _has_coordinate_value(entity.get("start"))
                and _has_coordinate_value(entity.get("end"))
            )
            or _has_coordinate_sequence(entity.get("points"), minimum_points=2)
            or _has_coordinate_sequence(entity.get("vertices"), minimum_points=2)
            or _has_numeric_fields(entity, ("x1", "y1", "x2", "y2"))
        )
    if kind in _POLYGON_ENTITY_KINDS:
        return _has_valid_polygon_area_geometry(
            entity.get("points")
        ) or _has_valid_polygon_area_geometry(entity.get("vertices"))
    if kind in _POINT_ENTITY_KINDS:
        return (
            _has_coordinate_value(entity.get("point"))
            or _has_coordinate_value(entity.get("position"))
            or _has_coordinate_value(entity.get("location"))
            or _has_numeric_fields(entity, ("x", "y"))
        )
    if kind in _CENTER_RADIUS_ENTITY_KINDS:
        return (
            _has_coordinate_value(entity.get("center"))
            or _has_coordinate_value(entity.get("origin"))
        ) and (
            _is_positive_number(entity.get("radius")) or _is_positive_number(entity.get("diameter"))
        )
    if kind in _BLOCK_REFERENCE_ENTITY_KINDS:
        return (
            _has_coordinate_value(entity.get("insert"))
            or _has_coordinate_value(entity.get("position"))
            or _has_coordinate_value(entity.get("location"))
            or _has_numeric_fields(entity, ("x", "y"))
        )

    return None


def _has_coordinate_sequence(candidate: Any, *, minimum_points: int) -> bool:
    if not isinstance(candidate, (list, tuple)):
        return False

    points = sum(_has_coordinate_value(item) for item in candidate)
    return points >= minimum_points


def _has_valid_polygon_area_geometry(candidate: Any) -> bool:
    if not isinstance(candidate, (list, tuple)):
        return False

    points = [point for item in candidate if (point := _coordinate_xy(item)) is not None]
    if len(points) < 3:
        return False

    distinct_points = list(dict.fromkeys(points))
    if len(distinct_points) < 3:
        return False

    ring_points = points[:-1] if points[0] == points[-1] else points
    if len(ring_points) < 3:
        return False

    area_twice = _polygon_signed_area_twice(ring_points)
    return isfinite(area_twice) and area_twice != 0.0


def _coordinate_xy(candidate: Any) -> tuple[float, float] | None:
    if isinstance(candidate, Mapping):
        x_value = _finite_float(candidate.get("x"))
        y_value = _finite_float(candidate.get("y"))
        if x_value is not None and y_value is not None:
            return x_value, y_value
        return None
    if not isinstance(candidate, (list, tuple)) or len(candidate) < 2:
        return None

    x_value = _finite_float(candidate[0])
    y_value = _finite_float(candidate[1])
    if x_value is not None and y_value is not None:
        return x_value, y_value

    return None


def _polygon_signed_area_twice(points: list[tuple[float, float]]) -> float:
    signed_area_twice = 0.0
    for index, (x_value, y_value) in enumerate(points):
        next_x, next_y = points[(index + 1) % len(points)]
        signed_area_twice += (x_value * next_y) - (next_x * y_value)

    return signed_area_twice


def _has_coordinate_value(candidate: Any) -> bool:
    if isinstance(candidate, Mapping):
        return _has_numeric_fields(candidate, ("x", "y"))
    if not isinstance(candidate, (list, tuple)):
        return False

    numeric_components = sum(_is_number(component) for component in candidate)
    return numeric_components >= 2


def _has_numeric_fields(candidate: Mapping[str, Any], fields: tuple[str, ...]) -> bool:
    return all(_is_number(candidate.get(field)) for field in fields)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _finite_float(value: Any) -> float | None:
    if not _is_finite_number(value):
        return None

    return float(value)


def _is_finite_number(value: Any) -> bool:
    return _is_number(value) and isfinite(value)


def _is_positive_number(value: Any) -> bool:
    return _is_number(value) and value > 0


def _polygon_entities(canonical_json: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    polygon_entities: list[Mapping[str, Any]] = []
    for entity in _entity_mappings(canonical_json):
        kind = str(entity.get("kind", "")).strip().lower()
        if kind in _POLYGON_ENTITY_KINDS:
            polygon_entities.append(entity)

    return polygon_entities


def _polygon_closed_state(entity: Mapping[str, Any]) -> bool | None:
    return _normalize_status_hint(
        entity.get("closed"),
        entity.get("is_closed"),
        entity.get("area_quantity_eligible"),
    )


def _has_block_transform_content(canonical_json: Mapping[str, Any]) -> bool:
    blocks = canonical_json.get("blocks")
    if isinstance(blocks, (list, tuple)) and bool(blocks):
        return True
    for entity in _entity_mappings(canonical_json):
        kind = str(entity.get("kind", "")).strip().lower()
        if kind in _BLOCK_REFERENCE_ENTITY_KINDS:
            return True
        if entity.get("transform") is not None or entity.get("block_name") is not None:
            return True

    return False


def _xref_ref(xref: Mapping[str, Any], *, index: int) -> str:
    for key in ("ref", "name", "path", "source_ref"):
        value = xref.get(key)
        if isinstance(value, str) and value.strip():
            return value

    return f"xref-{index}"


def _entity_ref(entity: Mapping[str, Any], *, index: int) -> str:
    for key in ("source_identity", "source_ref", "entity_id", "id", "handle", "ref"):
        value = entity.get(key)
        if isinstance(value, str) and value.strip():
            return value
        if value is not None:
            return str(value)

    return f"entity-{index}"
