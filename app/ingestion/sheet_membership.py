"""Tag each entity with printed-sheet membership from viewport windows (#568).

The printed deliverable is a paperspace layout: modelspace cropped to the layout's
VIEWPORT windows. #567 extracted those windows onto ``canonical_json["viewports"]``.
This pass tags every entity with whether it falls inside any window (``on_sheet``) so
downstream queries can scope to the printed sheet — without dropping off-sheet content
(title blocks, key plans, off-sheet model geometry are real and stay; the 670003
mullions are the cautionary example). Tagging lives in finalization **after** block
expansion (#541) so world-placed block geometry is tagged too, and consumes the
adapter-emitted viewport windows. Pure over ``canonical_json``.

v1 containment is AABB-overlap against axis-aligned windows. Rotated (``twist``) and
non-rectangular (``clip_boundary``) viewports are a documented follow-up — the data is
captured but not yet honored here.
"""

from __future__ import annotations

from typing import Any

from app.ingestion.entity_geometry import compute_entity_bbox

_SHEET_MEMBERSHIP_SCHEMA_VERSION = "0.1"


def tag_sheet_membership(canonical_json: dict[str, Any]) -> dict[str, Any]:
    """Tag each entity with ``properties.sheet_membership``; add a metadata rollup.

    No-op when there are no viewport windows (e.g. PDF sources, or a DWG with none):
    membership is genuinely undeterminable, so we don't fabricate a claim.
    """

    windows = _viewport_windows(canonical_json.get("viewports"))
    entities = canonical_json.get("entities")
    if not windows or not isinstance(entities, list):
        return canonical_json

    on = off = undetermined = 0
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        bbox = _entity_extent(entity)
        if bbox is None:
            membership: dict[str, Any] = {"on_sheet": None, "reason": "no_extent"}
            undetermined += 1
        else:
            hits = [i for i, w in enumerate(windows) if _overlaps(bbox, w)]
            membership = {"on_sheet": bool(hits), "viewport_indices": hits}
            if hits:
                on += 1
            else:
                off += 1
        membership["schema_version"] = _SHEET_MEMBERSHIP_SCHEMA_VERSION
        properties = entity.get("properties")
        if not isinstance(properties, dict):
            properties = {}
            entity["properties"] = properties
        properties["sheet_membership"] = membership

    metadata = canonical_json.get("metadata")
    if isinstance(metadata, dict):
        metadata["sheet_membership"] = {
            "schema_version": _SHEET_MEMBERSHIP_SCHEMA_VERSION,
            "viewport_count": len(windows),
            "on_sheet": on,
            "off_sheet": off,
            "undetermined": undetermined,
        }
    return canonical_json


def _viewport_windows(viewports: Any) -> list[tuple[float, float, float, float]]:
    items = viewports.get("items") if isinstance(viewports, dict) else None
    if not isinstance(items, (list, tuple)):
        return []
    windows: list[tuple[float, float, float, float]] = []
    for vp in items:
        if not isinstance(vp, dict):
            continue
        win = vp.get("model_window")
        if not isinstance(win, dict):
            continue
        coords = [_finite(win.get(k)) for k in ("min_x", "min_y", "max_x", "max_y")]
        if any(c is None for c in coords):
            continue
        min_x, min_y, max_x, max_y = (c for c in coords if c is not None)
        # Drop degenerate (zero-area) windows — e.g. the paperspace overview vp — which
        # would contain nothing useful and only risk matching points at the origin.
        if max_x <= min_x or max_y <= min_y:
            continue
        windows.append((min_x, min_y, max_x, max_y))
    return windows


def _entity_extent(entity: dict[str, Any]) -> tuple[float, float, float, float] | None:
    """AABB of an entity, falling back to an INSERT's insertion point as a degenerate box."""
    geometry = entity.get("geometry")
    bbox = compute_entity_bbox(geometry)
    if bbox is not None:
        return bbox
    if isinstance(geometry, dict):
        transform = geometry.get("transform")
        point = transform.get("insertion_point") if isinstance(transform, dict) else None
        if isinstance(point, dict):
            x, y = _finite(point.get("x")), _finite(point.get("y"))
            if x is not None and y is not None:
                return (x, y, x, y)
    return None


def _overlaps(
    bbox: tuple[float, float, float, float], window: tuple[float, float, float, float]
) -> bool:
    """AABB overlap — an entity touching any window is on-sheet (don't hide partial content)."""
    return not (
        bbox[2] < window[0] or bbox[0] > window[2] or bbox[3] < window[1] or bbox[1] > window[3]
    )


def _finite(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    value = float(value)
    return value if value == value and value not in (float("inf"), float("-inf")) else None
