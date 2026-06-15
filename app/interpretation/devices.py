"""Device / fixture-schedule interpretation engine.

Tier-3 semantic interpretation derived from (never mutating) the canonical model. Device instances
are INSERTs; the real device count lives in the block-instance tree (a placed block may itself
contain device INSERTs), so devices are enumerated by walking that tree and composing placement
transforms to get each instance's world position. Tags are nearby text, associated to each device
by spatial proximity. Pure read + compute over the materialized entities.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.interpretation.loaders import load_revision_entities_by_type
from app.interpretation.models import EntityRow
from app.models.revision_materialization import RevisionBlock, RevisionEntity

# Layer-name tokens that, by default, mark a text layer as carrying device tags.
_DEFAULT_TAG_TOKENS: tuple[str, ...] = ("tag", "device")
# Guards for the block-instance tree walk (mirror the historical materialization limits).
_MAX_DEVICE_NESTING_DEPTH = 8
_MAX_DEVICE_INSTANCES = 20000


@dataclass(frozen=True, slots=True)
class DeviceTag:
    """A text label associated with a device by spatial proximity."""

    entity_id: str
    text: str
    layer_ref: str | None
    distance: float


@dataclass(frozen=True, slots=True)
class Device:
    """A device instance with its resolved type, world placement, nesting depth, and tag."""

    entity_id: str
    sequence_index: int
    depth: int
    block_ref: str | None
    layer_ref: str | None
    position: dict[str, float] | None
    tag: DeviceTag | None


@dataclass(frozen=True, slots=True)
class _TagCandidate:
    entity_id: str
    text: str
    layer_ref: str | None
    x: float
    y: float


@dataclass(frozen=True, slots=True)
class _Affine:
    """2D similarity transform (translation + uniform scale + rotation) for instance placement."""

    tx: float
    ty: float
    scale: float
    rot: float  # radians

    def apply(self, x: float, y: float) -> tuple[float, float]:
        sx, sy = x * self.scale, y * self.scale
        cos_r, sin_r = math.cos(self.rot), math.sin(self.rot)
        return (self.tx + sx * cos_r - sy * sin_r, self.ty + sx * sin_r + sy * cos_r)


_IDENTITY = _Affine(0.0, 0.0, 1.0, 0.0)


def _number(value: Any) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _point_xy(value: Any) -> tuple[float, float] | None:
    if isinstance(value, Mapping):
        x, y = _number(value.get("x")), _number(value.get("y"))
        return (x, y) if x is not None and y is not None else None
    return None


def _entity_xy(entity: EntityRow) -> tuple[float, float] | None:
    """Planar (x, y) placement of an INSERT (transform.insertion_point) or text (insertion)."""

    geometry = entity.geometry_json or {}
    transform = geometry.get("transform")
    if isinstance(transform, Mapping):
        point = _point_xy(transform.get("insertion_point"))
        if point is not None:
            return point
    return _point_xy(geometry.get("insertion") or geometry.get("insert"))


def _entity_text(entity: EntityRow) -> str | None:
    geometry = entity.geometry_json or {}
    text = geometry.get("text")
    return text if isinstance(text, str) and text.strip() else None


def _placement(
    geometry: Mapping[str, Any] | None,
) -> tuple[tuple[float, float], float, float] | None:
    """Return ((insertion_x, insertion_y), uniform_scale, rotation_radians) for an INSERT."""

    if not isinstance(geometry, Mapping):
        return None
    transform = geometry.get("transform")
    if not isinstance(transform, Mapping):
        return None
    insertion = _point_xy(transform.get("insertion_point"))
    if insertion is None:
        return None
    scale_value = transform.get("scale")
    scale = 1.0
    if isinstance(scale_value, Mapping):
        scale = _number(scale_value.get("x")) or 1.0
    rotation = _number(transform.get("rotation_radians")) or 0.0
    return (insertion, scale, rotation)


def _compose(
    parent: _Affine,
    insertion: tuple[float, float],
    scale: float,
    rotation: float,
    base: tuple[float, float],
) -> _Affine:
    """Transform mapping a placed block's local coords to world: parent ∘ this placement."""

    cos_r, sin_r = math.cos(rotation), math.sin(rotation)
    bx, by = scale * base[0], scale * base[1]
    rbx, rby = bx * cos_r - by * sin_r, bx * sin_r + by * cos_r
    world_tx, world_ty = parent.apply(insertion[0] - rbx, insertion[1] - rby)
    return _Affine(world_tx, world_ty, parent.scale * scale, parent.rot + rotation)


async def _load_block_defs(
    db: AsyncSession, revision_id: UUID
) -> dict[str, tuple[tuple[float, float], list[Mapping[str, Any]]]]:
    """Map block_ref -> (base_point xy, child entity payloads) from the materialized blocks."""

    rows = (
        (
            await db.execute(
                select(RevisionBlock).where(RevisionBlock.drawing_revision_id == revision_id)
            )
        )
        .scalars()
        .all()
    )
    defs: dict[str, tuple[tuple[float, float], list[Mapping[str, Any]]]] = {}
    for row in rows:
        payload = row.payload_json or {}
        base = _point_xy(payload.get("base_point")) or (0.0, 0.0)
        raw_children = payload.get("entities")
        children = (
            [child for child in raw_children if isinstance(child, Mapping)]
            if isinstance(raw_children, (list, tuple))
            else []
        )
        defs[row.block_ref] = (base, children)
    return defs


async def enumerate_devices(
    db: AsyncSession,
    revision_id: UUID,
    *,
    device_layers: Sequence[str] | None = None,
    max_depth: int = _MAX_DEVICE_NESTING_DEPTH,
) -> list[Device]:
    """Enumerate device instances by walking the block-instance tree from top-level INSERTs.

    Each INSERT (at any depth) is emitted with its world position; recursion descends into the
    block it places, composing transforms, guarded against cycles, the depth cap, and a total cap.
    """

    block_defs = await _load_block_defs(db, revision_id)

    roots = await load_revision_entities_by_type(
        db, revision_id, ("insert",), layer_refs=device_layers
    )

    devices: list[Device] = []

    def _walk(
        *,
        entity_id: str,
        sequence_index: int,
        depth: int,
        block_ref: str | None,
        layer_ref: str | None,
        geometry: Mapping[str, Any] | None,
        parent: _Affine,
        visited: frozenset[str],
    ) -> None:
        if len(devices) >= _MAX_DEVICE_INSTANCES:
            return
        placement = _placement(geometry)
        position: dict[str, float] | None = None
        if placement is not None:
            world_x, world_y = parent.apply(placement[0][0], placement[0][1])
            position = {"x": world_x, "y": world_y}
        devices.append(
            Device(
                entity_id=entity_id,
                sequence_index=sequence_index,
                depth=depth,
                block_ref=block_ref,
                layer_ref=layer_ref,
                position=position,
                tag=None,
            )
        )
        if placement is None or block_ref is None or depth >= max_depth or block_ref in visited:
            return
        definition = block_defs.get(block_ref)
        if definition is None:
            return
        base, children = definition
        child_transform = _compose(parent, placement[0], placement[1], placement[2], base)
        next_visited = visited | {block_ref}
        for index, child in enumerate(children):
            if child.get("entity_type") != "insert":
                continue
            _walk(
                entity_id=f"{entity_id}/{index}",
                sequence_index=sequence_index,
                depth=depth + 1,
                block_ref=child.get("block_ref"),
                layer_ref=child.get("layer_ref"),
                geometry=child.get("geometry")
                if isinstance(child.get("geometry"), Mapping)
                else None,
                parent=child_transform,
                visited=next_visited,
            )

    for root in roots:
        _walk(
            entity_id=root.entity_id,
            sequence_index=root.sequence_index,
            depth=0,
            block_ref=root.block_ref,
            layer_ref=root.layer_ref,
            geometry=root.geometry_json,
            parent=_IDENTITY,
            visited=frozenset(),
        )
    return devices


async def load_tag_candidates(
    db: AsyncSession,
    revision_id: UUID,
    *,
    tag_layers: Sequence[str] | None,
) -> list[_TagCandidate]:
    base = select(RevisionEntity).where(
        RevisionEntity.drawing_revision_id == revision_id,
        RevisionEntity.entity_type == "text",
    )
    if tag_layers:
        query = base.where(RevisionEntity.layer_ref.in_(list(tag_layers)))
    else:
        query = base.where(
            or_(*[RevisionEntity.layer_ref.ilike(f"%{token}%") for token in _DEFAULT_TAG_TOKENS])
        )

    rows = (await db.execute(query)).scalars().all()
    # Fall back to every text entity when no tag-ish layer matched, so association still works on
    # drawings that don't follow a tag-layer naming convention.
    if not rows and tag_layers is None:
        rows = (await db.execute(base)).scalars().all()

    candidates: list[_TagCandidate] = []
    for row in rows:
        text = _entity_text(row)
        xy = _entity_xy(row)
        if text is None or xy is None:
            continue
        candidates.append(
            _TagCandidate(
                entity_id=row.entity_id, text=text, layer_ref=row.layer_ref, x=xy[0], y=xy[1]
            )
        )
    return candidates


def _nearest_tag(
    position: tuple[float, float],
    candidates: Sequence[_TagCandidate],
    *,
    max_distance: float | None,
) -> DeviceTag | None:
    best: _TagCandidate | None = None
    best_distance = math.inf
    for candidate in candidates:
        distance = math.hypot(candidate.x - position[0], candidate.y - position[1])
        if distance < best_distance:
            best_distance = distance
            best = candidate
    if best is None or (max_distance is not None and best_distance > max_distance):
        return None
    return DeviceTag(
        entity_id=best.entity_id, text=best.text, layer_ref=best.layer_ref, distance=best_distance
    )


def attach_tags(
    devices: Sequence[Device],
    candidates: Sequence[_TagCandidate],
    *,
    max_distance: float | None = None,
) -> list[Device]:
    """Return copies of ``devices`` with the nearest tag (to each world position) attached."""

    result: list[Device] = []
    for device in devices:
        tag = None
        if device.position is not None:
            tag = _nearest_tag(
                (device.position["x"], device.position["y"]), candidates, max_distance=max_distance
            )
        result.append(
            Device(
                entity_id=device.entity_id,
                sequence_index=device.sequence_index,
                depth=device.depth,
                block_ref=device.block_ref,
                layer_ref=device.layer_ref,
                position=device.position,
                tag=tag,
            )
        )
    return result


def schedule_from_devices(devices: Sequence[Device]) -> list[dict[str, Any]]:
    """Aggregate device counts by block reference (the fixture schedule)."""

    counts: dict[str | None, int] = {}
    for device in devices:
        counts[device.block_ref] = counts.get(device.block_ref, 0) + 1
    ordered = sorted(counts.items(), key=lambda kv: (-kv[1], str(kv[0] or "")))
    return [{"block_ref": block_ref, "count": count} for block_ref, count in ordered]
