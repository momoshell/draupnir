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
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.interpretation.legend_dictionary import LegendDictionary
from app.interpretation.loaders import load_revision_entities_by_type
from app.interpretation.models import EntityRow
from app.models.revision_materialization import RevisionBlock, RevisionEntity

if TYPE_CHECKING:
    from app.interpretation.device_identity import DeviceIdentity

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
    exclude_off_sheet: bool = False,
) -> list[Device]:
    """Enumerate device instances by walking the block-instance tree from top-level INSERTs.

    Each INSERT (at any depth) is emitted with its world position; recursion descends into the
    block it places, composing transforms, guarded against cycles, the depth cap, and a total cap.

    ``exclude_off_sheet`` scopes the walk to the printed sheet (#588): only the ROOT INSERTs carry
    a sheet-membership tag (#569), and a nested child belongs to the same printed sheet as the
    root it descends from, so dropping off-sheet roots cascades to their whole subtree. Undetermined
    (NULL) roots are kept, so a drawing without viewports is unaffected.
    """

    block_defs = await _load_block_defs(db, revision_id)

    roots = await load_revision_entities_by_type(
        db,
        revision_id,
        ("insert",),
        layer_refs=device_layers,
        exclude_off_sheet=exclude_off_sheet,
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
    exclude_off_sheet: bool = False,
) -> list[_TagCandidate]:
    base = select(RevisionEntity).where(
        RevisionEntity.drawing_revision_id == revision_id,
        RevisionEntity.entity_type == "text",
    )
    if exclude_off_sheet:
        base = base.where(RevisionEntity.on_sheet.isnot(False))
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

    return _rows_to_candidates(rows)


async def load_text_candidates(
    db: AsyncSession,
    revision_id: UUID,
    *,
    exclude_off_sheet: bool = False,
) -> list[_TagCandidate]:
    """Load every text entity (with layer) — the room-label source (#549).

    Room names + numbers live on a room-label layer that isn't a device-tag layer, so room
    interpretation needs all text, not the tag-filtered subset; the room pipeline then scopes
    to the number-bearing layer itself. ``exclude_off_sheet`` drops off-sheet labels (#583).
    """
    query = select(RevisionEntity).where(
        RevisionEntity.drawing_revision_id == revision_id,
        RevisionEntity.entity_type == "text",
    )
    if exclude_off_sheet:
        query = query.where(RevisionEntity.on_sheet.isnot(False))
    rows = (await db.execute(query)).scalars().all()
    return _rows_to_candidates(rows)


def _rows_to_candidates(rows: Sequence[RevisionEntity]) -> list[_TagCandidate]:
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


# ---------------------------------------------------------------------------
# TypedDevice — typed, positioned device for cross-member fusion (R-E, #719)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TypedDevice:
    """A device instance resolved to its type bucket, preserving its world anchor.

    ``kind`` is one of the KIND_* constants from ``device_identity``.
    ``type_name`` is legend/label-driven (ADR-002) — never derived from block_ref alone.
    ``position`` mirrors ``Device.position``: None when the INSERT has no resolvable placement.
    """

    device_id: str
    type_name: str
    kind: str
    position: dict[str, float] | None


def _typed_from_identities(
    devices: Sequence[Device],
    identities: Sequence[DeviceIdentity],
) -> list[TypedDevice]:
    """Apply the schedule_by_type bucketing rule to pre-built identities.

    Internal shared implementation consumed by both ``resolve_typed_devices``
    (which builds identities itself) and the route (which passes pre-built ones
    to avoid resolving twice).

    * KIND_LEGEND_EXEMPLAR → excluded from output (not bucketed, not counted).
    * KIND_ARCHITECTURE    → type_name="architecture".
    * KIND_DEVICE          → type_name = identity.type_name or "unresolved".
    * annotation/unknown   → type_name="unresolved".
    """
    from app.interpretation.device_identity import (
        KIND_ARCHITECTURE,
        KIND_DEVICE,
        KIND_LEGEND_EXEMPLAR,
    )

    result: list[TypedDevice] = []
    for device, identity in zip(devices, identities, strict=True):
        if identity.kind == KIND_LEGEND_EXEMPLAR:
            continue
        if identity.kind == KIND_ARCHITECTURE:
            type_name = "architecture"
        elif identity.kind == KIND_DEVICE:
            type_name = identity.type_name if identity.type_name else "unresolved"
        else:
            type_name = "unresolved"
        result.append(
            TypedDevice(
                device_id=device.entity_id,
                type_name=type_name,
                kind=identity.kind,
                position=device.position,
            )
        )
    return result


def resolve_typed_devices(
    devices: Sequence[Device],
    legend: LegendDictionary,
) -> list[TypedDevice]:
    """Resolve ``devices`` against ``legend`` and return typed, positioned records.

    Applies the same bucketing rule used by the ``/revisions/{id}/devices`` route
    ``schedule_by_type`` output — shared rule guarantees byte-stable counts.
    Callers that have already built identities should call ``_typed_from_identities``
    directly to avoid resolving twice.
    """
    from app.interpretation.device_identity import resolve_device_identities

    identities = resolve_device_identities(devices, legend)
    return _typed_from_identities(devices, identities)


async def load_typed_devices(
    db: AsyncSession,
    revision_id: UUID,
    *,
    scope: Literal["sheet", "modelspace"] = "sheet",
    max_depth: int = _MAX_DEVICE_NESTING_DEPTH,
    device_layers: Sequence[str] | None = None,
    tag_layer: Sequence[str] | None = None,
    max_tag_distance: float | None = None,
) -> list[TypedDevice]:
    """Load, tag, legend-resolve and type-bucket all devices for one revision.

    This is the async DB seam that wraps enumerate → tags → legend → resolve_typed_devices
    in one call, so the floor-counted fusion loader stays thin.
    """
    from app.interpretation.legend_dictionary import (
        FamilyInput,
        ProseInput,
        TagInput,
        from_block_families,
        from_prose_schedule,
        from_tag_layers,
        fuse,
    )
    from app.interpretation.loaders import load_legend_text_candidates

    exclude_off_sheet = scope == "sheet"

    raw_devices = await enumerate_devices(
        db,
        revision_id,
        device_layers=list(device_layers) if device_layers is not None else None,
        max_depth=max_depth,
        exclude_off_sheet=exclude_off_sheet,
    )
    candidates = await load_tag_candidates(
        db,
        revision_id,
        tag_layers=list(tag_layer) if tag_layer is not None else None,
        exclude_off_sheet=exclude_off_sheet,
    )
    all_tagged = attach_tags(raw_devices, candidates, max_distance=max_tag_distance)

    families = [FamilyInput(family_name=d.block_ref) for d in all_tagged if d.block_ref]
    prose_texts = await load_legend_text_candidates(db, revision_id)
    prose = [ProseInput(text=t) for t in prose_texts]
    tag_tokens = list({c.text for c in candidates})
    tags = [TagInput(token=t) for t in tag_tokens]
    legend = fuse(
        [*from_block_families(families), *from_prose_schedule(prose), *from_tag_layers(tags)]
    )

    return resolve_typed_devices(all_tagged, legend)
