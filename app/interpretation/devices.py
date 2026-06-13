"""Device / fixture-schedule interpretation engine.

Tier-3 semantic interpretation derived from the canonical model: device instances are INSERT
entities; tags are nearby text. The drawing only relates them spatially (no attribute link), so
each device is associated with the nearest tag-text within an optional distance cap. Pure read +
compute over the materialized entities — nothing is persisted or mutated here.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.revision_materialization import RevisionEntity

# Layer-name tokens that, by default, mark a text layer as carrying device tags.
_DEFAULT_TAG_TOKENS: tuple[str, ...] = ("tag", "device")


class _EntityRow(Protocol):
    """Read-only shape the pure interpretation helpers need (satisfied by RevisionEntity)."""

    @property
    def entity_id(self) -> str: ...
    @property
    def sequence_index(self) -> int: ...
    @property
    def block_ref(self) -> str | None: ...
    @property
    def layer_ref(self) -> str | None: ...
    @property
    def geometry_json(self) -> Mapping[str, Any] | None: ...


@dataclass(frozen=True, slots=True)
class DeviceTag:
    """A text label associated with a device by spatial proximity."""

    entity_id: str
    text: str
    layer_ref: str | None
    distance: float


@dataclass(frozen=True, slots=True)
class Device:
    """A device instance (INSERT) with its resolved type, placement, and associated tag."""

    entity_id: str
    sequence_index: int
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


def _entity_xy(entity: _EntityRow) -> tuple[float, float] | None:
    """Return the planar (x, y) placement of an INSERT or text entity, if present.

    INSERTs carry ``geometry.transform.insertion_point``; text carries ``geometry.insertion``.
    """

    geometry = entity.geometry_json or {}
    point: Any = None
    transform = geometry.get("transform")
    if isinstance(transform, dict):
        point = transform.get("insertion_point")
    if point is None:
        point = geometry.get("insertion") or geometry.get("insert")
    if (
        isinstance(point, dict)
        and isinstance(point.get("x"), (int, float))
        and isinstance(point.get("y"), (int, float))
    ):
        return (float(point["x"]), float(point["y"]))
    return None


def _entity_text(entity: _EntityRow) -> str | None:
    geometry = entity.geometry_json or {}
    text = geometry.get("text")
    return text if isinstance(text, str) and text.strip() else None


async def device_schedule(
    db: AsyncSession,
    revision_id: UUID,
    *,
    device_layers: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    """Return device counts grouped by block reference (the fixture schedule)."""

    query = (
        select(RevisionEntity.block_ref, func.count())
        .where(
            RevisionEntity.drawing_revision_id == revision_id,
            RevisionEntity.entity_type == "insert",
        )
        .group_by(RevisionEntity.block_ref)
    )
    if device_layers:
        query = query.where(RevisionEntity.layer_ref.in_(list(device_layers)))

    result = await db.execute(query)
    rows = [{"block_ref": block_ref, "count": int(count)} for block_ref, count in result.all()]
    rows.sort(key=lambda row: (-row["count"], str(row["block_ref"] or "")))
    return rows


async def _load_tag_candidates(
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
        # Default: text on layers whose name suggests tags/devices.
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
    if best is None:
        return None
    if max_distance is not None and best_distance > max_distance:
        return None
    return DeviceTag(
        entity_id=best.entity_id,
        text=best.text,
        layer_ref=best.layer_ref,
        distance=best_distance,
    )


def build_devices(
    device_rows: Sequence[_EntityRow],
    candidates: Sequence[_TagCandidate],
    *,
    max_distance: float | None = None,
) -> list[Device]:
    """Pure association: match each device row to its nearest tag candidate (no I/O)."""

    devices: list[Device] = []
    for row in device_rows:
        xy = _entity_xy(row)
        position = {"x": xy[0], "y": xy[1]} if xy is not None else None
        tag = _nearest_tag(xy, candidates, max_distance=max_distance) if xy is not None else None
        devices.append(
            Device(
                entity_id=row.entity_id,
                sequence_index=row.sequence_index,
                block_ref=row.block_ref,
                layer_ref=row.layer_ref,
                position=position,
                tag=tag,
            )
        )
    return devices


async def associate_devices(
    db: AsyncSession,
    device_rows: Sequence[RevisionEntity],
    *,
    revision_id: UUID,
    tag_layers: Sequence[str] | None = None,
    max_distance: float | None = None,
) -> list[Device]:
    """Associate each device INSERT row with its nearest tag-text within ``max_distance``."""

    candidates = await _load_tag_candidates(db, revision_id, tag_layers=tag_layers)
    return build_devices(device_rows, candidates, max_distance=max_distance)
