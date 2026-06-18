"""Revision materialization read routes."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import ColumnElement, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.pagination import DEFAULT_PAGE_SIZE as _DEFAULT_PAGE_SIZE
from app.api.pagination import MAX_PAGE_SIZE as _MAX_PAGE_SIZE
from app.api.pagination import paginate_overfetched as _paginate_overfetched
from app.api.v1.revision_cursors import (
    _decode_materialization_cursor,
    _encode_materialization_cursor,
)
from app.api.v1.revision_lineage import (
    _get_active_revision_manifest_or_409,
    _manifest_counts,
)
from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response, raise_not_found
from app.db.session import get_db
from app.models.revision_materialization import (
    RevisionBlock,
    RevisionEntity,
    RevisionLayer,
    RevisionLayout,
)
from app.schemas.revision import (
    RevisionBlockListResponse,
    RevisionBlockRead,
    RevisionEntityListResponse,
    RevisionEntityManifestRead,
    RevisionEntityRead,
    RevisionEntitySummary,
    RevisionLayerListResponse,
    RevisionLayerRead,
    RevisionLayoutListResponse,
    RevisionLayoutRead,
)

materialization_router = APIRouter()


@materialization_router.get(
    "/revisions/{revision_id}/layouts",
    response_model=RevisionLayoutListResponse,
)
async def list_revision_layouts(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
) -> RevisionLayoutListResponse:
    """List materialized layout rows for a drawing revision."""

    manifest = await _get_active_revision_manifest_or_409(revision_id, db)
    pagination_cursor = _decode_materialization_cursor(cursor) if cursor else None

    query = select(RevisionLayout).where(RevisionLayout.drawing_revision_id == revision_id)
    if pagination_cursor is not None:
        sequence_index, row_id = pagination_cursor
        query = query.where(
            (RevisionLayout.sequence_index > sequence_index)
            | ((RevisionLayout.sequence_index == sequence_index) & (RevisionLayout.id > row_id))
        )

    result = await db.execute(
        query.order_by(
            RevisionLayout.sequence_index.asc(),
            RevisionLayout.id.asc(),
        ).limit(limit + 1)
    )
    rows = result.scalars().all()
    page, next_cursor = _paginate_overfetched(
        rows,
        limit=limit,
        encode_cursor=lambda last_row: _encode_materialization_cursor(
            last_row.sequence_index, last_row.id
        ),
    )

    counts = _manifest_counts(manifest)
    return RevisionLayoutListResponse(
        manifest=RevisionEntityManifestRead.model_validate(manifest),
        counts=counts,
        items=[RevisionLayoutRead.model_validate(row) for row in page],
        next_cursor=next_cursor,
    )


@materialization_router.get(
    "/revisions/{revision_id}/layers",
    response_model=RevisionLayerListResponse,
)
async def list_revision_layers(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
) -> RevisionLayerListResponse:
    """List materialized layer rows for a drawing revision."""

    manifest = await _get_active_revision_manifest_or_409(revision_id, db)
    pagination_cursor = _decode_materialization_cursor(cursor) if cursor else None

    query = select(RevisionLayer).where(RevisionLayer.drawing_revision_id == revision_id)
    if pagination_cursor is not None:
        sequence_index, row_id = pagination_cursor
        query = query.where(
            (RevisionLayer.sequence_index > sequence_index)
            | ((RevisionLayer.sequence_index == sequence_index) & (RevisionLayer.id > row_id))
        )

    result = await db.execute(
        query.order_by(RevisionLayer.sequence_index.asc(), RevisionLayer.id.asc()).limit(limit + 1)
    )
    rows = result.scalars().all()
    page, next_cursor = _paginate_overfetched(
        rows,
        limit=limit,
        encode_cursor=lambda last_row: _encode_materialization_cursor(
            last_row.sequence_index, last_row.id
        ),
    )

    counts = _manifest_counts(manifest)
    return RevisionLayerListResponse(
        manifest=RevisionEntityManifestRead.model_validate(manifest),
        counts=counts,
        items=[RevisionLayerRead.model_validate(row) for row in page],
        next_cursor=next_cursor,
    )


@materialization_router.get(
    "/revisions/{revision_id}/blocks",
    response_model=RevisionBlockListResponse,
)
async def list_revision_blocks(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
) -> RevisionBlockListResponse:
    """List materialized block rows for a drawing revision."""

    manifest = await _get_active_revision_manifest_or_409(revision_id, db)
    pagination_cursor = _decode_materialization_cursor(cursor) if cursor else None

    query = select(RevisionBlock).where(RevisionBlock.drawing_revision_id == revision_id)
    if pagination_cursor is not None:
        sequence_index, row_id = pagination_cursor
        query = query.where(
            (RevisionBlock.sequence_index > sequence_index)
            | ((RevisionBlock.sequence_index == sequence_index) & (RevisionBlock.id > row_id))
        )

    result = await db.execute(
        query.order_by(RevisionBlock.sequence_index.asc(), RevisionBlock.id.asc()).limit(limit + 1)
    )
    rows = result.scalars().all()
    page, next_cursor = _paginate_overfetched(
        rows,
        limit=limit,
        encode_cursor=lambda last_row: _encode_materialization_cursor(
            last_row.sequence_index, last_row.id
        ),
    )

    counts = _manifest_counts(manifest)
    return RevisionBlockListResponse(
        manifest=RevisionEntityManifestRead.model_validate(manifest),
        counts=counts,
        items=[RevisionBlockRead.model_validate(row) for row in page],
        next_cursor=next_cursor,
    )


# Heavy projectable blocks: summary field name -> RevisionEntity ORM attribute.
_ENTITY_HEAVY_FIELDS = {
    "geometry": "geometry_json",
    "properties": "properties_json",
    "provenance": "provenance_json",
    "confidence": "confidence_json",
    "canonical": "canonical_entity_json",
}


def _parse_entity_fields(fields: str | None) -> frozenset[str]:
    """Parse + validate the ``fields=`` projection (heavy blocks to include)."""
    if not fields:
        return frozenset()
    requested = {token.strip() for token in fields.split(",") if token.strip()}
    unknown = sorted(requested - _ENTITY_HEAVY_FIELDS.keys())
    if unknown:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=create_error_response(
                code=ErrorCode.INPUT_INVALID,
                message="Unknown entity field(s) requested.",
                details={
                    "unknown_fields": unknown,
                    "allowed_fields": sorted(_ENTITY_HEAVY_FIELDS),
                },
            ),
        )
    return frozenset(requested)


def _entity_summary(row: RevisionEntity, *, include: frozenset[str]) -> RevisionEntitySummary:
    """Build a compact entity summary, populating only the requested heavy blocks."""
    return RevisionEntitySummary(
        id=row.id,
        sequence_index=row.sequence_index,
        entity_id=row.entity_id,
        entity_type=row.entity_type,
        entity_schema_version=row.entity_schema_version,
        parent_entity_ref=row.parent_entity_ref,
        layout_ref=row.layout_ref,
        layer_ref=row.layer_ref,
        block_ref=row.block_ref,
        source_identity=row.source_identity,
        source_hash=row.source_hash,
        bbox=_row_bbox(row),
        created_at=row.created_at,
        **{
            name: getattr(row, attr)
            for name, attr in _ENTITY_HEAVY_FIELDS.items()
            if name in include
        },
    )


def _row_bbox(row: RevisionEntity) -> list[float] | None:
    """Return the persisted AABB as [min_x, min_y, max_x, max_y], or None if absent."""
    corners = (row.bbox_min_x, row.bbox_min_y, row.bbox_max_x, row.bbox_max_y)
    if any(corner is None for corner in corners):
        return None
    return [float(corner) for corner in corners if corner is not None]


def _raise_spatial_invalid(message: str) -> None:
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=create_error_response(code=ErrorCode.INPUT_INVALID, message=message, details=None),
    )


def _spatial_conditions(
    *,
    min_x: float | None,
    min_y: float | None,
    max_x: float | None,
    max_y: float | None,
    near_x: float | None,
    near_y: float | None,
    radius: float | None,
) -> list[ColumnElement[bool]]:
    """Build indexed bbox-intersection / near-point conditions over the persisted AABB.

    Entities with a NULL bbox (no 2-D extent) are excluded from spatial results.
    """
    conditions: list[ColumnElement[bool]] = []

    box = (min_x, min_y, max_x, max_y)
    if any(value is not None for value in box):
        if any(value is None for value in box):
            _raise_spatial_invalid("in-bbox requires all of min_x, min_y, max_x, max_y.")
        assert min_x is not None and min_y is not None and max_x is not None and max_y is not None
        if min_x > max_x or min_y > max_y:
            _raise_spatial_invalid("in-bbox requires min_x <= max_x and min_y <= max_y.")
        conditions += [
            RevisionEntity.bbox_min_x.is_not(None),
            RevisionEntity.bbox_min_x <= max_x,
            RevisionEntity.bbox_max_x >= min_x,
            RevisionEntity.bbox_min_y <= max_y,
            RevisionEntity.bbox_max_y >= min_y,
        ]

    near = (near_x, near_y, radius)
    if any(value is not None for value in near):
        if any(value is None for value in near):
            _raise_spatial_invalid("near-point requires all of near_x, near_y, radius.")
        assert near_x is not None and near_y is not None and radius is not None
        if radius <= 0:
            _raise_spatial_invalid("near-point radius must be > 0.")
        # Squared point-to-AABB distance (0 inside the box) <= radius^2.
        dx = func.greatest(
            RevisionEntity.bbox_min_x - near_x, 0.0, near_x - RevisionEntity.bbox_max_x
        )
        dy = func.greatest(
            RevisionEntity.bbox_min_y - near_y, 0.0, near_y - RevisionEntity.bbox_max_y
        )
        conditions += [
            RevisionEntity.bbox_min_x.is_not(None),
            (dx * dx + dy * dy) <= radius * radius,
        ]

    return conditions


@materialization_router.get(
    "/revisions/{revision_id}/entities",
    response_model=RevisionEntityListResponse,
)
async def list_revision_entities(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
    entity_id: str | None = Query(default=None),
    entity_type: str | None = Query(default=None),
    layout_ref: str | None = Query(default=None),
    layer_ref: str | None = Query(default=None),
    block_ref: str | None = Query(default=None),
    parent_entity_ref: str | None = Query(default=None),
    source_identity: str | None = Query(default=None),
    source_hash: str | None = Query(default=None),
    min_x: float | None = Query(default=None, description="in-bbox: query box min x"),
    min_y: float | None = Query(default=None, description="in-bbox: query box min y"),
    max_x: float | None = Query(default=None, description="in-bbox: query box max x"),
    max_y: float | None = Query(default=None, description="in-bbox: query box max y"),
    near_x: float | None = Query(default=None, description="near-point: x (with near_y, radius)"),
    near_y: float | None = Query(default=None, description="near-point: y (with near_x, radius)"),
    radius: float | None = Query(default=None, description="near-point: max distance (> 0)"),
    fields: Annotated[
        str | None,
        Query(
            description=(
                "Comma-separated heavy blocks to include "
                "(geometry, properties, provenance, confidence, canonical). "
                "Default: none — a compact row spine."
            ),
        ),
    ] = None,
) -> RevisionEntityListResponse:
    """List compact materialized entity rows; heavy blocks opt-in via ``fields``.

    Optional spatial filters (over the persisted entity bounding box): ``min_x``/``min_y``/
    ``max_x``/``max_y`` (AABB intersection) and ``near_x``/``near_y``/``radius`` (point proximity).
    """

    include_fields = _parse_entity_fields(fields)
    spatial_conditions = _spatial_conditions(
        min_x=min_x,
        min_y=min_y,
        max_x=max_x,
        max_y=max_y,
        near_x=near_x,
        near_y=near_y,
        radius=radius,
    )
    manifest = await _get_active_revision_manifest_or_409(revision_id, db)
    pagination_cursor = _decode_materialization_cursor(cursor) if cursor else None

    query = select(RevisionEntity).where(RevisionEntity.drawing_revision_id == revision_id)
    if entity_id is not None:
        query = query.where(RevisionEntity.entity_id == entity_id)
    if entity_type is not None:
        query = query.where(RevisionEntity.entity_type == entity_type)
    if layout_ref is not None:
        query = query.where(RevisionEntity.layout_ref == layout_ref)
    if layer_ref is not None:
        query = query.where(RevisionEntity.layer_ref == layer_ref)
    if block_ref is not None:
        query = query.where(RevisionEntity.block_ref == block_ref)
    if parent_entity_ref is not None:
        query = query.where(RevisionEntity.parent_entity_ref == parent_entity_ref)
    if source_identity is not None:
        query = query.where(RevisionEntity.source_identity == source_identity)
    if source_hash is not None:
        query = query.where(RevisionEntity.source_hash == source_hash)
    for condition in spatial_conditions:
        query = query.where(condition)
    if pagination_cursor is not None:
        sequence_index, row_id = pagination_cursor
        query = query.where(
            (RevisionEntity.sequence_index > sequence_index)
            | ((RevisionEntity.sequence_index == sequence_index) & (RevisionEntity.id > row_id))
        )

    result = await db.execute(
        query.order_by(
            RevisionEntity.sequence_index.asc(),
            RevisionEntity.id.asc(),
        ).limit(limit + 1)
    )
    rows = result.scalars().all()
    page, next_cursor = _paginate_overfetched(
        rows,
        limit=limit,
        encode_cursor=lambda last_row: _encode_materialization_cursor(
            last_row.sequence_index, last_row.id
        ),
    )

    counts = _manifest_counts(manifest)
    return RevisionEntityListResponse(
        manifest=RevisionEntityManifestRead.model_validate(manifest),
        counts=counts,
        items=[_entity_summary(row, include=include_fields) for row in page],
        next_cursor=next_cursor,
    )


@materialization_router.get(
    "/revisions/{revision_id}/entities/{entity_id:path}",
    response_model=RevisionEntityRead,
)
async def get_revision_entity(
    revision_id: UUID,
    entity_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> RevisionEntityRead:
    """Return a materialized entity row for a drawing revision by entity identifier."""

    await _get_active_revision_manifest_or_409(revision_id, db)

    result = await db.execute(
        select(RevisionEntity).where(
            (RevisionEntity.drawing_revision_id == revision_id)
            & (RevisionEntity.entity_id == entity_id)
        )
    )
    entity = result.scalar_one_or_none()
    if entity is None:
        raise_not_found("Revision entity", entity_id)

    return RevisionEntityRead.model_validate(entity)
