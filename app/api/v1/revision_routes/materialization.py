"""Revision materialization read routes."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.pagination import DEFAULT_PAGE_SIZE as _DEFAULT_PAGE_SIZE
from app.api.pagination import MAX_PAGE_SIZE as _MAX_PAGE_SIZE
from app.api.v1.revision_cursors import (
    _decode_materialization_cursor,
    _encode_materialization_cursor,
)
from app.api.v1.revision_lineage import (
    _get_active_revision_manifest_or_409,
    _manifest_counts,
)
from app.core.exceptions import raise_not_found
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
    page = rows[:limit]
    next_cursor = None

    if len(rows) > limit and page:
        last_row = page[-1]
        next_cursor = _encode_materialization_cursor(last_row.sequence_index, last_row.id)

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
    page = rows[:limit]
    next_cursor = None

    if len(rows) > limit and page:
        last_row = page[-1]
        next_cursor = _encode_materialization_cursor(last_row.sequence_index, last_row.id)

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
    page = rows[:limit]
    next_cursor = None

    if len(rows) > limit and page:
        last_row = page[-1]
        next_cursor = _encode_materialization_cursor(last_row.sequence_index, last_row.id)

    counts = _manifest_counts(manifest)
    return RevisionBlockListResponse(
        manifest=RevisionEntityManifestRead.model_validate(manifest),
        counts=counts,
        items=[RevisionBlockRead.model_validate(row) for row in page],
        next_cursor=next_cursor,
    )


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
) -> RevisionEntityListResponse:
    """List materialized entity rows for a drawing revision."""

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
    page = rows[:limit]
    next_cursor = None

    if len(rows) > limit and page:
        last_row = page[-1]
        next_cursor = _encode_materialization_cursor(last_row.sequence_index, last_row.id)

    counts = _manifest_counts(manifest)
    return RevisionEntityListResponse(
        manifest=RevisionEntityManifestRead.model_validate(manifest),
        counts=counts,
        items=[RevisionEntityRead.model_validate(row) for row in page],
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
