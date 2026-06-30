"""Revision drawing-scale / units read route."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import raise_not_found
from app.db.session import get_db
from app.interpretation.revision_scale import (
    RevisionScaleNotFoundError,
)
from app.interpretation.revision_scale import (
    resolve_revision_scale as _resolve_revision_scale,
)
from app.schemas.revision import RevisionScaleRead

scale_router = APIRouter()


async def resolve_revision_scale(revision_id: UUID, db: AsyncSession) -> RevisionScaleRead:
    """Resolve scale for route code and preserve the public 404 behavior."""
    try:
        return await _resolve_revision_scale(revision_id, db)
    except RevisionScaleNotFoundError:
        raise_not_found("Drawing revision", str(revision_id))
        raise AssertionError("unreachable") from None


@scale_router.get(
    "/revisions/{revision_id}/scale",
    response_model=RevisionScaleRead,
)
async def get_revision_scale(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> RevisionScaleRead:
    """Return the drawing scale + units for an active revision.

    Sourced from the revision's adapter-run canonical payload. A revision with no
    adapter run (e.g. changeset-origin) honestly reports ``units = unknown``.
    """

    return await resolve_revision_scale(revision_id, db)
