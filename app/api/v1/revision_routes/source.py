"""Source-anchored entity drill-down route (#522).

Re-derive the raw source fragment behind a canonical entity by re-opening the
retained original input and addressing into it (DXF handle / IFC GlobalId). Lets an
agent verify a transform rather than trust it. No persistence; formats without a
stable, cheap address return an honest ``available: false``.
"""

import asyncio
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.revision_lineage import _get_active_revision_manifest_or_409
from app.core.exceptions import raise_not_found
from app.db.session import get_db
from app.ingestion.contracts import UploadFormat, input_families_for_upload_format
from app.ingestion.source import (
    OriginalSourceMaterialization,
    OriginalSourceReadError,
    OriginalSourceStageError,
    materialize_original_source,
)
from app.ingestion.source_fragment import (
    REDERIVABLE_FORMATS,
    fetch_source_fragment,
    unsupported_reason,
)
from app.models.drawing_revision import DrawingRevision
from app.models.file import File
from app.models.project import Project
from app.models.revision_materialization import RevisionEntity
from app.schemas.entity_source import RevisionEntitySourceRead
from app.storage import Storage, get_storage

source_router = APIRouter()


@source_router.get(
    "/revisions/{revision_id}/entities/{entity_id:path}/source",
    response_model=RevisionEntitySourceRead,
)
async def get_revision_entity_source(
    revision_id: UUID,
    entity_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    storage: Annotated[Storage, Depends(get_storage)],
) -> RevisionEntitySourceRead:
    """Return the raw source fragment behind a materialized entity.

    Re-opens the revision's original source and looks up the native object by the
    entity's recorded address. DWG/PDF (no stable cheap address yet) return the
    recorded identity with ``available: false`` and a reason.
    """

    await _get_active_revision_manifest_or_409(revision_id, db)

    entity = (
        await db.execute(
            select(RevisionEntity).where(
                (RevisionEntity.drawing_revision_id == revision_id)
                & (RevisionEntity.entity_id == entity_id)
            )
        )
    ).scalar_one_or_none()
    if entity is None:
        raise_not_found("Revision entity", entity_id)
    assert entity is not None

    source_file = await _load_source_file(db, revision_id)
    upload_format = _upload_format(source_file.detected_format if source_file else None)
    detected = source_file.detected_format if source_file else None

    base: dict[str, Any] = {
        "revision_id": revision_id,
        "entity_id": entity.entity_id,
        "source_identity": entity.source_identity,
        "source_hash": entity.source_hash,
        "upload_format": upload_format.value if upload_format is not None else detected,
    }

    if (
        source_file is None
        or upload_format is None
        or upload_format.value not in REDERIVABLE_FORMATS
    ):
        reason = (
            unsupported_reason(upload_format.value)
            if upload_format is not None
            else "The source format could not be determined."
        )
        return RevisionEntitySourceRead(**base, available=False, reason=reason)

    source_ref = entity.provenance_json.get("source_ref") if entity.provenance_json else None
    materialization = OriginalSourceMaterialization(
        file_id=source_file.id,
        checksum_sha256=source_file.checksum_sha256,
        upload_format=upload_format,
        input_family=input_families_for_upload_format(upload_format)[0],
        media_type=source_file.media_type,
        original_name=source_file.original_filename,
    )

    try:
        async with materialize_original_source(materialization, storage=storage) as adapter_source:
            result = await asyncio.to_thread(
                fetch_source_fragment,
                file_path=adapter_source.file_path,
                upload_format=upload_format.value,
                source_identity=entity.source_identity,
                source_ref=source_ref if isinstance(source_ref, str) else None,
            )
    except (OriginalSourceReadError, OriginalSourceStageError) as exc:
        return RevisionEntitySourceRead(
            **base, available=False, reason=f"Could not read the original source ({exc.reason})."
        )

    available = bool(result.pop("available", False))
    reason = result.pop("reason", None)
    return RevisionEntitySourceRead(
        **base,
        available=available,
        fragment=result if available else None,
        reason=reason,
    )


async def _load_source_file(db: AsyncSession, revision_id: UUID) -> File | None:
    """Load the (visible) original source file for a revision."""
    return (
        await db.execute(
            select(File)
            .join(DrawingRevision, DrawingRevision.source_file_id == File.id)
            .join(Project, Project.id == File.project_id)
            .where(
                (DrawingRevision.id == revision_id)
                & (File.deleted_at.is_(None))
                & (Project.deleted_at.is_(None))
            )
        )
    ).scalar_one_or_none()


def _upload_format(detected_format: str | None) -> UploadFormat | None:
    if detected_format is None:
        return None
    try:
        return UploadFormat(detected_format)
    except ValueError:
        return None
