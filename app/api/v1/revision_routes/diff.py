"""Revision-to-revision semantic diff route (#524).

Deterministic structural diff between two revisions of the same file: entity
added/removed/changed (keyed by source identity, with source_hash detecting
changes), layer-set delta, per-type count delta, and coverage delta. Reuses the
lineage helpers; no new persistence.
"""

from collections import Counter
from collections.abc import Sequence
from typing import Annotated, NamedTuple
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.revision_lineage import _get_active_revision, _get_active_validation_report
from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response, raise_not_found
from app.db.session import get_db
from app.models.revision_materialization import RevisionEntity, RevisionLayer
from app.schemas.revision_diff import (
    RevisionDiffCoverage,
    RevisionDiffEntities,
    RevisionDiffLayers,
    RevisionDiffRead,
    RevisionDiffTypeDelta,
)

diff_router = APIRouter()

_DIFF_FIELDS = frozenset({"added", "removed", "changed"})


class _EntityKey(NamedTuple):
    source_identity: str | None
    source_hash: str | None
    entity_type: str


@diff_router.get(
    "/revisions/{revision_id}/diff",
    response_model=RevisionDiffRead,
)
async def get_revision_diff(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    against: Annotated[UUID, Query(description="The base (older) revision to diff against")],
    fields: Annotated[
        str | None,
        Query(description="Comma-separated id lists to include: added, removed, changed."),
    ] = None,
) -> RevisionDiffRead:
    """Diff the target revision (path) against a base revision of the same file.

    Reports entities added/removed/changed (by source identity), the layer-set
    delta, per-type count deltas, and the coverage delta — a quick way to confirm a
    re-ingest did not silently lose or alter data. Entity id lists are opt-in via
    ``fields`` (compact counts by default).
    """

    include = _parse_diff_fields(fields)

    target = await _get_active_revision(revision_id, db)
    if target is None:
        raise_not_found("Drawing revision", str(revision_id))
    base = await _get_active_revision(against, db)
    if base is None:
        raise_not_found("Drawing revision", str(against))
    assert target is not None and base is not None

    if target.source_file_id != base.source_file_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=create_error_response(
                code=ErrorCode.INPUT_INVALID,
                message="Revisions belong to different files; a diff requires the same file.",
                details={
                    "target_source_file_id": str(target.source_file_id),
                    "base_source_file_id": str(base.source_file_id),
                },
            ),
        )

    target_entities = await _load_entity_keys(db, revision_id)
    base_entities = await _load_entity_keys(db, against)
    target_layers = await _load_layer_refs(db, revision_id)
    base_layers = await _load_layer_refs(db, against)
    target_coverage = await _load_mapped_ratio(db, revision_id)
    base_coverage = await _load_mapped_ratio(db, against)

    return compute_revision_diff(
        base_revision_id=against,
        target_revision_id=revision_id,
        base_entities=base_entities,
        target_entities=target_entities,
        base_layers=base_layers,
        target_layers=target_layers,
        base_mapped_ratio=base_coverage,
        target_mapped_ratio=target_coverage,
        include=include,
    )


def compute_revision_diff(
    *,
    base_revision_id: UUID,
    target_revision_id: UUID,
    base_entities: Sequence[_EntityKey],
    target_entities: Sequence[_EntityKey],
    base_layers: Sequence[str],
    target_layers: Sequence[str],
    base_mapped_ratio: float | None,
    target_mapped_ratio: float | None,
    include: frozenset[str],
) -> RevisionDiffRead:
    """Pure structural diff between two revisions' materialized rows."""

    base_by_id = {e.source_identity: e for e in base_entities if e.source_identity is not None}
    target_by_id = {e.source_identity: e for e in target_entities if e.source_identity is not None}

    added = set(target_by_id) - set(base_by_id)
    removed = set(base_by_id) - set(target_by_id)
    common = set(base_by_id) & set(target_by_id)
    changed = {
        key for key in common if base_by_id[key].source_hash != target_by_id[key].source_hash
    }
    unchanged = common - changed

    entities = RevisionDiffEntities(
        added=len(added),
        removed=len(removed),
        changed=len(changed),
        unchanged=len(unchanged),
        unkeyed_base=sum(1 for e in base_entities if e.source_identity is None),
        unkeyed_target=sum(1 for e in target_entities if e.source_identity is None),
        added_ids=sorted(added) if "added" in include else None,
        removed_ids=sorted(removed) if "removed" in include else None,
        changed_ids=sorted(changed) if "changed" in include else None,
    )

    base_layer_set, target_layer_set = set(base_layers), set(target_layers)
    layers = RevisionDiffLayers(
        added=sorted(target_layer_set - base_layer_set),
        removed=sorted(base_layer_set - target_layer_set),
    )

    base_types = Counter(e.entity_type for e in base_entities)
    target_types = Counter(e.entity_type for e in target_entities)
    counts_by_type = {
        entity_type: RevisionDiffTypeDelta(
            base=base_types.get(entity_type, 0),
            target=target_types.get(entity_type, 0),
            delta=target_types.get(entity_type, 0) - base_types.get(entity_type, 0),
        )
        for entity_type in sorted(set(base_types) | set(target_types))
    }

    delta = (
        target_mapped_ratio - base_mapped_ratio
        if base_mapped_ratio is not None and target_mapped_ratio is not None
        else None
    )
    coverage = RevisionDiffCoverage(
        base_mapped_ratio=base_mapped_ratio,
        target_mapped_ratio=target_mapped_ratio,
        mapped_ratio_delta=delta,
    )

    return RevisionDiffRead(
        base_revision_id=base_revision_id,
        target_revision_id=target_revision_id,
        entities=entities,
        layers=layers,
        counts_by_type=counts_by_type,
        coverage=coverage,
    )


def _parse_diff_fields(fields: str | None) -> frozenset[str]:
    if not fields:
        return frozenset()
    requested = {token.strip() for token in fields.split(",") if token.strip()}
    unknown = sorted(requested - _DIFF_FIELDS)
    if unknown:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=create_error_response(
                code=ErrorCode.INPUT_INVALID,
                message="Unknown diff field(s) requested.",
                details={"unknown_fields": unknown, "allowed_fields": sorted(_DIFF_FIELDS)},
            ),
        )
    return frozenset(requested)


async def _load_entity_keys(db: AsyncSession, revision_id: UUID) -> list[_EntityKey]:
    result = await db.execute(
        select(
            RevisionEntity.source_identity,
            RevisionEntity.source_hash,
            RevisionEntity.entity_type,
        ).where(RevisionEntity.drawing_revision_id == revision_id)
    )
    return [_EntityKey(row[0], row[1], row[2]) for row in result.all()]


async def _load_layer_refs(db: AsyncSession, revision_id: UUID) -> list[str]:
    result = await db.execute(
        select(RevisionLayer.layer_ref).where(RevisionLayer.drawing_revision_id == revision_id)
    )
    return [row[0] for row in result.all()]


async def _load_mapped_ratio(db: AsyncSession, revision_id: UUID) -> float | None:
    report = await _get_active_validation_report(revision_id, db)
    if report is None or not isinstance(report.report_json, dict):
        return None
    coverage = report.report_json.get("coverage")
    entities = coverage.get("entities") if isinstance(coverage, dict) else None
    ratio = entities.get("mapped_ratio") if isinstance(entities, dict) else None
    return float(ratio) if isinstance(ratio, (int, float)) else None
