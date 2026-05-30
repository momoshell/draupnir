"""Revision quantity takeoff routes."""

from collections.abc import Awaitable, Callable
from typing import Annotated, Any, cast
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.idempotency import (
    IdempotencyReservation,
    IdempotentMutationKnownError,
    IdempotentMutationOps,
    IdempotentMutationSuccess,
    get_idempotency_key,
    run_idempotent_mutation,
)
from app.api.idempotency import (
    build_idempotency_fingerprint as _build_idempotency_fingerprint_direct,
)
from app.api.idempotency import (
    claim_idempotency_response as _claim_idempotency_response_direct,
)
from app.api.idempotency import (
    complete_idempotency_response as _complete_idempotency_response_direct,
)
from app.api.idempotency import (
    replay_idempotency_response as _replay_idempotency_response_direct,
)
from app.api.v1.revision_cursors import _decode_timestamp_cursor, _encode_timestamp_cursor
from app.api.v1.revision_lineage import (
    _get_active_revision as _get_active_revision_direct,
)
from app.api.v1.revision_lineage import (
    _get_active_revision_manifest_or_409 as _lineage_get_active_revision_manifest_or_409_direct,
)
from app.api.v1.revision_lineage import (
    _get_active_validation_report as _get_active_validation_report_direct,
)
from app.api.v1.revision_lineage import (
    _get_active_validation_report_or_404 as _lineage_get_active_validation_report_or_404_direct,
)
from app.api.v1.revision_lineage import (
    _get_revision_manifest as _get_revision_manifest_direct,
)
from app.api.v1.revision_lineage import (
    _get_revision_quantity_takeoff_or_404 as _get_revision_quantity_takeoff_or_404_direct,
)
from app.api.v1.revision_lineage import (
    _raise_entities_not_materialized as _raise_entities_not_materialized_direct,
)
from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response, raise_not_found
from app.db.session import get_db
from app.jobs.worker import (
    enqueue_quantity_takeoff_job as _enqueue_quantity_takeoff_job_direct,
)
from app.jobs.worker import (
    prepare_job_enqueue_intent as _prepare_job_enqueue_intent_direct,
)
from app.jobs.worker import (
    publish_job_enqueue_intent as _publish_job_enqueue_intent_direct,
)
from app.models.drawing_revision import DrawingRevision
from app.models.file import File
from app.models.job import Job, JobType
from app.models.project import Project
from app.models.quantity_takeoff import QuantityGate, QuantityItem, QuantityTakeoff
from app.models.revision_materialization import RevisionEntityManifest
from app.models.validation_report import ValidationReport
from app.schemas.job import JobRead
from app.schemas.quantity_takeoff import (
    QuantityItemListResponse,
    QuantityItemRead,
    QuantityTakeoffListResponse,
    QuantityTakeoffRead,
)

quantity_takeoffs_router = APIRouter()
quantity_takeoff_create_router = APIRouter()

_DEFAULT_PAGE_SIZE = 50
_MAX_PAGE_SIZE = 200
_MISSING = object()

type _ActiveRevisionGetter = Callable[..., Awaitable[DrawingRevision | None]]
type _ValidationReportGetter = Callable[[UUID, AsyncSession], Awaitable[ValidationReport]]
type _RevisionManifestGetter = Callable[[UUID, AsyncSession], Awaitable[RevisionEntityManifest]]
type _RevisionTakeoffGetter = Callable[[UUID, UUID, AsyncSession], Awaitable[QuantityTakeoff]]
type _FingerprintBuilder = Callable[..., str]
type _ReplayIdempotencyResponse = Callable[..., Awaitable[Response | None]]
type _CompleteIdempotencyResponse = Callable[..., Awaitable[Response]]


def _compat_attr(*names: str, default: Any = _MISSING) -> Any:
    """Return the first matching revision_compat attribute."""

    from app.api.v1 import revision_compat

    for name in names:
        if hasattr(revision_compat, name):
            return getattr(revision_compat, name)
    if default is not _MISSING:
        return default
    joined = ", ".join(names)
    raise AttributeError(f"revision_compat is missing expected helper: {joined}")


async def _get_active_revision(*args: Any, **kwargs: Any) -> Any:
    """Compatibility wrapper for active revision lookup."""

    helper = cast(
        _ActiveRevisionGetter,
        _compat_attr(
            "_get_active_revision",
            "get_active_revision",
            default=_get_active_revision_direct,
        ),
    )
    return await helper(*args, **kwargs)


async def _get_active_validation_report_or_404(
    revision_id: UUID,
    db: AsyncSession,
) -> ValidationReport:
    """Return an active revision validation report or raise not found."""

    helper = cast(
        _ValidationReportGetter | None,
        _compat_attr(
            "_get_active_validation_report_or_404",
            "get_active_validation_report_or_404",
            default=None,
        ),
    )
    if helper is not None:
        return await helper(revision_id, db)

    return await _lineage_get_active_validation_report_or_404_direct(
        revision_id,
        db,
        get_active_validation_report=_get_active_validation_report_direct,
        get_active_revision=_get_active_revision_direct,
    )


async def _get_active_revision_manifest_or_409(
    revision_id: UUID,
    db: AsyncSession,
) -> RevisionEntityManifest:
    """Return an active revision manifest or raise the standard missing errors."""

    helper = cast(
        _RevisionManifestGetter | None,
        _compat_attr(
            "_get_active_revision_manifest_or_409",
            "get_active_revision_manifest_or_409",
            default=None,
        ),
    )
    if helper is not None:
        return await helper(revision_id, db)

    return await _lineage_get_active_revision_manifest_or_409_direct(
        revision_id,
        db,
        get_active_revision=_get_active_revision_direct,
        get_revision_manifest=_get_revision_manifest_direct,
        raise_entities_not_materialized=_raise_entities_not_materialized_direct,
    )


async def _get_revision_quantity_takeoff_or_404(
    revision_id: UUID,
    takeoff_id: UUID,
    db: AsyncSession,
) -> QuantityTakeoff:
    """Compatibility wrapper for revision quantity takeoff lookup."""

    helper = cast(
        _RevisionTakeoffGetter,
        _compat_attr(
            "_get_revision_quantity_takeoff_or_404",
            "get_revision_quantity_takeoff_or_404",
            default=_get_revision_quantity_takeoff_or_404_direct,
        ),
    )
    return await helper(revision_id, takeoff_id, db)


def _build_idempotency_fingerprint(*args: Any, **kwargs: Any) -> str:
    """Compatibility wrapper for fingerprint construction."""

    helper = cast(
        _FingerprintBuilder,
        _compat_attr(
            "build_idempotency_fingerprint",
            default=_build_idempotency_fingerprint_direct,
        ),
    )
    return helper(*args, **kwargs)


async def _replay_idempotency_response(*args: Any, **kwargs: Any) -> Response | None:
    """Compatibility wrapper for idempotency replay."""

    helper = cast(
        _ReplayIdempotencyResponse,
        _compat_attr(
            "replay_idempotency_response",
            default=_replay_idempotency_response_direct,
        ),
    )
    return await helper(*args, **kwargs)


async def _claim_idempotency_response(*args: Any, **kwargs: Any) -> Any:
    """Compatibility wrapper for idempotency claim."""

    helper = _compat_attr(
        "claim_idempotency_response",
        default=_claim_idempotency_response_direct,
    )
    return await helper(*args, **kwargs)


async def _complete_idempotency_response(*args: Any, **kwargs: Any) -> Response:
    """Compatibility wrapper for idempotency completion."""

    helper = cast(
        _CompleteIdempotencyResponse,
        _compat_attr(
            "complete_idempotency_response",
            default=_complete_idempotency_response_direct,
        ),
    )
    return await helper(*args, **kwargs)


async def _complete_mutation_response(
    db: AsyncSession,
    reservation: IdempotencyReservation | None,
    *,
    status_code: int,
    response_body: dict[str, Any] | None,
) -> Response:
    """Finalize create responses while preserving non-idempotent behavior."""

    if reservation is None:
        return JSONResponse(status_code=status_code, content=response_body)

    return await _complete_idempotency_response(
        db,
        reservation,
        status_code=status_code,
        response_body=response_body,
    )


def _idempotent_mutation_ops() -> IdempotentMutationOps:
    """Return compatibility-aware idempotency operations for mutation helpers."""

    return IdempotentMutationOps(
        replay=_replay_idempotency_response,
        claim=_claim_idempotency_response,
        complete=_complete_mutation_response,
    )


def _prepare_job_enqueue_intent(job: Job) -> None:
    """Compatibility wrapper for enqueue intent staging."""

    helper = _compat_attr(
        "prepare_job_enqueue_intent",
        default=_prepare_job_enqueue_intent_direct,
    )
    helper(job)


async def _publish_job_enqueue_intent(*args: Any, **kwargs: Any) -> None:
    """Compatibility wrapper for enqueue publication."""

    helper = _compat_attr(
        "publish_job_enqueue_intent",
        default=_publish_job_enqueue_intent_direct,
    )
    await helper(*args, **kwargs)


def enqueue_quantity_takeoff_job(job_id: UUID) -> None:
    """Compatibility wrapper kept for test fixture patching."""

    publisher = _compat_attr(
        "enqueue_quantity_takeoff_job",
        default=_enqueue_quantity_takeoff_job_direct,
    )
    publisher(job_id)


def _revisions_module() -> Any:
    """Return the revisions module for compatibility-sensitive callbacks."""

    from app.api.v1 import revisions

    return revisions


def _raise_quantity_takeoff_gate_invalid(report: ValidationReport) -> None:
    """Raise the standard pre-enqueue error for non-runnable quantity gates."""

    helper = _compat_attr(
        "_raise_quantity_takeoff_gate_invalid",
        "raise_quantity_takeoff_gate_invalid",
        default=None,
    )
    if helper is not None:
        helper(report)
        return

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=create_error_response(
            code=ErrorCode.INPUT_INVALID,
            message=("Quantity takeoff requires a revision with an allowed quantity gate."),
            details={
                "quantity_gate": report.quantity_gate,
                "review_state": report.review_state,
                "validation_status": report.validation_status,
            },
        ),
    )


@quantity_takeoffs_router.get(
    "/revisions/{revision_id}/quantity-takeoffs",
    response_model=QuantityTakeoffListResponse,
)
async def list_revision_quantity_takeoffs(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
) -> QuantityTakeoffListResponse:
    """List committed quantity takeoffs for an active drawing revision."""

    revision = await _get_active_revision(revision_id, db)
    if revision is None:
        raise_not_found("Drawing revision", str(revision_id))

    pagination_cursor = _decode_timestamp_cursor(cursor) if cursor else None
    query = (
        select(QuantityTakeoff)
        .join(
            File,
            (File.id == QuantityTakeoff.source_file_id)
            & (File.project_id == QuantityTakeoff.project_id),
        )
        .join(Project, Project.id == QuantityTakeoff.project_id)
        .where(
            (QuantityTakeoff.drawing_revision_id == revision_id)
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )
    if pagination_cursor is not None:
        created_at, row_id = pagination_cursor
        query = query.where(
            (QuantityTakeoff.created_at > created_at)
            | ((QuantityTakeoff.created_at == created_at) & (QuantityTakeoff.id > row_id))
        )

    result = await db.execute(
        query.order_by(QuantityTakeoff.created_at.asc(), QuantityTakeoff.id.asc()).limit(limit + 1)
    )
    rows = result.scalars().all()
    page = rows[:limit]
    next_cursor = None
    if len(rows) > limit and page:
        last_row = page[-1]
        next_cursor = _encode_timestamp_cursor(last_row.created_at, last_row.id)

    return QuantityTakeoffListResponse(
        items=[QuantityTakeoffRead.model_validate(row) for row in page],
        next_cursor=next_cursor,
    )


@quantity_takeoffs_router.get(
    "/revisions/{revision_id}/quantity-takeoffs/{takeoff_id}",
    response_model=QuantityTakeoffRead,
)
async def get_revision_quantity_takeoff(
    revision_id: UUID,
    takeoff_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> QuantityTakeoffRead:
    """Return a committed quantity takeoff for an active drawing revision."""

    revision = await _get_active_revision(revision_id, db)
    if revision is None:
        raise_not_found("Drawing revision", str(revision_id))
    assert revision is not None

    takeoff = await _get_revision_quantity_takeoff_or_404(revision_id, takeoff_id, db)
    return QuantityTakeoffRead.model_validate(takeoff)


@quantity_takeoffs_router.get(
    "/revisions/{revision_id}/quantity-takeoffs/{takeoff_id}/items",
    response_model=QuantityItemListResponse,
)
async def list_revision_quantity_takeoff_items(
    revision_id: UUID,
    takeoff_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
) -> QuantityItemListResponse:
    """List committed quantity items for a revision-scoped takeoff."""

    revision = await _get_active_revision(revision_id, db)
    if revision is None:
        raise_not_found("Drawing revision", str(revision_id))

    await _get_revision_quantity_takeoff_or_404(revision_id, takeoff_id, db)
    pagination_cursor = _decode_timestamp_cursor(cursor) if cursor else None
    query = (
        select(QuantityItem)
        .join(
            QuantityTakeoff,
            (QuantityTakeoff.id == QuantityItem.quantity_takeoff_id)
            & (QuantityTakeoff.project_id == QuantityItem.project_id)
            & (QuantityTakeoff.drawing_revision_id == QuantityItem.drawing_revision_id)
            & (QuantityTakeoff.quantity_gate == QuantityItem.quantity_gate),
        )
        .join(
            File,
            (File.id == QuantityTakeoff.source_file_id)
            & (File.project_id == QuantityTakeoff.project_id),
        )
        .join(Project, Project.id == QuantityTakeoff.project_id)
        .where(
            (QuantityItem.quantity_takeoff_id == takeoff_id)
            & (QuantityItem.drawing_revision_id == revision_id)
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )
    if pagination_cursor is not None:
        created_at, row_id = pagination_cursor
        query = query.where(
            (QuantityItem.created_at > created_at)
            | ((QuantityItem.created_at == created_at) & (QuantityItem.id > row_id))
        )

    result = await db.execute(
        query.order_by(QuantityItem.created_at.asc(), QuantityItem.id.asc()).limit(limit + 1)
    )
    rows = result.scalars().all()
    page = rows[:limit]
    next_cursor = None
    if len(rows) > limit and page:
        last_row = page[-1]
        next_cursor = _encode_timestamp_cursor(last_row.created_at, last_row.id)

    return QuantityItemListResponse(
        items=[QuantityItemRead.model_validate(row) for row in page],
        next_cursor=next_cursor,
    )


@quantity_takeoff_create_router.post(
    "/revisions/{revision_id}/quantity-takeoffs",
    response_model=JobRead,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_revision_quantity_takeoff(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    idempotency_key: Annotated[str | None, Depends(get_idempotency_key)] = None,
) -> Job | Response:
    """Create a pending quantity takeoff job for an active drawing revision."""

    fingerprint = _build_idempotency_fingerprint(
        f"revisions.quantity_takeoffs:{revision_id}",
        {"revision_id": str(revision_id)},
    )

    async def _preclaim() -> Response | None:
        revision = await _get_active_revision(revision_id, db)
        if revision is None:
            raise_not_found("Drawing revision", str(revision_id))

        await _get_active_revision_manifest_or_409(revision_id, db)
        report = await _get_active_validation_report_or_404(revision_id, db)
        if report.quantity_gate in {
            QuantityGate.REVIEW_GATED.value,
            QuantityGate.BLOCKED.value,
        }:
            _raise_quantity_takeoff_gate_invalid(report)
        return None

    async def _mutate() -> IdempotentMutationSuccess[Job] | IdempotentMutationKnownError:
        revision = await _get_active_revision(revision_id, db, for_update=True)
        if revision is None:
            raise_not_found("Drawing revision", str(revision_id))

        quantity_job = Job(
            id=uuid4(),
            project_id=revision.project_id,
            file_id=revision.source_file_id,
            extraction_profile_id=None,
            base_revision_id=revision.id,
            parent_job_id=None,
            job_type=JobType.QUANTITY_TAKEOFF.value,
            status="pending",
            attempts=0,
            max_attempts=3,
            enqueue_status="pending",
            enqueue_attempts=0,
            cancel_requested=False,
            error_code=None,
            error_message=None,
            started_at=None,
            finished_at=None,
        )
        db.add(quantity_job)
        _prepare_job_enqueue_intent(quantity_job)
        await db.flush()
        await db.refresh(quantity_job)

        async def _after_commit() -> None:
            await _publish_job_enqueue_intent(
                quantity_job.id,
                publisher=_revisions_module().enqueue_quantity_takeoff_job,
                suppress_exceptions=True,
            )

        return IdempotentMutationSuccess(
            quantity_job,
            status.HTTP_202_ACCEPTED,
            after_commit=_after_commit,
        )

    return await run_idempotent_mutation(
        db,
        key=idempotency_key,
        fingerprint=fingerprint,
        method="POST",
        path=f"/revisions/{revision_id}/quantity-takeoffs",
        preclaim=_preclaim,
        mutate=_mutate,
        serialize_result=lambda job: JobRead.model_validate(job).model_dump(mode="json"),
        ops=_idempotent_mutation_ops(),
    )
