"""Revision estimate routes."""

from datetime import UTC, date, datetime
from typing import Annotated, Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Query, status
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
from app.api.pagination import DEFAULT_PAGE_SIZE as _DEFAULT_PAGE_SIZE
from app.api.pagination import MAX_PAGE_SIZE as _MAX_PAGE_SIZE
from app.api.v1.revision_cursors import (
    _decode_estimate_item_cursor,
    _decode_estimate_snapshot_entry_cursor,
    _decode_timestamp_cursor,
    _encode_estimate_item_cursor,
    _encode_estimate_snapshot_entry_cursor,
    _encode_timestamp_cursor,
)
from app.api.v1.revision_estimate_inputs import (
    _build_estimate_job_input_payload as _build_estimate_job_input_payload_direct,
)
from app.api.v1.revision_estimate_inputs import (
    _build_mapped_instance as _build_mapped_instance_direct,
)
from app.api.v1.revision_estimate_inputs import (
    _catalog_selection_is_superseded as _catalog_selection_is_superseded_direct,
)
from app.api.v1.revision_estimate_inputs import (
    _first_present_attribute as _first_present_attribute_direct,
)
from app.api.v1.revision_estimate_inputs import (
    _get_estimate_catalog_selection as _get_estimate_catalog_selection_direct,
)
from app.api.v1.revision_estimate_inputs import (
    _load_app_model_modules as _load_app_model_modules_direct,
)
from app.api.v1.revision_estimate_inputs import (
    _mapped_attribute_keys as _mapped_attribute_keys_direct,
)
from app.api.v1.revision_estimate_inputs import (
    _normalize_estimate_request_body as _normalize_estimate_request_body_direct,
)
from app.api.v1.revision_estimate_inputs import (
    _NormalizedEstimateCatalogRef as _NormalizedEstimateCatalogRefDirect,
)
from app.api.v1.revision_estimate_inputs import (
    _quantity_item_is_executable as _quantity_item_is_executable_direct,
)
from app.api.v1.revision_estimate_inputs import (
    _raise_estimate_input_invalid as _raise_estimate_input_invalid_direct,
)
from app.api.v1.revision_estimate_inputs import (
    _raise_estimate_takeoff_gate_invalid as _raise_estimate_takeoff_gate_invalid_direct,
)
from app.api.v1.revision_estimate_inputs import (
    _resolve_catalog_model as _resolve_catalog_model_direct,
)
from app.api.v1.revision_estimate_inputs import (
    _resolve_catalog_supersession_model as _resolve_catalog_supersession_model_direct,
)
from app.api.v1.revision_estimate_inputs import (
    _resolve_estimate_catalog_refs as _resolve_estimate_catalog_refs_direct,
)
from app.api.v1.revision_estimate_inputs import (
    _resolve_estimate_job_model_classes as _resolve_estimate_job_model_classes_direct,
)
from app.api.v1.revision_estimate_inputs import (
    _resolve_estimate_pricing_contract as _resolve_estimate_pricing_contract_direct,
)
from app.api.v1.revision_lineage import (
    _get_active_revision as _get_active_revision_direct,
)
from app.api.v1.revision_lineage import (
    _get_revision_estimate_version_or_404 as _get_revision_estimate_version_or_404_direct,
)
from app.api.v1.revision_lineage import (
    _get_revision_quantity_takeoff_or_404 as _get_revision_quantity_takeoff_or_404_direct,
)
from app.core.exceptions import raise_not_found
from app.db.session import get_db
from app.jobs.worker import enqueue_estimate_job as _enqueue_estimate_job_direct
from app.jobs.worker import (
    prepare_job_enqueue_intent as _prepare_job_enqueue_intent_direct,
)
from app.jobs.worker import (
    publish_job_enqueue_intent as _publish_job_enqueue_intent_direct,
)
from app.models.drawing_revision import DrawingRevision
from app.models.estimate_version import EstimateItem, EstimateSnapshotEntry, EstimateVersion
from app.models.file import File
from app.models.job import Job, JobType
from app.models.project import Project
from app.models.quantity_takeoff import QuantityGate, QuantityItem, QuantityTakeoff
from app.schemas.estimate import (
    EstimateItemListResponse,
    EstimateItemRead,
    EstimateSnapshotEntryListResponse,
    EstimateSnapshotEntryRead,
    EstimateVersionCreateCatalogRef,
    EstimateVersionCreateRequest,
    EstimateVersionListResponse,
    EstimateVersionRead,
)
from app.schemas.job import JobRead

estimates_router = APIRouter()

_NormalizedEstimateCatalogRef = _NormalizedEstimateCatalogRefDirect


async def _get_active_revision(*args: Any, **kwargs: Any) -> DrawingRevision | None:
    """Route-local seam for active revision lookup."""

    return await _get_active_revision_direct(*args, **kwargs)


async def _get_revision_quantity_takeoff_or_404(
    revision_id: UUID,
    takeoff_id: UUID,
    db: AsyncSession,
) -> QuantityTakeoff:
    """Route-local seam for revision takeoff lookup."""

    return await _get_revision_quantity_takeoff_or_404_direct(revision_id, takeoff_id, db)


async def _get_revision_estimate_version_or_404(
    revision_id: UUID,
    estimate_version_id: UUID,
    db: AsyncSession,
) -> EstimateVersion:
    """Route-local seam for revision estimate lookup."""

    return await _get_revision_estimate_version_or_404_direct(
        revision_id,
        estimate_version_id,
        db,
    )


_build_idempotency_fingerprint = _build_idempotency_fingerprint_direct
_replay_idempotency_response = _replay_idempotency_response_direct
_claim_idempotency_response = _claim_idempotency_response_direct
_complete_idempotency_response = _complete_idempotency_response_direct
_prepare_job_enqueue_intent = _prepare_job_enqueue_intent_direct
_publish_job_enqueue_intent = _publish_job_enqueue_intent_direct
_raise_estimate_input_invalid = _raise_estimate_input_invalid_direct
_normalize_estimate_request_body = _normalize_estimate_request_body_direct


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


def enqueue_estimate_job(job_id: UUID) -> None:
    """Route-local estimate enqueue seam for test patching."""

    _enqueue_estimate_job_direct(job_id)


def _utc_now() -> datetime:
    """Route-local clock seam for estimate create flows."""

    return datetime.now(UTC)


def _raise_estimate_takeoff_gate_invalid(takeoff: QuantityTakeoff) -> None:
    """Raise the standard pre-enqueue error for non-runnable estimate inputs."""

    _raise_estimate_takeoff_gate_invalid_direct(
        takeoff,
        raise_estimate_input_invalid=_raise_estimate_input_invalid,
    )


def _mapped_attribute_keys(model_class: type[Any]) -> set[str]:
    """Return mapped SQLAlchemy attribute keys for a model class."""

    return _mapped_attribute_keys_direct(model_class)


def _build_mapped_instance(model_class: type[Any], values: dict[str, Any]) -> Any:
    """Instantiate a mapped object while ignoring unknown compatibility fields."""

    return _build_mapped_instance_direct(
        model_class,
        values,
        mapped_attribute_keys=_mapped_attribute_keys,
    )


def _first_present_attribute(instance: Any, names: tuple[str, ...]) -> Any:
    """Return the first present attribute value from a candidate name list."""

    return _first_present_attribute_direct(instance, names)


def _load_app_model_modules() -> None:
    """Import app model modules so mapped classes are registered."""

    _load_app_model_modules_direct()


def _resolve_estimate_job_model_classes() -> tuple[type[Any], type[Any]]:
    """Resolve mapped model classes used to persist estimate job inputs."""

    return _resolve_estimate_job_model_classes_direct(
        load_app_model_modules=_load_app_model_modules,
    )


def _resolve_catalog_model(ref_type: str) -> type[Any]:
    """Resolve the mapped catalog/formula model for a request ref type."""

    return _resolve_catalog_model_direct(
        ref_type,
        load_app_model_modules=_load_app_model_modules,
    )


def _resolve_catalog_supersession_model(ref_type: str) -> type[Any]:
    """Resolve the mapped supersession model for a request ref type."""

    return _resolve_catalog_supersession_model_direct(
        ref_type,
        load_app_model_modules=_load_app_model_modules,
    )


async def _catalog_selection_is_superseded(
    ref_type: str,
    selection_id: UUID,
    db: AsyncSession,
) -> bool:
    """Return whether a catalog selection has been superseded."""

    return await _catalog_selection_is_superseded_direct(
        ref_type,
        selection_id,
        db,
        resolve_catalog_supersession_model=_resolve_catalog_supersession_model,
    )


def _resolve_estimate_pricing_contract(
    request: EstimateVersionCreateRequest,
    *,
    job_created_at: datetime | None = None,
) -> tuple[str, date]:
    """Resolve the persisted pricing contract for an estimate request."""

    return _resolve_estimate_pricing_contract_direct(
        request,
        job_created_at=job_created_at,
    )


def _quantity_item_is_executable(quantity_item: QuantityItem) -> bool:
    """Return whether a quantity item can back a rate/material estimate ref."""

    return _quantity_item_is_executable_direct(quantity_item)


async def _get_estimate_catalog_selection(
    ref_type: str,
    selection_id: UUID,
    db: AsyncSession,
) -> Any | None:
    """Return the selected catalog/formula row for a request ref."""

    return await _get_estimate_catalog_selection_direct(
        ref_type,
        selection_id,
        db,
        resolve_catalog_model=_resolve_catalog_model,
    )


async def _resolve_estimate_catalog_refs(
    revision: DrawingRevision,
    takeoff: QuantityTakeoff,
    request_refs: list[EstimateVersionCreateCatalogRef],
    pricing_effective_date: date,
    db: AsyncSession,
) -> list[_NormalizedEstimateCatalogRef]:
    """Validate request refs and normalize worker mapping inputs."""

    return await _resolve_estimate_catalog_refs_direct(
        revision,
        takeoff,
        request_refs,
        pricing_effective_date,
        db,
        raise_estimate_input_invalid=_raise_estimate_input_invalid,
        get_estimate_catalog_selection=_get_estimate_catalog_selection,
        catalog_selection_is_superseded=_catalog_selection_is_superseded,
        first_present_attribute=_first_present_attribute,
        quantity_item_is_executable=_quantity_item_is_executable,
    )


def _build_estimate_job_input_payload(
    estimate_job: Job,
    revision: DrawingRevision,
    takeoff: QuantityTakeoff,
    request: EstimateVersionCreateRequest,
    normalized_refs: list[_NormalizedEstimateCatalogRef],
    *,
    pricing_mode: str,
    pricing_effective_date: date,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Build persistence payloads for estimate job input rows."""

    return _build_estimate_job_input_payload_direct(
        estimate_job,
        revision,
        takeoff,
        request,
        normalized_refs,
        pricing_mode=pricing_mode,
        pricing_effective_date=pricing_effective_date,
    )


@estimates_router.get(
    "/revisions/{revision_id}/estimates",
    response_model=EstimateVersionListResponse,
)
async def list_revision_estimates(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
) -> EstimateVersionListResponse:
    """List committed estimate versions for an active drawing revision."""

    revision = await _get_active_revision(revision_id, db)
    if revision is None:
        raise_not_found("Drawing revision", str(revision_id))

    pagination_cursor = _decode_timestamp_cursor(cursor) if cursor else None
    query = (
        select(EstimateVersion)
        .join(
            File,
            (File.id == EstimateVersion.source_file_id)
            & (File.project_id == EstimateVersion.project_id),
        )
        .join(Project, Project.id == EstimateVersion.project_id)
        .where(
            (EstimateVersion.drawing_revision_id == revision_id)
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )
    if pagination_cursor is not None:
        created_at, row_id = pagination_cursor
        query = query.where(
            (EstimateVersion.created_at > created_at)
            | ((EstimateVersion.created_at == created_at) & (EstimateVersion.id > row_id))
        )

    result = await db.execute(
        query.order_by(EstimateVersion.created_at.asc(), EstimateVersion.id.asc()).limit(limit + 1)
    )
    rows = result.scalars().all()
    page = rows[:limit]
    next_cursor = None
    if len(rows) > limit and page:
        last_row = page[-1]
        next_cursor = _encode_timestamp_cursor(last_row.created_at, last_row.id)

    return EstimateVersionListResponse(
        items=[EstimateVersionRead.model_validate(row) for row in page],
        next_cursor=next_cursor,
    )


@estimates_router.post(
    "/revisions/{revision_id}/quantity-takeoffs/{takeoff_id}/estimate-versions",
    response_model=JobRead,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_revision_estimate_version(
    revision_id: UUID,
    takeoff_id: UUID,
    request: EstimateVersionCreateRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    idempotency_key: Annotated[str | None, Depends(get_idempotency_key)] = None,
) -> Job | Response:
    """Create a pending estimate job for an active revision quantity takeoff."""

    if request.pricing.currency != "GBP":
        _raise_estimate_input_invalid(
            "Estimate jobs only support GBP pricing inputs.",
            details={"currency": request.pricing.currency},
        )

    normalized_body = _normalize_estimate_request_body(request)
    fingerprint = _build_idempotency_fingerprint(
        f"revisions.quantity_takeoffs.estimate_versions:{revision_id}:{takeoff_id}",
        {
            "revision_id": str(revision_id),
            "takeoff_id": str(takeoff_id),
            "body": normalized_body,
        },
    )
    job_created_at: datetime | None = None
    pricing_mode: str | None = None
    pricing_effective_date: date | None = None
    normalized_refs: list[_NormalizedEstimateCatalogRef] | None = None

    async def _preclaim() -> Response | None:
        nonlocal job_created_at, pricing_mode, pricing_effective_date, normalized_refs

        revision = await _get_active_revision(revision_id, db)
        if revision is None:
            raise_not_found("Drawing revision", str(revision_id))
        assert revision is not None

        takeoff = await _get_revision_quantity_takeoff_or_404(revision_id, takeoff_id, db)
        if takeoff.quantity_gate != QuantityGate.ALLOWED.value or not takeoff.trusted_totals:
            _raise_estimate_takeoff_gate_invalid(takeoff)

        job_created_at = _utc_now()
        pricing_mode, pricing_effective_date = _resolve_estimate_pricing_contract(
            request,
            job_created_at=job_created_at,
        )
        normalized_refs = await _resolve_estimate_catalog_refs(
            revision,
            takeoff,
            request.catalog_refs,
            pricing_effective_date,
            db,
        )
        return None

    async def _mutate() -> IdempotentMutationSuccess[Job] | IdempotentMutationKnownError:
        revision = await _get_active_revision(revision_id, db, for_update=True)
        if revision is None:
            raise_not_found("Drawing revision", str(revision_id))
        assert revision is not None

        takeoff = await _get_revision_quantity_takeoff_or_404(revision_id, takeoff_id, db)
        if takeoff.quantity_gate != QuantityGate.ALLOWED.value or not takeoff.trusted_totals:
            _raise_estimate_takeoff_gate_invalid(takeoff)
        assert job_created_at is not None
        assert pricing_mode is not None
        assert pricing_effective_date is not None
        assert normalized_refs is not None

        estimate_job = Job(
            id=uuid4(),
            project_id=revision.project_id,
            file_id=revision.source_file_id,
            extraction_profile_id=None,
            base_revision_id=revision.id,
            parent_job_id=(
                takeoff.source_job_id
                if takeoff.project_id == revision.project_id
                and takeoff.source_file_id == revision.source_file_id
                else None
            ),
            job_type=JobType.ESTIMATE.value,
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
            created_at=job_created_at,
        )
        db.add(estimate_job)
        _prepare_job_enqueue_intent(estimate_job)
        await db.flush()
        await db.refresh(estimate_job)

        estimate_job_input_model, estimate_job_input_ref_model = (
            _resolve_estimate_job_model_classes()
        )
        input_payload, ref_payloads = _build_estimate_job_input_payload(
            estimate_job,
            revision,
            takeoff,
            request,
            normalized_refs,
            pricing_mode=pricing_mode,
            pricing_effective_date=pricing_effective_date,
        )
        db.add(_build_mapped_instance(estimate_job_input_model, input_payload))
        for ref_payload in ref_payloads:
            db.add(_build_mapped_instance(estimate_job_input_ref_model, ref_payload))

        async def _after_commit() -> None:
            await _publish_job_enqueue_intent(
                estimate_job.id,
                publisher=enqueue_estimate_job,
                suppress_exceptions=True,
            )

        return IdempotentMutationSuccess(
            estimate_job,
            status.HTTP_202_ACCEPTED,
            after_commit=_after_commit,
        )

    return await run_idempotent_mutation(
        db,
        key=idempotency_key,
        fingerprint=fingerprint,
        method="POST",
        path=f"/revisions/{revision_id}/quantity-takeoffs/{takeoff_id}/estimate-versions",
        preclaim=_preclaim,
        mutate=_mutate,
        serialize_result=lambda job: JobRead.model_validate(job).model_dump(mode="json"),
        ops=_idempotent_mutation_ops(),
    )


@estimates_router.get(
    "/revisions/{revision_id}/estimates/{estimate_version_id}",
    response_model=EstimateVersionRead,
)
async def get_revision_estimate(
    revision_id: UUID,
    estimate_version_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> EstimateVersionRead:
    """Return a committed estimate version for an active drawing revision."""

    revision = await _get_active_revision(revision_id, db)
    if revision is None:
        raise_not_found("Drawing revision", str(revision_id))

    estimate_version = await _get_revision_estimate_version_or_404(
        revision_id, estimate_version_id, db
    )
    return EstimateVersionRead.model_validate(estimate_version)


@estimates_router.get(
    "/revisions/{revision_id}/estimates/{estimate_version_id}/items",
    response_model=EstimateItemListResponse,
)
async def list_revision_estimate_items(
    revision_id: UUID,
    estimate_version_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
) -> EstimateItemListResponse:
    """List committed estimate items for a revision-scoped estimate version."""

    revision = await _get_active_revision(revision_id, db)
    if revision is None:
        raise_not_found("Drawing revision", str(revision_id))

    await _get_revision_estimate_version_or_404(revision_id, estimate_version_id, db)
    pagination_cursor = _decode_estimate_item_cursor(cursor) if cursor else None
    query = (
        select(EstimateItem)
        .join(
            EstimateVersion,
            (EstimateVersion.id == EstimateItem.estimate_version_id)
            & (EstimateVersion.project_id == EstimateItem.project_id)
            & (EstimateVersion.drawing_revision_id == EstimateItem.drawing_revision_id),
        )
        .join(
            File,
            (File.id == EstimateVersion.source_file_id)
            & (File.project_id == EstimateVersion.project_id),
        )
        .join(Project, Project.id == EstimateVersion.project_id)
        .where(
            (EstimateItem.estimate_version_id == estimate_version_id)
            & (EstimateItem.drawing_revision_id == revision_id)
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )
    if pagination_cursor is not None:
        line_number, row_id = pagination_cursor
        query = query.where(
            (EstimateItem.line_number > line_number)
            | ((EstimateItem.line_number == line_number) & (EstimateItem.id > row_id))
        )

    result = await db.execute(
        query.order_by(EstimateItem.line_number.asc(), EstimateItem.id.asc()).limit(limit + 1)
    )
    rows = result.scalars().all()
    page = rows[:limit]
    next_cursor = None
    if len(rows) > limit and page:
        last_row = page[-1]
        next_cursor = _encode_estimate_item_cursor(last_row.line_number, last_row.id)

    return EstimateItemListResponse(
        items=[EstimateItemRead.model_validate(row) for row in page],
        next_cursor=next_cursor,
    )


@estimates_router.get(
    "/revisions/{revision_id}/estimates/{estimate_version_id}/snapshot-entries",
    response_model=EstimateSnapshotEntryListResponse,
)
async def list_revision_estimate_snapshot_entries(
    revision_id: UUID,
    estimate_version_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
) -> EstimateSnapshotEntryListResponse:
    """List committed estimate snapshot entries for a revision-scoped estimate version."""

    revision = await _get_active_revision(revision_id, db)
    if revision is None:
        raise_not_found("Drawing revision", str(revision_id))

    await _get_revision_estimate_version_or_404(revision_id, estimate_version_id, db)
    pagination_cursor = _decode_estimate_snapshot_entry_cursor(cursor) if cursor else None
    query = (
        select(EstimateSnapshotEntry)
        .join(
            EstimateVersion,
            (EstimateVersion.id == EstimateSnapshotEntry.estimate_version_id)
            & (EstimateVersion.project_id == EstimateSnapshotEntry.project_id)
            & (EstimateVersion.drawing_revision_id == EstimateSnapshotEntry.drawing_revision_id),
        )
        .join(
            File,
            (File.id == EstimateVersion.source_file_id)
            & (File.project_id == EstimateVersion.project_id),
        )
        .join(Project, Project.id == EstimateVersion.project_id)
        .where(
            (EstimateSnapshotEntry.estimate_version_id == estimate_version_id)
            & (EstimateSnapshotEntry.drawing_revision_id == revision_id)
            & (File.deleted_at.is_(None))
            & (Project.deleted_at.is_(None))
        )
    )
    if pagination_cursor is not None:
        sort_order, row_id = pagination_cursor
        query = query.where(
            (EstimateSnapshotEntry.sort_order > sort_order)
            | (
                (EstimateSnapshotEntry.sort_order == sort_order)
                & (EstimateSnapshotEntry.id > row_id)
            )
        )

    result = await db.execute(
        query.order_by(
            EstimateSnapshotEntry.sort_order.asc(), EstimateSnapshotEntry.id.asc()
        ).limit(limit + 1)
    )
    rows = result.scalars().all()
    page = rows[:limit]
    next_cursor = None
    if len(rows) > limit and page:
        last_row = page[-1]
        next_cursor = _encode_estimate_snapshot_entry_cursor(last_row.sort_order, last_row.id)

    return EstimateSnapshotEntryListResponse(
        items=[EstimateSnapshotEntryRead.model_validate(row) for row in page],
        next_cursor=next_cursor,
    )
