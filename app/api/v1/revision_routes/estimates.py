"""Revision estimate routes."""

from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime
from typing import Annotated, Any, cast
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Query, status
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.idempotency import (
    IdempotencyReservation,
    get_idempotency_key,
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
from app.jobs import worker as jobs_worker_module
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

_DEFAULT_PAGE_SIZE = 50
_MAX_PAGE_SIZE = 200
_MISSING = object()

type _NormalizedEstimateCatalogRef = _NormalizedEstimateCatalogRefDirect

type _ActiveRevisionGetter = Callable[..., Awaitable[DrawingRevision | None]]
type _RevisionTakeoffGetter = Callable[[UUID, UUID, AsyncSession], Awaitable[QuantityTakeoff]]
type _EstimateVersionGetter = Callable[[UUID, UUID, AsyncSession], Awaitable[EstimateVersion]]
type _FingerprintBuilder = Callable[..., str]
type _ReplayIdempotencyResponse = Callable[..., Awaitable[Response | None]]
type _CompleteIdempotencyResponse = Callable[..., Awaitable[Response]]
type _Clock = Callable[[], datetime]
type _EstimateRequestNormalizer = Callable[[EstimateVersionCreateRequest], dict[str, Any]]
type _EstimateJobModelResolver = Callable[[], tuple[type[Any], type[Any]]]
type _EstimatePricingContractResolver = Callable[..., tuple[str, date]]
type _EstimateCatalogRefResolver = Callable[
    [DrawingRevision, QuantityTakeoff, list[EstimateVersionCreateCatalogRef], date, AsyncSession],
    Awaitable[list[_NormalizedEstimateCatalogRef]],
]
type _EstimateJobInputPayloadBuilder = Callable[..., tuple[dict[str, Any], list[dict[str, Any]]]]


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


async def _get_revision_estimate_version_or_404(
    revision_id: UUID,
    estimate_version_id: UUID,
    db: AsyncSession,
) -> EstimateVersion:
    """Compatibility wrapper for revision estimate lookup."""

    helper = cast(
        _EstimateVersionGetter,
        _compat_attr(
            "_get_revision_estimate_version_or_404",
            "get_revision_estimate_version_or_404",
            default=_get_revision_estimate_version_or_404_direct,
        ),
    )
    return await helper(revision_id, estimate_version_id, db)


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


def enqueue_estimate_job(job_id: UUID) -> None:
    """Compatibility wrapper kept for test fixture patching."""

    publisher = _compat_attr(
        "enqueue_estimate_job",
        default=jobs_worker_module.enqueue_estimate_job,
    )
    publisher(job_id)


def _revisions_module() -> Any:
    """Return the revisions module for compatibility-sensitive callbacks."""

    from app.api.v1 import revisions

    return revisions


def _utc_now() -> datetime:
    """Compatibility wrapper for the facade clock."""

    helper = cast(
        _Clock,
        _compat_attr(
            "utc_now",
            default=lambda: datetime.now(UTC),
        ),
    )
    return helper()


def _raise_estimate_input_invalid(
    message: str,
    *,
    details: dict[str, Any] | None = None,
) -> None:
    """Raise the standard estimate input validation error."""

    helper = _compat_attr(
        "_raise_estimate_input_invalid",
        "raise_estimate_input_invalid",
        default=_raise_estimate_input_invalid_direct,
    )
    helper(message, details=details)


def _raise_estimate_takeoff_gate_invalid(takeoff: QuantityTakeoff) -> None:
    """Raise the standard pre-enqueue error for non-runnable estimate inputs."""

    helper = _compat_attr(
        "_raise_estimate_takeoff_gate_invalid",
        "raise_estimate_takeoff_gate_invalid",
        default=None,
    )
    if helper is not None:
        helper(takeoff)
        return

    _raise_estimate_takeoff_gate_invalid_direct(
        takeoff,
        raise_estimate_input_invalid=_raise_estimate_input_invalid,
    )


def _normalize_estimate_request_body(payload: EstimateVersionCreateRequest) -> dict[str, Any]:
    """Return the normalized create-request body used for idempotency fingerprints."""

    helper = cast(
        _EstimateRequestNormalizer,
        _compat_attr(
            "_normalize_estimate_request_body",
            "normalize_estimate_request_body",
            default=_normalize_estimate_request_body_direct,
        ),
    )
    return helper(payload)


def _mapped_attribute_keys(model_class: type[Any]) -> set[str]:
    """Return mapped SQLAlchemy attribute keys for a model class."""

    return _mapped_attribute_keys_direct(model_class)


def _build_mapped_instance(model_class: type[Any], values: dict[str, Any]) -> Any:
    """Instantiate a mapped object while ignoring unknown compatibility fields."""

    helper = _compat_attr(
        "_build_mapped_instance",
        "build_mapped_instance",
        default=None,
    )
    if helper is not None:
        return helper(model_class, values)

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

    helper = cast(
        _EstimateJobModelResolver | None,
        _compat_attr(
            "_resolve_estimate_job_model_classes",
            "resolve_estimate_job_model_classes",
            default=None,
        ),
    )
    if helper is not None:
        return helper()

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

    helper = cast(
        _EstimatePricingContractResolver | None,
        _compat_attr(
            "_resolve_estimate_pricing_contract",
            "resolve_estimate_pricing_contract",
            default=None,
        ),
    )
    if helper is not None:
        return helper(request, job_created_at=job_created_at)

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

    helper = cast(
        _EstimateCatalogRefResolver | None,
        _compat_attr(
            "_resolve_estimate_catalog_refs",
            "resolve_estimate_catalog_refs",
            default=None,
        ),
    )
    if helper is not None:
        return await helper(
            revision,
            takeoff,
            request_refs,
            pricing_effective_date,
            db,
        )

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

    helper = cast(
        _EstimateJobInputPayloadBuilder | None,
        _compat_attr(
            "_build_estimate_job_input_payload",
            "build_estimate_job_input_payload",
            default=None,
        ),
    )
    if helper is not None:
        return helper(
            estimate_job,
            revision,
            takeoff,
            request,
            normalized_refs,
            pricing_mode=pricing_mode,
            pricing_effective_date=pricing_effective_date,
        )

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
    reservation: IdempotencyReservation | None = None
    fingerprint: str | None = None
    if idempotency_key is not None:
        fingerprint = _build_idempotency_fingerprint(
            f"revisions.quantity_takeoffs.estimate_versions:{revision_id}:{takeoff_id}",
            {
                "revision_id": str(revision_id),
                "takeoff_id": str(takeoff_id),
                "body": normalized_body,
            },
        )
        replay_response = await _replay_idempotency_response(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
        )
        if replay_response is not None:
            return replay_response

    revision = await _get_active_revision(revision_id, db)
    if revision is None:
        raise_not_found("Drawing revision", str(revision_id))
    assert revision is not None

    takeoff = await _get_revision_quantity_takeoff_or_404(revision_id, takeoff_id, db)
    if takeoff.quantity_gate != QuantityGate.ALLOWED.value or not takeoff.trusted_totals:
        _raise_estimate_takeoff_gate_invalid(takeoff)

    locked_revision = await _get_active_revision(revision_id, db, for_update=True)
    if locked_revision is None:
        raise_not_found("Drawing revision", str(revision_id))
    assert locked_revision is not None
    revision = locked_revision
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

    if idempotency_key is not None:
        assert fingerprint is not None
        claim = await _claim_idempotency_response(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
            method="POST",
            path=f"/revisions/{revision_id}/quantity-takeoffs/{takeoff_id}/estimate-versions",
        )
        if isinstance(claim, Response):
            return claim
        assert claim is not None
        reservation = claim

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

    estimate_job_input_model, estimate_job_input_ref_model = _resolve_estimate_job_model_classes()
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

    success_response: Response | None = None
    if reservation is not None:
        success_response = await _complete_idempotency_response(
            db,
            reservation,
            status_code=status.HTTP_202_ACCEPTED,
            response_body=JobRead.model_validate(estimate_job).model_dump(mode="json"),
        )

    await db.commit()
    await _publish_job_enqueue_intent(
        estimate_job.id,
        publisher=_revisions_module().enqueue_estimate_job,
        suppress_exceptions=True,
    )

    if success_response is not None:
        return success_response

    return estimate_job


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
