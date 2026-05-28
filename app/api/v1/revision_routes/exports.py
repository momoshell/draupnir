"""Revision export creation routes."""

from collections.abc import Awaitable, Callable
from typing import Annotated, Any, cast
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, status
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.idempotency import IdempotencyReservation, get_idempotency_key
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
from app.api.v1.revision_lineage import _get_active_revision as _get_active_revision_direct
from app.api.v1.revision_lineage import (
    _get_revision_estimate_version_or_404 as _get_revision_estimate_version_or_404_direct,
)
from app.api.v1.revision_lineage import (
    _get_revision_quantity_takeoff_or_404 as _get_revision_quantity_takeoff_or_404_direct,
)
from app.core.exceptions import raise_not_found
from app.db.session import get_db
from app.jobs.worker import prepare_job_enqueue_intent as _prepare_job_enqueue_intent_direct
from app.models.drawing_revision import DrawingRevision
from app.models.estimate_version import EstimateVersion
from app.models.export_job_input import ExportJobInput
from app.models.job import Job, JobType
from app.models.quantity_takeoff import QuantityGate, QuantityTakeoff
from app.schemas.export import (
    EXPORT_KIND_MATRIX,
    EstimateExportCreateRequest,
    ExportJobInputCreate,
    ExportKind,
    QuantityCsvExportCreateRequest,
    RevisionJsonExportCreateRequest,
)
from app.schemas.job import JobRead

exports_router = APIRouter()

_MISSING = object()

type _ActiveRevisionGetter = Callable[..., Awaitable[DrawingRevision | None]]
type _RevisionTakeoffGetter = Callable[[UUID, UUID, AsyncSession], Awaitable[QuantityTakeoff]]
type _EstimateVersionGetter = Callable[[UUID, UUID, AsyncSession], Awaitable[EstimateVersion]]
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


async def _get_locked_active_revision_or_404(
    revision_id: UUID,
    db: AsyncSession,
) -> DrawingRevision:
    """Return the active revision under a row lock or raise 404."""

    revision = await _get_active_revision(revision_id, db, for_update=True)
    if revision is None:
        raise_not_found("Drawing revision", str(revision_id))
    return cast(DrawingRevision, revision)


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


def _raise_estimate_takeoff_gate_invalid(takeoff: QuantityTakeoff) -> None:
    """Raise the standard invalid-takeoff export error."""

    helper = _compat_attr(
        "_raise_estimate_takeoff_gate_invalid",
        "raise_estimate_takeoff_gate_invalid",
    )
    helper(takeoff)


def _build_export_request_body(
    export_kind: ExportKind,
    *,
    options: dict[str, Any],
) -> dict[str, Any]:
    """Return the normalized request payload for idempotency fingerprints."""

    return {
        "export_kind": export_kind.value,
        "options": options,
    }


def _parent_job_id_for_revision(revision: DrawingRevision) -> UUID | None:
    """Return the source parent job id for revision-scoped exports."""

    return revision.source_job_id


def _parent_job_id_for_takeoff(
    revision: DrawingRevision,
    takeoff: QuantityTakeoff,
) -> UUID | None:
    """Return the source parent job id for takeoff-scoped exports."""

    if (
        takeoff.project_id == revision.project_id
        and takeoff.source_file_id == revision.source_file_id
    ):
        return takeoff.source_job_id
    return None


def _parent_job_id_for_estimate(
    revision: DrawingRevision,
    estimate_version: EstimateVersion,
) -> UUID | None:
    """Return the source parent job id for estimate-scoped exports."""

    if (
        estimate_version.project_id == revision.project_id
        and estimate_version.source_file_id == revision.source_file_id
    ):
        return estimate_version.source_job_id
    return None


def _build_export_job(
    revision: DrawingRevision,
    *,
    parent_job_id: UUID | None,
) -> Job:
    """Return a pending export job instance."""

    return Job(
        id=uuid4(),
        project_id=revision.project_id,
        file_id=revision.source_file_id,
        extraction_profile_id=None,
        base_revision_id=revision.id,
        parent_job_id=parent_job_id,
        job_type=JobType.EXPORT.value,
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


def _build_export_job_input(
    export_job: Job,
    revision: DrawingRevision,
    *,
    export_kind: ExportKind,
    options_json: dict[str, Any],
    quantity_takeoff: QuantityTakeoff | None = None,
    estimate_version: EstimateVersion | None = None,
) -> ExportJobInput:
    """Return the persisted immutable export job input row."""

    export_format, media_type = EXPORT_KIND_MATRIX[export_kind]
    payload = ExportJobInputCreate(
        export_kind=export_kind,
        export_format=export_format,
        media_type=media_type,
        options_json=options_json,
        quantity_takeoff_id=(quantity_takeoff.id if quantity_takeoff is not None else None),
        estimate_version_id=(estimate_version.id if estimate_version is not None else None),
    ).model_dump()
    payload.update(
        source_job_id=export_job.id,
        project_id=revision.project_id,
        source_file_id=revision.source_file_id,
        drawing_revision_id=revision.id,
        quantity_gate=(
            estimate_version.quantity_gate
            if estimate_version is not None
            else quantity_takeoff.quantity_gate
            if quantity_takeoff is not None
            else None
        ),
        trusted_totals=(
            estimate_version.trusted_totals
            if estimate_version is not None
            else quantity_takeoff.trusted_totals
            if quantity_takeoff is not None
            else None
        ),
    )
    return ExportJobInput(**payload)


def _takeoff_is_exportable(takeoff: QuantityTakeoff) -> bool:
    """Return whether a takeoff is trusted and allowed for export."""

    return takeoff.quantity_gate == QuantityGate.ALLOWED.value and takeoff.trusted_totals


def _estimate_belongs_to_takeoff(
    estimate_version: EstimateVersion,
    takeoff_id: UUID,
) -> bool:
    """Return whether the estimate version belongs to the path takeoff."""

    return estimate_version.quantity_takeoff_id == takeoff_id


async def _get_exportable_takeoff_or_404(
    revision_id: UUID,
    takeoff_id: UUID,
    db: AsyncSession,
) -> QuantityTakeoff:
    """Return an exportable takeoff or raise the standard route error."""

    takeoff = await _get_revision_quantity_takeoff_or_404(revision_id, takeoff_id, db)
    if not _takeoff_is_exportable(takeoff):
        _raise_estimate_takeoff_gate_invalid(takeoff)
    return takeoff


async def _get_exportable_estimate_or_404(
    revision_id: UUID,
    takeoff_id: UUID,
    estimate_version_id: UUID,
    db: AsyncSession,
) -> tuple[QuantityTakeoff, EstimateVersion]:
    """Return the exportable takeoff/estimate pair for the route."""

    takeoff = await _get_exportable_takeoff_or_404(revision_id, takeoff_id, db)
    estimate_version = await _get_revision_estimate_version_or_404(
        revision_id,
        estimate_version_id,
        db,
    )
    if not _estimate_belongs_to_takeoff(estimate_version, takeoff.id):
        raise_not_found("Estimate version", str(estimate_version_id))
    return takeoff, estimate_version


@exports_router.post(
    "/revisions/{revision_id}/exports/revision-json",
    response_model=JobRead,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_revision_json_export(
    revision_id: UUID,
    request: RevisionJsonExportCreateRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    idempotency_key: Annotated[str | None, Depends(get_idempotency_key)] = None,
) -> Job | Response:
    """Create a pending revision JSON export job for an active drawing revision."""

    export_kind = ExportKind.REVISION_JSON
    normalized_body = _build_export_request_body(
        export_kind,
        options=request.options,
    )
    reservation: IdempotencyReservation | None = None
    fingerprint: str | None = None
    if idempotency_key is not None:
        fingerprint = _build_idempotency_fingerprint(
            f"revisions.exports.revision_json:{revision_id}",
            {
                "revision_id": str(revision_id),
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

    revision = await _get_locked_active_revision_or_404(revision_id, db)

    if idempotency_key is not None:
        assert fingerprint is not None
        claim = await _claim_idempotency_response(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
            method="POST",
            path=f"/revisions/{revision_id}/exports/revision-json",
        )
        if isinstance(claim, Response):
            return claim
        assert claim is not None
        reservation = claim
        revision = await _get_locked_active_revision_or_404(revision_id, db)

    export_job = _build_export_job(
        revision,
        parent_job_id=_parent_job_id_for_revision(revision),
    )
    db.add(export_job)
    _prepare_job_enqueue_intent(export_job)
    await db.flush()
    await db.refresh(export_job)

    db.add(
        _build_export_job_input(
            export_job,
            revision,
            export_kind=export_kind,
            options_json=request.options,
        )
    )

    success_response: Response | None = None
    if reservation is not None:
        success_response = await _complete_idempotency_response(
            db,
            reservation,
            status_code=status.HTTP_202_ACCEPTED,
            response_body=JobRead.model_validate(export_job).model_dump(mode="json"),
        )

    await db.commit()

    if success_response is not None:
        return success_response
    return export_job


@exports_router.post(
    "/revisions/{revision_id}/quantity-takeoffs/{takeoff_id}/exports/quantity-csv",
    response_model=JobRead,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_revision_quantity_csv_export(
    revision_id: UUID,
    takeoff_id: UUID,
    request: QuantityCsvExportCreateRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    idempotency_key: Annotated[str | None, Depends(get_idempotency_key)] = None,
) -> Job | Response:
    """Create a pending quantity CSV export job for a trusted revision takeoff."""

    export_kind = ExportKind.QUANTITY_CSV
    normalized_body = _build_export_request_body(
        export_kind,
        options=request.options,
    )
    reservation: IdempotencyReservation | None = None
    fingerprint: str | None = None
    if idempotency_key is not None:
        fingerprint = _build_idempotency_fingerprint(
            (f"revisions.quantity_takeoffs.exports.quantity_csv:{revision_id}:{takeoff_id}"),
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

    revision = await _get_locked_active_revision_or_404(revision_id, db)
    takeoff = await _get_exportable_takeoff_or_404(revision_id, takeoff_id, db)

    if idempotency_key is not None:
        assert fingerprint is not None
        claim = await _claim_idempotency_response(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
            method="POST",
            path=(f"/revisions/{revision_id}/quantity-takeoffs/{takeoff_id}/exports/quantity-csv"),
        )
        if isinstance(claim, Response):
            return claim
        assert claim is not None
        reservation = claim
        revision = await _get_locked_active_revision_or_404(revision_id, db)
        takeoff = await _get_exportable_takeoff_or_404(revision_id, takeoff_id, db)

    export_job = _build_export_job(
        revision,
        parent_job_id=_parent_job_id_for_takeoff(revision, takeoff),
    )
    db.add(export_job)
    _prepare_job_enqueue_intent(export_job)
    await db.flush()
    await db.refresh(export_job)

    db.add(
        _build_export_job_input(
            export_job,
            revision,
            export_kind=export_kind,
            options_json=request.options,
            quantity_takeoff=takeoff,
        )
    )

    success_response: Response | None = None
    if reservation is not None:
        success_response = await _complete_idempotency_response(
            db,
            reservation,
            status_code=status.HTTP_202_ACCEPTED,
            response_body=JobRead.model_validate(export_job).model_dump(mode="json"),
        )

    await db.commit()

    if success_response is not None:
        return success_response
    return export_job


@exports_router.post(
    "/revisions/{revision_id}/quantity-takeoffs/{takeoff_id}/estimates/{estimate_version_id}/exports",
    response_model=JobRead,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_revision_estimate_export(
    revision_id: UUID,
    takeoff_id: UUID,
    estimate_version_id: UUID,
    request: EstimateExportCreateRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    idempotency_key: Annotated[str | None, Depends(get_idempotency_key)] = None,
) -> Job | Response:
    """Create a pending estimate CSV/PDF export job for a trusted takeoff."""

    normalized_body = request.model_dump(mode="json")
    reservation: IdempotencyReservation | None = None
    fingerprint: str | None = None
    if idempotency_key is not None:
        fingerprint = _build_idempotency_fingerprint(
            (
                "revisions.quantity_takeoffs.estimates.exports:"
                f"{revision_id}:{takeoff_id}:{estimate_version_id}"
            ),
            {
                "revision_id": str(revision_id),
                "takeoff_id": str(takeoff_id),
                "estimate_version_id": str(estimate_version_id),
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

    revision = await _get_locked_active_revision_or_404(revision_id, db)
    takeoff, estimate_version = await _get_exportable_estimate_or_404(
        revision_id,
        takeoff_id,
        estimate_version_id,
        db,
    )

    if idempotency_key is not None:
        assert fingerprint is not None
        claim = await _claim_idempotency_response(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
            method="POST",
            path=(
                "/revisions/"
                f"{revision_id}/quantity-takeoffs/{takeoff_id}/estimates/"
                f"{estimate_version_id}/exports"
            ),
        )
        if isinstance(claim, Response):
            return claim
        assert claim is not None
        reservation = claim
        revision = await _get_locked_active_revision_or_404(revision_id, db)
        takeoff, estimate_version = await _get_exportable_estimate_or_404(
            revision_id,
            takeoff_id,
            estimate_version_id,
            db,
        )

    export_job = _build_export_job(
        revision,
        parent_job_id=_parent_job_id_for_estimate(revision, estimate_version),
    )
    db.add(export_job)
    _prepare_job_enqueue_intent(export_job)
    await db.flush()
    await db.refresh(export_job)

    db.add(
        _build_export_job_input(
            export_job,
            revision,
            export_kind=request.export_kind,
            options_json=request.options,
            quantity_takeoff=takeoff,
            estimate_version=estimate_version,
        )
    )

    success_response: Response | None = None
    if reservation is not None:
        success_response = await _complete_idempotency_response(
            db,
            reservation,
            status_code=status.HTTP_202_ACCEPTED,
            response_body=JobRead.model_validate(export_job).model_dump(mode="json"),
        )

    await db.commit()

    if success_response is not None:
        return success_response
    return export_job
