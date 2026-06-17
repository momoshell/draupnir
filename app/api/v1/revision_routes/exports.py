"""Revision export creation routes."""

from collections.abc import Awaitable, Callable
from typing import Annotated, Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse, Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.idempotency import (
    IdempotentMutationOps,
    IdempotentMutationSuccess,
    build_idempotency_fingerprint,
    claim_idempotency_response,
    complete_idempotency_response,
    get_idempotency_key,
    replay_idempotency_response,
    run_idempotent_mutation,
)
from app.api.v1.revision_lineage import (
    _get_active_revision,
    _get_revision_estimate_version_or_404,
    _get_revision_quantity_takeoff_or_404,
)
from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response, raise_not_found
from app.db.session import get_db
from app.jobs.worker import (
    enqueue_export_job,
    prepare_job_enqueue_intent,
    publish_job_enqueue_intent,
)
from app.models.drawing_revision import DrawingRevision
from app.models.estimate_version import EstimateVersion
from app.models.export_job_input import ExportJobInput
from app.models.job import Job, JobType
from app.models.quantity_takeoff import QuantityTakeoff
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

# Module-private aliases for the route collaborators. Assigned (not re-imported under
# the private name) so they are genuine module attributes that tests can swap with
# monkeypatch.setattr(exports, "_name", ...); the routes resolve them as module globals
# at call time, so no wrapper layer is needed.
_build_idempotency_fingerprint = build_idempotency_fingerprint
_claim_idempotency_response = claim_idempotency_response
_complete_idempotency_response = complete_idempotency_response
_replay_idempotency_response = replay_idempotency_response
_enqueue_export_job = enqueue_export_job
_prepare_job_enqueue_intent = prepare_job_enqueue_intent
_publish_job_enqueue_intent = publish_job_enqueue_intent

type _ExportPreclaimLoader = Callable[[], Awaitable[None]]
type _ExportJobCreator = Callable[[], Awaitable[Job]]
type _ExportPreclaim = Callable[[], Awaitable[Response | None]]


async def _get_locked_active_revision_or_404(
    revision_id: UUID,
    db: AsyncSession,
) -> DrawingRevision:
    """Return the active revision under a row lock or raise 404."""

    revision = await _get_active_revision(revision_id, db, for_update=True)
    if revision is None:
        raise_not_found("Drawing revision", str(revision_id))
    assert revision is not None
    return revision


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


def _revision_supports_revised_dxf_export(revision: DrawingRevision) -> bool:
    """Return whether the revision is eligible for revised DXF export."""

    return revision.revision_kind == "changeset" and revision.changeset_id is not None


def _revised_dxf_export_revision_invalid_response() -> Response:
    """Return the standard invalid-route response for non-changeset revisions."""

    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content=create_error_response(
            ErrorCode.INPUT_INVALID,
            "Revised DXF export requires an active changeset-origin revision.",
        ),
    )


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
    """Return the revision's takeoff or raise the standard route error.

    Path B 4: exports are no longer gated on quantity_gate / trusted_totals;
    any persisted takeoff is exportable.
    """

    return await _get_revision_quantity_takeoff_or_404(revision_id, takeoff_id, db)


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


def _export_create_idempotency_ops() -> IdempotentMutationOps:
    """Return shared export-route idempotency ops via local compat wrappers."""

    return IdempotentMutationOps(
        replay=_replay_idempotency_response,
        claim=_claim_idempotency_response,
        complete=_complete_idempotency_response,
    )


async def _publish_export_enqueue(job_id: UUID) -> None:
    """Publish the export job after the enclosing transaction commits."""

    await _publish_job_enqueue_intent(
        job_id,
        publisher=_enqueue_export_job,
        suppress_exceptions=True,
    )


async def _persist_export_job(
    db: AsyncSession,
    revision: DrawingRevision,
    *,
    parent_job_id: UUID | None,
    export_kind: ExportKind,
    options_json: dict[str, Any],
    quantity_takeoff: QuantityTakeoff | None = None,
    estimate_version: EstimateVersion | None = None,
) -> Job:
    """Create and stage a pending export job plus immutable input row."""

    export_job = _build_export_job(
        revision,
        parent_job_id=parent_job_id,
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
            options_json=options_json,
            quantity_takeoff=quantity_takeoff,
            estimate_version=estimate_version,
        )
    )
    return export_job


async def _create_export_route_job(
    db: AsyncSession,
    *,
    idempotency_key: str | None,
    fingerprint: str | None,
    path: str,
    load_before_claim: _ExportPreclaimLoader,
    create_export_job: _ExportJobCreator,
    preclaim: _ExportPreclaim | None = None,
) -> Job | Response:
    """Run the shared export-create flow with optional idempotency helpers."""

    async def default_preclaim() -> Response | None:
        await load_before_claim()
        return None

    preclaim_fn = preclaim or default_preclaim

    if idempotency_key is None:
        preclaim_response = await preclaim_fn()
        if preclaim_response is not None:
            return preclaim_response
        export_job = await create_export_job()
        await db.commit()
        await _publish_export_enqueue(export_job.id)
        return export_job

    assert fingerprint is not None

    async def mutate() -> IdempotentMutationSuccess[Job]:
        export_job = await create_export_job()

        async def after_commit() -> None:
            await _publish_export_enqueue(export_job.id)

        return IdempotentMutationSuccess(
            value=export_job,
            status_code=status.HTTP_202_ACCEPTED,
            after_commit=after_commit,
        )

    return await run_idempotent_mutation(
        db,
        key=idempotency_key,
        fingerprint=fingerprint,
        method="POST",
        path=path,
        mutate=mutate,
        serialize_result=lambda export_job: JobRead.model_validate(export_job).model_dump(
            mode="json"
        ),
        preclaim=preclaim_fn,
        reload_after_claim=load_before_claim,
        ops=_export_create_idempotency_ops(),
    )


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
    path = f"/revisions/{revision_id}/exports/revision-json"
    normalized_body = _build_export_request_body(
        export_kind,
        options=request.options,
    )
    fingerprint: str | None = None
    if idempotency_key is not None:
        fingerprint = _build_idempotency_fingerprint(
            f"revisions.exports.revision_json:{revision_id}",
            {
                "revision_id": str(revision_id),
                "body": normalized_body,
            },
        )

    revision: DrawingRevision | None = None

    async def load_before_claim() -> None:
        nonlocal revision
        revision = await _get_locked_active_revision_or_404(revision_id, db)

    async def create_export_job() -> Job:
        assert revision is not None
        return await _persist_export_job(
            db,
            revision,
            parent_job_id=_parent_job_id_for_revision(revision),
            export_kind=export_kind,
            options_json=request.options,
        )

    return await _create_export_route_job(
        db,
        idempotency_key=idempotency_key,
        fingerprint=fingerprint,
        path=path,
        load_before_claim=load_before_claim,
        create_export_job=create_export_job,
    )


@exports_router.post(
    "/revisions/{revision_id}/exports/dxf",
    response_model=JobRead,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_dxf_export(
    revision_id: UUID,
    request: RevisionJsonExportCreateRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    idempotency_key: Annotated[str | None, Depends(get_idempotency_key)] = None,
) -> Job | Response:
    """Create a pending DXF export job for the base extraction of any active revision.

    Unlike revised-dxf, this requires no changeset — it renders the revision's canonical
    geometry directly to the open DXF format.
    """

    export_kind = ExportKind.DXF
    path = f"/revisions/{revision_id}/exports/dxf"
    normalized_body = _build_export_request_body(
        export_kind,
        options=request.options,
    )
    fingerprint: str | None = None
    if idempotency_key is not None:
        fingerprint = _build_idempotency_fingerprint(
            f"revisions.exports.dxf:{revision_id}",
            {
                "revision_id": str(revision_id),
                "body": normalized_body,
            },
        )

    revision: DrawingRevision | None = None

    async def load_before_claim() -> None:
        nonlocal revision
        revision = await _get_locked_active_revision_or_404(revision_id, db)

    async def create_export_job() -> Job:
        assert revision is not None
        return await _persist_export_job(
            db,
            revision,
            parent_job_id=_parent_job_id_for_revision(revision),
            export_kind=export_kind,
            options_json=request.options,
        )

    return await _create_export_route_job(
        db,
        idempotency_key=idempotency_key,
        fingerprint=fingerprint,
        path=path,
        load_before_claim=load_before_claim,
        create_export_job=create_export_job,
    )


@exports_router.post(
    "/revisions/{revision_id}/exports/revised-dxf",
    response_model=JobRead,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_revised_dxf_export(
    revision_id: UUID,
    request: RevisionJsonExportCreateRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    idempotency_key: Annotated[str | None, Depends(get_idempotency_key)] = None,
) -> Job | Response:
    """Create a pending revised DXF export job for an active changeset revision."""

    export_kind = ExportKind.REVISED_DXF
    path = f"/revisions/{revision_id}/exports/revised-dxf"
    normalized_body = _build_export_request_body(
        export_kind,
        options=request.options,
    )
    fingerprint: str | None = None
    if idempotency_key is not None:
        fingerprint = _build_idempotency_fingerprint(
            f"revisions.exports.revised_dxf:{revision_id}",
            {
                "revision_id": str(revision_id),
                "body": normalized_body,
            },
        )

    revision: DrawingRevision | None = None

    invalid_preclaim_response: Response | None = None

    async def load_before_claim() -> None:
        nonlocal invalid_preclaim_response
        nonlocal revision
        invalid_preclaim_response = None
        revision = await _get_locked_active_revision_or_404(revision_id, db)
        if not _revision_supports_revised_dxf_export(revision):
            invalid_preclaim_response = _revised_dxf_export_revision_invalid_response()

    async def preclaim() -> Response | None:
        await load_before_claim()
        return invalid_preclaim_response

    async def create_export_job() -> Job:
        assert revision is not None
        return await _persist_export_job(
            db,
            revision,
            parent_job_id=_parent_job_id_for_revision(revision),
            export_kind=export_kind,
            options_json=request.options,
        )

    return await _create_export_route_job(
        db,
        idempotency_key=idempotency_key,
        fingerprint=fingerprint,
        path=path,
        load_before_claim=load_before_claim,
        create_export_job=create_export_job,
        preclaim=preclaim,
    )


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
    path = f"/revisions/{revision_id}/quantity-takeoffs/{takeoff_id}/exports/quantity-csv"
    normalized_body = _build_export_request_body(
        export_kind,
        options=request.options,
    )
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

    revision: DrawingRevision | None = None
    takeoff: QuantityTakeoff | None = None

    async def load_before_claim() -> None:
        nonlocal revision, takeoff
        revision = await _get_locked_active_revision_or_404(revision_id, db)
        takeoff = await _get_exportable_takeoff_or_404(revision_id, takeoff_id, db)

    async def create_export_job() -> Job:
        assert revision is not None
        assert takeoff is not None
        return await _persist_export_job(
            db,
            revision,
            parent_job_id=_parent_job_id_for_takeoff(revision, takeoff),
            export_kind=export_kind,
            options_json=request.options,
            quantity_takeoff=takeoff,
        )

    return await _create_export_route_job(
        db,
        idempotency_key=idempotency_key,
        fingerprint=fingerprint,
        path=path,
        load_before_claim=load_before_claim,
        create_export_job=create_export_job,
    )


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
    path = (
        "/revisions/"
        f"{revision_id}/quantity-takeoffs/{takeoff_id}/estimates/"
        f"{estimate_version_id}/exports"
    )
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

    revision: DrawingRevision | None = None
    takeoff: QuantityTakeoff | None = None
    estimate_version: EstimateVersion | None = None

    async def load_before_claim() -> None:
        nonlocal revision, takeoff, estimate_version
        revision = await _get_locked_active_revision_or_404(revision_id, db)
        takeoff, estimate_version = await _get_exportable_estimate_or_404(
            revision_id,
            takeoff_id,
            estimate_version_id,
            db,
        )

    async def create_export_job() -> Job:
        assert revision is not None
        assert takeoff is not None
        assert estimate_version is not None
        return await _persist_export_job(
            db,
            revision,
            parent_job_id=_parent_job_id_for_estimate(revision, estimate_version),
            export_kind=request.export_kind,
            options_json=request.options,
            quantity_takeoff=takeoff,
            estimate_version=estimate_version,
        )

    return await _create_export_route_job(
        db,
        idempotency_key=idempotency_key,
        fingerprint=fingerprint,
        path=path,
        load_before_claim=load_before_claim,
        create_export_job=create_export_job,
    )
