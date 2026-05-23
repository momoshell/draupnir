"""Revision API routes."""

from datetime import UTC as _UTC
from datetime import date, datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.idempotency import (
    claim_idempotency_response as _claim_idempotency_response,
)
from app.api.idempotency import (
    complete_idempotency_response as _complete_idempotency_response,
)
from app.api.idempotency import (
    replay_idempotency_response as _replay_idempotency_response,
)
from app.api.v1.revision_estimate_inputs import (
    _build_estimate_job_input_payload as _estimate_inputs_build_estimate_job_input_payload,
)
from app.api.v1.revision_estimate_inputs import (
    _build_mapped_instance as _estimate_inputs_build_mapped_instance,
)
from app.api.v1.revision_estimate_inputs import (
    _catalog_selection_is_superseded as _estimate_inputs_catalog_selection_is_superseded,
)
from app.api.v1.revision_estimate_inputs import (
    _first_present_attribute as _estimate_inputs_first_present_attribute,
)
from app.api.v1.revision_estimate_inputs import (
    _get_estimate_catalog_selection as _estimate_inputs_get_estimate_catalog_selection,
)
from app.api.v1.revision_estimate_inputs import (
    _load_app_model_modules as _estimate_inputs_load_app_model_modules,
)
from app.api.v1.revision_estimate_inputs import (
    _mapped_attribute_keys as _estimate_inputs_mapped_attribute_keys,
)
from app.api.v1.revision_estimate_inputs import (
    _normalize_estimate_request_body as _estimate_inputs_normalize_estimate_request_body,
)
from app.api.v1.revision_estimate_inputs import (
    _NormalizedEstimateCatalogRef as _estimate_inputs_NormalizedEstimateCatalogRef,
)
from app.api.v1.revision_estimate_inputs import (
    _quantity_item_is_executable as _estimate_inputs_quantity_item_is_executable,
)
from app.api.v1.revision_estimate_inputs import (
    _raise_estimate_input_invalid as _estimate_inputs_raise_estimate_input_invalid,
)
from app.api.v1.revision_estimate_inputs import (
    _raise_estimate_takeoff_gate_invalid as _estimate_inputs_raise_estimate_takeoff_gate_invalid,
)
from app.api.v1.revision_estimate_inputs import (
    _resolve_catalog_model as _estimate_inputs_resolve_catalog_model,
)
from app.api.v1.revision_estimate_inputs import (
    _resolve_catalog_supersession_model as _estimate_inputs_resolve_catalog_supersession_model,
)
from app.api.v1.revision_estimate_inputs import (
    _resolve_estimate_catalog_refs as _estimate_inputs_resolve_estimate_catalog_refs,
)
from app.api.v1.revision_estimate_inputs import (
    _resolve_estimate_job_model_classes as _estimate_inputs_resolve_estimate_job_model_classes,
)
from app.api.v1.revision_estimate_inputs import (
    _resolve_estimate_pricing_contract as _estimate_inputs_resolve_estimate_pricing_contract,
)
from app.api.v1.revision_lineage import (
    _get_active_file as _lineage_get_active_file,
)
from app.api.v1.revision_lineage import (
    _get_active_revision as _lineage_get_active_revision,
)
from app.api.v1.revision_lineage import (
    _get_active_revision_manifest_or_409 as _lineage_get_active_revision_manifest_or_409,
)
from app.api.v1.revision_lineage import (
    _get_active_validation_report,
    _get_revision_manifest,
    _raise_entities_not_materialized,
)
from app.api.v1.revision_lineage import (
    _get_active_validation_report_or_404 as _lineage_get_active_validation_report_or_404,
)
from app.api.v1.revision_lineage import (
    _get_revision_estimate_version_or_404 as _lineage_get_revision_estimate_version_or_404,
)
from app.api.v1.revision_lineage import (
    _get_revision_quantity_takeoff_or_404 as _lineage_get_revision_quantity_takeoff_or_404,
)
from app.api.v1.revision_lineage import (
    _manifest_counts as _lineage_manifest_counts,
)
from app.api.v1.revision_routes.adapter_outputs import (
    adapter_outputs_router,
)
from app.api.v1.revision_routes.estimates import (
    estimates_router,
)
from app.api.v1.revision_routes.file_revisions import (
    file_revisions_router,
)
from app.api.v1.revision_routes.generated_artifacts import (
    generated_artifacts_router,
)
from app.api.v1.revision_routes.materialization import (
    materialization_router,
)
from app.api.v1.revision_routes.quantity_takeoffs import (
    quantity_takeoff_create_router,
    quantity_takeoffs_router,
)
from app.api.v1.revision_routes.validation_reports import (
    validation_reports_router,
)
from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response
from app.jobs import worker as jobs_worker_module
from app.jobs.worker import (
    enqueue_quantity_takeoff_job as _enqueue_quantity_takeoff_job,
)
from app.jobs.worker import (
    prepare_job_enqueue_intent as _prepare_job_enqueue_intent,
)
from app.jobs.worker import (
    publish_job_enqueue_intent as _publish_job_enqueue_intent,
)
from app.models.drawing_revision import DrawingRevision
from app.models.job import Job
from app.models.quantity_takeoff import (
    QuantityItem,
    QuantityTakeoff,
)
from app.models.revision_materialization import RevisionEntityManifest
from app.models.validation_report import ValidationReport
from app.schemas.estimate import (
    EstimateVersionCreateCatalogRef,
    EstimateVersionCreateRequest,
)

revisions_router = APIRouter()

_DEFAULT_PAGE_SIZE = 50
_MAX_PAGE_SIZE = 200


_get_active_file = _lineage_get_active_file
_get_active_revision = _lineage_get_active_revision
_manifest_counts = _lineage_manifest_counts
_get_revision_quantity_takeoff_or_404 = _lineage_get_revision_quantity_takeoff_or_404
_get_revision_estimate_version_or_404 = _lineage_get_revision_estimate_version_or_404
_NormalizedEstimateCatalogRef = _estimate_inputs_NormalizedEstimateCatalogRef
UTC = _UTC
replay_idempotency_response = _replay_idempotency_response
claim_idempotency_response = _claim_idempotency_response
complete_idempotency_response = _complete_idempotency_response
prepare_job_enqueue_intent = _prepare_job_enqueue_intent
publish_job_enqueue_intent = _publish_job_enqueue_intent


def enqueue_quantity_takeoff_job(job_id: UUID) -> None:
    """Compatibility wrapper kept for test fixture patching."""

    _enqueue_quantity_takeoff_job(job_id)


def enqueue_estimate_job(job_id: UUID) -> None:
    """Compatibility wrapper kept for test fixture patching."""

    publisher = jobs_worker_module.enqueue_estimate_job
    publisher(job_id)


async def _get_active_validation_report_or_404(
    revision_id: UUID,
    db: AsyncSession,
) -> ValidationReport:
    """Return an active revision validation report or raise not found."""

    return await _lineage_get_active_validation_report_or_404(
        revision_id,
        db,
        get_active_validation_report=_get_active_validation_report,
        get_active_revision=_get_active_revision,
    )


async def _get_active_revision_manifest_or_409(
    revision_id: UUID,
    db: AsyncSession,
) -> RevisionEntityManifest:
    """Return an active revision manifest or raise the standard missing errors."""

    return await _lineage_get_active_revision_manifest_or_409(
        revision_id,
        db,
        get_active_revision=_get_active_revision,
        get_revision_manifest=_get_revision_manifest,
        raise_entities_not_materialized=_raise_entities_not_materialized,
    )


def _raise_quantity_takeoff_gate_invalid(report: ValidationReport) -> None:
    """Raise the standard pre-enqueue error for non-runnable quantity gates."""

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


def _raise_estimate_input_invalid(
    message: str,
    *,
    details: dict[str, Any] | None = None,
) -> None:
    """Raise the standard estimate input validation error."""

    _estimate_inputs_raise_estimate_input_invalid(message, details=details)


def _raise_estimate_takeoff_gate_invalid(takeoff: QuantityTakeoff) -> None:
    """Raise the standard pre-enqueue error for non-runnable estimate inputs."""

    _estimate_inputs_raise_estimate_takeoff_gate_invalid(
        takeoff,
        raise_estimate_input_invalid=_raise_estimate_input_invalid,
    )


def _normalize_estimate_request_body(payload: EstimateVersionCreateRequest) -> dict[str, Any]:
    """Return the normalized create-request body used for idempotency fingerprints."""

    return _estimate_inputs_normalize_estimate_request_body(payload)


def _mapped_attribute_keys(model_class: type[Any]) -> set[str]:
    """Return mapped SQLAlchemy attribute keys for a model class."""

    return _estimate_inputs_mapped_attribute_keys(model_class)


def _build_mapped_instance(model_class: type[Any], values: dict[str, Any]) -> Any:
    """Instantiate a mapped object while ignoring unknown compatibility fields."""

    return _estimate_inputs_build_mapped_instance(
        model_class,
        values,
        mapped_attribute_keys=_mapped_attribute_keys,
    )


def _first_present_attribute(instance: Any, names: tuple[str, ...]) -> Any:
    """Return the first present attribute value from a candidate name list."""

    return _estimate_inputs_first_present_attribute(instance, names)


def _load_app_model_modules() -> None:
    """Import app model modules so mapped classes are registered."""

    _estimate_inputs_load_app_model_modules()


def _resolve_estimate_job_model_classes() -> tuple[type[Any], type[Any]]:
    """Resolve mapped model classes used to persist estimate job inputs."""

    return _estimate_inputs_resolve_estimate_job_model_classes(
        load_app_model_modules=_load_app_model_modules,
    )


def _resolve_catalog_model(ref_type: str) -> type[Any]:
    """Resolve the mapped catalog/formula model for a request ref type."""

    return _estimate_inputs_resolve_catalog_model(
        ref_type,
        load_app_model_modules=_load_app_model_modules,
    )


def _resolve_catalog_supersession_model(ref_type: str) -> type[Any]:
    """Resolve the mapped supersession model for a request ref type."""

    return _estimate_inputs_resolve_catalog_supersession_model(
        ref_type,
        load_app_model_modules=_load_app_model_modules,
    )


async def _catalog_selection_is_superseded(
    ref_type: str,
    selection_id: UUID,
    db: AsyncSession,
) -> bool:
    """Return whether a catalog selection has been superseded."""

    return await _estimate_inputs_catalog_selection_is_superseded(
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

    return _estimate_inputs_resolve_estimate_pricing_contract(
        request,
        job_created_at=job_created_at,
    )


def _quantity_item_is_executable(quantity_item: QuantityItem) -> bool:
    """Return whether a quantity item can back a rate/material estimate ref."""

    return _estimate_inputs_quantity_item_is_executable(quantity_item)


async def _get_estimate_catalog_selection(
    ref_type: str,
    selection_id: UUID,
    db: AsyncSession,
) -> Any | None:
    """Return the selected catalog/formula row for a request ref."""

    return await _estimate_inputs_get_estimate_catalog_selection(
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

    return await _estimate_inputs_resolve_estimate_catalog_refs(
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

    return _estimate_inputs_build_estimate_job_input_payload(
        estimate_job,
        revision,
        takeoff,
        request,
        normalized_refs,
        pricing_mode=pricing_mode,
        pricing_effective_date=pricing_effective_date,
    )


revisions_router.include_router(file_revisions_router)
revisions_router.include_router(adapter_outputs_router)
revisions_router.include_router(generated_artifacts_router)
revisions_router.include_router(materialization_router)
revisions_router.include_router(quantity_takeoffs_router)
revisions_router.include_router(estimates_router)
revisions_router.include_router(quantity_takeoff_create_router)


revisions_router.include_router(validation_reports_router)
