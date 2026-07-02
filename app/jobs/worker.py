"""Celery worker application and persisted job handlers."""

# ruff: noqa: SLF001

import asyncio
import atexit
import hashlib
import inspect
import threading
from collections.abc import Callable, Coroutine
from contextlib import suppress
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Any, cast
from uuid import UUID

from celery import Celery
from celery.signals import worker_ready
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.cad.changeset import (
    ChangeSetApplyConflict,
    ChangeSetApplyError,
    ChangeSetApplyLoadError,
    ChangeSetApplySuccess,
    load_and_apply_change_set,
)
from app.core.config import settings
from app.core.errors import ErrorCode
from app.core.logging import get_logger
from app.db.session import get_session_maker
from app.estimating.catalog.resolver import resolve_formula, resolve_material, resolve_rate
from app.estimating.engine.errors import EstimateEngineError
from app.estimating.engine.service import compose_estimate
from app.estimating.quantities.engine import compute_quantities
from app.exports._base import ExportArtifact
from app.exports.csv import (
    EstimateCsvExportError,
    QuantityCsvExportError,
    render_estimate_csv_export,
    render_quantity_csv_export,
)
from app.exports.estimate_pdf import (
    EstimatePdfExportError,
    render_estimate_pdf_export,
)
from app.exports.revised_dxf import RevisedDxfExportError, render_revised_dxf_export
from app.exports.revision_json import (
    RevisionJsonExportError,
    render_revision_json_export,
)
from app.ingestion.contracts import AdapterTimeout, InputFamily, ProgressUpdate
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunnerError, IngestionRunRequest, run_ingestion
from app.jobs import lifecycle as job_lifecycle
from app.jobs import recovery as job_recovery
from app.jobs import runner as job_runner
from app.jobs.conflict_diagnostics import (
    _build_changeset_apply_conflict_details,
    _build_quantity_conflict_summaries,
)
from app.jobs.estimate_assembly import (
    _build_estimate_engine_input as _build_estimate_engine_input,
)
from app.jobs.estimate_mapping import (
    _ESTIMATE_JOB_INPUT_INVALID_ERROR_MESSAGE,
    _EstimateJobInputError,
)
from app.jobs.execution_inputs import (
    _ExportExecutionInput as _ExportExecutionInput,
)
from app.jobs.execution_inputs import (
    _QuantityTakeoffExecutionInput as _QuantityTakeoffExecutionInput,
)
from app.jobs.export_execution_input import (
    _EXPORT_LINEAGE_ANCHOR_CHANGESET as _EXPORT_LINEAGE_ANCHOR_CHANGESET,
)
from app.jobs.export_execution_input import (
    _EXPORT_LINEAGE_ANCHOR_ESTIMATE_VERSION as _EXPORT_LINEAGE_ANCHOR_ESTIMATE_VERSION,
)
from app.jobs.export_execution_input import (
    _EXPORT_LINEAGE_ANCHOR_QUANTITY_TAKEOFF as _EXPORT_LINEAGE_ANCHOR_QUANTITY_TAKEOFF,
)
from app.jobs.export_execution_input import (
    _EXPORT_LINEAGE_ANCHOR_REVISION as _EXPORT_LINEAGE_ANCHOR_REVISION,
)
from app.jobs.export_execution_input import (
    _build_export_job_input_error as _build_export_job_input_error,
)
from app.jobs.export_execution_input import (
    _ExportJobInputError as _ExportJobInputError,
)
from app.jobs.export_execution_input import (
    build_export_execution_input as build_export_execution_input,
)
from app.jobs.finalization_persister import (
    DEFAULT_FINALIZATION_PERSISTER,
    FinalizationPersister,
)
from app.jobs.finalizers import (
    _finalize_centerline_job as _finalize_centerline_job,
)
from app.jobs.finalizers import (
    _finalize_changeset_apply_job as _finalize_changeset_apply_job,
)
from app.jobs.finalizers import (
    _finalize_estimate_job as _finalize_estimate_job,
)
from app.jobs.finalizers import (
    _finalize_export_job as _finalize_export_job,
)
from app.jobs.finalizers import (
    _finalize_ingest_job as _finalize_ingest_job,
)
from app.jobs.finalizers import (
    _finalize_quantity_takeoff_job as _finalize_quantity_takeoff_job,
)
from app.jobs.finalizers import (
    _finalize_rooms_job as _finalize_rooms_job,
)
from app.jobs.quantity_execution_input import (
    _QUANTITY_TAKEOFF_MATERIALIZATION_MISSING_ERROR_MESSAGE as _QUANTITY_TAKEOFF_MATERIALIZATION_MISSING_ERROR_MESSAGE,  # noqa: E501
)
from app.jobs.quantity_execution_input import (
    _QUANTITY_TAKEOFF_VALIDATION_REPORT_MISSING_ERROR_MESSAGE as _QUANTITY_TAKEOFF_VALIDATION_REPORT_MISSING_ERROR_MESSAGE,  # noqa: E501
)
from app.jobs.quantity_execution_input import (
    _build_quantity_gate_metadata as _build_quantity_gate_metadata,
)
from app.jobs.quantity_execution_input import (
    _build_revision_entity_input as _build_revision_entity_input,
)
from app.jobs.quantity_execution_input import (
    _manifest_entity_count as _manifest_entity_count,
)
from app.jobs.quantity_execution_input import (
    _QuantityTakeoffJobError as _QuantityTakeoffJobError,
)
from app.jobs.quantity_execution_input import (
    build_quantity_takeoff_execution_input as build_quantity_takeoff_execution_input,
)
from app.jobs.revision_queries import _assert_job_base_revision_invariants
from app.jobs.revision_queries import (
    _build_revision_conflict_details as _build_revision_conflict_details,
)
from app.jobs.revision_queries import (
    _get_drawing_revision as _get_drawing_revision,
)
from app.jobs.revision_queries import (
    _get_latest_drawing_revision as _get_latest_drawing_revision,
)
from app.jobs.revision_queries import (
    _get_revision_blocks_for_revision as _get_revision_blocks_for_revision,
)
from app.jobs.revision_queries import (
    _get_revision_entities_for_revision as _get_revision_entities_for_revision,
)
from app.jobs.revision_queries import (
    _get_revision_entity_manifest_for_revision as _get_revision_entity_manifest_for_revision,
)
from app.jobs.revision_queries import (
    _get_revision_layers_for_revision as _get_revision_layers_for_revision,
)
from app.jobs.revision_queries import (
    _get_revision_layouts_for_revision as _get_revision_layouts_for_revision,
)
from app.jobs.revision_queries import (
    _get_validation_report_for_revision as _get_validation_report_for_revision,
)
from app.jobs.revision_queries import (
    _revision_reference as _revision_reference,
)
from app.jobs.worker_deps import WorkerDeps
from app.models.cad_changeset import CadChangeSet
from app.models.changeset_apply_job_input import ChangeSetApplyJobInput
from app.models.drawing_revision import DrawingRevision
from app.models.estimate_version import EstimateVersion
from app.models.export_job_input import ExportJobInput
from app.models.extraction_profile import ExtractionProfile
from app.models.job import Job, JobType
from app.models.quantity_takeoff import QuantityTakeoff
from app.models.revision_materialization import (
    RevisionEntity,
    RevisionEntityManifest,
)
from app.models.validation_report import ValidationReport
from app.storage import get_storage

logger = get_logger(__name__)

_RECOVERABLE_INGEST_JOB_TYPES = job_runner.INGEST_WORKER_JOB_TYPES
_RECOVERABLE_ENQUEUE_JOB_TYPES = job_runner.RECOVERABLE_ENQUEUE_JOB_TYPES
_KNOWN_ENQUEUE_JOB_TYPES_WITHOUT_PUBLISHER = job_runner.JOB_TYPES_WITHOUT_ENQUEUE_PUBLISHER
_TERMINAL_JOB_STATUSES = job_lifecycle._TERMINAL_JOB_STATUSES
_ENQUEUE_STATUS_PENDING = job_recovery._ENQUEUE_STATUS_PENDING
_ENQUEUE_STATUS_PUBLISHING = job_recovery._ENQUEUE_STATUS_PUBLISHING
_ENQUEUE_STATUS_PUBLISHED = job_recovery._ENQUEUE_STATUS_PUBLISHED
_DEFAULT_ADAPTER_TIMEOUT = timedelta(minutes=5)
_RUNNING_JOB_STALE_AFTER = job_recovery._RUNNING_JOB_STALE_AFTER
_JOB_ATTEMPT_LEASE_RENEW_INTERVAL = _RUNNING_JOB_STALE_AFTER / 3
_ENQUEUE_LEASE_DURATION = job_recovery._ENQUEUE_LEASE_DURATION
_JOB_CANCELLATION_POLL_INTERVAL_SECONDS = 0.1
_ENQUEUE_BACKOFF_BASE_SECONDS = job_recovery._ENQUEUE_BACKOFF_BASE_SECONDS
_ENQUEUE_BACKOFF_MAX_SECONDS = job_recovery._ENQUEUE_BACKOFF_MAX_SECONDS
_enqueue_backoff_seconds = job_recovery._enqueue_backoff_seconds
_current_enqueue_countdown = job_recovery._current_enqueue_countdown


_JOB_CANCELLED_ERROR_CODE = ErrorCode.JOB_CANCELLED.value
_ENQUEUE_INGEST_JOB_ERROR_MESSAGE = job_runner.ENQUEUE_ERROR_MESSAGES_BY_JOB_TYPE[
    JobType.INGEST.value
]
_ENQUEUE_REPROCESS_JOB_ERROR_MESSAGE = job_runner.ENQUEUE_ERROR_MESSAGES_BY_JOB_TYPE[
    JobType.REPROCESS.value
]
_ENQUEUE_QUANTITY_TAKEOFF_JOB_ERROR_MESSAGE = job_runner.ENQUEUE_ERROR_MESSAGES_BY_JOB_TYPE[
    JobType.QUANTITY_TAKEOFF.value
]
_ENQUEUE_CENTERLINE_JOB_ERROR_MESSAGE = job_runner.ENQUEUE_ERROR_MESSAGES_BY_JOB_TYPE[
    JobType.CENTERLINE.value
]
_ENQUEUE_ESTIMATE_JOB_ERROR_MESSAGE = job_runner.ENQUEUE_ERROR_MESSAGES_BY_JOB_TYPE[
    JobType.ESTIMATE.value
]
_ENQUEUE_EXPORT_JOB_ERROR_MESSAGE = job_runner.ENQUEUE_ERROR_MESSAGES_BY_JOB_TYPE[
    JobType.EXPORT.value
]
_ENQUEUE_CHANGESET_APPLY_JOB_ERROR_MESSAGE = job_runner.ENQUEUE_ERROR_MESSAGES_BY_JOB_TYPE[
    JobType.CHANGESET_APPLY.value
]
_ENQUEUE_ROOMS_JOB_ERROR_MESSAGE = job_runner.ENQUEUE_ERROR_MESSAGES_BY_JOB_TYPE[
    JobType.ROOMS.value
]
_FINALIZE_INGEST_JOB_ERROR_MESSAGE = "Failed to finalize ingest job"
_PROCESS_INGEST_JOB_ERROR_MESSAGE = "Ingest job failed unexpectedly."
_FINALIZE_QUANTITY_TAKEOFF_JOB_ERROR_MESSAGE = "Failed to finalize quantity takeoff job"
_FINALIZE_CENTERLINE_JOB_ERROR_MESSAGE = "Failed to finalize centerline job"
_FINALIZE_ROOMS_JOB_ERROR_MESSAGE = "Failed to finalize rooms job"
_FINALIZE_ESTIMATE_JOB_ERROR_MESSAGE = "Failed to finalize estimate job"
_FINALIZE_EXPORT_JOB_ERROR_MESSAGE = "Failed to finalize export job"
_FINALIZE_CHANGESET_APPLY_JOB_ERROR_MESSAGE = "Failed to finalize changeset apply job"
_PROCESS_QUANTITY_TAKEOFF_JOB_ERROR_MESSAGE = "Quantity takeoff job failed unexpectedly."
_PROCESS_CENTERLINE_JOB_ERROR_MESSAGE = "Centerline job failed unexpectedly."
_PROCESS_ROOMS_JOB_ERROR_MESSAGE = "Rooms job failed unexpectedly."
_PROCESS_ESTIMATE_JOB_ERROR_MESSAGE = "Estimate job failed unexpectedly."
_PROCESS_EXPORT_JOB_ERROR_MESSAGE = "Export job failed unexpectedly."
_PROCESS_CHANGESET_APPLY_JOB_ERROR_MESSAGE = "Changeset apply job failed unexpectedly."
_QUANTITY_TAKEOFF_CONFLICT_ERROR_MESSAGE = (
    "Quantity takeoff detected conflicting contributor inputs."
)
_SAFE_RUNNER_ERROR_DETAIL_KEYS = (
    "adapter_key",
    "input_family",
    "reason",
    "detail",
    "stage",
    "detected_format",
    "media_type",
    "output_kind",
    "max_output_bytes",
    "output_size_bytes",
)

_WORKER_LOOP_RUNNER: asyncio.Runner | None = None
_WORKER_LOOP_RUNNER_LOCK = threading.Lock()


def is_ingest_worker_job_type(job_type: JobType | str) -> bool:
    """Return whether a job type is published to the ingest worker."""
    normalized_job_type = job_type.value if isinstance(job_type, JobType) else job_type
    return normalized_job_type in _RECOVERABLE_INGEST_JOB_TYPES


def is_recoverable_enqueue_job_type(job_type: JobType | str) -> bool:
    """Return whether a job type participates in durable queue publication/recovery."""
    normalized_job_type = job_type.value if isinstance(job_type, JobType) else job_type
    return normalized_job_type in _RECOVERABLE_ENQUEUE_JOB_TYPES


def get_job_enqueue_publisher(job_type: JobType | str) -> Callable[[UUID], None] | None:
    """Return the queue publisher registered for a persisted worker job type."""
    normalized_job_type = job_type.value if isinstance(job_type, JobType) else job_type
    handler = job_runner.get_job_handler(normalized_job_type)
    if handler is None or normalized_job_type in _KNOWN_ENQUEUE_JOB_TYPES_WITHOUT_PUBLISHER:
        return None
    publisher_name = handler.enqueue_publisher_name
    if publisher_name is None:
        return None
    publisher = globals().get(publisher_name)
    if publisher is None:
        return None
    return cast(Callable[[UUID], None], publisher)


_InactiveSourceError = job_lifecycle._InactiveSourceError
_StaleJobAttemptError = job_lifecycle._StaleJobAttemptError


_RevisionConflictError = job_lifecycle._RevisionConflictError


@dataclass(frozen=True, slots=True)
class _QueuedJobEvent:
    """Buffered job event persisted by the progress drain."""

    level: str
    message: str
    data_json: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _ChangeSetApplyJobError(Exception):
    """Raised for deterministic changeset apply execution failures."""

    error_code: ErrorCode
    message: str
    details: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


type _RegisteredJobInputError = (
    _QuantityTakeoffJobError
    | _EstimateJobInputError
    | _ExportJobInputError
    | _ChangeSetApplyJobError
)


@dataclass(frozen=True, slots=True)
class _RegisteredJobAttemptResult:
    """Deferred finalization kwargs returned by a registered job execution step."""

    finalize_kwargs: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _RegisteredJobProcessSpec:
    """Shared shell configuration for registered persisted worker jobs."""

    job_type_name: job_runner.JobTypeName
    input_error_type: type[Exception] | None
    input_failure_log_event: str | None
    stale_attempt_log_event: str
    revision_conflict_log_event: str
    cancelled_during_execution_log_event: str
    cancelled_during_finalization_log_event: str
    process_failed_log_event: str
    finalization_failed_log_event: str
    succeeded_log_event: str
    process_error_message: str
    finalize_error_message: str
    execution_result_arg_name: str | None = None
    execution_error_type: type[Exception] | None = None
    execution_error_handler_name: str | None = None
    inactive_source_log_event: str | None = None
    finalization_exception_log_fields_name: str | None = None
    reraise_revision_conflict: bool = False


_INGEST_PROCESS_SPEC = _RegisteredJobProcessSpec(
    job_type_name="ingest",
    input_error_type=None,
    input_failure_log_event=None,
    stale_attempt_log_event="ingest_job_stale_attempt_skipped",
    revision_conflict_log_event="ingest_job_revision_conflict",
    cancelled_during_execution_log_event="ingest_job_cancelled_during_execution",
    cancelled_during_finalization_log_event="ingest_job_cancelled_during_finalization",
    process_failed_log_event="ingest_job_failed",
    finalization_failed_log_event="ingest_job_finalization_failed",
    succeeded_log_event="ingest_job_succeeded",
    process_error_message=_PROCESS_INGEST_JOB_ERROR_MESSAGE,
    finalize_error_message=_FINALIZE_INGEST_JOB_ERROR_MESSAGE,
    execution_result_arg_name="payload",
    execution_error_type=IngestionRunnerError,
    execution_error_handler_name="_handle_ingest_runner_error",
    inactive_source_log_event="ingest_job_cancelled_inactive_source",
    finalization_exception_log_fields_name="_build_ingest_finalization_error_log_fields",
    reraise_revision_conflict=True,
)


_QUANTITY_TAKEOFF_PROCESS_SPEC = _RegisteredJobProcessSpec(
    job_type_name="quantity_takeoff",
    input_error_type=_QuantityTakeoffJobError,
    input_failure_log_event="quantity_takeoff_job_input_failed",
    stale_attempt_log_event="quantity_takeoff_job_stale_attempt_skipped",
    revision_conflict_log_event="quantity_takeoff_job_revision_conflict",
    cancelled_during_execution_log_event="quantity_takeoff_job_cancelled_during_execution",
    cancelled_during_finalization_log_event="quantity_takeoff_job_cancelled_during_finalization",
    process_failed_log_event="quantity_takeoff_job_failed",
    finalization_failed_log_event="quantity_takeoff_job_finalization_failed",
    succeeded_log_event="quantity_takeoff_job_succeeded",
    process_error_message=_PROCESS_QUANTITY_TAKEOFF_JOB_ERROR_MESSAGE,
    finalize_error_message=_FINALIZE_QUANTITY_TAKEOFF_JOB_ERROR_MESSAGE,
)

_ESTIMATE_PROCESS_SPEC = _RegisteredJobProcessSpec(
    job_type_name="estimate",
    input_error_type=_EstimateJobInputError,
    input_failure_log_event="estimate_job_input_failed",
    stale_attempt_log_event="estimate_job_stale_attempt_skipped",
    revision_conflict_log_event="estimate_job_revision_conflict",
    cancelled_during_execution_log_event="estimate_job_cancelled_during_execution",
    cancelled_during_finalization_log_event="estimate_job_cancelled_during_finalization",
    process_failed_log_event="estimate_job_failed",
    finalization_failed_log_event="estimate_job_finalization_failed",
    succeeded_log_event="estimate_job_succeeded",
    process_error_message=_PROCESS_ESTIMATE_JOB_ERROR_MESSAGE,
    finalize_error_message=_FINALIZE_ESTIMATE_JOB_ERROR_MESSAGE,
)

_EXPORT_PROCESS_SPEC = _RegisteredJobProcessSpec(
    job_type_name="export",
    input_error_type=_ExportJobInputError,
    input_failure_log_event="export_job_input_failed",
    stale_attempt_log_event="export_job_stale_attempt_skipped",
    revision_conflict_log_event="export_job_revision_conflict",
    cancelled_during_execution_log_event="export_job_cancelled_during_execution",
    cancelled_during_finalization_log_event="export_job_cancelled_during_finalization",
    process_failed_log_event="export_job_failed",
    finalization_failed_log_event="export_job_finalization_failed",
    succeeded_log_event="export_job_succeeded",
    process_error_message=_PROCESS_EXPORT_JOB_ERROR_MESSAGE,
    finalize_error_message=_FINALIZE_EXPORT_JOB_ERROR_MESSAGE,
)

_CHANGESET_APPLY_PROCESS_SPEC = _RegisteredJobProcessSpec(
    job_type_name="changeset_apply",
    input_error_type=_ChangeSetApplyJobError,
    input_failure_log_event="changeset_apply_job_input_failed",
    stale_attempt_log_event="changeset_apply_job_stale_attempt_skipped",
    revision_conflict_log_event="changeset_apply_job_revision_conflict",
    cancelled_during_execution_log_event="changeset_apply_job_cancelled_during_execution",
    cancelled_during_finalization_log_event="changeset_apply_job_cancelled_during_finalization",
    process_failed_log_event="changeset_apply_job_failed",
    finalization_failed_log_event="changeset_apply_job_finalization_failed",
    succeeded_log_event="changeset_apply_job_succeeded",
    process_error_message=_PROCESS_CHANGESET_APPLY_JOB_ERROR_MESSAGE,
    finalize_error_message=_FINALIZE_CHANGESET_APPLY_JOB_ERROR_MESSAGE,
)

_CENTERLINE_PROCESS_SPEC = _RegisteredJobProcessSpec(
    job_type_name="centerline",
    input_error_type=None,
    input_failure_log_event=None,
    stale_attempt_log_event="centerline_job_stale_attempt_skipped",
    revision_conflict_log_event="centerline_job_revision_conflict",
    cancelled_during_execution_log_event="centerline_job_cancelled_during_execution",
    cancelled_during_finalization_log_event="centerline_job_cancelled_during_finalization",
    process_failed_log_event="centerline_job_failed",
    finalization_failed_log_event="centerline_job_finalization_failed",
    succeeded_log_event="centerline_job_succeeded",
    process_error_message=_PROCESS_CENTERLINE_JOB_ERROR_MESSAGE,
    finalize_error_message=_FINALIZE_CENTERLINE_JOB_ERROR_MESSAGE,
)

_ROOMS_PROCESS_SPEC = _RegisteredJobProcessSpec(
    job_type_name="rooms",
    input_error_type=None,
    input_failure_log_event=None,
    stale_attempt_log_event="rooms_job_stale_attempt_skipped",
    revision_conflict_log_event="rooms_job_revision_conflict",
    cancelled_during_execution_log_event="rooms_job_cancelled_during_execution",
    cancelled_during_finalization_log_event="rooms_job_cancelled_during_finalization",
    process_failed_log_event="rooms_job_failed",
    finalization_failed_log_event="rooms_job_finalization_failed",
    succeeded_log_event="rooms_job_succeeded",
    process_error_message=_PROCESS_ROOMS_JOB_ERROR_MESSAGE,
    finalize_error_message=_FINALIZE_ROOMS_JOB_ERROR_MESSAGE,
)


_ExportRenderFn = Callable[
    [AsyncSession, _ExportExecutionInput],
    Coroutine[Any, Any, ExportArtifact],
]
_ExportErrorDetailsFn = Callable[[_ExportExecutionInput], dict[str, Any]]
_ExportErrorMapperFn = Callable[[Exception, _ExportExecutionInput], _ExportJobInputError]

_REVISED_DXF_INPUT_ERROR_CODES = frozenset(
    {
        "INPUT_INVALID",
        "MANIFEST_NOT_FOUND",
        "MATERIALIZATION_MISSING",
        "MISSING_LAYER",
        "MISSING_LAYOUT",
        "NONFINITE_COORDINATE",
        "NONZERO_Z_COORDINATE",
        "REVISION_NOT_FOUND",
    }
)


@dataclass(frozen=True, slots=True)
class _ExportKindSpec:
    """Registry entry for one supported export worker kind."""

    format: str
    media_type: str
    render_fn: _ExportRenderFn
    error_type: type[Exception]
    lineage_anchor: str
    error_details_fn: _ExportErrorDetailsFn
    error_mapper_fn: _ExportErrorMapperFn | None = None


def _export_revision_error_details(execution: _ExportExecutionInput) -> dict[str, Any]:
    """Build not-found details for revision-scoped exports."""
    return {"drawing_revision_id": str(execution.drawing_revision_id)}


def _export_changeset_error_details(execution: _ExportExecutionInput) -> dict[str, Any]:
    """Build details for changeset-scoped exports."""
    return {
        "drawing_revision_id": str(execution.drawing_revision_id),
        "changeset_id": str(execution.changeset_id) if execution.changeset_id is not None else None,
    }


def _export_quantity_error_details(execution: _ExportExecutionInput) -> dict[str, Any]:
    """Build not-found details for quantity-scoped exports."""
    assert execution.quantity_takeoff_id is not None
    return {
        "drawing_revision_id": str(execution.drawing_revision_id),
        "quantity_takeoff_id": str(execution.quantity_takeoff_id),
    }


def _export_estimate_error_details(execution: _ExportExecutionInput) -> dict[str, Any]:
    """Build not-found details for estimate-scoped exports."""
    assert execution.estimate_version_id is not None
    return {
        "drawing_revision_id": str(execution.drawing_revision_id),
        "estimate_version_id": str(execution.estimate_version_id),
    }


async def _render_revision_json_export_artifact(
    session: AsyncSession,
    execution: _ExportExecutionInput,
) -> ExportArtifact:
    """Render a revision JSON export artifact."""
    return await render_revision_json_export(
        session,
        execution.drawing_revision_id,
        options=execution.options_json,
    )


def _map_revised_dxf_export_error(
    exc: Exception,
    execution: _ExportExecutionInput,
) -> _ExportJobInputError:
    """Map revised-DXF renderer failures to deterministic job errors."""
    assert isinstance(exc, RevisedDxfExportError)
    details = _export_changeset_error_details(execution)
    details.update(deepcopy(exc.details or {}))
    details["renderer_error_code"] = exc.code
    return _build_export_job_input_error(
        str(exc),
        error_code=(
            ErrorCode.ADAPTER_UNAVAILABLE
            if exc.code in {"ADAPTER_UNAVAILABLE", "ADAPTER_LOAD_FAILED"}
            else (
                ErrorCode.INPUT_INVALID
                if _is_revised_dxf_input_error_code(exc.code)
                else ErrorCode.ADAPTER_FAILED
            )
        ),
        details=details,
    )


def _is_revised_dxf_input_error_code(code: str) -> bool:
    return code in _REVISED_DXF_INPUT_ERROR_CODES or code.startswith(("INVALID_", "UNSUPPORTED_"))


async def _render_revised_dxf_export_artifact(
    session: AsyncSession,
    execution: _ExportExecutionInput,
) -> ExportArtifact:
    """Render a revised DXF export artifact."""
    return await render_revised_dxf_export(
        session,
        execution.drawing_revision_id,
        options=execution.options_json,
    )


async def _render_dxf_export_artifact(
    session: AsyncSession,
    execution: _ExportExecutionInput,
) -> ExportArtifact:
    """Render a base-revision DXF export artifact (no changeset required)."""
    return await render_revised_dxf_export(
        session,
        execution.drawing_revision_id,
        options=execution.options_json,
        require_changeset_origin=False,
    )


async def _render_quantity_csv_export_artifact(
    session: AsyncSession,
    execution: _ExportExecutionInput,
) -> ExportArtifact:
    """Render a quantity CSV export artifact."""
    assert execution.quantity_takeoff_id is not None
    return await render_quantity_csv_export(session, execution.quantity_takeoff_id)


async def _render_estimate_csv_export_artifact(
    session: AsyncSession,
    execution: _ExportExecutionInput,
) -> ExportArtifact:
    """Render an estimate CSV export artifact."""
    assert execution.estimate_version_id is not None
    return await render_estimate_csv_export(session, execution.estimate_version_id)


async def _render_estimate_pdf_export_artifact(
    session: AsyncSession,
    execution: _ExportExecutionInput,
) -> ExportArtifact:
    """Render an estimate PDF export artifact."""
    assert execution.estimate_version_id is not None
    return await render_estimate_pdf_export(
        session,
        execution.estimate_version_id,
        options=execution.options_json,
    )


_EXPORT_KIND_SPECS: dict[str, _ExportKindSpec] = {
    "revision_json": _ExportKindSpec(
        format="json",
        media_type="application/json",
        render_fn=_render_revision_json_export_artifact,
        error_type=RevisionJsonExportError,
        lineage_anchor=_EXPORT_LINEAGE_ANCHOR_REVISION,
        error_details_fn=_export_revision_error_details,
    ),
    "quantity_csv": _ExportKindSpec(
        format="csv",
        media_type="text/csv",
        render_fn=_render_quantity_csv_export_artifact,
        error_type=QuantityCsvExportError,
        lineage_anchor=_EXPORT_LINEAGE_ANCHOR_QUANTITY_TAKEOFF,
        error_details_fn=_export_quantity_error_details,
    ),
    "estimate_csv": _ExportKindSpec(
        format="csv",
        media_type="text/csv",
        render_fn=_render_estimate_csv_export_artifact,
        error_type=EstimateCsvExportError,
        lineage_anchor=_EXPORT_LINEAGE_ANCHOR_ESTIMATE_VERSION,
        error_details_fn=_export_estimate_error_details,
    ),
    "estimate_pdf": _ExportKindSpec(
        format="pdf",
        media_type="application/pdf",
        render_fn=_render_estimate_pdf_export_artifact,
        error_type=EstimatePdfExportError,
        lineage_anchor=_EXPORT_LINEAGE_ANCHOR_ESTIMATE_VERSION,
        error_details_fn=_export_estimate_error_details,
    ),
    "revised_dxf": _ExportKindSpec(
        format="dxf",
        media_type="application/dxf",
        render_fn=_render_revised_dxf_export_artifact,
        error_type=RevisedDxfExportError,
        lineage_anchor=_EXPORT_LINEAGE_ANCHOR_CHANGESET,
        error_details_fn=_export_changeset_error_details,
        error_mapper_fn=_map_revised_dxf_export_error,
    ),
    "dxf": _ExportKindSpec(
        format="dxf",
        media_type="application/dxf",
        render_fn=_render_dxf_export_artifact,
        error_type=RevisedDxfExportError,
        lineage_anchor=_EXPORT_LINEAGE_ANCHOR_REVISION,
        error_details_fn=_export_revision_error_details,
        error_mapper_fn=_map_revised_dxf_export_error,
    ),
}


def _get_export_kind_spec(export_kind: str) -> _ExportKindSpec:
    """Return the worker registry entry for a supported export kind."""
    export_spec = _EXPORT_KIND_SPECS.get(export_kind)
    if export_spec is None:
        raise _build_export_job_input_error(
            "Export job kind is not supported by the worker.",
            details={"export_kind": export_kind},
        )
    return export_spec


_JobAttemptLease = job_lifecycle._JobAttemptLease
_EnqueueIntentLease = job_recovery._EnqueueIntentLease
_ClaimedJobEnqueueIntent = job_recovery._ClaimedJobEnqueueIntent


_JobLockBootstrap = job_lifecycle._JobLockBootstrap
_LockedJobSource = job_lifecycle._LockedJobSource


class _PersistedJobCancellationHandle:
    """Cancellation handle backed by worker polling."""

    def __init__(self) -> None:
        self._cancel_requested = False

    def is_cancelled(self) -> bool:
        return self._cancel_requested

    def mark_cancelled(self) -> None:
        self._cancel_requested = True


class _JobProgressEventBridge:
    """Synchronous progress callback with async DB draining."""

    _STOP = object()

    def __init__(self, job_id: UUID, *, attempt_token: UUID) -> None:
        self._job_id = job_id
        self._attempt_token = attempt_token
        self._queue: asyncio.Queue[_QueuedJobEvent | object] = asyncio.Queue()
        self._drain_task = asyncio.create_task(self._drain())
        self._closed = False

    def callback(self, update: ProgressUpdate) -> None:
        if self._closed:
            raise RuntimeError("Progress callback received update after bridge closed.")

        self._queue.put_nowait(
            _QueuedJobEvent(
                level="info",
                message=update.message or f"Job progress: {update.stage}",
                data_json=_progress_event_data(update),
            )
        )

    async def flush(self) -> None:
        if self._closed:
            await self._drain_task
            return

        self._closed = True
        self._queue.put_nowait(self._STOP)
        await self._drain_task

    async def _drain(self) -> None:
        while True:
            queued = await self._queue.get()
            try:
                if queued is self._STOP:
                    return

                assert isinstance(queued, _QueuedJobEvent)
                await emit_job_event(
                    self._job_id,
                    level=queued.level,
                    message=queued.message,
                    data_json=queued.data_json,
                    attempt_token=self._attempt_token,
                )
            finally:
                self._queue.task_done()


celery_app = Celery(
    "draupnir",
    broker=settings.broker_url,
)
celery_app.conf.update(
    task_ignore_result=True,
    task_store_eager_result=False,
    task_publish_retry=False,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
)

# Auto-discover tasks from the jobs module
celery_app.autodiscover_tasks(["app.jobs"], force=True)


_utcnow = job_lifecycle._utcnow


def _get_worker_loop_runner() -> asyncio.Runner:
    """Return the reusable asyncio runner for sync Celery entrypoints."""
    global _WORKER_LOOP_RUNNER
    if _WORKER_LOOP_RUNNER is None:
        _WORKER_LOOP_RUNNER = asyncio.Runner()
    return _WORKER_LOOP_RUNNER


def _close_worker_loop_runner() -> None:
    """Close and clear the reusable asyncio runner."""
    global _WORKER_LOOP_RUNNER
    with _WORKER_LOOP_RUNNER_LOCK:
        runner = _WORKER_LOOP_RUNNER
        if runner is None:
            return
        runner.close()
        _WORKER_LOOP_RUNNER = None


def _run_worker_loop[WorkerLoopResultT](
    coro_factory: Callable[[], Coroutine[Any, Any, WorkerLoopResultT]],
) -> WorkerLoopResultT:
    """Run a worker coroutine on the process-local reusable event loop."""
    with _WORKER_LOOP_RUNNER_LOCK:
        return _get_worker_loop_runner().run(coro_factory())


atexit.register(_close_worker_loop_runner)


_clear_job_attempt_lease = job_lifecycle._clear_job_attempt_lease
_clear_enqueue_intent_lease = job_recovery._clear_enqueue_intent_lease
prepare_job_enqueue_intent = job_recovery.prepare_job_enqueue_intent


def _claim_job_attempt_lease(
    job: Job,
    *,
    now: datetime,
    increment_attempt: bool,
    stale_after: timedelta = _RUNNING_JOB_STALE_AFTER,
) -> _JobAttemptLease:
    """Mint and persist a fresh job-attempt ownership lease."""
    return job_lifecycle._claim_job_attempt_lease(
        job,
        now=now,
        increment_attempt=increment_attempt,
        stale_after=stale_after,
    )


_job_attempt_is_current = job_lifecycle._job_attempt_is_current
_renew_job_attempt_lease_for_update = job_lifecycle._renew_job_attempt_lease_for_update
_job_is_safe_recovery_failure_target = job_recovery._job_is_safe_recovery_failure_target


def _is_stale_running_job(
    job: Job,
    *,
    now: datetime,
    stale_after: timedelta = _RUNNING_JOB_STALE_AFTER,
) -> bool:
    """Return whether a running job is old enough to treat as orphaned."""
    return job_lifecycle._is_stale_running_job(
        job,
        now=now,
        stale_after=stale_after,
    )


async def _renew_job_attempt_lease(
    job_id: UUID,
    *,
    attempt_token: UUID,
    stale_after: timedelta = _RUNNING_JOB_STALE_AFTER,
) -> _JobAttemptLease | None:
    """Extend a persisted job-attempt lease if this worker still owns it."""
    return await _renew_job_attempt_lease_for_update(
        job_id,
        attempt_token=attempt_token,
        stale_after=stale_after,
        session_maker_factory=get_session_maker,
    )


async def _renew_job_attempt_lease_until_cancelled(
    job_id: UUID,
    *,
    attempt_token: UUID,
    stale_after: timedelta = _RUNNING_JOB_STALE_AFTER,
    interval: timedelta = _JOB_ATTEMPT_LEASE_RENEW_INTERVAL,
) -> None:
    """Keep an active job attempt from being reclaimed while execution is still alive."""
    while True:
        await asyncio.sleep(interval.total_seconds())
        lease = await _renew_job_attempt_lease(
            job_id,
            attempt_token=attempt_token,
            stale_after=stale_after,
        )
        if lease is None:
            raise _StaleJobAttemptError(f"Job attempt for '{job_id}' no longer owns the lease")


async def _with_job_attempt_lease_renewal[ResultT](
    work: Coroutine[Any, Any, ResultT],
    *,
    job_id: UUID,
    attempt_token: UUID,
) -> ResultT:
    """Run job work while a companion task renews the persisted attempt lease."""
    work_task = asyncio.create_task(work)
    renewal_task = asyncio.create_task(
        _renew_job_attempt_lease_until_cancelled(job_id, attempt_token=attempt_token)
    )
    try:
        done, _pending = await asyncio.wait(
            {work_task, renewal_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if renewal_task in done:
            work_task.cancel()
            with suppress(asyncio.CancelledError):
                await work_task
            await renewal_task

        renewal_task.cancel()
        with suppress(asyncio.CancelledError):
            await renewal_task
        return await work_task
    finally:
        for task in (work_task, renewal_task):
            if not task.done():
                task.cancel()
        with suppress(asyncio.CancelledError):
            await renewal_task


_is_stale_enqueue_intent = job_recovery._is_stale_enqueue_intent
_claim_enqueue_intent_lease = job_recovery._claim_enqueue_intent_lease


_get_job_for_update = job_lifecycle._get_job_for_update
_get_job_lock_bootstrap = job_lifecycle._get_job_lock_bootstrap
_get_project = job_lifecycle._get_project
_get_job_for_update_with_metadata = job_lifecycle._get_job_for_update_with_metadata
_get_source_file = job_lifecycle._get_source_file


# Module-global collaborator default, read by ``default_worker_deps`` so tests can
# monkeypatch ``worker_module.finalization_persister`` before starting a job flow.
finalization_persister: FinalizationPersister = DEFAULT_FINALIZATION_PERSISTER


def default_worker_deps() -> WorkerDeps:
    """Snapshot the current worker collaborators for one job flow.

    Reads worker.py's module globals at call time, so a test that monkeypatches
    e.g. ``worker_module.resolve_rate`` before starting a job flow is honored —
    and the extracted assembly/finalizers reach the patched collaborator through
    ``deps`` rather than their own module namespace (issue #387).
    """
    return WorkerDeps(
        resolve_rate=resolve_rate,
        resolve_material=resolve_material,
        resolve_formula=resolve_formula,
        get_storage=get_storage,
        emit_job_event=emit_job_event,
        get_project=_get_project,
        get_job_lock_bootstrap=_get_job_lock_bootstrap,
        lock_job_source=_lock_job_source_for_terminal_mutation,
        finalize_job_cancelled=_finalize_job_cancelled,
        cancel_job_for_inactive_source=_cancel_job_for_inactive_source,
        load_changeset_apply_job_input=_load_changeset_apply_job_input_if_valid,
        finalization_persister=finalization_persister,
    )


def worker_deps(**overrides: Any) -> WorkerDeps:
    """Build a WorkerDeps from the current defaults with explicit overrides.

    Preferred over monkeypatching ``worker`` module globals in tests:
    ``process_estimate_job(job_id, deps=worker_deps(resolve_rate=fake))``.
    """
    return replace(default_worker_deps(), **overrides)


async def _lock_job_source_for_terminal_mutation(
    session: AsyncSession,
    job_id: UUID,
) -> _LockedJobSource:
    """Lock project/job/file rows in the approved order for terminal writes."""
    return await job_lifecycle._lock_job_source_for_terminal_mutation(
        session,
        job_id,
        get_job_lock_bootstrap_func=_get_job_lock_bootstrap,
        get_project_func=_get_project,
        get_job_for_update_with_metadata_func=_get_job_for_update_with_metadata,
        get_source_file_func=_get_source_file,
    )


async def _get_extraction_profile(
    session: AsyncSession,
    *,
    extraction_profile_id: UUID,
) -> ExtractionProfile | None:
    """Load an extraction profile row by identifier."""

    return await session.get(ExtractionProfile, extraction_profile_id)


async def _get_drawing_revision_for_changeset(
    session: AsyncSession,
    *,
    project_id: UUID,
    change_set_id: UUID,
) -> DrawingRevision | None:
    """Load the committed drawing revision anchored to a changeset."""
    result = await session.execute(
        select(DrawingRevision)
        .where(
            (DrawingRevision.project_id == project_id)
            & (DrawingRevision.changeset_id == change_set_id)
        )
        .limit(1)
    )
    return result.scalar_one_or_none()


_get_enqueue_job_error_message = job_recovery._get_enqueue_job_error_message


def _requested_input_family_from_pdf_input_mode(
    pdf_input_mode: str | None,
) -> InputFamily | None:
    """Map persisted PDF input mode to an explicit runner input family override."""

    if pdf_input_mode == "vector":
        return InputFamily.PDF_VECTOR
    if pdf_input_mode == "raster":
        return InputFamily.PDF_RASTER
    return None


async def _build_ingestion_run_request(job_id: UUID, *, attempt_token: UUID) -> IngestionRunRequest:
    """Load persisted job and file metadata for the ingestion runner."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        bootstrap = await _get_job_lock_bootstrap(session, job_id)
        if bootstrap is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        project = await _get_project(session, bootstrap.project_id, for_update=True)
        if project is None:
            raise LookupError(
                f"Project with identifier '{bootstrap.project_id}' for job '{job_id}' not found"
            )

        job = await _get_job_for_update_with_metadata(
            session,
            job_id,
            expected_project_id=bootstrap.project_id,
            expected_file_id=bootstrap.file_id,
        )
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")
        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            raise _StaleJobAttemptError(f"Job attempt for '{job_id}' no longer owns the lease")
        _assert_job_base_revision_invariants(job)

        if project.deleted_at is not None:
            cancelled = await _cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
                attempt_token=attempt_token,
            )
            if not cancelled:
                raise _StaleJobAttemptError(f"Job attempt for '{job_id}' no longer owns the lease")
            raise _InactiveSourceError(
                f"Project with identifier '{job.project_id}' for job '{job_id}' is no longer active"
            )

        source_file = await _get_source_file(
            session,
            project_id=job.project_id,
            file_id=job.file_id,
            for_update=True,
        )
        if source_file is None or source_file.deleted_at is not None:
            cancelled = await _cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
                attempt_token=attempt_token,
            )
            if not cancelled:
                raise _StaleJobAttemptError(f"Job attempt for '{job_id}' no longer owns the lease")
            raise _InactiveSourceError(
                f"File with identifier '{job.file_id}' for job '{job_id}' is no longer active"
            )

        requested_input_family = None
        if job.extraction_profile_id is not None:
            extraction_profile = await _get_extraction_profile(
                session,
                extraction_profile_id=job.extraction_profile_id,
            )
            requested_input_family = _requested_input_family_from_pdf_input_mode(
                extraction_profile.pdf_input_mode if extraction_profile is not None else None
            )

        return IngestionRunRequest(
            job_id=job.id,
            file_id=source_file.id,
            checksum_sha256=source_file.checksum_sha256,
            detected_format=source_file.detected_format,
            media_type=source_file.media_type,
            original_name=source_file.original_filename,
            extraction_profile_id=job.extraction_profile_id,
            initial_job_id=source_file.initial_job_id,
            requested_input_family=requested_input_family,
        )


def _build_export_artifact_name(
    *,
    export_kind: str,
    export_format: str,
    drawing_revision_id: UUID,
    changeset_id: UUID | None = None,
    quantity_takeoff_id: UUID | None = None,
    estimate_version_id: UUID | None = None,
) -> str:
    """Build a deterministic filename for a generated export artifact."""
    export_spec = _get_export_kind_spec(export_kind)
    if export_spec.lineage_anchor == _EXPORT_LINEAGE_ANCHOR_REVISION:
        return f"revision-{drawing_revision_id}.{export_format}"
    if export_spec.lineage_anchor == _EXPORT_LINEAGE_ANCHOR_CHANGESET:
        assert changeset_id is not None
        return f"changeset-{changeset_id}.{export_format}"
    if export_spec.lineage_anchor == _EXPORT_LINEAGE_ANCHOR_QUANTITY_TAKEOFF:
        assert quantity_takeoff_id is not None
        return f"quantity-takeoff-{quantity_takeoff_id}.{export_format}"
    assert export_spec.lineage_anchor == _EXPORT_LINEAGE_ANCHOR_ESTIMATE_VERSION
    assert estimate_version_id is not None
    return f"estimate-{estimate_version_id}.{export_format}"


async def _render_export_artifact(
    session: AsyncSession,
    execution: _ExportExecutionInput,
) -> ExportArtifact:
    """Render bytes for a supported export job."""
    export_spec = _get_export_kind_spec(execution.export_kind)
    try:
        result = await export_spec.render_fn(session, execution)
    except Exception as exc:
        if isinstance(exc, export_spec.error_type):
            if export_spec.error_mapper_fn is not None:
                raise export_spec.error_mapper_fn(exc, execution) from exc
            raise _build_export_job_input_error(
                str(exc),
                error_code=ErrorCode.NOT_FOUND,
                details=export_spec.error_details_fn(execution),
            ) from exc
        raise

    if result.media_type != execution.media_type:
        raise ValueError("Rendered export media type does not match the persisted export job input")

    computed_checksum = hashlib.sha256(result.content_bytes).hexdigest()
    if computed_checksum != result.checksum_sha256:
        raise ValueError("Rendered export checksum does not match the generated bytes")

    if len(result.content_bytes) != result.size_bytes:
        raise ValueError("Rendered export size does not match the generated bytes")

    return result


class _SessionExportRowLoader:
    """Session-backed ``ExportRowLoader`` used by the worker export path."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_job(self, job_id: UUID) -> Job | None:
        return await self._session.get(Job, job_id)

    async def get_export_job_input(self, job_id: UUID) -> ExportJobInput | None:
        return await self._session.get(ExportJobInput, job_id)

    async def get_drawing_revision(self, revision_id: UUID) -> DrawingRevision | None:
        return await _get_drawing_revision(self._session, revision_id=revision_id)

    async def get_quantity_takeoff(self, takeoff_id: UUID) -> QuantityTakeoff | None:
        return await self._session.get(QuantityTakeoff, takeoff_id)

    async def get_estimate_version(self, version_id: UUID) -> EstimateVersion | None:
        return await self._session.get(EstimateVersion, version_id)


async def _build_export_execution_input(
    job_id: UUID,
    *,
    attempt_token: UUID,
) -> _ExportExecutionInput:
    """Load deterministic persisted inputs for a claimed export job."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        return await build_export_execution_input(
            job_id,
            attempt_token=attempt_token,
            loader=_SessionExportRowLoader(session),
            resolve_export_spec=_get_export_kind_spec,
            build_artifact_name=_build_export_artifact_name,
        )


class _SessionQuantityRowLoader:
    """Session-backed ``QuantityRowLoader`` used by the worker quantity path."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_job(self, job_id: UUID) -> Job | None:
        return await self._session.get(Job, job_id)

    async def get_drawing_revision(self, revision_id: UUID) -> DrawingRevision | None:
        return await _get_drawing_revision(self._session, revision_id=revision_id)

    async def get_validation_report(
        self, *, project_id: UUID, drawing_revision_id: UUID
    ) -> ValidationReport | None:
        return await _get_validation_report_for_revision(
            self._session,
            project_id=project_id,
            drawing_revision_id=drawing_revision_id,
        )

    async def get_entity_manifest(
        self, *, project_id: UUID, source_file_id: UUID, drawing_revision_id: UUID
    ) -> RevisionEntityManifest | None:
        return await _get_revision_entity_manifest_for_revision(
            self._session,
            project_id=project_id,
            source_file_id=source_file_id,
            drawing_revision_id=drawing_revision_id,
        )

    async def get_revision_entities(
        self, *, project_id: UUID, source_file_id: UUID, drawing_revision_id: UUID
    ) -> list[RevisionEntity]:
        return await _get_revision_entities_for_revision(
            self._session,
            project_id=project_id,
            source_file_id=source_file_id,
            drawing_revision_id=drawing_revision_id,
        )


async def _build_quantity_takeoff_execution_input(
    job_id: UUID,
    *,
    attempt_token: UUID,
) -> _QuantityTakeoffExecutionInput:
    """Load unlocked quantity engine inputs for a claimed persisted job."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        return await build_quantity_takeoff_execution_input(
            job_id,
            attempt_token=attempt_token,
            loader=_SessionQuantityRowLoader(session),
        )


async def emit_job_event(
    job_id: UUID,
    *,
    level: str,
    message: str,
    data_json: dict[str, Any] | None = None,
    attempt_token: UUID | None = None,
    session: AsyncSession | None = None,
) -> bool:
    """Persist a job lifecycle event."""
    return await job_lifecycle.emit_job_event(
        job_id,
        level=level,
        message=message,
        data_json=data_json,
        attempt_token=attempt_token,
        session=session,
        get_job_for_update_func=_get_job_for_update,
        job_attempt_is_current_func=_job_attempt_is_current,
        session_maker_factory=get_session_maker,
    )


def _progress_event_data(update: ProgressUpdate) -> dict[str, Any]:
    """Build a stable persisted progress event payload."""
    data_json: dict[str, Any] = {
        "status": "running",
        "event": "progress",
        "stage": update.stage,
    }
    if update.message is not None:
        data_json["detail"] = update.message
    if update.completed is not None:
        data_json["completed"] = update.completed
    if update.total is not None:
        data_json["total"] = update.total
    if update.percent is not None:
        data_json["percent"] = update.percent
    return data_json


def _runner_error_log_fields(exc: IngestionRunnerError) -> dict[str, Any]:
    """Return whitelisted structured fields for expected runner failures."""
    data_json: dict[str, Any] = {
        "error_code": exc.error_code.value,
        "failure_kind": exc.failure_kind.value,
        "error_message": exc.message,
    }
    for key in _SAFE_RUNNER_ERROR_DETAIL_KEYS:
        value = exc.details.get(key)
        if value is not None:
            data_json[key] = value
    return data_json


def _runner_supports_keyword(runner: Any, keyword: str) -> bool:
    """Return whether a runner callable accepts a given keyword."""
    signature = inspect.signature(runner)
    parameters = signature.parameters.values()
    if any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters):
        return True
    return keyword in signature.parameters


async def _invoke_ingestion_runner(
    request: IngestionRunRequest,
    *,
    timeout: AdapterTimeout,
    cancellation: _PersistedJobCancellationHandle,
    on_progress: Any,
) -> IngestFinalizationPayload:
    """Call the runner while remaining compatible with patched test doubles."""
    kwargs: dict[str, Any] = {}
    if _runner_supports_keyword(run_ingestion, "timeout"):
        kwargs["timeout"] = timeout
    if _runner_supports_keyword(run_ingestion, "cancellation"):
        kwargs["cancellation"] = cancellation
    if _runner_supports_keyword(run_ingestion, "on_progress"):
        kwargs["on_progress"] = on_progress
    return await run_ingestion(request, **kwargs)


async def _poll_job_cancellation(
    job_id: UUID,
    *,
    attempt_token: UUID,
    cancellation: _PersistedJobCancellationHandle,
    run_task: asyncio.Task[IngestFinalizationPayload],
    stop_event: asyncio.Event,
) -> None:
    """Poll persisted cancellation without holding DB locks during execution."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    while not stop_event.is_set() and not cancellation.is_cancelled():
        async with session_maker() as session:
            job = await session.get(Job, job_id)

        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if job.cancel_requested:
            cancellation.mark_cancelled()
            run_task.cancel()
            return

        if job.status == "cancelled":
            cancellation.mark_cancelled()
            run_task.cancel()
            return

        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            return

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=_JOB_CANCELLATION_POLL_INTERVAL_SECONDS,
            )
        except TimeoutError:
            continue


async def _cancel_registered_job_if_requested(
    job_id: UUID,
    *,
    attempt_token: UUID,
    log_event: str,
) -> bool:
    """Honor a cancellation requested before compute begins; return True if cancelled.

    Ingest runs a continuous cancellation poll because its adapter work yields at
    ``await`` points. The other job types compute synchronously (``compute_quantities``,
    ``compose_estimate``, export rendering) and block the event loop while they run, so
    they cannot be preempted mid-compute without threading cancellation into otherwise
    pure deterministic code. This checkpoint closes the common race — a cancel issued
    after the attempt was claimed but before compute starts — so such cancels take effect
    immediately instead of waiting until finalization. Cancellation that arrives *during*
    a synchronous compute is still only observed at the finalize row-lock.
    """
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await session.get(Job, job_id)

    if job is None:
        raise LookupError(f"Job with identifier '{job_id}' not found")

    if not _job_attempt_is_current(job, attempt_token=attempt_token):
        return False

    if not job.cancel_requested and job.status != "cancelled":
        return False

    cancelled = await _mark_job_cancelled(job_id, attempt_token=attempt_token)
    if cancelled:
        logger.info(log_event, job_id=str(job_id))
    return cancelled


async def _stop_job_execution_monitor(
    *,
    progress_bridge: _JobProgressEventBridge,
    stop_event: asyncio.Event,
    cancellation_task: asyncio.Task[None],
) -> None:
    """Flush queued progress and stop background execution monitors."""
    stop_event.set()
    try:
        await cancellation_task
    finally:
        await progress_bridge.flush()


async def _persist_job_failed(
    session: AsyncSession,
    job: Job,
    *,
    error_message: str,
    error_code: ErrorCode,
    error_details: dict[str, Any] | None = None,
) -> None:
    """Persist a failed job state and matching event within an active session."""
    await job_lifecycle._persist_job_failed(
        session,
        job,
        error_message=error_message,
        error_code=error_code,
        error_details=error_details,
        utcnow_func=_utcnow,
        clear_job_attempt_lease_func=_clear_job_attempt_lease,
        emit_job_event_func=emit_job_event,
    )


async def _mark_job_failed(
    job_id: UUID,
    *,
    error_message: str,
    error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
    attempt_token: UUID | None = None,
    error_details: dict[str, Any] | None = None,
) -> bool:
    """Persist a failed job state with the supplied message."""
    return await job_lifecycle._mark_job_failed(
        job_id,
        error_message=error_message,
        error_code=error_code,
        terminal_job_statuses=_TERMINAL_JOB_STATUSES,
        attempt_token=attempt_token,
        error_details=error_details,
        session_maker_factory=get_session_maker,
        lock_job_source_for_terminal_mutation_func=_lock_job_source_for_terminal_mutation,
        job_attempt_is_current_func=_job_attempt_is_current,
        cancel_job_for_inactive_source_func=_cancel_job_for_inactive_source,
        finalize_job_cancelled_func=_finalize_job_cancelled,
        persist_job_failed_func=_persist_job_failed,
        emit_job_event_func=emit_job_event,
        logger_instance=logger,
    )


async def _mark_job_cancelled(job_id: UUID, *, attempt_token: UUID | None = None) -> bool:
    """Persist a cancelled job state."""
    return await job_lifecycle._mark_job_cancelled(
        job_id,
        terminal_job_statuses=_TERMINAL_JOB_STATUSES,
        attempt_token=attempt_token,
        session_maker_factory=get_session_maker,
        get_job_for_update_func=_get_job_for_update,
        job_attempt_is_current_func=_job_attempt_is_current,
        finalize_job_cancelled_func=_finalize_job_cancelled,
        emit_job_event_func=emit_job_event,
        logger_instance=logger,
    )


def _ensure_worker_database_configured() -> None:
    """Raise when persisted worker processing is invoked without DB access."""
    if get_session_maker() is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")


async def _mark_job_cancelled_with_log(
    job_id: UUID,
    *,
    attempt_token: UUID,
    log_event: str,
    log_fields: dict[str, Any] | None = None,
) -> None:
    """Persist a cancelled terminal state and emit the matching worker log."""
    await _mark_job_cancelled(job_id, attempt_token=attempt_token)
    logger.info(log_event, job_id=str(job_id), **(log_fields or {}))


async def _mark_job_failed_for_revision_conflict(
    job_id: UUID,
    *,
    attempt_token: UUID,
    log_event: str,
    exc: _RevisionConflictError,
) -> None:
    """Persist and log the shared revision-conflict failure contract."""
    await _mark_job_failed(
        job_id,
        error_message=exc.message,
        error_code=ErrorCode.REVISION_CONFLICT,
        attempt_token=attempt_token,
        error_details=exc.details,
    )
    logger.warning(
        log_event,
        job_id=str(job_id),
        error_code=ErrorCode.REVISION_CONFLICT.value,
        **exc.details,
    )


async def _mark_job_failed_with_internal_error_log(
    job_id: UUID,
    *,
    attempt_token: UUID,
    error_message: str,
    log_event: str,
    log_fields: dict[str, Any] | None = None,
) -> None:
    """Persist and log unexpected internal worker failures."""
    await _mark_job_failed(
        job_id,
        error_message=error_message,
        attempt_token=attempt_token,
    )
    logger.error(
        log_event,
        job_id=str(job_id),
        **(
            log_fields
            if log_fields is not None
            else {
                "error_code": ErrorCode.INTERNAL_ERROR.value,
                "error_message": error_message,
            }
        ),
        exc_info=True,
    )


async def _mark_job_failed_if_recovery_safe(
    job_id: UUID,
    *,
    error_message: str,
    error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
    error_details: dict[str, Any] | None = None,
) -> bool:
    """Fail a recovered job only if it is still pending and unowned."""
    return await job_recovery._mark_job_failed_if_recovery_safe(
        job_id,
        error_message=error_message,
        error_code=error_code,
        error_details=error_details,
        lock_job_source_for_terminal_mutation_func=_lock_job_source_for_terminal_mutation,
        cancel_job_for_inactive_source_func=_cancel_job_for_inactive_source,
        persist_job_failed_func=_persist_job_failed,
        logger_instance=logger,
    )


async def _claim_job_enqueue_intent(job_id: UUID) -> _ClaimedJobEnqueueIntent | None:
    """Claim a durable enqueue intent for best-effort or recovery publication."""
    return await job_recovery._claim_job_enqueue_intent(
        job_id,
        is_recoverable_enqueue_job_type_func=is_recoverable_enqueue_job_type,
    )


async def _release_job_enqueue_intent(job_id: UUID, *, lease_token: UUID) -> bool:
    """Release a claimed durable enqueue intent after a publish failure."""
    return await job_recovery._release_job_enqueue_intent(job_id, lease_token=lease_token)


async def _mark_job_enqueue_published(job_id: UUID, *, lease_token: UUID) -> bool:
    """Finalize a claimed durable enqueue intent after broker publication."""
    return await job_recovery._mark_job_enqueue_published(job_id, lease_token=lease_token)


async def publish_job_enqueue_intent(
    job_id: UUID,
    *,
    recovery: bool = False,
    publisher: Callable[[UUID], None] | None = None,
    suppress_exceptions: bool = False,
) -> bool:
    """Best-effort publish for a durable enqueue intent recorded in Postgres."""
    return await job_recovery.publish_job_enqueue_intent(
        job_id,
        recovery=recovery,
        publisher=publisher,
        suppress_exceptions=suppress_exceptions,
        publisher_resolver=get_job_enqueue_publisher,
        claim_job_enqueue_intent_func=_claim_job_enqueue_intent,
        release_job_enqueue_intent_func=_release_job_enqueue_intent,
        mark_job_enqueue_published_func=_mark_job_enqueue_published,
        mark_recovery_enqueue_failed_func=_mark_recovery_enqueue_failed,
    )


async def _mark_recovery_enqueue_failed(job_id: UUID, *, job_type: str) -> bool:
    """Persist and log a sanitized worker-recovery enqueue failure."""
    return await job_recovery._mark_recovery_enqueue_failed(
        job_id,
        job_type=job_type,
        mark_job_failed_if_recovery_safe_func=_mark_job_failed_if_recovery_safe,
        logger_instance=logger,
    )


def _finalize_job_cancelled(job: Job) -> None:
    """Apply the persisted cancelled terminal state to a job."""
    job_lifecycle._finalize_job_cancelled(
        job,
        cancelled_error_code=_JOB_CANCELLED_ERROR_CODE,
        utcnow_func=_utcnow,
        clear_job_attempt_lease_func=_clear_job_attempt_lease,
    )


async def _cancel_job_for_inactive_source(
    session: AsyncSession,
    job: Job,
    *,
    reason: str,
    attempt_token: UUID | None = None,
) -> bool:
    """Persist cancellation when a job source project/file is no longer active."""
    return await job_lifecycle._cancel_job_for_inactive_source(
        session,
        job,
        reason=reason,
        attempt_token=attempt_token,
        terminal_job_statuses=_TERMINAL_JOB_STATUSES,
        job_attempt_is_current_func=_job_attempt_is_current,
        finalize_job_cancelled_func=_finalize_job_cancelled,
        emit_job_event_func=emit_job_event,
    )


def _get_begin_or_resume_route(process_name: str) -> job_runner.BeginOrResumeRoute:
    """Return registry-backed begin/resume metadata for a worker process."""
    route = job_runner.get_begin_or_resume_route(process_name)
    if route is None:
        raise LookupError(f"No begin/resume route registered for process '{process_name}'")
    return route


async def _begin_or_resume_registered_job(
    job_id: UUID,
    *,
    process_name: str,
) -> _JobAttemptLease | None:
    """Claim, resume, or cancel a persisted worker job via registry metadata."""
    route = _get_begin_or_resume_route(process_name)
    return await job_lifecycle._begin_or_resume_job(
        job_id,
        supported_job_types=route.supported_job_types,
        terminal_job_statuses=_TERMINAL_JOB_STATUSES,
        stale_after=_RUNNING_JOB_STALE_AFTER,
        log_keys=job_lifecycle._BeginOrResumeLogKeys(
            unsupported_type=route.log_keys.unsupported_type,
            terminal_status=route.log_keys.terminal_status,
            inactive_source=route.log_keys.inactive_source,
            reclaimed_stale_running=route.log_keys.reclaimed_stale_running,
            duplicate_delivery=route.log_keys.duplicate_delivery,
            max_attempts_exceeded=route.log_keys.max_attempts_exceeded,
            cancelled=route.log_keys.cancelled,
        ),
        session_maker_factory=get_session_maker,
        lock_job_source_for_terminal_mutation_func=_lock_job_source_for_terminal_mutation,
        cancel_job_for_inactive_source_func=_cancel_job_for_inactive_source,
        claim_job_attempt_lease_func=_claim_job_attempt_lease,
        is_stale_running_job_func=_is_stale_running_job,
        finalize_job_cancelled_func=_finalize_job_cancelled,
        emit_job_event_func=emit_job_event,
        logger_instance=logger,
    )


async def _begin_or_resume_ingest_job(job_id: UUID) -> _JobAttemptLease | None:
    """Claim, resume, or cancel a persisted ingest job under a row lock."""
    return await _begin_or_resume_registered_job(job_id, process_name="process_ingest_job")


async def _begin_or_resume_quantity_takeoff_job(job_id: UUID) -> _JobAttemptLease | None:
    """Claim, resume, or cancel a persisted quantity takeoff job under a row lock."""
    return await _begin_or_resume_registered_job(
        job_id,
        process_name="process_quantity_takeoff_job",
    )


async def _begin_or_resume_estimate_job(job_id: UUID) -> _JobAttemptLease | None:
    """Claim, resume, or cancel a persisted estimate job under a row lock."""
    return await _begin_or_resume_registered_job(job_id, process_name="process_estimate_job")


async def _begin_or_resume_export_job(job_id: UUID) -> _JobAttemptLease | None:
    """Claim, resume, or cancel a persisted export job under a row lock."""
    return await _begin_or_resume_registered_job(job_id, process_name="process_export_job")


async def _begin_or_resume_changeset_apply_job(job_id: UUID) -> _JobAttemptLease | None:
    """Claim, resume, or cancel a persisted changeset apply job under a row lock."""
    return await _begin_or_resume_registered_job(
        job_id,
        process_name="process_changeset_apply_job",
    )


async def _begin_or_resume_centerline_job(job_id: UUID) -> _JobAttemptLease | None:
    """Claim, resume, or cancel a persisted centerline job under a row lock."""
    return await _begin_or_resume_registered_job(job_id, process_name="process_centerline_job")


async def _begin_or_resume_rooms_job(job_id: UUID) -> _JobAttemptLease | None:
    """Claim, resume, or cancel a persisted rooms job under a row lock."""
    return await _begin_or_resume_registered_job(job_id, process_name="process_rooms_job")


async def _execute_ingest_job_attempt(
    job_id: UUID,
    *,
    attempt_token: UUID,
    deps: WorkerDeps,
) -> IngestFinalizationPayload:
    """Build inputs, run ingestion, and drain progress/cancellation monitors."""
    _ = deps  # uniform dispatch contract; ingest execution has no injected collaborators
    request = await _build_ingestion_run_request(job_id, attempt_token=attempt_token)
    progress_bridge = _JobProgressEventBridge(job_id, attempt_token=attempt_token)
    cancellation = _PersistedJobCancellationHandle()
    stop_event = asyncio.Event()
    run_task = asyncio.create_task(
        _invoke_ingestion_runner(
            request,
            timeout=AdapterTimeout(seconds=settings.adapter_timeout_seconds),
            cancellation=cancellation,
            on_progress=progress_bridge.callback,
        )
    )
    cancellation_task = asyncio.create_task(
        _poll_job_cancellation(
            job_id,
            attempt_token=attempt_token,
            cancellation=cancellation,
            run_task=run_task,
            stop_event=stop_event,
        )
    )
    try:
        return await run_task
    finally:
        await _stop_job_execution_monitor(
            progress_bridge=progress_bridge,
            stop_event=stop_event,
            cancellation_task=cancellation_task,
        )


async def _handle_ingest_runner_error(
    job_id: UUID,
    *,
    attempt_token: UUID,
    exc: IngestionRunnerError,
) -> None:
    """Persist and log the expected ingest runner failure contract."""
    if exc.error_code is ErrorCode.JOB_CANCELLED:
        await _mark_job_cancelled_with_log(
            job_id,
            attempt_token=attempt_token,
            log_event="ingest_job_cancelled_during_execution",
            log_fields={"error_code": exc.error_code.value},
        )
        return

    await _mark_job_failed(
        job_id,
        error_message=exc.message,
        error_code=exc.error_code,
        attempt_token=attempt_token,
        error_details=_runner_error_log_fields(exc),
    )
    logger.error("ingest_job_failed", job_id=str(job_id), **_runner_error_log_fields(exc))


def _build_ingest_finalization_error_log_fields(exc: Exception) -> dict[str, str]:
    """Preserve the legacy ingest finalization error log payload."""
    return {"error": str(exc)}


def _get_registered_job_handler(job_type_name: job_runner.JobTypeName) -> job_runner.JobHandler:
    """Return registered worker metadata for one persisted job type."""
    handler = job_runner.get_job_handler(job_type_name)
    if handler is None:
        raise LookupError(f"No worker handler registered for job type '{job_type_name}'")
    return handler


def _resolve_registered_job_callable(name: str) -> Any:
    """Late-bind a worker callable by name for monkeypatch-friendly wrappers."""
    resolved = globals().get(name)
    if resolved is None:
        raise LookupError(f"Worker callable '{name}' is not defined")
    return resolved


async def _process_registered_job(
    job_id: UUID, *, spec: _RegisteredJobProcessSpec, deps: WorkerDeps
) -> None:
    """Run one registered persisted worker job through the shared execution shell."""
    _ensure_worker_database_configured()

    handler = _get_registered_job_handler(spec.job_type_name)
    execute_name = handler.execute_name
    finalize_name = handler.finalize_name
    if execute_name is None:
        raise LookupError(f"Worker handler '{spec.job_type_name}' is missing execute_name")
    if finalize_name is None:
        raise LookupError(f"Worker handler '{spec.job_type_name}' is missing finalize_name")

    lease = await _begin_or_resume_registered_job(job_id, process_name=handler.process_name)
    if lease is None:
        return

    if await _cancel_registered_job_if_requested(
        job_id,
        attempt_token=lease.token,
        log_event=spec.cancelled_during_execution_log_event,
    ):
        return

    execute_job = cast(Any, _resolve_registered_job_callable(execute_name))
    finalize_job = cast(Any, _resolve_registered_job_callable(finalize_name))

    try:
        execution_result = await _with_job_attempt_lease_renewal(
            execute_job(job_id, attempt_token=lease.token, deps=deps),
            job_id=job_id,
            attempt_token=lease.token,
        )
    except _InactiveSourceError:
        if spec.inactive_source_log_event is None:
            raise
        logger.info(spec.inactive_source_log_event, job_id=str(job_id))
        return
    except _StaleJobAttemptError:
        logger.info(spec.stale_attempt_log_event, job_id=str(job_id))
        return
    except _RevisionConflictError as exc:
        if spec.job_type_name == "changeset_apply":
            await _mark_changeset_apply_job_failed_for_revision_conflict(
                job_id,
                attempt_token=lease.token,
                log_event=spec.revision_conflict_log_event,
                exc=exc,
            )
        else:
            await _mark_job_failed_for_revision_conflict(
                job_id,
                attempt_token=lease.token,
                log_event=spec.revision_conflict_log_event,
                exc=exc,
            )
        if spec.reraise_revision_conflict:
            raise
        return
    except asyncio.CancelledError:
        await _mark_job_cancelled_with_log(
            job_id,
            attempt_token=lease.token,
            log_event=spec.cancelled_during_execution_log_event,
        )
        raise
    except Exception as exc:
        if spec.input_error_type is not None and isinstance(exc, spec.input_error_type):
            input_error = cast(_RegisteredJobInputError, exc)
            failure_details = input_error.details
            if spec.input_failure_log_event is None:
                raise AssertionError(
                    f"Registered job '{spec.job_type_name}' is missing input_failure_log_event"
                ) from exc
            if spec.job_type_name == "changeset_apply":
                assert isinstance(input_error, _ChangeSetApplyJobError)
                await _mark_changeset_apply_job_failed_for_input_error(
                    job_id,
                    attempt_token=lease.token,
                    log_event=spec.input_failure_log_event,
                    exc=input_error,
                )
            else:
                await _mark_job_failed(
                    job_id,
                    error_message=input_error.message,
                    error_code=input_error.error_code,
                    attempt_token=lease.token,
                    error_details=failure_details,
                )
                logger.warning(
                    spec.input_failure_log_event,
                    job_id=str(job_id),
                    error_code=input_error.error_code.value,
                    **(failure_details or {}),
                )
            return

        if spec.execution_error_type is not None and isinstance(exc, spec.execution_error_type):
            if spec.execution_error_handler_name is None:
                raise AssertionError(
                    f"Registered job '{spec.job_type_name}' is missing execution_error_handler_name"
                ) from exc
            handle_execution_error = cast(
                Any,
                _resolve_registered_job_callable(spec.execution_error_handler_name),
            )
            await handle_execution_error(job_id, attempt_token=lease.token, exc=exc)
            raise

        await _mark_job_failed_with_internal_error_log(
            job_id,
            attempt_token=lease.token,
            error_message=spec.process_error_message,
            log_event=spec.process_failed_log_event,
        )
        raise

    if execution_result is None:
        return

    if isinstance(execution_result, _RegisteredJobAttemptResult):
        finalize_kwargs = execution_result.finalize_kwargs
    else:
        if spec.execution_result_arg_name is None:
            raise TypeError(
                "Registered job "
                f"'{spec.job_type_name}' returned a raw execution result without "
                "execution_result_arg_name"
            )
        finalize_kwargs = {spec.execution_result_arg_name: execution_result}

    try:
        finalized = await _with_job_attempt_lease_renewal(
            finalize_job(
                job_id,
                attempt_token=lease.token,
                deps=deps,
                **finalize_kwargs,
            ),
            job_id=job_id,
            attempt_token=lease.token,
        )
    except asyncio.CancelledError:
        await _mark_job_cancelled_with_log(
            job_id,
            attempt_token=lease.token,
            log_event=spec.cancelled_during_finalization_log_event,
        )
        raise
    except _RevisionConflictError as exc:
        if spec.job_type_name == "changeset_apply":
            await _mark_changeset_apply_job_failed_for_revision_conflict(
                job_id,
                attempt_token=lease.token,
                log_event=spec.revision_conflict_log_event,
                exc=exc,
            )
        else:
            await _mark_job_failed_for_revision_conflict(
                job_id,
                attempt_token=lease.token,
                log_event=spec.revision_conflict_log_event,
                exc=exc,
            )
        if spec.reraise_revision_conflict:
            raise
        return
    except Exception as exc:
        log_fields: dict[str, Any] | None = None
        if spec.finalization_exception_log_fields_name is not None:
            build_log_fields = cast(
                Callable[[Exception], dict[str, Any]],
                _resolve_registered_job_callable(spec.finalization_exception_log_fields_name),
            )
            log_fields = build_log_fields(exc)
        await _mark_job_failed_with_internal_error_log(
            job_id,
            attempt_token=lease.token,
            error_message=spec.finalize_error_message,
            log_event=spec.finalization_failed_log_event,
            log_fields=log_fields,
        )
        raise

    if finalized:
        logger.info(spec.succeeded_log_event, job_id=str(job_id))


async def _execute_quantity_takeoff_job_attempt(
    job_id: UUID,
    *,
    attempt_token: UUID,
    deps: WorkerDeps,
) -> _RegisteredJobAttemptResult | None:
    """Build quantity inputs, compute deterministic outputs, and defer finalization."""
    _ = deps  # uniform dispatch contract; no injected collaborators in this stage
    execution = await _build_quantity_takeoff_execution_input(job_id, attempt_token=attempt_token)
    # Quantities are always computed; the gate is recorded as informational
    # provenance on the takeoff rather than blocking the job (Path B 2).
    result = compute_quantities(execution.gate, execution.entities)
    if result.conflicts:
        error_details = {
            "drawing_revision_id": str(execution.drawing_revision_id),
            "conflict_count": len(result.conflicts),
            "conflicts": _build_quantity_conflict_summaries(result.conflicts),
        }
        await _mark_job_failed(
            job_id,
            error_message=_QUANTITY_TAKEOFF_CONFLICT_ERROR_MESSAGE,
            error_code=ErrorCode.INPUT_INVALID,
            attempt_token=attempt_token,
            error_details=error_details,
        )
        logger.warning(
            "quantity_takeoff_job_conflicts_detected",
            job_id=str(job_id),
            error_code=ErrorCode.INPUT_INVALID.value,
            **error_details,
        )
        return None

    return _RegisteredJobAttemptResult(finalize_kwargs={"execution": execution, "result": result})


async def _execute_centerline_job_attempt(
    _job_id: UUID,
    *,
    attempt_token: UUID,
    deps: WorkerDeps,
) -> _RegisteredJobAttemptResult:
    """Defer all centerline work to the finalization step (no separate execute phase)."""
    _ = deps  # uniform dispatch contract; execution is embedded in the finalizer
    _ = attempt_token
    return _RegisteredJobAttemptResult(finalize_kwargs={})


async def _execute_rooms_job_attempt(
    _job_id: UUID,
    *,
    attempt_token: UUID,
    deps: WorkerDeps,
) -> _RegisteredJobAttemptResult:
    """Defer all rooms work to the finalization step (no separate execute phase)."""
    _ = deps  # uniform dispatch contract; execution is embedded in the finalizer
    _ = attempt_token
    return _RegisteredJobAttemptResult(finalize_kwargs={})


async def _execute_estimate_job_attempt(
    job_id: UUID,
    *,
    attempt_token: UUID,
    deps: WorkerDeps,
) -> _RegisteredJobAttemptResult:
    """Build deterministic estimate inputs and defer output persistence."""
    engine_input = await _build_estimate_engine_input(
        job_id, attempt_token=attempt_token, deps=deps
    )
    try:
        estimate_output = compose_estimate(engine_input)
    except EstimateEngineError as exc:
        raise _EstimateJobInputError(
            error_code=ErrorCode.INPUT_INVALID,
            message=_ESTIMATE_JOB_INPUT_INVALID_ERROR_MESSAGE,
            details={"reason": exc.reason},
        ) from exc

    logger.info(
        "estimate_job_composed_pending_finalization",
        job_id=str(job_id),
        quantity_entry_count=len(engine_input.quantity_entries),
        rate_entry_count=len(engine_input.rate_entries),
        material_entry_count=len(engine_input.material_entries),
        formula_entry_count=len(engine_input.formula_entries),
        line_count=len(engine_input.line_inputs),
    )
    return _RegisteredJobAttemptResult(finalize_kwargs={"output": estimate_output})


async def _execute_export_job(
    job_id: UUID,
    *,
    attempt_token: UUID,
    deps: WorkerDeps,
) -> _RegisteredJobAttemptResult:
    """Build export inputs, render deterministic bytes, and defer finalization."""
    _ = deps  # uniform dispatch contract; no injected collaborators in this stage
    execution = await _build_export_execution_input(job_id, attempt_token=attempt_token)
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        rendered = await _render_export_artifact(session, execution)

    return _RegisteredJobAttemptResult(
        finalize_kwargs={"execution": execution, "rendered": rendered}
    )


async def _query_changeset_apply_job_inputs(
    session: AsyncSession,
    *,
    job_id: UUID,
) -> list[ChangeSetApplyJobInput]:
    """Load persisted immutable inputs for one changeset-apply job."""
    with session.no_autoflush:
        result = await session.execute(
            select(ChangeSetApplyJobInput)
            .where(ChangeSetApplyJobInput.source_job_id == job_id)
            .order_by(
                ChangeSetApplyJobInput.created_at.asc(),
                ChangeSetApplyJobInput.source_job_id.asc(),
            )
        )
    return list(result.scalars().all())


async def _load_changeset_apply_job_input(
    session: AsyncSession,
    *,
    job: Job,
) -> ChangeSetApplyJobInput:
    """Validate that one immutable apply input exists and matches the persisted job."""
    apply_inputs = await _query_changeset_apply_job_inputs(session, job_id=job.id)
    if not apply_inputs:
        raise _ChangeSetApplyJobError(
            error_code=ErrorCode.NOT_FOUND,
            message="Changeset apply job input is missing.",
            details=None,
        )
    if len(apply_inputs) != 1:
        raise _ChangeSetApplyJobError(
            error_code=ErrorCode.INPUT_INVALID,
            message="Changeset apply job has multiple immutable inputs.",
            details={"input_count": len(apply_inputs)},
        )

    apply_input = apply_inputs[0]
    if (
        apply_input.project_id != job.project_id
        or apply_input.source_file_id != job.file_id
        or apply_input.drawing_revision_id != job.base_revision_id
        or apply_input.source_job_id != job.id
        or apply_input.source_job_type != JobType.CHANGESET_APPLY.value
    ):
        raise _ChangeSetApplyJobError(
            error_code=ErrorCode.INPUT_INVALID,
            message="Changeset apply job input lineage does not match the persisted job.",
            details={
                "source_job_type": apply_input.source_job_type,
                "source_file_id": str(apply_input.source_file_id),
                "file_id": str(job.file_id),
                "drawing_revision_id": str(apply_input.drawing_revision_id),
                "base_revision_id": (
                    str(job.base_revision_id) if job.base_revision_id is not None else None
                ),
            },
        )
    return apply_input


async def _load_changeset_apply_job_input_if_valid(
    session: AsyncSession,
    *,
    job: Job,
) -> ChangeSetApplyJobInput | None:
    """Return the immutable apply input when exactly one lineage-matching row exists."""
    apply_inputs = await _query_changeset_apply_job_inputs(session, job_id=job.id)
    if len(apply_inputs) != 1:
        return None

    apply_input = apply_inputs[0]
    if (
        apply_input.project_id != job.project_id
        or apply_input.source_file_id != job.file_id
        or apply_input.drawing_revision_id != job.base_revision_id
        or apply_input.source_job_id != job.id
        or apply_input.source_job_type != JobType.CHANGESET_APPLY.value
    ):
        return None
    return apply_input


def _parse_uuid_value(value: Any) -> UUID | None:
    """Best-effort UUID parsing for persisted error payloads."""
    if isinstance(value, UUID):
        return value
    if not isinstance(value, str):
        return None
    try:
        return UUID(value)
    except ValueError:
        return None


async def _resolve_changeset_apply_failure_target(
    session: AsyncSession,
    *,
    job: Job,
    fallback_change_set_id: UUID | None = None,
) -> CadChangeSet | None:
    """Resolve the mutable changeset row for an apply-job terminal failure."""
    apply_input = await _load_changeset_apply_job_input_if_valid(session, job=job)
    change_set_id = apply_input.change_set_id if apply_input is not None else fallback_change_set_id
    if change_set_id is None:
        return None

    change_set = await session.get(CadChangeSet, change_set_id)
    if change_set is None or change_set.project_id != job.project_id:
        return None
    if job.base_revision_id is not None and change_set.base_revision_id != job.base_revision_id:
        return None
    return change_set


async def _mark_changeset_apply_job_failed(
    job_id: UUID,
    *,
    error_message: str,
    error_code: ErrorCode,
    attempt_token: UUID,
    error_details: dict[str, Any] | None,
    change_set_status: str | None,
) -> bool:
    """Persist a changeset-apply job failure and matching changeset status atomically."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    fallback_change_set_id = _parse_uuid_value((error_details or {}).get("change_set_id"))
    async with session_maker() as session:
        locked_source = await _lock_job_source_for_terminal_mutation(session, job_id)
        job = locked_source.job

        if job.status in _TERMINAL_JOB_STATUSES:
            return False

        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            return False

        if (
            locked_source.project.deleted_at is not None
            or locked_source.source_file is None
            or locked_source.source_file.deleted_at is not None
        ):
            return await _cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
                attempt_token=attempt_token,
            )

        if change_set_status is not None:
            change_set = await _resolve_changeset_apply_failure_target(
                session,
                job=job,
                fallback_change_set_id=fallback_change_set_id,
            )
            if change_set is not None and change_set.status != "applied":
                change_set.status = change_set_status

        await _persist_job_failed(
            session,
            job,
            error_message=error_message,
            error_code=error_code,
            error_details=error_details,
        )
        await session.commit()

    return True


async def _mark_changeset_apply_job_failed_for_revision_conflict(
    job_id: UUID,
    *,
    attempt_token: UUID,
    log_event: str,
    exc: _RevisionConflictError,
) -> None:
    """Persist and log a changeset-apply revision conflict atomically."""
    await _mark_changeset_apply_job_failed(
        job_id,
        error_message=exc.message,
        error_code=ErrorCode.REVISION_CONFLICT,
        attempt_token=attempt_token,
        error_details=exc.details,
        change_set_status="revision_conflict",
    )
    logger.warning(
        log_event,
        job_id=str(job_id),
        error_code=ErrorCode.REVISION_CONFLICT.value,
        **exc.details,
    )


async def _mark_changeset_apply_job_failed_for_input_error(
    job_id: UUID,
    *,
    attempt_token: UUID,
    log_event: str,
    exc: _ChangeSetApplyJobError,
) -> None:
    """Persist and log a deterministic changeset-apply failure atomically."""
    failure_details = exc.details
    await _mark_changeset_apply_job_failed(
        job_id,
        error_message=exc.message,
        error_code=exc.error_code,
        attempt_token=attempt_token,
        error_details=failure_details,
        change_set_status="apply_failed",
    )
    logger.warning(
        log_event,
        job_id=str(job_id),
        error_code=exc.error_code.value,
        **(failure_details or {}),
    )


def _build_changeset_apply_error_details(result: ChangeSetApplyError) -> dict[str, Any]:
    """Convert a deterministic apply-engine failure into worker job failure details."""
    details: dict[str, Any] = {
        "change_set_id": str(result.change_set_id),
        "apply_error_code": result.error_code,
    }
    if result.details:
        details.update(dict(result.details))
    return details


async def _execute_changeset_apply_job_attempt(
    job_id: UUID,
    *,
    attempt_token: UUID,
    deps: WorkerDeps,
) -> _RegisteredJobAttemptResult | None:
    """Load immutable apply input, re-run apply loading, and defer success finalization."""
    _ = deps  # uniform dispatch contract; no injected collaborators in this stage
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await session.get(Job, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")
        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            raise _StaleJobAttemptError(f"Job attempt for '{job_id}' no longer owns the lease")
        if job.job_type != JobType.CHANGESET_APPLY.value:
            raise ValueError(f"Unsupported changeset apply job type '{job.job_type}'")
        if job.base_revision_id is None:
            raise _RevisionConflictError(
                message="Changeset apply job is missing its finalized base revision.",
                details={
                    "base_revision_id": None,
                    "current_revision_id": None,
                },
            )

        apply_input = await _load_changeset_apply_job_input(session, job=job)
        try:
            apply_result = await load_and_apply_change_set(
                session,
                project_id=job.project_id,
                change_set_id=apply_input.change_set_id,
            )
        except ChangeSetApplyLoadError as exc:
            raise _ChangeSetApplyJobError(
                error_code=ErrorCode.INPUT_INVALID,
                message=str(exc),
                details={
                    "change_set_id": str(apply_input.change_set_id),
                },
            ) from exc

    if isinstance(apply_result, ChangeSetApplyConflict):
        raise _RevisionConflictError(
            message="Changeset apply base revision is stale relative to the current revision.",
            details=_build_changeset_apply_conflict_details(apply_result),
        )
    if isinstance(apply_result, ChangeSetApplyError):
        raise _ChangeSetApplyJobError(
            error_code=ErrorCode.INPUT_INVALID,
            message=apply_result.message,
            details=_build_changeset_apply_error_details(apply_result),
        )

    assert isinstance(apply_result, ChangeSetApplySuccess)
    return _RegisteredJobAttemptResult(finalize_kwargs={"apply_result": apply_result})


async def process_ingest_job(job_id: UUID, *, deps: WorkerDeps | None = None) -> None:
    """Load a persisted ingest job, run ingestion, and persist state transitions."""
    await _process_registered_job(
        job_id, spec=_INGEST_PROCESS_SPEC, deps=deps or default_worker_deps()
    )


async def recover_incomplete_jobs() -> list[UUID]:
    """Requeue incomplete persisted worker jobs on worker startup."""
    return await job_recovery.recover_incomplete_jobs(
        recoverable_enqueue_job_types=_RECOVERABLE_ENQUEUE_JOB_TYPES,
        publish_job_enqueue_intent_func=publish_job_enqueue_intent,
    )


async def recover_incomplete_ingest_jobs() -> list[UUID]:
    """Compatibility alias for incomplete worker job recovery."""
    return await job_recovery.recover_incomplete_ingest_jobs(
        recoverable_enqueue_job_types=_RECOVERABLE_ENQUEUE_JOB_TYPES,
        publish_job_enqueue_intent_func=publish_job_enqueue_intent,
    )


def recover_incomplete_ingest_jobs_on_worker_start(**_: object) -> None:
    """Requeue incomplete persisted worker jobs when a worker starts."""
    job_recovery.recover_incomplete_ingest_jobs_on_worker_start(
        run_worker_loop_func=_run_worker_loop,
        recover_incomplete_ingest_jobs_func=recover_incomplete_ingest_jobs,
    )


worker_ready.connect(recover_incomplete_ingest_jobs_on_worker_start)


@celery_app.task(
    name="app.jobs.worker.run_ingest_job",
    ignore_result=True,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_ingest_job(job_id: str) -> None:
    """Celery task wrapper for the persisted ingest job processor."""
    _run_worker_loop(lambda: process_ingest_job(UUID(job_id)))


def enqueue_ingest_job(job_id: UUID) -> None:
    """Publish a persisted ingest job to Celery."""
    run_ingest_job.apply_async(
        args=(str(job_id),),
        task_id=str(job_id),
        retry=False,
        countdown=_current_enqueue_countdown(),
    )


async def process_quantity_takeoff_job(job_id: UUID, *, deps: WorkerDeps | None = None) -> None:
    """Load a persisted quantity takeoff job and atomically persist its result."""
    await _process_registered_job(
        job_id, spec=_QUANTITY_TAKEOFF_PROCESS_SPEC, deps=deps or default_worker_deps()
    )


@celery_app.task(
    name="app.jobs.worker.run_quantity_takeoff_job",
    ignore_result=True,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_quantity_takeoff_job(job_id: str) -> None:
    """Celery task wrapper for persisted quantity takeoff jobs."""
    _run_worker_loop(lambda: process_quantity_takeoff_job(UUID(job_id)))


def enqueue_quantity_takeoff_job(job_id: UUID) -> None:
    """Publish a persisted quantity takeoff job to Celery."""
    run_quantity_takeoff_job.apply_async(
        args=(str(job_id),),
        task_id=str(job_id),
        retry=False,
        countdown=_current_enqueue_countdown(),
    )


async def process_estimate_job(job_id: UUID, *, deps: WorkerDeps | None = None) -> None:
    """Load a persisted estimate job and assemble deterministic engine inputs."""
    await _process_registered_job(
        job_id, spec=_ESTIMATE_PROCESS_SPEC, deps=deps or default_worker_deps()
    )


@celery_app.task(
    name="app.jobs.worker.run_estimate_job",
    ignore_result=True,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_estimate_job(job_id: str) -> None:
    """Celery task wrapper for persisted estimate jobs."""
    _run_worker_loop(lambda: process_estimate_job(UUID(job_id)))


def enqueue_estimate_job(job_id: UUID) -> None:
    """Publish a persisted estimate job to Celery."""
    run_estimate_job.apply_async(
        args=(str(job_id),),
        task_id=str(job_id),
        retry=False,
        countdown=_current_enqueue_countdown(),
    )


async def process_export_job(job_id: UUID, *, deps: WorkerDeps | None = None) -> None:
    """Load a persisted export job and atomically persist its generated artifact."""
    await _process_registered_job(
        job_id, spec=_EXPORT_PROCESS_SPEC, deps=deps or default_worker_deps()
    )


async def process_changeset_apply_job(job_id: UUID, *, deps: WorkerDeps | None = None) -> None:
    """Load a persisted changeset apply job through the shared worker shell."""
    await _process_registered_job(
        job_id, spec=_CHANGESET_APPLY_PROCESS_SPEC, deps=deps or default_worker_deps()
    )


@celery_app.task(
    name="app.jobs.worker.run_export_job",
    ignore_result=True,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_export_job(job_id: str) -> None:
    """Celery task wrapper for persisted export jobs."""
    _run_worker_loop(lambda: process_export_job(UUID(job_id)))


def enqueue_export_job(job_id: UUID) -> None:
    """Publish a persisted export job to Celery."""

    run_export_job.apply_async(
        args=(str(job_id),),
        task_id=str(job_id),
        retry=False,
        countdown=_current_enqueue_countdown(),
    )


@celery_app.task(
    name="app.jobs.worker.run_changeset_apply_job",
    ignore_result=True,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_changeset_apply_job(job_id: str) -> None:
    """Celery task wrapper for persisted changeset apply jobs."""
    _run_worker_loop(lambda: process_changeset_apply_job(UUID(job_id)))


def enqueue_changeset_apply_job(job_id: UUID) -> None:
    """Publish a persisted changeset apply job to Celery."""
    run_changeset_apply_job.apply_async(
        args=(str(job_id),),
        task_id=str(job_id),
        retry=False,
        countdown=_current_enqueue_countdown(),
    )


async def process_centerline_job(job_id: UUID, *, deps: WorkerDeps | None = None) -> None:
    """Load a persisted centerline job and materialize per-group routed lengths."""
    await _process_registered_job(
        job_id, spec=_CENTERLINE_PROCESS_SPEC, deps=deps or default_worker_deps()
    )


@celery_app.task(
    name="app.jobs.worker.run_centerline_job",
    ignore_result=True,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_centerline_job(job_id: str) -> None:
    """Celery task wrapper for persisted centerline jobs."""
    _run_worker_loop(lambda: process_centerline_job(UUID(job_id)))


def enqueue_centerline_job(job_id: UUID) -> None:
    """Publish a persisted centerline job to Celery."""
    run_centerline_job.apply_async(
        args=(str(job_id),),
        task_id=str(job_id),
        retry=False,
        countdown=_current_enqueue_countdown(),
    )


async def process_rooms_job(job_id: UUID, *, deps: WorkerDeps | None = None) -> None:
    """Load a persisted rooms job and materialize the full room registry."""
    await _process_registered_job(
        job_id, spec=_ROOMS_PROCESS_SPEC, deps=deps or default_worker_deps()
    )


@celery_app.task(
    name="app.jobs.worker.run_rooms_job",
    ignore_result=True,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_rooms_job(job_id: str) -> None:
    """Celery task wrapper for persisted rooms jobs."""
    _run_worker_loop(lambda: process_rooms_job(UUID(job_id)))


def enqueue_rooms_job(job_id: UUID) -> None:
    """Publish a persisted rooms job to Celery."""
    run_rooms_job.apply_async(
        args=(str(job_id),),
        task_id=str(job_id),
        retry=False,
        countdown=_current_enqueue_countdown(),
    )
