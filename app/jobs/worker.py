"""Celery worker application and persisted job handlers."""

# ruff: noqa: SLF001

import asyncio
import atexit
import hashlib
import inspect
import json
import math
import threading
import uuid
from collections.abc import Callable, Coroutine, Sequence
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, cast
from uuid import UUID

from celery import Celery
from celery.signals import worker_ready
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.cad.changeset import (
    ChangeSetApplyConflict,
    ChangeSetApplyError,
    ChangeSetApplyLoadError,
    ChangeSetApplySuccess,
    load_and_apply_change_set,
)
from app.core.clock import utcnow as _clock_utcnow
from app.core.config import settings
from app.core.errors import ErrorCode
from app.core.logging import get_logger
from app.db.session import get_session_maker
from app.estimating.catalog import CatalogFormulaRef, CatalogMaterialRef, CatalogRateRef
from app.estimating.catalog.resolver import resolve_formula, resolve_material, resolve_rate
from app.estimating.catalog.selection import CatalogSelectionError
from app.estimating.engine import formula_definition_from_selected_formula
from app.estimating.engine.contracts import (
    EstimateEngineInput,
    EstimateEngineOutput,
    EstimateFormulaEntryInput,
    EstimateLineInputSpec,
    EstimateMaterialEntryInput,
    EstimateQuantityEntryInput,
    EstimateRateEntryInput,
)
from app.estimating.engine.errors import EstimateEngineError
from app.estimating.engine.service import compose_estimate
from app.estimating.quantities.contracts import (
    GateStatus,
    QuantityEngineResult,
    RevisionEntityInput,
    RevisionGateMetadata,
)
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
from app.ingestion.debug_overlay import plan_svg_debug_overlay
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunnerError, IngestionRunRequest, run_ingestion
from app.jobs import lifecycle as job_lifecycle
from app.jobs import runner as job_runner
from app.jobs.conflict_diagnostics import (
    _build_changeset_apply_conflict_details,
    _build_quantity_conflict_summaries,
    _quantity_gate_details,
)
from app.jobs.estimate_mapping import (
    _ESTIMATE_JOB_INPUT_INVALID_ERROR_MESSAGE,
    _build_estimate_job_input_error,
    _build_estimate_worker_mapping_v1,
    _estimate_formula_binding_tokens,
    _EstimateJobInputError,
    _EstimateWorkerLineInput,
    _resolve_formula_binding_snapshot_key,
)
from app.jobs.report_lineage import (
    _build_changeset_validation_report_json,
    _build_debug_overlay_lineage_json,
    _build_export_artifact_lineage_json,
    _build_persisted_validation_report_json,
)
from app.jobs.revision_materialization import (
    _build_changeset_revision_materialization_rows,
    _build_revision_materialization_rows,
    _order_revision_entity_insert_rows,
)
from app.jobs.worker_deps import WorkerDeps
from app.models.adapter_run_output import AdapterRunOutput
from app.models.cad_changeset import CadChangeSet
from app.models.changeset_apply_job_input import ChangeSetApplyJobInput
from app.models.drawing_revision import DrawingRevision
from app.models.estimate_job_input import EstimateJobInput, EstimateJobInputCatalogRef
from app.models.estimate_version import EstimateItem, EstimateSnapshotEntry, EstimateVersion
from app.models.export_job_input import ExportJobInput
from app.models.extraction_profile import ExtractionProfile
from app.models.file import File
from app.models.generated_artifact import GeneratedArtifact
from app.models.job import Job, JobType
from app.models.quantity_takeoff import QuantityItem, QuantityItemKind, QuantityTakeoff
from app.models.revision_materialization import (
    RevisionBlock,
    RevisionEntity,
    RevisionEntityManifest,
    RevisionLayer,
    RevisionLayout,
)
from app.models.validation_report import ValidationReport
from app.storage import get_storage
from app.storage.keys import build_generated_artifact_storage_key

logger = get_logger(__name__)

_RECOVERABLE_INGEST_JOB_TYPES = job_runner.INGEST_WORKER_JOB_TYPES
_RECOVERABLE_ENQUEUE_JOB_TYPES = job_runner.RECOVERABLE_ENQUEUE_JOB_TYPES
_KNOWN_ENQUEUE_JOB_TYPES_WITHOUT_PUBLISHER = job_runner.JOB_TYPES_WITHOUT_ENQUEUE_PUBLISHER
_TERMINAL_JOB_STATUSES = {"failed", "succeeded", "cancelled"}
_ENQUEUE_STATUS_PENDING = "pending"
_ENQUEUE_STATUS_PUBLISHING = "publishing"
_ENQUEUE_STATUS_PUBLISHED = "published"
_DEFAULT_ADAPTER_TIMEOUT = timedelta(minutes=5)
_RUNNING_JOB_STALE_AFTER = _DEFAULT_ADAPTER_TIMEOUT * 2
_ENQUEUE_LEASE_DURATION = timedelta(minutes=1)
_JOB_CANCELLATION_POLL_INTERVAL_SECONDS = 0.1
# Capped exponential backoff between job attempts. The first attempt runs with no
# delay; each subsequent re-enqueue waits base * 2**(attempt-1), capped, so a job
# that keeps failing is spaced out instead of being retried as fast as the broker
# can redeliver it (the attempt count is still bounded by ``Job.max_attempts``).
_ENQUEUE_BACKOFF_BASE_SECONDS = 5.0
_ENQUEUE_BACKOFF_MAX_SECONDS = 300.0


def _enqueue_backoff_seconds(attempts: int) -> float:
    """Return the retry delay for a job that has already made ``attempts`` tries."""
    if attempts <= 1:
        return 0.0
    delay = _ENQUEUE_BACKOFF_BASE_SECONDS * (2.0 ** (attempts - 2))
    return min(delay, _ENQUEUE_BACKOFF_MAX_SECONDS)


# Carries the computed backoff to the real ``enqueue_*`` publishers without changing
# their ``(job_id)`` call signature (test fakes and direct calls see the 0.0 default).
_ENQUEUE_COUNTDOWN_SECONDS: ContextVar[float] = ContextVar("enqueue_countdown_seconds", default=0.0)


def _current_enqueue_countdown() -> float | None:
    """Return the active enqueue backoff in seconds, or None when there is none."""
    countdown = _ENQUEUE_COUNTDOWN_SECONDS.get()
    return countdown if countdown > 0.0 else None


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
_ENQUEUE_ESTIMATE_JOB_ERROR_MESSAGE = job_runner.ENQUEUE_ERROR_MESSAGES_BY_JOB_TYPE[
    JobType.ESTIMATE.value
]
_ENQUEUE_EXPORT_JOB_ERROR_MESSAGE = job_runner.ENQUEUE_ERROR_MESSAGES_BY_JOB_TYPE[
    JobType.EXPORT.value
]
_ENQUEUE_CHANGESET_APPLY_JOB_ERROR_MESSAGE = job_runner.ENQUEUE_ERROR_MESSAGES_BY_JOB_TYPE[
    JobType.CHANGESET_APPLY.value
]
_FINALIZE_INGEST_JOB_ERROR_MESSAGE = "Failed to finalize ingest job"
_PROCESS_INGEST_JOB_ERROR_MESSAGE = "Ingest job failed unexpectedly."
_FINALIZE_QUANTITY_TAKEOFF_JOB_ERROR_MESSAGE = "Failed to finalize quantity takeoff job"
_FINALIZE_ESTIMATE_JOB_ERROR_MESSAGE = "Failed to finalize estimate job"
_FINALIZE_EXPORT_JOB_ERROR_MESSAGE = "Failed to finalize export job"
_FINALIZE_CHANGESET_APPLY_JOB_ERROR_MESSAGE = "Failed to finalize changeset apply job"
_PROCESS_QUANTITY_TAKEOFF_JOB_ERROR_MESSAGE = "Quantity takeoff job failed unexpectedly."
_PROCESS_ESTIMATE_JOB_ERROR_MESSAGE = "Estimate job failed unexpectedly."
_PROCESS_EXPORT_JOB_ERROR_MESSAGE = "Export job failed unexpectedly."
_PROCESS_CHANGESET_APPLY_JOB_ERROR_MESSAGE = "Changeset apply job failed unexpectedly."
_INITIAL_INGEST_REVISION_KIND = "ingest"
_REPROCESS_REVISION_KIND = "reprocess"
_DEBUG_OVERLAY_ARTIFACT_KIND = "debug_overlay"
_DEBUG_OVERLAY_ARTIFACT_FORMAT = "svg"
_DEBUG_OVERLAY_GENERATOR_NAME = "app.ingestion.debug_overlay"
_DEBUG_OVERLAY_GENERATOR_VERSION = "1"
_NORMALIZED_ENTITY_INSERT_CHUNK_SIZE = 500
_QUANTITY_TAKEOFF_GATE_BLOCKED_ERROR_MESSAGE = (
    "Quantity takeoff is blocked by the revision validation gate."
)
_QUANTITY_TAKEOFF_CONFLICT_ERROR_MESSAGE = (
    "Quantity takeoff detected conflicting contributor inputs."
)
_QUANTITY_TAKEOFF_VALIDATION_REPORT_MISSING_ERROR_MESSAGE = (
    "Quantity takeoff base revision is missing its validation report."
)
_QUANTITY_TAKEOFF_MATERIALIZATION_MISSING_ERROR_MESSAGE = (
    "Quantity takeoff base revision is missing normalized entities."
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


@dataclass(frozen=True, slots=True)
class _RevisionConflictError(Exception):
    """Raised when a persisted ingest/reprocess revision base is invalid or stale."""

    message: str
    details: dict[str, Any]

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True, slots=True)
class _QueuedJobEvent:
    """Buffered job event persisted by the progress drain."""

    level: str
    message: str
    data_json: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _QuantityTakeoffJobError(Exception):
    """Raised for deterministic quantity takeoff failures."""

    error_code: ErrorCode
    message: str
    details: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True, slots=True)
class _ExportJobInputError(Exception):
    """Raised for deterministic export job input failures."""

    error_code: ErrorCode
    message: str
    details: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


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


@dataclass(frozen=True, slots=True)
class _ExportExecutionInput:
    """Resolved persisted inputs for a supported export job."""

    drawing_revision_id: UUID
    export_kind: str
    export_format: str
    media_type: str
    artifact_name: str
    options_json: dict[str, Any]
    changeset_id: UUID | None = None
    quantity_takeoff_id: UUID | None = None
    estimate_version_id: UUID | None = None


_ExportRenderFn = Callable[
    [AsyncSession, _ExportExecutionInput],
    Coroutine[Any, Any, ExportArtifact],
]
_ExportErrorDetailsFn = Callable[[_ExportExecutionInput], dict[str, Any]]
_ExportErrorMapperFn = Callable[[Exception, _ExportExecutionInput], _ExportJobInputError]

_EXPORT_LINEAGE_ANCHOR_REVISION = "revision"
_EXPORT_LINEAGE_ANCHOR_CHANGESET = "changeset"
_EXPORT_LINEAGE_ANCHOR_QUANTITY_TAKEOFF = "quantity_takeoff"
_EXPORT_LINEAGE_ANCHOR_ESTIMATE_VERSION = "estimate_version"
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


@dataclass(frozen=True, slots=True)
class _EnqueueIntentLease:
    """Persisted ownership token for a claimed durable enqueue intent."""

    token: UUID
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class _ClaimedJobEnqueueIntent:
    """Persisted enqueue claim bundled with the routed worker job type."""

    lease: _EnqueueIntentLease
    job_type: str
    attempts: int


@dataclass(frozen=True, slots=True)
class _QuantityTakeoffExecutionInput:
    """Loaded immutable quantity takeoff execution inputs."""

    drawing_revision_id: UUID
    review_state: str
    validation_status: str
    quantity_gate: str
    gate: RevisionGateMetadata
    entities: list[RevisionEntityInput]


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


def _utcnow() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return _clock_utcnow()


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


def _clear_job_attempt_lease(job: Job) -> None:
    """Clear persisted ownership fencing for a job attempt."""
    job.attempt_token = None
    job.attempt_lease_expires_at = None


def _clear_enqueue_intent_lease(job: Job) -> None:
    """Clear persisted ownership fencing for a durable enqueue intent."""
    job.enqueue_owner_token = None
    job.enqueue_lease_expires_at = None


def prepare_job_enqueue_intent(job: Job) -> None:
    """Reset a job's durable enqueue intent to the pending outbox state."""
    job.enqueue_status = _ENQUEUE_STATUS_PENDING
    job.enqueue_attempts = 0
    job.enqueue_last_attempted_at = None
    job.enqueue_published_at = None
    _clear_enqueue_intent_lease(job)


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


def _job_is_safe_recovery_failure_target(job: Job) -> bool:
    """Return whether recovery can still safely mark the job failed."""
    return (
        job.status == "pending"
        and job.attempt_token is None
        and job.attempt_lease_expires_at is None
        and job.enqueue_status == _ENQUEUE_STATUS_PENDING
        and job.enqueue_owner_token is None
        and job.enqueue_lease_expires_at is None
    )


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


def _is_stale_enqueue_intent(job: Job, *, now: datetime) -> bool:
    """Return whether an in-flight enqueue publish claim can be reclaimed."""
    lease_expires_at = job.enqueue_lease_expires_at
    if lease_expires_at is None:
        return True
    if lease_expires_at.tzinfo is None:
        lease_expires_at = lease_expires_at.replace(tzinfo=UTC)
    return lease_expires_at <= now


def _claim_enqueue_intent_lease(job: Job, *, now: datetime) -> _EnqueueIntentLease:
    """Mint and persist a fresh ownership lease for broker publication."""
    token = uuid.uuid4()
    lease_expires_at = now + _ENQUEUE_LEASE_DURATION
    job.enqueue_status = _ENQUEUE_STATUS_PUBLISHING
    job.enqueue_attempts += 1
    job.enqueue_owner_token = token
    job.enqueue_lease_expires_at = lease_expires_at
    job.enqueue_last_attempted_at = now
    return _EnqueueIntentLease(token=token, lease_expires_at=lease_expires_at)


_get_job_for_update = job_lifecycle._get_job_for_update
_get_job_lock_bootstrap = job_lifecycle._get_job_lock_bootstrap
_get_project = job_lifecycle._get_project
_get_job_for_update_with_metadata = job_lifecycle._get_job_for_update_with_metadata
_get_source_file = job_lifecycle._get_source_file


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


async def _get_existing_adapter_run_output(
    session: AsyncSession,
    *,
    source_job_id: UUID,
) -> AdapterRunOutput | None:
    """Load an existing committed adapter output for a job."""
    result = await session.execute(
        select(AdapterRunOutput).where(AdapterRunOutput.source_job_id == source_job_id)
    )
    return result.scalar_one_or_none()


async def _get_existing_quantity_takeoff(
    session: AsyncSession,
    *,
    source_job_id: UUID,
) -> QuantityTakeoff | None:
    """Load an existing committed quantity takeoff for a job."""
    result = await session.execute(
        select(QuantityTakeoff).where(QuantityTakeoff.source_job_id == source_job_id)
    )
    return result.scalar_one_or_none()


async def _get_existing_estimate_version(
    session: AsyncSession,
    *,
    source_job_id: UUID,
) -> EstimateVersion | None:
    """Load an existing committed estimate version for a job."""
    result = await session.execute(
        select(EstimateVersion).where(EstimateVersion.source_job_id == source_job_id)
    )
    return result.scalar_one_or_none()


async def _get_existing_drawing_revision(
    session: AsyncSession,
    *,
    source_job_id: UUID,
) -> DrawingRevision | None:
    """Load an existing committed drawing revision for a job."""
    result = await session.execute(
        select(DrawingRevision).where(DrawingRevision.source_job_id == source_job_id)
    )
    return result.scalar_one_or_none()


async def _get_existing_generated_artifact(
    session: AsyncSession,
    *,
    source_job_id: UUID,
) -> GeneratedArtifact | None:
    """Load an existing committed generated artifact for a job."""
    result = await session.execute(
        select(GeneratedArtifact)
        .where(
            (GeneratedArtifact.job_id == source_job_id) & (GeneratedArtifact.deleted_at.is_(None))
        )
        .order_by(GeneratedArtifact.created_at.asc(), GeneratedArtifact.id.asc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def _get_latest_drawing_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    source_file_id: UUID,
) -> DrawingRevision | None:
    """Load the latest drawing revision for a source file."""
    result = await session.execute(
        select(DrawingRevision)
        .where(
            (DrawingRevision.project_id == project_id)
            & (DrawingRevision.source_file_id == source_file_id)
        )
        .order_by(DrawingRevision.revision_sequence.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def _get_drawing_revision(
    session: AsyncSession,
    *,
    revision_id: UUID,
) -> DrawingRevision | None:
    """Load a drawing revision row by identifier."""

    return await session.get(DrawingRevision, revision_id)


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


async def _get_validation_report_for_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    drawing_revision_id: UUID,
) -> ValidationReport | None:
    """Load the canonical validation report for a drawing revision."""
    result = await session.execute(
        select(ValidationReport).where(
            (ValidationReport.project_id == project_id)
            & (ValidationReport.drawing_revision_id == drawing_revision_id)
        )
    )
    return result.scalar_one_or_none()


async def _get_revision_layouts_for_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    source_file_id: UUID,
    drawing_revision_id: UUID,
) -> list[RevisionLayout]:
    """Load deterministic layout rows for a drawing revision."""
    result = await session.execute(
        select(RevisionLayout)
        .where(
            (RevisionLayout.project_id == project_id)
            & (RevisionLayout.source_file_id == source_file_id)
            & (RevisionLayout.drawing_revision_id == drawing_revision_id)
        )
        .order_by(RevisionLayout.sequence_index.asc(), RevisionLayout.id.asc())
    )
    return list(result.scalars().all())


async def _get_revision_layers_for_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    source_file_id: UUID,
    drawing_revision_id: UUID,
) -> list[RevisionLayer]:
    """Load deterministic layer rows for a drawing revision."""
    result = await session.execute(
        select(RevisionLayer)
        .where(
            (RevisionLayer.project_id == project_id)
            & (RevisionLayer.source_file_id == source_file_id)
            & (RevisionLayer.drawing_revision_id == drawing_revision_id)
        )
        .order_by(RevisionLayer.sequence_index.asc(), RevisionLayer.id.asc())
    )
    return list(result.scalars().all())


async def _get_revision_blocks_for_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    source_file_id: UUID,
    drawing_revision_id: UUID,
) -> list[RevisionBlock]:
    """Load deterministic block rows for a drawing revision."""
    result = await session.execute(
        select(RevisionBlock)
        .where(
            (RevisionBlock.project_id == project_id)
            & (RevisionBlock.source_file_id == source_file_id)
            & (RevisionBlock.drawing_revision_id == drawing_revision_id)
        )
        .order_by(RevisionBlock.sequence_index.asc(), RevisionBlock.id.asc())
    )
    return list(result.scalars().all())


async def _get_revision_entity_manifest_for_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    source_file_id: UUID,
    drawing_revision_id: UUID,
) -> RevisionEntityManifest | None:
    """Load the revision entity manifest for a drawing revision."""
    result = await session.execute(
        select(RevisionEntityManifest).where(
            (RevisionEntityManifest.project_id == project_id)
            & (RevisionEntityManifest.source_file_id == source_file_id)
            & (RevisionEntityManifest.drawing_revision_id == drawing_revision_id)
        )
    )
    return result.scalar_one_or_none()


async def _get_revision_entities_for_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    source_file_id: UUID,
    drawing_revision_id: UUID,
) -> list[RevisionEntity]:
    """Load deterministic materialized entities for a drawing revision."""
    result = await session.execute(
        select(RevisionEntity)
        .where(
            (RevisionEntity.project_id == project_id)
            & (RevisionEntity.source_file_id == source_file_id)
            & (RevisionEntity.drawing_revision_id == drawing_revision_id)
        )
        .order_by(RevisionEntity.sequence_index.asc(), RevisionEntity.id.asc())
    )
    return list(result.scalars().all())


def _expected_revision_kind_for_job(job: Job) -> str:
    """Return the expected persisted revision kind for a job type."""

    if job.job_type == JobType.INGEST.value:
        return _INITIAL_INGEST_REVISION_KIND
    if job.job_type == JobType.REPROCESS.value:
        return _REPROCESS_REVISION_KIND
    raise ValueError(f"Unsupported ingest job type '{job.job_type}'")


def _assert_job_base_revision_invariants(job: Job) -> None:
    """Reject persisted jobs whose job_type/base_revision_id pairing is invalid."""

    if job.job_type == JobType.INGEST.value:
        if job.base_revision_id is not None:
            raise ValueError("Initial ingest job cannot retain a base revision")
        return

    if job.job_type == JobType.REPROCESS.value:
        if job.base_revision_id is None:
            raise _RevisionConflictError(
                message="Reprocess job is missing its finalized base revision.",
                details={
                    "base_revision_id": None,
                    "base_revision_sequence": None,
                    "current_revision_id": None,
                    "current_revision_sequence": None,
                },
            )
        return

    raise ValueError(f"Unsupported ingest job type '{job.job_type}'")


def _revision_reference(
    revision: DrawingRevision | None,
) -> tuple[str | None, int | None]:
    """Return stable revision identifier/sequence details for conflict payloads."""

    if revision is None:
        return None, None

    return str(revision.id), revision.revision_sequence


def _build_revision_conflict_details(
    *,
    base_revision: DrawingRevision | None,
    current_revision: DrawingRevision | None,
) -> dict[str, str | int | None]:
    """Build structured stale-base details for durable job events."""

    base_revision_id, base_revision_sequence = _revision_reference(base_revision)
    current_revision_id, current_revision_sequence = _revision_reference(current_revision)
    return {
        "base_revision_id": base_revision_id,
        "base_revision_sequence": base_revision_sequence,
        "current_revision_id": current_revision_id,
        "current_revision_sequence": current_revision_sequence,
    }


async def _resolve_finalization_predecessor_revision(
    session: AsyncSession,
    *,
    job: Job,
    source_file: File,
    payload_revision_kind: str,
) -> DrawingRevision | None:
    """Validate job lineage invariants and return the predecessor revision to append to."""

    _assert_job_base_revision_invariants(job)
    expected_revision_kind = _expected_revision_kind_for_job(job)
    if payload_revision_kind != expected_revision_kind:
        raise _RevisionConflictError(
            message="Ingest job revision kind changed before finalization.",
            details={
                "expected_revision_kind": expected_revision_kind,
                "payload_revision_kind": payload_revision_kind,
            },
        )

    current_revision = await _get_latest_drawing_revision(
        session,
        project_id=job.project_id,
        source_file_id=source_file.id,
    )
    if expected_revision_kind == _INITIAL_INGEST_REVISION_KIND:
        if current_revision is not None:
            raise _RevisionConflictError(
                message="Initial ingest cannot finalize after another revision already exists.",
                details=_build_revision_conflict_details(
                    base_revision=None,
                    current_revision=current_revision,
                ),
            )
        return None

    assert job.base_revision_id is not None
    base_revision = await _get_drawing_revision(session, revision_id=job.base_revision_id)
    if base_revision is None:
        raise _RevisionConflictError(
            message="Reprocess job base revision no longer exists.",
            details=_build_revision_conflict_details(
                base_revision=None,
                current_revision=current_revision,
            ),
        )
    if base_revision.project_id != job.project_id or base_revision.source_file_id != source_file.id:
        raise ValueError("Reprocess job base revision does not belong to the source file")
    if current_revision is None or current_revision.id != base_revision.id:
        raise _RevisionConflictError(
            message="Reprocess base revision became stale before finalization.",
            details=_build_revision_conflict_details(
                base_revision=base_revision,
                current_revision=current_revision,
            ),
        )

    return base_revision


async def _get_generated_artifact_for_revision(
    session: AsyncSession,
    *,
    project_id: UUID,
    drawing_revision_id: UUID,
    artifact_kind: str,
) -> GeneratedArtifact | None:
    """Load a committed artifact of a given kind for a drawing revision."""
    result = await session.execute(
        select(GeneratedArtifact)
        .where(
            (GeneratedArtifact.project_id == project_id)
            & (GeneratedArtifact.drawing_revision_id == drawing_revision_id)
            & (GeneratedArtifact.artifact_kind == artifact_kind)
            & (GeneratedArtifact.deleted_at.is_(None))
        )
        .limit(1)
    )
    return result.scalar_one_or_none()


def _build_debug_overlay_generator_config(
    *,
    title: str,
    source_label: str,
    review_state: str,
    confidence_score: float,
) -> dict[str, Any]:
    """Build persisted generator settings for the debug overlay artifact."""
    return {
        "title": title,
        "source_label": source_label,
        "review_state": review_state,
        "confidence_score": confidence_score,
    }


def _estimate_input_checksum(payload: dict[str, Any]) -> str:
    """Build a deterministic checksum for worker-synthesized estimate inputs."""
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _estimate_decimal(
    value: Any,
    *,
    reason: str,
    extra_details: dict[str, Any] | None = None,
) -> Decimal:
    """Parse one persisted estimate numeric input as a finite Decimal."""
    if isinstance(value, bool):
        raise _build_estimate_job_input_error(reason, extra_details=extra_details)
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise _build_estimate_job_input_error(reason, extra_details=extra_details) from exc
    if not decimal_value.is_finite():
        raise _build_estimate_job_input_error(reason, extra_details=extra_details)
    return decimal_value


def _estimate_tax_rate(assumptions_json: dict[str, Any]) -> Decimal:
    """Load the persisted estimate tax rate with a default of zero."""
    raw_tax_rate = assumptions_json.get("tax_rate", 0)
    tax_rate = _estimate_decimal(
        raw_tax_rate,
        reason="invalid_tax_rate",
        extra_details={"field": "tax_rate"},
    )
    if tax_rate < 0:
        raise _build_estimate_job_input_error(
            "invalid_tax_rate",
            extra_details={"field": "tax_rate"},
        )
    return tax_rate


async def _build_estimate_engine_input(
    job_id: UUID,
    *,
    attempt_token: UUID,
    deps: WorkerDeps,
) -> EstimateEngineInput:
    """Load deterministic engine inputs for a claimed persisted estimate job."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await session.get(Job, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")
        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            raise _StaleJobAttemptError(f"Job attempt for '{job_id}' no longer owns the lease")
        if job.job_type != JobType.ESTIMATE.value:
            raise ValueError(f"Unsupported estimate job type '{job.job_type}'")
        if job.base_revision_id is None:
            raise _RevisionConflictError(
                message="Estimate job is missing its finalized base revision.",
                details={
                    "base_revision_id": None,
                    "current_revision_id": None,
                },
            )

        estimate_input = await session.get(EstimateJobInput, job.id)
        if estimate_input is None:
            raise _build_estimate_job_input_error("missing_estimate_job_input")
        if (
            estimate_input.project_id != job.project_id
            or estimate_input.source_file_id != job.file_id
            or estimate_input.drawing_revision_id != job.base_revision_id
        ):
            raise _build_estimate_job_input_error(
                "estimate_input_lineage_mismatch",
                extra_details={
                    "drawing_revision_id": str(estimate_input.drawing_revision_id),
                    "base_revision_id": str(job.base_revision_id),
                },
            )

        quantity_takeoff = await session.get(QuantityTakeoff, estimate_input.quantity_takeoff_id)
        if quantity_takeoff is None:
            raise _build_estimate_job_input_error(
                "missing_quantity_takeoff",
                extra_details={"quantity_takeoff_id": str(estimate_input.quantity_takeoff_id)},
            )
        if (
            quantity_takeoff.project_id != job.project_id
            or quantity_takeoff.source_file_id != job.file_id
            or quantity_takeoff.drawing_revision_id != job.base_revision_id
            or quantity_takeoff.id != estimate_input.quantity_takeoff_id
            or quantity_takeoff.quantity_gate != estimate_input.quantity_gate
            or quantity_takeoff.trusted_totals is not estimate_input.trusted_totals
            or quantity_takeoff.source_job_type != JobType.QUANTITY_TAKEOFF.value
            or estimate_input.source_job_type != JobType.ESTIMATE.value
        ):
            raise _build_estimate_job_input_error(
                "quantity_takeoff_lineage_mismatch",
                extra_details={
                    "quantity_takeoff_id": str(quantity_takeoff.id),
                    "drawing_revision_id": str(quantity_takeoff.drawing_revision_id),
                },
            )

        catalog_refs_result = await session.execute(
            select(EstimateJobInputCatalogRef)
            .where(EstimateJobInputCatalogRef.estimate_job_id == job.id)
            .order_by(
                EstimateJobInputCatalogRef.ref_order.asc(),
                EstimateJobInputCatalogRef.ref_type.asc(),
                EstimateJobInputCatalogRef.selection_key.asc(),
            )
        )
        catalog_refs = list(catalog_refs_result.scalars().all())
        assembly = _build_estimate_worker_mapping_v1(catalog_refs)

        quantity_items_result = await session.execute(
            select(QuantityItem)
            .where(QuantityItem.quantity_takeoff_id == quantity_takeoff.id)
            .order_by(QuantityItem.created_at.asc(), QuantityItem.id.asc())
        )
        quantity_items = list(quantity_items_result.scalars().all())
        quantity_items_by_id = {item.id: item for item in quantity_items}

        next_snapshot_sort_order = 1

        def _next_snapshot_sort_order() -> int:
            nonlocal next_snapshot_sort_order
            sort_order = next_snapshot_sort_order
            next_snapshot_sort_order += 1
            return sort_order

        quantity_entries: list[EstimateQuantityEntryInput] = []
        for quantity_dependency in assembly.quantity_entries:
            quantity_item = quantity_items_by_id.get(quantity_dependency.quantity_item_id)
            if quantity_item is None:
                raise _build_estimate_job_input_error(
                    "missing_quantity_item",
                    extra_details={
                        "quantity_item_id": str(quantity_dependency.quantity_item_id),
                        "quantity_entry_key": quantity_dependency.entry_key,
                    },
                )
            if quantity_item.item_kind in {
                QuantityItemKind.EXCLUSION.value,
                QuantityItemKind.CONFLICT.value,
            }:
                raise _build_estimate_job_input_error(
                    "invalid_quantity_item_kind",
                    extra_details={
                        "quantity_item_id": str(quantity_item.id),
                        "item_kind": quantity_item.item_kind,
                    },
                )
            if quantity_item.quantity_gate != "allowed":
                raise _build_estimate_job_input_error(
                    "quantity_item_gate_not_allowed",
                    extra_details={
                        "quantity_item_id": str(quantity_item.id),
                        "quantity_gate": quantity_item.quantity_gate,
                    },
                )
            if quantity_item.value is None or not math.isfinite(quantity_item.value):
                raise _build_estimate_job_input_error(
                    "invalid_quantity_value",
                    extra_details={"quantity_item_id": str(quantity_item.id)},
                )

            quantity_payload = {
                "quantity_takeoff_id": str(quantity_takeoff.id),
                "quantity_item_id": str(quantity_item.id),
                "item_kind": quantity_item.item_kind,
                "quantity_type": quantity_item.quantity_type,
                "value": quantity_item.value,
                "unit": quantity_item.unit,
                "review_state": quantity_item.review_state,
                "validation_status": quantity_item.validation_status,
                "quantity_gate": quantity_item.quantity_gate,
                "source_entity_id": quantity_item.source_entity_id,
                "excluded_source_entity_ids_json": deepcopy(
                    quantity_item.excluded_source_entity_ids_json
                ),
            }
            quantity_entries.append(
                EstimateQuantityEntryInput(
                    entry_key=quantity_dependency.entry_key,
                    entry_label=quantity_item.quantity_type,
                    sort_order=_next_snapshot_sort_order(),
                    source_quantity_item_id=quantity_item.id,
                    source_checksum_sha256=_estimate_input_checksum(quantity_payload),
                    quantity_value=Decimal(str(quantity_item.value)),
                    unit=quantity_item.unit,
                    source_quantity_takeoff_id=quantity_takeoff.id,
                    source_payload=quantity_payload,
                )
            )

        lines_by_key = {line.line_key: line for line in assembly.lines}
        quantity_entry_keys = {entry.entry_key for entry in quantity_entries}
        rate_entry_keys = {
            line.catalog_entry_key
            for line in assembly.lines
            if line.rate_catalog_entry_id is not None
        }
        material_entry_keys = {
            line.catalog_entry_key
            for line in assembly.lines
            if line.material_catalog_entry_id is not None
        }
        rate_entries: list[EstimateRateEntryInput] = []
        material_entries: list[EstimateMaterialEntryInput] = []
        formula_entries: list[EstimateFormulaEntryInput] = []
        line_inputs: list[EstimateLineInputSpec] = []
        emitted_catalog_entry_sources: dict[str, tuple[str, UUID, str]] = {}

        def _register_catalog_entry_source(
            *,
            line: _EstimateWorkerLineInput,
            source_id: UUID,
            source_checksum_sha256: str,
        ) -> bool:
            candidate = (line.ref_type, source_id, source_checksum_sha256)
            existing = emitted_catalog_entry_sources.get(line.catalog_entry_key)
            if existing is None:
                emitted_catalog_entry_sources[line.catalog_entry_key] = candidate
                return True
            if existing != candidate:
                existing_ref_type, existing_source_id, existing_checksum = existing
                raise _build_estimate_job_input_error(
                    "colliding_catalog_entry_key",
                    ref_type=line.ref_type,
                    selection_key=line.selection_key,
                    line_key=line.line_key,
                    extra_details={
                        "catalog_entry_key": line.catalog_entry_key,
                        "existing_ref_type": existing_ref_type,
                        "existing_source_id": str(existing_source_id),
                        "existing_checksum_sha256": existing_checksum,
                    },
                )
            return False

        for line in assembly.lines:
            if line.ref_type == "rate":
                assert line.rate_catalog_entry_id is not None
                try:
                    matched_rate = await deps.resolve_rate(
                        session,
                        ref=CatalogRateRef(
                            id=line.rate_catalog_entry_id,
                            checksum_sha256=line.catalog_checksum_sha256,
                        ),
                    )
                except CatalogSelectionError as exc:
                    raise _build_estimate_job_input_error(
                        "catalog_ref_unresolved",
                        ref_type=line.ref_type,
                        selection_key=line.selection_key,
                        line_key=line.line_key,
                        extra_details={
                            "conflict_count": len(exc.conflicting_candidate_ids),
                        },
                    ) from exc

                if _register_catalog_entry_source(
                    line=line,
                    source_id=matched_rate.id,
                    source_checksum_sha256=matched_rate.checksum_sha256,
                ):
                    rate_entries.append(
                        EstimateRateEntryInput(
                            entry_key=line.catalog_entry_key,
                            entry_label=line.description,
                            sort_order=_next_snapshot_sort_order(),
                            source_rate_id=matched_rate.id,
                            source_checksum_sha256=matched_rate.checksum_sha256,
                            unit=matched_rate.unit,
                            effective_date=matched_rate.effective_start,
                            unit_amount=matched_rate.value,
                            source_payload={
                                "selection_key": line.selection_key,
                                "rate_key": matched_rate.rate_key,
                                "item_type": matched_rate.item_type,
                                "metadata": deepcopy(matched_rate.metadata or {}),
                            },
                            currency=cast(Any, matched_rate.currency),
                        )
                    )

                line_inputs.append(
                    EstimateLineInputSpec(
                        line_key=line.line_key,
                        line_type=cast(Any, line.line_type),
                        description=line.description,
                        quantity_entry_key=line.quantity_entry_key,
                        rate_entry_key=line.catalog_entry_key,
                    )
                )
                continue

            if line.ref_type == "material":
                assert line.material_catalog_entry_id is not None
                try:
                    matched_material = await deps.resolve_material(
                        session,
                        ref=CatalogMaterialRef(
                            id=line.material_catalog_entry_id,
                            checksum_sha256=line.catalog_checksum_sha256,
                        ),
                    )
                except CatalogSelectionError as exc:
                    raise _build_estimate_job_input_error(
                        "catalog_ref_unresolved",
                        ref_type=line.ref_type,
                        selection_key=line.selection_key,
                        line_key=line.line_key,
                        extra_details={
                            "conflict_count": len(exc.conflicting_candidate_ids),
                        },
                    ) from exc

                if _register_catalog_entry_source(
                    line=line,
                    source_id=matched_material.id,
                    source_checksum_sha256=matched_material.checksum_sha256,
                ):
                    material_entries.append(
                        EstimateMaterialEntryInput(
                            entry_key=line.catalog_entry_key,
                            entry_label=line.description,
                            sort_order=_next_snapshot_sort_order(),
                            source_material_id=matched_material.id,
                            source_checksum_sha256=matched_material.checksum_sha256,
                            unit=matched_material.unit,
                            effective_date=matched_material.effective_start,
                            unit_amount=matched_material.value,
                            source_payload={
                                "selection_key": line.selection_key,
                                "material_key": matched_material.material_key,
                                "metadata": deepcopy(matched_material.metadata or {}),
                            },
                            currency=cast(Any, matched_material.currency),
                        )
                    )

                line_inputs.append(
                    EstimateLineInputSpec(
                        line_key=line.line_key,
                        line_type=cast(Any, line.line_type),
                        description=line.description,
                        quantity_entry_key=line.quantity_entry_key,
                        material_entry_key=line.catalog_entry_key,
                    )
                )
                continue

            assert line.formula_definition_id is not None
            try:
                selected_formula = await deps.resolve_formula(
                    session,
                    ref=CatalogFormulaRef(
                        id=line.formula_definition_id,
                        checksum_sha256=line.catalog_checksum_sha256,
                    ),
                )
            except CatalogSelectionError as exc:
                raise _build_estimate_job_input_error(
                    "catalog_ref_unresolved",
                    ref_type=line.ref_type,
                    selection_key=line.selection_key,
                    line_key=line.line_key,
                    extra_details={
                        "conflict_count": len(exc.conflicting_candidate_ids),
                    },
                ) from exc

            try:
                formula_definition = formula_definition_from_selected_formula(selected_formula)
            except Exception as exc:
                raise _build_estimate_job_input_error(
                    "invalid_formula_definition",
                    ref_type=line.ref_type,
                    selection_key=line.selection_key,
                    line_key=line.line_key,
                ) from exc
            if _register_catalog_entry_source(
                line=line,
                source_id=selected_formula.definition_id,
                source_checksum_sha256=selected_formula.checksum_sha256,
            ):
                formula_entries.append(
                    EstimateFormulaEntryInput(
                        entry_key=line.catalog_entry_key,
                        entry_label=line.description,
                        sort_order=_next_snapshot_sort_order(),
                        source_formula_id=selected_formula.definition_id,
                        source_checksum_sha256=selected_formula.checksum_sha256,
                        definition=formula_definition,
                        source_payload={
                            "selection_key": line.selection_key,
                            "formula_id": selected_formula.formula_id,
                            "formula_version": selected_formula.version,
                        },
                    )
                )

            raw_formula_inputs = line.formula_inputs or {}
            declared_input_names = tuple(
                declared_input.name for declared_input in formula_definition.declared_inputs
            )
            binding_tokens = _estimate_formula_binding_tokens(
                raw_formula_inputs,
                line=line,
                declared_input_names=declared_input_names,
            )
            for declared_input_name in declared_input_names:
                if declared_input_name not in binding_tokens:
                    raise _build_estimate_job_input_error(
                        "missing_formula_input_binding",
                        ref_type=line.ref_type,
                        selection_key=line.selection_key,
                        line_key=line.line_key,
                        extra_details={"input": declared_input_name},
                    )
            resolved_formula_inputs = {
                declared_input.name: _resolve_formula_binding_snapshot_key(
                    binding_tokens[declared_input.name],
                    line=line,
                    contract_kind=declared_input.contract.kind,
                    lines_by_key=lines_by_key,
                    quantity_entry_keys=quantity_entry_keys,
                    rate_entry_keys=rate_entry_keys,
                    material_entry_keys=material_entry_keys,
                )
                for declared_input in formula_definition.declared_inputs
            }
            line_inputs.append(
                EstimateLineInputSpec(
                    line_key=line.line_key,
                    line_type=cast(Any, line.line_type),
                    description=line.description,
                    formula_entry_key=line.catalog_entry_key,
                    formula_inputs=resolved_formula_inputs,
                )
            )

        return EstimateEngineInput(
            estimate_job_id=job.id,
            project_id=job.project_id,
            file_id=job.file_id,
            source_job_id=job.id,
            drawing_revision_id=estimate_input.drawing_revision_id,
            quantity_takeoff_id=quantity_takeoff.id,
            currency=cast(Any, estimate_input.currency),
            quantity_gate=cast(Any, estimate_input.quantity_gate),
            trusted_totals=estimate_input.trusted_totals,
            tax_rate=_estimate_tax_rate(estimate_input.assumptions_json),
            quantity_entries=tuple(quantity_entries),
            rate_entries=tuple(rate_entries),
            material_entries=tuple(material_entries),
            formula_entries=tuple(formula_entries),
            line_inputs=tuple(line_inputs),
        )


async def _bulk_insert_model_rows(
    session: AsyncSession,
    model: Any,
    rows: list[dict[str, Any]],
) -> None:
    """Insert prepared mappings in deterministic chunks."""
    if not rows:
        return

    for start in range(0, len(rows), _NORMALIZED_ENTITY_INSERT_CHUNK_SIZE):
        chunk = rows[start : start + _NORMALIZED_ENTITY_INSERT_CHUNK_SIZE]
        await session.execute(insert(model), chunk)


async def _persist_revision_materialization(
    session: AsyncSession,
    *,
    job: Job,
    source_file: File,
    payload: IngestFinalizationPayload,
    drawing_revision_id: UUID,
    adapter_run_output_id: UUID,
) -> UUID:
    """Persist revision-scoped normalized entity rows and manifest atomically."""
    if job.extraction_profile_id is None:
        raise RuntimeError("Persisted ingest outputs require an extraction profile id.")

    materialization_rows = _build_revision_materialization_rows(payload)
    manifest_id = uuid.uuid4()
    base_row = {
        "project_id": job.project_id,
        "source_file_id": source_file.id,
        "extraction_profile_id": job.extraction_profile_id,
        "source_job_id": job.id,
        "drawing_revision_id": drawing_revision_id,
        "adapter_run_output_id": adapter_run_output_id,
        "canonical_entity_schema_version": payload.canonical_entity_schema_version,
    }

    session.add(
        RevisionEntityManifest(
            id=manifest_id,
            project_id=job.project_id,
            source_file_id=source_file.id,
            extraction_profile_id=job.extraction_profile_id,
            source_job_id=job.id,
            drawing_revision_id=drawing_revision_id,
            adapter_run_output_id=adapter_run_output_id,
            canonical_entity_schema_version=payload.canonical_entity_schema_version,
            counts_json=materialization_rows.counts_json,
        )
    )

    await _bulk_insert_model_rows(
        session,
        RevisionLayout,
        [
            {
                **base_row,
                **row,
            }
            for row in materialization_rows.layouts
        ],
    )
    await _bulk_insert_model_rows(
        session,
        RevisionLayer,
        [
            {
                **base_row,
                **row,
            }
            for row in materialization_rows.layers
        ],
    )
    await _bulk_insert_model_rows(
        session,
        RevisionBlock,
        [
            {
                **base_row,
                **row,
            }
            for row in materialization_rows.blocks
        ],
    )
    await _bulk_insert_model_rows(
        session,
        RevisionEntity,
        _order_revision_entity_insert_rows(
            [
                {
                    **base_row,
                    **row,
                }
                for row in materialization_rows.entities
            ]
        ),
    )

    return manifest_id


async def _persist_changeset_revision_materialization(
    session: AsyncSession,
    *,
    job: Job,
    source_file: File,
    drawing_revision_id: UUID,
    apply_result: ChangeSetApplySuccess,
    base_manifest: RevisionEntityManifest,
    base_layouts: Sequence[RevisionLayout],
    base_layers: Sequence[RevisionLayer],
    base_blocks: Sequence[RevisionBlock],
) -> UUID:
    """Persist changeset-origin revision materialization with null ingest lineage fields."""
    materialization_rows = _build_changeset_revision_materialization_rows(
        apply_result,
        base_manifest=base_manifest,
        base_layouts=base_layouts,
        base_layers=base_layers,
        base_blocks=base_blocks,
    )
    manifest_id = uuid.uuid4()
    base_row = {
        "project_id": job.project_id,
        "source_file_id": source_file.id,
        "extraction_profile_id": None,
        "source_job_id": job.id,
        "drawing_revision_id": drawing_revision_id,
        "adapter_run_output_id": None,
        "canonical_entity_schema_version": base_manifest.canonical_entity_schema_version,
    }

    session.add(
        RevisionEntityManifest(
            id=manifest_id,
            project_id=job.project_id,
            source_file_id=source_file.id,
            extraction_profile_id=None,
            source_job_id=job.id,
            drawing_revision_id=drawing_revision_id,
            adapter_run_output_id=None,
            canonical_entity_schema_version=base_manifest.canonical_entity_schema_version,
            counts_json=materialization_rows.counts_json,
        )
    )

    await _bulk_insert_model_rows(
        session,
        RevisionLayout,
        [{**base_row, **row} for row in materialization_rows.layouts],
    )
    await _bulk_insert_model_rows(
        session,
        RevisionLayer,
        [{**base_row, **row} for row in materialization_rows.layers],
    )
    await _bulk_insert_model_rows(
        session,
        RevisionBlock,
        [{**base_row, **row} for row in materialization_rows.blocks],
    )
    await _bulk_insert_model_rows(
        session,
        RevisionEntity,
        _order_revision_entity_insert_rows(
            [{**base_row, **row} for row in materialization_rows.entities]
        ),
    )

    return manifest_id


async def _cleanup_failed_storage_writes(
    storage: Any,
    writes: list[tuple[str, str]],
    *,
    job_id: UUID,
) -> None:
    """Best-effort cleanup for pre-commit immutable storage writes."""
    for key, storage_uri in reversed(writes):
        try:
            await storage.delete_failed_put(key, storage_uri=storage_uri)
        except Exception:
            logger.warning(
                "generated_artifact_cleanup_failed",
                job_id=str(job_id),
                storage_key=key,
                storage_uri=storage_uri,
                exc_info=True,
            )


def _get_enqueue_job_error_message(job_type: str) -> str:
    """Return the persisted enqueue failure message for a worker job type."""
    return job_runner.ENQUEUE_ERROR_MESSAGES_BY_JOB_TYPE.get(job_type, "Failed to enqueue job")


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


async def _finalize_ingest_job(
    job_id: UUID,
    *,
    attempt_token: UUID,
    deps: WorkerDeps,
    payload: IngestFinalizationPayload,
) -> bool:
    """Atomically publish durable ingest outputs and terminal job success."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        bootstrap = await deps.get_job_lock_bootstrap(session, job_id)
        if bootstrap is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        project = await deps.get_project(session, bootstrap.project_id, for_update=True)
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

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "ingest_job_completion_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            logger.info(
                "ingest_job_completion_skipped_stale_attempt",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if job.cancel_requested:
            _finalize_job_cancelled(job)
            await deps.emit_job_event(
                job.id,
                level="warning",
                message="Job cancelled",
                data_json={"status": "cancelled"},
                session=session,
            )
            await session.commit()
            logger.info("ingest_job_cancelled", job_id=str(job_id))
            return False

        if job.status != "running":
            logger.info(
                "ingest_job_completion_skipped_non_running_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        existing_output = await _get_existing_adapter_run_output(session, source_job_id=job.id)
        if existing_output is not None:
            logger.info(
                "ingest_job_completion_skipped_existing_output",
                job_id=str(job_id),
                adapter_run_output_id=str(existing_output.id),
            )
            return False

        if job.extraction_profile_id is None:
            raise ValueError("Ingest job missing extraction profile during finalization")

        if project.deleted_at is not None:
            await _cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
                attempt_token=attempt_token,
            )
            logger.info("ingest_job_cancelled_inactive_source", job_id=str(job_id))
            return False

        source_file = await _get_source_file(
            session,
            project_id=job.project_id,
            file_id=job.file_id,
            for_update=True,
        )
        if source_file is None or source_file.deleted_at is not None:
            await _cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
                attempt_token=attempt_token,
            )
            logger.info("ingest_job_cancelled_inactive_source", job_id=str(job_id))
            return False

        predecessor_revision = await _resolve_finalization_predecessor_revision(
            session,
            job=job,
            source_file=source_file,
            payload_revision_kind=payload.revision_kind,
        )
        revision_sequence = 1
        predecessor_revision_id: UUID | None = None
        if predecessor_revision is not None:
            revision_sequence = predecessor_revision.revision_sequence + 1
            predecessor_revision_id = predecessor_revision.id

        predecessor_debug_overlay = None
        if predecessor_revision_id is not None:
            predecessor_debug_overlay = await _get_generated_artifact_for_revision(
                session,
                project_id=job.project_id,
                drawing_revision_id=predecessor_revision_id,
                artifact_kind=_DEBUG_OVERLAY_ARTIFACT_KIND,
            )

        adapter_run_output_id = uuid.uuid4()
        drawing_revision_id = uuid.uuid4()
        validation_report_id = uuid.uuid4()
        revision_entity_manifest_id = uuid.uuid4()
        debug_overlay_artifact_id = uuid.uuid4()
        finished_at = _utcnow()
        overlay_source_label = source_file.original_filename
        overlay_title = f"{overlay_source_label} revision {revision_sequence}"
        overlay_plan = plan_svg_debug_overlay(
            payload.canonical_json,
            title=overlay_title,
            source_label=overlay_source_label,
            review_state=payload.review_state,
            confidence_score=payload.confidence_score,
        )
        overlay_storage_key = build_generated_artifact_storage_key(
            debug_overlay_artifact_id,
            overlay_plan.filename,
        )
        overlay_generator_config = _build_debug_overlay_generator_config(
            title=overlay_title,
            source_label=overlay_source_label,
            review_state=payload.review_state,
            confidence_score=payload.confidence_score,
        )
        overlay_lineage_json = _build_debug_overlay_lineage_json(
            source_file=source_file,
            job=job,
            payload=payload,
            drawing_revision_id=drawing_revision_id,
            revision_sequence=revision_sequence,
            predecessor_revision_id=predecessor_revision_id,
            adapter_run_output_id=adapter_run_output_id,
        )
        storage = deps.get_storage()
        written_storage_objects: list[tuple[str, str]] = []
        commit_started = False

        try:
            stored_overlay = await storage.put(
                overlay_storage_key,
                overlay_plan.payload,
                immutable=True,
            )
            written_storage_objects.append((stored_overlay.key, stored_overlay.storage_uri))

            session.add(
                AdapterRunOutput(
                    id=adapter_run_output_id,
                    project_id=job.project_id,
                    source_file_id=source_file.id,
                    extraction_profile_id=job.extraction_profile_id,
                    source_job_id=job.id,
                    adapter_key=payload.adapter_key,
                    adapter_version=payload.adapter_version,
                    input_family=payload.input_family,
                    canonical_entity_schema_version=payload.canonical_entity_schema_version,
                    canonical_json=payload.canonical_json,
                    provenance_json=payload.provenance_json,
                    confidence_json=payload.confidence_json,
                    confidence_score=payload.confidence_score,
                    warnings_json=payload.warnings_json,
                    diagnostics_json=payload.diagnostics_json,
                    result_checksum_sha256=payload.result_checksum_sha256,
                )
            )
            session.add(
                DrawingRevision(
                    id=drawing_revision_id,
                    project_id=job.project_id,
                    source_file_id=source_file.id,
                    extraction_profile_id=job.extraction_profile_id,
                    source_job_id=job.id,
                    adapter_run_output_id=adapter_run_output_id,
                    predecessor_revision_id=predecessor_revision_id,
                    revision_sequence=revision_sequence,
                    revision_kind=payload.revision_kind,
                    review_state=payload.review_state,
                    canonical_entity_schema_version=payload.canonical_entity_schema_version,
                    confidence_score=payload.confidence_score,
                )
            )
            session.add(
                ValidationReport(
                    id=validation_report_id,
                    project_id=job.project_id,
                    drawing_revision_id=drawing_revision_id,
                    source_job_id=job.id,
                    validation_report_schema_version=payload.validation_report_schema_version,
                    canonical_entity_schema_version=payload.canonical_entity_schema_version,
                    validation_status=payload.validation_status,
                    review_state=payload.review_state,
                    quantity_gate=payload.quantity_gate,
                    effective_confidence=payload.effective_confidence,
                    validator_name=payload.validator_name,
                    validator_version=payload.validator_version,
                    report_json=_build_persisted_validation_report_json(
                        payload,
                        drawing_revision_id=drawing_revision_id,
                        source_job_id=job.id,
                        validation_report_id=validation_report_id,
                    ),
                    generated_at=payload.generated_at,
                )
            )
            session.add(
                GeneratedArtifact(
                    id=debug_overlay_artifact_id,
                    project_id=job.project_id,
                    source_file_id=source_file.id,
                    job_id=job.id,
                    drawing_revision_id=drawing_revision_id,
                    adapter_run_output_id=adapter_run_output_id,
                    artifact_kind=_DEBUG_OVERLAY_ARTIFACT_KIND,
                    name=overlay_plan.filename,
                    format=_DEBUG_OVERLAY_ARTIFACT_FORMAT,
                    media_type=overlay_plan.media_type,
                    size_bytes=stored_overlay.size_bytes,
                    checksum_sha256=stored_overlay.checksum_sha256,
                    generator_name=_DEBUG_OVERLAY_GENERATOR_NAME,
                    generator_version=_DEBUG_OVERLAY_GENERATOR_VERSION,
                    generator_config_json=overlay_generator_config,
                    storage_key=stored_overlay.key,
                    storage_uri=stored_overlay.storage_uri,
                    lineage_json=overlay_lineage_json,
                    predecessor_artifact_id=(
                        predecessor_debug_overlay.id
                        if predecessor_debug_overlay is not None
                        else None
                    ),
                )
            )
            await session.flush()
            revision_entity_manifest_id = await _persist_revision_materialization(
                session,
                job=job,
                source_file=source_file,
                payload=payload,
                drawing_revision_id=drawing_revision_id,
                adapter_run_output_id=adapter_run_output_id,
            )

            job.status = "succeeded"
            job.finished_at = finished_at
            job.error_code = None
            job.error_message = None
            _clear_job_attempt_lease(job)
            await deps.emit_job_event(
                job.id,
                level="info",
                message="Job succeeded",
                data_json={
                    "status": "succeeded",
                    "attempts": job.attempts,
                    "adapter_run_output_id": str(adapter_run_output_id),
                    "drawing_revision_id": str(drawing_revision_id),
                    "validation_report_id": str(validation_report_id),
                    "revision_entity_manifest_id": str(revision_entity_manifest_id),
                    "generated_artifact_id": str(debug_overlay_artifact_id),
                },
                session=session,
            )
            commit_started = True
            await session.commit()
        except asyncio.CancelledError:
            if not commit_started:
                await session.rollback()
                await _cleanup_failed_storage_writes(
                    storage,
                    written_storage_objects,
                    job_id=job_id,
                )
            raise
        except Exception:
            if not commit_started:
                await session.rollback()
                await _cleanup_failed_storage_writes(
                    storage,
                    written_storage_objects,
                    job_id=job_id,
                )
            raise

    return True


def _manifest_entity_count(manifest: RevisionEntityManifest) -> int | None:
    """Return the expected entity count when recorded on the manifest."""
    raw_count = (
        manifest.counts_json.get("entities") if isinstance(manifest.counts_json, dict) else None
    )
    return raw_count if isinstance(raw_count, int) else None


def _build_quantity_gate_metadata(report: ValidationReport) -> RevisionGateMetadata:
    """Build quantity engine gate metadata from the persisted validation report."""
    return RevisionGateMetadata(
        status=cast(GateStatus, report.quantity_gate),
        validation_status=report.validation_status,
        reason=report.review_state if report.quantity_gate in {"review_gated", "blocked"} else None,
        details={
            "drawing_revision_id": str(report.drawing_revision_id),
            "review_state": report.review_state,
            "effective_confidence": report.effective_confidence,
        },
    )


def _build_revision_entity_input(entity: RevisionEntity) -> RevisionEntityInput:
    """Map a materialized revision entity row to the quantity engine contract."""
    return RevisionEntityInput(
        entity_id=entity.entity_id,
        entity_type=entity.entity_type,
        sequence_index=entity.sequence_index,
        geometry_json=entity.geometry_json,
        properties_json=entity.properties_json,
        provenance_json=entity.provenance_json,
        canonical_entity_json=(
            entity.canonical_entity_json if entity.canonical_entity_json is not None else {}
        ),
        source_identity=entity.source_identity,
        source_hash=entity.source_hash,
    )


def _nonempty_quantity_type(value: str | None) -> str:
    """Normalize persisted quantity type labels to non-empty strings."""
    if value is None:
        return "unknown"
    normalized = value.strip()
    return normalized or "unknown"


def _nonempty_quantity_unit(value: str | None) -> str:
    """Normalize persisted quantity units to non-empty strings."""
    if value is None:
        return "unknown"
    normalized = value.strip()
    return normalized or "unknown"


def _duplicate_entity_ids_json(values: tuple[str, ...]) -> list[str]:
    """Copy duplicate contributor lineage ids into a JSON-safe list."""
    return [value for value in values if value]


def _serialize_quantity_context(context: Any) -> str | None:
    """Render quantity context as a deterministic persisted label suffix."""
    if context is None:
        return None
    if isinstance(context, str):
        normalized = context.strip()
        return normalized or None

    try:
        serialized = json.dumps(context, sort_keys=True, separators=(",", ":"))
    except TypeError:
        serialized = str(context).strip()

    return serialized or None


def _quantity_item_type_label(quantity_type: str | None, context: Any) -> str:
    """Persist the quantity type with stable quantity-context disambiguation."""
    normalized_quantity_type = _nonempty_quantity_type(quantity_type)
    serialized_context = _serialize_quantity_context(context)
    if serialized_context is None:
        return normalized_quantity_type

    return f"{normalized_quantity_type}:{serialized_context}"


def _build_quantity_items(
    *,
    quantity_takeoff_id: UUID,
    project_id: UUID,
    drawing_revision_id: UUID,
    review_state: str,
    validation_status: str,
    quantity_gate: str,
    result: QuantityEngineResult,
) -> list[QuantityItem]:
    """Build immutable quantity item rows for a takeoff result."""
    items: list[QuantityItem] = []

    for contributor in result.contributors:
        items.append(
            QuantityItem(
                id=uuid.uuid4(),
                quantity_takeoff_id=quantity_takeoff_id,
                project_id=project_id,
                drawing_revision_id=drawing_revision_id,
                item_kind=QuantityItemKind.CONTRIBUTOR.value,
                quantity_type=_quantity_item_type_label(
                    contributor.quantity_type,
                    getattr(contributor, "context", None),
                ),
                value=contributor.value,
                unit=_nonempty_quantity_unit(contributor.unit),
                review_state=review_state,
                validation_status=validation_status,
                quantity_gate=quantity_gate,
                source_entity_id=contributor.entity_id,
                excluded_source_entity_ids_json=_duplicate_entity_ids_json(
                    contributor.duplicate_entity_ids
                ),
            )
        )

    for aggregate in result.aggregates:
        items.append(
            QuantityItem(
                id=uuid.uuid4(),
                quantity_takeoff_id=quantity_takeoff_id,
                project_id=project_id,
                drawing_revision_id=drawing_revision_id,
                item_kind=QuantityItemKind.AGGREGATE.value,
                quantity_type=_quantity_item_type_label(
                    aggregate.quantity_type,
                    getattr(aggregate, "context", None),
                ),
                value=aggregate.total,
                unit=_nonempty_quantity_unit(aggregate.unit),
                review_state=review_state,
                validation_status=validation_status,
                quantity_gate=quantity_gate,
                source_entity_id=None,
                excluded_source_entity_ids_json=[],
            )
        )

    for exclusion in result.exclusions:
        items.append(
            QuantityItem(
                id=uuid.uuid4(),
                quantity_takeoff_id=quantity_takeoff_id,
                project_id=project_id,
                drawing_revision_id=drawing_revision_id,
                item_kind=QuantityItemKind.EXCLUSION.value,
                quantity_type=_quantity_item_type_label(
                    exclusion.quantity_type,
                    getattr(exclusion, "context", None),
                ),
                value=None,
                unit="unknown",
                review_state=review_state,
                validation_status=validation_status,
                quantity_gate=quantity_gate,
                source_entity_id=exclusion.entity_id,
                excluded_source_entity_ids_json=[],
            )
        )

    return items


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


def _build_export_job_input_error(
    message: str,
    *,
    error_code: ErrorCode = ErrorCode.INPUT_INVALID,
    details: dict[str, Any] | None = None,
) -> _ExportJobInputError:
    """Build a deterministic export job input failure."""
    return _ExportJobInputError(error_code=error_code, message=message, details=details)


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
        job = await session.get(Job, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")
        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            raise _StaleJobAttemptError(f"Job attempt for '{job_id}' no longer owns the lease")
        if job.job_type != JobType.EXPORT.value:
            raise ValueError(f"Unsupported export job type '{job.job_type}'")
        if job.base_revision_id is None:
            raise _RevisionConflictError(
                message="Export job is missing its finalized base revision.",
                details={
                    "base_revision_id": None,
                    "current_revision_id": None,
                },
            )

        export_input = await session.get(ExportJobInput, job.id)
        if export_input is None:
            raise _build_export_job_input_error(
                "Export job input is missing.",
                error_code=ErrorCode.NOT_FOUND,
                details={"job_id": str(job.id)},
            )
        if (
            export_input.project_id != job.project_id
            or export_input.source_file_id != job.file_id
            or export_input.drawing_revision_id != job.base_revision_id
            or export_input.source_job_id != job.id
            or export_input.source_job_type != JobType.EXPORT.value
        ):
            raise _build_export_job_input_error(
                "Export job input lineage does not match the persisted job.",
                details={
                    "drawing_revision_id": str(export_input.drawing_revision_id),
                    "base_revision_id": str(job.base_revision_id),
                    "source_job_type": export_input.source_job_type,
                },
            )

        drawing_revision = await _get_drawing_revision(session, revision_id=job.base_revision_id)
        if drawing_revision is None:
            raise _RevisionConflictError(
                message="Export job base revision no longer exists.",
                details={
                    "base_revision_id": str(job.base_revision_id),
                    "current_revision_id": None,
                },
            )
        if (
            drawing_revision.project_id != job.project_id
            or drawing_revision.source_file_id != job.file_id
        ):
            raise ValueError("Export job base revision does not belong to the source file")

        export_spec = _get_export_kind_spec(export_input.export_kind)
        if (
            export_input.export_format != export_spec.format
            or export_input.media_type != export_spec.media_type
        ):
            raise _build_export_job_input_error(
                "Export job metadata does not match the supported export kind.",
                details={
                    "export_kind": export_input.export_kind,
                    "export_format": export_input.export_format,
                    "media_type": export_input.media_type,
                },
            )

        options_json = deepcopy(export_input.options_json)
        if export_spec.lineage_anchor in {
            _EXPORT_LINEAGE_ANCHOR_REVISION,
            _EXPORT_LINEAGE_ANCHOR_CHANGESET,
        }:
            if (
                export_input.quantity_takeoff_id is not None
                or export_input.estimate_version_id is not None
                or export_input.quantity_gate is not None
                or export_input.trusted_totals is not None
            ):
                raise _build_export_job_input_error(
                    (
                        "Revision-scoped export input contains unexpected quantity or estimate "
                        "lineage."
                    ),
                    details={"export_kind": export_input.export_kind},
                )
            if export_spec.lineage_anchor == _EXPORT_LINEAGE_ANCHOR_CHANGESET:
                if (
                    drawing_revision.revision_kind != "changeset"
                    or drawing_revision.changeset_id is None
                ):
                    raise _build_export_job_input_error(
                        "Revised DXF export requires a changeset-origin drawing revision.",
                        details={"drawing_revision_id": str(drawing_revision.id)},
                    )
                return _ExportExecutionInput(
                    drawing_revision_id=drawing_revision.id,
                    changeset_id=drawing_revision.changeset_id,
                    export_kind=export_input.export_kind,
                    export_format=export_input.export_format,
                    media_type=export_input.media_type,
                    artifact_name=_build_export_artifact_name(
                        export_kind=export_input.export_kind,
                        export_format=export_input.export_format,
                        drawing_revision_id=drawing_revision.id,
                        changeset_id=drawing_revision.changeset_id,
                    ),
                    options_json=options_json,
                )
            return _ExportExecutionInput(
                drawing_revision_id=drawing_revision.id,
                export_kind=export_input.export_kind,
                export_format=export_input.export_format,
                media_type=export_input.media_type,
                artifact_name=_build_export_artifact_name(
                    export_kind=export_input.export_kind,
                    export_format=export_input.export_format,
                    drawing_revision_id=drawing_revision.id,
                ),
                options_json=options_json,
            )

        quantity_takeoff_id = export_input.quantity_takeoff_id
        if quantity_takeoff_id is None:
            raise _build_export_job_input_error(
                "Export job input is missing its quantity takeoff linkage.",
                details={"export_kind": export_input.export_kind},
            )
        if export_input.quantity_gate != "allowed" or export_input.trusted_totals is not True:
            raise _build_export_job_input_error(
                "Export job input requires a trusted quantity takeoff with allowed gate.",
                details={
                    "quantity_takeoff_id": str(quantity_takeoff_id),
                    "quantity_gate": export_input.quantity_gate,
                    "trusted_totals": export_input.trusted_totals,
                },
            )

        quantity_takeoff = await session.get(QuantityTakeoff, quantity_takeoff_id)
        if quantity_takeoff is None:
            raise _build_export_job_input_error(
                "Export job quantity takeoff was not found.",
                error_code=ErrorCode.NOT_FOUND,
                details={"quantity_takeoff_id": str(quantity_takeoff_id)},
            )
        if (
            quantity_takeoff.project_id != job.project_id
            or quantity_takeoff.source_file_id != job.file_id
            or quantity_takeoff.drawing_revision_id != drawing_revision.id
            or quantity_takeoff.quantity_gate != export_input.quantity_gate
            or quantity_takeoff.trusted_totals is not export_input.trusted_totals
            or quantity_takeoff.source_job_type != JobType.QUANTITY_TAKEOFF.value
        ):
            raise _build_export_job_input_error(
                "Export job quantity takeoff lineage does not match the persisted job input.",
                details={
                    "quantity_takeoff_id": str(quantity_takeoff.id),
                    "drawing_revision_id": str(quantity_takeoff.drawing_revision_id),
                    "quantity_gate": quantity_takeoff.quantity_gate,
                    "trusted_totals": quantity_takeoff.trusted_totals,
                },
            )

        if export_spec.lineage_anchor == _EXPORT_LINEAGE_ANCHOR_QUANTITY_TAKEOFF:
            if export_input.estimate_version_id is not None:
                raise _build_export_job_input_error(
                    "Quantity CSV export input contains unexpected estimate linkage.",
                    details={
                        "quantity_takeoff_id": str(quantity_takeoff.id),
                        "estimate_version_id": str(export_input.estimate_version_id),
                    },
                )
            return _ExportExecutionInput(
                drawing_revision_id=drawing_revision.id,
                export_kind=export_input.export_kind,
                export_format=export_input.export_format,
                media_type=export_input.media_type,
                artifact_name=_build_export_artifact_name(
                    export_kind=export_input.export_kind,
                    export_format=export_input.export_format,
                    drawing_revision_id=drawing_revision.id,
                    quantity_takeoff_id=quantity_takeoff.id,
                ),
                options_json=options_json,
                quantity_takeoff_id=quantity_takeoff.id,
            )

        estimate_version_id = export_input.estimate_version_id
        if estimate_version_id is None:
            raise _build_export_job_input_error(
                "Estimate export input is missing its estimate version linkage.",
                details={"quantity_takeoff_id": str(quantity_takeoff.id)},
            )

        estimate_version = await session.get(EstimateVersion, estimate_version_id)
        if estimate_version is None:
            raise _build_export_job_input_error(
                "Export job estimate version was not found.",
                error_code=ErrorCode.NOT_FOUND,
                details={"estimate_version_id": str(estimate_version_id)},
            )
        if (
            estimate_version.project_id != job.project_id
            or estimate_version.source_file_id != job.file_id
            or estimate_version.drawing_revision_id != drawing_revision.id
            or estimate_version.quantity_takeoff_id != quantity_takeoff.id
            or estimate_version.quantity_gate != export_input.quantity_gate
            or estimate_version.trusted_totals is not export_input.trusted_totals
        ):
            raise _build_export_job_input_error(
                "Export job estimate lineage does not match the persisted job input.",
                details={
                    "estimate_version_id": str(estimate_version.id),
                    "drawing_revision_id": str(estimate_version.drawing_revision_id),
                    "quantity_takeoff_id": str(estimate_version.quantity_takeoff_id),
                    "quantity_gate": estimate_version.quantity_gate,
                    "trusted_totals": estimate_version.trusted_totals,
                },
            )

        return _ExportExecutionInput(
            drawing_revision_id=drawing_revision.id,
            export_kind=export_input.export_kind,
            export_format=export_input.export_format,
            media_type=export_input.media_type,
            artifact_name=_build_export_artifact_name(
                export_kind=export_input.export_kind,
                export_format=export_input.export_format,
                drawing_revision_id=drawing_revision.id,
                quantity_takeoff_id=quantity_takeoff.id,
                estimate_version_id=estimate_version.id,
            ),
            options_json=options_json,
            quantity_takeoff_id=quantity_takeoff.id,
            estimate_version_id=estimate_version.id,
        )


async def _finalize_export_job(
    job_id: UUID,
    *,
    attempt_token: UUID,
    deps: WorkerDeps,
    execution: _ExportExecutionInput,
    rendered: ExportArtifact,
) -> bool:
    """Atomically publish one export artifact and terminal job success."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        locked_source = await _lock_job_source_for_terminal_mutation(session, job_id)
        job = locked_source.job
        source_file = locked_source.source_file

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "export_job_completion_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            logger.info(
                "export_job_completion_skipped_stale_attempt",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if job.cancel_requested:
            _finalize_job_cancelled(job)
            await deps.emit_job_event(
                job.id,
                level="warning",
                message="Job cancelled",
                data_json={"status": "cancelled"},
                session=session,
            )
            await session.commit()
            logger.info("export_job_cancelled", job_id=str(job_id))
            return False

        if job.status != "running":
            logger.info(
                "export_job_completion_skipped_non_running_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if (
            locked_source.project.deleted_at is not None
            or source_file is None
            or source_file.deleted_at is not None
        ):
            await _cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
                attempt_token=attempt_token,
            )
            logger.info("export_job_cancelled_inactive_source", job_id=str(job_id))
            return False

        if job.base_revision_id != execution.drawing_revision_id:
            raise _RevisionConflictError(
                message="Export base revision changed before finalization.",
                details={
                    "base_revision_id": (
                        str(job.base_revision_id) if job.base_revision_id is not None else None
                    ),
                    "drawing_revision_id": str(execution.drawing_revision_id),
                },
            )

        existing_artifact = await _get_existing_generated_artifact(session, source_job_id=job.id)
        if existing_artifact is not None:
            logger.info(
                "export_job_completion_skipped_existing_artifact",
                job_id=str(job_id),
                generated_artifact_id=str(existing_artifact.id),
            )
            return False

        generated_artifact_id = uuid.uuid4()
        storage_key = build_generated_artifact_storage_key(
            generated_artifact_id,
            execution.artifact_name,
        )
        lineage_json = _build_export_artifact_lineage_json(
            source_file=source_file,
            job=job,
            execution=execution,
        )
        storage = deps.get_storage()
        written_storage_objects: list[tuple[str, str]] = []
        commit_started = False

        try:
            stored_object = await storage.put(storage_key, rendered.content_bytes, immutable=True)
            written_storage_objects.append((stored_object.key, stored_object.storage_uri))

            if stored_object.checksum_sha256 != rendered.checksum_sha256:
                raise ValueError(
                    "Stored export artifact checksum does not match the rendered bytes"
                )
            if stored_object.size_bytes != rendered.size_bytes:
                raise ValueError("Stored export artifact size does not match the rendered bytes")

            session.add(
                GeneratedArtifact(
                    id=generated_artifact_id,
                    project_id=job.project_id,
                    source_file_id=source_file.id,
                    job_id=job.id,
                    drawing_revision_id=execution.drawing_revision_id,
                    changeset_id=execution.changeset_id,
                    quantity_takeoff_id=execution.quantity_takeoff_id,
                    estimate_version_id=execution.estimate_version_id,
                    artifact_kind=execution.export_kind,
                    name=execution.artifact_name,
                    format=execution.export_format,
                    media_type=rendered.media_type,
                    size_bytes=stored_object.size_bytes,
                    checksum_sha256=stored_object.checksum_sha256,
                    generator_name=rendered.generator_name,
                    generator_version=rendered.generator_version,
                    generator_config_json=deepcopy(execution.options_json),
                    storage_key=stored_object.key,
                    storage_uri=stored_object.storage_uri,
                    lineage_json=lineage_json,
                )
            )

            job.status = "succeeded"
            job.finished_at = _utcnow()
            job.error_code = None
            job.error_message = None
            _clear_job_attempt_lease(job)
            await deps.emit_job_event(
                job.id,
                level="info",
                message="Job succeeded",
                data_json={
                    "status": "succeeded",
                    "attempts": job.attempts,
                    "generated_artifact_id": str(generated_artifact_id),
                    "export_kind": execution.export_kind,
                },
                session=session,
            )
            await session.flush()
            commit_started = True
            await session.commit()
        except asyncio.CancelledError:
            if not commit_started:
                await session.rollback()
                await _cleanup_failed_storage_writes(
                    storage,
                    written_storage_objects,
                    job_id=job_id,
                )
            raise
        except Exception:
            if not commit_started:
                await session.rollback()
                await _cleanup_failed_storage_writes(
                    storage,
                    written_storage_objects,
                    job_id=job_id,
                )
            raise

    return True


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
        job = await session.get(Job, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")
        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            raise _StaleJobAttemptError(f"Job attempt for '{job_id}' no longer owns the lease")
        if job.job_type != JobType.QUANTITY_TAKEOFF.value:
            raise ValueError(f"Unsupported quantity takeoff job type '{job.job_type}'")
        if job.base_revision_id is None:
            raise _RevisionConflictError(
                message="Quantity takeoff job is missing its finalized base revision.",
                details={
                    "base_revision_id": None,
                    "current_revision_id": None,
                },
            )

        drawing_revision = await _get_drawing_revision(session, revision_id=job.base_revision_id)
        if drawing_revision is None:
            raise _RevisionConflictError(
                message="Quantity takeoff base revision no longer exists.",
                details={
                    "base_revision_id": str(job.base_revision_id),
                    "current_revision_id": None,
                },
            )
        if (
            drawing_revision.project_id != job.project_id
            or drawing_revision.source_file_id != job.file_id
        ):
            raise ValueError("Quantity takeoff base revision does not belong to the source file")

        report = await _get_validation_report_for_revision(
            session,
            project_id=job.project_id,
            drawing_revision_id=drawing_revision.id,
        )
        if report is None:
            raise _QuantityTakeoffJobError(
                error_code=ErrorCode.NOT_FOUND,
                message=_QUANTITY_TAKEOFF_VALIDATION_REPORT_MISSING_ERROR_MESSAGE,
                details={"drawing_revision_id": str(drawing_revision.id)},
            )

        if report.quantity_gate in {"review_gated", "blocked"}:
            return _QuantityTakeoffExecutionInput(
                drawing_revision_id=drawing_revision.id,
                review_state=report.review_state,
                validation_status=report.validation_status,
                quantity_gate=report.quantity_gate,
                gate=_build_quantity_gate_metadata(report),
                entities=[],
            )

        manifest = await _get_revision_entity_manifest_for_revision(
            session,
            project_id=job.project_id,
            source_file_id=job.file_id,
            drawing_revision_id=drawing_revision.id,
        )
        if manifest is None:
            raise _QuantityTakeoffJobError(
                error_code=ErrorCode.NORMALIZED_ENTITIES_NOT_MATERIALIZED,
                message=_QUANTITY_TAKEOFF_MATERIALIZATION_MISSING_ERROR_MESSAGE,
                details={"drawing_revision_id": str(drawing_revision.id)},
            )

        entities = await _get_revision_entities_for_revision(
            session,
            project_id=job.project_id,
            source_file_id=job.file_id,
            drawing_revision_id=drawing_revision.id,
        )
        expected_entity_count = _manifest_entity_count(manifest)
        if expected_entity_count is not None and expected_entity_count != len(entities):
            raise _QuantityTakeoffJobError(
                error_code=ErrorCode.NORMALIZED_ENTITIES_NOT_MATERIALIZED,
                message=_QUANTITY_TAKEOFF_MATERIALIZATION_MISSING_ERROR_MESSAGE,
                details={
                    "drawing_revision_id": str(drawing_revision.id),
                    "expected_entities": expected_entity_count,
                    "loaded_entities": len(entities),
                },
            )

        return _QuantityTakeoffExecutionInput(
            drawing_revision_id=drawing_revision.id,
            review_state=report.review_state,
            validation_status=report.validation_status,
            quantity_gate=report.quantity_gate,
            gate=_build_quantity_gate_metadata(report),
            entities=[_build_revision_entity_input(entity) for entity in entities],
        )


async def _finalize_quantity_takeoff_job(
    job_id: UUID,
    *,
    attempt_token: UUID,
    deps: WorkerDeps,
    execution: _QuantityTakeoffExecutionInput,
    result: QuantityEngineResult,
) -> bool:
    """Atomically publish quantity takeoff rows and terminal job success."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    if result.conflicts:
        raise ValueError("Conflicting quantity results cannot be finalized")

    async with session_maker() as session:
        locked_source = await _lock_job_source_for_terminal_mutation(session, job_id)
        job = locked_source.job

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "quantity_takeoff_job_completion_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            logger.info(
                "quantity_takeoff_job_completion_skipped_stale_attempt",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if job.cancel_requested:
            _finalize_job_cancelled(job)
            await deps.emit_job_event(
                job.id,
                level="warning",
                message="Job cancelled",
                data_json={"status": "cancelled"},
                session=session,
            )
            await session.commit()
            logger.info("quantity_takeoff_job_cancelled", job_id=str(job_id))
            return False

        if job.status != "running":
            logger.info(
                "quantity_takeoff_job_completion_skipped_non_running_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if (
            locked_source.project.deleted_at is not None
            or locked_source.source_file is None
            or locked_source.source_file.deleted_at is not None
        ):
            await _cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
                attempt_token=attempt_token,
            )
            logger.info("quantity_takeoff_job_cancelled_inactive_source", job_id=str(job_id))
            return False

        if job.base_revision_id != execution.drawing_revision_id:
            raise _RevisionConflictError(
                message="Quantity takeoff base revision changed before finalization.",
                details={
                    "base_revision_id": (
                        str(job.base_revision_id) if job.base_revision_id is not None else None
                    ),
                    "drawing_revision_id": str(execution.drawing_revision_id),
                },
            )

        existing_takeoff = await _get_existing_quantity_takeoff(session, source_job_id=job.id)
        if existing_takeoff is not None:
            logger.info(
                "quantity_takeoff_job_completion_skipped_existing_takeoff",
                job_id=str(job_id),
                quantity_takeoff_id=str(existing_takeoff.id),
            )
            return False

        quantity_takeoff_id = uuid.uuid4()
        quantity_takeoff = QuantityTakeoff(
            id=quantity_takeoff_id,
            project_id=job.project_id,
            source_file_id=job.file_id,
            drawing_revision_id=execution.drawing_revision_id,
            source_job_id=job.id,
            source_job_type=JobType.QUANTITY_TAKEOFF.value,
            review_state=execution.review_state,
            validation_status=execution.validation_status,
            quantity_gate=execution.quantity_gate,
            trusted_totals=result.trusted_totals,
        )
        quantity_items = _build_quantity_items(
            quantity_takeoff_id=quantity_takeoff_id,
            project_id=job.project_id,
            drawing_revision_id=execution.drawing_revision_id,
            review_state=execution.review_state,
            validation_status=execution.validation_status,
            quantity_gate=execution.quantity_gate,
            result=result,
        )

        session.add(quantity_takeoff)
        await session.flush()
        session.add_all(quantity_items)

        job.status = "succeeded"
        job.finished_at = _utcnow()
        job.error_code = None
        job.error_message = None
        _clear_job_attempt_lease(job)
        await deps.emit_job_event(
            job.id,
            level="info",
            message="Job succeeded",
            data_json={
                "status": "succeeded",
                "attempts": job.attempts,
                "quantity_takeoff_id": str(quantity_takeoff_id),
                "quantity_item_count": len(quantity_items),
                "trusted_totals": result.trusted_totals,
                "quantity_gate": execution.quantity_gate,
            },
            session=session,
        )
        await session.commit()

    return True


async def _finalize_estimate_job(
    job_id: UUID,
    *,
    attempt_token: UUID,
    deps: WorkerDeps,
    output: EstimateEngineOutput,
) -> bool:
    """Atomically publish estimate rows and terminal job success."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    estimate_version_kwargs = output.estimate_version_model_kwargs()
    snapshot_entry_kwargs = list(output.snapshot_entry_model_kwargs())
    line_item_kwargs = list(output.line_item_model_kwargs())

    async with session_maker() as session:
        locked_source = await _lock_job_source_for_terminal_mutation(session, job_id)
        job = locked_source.job

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "estimate_job_completion_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            logger.info(
                "estimate_job_completion_skipped_stale_attempt",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if job.cancel_requested:
            _finalize_job_cancelled(job)
            await deps.emit_job_event(
                job.id,
                level="warning",
                message="Job cancelled",
                data_json={"status": "cancelled"},
                session=session,
            )
            await session.commit()
            logger.info("estimate_job_cancelled", job_id=str(job_id))
            return False

        if job.status != "running":
            logger.info(
                "estimate_job_completion_skipped_non_running_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if (
            locked_source.project.deleted_at is not None
            or locked_source.source_file is None
            or locked_source.source_file.deleted_at is not None
        ):
            await _cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
                attempt_token=attempt_token,
            )
            logger.info("estimate_job_cancelled_inactive_source", job_id=str(job_id))
            return False

        output_revision_id = cast(UUID | None, estimate_version_kwargs.get("drawing_revision_id"))
        if job.base_revision_id != output_revision_id:
            raise _RevisionConflictError(
                message="Estimate base revision changed before finalization.",
                details={
                    "base_revision_id": (
                        str(job.base_revision_id) if job.base_revision_id is not None else None
                    ),
                    "drawing_revision_id": str(output_revision_id) if output_revision_id else None,
                },
            )

        existing_version = await _get_existing_estimate_version(session, source_job_id=job.id)
        if existing_version is not None:
            logger.info(
                "estimate_job_completion_skipped_existing_version",
                job_id=str(job_id),
                estimate_version_id=str(existing_version.id),
            )
            return False

        estimate_version = EstimateVersion(**estimate_version_kwargs)
        snapshot_entries = [EstimateSnapshotEntry(**kwargs) for kwargs in snapshot_entry_kwargs]
        line_items = [EstimateItem(**kwargs) for kwargs in line_item_kwargs]

        session.add(estimate_version)
        await session.flush()
        session.add_all(snapshot_entries)
        await session.flush()
        session.add_all(line_items)

        job.status = "succeeded"
        job.finished_at = _utcnow()
        job.error_code = None
        job.error_message = None
        _clear_job_attempt_lease(job)
        await deps.emit_job_event(
            job.id,
            level="info",
            message="Job succeeded",
            data_json={
                "status": "succeeded",
                "attempts": job.attempts,
                "estimate_version_id": str(estimate_version.id),
                "snapshot_entry_count": len(snapshot_entries),
                "line_item_count": len(line_items),
            },
            session=session,
        )
        await session.commit()

    return True


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
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        locked_source = await _lock_job_source_for_terminal_mutation(session, job_id)
        job = locked_source.job

        if (
            locked_source.project.deleted_at is not None
            or locked_source.source_file is None
            or locked_source.source_file.deleted_at is not None
        ):
            await _cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
            )
            logger.info(
                "job_recovery_enqueue_failure_mark_skipped_inactive_source",
                job_id=str(job_id),
                job_type=job.job_type,
                status=job.status,
            )
            return False

        if not _job_is_safe_recovery_failure_target(job):
            logger.info(
                "job_recovery_enqueue_failure_mark_skipped_changed_state",
                job_id=str(job_id),
                job_type=job.job_type,
                status=job.status,
            )
            return False

        await _persist_job_failed(
            session,
            job,
            error_message=error_message,
            error_code=error_code,
            error_details=error_details,
        )
        await session.commit()

    return True


async def _claim_job_enqueue_intent(job_id: UUID) -> _ClaimedJobEnqueueIntent | None:
    """Claim a durable enqueue intent for best-effort or recovery publication."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    now = _utcnow()
    async with session_maker() as session:
        job = await _get_job_for_update(session, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if not is_recoverable_enqueue_job_type(job.job_type) or job.status != "pending":
            return None

        if job.enqueue_status == _ENQUEUE_STATUS_PUBLISHED:
            return None

        if job.enqueue_status == _ENQUEUE_STATUS_PUBLISHING and not _is_stale_enqueue_intent(
            job,
            now=now,
        ):
            return None

        lease = _claim_enqueue_intent_lease(job, now=now)
        attempts = job.attempts
        await session.commit()
        return _ClaimedJobEnqueueIntent(lease=lease, job_type=job.job_type, attempts=attempts)


async def _release_job_enqueue_intent(job_id: UUID, *, lease_token: UUID) -> bool:
    """Release a claimed durable enqueue intent after a publish failure."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await _get_job_for_update(session, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if (
            job.enqueue_status != _ENQUEUE_STATUS_PUBLISHING
            or job.enqueue_owner_token != lease_token
        ):
            return False

        if job.status == "pending":
            job.enqueue_status = _ENQUEUE_STATUS_PENDING
            _clear_enqueue_intent_lease(job)
            await session.commit()
            return True

        job.enqueue_status = _ENQUEUE_STATUS_PUBLISHED
        job.enqueue_published_at = _utcnow()
        _clear_enqueue_intent_lease(job)
        await session.commit()
        return True


async def _mark_job_enqueue_published(job_id: UUID, *, lease_token: UUID) -> bool:
    """Finalize a claimed durable enqueue intent after broker publication."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await _get_job_for_update(session, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")

        if (
            job.enqueue_status != _ENQUEUE_STATUS_PUBLISHING
            or job.enqueue_owner_token != lease_token
        ):
            return False

        job.enqueue_status = _ENQUEUE_STATUS_PUBLISHED
        job.enqueue_published_at = _utcnow()
        _clear_enqueue_intent_lease(job)
        await session.commit()
        return True


async def publish_job_enqueue_intent(
    job_id: UUID,
    *,
    recovery: bool = False,
    publisher: Callable[[UUID], None] | None = None,
    suppress_exceptions: bool = False,
) -> bool:
    """Best-effort publish for a durable enqueue intent recorded in Postgres."""
    claimed_intent: _ClaimedJobEnqueueIntent | None = None
    try:
        claimed_intent = await _claim_job_enqueue_intent(job_id)
        if claimed_intent is None:
            return False

        publish = publisher or get_job_enqueue_publisher(claimed_intent.job_type)
        if publish is None:
            await _release_job_enqueue_intent(job_id, lease_token=claimed_intent.lease.token)
            return False
        countdown_token = _ENQUEUE_COUNTDOWN_SECONDS.set(
            _enqueue_backoff_seconds(claimed_intent.attempts)
        )
        try:
            publish(job_id)
        except Exception:
            _ENQUEUE_COUNTDOWN_SECONDS.reset(countdown_token)
            await _release_job_enqueue_intent(job_id, lease_token=claimed_intent.lease.token)
            if recovery:
                await _mark_recovery_enqueue_failed(job_id, job_type=claimed_intent.job_type)
            else:
                logger.warning(
                    "job_enqueue_deferred",
                    job_id=str(job_id),
                    job_type=claimed_intent.job_type,
                    recovery_action="worker_start_recovery",
                )
            return False
        else:
            _ENQUEUE_COUNTDOWN_SECONDS.reset(countdown_token)

        await _mark_job_enqueue_published(job_id, lease_token=claimed_intent.lease.token)
        return True
    except Exception:
        if not suppress_exceptions:
            raise

        logger.warning(
            "job_enqueue_deferred",
            job_id=str(job_id),
            job_type=claimed_intent.job_type if claimed_intent is not None else None,
            recovery_action="worker_start_recovery",
        )
        return False


async def _mark_recovery_enqueue_failed(job_id: UUID, *, job_type: str) -> bool:
    """Persist and log a sanitized worker-recovery enqueue failure."""
    marked_failed = await _mark_job_failed_if_recovery_safe(
        job_id,
        error_message=_get_enqueue_job_error_message(job_type),
    )
    if not marked_failed:
        return False

    logger.error(
        "job_recovery_enqueue_failed",
        job_id=str(job_id),
        job_type=job_type,
        error_code=ErrorCode.INTERNAL_ERROR.value,
        recovery_action="mark_failed",
    )
    return True


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
            timeout=AdapterTimeout(seconds=_DEFAULT_ADAPTER_TIMEOUT.total_seconds()),
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
        execution_result = await execute_job(job_id, attempt_token=lease.token, deps=deps)
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
        finalized = await finalize_job(
            job_id,
            attempt_token=lease.token,
            deps=deps,
            **finalize_kwargs,
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
    if execution.quantity_gate in {"review_gated", "blocked"}:
        error_details = _quantity_gate_details(
            drawing_revision_id=execution.drawing_revision_id,
            review_state=execution.review_state,
            validation_status=execution.validation_status,
            quantity_gate=execution.quantity_gate,
        )
        await _mark_job_failed(
            job_id,
            error_message=_QUANTITY_TAKEOFF_GATE_BLOCKED_ERROR_MESSAGE,
            error_code=ErrorCode.INPUT_INVALID,
            attempt_token=attempt_token,
            error_details=error_details,
        )
        logger.warning(
            "quantity_takeoff_job_gate_blocked",
            job_id=str(job_id),
            error_code=ErrorCode.INPUT_INVALID.value,
            **error_details,
        )
        return None

    result = compute_quantities(execution.gate, execution.entities)
    if result.conflicts:
        error_details = {
            "drawing_revision_id": str(execution.drawing_revision_id),
            "quantity_gate": execution.quantity_gate,
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


async def _finalize_changeset_apply_job(
    job_id: UUID,
    *,
    attempt_token: UUID,
    deps: WorkerDeps,
    apply_result: ChangeSetApplySuccess,
) -> bool:
    """Atomically publish a changeset-origin revision and terminal job success."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        locked_source = await _lock_job_source_for_terminal_mutation(session, job_id)
        job = locked_source.job
        source_file = locked_source.source_file

        if job.status in _TERMINAL_JOB_STATUSES:
            logger.info(
                "changeset_apply_job_completion_skipped_terminal_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if not _job_attempt_is_current(job, attempt_token=attempt_token):
            logger.info(
                "changeset_apply_job_completion_skipped_stale_attempt",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if job.cancel_requested:
            _finalize_job_cancelled(job)
            await deps.emit_job_event(
                job.id,
                level="warning",
                message="Job cancelled",
                data_json={"status": "cancelled"},
                session=session,
            )
            await session.commit()
            logger.info("changeset_apply_job_cancelled", job_id=str(job_id))
            return False

        if job.status != "running":
            logger.info(
                "changeset_apply_job_completion_skipped_non_running_status",
                job_id=str(job_id),
                status=job.status,
            )
            return False

        if (
            locked_source.project.deleted_at is not None
            or source_file is None
            or source_file.deleted_at is not None
        ):
            await _cancel_job_for_inactive_source(
                session,
                job,
                reason="source_deleted",
                attempt_token=attempt_token,
            )
            logger.info("changeset_apply_job_cancelled_inactive_source", job_id=str(job_id))
            return False

        if job.base_revision_id != apply_result.base_revision.revision_id:
            raise _RevisionConflictError(
                message="Changeset apply base revision changed before finalization.",
                details={
                    "base_revision_id": (
                        str(job.base_revision_id) if job.base_revision_id is not None else None
                    ),
                    "apply_result_base_revision_id": str(apply_result.base_revision.revision_id),
                },
            )

        existing_revision = await _get_existing_drawing_revision(session, source_job_id=job.id)
        if existing_revision is not None:
            logger.info(
                "changeset_apply_job_completion_skipped_existing_revision",
                job_id=str(job_id),
                drawing_revision_id=str(existing_revision.id),
            )
            return False

        current_revision = await _get_latest_drawing_revision(
            session,
            project_id=job.project_id,
            source_file_id=source_file.id,
        )
        if (
            current_revision is None
            or current_revision.id != apply_result.current_revision.revision_id
        ):
            base_revision = await _get_drawing_revision(
                session,
                revision_id=apply_result.base_revision.revision_id,
            )
            raise _RevisionConflictError(
                message="Changeset apply base revision became stale before finalization.",
                details={
                    **_build_revision_conflict_details(
                        base_revision=base_revision,
                        current_revision=current_revision,
                    ),
                    "change_set_id": str(apply_result.change_set_id),
                },
            )

        validation_report = await _get_validation_report_for_revision(
            session,
            project_id=job.project_id,
            drawing_revision_id=current_revision.id,
        )
        if validation_report is None:
            raise RuntimeError("Changeset apply base revision is missing its validation report.")

        apply_input = await _load_changeset_apply_job_input_if_valid(session, job=job)

        base_manifest = await _get_revision_entity_manifest_for_revision(
            session,
            project_id=job.project_id,
            source_file_id=source_file.id,
            drawing_revision_id=current_revision.id,
        )
        if base_manifest is None:
            raise RuntimeError("Changeset apply base revision is missing normalized entities.")

        base_layouts = await _get_revision_layouts_for_revision(
            session,
            project_id=job.project_id,
            source_file_id=source_file.id,
            drawing_revision_id=current_revision.id,
        )
        base_layers = await _get_revision_layers_for_revision(
            session,
            project_id=job.project_id,
            source_file_id=source_file.id,
            drawing_revision_id=current_revision.id,
        )
        base_blocks = await _get_revision_blocks_for_revision(
            session,
            project_id=job.project_id,
            source_file_id=source_file.id,
            drawing_revision_id=current_revision.id,
        )

        change_set = await session.get(CadChangeSet, apply_result.change_set_id)
        if change_set is None:
            raise RuntimeError("Changeset apply change set no longer exists.")
        if (
            change_set.project_id != job.project_id
            or change_set.base_revision_id != apply_result.base_revision.revision_id
        ):
            raise ValueError("Changeset apply change set lineage does not match the source job")

        drawing_revision_id = uuid.uuid4()
        validation_report_id = uuid.uuid4()
        finished_at = _utcnow()
        drawing_revision = DrawingRevision(
            id=drawing_revision_id,
            project_id=job.project_id,
            source_file_id=source_file.id,
            extraction_profile_id=None,
            source_job_id=job.id,
            adapter_run_output_id=None,
            changeset_id=apply_result.change_set_id,
            predecessor_revision_id=current_revision.id,
            revision_sequence=current_revision.revision_sequence + 1,
            revision_kind="changeset",
            review_state=validation_report.review_state,
            canonical_entity_schema_version=base_manifest.canonical_entity_schema_version,
            confidence_score=validation_report.effective_confidence,
        )

        session.add(drawing_revision)
        await session.flush()

        session.add(
            ValidationReport(
                id=validation_report_id,
                project_id=job.project_id,
                drawing_revision_id=drawing_revision_id,
                source_job_id=job.id,
                validation_report_schema_version=validation_report.validation_report_schema_version,
                canonical_entity_schema_version=validation_report.canonical_entity_schema_version,
                validation_status=validation_report.validation_status,
                review_state=validation_report.review_state,
                quantity_gate=validation_report.quantity_gate,
                effective_confidence=validation_report.effective_confidence,
                validator_name=validation_report.validator_name,
                validator_version=validation_report.validator_version,
                report_json=_build_changeset_validation_report_json(
                    validation_report,
                    change_set_id=apply_result.change_set_id,
                    drawing_revision_id=drawing_revision_id,
                    predecessor_revision_id=current_revision.id,
                    pinned_validation_result_id=(
                        apply_input.latest_validation_result_id if apply_input is not None else None
                    ),
                    pinned_validation_status=(
                        apply_input.latest_validation_status if apply_input is not None else None
                    ),
                    source_job_id=job.id,
                    validation_report_id=validation_report_id,
                    generated_at=finished_at,
                ),
                generated_at=finished_at,
            )
        )
        await _persist_changeset_revision_materialization(
            session,
            job=job,
            source_file=source_file,
            drawing_revision_id=drawing_revision_id,
            apply_result=apply_result,
            base_manifest=base_manifest,
            base_layouts=base_layouts,
            base_layers=base_layers,
            base_blocks=base_blocks,
        )

        change_set.status = "applied"
        job.status = "succeeded"
        job.finished_at = finished_at
        job.error_code = None
        job.error_message = None
        _clear_job_attempt_lease(job)
        await deps.emit_job_event(
            job.id,
            level="info",
            message="Job succeeded",
            data_json={
                "status": "succeeded",
                "attempts": job.attempts,
                "drawing_revision_id": str(drawing_revision_id),
                "validation_report_id": str(validation_report_id),
                "change_set_id": str(apply_result.change_set_id),
            },
            session=session,
        )
        await session.commit()

    return True


async def process_ingest_job(job_id: UUID, *, deps: WorkerDeps | None = None) -> None:
    """Load a persisted ingest job, run ingestion, and persist state transitions."""
    await _process_registered_job(
        job_id, spec=_INGEST_PROCESS_SPEC, deps=deps or default_worker_deps()
    )


async def recover_incomplete_jobs() -> list[UUID]:
    """Requeue incomplete persisted worker jobs on worker startup."""
    session_maker = get_session_maker()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    now = _utcnow()

    async with session_maker() as session:
        result = await session.execute(
            select(Job)
            .where(
                (Job.job_type.in_(_RECOVERABLE_ENQUEUE_JOB_TYPES))
                & (
                    (Job.status == "running")
                    | (
                        (Job.status == "pending")
                        & (
                            Job.enqueue_status.in_(
                                (_ENQUEUE_STATUS_PENDING, _ENQUEUE_STATUS_PUBLISHING)
                            )
                        )
                    )
                )
            )
            .order_by(Job.created_at.asc(), Job.id.asc())
            .with_for_update(skip_locked=True)
        )
        jobs = result.scalars().all()

        recovered_job_ids: list[UUID] = []
        for job in jobs:
            if job.status == "running":
                if not _is_stale_running_job(job, now=now):
                    continue
                job.status = "pending"
                job.started_at = None
                job.finished_at = None
                job.error_code = None
                job.error_message = None
                _clear_job_attempt_lease(job)
                prepare_job_enqueue_intent(job)
                recovered_job_ids.append(job.id)
                continue

            if job.enqueue_status == _ENQUEUE_STATUS_PUBLISHING and not _is_stale_enqueue_intent(
                job,
                now=now,
            ):
                continue

            if job.enqueue_status == _ENQUEUE_STATUS_PUBLISHING:
                job.enqueue_status = _ENQUEUE_STATUS_PENDING
                _clear_enqueue_intent_lease(job)

            if job.enqueue_status != _ENQUEUE_STATUS_PENDING:
                continue

            recovered_job_ids.append(job.id)

        await session.commit()

    enqueued_job_ids: list[UUID] = []
    for job_id in recovered_job_ids:
        if await publish_job_enqueue_intent(job_id, recovery=True):
            enqueued_job_ids.append(job_id)

    return enqueued_job_ids


async def recover_incomplete_ingest_jobs() -> list[UUID]:
    """Compatibility alias for incomplete worker job recovery."""
    return await recover_incomplete_jobs()


def recover_incomplete_ingest_jobs_on_worker_start(**_: object) -> None:
    """Requeue incomplete persisted worker jobs when a worker starts."""
    try:
        recovered_job_ids = _run_worker_loop(recover_incomplete_ingest_jobs)
    except Exception as exc:
        logger.error("ingest_job_recovery_failed", error=str(exc), exc_info=True)
        return

    if recovered_job_ids:
        logger.info(
            "ingest_job_recovery_completed",
            recovered_job_ids=[str(job_id) for job_id in recovered_job_ids],
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
