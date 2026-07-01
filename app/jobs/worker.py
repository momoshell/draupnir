"""Celery worker application and persisted job handlers."""

# ruff: noqa: SLF001

import asyncio
from collections.abc import Callable, Coroutine
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any, cast
from uuid import UUID

from celery import Celery
from celery.signals import worker_ready
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.cad.changeset import (
    ChangeSetApplyError,
)
from app.core.config import settings
from app.core.errors import ErrorCode
from app.core.logging import get_logger
from app.db.session import get_session_maker
from app.estimating.catalog.resolver import resolve_formula, resolve_material, resolve_rate
from app.estimating.engine.errors import EstimateEngineError
from app.estimating.engine.service import compose_estimate
from app.estimating.quantities.engine import compute_quantities
from app.ingestion.contracts import AdapterTimeout, InputFamily
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunnerError, IngestionRunRequest, run_ingestion
from app.jobs import attempt_leases as job_attempt_leases
from app.jobs import changeset_apply_execution as job_changeset_apply_execution
from app.jobs import enqueueing as job_enqueueing
from app.jobs import execution_adapters as job_execution_adapters
from app.jobs import execution_monitoring as job_execution_monitoring
from app.jobs import export_artifacts as job_export_artifacts
from app.jobs import ingest_execution as job_ingest_execution
from app.jobs import lifecycle as job_lifecycle
from app.jobs import recovery as job_recovery
from app.jobs import registered_processor as job_registered_processor
from app.jobs import runner as job_runner
from app.jobs import terminal_states as job_terminal_states
from app.jobs import worker_loop as job_worker_loop
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
    _build_export_job_input_error as _build_export_job_input_error,
)
from app.jobs.export_execution_input import (
    _ExportJobInputError as _ExportJobInputError,
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
from app.models.extraction_profile import ExtractionProfile
from app.models.job import Job, JobType
from app.storage import get_storage

logger = get_logger(__name__)

_RECOVERABLE_INGEST_JOB_TYPES = job_runner.INGEST_WORKER_JOB_TYPES
_RECOVERABLE_ENQUEUE_JOB_TYPES = job_runner.RECOVERABLE_ENQUEUE_JOB_TYPES
_KNOWN_ENQUEUE_JOB_TYPES_WITHOUT_PUBLISHER = job_runner.JOB_TYPES_WITHOUT_ENQUEUE_PUBLISHER
_TERMINAL_JOB_STATUSES = job_lifecycle._TERMINAL_JOB_STATUSES
_ENQUEUE_STATUS_PENDING = job_enqueueing.ENQUEUE_STATUS_PENDING
_ENQUEUE_STATUS_PUBLISHING = job_enqueueing.ENQUEUE_STATUS_PUBLISHING
_ENQUEUE_STATUS_PUBLISHED = job_enqueueing.ENQUEUE_STATUS_PUBLISHED
_DEFAULT_ADAPTER_TIMEOUT = timedelta(minutes=5)
_RUNNING_JOB_STALE_AFTER = _DEFAULT_ADAPTER_TIMEOUT * 2
_JOB_ATTEMPT_LEASE_RENEW_INTERVAL = _RUNNING_JOB_STALE_AFTER / 3
_JOB_CANCELLATION_POLL_INTERVAL_SECONDS = 0.1
_ENQUEUE_BACKOFF_BASE_SECONDS = job_enqueueing.ENQUEUE_BACKOFF_BASE_SECONDS
_ENQUEUE_BACKOFF_MAX_SECONDS = job_enqueueing.ENQUEUE_BACKOFF_MAX_SECONDS
_enqueue_backoff_seconds = job_enqueueing.enqueue_backoff_seconds
_current_enqueue_countdown = job_enqueueing.current_enqueue_countdown


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
_FINALIZE_INGEST_JOB_ERROR_MESSAGE = "Failed to finalize ingest job"
_PROCESS_INGEST_JOB_ERROR_MESSAGE = "Ingest job failed unexpectedly."
_FINALIZE_QUANTITY_TAKEOFF_JOB_ERROR_MESSAGE = "Failed to finalize quantity takeoff job"
_FINALIZE_CENTERLINE_JOB_ERROR_MESSAGE = "Failed to finalize centerline job"
_FINALIZE_ESTIMATE_JOB_ERROR_MESSAGE = "Failed to finalize estimate job"
_FINALIZE_EXPORT_JOB_ERROR_MESSAGE = "Failed to finalize export job"
_FINALIZE_CHANGESET_APPLY_JOB_ERROR_MESSAGE = "Failed to finalize changeset apply job"
_PROCESS_QUANTITY_TAKEOFF_JOB_ERROR_MESSAGE = "Quantity takeoff job failed unexpectedly."
_PROCESS_CENTERLINE_JOB_ERROR_MESSAGE = "Centerline job failed unexpectedly."
_PROCESS_ESTIMATE_JOB_ERROR_MESSAGE = "Estimate job failed unexpectedly."
_PROCESS_EXPORT_JOB_ERROR_MESSAGE = "Export job failed unexpectedly."
_PROCESS_CHANGESET_APPLY_JOB_ERROR_MESSAGE = "Changeset apply job failed unexpectedly."
_QUANTITY_TAKEOFF_CONFLICT_ERROR_MESSAGE = (
    "Quantity takeoff detected conflicting contributor inputs."
)


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


_QueuedJobEvent = job_execution_monitoring.QueuedJobEvent


_ChangeSetApplyJobError = job_changeset_apply_execution.ChangeSetApplyJobError
_RegisteredJobAttemptResult = job_registered_processor.RegisteredJobAttemptResult
_RegisteredJobProcessSpec = job_registered_processor.RegisteredJobProcessSpec


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


_ExportKindSpec = job_export_artifacts.ExportKindSpec
_EXPORT_KIND_SPECS = job_export_artifacts.EXPORT_KIND_SPECS
_get_export_kind_spec = job_export_artifacts.get_export_kind_spec
_build_export_artifact_name = job_export_artifacts.build_export_artifact_name
_render_export_artifact = job_export_artifacts.render_export_artifact
_EXPORT_LINEAGE_ANCHOR_REVISION = job_export_artifacts.EXPORT_LINEAGE_ANCHOR_REVISION
_EXPORT_LINEAGE_ANCHOR_CHANGESET = job_export_artifacts.EXPORT_LINEAGE_ANCHOR_CHANGESET
_EXPORT_LINEAGE_ANCHOR_QUANTITY_TAKEOFF = (
    job_export_artifacts.EXPORT_LINEAGE_ANCHOR_QUANTITY_TAKEOFF
)
_EXPORT_LINEAGE_ANCHOR_ESTIMATE_VERSION = (
    job_export_artifacts.EXPORT_LINEAGE_ANCHOR_ESTIMATE_VERSION
)


_JobAttemptLease = job_lifecycle._JobAttemptLease
_EnqueueIntentLease = job_enqueueing.EnqueueIntentLease
_ClaimedJobEnqueueIntent = job_enqueueing.ClaimedJobEnqueueIntent


_JobLockBootstrap = job_lifecycle._JobLockBootstrap
_LockedJobSource = job_lifecycle._LockedJobSource


_PersistedJobCancellationHandle = job_execution_monitoring.PersistedJobCancellationHandle


class _JobProgressEventBridge(job_execution_monitoring.JobProgressEventBridge):
    """Synchronous progress callback with async DB draining."""

    def __init__(self, job_id: UUID, *, attempt_token: UUID) -> None:
        super().__init__(
            job_id,
            attempt_token=attempt_token,
            emit_job_event=emit_job_event,
        )


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
    return job_worker_loop.get_worker_loop_runner()


def _close_worker_loop_runner() -> None:
    """Close and clear the reusable asyncio runner."""
    job_worker_loop.close_worker_loop_runner()


def _run_worker_loop[WorkerLoopResultT](
    coro_factory: Callable[[], Coroutine[Any, Any, WorkerLoopResultT]],
) -> WorkerLoopResultT:
    """Run a worker coroutine on the process-local reusable event loop."""
    return job_worker_loop.run_worker_loop(coro_factory)


_clear_job_attempt_lease = job_lifecycle._clear_job_attempt_lease


def _clear_enqueue_intent_lease(job: Job) -> None:
    """Clear persisted ownership fencing for a durable enqueue intent."""
    job_enqueueing.clear_enqueue_intent_lease(job)


def prepare_job_enqueue_intent(job: Job) -> None:
    """Reset a job's durable enqueue intent to the pending outbox state."""
    job_enqueueing.prepare_job_enqueue_intent(job)


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


def _job_is_safe_recovery_failure_target(job: Job) -> bool:
    """Return whether recovery can still safely mark the job failed."""
    return job_attempt_leases.job_is_safe_recovery_failure_target(
        job,
        enqueue_pending_status=_ENQUEUE_STATUS_PENDING,
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
    await job_attempt_leases.renew_job_attempt_lease_until_cancelled(
        job_id,
        attempt_token=attempt_token,
        renew_job_attempt_lease=_renew_job_attempt_lease,
        stale_job_attempt_error_type=_StaleJobAttemptError,
        stale_after=stale_after,
        interval=interval,
    )


async def _with_job_attempt_lease_renewal[ResultT](
    work: Coroutine[Any, Any, ResultT],
    *,
    job_id: UUID,
    attempt_token: UUID,
) -> ResultT:
    """Run job work while a companion task renews the persisted attempt lease."""
    return await job_attempt_leases.with_job_attempt_lease_renewal(
        work,
        job_id=job_id,
        attempt_token=attempt_token,
        renew_job_attempt_lease_until_cancelled=_renew_job_attempt_lease_until_cancelled,
    )


def _is_stale_enqueue_intent(job: Job, *, now: datetime) -> bool:
    """Return whether an in-flight enqueue publish claim can be reclaimed."""
    return job_enqueueing.is_stale_enqueue_intent(job, now=now)


def _claim_enqueue_intent_lease(job: Job, *, now: datetime) -> _EnqueueIntentLease:
    """Mint and persist a fresh ownership lease for broker publication."""
    return job_enqueueing.claim_enqueue_intent_lease(job, now=now)


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


def _get_enqueue_job_error_message(job_type: str) -> str:
    """Return the persisted enqueue failure message for a worker job type."""
    return job_runner.ENQUEUE_ERROR_MESSAGES_BY_JOB_TYPE.get(job_type, "Failed to enqueue job")


def _requested_input_family_from_pdf_input_mode(
    pdf_input_mode: str | None,
) -> InputFamily | None:
    """Map persisted PDF input mode to an explicit runner input family override."""
    return job_execution_adapters.requested_input_family_from_pdf_input_mode(pdf_input_mode)


async def _build_ingestion_run_request(job_id: UUID, *, attempt_token: UUID) -> IngestionRunRequest:
    """Load persisted job and file metadata for the ingestion runner."""
    return await job_execution_adapters.build_ingestion_run_request(
        job_id,
        attempt_token=attempt_token,
        session_maker_factory=get_session_maker,
        get_job_lock_bootstrap=_get_job_lock_bootstrap,
        get_project=_get_project,
        get_job_for_update_with_metadata=_get_job_for_update_with_metadata,
        job_attempt_is_current=_job_attempt_is_current,
        assert_job_base_revision_invariants=_assert_job_base_revision_invariants,
        cancel_job_for_inactive_source=_cancel_job_for_inactive_source,
        get_source_file=_get_source_file,
        get_extraction_profile=_get_extraction_profile,
        inactive_source_error_type=_InactiveSourceError,
        stale_job_attempt_error_type=_StaleJobAttemptError,
    )


class _SessionExportRowLoader(job_execution_adapters.SessionExportRowLoader):
    """Session-backed ``ExportRowLoader`` used by the worker export path."""

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, get_drawing_revision=_get_drawing_revision)


async def _build_export_execution_input(
    job_id: UUID,
    *,
    attempt_token: UUID,
) -> _ExportExecutionInput:
    """Load deterministic persisted inputs for a claimed export job."""
    return await job_execution_adapters.build_export_execution_input(
        job_id,
        attempt_token=attempt_token,
        session_maker_factory=get_session_maker,
        row_loader_factory=_SessionExportRowLoader,
        resolve_export_spec=_get_export_kind_spec,
        build_artifact_name=_build_export_artifact_name,
    )


class _SessionQuantityRowLoader(job_execution_adapters.SessionQuantityRowLoader):
    """Session-backed ``QuantityRowLoader`` used by the worker quantity path."""

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(
            session,
            get_drawing_revision=_get_drawing_revision,
            get_validation_report_for_revision=_get_validation_report_for_revision,
            get_revision_entity_manifest_for_revision=(_get_revision_entity_manifest_for_revision),
            get_revision_entities_for_revision=_get_revision_entities_for_revision,
        )


async def _build_quantity_takeoff_execution_input(
    job_id: UUID,
    *,
    attempt_token: UUID,
) -> _QuantityTakeoffExecutionInput:
    """Load unlocked quantity engine inputs for a claimed persisted job."""
    return await job_execution_adapters.build_quantity_takeoff_execution_input(
        job_id,
        attempt_token=attempt_token,
        session_maker_factory=get_session_maker,
        row_loader_factory=_SessionQuantityRowLoader,
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


_progress_event_data = job_execution_monitoring.progress_event_data


def _runner_error_log_fields(exc: IngestionRunnerError) -> dict[str, Any]:
    """Return whitelisted structured fields for expected runner failures."""
    return job_ingest_execution.runner_error_log_fields(exc)


def _runner_supports_keyword(runner: Any, keyword: str) -> bool:
    """Return whether a runner callable accepts a given keyword."""
    return job_ingest_execution.runner_supports_keyword(runner, keyword)


async def _invoke_ingestion_runner(
    request: IngestionRunRequest,
    *,
    timeout: AdapterTimeout,
    cancellation: _PersistedJobCancellationHandle,
    on_progress: Any,
) -> IngestFinalizationPayload:
    """Call the runner while remaining compatible with patched test doubles."""
    return await job_ingest_execution.invoke_ingestion_runner(
        request,
        runner=run_ingestion,
        timeout=timeout,
        cancellation=cancellation,
        on_progress=on_progress,
    )


async def _poll_job_cancellation(
    job_id: UUID,
    *,
    attempt_token: UUID,
    cancellation: _PersistedJobCancellationHandle,
    run_task: asyncio.Task[IngestFinalizationPayload],
    stop_event: asyncio.Event,
) -> None:
    """Poll persisted cancellation without holding DB locks during execution."""
    await job_execution_monitoring.poll_job_cancellation(
        job_id,
        attempt_token=attempt_token,
        cancellation=cancellation,
        run_task=run_task,
        stop_event=stop_event,
        session_maker_factory=get_session_maker,
        job_attempt_is_current=_job_attempt_is_current,
        poll_interval_seconds=_JOB_CANCELLATION_POLL_INTERVAL_SECONDS,
    )


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
    return await job_execution_monitoring.cancel_registered_job_if_requested(
        job_id,
        attempt_token=attempt_token,
        log_event=log_event,
        session_maker_factory=get_session_maker,
        job_attempt_is_current=_job_attempt_is_current,
        mark_job_cancelled=_mark_job_cancelled,
        logger_instance=logger,
    )


async def _stop_job_execution_monitor(
    *,
    progress_bridge: _JobProgressEventBridge,
    stop_event: asyncio.Event,
    cancellation_task: asyncio.Task[None],
) -> None:
    """Flush queued progress and stop background execution monitors."""
    await job_execution_monitoring.stop_job_execution_monitor(
        progress_bridge=progress_bridge,
        stop_event=stop_event,
        cancellation_task=cancellation_task,
    )


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
    await job_terminal_states.mark_job_cancelled_with_log(
        job_id,
        attempt_token=attempt_token,
        log_event=log_event,
        mark_job_cancelled=_mark_job_cancelled,
        logger_instance=logger,
        log_fields=log_fields,
    )


async def _mark_job_failed_for_revision_conflict(
    job_id: UUID,
    *,
    attempt_token: UUID,
    log_event: str,
    exc: _RevisionConflictError,
) -> None:
    """Persist and log the shared revision-conflict failure contract."""
    await job_terminal_states.mark_job_failed_for_revision_conflict(
        job_id,
        attempt_token=attempt_token,
        log_event=log_event,
        exc=exc,
        mark_job_failed=_mark_job_failed,
        logger_instance=logger,
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
    await job_terminal_states.mark_job_failed_with_internal_error_log(
        job_id,
        attempt_token=attempt_token,
        error_message=error_message,
        log_event=log_event,
        mark_job_failed=_mark_job_failed,
        logger_instance=logger,
        log_fields=log_fields,
    )


async def _mark_job_failed_if_recovery_safe(
    job_id: UUID,
    *,
    error_message: str,
    error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
    error_details: dict[str, Any] | None = None,
) -> bool:
    """Fail a recovered job only if it is still pending and unowned."""
    return await job_terminal_states.mark_job_failed_if_recovery_safe(
        job_id,
        error_message=error_message,
        error_code=error_code,
        error_details=error_details,
        session_maker_factory=get_session_maker,
        lock_job_source_for_terminal_mutation=_lock_job_source_for_terminal_mutation,
        cancel_job_for_inactive_source=_cancel_job_for_inactive_source,
        job_is_safe_recovery_failure_target=_job_is_safe_recovery_failure_target,
        persist_job_failed=_persist_job_failed,
        logger_instance=logger,
    )


async def _claim_job_enqueue_intent(job_id: UUID) -> _ClaimedJobEnqueueIntent | None:
    """Claim a durable enqueue intent for best-effort or recovery publication."""
    return await job_enqueueing.claim_job_enqueue_intent(
        job_id,
        session_maker_factory=get_session_maker,
        get_job_for_update=_get_job_for_update,
        is_recoverable_enqueue_job_type=is_recoverable_enqueue_job_type,
        utcnow_func=_utcnow,
    )


async def _release_job_enqueue_intent(job_id: UUID, *, lease_token: UUID) -> bool:
    """Release a claimed durable enqueue intent after a publish failure."""
    return await job_enqueueing.release_job_enqueue_intent(
        job_id,
        lease_token=lease_token,
        session_maker_factory=get_session_maker,
        get_job_for_update=_get_job_for_update,
        utcnow_func=_utcnow,
    )


async def _mark_job_enqueue_published(job_id: UUID, *, lease_token: UUID) -> bool:
    """Finalize a claimed durable enqueue intent after broker publication."""
    return await job_enqueueing.mark_job_enqueue_published(
        job_id,
        lease_token=lease_token,
        session_maker_factory=get_session_maker,
        get_job_for_update=_get_job_for_update,
        utcnow_func=_utcnow,
    )


async def publish_job_enqueue_intent(
    job_id: UUID,
    *,
    recovery: bool = False,
    publisher: Callable[[UUID], None] | None = None,
    suppress_exceptions: bool = False,
) -> bool:
    """Best-effort publish for a durable enqueue intent recorded in Postgres."""
    return await job_enqueueing.publish_job_enqueue_intent(
        job_id,
        claim_job_enqueue_intent=_claim_job_enqueue_intent,
        release_job_enqueue_intent=_release_job_enqueue_intent,
        mark_job_enqueue_published=_mark_job_enqueue_published,
        mark_recovery_enqueue_failed=_mark_recovery_enqueue_failed,
        get_job_enqueue_publisher=get_job_enqueue_publisher,
        logger_instance=logger,
        recovery=recovery,
        publisher=publisher,
        suppress_exceptions=suppress_exceptions,
    )


async def _mark_recovery_enqueue_failed(job_id: UUID, *, job_type: str) -> bool:
    """Persist and log a sanitized worker-recovery enqueue failure."""
    return await job_terminal_states.mark_recovery_enqueue_failed(
        job_id,
        job_type=job_type,
        get_enqueue_job_error_message=_get_enqueue_job_error_message,
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
    await job_ingest_execution.handle_ingest_runner_error(
        job_id,
        attempt_token=attempt_token,
        exc=exc,
        mark_job_cancelled_with_log=_mark_job_cancelled_with_log,
        mark_job_failed=_mark_job_failed,
        logger_instance=logger,
    )


def _build_ingest_finalization_error_log_fields(exc: Exception) -> dict[str, str]:
    """Preserve the legacy ingest finalization error log payload."""
    return job_ingest_execution.build_ingest_finalization_error_log_fields(exc)


def _get_registered_job_handler(job_type_name: job_runner.JobTypeName) -> job_runner.JobHandler:
    """Return registered worker metadata for one persisted job type."""
    return job_registered_processor.get_registered_job_handler(job_type_name)


def _resolve_registered_job_callable(name: str) -> Any:
    """Late-bind a worker callable by name for monkeypatch-friendly wrappers."""
    return job_registered_processor.resolve_registered_job_callable(name, namespace=globals())


async def _process_registered_job(
    job_id: UUID, *, spec: _RegisteredJobProcessSpec, deps: WorkerDeps
) -> None:
    """Run one registered persisted worker job through the shared execution shell."""
    await job_registered_processor.process_registered_job(
        job_id,
        spec=spec,
        deps=deps,
        ensure_worker_database_configured=_ensure_worker_database_configured,
        get_registered_job_handler_func=_get_registered_job_handler,
        begin_or_resume_registered_job=_begin_or_resume_registered_job,
        cancel_registered_job_if_requested=_cancel_registered_job_if_requested,
        resolve_registered_job_callable_func=_resolve_registered_job_callable,
        with_job_attempt_lease_renewal=_with_job_attempt_lease_renewal,
        mark_job_failed_for_revision_conflict=_mark_job_failed_for_revision_conflict,
        mark_job_cancelled_with_log=_mark_job_cancelled_with_log,
        mark_changeset_apply_job_failed_for_revision_conflict=(
            _mark_changeset_apply_job_failed_for_revision_conflict
        ),
        mark_changeset_apply_job_failed_for_input_error=(
            _mark_changeset_apply_job_failed_for_input_error
        ),
        mark_job_failed=_mark_job_failed,
        mark_job_failed_with_internal_error_log=_mark_job_failed_with_internal_error_log,
        inactive_source_error_type=_InactiveSourceError,
        stale_job_attempt_error_type=_StaleJobAttemptError,
        revision_conflict_error_type=_RevisionConflictError,
        changeset_apply_job_error_type=_ChangeSetApplyJobError,
        logger_instance=logger,
    )


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
    return await job_changeset_apply_execution.query_changeset_apply_job_inputs(
        session,
        job_id=job_id,
    )


async def _load_changeset_apply_job_input(
    session: AsyncSession,
    *,
    job: Job,
) -> ChangeSetApplyJobInput:
    """Validate that one immutable apply input exists and matches the persisted job."""
    return await job_changeset_apply_execution.load_changeset_apply_job_input(
        session,
        job=job,
        query_job_inputs=_query_changeset_apply_job_inputs,
    )


async def _load_changeset_apply_job_input_if_valid(
    session: AsyncSession,
    *,
    job: Job,
) -> ChangeSetApplyJobInput | None:
    """Return the immutable apply input when exactly one lineage-matching row exists."""
    return await job_changeset_apply_execution.load_changeset_apply_job_input_if_valid(
        session,
        job=job,
        query_job_inputs=_query_changeset_apply_job_inputs,
    )


def _parse_uuid_value(value: Any) -> UUID | None:
    """Best-effort UUID parsing for persisted error payloads."""
    return job_changeset_apply_execution.parse_uuid_value(value)


async def _resolve_changeset_apply_failure_target(
    session: AsyncSession,
    *,
    job: Job,
    fallback_change_set_id: UUID | None = None,
) -> CadChangeSet | None:
    """Resolve the mutable changeset row for an apply-job terminal failure."""
    return await job_changeset_apply_execution.resolve_changeset_apply_failure_target(
        session,
        job=job,
        fallback_change_set_id=fallback_change_set_id,
        load_input_if_valid=_load_changeset_apply_job_input_if_valid,
    )


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
    return job_changeset_apply_execution.build_changeset_apply_error_details(result)


async def _execute_changeset_apply_job_attempt(
    job_id: UUID,
    *,
    attempt_token: UUID,
    deps: WorkerDeps,
) -> _RegisteredJobAttemptResult | None:
    """Load immutable apply input, re-run apply loading, and defer success finalization."""
    _ = deps  # uniform dispatch contract; no injected collaborators in this stage
    apply_result = await job_changeset_apply_execution.execute_changeset_apply_job_attempt(
        job_id,
        attempt_token=attempt_token,
        session_maker_factory=get_session_maker,
        job_attempt_is_current=_job_attempt_is_current,
        load_apply_job_input=_load_changeset_apply_job_input,
        build_conflict_details=_build_changeset_apply_conflict_details,
        stale_job_attempt_error_type=_StaleJobAttemptError,
        revision_conflict_error_type=_RevisionConflictError,
    )
    return _RegisteredJobAttemptResult(finalize_kwargs={"apply_result": apply_result})


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

        recovered_job_ids = job_recovery.recover_incomplete_job_ids(
            jobs,
            now=now,
            policy=job_recovery.RecoveryPolicy(
                enqueue_status_pending=_ENQUEUE_STATUS_PENDING,
                enqueue_status_publishing=_ENQUEUE_STATUS_PUBLISHING,
                is_stale_running_job=_is_stale_running_job,
                is_stale_enqueue_intent=_is_stale_enqueue_intent,
                clear_job_attempt_lease=_clear_job_attempt_lease,
                clear_enqueue_intent_lease=_clear_enqueue_intent_lease,
                prepare_job_enqueue_intent=prepare_job_enqueue_intent,
            ),
        )

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
