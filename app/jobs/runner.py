"""Worker job handler metadata registry."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final, Literal

type JobTypeName = Literal[
    "ingest",
    "reprocess",
    "quantity_takeoff",
    "estimate",
    "export",
    "changeset_apply",
]


@dataclass(frozen=True, slots=True)
class BeginOrResumeLogKeys:
    unsupported_type: str
    terminal_status: str
    inactive_source: str
    reclaimed_stale_running: str
    duplicate_delivery: str
    max_attempts_exceeded: str
    cancelled: str


@dataclass(frozen=True, slots=True)
class BeginOrResumeRoute:
    process_name: str
    supported_job_types: tuple[JobTypeName, ...]
    log_keys: BeginOrResumeLogKeys


@dataclass(frozen=True, slots=True)
class JobHandler:
    job_type_name: JobTypeName
    execute_name: str | None
    finalize_name: str | None
    process_name: str
    run_task_name: str
    enqueue_publisher_name: str | None
    enqueue_error_message: str


JOB_HANDLERS: Final[tuple[JobHandler, ...]] = (
    JobHandler(
        job_type_name="ingest",
        execute_name="_execute_ingest_job_attempt",
        finalize_name="_finalize_ingest_job",
        process_name="process_ingest_job",
        run_task_name="run_ingest_job",
        enqueue_publisher_name="enqueue_ingest_job",
        enqueue_error_message="Failed to enqueue ingest job",
    ),
    JobHandler(
        job_type_name="reprocess",
        execute_name="_execute_ingest_job_attempt",
        finalize_name="_finalize_ingest_job",
        process_name="process_ingest_job",
        run_task_name="run_ingest_job",
        enqueue_publisher_name="enqueue_ingest_job",
        enqueue_error_message="Failed to enqueue reprocess job",
    ),
    JobHandler(
        job_type_name="quantity_takeoff",
        execute_name="_execute_quantity_takeoff_job_attempt",
        finalize_name="_finalize_quantity_takeoff_job",
        process_name="process_quantity_takeoff_job",
        run_task_name="run_quantity_takeoff_job",
        enqueue_publisher_name="enqueue_quantity_takeoff_job",
        enqueue_error_message="Failed to enqueue quantity takeoff job",
    ),
    JobHandler(
        job_type_name="estimate",
        execute_name="_execute_estimate_job_attempt",
        finalize_name="_finalize_estimate_job",
        process_name="process_estimate_job",
        run_task_name="run_estimate_job",
        enqueue_publisher_name="enqueue_estimate_job",
        enqueue_error_message="Failed to enqueue estimate job",
    ),
    JobHandler(
        job_type_name="export",
        execute_name="_execute_export_job",
        finalize_name="_finalize_export_job",
        process_name="process_export_job",
        run_task_name="run_export_job",
        enqueue_publisher_name="enqueue_export_job",
        enqueue_error_message="Failed to enqueue export job",
    ),
    JobHandler(
        job_type_name="changeset_apply",
        execute_name="_execute_changeset_apply_job_attempt",
        finalize_name="_finalize_changeset_apply_job",
        process_name="process_changeset_apply_job",
        run_task_name="run_changeset_apply_job",
        enqueue_publisher_name="enqueue_changeset_apply_job",
        enqueue_error_message="Failed to enqueue changeset apply job",
    ),
)

_JOB_HANDLERS_BY_TYPE: Final[dict[str, JobHandler]] = {
    handler.job_type_name: handler for handler in JOB_HANDLERS
}

JOB_HANDLERS_BY_TYPE: Final[Mapping[str, JobHandler]] = MappingProxyType(_JOB_HANDLERS_BY_TYPE)


def _job_types_for_process(process_name: str) -> tuple[JobTypeName, ...]:
    return tuple(
        handler.job_type_name for handler in JOB_HANDLERS if handler.process_name == process_name
    )


BEGIN_OR_RESUME_ROUTES: Final[tuple[BeginOrResumeRoute, ...]] = (
    BeginOrResumeRoute(
        process_name="process_ingest_job",
        supported_job_types=_job_types_for_process("process_ingest_job"),
        log_keys=BeginOrResumeLogKeys(
            unsupported_type="ingest_job_unsupported_type_skipped",
            terminal_status="ingest_job_cancel_skipped_terminal_status",
            inactive_source="ingest_job_cancelled_inactive_source",
            reclaimed_stale_running="ingest_job_reclaimed_stale_running_status",
            duplicate_delivery="ingest_job_duplicate_delivery_skipped_running_attempt",
            max_attempts_exceeded="ingest_job_max_attempts_exceeded",
            cancelled="ingest_job_cancelled",
        ),
    ),
    BeginOrResumeRoute(
        process_name="process_quantity_takeoff_job",
        supported_job_types=_job_types_for_process("process_quantity_takeoff_job"),
        log_keys=BeginOrResumeLogKeys(
            unsupported_type="quantity_takeoff_job_unsupported_type_skipped",
            terminal_status="quantity_takeoff_job_skipped_terminal_status",
            inactive_source="quantity_takeoff_job_cancelled_inactive_source",
            reclaimed_stale_running="quantity_takeoff_job_reclaimed_stale_running_status",
            duplicate_delivery="quantity_takeoff_job_duplicate_delivery_skipped_running_attempt",
            max_attempts_exceeded="quantity_takeoff_job_max_attempts_exceeded",
            cancelled="quantity_takeoff_job_cancelled",
        ),
    ),
    BeginOrResumeRoute(
        process_name="process_estimate_job",
        supported_job_types=_job_types_for_process("process_estimate_job"),
        log_keys=BeginOrResumeLogKeys(
            unsupported_type="estimate_job_unsupported_type_skipped",
            terminal_status="estimate_job_skipped_terminal_status",
            inactive_source="estimate_job_cancelled_inactive_source",
            reclaimed_stale_running="estimate_job_reclaimed_stale_running_status",
            duplicate_delivery="estimate_job_duplicate_delivery_skipped_running_attempt",
            max_attempts_exceeded="estimate_job_max_attempts_exceeded",
            cancelled="estimate_job_cancelled",
        ),
    ),
    BeginOrResumeRoute(
        process_name="process_export_job",
        supported_job_types=_job_types_for_process("process_export_job"),
        log_keys=BeginOrResumeLogKeys(
            unsupported_type="export_job_unsupported_type_skipped",
            terminal_status="export_job_skipped_terminal_status",
            inactive_source="export_job_cancelled_inactive_source",
            reclaimed_stale_running="export_job_reclaimed_stale_running_status",
            duplicate_delivery="export_job_duplicate_delivery_skipped_running_attempt",
            max_attempts_exceeded="export_job_max_attempts_exceeded",
            cancelled="export_job_cancelled",
        ),
    ),
    BeginOrResumeRoute(
        process_name="process_changeset_apply_job",
        supported_job_types=_job_types_for_process("process_changeset_apply_job"),
        log_keys=BeginOrResumeLogKeys(
            unsupported_type="changeset_apply_job_unsupported_type_skipped",
            terminal_status="changeset_apply_job_skipped_terminal_status",
            inactive_source="changeset_apply_job_cancelled_inactive_source",
            reclaimed_stale_running="changeset_apply_job_reclaimed_stale_running_status",
            duplicate_delivery="changeset_apply_job_duplicate_delivery_skipped_running_attempt",
            max_attempts_exceeded="changeset_apply_job_max_attempts_exceeded",
            cancelled="changeset_apply_job_cancelled",
        ),
    ),
)

_BEGIN_OR_RESUME_ROUTES_BY_PROCESS_NAME: Final[dict[str, BeginOrResumeRoute]] = {
    route.process_name: route for route in BEGIN_OR_RESUME_ROUTES
}

BEGIN_OR_RESUME_ROUTES_BY_PROCESS_NAME: Final[Mapping[str, BeginOrResumeRoute]] = MappingProxyType(
    _BEGIN_OR_RESUME_ROUTES_BY_PROCESS_NAME
)

RECOVERABLE_ENQUEUE_JOB_TYPES: Final[tuple[str, ...]] = tuple(
    handler.job_type_name for handler in JOB_HANDLERS
)
INGEST_WORKER_JOB_TYPES: Final[tuple[str, ...]] = tuple(
    handler.job_type_name for handler in JOB_HANDLERS if handler.run_task_name == "run_ingest_job"
)
JOB_TYPES_WITHOUT_ENQUEUE_PUBLISHER: Final[frozenset[str]] = frozenset(
    handler.job_type_name for handler in JOB_HANDLERS if handler.enqueue_publisher_name is None
)
ENQUEUE_ERROR_MESSAGES_BY_JOB_TYPE: Final[Mapping[str, str]] = MappingProxyType(
    {handler.job_type_name: handler.enqueue_error_message for handler in JOB_HANDLERS}
)


def get_job_handler(job_type: str) -> JobHandler | None:
    """Return worker metadata for a persisted job type."""
    return JOB_HANDLERS_BY_TYPE.get(job_type)


def get_begin_or_resume_route(process_name: str) -> BeginOrResumeRoute | None:
    """Return begin/resume routing metadata for a persisted worker process."""
    return BEGIN_OR_RESUME_ROUTES_BY_PROCESS_NAME.get(process_name)
