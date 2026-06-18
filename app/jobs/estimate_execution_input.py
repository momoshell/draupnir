"""Estimate execution-input lineage validation (issue D7c).

The estimate engine-input assembly (``app.jobs.estimate_assembly``) interleaves
DB loading, lineage validation, and catalog resolution. The catalog resolution
genuinely needs the live session, but the *lineage validation* — the job /
estimate-input / quantity-takeoff contract checks — is pure over already-loaded
rows. This module extracts those checks so they are unit-testable with in-memory
fakes; the assembly loads each row and hands it to the matching validator,
preserving the original load/validate ordering and error precedence.
"""

from __future__ import annotations

from uuid import UUID

from app.jobs.estimate_mapping import _build_estimate_job_input_error
from app.jobs.lifecycle import (
    _job_attempt_is_current,
    _RevisionConflictError,
    _StaleJobAttemptError,
)
from app.models.estimate_job_input import EstimateJobInput
from app.models.job import Job, JobType
from app.models.quantity_takeoff import QuantityTakeoff


def validate_estimate_job(job: Job | None, *, attempt_token: UUID, job_id: UUID) -> Job:
    """Validate the claimed estimate job and narrow it to a non-null row."""
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
    return job


def validate_estimate_input(
    estimate_input: EstimateJobInput | None, *, job: Job
) -> EstimateJobInput:
    """Validate the persisted estimate job input lineage against the job."""
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
    return estimate_input


def validate_quantity_takeoff(
    quantity_takeoff: QuantityTakeoff | None,
    *,
    estimate_input: EstimateJobInput,
    job: Job,
) -> QuantityTakeoff:
    """Validate the quantity takeoff lineage against the job and estimate input."""
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
    return quantity_takeoff
