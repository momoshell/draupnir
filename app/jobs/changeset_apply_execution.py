"""Changeset-apply job input validation and deterministic error helpers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.cad.changeset import (
    ChangeSetApplyConflict,
    ChangeSetApplyError,
    ChangeSetApplyLoadError,
    ChangeSetApplySuccess,
    load_and_apply_change_set,
)
from app.core.errors import ErrorCode
from app.models.cad_changeset import CadChangeSet
from app.models.changeset_apply_job_input import ChangeSetApplyJobInput
from app.models.job import Job, JobType


@dataclass(frozen=True, slots=True)
class ChangeSetApplyJobError(Exception):
    """Raised for deterministic changeset apply execution failures."""

    error_code: ErrorCode
    message: str
    details: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


type QueryChangesetApplyJobInputs = Callable[
    [AsyncSession],
    Awaitable[list[ChangeSetApplyJobInput]],
]


async def query_changeset_apply_job_inputs(
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


def changeset_apply_input_matches_job(
    apply_input: ChangeSetApplyJobInput,
    *,
    job: Job,
) -> bool:
    """Return whether an immutable apply input matches its persisted job lineage."""
    return (
        apply_input.project_id == job.project_id
        and apply_input.source_file_id == job.file_id
        and apply_input.drawing_revision_id == job.base_revision_id
        and apply_input.source_job_id == job.id
        and apply_input.source_job_type == JobType.CHANGESET_APPLY.value
    )


def changeset_apply_lineage_error_details(
    apply_input: ChangeSetApplyJobInput,
    *,
    job: Job,
) -> dict[str, Any]:
    """Build safe lineage mismatch details for a changeset-apply job input."""
    return {
        "source_job_type": apply_input.source_job_type,
        "source_file_id": str(apply_input.source_file_id),
        "file_id": str(job.file_id),
        "drawing_revision_id": str(apply_input.drawing_revision_id),
        "base_revision_id": str(job.base_revision_id) if job.base_revision_id is not None else None,
    }


async def load_changeset_apply_job_input(
    session: AsyncSession,
    *,
    job: Job,
    query_job_inputs: Callable[..., Awaitable[list[ChangeSetApplyJobInput]]] = (
        query_changeset_apply_job_inputs
    ),
) -> ChangeSetApplyJobInput:
    """Validate that one immutable apply input exists and matches the persisted job."""
    apply_inputs = await query_job_inputs(session, job_id=job.id)
    if not apply_inputs:
        raise ChangeSetApplyJobError(
            error_code=ErrorCode.NOT_FOUND,
            message="Changeset apply job input is missing.",
            details=None,
        )
    if len(apply_inputs) != 1:
        raise ChangeSetApplyJobError(
            error_code=ErrorCode.INPUT_INVALID,
            message="Changeset apply job has multiple immutable inputs.",
            details={"input_count": len(apply_inputs)},
        )

    apply_input = apply_inputs[0]
    if not changeset_apply_input_matches_job(apply_input, job=job):
        raise ChangeSetApplyJobError(
            error_code=ErrorCode.INPUT_INVALID,
            message="Changeset apply job input lineage does not match the persisted job.",
            details=changeset_apply_lineage_error_details(apply_input, job=job),
        )
    return apply_input


async def load_changeset_apply_job_input_if_valid(
    session: AsyncSession,
    *,
    job: Job,
    query_job_inputs: Callable[..., Awaitable[list[ChangeSetApplyJobInput]]] = (
        query_changeset_apply_job_inputs
    ),
) -> ChangeSetApplyJobInput | None:
    """Return the immutable apply input when exactly one lineage-matching row exists."""
    apply_inputs = await query_job_inputs(session, job_id=job.id)
    if len(apply_inputs) != 1:
        return None

    apply_input = apply_inputs[0]
    if not changeset_apply_input_matches_job(apply_input, job=job):
        return None
    return apply_input


def parse_uuid_value(value: Any) -> UUID | None:
    """Best-effort UUID parsing for persisted error payloads."""
    if isinstance(value, UUID):
        return value
    if not isinstance(value, str):
        return None
    try:
        return UUID(value)
    except ValueError:
        return None


async def resolve_changeset_apply_failure_target(
    session: AsyncSession,
    *,
    job: Job,
    fallback_change_set_id: UUID | None = None,
    load_input_if_valid: Callable[..., Awaitable[ChangeSetApplyJobInput | None]] = (
        load_changeset_apply_job_input_if_valid
    ),
) -> CadChangeSet | None:
    """Resolve the mutable changeset row for an apply-job terminal failure."""
    apply_input = await load_input_if_valid(session, job=job)
    change_set_id = apply_input.change_set_id if apply_input is not None else fallback_change_set_id
    if change_set_id is None:
        return None

    change_set = await session.get(CadChangeSet, change_set_id)
    if change_set is None or change_set.project_id != job.project_id:
        return None
    if job.base_revision_id is not None and change_set.base_revision_id != job.base_revision_id:
        return None
    return change_set


def build_changeset_apply_error_details(result: ChangeSetApplyError) -> dict[str, Any]:
    """Convert a deterministic apply-engine failure into worker job failure details."""
    details: dict[str, Any] = {
        "change_set_id": str(result.change_set_id),
        "apply_error_code": result.error_code,
    }
    if result.details:
        details.update(dict(result.details))
    return details


async def execute_changeset_apply_job_attempt(
    job_id: UUID,
    *,
    attempt_token: UUID,
    session_maker_factory: Callable[[], Any],
    job_attempt_is_current: Callable[..., bool],
    load_apply_job_input: Callable[..., Awaitable[ChangeSetApplyJobInput]],
    build_conflict_details: Callable[[ChangeSetApplyConflict], dict[str, Any]],
    stale_job_attempt_error_type: type[Exception],
    revision_conflict_error_type: Callable[..., Exception],
    apply_change_set: Callable[..., Awaitable[Any]] = load_and_apply_change_set,
) -> ChangeSetApplySuccess:
    """Load immutable apply input, re-run apply loading, and return apply success."""
    session_maker = session_maker_factory()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await session.get(Job, job_id)
        if job is None:
            raise LookupError(f"Job with identifier '{job_id}' not found")
        if not job_attempt_is_current(job, attempt_token=attempt_token):
            raise stale_job_attempt_error_type(
                f"Job attempt for '{job_id}' no longer owns the lease"
            )
        if job.job_type != JobType.CHANGESET_APPLY.value:
            raise ValueError(f"Unsupported changeset apply job type '{job.job_type}'")
        if job.base_revision_id is None:
            raise revision_conflict_error_type(
                message="Changeset apply job is missing its finalized base revision.",
                details={
                    "base_revision_id": None,
                    "current_revision_id": None,
                },
            )

        apply_input = await load_apply_job_input(session, job=job)
        try:
            apply_result = await apply_change_set(
                session,
                project_id=job.project_id,
                change_set_id=apply_input.change_set_id,
            )
        except ChangeSetApplyLoadError as exc:
            raise ChangeSetApplyJobError(
                error_code=ErrorCode.INPUT_INVALID,
                message=str(exc),
                details={
                    "change_set_id": str(apply_input.change_set_id),
                },
            ) from exc

    if isinstance(apply_result, ChangeSetApplyConflict):
        raise revision_conflict_error_type(
            message="Changeset apply base revision is stale relative to the current revision.",
            details=build_conflict_details(apply_result),
        )
    if isinstance(apply_result, ChangeSetApplyError):
        raise ChangeSetApplyJobError(
            error_code=ErrorCode.INPUT_INVALID,
            message=apply_result.message,
            details=build_changeset_apply_error_details(apply_result),
        )

    assert isinstance(apply_result, ChangeSetApplySuccess)
    return apply_result
