"""Unit tests for the registered worker job processor."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from app.core.errors import ErrorCode
from app.jobs import registered_processor
from app.jobs.runner import JobHandler


@dataclass(frozen=True, slots=True)
class _FakeLease:
    token: uuid.UUID
    lease_expires_at: datetime


class _FakeLogger:
    def __init__(self) -> None:
        self.infos: list[tuple[str, dict[str, Any]]] = []
        self.warnings: list[tuple[str, dict[str, Any]]] = []

    def info(self, event: str, **fields: Any) -> None:
        self.infos.append((event, fields))

    def warning(self, event: str, **fields: Any) -> None:
        self.warnings.append((event, fields))


@dataclass(frozen=True, slots=True)
class _InputError(Exception):
    error_code: ErrorCode
    message: str
    details: dict[str, Any] | None = None


class _InactiveSourceError(Exception):
    pass


class _StaleJobAttemptError(Exception):
    pass


class _RevisionConflictError(Exception):
    pass


class _ChangeSetApplyJobError(Exception):
    pass


def _build_spec(
    *,
    execution_result_arg_name: str | None = None,
) -> registered_processor.RegisteredJobProcessSpec:
    return registered_processor.RegisteredJobProcessSpec(
        job_type_name="quantity_takeoff",
        input_error_type=_InputError,
        input_failure_log_event="input_failed",
        stale_attempt_log_event="stale",
        revision_conflict_log_event="revision_conflict",
        cancelled_during_execution_log_event="cancelled_execution",
        cancelled_during_finalization_log_event="cancelled_finalization",
        process_failed_log_event="process_failed",
        finalization_failed_log_event="finalize_failed",
        succeeded_log_event="succeeded",
        process_error_message="Process failed.",
        finalize_error_message="Finalize failed.",
        execution_result_arg_name=execution_result_arg_name,
    )


def _handler() -> JobHandler:
    return JobHandler(
        job_type_name="quantity_takeoff",
        execute_name="execute",
        finalize_name="finalize",
        process_name="process_quantity_takeoff_job",
        run_task_name="run_quantity_takeoff_job",
        enqueue_publisher_name="enqueue_quantity_takeoff_job",
        enqueue_error_message="Failed to enqueue quantity takeoff job",
    )


async def _identity_lease_renewal(coro: Any, **kwargs: Any) -> Any:
    _ = kwargs
    return await coro


def _base_callbacks(
    *,
    lease: _FakeLease,
    namespace: dict[str, Any],
    logger: _FakeLogger,
    records: dict[str, Any],
) -> dict[str, Any]:
    async def begin_or_resume(job_id: uuid.UUID, *, process_name: str) -> _FakeLease:
        records["begin"] = (job_id, process_name)
        return lease

    async def cancel_requested(
        job_id: uuid.UUID,
        *,
        attempt_token: uuid.UUID,
        log_event: str,
    ) -> bool:
        records["cancel_check"] = (job_id, attempt_token, log_event)
        return False

    async def mark_revision_conflict(*args: Any, **kwargs: Any) -> None:
        records["revision_conflict"] = (args, kwargs)

    async def mark_cancelled(*args: Any, **kwargs: Any) -> None:
        records["cancelled"] = (args, kwargs)

    async def mark_changeset_revision_conflict(*args: Any, **kwargs: Any) -> None:
        records["changeset_revision_conflict"] = (args, kwargs)

    async def mark_changeset_input_error(*args: Any, **kwargs: Any) -> None:
        records["changeset_input_error"] = (args, kwargs)

    async def mark_job_failed(*args: Any, **kwargs: Any) -> bool:
        records["job_failed"] = (args, kwargs)
        return True

    async def mark_internal_error(*args: Any, **kwargs: Any) -> None:
        records["internal_error"] = (args, kwargs)

    return {
        "ensure_worker_database_configured": lambda: records.setdefault("ensured", True),
        "get_registered_job_handler_func": lambda _job_type_name: _handler(),
        "begin_or_resume_registered_job": begin_or_resume,
        "cancel_registered_job_if_requested": cancel_requested,
        "resolve_registered_job_callable_func": namespace.__getitem__,
        "with_job_attempt_lease_renewal": _identity_lease_renewal,
        "mark_job_failed_for_revision_conflict": mark_revision_conflict,
        "mark_job_cancelled_with_log": mark_cancelled,
        "mark_changeset_apply_job_failed_for_revision_conflict": mark_changeset_revision_conflict,
        "mark_changeset_apply_job_failed_for_input_error": mark_changeset_input_error,
        "mark_job_failed": mark_job_failed,
        "mark_job_failed_with_internal_error_log": mark_internal_error,
        "inactive_source_error_type": _InactiveSourceError,
        "stale_job_attempt_error_type": _StaleJobAttemptError,
        "revision_conflict_error_type": _RevisionConflictError,
        "changeset_apply_job_error_type": _ChangeSetApplyJobError,
        "logger_instance": logger,
    }


@pytest.mark.asyncio
async def test_process_registered_job_finalizes_success() -> None:
    job_id = uuid.uuid4()
    lease = _FakeLease(uuid.uuid4(), datetime.now(UTC) + timedelta(minutes=1))
    records: dict[str, Any] = {}
    logger = _FakeLogger()

    async def execute(job_id: uuid.UUID, *, attempt_token: uuid.UUID, deps: Any) -> Any:
        records["execute"] = (job_id, attempt_token, deps)
        return registered_processor.RegisteredJobAttemptResult(finalize_kwargs={"result": 42})

    async def finalize(
        job_id: uuid.UUID,
        *,
        attempt_token: uuid.UUID,
        deps: Any,
        result: int,
    ) -> bool:
        records["finalize"] = (job_id, attempt_token, deps, result)
        return True

    await registered_processor.process_registered_job(
        job_id,
        spec=_build_spec(),
        deps={"deps": True},
        **_base_callbacks(
            lease=lease,
            namespace={"execute": execute, "finalize": finalize},
            logger=logger,
            records=records,
        ),
    )

    assert records["execute"] == (job_id, lease.token, {"deps": True})
    assert records["finalize"] == (job_id, lease.token, {"deps": True}, 42)
    assert logger.infos == [("succeeded", {"job_id": str(job_id)})]


@pytest.mark.asyncio
async def test_process_registered_job_marks_input_error() -> None:
    job_id = uuid.uuid4()
    lease = _FakeLease(uuid.uuid4(), datetime.now(UTC) + timedelta(minutes=1))
    records: dict[str, Any] = {}
    logger = _FakeLogger()

    async def execute(job_id: uuid.UUID, *, attempt_token: uuid.UUID, deps: Any) -> None:
        _ = (job_id, attempt_token, deps)
        raise _InputError(
            error_code=ErrorCode.INPUT_INVALID,
            message="Bad input.",
            details={"reason": "bad"},
        )

    async def finalize(*args: Any, **kwargs: Any) -> bool:
        raise AssertionError("finalize should not run after input error")

    await registered_processor.process_registered_job(
        job_id,
        spec=_build_spec(),
        deps=None,
        **_base_callbacks(
            lease=lease,
            namespace={"execute": execute, "finalize": finalize},
            logger=logger,
            records=records,
        ),
    )

    assert records["job_failed"][1] == {
        "error_message": "Bad input.",
        "error_code": ErrorCode.INPUT_INVALID,
        "attempt_token": lease.token,
        "error_details": {"reason": "bad"},
    }
    assert logger.warnings == [
        (
            "input_failed",
            {
                "job_id": str(job_id),
                "error_code": ErrorCode.INPUT_INVALID.value,
                "reason": "bad",
            },
        )
    ]


@pytest.mark.asyncio
async def test_process_registered_job_rejects_raw_result_without_arg_name() -> None:
    job_id = uuid.uuid4()
    lease = _FakeLease(uuid.uuid4(), datetime.now(UTC) + timedelta(minutes=1))
    records: dict[str, Any] = {}

    async def execute(job_id: uuid.UUID, *, attempt_token: uuid.UUID, deps: Any) -> int:
        _ = (job_id, attempt_token, deps)
        return 42

    async def finalize(*args: Any, **kwargs: Any) -> bool:
        raise AssertionError("finalize should not run after raw result contract failure")

    with pytest.raises(TypeError, match="execution_result_arg_name"):
        await registered_processor.process_registered_job(
            job_id,
            spec=_build_spec(),
            deps=None,
            **_base_callbacks(
                lease=lease,
                namespace={"execute": execute, "finalize": finalize},
                logger=_FakeLogger(),
                records=records,
            ),
        )


def test_resolve_registered_job_callable_rejects_missing_name() -> None:
    with pytest.raises(LookupError, match="missing"):
        registered_processor.resolve_registered_job_callable("missing", namespace={})
