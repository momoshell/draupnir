"""Progress and cancellation monitoring helpers for worker job execution."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from app.ingestion.contracts import ProgressUpdate
from app.models.job import Job


@dataclass(frozen=True, slots=True)
class QueuedJobEvent:
    """Buffered job event persisted by the progress drain."""

    level: str
    message: str
    data_json: dict[str, Any]


class PersistedJobCancellationHandle:
    """Cancellation handle backed by worker polling."""

    def __init__(self) -> None:
        self._cancel_requested = False

    def is_cancelled(self) -> bool:
        return self._cancel_requested

    def mark_cancelled(self) -> None:
        self._cancel_requested = True


class EmitJobEvent(Protocol):
    async def __call__(
        self,
        job_id: UUID,
        *,
        level: str,
        message: str,
        data_json: dict[str, Any] | None = None,
        attempt_token: UUID | None = None,
    ) -> bool: ...


type JobAttemptIsCurrent = Callable[..., bool]


class MarkJobCancelled(Protocol):
    async def __call__(self, job_id: UUID, *, attempt_token: UUID | None = None) -> bool: ...


def progress_event_data(update: ProgressUpdate) -> dict[str, Any]:
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


class JobProgressEventBridge:
    """Synchronous progress callback with async event draining."""

    _STOP = object()

    def __init__(
        self,
        job_id: UUID,
        *,
        attempt_token: UUID,
        emit_job_event: EmitJobEvent,
        build_progress_event_data: Callable[[ProgressUpdate], dict[str, Any]] = progress_event_data,
    ) -> None:
        self._job_id = job_id
        self._attempt_token = attempt_token
        self._emit_job_event = emit_job_event
        self._build_progress_event_data = build_progress_event_data
        self._queue: asyncio.Queue[QueuedJobEvent | object] = asyncio.Queue()
        self._drain_task = asyncio.create_task(self._drain())
        self._closed = False

    def callback(self, update: ProgressUpdate) -> None:
        if self._closed:
            raise RuntimeError("Progress callback received update after bridge closed.")

        self._queue.put_nowait(
            QueuedJobEvent(
                level="info",
                message=update.message or f"Job progress: {update.stage}",
                data_json=self._build_progress_event_data(update),
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

                assert isinstance(queued, QueuedJobEvent)
                await self._emit_job_event(
                    self._job_id,
                    level=queued.level,
                    message=queued.message,
                    data_json=queued.data_json,
                    attempt_token=self._attempt_token,
                )
            finally:
                self._queue.task_done()


async def poll_job_cancellation(
    job_id: UUID,
    *,
    attempt_token: UUID,
    cancellation: PersistedJobCancellationHandle,
    run_task: asyncio.Task[Any],
    stop_event: asyncio.Event,
    session_maker_factory: Callable[[], Any],
    job_attempt_is_current: JobAttemptIsCurrent,
    poll_interval_seconds: float,
) -> None:
    """Poll persisted cancellation without holding DB locks during execution."""
    session_maker = session_maker_factory()
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

        if not job_attempt_is_current(job, attempt_token=attempt_token):
            return

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=poll_interval_seconds)
        except TimeoutError:
            continue


async def cancel_registered_job_if_requested(
    job_id: UUID,
    *,
    attempt_token: UUID,
    log_event: str,
    session_maker_factory: Callable[[], Any],
    job_attempt_is_current: JobAttemptIsCurrent,
    mark_job_cancelled: MarkJobCancelled,
    logger_instance: Any,
) -> bool:
    """Honor a cancellation requested before compute begins; return True if cancelled."""
    session_maker = session_maker_factory()
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        job = await session.get(Job, job_id)

    if job is None:
        raise LookupError(f"Job with identifier '{job_id}' not found")

    if not job_attempt_is_current(job, attempt_token=attempt_token):
        return False

    if not job.cancel_requested and job.status != "cancelled":
        return False

    cancelled = await mark_job_cancelled(job_id, attempt_token=attempt_token)
    if cancelled:
        logger_instance.info(log_event, job_id=str(job_id))
    return cancelled


async def stop_job_execution_monitor(
    *,
    progress_bridge: JobProgressEventBridge,
    stop_event: asyncio.Event,
    cancellation_task: asyncio.Task[None],
) -> None:
    """Flush queued progress and stop background execution monitors."""
    stop_event.set()
    try:
        await cancellation_task
    finally:
        await progress_bridge.flush()
