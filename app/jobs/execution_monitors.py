"""Persisted job progress/cancellation monitors and the reusable worker event loop.

Owns the background collaborators that watch a single job execution: the
progress-event drain bridge (``_JobProgressEventBridge``), the persisted
cancellation poll/checkpoint pair, and the process-local asyncio runner that
Celery's synchronous task entrypoints use to drive async worker code
(``_run_worker_loop``).

Worker-owned collaborators (``emit_job_event``, ``_job_attempt_is_current``,
``_mark_job_cancelled``) are injected rather than imported, so
``app.jobs.worker`` test monkeypatches of those names are honored.
"""

import asyncio
import atexit
import threading
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from app.db.session import get_session_maker
from app.ingestion.contracts import ProgressUpdate
from app.ingestion.finalization import IngestFinalizationPayload
from app.models.job import Job

_JOB_CANCELLATION_POLL_INTERVAL_SECONDS = 0.1

_WORKER_LOOP_RUNNER: asyncio.Runner | None = None
_WORKER_LOOP_RUNNER_LOCK = threading.Lock()


@dataclass(frozen=True, slots=True)
class _QueuedJobEvent:
    """Buffered job event persisted by the progress drain."""

    level: str
    message: str
    data_json: dict[str, Any]


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

    def __init__(
        self,
        job_id: UUID,
        *,
        attempt_token: UUID,
        emit_job_event_func: Callable[..., Awaitable[bool]],
    ) -> None:
        self._job_id = job_id
        self._attempt_token = attempt_token
        self._emit_job_event_func = emit_job_event_func
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
                await self._emit_job_event_func(
                    self._job_id,
                    level=queued.level,
                    message=queued.message,
                    data_json=queued.data_json,
                    attempt_token=self._attempt_token,
                )
            finally:
                self._queue.task_done()


async def _poll_job_cancellation(
    job_id: UUID,
    *,
    attempt_token: UUID,
    cancellation: _PersistedJobCancellationHandle,
    run_task: asyncio.Task[IngestFinalizationPayload],
    stop_event: asyncio.Event,
    job_attempt_is_current_func: Callable[..., bool],
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

        if not job_attempt_is_current_func(job, attempt_token=attempt_token):
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
    job_attempt_is_current_func: Callable[..., bool],
    mark_job_cancelled_func: Callable[..., Awaitable[bool]],
    logger_instance: Any,
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

    if not job_attempt_is_current_func(job, attempt_token=attempt_token):
        return False

    if not job.cancel_requested and job.status != "cancelled":
        return False

    cancelled = await mark_job_cancelled_func(job_id, attempt_token=attempt_token)
    if cancelled:
        logger_instance.info(log_event, job_id=str(job_id))
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
