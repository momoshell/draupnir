"""Unit tests for worker execution monitoring helpers."""

from __future__ import annotations

import asyncio
import uuid
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

import pytest

from app.ingestion.contracts import ProgressUpdate
from app.jobs import execution_monitoring


@dataclass(slots=True)
class _FakeJob:
    cancel_requested: bool
    status: str


class _FakeSession:
    def __init__(self, job: _FakeJob | None) -> None:
        self._job = job

    async def get(self, model: object, job_id: uuid.UUID) -> _FakeJob | None:
        _ = (model, job_id)
        return self._job


class _FakeSessionContext:
    def __init__(self, job: _FakeJob | None) -> None:
        self._session = _FakeSession(job)

    async def __aenter__(self) -> _FakeSession:
        return self._session

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        _ = (exc_type, exc, traceback)


class _FakeLogger:
    def __init__(self) -> None:
        self.infos: list[tuple[str, dict[str, Any]]] = []

    def info(self, event: str, **fields: Any) -> None:
        self.infos.append((event, fields))


def _session_maker_factory(job: _FakeJob | None) -> Any:
    return lambda: lambda: _FakeSessionContext(job)


def _current_job_attempt(_job: Any, *, attempt_token: uuid.UUID) -> bool:
    assert isinstance(attempt_token, uuid.UUID)
    return True


def test_progress_event_data_includes_optional_fields() -> None:
    payload = execution_monitoring.progress_event_data(
        ProgressUpdate(
            stage="parse",
            message="Parsing",
            completed=1,
            total=4,
            percent=0.25,
        )
    )

    assert payload == {
        "status": "running",
        "event": "progress",
        "stage": "parse",
        "detail": "Parsing",
        "completed": 1,
        "total": 4,
        "percent": 0.25,
    }


@pytest.mark.asyncio
async def test_job_progress_event_bridge_drains_and_rejects_after_flush() -> None:
    job_id = uuid.uuid4()
    attempt_token = uuid.uuid4()
    emitted: list[dict[str, Any]] = []

    async def emit_job_event(
        job_id: uuid.UUID,
        *,
        level: str,
        message: str,
        data_json: dict[str, Any] | None = None,
        attempt_token: uuid.UUID | None = None,
    ) -> bool:
        emitted.append(
            {
                "job_id": str(job_id),
                "level": level,
                "message": message,
                "data_json": data_json,
                "attempt_token": str(attempt_token),
            }
        )
        return True

    bridge = execution_monitoring.JobProgressEventBridge(
        job_id,
        attempt_token=attempt_token,
        emit_job_event=emit_job_event,
    )

    bridge.callback(ProgressUpdate(stage="parse", message="Parsing"))
    await bridge.flush()

    assert emitted == [
        {
            "job_id": str(job_id),
            "level": "info",
            "message": "Parsing",
            "data_json": {
                "status": "running",
                "event": "progress",
                "stage": "parse",
                "detail": "Parsing",
            },
            "attempt_token": str(attempt_token),
        }
    ]
    with pytest.raises(RuntimeError, match="after bridge closed"):
        bridge.callback(ProgressUpdate(stage="done"))


@pytest.mark.asyncio
async def test_poll_job_cancellation_marks_handle_and_cancels_run_task() -> None:
    job_id = uuid.uuid4()
    attempt_token = uuid.uuid4()
    cancellation = execution_monitoring.PersistedJobCancellationHandle()
    stop_event = asyncio.Event()
    run_task = asyncio.create_task(asyncio.sleep(60))

    await execution_monitoring.poll_job_cancellation(
        job_id,
        attempt_token=attempt_token,
        cancellation=cancellation,
        run_task=run_task,
        stop_event=stop_event,
        session_maker_factory=_session_maker_factory(
            _FakeJob(cancel_requested=True, status="running")
        ),
        job_attempt_is_current=_current_job_attempt,
        poll_interval_seconds=0.001,
    )

    assert cancellation.is_cancelled() is True
    assert run_task.cancelled() or run_task.cancelling() > 0
    with suppress(asyncio.CancelledError):
        await run_task


@pytest.mark.asyncio
async def test_cancel_registered_job_if_requested_marks_and_logs() -> None:
    job_id = uuid.uuid4()
    attempt_token = uuid.uuid4()
    marked: list[tuple[uuid.UUID, uuid.UUID | None]] = []
    logger = _FakeLogger()

    async def mark_job_cancelled(
        job_id: uuid.UUID,
        *,
        attempt_token: uuid.UUID | None = None,
    ) -> bool:
        marked.append((job_id, attempt_token))
        return True

    cancelled = await execution_monitoring.cancel_registered_job_if_requested(
        job_id,
        attempt_token=attempt_token,
        log_event="job_cancelled",
        session_maker_factory=_session_maker_factory(
            _FakeJob(cancel_requested=True, status="running")
        ),
        job_attempt_is_current=_current_job_attempt,
        mark_job_cancelled=mark_job_cancelled,
        logger_instance=logger,
    )

    assert cancelled is True
    assert marked == [(job_id, attempt_token)]
    assert logger.infos == [("job_cancelled", {"job_id": str(job_id)})]
