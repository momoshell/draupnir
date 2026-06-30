"""Reusable event-loop runner for synchronous worker entrypoints."""

from __future__ import annotations

import asyncio
import atexit
import threading
from collections.abc import Callable, Coroutine
from typing import Any

_WORKER_LOOP_RUNNER: asyncio.Runner | None = None
_WORKER_LOOP_RUNNER_LOCK = threading.Lock()


def get_worker_loop_runner() -> asyncio.Runner:
    """Return the reusable asyncio runner for sync worker entrypoints."""
    global _WORKER_LOOP_RUNNER
    if _WORKER_LOOP_RUNNER is None:
        _WORKER_LOOP_RUNNER = asyncio.Runner()
    return _WORKER_LOOP_RUNNER


def close_worker_loop_runner() -> None:
    """Close and clear the reusable asyncio runner."""
    global _WORKER_LOOP_RUNNER
    with _WORKER_LOOP_RUNNER_LOCK:
        runner = _WORKER_LOOP_RUNNER
        if runner is None:
            return
        runner.close()
        _WORKER_LOOP_RUNNER = None


def run_worker_loop[WorkerLoopResultT](
    coro_factory: Callable[[], Coroutine[Any, Any, WorkerLoopResultT]],
) -> WorkerLoopResultT:
    """Run a worker coroutine on the process-local reusable event loop."""
    with _WORKER_LOOP_RUNNER_LOCK:
        return get_worker_loop_runner().run(coro_factory())


atexit.register(close_worker_loop_runner)
