"""Unit tests for the reusable worker event-loop runner."""

from __future__ import annotations

from app.jobs import worker_loop


def teardown_function() -> None:
    worker_loop.close_worker_loop_runner()


def test_get_worker_loop_runner_reuses_runner_until_closed() -> None:
    runner = worker_loop.get_worker_loop_runner()

    assert worker_loop.get_worker_loop_runner() is runner

    worker_loop.close_worker_loop_runner()

    assert worker_loop.get_worker_loop_runner() is not runner


def test_run_worker_loop_runs_coroutine_factory() -> None:
    async def work() -> str:
        return "done"

    assert worker_loop.run_worker_loop(work) == "done"


def test_close_worker_loop_runner_is_idempotent() -> None:
    worker_loop.close_worker_loop_runner()
    worker_loop.close_worker_loop_runner()
