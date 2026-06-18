"""Tests for the finalization write seam (issue D5b).

Two layers, both DB-free:
- the default ``_SqlFinalizationPersister`` issues the expected session calls
  (recorded by a fake session), and
- a finalizer routes its writes through ``deps.finalization_persister``, so the
  orchestration can be exercised with a fake persister that captures the rowset
  instead of asserting against persisted database rows.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any, cast

import app.jobs.finalizers as finalizers_module
from app.jobs.finalization_persister import _SqlFinalizationPersister
from app.jobs.result_builders import (
    EstimateRows,
    QuantityTakeoffRows,
    build_quantity_takeoff_rows,
)


class _RecordingSession:
    """Captures the ordered add/flush calls a persister makes."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def add(self, row: Any) -> None:
        self.calls.append(("add", row))

    def add_all(self, rows: Any) -> None:
        self.calls.append(("add_all", list(rows)))

    async def flush(self) -> None:
        self.calls.append(("flush", None))


async def test_sql_persister_quantity_takeoff_flushes_header_before_items() -> None:
    session = _RecordingSession()
    takeoff = SimpleNamespace(id=uuid.uuid4())
    items = [SimpleNamespace(id=uuid.uuid4()), SimpleNamespace(id=uuid.uuid4())]
    rows = QuantityTakeoffRows(takeoff=cast(Any, takeoff), items=cast(Any, items))

    await _SqlFinalizationPersister().persist_quantity_takeoff(cast(Any, session), rows)

    assert [name for name, _ in session.calls] == ["add", "flush", "add_all"]
    assert session.calls[0][1] is takeoff
    assert session.calls[2][1] == items


async def test_sql_persister_estimate_flushes_version_then_entries_then_items() -> None:
    session = _RecordingSession()
    version = SimpleNamespace(id=uuid.uuid4())
    entries = [SimpleNamespace(id=uuid.uuid4())]
    line_items = [SimpleNamespace(id=uuid.uuid4())]
    rows = EstimateRows(
        version=cast(Any, version),
        snapshot_entries=cast(Any, entries),
        line_items=cast(Any, line_items),
    )

    await _SqlFinalizationPersister().persist_estimate(cast(Any, session), rows)

    assert [name for name, _ in session.calls] == [
        "add",
        "flush",
        "add_all",
        "flush",
        "add_all",
    ]
    assert session.calls[0][1] is version


# --- orchestration: finalizer routes writes through the injected persister ---


class _CapturingPersister:
    """Fake persister capturing the rowset instead of writing to a database."""

    def __init__(self) -> None:
        self.quantity_rows: QuantityTakeoffRows | None = None

    async def persist_quantity_takeoff(self, _session: Any, rows: QuantityTakeoffRows) -> None:
        self.quantity_rows = rows

    async def persist_estimate(self, _session: Any, _rows: Any) -> None:  # pragma: no cover
        raise AssertionError("estimate persister should not be called here")


def _execution(revision_id: uuid.UUID) -> Any:
    return SimpleNamespace(
        drawing_revision_id=revision_id,
        validation_status="valid",
    )


def _result() -> Any:
    contributor = SimpleNamespace(
        entity_id="ent-1",
        quantity_type="length",
        value=1.0,
        unit="m",
        context=None,
        duplicate_entity_ids=(),
    )
    return SimpleNamespace(
        contributors=(contributor,),
        aggregates=(),
        exclusions=(),
        conflicts=(),
    )


async def test_finalize_quantity_takeoff_routes_writes_through_persister(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    revision_id = uuid.uuid4()
    token = uuid.uuid4()
    job = SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        file_id=uuid.uuid4(),
        status="running",
        attempt_token=token,
        cancel_requested=False,
        base_revision_id=revision_id,
        attempts=1,
        finished_at=None,
        error_code="x",
        error_message="y",
        attempt_lease_expires_at=None,
    )
    locked_source = SimpleNamespace(
        project=SimpleNamespace(deleted_at=None),
        job=job,
        source_file=SimpleNamespace(deleted_at=None),
    )

    commits: list[bool] = []

    class _FakeSession:
        async def commit(self) -> None:
            commits.append(True)

    @asynccontextmanager
    async def _session_cm() -> Any:
        yield _FakeSession()

    monkeypatch.setattr(finalizers_module, "get_session_maker", lambda: _session_cm)

    # Skip the DB existing-takeoff query.
    async def _no_existing_takeoff(_session: Any, *, source_job_id: uuid.UUID) -> None:
        return None

    monkeypatch.setattr(finalizers_module, "_get_existing_quantity_takeoff", _no_existing_takeoff)

    persister = _CapturingPersister()
    events: list[dict[str, Any]] = []

    async def _lock_job_source(_session: Any, _job_id: uuid.UUID) -> Any:
        return locked_source

    async def _emit_job_event(*_args: Any, **kwargs: Any) -> bool:
        events.append(kwargs.get("data_json", {}))
        return True

    deps = cast(
        Any,
        SimpleNamespace(
            lock_job_source=_lock_job_source,
            emit_job_event=_emit_job_event,
            finalization_persister=persister,
            finalize_job_cancelled=lambda _job: None,
            cancel_job_for_inactive_source=None,
        ),
    )

    expected = build_quantity_takeoff_rows(
        project_id=job.project_id,
        source_file_id=job.file_id,
        source_job_id=job.id,
        execution=_execution(revision_id),
        result=_result(),
    )

    published = await finalizers_module._finalize_quantity_takeoff_job(
        job.id,
        attempt_token=token,
        deps=deps,
        execution=_execution(revision_id),
        result=_result(),
    )

    assert published is True
    assert commits == [True]
    assert job.status == "succeeded"
    assert job.attempt_token is None  # lease cleared
    # The persister captured a rowset equivalent to the builder output (no DB rows).
    assert persister.quantity_rows is not None
    assert len(persister.quantity_rows.items) == len(expected.items)
    assert events and events[0]["status"] == "succeeded"
