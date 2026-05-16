"""API tests for revision quantity takeoff routes."""

from __future__ import annotations

from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.idempotency import IdempotencyReplay, IdempotencyReservation
from app.api.v1 import revisions as revisions_module
from app.api.v1.revisions import revisions_router
from app.db import session as session_module
from app.db.session import get_db
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.extraction_profile import ExtractionProfile
from app.models.file import File
from app.models.job import Job, JobType
from app.models.project import Project
from app.models.quantity_takeoff import (
    QuantityGate,
    QuantityItem,
    QuantityItemKind,
    QuantityTakeoff,
)
from tests.conftest import requires_database


class _ScalarResultStub:
    def __init__(self, value: Any) -> None:
        self._value = value

    def scalars(self) -> _ScalarResultStub:
        return self

    def all(self) -> list[Any]:
        if isinstance(self._value, list):
            return self._value
        raise AssertionError("Expected list result")

    def scalar_one_or_none(self) -> Any:
        return self._value


class _AsyncSessionStub:
    def __init__(self, *, execute_results: list[Any] | None = None) -> None:
        self._execute_results: deque[Any] = deque(execute_results or [])
        self.added: list[Any] = []
        self.commits = 0

    async def execute(self, _statement: Any) -> _ScalarResultStub:
        if not self._execute_results:
            raise AssertionError("Unexpected execute call")
        return _ScalarResultStub(self._execute_results.popleft())

    def add(self, value: Any) -> None:
        self.added.append(value)

    async def flush(self) -> None:
        return None

    async def refresh(self, value: Any) -> None:
        if getattr(value, "created_at", None) is None:
            value.created_at = datetime.now(UTC)

    async def commit(self) -> None:
        self.commits += 1


@dataclass(slots=True)
class _QuantityLineageSeed:
    project_id: UUID
    file_id: UUID
    revision_id: UUID
    takeoff_id: UUID


def _build_app(session: _AsyncSessionStub) -> FastAPI:
    app = FastAPI()
    app.include_router(revisions_router, prefix="/v1")

    async def override_db() -> AsyncIterator[AsyncSession]:
        yield session  # type: ignore[misc]

    app.dependency_overrides[get_db] = override_db
    return app


def _build_revision(*, revision_id: UUID | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        id=revision_id or uuid4(),
        project_id=uuid4(),
        source_file_id=uuid4(),
    )


def _build_validation_report(*, quantity_gate: str = QuantityGate.ALLOWED.value) -> SimpleNamespace:
    return SimpleNamespace(
        quantity_gate=quantity_gate,
        review_state="approved",
        validation_status="valid",
    )


async def _seed_quantity_lineage() -> _QuantityLineageSeed:
    session_factory = session_module.AsyncSessionLocal
    assert session_factory is not None

    project_id = uuid4()
    file_id = uuid4()
    profile_id = uuid4()
    ingest_job_id = uuid4()
    adapter_output_id = uuid4()
    revision_id = uuid4()
    quantity_job_id = uuid4()
    takeoff_id = uuid4()

    async with session_factory() as db:
        project = Project(id=project_id, name="Quantity visibility test")
        file = File(
            id=file_id,
            project_id=project_id,
            original_filename="plan.pdf",
            media_type="application/pdf",
            detected_format="pdf",
            storage_uri=f"file:///tmp/{file_id}.pdf",
            size_bytes=128,
            checksum_sha256="0" * 64,
            immutable=True,
            initial_job_id=None,
            initial_extraction_profile_id=None,
        )
        profile = ExtractionProfile(
            id=profile_id,
            project_id=project_id,
            profile_version="1",
            layout_mode="model_space",
            xref_handling="bind",
            block_handling="keep",
            text_extraction=True,
            dimension_extraction=True,
            pdf_page_range=None,
            raster_calibration=None,
            confidence_threshold=0.6,
        )
        ingest_job = Job(
            id=ingest_job_id,
            project_id=project_id,
            file_id=file_id,
            extraction_profile_id=profile_id,
            base_revision_id=None,
            parent_job_id=None,
            job_type=JobType.INGEST.value,
            status="succeeded",
            attempts=1,
            max_attempts=3,
            enqueue_status="published",
            enqueue_attempts=1,
            cancel_requested=False,
            error_code=None,
            error_message=None,
            started_at=None,
            finished_at=None,
        )
        adapter_output = AdapterRunOutput(
            id=adapter_output_id,
            project_id=project_id,
            source_file_id=file_id,
            extraction_profile_id=profile_id,
            source_job_id=ingest_job_id,
            adapter_key="test-adapter",
            adapter_version="1.0.0",
            input_family="pdf_vector",
            canonical_entity_schema_version="1.0.0",
            canonical_json={},
            provenance_json={},
            confidence_json={},
            confidence_score=1.0,
            warnings_json=[],
            diagnostics_json={},
            result_checksum_sha256="1" * 64,
        )
        revision = DrawingRevision(
            id=revision_id,
            project_id=project_id,
            source_file_id=file_id,
            extraction_profile_id=profile_id,
            source_job_id=ingest_job_id,
            adapter_run_output_id=adapter_output_id,
            predecessor_revision_id=None,
            revision_sequence=1,
            revision_kind="ingest",
            review_state="approved",
            canonical_entity_schema_version="1.0.0",
            confidence_score=1.0,
        )
        quantity_job = Job(
            id=quantity_job_id,
            project_id=project_id,
            file_id=file_id,
            extraction_profile_id=None,
            base_revision_id=revision_id,
            parent_job_id=None,
            job_type=JobType.QUANTITY_TAKEOFF.value,
            status="succeeded",
            attempts=1,
            max_attempts=3,
            enqueue_status="published",
            enqueue_attempts=1,
            cancel_requested=False,
            error_code=None,
            error_message=None,
            started_at=None,
            finished_at=None,
        )
        takeoff = QuantityTakeoff(
            id=takeoff_id,
            project_id=project_id,
            source_file_id=file_id,
            drawing_revision_id=revision_id,
            source_job_id=quantity_job_id,
            source_job_type=JobType.QUANTITY_TAKEOFF.value,
            review_state="approved",
            validation_status="valid",
            quantity_gate=QuantityGate.ALLOWED.value,
            trusted_totals=True,
        )
        item = QuantityItem(
            quantity_takeoff_id=takeoff_id,
            project_id=project_id,
            drawing_revision_id=revision_id,
            item_kind=QuantityItemKind.AGGREGATE.value,
            quantity_type="area",
            value=42.0,
            unit="sq_ft",
            review_state="approved",
            validation_status="valid",
            quantity_gate=QuantityGate.ALLOWED.value,
            source_entity_id=None,
            excluded_source_entity_ids_json=[],
        )

        db.add(project)
        await db.commit()

        db.add(file)
        db.add(profile)
        await db.commit()

        db.add(ingest_job)
        await db.commit()

        db.add(adapter_output)
        await db.commit()

        db.add(revision)
        await db.commit()

        db.add(quantity_job)
        await db.commit()

        db.add(takeoff)
        await db.commit()

        db.add(item)
        await db.commit()

    return _QuantityLineageSeed(
        project_id=project_id,
        file_id=file_id,
        revision_id=revision_id,
        takeoff_id=takeoff_id,
    )


async def _soft_delete_lineage(seed: _QuantityLineageSeed, *, target: str) -> None:
    session_factory = session_module.AsyncSessionLocal
    assert session_factory is not None

    async with session_factory() as db:
        if target == "file":
            file = await db.get(File, seed.file_id)
            assert file is not None
            file.deleted_at = datetime.now(UTC)
        else:
            project = await db.get(Project, seed.project_id)
            assert project is not None
            project.deleted_at = datetime.now(UTC)
        await db.commit()


def test_create_revision_quantity_takeoff_replays_idempotent_response(monkeypatch: Any) -> None:
    revision = _build_revision()
    session = _AsyncSessionStub()
    app = _build_app(session)
    client = TestClient(app)
    manifest = SimpleNamespace(id=uuid4())
    idempotency_state: dict[str, dict[str, Any]] = {}
    enqueued_job_ids: list[UUID] = []

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (db, for_update)
        return revision if revision_id == revision.id else None

    async def fake_get_revision_manifest(
        revision_id: UUID,
        db: AsyncSession,
    ) -> SimpleNamespace | None:
        _ = db
        return manifest if revision_id == revision.id else None

    async def fake_get_validation_report(
        revision_id: UUID,
        db: AsyncSession,
    ) -> SimpleNamespace:
        _ = db
        if revision_id != revision.id:
            raise AssertionError("Unexpected revision lookup")
        return _build_validation_report()

    async def fake_replay_idempotency(
        db: AsyncSession,
        *,
        key: str,
        fingerprint: str,
    ) -> IdempotencyReplay | None:
        _ = db
        record = idempotency_state.get(key)
        if record is None or record["fingerprint"] != fingerprint or not record["completed"]:
            return None
        return IdempotencyReplay(
            response=JSONResponse(
                status_code=record["status_code"],
                content=record["response_body"],
            )
        )

    async def fake_claim_idempotency(
        db: AsyncSession,
        *,
        key: str,
        fingerprint: str,
        method: str,
        path: str,
    ) -> IdempotencyReservation:
        _ = (db, method, path)
        record = idempotency_state.get(key)
        if record is None:
            record = {
                "fingerprint": fingerprint,
                "completed": False,
            }
            idempotency_state[key] = record
        return IdempotencyReservation(record_id=uuid4())

    async def fake_mark_idempotency_completed(
        db: AsyncSession,
        reservation: IdempotencyReservation,
        *,
        status_code: int,
        response_body: dict[str, Any] | None,
    ) -> None:
        _ = (db, reservation)
        idempotency_state["quantity-1"].update(
            completed=True,
            status_code=status_code,
            response_body=response_body,
        )

    def fake_prepare_job_enqueue_intent(job: Any) -> None:
        _ = job

    async def fake_publish_job_enqueue_intent(
        job_id: UUID,
        *,
        publisher: Any = None,
        suppress_exceptions: bool = False,
    ) -> None:
        _ = suppress_exceptions
        assert publisher is revisions_module.enqueue_quantity_takeoff_job
        publisher(job_id)

    def fake_enqueue_quantity_takeoff_job(job_id: UUID) -> None:
        enqueued_job_ids.append(job_id)

    def fail_enqueue_ingest_job(job_id: UUID) -> None:
        raise AssertionError(f"unexpected ingest enqueue for {job_id}")

    monkeypatch.setattr(revisions_module, "_get_active_revision", fake_get_active_revision)
    monkeypatch.setattr(revisions_module, "_get_revision_manifest", fake_get_revision_manifest)
    monkeypatch.setattr(
        revisions_module,
        "_get_active_validation_report_or_404",
        fake_get_validation_report,
    )
    monkeypatch.setattr(revisions_module, "replay_idempotency", fake_replay_idempotency)
    monkeypatch.setattr(revisions_module, "claim_idempotency", fake_claim_idempotency)
    monkeypatch.setattr(
        revisions_module,
        "mark_idempotency_completed",
        fake_mark_idempotency_completed,
    )
    monkeypatch.setattr(
        revisions_module,
        "prepare_job_enqueue_intent",
        fake_prepare_job_enqueue_intent,
    )
    monkeypatch.setattr(
        revisions_module,
        "publish_job_enqueue_intent",
        fake_publish_job_enqueue_intent,
    )
    monkeypatch.setattr(
        revisions_module,
        "_enqueue_quantity_takeoff_job",
        fake_enqueue_quantity_takeoff_job,
        raising=False,
    )
    monkeypatch.setattr(
        revisions_module,
        "_enqueue_ingest_job",
        fail_enqueue_ingest_job,
        raising=False,
    )

    response = client.post(
        f"/v1/revisions/{revision.id}/quantity-takeoffs",
        headers={"Idempotency-Key": "quantity-1"},
    )

    assert response.status_code == 202
    first_body = response.json()
    assert first_body["job_type"] == "quantity_takeoff"
    assert first_body["base_revision_id"] == str(revision.id)
    assert len(session.added) == 1
    assert enqueued_job_ids == [UUID(first_body["id"])]

    replay_response = client.post(
        f"/v1/revisions/{revision.id}/quantity-takeoffs",
        headers={"Idempotency-Key": "quantity-1"},
    )

    assert replay_response.status_code == 202
    assert replay_response.json() == first_body
    assert len(session.added) == 1


def test_list_and_get_revision_quantity_takeoffs(monkeypatch: Any) -> None:
    revision = _build_revision()
    created_at = datetime(2026, 5, 16, tzinfo=UTC)
    first_takeoff = SimpleNamespace(
        id=uuid4(),
        project_id=revision.project_id,
        source_file_id=revision.source_file_id,
        drawing_revision_id=revision.id,
        source_job_id=uuid4(),
        source_job_type="quantity_takeoff",
        review_state="approved",
        validation_status="valid",
        quantity_gate="allowed",
        trusted_totals=True,
        created_at=created_at,
    )
    second_takeoff = SimpleNamespace(
        id=uuid4(),
        project_id=revision.project_id,
        source_file_id=revision.source_file_id,
        drawing_revision_id=revision.id,
        source_job_id=uuid4(),
        source_job_type="quantity_takeoff",
        review_state="provisional",
        validation_status="valid_with_warnings",
        quantity_gate="allowed_provisional",
        trusted_totals=False,
        created_at=created_at.replace(second=1),
    )
    session = _AsyncSessionStub(execute_results=[[first_takeoff, second_takeoff], first_takeoff])
    app = _build_app(session)
    client = TestClient(app)

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (db, for_update)
        return revision if revision_id == revision.id else None

    monkeypatch.setattr(revisions_module, "_get_active_revision", fake_get_active_revision)

    list_response = client.get(f"/v1/revisions/{revision.id}/quantity-takeoffs?limit=1")

    assert list_response.status_code == 200
    list_body = list_response.json()
    assert [item["id"] for item in list_body["items"]] == [str(first_takeoff.id)]
    assert list_body["next_cursor"] is not None

    read_response = client.get(
        f"/v1/revisions/{revision.id}/quantity-takeoffs/{first_takeoff.id}"
    )

    assert read_response.status_code == 200
    assert read_response.json()["id"] == str(first_takeoff.id)


def test_list_revision_quantity_takeoff_items(monkeypatch: Any) -> None:
    revision = _build_revision()
    takeoff = SimpleNamespace(id=uuid4())
    created_at = datetime(2026, 5, 16, tzinfo=UTC)
    first_item = SimpleNamespace(
        id=uuid4(),
        quantity_takeoff_id=takeoff.id,
        project_id=revision.project_id,
        drawing_revision_id=revision.id,
        item_kind="aggregate",
        quantity_type="area",
        value=120.5,
        unit="sq_ft",
        review_state="approved",
        validation_status="valid",
        quantity_gate="allowed",
        source_entity_id=None,
        excluded_source_entity_ids_json=[],
        created_at=created_at,
    )
    second_item = SimpleNamespace(
        id=uuid4(),
        quantity_takeoff_id=takeoff.id,
        project_id=revision.project_id,
        drawing_revision_id=revision.id,
        item_kind="contributor",
        quantity_type="area",
        value=20.0,
        unit="sq_ft",
        review_state="approved",
        validation_status="valid",
        quantity_gate="allowed",
        source_entity_id="entity-1",
        excluded_source_entity_ids_json=["entity-2"],
        created_at=created_at.replace(second=1),
    )
    session = _AsyncSessionStub(execute_results=[takeoff, [first_item, second_item]])
    app = _build_app(session)
    client = TestClient(app)

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (db, for_update)
        return revision if revision_id == revision.id else None

    monkeypatch.setattr(revisions_module, "_get_active_revision", fake_get_active_revision)

    response = client.get(
        f"/v1/revisions/{revision.id}/quantity-takeoffs/{takeoff.id}/items?limit=1"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["items"][0]["id"] == str(first_item.id)
    assert body["items"][0]["excluded_source_entity_ids"] == []
    assert body["next_cursor"] is not None
    assert session.commits == 0


@pytest.mark.anyio
@pytest.mark.parametrize("soft_delete_target", ["file", "project"])
@requires_database
async def test_list_revision_quantity_takeoffs_hides_soft_deleted_lineage_after_stale_check(
    async_client: AsyncClient,
    monkeypatch: Any,
    soft_delete_target: str,
) -> None:
    seed = await _seed_quantity_lineage()
    await _soft_delete_lineage(seed, target=soft_delete_target)
    stale_revision = _build_revision(revision_id=seed.revision_id)
    stale_revision.project_id = seed.project_id
    stale_revision.source_file_id = seed.file_id

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (db, for_update)
        return stale_revision if revision_id == seed.revision_id else None

    monkeypatch.setattr(revisions_module, "_get_active_revision", fake_get_active_revision)

    response = await async_client.get(f"/v1/revisions/{seed.revision_id}/quantity-takeoffs")

    assert response.status_code == 200
    assert response.json() == {"items": [], "next_cursor": None}


@pytest.mark.anyio
@pytest.mark.parametrize("soft_delete_target", ["file", "project"])
@requires_database
async def test_get_revision_quantity_takeoff_hides_soft_deleted_lineage_after_stale_check(
    async_client: AsyncClient,
    monkeypatch: Any,
    soft_delete_target: str,
) -> None:
    seed = await _seed_quantity_lineage()
    await _soft_delete_lineage(seed, target=soft_delete_target)
    stale_revision = _build_revision(revision_id=seed.revision_id)
    stale_revision.project_id = seed.project_id
    stale_revision.source_file_id = seed.file_id

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (db, for_update)
        return stale_revision if revision_id == seed.revision_id else None

    monkeypatch.setattr(revisions_module, "_get_active_revision", fake_get_active_revision)

    response = await async_client.get(
        f"/v1/revisions/{seed.revision_id}/quantity-takeoffs/{seed.takeoff_id}"
    )

    assert response.status_code == 404


@pytest.mark.anyio
@pytest.mark.parametrize("soft_delete_target", ["file", "project"])
@requires_database
async def test_list_revision_quantity_takeoff_items_hides_soft_deleted_lineage_after_stale_checks(
    async_client: AsyncClient,
    monkeypatch: Any,
    soft_delete_target: str,
) -> None:
    seed = await _seed_quantity_lineage()
    await _soft_delete_lineage(seed, target=soft_delete_target)
    stale_revision = _build_revision(revision_id=seed.revision_id)
    stale_revision.project_id = seed.project_id
    stale_revision.source_file_id = seed.file_id
    stale_takeoff = SimpleNamespace(id=seed.takeoff_id)

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (db, for_update)
        return stale_revision if revision_id == seed.revision_id else None

    async def fake_get_revision_quantity_takeoff_or_404(
        revision_id: UUID,
        takeoff_id: UUID,
        db: AsyncSession,
    ) -> SimpleNamespace:
        _ = db
        assert revision_id == seed.revision_id
        assert takeoff_id == seed.takeoff_id
        return stale_takeoff

    monkeypatch.setattr(revisions_module, "_get_active_revision", fake_get_active_revision)
    monkeypatch.setattr(
        revisions_module,
        "_get_revision_quantity_takeoff_or_404",
        fake_get_revision_quantity_takeoff_or_404,
    )

    response = await async_client.get(
        f"/v1/revisions/{seed.revision_id}/quantity-takeoffs/{seed.takeoff_id}/items"
    )

    assert response.status_code == 200
    assert response.json() == {"items": [], "next_cursor": None}


@pytest.mark.anyio
@pytest.mark.parametrize("soft_delete_target", ["file", "project"])
@requires_database
async def test_create_revision_quantity_takeoff_rechecks_locked_lineage_before_insert(
    async_client: AsyncClient,
    enqueued_job_ids: list[UUID],
    monkeypatch: Any,
    soft_delete_target: str,
) -> None:
    seed = await _seed_quantity_lineage()
    await _soft_delete_lineage(seed, target=soft_delete_target)
    stale_revision = _build_revision(revision_id=seed.revision_id)
    stale_revision.project_id = seed.project_id
    stale_revision.source_file_id = seed.file_id
    original_get_active_revision = revisions_module._get_active_revision

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> Any:
        if revision_id != seed.revision_id:
            return None
        if for_update:
            return await original_get_active_revision(revision_id, db, for_update=True)
        return stale_revision

    async def fake_get_active_revision_manifest_or_409(
        revision_id: UUID,
        db: AsyncSession,
    ) -> SimpleNamespace:
        _ = db
        assert revision_id == seed.revision_id
        return SimpleNamespace(id=uuid4())

    async def fake_get_active_validation_report_or_404(
        revision_id: UUID,
        db: AsyncSession,
    ) -> SimpleNamespace:
        _ = db
        assert revision_id == seed.revision_id
        return _build_validation_report()

    monkeypatch.setattr(revisions_module, "_get_active_revision", fake_get_active_revision)
    monkeypatch.setattr(
        revisions_module,
        "_get_active_revision_manifest_or_409",
        fake_get_active_revision_manifest_or_409,
    )
    monkeypatch.setattr(
        revisions_module,
        "_get_active_validation_report_or_404",
        fake_get_active_validation_report_or_404,
    )

    response = await async_client.post(f"/v1/revisions/{seed.revision_id}/quantity-takeoffs")

    assert response.status_code == 404
    assert enqueued_job_ids == []


@pytest.mark.parametrize(
    "quantity_gate",
    [QuantityGate.REVIEW_GATED.value, QuantityGate.BLOCKED.value],
)
def test_create_revision_quantity_takeoff_rejects_review_gated_or_blocked(
    monkeypatch: Any,
    quantity_gate: str,
) -> None:
    revision = _build_revision()
    session = _AsyncSessionStub()
    app = _build_app(session)
    client = TestClient(app)
    manifest = SimpleNamespace(id=uuid4())
    enqueue_calls: list[UUID] = []
    publish_calls: list[UUID] = []

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (db, for_update)
        return revision if revision_id == revision.id else None

    async def fake_get_revision_manifest(
        revision_id: UUID,
        db: AsyncSession,
    ) -> SimpleNamespace | None:
        _ = db
        return manifest if revision_id == revision.id else None

    async def fake_get_validation_report(
        revision_id: UUID,
        db: AsyncSession,
    ) -> SimpleNamespace:
        _ = db
        if revision_id != revision.id:
            raise AssertionError("Unexpected revision lookup")
        return _build_validation_report(quantity_gate=quantity_gate)

    def fake_prepare_job_enqueue_intent(job: Any) -> None:
        raise AssertionError(f"unexpected prepare for {job.id}")

    async def fake_publish_job_enqueue_intent(
        job_id: UUID,
        *,
        publisher: Any = None,
        suppress_exceptions: bool = False,
    ) -> None:
        _ = (publisher, suppress_exceptions)
        publish_calls.append(job_id)

    def fake_enqueue_quantity_takeoff_job(job_id: UUID) -> None:
        enqueue_calls.append(job_id)

    monkeypatch.setattr(revisions_module, "_get_active_revision", fake_get_active_revision)
    monkeypatch.setattr(revisions_module, "_get_revision_manifest", fake_get_revision_manifest)
    monkeypatch.setattr(
        revisions_module,
        "_get_active_validation_report_or_404",
        fake_get_validation_report,
    )
    monkeypatch.setattr(
        revisions_module,
        "prepare_job_enqueue_intent",
        fake_prepare_job_enqueue_intent,
    )
    monkeypatch.setattr(
        revisions_module,
        "publish_job_enqueue_intent",
        fake_publish_job_enqueue_intent,
    )
    monkeypatch.setattr(
        revisions_module,
        "enqueue_quantity_takeoff_job",
        fake_enqueue_quantity_takeoff_job,
    )

    response = client.post(f"/v1/revisions/{revision.id}/quantity-takeoffs")

    assert response.status_code == 400
    assert response.json()["detail"]["error"]["code"] == "INPUT_INVALID"
    assert session.added == []
    assert session.commits == 0
    assert publish_calls == []
    assert enqueue_calls == []


def test_revision_quantity_takeoff_routes_return_404_for_unknown_revision(
    monkeypatch: Any,
) -> None:
    session = _AsyncSessionStub()
    app = _build_app(session)
    client = TestClient(app)
    unknown_revision_id = uuid4()
    unknown_takeoff_id = uuid4()

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (revision_id, db, for_update)
        return None

    monkeypatch.setattr(revisions_module, "_get_active_revision", fake_get_active_revision)

    create_response = client.post(f"/v1/revisions/{unknown_revision_id}/quantity-takeoffs")
    list_response = client.get(f"/v1/revisions/{unknown_revision_id}/quantity-takeoffs")
    read_response = client.get(
        f"/v1/revisions/{unknown_revision_id}/quantity-takeoffs/{unknown_takeoff_id}"
    )
    items_response = client.get(
        f"/v1/revisions/{unknown_revision_id}/quantity-takeoffs/{unknown_takeoff_id}/items"
    )

    for response in (create_response, list_response, read_response, items_response):
        assert response.status_code == 404


def test_list_revision_quantity_takeoffs_rejects_invalid_cursor(monkeypatch: Any) -> None:
    revision = _build_revision()
    session = _AsyncSessionStub()
    app = _build_app(session)
    client = TestClient(app)

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (db, for_update)
        return revision if revision_id == revision.id else None

    monkeypatch.setattr(revisions_module, "_get_active_revision", fake_get_active_revision)

    response = client.get(f"/v1/revisions/{revision.id}/quantity-takeoffs?cursor=not-base64")

    assert response.status_code == 400
    assert response.json()["detail"]["error"]["code"] == "INVALID_CURSOR"


def test_list_revision_quantity_takeoff_items_rejects_invalid_cursor(
    monkeypatch: Any,
) -> None:
    revision = _build_revision()
    takeoff = SimpleNamespace(id=uuid4())
    session = _AsyncSessionStub(execute_results=[takeoff])
    app = _build_app(session)
    client = TestClient(app)

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (db, for_update)
        return revision if revision_id == revision.id else None

    monkeypatch.setattr(revisions_module, "_get_active_revision", fake_get_active_revision)

    response = client.get(
        f"/v1/revisions/{revision.id}/quantity-takeoffs/{takeoff.id}/items?cursor=not-base64"
    )

    assert response.status_code == 400
    assert response.json()["detail"]["error"]["code"] == "INVALID_CURSOR"
