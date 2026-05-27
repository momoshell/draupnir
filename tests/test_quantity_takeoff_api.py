"""API tests for revision quantity takeoff routes."""

from __future__ import annotations

from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.idempotency import IdempotencyReservation
from app.api.v1 import revisions as revisions_module
from app.api.v1.revisions import revisions_router
from app.db import session as session_module
from app.db.session import get_db
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.estimate_job_input import EstimateJobInput, EstimateJobInputCatalogRef
from app.models.estimate_version import EstimateItem, EstimateSnapshotEntry, EstimateVersion
from app.models.estimation_catalog import (
    EstimationFormula,
    EstimationRate,
    EstimationRateSupersession,
)
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
from app.models.revision_materialization import RevisionEntity
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
    def __init__(
        self,
        *,
        execute_results: list[Any] | None = None,
        refresh_created_at: datetime | None = None,
    ) -> None:
        self._execute_results: deque[Any] = deque(execute_results or [])
        self._refresh_created_at = refresh_created_at
        self.added: list[Any] = []
        self.commits = 0
        self.refresh_calls = 0

    async def execute(self, _statement: Any) -> _ScalarResultStub:
        if not self._execute_results:
            raise AssertionError("Unexpected execute call")
        return _ScalarResultStub(self._execute_results.popleft())

    def add(self, value: Any) -> None:
        self.added.append(value)

    async def flush(self) -> None:
        return None

    async def refresh(self, value: Any) -> None:
        self.refresh_calls += 1
        if getattr(value, "created_at", None) is None:
            value.created_at = self._refresh_created_at or datetime.now(UTC)

    async def commit(self) -> None:
        self.commits += 1


def _response_error_code(response: Any) -> str:
    body = response.json()
    error = body.get("error")
    if error is None:
        detail = body.get("detail")
        if isinstance(detail, dict):
            error = detail.get("error")
    assert isinstance(error, dict)
    code = error.get("code")
    assert isinstance(code, str)
    return code


@dataclass(slots=True)
class _QuantityLineageSeed:
    project_id: UUID
    file_id: UUID
    revision_id: UUID
    takeoff_id: UUID
    quantity_item_id: UUID


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


def _build_takeoff(
    revision: SimpleNamespace,
    *,
    takeoff_id: UUID | None = None,
    quantity_gate: str = QuantityGate.ALLOWED.value,
    trusted_totals: bool = True,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=takeoff_id or uuid4(),
        project_id=revision.project_id,
        source_file_id=revision.source_file_id,
        drawing_revision_id=revision.id,
        source_job_id=uuid4(),
        source_job_type=JobType.QUANTITY_TAKEOFF.value,
        review_state="approved",
        validation_status="valid",
        quantity_gate=quantity_gate,
        trusted_totals=trusted_totals,
    )


def _build_estimate_request_body(
    *, quantity_item_id: UUID, currency: str = "GBP"
) -> dict[str, Any]:
    return {
        "pricing": {"currency": currency},
        "assumptions": {"crew": "default"},
        "catalog_refs": [
            {
                "ref_type": "rate",
                "selection_id": str(uuid4()),
                "selection_key": "labour:install",
                "selection_checksum_sha256": "a" * 64,
                "description": "Install labour",
                "line_key": "line-rate-1",
                "quantity_item_id": str(quantity_item_id),
            },
            {
                "ref_type": "formula",
                "selection_id": str(uuid4()),
                "selection_key": "formula:waste",
                "selection_checksum_sha256": "b" * 64,
                "description": "Waste uplift",
                "line_key": "line-formula-2",
                "formula_inputs": {"waste_factor": "0.10"},
            },
        ],
    }


async def _seed_quantity_lineage(
    *,
    quantity_gate: str = QuantityGate.ALLOWED.value,
    trusted_totals: bool = True,
) -> _QuantityLineageSeed:
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
            quantity_gate=quantity_gate,
            trusted_totals=trusted_totals,
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
            quantity_gate=quantity_gate,
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
        quantity_item_id=item.id,
    )


async def _assert_no_estimate_persistence_for_project(seed: _QuantityLineageSeed) -> None:
    session_factory = session_module.AsyncSessionLocal
    assert session_factory is not None

    async with session_factory() as db:
        estimate_jobs = (
            (
                await db.execute(
                    select(Job).where(
                        (Job.project_id == seed.project_id)
                        & (Job.job_type == JobType.ESTIMATE.value)
                    )
                )
            )
            .scalars()
            .all()
        )
        estimate_inputs = (
            (
                await db.execute(
                    select(EstimateJobInput).where(EstimateJobInput.project_id == seed.project_id)
                )
            )
            .scalars()
            .all()
        )
        estimate_refs = (
            (
                await db.execute(
                    select(EstimateJobInputCatalogRef).where(
                        EstimateJobInputCatalogRef.estimate_job_id.in_(
                            select(Job.id).where(
                                (Job.project_id == seed.project_id)
                                & (Job.job_type == JobType.ESTIMATE.value)
                            )
                        )
                    )
                )
            )
            .scalars()
            .all()
        )
        estimate_versions = (
            (
                await db.execute(
                    select(EstimateVersion).where(EstimateVersion.project_id == seed.project_id)
                )
            )
            .scalars()
            .all()
        )
        snapshot_entries = (
            (
                await db.execute(
                    select(EstimateSnapshotEntry).where(
                        EstimateSnapshotEntry.estimate_version_id.in_(
                            select(EstimateVersion.id).where(
                                EstimateVersion.project_id == seed.project_id
                            )
                        )
                    )
                )
            )
            .scalars()
            .all()
        )
        estimate_items = (
            (
                await db.execute(
                    select(EstimateItem).where(
                        EstimateItem.estimate_version_id.in_(
                            select(EstimateVersion.id).where(
                                EstimateVersion.project_id == seed.project_id
                            )
                        )
                    )
                )
            )
            .scalars()
            .all()
        )

    assert estimate_jobs == []
    assert estimate_inputs == []
    assert estimate_refs == []
    assert estimate_versions == []
    assert snapshot_entries == []
    assert estimate_items == []


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


def _build_rate_catalog_entry(
    *,
    scope_type: str,
    project_id: UUID | None,
    rate_key: str,
    checksum_sha256: str,
    effective_from: date,
    effective_to: date | None = None,
) -> EstimationRate:
    return EstimationRate(
        scope_type=scope_type,
        project_id=project_id,
        rate_key=rate_key,
        source="fixture",
        metadata_json={},
        name="Install labour",
        item_type="labour",
        per_unit="sq_ft",
        currency="GBP",
        amount=Decimal("12.340000"),
        effective_from=effective_from,
        effective_to=effective_to,
        checksum_sha256=checksum_sha256,
    )


def _build_formula_definition(
    *,
    scope_type: str,
    project_id: UUID | None,
    formula_id: str,
    checksum_sha256: str,
) -> EstimationFormula:
    return EstimationFormula(
        scope_type=scope_type,
        project_id=project_id,
        formula_id=formula_id,
        version=1,
        name="Waste uplift",
        dsl_version="1.0.0",
        output_key="total",
        output_contract_json={"type": "number"},
        declared_inputs_json=[{"key": "waste_factor", "type": "number"}],
        expression_json={"kind": "literal", "value": "1.10"},
        rounding_json={},
        checksum_sha256=checksum_sha256,
    )


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

    async def fake_replay_idempotency_response(
        db: AsyncSession,
        *,
        key: str,
        fingerprint: str,
    ) -> JSONResponse | None:
        _ = db
        record = idempotency_state.get(key)
        if record is None or record["fingerprint"] != fingerprint or not record["completed"]:
            return None
        return JSONResponse(
            status_code=record["status_code"],
            content=record["response_body"],
        )

    async def fake_claim_idempotency_response(
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

    async def fake_complete_idempotency_response(
        db: AsyncSession,
        reservation: IdempotencyReservation | None,
        *,
        status_code: int,
        response_body: dict[str, Any] | None,
    ) -> JSONResponse | None:
        _ = (db, reservation)
        assert reservation is not None
        idempotency_state["quantity-1"].update(
            completed=True,
            status_code=status_code,
            response_body=response_body,
        )
        return JSONResponse(status_code=status_code, content=response_body)

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
    monkeypatch.setattr(
        revisions_module,
        "replay_idempotency_response",
        fake_replay_idempotency_response,
    )
    monkeypatch.setattr(
        revisions_module,
        "claim_idempotency_response",
        fake_claim_idempotency_response,
    )
    monkeypatch.setattr(
        revisions_module,
        "complete_idempotency_response",
        fake_complete_idempotency_response,
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


def test_create_revision_quantity_takeoff_returns_job_without_idempotency_key(
    monkeypatch: Any,
) -> None:
    revision = _build_revision()
    session = _AsyncSessionStub()
    app = _build_app(session)
    client = TestClient(app)
    manifest = SimpleNamespace(id=uuid4())
    enqueued_job_ids: list[UUID] = []
    complete_calls = 0

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

    async def fake_complete_idempotency_response(
        db: AsyncSession,
        reservation: IdempotencyReservation | None,
        *,
        status_code: int,
        response_body: dict[str, Any] | None,
    ) -> JSONResponse | None:
        _ = (db, reservation, response_body)
        nonlocal complete_calls
        complete_calls += 1
        return JSONResponse(status_code=status_code, content=None)

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

    monkeypatch.setattr(revisions_module, "_get_active_revision", fake_get_active_revision)
    monkeypatch.setattr(revisions_module, "_get_revision_manifest", fake_get_revision_manifest)
    monkeypatch.setattr(
        revisions_module,
        "_get_active_validation_report_or_404",
        fake_get_validation_report,
    )
    monkeypatch.setattr(
        revisions_module,
        "complete_idempotency_response",
        fake_complete_idempotency_response,
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

    assert response.status_code == 202
    assert response.json()["job_type"] == "quantity_takeoff"
    assert response.json()["base_revision_id"] == str(revision.id)
    assert len(session.added) == 1
    assert session.commits == 1
    assert enqueued_job_ids == [UUID(response.json()["id"])]
    assert complete_calls == 0


def test_create_revision_estimate_version_replays_idempotent_response(monkeypatch: Any) -> None:
    revision = _build_revision()
    takeoff = _build_takeoff(revision)
    quantity_item_id = uuid4()
    request_body = _build_estimate_request_body(quantity_item_id=quantity_item_id)
    job_created_at = datetime(2026, 5, 21, 1, 0, tzinfo=UTC)
    session = _AsyncSessionStub(refresh_created_at=datetime(2026, 5, 20, 9, 30, tzinfo=UTC))
    app = _build_app(session)
    client = TestClient(app)
    idempotency_state: dict[str, dict[str, Any]] = {}
    enqueued_job_ids: list[UUID] = []
    pricing_effective_dates: list[date] = []

    class _EstimateJobInputRecord:
        pass

    class _EstimateJobInputCatalogRefRecord:
        pass

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (db, for_update)
        return revision if revision_id == revision.id else None

    async def fake_get_revision_quantity_takeoff_or_404(
        revision_id: UUID,
        takeoff_id: UUID,
        db: AsyncSession,
    ) -> SimpleNamespace:
        _ = db
        if revision_id != revision.id or takeoff_id != takeoff.id:
            raise AssertionError("Unexpected takeoff lookup")
        return takeoff

    async def fake_resolve_estimate_catalog_refs(
        revision_value: Any,
        takeoff_value: Any,
        request_refs: Any,
        pricing_effective_date: date,
        db: AsyncSession,
    ) -> list[Any]:
        _ = (revision_value, takeoff_value, db)
        pricing_effective_dates.append(pricing_effective_date)
        return [
            revisions_module._NormalizedEstimateCatalogRef(
                ref_type=request_refs[0].ref_type,
                selection_id=request_refs[0].selection_id,
                selection_key=request_refs[0].selection_key,
                selection_checksum_sha256=request_refs[0].selection_checksum_sha256,
                description=request_refs[0].description,
                line_key=request_refs[0].line_key,
                quantity_item_id=request_refs[0].quantity_item_id,
                formula_inputs=request_refs[0].formula_inputs,
            ),
            revisions_module._NormalizedEstimateCatalogRef(
                ref_type=request_refs[1].ref_type,
                selection_id=request_refs[1].selection_id,
                selection_key=request_refs[1].selection_key,
                selection_checksum_sha256=request_refs[1].selection_checksum_sha256,
                description=request_refs[1].description,
                line_key=request_refs[1].line_key,
                quantity_item_id=request_refs[1].quantity_item_id,
                formula_inputs=request_refs[1].formula_inputs,
            ),
        ]

    async def fake_replay_idempotency_response(
        db: AsyncSession,
        *,
        key: str,
        fingerprint: str,
    ) -> JSONResponse | None:
        _ = db
        record = idempotency_state.get(key)
        if record is None or record["fingerprint"] != fingerprint or not record["completed"]:
            return None
        return JSONResponse(
            status_code=record["status_code"],
            content=record["response_body"],
        )

    async def fake_claim_idempotency_response(
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
            idempotency_state[key] = {"fingerprint": fingerprint, "completed": False}
        return IdempotencyReservation(record_id=uuid4())

    async def fake_complete_idempotency_response(
        db: AsyncSession,
        reservation: IdempotencyReservation | None,
        *,
        status_code: int,
        response_body: dict[str, Any] | None,
    ) -> JSONResponse | None:
        _ = (db, reservation)
        assert reservation is not None
        idempotency_state["estimate-1"].update(
            completed=True,
            status_code=status_code,
            response_body=response_body,
        )
        return JSONResponse(status_code=status_code, content=response_body)

    def fake_prepare_job_enqueue_intent(job: Any) -> None:
        _ = job

    async def fake_publish_job_enqueue_intent(
        job_id: UUID,
        *,
        publisher: Any = None,
        suppress_exceptions: bool = False,
    ) -> None:
        _ = suppress_exceptions
        assert publisher is revisions_module.enqueue_estimate_job
        publisher(job_id)

    def fake_enqueue_estimate_job(job_id: UUID) -> None:
        enqueued_job_ids.append(job_id)

    class _AppClock(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> _AppClock:
            app_now = job_created_at
            if tz is None:
                return cls.fromtimestamp(app_now.timestamp(), UTC)
            return cls.fromtimestamp(app_now.astimezone(tz).timestamp(), tz)

    monkeypatch.setattr(revisions_module, "_get_active_revision", fake_get_active_revision)
    monkeypatch.setattr(
        revisions_module,
        "_get_revision_quantity_takeoff_or_404",
        fake_get_revision_quantity_takeoff_or_404,
    )
    monkeypatch.setattr(
        revisions_module,
        "_resolve_estimate_catalog_refs",
        fake_resolve_estimate_catalog_refs,
    )
    monkeypatch.setattr(
        revisions_module,
        "replay_idempotency_response",
        fake_replay_idempotency_response,
    )
    monkeypatch.setattr(
        revisions_module,
        "claim_idempotency_response",
        fake_claim_idempotency_response,
    )
    monkeypatch.setattr(
        revisions_module,
        "complete_idempotency_response",
        fake_complete_idempotency_response,
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
    monkeypatch.setattr(revisions_module, "enqueue_estimate_job", fake_enqueue_estimate_job)
    monkeypatch.setattr(revisions_module, "datetime", _AppClock)
    monkeypatch.setattr(
        revisions_module,
        "_resolve_estimate_job_model_classes",
        lambda: (_EstimateJobInputRecord, _EstimateJobInputCatalogRefRecord),
    )
    monkeypatch.setattr(
        revisions_module,
        "_build_mapped_instance",
        lambda _model_class, values: type("StubRecord", (), values.copy())(),
    )

    response = client.post(
        f"/v1/revisions/{revision.id}/quantity-takeoffs/{takeoff.id}/estimate-versions",
        headers={"Idempotency-Key": "estimate-1"},
        json=request_body,
    )

    assert response.status_code == 202
    first_body = response.json()
    assert first_body["job_type"] == "estimate"
    assert first_body["base_revision_id"] == str(revision.id)
    assert first_body["parent_job_id"] == str(takeoff.source_job_id)
    assert pricing_effective_dates == [job_created_at.date()]
    assert len(session.added) == 4
    estimate_job = session.added[0]
    assert estimate_job.created_at == job_created_at
    estimate_input = session.added[1]
    assert estimate_input.estimate_job_id == estimate_job.id
    assert estimate_input.source_job_type == JobType.ESTIMATE.value
    assert estimate_input.currency == "GBP"
    assert estimate_input.pricing_mode == "derived_from_job_created_at_utc"
    assert estimate_input.pricing_effective_date == estimate_job.created_at.date()
    assert estimate_input.assumptions_json == {"crew": "default"}
    first_ref = session.added[2]
    second_ref = session.added[3]
    assert first_ref.ref_order == 1
    assert first_ref.estimate_job_id == estimate_job.id
    assert first_ref.rate_catalog_entry_id == UUID(request_body["catalog_refs"][0]["selection_id"])
    assert first_ref.catalog_checksum_sha256 == "a" * 64
    assert first_ref.selection_context_json["worker_mapping_version"] == "estimate-line-v1"
    assert first_ref.selection_context_json["quantity_item_id"] == str(quantity_item_id)
    assert second_ref.ref_order == 2
    assert second_ref.formula_definition_id == UUID(request_body["catalog_refs"][1]["selection_id"])
    assert second_ref.catalog_checksum_sha256 == "b" * 64
    assert second_ref.selection_context_json["formula_inputs"] == {"waste_factor": "0.10"}
    assert enqueued_job_ids == [UUID(first_body["id"])]

    replay_response = client.post(
        f"/v1/revisions/{revision.id}/quantity-takeoffs/{takeoff.id}/estimate-versions",
        headers={"Idempotency-Key": "estimate-1"},
        json=request_body,
    )

    assert replay_response.status_code == 202
    assert replay_response.json() == first_body
    assert len(session.added) == 4


def test_create_revision_estimate_version_returns_job_without_idempotency_key(
    monkeypatch: Any,
) -> None:
    revision = _build_revision()
    takeoff = _build_takeoff(revision)
    quantity_item_id = uuid4()
    request_body = _build_estimate_request_body(quantity_item_id=quantity_item_id)
    session = _AsyncSessionStub(refresh_created_at=datetime(2026, 5, 20, 9, 30, tzinfo=UTC))
    app = _build_app(session)
    client = TestClient(app)
    enqueued_job_ids: list[UUID] = []
    complete_calls = 0

    class _EstimateJobInputRecord:
        pass

    class _EstimateJobInputCatalogRefRecord:
        pass

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (db, for_update)
        return revision if revision_id == revision.id else None

    async def fake_get_revision_quantity_takeoff_or_404(
        revision_id: UUID,
        takeoff_id: UUID,
        db: AsyncSession,
    ) -> SimpleNamespace:
        _ = db
        if revision_id != revision.id or takeoff_id != takeoff.id:
            raise AssertionError("Unexpected takeoff lookup")
        return takeoff

    async def fake_resolve_estimate_catalog_refs(
        revision_value: Any,
        takeoff_value: Any,
        request_refs: Any,
        pricing_effective_date: date,
        db: AsyncSession,
    ) -> list[Any]:
        _ = (revision_value, takeoff_value, pricing_effective_date, db)
        return [
            revisions_module._NormalizedEstimateCatalogRef(
                ref_type=request_refs[0].ref_type,
                selection_id=request_refs[0].selection_id,
                selection_key=request_refs[0].selection_key,
                selection_checksum_sha256=request_refs[0].selection_checksum_sha256,
                description=request_refs[0].description,
                line_key=request_refs[0].line_key,
                quantity_item_id=request_refs[0].quantity_item_id,
                formula_inputs=request_refs[0].formula_inputs,
            ),
            revisions_module._NormalizedEstimateCatalogRef(
                ref_type=request_refs[1].ref_type,
                selection_id=request_refs[1].selection_id,
                selection_key=request_refs[1].selection_key,
                selection_checksum_sha256=request_refs[1].selection_checksum_sha256,
                description=request_refs[1].description,
                line_key=request_refs[1].line_key,
                quantity_item_id=request_refs[1].quantity_item_id,
                formula_inputs=request_refs[1].formula_inputs,
            ),
        ]

    async def fake_complete_idempotency_response(
        db: AsyncSession,
        reservation: IdempotencyReservation | None,
        *,
        status_code: int,
        response_body: dict[str, Any] | None,
    ) -> JSONResponse | None:
        _ = (db, reservation, response_body)
        nonlocal complete_calls
        complete_calls += 1
        return JSONResponse(status_code=status_code, content=None)

    def fake_prepare_job_enqueue_intent(job: Any) -> None:
        _ = job

    async def fake_publish_job_enqueue_intent(
        job_id: UUID,
        *,
        publisher: Any = None,
        suppress_exceptions: bool = False,
    ) -> None:
        _ = suppress_exceptions
        assert publisher is revisions_module.enqueue_estimate_job
        publisher(job_id)

    def fake_enqueue_estimate_job(job_id: UUID) -> None:
        enqueued_job_ids.append(job_id)

    monkeypatch.setattr(revisions_module, "_get_active_revision", fake_get_active_revision)
    monkeypatch.setattr(
        revisions_module,
        "_get_revision_quantity_takeoff_or_404",
        fake_get_revision_quantity_takeoff_or_404,
    )
    monkeypatch.setattr(
        revisions_module,
        "_resolve_estimate_catalog_refs",
        fake_resolve_estimate_catalog_refs,
    )
    monkeypatch.setattr(
        revisions_module,
        "complete_idempotency_response",
        fake_complete_idempotency_response,
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
    monkeypatch.setattr(revisions_module, "enqueue_estimate_job", fake_enqueue_estimate_job)
    monkeypatch.setattr(
        revisions_module,
        "_resolve_estimate_job_model_classes",
        lambda: (_EstimateJobInputRecord, _EstimateJobInputCatalogRefRecord),
    )
    monkeypatch.setattr(
        revisions_module,
        "_build_mapped_instance",
        lambda _model_class, values: type("StubRecord", (), values.copy())(),
    )

    response = client.post(
        f"/v1/revisions/{revision.id}/quantity-takeoffs/{takeoff.id}/estimate-versions",
        json=request_body,
    )

    assert response.status_code == 202
    assert response.json()["job_type"] == "estimate"
    assert response.json()["base_revision_id"] == str(revision.id)
    assert response.json()["parent_job_id"] == str(takeoff.source_job_id)
    assert len(session.added) == 4
    assert session.commits == 1
    assert enqueued_job_ids == [UUID(response.json()["id"])]
    assert complete_calls == 0


def test_create_revision_estimate_version_rejects_idempotency_reuse_with_different_payload(
    monkeypatch: Any,
) -> None:
    revision = _build_revision()
    takeoff = _build_takeoff(revision)
    quantity_item_id = uuid4()
    request_body = _build_estimate_request_body(quantity_item_id=quantity_item_id)
    conflicting_request_body = _build_estimate_request_body(quantity_item_id=quantity_item_id)
    conflicting_request_body["assumptions"] = {"crew": "alternate"}
    session = _AsyncSessionStub()
    app = _build_app(session)
    client = TestClient(app)
    idempotency_state: dict[str, dict[str, Any]] = {}
    enqueued_job_ids: list[UUID] = []

    class _EstimateJobInputRecord:
        pass

    class _EstimateJobInputCatalogRefRecord:
        pass

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (db, for_update)
        return revision if revision_id == revision.id else None

    async def fake_get_revision_quantity_takeoff_or_404(
        revision_id: UUID,
        takeoff_id: UUID,
        db: AsyncSession,
    ) -> SimpleNamespace:
        _ = db
        if revision_id != revision.id or takeoff_id != takeoff.id:
            raise AssertionError("Unexpected takeoff lookup")
        return takeoff

    async def fake_resolve_estimate_catalog_refs(
        revision_value: Any,
        takeoff_value: Any,
        request_refs: Any,
        pricing_effective_date: date,
        db: AsyncSession,
    ) -> list[Any]:
        _ = (revision_value, takeoff_value, pricing_effective_date, db)
        return [
            revisions_module._NormalizedEstimateCatalogRef(
                ref_type=request_refs[0].ref_type,
                selection_id=request_refs[0].selection_id,
                selection_key=request_refs[0].selection_key,
                selection_checksum_sha256=request_refs[0].selection_checksum_sha256,
                description=request_refs[0].description,
                line_key=request_refs[0].line_key,
                quantity_item_id=request_refs[0].quantity_item_id,
                formula_inputs=request_refs[0].formula_inputs,
            ),
            revisions_module._NormalizedEstimateCatalogRef(
                ref_type=request_refs[1].ref_type,
                selection_id=request_refs[1].selection_id,
                selection_key=request_refs[1].selection_key,
                selection_checksum_sha256=request_refs[1].selection_checksum_sha256,
                description=request_refs[1].description,
                line_key=request_refs[1].line_key,
                quantity_item_id=request_refs[1].quantity_item_id,
                formula_inputs=request_refs[1].formula_inputs,
            ),
        ]

    async def fake_replay_idempotency_response(
        db: AsyncSession,
        *,
        key: str,
        fingerprint: str,
    ) -> JSONResponse | None:
        _ = db
        record = idempotency_state.get(key)
        if record is None:
            return None
        if record["fingerprint"] != fingerprint:
            return JSONResponse(
                status_code=409,
                content={
                    "detail": {
                        "error": {
                            "code": "IDEMPOTENCY_CONFLICT",
                            "message": "Idempotency-Key already used with different payload.",
                            "details": None,
                        }
                    }
                },
            )
        if not record["completed"]:
            return None
        return JSONResponse(
            status_code=record["status_code"],
            content=record["response_body"],
        )

    async def fake_claim_idempotency_response(
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
            idempotency_state[key] = {"fingerprint": fingerprint, "completed": False}
        return IdempotencyReservation(record_id=uuid4())

    async def fake_complete_idempotency_response(
        db: AsyncSession,
        reservation: IdempotencyReservation | None,
        *,
        status_code: int,
        response_body: dict[str, Any] | None,
    ) -> JSONResponse | None:
        _ = (db, reservation)
        assert reservation is not None
        idempotency_state["estimate-1"].update(
            completed=True,
            status_code=status_code,
            response_body=response_body,
        )
        return JSONResponse(status_code=status_code, content=response_body)

    def fake_prepare_job_enqueue_intent(job: Any) -> None:
        _ = job

    async def fake_publish_job_enqueue_intent(
        job_id: UUID,
        *,
        publisher: Any = None,
        suppress_exceptions: bool = False,
    ) -> None:
        _ = suppress_exceptions
        assert publisher is revisions_module.enqueue_estimate_job
        publisher(job_id)

    def fake_enqueue_estimate_job(job_id: UUID) -> None:
        enqueued_job_ids.append(job_id)

    monkeypatch.setattr(revisions_module, "_get_active_revision", fake_get_active_revision)
    monkeypatch.setattr(
        revisions_module,
        "_get_revision_quantity_takeoff_or_404",
        fake_get_revision_quantity_takeoff_or_404,
    )
    monkeypatch.setattr(
        revisions_module,
        "_resolve_estimate_catalog_refs",
        fake_resolve_estimate_catalog_refs,
    )
    monkeypatch.setattr(
        revisions_module,
        "replay_idempotency_response",
        fake_replay_idempotency_response,
    )
    monkeypatch.setattr(
        revisions_module,
        "claim_idempotency_response",
        fake_claim_idempotency_response,
    )
    monkeypatch.setattr(
        revisions_module,
        "complete_idempotency_response",
        fake_complete_idempotency_response,
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
    monkeypatch.setattr(revisions_module, "enqueue_estimate_job", fake_enqueue_estimate_job)
    monkeypatch.setattr(
        revisions_module,
        "_resolve_estimate_job_model_classes",
        lambda: (_EstimateJobInputRecord, _EstimateJobInputCatalogRefRecord),
    )
    monkeypatch.setattr(
        revisions_module,
        "_build_mapped_instance",
        lambda _model_class, values: type("StubRecord", (), values.copy())(),
    )

    response = client.post(
        f"/v1/revisions/{revision.id}/quantity-takeoffs/{takeoff.id}/estimate-versions",
        headers={"Idempotency-Key": "estimate-1"},
        json=request_body,
    )

    assert response.status_code == 202
    first_body = response.json()
    assert len(session.added) == 4
    assert session.commits == 1
    assert enqueued_job_ids == [UUID(first_body["id"])]

    conflict_response = client.post(
        f"/v1/revisions/{revision.id}/quantity-takeoffs/{takeoff.id}/estimate-versions",
        headers={"Idempotency-Key": "estimate-1"},
        json=conflicting_request_body,
    )

    assert conflict_response.status_code == 409
    assert _response_error_code(conflict_response) == "IDEMPOTENCY_CONFLICT"
    assert len(session.added) == 4
    assert session.commits == 1
    assert enqueued_job_ids == [UUID(first_body["id"])]


def test_create_revision_estimate_version_rejects_non_gbp(monkeypatch: Any) -> None:
    revision = _build_revision()
    takeoff = _build_takeoff(revision)
    session = _AsyncSessionStub()
    app = _build_app(session)
    client = TestClient(app)
    publish_calls: list[UUID] = []

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (db, for_update)
        return revision if revision_id == revision.id else None

    async def fake_get_revision_quantity_takeoff_or_404(
        revision_id: UUID,
        takeoff_id: UUID,
        db: AsyncSession,
    ) -> SimpleNamespace:
        _ = db
        if revision_id != revision.id or takeoff_id != takeoff.id:
            raise AssertionError("Unexpected takeoff lookup")
        return takeoff

    async def fake_publish_job_enqueue_intent(
        job_id: UUID,
        *,
        publisher: Any = None,
        suppress_exceptions: bool = False,
    ) -> None:
        _ = (publisher, suppress_exceptions)
        publish_calls.append(job_id)

    monkeypatch.setattr(revisions_module, "_get_active_revision", fake_get_active_revision)
    monkeypatch.setattr(
        revisions_module,
        "_get_revision_quantity_takeoff_or_404",
        fake_get_revision_quantity_takeoff_or_404,
    )
    monkeypatch.setattr(
        revisions_module,
        "publish_job_enqueue_intent",
        fake_publish_job_enqueue_intent,
    )

    response = client.post(
        f"/v1/revisions/{revision.id}/quantity-takeoffs/{takeoff.id}/estimate-versions",
        json=_build_estimate_request_body(quantity_item_id=uuid4(), currency="USD"),
    )

    assert response.status_code == 400
    assert _response_error_code(response) == "INPUT_INVALID"
    assert session.added == []
    assert session.commits == 0
    assert publish_calls == []


def test_create_revision_estimate_version_rejects_non_allowed_quantity_gate_before_enqueue(
    monkeypatch: Any,
) -> None:
    revision = _build_revision()
    takeoff = _build_takeoff(revision, quantity_gate=QuantityGate.REVIEW_GATED.value)
    session = _AsyncSessionStub()
    app = _build_app(session)
    client = TestClient(app)
    publish_calls: list[UUID] = []
    enqueue_calls: list[UUID] = []

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (db, for_update)
        return revision if revision_id == revision.id else None

    async def fake_get_revision_quantity_takeoff_or_404(
        revision_id: UUID,
        takeoff_id: UUID,
        db: AsyncSession,
    ) -> SimpleNamespace:
        _ = db
        if revision_id != revision.id or takeoff_id != takeoff.id:
            raise AssertionError("Unexpected takeoff lookup")
        return takeoff

    async def fake_publish_job_enqueue_intent(
        job_id: UUID,
        *,
        publisher: Any = None,
        suppress_exceptions: bool = False,
    ) -> None:
        _ = (publisher, suppress_exceptions)
        publish_calls.append(job_id)

    def fake_enqueue_estimate_job(job_id: UUID) -> None:
        enqueue_calls.append(job_id)

    monkeypatch.setattr(revisions_module, "_get_active_revision", fake_get_active_revision)
    monkeypatch.setattr(
        revisions_module,
        "_get_revision_quantity_takeoff_or_404",
        fake_get_revision_quantity_takeoff_or_404,
    )
    monkeypatch.setattr(
        revisions_module,
        "publish_job_enqueue_intent",
        fake_publish_job_enqueue_intent,
    )
    monkeypatch.setattr(revisions_module, "enqueue_estimate_job", fake_enqueue_estimate_job)

    response = client.post(
        f"/v1/revisions/{revision.id}/quantity-takeoffs/{takeoff.id}/estimate-versions",
        json=_build_estimate_request_body(quantity_item_id=uuid4()),
    )

    assert response.status_code == 400
    assert _response_error_code(response) == "INPUT_INVALID"
    assert session.added == []
    assert session.commits == 0
    assert publish_calls == []
    assert enqueue_calls == []


def test_create_revision_estimate_version_rejects_untrusted_takeoff(monkeypatch: Any) -> None:
    revision = _build_revision()
    takeoff = _build_takeoff(revision, trusted_totals=False)
    session = _AsyncSessionStub()
    app = _build_app(session)
    client = TestClient(app)
    publish_calls: list[UUID] = []

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (db, for_update)
        return revision if revision_id == revision.id else None

    async def fake_get_revision_quantity_takeoff_or_404(
        revision_id: UUID,
        takeoff_id: UUID,
        db: AsyncSession,
    ) -> SimpleNamespace:
        _ = db
        if revision_id != revision.id or takeoff_id != takeoff.id:
            raise AssertionError("Unexpected takeoff lookup")
        return takeoff

    async def fake_publish_job_enqueue_intent(
        job_id: UUID,
        *,
        publisher: Any = None,
        suppress_exceptions: bool = False,
    ) -> None:
        _ = (publisher, suppress_exceptions)
        publish_calls.append(job_id)

    monkeypatch.setattr(revisions_module, "_get_active_revision", fake_get_active_revision)
    monkeypatch.setattr(
        revisions_module,
        "_get_revision_quantity_takeoff_or_404",
        fake_get_revision_quantity_takeoff_or_404,
    )
    monkeypatch.setattr(
        revisions_module,
        "publish_job_enqueue_intent",
        fake_publish_job_enqueue_intent,
    )

    response = client.post(
        f"/v1/revisions/{revision.id}/quantity-takeoffs/{takeoff.id}/estimate-versions",
        json=_build_estimate_request_body(quantity_item_id=uuid4()),
    )

    assert response.status_code == 400
    assert _response_error_code(response) == "INPUT_INVALID"
    assert session.added == []
    assert session.commits == 0
    assert publish_calls == []


def test_create_revision_estimate_version_rejects_catalog_validation_before_idempotency_claim(
    monkeypatch: Any,
) -> None:
    revision = _build_revision()
    takeoff = _build_takeoff(revision)
    quantity_item_id = uuid4()
    session = _AsyncSessionStub()
    app = _build_app(session)
    client = TestClient(app)
    idempotency_claimed = False
    idempotency_completed = False
    publish_calls: list[UUID] = []

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (db, for_update)
        return revision if revision_id == revision.id else None

    async def fake_get_revision_quantity_takeoff_or_404(
        revision_id: UUID,
        takeoff_id: UUID,
        db: AsyncSession,
    ) -> SimpleNamespace:
        _ = db
        if revision_id != revision.id or takeoff_id != takeoff.id:
            raise AssertionError("Unexpected takeoff lookup")
        return takeoff

    async def fake_resolve_estimate_catalog_refs(
        revision_value: Any,
        takeoff_value: Any,
        request_refs: Any,
        pricing_effective_date: date,
        db: AsyncSession,
    ) -> list[Any]:
        _ = (revision_value, takeoff_value, request_refs, pricing_effective_date, db)
        revisions_module._raise_estimate_input_invalid(
            "Estimate refs must use the current catalog selection checksum.",
            details={"selection_checksum_sha256": "mismatch"},
        )
        raise AssertionError("unreachable")

    async def fake_claim_idempotency_response(
        db: AsyncSession,
        *,
        key: str,
        fingerprint: str,
        method: str,
        path: str,
    ) -> IdempotencyReservation:
        _ = (db, key, fingerprint, method, path)
        nonlocal idempotency_claimed
        idempotency_claimed = True
        return IdempotencyReservation(record_id=uuid4())

    async def fake_replay_idempotency_response(
        db: AsyncSession,
        *,
        key: str,
        fingerprint: str,
    ) -> JSONResponse | None:
        _ = (db, key, fingerprint)
        return None

    async def fake_complete_idempotency_response(
        db: AsyncSession,
        reservation: IdempotencyReservation | None,
        *,
        status_code: int,
        response_body: dict[str, Any] | None,
    ) -> JSONResponse | None:
        _ = (db, reservation, status_code, response_body)
        nonlocal idempotency_completed
        idempotency_completed = True
        return JSONResponse(status_code=status_code, content=response_body)

    async def fake_publish_job_enqueue_intent(
        job_id: UUID,
        *,
        publisher: Any = None,
        suppress_exceptions: bool = False,
    ) -> None:
        _ = (publisher, suppress_exceptions)
        publish_calls.append(job_id)

    monkeypatch.setattr(revisions_module, "_get_active_revision", fake_get_active_revision)
    monkeypatch.setattr(
        revisions_module,
        "_get_revision_quantity_takeoff_or_404",
        fake_get_revision_quantity_takeoff_or_404,
    )
    monkeypatch.setattr(
        revisions_module,
        "_resolve_estimate_catalog_refs",
        fake_resolve_estimate_catalog_refs,
    )
    monkeypatch.setattr(
        revisions_module,
        "replay_idempotency_response",
        fake_replay_idempotency_response,
    )
    monkeypatch.setattr(
        revisions_module,
        "claim_idempotency_response",
        fake_claim_idempotency_response,
    )
    monkeypatch.setattr(
        revisions_module,
        "complete_idempotency_response",
        fake_complete_idempotency_response,
    )
    monkeypatch.setattr(
        revisions_module,
        "publish_job_enqueue_intent",
        fake_publish_job_enqueue_intent,
    )

    response = client.post(
        f"/v1/revisions/{revision.id}/quantity-takeoffs/{takeoff.id}/estimate-versions",
        headers={"Idempotency-Key": "estimate-1"},
        json=_build_estimate_request_body(quantity_item_id=quantity_item_id),
    )

    assert response.status_code == 400
    assert _response_error_code(response) == "INPUT_INVALID"
    assert idempotency_claimed is False
    assert session.refresh_calls == 0
    assert session.added == []
    assert session.commits == 0
    assert idempotency_completed is False
    assert publish_calls == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("quantity_gate", "trusted_totals"),
    [
        pytest.param(
            QuantityGate.REVIEW_GATED.value,
            False,
            id="review_gated",
        ),
        pytest.param(QuantityGate.ALLOWED.value, False, id="untrusted"),
    ],
)
@requires_database
async def test_create_revision_estimate_version_rejects_trust_gate_takeoff_before_insert(
    async_client: AsyncClient,
    monkeypatch: Any,
    quantity_gate: str,
    trusted_totals: bool,
) -> None:
    seed = await _seed_quantity_lineage(quantity_gate=quantity_gate, trusted_totals=trusted_totals)
    request_body = _build_estimate_request_body(quantity_item_id=seed.quantity_item_id)
    publish_calls: list[UUID] = []
    enqueue_calls: list[UUID] = []
    catalog_resolver_called = False

    async def fake_publish_job_enqueue_intent(
        job_id: UUID,
        *,
        publisher: Any = None,
        suppress_exceptions: bool = False,
    ) -> None:
        _ = (publisher, suppress_exceptions)
        publish_calls.append(job_id)

    def fake_enqueue_estimate_job(job_id: UUID) -> None:
        enqueue_calls.append(job_id)

    async def fake_resolve_estimate_catalog_refs(
        revision_value: Any,
        takeoff_value: Any,
        request_refs: Any,
        pricing_effective_date: date,
        db: AsyncSession,
    ) -> list[Any]:
        _ = (revision_value, takeoff_value, request_refs, pricing_effective_date, db)
        nonlocal catalog_resolver_called
        catalog_resolver_called = True
        return []

    monkeypatch.setattr(
        revisions_module,
        "publish_job_enqueue_intent",
        fake_publish_job_enqueue_intent,
    )
    monkeypatch.setattr(revisions_module, "enqueue_estimate_job", fake_enqueue_estimate_job)
    monkeypatch.setattr(
        revisions_module,
        "_resolve_estimate_catalog_refs",
        fake_resolve_estimate_catalog_refs,
    )

    response = await async_client.post(
        f"/v1/revisions/{seed.revision_id}/quantity-takeoffs/{seed.takeoff_id}/estimate-versions",
        json=request_body,
    )

    assert response.status_code == 400
    assert _response_error_code(response) == "INPUT_INVALID"
    assert catalog_resolver_called is False
    assert publish_calls == []
    assert enqueue_calls == []
    await _assert_no_estimate_persistence_for_project(seed)


@pytest.mark.anyio
@requires_database
async def test_create_revision_estimate_version_persists_mapped_input_fields_and_allows_global_refs(
    async_client: AsyncClient,
    monkeypatch: Any,
) -> None:
    seed = await _seed_quantity_lineage()
    request_body = _build_estimate_request_body(quantity_item_id=seed.quantity_item_id)
    request_body["pricing"]["effective_date"] = "2026-05-20"
    publish_calls: list[UUID] = []
    session_factory = session_module.AsyncSessionLocal
    assert session_factory is not None

    rate = _build_rate_catalog_entry(
        scope_type="global",
        project_id=None,
        rate_key="labour:install",
        checksum_sha256="a" * 64,
        effective_from=date(2026, 1, 1),
    )
    formula = _build_formula_definition(
        scope_type="global",
        project_id=None,
        formula_id="formula:waste",
        checksum_sha256="b" * 64,
    )

    async with session_factory() as db:
        db.add(rate)
        db.add(formula)
        await db.commit()

    request_body["catalog_refs"][0]["selection_id"] = str(rate.id)
    request_body["catalog_refs"][1]["selection_id"] = str(formula.id)

    async def fake_publish_job_enqueue_intent(
        job_id: UUID,
        *,
        publisher: Any = None,
        suppress_exceptions: bool = False,
    ) -> None:
        _ = (publisher, suppress_exceptions)
        publish_calls.append(job_id)

    monkeypatch.setattr(
        revisions_module,
        "publish_job_enqueue_intent",
        fake_publish_job_enqueue_intent,
    )

    response = await async_client.post(
        f"/v1/revisions/{seed.revision_id}/quantity-takeoffs/{seed.takeoff_id}/estimate-versions",
        json=request_body,
    )

    assert response.status_code == 202
    estimate_job_id = UUID(response.json()["id"])
    assert publish_calls == [estimate_job_id]

    async with session_factory() as db:
        estimate_input = await db.get(EstimateJobInput, estimate_job_id)
        assert estimate_input is not None
        assert estimate_input.estimate_job_id == estimate_job_id
        assert estimate_input.project_id == seed.project_id
        assert estimate_input.source_file_id == seed.file_id
        assert estimate_input.drawing_revision_id == seed.revision_id
        assert estimate_input.quantity_takeoff_id == seed.takeoff_id
        assert estimate_input.source_job_type == JobType.ESTIMATE.value
        assert estimate_input.currency == "GBP"
        assert estimate_input.pricing_mode == "explicit"
        assert estimate_input.pricing_effective_date == date(2026, 5, 20)
        assert estimate_input.assumptions_json == {"crew": "default"}

        ref_rows = (
            (
                await db.execute(
                    select(EstimateJobInputCatalogRef)
                    .where(EstimateJobInputCatalogRef.estimate_job_id == estimate_job_id)
                    .order_by(EstimateJobInputCatalogRef.ref_order.asc())
                )
            )
            .scalars()
            .all()
        )

    assert len(ref_rows) == 2
    assert ref_rows[0].ref_type == "rate"
    assert ref_rows[0].selection_key == "labour:install"
    assert ref_rows[0].rate_catalog_entry_id == rate.id
    assert ref_rows[0].material_catalog_entry_id is None
    assert ref_rows[0].formula_definition_id is None
    assert ref_rows[0].catalog_checksum_sha256 == "a" * 64
    assert ref_rows[0].selection_context_json["line_key"] == "line-rate-1"
    assert ref_rows[0].selection_context_json["quantity_item_id"] == str(seed.quantity_item_id)
    assert ref_rows[1].ref_type == "formula"
    assert ref_rows[1].selection_key == "formula:waste"
    assert ref_rows[1].rate_catalog_entry_id is None
    assert ref_rows[1].formula_definition_id == formula.id
    assert ref_rows[1].catalog_checksum_sha256 == "b" * 64
    assert ref_rows[1].selection_context_json["formula_inputs"] == {"waste_factor": "0.10"}


@pytest.mark.anyio
@pytest.mark.parametrize("invalid_ref_mode", ["stale", "superseded"])
@requires_database
async def test_create_revision_estimate_version_rejects_stale_or_superseded_rate_refs_before_insert(
    async_client: AsyncClient,
    monkeypatch: Any,
    invalid_ref_mode: str,
) -> None:
    seed = await _seed_quantity_lineage()
    request_body = _build_estimate_request_body(quantity_item_id=seed.quantity_item_id)
    request_body["pricing"]["effective_date"] = "2026-05-20"
    publish_calls: list[UUID] = []
    session_factory = session_module.AsyncSessionLocal
    assert session_factory is not None

    rate = _build_rate_catalog_entry(
        scope_type="project",
        project_id=seed.project_id,
        rate_key="labour:install",
        checksum_sha256="a" * 64,
        effective_from=date(2026, 1, 1),
        effective_to=(date(2026, 4, 1) if invalid_ref_mode == "stale" else date(2026, 5, 20)),
    )
    formula = _build_formula_definition(
        scope_type="project",
        project_id=seed.project_id,
        formula_id="formula:waste",
        checksum_sha256="b" * 64,
    )

    async with session_factory() as db:
        db.add(rate)
        db.add(formula)
        await db.commit()

        if invalid_ref_mode == "superseded":
            successor_rate = _build_rate_catalog_entry(
                scope_type="project",
                project_id=seed.project_id,
                rate_key="labour:install",
                checksum_sha256="c" * 64,
                effective_from=date(2026, 5, 21),
            )
            db.add(successor_rate)
            await db.commit()
            db.add(
                EstimationRateSupersession(
                    predecessor_rate_id=rate.id,
                    successor_rate_id=successor_rate.id,
                )
            )
            await db.commit()

    request_body["catalog_refs"][0]["selection_id"] = str(rate.id)
    request_body["catalog_refs"][1]["selection_id"] = str(formula.id)

    async def fake_publish_job_enqueue_intent(
        job_id: UUID,
        *,
        publisher: Any = None,
        suppress_exceptions: bool = False,
    ) -> None:
        _ = (publisher, suppress_exceptions)
        publish_calls.append(job_id)

    monkeypatch.setattr(
        revisions_module,
        "publish_job_enqueue_intent",
        fake_publish_job_enqueue_intent,
    )

    response = await async_client.post(
        f"/v1/revisions/{seed.revision_id}/quantity-takeoffs/{seed.takeoff_id}/estimate-versions",
        json=request_body,
    )

    assert response.status_code == 400
    assert _response_error_code(response) == "INPUT_INVALID"
    assert publish_calls == []

    async with session_factory() as db:
        estimate_jobs = (
            (
                await db.execute(
                    select(Job).where(
                        (Job.project_id == seed.project_id)
                        & (Job.job_type == JobType.ESTIMATE.value)
                    )
                )
            )
            .scalars()
            .all()
        )
        estimate_inputs = (
            (
                await db.execute(
                    select(EstimateJobInput).where(EstimateJobInput.project_id == seed.project_id)
                )
            )
            .scalars()
            .all()
        )
        estimate_refs = (
            (
                await db.execute(
                    select(EstimateJobInputCatalogRef).where(
                        EstimateJobInputCatalogRef.estimate_job_id.in_(
                            [job.id for job in estimate_jobs]
                        )
                    )
                )
            )
            .scalars()
            .all()
        )

    assert estimate_jobs == []
    assert estimate_inputs == []
    assert estimate_refs == []


@pytest.mark.anyio
@requires_database
async def test_create_revision_estimate_version_invalid_catalog_retry_reuses_idempotency_key(
    async_client: AsyncClient,
    monkeypatch: Any,
) -> None:
    seed = await _seed_quantity_lineage()
    request_body = _build_estimate_request_body(quantity_item_id=seed.quantity_item_id)
    request_body["pricing"]["effective_date"] = "2026-05-20"
    publish_calls: list[UUID] = []
    session_factory = session_module.AsyncSessionLocal
    assert session_factory is not None

    stale_rate = _build_rate_catalog_entry(
        scope_type="project",
        project_id=seed.project_id,
        rate_key="labour:install",
        checksum_sha256="a" * 64,
        effective_from=date(2026, 1, 1),
        effective_to=date(2026, 4, 1),
    )
    valid_rate = _build_rate_catalog_entry(
        scope_type="project",
        project_id=seed.project_id,
        rate_key="labour:install",
        checksum_sha256="c" * 64,
        effective_from=date(2026, 4, 1),
    )
    formula = _build_formula_definition(
        scope_type="project",
        project_id=seed.project_id,
        formula_id="formula:waste",
        checksum_sha256="b" * 64,
    )

    async with session_factory() as db:
        db.add(stale_rate)
        db.add(valid_rate)
        db.add(formula)
        await db.commit()

    request_body["catalog_refs"][0]["selection_id"] = str(stale_rate.id)
    request_body["catalog_refs"][1]["selection_id"] = str(formula.id)

    async def fake_publish_job_enqueue_intent(
        job_id: UUID,
        *,
        publisher: Any = None,
        suppress_exceptions: bool = False,
    ) -> None:
        _ = (publisher, suppress_exceptions)
        publish_calls.append(job_id)

    monkeypatch.setattr(
        revisions_module,
        "publish_job_enqueue_intent",
        fake_publish_job_enqueue_intent,
    )

    first_response = await async_client.post(
        f"/v1/revisions/{seed.revision_id}/quantity-takeoffs/{seed.takeoff_id}/estimate-versions",
        headers={"Idempotency-Key": "estimate-1"},
        json=request_body,
    )

    assert first_response.status_code == 400
    assert _response_error_code(first_response) == "INPUT_INVALID"
    assert publish_calls == []
    await _assert_no_estimate_persistence_for_project(seed)

    request_body["catalog_refs"][0]["selection_id"] = str(valid_rate.id)
    request_body["catalog_refs"][0]["selection_checksum_sha256"] = valid_rate.checksum_sha256

    second_response = await async_client.post(
        f"/v1/revisions/{seed.revision_id}/quantity-takeoffs/{seed.takeoff_id}/estimate-versions",
        headers={"Idempotency-Key": "estimate-1"},
        json=request_body,
    )

    assert second_response.status_code == 202
    assert publish_calls == [UUID(second_response.json()["id"])]


@pytest.mark.anyio
@requires_database
async def test_create_revision_estimate_version_rejects_cross_project_project_refs_before_insert(
    async_client: AsyncClient,
    monkeypatch: Any,
) -> None:
    seed = await _seed_quantity_lineage()
    request_body = _build_estimate_request_body(quantity_item_id=seed.quantity_item_id)
    request_body["pricing"]["effective_date"] = "2026-05-20"
    publish_calls: list[UUID] = []
    session_factory = session_module.AsyncSessionLocal
    assert session_factory is not None

    other_project_id = uuid4()
    rate = _build_rate_catalog_entry(
        scope_type="project",
        project_id=other_project_id,
        rate_key="labour:install",
        checksum_sha256="a" * 64,
        effective_from=date(2026, 1, 1),
    )
    formula = _build_formula_definition(
        scope_type="project",
        project_id=seed.project_id,
        formula_id="formula:waste",
        checksum_sha256="b" * 64,
    )

    async with session_factory() as db:
        db.add(Project(id=other_project_id, name="Other project"))
        await db.commit()
        db.add(rate)
        db.add(formula)
        await db.commit()

    request_body["catalog_refs"][0]["selection_id"] = str(rate.id)
    request_body["catalog_refs"][1]["selection_id"] = str(formula.id)

    async def fake_publish_job_enqueue_intent(
        job_id: UUID,
        *,
        publisher: Any = None,
        suppress_exceptions: bool = False,
    ) -> None:
        _ = (publisher, suppress_exceptions)
        publish_calls.append(job_id)

    monkeypatch.setattr(
        revisions_module,
        "publish_job_enqueue_intent",
        fake_publish_job_enqueue_intent,
    )

    response = await async_client.post(
        f"/v1/revisions/{seed.revision_id}/quantity-takeoffs/{seed.takeoff_id}/estimate-versions",
        json=request_body,
    )

    assert response.status_code == 400
    assert _response_error_code(response) == "INPUT_INVALID"
    assert publish_calls == []

    async with session_factory() as db:
        estimate_jobs = (
            (
                await db.execute(
                    select(Job).where(
                        (Job.project_id == seed.project_id)
                        & (Job.job_type == JobType.ESTIMATE.value)
                    )
                )
            )
            .scalars()
            .all()
        )
        estimate_inputs = (
            (
                await db.execute(
                    select(EstimateJobInput).where(EstimateJobInput.project_id == seed.project_id)
                )
            )
            .scalars()
            .all()
        )

    assert estimate_jobs == []
    assert estimate_inputs == []


@pytest.mark.anyio
@requires_database
async def test_create_revision_estimate_version_rejects_non_executable_quantity_items_before_insert(
    async_client: AsyncClient,
    monkeypatch: Any,
) -> None:
    seed = await _seed_quantity_lineage()
    request_body = _build_estimate_request_body(quantity_item_id=seed.quantity_item_id)
    request_body["pricing"]["effective_date"] = "2026-05-20"
    publish_calls: list[UUID] = []
    session_factory = session_module.AsyncSessionLocal
    assert session_factory is not None

    rate = _build_rate_catalog_entry(
        scope_type="project",
        project_id=seed.project_id,
        rate_key="labour:install",
        checksum_sha256="a" * 64,
        effective_from=date(2026, 1, 1),
    )
    formula = _build_formula_definition(
        scope_type="project",
        project_id=seed.project_id,
        formula_id="formula:waste",
        checksum_sha256="b" * 64,
    )

    async with session_factory() as db:
        db.add(rate)
        db.add(formula)
        await db.commit()

        revision = await db.get(DrawingRevision, seed.revision_id)
        assert revision is not None

        revision_entity = RevisionEntity(
            project_id=seed.project_id,
            source_file_id=seed.file_id,
            extraction_profile_id=revision.extraction_profile_id,
            source_job_id=revision.source_job_id,
            drawing_revision_id=seed.revision_id,
            adapter_run_output_id=revision.adapter_run_output_id,
            canonical_entity_schema_version=revision.canonical_entity_schema_version,
            sequence_index=0,
            entity_id="entity-1",
            entity_type="polygon",
            entity_schema_version="1.0.0",
            confidence_score=1.0,
            confidence_json={},
            geometry_json={},
            properties_json={},
            provenance_json={},
            canonical_entity_json=None,
            source_identity="entity-1",
            source_hash="1" * 64,
        )
        db.add(revision_entity)
        await db.commit()

        quantity_item = QuantityItem(
            quantity_takeoff_id=seed.takeoff_id,
            project_id=seed.project_id,
            drawing_revision_id=seed.revision_id,
            item_kind=QuantityItemKind.EXCLUSION.value,
            quantity_type="area",
            value=None,
            unit="sq_ft",
            review_state="approved",
            validation_status="valid",
            quantity_gate=QuantityGate.ALLOWED.value,
            source_entity_id="entity-1",
            excluded_source_entity_ids_json=[],
        )
        db.add(quantity_item)
        await db.commit()

    request_body["catalog_refs"][0]["quantity_item_id"] = str(quantity_item.id)
    request_body["catalog_refs"][0]["selection_id"] = str(rate.id)
    request_body["catalog_refs"][1]["selection_id"] = str(formula.id)

    async def fake_publish_job_enqueue_intent(
        job_id: UUID,
        *,
        publisher: Any = None,
        suppress_exceptions: bool = False,
    ) -> None:
        _ = (publisher, suppress_exceptions)
        publish_calls.append(job_id)

    monkeypatch.setattr(
        revisions_module,
        "publish_job_enqueue_intent",
        fake_publish_job_enqueue_intent,
    )

    response = await async_client.post(
        f"/v1/revisions/{seed.revision_id}/quantity-takeoffs/{seed.takeoff_id}/estimate-versions",
        json=request_body,
    )

    assert response.status_code == 400
    assert _response_error_code(response) == "INPUT_INVALID"
    assert publish_calls == []

    async with session_factory() as db:
        estimate_jobs = (
            (
                await db.execute(
                    select(Job).where(
                        (Job.project_id == seed.project_id)
                        & (Job.job_type == JobType.ESTIMATE.value)
                    )
                )
            )
            .scalars()
            .all()
        )
        estimate_inputs = (
            (
                await db.execute(
                    select(EstimateJobInput).where(EstimateJobInput.project_id == seed.project_id)
                )
            )
            .scalars()
            .all()
        )

    assert estimate_jobs == []
    assert estimate_inputs == []


@pytest.mark.anyio
@requires_database
async def test_list_and_get_revision_quantity_takeoffs(async_client: AsyncClient) -> None:
    seed = await _seed_quantity_lineage()
    session_factory = session_module.AsyncSessionLocal
    assert session_factory is not None

    second_takeoff_id = uuid4()
    second_quantity_job_id = uuid4()

    async with session_factory() as db:
        second_quantity_job = Job(
            id=second_quantity_job_id,
            project_id=seed.project_id,
            file_id=seed.file_id,
            extraction_profile_id=None,
            base_revision_id=seed.revision_id,
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
        second_takeoff = QuantityTakeoff(
            id=second_takeoff_id,
            project_id=seed.project_id,
            source_file_id=seed.file_id,
            drawing_revision_id=seed.revision_id,
            source_job_id=second_quantity_job_id,
            source_job_type=JobType.QUANTITY_TAKEOFF.value,
            review_state="provisional",
            validation_status="valid_with_warnings",
            quantity_gate="allowed_provisional",
            trusted_totals=False,
            created_at=datetime(2100, 1, 1, tzinfo=UTC),
        )
        db.add(second_quantity_job)
        await db.commit()
        db.add(second_takeoff)
        await db.commit()

    list_response = await async_client.get(
        f"/v1/revisions/{seed.revision_id}/quantity-takeoffs?limit=1"
    )

    assert list_response.status_code == 200
    list_body = list_response.json()
    assert [item["id"] for item in list_body["items"]] == [str(seed.takeoff_id)]
    assert list_body["next_cursor"] is not None

    read_response = await async_client.get(
        f"/v1/revisions/{seed.revision_id}/quantity-takeoffs/{seed.takeoff_id}"
    )

    assert read_response.status_code == 200
    assert read_response.json()["id"] == str(seed.takeoff_id)


@pytest.mark.anyio
@requires_database
async def test_list_revision_quantity_takeoff_items(async_client: AsyncClient) -> None:
    seed = await _seed_quantity_lineage()
    session_factory = session_module.AsyncSessionLocal
    assert session_factory is not None

    second_item_id = uuid4()

    async with session_factory() as db:
        second_item = QuantityItem(
            id=second_item_id,
            quantity_takeoff_id=seed.takeoff_id,
            project_id=seed.project_id,
            drawing_revision_id=seed.revision_id,
            item_kind=QuantityItemKind.AGGREGATE.value,
            quantity_type="area",
            value=20.0,
            unit="sq_ft",
            review_state="approved",
            validation_status="valid",
            quantity_gate=QuantityGate.ALLOWED.value,
            source_entity_id=None,
            excluded_source_entity_ids_json=[],
            created_at=datetime(2100, 1, 1, tzinfo=UTC),
        )
        db.add(second_item)
        await db.commit()

    response = await async_client.get(
        f"/v1/revisions/{seed.revision_id}/quantity-takeoffs/{seed.takeoff_id}/items?limit=1"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["items"][0]["id"] == str(seed.quantity_item_id)
    assert body["items"][0]["excluded_source_entity_ids"] == []
    assert body["next_cursor"] is not None


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
    assert _response_error_code(response) == "INPUT_INVALID"
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


@pytest.mark.anyio
@requires_database
async def test_list_revision_quantity_takeoffs_rejects_invalid_cursor(
    async_client: AsyncClient,
) -> None:
    seed = await _seed_quantity_lineage()

    response = await async_client.get(
        f"/v1/revisions/{seed.revision_id}/quantity-takeoffs?cursor=not-base64"
    )

    assert response.status_code == 400
    assert _response_error_code(response) == "INVALID_CURSOR"


@pytest.mark.anyio
@requires_database
async def test_list_revision_quantity_takeoff_items_rejects_invalid_cursor(
    async_client: AsyncClient,
) -> None:
    seed = await _seed_quantity_lineage()

    response = await async_client.get(
        f"/v1/revisions/{seed.revision_id}/quantity-takeoffs/{seed.takeoff_id}/items?cursor=not-base64"
    )

    assert response.status_code == 400
    assert _response_error_code(response) == "INVALID_CURSOR"
