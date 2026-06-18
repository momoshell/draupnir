"""API tests for revision estimate read routes."""

from __future__ import annotations

from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.pagination import decode_cursor_payload
from app.api.v1.revision_routes import estimates as estimates_routes_module
from app.api.v1.revisions import revisions_router
from app.db import session as session_module
from app.db.session import get_db
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.estimate_version import EstimateItem, EstimateSnapshotEntry, EstimateVersion
from app.models.extraction_profile import ExtractionProfile
from app.models.file import File
from app.models.job import Job, JobType
from app.models.project import Project
from app.models.quantity_takeoff import (
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
        self.commits = 0
        self.statements: list[Any] = []

    async def execute(self, statement: Any) -> _ScalarResultStub:
        self.statements.append(statement)
        if not self._execute_results:
            raise AssertionError("Unexpected execute call")
        return _ScalarResultStub(self._execute_results.popleft())

    async def commit(self) -> None:
        self.commits += 1


@dataclass(slots=True)
class _EstimateLineageSeed:
    project_id: UUID
    file_id: UUID
    revision_id: UUID
    estimate_version_id: UUID


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


async def _seed_estimate_lineage() -> _EstimateLineageSeed:
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
    quantity_item_id = uuid4()
    estimate_job_id = uuid4()
    estimate_version_id = uuid4()
    snapshot_entry_id = uuid4()
    estimate_item_id = uuid4()

    async with session_factory() as db:
        project = Project(id=project_id, name="Estimate visibility test")
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
            canonical_entity_schema_version="1.0.0",
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
            validation_status="valid",
        )
        quantity_item = QuantityItem(
            id=quantity_item_id,
            quantity_takeoff_id=takeoff_id,
            project_id=project_id,
            drawing_revision_id=revision_id,
            item_kind=QuantityItemKind.AGGREGATE.value,
            quantity_type="area",
            value=42.0,
            unit="sq_ft",
            validation_status="valid",
            source_entity_id=None,
            excluded_source_entity_ids_json=[],
        )
        estimate_job = Job(
            id=estimate_job_id,
            project_id=project_id,
            file_id=file_id,
            extraction_profile_id=None,
            base_revision_id=revision_id,
            parent_job_id=None,
            job_type=JobType.ESTIMATE.value,
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
        estimate_version = EstimateVersion(
            id=estimate_version_id,
            project_id=project_id,
            source_file_id=file_id,
            drawing_revision_id=revision_id,
            quantity_takeoff_id=takeoff_id,
            source_job_id=estimate_job_id,
            currency="GBP",
            subtotal_amount=Decimal("100.00"),
            tax_amount=Decimal("20.00"),
            total_amount=Decimal("120.00"),
        )
        snapshot_entry = EstimateSnapshotEntry(
            id=snapshot_entry_id,
            estimate_version_id=estimate_version_id,
            project_id=project_id,
            drawing_revision_id=revision_id,
            entry_type="assumption",
            entry_key="assumption:1",
            entry_label="Frozen assumption",
            sort_order=1,
            currency=None,
            quantity_value=None,
            unit=None,
            effective_date=None,
            unit_amount=None,
            source_payload_json={"text": "Include preliminaries"},
            rounding_json={},
            source_rate_id=None,
            source_material_id=None,
            source_formula_id=None,
            source_quantity_takeoff_id=None,
            source_quantity_item_id=None,
            source_checksum_sha256=None,
        )
        estimate_item = EstimateItem(
            id=estimate_item_id,
            estimate_version_id=estimate_version_id,
            project_id=project_id,
            drawing_revision_id=revision_id,
            line_type="assumption",
            line_number=1,
            line_key="line:1:assumption",
            description="Assumption line",
            currency="GBP",
            quantity_value=None,
            quantity_unit=None,
            unit_rate_amount=None,
            effective_date=None,
            subtotal_amount=Decimal("10.00"),
            tax_amount=Decimal("2.00"),
            total_amount=Decimal("12.00"),
            rounding_json={"scale": 2},
            quantity_snapshot_entry_id=None,
            quantity_snapshot_entry_type="quantity_input",
            rate_snapshot_entry_id=None,
            rate_snapshot_entry_type="rate",
            material_snapshot_entry_id=None,
            material_snapshot_entry_type="material",
            formula_snapshot_entry_id=None,
            formula_snapshot_entry_type="formula",
            assumption_snapshot_entry_id=snapshot_entry_id,
            assumption_snapshot_entry_type="assumption",
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

        db.add(quantity_item)
        await db.commit()

        db.add(estimate_job)
        await db.commit()

        db.add(estimate_version)
        await db.commit()

        db.add(snapshot_entry)
        await db.commit()

        db.add(estimate_item)
        await db.commit()

    return _EstimateLineageSeed(
        project_id=project_id,
        file_id=file_id,
        revision_id=revision_id,
        estimate_version_id=estimate_version_id,
    )


async def _soft_delete_lineage(seed: _EstimateLineageSeed, *, target: str) -> None:
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


def test_list_and_get_revision_estimates(monkeypatch: Any) -> None:
    revision = _build_revision()
    created_at = datetime(2026, 5, 16, tzinfo=UTC)
    first_estimate = SimpleNamespace(
        id=uuid4(),
        project_id=revision.project_id,
        source_file_id=revision.source_file_id,
        drawing_revision_id=revision.id,
        quantity_takeoff_id=uuid4(),
        source_job_id=uuid4(),
        quantity_gate="allowed",
        trusted_totals=True,
        currency="GBP",
        subtotal_amount=100.0,
        tax_amount=20.0,
        total_amount=120.0,
        created_at=created_at,
    )
    second_estimate = SimpleNamespace(
        id=uuid4(),
        project_id=revision.project_id,
        source_file_id=revision.source_file_id,
        drawing_revision_id=revision.id,
        quantity_takeoff_id=uuid4(),
        source_job_id=uuid4(),
        quantity_gate="allowed",
        trusted_totals=True,
        currency="GBP",
        subtotal_amount=150.0,
        tax_amount=30.0,
        total_amount=180.0,
        created_at=created_at.replace(second=1),
    )
    session = _AsyncSessionStub(execute_results=[[first_estimate, second_estimate], first_estimate])
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

    monkeypatch.setattr(estimates_routes_module, "_get_active_revision", fake_get_active_revision)

    list_response = client.get(f"/v1/revisions/{revision.id}/estimates?limit=1")

    assert list_response.status_code == 200
    list_body = list_response.json()
    assert [item["id"] for item in list_body["items"]] == [str(first_estimate.id)]
    assert list_body["next_cursor"] is not None
    assert decode_cursor_payload(list_body["next_cursor"]) == {
        "created_at": created_at.isoformat(),
        "id": str(first_estimate.id),
    }

    read_response = client.get(f"/v1/revisions/{revision.id}/estimates/{first_estimate.id}")

    assert read_response.status_code == 200
    assert read_response.json()["id"] == str(first_estimate.id)


def test_list_revision_estimate_items(monkeypatch: Any) -> None:
    revision = _build_revision()
    estimate_version = SimpleNamespace(id=uuid4())
    created_at = datetime(2026, 5, 16, tzinfo=UTC)
    first_item = SimpleNamespace(
        id=uuid4(),
        estimate_version_id=estimate_version.id,
        project_id=revision.project_id,
        drawing_revision_id=revision.id,
        line_type="assumption",
        line_number=1,
        line_key="line:1:assumption",
        description="Assumption line",
        currency="GBP",
        quantity_value=None,
        quantity_unit=None,
        unit_rate_amount=None,
        effective_date=None,
        subtotal_amount=10.0,
        tax_amount=2.0,
        total_amount=12.0,
        rounding_json={"scale": 2},
        quantity_snapshot_entry_id=None,
        quantity_snapshot_entry_type="quantity_input",
        rate_snapshot_entry_id=None,
        rate_snapshot_entry_type="rate",
        material_snapshot_entry_id=None,
        material_snapshot_entry_type="material",
        formula_snapshot_entry_id=None,
        formula_snapshot_entry_type="formula",
        assumption_snapshot_entry_id=uuid4(),
        assumption_snapshot_entry_type="assumption",
        created_at=created_at.replace(second=1),
    )
    second_item = SimpleNamespace(
        **{
            **first_item.__dict__,
            "id": uuid4(),
            "line_number": 2,
            "line_key": "line:2:assumption",
            "created_at": created_at,
        }
    )
    session = _AsyncSessionStub(
        execute_results=[
            estimate_version,
            [first_item, second_item],
            estimate_version,
            [second_item],
        ]
    )
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

    monkeypatch.setattr(estimates_routes_module, "_get_active_revision", fake_get_active_revision)

    response = client.get(
        f"/v1/revisions/{revision.id}/estimates/{estimate_version.id}/items?limit=1"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["items"][0]["id"] == str(first_item.id)
    assert body["items"][0]["rounding"] == {"scale": 2}
    assert body["next_cursor"] is not None
    assert decode_cursor_payload(body["next_cursor"]) == {
        "line_number": first_item.line_number,
        "id": str(first_item.id),
    }

    page_two_response = client.get(
        f"/v1/revisions/{revision.id}/estimates/{estimate_version.id}/items",
        params={"limit": 1, "cursor": body["next_cursor"]},
    )

    assert page_two_response.status_code == 200
    assert page_two_response.json() == {
        "items": [
            {
                "id": str(second_item.id),
                "estimate_version_id": str(estimate_version.id),
                "project_id": str(revision.project_id),
                "drawing_revision_id": str(revision.id),
                "line_type": "assumption",
                "line_number": 2,
                "line_key": "line:2:assumption",
                "description": "Assumption line",
                "currency": "GBP",
                "quantity_value": None,
                "quantity_unit": None,
                "unit_rate_amount": None,
                "effective_date": None,
                "subtotal_amount": "10.0",
                "tax_amount": "2.0",
                "total_amount": "12.0",
                "rounding": {"scale": 2},
                "quantity_snapshot_entry_id": None,
                "quantity_snapshot_entry_type": "quantity_input",
                "rate_snapshot_entry_id": None,
                "rate_snapshot_entry_type": "rate",
                "material_snapshot_entry_id": None,
                "material_snapshot_entry_type": "material",
                "formula_snapshot_entry_id": None,
                "formula_snapshot_entry_type": "formula",
                "assumption_snapshot_entry_id": str(second_item.assumption_snapshot_entry_id),
                "assumption_snapshot_entry_type": "assumption",
                "created_at": second_item.created_at.isoformat().replace("+00:00", "Z"),
            }
        ],
        "next_cursor": None,
    }

    order_by_sql = " ".join(str(clause) for clause in session.statements[1]._order_by_clauses)
    assert "line_number" in order_by_sql
    assert "created_at" not in order_by_sql

    page_two_where_sql = " ".join(str(clause) for clause in session.statements[3]._where_criteria)
    assert "line_number >" in page_two_where_sql
    assert "line_number =" in page_two_where_sql
    assert "id >" in page_two_where_sql
    assert "created_at" not in page_two_where_sql


def test_list_revision_estimate_snapshot_entries(monkeypatch: Any) -> None:
    revision = _build_revision()
    estimate_version = SimpleNamespace(id=uuid4())
    created_at = datetime(2026, 5, 16, tzinfo=UTC)
    first_entry = SimpleNamespace(
        id=uuid4(),
        estimate_version_id=estimate_version.id,
        project_id=revision.project_id,
        drawing_revision_id=revision.id,
        entry_type="assumption",
        entry_key="assumption:1",
        entry_label="Frozen assumption",
        sort_order=1,
        currency=None,
        quantity_value=None,
        unit=None,
        effective_date=None,
        unit_amount=None,
        source_payload_json={"text": "Include preliminaries"},
        rounding_json={"scale": 2},
        source_rate_id=None,
        source_material_id=None,
        source_formula_id=None,
        source_quantity_takeoff_id=None,
        source_quantity_item_id=None,
        source_checksum_sha256=None,
        created_at=created_at.replace(second=1),
    )
    second_entry = SimpleNamespace(
        **{
            **first_entry.__dict__,
            "id": uuid4(),
            "entry_key": "assumption:2",
            "sort_order": 2,
            "created_at": created_at,
        }
    )
    session = _AsyncSessionStub(
        execute_results=[
            estimate_version,
            [first_entry, second_entry],
            estimate_version,
            [second_entry],
        ]
    )
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

    monkeypatch.setattr(estimates_routes_module, "_get_active_revision", fake_get_active_revision)

    response = client.get(
        f"/v1/revisions/{revision.id}/estimates/{estimate_version.id}/snapshot-entries?limit=1"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["items"][0]["id"] == str(first_entry.id)
    assert body["items"][0]["source_payload"] == {"text": "Include preliminaries"}
    assert body["items"][0]["rounding"] == {"scale": 2}
    assert body["next_cursor"] is not None
    assert decode_cursor_payload(body["next_cursor"]) == {
        "sort_order": first_entry.sort_order,
        "id": str(first_entry.id),
    }

    page_two_response = client.get(
        f"/v1/revisions/{revision.id}/estimates/{estimate_version.id}/snapshot-entries",
        params={"limit": 1, "cursor": body["next_cursor"]},
    )

    assert page_two_response.status_code == 200
    assert page_two_response.json() == {
        "items": [
            {
                "id": str(second_entry.id),
                "estimate_version_id": str(estimate_version.id),
                "project_id": str(revision.project_id),
                "drawing_revision_id": str(revision.id),
                "entry_type": "assumption",
                "entry_key": "assumption:2",
                "entry_label": "Frozen assumption",
                "sort_order": 2,
                "currency": None,
                "quantity_value": None,
                "unit": None,
                "effective_date": None,
                "unit_amount": None,
                "source_payload": {"text": "Include preliminaries"},
                "rounding": {"scale": 2},
                "source_rate_id": None,
                "source_material_id": None,
                "source_formula_id": None,
                "source_quantity_takeoff_id": None,
                "source_quantity_item_id": None,
                "source_checksum_sha256": None,
                "created_at": second_entry.created_at.isoformat().replace("+00:00", "Z"),
            }
        ],
        "next_cursor": None,
    }

    order_by_sql = " ".join(str(clause) for clause in session.statements[1]._order_by_clauses)
    assert "sort_order" in order_by_sql
    assert "created_at" not in order_by_sql

    page_two_where_sql = " ".join(str(clause) for clause in session.statements[3]._where_criteria)
    assert "sort_order >" in page_two_where_sql
    assert "sort_order =" in page_two_where_sql
    assert "id >" in page_two_where_sql
    assert "created_at" not in page_two_where_sql


def test_revision_estimate_routes_return_404_for_unknown_revision(monkeypatch: Any) -> None:
    session = _AsyncSessionStub()
    app = _build_app(session)
    client = TestClient(app)
    unknown_revision_id = uuid4()
    estimate_version_id = uuid4()

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (revision_id, db, for_update)
        return None

    monkeypatch.setattr(estimates_routes_module, "_get_active_revision", fake_get_active_revision)

    responses = (
        client.get(f"/v1/revisions/{unknown_revision_id}/estimates"),
        client.get(f"/v1/revisions/{unknown_revision_id}/estimates/{estimate_version_id}"),
        client.get(f"/v1/revisions/{unknown_revision_id}/estimates/{estimate_version_id}/items"),
        client.get(
            f"/v1/revisions/{unknown_revision_id}/estimates/{estimate_version_id}/snapshot-entries"
        ),
    )

    for response in responses:
        assert response.status_code == 404


def test_list_revision_estimates_rejects_invalid_cursor(monkeypatch: Any) -> None:
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

    monkeypatch.setattr(estimates_routes_module, "_get_active_revision", fake_get_active_revision)

    response = client.get(f"/v1/revisions/{revision.id}/estimates?cursor=not-base64")

    assert response.status_code == 400
    assert response.json()["detail"]["error"]["code"] == "INVALID_CURSOR"


def test_list_revision_estimate_items_rejects_invalid_cursor(monkeypatch: Any) -> None:
    revision = _build_revision()
    estimate_version = SimpleNamespace(id=uuid4())
    session = _AsyncSessionStub(execute_results=[estimate_version])
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

    monkeypatch.setattr(estimates_routes_module, "_get_active_revision", fake_get_active_revision)

    response = client.get(
        f"/v1/revisions/{revision.id}/estimates/{estimate_version.id}/items?cursor=not-base64"
    )

    assert response.status_code == 400
    assert response.json()["detail"]["error"]["code"] == "INVALID_CURSOR"


def test_list_revision_estimate_snapshot_entries_rejects_invalid_cursor(
    monkeypatch: Any,
) -> None:
    revision = _build_revision()
    estimate_version = SimpleNamespace(id=uuid4())
    session = _AsyncSessionStub(execute_results=[estimate_version])
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

    monkeypatch.setattr(estimates_routes_module, "_get_active_revision", fake_get_active_revision)

    response = client.get(
        f"/v1/revisions/{revision.id}/estimates/{estimate_version.id}/snapshot-entries?cursor=not-base64"
    )

    assert response.status_code == 400
    assert response.json()["detail"]["error"]["code"] == "INVALID_CURSOR"


def test_revision_estimate_read_returns_404_for_revision_mismatch(monkeypatch: Any) -> None:
    revision = _build_revision()
    other_estimate_version_id = uuid4()
    session = _AsyncSessionStub(execute_results=[None])
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

    monkeypatch.setattr(estimates_routes_module, "_get_active_revision", fake_get_active_revision)

    response = client.get(f"/v1/revisions/{revision.id}/estimates/{other_estimate_version_id}")

    assert response.status_code == 404


def test_revision_estimate_nested_routes_return_404_for_revision_mismatch(
    monkeypatch: Any,
) -> None:
    revision = _build_revision()
    other_estimate_version_id = uuid4()
    session = _AsyncSessionStub(execute_results=[None, None])
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

    monkeypatch.setattr(estimates_routes_module, "_get_active_revision", fake_get_active_revision)

    responses = (
        client.get(f"/v1/revisions/{revision.id}/estimates/{other_estimate_version_id}/items"),
        client.get(
            f"/v1/revisions/{revision.id}/estimates/{other_estimate_version_id}/snapshot-entries"
        ),
    )

    for response in responses:
        assert response.status_code == 404


@pytest.mark.anyio
@pytest.mark.parametrize("soft_delete_target", ["file", "project"])
@requires_database
async def test_list_revision_estimates_hides_soft_deleted_lineage_after_stale_check(
    async_client: AsyncClient,
    monkeypatch: Any,
    soft_delete_target: str,
) -> None:
    seed = await _seed_estimate_lineage()
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

    monkeypatch.setattr(estimates_routes_module, "_get_active_revision", fake_get_active_revision)

    response = await async_client.get(f"/v1/revisions/{seed.revision_id}/estimates")

    assert response.status_code == 200
    assert response.json() == {"items": [], "next_cursor": None}


@pytest.mark.anyio
@pytest.mark.parametrize("soft_delete_target", ["file", "project"])
@requires_database
async def test_get_revision_estimate_hides_soft_deleted_lineage_after_stale_check(
    async_client: AsyncClient,
    monkeypatch: Any,
    soft_delete_target: str,
) -> None:
    seed = await _seed_estimate_lineage()
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

    monkeypatch.setattr(estimates_routes_module, "_get_active_revision", fake_get_active_revision)

    response = await async_client.get(
        f"/v1/revisions/{seed.revision_id}/estimates/{seed.estimate_version_id}"
    )

    assert response.status_code == 404


@pytest.mark.anyio
@pytest.mark.parametrize("soft_delete_target", ["file", "project"])
@requires_database
async def test_list_revision_estimate_items_hides_soft_deleted_lineage_after_stale_checks(
    async_client: AsyncClient,
    monkeypatch: Any,
    soft_delete_target: str,
) -> None:
    seed = await _seed_estimate_lineage()
    await _soft_delete_lineage(seed, target=soft_delete_target)
    stale_revision = _build_revision(revision_id=seed.revision_id)
    stale_revision.project_id = seed.project_id
    stale_revision.source_file_id = seed.file_id
    stale_estimate_version = SimpleNamespace(id=seed.estimate_version_id)

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (db, for_update)
        return stale_revision if revision_id == seed.revision_id else None

    async def fake_get_revision_estimate_version_or_404(
        revision_id: UUID,
        estimate_version_id: UUID,
        db: AsyncSession,
    ) -> SimpleNamespace:
        _ = db
        assert revision_id == seed.revision_id
        assert estimate_version_id == seed.estimate_version_id
        return stale_estimate_version

    monkeypatch.setattr(estimates_routes_module, "_get_active_revision", fake_get_active_revision)
    monkeypatch.setattr(
        estimates_routes_module,
        "_get_revision_estimate_version_or_404",
        fake_get_revision_estimate_version_or_404,
    )

    response = await async_client.get(
        f"/v1/revisions/{seed.revision_id}/estimates/{seed.estimate_version_id}/items"
    )

    assert response.status_code == 200
    assert response.json() == {"items": [], "next_cursor": None}


@pytest.mark.anyio
@pytest.mark.parametrize("soft_delete_target", ["file", "project"])
@requires_database
async def test_list_revision_estimate_snapshot_entries_hide_soft_deleted_lineage_after_stale_checks(
    async_client: AsyncClient,
    monkeypatch: Any,
    soft_delete_target: str,
) -> None:
    seed = await _seed_estimate_lineage()
    await _soft_delete_lineage(seed, target=soft_delete_target)
    stale_revision = _build_revision(revision_id=seed.revision_id)
    stale_revision.project_id = seed.project_id
    stale_revision.source_file_id = seed.file_id
    stale_estimate_version = SimpleNamespace(id=seed.estimate_version_id)

    async def fake_get_active_revision(
        revision_id: UUID,
        db: AsyncSession,
        *,
        for_update: bool = False,
    ) -> SimpleNamespace | None:
        _ = (db, for_update)
        return stale_revision if revision_id == seed.revision_id else None

    async def fake_get_revision_estimate_version_or_404(
        revision_id: UUID,
        estimate_version_id: UUID,
        db: AsyncSession,
    ) -> SimpleNamespace:
        _ = db
        assert revision_id == seed.revision_id
        assert estimate_version_id == seed.estimate_version_id
        return stale_estimate_version

    monkeypatch.setattr(estimates_routes_module, "_get_active_revision", fake_get_active_revision)
    monkeypatch.setattr(
        estimates_routes_module,
        "_get_revision_estimate_version_or_404",
        fake_get_revision_estimate_version_or_404,
    )

    response = await async_client.get(
        f"/v1/revisions/{seed.revision_id}/estimates/{seed.estimate_version_id}/snapshot-entries"
    )

    assert response.status_code == 200
    assert response.json() == {"items": [], "next_cursor": None}
