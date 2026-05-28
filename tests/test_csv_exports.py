"""Integration tests for deterministic CSV export rendering."""

from __future__ import annotations

import csv
import hashlib
import io
import uuid
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

import app.db.session as session_module
from app.exports.csv import (
    CSV_EXPORT_MEDIA_TYPE,
    ESTIMATE_CSV_EXPORT_GENERATOR_NAME,
    ESTIMATE_CSV_EXPORT_GENERATOR_VERSION,
    ESTIMATE_CSV_EXPORT_HEADERS,
    QUANTITY_CSV_EXPORT_GENERATOR_NAME,
    QUANTITY_CSV_EXPORT_GENERATOR_VERSION,
    QUANTITY_CSV_EXPORT_HEADERS,
    CsvExportResult,
    EstimateCsvExportError,
    QuantityCsvExportError,
    _iter_estimate_rows,
    render_estimate_csv_export,
    render_quantity_csv_export,
)
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from app.models.estimate_version import (
    EstimateItem,
    EstimateSnapshotEntry,
    EstimateVersion,
)
from app.models.extraction_profile import ExtractionProfile
from app.models.file import File
from app.models.job import Job, JobStatus, JobType
from app.models.project import Project
from app.models.quantity_takeoff import (
    QuantityGate,
    QuantityItem,
    QuantityItemKind,
    QuantityReviewState,
    QuantityTakeoff,
    QuantityValidationStatus,
)
from tests.conftest import requires_database

pytestmark = [pytest.mark.asyncio, requires_database]


@dataclass(frozen=True, slots=True)
class SeededExportFixture:
    quantity_takeoff: QuantityTakeoff
    estimate_version: EstimateVersion
    quantity_items: tuple[QuantityItem, QuantityItem]
    estimate_items: tuple[EstimateItem, EstimateItem]


@pytest_asyncio.fixture
async def db_session(cleanup_projects: None) -> AsyncGenerator[AsyncSession, None]:
    _ = cleanup_projects
    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        yield session
        await session.rollback()


async def test_render_quantity_csv_export_is_deterministic_and_stable(
    db_session: AsyncSession,
) -> None:
    seeded = await _seed_export_fixture(db_session)

    first = await render_quantity_csv_export(db_session, seeded.quantity_takeoff.id)
    second = await render_quantity_csv_export(db_session, seeded.quantity_takeoff.id)

    assert first == second
    assert first.media_type == CSV_EXPORT_MEDIA_TYPE
    assert first.generator_name == QUANTITY_CSV_EXPORT_GENERATOR_NAME
    assert first.generator_version == QUANTITY_CSV_EXPORT_GENERATOR_VERSION
    assert first.checksum_sha256 == hashlib.sha256(first.content_bytes).hexdigest()

    rows = _read_csv_rows(first)
    assert rows[0] == list(QUANTITY_CSV_EXPORT_HEADERS)
    assert rows[1][1] == str(seeded.quantity_items[0].id)
    assert rows[2][1] == str(seeded.quantity_items[1].id)
    assert rows[1][7] == '\'=length "A", B'
    assert rows[1][8] == "3.14159265358979"
    assert rows[2][8] == "0.25"
    assert rows[1][13] == ""
    assert rows[1][14] == '["entity-2","entity\\"3"]'
    assert rows[2][13] == ""
    assert rows[2][14] == "[]"
    assert rows[1][15] == "2026-05-27T18:00:00Z"
    assert rows[2][15] == "2026-05-27T18:01:00Z"

    text = first.content_bytes.decode("utf-8")
    assert text.endswith("\n")
    assert "\r\n" not in text
    assert '"\'=length ""A"", B"' in text
    assert '"[""entity-2"",""entity\\""3""]"' in text


async def test_render_estimate_csv_export_is_deterministic_and_stable(
    db_session: AsyncSession,
) -> None:
    seeded = await _seed_export_fixture(db_session)

    first = await render_estimate_csv_export(db_session, seeded.estimate_version.id)
    second = await render_estimate_csv_export(db_session, seeded.estimate_version.id)

    assert first == second
    assert first.media_type == CSV_EXPORT_MEDIA_TYPE
    assert first.generator_name == ESTIMATE_CSV_EXPORT_GENERATOR_NAME
    assert first.generator_version == ESTIMATE_CSV_EXPORT_GENERATOR_VERSION
    assert first.checksum_sha256 == hashlib.sha256(first.content_bytes).hexdigest()

    rows = _read_csv_rows(first)
    assert rows[0] == list(ESTIMATE_CSV_EXPORT_HEADERS)
    assert rows[1][1] == str(seeded.estimate_items[0].id)
    assert rows[2][1] == str(seeded.estimate_items[1].id)
    assert rows[1][7] == "1"
    assert rows[2][7] == "2"
    assert rows[1][10] == '\'@Allowance "A", base'
    assert rows[1][12] == ""
    assert rows[1][13] == ""
    assert rows[1][14] == ""
    assert rows[1][15] == ""
    assert rows[1][16] == "5.00"
    assert rows[1][19] == '{"mode":"bankers","scale":2}'
    assert rows[2][10] == 'Allowance "B", secondary'
    assert rows[2][12] == ""
    assert rows[2][13] == ""
    assert rows[2][14] == ""
    assert rows[2][15] == ""
    assert rows[2][16] == "570.88"
    assert rows[2][17] == "0.00"
    assert rows[2][18] == "570.88"
    assert rows[2][19] == '{"mode":"half_up","scale":2}'
    assert rows[2][25] == "2026-05-27T18:11:00Z"

    text = first.content_bytes.decode("utf-8")
    assert text.endswith("\n")
    assert "\r\n" not in text
    assert '"\'@Allowance ""A"", base"' in text
    assert '"{""mode"":""bankers"",""scale"":2}"' in text


async def test_iter_estimate_rows_formats_non_null_quantity_and_rate_with_fixed_scale() -> None:
    estimate_version = EstimateVersion(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        source_file_id=uuid.uuid4(),
        drawing_revision_id=uuid.uuid4(),
        quantity_takeoff_id=uuid.uuid4(),
        source_job_id=uuid.uuid4(),
        quantity_gate=QuantityGate.ALLOWED.value,
        trusted_totals=True,
        currency="GBP",
        subtotal_amount=Decimal("55.35"),
        tax_amount=Decimal("0.00"),
        total_amount=Decimal("55.35"),
        created_at=datetime(2026, 5, 27, 18, 7, tzinfo=UTC),
    )
    item = EstimateItem(
        id=uuid.uuid4(),
        estimate_version_id=estimate_version.id,
        project_id=estimate_version.project_id,
        drawing_revision_id=estimate_version.drawing_revision_id,
        line_type="rate",
        line_number=1,
        line_key="line:rate-a",
        description="Rate A",
        currency="GBP",
        quantity_value=Decimal("12.3"),
        quantity_unit="m",
        unit_rate_amount=Decimal("4.5"),
        effective_date=date(2026, 5, 27),
        subtotal_amount=Decimal("55.35"),
        tax_amount=Decimal("0.00"),
        total_amount=Decimal("55.35"),
        rounding_json={"mode": "half_up", "scale": 2},
        quantity_snapshot_entry_id=uuid.uuid4(),
        quantity_snapshot_entry_type="quantity_input",
        rate_snapshot_entry_id=uuid.uuid4(),
        rate_snapshot_entry_type="rate",
        material_snapshot_entry_id=None,
        material_snapshot_entry_type="material",
        formula_snapshot_entry_id=None,
        formula_snapshot_entry_type="formula",
        assumption_snapshot_entry_id=None,
        assumption_snapshot_entry_type="assumption",
        created_at=datetime(2026, 5, 27, 18, 12, tzinfo=UTC),
    )

    row = next(iter(_iter_estimate_rows(estimate_version, [item])))

    assert row[12] == "12.300000"
    assert row[14] == "4.500000"


async def test_render_quantity_csv_export_raises_for_missing_takeoff(
    db_session: AsyncSession,
) -> None:
    missing_id = uuid.uuid4()

    with pytest.raises(QuantityCsvExportError, match=str(missing_id)):
        await render_quantity_csv_export(db_session, missing_id)


async def test_render_estimate_csv_export_raises_for_missing_estimate_version(
    db_session: AsyncSession,
) -> None:
    missing_id = uuid.uuid4()

    with pytest.raises(EstimateCsvExportError, match=str(missing_id)):
        await render_estimate_csv_export(db_session, missing_id)


def _read_csv_rows(result: CsvExportResult) -> list[list[str]]:
    return list(csv.reader(io.StringIO(result.content_bytes.decode("utf-8"))))


async def _seed_export_fixture(db_session: AsyncSession) -> SeededExportFixture:
    project = Project(
        id=uuid.uuid4(),
        name="CSV Export Project",
        description="Fixture project",
    )
    source_file = File(
        id=uuid.uuid4(),
        project_id=project.id,
        original_filename="fixture.dxf",
        media_type="application/dxf",
        detected_format="dxf",
        storage_uri="file:///tmp/fixture.dxf",
        size_bytes=256,
        checksum_sha256="a" * 64,
        immutable=True,
        created_at=datetime(2026, 5, 27, 17, 0, tzinfo=UTC),
    )
    extraction_profile = ExtractionProfile(
        id=uuid.uuid4(),
        project_id=project.id,
        profile_version="1.0",
        layout_mode="all",
        xref_handling="embed",
        block_handling="expand",
        text_extraction=True,
        dimension_extraction=True,
        confidence_threshold=0.6,
        created_at=datetime(2026, 5, 27, 17, 1, tzinfo=UTC),
    )
    ingest_job = Job(
        id=uuid.uuid4(),
        project_id=project.id,
        file_id=source_file.id,
        extraction_profile_id=extraction_profile.id,
        base_revision_id=None,
        parent_job_id=None,
        job_type=JobType.INGEST.value,
        status=JobStatus.SUCCEEDED.value,
        attempts=1,
        max_attempts=3,
        enqueue_status="published",
        enqueue_attempts=1,
        cancel_requested=False,
        created_at=datetime(2026, 5, 27, 17, 2, tzinfo=UTC),
        finished_at=datetime(2026, 5, 27, 17, 3, tzinfo=UTC),
    )
    adapter_output = AdapterRunOutput(
        id=uuid.uuid4(),
        project_id=project.id,
        source_file_id=source_file.id,
        extraction_profile_id=extraction_profile.id,
        source_job_id=ingest_job.id,
        adapter_key="tests.fake",
        adapter_version="1.0",
        input_family="dxf",
        canonical_entity_schema_version="1.0",
        canonical_json={"entities": [], "layers": [], "layouts": [], "blocks": []},
        provenance_json={"adapter": {"key": "tests.fake", "version": "1.0"}},
        confidence_json={"score": 0.9},
        confidence_score=0.9,
        warnings_json=[],
        diagnostics_json={"duration_ms": 4},
        result_checksum_sha256="b" * 64,
        created_at=datetime(2026, 5, 27, 17, 4, tzinfo=UTC),
    )
    revision = DrawingRevision(
        id=uuid.uuid4(),
        project_id=project.id,
        source_file_id=source_file.id,
        extraction_profile_id=extraction_profile.id,
        source_job_id=ingest_job.id,
        adapter_run_output_id=adapter_output.id,
        predecessor_revision_id=None,
        revision_sequence=1,
        revision_kind="ingest",
        review_state="approved",
        canonical_entity_schema_version="1.0",
        confidence_score=0.9,
        created_at=datetime(2026, 5, 27, 17, 5, tzinfo=UTC),
    )
    quantity_job = Job(
        id=uuid.uuid4(),
        project_id=project.id,
        file_id=source_file.id,
        extraction_profile_id=None,
        base_revision_id=revision.id,
        parent_job_id=ingest_job.id,
        job_type=JobType.QUANTITY_TAKEOFF.value,
        status=JobStatus.SUCCEEDED.value,
        attempts=1,
        max_attempts=3,
        enqueue_status="published",
        enqueue_attempts=1,
        cancel_requested=False,
        created_at=datetime(2026, 5, 27, 17, 6, tzinfo=UTC),
        finished_at=datetime(2026, 5, 27, 17, 7, tzinfo=UTC),
    )
    quantity_takeoff = QuantityTakeoff(
        id=uuid.uuid4(),
        project_id=project.id,
        source_file_id=source_file.id,
        drawing_revision_id=revision.id,
        source_job_id=quantity_job.id,
        source_job_type=JobType.QUANTITY_TAKEOFF.value,
        review_state=QuantityReviewState.APPROVED.value,
        validation_status=QuantityValidationStatus.VALID.value,
        quantity_gate=QuantityGate.ALLOWED.value,
        trusted_totals=True,
        created_at=datetime(2026, 5, 27, 17, 8, tzinfo=UTC),
    )

    db_session.add(project)
    await db_session.flush()
    db_session.add(source_file)
    db_session.add(extraction_profile)
    await db_session.flush()
    db_session.add(ingest_job)
    await db_session.flush()
    db_session.add(adapter_output)
    await db_session.flush()
    db_session.add(revision)
    await db_session.flush()
    db_session.add(quantity_job)
    await db_session.flush()
    db_session.add(quantity_takeoff)
    await db_session.flush()

    quantity_item_first = QuantityItem(
        id=uuid.uuid4(),
        quantity_takeoff_id=quantity_takeoff.id,
        project_id=project.id,
        drawing_revision_id=revision.id,
        item_kind=QuantityItemKind.AGGREGATE.value,
        quantity_type='=length "A", B',
        value=3.14159265358979,
        unit="m",
        review_state=QuantityReviewState.APPROVED.value,
        validation_status=QuantityValidationStatus.VALID.value,
        quantity_gate=QuantityGate.ALLOWED.value,
        source_entity_id=None,
        excluded_source_entity_ids_json=["entity-2", 'entity"3'],
        created_at=datetime(2026, 5, 27, 18, 0, tzinfo=UTC),
    )
    quantity_item_second = QuantityItem(
        id=uuid.uuid4(),
        quantity_takeoff_id=quantity_takeoff.id,
        project_id=project.id,
        drawing_revision_id=revision.id,
        item_kind=QuantityItemKind.AGGREGATE.value,
        quantity_type="area",
        value=0.25,
        unit="m2",
        review_state=QuantityReviewState.APPROVED.value,
        validation_status=QuantityValidationStatus.VALID.value,
        quantity_gate=QuantityGate.ALLOWED.value,
        source_entity_id=None,
        excluded_source_entity_ids_json=[],
        created_at=datetime(2026, 5, 27, 18, 1, tzinfo=UTC),
    )
    db_session.add(quantity_item_second)
    db_session.add(quantity_item_first)
    await db_session.flush()

    estimate_job = Job(
        id=uuid.uuid4(),
        project_id=project.id,
        file_id=source_file.id,
        extraction_profile_id=None,
        base_revision_id=revision.id,
        parent_job_id=quantity_job.id,
        job_type=JobType.ESTIMATE.value,
        status=JobStatus.SUCCEEDED.value,
        attempts=1,
        max_attempts=3,
        enqueue_status="published",
        enqueue_attempts=1,
        cancel_requested=False,
        created_at=datetime(2026, 5, 27, 18, 5, tzinfo=UTC),
        finished_at=datetime(2026, 5, 27, 18, 6, tzinfo=UTC),
    )
    db_session.add(estimate_job)
    await db_session.flush()

    estimate_version = EstimateVersion(
        id=uuid.uuid4(),
        project_id=project.id,
        source_file_id=source_file.id,
        drawing_revision_id=revision.id,
        quantity_takeoff_id=quantity_takeoff.id,
        source_job_id=estimate_job.id,
        quantity_gate=QuantityGate.ALLOWED.value,
        trusted_totals=True,
        currency="GBP",
        subtotal_amount=Decimal("575.88"),
        tax_amount=Decimal("0.00"),
        total_amount=Decimal("575.88"),
        created_at=datetime(2026, 5, 27, 18, 7, tzinfo=UTC),
    )
    db_session.add(estimate_version)
    await db_session.flush()

    assumption_snapshot_entry_first = EstimateSnapshotEntry(
        id=uuid.uuid4(),
        estimate_version_id=estimate_version.id,
        project_id=project.id,
        drawing_revision_id=revision.id,
        entry_type="assumption",
        entry_key="assumption:allowance-a",
        entry_label="Allowance note A",
        sort_order=1,
        currency=None,
        quantity_value=None,
        unit=None,
        effective_date=None,
        unit_amount=None,
        source_payload_json={"text": "Allowance A"},
        rounding_json={"mode": "bankers", "scale": 2},
        source_rate_id=None,
        source_material_id=None,
        source_formula_id=None,
        source_quantity_takeoff_id=None,
        source_quantity_item_id=None,
        source_checksum_sha256=None,
        created_at=datetime(2026, 5, 27, 18, 8, tzinfo=UTC),
    )
    assumption_snapshot_entry_second = EstimateSnapshotEntry(
        id=uuid.uuid4(),
        estimate_version_id=estimate_version.id,
        project_id=project.id,
        drawing_revision_id=revision.id,
        entry_type="assumption",
        entry_key="assumption:allowance-b",
        entry_label="Allowance note B",
        sort_order=2,
        currency=None,
        quantity_value=None,
        unit=None,
        effective_date=None,
        unit_amount=None,
        source_payload_json={"text": "Allowance B"},
        rounding_json={"mode": "half_up", "scale": 2},
        source_rate_id=None,
        source_material_id=None,
        source_formula_id=None,
        source_quantity_takeoff_id=None,
        source_quantity_item_id=None,
        source_checksum_sha256=None,
        created_at=datetime(2026, 5, 27, 18, 9, tzinfo=UTC),
    )
    db_session.add(assumption_snapshot_entry_first)
    db_session.add(assumption_snapshot_entry_second)
    await db_session.flush()

    estimate_item_second = EstimateItem(
        id=uuid.uuid4(),
        estimate_version_id=estimate_version.id,
        project_id=project.id,
        drawing_revision_id=revision.id,
        line_type="assumption",
        line_number=2,
        line_key="line:allowance-b",
        description='Allowance "B", secondary',
        currency="GBP",
        quantity_value=None,
        quantity_unit=None,
        unit_rate_amount=None,
        effective_date=None,
        subtotal_amount=Decimal("570.88"),
        tax_amount=Decimal("0.00"),
        total_amount=Decimal("570.88"),
        rounding_json={"mode": "half_up", "scale": 2},
        quantity_snapshot_entry_id=None,
        quantity_snapshot_entry_type="quantity_input",
        rate_snapshot_entry_id=None,
        rate_snapshot_entry_type="rate",
        material_snapshot_entry_id=None,
        material_snapshot_entry_type="material",
        formula_snapshot_entry_id=None,
        formula_snapshot_entry_type="formula",
        assumption_snapshot_entry_id=assumption_snapshot_entry_second.id,
        assumption_snapshot_entry_type="assumption",
        created_at=datetime(2026, 5, 27, 18, 11, tzinfo=UTC),
    )
    estimate_item_first = EstimateItem(
        id=uuid.uuid4(),
        estimate_version_id=estimate_version.id,
        project_id=project.id,
        drawing_revision_id=revision.id,
        line_type="assumption",
        line_number=1,
        line_key="line:allowance",
        description='@Allowance "A", base',
        currency="GBP",
        quantity_value=None,
        quantity_unit=None,
        unit_rate_amount=None,
        effective_date=None,
        subtotal_amount=Decimal("5.00"),
        tax_amount=Decimal("0.00"),
        total_amount=Decimal("5.00"),
        rounding_json={"mode": "bankers", "scale": 2},
        quantity_snapshot_entry_id=None,
        quantity_snapshot_entry_type="quantity_input",
        rate_snapshot_entry_id=None,
        rate_snapshot_entry_type="rate",
        material_snapshot_entry_id=None,
        material_snapshot_entry_type="material",
        formula_snapshot_entry_id=None,
        formula_snapshot_entry_type="formula",
        assumption_snapshot_entry_id=assumption_snapshot_entry_first.id,
        assumption_snapshot_entry_type="assumption",
        created_at=datetime(2026, 5, 27, 18, 12, tzinfo=UTC),
    )
    db_session.add(estimate_item_second)
    db_session.add(estimate_item_first)
    await db_session.flush()

    return SeededExportFixture(
        quantity_takeoff=quantity_takeoff,
        estimate_version=estimate_version,
        quantity_items=(quantity_item_first, quantity_item_second),
        estimate_items=(estimate_item_first, estimate_item_second),
    )
