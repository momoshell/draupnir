from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from datetime import date
from decimal import Decimal

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import app.db.session as session_module
from app.estimating.engine import formula_definition_from_json
from app.estimating.engine.contracts import (
    EstimateAssumptionEntryInput,
    EstimateEngineInput,
    EstimateFormulaEntryInput,
    EstimateLineInputSpec,
    EstimateMaterialEntryInput,
    EstimateQuantityEntryInput,
    EstimateRateEntryInput,
)
from app.estimating.engine.service import compose_estimate
from app.models.estimate_version import EstimateItem, EstimateSnapshotEntry, EstimateVersion
from app.models.estimation_catalog import EstimationFormula, EstimationMaterial, EstimationRate
from app.models.job import Job, JobStatus, JobType
from tests import test_quantity_takeoff_persistence as quantity_takeoff_persistence
from tests.conftest import requires_database

pytest_plugins = ("tests.test_quantity_takeoff_persistence",)
pytestmark = [pytest.mark.asyncio, requires_database]


@pytest_asyncio.fixture
async def db_session(cleanup_projects: None) -> AsyncGenerator[AsyncSession, None]:
    _ = cleanup_projects
    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        yield session
        await session.rollback()


async def _create_estimate_job(
    db_session: AsyncSession,
    seed: quantity_takeoff_persistence._QuantityPersistenceSeed,
) -> uuid.UUID:
    estimate_job_id = uuid.uuid4()
    db_session.add(
        Job(
            id=estimate_job_id,
            project_id=seed.project_id,
            file_id=seed.file_id,
            extraction_profile_id=None,
            base_revision_id=seed.drawing_revision_id,
            parent_job_id=seed.quantity_job_id,
            job_type=JobType.ESTIMATE.value,
            status=JobStatus.SUCCEEDED.value,
            enqueue_status="published",
            enqueue_attempts=0,
            cancel_requested=False,
        )
    )
    await db_session.commit()
    return estimate_job_id


async def _create_catalog_rows(
    db_session: AsyncSession,
    project_id: uuid.UUID,
) -> tuple[EstimationRate, EstimationRate, EstimationMaterial, EstimationFormula]:
    installer_rate = EstimationRate(
        id=uuid.uuid4(),
        scope_type="project",
        project_id=project_id,
        rate_key="labour:installer",
        source="test",
        metadata_json={"source": "test"},
        name="Installer labour",
        item_type="linear_length",
        per_unit="m",
        currency="GBP",
        amount=Decimal("12.345678"),
        effective_from=date(2026, 1, 1),
        checksum_sha256="c" * 64,
    )
    formula_rate = EstimationRate(
        id=uuid.uuid4(),
        scope_type="project",
        project_id=project_id,
        rate_key="labour:formula",
        source="test",
        metadata_json={"source": "test"},
        name="Formula rate",
        item_type="linear_length",
        per_unit="m",
        currency="GBP",
        amount=Decimal("0.496000"),
        effective_from=date(2026, 1, 1),
        checksum_sha256="d" * 64,
    )
    material = EstimationMaterial(
        id=uuid.uuid4(),
        scope_type="project",
        project_id=project_id,
        material_key="paint:standard",
        source="test",
        metadata_json={"source": "test"},
        name="Paint",
        unit="m",
        currency="GBP",
        unit_cost=Decimal("3.210000"),
        effective_from=date(2026, 1, 1),
        checksum_sha256="1" * 64,
    )
    formula = EstimationFormula(
        id=uuid.uuid4(),
        scope_type="project",
        project_id=project_id,
        formula_id="formula.rounded_markup",
        version=1,
        name="Rounded markup",
        dsl_version="0.1",
        output_key="estimate.rounded_markup",
        output_contract_json={"kind": "money", "currency": "GBP"},
        declared_inputs_json=[
            {"name": "rate", "contract": {"kind": "rate", "currency": "GBP", "per_unit": "m"}},
            {"name": "quantity", "contract": {"kind": "quantity", "unit": "m"}},
        ],
        expression_json={
            "kind": "multiply",
            "args": [
                {"kind": "input", "name": "rate"},
                {"kind": "input", "name": "quantity"},
            ],
        },
        rounding_json={"scale": 1, "mode": "ROUND_HALF_UP"},
        checksum_sha256="f" * 64,
    )
    db_session.add_all([installer_rate, formula_rate, material, formula])
    await db_session.commit()
    return installer_rate, formula_rate, material, formula


def _build_engine_input(
    seed: quantity_takeoff_persistence._QuantityPersistenceSeed,
    *,
    estimate_job_id: uuid.UUID,
    installer_rate: EstimationRate,
    formula_rate: EstimationRate,
    material: EstimationMaterial,
    formula: EstimationFormula,
) -> EstimateEngineInput:
    formula_definition = formula_definition_from_json(
        formula_id=formula.formula_id,
        name=formula.name,
        version=formula.version,
        checksum=formula.checksum_sha256,
        output_key=formula.output_key,
        output_contract_json=formula.output_contract_json,
        declared_inputs_json=formula.declared_inputs_json,
        expression_json=formula.expression_json,
        rounding_json=formula.rounding_json,
    )
    return EstimateEngineInput(
        estimate_job_id=estimate_job_id,
        project_id=seed.project_id,
        file_id=seed.file_id,
        drawing_revision_id=seed.drawing_revision_id,
        quantity_takeoff_id=seed.quantity_takeoff_id,
        source_job_id=estimate_job_id,
        tax_rate=Decimal("0.20"),
        quantity_entries=(
            EstimateQuantityEntryInput(
                entry_key="quantity:labour",
                entry_label="Labour quantity",
                sort_order=1,
                source_quantity_item_id=seed.quantity_item_id,
                source_checksum_sha256="a" * 64,
                quantity_value=Decimal("2.5"),
                unit="m",
                source_quantity_takeoff_id=seed.quantity_takeoff_id,
                source_payload={"source": "takeoff"},
            ),
            EstimateQuantityEntryInput(
                entry_key="quantity:material",
                entry_label="Material quantity",
                sort_order=2,
                source_quantity_item_id=seed.quantity_item_id,
                source_checksum_sha256="b" * 64,
                quantity_value=Decimal("1.25"),
                unit="m",
                source_quantity_takeoff_id=seed.quantity_takeoff_id,
            ),
        ),
        rate_entries=(
            EstimateRateEntryInput(
                entry_key="rate:installer",
                entry_label="Installer labour",
                sort_order=3,
                source_rate_id=installer_rate.id,
                source_checksum_sha256=installer_rate.checksum_sha256,
                unit=installer_rate.per_unit,
                effective_date=installer_rate.effective_from,
                unit_amount=installer_rate.amount,
            ),
            EstimateRateEntryInput(
                entry_key="rate:formula",
                entry_label="Formula rate",
                sort_order=4,
                source_rate_id=formula_rate.id,
                source_checksum_sha256=formula_rate.checksum_sha256,
                unit=formula_rate.per_unit,
                effective_date=formula_rate.effective_from,
                unit_amount=formula_rate.amount,
            ),
        ),
        material_entries=(
            EstimateMaterialEntryInput(
                entry_key="material:paint",
                entry_label="Paint",
                sort_order=5,
                source_material_id=material.id,
                source_checksum_sha256=material.checksum_sha256,
                unit=material.unit,
                effective_date=material.effective_from,
                unit_amount=material.unit_cost,
            ),
        ),
        formula_entries=(
            EstimateFormulaEntryInput(
                entry_key="formula:rounded_markup",
                entry_label="Rounded markup",
                sort_order=6,
                source_formula_id=formula.id,
                source_checksum_sha256=formula.checksum_sha256,
                definition=formula_definition,
            ),
        ),
        assumption_entries=(
            EstimateAssumptionEntryInput(
                entry_key="assumption:mobilisation",
                entry_label="Mobilisation",
                sort_order=7,
                source_checksum_sha256="2" * 64,
                amount=Decimal("5.555"),
            ),
        ),
        line_inputs=(
            EstimateLineInputSpec(
                line_key="line:labour",
                line_type="rate",
                description="Installer labour",
                quantity_entry_key="quantity:labour",
                rate_entry_key="rate:installer",
            ),
            EstimateLineInputSpec(
                line_key="line:material",
                line_type="material",
                description="Paint",
                quantity_entry_key="quantity:material",
                material_entry_key="material:paint",
            ),
            EstimateLineInputSpec(
                line_key="line:formula",
                line_type="formula",
                description="Rounded markup",
                formula_entry_key="formula:rounded_markup",
                formula_inputs={"rate": "rate:formula", "quantity": "quantity:labour"},
            ),
            EstimateLineInputSpec(
                line_key="line:assumption",
                line_type="assumption",
                description="Mobilisation",
                assumption_entry_key="assumption:mobilisation",
            ),
            EstimateLineInputSpec(
                line_key="line:adjustment",
                line_type="adjustment",
                description="Access uplift",
                assumption_entry_key="assumption:mobilisation",
                adjustment_amount=Decimal("2.225"),
            ),
        ),
    )


async def test_engine_output_persists_and_replays_with_orm_payloads(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    seed = await quantity_takeoff_persistence._seed_quantity_lineage(async_client)
    estimate_job_id = await _create_estimate_job(db_session, seed)
    installer_rate, formula_rate, material, formula = await _create_catalog_rows(
        db_session,
        seed.project_id,
    )
    engine_input = _build_engine_input(
        seed,
        estimate_job_id=estimate_job_id,
        installer_rate=installer_rate,
        formula_rate=formula_rate,
        material=material,
        formula=formula,
    )

    result = compose_estimate(engine_input)
    replay = compose_estimate(engine_input)

    assert result.estimate_version_model_kwargs() == replay.estimate_version_model_kwargs()
    assert result.snapshot_entry_model_kwargs() == replay.snapshot_entry_model_kwargs()
    assert result.line_item_model_kwargs() == replay.line_item_model_kwargs()

    estimate_version = EstimateVersion(**result.estimate_version_model_kwargs())
    snapshot_entries = [
        EstimateSnapshotEntry(**payload) for payload in result.snapshot_entry_model_kwargs()
    ]
    line_items = [EstimateItem(**payload) for payload in result.line_item_model_kwargs()]

    db_session.add(estimate_version)
    await db_session.commit()
    db_session.add_all(snapshot_entries)
    await db_session.commit()
    db_session.add_all(line_items)
    await db_session.commit()

    persisted_snapshots = list(
        await db_session.scalars(
            select(EstimateSnapshotEntry)
            .where(EstimateSnapshotEntry.estimate_version_id == result.estimate_version_id)
            .order_by(EstimateSnapshotEntry.sort_order)
        )
    )
    persisted_items = list(
        await db_session.scalars(
            select(EstimateItem)
            .where(EstimateItem.estimate_version_id == result.estimate_version_id)
            .order_by(EstimateItem.line_number)
        )
    )

    assert len(persisted_snapshots) == 7
    assert len(persisted_items) == 5

    quantity_snapshot = next(
        entry for entry in persisted_snapshots if entry.entry_key == "quantity:labour"
    )
    formula_snapshot = next(
        entry for entry in persisted_snapshots if entry.entry_key == "formula:rounded_markup"
    )
    assumption_snapshot = next(
        entry for entry in persisted_snapshots if entry.entry_key == "assumption:mobilisation"
    )
    formula_item = next(item for item in persisted_items if item.line_key == "line:formula")
    adjustment_item = next(item for item in persisted_items if item.line_key == "line:adjustment")

    assert quantity_snapshot.source_checksum_sha256 is None
    assert formula_snapshot.source_payload_json["formula_definition"] == {
        "formula_id": "formula.rounded_markup",
        "name": "Rounded markup",
        "version": 1,
        "checksum": "f" * 64,
        "output_key": "estimate.rounded_markup",
        "output_contract": {"kind": "money", "currency": "GBP", "unit": None, "per_unit": None},
        "declared_inputs": [
            {
                "name": "rate",
                "contract": {"kind": "rate", "currency": "GBP", "unit": None, "per_unit": "m"},
            },
            {
                "name": "quantity",
                "contract": {
                    "kind": "quantity",
                    "currency": None,
                    "unit": "m",
                    "per_unit": None,
                },
            },
        ],
        "expression": {
            "kind": "multiply",
            "value": None,
            "name": None,
            "args": [
                {"kind": "input", "value": None, "name": "rate", "args": [], "rounding": None},
                {"kind": "input", "value": None, "name": "quantity", "args": [], "rounding": None},
            ],
            "rounding": None,
        },
        "rounding": {"scale": 1, "mode": "ROUND_HALF_UP"},
    }
    assert assumption_snapshot.currency is None
    assert assumption_snapshot.source_checksum_sha256 is None
    assert assumption_snapshot.source_payload_json == {"money_amount": "5.555"}
    assert formula_item.formula_snapshot_entry_id == formula_snapshot.id
    assert formula_item.quantity_snapshot_entry_id == quantity_snapshot.id
    assert formula_item.quantity_value == Decimal("2.500000")
    assert formula_item.quantity_unit == "m"
    assert formula_item.rate_snapshot_entry_id is None
    assert formula_item.material_snapshot_entry_id is None
    assert formula_item.assumption_snapshot_entry_id is None
    assert adjustment_item.formula_snapshot_entry_id is None
    assert adjustment_item.assumption_snapshot_entry_id == assumption_snapshot.id


async def test_engine_output_replays_rate_lines_from_persisted_quantity_scale(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    seed = await quantity_takeoff_persistence._seed_quantity_lineage(async_client)
    estimate_job_id = await _create_estimate_job(db_session, seed)
    replay_rate = EstimationRate(
        id=uuid.uuid4(),
        scope_type="project",
        project_id=seed.project_id,
        rate_key="labour:replay",
        source="test",
        metadata_json={"source": "test"},
        name="Replay labour",
        item_type="linear_length",
        per_unit="m",
        currency="GBP",
        amount=Decimal("0.500000"),
        effective_from=date(2026, 1, 1),
        checksum_sha256="e" * 64,
    )
    db_session.add(replay_rate)
    await db_session.commit()

    def _engine_input(quantity_value: Decimal) -> EstimateEngineInput:
        return EstimateEngineInput(
            estimate_job_id=estimate_job_id,
            project_id=seed.project_id,
            file_id=seed.file_id,
            drawing_revision_id=seed.drawing_revision_id,
            quantity_takeoff_id=seed.quantity_takeoff_id,
            source_job_id=estimate_job_id,
            tax_rate=Decimal("0"),
            quantity_entries=(
                EstimateQuantityEntryInput(
                    entry_key="quantity:labour",
                    entry_label="Labour quantity",
                    sort_order=1,
                    source_quantity_item_id=seed.quantity_item_id,
                    source_checksum_sha256="a" * 64,
                    quantity_value=quantity_value,
                    unit="m",
                    source_quantity_takeoff_id=seed.quantity_takeoff_id,
                ),
            ),
            rate_entries=(
                EstimateRateEntryInput(
                    entry_key="rate:replay",
                    entry_label="Replay labour",
                    sort_order=2,
                    source_rate_id=replay_rate.id,
                    source_checksum_sha256=replay_rate.checksum_sha256,
                    unit=replay_rate.per_unit,
                    effective_date=replay_rate.effective_from,
                    unit_amount=replay_rate.amount,
                ),
            ),
            line_inputs=(
                EstimateLineInputSpec(
                    line_key="line:replay",
                    line_type="rate",
                    description="Replay labour",
                    quantity_entry_key="quantity:labour",
                    rate_entry_key="rate:replay",
                ),
            ),
        )

    result = compose_estimate(_engine_input(Decimal("0.29")))
    replay = compose_estimate(_engine_input(Decimal("0.290000")))

    estimate_version = EstimateVersion(**result.estimate_version_model_kwargs())
    snapshot_entries = [
        EstimateSnapshotEntry(**payload) for payload in result.snapshot_entry_model_kwargs()
    ]
    line_items = [EstimateItem(**payload) for payload in result.line_item_model_kwargs()]

    db_session.add(estimate_version)
    await db_session.commit()
    db_session.add_all(snapshot_entries)
    await db_session.commit()
    db_session.add_all(line_items)
    await db_session.commit()

    persisted_snapshot = await db_session.scalar(
        select(EstimateSnapshotEntry).where(EstimateSnapshotEntry.entry_key == "quantity:labour")
    )
    persisted_item = await db_session.scalar(
        select(EstimateItem).where(EstimateItem.line_key == "line:replay")
    )

    assert persisted_snapshot is not None
    assert persisted_item is not None
    assert persisted_snapshot.quantity_value == Decimal("0.290000")
    assert persisted_item.quantity_value == Decimal("0.290000")
    assert persisted_item.subtotal_amount == Decimal("0.15")
    assert result.snapshot_entry_model_kwargs() == replay.snapshot_entry_model_kwargs()
    assert result.line_item_model_kwargs() == replay.line_item_model_kwargs()
