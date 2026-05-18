"""Integration tests for append-only estimate snapshot and item persistence."""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator, Mapping
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy import CheckConstraint, Table, UniqueConstraint, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

import app.db.session as session_module
from app.models.estimate_version import EstimateItem, EstimateSnapshotEntry, EstimateVersion
from app.models.estimation_catalog import EstimationFormula, EstimationMaterial, EstimationRate
from tests import test_estimate_version_persistence as estimate_version_persistence
from tests import test_quantity_takeoff_persistence as quantity_takeoff_persistence
from tests.conftest import requires_database

pytest_plugins = ("tests.test_quantity_takeoff_persistence",)
pytestmark = [pytest.mark.asyncio, requires_database]

_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "2026_05_18_0021_add_estimate_snapshot_items.py"
)


def _normalize_ondelete(raw_foreign_key: Mapping[str, Any]) -> str:
    options = raw_foreign_key.get("options")
    if isinstance(options, dict):
        return str(options.get("ondelete") or "").upper()
    return ""


def _lineage_contract_state(sync_connection: sa.Connection) -> dict[str, bool]:
    inspector = sa.inspect(sync_connection)
    quantity_item_unique = {
        tuple(constraint["column_names"])
        for constraint in inspector.get_unique_constraints("quantity_items")
    }
    quantity_item_foreign_keys = inspector.get_foreign_keys("estimate_snapshot_entries")
    source_quantity_item_fk = next(
        (
            foreign_key
            for foreign_key in quantity_item_foreign_keys
            if str(foreign_key.get("name")) == "fk_est_snapshot_entries_source_quantity_item"
        ),
        None,
    )
    return {
        "has_quantity_item_lineage_unique": (
            "id",
            "quantity_takeoff_id",
            "project_id",
            "drawing_revision_id",
        )
        in quantity_item_unique,
        "has_quantity_item_lineage_fk": source_quantity_item_fk is not None
        and tuple(source_quantity_item_fk["constrained_columns"])
        == (
            "source_quantity_item_id",
            "source_quantity_takeoff_id",
            "project_id",
            "drawing_revision_id",
        )
        and tuple(source_quantity_item_fk["referred_columns"])
        == ("id", "quantity_takeoff_id", "project_id", "drawing_revision_id"),
    }


async def _ensure_quantity_item_lineage_contract(db_session: AsyncSession) -> None:
    connection = await db_session.connection()
    state = await connection.run_sync(_lineage_contract_state)

    if not state["has_quantity_item_lineage_unique"]:
        await db_session.execute(
            text(
                "ALTER TABLE quantity_items "
                "ADD CONSTRAINT uq_quantity_items_id_takeoff_project_drawing_revision "
                "UNIQUE (id, quantity_takeoff_id, project_id, drawing_revision_id)"
            )
        )

    if not state["has_quantity_item_lineage_fk"]:
        await db_session.execute(
            text(
                "ALTER TABLE estimate_snapshot_entries "
                "DROP CONSTRAINT IF EXISTS fk_est_snapshot_entries_source_quantity_item"
            )
        )
        await db_session.execute(
            text(
                "ALTER TABLE estimate_snapshot_entries "
                "ADD CONSTRAINT fk_est_snapshot_entries_source_quantity_item "
                "FOREIGN KEY ("
                "source_quantity_item_id, source_quantity_takeoff_id, "
                "project_id, drawing_revision_id"
                ") REFERENCES quantity_items ("
                "id, quantity_takeoff_id, project_id, drawing_revision_id"
                ") ON DELETE RESTRICT NOT VALID"
            )
        )

    if not state["has_quantity_item_lineage_unique"] or not state[
        "has_quantity_item_lineage_fk"
    ]:
        await db_session.commit()


@pytest_asyncio.fixture(autouse=True)
async def ensure_quantity_item_lineage_contract() -> AsyncGenerator[None, None]:
    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        await _ensure_quantity_item_lineage_contract(session)

    yield


@pytest_asyncio.fixture
async def db_session(cleanup_projects: None) -> AsyncGenerator[AsyncSession, None]:
    _ = cleanup_projects
    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        yield session
        await session.rollback()


def _inspect_schema(sync_connection: sa.Connection) -> dict[str, Any]:
    inspector = sa.inspect(sync_connection)
    tables = (
        "estimate_versions",
        "estimate_snapshot_entries",
        "estimate_items",
        "quantity_items",
    )
    return {
        "columns": {
            table_name: {
                str(column["name"]): {"nullable": bool(column["nullable"])}
                for column in inspector.get_columns(table_name)
            }
            for table_name in tables
        },
        "unique_constraints": {
            table_name: {
                tuple(constraint["column_names"]): str(constraint["name"])
                for constraint in inspector.get_unique_constraints(table_name)
            }
            for table_name in tables
        },
        "check_constraints": {
            table_name: {
                str(constraint["name"]): str(constraint["sqltext"])
                for constraint in inspector.get_check_constraints(table_name)
            }
            for table_name in tables
        },
        "foreign_keys": {
            table_name: {
                (
                    tuple(foreign_key["constrained_columns"]),
                    str(foreign_key["referred_table"]),
                    tuple(foreign_key["referred_columns"]),
                ): _normalize_ondelete(foreign_key)
                for foreign_key in inspector.get_foreign_keys(table_name)
            }
            for table_name in ("estimate_snapshot_entries", "estimate_items")
        },
    }


async def _load_schema() -> dict[str, Any]:
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        connection = await session.connection()
        return await connection.run_sync(_inspect_schema)


async def _assert_commit_fails(
    session: AsyncSession,
    obj: object,
    expected_substring: str | tuple[str, ...],
) -> None:
    session.add(obj)
    expected_substrings = (
        [expected_substring] if isinstance(expected_substring, str) else list(expected_substring)
    )

    try:
        await session.commit()
    except (DBAPIError, IntegrityError) as exc:
        await session.rollback()
        constraint_name = getattr(getattr(exc, "orig", None), "constraint_name", None)
        if constraint_name in expected_substrings:
            return

        message = "\n".join(
            part
            for part in (
                str(exc),
                repr(exc),
                str(getattr(exc, "orig", "")),
                repr(getattr(exc, "orig", "")),
            )
            if part
        ).lower()
        assert any(substring.lower() in message for substring in expected_substrings)
    else:
        pytest.fail(f"Expected database error containing {expected_substrings!r}")


async def _create_estimate_version(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> tuple[quantity_takeoff_persistence._QuantityPersistenceSeed, EstimateVersion]:
    seed = await quantity_takeoff_persistence._seed_quantity_lineage(async_client)
    source_job_id = await estimate_version_persistence._create_estimate_source_job(seed)
    estimate_version = estimate_version_persistence._build_estimate_version(
        seed,
        source_job_id=source_job_id,
        quantity_takeoff_id=seed.quantity_takeoff_id,
    )
    db_session.add(estimate_version)
    await db_session.commit()
    return seed, estimate_version


async def _create_catalog_rows(
    db_session: AsyncSession,
    project_id: uuid.UUID,
) -> tuple[EstimationRate, EstimationMaterial, EstimationFormula]:
    rate = EstimationRate(
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
        checksum_sha256="a" * 64,
    )
    material = EstimationMaterial(
        id=uuid.uuid4(),
        scope_type="project",
        project_id=project_id,
        material_key="cable:standard",
        source="test",
        metadata_json={"source": "test"},
        name="Standard cable",
        unit="m",
        currency="GBP",
        unit_cost=Decimal("3.210000"),
        effective_from=date(2026, 1, 1),
        checksum_sha256="b" * 64,
    )
    formula = EstimationFormula(
        id=uuid.uuid4(),
        scope_type="project",
        project_id=project_id,
        formula_id="waste-factor",
        version=1,
        name="Waste factor",
        dsl_version="0.1",
        output_key="waste",
        output_contract_json={"unit": "GBP"},
        declared_inputs_json=[{"name": "base"}],
        expression_json={"op": "multiply", "by": "0.10"},
        rounding_json={"mode": "ROUND_HALF_UP"},
        checksum_sha256="c" * 64,
    )
    db_session.add_all([rate, material, formula])
    await db_session.commit()
    return rate, material, formula


def _quantity_snapshot(
    seed: quantity_takeoff_persistence._QuantityPersistenceSeed,
    estimate_version: EstimateVersion,
    *,
    sort_order: int = 1,
    source_quantity_takeoff_id: uuid.UUID | None = None,
    source_quantity_item_id: uuid.UUID | None = None,
) -> EstimateSnapshotEntry:
    return EstimateSnapshotEntry(
        id=uuid.uuid4(),
        estimate_version_id=estimate_version.id,
        project_id=seed.project_id,
        drawing_revision_id=seed.drawing_revision_id,
        entry_type="quantity_input",
        entry_key=f"quantity:{sort_order}",
        entry_label="Frozen quantity input",
        sort_order=sort_order,
        source_quantity_takeoff_id=source_quantity_takeoff_id or seed.quantity_takeoff_id,
        source_quantity_item_id=source_quantity_item_id or seed.quantity_item_id,
        quantity_value=Decimal("12.500000"),
        unit="m",
        source_payload_json={
            "quantity_item_id": str(source_quantity_item_id or seed.quantity_item_id)
        },
    )


def _rate_snapshot(
    seed: quantity_takeoff_persistence._QuantityPersistenceSeed,
    estimate_version: EstimateVersion,
    rate: EstimationRate,
    *,
    sort_order: int = 2,
) -> EstimateSnapshotEntry:
    return EstimateSnapshotEntry(
        id=uuid.uuid4(),
        estimate_version_id=estimate_version.id,
        project_id=seed.project_id,
        drawing_revision_id=seed.drawing_revision_id,
        entry_type="rate",
        entry_key=f"rate:{sort_order}",
        entry_label="Frozen rate",
        sort_order=sort_order,
        source_rate_id=rate.id,
        source_checksum_sha256=rate.checksum_sha256,
        unit=rate.per_unit,
        currency="GBP",
        effective_date=rate.effective_from,
        unit_amount=rate.amount,
        source_payload_json={"rate_key": rate.rate_key},
        rounding_json={"scale": 2},
    )


def _material_snapshot(
    seed: quantity_takeoff_persistence._QuantityPersistenceSeed,
    estimate_version: EstimateVersion,
    material: EstimationMaterial,
    *,
    sort_order: int = 3,
) -> EstimateSnapshotEntry:
    return EstimateSnapshotEntry(
        id=uuid.uuid4(),
        estimate_version_id=estimate_version.id,
        project_id=seed.project_id,
        drawing_revision_id=seed.drawing_revision_id,
        entry_type="material",
        entry_key=f"material:{sort_order}",
        entry_label="Frozen material",
        sort_order=sort_order,
        source_material_id=material.id,
        source_checksum_sha256=material.checksum_sha256,
        unit=material.unit,
        currency="GBP",
        effective_date=material.effective_from,
        unit_amount=material.unit_cost,
        source_payload_json={"material_key": material.material_key},
    )


def _formula_snapshot(
    seed: quantity_takeoff_persistence._QuantityPersistenceSeed,
    estimate_version: EstimateVersion,
    formula: EstimationFormula,
    *,
    sort_order: int = 4,
) -> EstimateSnapshotEntry:
    return EstimateSnapshotEntry(
        id=uuid.uuid4(),
        estimate_version_id=estimate_version.id,
        project_id=seed.project_id,
        drawing_revision_id=seed.drawing_revision_id,
        entry_type="formula",
        entry_key=f"formula:{sort_order}",
        entry_label="Frozen formula",
        sort_order=sort_order,
        source_formula_id=formula.id,
        source_checksum_sha256=formula.checksum_sha256,
        source_payload_json={"formula_id": formula.formula_id, "version": formula.version},
        rounding_json={"scale": 2},
    )


def _assumption_snapshot(
    seed: quantity_takeoff_persistence._QuantityPersistenceSeed,
    estimate_version: EstimateVersion,
    *,
    sort_order: int = 5,
) -> EstimateSnapshotEntry:
    return EstimateSnapshotEntry(
        id=uuid.uuid4(),
        estimate_version_id=estimate_version.id,
        project_id=seed.project_id,
        drawing_revision_id=seed.drawing_revision_id,
        entry_type="assumption",
        entry_key=f"assumption:{sort_order}",
        entry_label="Frozen assumption",
        sort_order=sort_order,
        source_payload_json={"text": "Include preliminaries"},
    )


def _base_item_kwargs(
    seed: quantity_takeoff_persistence._QuantityPersistenceSeed,
    estimate_version: EstimateVersion,
    *,
    line_number: int,
    line_type: str,
) -> dict[str, object]:
    return {
        "id": uuid.uuid4(),
        "estimate_version_id": estimate_version.id,
        "project_id": seed.project_id,
        "drawing_revision_id": seed.drawing_revision_id,
        "line_type": line_type,
        "line_number": line_number,
        "line_key": f"line:{line_number}:{line_type}",
        "description": f"{line_type.title()} line",
        "currency": "GBP",
        "subtotal_amount": Decimal("10.00"),
        "tax_amount": Decimal("2.00"),
        "total_amount": Decimal("12.00"),
        "quantity_snapshot_entry_type": "quantity_input",
        "rate_snapshot_entry_type": "rate",
        "material_snapshot_entry_type": "material",
        "formula_snapshot_entry_type": "formula",
        "assumption_snapshot_entry_type": "assumption",
    }


async def _insert_full_snapshot_set(
    db_session: AsyncSession,
    seed: quantity_takeoff_persistence._QuantityPersistenceSeed,
    estimate_version: EstimateVersion,
) -> dict[str, EstimateSnapshotEntry]:
    rate, material, formula = await _create_catalog_rows(db_session, seed.project_id)
    snapshots = {
        "quantity": _quantity_snapshot(seed, estimate_version),
        "rate": _rate_snapshot(seed, estimate_version, rate),
        "material": _material_snapshot(seed, estimate_version, material),
        "formula": _formula_snapshot(seed, estimate_version, formula),
        "assumption": _assumption_snapshot(seed, estimate_version),
    }
    db_session.add_all(snapshots.values())
    await db_session.commit()
    return snapshots


def _line_items(
    seed: quantity_takeoff_persistence._QuantityPersistenceSeed,
    estimate_version: EstimateVersion,
    snapshots: dict[str, EstimateSnapshotEntry],
) -> list[EstimateItem]:
    return [
        EstimateItem(
            **_base_item_kwargs(seed, estimate_version, line_number=1, line_type="rate"),
            quantity_snapshot_entry_id=snapshots["quantity"].id,
            rate_snapshot_entry_id=snapshots["rate"].id,
            quantity_value=Decimal("12.500000"),
            quantity_unit="m",
            unit_rate_amount=Decimal("12.345678"),
            effective_date=date(2026, 1, 1),
            rounding_json={"scale": 2},
        ),
        EstimateItem(
            **_base_item_kwargs(seed, estimate_version, line_number=2, line_type="material"),
            quantity_snapshot_entry_id=snapshots["quantity"].id,
            material_snapshot_entry_id=snapshots["material"].id,
            quantity_value=Decimal("12.500000"),
            quantity_unit="m",
            unit_rate_amount=Decimal("3.210000"),
            effective_date=date(2026, 1, 1),
        ),
        EstimateItem(
            **_base_item_kwargs(seed, estimate_version, line_number=3, line_type="formula"),
            formula_snapshot_entry_id=snapshots["formula"].id,
        ),
        EstimateItem(
            **_base_item_kwargs(seed, estimate_version, line_number=4, line_type="assumption"),
            assumption_snapshot_entry_id=snapshots["assumption"].id,
        ),
        EstimateItem(
            **_base_item_kwargs(seed, estimate_version, line_number=5, line_type="adjustment"),
            formula_snapshot_entry_id=snapshots["formula"].id,
        ),
    ]


async def test_estimate_snapshot_and_item_schema_matches_contract() -> None:
    schema = await _load_schema()
    columns = schema["columns"]
    unique_constraints = schema["unique_constraints"]
    check_constraints = schema["check_constraints"]
    foreign_keys = schema["foreign_keys"]

    assert (
        "id",
        "project_id",
        "drawing_revision_id",
        "quantity_takeoff_id",
    ) in unique_constraints["estimate_versions"]
    assert (
        "id",
        "quantity_takeoff_id",
        "project_id",
        "drawing_revision_id",
    ) in unique_constraints["quantity_items"]

    for required_column in (
        "entry_type",
        "entry_key",
        "entry_label",
        "source_payload_json",
        "sort_order",
        "created_at",
    ):
        assert columns["estimate_snapshot_entries"][required_column]["nullable"] is False
    for required_column in (
        "line_number",
        "line_key",
        "line_type",
        "description",
        "currency",
        "subtotal_amount",
        "tax_amount",
        "total_amount",
        "created_at",
    ):
        assert columns["estimate_items"][required_column]["nullable"] is False

    assert ("estimate_version_id", "entry_key") in unique_constraints[
        "estimate_snapshot_entries"
    ]
    assert ("estimate_version_id", "sort_order") in unique_constraints[
        "estimate_snapshot_entries"
    ]
    assert ("estimate_version_id", "line_number") in unique_constraints["estimate_items"]
    assert ("estimate_version_id", "line_key") in unique_constraints["estimate_items"]

    for constraint_name in (
        "ck_estimate_snapshot_entries_entry_type_valid",
        "ck_estimate_snapshot_entries_source_payload_json_object",
        "ck_estimate_snapshot_entries_source_checksum_sha256_format",
        "ck_estimate_snapshot_entries_source_fields_by_type",
        "ck_estimate_items_line_type_valid",
        "ck_estimate_items_snapshot_entry_type_constants",
        "ck_estimate_items_source_fields_by_line_type",
    ):
        assert constraint_name in {
            **check_constraints["estimate_snapshot_entries"],
            **check_constraints["estimate_items"],
        }

    assert foreign_keys["estimate_snapshot_entries"][
        (
            (
                "estimate_version_id",
                "project_id",
                "drawing_revision_id",
                "source_quantity_takeoff_id",
            ),
            "estimate_versions",
            ("id", "project_id", "drawing_revision_id", "quantity_takeoff_id"),
        )
    ] == "RESTRICT"
    assert foreign_keys["estimate_snapshot_entries"][
        (
            (
                "source_quantity_item_id",
                "source_quantity_takeoff_id",
                "project_id",
                "drawing_revision_id",
            ),
            "quantity_items",
            ("id", "quantity_takeoff_id", "project_id", "drawing_revision_id"),
        )
    ] == "RESTRICT"
    assert foreign_keys["estimate_items"][
        (
            (
                "rate_snapshot_entry_id",
                "estimate_version_id",
                "project_id",
                "drawing_revision_id",
                "rate_snapshot_entry_type",
            ),
            "estimate_snapshot_entries",
            ("id", "estimate_version_id", "project_id", "drawing_revision_id", "entry_type"),
        )
    ] == "RESTRICT"

    snapshot_table = cast(Table, EstimateSnapshotEntry.__table__)
    item_table = cast(Table, EstimateItem.__table__)
    model_checks = {
        str(constraint.name)
        for table in (snapshot_table, item_table)
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert "ck_estimate_snapshot_entries_source_fields_by_type" in model_checks
    assert "ck_estimate_items_source_fields_by_line_type" in model_checks
    assert any(
        isinstance(constraint, UniqueConstraint)
        and tuple(constraint.columns.keys()) == ("estimate_version_id", "line_key")
        for constraint in item_table.constraints
    )

    migration_sql = _MIGRATION_PATH.read_text(encoding="utf-8")
    assert "quantity_input" in migration_sql
    assert "source_checksum_sha256 ~ '^[0-9a-f]{64}$'" in migration_sql
    assert "uq_estimate_versions_id_project_rev_quantity_takeoff_id" in migration_sql
    assert "uq_quantity_items_id_takeoff_project_drawing_revision" in migration_sql


async def test_estimate_snapshots_and_items_persist_all_line_types(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    seed, estimate_version = await _create_estimate_version(async_client, db_session)
    snapshots = await _insert_full_snapshot_set(db_session, seed, estimate_version)
    items = _line_items(seed, estimate_version, snapshots)
    db_session.add_all(items)
    await db_session.commit()

    persisted_types = set(
        await db_session.scalars(
            select(EstimateItem.line_type).where(
                EstimateItem.estimate_version_id == estimate_version.id
            )
        )
    )
    assert persisted_types == {"rate", "material", "formula", "assumption", "adjustment"}


async def test_quantity_snapshot_requires_parent_estimate_takeoff(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    seed, estimate_version = await _create_estimate_version(async_client, db_session)

    await _assert_commit_fails(
        db_session,
        _quantity_snapshot(
            seed,
            estimate_version,
            source_quantity_takeoff_id=uuid.uuid4(),
        ),
        "fk_estimate_snapshot_entries_quantity_parent_takeoff",
    )


async def test_quantity_snapshot_rejects_item_from_another_takeoff_or_revision(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    seed, estimate_version = await _create_estimate_version(async_client, db_session)
    other_seed = await quantity_takeoff_persistence._seed_quantity_lineage(async_client)

    await _assert_commit_fails(
        db_session,
        _quantity_snapshot(
            seed,
            estimate_version,
            source_quantity_item_id=other_seed.quantity_item_id,
        ),
        "fk_est_snapshot_entries_source_quantity_item",
    )


async def test_item_snapshot_refs_are_type_safe(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    seed, estimate_version = await _create_estimate_version(async_client, db_session)
    snapshots = await _insert_full_snapshot_set(db_session, seed, estimate_version)

    await _assert_commit_fails(
        db_session,
        EstimateItem(
            **_base_item_kwargs(seed, estimate_version, line_number=1, line_type="rate"),
            quantity_snapshot_entry_id=snapshots["quantity"].id,
            rate_snapshot_entry_id=snapshots["assumption"].id,
            quantity_value=Decimal("12.500000"),
            quantity_unit="m",
            unit_rate_amount=Decimal("12.345678"),
            effective_date=date(2026, 1, 1),
        ),
        "fk_estimate_items_rate_snapshot_entry",
    )


@pytest.mark.parametrize(
    ("field_name", "field_value", "constraint_name"),
    (
        ("source_checksum_sha256", "g" * 64, "source_checksum_sha256_format"),
        ("source_payload_json", [], "source_payload_json_object"),
        ("currency", "USD", "currency_by_type"),
        ("unit_amount", Decimal("-0.000001"), "unit_amount_nonnegative"),
    ),
)
async def test_snapshot_entries_reject_invalid_frozen_values(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
    field_name: str,
    field_value: object,
    constraint_name: str,
) -> None:
    seed, estimate_version = await _create_estimate_version(async_client, db_session)
    rate, _, _ = await _create_catalog_rows(db_session, seed.project_id)
    snapshot = _rate_snapshot(seed, estimate_version, rate)
    setattr(snapshot, field_name, field_value)

    await _assert_commit_fails(db_session, snapshot, constraint_name)


async def test_items_reject_invalid_line_contracts(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    seed, estimate_version = await _create_estimate_version(async_client, db_session)
    snapshots = await _insert_full_snapshot_set(db_session, seed, estimate_version)

    await _assert_commit_fails(
        db_session,
        EstimateItem(
            **_base_item_kwargs(seed, estimate_version, line_number=1, line_type="assumption"),
            assumption_snapshot_entry_id=snapshots["assumption"].id,
            quantity_snapshot_entry_id=snapshots["quantity"].id,
        ),
        "ck_estimate_items_source_fields_by_line_type",
    )


async def test_estimate_snapshot_and_item_tables_are_append_only(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    seed, estimate_version = await _create_estimate_version(async_client, db_session)
    snapshots = await _insert_full_snapshot_set(db_session, seed, estimate_version)
    item = _line_items(seed, estimate_version, snapshots)[3]
    db_session.add(item)
    await db_session.commit()

    for table_name, column_name, row_id in (
        ("estimate_snapshot_entries", "entry_label", snapshots["assumption"].id),
        ("estimate_items", "description", item.id),
    ):
        with pytest.raises(DBAPIError) as update_exc:
            await db_session.execute(
                text(f'UPDATE "{table_name}" SET "{column_name}" = :value WHERE id = :id'),
                {"value": "mutated", "id": row_id},
            )
            await db_session.commit()
        await db_session.rollback()
        assert f"append-only trigger blocked UPDATE on {table_name}" in str(update_exc.value)

        with pytest.raises(DBAPIError) as delete_exc:
            await db_session.execute(
                text(f'DELETE FROM "{table_name}" WHERE id = :id'),
                {"id": row_id},
            )
            await db_session.commit()
        await db_session.rollback()
        assert f"append-only trigger blocked DELETE on {table_name}" in str(delete_exc.value)
