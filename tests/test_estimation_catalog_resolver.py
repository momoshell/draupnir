from __future__ import annotations

import importlib
from datetime import date
from typing import Any, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.estimating.catalog.contracts import (
    CatalogFormulaRef,
    CatalogMaterialAutoSelectRequest,
    CatalogRateAutoSelectRequest,
    CatalogRateRef,
)
from app.estimating.catalog.resolver import resolve_formula, resolve_material, resolve_rate
from app.estimating.catalog.selection import CatalogSelectionError
from app.models.estimation_catalog import EstimationRateSupersession
from tests.test_estimation_catalog_persistence import (
    _build_formula,
    _build_material,
    _build_rate,
    _checksum,
    _create_project,
    _persist,
)

requires_database = importlib.import_module(
    "tests.test_estimation_catalog_persistence"
).requires_database
pytestmark = [pytest.mark.asyncio, cast(Any, requires_database)]


async def test_resolve_formula_explicit_ref_uses_row_uuid_and_returns_snapshot_metadata(
    db_session: AsyncSession,
) -> None:
    project_id = await _create_project(db_session)
    formula = _build_formula(
        scope_type="project",
        project_id=project_id,
        formula_id="formula.explicit",
        name="Explicit formula",
        output_key="estimate.paint_total",
        output_contract_json={"type": "money", "currency": "GBP"},
        declared_inputs_json=[{"key": "quantity", "type": "decimal"}],
        expression_json={"op": "literal", "value": "12.34"},
        rounding_json={"mode": "ROUND_HALF_UP", "scale": 2},
        checksum_sha256=_checksum("formula-explicit"),
    )
    await _persist(db_session, formula)

    resolved = await resolve_formula(
        db_session,
        ref=CatalogFormulaRef(id=formula.id, checksum_sha256=formula.checksum_sha256),
    )

    assert resolved.definition_id == formula.id
    assert resolved.scope_type == "project"
    assert resolved.project_id == project_id
    assert resolved.formula_id == "formula.explicit"
    assert resolved.version == formula.version
    assert resolved.name == "Explicit formula"
    assert resolved.dsl_version == formula.dsl_version
    assert resolved.output_key == "estimate.paint_total"
    assert resolved.output_contract == {"type": "money", "currency": "GBP"}
    assert resolved.declared_inputs == [{"key": "quantity", "type": "decimal"}]
    assert resolved.expression == {"op": "literal", "value": "12.34"}
    assert resolved.rounding == {"mode": "ROUND_HALF_UP", "scale": 2}
    assert resolved.checksum_sha256 == formula.checksum_sha256


async def test_resolve_material_auto_prefers_project_scope_over_global(
    db_session: AsyncSession,
) -> None:
    project_id = await _create_project(db_session)
    global_material = _build_material(
        material_key="material.precedence",
        checksum_sha256=_checksum("material-global"),
    )
    project_material = _build_material(
        scope_type="project",
        project_id=project_id,
        material_key="material.precedence",
        unit_cost="18.750000",
        checksum_sha256=_checksum("material-project"),
    )
    await _persist(db_session, global_material, project_material)

    resolved = await resolve_material(
        db_session,
        auto=CatalogMaterialAutoSelectRequest(
            project_id=project_id,
            material_key="material.precedence",
            unit=project_material.unit,
            currency=project_material.currency,
            as_of=date(2026, 1, 15),
        ),
    )

    assert resolved.id == project_material.id
    assert resolved.project_id == project_id
    assert resolved.value == project_material.unit_cost


async def test_resolve_rate_rejects_stale_explicit_ref_and_falls_back_to_successor(
    db_session: AsyncSession,
) -> None:
    predecessor = _build_rate(
        rate_key="labour.superseded",
        checksum_sha256=_checksum("rate-predecessor"),
    )
    successor = _build_rate(
        rate_key="labour.superseded",
        amount="11.500000",
        checksum_sha256=_checksum("rate-successor"),
    )
    await _persist(db_session, predecessor, successor)
    await _persist(
        db_session,
        EstimationRateSupersession(
            predecessor_rate_id=predecessor.id,
            successor_rate_id=successor.id,
        ),
    )

    with pytest.raises(CatalogSelectionError, match="stale or missing"):
        await resolve_rate(
            db_session,
            ref=CatalogRateRef(
                id=predecessor.id,
                checksum_sha256=predecessor.checksum_sha256,
            ),
        )

    resolved = await resolve_rate(
        db_session,
        auto=CatalogRateAutoSelectRequest(
            project_id=None,
            rate_key="labour.superseded",
            item_type=successor.item_type,
            unit=successor.per_unit,
            currency=successor.currency,
            as_of=date(2026, 1, 15),
        ),
    )

    assert resolved.id == successor.id
    assert resolved.superseded_by_id is None
    assert resolved.value == successor.amount
