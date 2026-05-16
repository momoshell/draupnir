from __future__ import annotations

from typing import Any

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.estimating.catalog.contracts import (
    CatalogFormulaAutoSelectRequest,
    CatalogFormulaMatch,
    CatalogFormulaRef,
    CatalogMaterialAutoSelectRequest,
    CatalogMaterialMatch,
    CatalogMaterialRef,
    CatalogRateAutoSelectRequest,
    CatalogRateMatch,
    CatalogRateRef,
)
from app.estimating.catalog.selection import (
    SelectedFormula,
    select_formula,
    select_material,
    select_rate,
)
from app.models.estimation_catalog import (
    EstimationFormula,
    EstimationFormulaSupersession,
    EstimationMaterial,
    EstimationMaterialSupersession,
    EstimationRate,
    EstimationRateSupersession,
)


async def resolve_rate(
    session: AsyncSession,
    *,
    auto: CatalogRateAutoSelectRequest | None = None,
    ref: CatalogRateRef | None = None,
) -> CatalogRateMatch:
    query = _build_rate_query(auto=auto, ref=ref)
    result = await session.execute(query)
    candidates = [
        _map_rate_candidate(rate_entry, superseded_by_id)
        for rate_entry, superseded_by_id in result.all()
    ]
    return select_rate(candidates, auto=auto, ref=ref)


async def resolve_material(
    session: AsyncSession,
    *,
    auto: CatalogMaterialAutoSelectRequest | None = None,
    ref: CatalogMaterialRef | None = None,
) -> CatalogMaterialMatch:
    query = _build_material_query(auto=auto, ref=ref)
    result = await session.execute(query)
    candidates = [
        _map_material_candidate(material_entry, superseded_by_id)
        for material_entry, superseded_by_id in result.all()
    ]
    return select_material(candidates, auto=auto, ref=ref)


async def resolve_formula(
    session: AsyncSession,
    *,
    auto: CatalogFormulaAutoSelectRequest | None = None,
    ref: CatalogFormulaRef | None = None,
) -> SelectedFormula:
    query = _build_formula_query(auto=auto, ref=ref)
    result = await session.execute(query)
    candidates = [
        _map_formula_candidate(formula_definition, superseded_by_id)
        for formula_definition, superseded_by_id in result.all()
    ]
    return select_formula(candidates, auto=auto, ref=ref)


def _build_rate_query(
    *,
    auto: CatalogRateAutoSelectRequest | None,
    ref: CatalogRateRef | None,
) -> Select[tuple[EstimationRate, Any]]:
    query = (
        select(EstimationRate, EstimationRateSupersession.successor_rate_id)
        .outerjoin(
            EstimationRateSupersession,
            EstimationRateSupersession.predecessor_rate_id == EstimationRate.id,
        )
    )
    if ref is not None:
        return query.where(EstimationRate.id == ref.id)
    if auto is None:
        return query
    return query.where(
        EstimationRate.rate_key == auto.rate_key,
        EstimationRate.item_type == auto.item_type,
        EstimationRate.per_unit == auto.unit,
        EstimationRate.currency == auto.currency,
    )


def _build_material_query(
    *,
    auto: CatalogMaterialAutoSelectRequest | None,
    ref: CatalogMaterialRef | None,
) -> Select[tuple[EstimationMaterial, Any]]:
    query = (
        select(EstimationMaterial, EstimationMaterialSupersession.successor_material_id)
        .outerjoin(
            EstimationMaterialSupersession,
            EstimationMaterialSupersession.predecessor_material_id == EstimationMaterial.id,
        )
    )
    if ref is not None:
        return query.where(EstimationMaterial.id == ref.id)
    if auto is None:
        return query
    return query.where(
        EstimationMaterial.material_key == auto.material_key,
        EstimationMaterial.unit == auto.unit,
        EstimationMaterial.currency == auto.currency,
    )


def _build_formula_query(
    *,
    auto: CatalogFormulaAutoSelectRequest | None,
    ref: CatalogFormulaRef | None,
) -> Select[tuple[EstimationFormula, Any]]:
    query = (
        select(EstimationFormula, EstimationFormulaSupersession.successor_formula_id)
        .outerjoin(
            EstimationFormulaSupersession,
            EstimationFormulaSupersession.predecessor_formula_id == EstimationFormula.id,
        )
    )
    if ref is not None:
        return query.where(EstimationFormula.id == ref.id)
    if auto is None:
        return query

    query = query.where(EstimationFormula.formula_id == auto.formula_id)
    if auto.version is not None:
        query = query.where(EstimationFormula.version == auto.version)
    return query


def _map_rate_candidate(
    entry: EstimationRate,
    superseded_by_id: Any,
) -> CatalogRateMatch:
    return CatalogRateMatch(
        id=entry.id,
        project_id=entry.project_id,
        rate_key=entry.rate_key,
        item_type=entry.item_type,
        unit=entry.per_unit,
        currency=entry.currency,
        value=entry.amount,
        effective_start=entry.effective_from,
        effective_end=entry.effective_to,
        checksum_sha256=entry.checksum_sha256,
        superseded_by_id=superseded_by_id,
        metadata=entry.metadata_json,
    )


def _map_material_candidate(
    entry: EstimationMaterial,
    superseded_by_id: Any,
) -> CatalogMaterialMatch:
    return CatalogMaterialMatch(
        id=entry.id,
        project_id=entry.project_id,
        material_key=entry.material_key,
        unit=entry.unit,
        currency=entry.currency,
        value=entry.unit_cost,
        effective_start=entry.effective_from,
        effective_end=entry.effective_to,
        checksum_sha256=entry.checksum_sha256,
        superseded_by_id=superseded_by_id,
        metadata=entry.metadata_json,
    )


def _map_formula_candidate(
    definition: EstimationFormula,
    superseded_by_id: Any,
) -> CatalogFormulaMatch:
    return CatalogFormulaMatch(
        id=definition.id,
        scope_type=definition.scope_type,
        formula_id=definition.formula_id,
        version=definition.version,
        project_id=definition.project_id,
        name=definition.name,
        dsl_version=definition.dsl_version,
        output_key=definition.output_key,
        output_contract=definition.output_contract_json,
        declared_inputs=definition.declared_inputs_json,
        checksum_sha256=definition.checksum_sha256,
        expression=definition.expression_json,
        rounding=definition.rounding_json,
        superseded_by_id=superseded_by_id,
    )
