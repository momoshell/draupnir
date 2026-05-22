"""Estimation catalog API endpoints."""

from __future__ import annotations

from datetime import date, datetime
from typing import Annotated, Any, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import Response
from pydantic import BaseModel
from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from app.api.idempotency import (
    IdempotencyReservation,
    build_idempotency_fingerprint,
    claim_idempotency_response,
    complete_idempotency_response,
    get_idempotency_key,
    replay_idempotency_response,
)
from app.api.pagination import decode_cursor_payload, encode_cursor_payload, raise_invalid_cursor
from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response, raise_not_found
from app.db.session import get_db
from app.estimating.catalog.api_checksums import (
    formula_checksum_sha256,
    material_checksum_sha256,
    rate_checksum_sha256,
)
from app.models.estimation_catalog import (
    EstimationFormula,
    EstimationFormulaSupersession,
    EstimationMaterial,
    EstimationMaterialSupersession,
    EstimationRate,
    EstimationRateSupersession,
)
from app.models.project import Project
from app.schemas.estimation_catalog import (
    CatalogCurrencyCode,
    CatalogScopeType,
    EstimationFormulaCreate,
    EstimationFormulaListResponse,
    EstimationFormulaRead,
    EstimationMaterialCreate,
    EstimationMaterialListResponse,
    EstimationMaterialRead,
    EstimationRateCreate,
    EstimationRateListResponse,
    EstimationRateRead,
)

estimation_router = APIRouter()


class _TimestampCursor(BaseModel):
    created_at: datetime
    id: UUID


def _encode_timestamp_cursor(created_at: datetime, row_id: UUID) -> str:
    return encode_cursor_payload(
        {
            "created_at": created_at.isoformat(),
            "id": str(row_id),
        }
    )


def _decode_timestamp_cursor(cursor: str) -> tuple[datetime, UUID]:
    try:
        payload = _TimestampCursor.model_validate(decode_cursor_payload(cursor))
    except Exception as exc:  # pragma: no cover - normalized by helper
        raise_invalid_cursor(exc)
    return payload.created_at, payload.id


async def _get_active_project_or_404(db: AsyncSession, project_id: UUID) -> Project:
    project = (
        await db.execute(
            select(Project)
            .where((Project.id == project_id) & (Project.deleted_at.is_(None)))
            .with_for_update()
        )
    ).scalar_one_or_none()
    if project is None:
        raise_not_found("Project", str(project_id))
    assert project is not None
    return project


def _active_project_visibility(entry_project_id: Any) -> Any:
    return or_(
        entry_project_id.is_(None),
        select(Project.id)
        .where((Project.id == entry_project_id) & (Project.deleted_at.is_(None)))
        .exists(),
    )


def _catalog_conflict_body(kind: str) -> dict[str, Any]:
    return create_error_response(
        code=ErrorCode.DB_CONFLICT,
        message=f"{kind} conflicts with an existing persisted catalog record.",
        details=None,
    )


async def _raise_catalog_conflict(
    db: AsyncSession,
    reservation: IdempotencyReservation | None,
    *,
    kind: str,
) -> None:
    error_body = _catalog_conflict_body(kind)
    if reservation is not None:
        await complete_idempotency_response(
            db,
            reservation,
            status_code=status.HTTP_409_CONFLICT,
            response_body=error_body,
        )
        await db.commit()
    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=error_body)


def _raise_input_invalid(message: str) -> None:
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=create_error_response(
            code=ErrorCode.INPUT_INVALID,
            message=message,
            details=None,
        ),
    )


def _serialize_rate(
    rate: EstimationRate,
    *,
    superseded_by_id: UUID | None,
    supersedes_rate_id: UUID | None,
) -> EstimationRateRead:
    return EstimationRateRead(
        id=rate.id,
        scope_type=cast(CatalogScopeType, rate.scope_type),
        project_id=rate.project_id,
        rate_key=rate.rate_key,
        source=rate.source,
        metadata_json=rate.metadata_json,
        name=rate.name,
        item_type=rate.item_type,
        per_unit=rate.per_unit,
        currency=cast(CatalogCurrencyCode, rate.currency),
        amount=rate.amount,
        effective_from=rate.effective_from,
        effective_to=rate.effective_to,
        checksum_sha256=rate.checksum_sha256,
        superseded_by_id=superseded_by_id,
        supersedes_rate_id=supersedes_rate_id,
        created_at=rate.created_at,
    )


def _serialize_material(
    material: EstimationMaterial,
    *,
    superseded_by_id: UUID | None,
    supersedes_material_id: UUID | None,
) -> EstimationMaterialRead:
    return EstimationMaterialRead(
        id=material.id,
        scope_type=cast(CatalogScopeType, material.scope_type),
        project_id=material.project_id,
        material_key=material.material_key,
        source=material.source,
        metadata_json=material.metadata_json,
        name=material.name,
        unit=material.unit,
        currency=cast(CatalogCurrencyCode, material.currency),
        unit_cost=material.unit_cost,
        effective_from=material.effective_from,
        effective_to=material.effective_to,
        checksum_sha256=material.checksum_sha256,
        superseded_by_id=superseded_by_id,
        supersedes_material_id=supersedes_material_id,
        created_at=material.created_at,
    )


def _serialize_formula(
    formula: EstimationFormula,
    *,
    superseded_by_id: UUID | None,
    supersedes_formula_id: UUID | None,
) -> EstimationFormulaRead:
    return EstimationFormulaRead(
        id=formula.id,
        scope_type=cast(CatalogScopeType, formula.scope_type),
        project_id=formula.project_id,
        formula_id=formula.formula_id,
        version=formula.version,
        name=formula.name,
        dsl_version=formula.dsl_version,
        output_key=formula.output_key,
        output_contract_json=formula.output_contract_json,
        declared_inputs_json=formula.declared_inputs_json,
        expression_json=formula.expression_json,
        rounding_json=formula.rounding_json,
        checksum_sha256=formula.checksum_sha256,
        superseded_by_id=superseded_by_id,
        supersedes_formula_id=supersedes_formula_id,
        created_at=formula.created_at,
    )


def _rate_query() -> tuple[Any, Any]:
    superseded_link = aliased(EstimationRateSupersession)
    supersedes_link = aliased(EstimationRateSupersession)
    statement = (
        select(
            EstimationRate,
            superseded_link.successor_rate_id,
            supersedes_link.predecessor_rate_id,
        )
        .outerjoin(
            superseded_link,
            superseded_link.predecessor_rate_id == EstimationRate.id,
        )
        .outerjoin(
            supersedes_link,
            supersedes_link.successor_rate_id == EstimationRate.id,
        )
        .where(_active_project_visibility(EstimationRate.project_id))
    )
    return statement, superseded_link.successor_rate_id


def _material_query() -> tuple[Any, Any]:
    superseded_link = aliased(EstimationMaterialSupersession)
    supersedes_link = aliased(EstimationMaterialSupersession)
    statement = (
        select(
            EstimationMaterial,
            superseded_link.successor_material_id,
            supersedes_link.predecessor_material_id,
        )
        .outerjoin(
            superseded_link,
            superseded_link.predecessor_material_id == EstimationMaterial.id,
        )
        .outerjoin(
            supersedes_link,
            supersedes_link.successor_material_id == EstimationMaterial.id,
        )
        .where(_active_project_visibility(EstimationMaterial.project_id))
    )
    return statement, superseded_link.successor_material_id


def _formula_query() -> tuple[Any, Any]:
    superseded_link = aliased(EstimationFormulaSupersession)
    supersedes_link = aliased(EstimationFormulaSupersession)
    statement = (
        select(
            EstimationFormula,
            superseded_link.successor_formula_id,
            supersedes_link.predecessor_formula_id,
        )
        .outerjoin(
            superseded_link,
            superseded_link.predecessor_formula_id == EstimationFormula.id,
        )
        .outerjoin(
            supersedes_link,
            supersedes_link.successor_formula_id == EstimationFormula.id,
        )
        .where(_active_project_visibility(EstimationFormula.project_id))
    )
    return statement, superseded_link.successor_formula_id


async def _get_rate_predecessor_or_404(db: AsyncSession, rate_id: UUID) -> EstimationRate:
    rate = (
        await db.execute(
            select(EstimationRate)
            .where(
                (EstimationRate.id == rate_id)
                & _active_project_visibility(EstimationRate.project_id)
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if rate is None:
        raise_not_found("Rate catalog entry", str(rate_id))
    assert rate is not None
    return rate


async def _get_material_predecessor_or_404(
    db: AsyncSession,
    material_id: UUID,
) -> EstimationMaterial:
    material = (
        await db.execute(
            select(EstimationMaterial)
            .where(
                (EstimationMaterial.id == material_id)
                & _active_project_visibility(EstimationMaterial.project_id)
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if material is None:
        raise_not_found("Material catalog entry", str(material_id))
    assert material is not None
    return material


async def _get_formula_predecessor_or_404(
    db: AsyncSession,
    formula_id: UUID,
) -> EstimationFormula:
    formula = (
        await db.execute(
            select(EstimationFormula)
            .where(
                (EstimationFormula.id == formula_id)
                & _active_project_visibility(EstimationFormula.project_id)
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if formula is None:
        raise_not_found("Formula definition", str(formula_id))
    assert formula is not None
    return formula


def _validate_rate_supersession(
    payload: EstimationRateCreate,
    predecessor: EstimationRate,
) -> None:
    if (
        predecessor.scope_type != payload.scope_type
        or predecessor.project_id != payload.project_id
        or predecessor.rate_key != payload.rate_key
        or predecessor.item_type != payload.item_type
        or predecessor.per_unit != payload.per_unit
        or predecessor.currency != payload.currency
    ):
        _raise_input_invalid(
            "supersedes_rate_id must reference a rate entry with the same "
            "scope, project, key, item type, unit, and currency.",
        )


def _validate_material_supersession(
    payload: EstimationMaterialCreate,
    predecessor: EstimationMaterial,
) -> None:
    if (
        predecessor.scope_type != payload.scope_type
        or predecessor.project_id != payload.project_id
        or predecessor.material_key != payload.material_key
        or predecessor.unit != payload.unit
        or predecessor.currency != payload.currency
    ):
        _raise_input_invalid(
            "supersedes_material_id must reference a material entry with the "
            "same scope, project, key, unit, and currency.",
        )


def _validate_formula_supersession(
    payload: EstimationFormulaCreate,
    predecessor: EstimationFormula,
) -> None:
    if (
        predecessor.scope_type != payload.scope_type
        or predecessor.project_id != payload.project_id
        or predecessor.formula_id != payload.formula_id
    ):
        _raise_input_invalid(
            "supersedes_formula_id must reference a formula definition with "
            "the same scope, project, and formula_id.",
        )


def _validate_successor_effective_window(
    predecessor_effective_to: date | None,
    successor_effective_from: date,
    *,
    resource_name: str,
) -> None:
    if predecessor_effective_to is None or predecessor_effective_to > successor_effective_from:
        _raise_input_invalid(
            f"supersedes_{resource_name}_id must reference an entry whose effective "
            "window ends on or before the successor effective_from date.",
        )


@estimation_router.post(
    "/estimation/catalog/rates",
    response_model=EstimationRateRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_rate(
    rate_in: EstimationRateCreate,
    db: Annotated[AsyncSession, Depends(get_db)],
    idempotency_key: Annotated[str | None, Depends(get_idempotency_key)] = None,
) -> EstimationRateRead | Response:
    reservation: IdempotencyReservation | None = None
    fingerprint: str | None = None
    if idempotency_key is not None:
        fingerprint = build_idempotency_fingerprint(
            "estimation.rates.create",
            rate_in.model_dump(mode="json"),
        )
        replay = await replay_idempotency_response(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
        )
        if replay is not None:
            return replay

    if rate_in.project_id is not None:
        await _get_active_project_or_404(db, rate_in.project_id)

    predecessor: EstimationRate | None = None
    if rate_in.supersedes_rate_id is not None:
        predecessor = await _get_rate_predecessor_or_404(db, rate_in.supersedes_rate_id)
        _validate_rate_supersession(rate_in, predecessor)
        _validate_successor_effective_window(
            predecessor.effective_to,
            rate_in.effective_from,
            resource_name="rate",
        )

    if idempotency_key is not None:
        assert fingerprint is not None
        claim = await claim_idempotency_response(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
            method="POST",
            path="/estimation/catalog/rates",
        )
        if isinstance(claim, Response):
            return claim
        assert claim is not None
        reservation = claim

    rate = EstimationRate(
        scope_type=rate_in.scope_type,
        project_id=rate_in.project_id,
        rate_key=rate_in.rate_key,
        source=rate_in.source,
        metadata_json=rate_in.metadata_json,
        name=rate_in.name,
        item_type=rate_in.item_type,
        per_unit=rate_in.per_unit,
        currency=rate_in.currency,
        amount=rate_in.amount,
        effective_from=rate_in.effective_from,
        effective_to=rate_in.effective_to,
        checksum_sha256=rate_checksum_sha256(
            scope_type=rate_in.scope_type,
            project_id=rate_in.project_id,
            rate_key=rate_in.rate_key,
            source=rate_in.source,
            metadata_json=rate_in.metadata_json,
            name=rate_in.name,
            item_type=rate_in.item_type,
            per_unit=rate_in.per_unit,
            currency=rate_in.currency,
            amount=rate_in.amount,
            effective_from=rate_in.effective_from,
            effective_to=rate_in.effective_to,
        ),
    )
    db.add(rate)

    try:
        await db.flush()
        if predecessor is not None:
            db.add(
                EstimationRateSupersession(
                    predecessor_rate_id=predecessor.id,
                    successor_rate_id=rate.id,
                )
            )
            await db.flush()
        await db.refresh(rate)
    except IntegrityError:
        await db.rollback()
        await _raise_catalog_conflict(db, reservation, kind="Rate catalog entry")

    body = _serialize_rate(
        rate,
        superseded_by_id=None,
        supersedes_rate_id=rate_in.supersedes_rate_id,
    )
    response_body = body.model_dump(mode="json")
    idempotent_response: Response | None = None
    if reservation is not None:
        idempotent_response = await complete_idempotency_response(
            db,
            reservation,
            status_code=status.HTTP_201_CREATED,
            response_body=response_body,
        )
    await db.commit()

    if idempotent_response is not None:
        return idempotent_response
    return body


@estimation_router.get(
    "/estimation/catalog/rates/{rate_id}",
    response_model=EstimationRateRead,
)
async def get_rate(
    rate_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> EstimationRateRead:
    statement, _ = _rate_query()
    row = (await db.execute(statement.where(EstimationRate.id == rate_id))).one_or_none()
    if row is None:
        raise_not_found("Rate catalog entry", str(rate_id))
    assert row is not None
    return _serialize_rate(row[0], superseded_by_id=row[1], supersedes_rate_id=row[2])


@estimation_router.get(
    "/estimation/catalog/rates",
    response_model=EstimationRateListResponse,
)
async def list_rates(
    db: Annotated[AsyncSession, Depends(get_db)],
    cursor: Annotated[str | None, Query(description="Opaque pagination cursor")] = None,
    limit: Annotated[int, Query(ge=1, le=200, description="Page size")] = 50,
    scope_type: Annotated[
        CatalogScopeType | None,
        Query(description="Filter by scope type"),
    ] = None,
    project_id: Annotated[UUID | None, Query(description="Filter by owning project id")] = None,
    rate_key: Annotated[str | None, Query(description="Filter by stable rate key")] = None,
    source: Annotated[str | None, Query(description="Filter by source label")] = None,
    item_type: Annotated[str | None, Query(description="Filter by rate item type")] = None,
    per_unit: Annotated[str | None, Query(description="Filter by unit")] = None,
    currency: Annotated[CatalogCurrencyCode | None, Query(description="Filter by currency")] = None,
    as_of: Annotated[
        date | None,
        Query(description="Filter to entries effective on the given date"),
    ] = None,
    include_superseded: Annotated[
        bool,
        Query(description="Include superseded entries; default false hides stale entries"),
    ] = False,
) -> EstimationRateListResponse:
    statement, superseded_by_column = _rate_query()
    if scope_type is not None:
        statement = statement.where(EstimationRate.scope_type == scope_type)
    if project_id is not None:
        statement = statement.where(EstimationRate.project_id == project_id)
    if rate_key is not None:
        statement = statement.where(EstimationRate.rate_key == rate_key)
    if source is not None:
        statement = statement.where(EstimationRate.source == source)
    if item_type is not None:
        statement = statement.where(EstimationRate.item_type == item_type)
    if per_unit is not None:
        statement = statement.where(EstimationRate.per_unit == per_unit)
    if currency is not None:
        statement = statement.where(EstimationRate.currency == currency)
    if as_of is not None:
        statement = statement.where(
            (EstimationRate.effective_from <= as_of)
            & (EstimationRate.effective_to.is_(None) | (EstimationRate.effective_to > as_of))
        )
    if not include_superseded:
        statement = statement.where(superseded_by_column.is_(None))
    if cursor is not None:
        created_at, row_id = _decode_timestamp_cursor(cursor)
        statement = statement.where(
            (EstimationRate.created_at < created_at)
            | ((EstimationRate.created_at == created_at) & (EstimationRate.id < row_id))
        )

    rows = (
        await db.execute(
            statement.order_by(EstimationRate.created_at.desc(), EstimationRate.id.desc()).limit(
                limit + 1
            )
        )
    ).all()
    page = rows[:limit]
    next_cursor = None
    if len(rows) > limit:
        last_row = page[-1]
        next_cursor = _encode_timestamp_cursor(last_row[0].created_at, last_row[0].id)
    return EstimationRateListResponse(
        items=[
            _serialize_rate(row[0], superseded_by_id=row[1], supersedes_rate_id=row[2])
            for row in page
        ],
        next_cursor=next_cursor,
    )


@estimation_router.post(
    "/estimation/catalog/materials",
    response_model=EstimationMaterialRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_material(
    material_in: EstimationMaterialCreate,
    db: Annotated[AsyncSession, Depends(get_db)],
    idempotency_key: Annotated[str | None, Depends(get_idempotency_key)] = None,
) -> EstimationMaterialRead | Response:
    reservation: IdempotencyReservation | None = None
    fingerprint: str | None = None
    if idempotency_key is not None:
        fingerprint = build_idempotency_fingerprint(
            "estimation.materials.create",
            material_in.model_dump(mode="json"),
        )
        replay = await replay_idempotency_response(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
        )
        if replay is not None:
            return replay

    if material_in.project_id is not None:
        await _get_active_project_or_404(db, material_in.project_id)

    predecessor: EstimationMaterial | None = None
    if material_in.supersedes_material_id is not None:
        predecessor = await _get_material_predecessor_or_404(db, material_in.supersedes_material_id)
        _validate_material_supersession(material_in, predecessor)
        _validate_successor_effective_window(
            predecessor.effective_to,
            material_in.effective_from,
            resource_name="material",
        )

    if idempotency_key is not None:
        assert fingerprint is not None
        claim = await claim_idempotency_response(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
            method="POST",
            path="/estimation/catalog/materials",
        )
        if isinstance(claim, Response):
            return claim
        assert claim is not None
        reservation = claim

    material = EstimationMaterial(
        scope_type=material_in.scope_type,
        project_id=material_in.project_id,
        material_key=material_in.material_key,
        source=material_in.source,
        metadata_json=material_in.metadata_json,
        name=material_in.name,
        unit=material_in.unit,
        currency=material_in.currency,
        unit_cost=material_in.unit_cost,
        effective_from=material_in.effective_from,
        effective_to=material_in.effective_to,
        checksum_sha256=material_checksum_sha256(
            scope_type=material_in.scope_type,
            project_id=material_in.project_id,
            material_key=material_in.material_key,
            source=material_in.source,
            metadata_json=material_in.metadata_json,
            name=material_in.name,
            unit=material_in.unit,
            currency=material_in.currency,
            unit_cost=material_in.unit_cost,
            effective_from=material_in.effective_from,
            effective_to=material_in.effective_to,
        ),
    )
    db.add(material)

    try:
        await db.flush()
        if predecessor is not None:
            db.add(
                EstimationMaterialSupersession(
                    predecessor_material_id=predecessor.id,
                    successor_material_id=material.id,
                )
            )
            await db.flush()
        await db.refresh(material)
    except IntegrityError:
        await db.rollback()
        await _raise_catalog_conflict(db, reservation, kind="Material catalog entry")

    body = _serialize_material(
        material,
        superseded_by_id=None,
        supersedes_material_id=material_in.supersedes_material_id,
    )
    response_body = body.model_dump(mode="json")
    idempotent_response: Response | None = None
    if reservation is not None:
        idempotent_response = await complete_idempotency_response(
            db,
            reservation,
            status_code=status.HTTP_201_CREATED,
            response_body=response_body,
        )
    await db.commit()

    if idempotent_response is not None:
        return idempotent_response
    return body


@estimation_router.get(
    "/estimation/catalog/materials/{material_id}",
    response_model=EstimationMaterialRead,
)
async def get_material(
    material_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> EstimationMaterialRead:
    statement, _ = _material_query()
    row = (await db.execute(statement.where(EstimationMaterial.id == material_id))).one_or_none()
    if row is None:
        raise_not_found("Material catalog entry", str(material_id))
    assert row is not None
    return _serialize_material(row[0], superseded_by_id=row[1], supersedes_material_id=row[2])


@estimation_router.get(
    "/estimation/catalog/materials",
    response_model=EstimationMaterialListResponse,
)
async def list_materials(
    db: Annotated[AsyncSession, Depends(get_db)],
    cursor: Annotated[str | None, Query(description="Opaque pagination cursor")] = None,
    limit: Annotated[int, Query(ge=1, le=200, description="Page size")] = 50,
    scope_type: Annotated[
        CatalogScopeType | None,
        Query(description="Filter by scope type"),
    ] = None,
    project_id: Annotated[UUID | None, Query(description="Filter by owning project id")] = None,
    material_key: Annotated[str | None, Query(description="Filter by stable material key")] = None,
    source: Annotated[str | None, Query(description="Filter by source label")] = None,
    unit: Annotated[str | None, Query(description="Filter by unit")] = None,
    currency: Annotated[CatalogCurrencyCode | None, Query(description="Filter by currency")] = None,
    as_of: Annotated[
        date | None,
        Query(description="Filter to entries effective on the given date"),
    ] = None,
    include_superseded: Annotated[
        bool,
        Query(description="Include superseded entries; default false hides stale entries"),
    ] = False,
) -> EstimationMaterialListResponse:
    statement, superseded_by_column = _material_query()
    if scope_type is not None:
        statement = statement.where(EstimationMaterial.scope_type == scope_type)
    if project_id is not None:
        statement = statement.where(EstimationMaterial.project_id == project_id)
    if material_key is not None:
        statement = statement.where(EstimationMaterial.material_key == material_key)
    if source is not None:
        statement = statement.where(EstimationMaterial.source == source)
    if unit is not None:
        statement = statement.where(EstimationMaterial.unit == unit)
    if currency is not None:
        statement = statement.where(EstimationMaterial.currency == currency)
    if as_of is not None:
        statement = statement.where(
            (EstimationMaterial.effective_from <= as_of)
            & (
                EstimationMaterial.effective_to.is_(None)
                | (EstimationMaterial.effective_to > as_of)
            )
        )
    if not include_superseded:
        statement = statement.where(superseded_by_column.is_(None))
    if cursor is not None:
        created_at, row_id = _decode_timestamp_cursor(cursor)
        statement = statement.where(
            (EstimationMaterial.created_at < created_at)
            | ((EstimationMaterial.created_at == created_at) & (EstimationMaterial.id < row_id))
        )

    rows = (
        await db.execute(
            statement.order_by(
                EstimationMaterial.created_at.desc(), EstimationMaterial.id.desc()
            ).limit(limit + 1)
        )
    ).all()
    page = rows[:limit]
    next_cursor = None
    if len(rows) > limit:
        last_row = page[-1]
        next_cursor = _encode_timestamp_cursor(last_row[0].created_at, last_row[0].id)
    return EstimationMaterialListResponse(
        items=[
            _serialize_material(
                row[0],
                superseded_by_id=row[1],
                supersedes_material_id=row[2],
            )
            for row in page
        ],
        next_cursor=next_cursor,
    )


@estimation_router.post(
    "/estimation/formulas",
    response_model=EstimationFormulaRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_formula(
    formula_in: EstimationFormulaCreate,
    db: Annotated[AsyncSession, Depends(get_db)],
    idempotency_key: Annotated[str | None, Depends(get_idempotency_key)] = None,
) -> EstimationFormulaRead | Response:
    reservation: IdempotencyReservation | None = None
    fingerprint: str | None = None
    if idempotency_key is not None:
        fingerprint = build_idempotency_fingerprint(
            "estimation.formulas.create",
            formula_in.model_dump(mode="json"),
        )
        replay = await replay_idempotency_response(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
        )
        if replay is not None:
            return replay

    if formula_in.project_id is not None:
        await _get_active_project_or_404(db, formula_in.project_id)

    predecessor: EstimationFormula | None = None
    if formula_in.supersedes_formula_id is not None:
        predecessor = await _get_formula_predecessor_or_404(db, formula_in.supersedes_formula_id)
        _validate_formula_supersession(formula_in, predecessor)

    if idempotency_key is not None:
        assert fingerprint is not None
        claim = await claim_idempotency_response(
            db,
            key=idempotency_key,
            fingerprint=fingerprint,
            method="POST",
            path="/estimation/formulas",
        )
        if isinstance(claim, Response):
            return claim
        assert claim is not None
        reservation = claim

    formula = EstimationFormula(
        scope_type=formula_in.scope_type,
        project_id=formula_in.project_id,
        formula_id=formula_in.formula_id,
        version=formula_in.version,
        name=formula_in.name,
        dsl_version=formula_in.dsl_version,
        output_key=formula_in.output_key,
        output_contract_json=formula_in.output_contract_json,
        declared_inputs_json=formula_in.declared_inputs_json,
        expression_json=formula_in.expression_json,
        rounding_json=formula_in.rounding_json,
        checksum_sha256=formula_checksum_sha256(
            scope_type=formula_in.scope_type,
            project_id=formula_in.project_id,
            formula_id=formula_in.formula_id,
            version=formula_in.version,
            name=formula_in.name,
            dsl_version=formula_in.dsl_version,
            output_key=formula_in.output_key,
            output_contract_json=formula_in.output_contract_json,
            declared_inputs_json=formula_in.declared_inputs_json,
            expression_json=formula_in.expression_json,
            rounding_json=formula_in.rounding_json,
        ),
    )
    db.add(formula)

    try:
        await db.flush()
        if predecessor is not None:
            db.add(
                EstimationFormulaSupersession(
                    predecessor_formula_id=predecessor.id,
                    successor_formula_id=formula.id,
                )
            )
            await db.flush()
        await db.refresh(formula)
    except IntegrityError:
        await db.rollback()
        await _raise_catalog_conflict(db, reservation, kind="Formula definition")

    body = _serialize_formula(
        formula,
        superseded_by_id=None,
        supersedes_formula_id=formula_in.supersedes_formula_id,
    )
    response_body = body.model_dump(mode="json")
    idempotent_response: Response | None = None
    if reservation is not None:
        idempotent_response = await complete_idempotency_response(
            db,
            reservation,
            status_code=status.HTTP_201_CREATED,
            response_body=response_body,
        )
    await db.commit()

    if idempotent_response is not None:
        return idempotent_response
    return body


@estimation_router.get(
    "/estimation/formulas/{formula_definition_id}",
    response_model=EstimationFormulaRead,
)
async def get_formula(
    formula_definition_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> EstimationFormulaRead:
    statement, _ = _formula_query()
    row = (
        await db.execute(statement.where(EstimationFormula.id == formula_definition_id))
    ).one_or_none()
    if row is None:
        raise_not_found("Formula definition", str(formula_definition_id))
    assert row is not None
    return _serialize_formula(row[0], superseded_by_id=row[1], supersedes_formula_id=row[2])


@estimation_router.get(
    "/estimation/formulas",
    response_model=EstimationFormulaListResponse,
)
async def list_formulas(
    db: Annotated[AsyncSession, Depends(get_db)],
    cursor: Annotated[str | None, Query(description="Opaque pagination cursor")] = None,
    limit: Annotated[int, Query(ge=1, le=200, description="Page size")] = 50,
    scope_type: Annotated[
        CatalogScopeType | None,
        Query(description="Filter by scope type"),
    ] = None,
    project_id: Annotated[UUID | None, Query(description="Filter by owning project id")] = None,
    formula_id: Annotated[str | None, Query(description="Filter by stable formula id")] = None,
    version: Annotated[int | None, Query(ge=1, description="Filter by formula version")] = None,
    dsl_version: Annotated[str | None, Query(description="Filter by DSL version")] = None,
    output_key: Annotated[str | None, Query(description="Filter by output key")] = None,
    include_superseded: Annotated[
        bool,
        Query(description="Include superseded entries; default false hides stale entries"),
    ] = False,
) -> EstimationFormulaListResponse:
    statement, superseded_by_column = _formula_query()
    if scope_type is not None:
        statement = statement.where(EstimationFormula.scope_type == scope_type)
    if project_id is not None:
        statement = statement.where(EstimationFormula.project_id == project_id)
    if formula_id is not None:
        statement = statement.where(EstimationFormula.formula_id == formula_id)
    if version is not None:
        statement = statement.where(EstimationFormula.version == version)
    if dsl_version is not None:
        statement = statement.where(EstimationFormula.dsl_version == dsl_version)
    if output_key is not None:
        statement = statement.where(EstimationFormula.output_key == output_key)
    if not include_superseded:
        statement = statement.where(superseded_by_column.is_(None))
    if cursor is not None:
        created_at, row_id = _decode_timestamp_cursor(cursor)
        statement = statement.where(
            (EstimationFormula.created_at < created_at)
            | ((EstimationFormula.created_at == created_at) & (EstimationFormula.id < row_id))
        )

    rows = (
        await db.execute(
            statement.order_by(
                EstimationFormula.created_at.desc(), EstimationFormula.id.desc()
            ).limit(limit + 1)
        )
    ).all()
    page = rows[:limit]
    next_cursor = None
    if len(rows) > limit:
        last_row = page[-1]
        next_cursor = _encode_timestamp_cursor(last_row[0].created_at, last_row[0].id)
    return EstimationFormulaListResponse(
        items=[
            _serialize_formula(row[0], superseded_by_id=row[1], supersedes_formula_id=row[2])
            for row in page
        ],
        next_cursor=next_cursor,
    )
