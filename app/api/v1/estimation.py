"""Estimation catalog API endpoints."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import date, datetime
from typing import Annotated, Any, NoReturn, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import Response
from pydantic import BaseModel
from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from app.api.idempotency import (
    IdempotentMutationKnownError,
    IdempotentMutationOps,
    IdempotentMutationSuccess,
    build_idempotency_fingerprint,
    claim_idempotency_response,
    complete_idempotency_response,
    get_idempotency_key,
    replay_idempotency_response,
    run_idempotent_mutation,
)
from app.api.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    decode_keyset_cursor,
    encode_keyset_cursor,
    paginate_overfetched,
)
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
    ValidatedEstimationFormulaCreate,
    validate_formula_create,
)

estimation_router = APIRouter()

_IDEMPOTENT_MUTATION_OPS = IdempotentMutationOps(
    replay=replay_idempotency_response,
    claim=claim_idempotency_response,
    complete=complete_idempotency_response,
)


class _TimestampCursor(BaseModel):
    created_at: datetime
    id: UUID


def _encode_timestamp_cursor(created_at: datetime, row_id: UUID) -> str:
    return encode_keyset_cursor(
        {
            "created_at": created_at.isoformat(),
            "id": str(row_id),
        }
    )


def _decode_timestamp_cursor(cursor: str) -> tuple[datetime, UUID]:
    payload = decode_keyset_cursor(cursor, _TimestampCursor)
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


def _catalog_conflict_result(kind: str) -> IdempotentMutationKnownError:
    return IdempotentMutationKnownError(
        status_code=status.HTTP_409_CONFLICT,
        response_body=_catalog_conflict_body(kind),
    )


def _raise_input_invalid(message: str) -> NoReturn:
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=create_error_response(
            code=ErrorCode.INPUT_INVALID,
            message=message,
            details=None,
        ),
    )


def _raise_unprocessable_input_invalid(message: str) -> NoReturn:
    raise HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        detail=create_error_response(
            code=ErrorCode.INPUT_INVALID,
            message=message,
            details=None,
        ),
    )


def _catalog_query(
    *,
    model: Any,
    supersession_model: Any,
    predecessor_attr: str,
    successor_attr: str,
) -> tuple[Any, Any]:
    superseded_link = aliased(supersession_model)
    supersedes_link = aliased(supersession_model)
    superseded_predecessor_column = getattr(superseded_link, predecessor_attr)
    superseded_successor_column = getattr(superseded_link, successor_attr)
    supersedes_predecessor_column = getattr(supersedes_link, predecessor_attr)
    supersedes_successor_column = getattr(supersedes_link, successor_attr)
    statement = (
        select(model, superseded_successor_column, supersedes_predecessor_column)
        .outerjoin(superseded_link, superseded_predecessor_column == model.id)
        .outerjoin(supersedes_link, supersedes_successor_column == model.id)
        .where(_active_project_visibility(model.project_id))
    )
    return statement, superseded_successor_column


async def _get_catalog_predecessor_or_404(
    *,
    db: AsyncSession,
    model: Any,
    entry_id: UUID,
    resource_name: str,
) -> Any:
    entry = (
        await db.execute(
            select(model)
            .where((model.id == entry_id) & _active_project_visibility(model.project_id))
            .with_for_update()
        )
    ).scalar_one_or_none()
    if entry is None:
        raise_not_found(resource_name, str(entry_id))
    return entry


async def _create_catalog_entry[
    CatalogCreateT: BaseModel,
    CatalogEntryT,
    CatalogPredecessorT,
    CatalogReadT: BaseModel,
](
    *,
    payload: CatalogCreateT,
    db: AsyncSession,
    idempotency_key: str | None,
    fingerprint_namespace: str,
    path: str,
    conflict_kind: str,
    project_id: UUID | None,
    supersedes_id: UUID | None,
    get_predecessor: Callable[[AsyncSession, UUID], Awaitable[CatalogPredecessorT]],
    validate_predecessor: Callable[[CatalogCreateT, CatalogPredecessorT], None],
    validate_effective_window: Callable[[CatalogCreateT, CatalogPredecessorT], None] | None,
    build_entry: Callable[[CatalogCreateT], CatalogEntryT],
    build_supersession: Callable[[CatalogPredecessorT, CatalogEntryT], Any] | None,
    serialize_created: Callable[[CatalogEntryT, UUID | None], CatalogReadT],
) -> CatalogReadT | Response:
    fingerprint = build_idempotency_fingerprint(
        fingerprint_namespace,
        payload.model_dump(mode="json"),
    )
    predecessor: CatalogPredecessorT | None = None

    async def _preclaim() -> Response | None:
        nonlocal predecessor
        if project_id is not None:
            await _get_active_project_or_404(db, project_id)
        predecessor = None
        if supersedes_id is not None:
            predecessor = await get_predecessor(db, supersedes_id)
            validate_predecessor(payload, predecessor)
            if validate_effective_window is not None:
                validate_effective_window(payload, predecessor)
        return None

    async def _reload_after_claim() -> None:
        await _preclaim()

    async def _mutate() -> IdempotentMutationSuccess[CatalogReadT] | IdempotentMutationKnownError:
        entry = build_entry(payload)
        db.add(entry)
        try:
            await db.flush()
            if predecessor is not None and build_supersession is not None:
                db.add(build_supersession(predecessor, entry))
                await db.flush()
            await db.refresh(entry)
        except IntegrityError:
            await db.rollback()
            return _catalog_conflict_result(conflict_kind)

        return IdempotentMutationSuccess(
            value=serialize_created(entry, supersedes_id),
            status_code=status.HTTP_201_CREATED,
        )

    return await run_idempotent_mutation(
        db,
        key=idempotency_key,
        fingerprint=fingerprint,
        method="POST",
        path=path,
        preclaim=_preclaim,
        reload_after_claim=_reload_after_claim,
        mutate=_mutate,
        serialize_result=lambda body: body.model_dump(mode="json"),
        ops=_IDEMPOTENT_MUTATION_OPS,
    )


async def _get_catalog_entry_or_404[CatalogReadT: BaseModel](
    *,
    db: AsyncSession,
    statement: Any,
    model: Any,
    entry_id: UUID,
    resource_name: str,
    serialize_row: Callable[[Any], CatalogReadT],
) -> CatalogReadT:
    row = (await db.execute(statement.where(model.id == entry_id))).one_or_none()
    if row is None:
        raise_not_found(resource_name, str(entry_id))
    return serialize_row(row)


def _apply_catalog_list_page(
    *,
    statement: Any,
    model: Any,
    superseded_by_column: Any,
    include_superseded: bool,
    cursor: str | None,
) -> Any:
    if not include_superseded:
        statement = statement.where(superseded_by_column.is_(None))
    if cursor is not None:
        created_at, row_id = _decode_timestamp_cursor(cursor)
        statement = statement.where(
            (model.created_at < created_at)
            | ((model.created_at == created_at) & (model.id < row_id))
        )
    return statement


async def _list_catalog_entries[CatalogReadT: BaseModel, CatalogListT: BaseModel](
    *,
    db: AsyncSession,
    statement: Any,
    model: Any,
    limit: int,
    serialize_row: Callable[[Any], CatalogReadT],
    build_response: Callable[[list[CatalogReadT], str | None], CatalogListT],
) -> CatalogListT:
    rows = (
        await db.execute(
            statement.order_by(model.created_at.desc(), model.id.desc()).limit(limit + 1)
        )
    ).all()
    page, next_cursor = paginate_overfetched(
        rows,
        limit=limit,
        encode_cursor=lambda row: _encode_timestamp_cursor(row[0].created_at, row[0].id),
    )
    return build_response([serialize_row(row) for row in page], next_cursor)


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
    return _catalog_query(
        model=EstimationRate,
        supersession_model=EstimationRateSupersession,
        predecessor_attr="predecessor_rate_id",
        successor_attr="successor_rate_id",
    )


def _material_query() -> tuple[Any, Any]:
    return _catalog_query(
        model=EstimationMaterial,
        supersession_model=EstimationMaterialSupersession,
        predecessor_attr="predecessor_material_id",
        successor_attr="successor_material_id",
    )


def _formula_query() -> tuple[Any, Any]:
    return _catalog_query(
        model=EstimationFormula,
        supersession_model=EstimationFormulaSupersession,
        predecessor_attr="predecessor_formula_id",
        successor_attr="successor_formula_id",
    )


async def _get_rate_predecessor_or_404(db: AsyncSession, rate_id: UUID) -> EstimationRate:
    return cast(
        EstimationRate,
        await _get_catalog_predecessor_or_404(
            db=db,
            model=EstimationRate,
            entry_id=rate_id,
            resource_name="Rate catalog entry",
        ),
    )


async def _get_material_predecessor_or_404(
    db: AsyncSession,
    material_id: UUID,
) -> EstimationMaterial:
    return cast(
        EstimationMaterial,
        await _get_catalog_predecessor_or_404(
            db=db,
            model=EstimationMaterial,
            entry_id=material_id,
            resource_name="Material catalog entry",
        ),
    )


async def _get_formula_predecessor_or_404(
    db: AsyncSession,
    formula_id: UUID,
) -> EstimationFormula:
    return cast(
        EstimationFormula,
        await _get_catalog_predecessor_or_404(
            db=db,
            model=EstimationFormula,
            entry_id=formula_id,
            resource_name="Formula definition",
        ),
    )


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
    payload: EstimationFormulaCreate | ValidatedEstimationFormulaCreate,
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


def _validate_rate_successor_effective_window(
    payload: EstimationRateCreate,
    predecessor: EstimationRate,
) -> None:
    _validate_successor_effective_window(
        predecessor.effective_to,
        payload.effective_from,
        resource_name="rate",
    )


def _validate_material_successor_effective_window(
    payload: EstimationMaterialCreate,
    predecessor: EstimationMaterial,
) -> None:
    _validate_successor_effective_window(
        predecessor.effective_to,
        payload.effective_from,
        resource_name="material",
    )


def _build_rate(rate_in: EstimationRateCreate) -> EstimationRate:
    return EstimationRate(
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


def _build_material(material_in: EstimationMaterialCreate) -> EstimationMaterial:
    return EstimationMaterial(
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


def _build_formula(formula_in: ValidatedEstimationFormulaCreate) -> EstimationFormula:
    return EstimationFormula(
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


def _build_rate_supersession(
    predecessor: EstimationRate,
    successor: EstimationRate,
) -> EstimationRateSupersession:
    return EstimationRateSupersession(
        predecessor_rate_id=predecessor.id,
        successor_rate_id=successor.id,
    )


def _build_material_supersession(
    predecessor: EstimationMaterial,
    successor: EstimationMaterial,
) -> EstimationMaterialSupersession:
    return EstimationMaterialSupersession(
        predecessor_material_id=predecessor.id,
        successor_material_id=successor.id,
    )


def _build_formula_supersession(
    predecessor: EstimationFormula,
    successor: EstimationFormula,
) -> EstimationFormulaSupersession:
    return EstimationFormulaSupersession(
        predecessor_formula_id=predecessor.id,
        successor_formula_id=successor.id,
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
    return await _create_catalog_entry(
        payload=rate_in,
        db=db,
        idempotency_key=idempotency_key,
        fingerprint_namespace="estimation.rates.create",
        path="/estimation/catalog/rates",
        conflict_kind="Rate catalog entry",
        project_id=rate_in.project_id,
        supersedes_id=rate_in.supersedes_rate_id,
        get_predecessor=_get_rate_predecessor_or_404,
        validate_predecessor=_validate_rate_supersession,
        validate_effective_window=_validate_rate_successor_effective_window,
        build_entry=_build_rate,
        build_supersession=_build_rate_supersession,
        serialize_created=lambda rate, supersedes_rate_id: _serialize_rate(
            rate,
            superseded_by_id=None,
            supersedes_rate_id=supersedes_rate_id,
        ),
    )


@estimation_router.get(
    "/estimation/catalog/rates/{rate_id}",
    response_model=EstimationRateRead,
)
async def get_rate(
    rate_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> EstimationRateRead:
    statement, _ = _rate_query()
    return await _get_catalog_entry_or_404(
        db=db,
        statement=statement,
        model=EstimationRate,
        entry_id=rate_id,
        resource_name="Rate catalog entry",
        serialize_row=lambda row: _serialize_rate(
            row[0],
            superseded_by_id=row[1],
            supersedes_rate_id=row[2],
        ),
    )


@estimation_router.get(
    "/estimation/catalog/rates",
    response_model=EstimationRateListResponse,
)
async def list_rates(
    db: Annotated[AsyncSession, Depends(get_db)],
    cursor: Annotated[str | None, Query(description="Opaque pagination cursor")] = None,
    limit: Annotated[
        int,
        Query(ge=1, le=MAX_PAGE_SIZE, description="Page size"),
    ] = DEFAULT_PAGE_SIZE,
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
    statement = _apply_catalog_list_page(
        statement=statement,
        model=EstimationRate,
        superseded_by_column=superseded_by_column,
        include_superseded=include_superseded,
        cursor=cursor,
    )
    return await _list_catalog_entries(
        db=db,
        statement=statement,
        model=EstimationRate,
        limit=limit,
        serialize_row=lambda row: _serialize_rate(
            row[0],
            superseded_by_id=row[1],
            supersedes_rate_id=row[2],
        ),
        build_response=lambda items, next_cursor: EstimationRateListResponse(
            items=items,
            next_cursor=next_cursor,
        ),
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
    return await _create_catalog_entry(
        payload=material_in,
        db=db,
        idempotency_key=idempotency_key,
        fingerprint_namespace="estimation.materials.create",
        path="/estimation/catalog/materials",
        conflict_kind="Material catalog entry",
        project_id=material_in.project_id,
        supersedes_id=material_in.supersedes_material_id,
        get_predecessor=_get_material_predecessor_or_404,
        validate_predecessor=_validate_material_supersession,
        validate_effective_window=_validate_material_successor_effective_window,
        build_entry=_build_material,
        build_supersession=_build_material_supersession,
        serialize_created=lambda material, supersedes_material_id: _serialize_material(
            material,
            superseded_by_id=None,
            supersedes_material_id=supersedes_material_id,
        ),
    )


@estimation_router.get(
    "/estimation/catalog/materials/{material_id}",
    response_model=EstimationMaterialRead,
)
async def get_material(
    material_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> EstimationMaterialRead:
    statement, _ = _material_query()
    return await _get_catalog_entry_or_404(
        db=db,
        statement=statement,
        model=EstimationMaterial,
        entry_id=material_id,
        resource_name="Material catalog entry",
        serialize_row=lambda row: _serialize_material(
            row[0],
            superseded_by_id=row[1],
            supersedes_material_id=row[2],
        ),
    )


@estimation_router.get(
    "/estimation/catalog/materials",
    response_model=EstimationMaterialListResponse,
)
async def list_materials(
    db: Annotated[AsyncSession, Depends(get_db)],
    cursor: Annotated[str | None, Query(description="Opaque pagination cursor")] = None,
    limit: Annotated[
        int,
        Query(ge=1, le=MAX_PAGE_SIZE, description="Page size"),
    ] = DEFAULT_PAGE_SIZE,
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
    statement = _apply_catalog_list_page(
        statement=statement,
        model=EstimationMaterial,
        superseded_by_column=superseded_by_column,
        include_superseded=include_superseded,
        cursor=cursor,
    )
    return await _list_catalog_entries(
        db=db,
        statement=statement,
        model=EstimationMaterial,
        limit=limit,
        serialize_row=lambda row: _serialize_material(
            row[0],
            superseded_by_id=row[1],
            supersedes_material_id=row[2],
        ),
        build_response=lambda items, next_cursor: EstimationMaterialListResponse(
            items=items,
            next_cursor=next_cursor,
        ),
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
    try:
        validated_formula_in = validate_formula_create(formula_in)
    except ValueError as exc:
        _raise_unprocessable_input_invalid(str(exc))

    return await _create_catalog_entry(
        payload=validated_formula_in,
        db=db,
        idempotency_key=idempotency_key,
        fingerprint_namespace="estimation.formulas.create",
        path="/estimation/formulas",
        conflict_kind="Formula definition",
        project_id=validated_formula_in.project_id,
        supersedes_id=validated_formula_in.supersedes_formula_id,
        get_predecessor=_get_formula_predecessor_or_404,
        validate_predecessor=_validate_formula_supersession,
        validate_effective_window=None,
        build_entry=_build_formula,
        build_supersession=_build_formula_supersession,
        serialize_created=lambda formula, supersedes_formula_id: _serialize_formula(
            formula,
            superseded_by_id=None,
            supersedes_formula_id=supersedes_formula_id,
        ),
    )


@estimation_router.get(
    "/estimation/formulas/{formula_definition_id}",
    response_model=EstimationFormulaRead,
)
async def get_formula(
    formula_definition_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> EstimationFormulaRead:
    statement, _ = _formula_query()
    return await _get_catalog_entry_or_404(
        db=db,
        statement=statement,
        model=EstimationFormula,
        entry_id=formula_definition_id,
        resource_name="Formula definition",
        serialize_row=lambda row: _serialize_formula(
            row[0],
            superseded_by_id=row[1],
            supersedes_formula_id=row[2],
        ),
    )


@estimation_router.get(
    "/estimation/formulas",
    response_model=EstimationFormulaListResponse,
)
async def list_formulas(
    db: Annotated[AsyncSession, Depends(get_db)],
    cursor: Annotated[str | None, Query(description="Opaque pagination cursor")] = None,
    limit: Annotated[
        int,
        Query(ge=1, le=MAX_PAGE_SIZE, description="Page size"),
    ] = DEFAULT_PAGE_SIZE,
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
    statement = _apply_catalog_list_page(
        statement=statement,
        model=EstimationFormula,
        superseded_by_column=superseded_by_column,
        include_superseded=include_superseded,
        cursor=cursor,
    )
    return await _list_catalog_entries(
        db=db,
        statement=statement,
        model=EstimationFormula,
        limit=limit,
        serialize_row=lambda row: _serialize_formula(
            row[0],
            superseded_by_id=row[1],
            supersedes_formula_id=row[2],
        ),
        build_response=lambda items, next_cursor: EstimationFormulaListResponse(
            items=items,
            next_cursor=next_cursor,
        ),
    )
