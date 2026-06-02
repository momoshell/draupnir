from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from json import JSONDecodeError
from typing import Annotated, Any, cast
from uuid import UUID

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    Request,
    Response,
    status,
)
from pydantic import ValidationError
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api import idempotency, pagination
from app.api.v1 import revision_lineage
from app.cad import changesets as changeset_service
from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response, raise_not_found
from app.db.session import get_db
from app.models.cad_changeset import CadChangeOperation, CadChangeSet
from app.models.drawing_revision import DrawingRevision
from app.schemas.changeset import (
    CadChangeOperationRead,
    CadChangeSetCreateRequest,
    CadChangeSetCursor,
    CadChangeSetListResponse,
    CadChangeSetRead,
)

changesets_router = APIRouter()

DbSession = Annotated[AsyncSession, Depends(get_db)]
IdempotencyKey = Annotated[
    str | None,
    Depends(idempotency.get_idempotency_key),
]

_create_change_set = changeset_service.create_change_set
_list_change_set_operations = changeset_service.list_change_set_operations
_run_idempotent_mutation = idempotency.run_idempotent_mutation
_IdempotentMutationSuccess = idempotency.IdempotentMutationSuccess
_encode_keyset_cursor = pagination.encode_keyset_cursor
_decode_keyset_cursor = pagination.decode_keyset_cursor


def _get_active_revision(*args: Any, **kwargs: Any) -> Any:
    return revision_lineage._get_active_revision(*args, **kwargs)  # noqa: SLF001


def _raise_input_invalid(message: str, *, details: Any | None = None) -> None:
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=create_error_response(ErrorCode.INPUT_INVALID, message, details),
    )


def _normalize_operation_target(
    operation: CadChangeOperation,
    raw_target: Any,
) -> dict[str, Any] | None:
    if not isinstance(raw_target, Mapping):
        target_payload: dict[str, Any] = {}
    else:
        target_payload = dict(raw_target)

    target_payload.pop("expected_source_identity", None)
    target_payload.pop("expected_source_hash", None)
    if operation.target_revision_entity_id is not None:
        target_payload.setdefault("revision_entity_id", str(operation.target_revision_entity_id))
    if not target_payload:
        return None
    return target_payload


def _serialize_operation(operation: CadChangeOperation) -> CadChangeOperationRead:
    stored_operation = dict(operation.operation_json)
    payload = stored_operation.get("payload")
    if not isinstance(payload, Mapping):
        payload = stored_operation

    provenance = stored_operation.get("provenance")
    normalized_provenance = dict(provenance) if isinstance(provenance, Mapping) else None
    reason = stored_operation.get("reason")
    normalized_reason = reason if isinstance(reason, str) and reason.strip() else None

    return CadChangeOperationRead(
        operation_id=operation.id,
        sequence_index=operation.sequence_index,
        payload_version=1,
        operation_type=operation.operation_type,
        target=_normalize_operation_target(operation, stored_operation.get("target")),
        expected_source_identity=operation.expected_source_identity,
        expected_source_hash=operation.expected_source_hash,
        payload=dict(payload),
        reason=normalized_reason,
        provenance=normalized_provenance,
        created_at=operation.created_at,
    )


def _serialize_change_set(
    change_set: CadChangeSet,
    *,
    operations: list[CadChangeOperation],
) -> CadChangeSetRead:
    ordered_operations = sorted(
        operations,
        key=lambda operation: (operation.sequence_index, operation.id),
    )
    return CadChangeSetRead(
        id=change_set.id,
        project_id=change_set.project_id,
        base_revision_id=change_set.base_revision_id,
        status=change_set.status,
        created_by=change_set.created_by,
        created_at=change_set.created_at,
        updated_at=change_set.updated_at,
        operations=[_serialize_operation(operation) for operation in ordered_operations],
    )


async def _require_active_revision(
    db: AsyncSession,
    revision_id: UUID,
    *,
    for_update: bool = False,
) -> DrawingRevision:
    revision = cast(
        DrawingRevision | None,
        await _get_active_revision(revision_id, db, for_update=for_update),
    )
    if revision is None:
        raise_not_found("revision", str(revision_id))
    assert revision is not None
    return revision


async def _load_change_set_operations(
    db: AsyncSession,
    *,
    project_id: UUID,
    change_set_ids: list[UUID],
) -> dict[UUID, list[CadChangeOperation]]:
    if not change_set_ids:
        return {}

    result = await db.execute(
        select(CadChangeOperation)
        .where(
            CadChangeOperation.project_id == project_id,
            CadChangeOperation.change_set_id.in_(change_set_ids),
        )
        .order_by(
            CadChangeOperation.change_set_id.asc(),
            CadChangeOperation.sequence_index.asc(),
            CadChangeOperation.id.asc(),
        )
    )
    grouped: dict[UUID, list[CadChangeOperation]] = defaultdict(list)
    for operation in result.scalars():
        grouped[operation.change_set_id].append(operation)
    return dict(grouped)


async def _read_create_request(
    request: Request,
) -> tuple[dict[str, Any], CadChangeSetCreateRequest]:
    try:
        payload = await request.json()
    except JSONDecodeError as exc:
        _raise_input_invalid("Request body must be valid JSON.", details={"error": str(exc)})
    except Exception as exc:  # pragma: no cover - defensive fallback for request parsers
        _raise_input_invalid("Request body must be valid JSON.", details={"error": str(exc)})

    if not isinstance(payload, dict):
        _raise_input_invalid("Request body must be a JSON object.")

    try:
        parsed = CadChangeSetCreateRequest.model_validate(payload)
    except ValidationError as exc:
        _raise_input_invalid(
            "Changeset request body is invalid.",
            details={"errors": exc.errors()},
        )

    return payload, parsed


def _build_fingerprint(revision_id: UUID, payload: dict[str, Any]) -> str:
    return idempotency.build_idempotency_fingerprint(
        "revision_changeset_create",
        {"revision_id": str(revision_id), "body": payload},
    )


def _build_next_cursor(change_sets: list[CadChangeSet], *, has_more: bool) -> str | None:
    if not has_more or not change_sets:
        return None
    last_item = change_sets[-1]
    return _encode_keyset_cursor(
        CadChangeSetCursor(created_at=last_item.created_at, id=last_item.id)
    )


def _apply_after_cursor(query: Any, cursor: CadChangeSetCursor) -> Any:
    return query.where(
        or_(
            CadChangeSet.created_at > cursor.created_at,
            and_(
                CadChangeSet.created_at == cursor.created_at,
                CadChangeSet.id > cursor.id,
            ),
        )
    )


@changesets_router.post(
    "/revisions/{revision_id}/changesets",
    response_model=CadChangeSetRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_revision_changeset(
    revision_id: UUID,
    request: Request,
    db: DbSession,
    idempotency_key: IdempotencyKey,
) -> Response:
    raw_payload, create_request = await _read_create_request(request)

    async def preclaim() -> Response | None:
        await _require_active_revision(db, revision_id)
        return None

    async def mutate() -> idempotency.IdempotentMutationSuccess[CadChangeSetRead]:
        active_revision = await _require_active_revision(db, revision_id, for_update=True)
        change_set = await _create_change_set(
            session=db,
            project_id=active_revision.project_id,
            base_revision_id=active_revision.id,
            status="proposed",
            created_by=create_request.created_by,
            operations=[operation.to_domain() for operation in create_request.operations],
        )
        await db.flush()
        operations = list(
            await _list_change_set_operations(
                db,
                project_id=active_revision.project_id,
                change_set_id=change_set.id,
            )
        )
        return _IdempotentMutationSuccess(
            value=_serialize_change_set(change_set, operations=operations),
            status_code=status.HTTP_201_CREATED,
        )

    return await _run_idempotent_mutation(
        db,
        key=idempotency_key,
        fingerprint=_build_fingerprint(revision_id, raw_payload),
        method="POST",
        path=str(request.url.path),
        mutate=mutate,
        serialize_result=lambda result: result.model_dump(mode="json"),
        preclaim=preclaim,
    )


@changesets_router.get(
    "/revisions/{revision_id}/changesets",
    response_model=CadChangeSetListResponse,
)
async def list_revision_changesets(
    revision_id: UUID,
    db: DbSession,
    limit: int = Query(default=pagination.DEFAULT_PAGE_SIZE, ge=1, le=pagination.MAX_PAGE_SIZE),
    cursor: str | None = Query(default=None),
) -> CadChangeSetListResponse:
    active_revision = await _require_active_revision(db, revision_id)
    decoded_cursor = None if cursor is None else _decode_keyset_cursor(cursor, CadChangeSetCursor)

    query = (
        select(CadChangeSet)
        .where(
            CadChangeSet.project_id == active_revision.project_id,
            CadChangeSet.base_revision_id == active_revision.id,
        )
        .order_by(CadChangeSet.created_at.asc(), CadChangeSet.id.asc())
    )
    if decoded_cursor is not None:
        query = _apply_after_cursor(query, decoded_cursor)

    result = await db.execute(query.limit(limit + 1))
    rows = list(result.scalars())
    has_more = len(rows) > limit
    items = rows[:limit]
    operations_by_change_set = await _load_change_set_operations(
        db,
        project_id=active_revision.project_id,
        change_set_ids=[change_set.id for change_set in items],
    )

    return CadChangeSetListResponse(
        items=[
            _serialize_change_set(
                change_set,
                operations=operations_by_change_set.get(change_set.id, []),
            )
            for change_set in items
        ],
        next_cursor=_build_next_cursor(items, has_more=has_more),
    )


@changesets_router.get(
    "/revisions/{revision_id}/changesets/{change_set_id}",
    response_model=CadChangeSetRead,
)
async def get_revision_changeset(
    revision_id: UUID,
    change_set_id: UUID,
    db: DbSession,
) -> CadChangeSetRead:
    active_revision = await _require_active_revision(db, revision_id)

    result = await db.execute(
        select(CadChangeSet).where(
            CadChangeSet.id == change_set_id,
            CadChangeSet.project_id == active_revision.project_id,
            CadChangeSet.base_revision_id == active_revision.id,
        )
    )
    change_set = result.scalar_one_or_none()
    if change_set is None:
        raise_not_found("change_set", str(change_set_id))
    assert change_set is not None

    operations = list(
        await _list_change_set_operations(
            db,
            project_id=active_revision.project_id,
            change_set_id=change_set.id,
        )
    )
    return _serialize_change_set(change_set, operations=operations)
