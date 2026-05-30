"""Endpoint-integrated idempotency helpers for mutating API routes."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Any, Protocol

from fastapi import Header, HTTPException, status
from fastapi.responses import JSONResponse, Response
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response
from app.models.idempotency_key import IdempotencyKey, IdempotencyStatus

_IDEMPOTENCY_KEY_PATTERN = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_RETRY_AFTER_SECONDS = "1"


@dataclass(frozen=True)
class IdempotencyReservation:
    """Claimed reservation that may be finalized after mutation succeeds."""

    record_id: uuid.UUID


@dataclass(frozen=True)
class IdempotencyReplay:
    """Precomputed short-circuit response for an idempotent request."""

    response: Response


IdempotencyClaim = IdempotencyReservation | IdempotencyReplay


@dataclass(frozen=True)
class IdempotentMutationSuccess[ResultT]:
    """Successful mutation result awaiting response serialization."""

    value: ResultT
    status_code: int
    after_commit: Callable[[], Awaitable[None]] | None = None


@dataclass(frozen=True)
class IdempotentMutationKnownError:
    """Explicit known-error snapshot that should be finalized and replayed."""

    status_code: int
    response_body: dict[str, Any] | None
    after_commit: Callable[[], Awaitable[None]] | None = None


class ReplayIdempotencyResponseCallable(Protocol):
    """Typed replay helper contract for idempotent mutation orchestration."""

    async def __call__(
        self,
        db: AsyncSession,
        *,
        key: str | None,
        fingerprint: str,
    ) -> Response | None: ...


class ClaimIdempotencyResponseCallable(Protocol):
    """Typed claim helper contract for idempotent mutation orchestration."""

    async def __call__(
        self,
        db: AsyncSession,
        *,
        key: str | None,
        fingerprint: str,
        method: str,
        path: str,
    ) -> IdempotencyReservation | Response | None: ...


class CompleteIdempotencyResponseCallable(Protocol):
    """Typed completion helper contract for idempotent mutation orchestration."""

    async def __call__(
        self,
        db: AsyncSession,
        reservation: IdempotencyReservation | None,
        *,
        status_code: int,
        response_body: dict[str, Any] | None,
    ) -> Response: ...


@dataclass(frozen=True)
class IdempotentMutationOps:
    """Injectable idempotency helper operations for mutation orchestration."""

    replay: ReplayIdempotencyResponseCallable
    claim: ClaimIdempotencyResponseCallable
    complete: CompleteIdempotencyResponseCallable


def _resolve_hash_secret() -> str:
    """Resolve the stable HMAC secret for raw idempotency key hashing."""

    configured_secret = settings.idempotency_key_hash_secret
    if configured_secret:
        return configured_secret
    return settings.service_name


def hash_idempotency_key(raw_key: str) -> str:
    """Return the HMAC-SHA256 hex digest for a raw idempotency key."""

    return hmac.new(
        _resolve_hash_secret().encode("utf-8"),
        raw_key.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def build_idempotency_fingerprint(scope: str, payload: Any) -> str:
    """Return a canonical request fingerprint for idempotency comparisons."""

    canonical_payload = json.dumps(
        {"scope": scope, "payload": payload},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()


def _build_conflict_response(*, reason: str, in_progress: bool) -> Response:
    """Build the standardized idempotency conflict error response."""

    message = (
        "A request with this Idempotency-Key is already in progress."
        if in_progress
        else "This Idempotency-Key is already associated with a different request."
    )
    headers = {"Retry-After": _RETRY_AFTER_SECONDS} if in_progress else None
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        headers=headers,
        content=create_error_response(
            code=ErrorCode.IDEMPOTENCY_CONFLICT,
            message=message,
            details={"reason": reason},
        ),
    )


def _build_replay_response(record: IdempotencyKey) -> Response:
    """Construct a replay response from a completed reservation snapshot."""

    if record.response_status_code is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=create_error_response(
                code=ErrorCode.INTERNAL_ERROR,
                message="Completed idempotency record is missing a response snapshot.",
                details=None,
            ),
        )

    if record.response_status_code == status.HTTP_204_NO_CONTENT:
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    if record.response_body_json is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=create_error_response(
                code=ErrorCode.INTERNAL_ERROR,
                message="Completed idempotency record is missing a response snapshot.",
                details=None,
            ),
        )

    return JSONResponse(status_code=record.response_status_code, content=record.response_body_json)


async def get_idempotency_key(
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> str | None:
    """Return a validated optional Idempotency-Key header value."""

    if idempotency_key is None:
        return None
    if not _IDEMPOTENCY_KEY_PATTERN.fullmatch(idempotency_key):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=create_error_response(
                code=ErrorCode.INPUT_INVALID,
                message="Idempotency-Key must be a valid RFC 9110 token.",
                details=None,
            ),
        )
    return idempotency_key


async def replay_idempotency(
    db: AsyncSession,
    *,
    key: str,
    fingerprint: str,
) -> IdempotencyReplay | None:
    """Return a canonical short-circuit response for an existing reservation."""

    key_hash = hash_idempotency_key(key)
    existing = (
        await db.execute(select(IdempotencyKey).where(IdempotencyKey.key_hash == key_hash))
    ).scalar_one_or_none()
    if existing is None:
        return None
    if existing.request_fingerprint != fingerprint:
        return IdempotencyReplay(
            response=_build_conflict_response(reason="request_mismatch", in_progress=False)
        )
    if existing.status == IdempotencyStatus.COMPLETED.value:
        return IdempotencyReplay(response=_build_replay_response(existing))
    return IdempotencyReplay(
        response=_build_conflict_response(reason="in_progress", in_progress=True)
    )


async def replay_idempotency_response(
    db: AsyncSession,
    *,
    key: str | None,
    fingerprint: str,
) -> Response | None:
    """Return an optional replay response for routes with an Idempotency-Key."""

    if key is None:
        return None
    replay = await replay_idempotency(db, key=key, fingerprint=fingerprint)
    if replay is None:
        return None
    return replay.response


async def claim_idempotency(
    db: AsyncSession,
    *,
    key: str,
    fingerprint: str,
    method: str,
    path: str,
) -> IdempotencyClaim:
    """Reserve or replay a request-scoped idempotency record."""

    key_hash = hash_idempotency_key(key)
    insert_statement = (
        insert(IdempotencyKey)
        .values(
            key_hash=key_hash,
            request_fingerprint=fingerprint,
            request_method=method,
            request_path=path,
            status=IdempotencyStatus.IN_PROGRESS.value,
        )
        .on_conflict_do_nothing(index_elements=[IdempotencyKey.key_hash])
        .returning(IdempotencyKey.id)
    )
    reservation_id = (await db.execute(insert_statement)).scalar_one_or_none()
    if reservation_id is not None:
        await db.commit()
        return IdempotencyReservation(record_id=reservation_id)

    await db.rollback()
    replay = await replay_idempotency(db, key=key, fingerprint=fingerprint)
    if replay is not None:
        return replay
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=create_error_response(
            code=ErrorCode.INTERNAL_ERROR,
            message="Failed to load the conflicting idempotency reservation.",
            details=None,
        ),
    )


async def claim_idempotency_response(
    db: AsyncSession,
    *,
    key: str | None,
    fingerprint: str,
    method: str,
    path: str,
) -> IdempotencyReservation | Response | None:
    """Claim an optional idempotency reservation or return an immediate response."""

    if key is None:
        return None
    claim = await claim_idempotency(
        db,
        key=key,
        fingerprint=fingerprint,
        method=method,
        path=path,
    )
    if isinstance(claim, IdempotencyReplay):
        return claim.response
    return claim


async def mark_idempotency_completed(
    db: AsyncSession,
    reservation: IdempotencyReservation,
    *,
    status_code: int,
    response_body: dict[str, Any] | None,
) -> None:
    """Persist a completed replay snapshot for an already-claimed reservation."""

    record = (
        await db.execute(
            select(IdempotencyKey)
            .where(IdempotencyKey.id == reservation.record_id)
            .with_for_update()
        )
    ).scalar_one()
    if record.status == IdempotencyStatus.COMPLETED.value:
        return
    record.status = IdempotencyStatus.COMPLETED.value
    record.response_status_code = status_code
    record.response_body_json = response_body
    record.completed_at = datetime.now(UTC)


async def complete_idempotency_response(
    db: AsyncSession,
    reservation: IdempotencyReservation | None,
    *,
    status_code: int,
    response_body: dict[str, Any] | None,
) -> Response:
    """Snapshot an optional reservation and build a replay-safe HTTP response."""

    if reservation is not None:
        await mark_idempotency_completed(
            db,
            reservation,
            status_code=status_code,
            response_body=response_body,
        )
    if status_code == status.HTTP_204_NO_CONTENT:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return JSONResponse(status_code=status_code, content=response_body)


DEFAULT_IDEMPOTENT_MUTATION_OPS = IdempotentMutationOps(
    replay=replay_idempotency_response,
    claim=claim_idempotency_response,
    complete=complete_idempotency_response,
)


async def run_idempotent_mutation[ResultT](
    db: AsyncSession,
    *,
    key: str | None,
    fingerprint: str,
    method: str,
    path: str,
    mutate: Callable[
        [],
        Awaitable[IdempotentMutationSuccess[ResultT] | IdempotentMutationKnownError],
    ],
    serialize_result: Callable[[ResultT], dict[str, Any] | None],
    preclaim: Callable[[], Awaitable[Response | None]] | None = None,
    reload_after_claim: Callable[[], Awaitable[None]] | None = None,
    ops: IdempotentMutationOps | None = None,
) -> Response:
    """Run the shared replay/claim/mutate/complete/commit mutation flow."""

    resolved_ops = ops or DEFAULT_IDEMPOTENT_MUTATION_OPS
    replay_response = await resolved_ops.replay(
        db,
        key=key,
        fingerprint=fingerprint,
    )
    if replay_response is not None:
        return replay_response

    if preclaim is not None:
        preclaim_response = await preclaim()
        if preclaim_response is not None:
            return preclaim_response

    reservation_or_response = await resolved_ops.claim(
        db,
        key=key,
        fingerprint=fingerprint,
        method=method,
        path=path,
    )
    if isinstance(reservation_or_response, Response):
        return reservation_or_response

    if reservation_or_response is not None and reload_after_claim is not None:
        await reload_after_claim()

    mutation_result = await mutate()
    if isinstance(mutation_result, IdempotentMutationKnownError):
        response_body = mutation_result.response_body
        after_commit = mutation_result.after_commit
        status_code = mutation_result.status_code
    else:
        response_body = serialize_result(mutation_result.value)
        after_commit = mutation_result.after_commit
        status_code = mutation_result.status_code

    response = await resolved_ops.complete(
        db,
        reservation_or_response,
        status_code=status_code,
        response_body=response_body,
    )
    await db.commit()
    if after_commit is not None:
        await after_commit()
    return response
