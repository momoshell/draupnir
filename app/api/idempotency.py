"""Endpoint-integrated idempotency helpers for mutating API routes."""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, timedelta
from typing import Annotated, Any, Protocol

from fastapi import Header, HTTPException, status
from fastapi.responses import JSONResponse, Response
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clock import utcnow
from app.core.config import settings
from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response
from app.core.logging import get_logger
from app.models.idempotency_key import IdempotencyKey, IdempotencyStatus

logger = get_logger(__name__)

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
    # Fall back to the (publicly known) service name only for local/dev convenience.
    # Outside debug this provides no protection if the keys table leaks, so surface it.
    if not settings.debug:
        logger.warning(
            "idempotency_key_hash_secret_unset",
            detail=(
                "IDEMPOTENCY_KEY_HASH_SECRET is not set; falling back to a predictable "
                "secret. Set it before production use."
            ),
        )
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


def _extract_replayable_http_error_snapshot(
    exc: HTTPException,
) -> tuple[int, dict[str, Any]] | None:
    """Return a replay-safe HTTP error snapshot when the exception is deterministic."""

    if exc.status_code < 400 or exc.status_code >= 500:
        return None
    if exc.headers:
        return None
    if not isinstance(exc.detail, dict) or set(exc.detail.keys()) != {"error"}:
        return None

    error = exc.detail.get("error")
    if not isinstance(error, dict) or set(error.keys()) != {"code", "message", "details"}:
        return None

    code = error.get("code")
    message = error.get("message")
    details = error.get("details")
    if not isinstance(code, str) or not isinstance(message, str):
        return None
    if not _is_json_compatible(details):
        return None

    try:
        ErrorCode(code)
    except ValueError:
        return None

    return exc.status_code, exc.detail


def _is_json_compatible(value: Any) -> bool:
    """Return whether a value matches the project's standard JSON-safe envelope contract."""

    if value is None or isinstance(value, str | bool | int):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, list):
        return all(_is_json_compatible(item) for item in value)
    if isinstance(value, dict):
        return all(
            isinstance(key, str) and _is_json_compatible(item) for key, item in value.items()
        )
    return False


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


def _reservation_is_stale(record: IdempotencyKey) -> bool:
    """Return whether an in-progress reservation is old enough to be abandoned.

    A reservation whose owning request crashed before completing stays
    ``in_progress`` forever; once it is older than the configured TTL a matching
    retry is allowed to take it over instead of being blocked indefinitely.
    """

    if record.status != IdempotencyStatus.IN_PROGRESS.value:
        return False
    created_at = record.created_at
    if created_at is None:
        # Not yet persisted (no creation timestamp) — treat as freshly created.
        return False
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)
    cutoff = utcnow() - timedelta(seconds=settings.idempotency_in_progress_ttl_seconds)
    return created_at <= cutoff


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
    if _reservation_is_stale(existing):
        # Abandoned by a crashed request: don't report an in-progress conflict. Returning
        # None lets the mutation flow proceed to claim, which takes the reservation over.
        return None
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


async def _reclaim_stale_reservation(
    db: AsyncSession,
    *,
    key_hash: str,
    fingerprint: str,
    method: str,
    path: str,
) -> IdempotencyReservation | None:
    """Take over an in-progress reservation abandoned by a crashed request.

    Without this, a process that claims a reservation and then dies (crash, OOM,
    SIGKILL) before completing it leaves an ``in_progress`` row that makes every
    identical retry receive a 409 forever. Once the reservation is older than the
    configured TTL we treat it as abandoned and let a matching retry re-own it.
    Returns ``None`` (deferring to the normal replay/conflict path) when the row is
    absent, already terminal, fingerprint-mismatched, or not yet stale.
    """

    record = (
        await db.execute(
            select(IdempotencyKey).where(IdempotencyKey.key_hash == key_hash).with_for_update()
        )
    ).scalar_one_or_none()
    # Re-check staleness under the row lock to resolve races with the owning request.
    if (
        record is None
        or record.request_fingerprint != fingerprint
        or not _reservation_is_stale(record)
    ):
        await db.rollback()
        return None

    # Re-own the abandoned reservation and restart its staleness clock.
    record.request_method = method
    record.request_path = path
    record.created_at = utcnow()
    record_id = record.id
    await db.commit()
    logger.warning(
        "idempotency_reservation_reclaimed",
        key_hash=key_hash,
        detail="Took over an abandoned in-progress idempotency reservation.",
    )
    return IdempotencyReservation(record_id=record_id)


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
    reclaimed = await _reclaim_stale_reservation(
        db,
        key_hash=key_hash,
        fingerprint=fingerprint,
        method=method,
        path=path,
    )
    if reclaimed is not None:
        return reclaimed
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
    record.completed_at = utcnow()


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
    pre_commit_failure_cleanup: Callable[[], Awaitable[None]] | None = None,
) -> Response:
    """Run the shared replay/claim/mutate/complete/commit mutation flow."""

    resolved_ops = ops or DEFAULT_IDEMPOTENT_MUTATION_OPS
    after_commit: Callable[[], Awaitable[None]] | None = None
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

    commit_started = False
    try:
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
        commit_started = True
        await db.commit()
    except HTTPException as exc:
        snapshot = (
            _extract_replayable_http_error_snapshot(exc)
            if reservation_or_response is not None
            else None
        )
        if snapshot is None or commit_started:
            if not commit_started and pre_commit_failure_cleanup is not None:
                await pre_commit_failure_cleanup()
            raise

        try:
            await db.rollback()
            status_code, response_body = snapshot
            response = await resolved_ops.complete(
                db,
                reservation_or_response,
                status_code=status_code,
                response_body=response_body,
            )
            commit_started = True
            await db.commit()
        except BaseException:
            if not commit_started and pre_commit_failure_cleanup is not None:
                await pre_commit_failure_cleanup()
            raise
    except BaseException:
        if not commit_started and pre_commit_failure_cleanup is not None:
            await pre_commit_failure_cleanup()
        raise

    if after_commit is not None:
        await after_commit()
    return response
