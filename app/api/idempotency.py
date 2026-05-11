"""Endpoint-integrated idempotency helpers for mutating API routes."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Any

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
