"""Shared cursor pagination helpers for API routers."""

import base64
import binascii
import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Never, cast
from uuid import UUID

from fastapi import HTTPException, status
from pydantic import BaseModel, ValidationError

from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200


def encode_cursor_payload(
    payload: Mapping[str, object],
    *,
    compact: bool = False,
) -> str:
    """Encode a JSON cursor payload as URL-safe base64 without padding."""
    json_payload = (
        json.dumps(
            payload,
            separators=(",", ":"),
        )
        if compact
        else json.dumps(payload)
    )
    return base64.urlsafe_b64encode(json_payload.encode("utf-8")).decode("utf-8").rstrip("=")


def decode_cursor_payload(cursor: str) -> dict[str, Any]:
    """Decode a URL-safe base64 JSON cursor payload."""
    try:
        padded_cursor = cursor + ("=" * (-len(cursor) % 4))
        decoded = base64.urlsafe_b64decode(padded_cursor.encode("utf-8"))
        payload_raw = json.loads(decoded.decode("utf-8"))
        if not isinstance(payload_raw, dict):
            raise TypeError("Cursor payload must be a JSON object")
        return cast(dict[str, Any], payload_raw)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
        raise_invalid_cursor(exc)


def encode_keyset_cursor(
    payload: Mapping[str, object] | BaseModel,
    *,
    compact: bool = False,
) -> str:
    """Encode a mapping or Pydantic model as an opaque keyset cursor."""
    if isinstance(payload, BaseModel):
        return (
            base64.urlsafe_b64encode(payload.model_dump_json().encode("utf-8"))
            .decode("utf-8")
            .rstrip("=")
        )
    return encode_cursor_payload(payload, compact=compact)


def decode_keyset_cursor[CursorModelT: BaseModel](
    cursor: str, cursor_model: type[CursorModelT]
) -> CursorModelT:
    """Decode an opaque keyset cursor into a typed Pydantic model."""
    try:
        return cursor_model.model_validate(decode_cursor_payload(cursor))
    except ValidationError as exc:
        raise_invalid_cursor(exc)


def read_cursor_datetime(payload: Mapping[str, object], key: str) -> datetime:
    """Read an ISO datetime field from a decoded cursor payload."""
    try:
        value = payload[key]
        if not isinstance(value, str):
            raise TypeError(f"Cursor field {key!r} must be a string")
        return datetime.fromisoformat(value)
    except (KeyError, TypeError, ValueError) as exc:
        raise_invalid_cursor(exc)


def read_cursor_uuid(payload: Mapping[str, object], key: str) -> UUID:
    """Read a UUID field from a decoded cursor payload."""
    try:
        return UUID(str(payload[key]))
    except (KeyError, TypeError, ValueError) as exc:
        raise_invalid_cursor(exc)


def read_cursor_int(payload: Mapping[str, object], key: str) -> int:
    """Read an integer field from a decoded cursor payload."""
    try:
        value = payload[key]
        if not isinstance(value, str | int):
            raise TypeError(f"Cursor field {key!r} must be a string or integer")
        return int(value)
    except (KeyError, TypeError, ValueError) as exc:
        raise_invalid_cursor(exc)


def raise_invalid_cursor(exc: Exception) -> Never:
    """Raise the standard invalid-cursor HTTP error envelope."""
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=create_error_response(
            code=ErrorCode.INVALID_CURSOR,
            message="Invalid cursor format",
            details=None,
        ),
    ) from exc
