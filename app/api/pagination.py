"""Shared cursor pagination helpers for API routers."""

import base64
import binascii
import json
from collections.abc import Mapping
from typing import Any, Never, cast

from fastapi import HTTPException, status

from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response


def encode_cursor_payload(
    payload: Mapping[str, object],
    *,
    compact: bool = False,
) -> str:
    """Encode a JSON cursor payload as URL-safe base64 without padding."""
    json_payload = json.dumps(
        payload,
        separators=(",", ":"),
    ) if compact else json.dumps(payload)
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
