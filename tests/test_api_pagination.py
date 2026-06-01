"""Tests for shared API pagination helpers."""

from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

import pytest
from fastapi import HTTPException
from pydantic import BaseModel

from app.api.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    decode_cursor_payload,
    decode_keyset_cursor,
    encode_keyset_cursor,
    read_cursor_datetime,
    read_cursor_int,
    read_cursor_uuid,
)
from app.core.errors import ErrorCode

_ROW_ID = UUID("12345678-1234-5678-1234-567812345678")


class _TimestampCursor(BaseModel):
    """Typed timestamp/id cursor used by API routes."""

    created_at: datetime
    id: UUID


class _LineCursor(BaseModel):
    """Typed line/id cursor used by API routes."""

    line_number: int
    id: UUID


def _assert_invalid_cursor(exc: HTTPException) -> None:
    assert exc.status_code == 400
    detail = cast(dict[str, Any], exc.detail)
    assert detail == {
        "error": {
            "code": ErrorCode.INVALID_CURSOR.value,
            "message": "Invalid cursor format",
            "details": None,
        }
    }


def test_shared_pagination_limits_match_trd_contract() -> None:
    assert DEFAULT_PAGE_SIZE == 50
    assert MAX_PAGE_SIZE == 200


def test_encode_keyset_cursor_round_trips_mapping_payload() -> None:
    payload = {"created_at": "2024-01-02T03:04:05+00:00", "id": str(_ROW_ID)}

    cursor = encode_keyset_cursor(payload)

    assert decode_cursor_payload(cursor) == payload


def test_encode_keyset_cursor_round_trips_pydantic_payload() -> None:
    payload = _TimestampCursor(created_at=datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC), id=_ROW_ID)

    cursor = encode_keyset_cursor(payload)

    decoded = decode_keyset_cursor(cursor, _TimestampCursor)

    assert decoded == payload


def test_decode_keyset_cursor_rejects_invalid_cursor_envelope() -> None:
    with pytest.raises(HTTPException) as exc_info:
        decode_keyset_cursor("not-valid-base64", _TimestampCursor)

    _assert_invalid_cursor(exc_info.value)


def test_decode_keyset_cursor_rejects_schema_mismatch_with_invalid_cursor() -> None:
    cursor = encode_keyset_cursor({"created_at": "2024-01-02T03:04:05+00:00"})

    with pytest.raises(HTTPException) as exc_info:
        decode_keyset_cursor(cursor, _TimestampCursor)

    _assert_invalid_cursor(exc_info.value)


def test_mapping_legacy_golden_cursor_payload_still_decodes() -> None:
    legacy_cursor = (
        "eyJjcmVhdGVkX2F0IjogIjIwMjQtMDEtMDJUMDM6MDQ6MDUrMDA6MDAiLCAiaWQiOiAiMTIzND"
        "U2NzgtMTIzNC01Njc4LTEyMzQtNTY3ODEyMzQ1Njc4In0"
    )

    decoded = decode_cursor_payload(legacy_cursor)

    assert decoded == {"created_at": "2024-01-02T03:04:05+00:00", "id": str(_ROW_ID)}


def test_pydantic_legacy_golden_cursor_payload_still_round_trips() -> None:
    legacy_cursor = (
        "eyJjcmVhdGVkX2F0IjoiMjAyNC0wMS0wMlQwMzowNDowNVoiLCJpZCI6IjEyMzQ1Njc4LTEy"
        "MzQtNTY3OC0xMjM0LTU2NzgxMjM0NTY3OCJ9"
    )
    payload = _TimestampCursor(created_at=datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC), id=_ROW_ID)

    assert encode_keyset_cursor(payload) == legacy_cursor
    assert decode_keyset_cursor(legacy_cursor, _TimestampCursor) == payload


def test_typed_cursor_payload_readers_parse_expected_types() -> None:
    payload = {
        "created_at": "2024-01-02T03:04:05+00:00",
        "id": str(_ROW_ID),
        "line_number": "7",
    }

    assert read_cursor_datetime(payload, "created_at") == datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert read_cursor_uuid(payload, "id") == _ROW_ID
    assert read_cursor_int(payload, "line_number") == 7


def test_typed_cursor_payload_readers_raise_invalid_cursor() -> None:
    with pytest.raises(HTTPException) as exc_info:
        read_cursor_int({"line_number": "not-an-int"}, "line_number")

    _assert_invalid_cursor(exc_info.value)


def test_decode_keyset_cursor_validates_integer_cursor_model() -> None:
    cursor = encode_keyset_cursor({"line_number": 7, "id": str(_ROW_ID)})

    assert decode_keyset_cursor(cursor, _LineCursor) == _LineCursor(line_number=7, id=_ROW_ID)
