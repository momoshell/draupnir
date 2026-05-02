"""Tests for shared error taxonomy and envelope helpers."""

import json
import re
from pathlib import Path

import httpx
import pytest
from fastapi import HTTPException, status
from fastapi.exceptions import RequestValidationError
from httpx import ASGITransport
from starlette.requests import Request

from app.core.errors import ErrorCode
from app.core.exceptions import (
    create_error_response,
    custom_http_exception_handler,
    request_validation_exception_handler,
)
from app.main import app as fastapi_app


def _build_request() -> Request:
    """Build a minimal ASGI request for handler unit tests."""
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": "/",
            "raw_path": b"/",
            "query_string": b"",
            "headers": [],
            "client": ("testclient", 50000),
            "server": ("testserver", 80),
        }
    )


def test_create_error_response_uses_enum_values() -> None:
    """Error envelope helper should serialize enum values exactly."""
    assert create_error_response(
        ErrorCode.INTERNAL_ERROR,
        "Unexpected failure",
        {"request_id": "req-123"},
    ) == {
        "error": {
            "code": "INTERNAL_ERROR",
            "message": "Unexpected failure",
            "details": {"request_id": "req-123"},
        }
    }


@pytest.mark.asyncio
async def test_custom_http_exception_handler_wraps_plain_http_exception() -> None:
    """Bare HTTPException responses should be normalized to the standard envelope."""
    response = await custom_http_exception_handler(
        _build_request(),
        HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found"),
    )

    assert response.status_code == 404
    assert json.loads(bytes(response.body)) == {
        "error": {
            "code": "NOT_FOUND",
            "message": "Not Found",
            "details": None,
        }
    }


@pytest.mark.asyncio
async def test_custom_http_exception_handler_preserves_existing_error_envelope() -> None:
    """Existing standardized envelopes should pass through unchanged."""
    envelope = create_error_response(ErrorCode.DB_CONFLICT, "Conflict", None)

    response = await custom_http_exception_handler(
        _build_request(),
        HTTPException(status_code=status.HTTP_409_CONFLICT, detail=envelope),
    )

    assert response.status_code == 409
    assert json.loads(bytes(response.body)) == envelope


@pytest.mark.asyncio
async def test_custom_http_exception_handler_wraps_non_string_detail() -> None:
    """Non-string HTTPException detail should become envelope details."""
    response = await custom_http_exception_handler(
        _build_request(),
        HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"reason": "broker unavailable"},
        ),
    )

    assert response.status_code == 500
    assert json.loads(bytes(response.body)) == {
        "error": {
            "code": "INTERNAL_ERROR",
            "message": "Internal Server Error",
            "details": {"reason": "broker unavailable"},
        }
    }


@pytest.mark.asyncio
async def test_request_validation_exception_handler_uses_validation_error_code() -> None:
    """Request validation handler should emit the shared validation envelope."""
    exc = RequestValidationError(
        [
            {
                "type": "missing",
                "loc": ("query", "cursor"),
                "msg": "Field required",
                "input": None,
            }
        ]
    )

    response = await request_validation_exception_handler(_build_request(), exc)

    assert response.status_code == 422
    assert json.loads(bytes(response.body)) == {
        "error": {
            "code": "VALIDATION_ERROR",
            "message": "Request validation failed",
            "details": [
                {
                    "type": "missing",
                    "loc": ["query", "cursor"],
                    "msg": "Field required",
                    "input": None,
                }
            ],
        }
    }


@pytest.mark.asyncio
async def test_unknown_route_uses_standard_error_envelope() -> None:
    """Framework-generated 404 responses should use the shared error envelope."""
    transport = ASGITransport(app=fastapi_app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/unknown-route")

    assert response.status_code == 404
    assert response.json() == {
        "error": {
            "code": "NOT_FOUND",
            "message": "Not Found",
            "details": None,
        }
    }


def test_error_code_enum_matches_trd_taxonomy() -> None:
    """TRD error taxonomy must stay in lockstep with the shared enum."""
    trd_path = Path(__file__).resolve().parents[1] / "docs" / "TRD.md"
    trd_text = trd_path.read_text(encoding="utf-8")
    match = re.search(r"## Error Taxonomy\n\n(.*?)(?:\n## |\Z)", trd_text, re.DOTALL)

    assert match is not None
    documented_codes = re.findall(r"^- `([A-Z_]+)` - ", match.group(1), re.MULTILINE)

    assert documented_codes == [code.value for code in ErrorCode]
