"""Smoke tests for Draupnir."""

import io
import json
import logging
import os
from collections.abc import AsyncGenerator

import httpx
import pytest
from fastapi import FastAPI
from httpx import ASGITransport

from app import __version__
from app.core.config import settings
from app.core.middleware import REQUEST_ID_PATTERN
from app.main import app as fastapi_app


def test_version() -> None:
    """Test that version is a non-empty string."""
    assert isinstance(__version__, str)
    assert len(__version__) > 0


@pytest.fixture
def app() -> FastAPI:
    """Provide the FastAPI application instance for testing."""
    return fastapi_app


@pytest.fixture
async def async_client(app: FastAPI) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an async HTTP client for testing."""
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


class TestHealthEndpoint:
    """Smoke tests for the health check endpoint."""

    async def test_health_endpoint_returns_200_and_json_shape(
        self,
        async_client: httpx.AsyncClient,
    ) -> None:
        """Test that GET /v1/health returns 200 with correct JSON shape.

        Success case: status is "ok", version field is present and non-null
        when settings.expose_version_in_health is True, otherwise null.
        """
        response = await async_client.get("/v1/health")

        # Assert status code
        assert response.status_code == 200

        # Parse JSON response
        data = response.json()

        # Assert required status field
        assert data["status"] == "ok"

        # Assert exact keys in response
        assert set(data.keys()) == {"status", "version"}

        # Assert version field based on expose_version_in_health setting
        if settings.expose_version_in_health:
            assert isinstance(data["version"], str)
            assert len(data["version"]) > 0
        else:
            assert data["version"] is None

    async def test_health_endpoint_returns_request_id(
        self,
        async_client: httpx.AsyncClient,
    ) -> None:
        """Test that GET /v1/health returns a valid X-Request-Id header.

        Success case: X-Request-Id header exists and matches the expected pattern
        (alphanumeric, hyphens, underscores, max 64 chars).
        """
        response = await async_client.get("/v1/health")

        # Assert status code
        assert response.status_code == 200

        # Assert X-Request-Id header exists
        assert "X-Request-Id" in response.headers

        request_id = response.headers["X-Request-Id"]

        # Assert request ID matches the expected pattern
        assert REQUEST_ID_PATTERN.match(request_id) is not None, (
            f"Request ID '{request_id}' does not match pattern "
            f"{REQUEST_ID_PATTERN.pattern}"
        )

        # Assert request ID length is within bounds (max 64 chars)
        assert len(request_id) <= 64, (
            f"Request ID '{request_id}' exceeds maximum length of 64 characters"
        )

        # Assert request ID is not empty
        assert len(request_id) > 0, "Request ID should not be empty"

    async def test_health_endpoint_logs_request_id(
        self,
        async_client: httpx.AsyncClient,
    ) -> None:
        """Test that request_id appears in structlog JSON output.

        Success case: The same request_id from the X-Request-Id response header
        appears in the log output as the `request_id` field in a JSON log line.

        structlog writes JSON to stdout via a StreamHandler. We temporarily
        replace the handler's stream with a StringIO to capture the output.
        """
        # Get the root logger and its handler
        root_logger = logging.getLogger()
        handler = root_logger.handlers[0] if root_logger.handlers else None

        if handler is None or not isinstance(handler, logging.StreamHandler):
            pytest.skip("No StreamHandler configured")

        # Save original stream and replace with StringIO
        original_stream = handler.stream
        captured_output = io.StringIO()
        handler.stream = captured_output

        try:
            response = await async_client.get("/v1/health")

            # Assert status code
            assert response.status_code == 200

            # Get the request_id from the response header
            assert "X-Request-Id" in response.headers
            request_id = response.headers["X-Request-Id"]
        finally:
            # Restore original stream
            handler.stream = original_stream

        # Get captured output where structlog writes JSON lines
        stdout_output = captured_output.getvalue()

        # Parse each line as JSON and look for request_id field
        found_request_id_in_logs = False
        for line in stdout_output.strip().split("\n"):
            if not line.strip():
                continue
            try:
                log_entry = json.loads(line)
                if log_entry.get("request_id") == request_id:
                    found_request_id_in_logs = True
                    break
            except json.JSONDecodeError:
                # Skip non-JSON lines
                continue

        assert found_request_id_in_logs, (
            f"Request ID '{request_id}' from response header not found "
            f"in structlog JSON output. Captured stdout: {stdout_output[:500]}"
        )


# Skip unless SMOKE_BASE_URL is set (for compose-stack testing)
SMOKE_BASE_URL = os.environ.get("SMOKE_BASE_URL", "")


@pytest.mark.skipif(
    not SMOKE_BASE_URL,
    reason="SMOKE_BASE_URL not set (set to run against real server)",
)
class TestHealthEndpointRealServer:
    """Smoke tests against a real running server (compose stack)."""

    @pytest.fixture
    async def real_async_client(self) -> AsyncGenerator[httpx.AsyncClient, None]:
        """Provide an async HTTP client for testing against real server."""
        base_url = SMOKE_BASE_URL or "http://localhost:8000"
        async with httpx.AsyncClient(base_url=base_url) as client:
            yield client

    async def test_health_endpoint_against_real_server(
        self,
        real_async_client: httpx.AsyncClient,
    ) -> None:
        """Test that GET /v1/health returns 200 against real server.

        Success case: Real server responds with 200 and correct JSON shape.
        This test only runs when SMOKE_BASE_URL environment variable is set,
        allowing CI to test against the compose stack after `docker compose up`.
        """
        response = await real_async_client.get("/v1/health")

        # Assert status code
        assert response.status_code == 200

        # Parse JSON response
        data = response.json()

        # Assert required status field
        assert data["status"] == "ok"

        # Assert X-Request-Id header exists
        assert "X-Request-Id" in response.headers
        request_id = response.headers["X-Request-Id"]

        # Assert request ID matches the expected pattern
        assert REQUEST_ID_PATTERN.match(request_id) is not None

        # Assert request ID length is within bounds
        assert len(request_id) <= 64
        assert len(request_id) > 0
