"""Tests for RequestIdMiddleware."""

import uuid

from fastapi import FastAPI
from starlette.testclient import TestClient

from app.core.config import settings
from app.core.logging import configure_logging
from app.core.middleware import RequestIdMiddleware


def create_test_app() -> FastAPI:
    """Create a minimal test app with the middleware."""
    configure_logging(service_name=settings.service_name, log_level="DEBUG")
    app = FastAPI()
    app.add_middleware(RequestIdMiddleware)

    @app.get("/test")
    def test_endpoint() -> dict:
        return {"ok": True}

    return app


client = TestClient(create_test_app())


class TestRequestIdMiddleware:
    """Test cases for X-Request-Id middleware."""

    def test_generates_request_id_when_absent(self) -> None:
        """Should generate a request ID when X-Request-Id header is absent."""
        response = client.get("/test")

        assert response.status_code == 200
        assert "X-Request-Id" in response.headers

        request_id = response.headers["X-Request-Id"]
        # Should be a valid UUID
        try:
            uuid.UUID(request_id, version=4)
        except ValueError:
            raise AssertionError(
                f"Generated request ID is not a valid UUID: {request_id}"
            ) from None

    def test_preserves_valid_existing_request_id(self) -> None:
        """Should preserve a valid X-Request-Id header if provided."""
        test_request_id = "valid-id-12345"
        response = client.get("/test", headers={"X-Request-Id": test_request_id})

        assert response.status_code == 200
        assert response.headers["X-Request-Id"] == test_request_id

    def test_rejects_invalid_request_id_and_generates_new(self) -> None:
        """Should reject invalid request IDs and generate a new UUID."""
        # Test with too long ID
        invalid_long_id = "a" * 100
        response = client.get("/test", headers={"X-Request-Id": invalid_long_id})

        assert response.status_code == 200
        request_id = response.headers["X-Request-Id"]
        # Should be a valid UUID (not the invalid long string)
        try:
            uuid.UUID(request_id, version=4)
        except ValueError:
            raise AssertionError(
                f"Should have generated valid UUID, got: {request_id}"
            ) from None

    def test_rejects_malformed_request_id(self) -> None:
        """Should reject request IDs with invalid characters."""
        invalid_id = "../../../etc/passwd"
        response = client.get("/test", headers={"X-Request-Id": invalid_id})

        assert response.status_code == 200
        request_id = response.headers["X-Request-Id"]
        # Should be a valid UUID
        try:
            uuid.UUID(request_id, version=4)
        except ValueError:
            raise AssertionError(
                f"Should have generated valid UUID, got: {request_id}"
            ) from None

    def test_different_requests_get_different_ids(self) -> None:
        """Each request should get a unique request ID."""
        response1 = client.get("/test")
        response2 = client.get("/test")

        id1 = response1.headers["X-Request-Id"]
        id2 = response2.headers["X-Request-Id"]

        assert id1 != id2
