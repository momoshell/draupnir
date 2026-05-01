"""Tests for RequestIdMiddleware."""

import uuid
from typing import Any

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from app.core.config import settings
from app.core.logging import configure_logging
from app.core.middleware import ContentLengthLimitMiddleware, RequestIdMiddleware


def create_test_app() -> FastAPI:
    """Create a minimal test app with the middleware."""
    configure_logging(service_name=settings.service_name, log_level="DEBUG")
    app = FastAPI()
    app.add_middleware(ContentLengthLimitMiddleware)
    app.add_middleware(RequestIdMiddleware)

    @app.get("/test")
    def test_endpoint() -> dict[str, Any]:
        return {"ok": True}

    @app.post("/v1/projects/{project_id}/files")
    def upload_endpoint(project_id: str) -> dict[str, Any]:
        _ = project_id
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

    def test_rejects_oversized_upload_request_with_413(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Oversized upload requests should be rejected before handler execution."""
        monkeypatch.setattr(settings, "max_upload_mb", 1)
        max_request_body_bytes = (
            settings.max_upload_mb * 1024 * 1024
            + ContentLengthLimitMiddleware._MULTIPART_OVERHEAD_BYTES
        )

        def stream() -> Any:
            yield b"streamed-upload-body"

        request = client.build_request(
            "POST",
            f"/v1/projects/{uuid.uuid4()}/files",
            content=stream(),
            headers={"content-length": str(max_request_body_bytes + 1)},
        )
        response = client.send(request)

        assert response.status_code == 413
        assert response.json() == {
            "error": {
                "code": "INPUT_INVALID",
                "message": "Request body exceeds maximum allowed size for uploads.",
                "details": None,
            }
        }

    def test_allows_upload_within_file_cap_plus_multipart_overhead(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Multipart requests should allow bounded envelope overhead."""
        monkeypatch.setattr(settings, "max_upload_mb", 1)
        file_cap_bytes = settings.max_upload_mb * 1024 * 1024
        payload = b"%PDF-1.7\n" + (b"x" * (file_cap_bytes - len(b"%PDF-1.7\n")))

        response = client.post(
            f"/v1/projects/{uuid.uuid4()}/files",
            files={"file": ("within-cap.pdf", payload, "application/pdf")},
        )

        assert response.status_code == 200
        assert response.json() == {"ok": True}

    def test_rejects_upload_request_missing_content_length_with_411(self) -> None:
        """Upload requests without Content-Length should fail pre-parse."""

        def stream() -> Any:
            yield b"streamed-upload-body"

        request = client.build_request(
            "POST",
            f"/v1/projects/{uuid.uuid4()}/files",
            content=stream(),
        )

        response = client.send(request)
        assert response.status_code == 411
        assert response.json() == {
            "error": {
                "code": "INPUT_INVALID",
                "message": (
                    "Content-Length header is required for upload requests. "
                    "This API enforces max_upload_mb as a maximum request body size "
                    "pre-parse guard."
                ),
                "details": None,
            }
        }

    def test_rejects_upload_request_invalid_content_length_with_400(self) -> None:
        """Upload requests with invalid Content-Length should fail pre-parse."""

        def stream() -> Any:
            yield b"streamed-upload-body"

        request = client.build_request(
            "POST",
            f"/v1/projects/{uuid.uuid4()}/files",
            content=stream(),
            headers={"content-length": "not-an-integer"},
        )

        response = client.send(request)
        assert response.status_code == 400
        assert response.json() == {
            "error": {
                "code": "INPUT_INVALID",
                "message": "Content-Length header must be a valid integer.",
                "details": None,
            }
        }
