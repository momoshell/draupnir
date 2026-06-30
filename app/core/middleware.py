"""Request ID middleware for tracking requests across the system."""

import re
import uuid
from collections.abc import Awaitable, Callable

from fastapi import status
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from structlog.contextvars import bind_contextvars, clear_contextvars

from app.core.config import settings
from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response

# Valid request ID pattern: alphanumeric, hyphens, underscores, max 64 chars
REQUEST_ID_PATTERN = re.compile(r"^[a-zA-Z0-9\-_]{1,64}$")
MAX_REQUEST_ID_LENGTH = 64


class ContentLengthLimitMiddleware(BaseHTTPMiddleware):
    """Pre-parse request-size guard for upload and JSON/API request bodies."""

    _BODY_METHODS = frozenset({"POST", "PUT", "PATCH"})
    _MULTIPART_OVERHEAD_BYTES = 1024 * 1024
    _UPLOAD_ROUTE_PREFIX = "" if settings.api_prefix == "/" else settings.api_prefix
    _UPLOAD_PATH_PATTERN = re.compile(
        rf"^{re.escape(_UPLOAD_ROUTE_PREFIX)}/projects/[^/]+/files/?$"
    )

    @classmethod
    def _is_upload_request(cls, request: Request) -> bool:
        """Return True only for POST upload route."""
        return request.method == "POST" and bool(cls._UPLOAD_PATH_PATTERN.match(request.url.path))

    @classmethod
    def _has_limited_body(cls, request: Request) -> bool:
        return request.method in cls._BODY_METHODS

    @staticmethod
    def _invalid_content_length_response() -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=create_error_response(
                code=ErrorCode.INPUT_INVALID,
                message="Content-Length header must be a valid integer.",
                details=None,
            ),
        )

    @staticmethod
    def _oversized_body_response(*, upload: bool) -> JSONResponse:
        message = (
            "Request body exceeds maximum allowed size for uploads."
            if upload
            else "Request body exceeds maximum allowed size."
        )
        return JSONResponse(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            content=create_error_response(
                code=ErrorCode.INPUT_INVALID,
                message=message,
                details=None,
            ),
        )

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        if self._is_upload_request(request):
            content_length = request.headers.get("content-length")
            if content_length is None:
                return JSONResponse(
                    status_code=status.HTTP_411_LENGTH_REQUIRED,
                    content=create_error_response(
                        code=ErrorCode.INPUT_INVALID,
                        message=(
                            "Content-Length header is required for upload requests. "
                            "This API enforces max_upload_mb as a maximum request body size "
                            "pre-parse guard."
                        ),
                        details=None,
                    ),
                )

            try:
                content_length_bytes = int(content_length)
            except ValueError:
                return self._invalid_content_length_response()

            # Middleware allows bounded multipart overhead; endpoint enforces exact
            # uploaded file-byte cap.
            max_file_bytes = settings.max_upload_mb * 1024 * 1024
            max_request_body_bytes = max_file_bytes + self._MULTIPART_OVERHEAD_BYTES
            if content_length_bytes > max_request_body_bytes:
                return self._oversized_body_response(upload=True)

        elif self._has_limited_body(request):
            content_length = request.headers.get("content-length")
            if content_length is not None:
                try:
                    content_length_bytes = int(content_length)
                except ValueError:
                    return self._invalid_content_length_response()

                max_request_body_bytes = settings.max_request_body_mb * 1024 * 1024
                if content_length_bytes > max_request_body_bytes:
                    return self._oversized_body_response(upload=False)

        return await call_next(request)


class RequestIdMiddleware(BaseHTTPMiddleware):
    """Middleware to handle X-Request-Id header for request tracing.

    Reads X-Request-Id from incoming requests, generates one if absent or invalid,
    and binds it to the structlog context for the request lifetime.
    """

    REQUEST_ID_HEADER = "X-Request-Id"

    def _get_or_generate_request_id(self, request: Request) -> str:
        """Extract or generate a valid request ID."""
        request_id = request.headers.get(self.REQUEST_ID_HEADER)

        # Validate: must match pattern and length
        if request_id and REQUEST_ID_PATTERN.match(request_id):
            return request_id

        # Generate new UUID4 if invalid or missing
        return str(uuid.uuid4())

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        """Process the request, extract or generate request ID, then call next middleware."""
        # Get or generate valid request ID
        request_id = self._get_or_generate_request_id(request)

        # Bind to structlog context for this request
        # Note: clear_contextvars() will remove service, but add_service_name processor
        # will re-add it if missing
        clear_contextvars()
        bind_contextvars(request_id=request_id)

        # Process request
        response = await call_next(request)

        # Add request ID to response headers
        response.headers[self.REQUEST_ID_HEADER] = request_id

        return response
