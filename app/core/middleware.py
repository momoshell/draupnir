"""Request ID middleware for tracking requests across the system."""

import re
import uuid
from collections.abc import Awaitable, Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from structlog.contextvars import bind_contextvars, clear_contextvars

# Valid request ID pattern: alphanumeric, hyphens, underscores, max 64 chars
REQUEST_ID_PATTERN = re.compile(r"^[a-zA-Z0-9\-_]{1,64}$")
MAX_REQUEST_ID_LENGTH = 64


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
