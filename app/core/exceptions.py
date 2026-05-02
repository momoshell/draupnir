"""Custom exception handlers and error response utilities."""

from http import HTTPStatus
from typing import Any

from fastapi import HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.core.errors import ErrorCode


class APIError(BaseModel):
    """Standard API error response structure."""

    code: ErrorCode
    message: str
    details: Any | None = None


class APIErrorResponse(BaseModel):
    """Wrapper for API error responses."""

    error: APIError


def create_error_response(
    code: ErrorCode,
    message: str,
    details: Any | None = None,
) -> dict[str, Any]:
    """Create a standardized error response dictionary.

    Args:
        code: Enumerated error code
        message: Human-readable error message
        details: Optional additional error details

    Returns:
        Dictionary matching the APIErrorResponse schema
    """
    return APIErrorResponse(
        error=APIError(code=code, message=message, details=details)
    ).model_dump(mode="json")


def _status_to_error_code(status_code: int) -> ErrorCode:
    """Map bare HTTP status codes to the shared taxonomy."""
    if status_code == status.HTTP_404_NOT_FOUND:
        return ErrorCode.NOT_FOUND
    if status_code == status.HTTP_409_CONFLICT:
        return ErrorCode.DB_CONFLICT
    if status_code == status.HTTP_422_UNPROCESSABLE_CONTENT:
        return ErrorCode.VALIDATION_ERROR
    if 400 <= status_code < 500:
        return ErrorCode.INPUT_INVALID
    return ErrorCode.INTERNAL_ERROR


def _default_http_message(status_code: int) -> str:
    """Return the standard message for a bare HTTP status code."""
    try:
        return HTTPStatus(status_code).phrase
    except ValueError:
        return "HTTP error"


def raise_not_found(resource: str, identifier: str) -> None:
    """Raise a 404 HTTPException with standardized error format.

    Args:
        resource: Type of resource (e.g., 'Project')
        identifier: The identifier that was not found
    """
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=create_error_response(
            code=ErrorCode.NOT_FOUND,
            message=f"{resource} with identifier '{identifier}' not found",
            details=None,
        ),
    )


async def custom_http_exception_handler(request: Request, exc: Exception) -> Response:
    """Return the standard error envelope for all HTTPException responses."""
    _ = request
    if not isinstance(exc, StarletteHTTPException):
        raise exc

    if isinstance(exc.detail, dict) and "error" in exc.detail:
        return JSONResponse(
            status_code=exc.status_code,
            content=exc.detail,
            headers=exc.headers,
        )

    message = exc.detail if isinstance(exc.detail, str) else _default_http_message(exc.status_code)
    details = None if isinstance(exc.detail, str) else exc.detail
    return JSONResponse(
        status_code=exc.status_code,
        headers=exc.headers,
        content=create_error_response(
            code=_status_to_error_code(exc.status_code),
            message=message,
            details=details,
        ),
    )


async def request_validation_exception_handler(request: Request, exc: Exception) -> Response:
    """Return standardized validation error envelope for 422 responses."""
    _ = request
    if not isinstance(exc, RequestValidationError):
        raise exc

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content=create_error_response(
            code=ErrorCode.VALIDATION_ERROR,
            message="Request validation failed",
            details=exc.errors(),
        ),
    )
