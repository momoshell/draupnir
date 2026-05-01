"""Custom exception handlers and error response utilities."""

from typing import Any

from fastapi import HTTPException, Request, status
from fastapi.exception_handlers import http_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel


class APIError(BaseModel):
    """Standard API error response structure."""

    code: str
    message: str
    details: Any | None = None


class APIErrorResponse(BaseModel):
    """Wrapper for API error responses."""

    error: APIError


def create_error_response(code: str, message: str, details: Any | None = None) -> dict[str, Any]:
    """Create a standardized error response dictionary.

    Args:
        code: Error code (e.g., 'NOT_FOUND', 'VALIDATION_ERROR')
        message: Human-readable error message
        details: Optional additional error details

    Returns:
        Dictionary matching the APIErrorResponse schema
    """
    return {"error": {"code": code, "message": message, "details": details}}


def raise_not_found(resource: str, identifier: str) -> None:
    """Raise a 404 HTTPException with standardized error format.

    Args:
        resource: Type of resource (e.g., 'Project')
        identifier: The identifier that was not found
    """
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=create_error_response(
            code="NOT_FOUND",
            message=f"{resource} with identifier '{identifier}' not found",
            details=None,
        ),
    )


async def custom_http_exception_handler(request: Request, exc: Exception) -> Response:
    """Return custom envelope when HTTPException detail already contains `error`."""
    if not isinstance(exc, HTTPException):
        raise exc

    if isinstance(exc.detail, dict) and "error" in exc.detail:
        return JSONResponse(status_code=exc.status_code, content=exc.detail)
    return await http_exception_handler(request, exc)


async def request_validation_exception_handler(request: Request, exc: Exception) -> Response:
    """Return standardized validation error envelope for 422 responses."""
    _ = request
    if not isinstance(exc, RequestValidationError):
        raise exc

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=create_error_response(
            code="VALIDATION_ERROR",
            message="Request validation failed",
            details=exc.errors(),
        ),
    )
