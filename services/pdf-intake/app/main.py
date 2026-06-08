"""Containerized PDF intake service (boundary scaffold).

This service owns the PDF-specific dependencies (notably the AGPL-or-commercial
PyMuPDF stack and the raster tooling) so they stay out of the lean core API and
worker runtime. It exposes a stable health/capabilities contract and a
placeholder ingest endpoint.

Real PDF extraction over this boundary is deferred to a follow-up; the ingest
endpoint intentionally returns HTTP 501. See ADR 0010 in the core repository.
"""

from __future__ import annotations

from typing import Final

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

SERVICE_NAME: Final[str] = "draupnir-pdf-intake"
SERVICE_VERSION: Final[str] = "0.1.0"
# Modes advertised through the boundary; mirrors the core InputFamily PDF modes.
SUPPORTED_MODES: Final[tuple[str, ...]] = ("vector", "raster")

app = FastAPI(title="Draupnir PDF Intake Service", version=SERVICE_VERSION)


class HealthResponse(BaseModel):
    """Liveness contract consumed by the core capability/health probes."""

    status: str
    service: str
    version: str


class CapabilitiesResponse(BaseModel):
    """Capabilities contract advertising reachability and supported modes."""

    service: str
    version: str
    modes: list[str]
    extraction_implemented: bool


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    """Report service liveness."""

    return HealthResponse(status="ok", service=SERVICE_NAME, version=SERVICE_VERSION)


@app.get("/capabilities", response_model=CapabilitiesResponse)
def capabilities() -> CapabilitiesResponse:
    """Report supported modes and whether extraction is wired yet."""

    return CapabilitiesResponse(
        service=SERVICE_NAME,
        version=SERVICE_VERSION,
        modes=list(SUPPORTED_MODES),
        extraction_implemented=False,
    )


@app.post("/v1/ingest")
def ingest() -> JSONResponse:
    """Placeholder ingest endpoint.

    Returns 501 until remote PDF extraction is implemented behind this boundary.
    """

    return JSONResponse(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        content={
            "code": "NOT_IMPLEMENTED",
            "message": "Remote PDF extraction is not yet implemented in the intake service.",
        },
    )
