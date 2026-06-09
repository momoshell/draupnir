"""Remote PDF intake service adapter (boundary stub).

This module is the implementation target for the non-routable
``pdf_intake_service`` registry descriptor. It is intentionally NOT wired into
runner selection yet: the descriptor declares ``can_read=False``, so
``load_adapter`` refuses it and the in-process PyMuPDF and VTracer adapters
remain the active read paths.

It exists to (a) prove the HTTP service boundary end to end and (b) define how
remote transport failures map onto the adapter contract. Moving real PDF
extraction through the service is deferred to a follow-up. See ADR 0010.
"""

from __future__ import annotations

import asyncio

import httpx

from app.core.config import settings
from app.ingestion.contracts import (
    AdapterAvailability,
    AdapterExecutionOptions,
    AdapterResult,
    AdapterSource,
    AdapterUnavailableError,
    AvailabilityReason,
    IngestionAdapter,
    InputFamily,
    ProbeKind,
    ProbeObservation,
    ProbeStatus,
)
from app.ingestion.registry import evaluate_availability, get_descriptor_by_key

_DESCRIPTOR_KEY = "pdf_intake_service"
_PROBE_NAME = "pdf-intake-service"
_INGEST_PATH = "/v1/ingest"

# Wire vocabulary the remote service dispatches on. These string values are the
# boundary contract and MUST stay in sync with ``SUPPORTED_MODES`` in
# ``services/pdf-intake/app/main.py``; the core ``InputFamily`` names (``pdf_vector``
# /``pdf_raster``) are deliberately mapped to the shorter wire modes here rather than
# leaked over the boundary. See ADR 0010.
WIRE_MODE_BY_INPUT_FAMILY: dict[InputFamily, str] = {
    InputFamily.PDF_VECTOR: "vector",
    InputFamily.PDF_RASTER: "raster",
}


class PdfIntakeServiceAdapter(IngestionAdapter):
    """HTTP client adapter for the optional containerized PDF intake service."""

    def __init__(self, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        # Fetch by key, not by family: the descriptor is can_read=False and is
        # therefore absent from the family-keyed read registry.
        self.descriptor = get_descriptor_by_key(_DESCRIPTOR_KEY)
        self._transport = transport

    def probe(self) -> AdapterAvailability:
        """Resolve static availability from configuration only (no network call)."""

        return evaluate_availability(self.descriptor, observations=self._probe_observations())

    def _probe_observations(self) -> tuple[ProbeObservation, ...]:
        if not settings.pdf_intake_service_url:
            return (
                ProbeObservation(
                    kind=ProbeKind.SERVICE,
                    name=_PROBE_NAME,
                    status=ProbeStatus.MISSING,
                    detail="PDF intake service URL is not configured (PDF_INTAKE_SERVICE_URL).",
                ),
            )
        # Live reachability is verified by the bounded system probe and by
        # ingest(); the static probe only reflects configuration.
        return (
            ProbeObservation(
                kind=ProbeKind.SERVICE,
                name=_PROBE_NAME,
                status=ProbeStatus.AVAILABLE,
                detail="PDF intake service URL is configured.",
            ),
        )

    async def ingest(
        self,
        source: AdapterSource,
        options: AdapterExecutionOptions,
    ) -> AdapterResult:
        """Forward a source to the remote service and map transport outcomes.

        Failure mapping (consumed by the ingestion runner):
        - unset URL or transport/connect error -> AdapterUnavailableError (UNAVAILABLE)
        - request timeout -> TimeoutError (TIMEOUT)
        - HTTP 501 -> AdapterUnavailableError (remote extraction not yet implemented)
        - other non-2xx -> RuntimeError (FAILED)
        """

        service_url = settings.pdf_intake_service_url
        if not service_url:
            raise AdapterUnavailableError(
                AvailabilityReason.DISABLED_BY_CONFIG,
                detail="PDF intake service URL is not configured (PDF_INTAKE_SERVICE_URL).",
            )

        wire_mode = WIRE_MODE_BY_INPUT_FAMILY.get(source.input_family)
        if wire_mode is None:
            # The runner must only route PDF families here; anything else is a bug.
            raise RuntimeError(
                f"PDF intake service cannot handle input family '{source.input_family.value}'."
            )

        content = await asyncio.to_thread(source.file_path.read_bytes)
        files = {
            "file": (
                source.original_name or source.file_path.name,
                content,
                source.media_type or "application/pdf",
            )
        }
        data = {"mode": wire_mode}

        # Honor the runner-provided request budget when present.
        timeout_seconds = settings.pdf_intake_service_timeout_seconds
        if options.timeout is not None:
            timeout_seconds = options.timeout.seconds

        try:
            async with httpx.AsyncClient(
                base_url=service_url,
                transport=self._transport,
                timeout=timeout_seconds,
            ) as client:
                response = await client.post(_INGEST_PATH, files=files, data=data)
        except httpx.TimeoutException as exc:
            raise TimeoutError("PDF intake service request timed out.") from exc
        except httpx.TransportError as exc:
            raise AdapterUnavailableError(
                AvailabilityReason.PROBE_FAILED,
                detail="PDF intake service is configured but unreachable.",
            ) from exc

        if response.status_code == httpx.codes.NOT_IMPLEMENTED:
            raise AdapterUnavailableError(
                AvailabilityReason.DISABLED_BY_CONFIG,
                detail="PDF intake service does not yet implement remote extraction.",
            )
        if not response.is_success:
            # Anything outside 2xx (including unexpected 3xx redirects, which httpx
            # does not follow by default and usually signal a misconfigured URL) is a
            # failure, not a benign "not wired yet" outcome.
            raise RuntimeError(f"PDF intake service returned HTTP {response.status_code}.")

        # Real result handling is deferred; a 2xx today still means the boundary
        # is not carrying extraction yet.
        raise AdapterUnavailableError(
            AvailabilityReason.DISABLED_BY_CONFIG,
            detail="Remote PDF extraction result handling is not yet wired.",
        )


def create_adapter() -> IngestionAdapter:
    """Standard read-adapter factory used by the ingestion loader."""

    return PdfIntakeServiceAdapter()
