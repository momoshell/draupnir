"""Tests for the remote PDF intake service adapter stub."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from app.core.config import settings
from app.ingestion.adapters.pdf_service import PdfIntakeServiceAdapter, create_adapter
from app.ingestion.contracts import (
    AdapterExecutionOptions,
    AdapterSource,
    AdapterStatus,
    AdapterUnavailableError,
    AvailabilityReason,
    InputFamily,
    UploadFormat,
)


@pytest.fixture
def pdf_source(tmp_path: Path) -> AdapterSource:
    """Provide a minimal PDF source pointing at a real on-disk file."""

    file_path = tmp_path / "drawing.pdf"
    file_path.write_bytes(b"%PDF-1.7\n%stub\n")
    return AdapterSource(
        file_path=file_path,
        upload_format=UploadFormat.PDF,
        input_family=InputFamily.PDF_VECTOR,
        media_type="application/pdf",
        original_name="drawing.pdf",
    )


def _adapter_with_handler(
    handler: object,
) -> PdfIntakeServiceAdapter:
    transport = httpx.MockTransport(handler)  # type: ignore[arg-type]
    return PdfIntakeServiceAdapter(transport=transport)


def test_probe_reports_disabled_when_url_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "pdf_intake_service_url", None)

    availability = create_adapter().probe()

    assert availability.status is AdapterStatus.DEGRADED
    assert availability.availability_reason is AvailabilityReason.DISABLED_BY_CONFIG


def test_probe_reports_available_when_url_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "pdf_intake_service_url", "http://pdf-intake:8100")

    availability = create_adapter().probe()

    assert availability.status is AdapterStatus.AVAILABLE
    assert availability.availability_reason is None


@pytest.mark.asyncio
async def test_ingest_unavailable_when_url_unset(
    monkeypatch: pytest.MonkeyPatch,
    pdf_source: AdapterSource,
) -> None:
    monkeypatch.setattr(settings, "pdf_intake_service_url", None)

    with pytest.raises(AdapterUnavailableError) as excinfo:
        await create_adapter().ingest(pdf_source, AdapterExecutionOptions())

    assert excinfo.value.availability_reason is AvailabilityReason.DISABLED_BY_CONFIG


@pytest.mark.asyncio
async def test_ingest_maps_timeout_to_timeout_error(
    monkeypatch: pytest.MonkeyPatch,
    pdf_source: AdapterSource,
) -> None:
    monkeypatch.setattr(settings, "pdf_intake_service_url", "http://pdf-intake:8100")

    def _handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=request)

    with pytest.raises(TimeoutError):
        await _adapter_with_handler(_handler).ingest(pdf_source, AdapterExecutionOptions())


@pytest.mark.asyncio
async def test_ingest_maps_connect_error_to_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    pdf_source: AdapterSource,
) -> None:
    monkeypatch.setattr(settings, "pdf_intake_service_url", "http://pdf-intake:8100")

    def _handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(AdapterUnavailableError) as excinfo:
        await _adapter_with_handler(_handler).ingest(pdf_source, AdapterExecutionOptions())

    assert excinfo.value.availability_reason is AvailabilityReason.PROBE_FAILED


@pytest.mark.asyncio
async def test_ingest_maps_501_to_unavailable_not_implemented(
    monkeypatch: pytest.MonkeyPatch,
    pdf_source: AdapterSource,
) -> None:
    monkeypatch.setattr(settings, "pdf_intake_service_url", "http://pdf-intake:8100")

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(501, json={"code": "NOT_IMPLEMENTED"})

    with pytest.raises(AdapterUnavailableError) as excinfo:
        await _adapter_with_handler(_handler).ingest(pdf_source, AdapterExecutionOptions())

    assert excinfo.value.availability_reason is AvailabilityReason.DISABLED_BY_CONFIG


@pytest.mark.asyncio
async def test_ingest_maps_server_error_to_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
    pdf_source: AdapterSource,
) -> None:
    monkeypatch.setattr(settings, "pdf_intake_service_url", "http://pdf-intake:8100")

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    with pytest.raises(RuntimeError):
        await _adapter_with_handler(_handler).ingest(pdf_source, AdapterExecutionOptions())
