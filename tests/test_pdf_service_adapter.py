"""Tests for the remote PDF intake service adapter stub."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from app.core.config import settings
from app.ingestion.adapters.pdf_service import (
    WIRE_MODE_BY_INPUT_FAMILY,
    PdfIntakeServiceAdapter,
    create_adapter,
)
from app.ingestion.contracts import (
    AdapterExecutionOptions,
    AdapterSource,
    AdapterStatus,
    AdapterUnavailableError,
    AvailabilityReason,
    InputFamily,
    UploadFormat,
)

# The intake service (services/pdf-intake/app/main.py) dispatches on these wire modes.
# Mirrored here because the service is an isolated package the core cannot import; this
# constant is the pin that keeps the two sides of the boundary in sync.
_SERVICE_SUPPORTED_MODES = ("vector", "raster")


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


@pytest.mark.asyncio
async def test_ingest_maps_redirect_to_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
    pdf_source: AdapterSource,
) -> None:
    """An unexpected 3xx (e.g. a misconfigured URL) is a failure, not 'not wired'."""
    monkeypatch.setattr(settings, "pdf_intake_service_url", "http://pdf-intake:8100")

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": "http://elsewhere/"})

    with pytest.raises(RuntimeError):
        await _adapter_with_handler(_handler).ingest(pdf_source, AdapterExecutionOptions())


def test_wire_mode_mapping_matches_service_supported_modes() -> None:
    """Client wire modes must be exactly what the intake service dispatches on."""
    # Every PDF family is mapped, and only PDF families are.
    assert set(WIRE_MODE_BY_INPUT_FAMILY) == {
        InputFamily.PDF_VECTOR,
        InputFamily.PDF_RASTER,
    }
    # The emitted wire values are a subset of (in fact equal to) the service contract.
    assert set(WIRE_MODE_BY_INPUT_FAMILY.values()) == set(_SERVICE_SUPPORTED_MODES)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("input_family", "expected_mode"),
    [(InputFamily.PDF_VECTOR, b"vector"), (InputFamily.PDF_RASTER, b"raster")],
)
async def test_ingest_posts_service_wire_mode(
    monkeypatch: pytest.MonkeyPatch,
    pdf_source: AdapterSource,
    input_family: InputFamily,
    expected_mode: bytes,
) -> None:
    """The adapter posts the short service wire mode, not the InputFamily value."""
    monkeypatch.setattr(settings, "pdf_intake_service_url", "http://pdf-intake:8100")
    source = AdapterSource(
        file_path=pdf_source.file_path,
        upload_format=pdf_source.upload_format,
        input_family=input_family,
        media_type=pdf_source.media_type,
        original_name=pdf_source.original_name,
    )
    captured: dict[str, bytes] = {}

    def _handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = request.content
        return httpx.Response(501, json={"code": "NOT_IMPLEMENTED"})

    with pytest.raises(AdapterUnavailableError):
        await _adapter_with_handler(_handler).ingest(source, AdapterExecutionOptions())

    body = captured["body"]
    assert b'name="mode"' in body
    assert expected_mode in body
    # The raw InputFamily value must not leak over the wire.
    assert source.input_family.value.encode() not in body
