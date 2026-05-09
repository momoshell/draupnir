"""Tests for system capability and health endpoints."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncGenerator
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from httpx import ASGITransport

from app.api.v1 import system as system_api
from app.core.errors import ErrorCode
from app.ingestion.contracts import (
    AdapterDescriptor,
    ProbeKind,
    ProbeObservation,
    ProbeRequirement,
    ProbeStatus,
)
from app.ingestion.registry import evaluate_availability, list_descriptors
from app.main import app as fastapi_app
from app.schemas.system import (
    AdapterHealthCheck,
    DependencyHealthCheck,
    SystemCheckStatus,
)
from app.storage import LocalFilesystemStorage
from app.storage.base import StorageHealthReport


@pytest.fixture
def app() -> FastAPI:
    """Provide the FastAPI application instance for testing."""

    return fastapi_app


@pytest.fixture
async def async_client(app: FastAPI) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an async HTTP client for system endpoint tests."""

    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


def _fake_probe_observation(requirement: ProbeRequirement) -> ProbeObservation:
    """Return deterministic probe observations for adapter endpoint tests."""

    if requirement.kind is ProbeKind.LICENSE:
        return ProbeObservation(
            kind=requirement.kind,
            name=requirement.name,
            status=ProbeStatus.AVAILABLE,
        )
    if requirement.kind is ProbeKind.PYTHON_PACKAGE:
        return ProbeObservation(
            kind=requirement.kind,
            name=requirement.name,
            status=ProbeStatus.AVAILABLE,
        )
    if requirement.kind is ProbeKind.BINARY and requirement.name == "tesseract":
        return ProbeObservation(
            kind=requirement.kind,
            name=requirement.name,
            status=ProbeStatus.MISSING,
            detail=requirement.detail,
        )
    return ProbeObservation(
        kind=requirement.kind,
        name=requirement.name,
        status=ProbeStatus.AVAILABLE,
    )


@pytest.mark.asyncio
async def test_system_capabilities_reflect_registry_availability_consistently(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Capabilities should expose registry metadata plus evaluated availability."""

    monkeypatch.setattr(system_api, "_probe_requirement", _fake_probe_observation)

    response = await async_client.get("/v1/system/capabilities")

    assert response.status_code == 200
    payload = response.json()
    assert set(payload) == {"adapters"}

    adapters_by_key = {adapter["adapter_key"]: adapter for adapter in payload["adapters"]}
    expected_keys = {descriptor.adapter_key for descriptor in list_descriptors()}
    assert set(adapters_by_key) == expected_keys

    for descriptor in list_descriptors():
        expected_availability = evaluate_availability(
            descriptor,
            tuple(_fake_probe_observation(requirement) for requirement in descriptor.probes),
        )
        adapter_payload = adapters_by_key[descriptor.adapter_key]

        assert set(adapter_payload) == {
            "adapter_key",
            "input_family",
            "adapter_name",
            "adapter_version",
            "input_formats",
            "output_formats",
            "status",
            "availability_reason",
            "license_state",
            "license_name",
            "can_read",
            "can_write",
            "extracts_geometry",
            "extracts_text",
            "extracts_layers",
            "extracts_blocks",
            "extracts_materials",
            "supports_exports",
            "supports_quantity_hints",
            "supports_layout_selection",
            "supports_xref_resolution",
            "experimental",
            "confidence_range",
            "bounded_probe_ms",
            "last_checked_at",
            "details",
        }
        assert adapter_payload["adapter_name"] == descriptor.display_name
        assert adapter_payload["status"] == expected_availability.status.value
        expected_reason = (
            expected_availability.availability_reason.value
            if expected_availability.availability_reason is not None
            else None
        )
        assert adapter_payload["availability_reason"] == expected_reason
        assert adapter_payload["license_state"] == expected_availability.license_state.value
        assert adapter_payload["can_read"] is descriptor.capabilities.can_read
        assert adapter_payload["can_write"] is descriptor.capabilities.can_write
        assert adapter_payload["extracts_geometry"] is descriptor.capabilities.extracts_geometry
        assert (
            adapter_payload["supports_quantity_hints"]
            is descriptor.capabilities.supports_quantity_hints
        )
        assert adapter_payload["bounded_probe_ms"] == descriptor.bounded_probe_ms
        assert adapter_payload["last_checked_at"] is not None

    assert adapters_by_key["vtracer_tesseract"]["status"] == "degraded"
    assert adapters_by_key["vtracer_tesseract"]["availability_reason"] == "missing_binary"


@pytest.mark.asyncio
async def test_system_health_returns_ok_and_200_when_all_checks_are_healthy(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Healthy dependencies and adapters should yield a 200 ok response."""

    async def _ok_dependency() -> DependencyHealthCheck:
        return DependencyHealthCheck(status=SystemCheckStatus.OK, latency_ms=1.0)

    async def _ok_adapters() -> tuple[list[AdapterHealthCheck], SystemCheckStatus]:
        return (
            [
                AdapterHealthCheck(
                    adapter_key="ezdxf",
                    status=SystemCheckStatus.OK,
                    latency_ms=2.0,
                )
            ],
            SystemCheckStatus.OK,
        )

    monkeypatch.setattr(system_api, "_probe_database_check", _ok_dependency)
    monkeypatch.setattr(system_api, "_probe_storage_check", _ok_dependency)
    monkeypatch.setattr(system_api, "_probe_broker_check", _ok_dependency)
    monkeypatch.setattr(system_api, "_build_adapter_health_checks", _ok_adapters)

    response = await async_client.get("/v1/system/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "checks": {
            "database": {"status": "ok", "latency_ms": 1.0, "details": None},
            "storage": {"status": "ok", "latency_ms": 1.0, "details": None},
            "broker": {"status": "ok", "latency_ms": 1.0, "details": None},
            "adapters": [
                {
                    "adapter_key": "ezdxf",
                    "status": "ok",
                    "error_code": None,
                    "latency_ms": 2.0,
                    "details": None,
                }
            ],
        },
    }


@pytest.mark.asyncio
async def test_system_health_returns_503_and_degraded_when_some_adapters_are_unavailable(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unavailable optional adapters should degrade overall system health."""

    async def _ok_dependency() -> DependencyHealthCheck:
        return DependencyHealthCheck(status=SystemCheckStatus.OK, latency_ms=1.0)

    monkeypatch.setattr(system_api, "_probe_database_check", _ok_dependency)
    monkeypatch.setattr(system_api, "_probe_storage_check", _ok_dependency)
    monkeypatch.setattr(system_api, "_probe_broker_check", _ok_dependency)
    monkeypatch.setattr(system_api, "_probe_requirement", _fake_probe_observation)

    response = await async_client.get("/v1/system/health")

    assert response.status_code == 503
    payload = response.json()
    assert payload["status"] == "degraded"

    adapter_payloads = {item["adapter_key"]: item for item in payload["checks"]["adapters"]}
    assert adapter_payloads["vtracer_tesseract"]["status"] == "degraded"
    assert adapter_payloads["vtracer_tesseract"]["error_code"] == "ADAPTER_UNAVAILABLE"
    assert (
        adapter_payloads["vtracer_tesseract"]["details"]["availability_reason"]
        == "missing_binary"
    )
    assert adapter_payloads["libredwg"]["status"] == "ok"


@pytest.mark.asyncio
async def test_system_health_returns_503_and_down_when_core_dependency_is_down(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A down core dependency should force top-level system status down."""

    async def _ok_dependency() -> DependencyHealthCheck:
        return DependencyHealthCheck(status=SystemCheckStatus.OK, latency_ms=1.0)

    async def _down_database() -> DependencyHealthCheck:
        return DependencyHealthCheck(
            status=SystemCheckStatus.DOWN,
            latency_ms=3.0,
            details={"reachable": False},
        )

    async def _ok_adapters() -> tuple[list[AdapterHealthCheck], SystemCheckStatus]:
        return (
            [
                AdapterHealthCheck(
                    adapter_key="ezdxf",
                    status=SystemCheckStatus.OK,
                    latency_ms=2.0,
                )
            ],
            SystemCheckStatus.OK,
        )

    monkeypatch.setattr(system_api, "_probe_database_check", _down_database)
    monkeypatch.setattr(system_api, "_probe_storage_check", _ok_dependency)
    monkeypatch.setattr(system_api, "_probe_broker_check", _ok_dependency)
    monkeypatch.setattr(system_api, "_build_adapter_health_checks", _ok_adapters)

    response = await async_client.get("/v1/system/health")

    assert response.status_code == 503
    assert response.json()["status"] == "down"


@pytest.mark.asyncio
async def test_v1_health_remains_shallow_when_system_probes_fail(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Shallow health should stay independent from system probe failures."""

    async def _failing_dependency() -> DependencyHealthCheck:
        raise RuntimeError("probe failed")

    monkeypatch.setattr(system_api, "_probe_database_check", _failing_dependency)
    monkeypatch.setattr(system_api, "_probe_storage_check", _failing_dependency)
    monkeypatch.setattr(system_api, "_probe_broker_check", _failing_dependency)

    response = await async_client.get("/v1/health")

    assert response.status_code == 200
    payload = response.json()
    assert set(payload) == {"status", "version"}
    assert payload["status"] == "ok"


@pytest.mark.asyncio
async def test_system_capabilities_hides_raw_probe_observation_shape(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Capabilities should expose contract fields, not raw probe observation payloads."""

    monkeypatch.setattr(system_api, "_probe_requirement", _fake_probe_observation)

    response = await async_client.get("/v1/system/capabilities")

    assert response.status_code == 200
    payload = response.json()

    for adapter in payload["adapters"]:
        assert "probes" not in adapter
        assert "requirements" not in adapter
        assert "observations" not in adapter

        details = adapter["details"]
        if details is not None:
            assert "probe_observations" not in details
            assert "raw_observations" not in details


@pytest.mark.asyncio
async def test_system_health_mixed_adapter_statuses_report_degraded_not_down_when_core_ok(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mixed adapter failures should degrade health without forcing overall down."""

    async def _ok_dependency() -> DependencyHealthCheck:
        return DependencyHealthCheck(status=SystemCheckStatus.OK, latency_ms=1.0)

    async def _mixed_adapters() -> tuple[list[AdapterHealthCheck], SystemCheckStatus]:
        return (
            [
                AdapterHealthCheck(
                    adapter_key="ezdxf",
                    status=SystemCheckStatus.OK,
                    latency_ms=2.0,
                ),
                AdapterHealthCheck(
                    adapter_key="vtracer_tesseract",
                    status=SystemCheckStatus.DEGRADED,
                    error_code=ErrorCode.ADAPTER_UNAVAILABLE,
                    latency_ms=2.0,
                    details={"availability_reason": "missing_binary"},
                ),
                AdapterHealthCheck(
                    adapter_key="ifcopenshell",
                    status=SystemCheckStatus.DOWN,
                    error_code=ErrorCode.ADAPTER_FAILED,
                    latency_ms=2.0,
                    details={"availability_reason": "probe_failed"},
                ),
            ],
            SystemCheckStatus.DEGRADED,
        )

    monkeypatch.setattr(system_api, "_probe_database_check", _ok_dependency)
    monkeypatch.setattr(system_api, "_probe_storage_check", _ok_dependency)
    monkeypatch.setattr(system_api, "_probe_broker_check", _ok_dependency)
    monkeypatch.setattr(system_api, "_build_adapter_health_checks", _mixed_adapters)

    response = await async_client.get("/v1/system/health")

    assert response.status_code == 503
    payload = response.json()
    assert payload["status"] == "degraded"

    adapters = {adapter["adapter_key"]: adapter for adapter in payload["checks"]["adapters"]}
    assert adapters["ezdxf"]["status"] == "ok"
    assert adapters["vtracer_tesseract"]["status"] == "degraded"
    assert adapters["ifcopenshell"]["status"] == "down"


@pytest.mark.asyncio
async def test_probe_descriptor_availability_reuses_inflight_thread_probe_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent timeouts should share one background descriptor probe."""

    descriptor = list_descriptors()[0]
    probe_started = threading.Event()
    release_probe = threading.Event()
    call_count = 0

    def _blocking_probe(_: AdapterDescriptor) -> tuple[ProbeObservation, ...]:
        nonlocal call_count
        call_count += 1
        probe_started.set()
        release_probe.wait(timeout=1.0)
        return ()

    monkeypatch.setattr(system_api, "_probe_observations_for_descriptor", _blocking_probe)
    monkeypatch.setattr(system_api, "_availability_timeout_seconds", lambda _: 0.01)
    system_api._INFLIGHT_ADAPTER_PROBE_TASKS.clear()

    try:
        first_probe = asyncio.create_task(system_api._probe_descriptor_availability(descriptor))
        await asyncio.to_thread(probe_started.wait, 1.0)
        second_probe = asyncio.create_task(system_api._probe_descriptor_availability(descriptor))

        first_result, second_result = await asyncio.gather(first_probe, second_probe)

        expected_details = {
            "probe_timed_out": True,
            "required_probe_count": len(descriptor.probes),
        }
        assert first_result.details == expected_details
        assert second_result.details == expected_details
        assert call_count == 1
    finally:
        release_probe.set()

    for _ in range(100):
        if descriptor.adapter_key not in system_api._INFLIGHT_ADAPTER_PROBE_TASKS:
            break
        await asyncio.sleep(0.01)

    assert descriptor.adapter_key not in system_api._INFLIGHT_ADAPTER_PROBE_TASKS


@pytest.mark.asyncio
async def test_probe_storage_check_uses_storage_health_abstraction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Storage health should come from the storage protocol, not backend internals."""

    class SlotBackedStorage:
        __slots__ = ()

        async def healthcheck(self) -> StorageHealthReport:
            return StorageHealthReport(
                ok=True,
                details={"backend": "slot_storage", "reachable": True},
            )

    monkeypatch.setattr(system_api, "get_storage", lambda: SlotBackedStorage())

    result = await system_api._probe_storage_check()

    assert result.status is SystemCheckStatus.OK
    assert result.details == {"backend": "slot_storage", "reachable": True}


@pytest.mark.asyncio
async def test_probe_storage_check_surfaces_local_storage_details(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Local storage health should surface operator-useful backend details."""

    storage = LocalFilesystemStorage(tmp_path / "storage-root")
    monkeypatch.setattr(system_api, "get_storage", lambda: storage)

    result = await system_api._probe_storage_check()

    assert result.status is SystemCheckStatus.OK
    assert result.details == {
        "backend": "local_filesystem",
        "reachable": True,
        "root_configured": True,
        "root_exists": False,
    }
    assert "root" not in result.details
    assert "nearest_existing_ancestor" not in result.details
