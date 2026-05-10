"""System capability and bounded health endpoints."""

from __future__ import annotations

import asyncio
import importlib.util
import os
import shutil
import socket
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from time import perf_counter
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException, Response, status
from sqlalchemy import text

from app.core.config import settings
from app.core.errors import ErrorCode
from app.core.exceptions import create_error_response
from app.core.logging import get_logger
from app.db.session import get_engine
from app.ingestion.contracts import (
    AdapterAvailability,
    AdapterDescriptor,
    AdapterStatus,
    AvailabilityReason,
    LicenseState,
    ProbeKind,
    ProbeObservation,
    ProbeRequirement,
    ProbeStatus,
)
from app.ingestion.registry import evaluate_availability, list_descriptors
from app.schemas.system import (
    AdapterCapabilityRead,
    AdapterHealthCheck,
    ConfidenceRange,
    DependencyHealthCheck,
    SystemCapabilitiesResponse,
    SystemCheckStatus,
    SystemHealthChecks,
    SystemHealthResponse,
    SystemHealthStatus,
)
from app.storage import get_storage

logger = get_logger(__name__)

system_router = APIRouter()

_DEPENDENCY_PROBE_TIMEOUT_SECONDS = 0.5
_GENERIC_ADAPTER_PROBE_TIMEOUT_SECONDS = 0.5
_LICENSE_APPROVAL_ENV = "DRAUPNIR_APPROVED_LICENSE_PROBES"
_INFLIGHT_ADAPTER_PROBE_TASKS: dict[str, asyncio.Task[tuple[ProbeObservation, ...]]] = {}
_INFLIGHT_ADAPTER_PROBE_TASKS_LOCK = asyncio.Lock()
_ADAPTER_PROBE_CLEANUP_TASKS: set[asyncio.Task[None]] = set()


def _approved_license_probes() -> frozenset[str] | None:
    """Return the optional set of explicitly approved license probe names."""

    raw_value = os.environ.get(_LICENSE_APPROVAL_ENV)
    if raw_value is None:
        return None

    approved = {item.strip() for item in raw_value.split(",") if item.strip()}
    return frozenset(approved)


def _probe_requirement(requirement: ProbeRequirement) -> ProbeObservation:
    """Resolve a side-effect-free observation for a registry probe requirement."""

    if requirement.kind is ProbeKind.BINARY:
        return _probe_binary_requirement(requirement)
    if requirement.kind is ProbeKind.PYTHON_PACKAGE:
        return _probe_python_package_requirement(requirement)
    if requirement.kind is ProbeKind.LICENSE:
        return _probe_license_requirement(requirement)

    return ProbeObservation(
        kind=requirement.kind,
        name=requirement.name,
        status=ProbeStatus.UNKNOWN,
        detail="Probe kind is not supported by the system endpoint.",
    )


def _probe_binary_requirement(requirement: ProbeRequirement) -> ProbeObservation:
    """Check whether a required binary is present on PATH."""

    binary_path = shutil.which(requirement.name)
    return ProbeObservation(
        kind=requirement.kind,
        name=requirement.name,
        status=ProbeStatus.AVAILABLE if binary_path is not None else ProbeStatus.MISSING,
        detail=None if binary_path is not None else requirement.detail,
    )


def _probe_python_package_requirement(requirement: ProbeRequirement) -> ProbeObservation:
    """Check whether a required Python package can be resolved without importing it."""

    try:
        package_spec = importlib.util.find_spec(requirement.name)
    except (ImportError, ModuleNotFoundError, ValueError):
        package_spec = None

    return ProbeObservation(
        kind=requirement.kind,
        name=requirement.name,
        status=ProbeStatus.AVAILABLE if package_spec is not None else ProbeStatus.MISSING,
        detail=None if package_spec is not None else requirement.detail,
    )


def _probe_license_requirement(requirement: ProbeRequirement) -> ProbeObservation:
    """Resolve runtime license approval without importing optional adapters."""

    approved = _approved_license_probes()
    if approved is None:
        return ProbeObservation(
            kind=requirement.kind,
            name=requirement.name,
            status=ProbeStatus.UNKNOWN,
            detail="Runtime license approval signal is not configured.",
        )

    if requirement.name in approved:
        return ProbeObservation(
            kind=requirement.kind,
            name=requirement.name,
            status=ProbeStatus.AVAILABLE,
            detail="Runtime license approval is configured.",
        )

    return ProbeObservation(
        kind=requirement.kind,
        name=requirement.name,
        status=ProbeStatus.MISSING,
        detail=requirement.detail,
    )


def _probe_observations_for_descriptor(
    descriptor: AdapterDescriptor,
) -> tuple[ProbeObservation, ...]:
    """Collect bounded, side-effect-free observations for a descriptor."""

    return tuple(_probe_requirement(requirement) for requirement in descriptor.probes)


def _availability_timeout_seconds(descriptor: AdapterDescriptor) -> float:
    """Return the effective probe timeout for a descriptor."""

    return min(descriptor.bounded_probe_ms / 1000, _GENERIC_ADAPTER_PROBE_TIMEOUT_SECONDS)


async def _run_descriptor_probe_task(
    descriptor: AdapterDescriptor,
) -> tuple[ProbeObservation, ...]:
    """Run the descriptor probe in the threadpool."""

    return await asyncio.to_thread(_probe_observations_for_descriptor, descriptor)


async def _discard_inflight_adapter_probe_task(
    adapter_key: str,
    task: asyncio.Task[tuple[ProbeObservation, ...]],
) -> None:
    """Remove a completed shared probe task from the in-flight cache."""

    async with _INFLIGHT_ADAPTER_PROBE_TASKS_LOCK:
        if _INFLIGHT_ADAPTER_PROBE_TASKS.get(adapter_key) is task:
            _INFLIGHT_ADAPTER_PROBE_TASKS.pop(adapter_key, None)


def _schedule_inflight_adapter_probe_task_cleanup(
    adapter_key: str,
    task: asyncio.Task[tuple[ProbeObservation, ...]],
) -> None:
    """Schedule cleanup for a completed shared probe task."""

    cleanup_task = asyncio.create_task(_discard_inflight_adapter_probe_task(adapter_key, task))
    _ADAPTER_PROBE_CLEANUP_TASKS.add(cleanup_task)
    cleanup_task.add_done_callback(_ADAPTER_PROBE_CLEANUP_TASKS.discard)


def _inflight_adapter_probe_task_cleanup_callback(
    adapter_key: str,
) -> Callable[[asyncio.Task[tuple[ProbeObservation, ...]]], None]:
    """Build a done callback that clears a shared probe task."""

    def _callback(task: asyncio.Task[tuple[ProbeObservation, ...]]) -> None:
        _schedule_inflight_adapter_probe_task_cleanup(adapter_key, task)

    return _callback


async def _get_or_create_inflight_adapter_probe_task(
    descriptor: AdapterDescriptor,
) -> asyncio.Task[tuple[ProbeObservation, ...]]:
    """Reuse a single in-flight probe task per adapter descriptor."""

    async with _INFLIGHT_ADAPTER_PROBE_TASKS_LOCK:
        existing_task = _INFLIGHT_ADAPTER_PROBE_TASKS.get(descriptor.adapter_key)
        if existing_task is not None and not existing_task.done():
            return existing_task

        probe_task = asyncio.create_task(_run_descriptor_probe_task(descriptor))
        _INFLIGHT_ADAPTER_PROBE_TASKS[descriptor.adapter_key] = probe_task
        probe_task.add_done_callback(
            _inflight_adapter_probe_task_cleanup_callback(descriptor.adapter_key)
        )
        return probe_task


async def _probe_descriptor_availability(descriptor: AdapterDescriptor) -> AdapterAvailability:
    """Evaluate a descriptor's runtime availability within a bounded timeout."""

    started_at = perf_counter()
    checked_at = datetime.now(UTC)
    probe_task = await _get_or_create_inflight_adapter_probe_task(descriptor)
    try:
        async with asyncio.timeout(_availability_timeout_seconds(descriptor)):
            observations = await asyncio.shield(probe_task)
    except TimeoutError:
        return AdapterAvailability(
            status=AdapterStatus.UNAVAILABLE,
            availability_reason=AvailabilityReason.PROBE_FAILED,
            license_state=LicenseState.UNKNOWN
            if any(requirement.kind is ProbeKind.LICENSE for requirement in descriptor.probes)
            else LicenseState.NOT_REQUIRED,
            issues=(),
            observed=(),
            last_checked_at=checked_at,
            details={
                "probe_timed_out": True,
                "required_probe_count": len(descriptor.probes),
            },
            probe_elapsed_ms=(perf_counter() - started_at) * 1000,
        )

    return evaluate_availability(
        descriptor,
        observations,
        last_checked_at=checked_at,
        probe_elapsed_ms=(perf_counter() - started_at) * 1000,
    )


def _confidence_range_from_descriptor(descriptor: AdapterDescriptor) -> ConfidenceRange | None:
    """Convert a descriptor confidence range into the API response model."""

    if descriptor.confidence_range is None:
        return None

    minimum, maximum = descriptor.confidence_range
    return ConfidenceRange(min=minimum, max=maximum)


def _availability_details(availability: AdapterAvailability) -> dict[str, object] | None:
    """Return JSON-friendly adapter availability details."""

    if availability.details is None:
        return None
    return dict(availability.details)


def _capability_from_descriptor(
    descriptor: AdapterDescriptor,
    availability: AdapterAvailability,
) -> AdapterCapabilityRead:
    """Build a capability response entry from registry metadata and availability."""

    capabilities = descriptor.capabilities
    return AdapterCapabilityRead(
        adapter_key=descriptor.adapter_key,
        input_family=descriptor.family,
        adapter_name=descriptor.display_name,
        adapter_version=descriptor.adapter_version,
        input_formats=list(descriptor.input_formats),
        output_formats=list(descriptor.output_formats),
        status=availability.status,
        availability_reason=availability.availability_reason,
        license_state=availability.license_state,
        license_name=descriptor.license_name,
        can_read=capabilities.can_read,
        can_write=capabilities.can_write,
        extracts_geometry=capabilities.extracts_geometry,
        extracts_text=capabilities.extracts_text,
        extracts_layers=capabilities.extracts_layers,
        extracts_blocks=capabilities.extracts_blocks,
        extracts_materials=capabilities.extracts_materials,
        supports_exports=capabilities.supports_exports,
        supports_quantity_hints=capabilities.supports_quantity_hints,
        supports_layout_selection=capabilities.supports_layout_selection,
        supports_xref_resolution=capabilities.supports_xref_resolution,
        experimental=descriptor.experimental,
        confidence_range=_confidence_range_from_descriptor(descriptor),
        bounded_probe_ms=descriptor.bounded_probe_ms,
        last_checked_at=availability.last_checked_at,
        details=_availability_details(availability),
    )


def _status_from_adapter_availability(availability: AdapterAvailability) -> SystemCheckStatus:
    """Map adapter availability states onto system health check states."""

    if availability.status is AdapterStatus.AVAILABLE:
        return SystemCheckStatus.OK
    if availability.status is AdapterStatus.DEGRADED:
        return SystemCheckStatus.DEGRADED
    return SystemCheckStatus.DOWN


def _error_code_from_adapter_availability(availability: AdapterAvailability) -> ErrorCode | None:
    """Map adapter availability into the public operator error taxonomy."""

    if availability.status is AdapterStatus.AVAILABLE:
        return None

    details = availability.details or {}
    if details.get("probe_timed_out") is True:
        return ErrorCode.ADAPTER_TIMEOUT
    if availability.availability_reason is AvailabilityReason.PROBE_FAILED:
        return ErrorCode.ADAPTER_FAILED
    return ErrorCode.ADAPTER_UNAVAILABLE


def _adapter_health_details(availability: AdapterAvailability) -> dict[str, object] | None:
    """Return health-safe details for a probed adapter."""

    details: dict[str, object] = {}
    if availability.availability_reason is not None:
        details["availability_reason"] = availability.availability_reason.value
    details["license_state"] = availability.license_state.value
    if availability.details is not None:
        details.update(dict(availability.details))
    return details or None


def _adapter_health_from_availability(
    descriptor: AdapterDescriptor,
    availability: AdapterAvailability,
) -> AdapterHealthCheck:
    """Build an adapter health entry from availability state."""

    return AdapterHealthCheck(
        adapter_key=descriptor.adapter_key,
        status=_status_from_adapter_availability(availability),
        error_code=_error_code_from_adapter_availability(availability),
        latency_ms=availability.probe_elapsed_ms,
        details=_adapter_health_details(availability),
    )


def _aggregate_adapter_check_status(adapters: Iterable[AdapterHealthCheck]) -> SystemCheckStatus:
    """Reduce individual adapter checks into a single adapter-group status."""

    adapter_list = list(adapters)
    if not adapter_list:
        return SystemCheckStatus.UNKNOWN

    statuses = {adapter.status for adapter in adapter_list}
    if statuses == {SystemCheckStatus.OK}:
        return SystemCheckStatus.OK
    if statuses == {SystemCheckStatus.DOWN}:
        return SystemCheckStatus.DOWN
    if SystemCheckStatus.OK in statuses:
        return SystemCheckStatus.DEGRADED
    if SystemCheckStatus.DEGRADED in statuses:
        return SystemCheckStatus.DEGRADED
    return SystemCheckStatus.DOWN


async def _probe_database_check() -> DependencyHealthCheck:
    """Run a bounded liveness query against the configured database."""

    engine = get_engine()
    if engine is None:
        return DependencyHealthCheck(
            status=SystemCheckStatus.DOWN,
            details={"configured": False},
        )

    started_at = perf_counter()
    try:
        async with asyncio.timeout(_DEPENDENCY_PROBE_TIMEOUT_SECONDS):
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
    except TimeoutError:
        return DependencyHealthCheck(
            status=SystemCheckStatus.DOWN,
            latency_ms=(perf_counter() - started_at) * 1000,
            details={"timed_out": True},
        )
    except Exception:
        return DependencyHealthCheck(
            status=SystemCheckStatus.DOWN,
            latency_ms=(perf_counter() - started_at) * 1000,
            details={"reachable": False},
        )

    return DependencyHealthCheck(
        status=SystemCheckStatus.OK,
        latency_ms=(perf_counter() - started_at) * 1000,
    )


async def _probe_storage_check() -> DependencyHealthCheck:
    """Run a bounded health check for the configured storage backend."""

    storage = get_storage()
    started_at = perf_counter()

    try:
        async with asyncio.timeout(_DEPENDENCY_PROBE_TIMEOUT_SECONDS):
            report = await storage.healthcheck()
    except TimeoutError:
        return DependencyHealthCheck(
            status=SystemCheckStatus.DOWN,
            latency_ms=(perf_counter() - started_at) * 1000,
            details={"timed_out": True},
        )
    except Exception:
        return DependencyHealthCheck(
            status=SystemCheckStatus.DOWN,
            latency_ms=(perf_counter() - started_at) * 1000,
            details={"reachable": False},
        )

    return DependencyHealthCheck(
        status=SystemCheckStatus.OK if report.ok else SystemCheckStatus.DOWN,
        latency_ms=(perf_counter() - started_at) * 1000,
        details=report.details if report.ok or report.details is not None else {"reachable": False},
    )


def _broker_endpoint() -> tuple[str, int]:
    """Return the host and port for the configured message broker."""

    parsed = urlparse(settings.broker_url)
    host = parsed.hostname
    if host is None:
        raise ValueError("Broker host is not configured.")

    if parsed.port is not None:
        return host, parsed.port
    if parsed.scheme in {"amqp", "pyamqp"}:
        return host, 5672
    if parsed.scheme == "amqps":
        return host, 5671
    raise ValueError("Broker URL scheme is not supported for health probing.")


def _probe_broker_socket(timeout_seconds: float) -> None:
    """Perform a bounded TCP handshake against the configured broker."""

    host, port = _broker_endpoint()
    with socket.create_connection((host, port), timeout=timeout_seconds):
        pass


async def _probe_broker_check() -> DependencyHealthCheck:
    """Run a bounded broker reachability check."""

    started_at = perf_counter()
    try:
        async with asyncio.timeout(_DEPENDENCY_PROBE_TIMEOUT_SECONDS):
            await asyncio.to_thread(_probe_broker_socket, _DEPENDENCY_PROBE_TIMEOUT_SECONDS)
    except TimeoutError:
        return DependencyHealthCheck(
            status=SystemCheckStatus.DOWN,
            latency_ms=(perf_counter() - started_at) * 1000,
            details={"timed_out": True},
        )
    except (OSError, ValueError):
        return DependencyHealthCheck(
            status=SystemCheckStatus.DOWN,
            latency_ms=(perf_counter() - started_at) * 1000,
            details={"reachable": False},
        )

    return DependencyHealthCheck(
        status=SystemCheckStatus.OK,
        latency_ms=(perf_counter() - started_at) * 1000,
    )


async def _build_adapter_health_checks() -> tuple[list[AdapterHealthCheck], SystemCheckStatus]:
    """Build per-adapter health checks and the aggregate adapter-group status."""

    descriptors = list_descriptors()
    availabilities = await asyncio.gather(
        *(_probe_descriptor_availability(descriptor) for descriptor in descriptors)
    )
    adapter_checks = [
        _adapter_health_from_availability(descriptor, availability)
        for descriptor, availability in zip(descriptors, availabilities, strict=True)
    ]
    return adapter_checks, _aggregate_adapter_check_status(adapter_checks)


def _aggregate_system_health_status(
    database: DependencyHealthCheck,
    storage: DependencyHealthCheck,
    broker: DependencyHealthCheck,
    adapters: SystemCheckStatus,
) -> SystemHealthStatus:
    """Reduce dependency and adapter checks into the public top-level status."""

    core_statuses = {database.status, storage.status, broker.status}
    if SystemCheckStatus.DOWN in core_statuses:
        return SystemHealthStatus.DOWN
    if adapters is SystemCheckStatus.DOWN:
        return SystemHealthStatus.DOWN
    if SystemCheckStatus.DEGRADED in core_statuses or SystemCheckStatus.UNKNOWN in core_statuses:
        return SystemHealthStatus.DEGRADED
    if adapters in {SystemCheckStatus.DEGRADED, SystemCheckStatus.UNKNOWN}:
        return SystemHealthStatus.DEGRADED
    return SystemHealthStatus.OK


@system_router.get("/system/capabilities", response_model=SystemCapabilitiesResponse)
async def get_system_capabilities() -> SystemCapabilitiesResponse:
    """Return the bounded runtime capability registry for configured adapters."""

    try:
        descriptors = list_descriptors()
        availabilities = await asyncio.gather(
            *(_probe_descriptor_availability(descriptor) for descriptor in descriptors)
        )
    except Exception:
        logger.error("system_capabilities_failed", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=create_error_response(
                code=ErrorCode.INTERNAL_ERROR,
                message="Failed to build system capability registry",
                details=None,
            ),
        ) from None

    return SystemCapabilitiesResponse(
        adapters=[
            _capability_from_descriptor(descriptor, availability)
            for descriptor, availability in zip(descriptors, availabilities, strict=True)
        ]
    )


@system_router.get("/system/health", response_model=SystemHealthResponse)
async def get_system_health(response: Response) -> SystemHealthResponse:
    """Return bounded dependency and adapter health for operators."""

    try:
        database_check, storage_check, broker_check, adapter_bundle = await asyncio.gather(
            _probe_database_check(),
            _probe_storage_check(),
            _probe_broker_check(),
            _build_adapter_health_checks(),
        )
    except Exception:
        logger.error("system_health_failed", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=create_error_response(
                code=ErrorCode.INTERNAL_ERROR,
                message="Failed to build system health response",
                details=None,
            ),
        ) from None

    adapter_checks, adapter_status = adapter_bundle
    overall_status = _aggregate_system_health_status(
        database_check,
        storage_check,
        broker_check,
        adapter_status,
    )
    if overall_status is not SystemHealthStatus.OK:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return SystemHealthResponse(
        status=overall_status,
        checks=SystemHealthChecks(
            database=database_check,
            storage=storage_check,
            broker=broker_check,
            adapters=adapter_checks,
        ),
    )
