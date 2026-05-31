"""Dynamic adapter loader for ingestion runners."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import cast

from app.ingestion.contracts import AdapterDescriptor, ExportAdapter, IngestionAdapter

_READ_FACTORY_NAME = "create_adapter"
_EXPORT_FACTORY_NAME = "create_export_adapter"


def _load_factory(descriptor: AdapterDescriptor, factory_name: str) -> Callable[[], object]:
    module = importlib.import_module(descriptor.module)

    try:
        factory = getattr(module, factory_name)
    except AttributeError as exc:
        raise AttributeError(
            f"Adapter module '{descriptor.module}' does not define '{factory_name}'."
        ) from exc

    if not callable(factory):
        raise TypeError(
            f"Adapter module '{descriptor.module}' exposes non-callable '{factory_name}'."
        )

    return cast(Callable[[], object], factory)


def load_adapter(descriptor: AdapterDescriptor) -> IngestionAdapter:
    """Import an adapter module and call its standard factory."""
    if not descriptor.capabilities.can_read:
        raise ValueError(f"Adapter '{descriptor.key}' does not support read ingestion.")

    adapter_factory = cast(
        Callable[[], IngestionAdapter],
        _load_factory(descriptor, _READ_FACTORY_NAME),
    )
    return adapter_factory()


def load_export_adapter(descriptor: AdapterDescriptor) -> ExportAdapter:
    """Import an adapter module and call its export-capable factory."""

    if not descriptor.capabilities.can_write or not descriptor.capabilities.supports_exports:
        raise ValueError(f"Adapter '{descriptor.key}' does not support exports.")

    export_factory = cast(
        Callable[[], ExportAdapter],
        _load_factory(descriptor, _EXPORT_FACTORY_NAME),
    )
    return export_factory()
