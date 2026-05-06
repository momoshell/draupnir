"""Dynamic adapter loader for ingestion runners."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import cast

from app.ingestion.contracts import AdapterDescriptor, IngestionAdapter

_FACTORY_NAME = "create_adapter"


def load_adapter(descriptor: AdapterDescriptor) -> IngestionAdapter:
    """Import an adapter module and call its standard factory."""
    module = importlib.import_module(descriptor.module)

    try:
        factory = getattr(module, _FACTORY_NAME)
    except AttributeError as exc:
        raise AttributeError(
            f"Adapter module '{descriptor.module}' does not define '{_FACTORY_NAME}'."
        ) from exc

    if not callable(factory):
        raise TypeError(
            f"Adapter module '{descriptor.module}' exposes non-callable '{_FACTORY_NAME}'."
        )

    adapter_factory = cast(Callable[[], IngestionAdapter], factory)
    return adapter_factory()
