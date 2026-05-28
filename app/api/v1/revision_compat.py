"""Runtime compatibility wrappers for revision route collaborators.

Split revision route modules should call these wrappers instead of importing
``app.api.v1.revisions`` at module import time. Each wrapper resolves the
facade symbol at request time so tests and callers that monkeypatch the legacy
``app.api.v1.revisions`` names continue to affect route execution.
"""

from __future__ import annotations

from collections.abc import Callable
from importlib import import_module
from typing import Any, cast


def _resolve(name: str) -> Any:
    """Resolve a symbol from the revision facade at call time."""

    return getattr(import_module("app.api.v1.revisions"), name)


def _resolve_callable(name: str) -> Callable[..., Any]:
    """Resolve a callable from the revision facade at call time."""

    return cast(Callable[..., Any], _resolve(name))


async def _get_active_file(*args: Any, **kwargs: Any) -> Any:
    return await _resolve_callable("_get_active_file")(*args, **kwargs)


async def _get_active_revision(*args: Any, **kwargs: Any) -> Any:
    return await _resolve_callable("_get_active_revision")(*args, **kwargs)


async def _get_active_validation_report(*args: Any, **kwargs: Any) -> Any:
    return await _resolve_callable("_get_active_validation_report")(*args, **kwargs)


async def _get_active_validation_report_or_404(*args: Any, **kwargs: Any) -> Any:
    return await _resolve_callable("_get_active_validation_report_or_404")(*args, **kwargs)


async def _get_revision_manifest(*args: Any, **kwargs: Any) -> Any:
    return await _resolve_callable("_get_revision_manifest")(*args, **kwargs)


async def _get_active_revision_manifest_or_409(*args: Any, **kwargs: Any) -> Any:
    return await _resolve_callable("_get_active_revision_manifest_or_409")(*args, **kwargs)


async def _get_revision_quantity_takeoff_or_404(*args: Any, **kwargs: Any) -> Any:
    return await _resolve_callable("_get_revision_quantity_takeoff_or_404")(*args, **kwargs)


async def _get_revision_estimate_version_or_404(*args: Any, **kwargs: Any) -> Any:
    return await _resolve_callable("_get_revision_estimate_version_or_404")(*args, **kwargs)


def _raise_entities_not_materialized(*args: Any, **kwargs: Any) -> Any:
    return _resolve_callable("_raise_entities_not_materialized")(*args, **kwargs)


def _manifest_counts(*args: Any, **kwargs: Any) -> Any:
    return _resolve_callable("_manifest_counts")(*args, **kwargs)


def _raise_quantity_takeoff_gate_invalid(*args: Any, **kwargs: Any) -> Any:
    return _resolve_callable("_raise_quantity_takeoff_gate_invalid")(*args, **kwargs)


def _raise_estimate_input_invalid(*args: Any, **kwargs: Any) -> Any:
    return _resolve_callable("_raise_estimate_input_invalid")(*args, **kwargs)


def _raise_estimate_takeoff_gate_invalid(*args: Any, **kwargs: Any) -> Any:
    return _resolve_callable("_raise_estimate_takeoff_gate_invalid")(*args, **kwargs)


async def _resolve_estimate_catalog_refs(*args: Any, **kwargs: Any) -> Any:
    return await _resolve_callable("_resolve_estimate_catalog_refs")(*args, **kwargs)


def _resolve_estimate_job_model_classes(*args: Any, **kwargs: Any) -> Any:
    return _resolve_callable("_resolve_estimate_job_model_classes")(*args, **kwargs)


def _build_mapped_instance(*args: Any, **kwargs: Any) -> Any:
    return _resolve_callable("_build_mapped_instance")(*args, **kwargs)


def _build_estimate_job_input_payload(*args: Any, **kwargs: Any) -> Any:
    return _resolve_callable("_build_estimate_job_input_payload")(*args, **kwargs)


def enqueue_quantity_takeoff_job(*args: Any, **kwargs: Any) -> Any:
    return _resolve_callable("enqueue_quantity_takeoff_job")(*args, **kwargs)


def enqueue_estimate_job(*args: Any, **kwargs: Any) -> Any:
    return _resolve_callable("enqueue_estimate_job")(*args, **kwargs)


def enqueue_export_job(*args: Any, **kwargs: Any) -> Any:
    return _resolve_callable("enqueue_export_job")(*args, **kwargs)


async def replay_idempotency_response(*args: Any, **kwargs: Any) -> Any:
    return await _resolve_callable("replay_idempotency_response")(*args, **kwargs)


async def claim_idempotency_response(*args: Any, **kwargs: Any) -> Any:
    return await _resolve_callable("claim_idempotency_response")(*args, **kwargs)


async def complete_idempotency_response(*args: Any, **kwargs: Any) -> Any:
    return await _resolve_callable("complete_idempotency_response")(*args, **kwargs)


def prepare_job_enqueue_intent(*args: Any, **kwargs: Any) -> Any:
    return _resolve_callable("prepare_job_enqueue_intent")(*args, **kwargs)


async def publish_job_enqueue_intent(*args: Any, **kwargs: Any) -> Any:
    return await _resolve_callable("publish_job_enqueue_intent")(*args, **kwargs)


def utc_now() -> Any:
    """Return current UTC time through the monkeypatchable facade clock."""

    facade_datetime = _resolve("datetime")
    facade_utc = _resolve("UTC")
    return facade_datetime.now(facade_utc)


__all__ = [
    "_build_estimate_job_input_payload",
    "_build_mapped_instance",
    "_get_active_file",
    "_get_active_revision",
    "_get_active_revision_manifest_or_409",
    "_get_active_validation_report",
    "_get_active_validation_report_or_404",
    "_get_revision_estimate_version_or_404",
    "_get_revision_manifest",
    "_get_revision_quantity_takeoff_or_404",
    "_manifest_counts",
    "_raise_entities_not_materialized",
    "_raise_estimate_input_invalid",
    "_raise_estimate_takeoff_gate_invalid",
    "_raise_quantity_takeoff_gate_invalid",
    "_resolve_estimate_catalog_refs",
    "_resolve_estimate_job_model_classes",
    "claim_idempotency_response",
    "complete_idempotency_response",
    "enqueue_estimate_job",
    "enqueue_export_job",
    "enqueue_quantity_takeoff_job",
    "prepare_job_enqueue_intent",
    "publish_job_enqueue_intent",
    "replay_idempotency_response",
    "utc_now",
]
