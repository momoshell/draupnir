"""Injected collaborators for the job-worker coupled core (assembly + finalizers).

See issue #387. The estimate-input assembly and the per-type finalizers call
collaborators that tests historically monkeypatch on ``app.jobs.worker``. Passing
them in via this frozen ``WorkerDeps`` lets those functions move to sibling
modules without breaking the seams: the default snapshot (built once per job
flow by ``worker.default_worker_deps``) reads worker.py's — possibly
test-patched — module globals, and the extracted code only ever touches ``deps``.

Tests may inject fakes explicitly by passing ``deps=`` to the ``process_*``
entry points instead of patching module globals.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.estimating.catalog.contracts import CatalogMaterialMatch, CatalogRateMatch
    from app.estimating.catalog.selection import SelectedFormula
    from app.jobs.lifecycle import _JobLockBootstrap, _LockedJobSource
    from app.models.job import Job
    from app.models.project import Project
    from app.storage import Storage


@dataclass(frozen=True, slots=True)
class WorkerDeps:
    """Swappable app collaborators for the estimate assembly and finalizers.

    ``get_session_maker`` is deliberately *not* here: it is a process-wide infra
    seam patched globally by the test harness, not a per-job collaborator.
    """

    resolve_rate: Callable[..., Awaitable[CatalogRateMatch]]
    resolve_material: Callable[..., Awaitable[CatalogMaterialMatch]]
    resolve_formula: Callable[..., Awaitable[SelectedFormula]]
    get_storage: Callable[[], Storage]
    emit_job_event: Callable[..., Awaitable[bool]]
    get_project: Callable[..., Awaitable[Project | None]]
    get_job_lock_bootstrap: Callable[..., Awaitable[_JobLockBootstrap | None]]
    lock_job_source: Callable[..., Awaitable[_LockedJobSource]]
    finalize_job_cancelled: Callable[[Job], None]
    cancel_job_for_inactive_source: Callable[..., Awaitable[bool]]
