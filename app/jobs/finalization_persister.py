"""The write seam for job finalization (issue D5b).

Finalizers build immutable rowsets (issue D5a) and then commit them. This module
puts the *write* of those rowsets behind a ``FinalizationPersister`` protocol so
that finalizer orchestration can be exercised with a fake persister that captures
the rowset, instead of asserting against persisted database rows. The default
implementation performs the exact ``session.add``/``flush`` sequence the
finalizers used inline; the surrounding job-status mutation and ``commit`` stay in
the finalizer so atomicity is unchanged.
"""

from __future__ import annotations

from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from app.jobs.result_builders import EstimateRows, QuantityTakeoffRows


class FinalizationPersister(Protocol):
    """Owns the row writes for the quantity-takeoff and estimate finalizers."""

    async def persist_quantity_takeoff(
        self, session: AsyncSession, rows: QuantityTakeoffRows
    ) -> None: ...

    async def persist_estimate(self, session: AsyncSession, rows: EstimateRows) -> None: ...


class _SqlFinalizationPersister:
    """Default persister: writes finalization rowsets through the active session.

    The flush ordering matches the previous inline code: the header row is flushed
    before its dependent rows so server-side defaults and FK targets are available.
    """

    async def persist_quantity_takeoff(
        self, session: AsyncSession, rows: QuantityTakeoffRows
    ) -> None:
        session.add(rows.takeoff)
        await session.flush()
        session.add_all(rows.items)

    async def persist_estimate(self, session: AsyncSession, rows: EstimateRows) -> None:
        session.add(rows.version)
        await session.flush()
        session.add_all(rows.snapshot_entries)
        await session.flush()
        session.add_all(rows.line_items)


DEFAULT_FINALIZATION_PERSISTER: FinalizationPersister = _SqlFinalizationPersister()
