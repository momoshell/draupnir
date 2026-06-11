"""Shared deterministic bulk-write helpers for worker job persistence.

Extracted from ``app.jobs.worker`` (issue #387) so both the worker and the
extracted per-type finalizers can persist prepared row mappings without a
circular import.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

_NORMALIZED_ENTITY_INSERT_CHUNK_SIZE = 500


async def _bulk_insert_model_rows(
    session: AsyncSession,
    model: Any,
    rows: list[dict[str, Any]],
) -> None:
    """Insert prepared mappings in deterministic chunks."""
    if not rows:
        return

    for start in range(0, len(rows), _NORMALIZED_ENTITY_INSERT_CHUNK_SIZE):
        chunk = rows[start : start + _NORMALIZED_ENTITY_INSERT_CHUNK_SIZE]
        await session.execute(insert(model), chunk)
