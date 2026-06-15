"""Shared read-only shapes for the interpretation tier.

Pure interpretation helpers accept this Protocol (satisfied by RevisionEntity, or a test fake)
rather than the ORM model, so geometry/logic can be unit-tested without a database.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol


class EntityRow(Protocol):
    """Read-only entity shape the interpretation helpers need (satisfied by RevisionEntity)."""

    @property
    def entity_id(self) -> str: ...
    @property
    def sequence_index(self) -> int: ...
    @property
    def block_ref(self) -> str | None: ...
    @property
    def layer_ref(self) -> str | None: ...
    @property
    def geometry_json(self) -> Mapping[str, Any] | None: ...
