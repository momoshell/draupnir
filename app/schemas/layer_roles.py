"""Schemas for the semantic layer-role interpretation endpoint."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class LayerRoleRead(BaseModel):
    """A layer with its derived semantic role and the rule basis."""

    layer_ref: str | None = Field(None, description="Stable layer reference")
    name: str | None = Field(None, description="Canonical layer name")
    role: str = Field(..., description="Semantic role: background, foreground, services, unknown")
    basis: str = Field(..., description="Deterministic basis for the role (which rule fired)")


class RevisionLayerRoleListResponse(BaseModel):
    """Derived semantic roles for a revision's layers."""

    model_config = ConfigDict(extra="forbid")

    items: list[LayerRoleRead] = Field(..., description="Per-layer derived roles")
    rule_version: str = Field(..., description="Version of the role rule table that was applied")
    summary: dict[str, Any] = Field(..., description="Role counts")
