from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any
from uuid import UUID

_HEX_CHARACTERS = frozenset("0123456789abcdef")


def _validate_checksum_sha256(value: str) -> None:
    if len(value) != 64 or any(character not in _HEX_CHARACTERS for character in value):
        raise ValueError("checksum_sha256 must be raw lowercase 64-char SHA-256 hex")


@dataclass(frozen=True, slots=True)
class CatalogRateRef:
    id: UUID
    checksum_sha256: str

    def __post_init__(self) -> None:
        _validate_checksum_sha256(self.checksum_sha256)


@dataclass(frozen=True, slots=True)
class CatalogMaterialRef:
    id: UUID
    checksum_sha256: str

    def __post_init__(self) -> None:
        _validate_checksum_sha256(self.checksum_sha256)


@dataclass(frozen=True, slots=True)
class CatalogFormulaRef:
    id: UUID
    checksum_sha256: str

    def __post_init__(self) -> None:
        _validate_checksum_sha256(self.checksum_sha256)


@dataclass(frozen=True, slots=True)
class CatalogRateMatch:
    id: UUID
    project_id: UUID | None
    rate_key: str
    item_type: str
    unit: str
    currency: str
    value: Decimal
    effective_start: date
    effective_end: date | None
    checksum_sha256: str
    superseded_by_id: UUID | None = None
    metadata: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        _validate_checksum_sha256(self.checksum_sha256)


@dataclass(frozen=True, slots=True)
class CatalogMaterialMatch:
    id: UUID
    project_id: UUID | None
    material_key: str
    unit: str
    currency: str
    value: Decimal
    effective_start: date
    effective_end: date | None
    checksum_sha256: str
    superseded_by_id: UUID | None = None
    metadata: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        _validate_checksum_sha256(self.checksum_sha256)


@dataclass(frozen=True, slots=True)
class CatalogFormulaMatch:
    id: UUID
    formula_id: str
    version: int
    project_id: UUID | None
    checksum_sha256: str
    expression: dict[str, Any]
    scope_type: str = "global"
    name: str = ""
    dsl_version: str = "1.0"
    output_key: str = ""
    output_contract: dict[str, Any] = field(default_factory=dict)
    declared_inputs: list[dict[str, Any]] = field(default_factory=list)
    rounding: dict[str, Any] | None = None
    superseded_by_id: UUID | None = None
    metadata: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        _validate_checksum_sha256(self.checksum_sha256)


@dataclass(frozen=True, slots=True)
class CatalogRateAutoSelectRequest:
    project_id: UUID | None
    rate_key: str
    item_type: str
    unit: str
    currency: str
    as_of: date


@dataclass(frozen=True, slots=True)
class CatalogMaterialAutoSelectRequest:
    project_id: UUID | None
    material_key: str
    unit: str
    currency: str
    as_of: date


@dataclass(frozen=True, slots=True)
class CatalogFormulaAutoSelectRequest:
    project_id: UUID | None
    formula_id: str
    version: int | None = None
