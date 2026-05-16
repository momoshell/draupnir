from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

FormulaNodeKind = Literal[
    "literal",
    "input",
    "add",
    "subtract",
    "multiply",
    "divide",
    "negate",
    "min",
    "max",
    "sum",
    "round",
]
RoundingMode = Literal["ROUND_HALF_UP"]
ErrorCode = Literal["INPUT_INVALID"]
ValueContractKind = Literal["scalar", "money", "quantity", "rate"]


@dataclass(frozen=True, slots=True)
class ValueContract:
    kind: ValueContractKind
    currency: str | None = None
    unit: str | None = None
    per_unit: str | None = None


@dataclass(frozen=True, slots=True)
class FormulaValue:
    amount: Decimal
    contract: ValueContract


@dataclass(frozen=True, slots=True)
class FormulaInputDefinition:
    name: str
    contract: ValueContract


@dataclass(frozen=True, slots=True)
class RoundingSpec:
    scale: int
    mode: RoundingMode


@dataclass(frozen=True, slots=True)
class FormulaNode:
    kind: FormulaNodeKind
    value: str | None = None
    name: str | None = None
    args: tuple[FormulaNode, ...] = ()
    rounding: RoundingSpec | None = None


@dataclass(frozen=True, slots=True)
class FormulaDefinition:
    formula_id: str
    name: str
    version: int
    checksum: str
    output_key: str
    output_contract: ValueContract
    declared_inputs: tuple[FormulaInputDefinition, ...]
    expression: FormulaNode
    rounding: RoundingSpec | None = None


@dataclass(frozen=True, slots=True)
class FormulaEvaluationResult:
    formula_id: str
    version: int
    checksum: str
    output_key: str
    value: FormulaValue
    rounding: RoundingSpec | None = None


@dataclass(frozen=True, slots=True)
class FormulaEvaluationError(ValueError):
    code: ErrorCode
    reason: str
    message: str

    def __str__(self) -> str:
        return self.message
