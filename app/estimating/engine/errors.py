from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, NoReturn

type EstimateEngineErrorCode = Literal[
    "INPUT_INVALID",
    "UNSUPPORTED_FORMULA_JSON",
]


@dataclass(frozen=True, slots=True)
class EstimateEngineError(ValueError):
    code: EstimateEngineErrorCode
    reason: str
    message: str

    def __str__(self) -> str:
        return self.message


def raise_input_invalid(reason: str, message: str) -> NoReturn:
    raise EstimateEngineError(code="INPUT_INVALID", reason=reason, message=message)


def raise_unsupported_formula_json(reason: str, message: str) -> NoReturn:
    raise EstimateEngineError(code="UNSUPPORTED_FORMULA_JSON", reason=reason, message=message)
