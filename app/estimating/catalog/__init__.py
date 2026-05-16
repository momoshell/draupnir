"""Estimation catalog selection contracts and deterministic selectors."""

from .contracts import (
    CatalogFormulaAutoSelectRequest,
    CatalogFormulaMatch,
    CatalogFormulaRef,
    CatalogMaterialAutoSelectRequest,
    CatalogMaterialMatch,
    CatalogMaterialRef,
    CatalogRateAutoSelectRequest,
    CatalogRateMatch,
    CatalogRateRef,
)
from .selection import (
    CatalogSelectionError,
    SelectedFormula,
    select_formula,
    select_material,
    select_rate,
)

__all__ = [
    "CatalogFormulaAutoSelectRequest",
    "CatalogFormulaMatch",
    "CatalogFormulaRef",
    "CatalogMaterialAutoSelectRequest",
    "CatalogMaterialMatch",
    "CatalogMaterialRef",
    "CatalogRateAutoSelectRequest",
    "CatalogRateMatch",
    "CatalogRateRef",
    "CatalogSelectionError",
    "SelectedFormula",
    "select_formula",
    "select_material",
    "select_rate",
]
