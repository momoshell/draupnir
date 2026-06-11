"""Unit tests for scalar/money formula-input support (assumption-backed bindings)."""

from __future__ import annotations

import itertools
from collections.abc import Callable
from decimal import Decimal

import pytest

from app.jobs.estimate_assembly import _build_estimate_assumption_entries
from app.jobs.estimate_mapping import (
    _EstimateJobInputError,
    _EstimateWorkerLineInput,
    _resolve_formula_binding_snapshot_key,
)


def _sort_orders() -> Callable[[], int]:
    counter = itertools.count(1)
    return lambda: next(counter)


def test_build_assumption_entries_parses_scalar_and_money_sorted_by_name() -> None:
    entries = _build_estimate_assumption_entries(
        {
            "tax_rate": "0.20",
            "inputs": {
                "mobilization": {"kind": "money", "amount": "500.00"},
                "markup": {"kind": "scalar", "amount": "0.15"},
            },
        },
        next_sort_order=_sort_orders(),
    )

    # Deterministic order by assumption name.
    assert [e.entry_key for e in entries] == ["assumption:markup", "assumption:mobilization"]
    markup, mobilization = entries
    assert markup.kind == "scalar"
    assert markup.amount == Decimal("0.15")
    assert markup.source_payload == {"kind": "scalar", "amount": "0.15"}
    assert mobilization.kind == "money"
    assert mobilization.amount == Decimal("500.00")
    assert mobilization.source_payload == {
        "kind": "money",
        "amount": "500.00",
        "currency": "GBP",
    }
    # Each entry carries a deterministic checksum.
    assert len(markup.source_checksum_sha256) == 64


def test_build_assumption_entries_empty_when_no_inputs() -> None:
    entries = _build_estimate_assumption_entries({"tax_rate": "0"}, next_sort_order=_sort_orders())
    assert entries == []


@pytest.mark.parametrize(
    "inputs",
    [
        {"x": {"kind": "bogus", "amount": "1"}},
        {"x": {"kind": "money", "amount": "not-a-number"}},
        {"x": {"kind": "money", "amount": "-1.00"}},
        {"x": {"kind": "money", "amount": "1.00", "currency": "USD"}},
        {"x": "not-a-mapping"},
    ],
)
def test_build_assumption_entries_rejects_invalid(inputs: dict[str, object]) -> None:
    with pytest.raises(_EstimateJobInputError):
        _build_estimate_assumption_entries({"inputs": inputs}, next_sort_order=_sort_orders())


def _formula_line() -> _EstimateWorkerLineInput:
    return _EstimateWorkerLineInput(
        line_key="line-formula",
        line_type="formula",
        description="Formula",
        ref_type="formula",
        selection_key="markup-formula",
        catalog_entry_key="formula:markup-formula",
        ref_order=1,
        catalog_checksum_sha256="a" * 64,
        rate_catalog_entry_id=None,
        material_catalog_entry_id=None,
        formula_definition_id=None,
        quantity_entry_key=None,
        quantity_item_id=None,
        formula_inputs={},
    )


@pytest.mark.parametrize("contract_kind", ["scalar", "money"])
def test_resolve_binding_maps_scalar_money_to_assumption_entry(contract_kind: str) -> None:
    key = _resolve_formula_binding_snapshot_key(
        "markup",
        line=_formula_line(),
        contract_kind=contract_kind,
        lines_by_key={},
        quantity_entry_keys=set(),
        rate_entry_keys=set(),
        material_entry_keys=set(),
        assumption_entry_keys={"assumption:markup"},
    )
    assert key == "assumption:markup"


@pytest.mark.parametrize("contract_kind", ["scalar", "money"])
def test_resolve_binding_rejects_unknown_assumption(contract_kind: str) -> None:
    with pytest.raises(_EstimateJobInputError):
        _resolve_formula_binding_snapshot_key(
            "missing",
            line=_formula_line(),
            contract_kind=contract_kind,
            lines_by_key={},
            quantity_entry_keys=set(),
            rate_entry_keys=set(),
            material_entry_keys=set(),
            assumption_entry_keys={"assumption:markup"},
        )
