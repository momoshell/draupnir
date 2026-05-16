from __future__ import annotations

from datetime import date
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from app.estimating.catalog.contracts import (
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
from app.estimating.catalog.selection import (
    CatalogSelectionError,
    select_formula,
    select_material,
    select_rate,
)

CHECKSUM_A = "a" * 64
CHECKSUM_B = "b" * 64


def make_rate(**overrides: object) -> CatalogRateMatch:
    id_value = overrides.get("id")
    project_id = overrides.get("project_id")
    rate_key = overrides.get("rate_key")
    item_type = overrides.get("item_type")
    unit = overrides.get("unit")
    currency = overrides.get("currency")
    value = overrides.get("value")
    effective_start = overrides.get("effective_start")
    effective_end = overrides.get("effective_end")
    checksum_sha256 = overrides.get("checksum_sha256")
    superseded_by_id = overrides.get("superseded_by_id")

    return CatalogRateMatch(
        id=id_value if isinstance(id_value, UUID) else uuid4(),
        project_id=project_id if isinstance(project_id, UUID) else None,
        rate_key=rate_key if isinstance(rate_key, str) else "labour",
        item_type=item_type if isinstance(item_type, str) else "wall",
        unit=unit if isinstance(unit, str) else "m2",
        currency=currency if isinstance(currency, str) else "GBP",
        value=value if isinstance(value, Decimal) else Decimal("12.34"),
        effective_start=(
            effective_start if isinstance(effective_start, date) else date(2025, 1, 1)
        ),
        effective_end=effective_end if isinstance(effective_end, date) else None,
        checksum_sha256=(
            checksum_sha256 if isinstance(checksum_sha256, str) else CHECKSUM_A
        ),
        superseded_by_id=(
            superseded_by_id if isinstance(superseded_by_id, UUID) else None
        ),
    )


def make_material(**overrides: object) -> CatalogMaterialMatch:
    id_value = overrides.get("id")
    project_id = overrides.get("project_id")
    material_key = overrides.get("material_key")
    unit = overrides.get("unit")
    currency = overrides.get("currency")
    value = overrides.get("value")
    effective_start = overrides.get("effective_start")
    effective_end = overrides.get("effective_end")
    checksum_sha256 = overrides.get("checksum_sha256")
    superseded_by_id = overrides.get("superseded_by_id")

    return CatalogMaterialMatch(
        id=id_value if isinstance(id_value, UUID) else uuid4(),
        project_id=project_id if isinstance(project_id, UUID) else None,
        material_key=material_key if isinstance(material_key, str) else "brick",
        unit=unit if isinstance(unit, str) else "ea",
        currency=currency if isinstance(currency, str) else "GBP",
        value=value if isinstance(value, Decimal) else Decimal("1.23"),
        effective_start=(
            effective_start if isinstance(effective_start, date) else date(2025, 1, 1)
        ),
        effective_end=effective_end if isinstance(effective_end, date) else None,
        checksum_sha256=(
            checksum_sha256 if isinstance(checksum_sha256, str) else CHECKSUM_A
        ),
        superseded_by_id=(
            superseded_by_id if isinstance(superseded_by_id, UUID) else None
        ),
    )


def make_formula(**overrides: object) -> CatalogFormulaMatch:
    id_value = overrides.get("id")
    formula_id = overrides.get("formula_id")
    version = overrides.get("version")
    project_id = overrides.get("project_id")
    checksum_sha256 = overrides.get("checksum_sha256")
    expression = overrides.get("expression")
    superseded_by_id = overrides.get("superseded_by_id")

    return CatalogFormulaMatch(
        id=id_value if isinstance(id_value, UUID) else uuid4(),
        formula_id=formula_id if isinstance(formula_id, str) else "wall-area",
        version=version if isinstance(version, int) else 1,
        project_id=project_id if isinstance(project_id, UUID) else None,
        checksum_sha256=(
            checksum_sha256 if isinstance(checksum_sha256, str) else CHECKSUM_A
        ),
        expression=(
            expression
            if isinstance(expression, dict)
            else {"op": "input", "name": "quantity"}
        ),
        superseded_by_id=(
            superseded_by_id if isinstance(superseded_by_id, UUID) else None
        ),
    )


def test_explicit_rate_ref_validates_checksum() -> None:
    rate = make_rate()

    selected = select_rate(
        [rate],
        ref=CatalogRateRef(id=rate.id, checksum_sha256=rate.checksum_sha256),
    )

    assert selected == rate


def test_explicit_rate_ref_rejects_checksum_mismatch() -> None:
    rate = make_rate()

    with pytest.raises(CatalogSelectionError, match="checksum mismatch"):
        select_rate([rate], ref=CatalogRateRef(id=rate.id, checksum_sha256=CHECKSUM_B))


def test_explicit_rate_ref_rejects_stale_row() -> None:
    rate = make_rate(superseded_by_id=uuid4())

    with pytest.raises(CatalogSelectionError, match="stale or missing"):
        select_rate([rate], ref=CatalogRateRef(id=rate.id, checksum_sha256=rate.checksum_sha256))


def test_auto_rate_prefers_non_stale_project_match() -> None:
    project_id = uuid4()
    stale_project = make_rate(project_id=project_id, superseded_by_id=uuid4())
    project_rate = make_rate(project_id=project_id, checksum_sha256=CHECKSUM_B)
    global_rate = make_rate(project_id=None, rate_key="ignore-me")

    selected = select_rate(
        [stale_project, global_rate, project_rate],
        auto=CatalogRateAutoSelectRequest(
            project_id=project_id,
            rate_key="labour",
            item_type="wall",
            unit="m2",
            currency="GBP",
            as_of=date(2025, 6, 1),
        ),
    )

    assert selected == project_rate


def test_auto_rate_falls_back_to_global_only_when_no_non_stale_project_match_remains() -> None:
    project_id = uuid4()
    stale_project = make_rate(project_id=project_id, superseded_by_id=uuid4())
    global_rate = make_rate(project_id=None)

    selected = select_rate(
        [stale_project, global_rate],
        auto=CatalogRateAutoSelectRequest(
            project_id=project_id,
            rate_key="labour",
            item_type="wall",
            unit="m2",
            currency="GBP",
            as_of=date(2025, 6, 1),
        ),
    )

    assert selected == global_rate


def test_auto_rate_rejects_multiple_project_matches() -> None:
    project_id = uuid4()
    first_id = UUID("00000000-0000-0000-0000-000000000002")
    second_id = UUID("00000000-0000-0000-0000-000000000001")

    with pytest.raises(CatalogSelectionError, match="multiple non-stale project") as exc_info:
        select_rate(
            [
                make_rate(project_id=project_id, id=first_id),
                make_rate(project_id=project_id, id=second_id, checksum_sha256=CHECKSUM_B),
            ],
            auto=CatalogRateAutoSelectRequest(
                project_id=project_id,
                rate_key="labour",
                item_type="wall",
                unit="m2",
                currency="GBP",
                as_of=date(2025, 6, 1),
            ),
        )

    assert exc_info.value.conflicting_candidate_ids == (second_id, first_id)
    assert str(exc_info.value).endswith(
        f"conflicting_candidate_ids={(second_id, first_id)}"
    )


def test_auto_rate_rejects_multiple_global_matches() -> None:
    with pytest.raises(CatalogSelectionError, match="multiple non-stale global"):
        select_rate(
            [make_rate(project_id=None), make_rate(project_id=None, checksum_sha256=CHECKSUM_B)],
            auto=CatalogRateAutoSelectRequest(
                project_id=None,
                rate_key="labour",
                item_type="wall",
                unit="m2",
                currency="GBP",
                as_of=date(2025, 6, 1),
            ),
        )


def test_auto_rate_uses_half_open_effective_dates() -> None:
    ending = make_rate(effective_end=date(2025, 6, 1))
    newer = make_rate(effective_start=date(2025, 6, 1), checksum_sha256=CHECKSUM_B)

    selected = select_rate(
        [ending, newer],
        auto=CatalogRateAutoSelectRequest(
            project_id=None,
            rate_key="labour",
            item_type="wall",
            unit="m2",
            currency="GBP",
            as_of=date(2025, 6, 1),
        ),
    )

    assert selected == newer


def test_explicit_material_ref_validates_checksum() -> None:
    material = make_material()

    selected = select_material(
        [material],
        ref=CatalogMaterialRef(id=material.id, checksum_sha256=material.checksum_sha256),
    )

    assert selected == material


def test_auto_material_prefers_project_then_global() -> None:
    project_id = uuid4()
    project_material = make_material(project_id=project_id)
    global_material = make_material(project_id=None, checksum_sha256=CHECKSUM_B)

    selected = select_material(
        [global_material, project_material],
        auto=CatalogMaterialAutoSelectRequest(
            project_id=project_id,
            material_key="brick",
            unit="ea",
            currency="GBP",
            as_of=date(2025, 6, 1),
        ),
    )

    assert selected == project_material


def test_explicit_formula_ref_validates_checksum() -> None:
    formula = make_formula()

    selected = select_formula(
        [formula],
        ref=CatalogFormulaRef(id=formula.id, checksum_sha256=formula.checksum_sha256),
    )

    assert selected.formula_id == formula.formula_id
    assert selected.version == formula.version
    assert selected.definition_id == formula.id


def test_explicit_formula_ref_rejects_stale_row() -> None:
    formula = make_formula(superseded_by_id=uuid4())

    with pytest.raises(CatalogSelectionError, match="stale or missing"):
        select_formula(
            [formula],
            ref=CatalogFormulaRef(id=formula.id, checksum_sha256=formula.checksum_sha256),
        )


def test_auto_formula_requires_explicit_version_when_multiple_versions_exist() -> None:
    first_id = UUID("00000000-0000-0000-0000-00000000000a")
    second_id = UUID("00000000-0000-0000-0000-000000000009")

    with pytest.raises(CatalogSelectionError, match="multiple non-stale global") as exc_info:
        select_formula(
            [
                make_formula(id=first_id, version=1),
                make_formula(id=second_id, version=2, checksum_sha256=CHECKSUM_B),
            ],
            auto=CatalogFormulaAutoSelectRequest(
                project_id=None,
                formula_id="wall-area",
                version=None,
            ),
        )

    assert exc_info.value.conflicting_candidate_ids == (second_id, first_id)
    assert str(exc_info.value).endswith(
        f"conflicting_candidate_ids={(second_id, first_id)}"
    )


def test_auto_formula_uses_requested_version() -> None:
    v1 = make_formula(version=1)
    v2 = make_formula(version=2, checksum_sha256=CHECKSUM_B)

    selected = select_formula(
        [v1, v2],
        auto=CatalogFormulaAutoSelectRequest(project_id=None, formula_id="wall-area", version=2),
    )

    assert selected.version == 2
    assert selected.checksum_sha256 == CHECKSUM_B


def test_rate_contract_rejects_non_raw_checksum() -> None:
    with pytest.raises(ValueError, match="raw lowercase 64-char"):
        make_rate(checksum_sha256=f"sha256:{CHECKSUM_A}")


@pytest.mark.parametrize("checksum", [CHECKSUM_A, CHECKSUM_B])
def test_checksums_are_raw_lowercase_sha256_hex(checksum: str) -> None:
    assert len(checksum) == 64
    assert checksum == checksum.lower()
    assert all(character in "0123456789abcdef" for character in checksum)
