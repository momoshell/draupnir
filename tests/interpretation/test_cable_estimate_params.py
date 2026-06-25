"""Tests for app.interpretation.cable_estimate_params (issue #695).

Covers: default factory values, drop_for, with_overrides (immutability +
overrides), as_stamp (JSON round-trip + determinism), and validation errors.

No DB, ORM, or FastAPI dependencies — pure stdlib.
"""

from __future__ import annotations

import json

import pytest

from app.interpretation.cable_estimate_params import (
    CATEGORY_DISTRIBUTION_BOARD,
    CATEGORY_LUMINAIRE,
    CATEGORY_SOCKET,
    CATEGORY_SWITCH,
    SOURCE_ASSUMPTION,
    SOURCE_SPEC_NOTE,
    SOURCE_USER_OVERRIDE,
    CableEstimateParams,
    DropParam,
    default_estimate_params,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _default() -> CableEstimateParams:
    return default_estimate_params()


# ---------------------------------------------------------------------------
# Default factory
# ---------------------------------------------------------------------------


class TestDefaultEstimateParams:
    def test_has_all_four_categories(self) -> None:
        params = _default()
        categories = {d.category for d in params.vertical_drops}
        assert categories == {
            CATEGORY_LUMINAIRE,
            CATEGORY_SWITCH,
            CATEGORY_SOCKET,
            CATEGORY_DISTRIBUTION_BOARD,
        }

    def test_luminaire_drop_and_source(self) -> None:
        params = _default()
        drop = params.drop_for(CATEGORY_LUMINAIRE)
        assert drop is not None
        assert drop.drop_m == 2.0
        assert drop.source == SOURCE_SPEC_NOTE

    def test_switch_drop_and_source(self) -> None:
        params = _default()
        drop = params.drop_for(CATEGORY_SWITCH)
        assert drop is not None
        assert drop.drop_m == 1.2
        assert drop.source == SOURCE_ASSUMPTION

    def test_socket_drop_and_source(self) -> None:
        params = _default()
        drop = params.drop_for(CATEGORY_SOCKET)
        assert drop is not None
        assert drop.drop_m == 1.2
        assert drop.source == SOURCE_ASSUMPTION

    def test_distribution_board_drop_and_source(self) -> None:
        params = _default()
        drop = params.drop_for(CATEGORY_DISTRIBUTION_BOARD)
        assert drop is not None
        assert drop.drop_m == 0.0
        assert drop.source == SOURCE_ASSUMPTION

    def test_spare_fraction(self) -> None:
        params = _default()
        assert params.spare_fraction == pytest.approx(0.10)

    def test_spare_source(self) -> None:
        params = _default()
        assert params.spare_source == SOURCE_ASSUMPTION

    def test_vertical_drops_sorted_by_category(self) -> None:
        params = _default()
        cats = [d.category for d in params.vertical_drops]
        assert cats == sorted(cats)

    def test_basis_strings_non_empty(self) -> None:
        params = _default()
        for drop in params.vertical_drops:
            assert drop.basis, f"empty basis for {drop.category}"
        assert params.spare_basis


# ---------------------------------------------------------------------------
# drop_for
# ---------------------------------------------------------------------------


class TestDropFor:
    def test_known_category_returns_drop(self) -> None:
        params = _default()
        result = params.drop_for(CATEGORY_LUMINAIRE)
        assert result is not None
        assert result.category == CATEGORY_LUMINAIRE

    def test_unknown_category_returns_none(self) -> None:
        params = _default()
        assert params.drop_for("nonexistent_category") is None

    def test_empty_string_returns_none(self) -> None:
        params = _default()
        assert params.drop_for("") is None


# ---------------------------------------------------------------------------
# with_overrides
# ---------------------------------------------------------------------------


class TestWithOverrides:
    def test_override_drop_returns_new_instance(self) -> None:
        params = _default()
        new_params = params.with_overrides(drops={CATEGORY_LUMINAIRE: 3.0})
        assert new_params is not params

    def test_original_unchanged_after_drop_override(self) -> None:
        params = _default()
        _ = params.with_overrides(drops={CATEGORY_LUMINAIRE: 3.0})
        original_drop = params.drop_for(CATEGORY_LUMINAIRE)
        assert original_drop is not None
        assert original_drop.drop_m == 2.0

    def test_overridden_drop_has_new_value(self) -> None:
        params = _default()
        new_params = params.with_overrides(drops={CATEGORY_LUMINAIRE: 3.0})
        drop = new_params.drop_for(CATEGORY_LUMINAIRE)
        assert drop is not None
        assert drop.drop_m == pytest.approx(3.0)

    def test_overridden_drop_source_is_user_override(self) -> None:
        params = _default()
        new_params = params.with_overrides(drops={CATEGORY_SWITCH: 0.9})
        drop = new_params.drop_for(CATEGORY_SWITCH)
        assert drop is not None
        assert drop.source == SOURCE_USER_OVERRIDE

    def test_override_spare_fraction(self) -> None:
        params = _default()
        new_params = params.with_overrides(spare_fraction=0.15)
        assert new_params.spare_fraction == pytest.approx(0.15)
        assert new_params.spare_source == SOURCE_USER_OVERRIDE
        assert new_params.spare_basis == "user override"

    def test_original_spare_unchanged(self) -> None:
        params = _default()
        _ = params.with_overrides(spare_fraction=0.20)
        assert params.spare_fraction == pytest.approx(0.10)

    def test_add_brand_new_category_via_override(self) -> None:
        params = _default()
        new_params = params.with_overrides(drops={"smoke_detector": 0.5})
        drop = new_params.drop_for("smoke_detector")
        assert drop is not None
        assert drop.drop_m == pytest.approx(0.5)
        assert drop.source == SOURCE_USER_OVERRIDE

    def test_new_params_vertical_drops_still_sorted(self) -> None:
        params = _default()
        new_params = params.with_overrides(drops={"aardvark_device": 1.0})
        cats = [d.category for d in new_params.vertical_drops]
        assert cats == sorted(cats)

    def test_no_overrides_returns_equal_instance(self) -> None:
        params = _default()
        new_params = params.with_overrides()
        # Should be the same object (short-circuit) or at least equal.
        assert new_params == params


# ---------------------------------------------------------------------------
# as_stamp
# ---------------------------------------------------------------------------


class TestAsStamp:
    def test_json_round_trip(self) -> None:
        params = _default()
        stamp = params.as_stamp()
        # Must not raise — all values must be JSON-serialisable.
        serialised = json.dumps(stamp)
        recovered = json.loads(serialised)
        assert recovered["spare"]["fraction"] == pytest.approx(0.10)

    def test_deterministic_across_two_calls(self) -> None:
        params = _default()
        assert params.as_stamp() == params.as_stamp()

    def test_contains_all_categories(self) -> None:
        params = _default()
        stamp = params.as_stamp()
        stamped_cats = {d["category"] for d in stamp["vertical_drops"]}
        assert stamped_cats == {
            CATEGORY_LUMINAIRE,
            CATEGORY_SWITCH,
            CATEGORY_SOCKET,
            CATEGORY_DISTRIBUTION_BOARD,
        }

    def test_drops_sorted_in_stamp(self) -> None:
        params = _default()
        stamp = params.as_stamp()
        cats = [d["category"] for d in stamp["vertical_drops"]]
        assert cats == sorted(cats)

    def test_each_drop_has_required_fields(self) -> None:
        params = _default()
        for drop_dict in params.as_stamp()["vertical_drops"]:
            assert "category" in drop_dict
            assert "drop_m" in drop_dict
            assert "source" in drop_dict
            assert "basis" in drop_dict

    def test_spare_fields_present(self) -> None:
        params = _default()
        spare = params.as_stamp()["spare"]
        assert "fraction" in spare
        assert "source" in spare
        assert "basis" in spare

    def test_stamp_reflects_overrides(self) -> None:
        params = _default()
        new_params = params.with_overrides(drops={CATEGORY_LUMINAIRE: 3.5})
        stamp = new_params.as_stamp()
        luminaire = next(d for d in stamp["vertical_drops"] if d["category"] == CATEGORY_LUMINAIRE)
        assert luminaire["drop_m"] == pytest.approx(3.5)
        assert luminaire["source"] == SOURCE_USER_OVERRIDE


# ---------------------------------------------------------------------------
# Validation — negative values
# ---------------------------------------------------------------------------


class TestValidation:
    def test_negative_drop_m_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="drop_m"):
            DropParam(
                category=CATEGORY_LUMINAIRE,
                drop_m=-0.1,
                source=SOURCE_ASSUMPTION,
                basis="test",
            )

    def test_negative_spare_fraction_raises_value_error(self) -> None:
        params = _default()
        with pytest.raises(ValueError, match="spare_fraction"):
            CableEstimateParams(
                vertical_drops=params.vertical_drops,
                spare_fraction=-0.01,
                spare_source=SOURCE_ASSUMPTION,
                spare_basis="test",
            )

    def test_override_negative_drop_raises_value_error(self) -> None:
        params = _default()
        with pytest.raises(ValueError, match="drop_m"):
            params.with_overrides(drops={CATEGORY_SWITCH: -1.0})

    def test_override_negative_spare_raises_value_error(self) -> None:
        params = _default()
        with pytest.raises(ValueError, match="spare_fraction"):
            params.with_overrides(spare_fraction=-0.05)

    def test_zero_drop_is_valid(self) -> None:
        drop = DropParam(
            category=CATEGORY_DISTRIBUTION_BOARD,
            drop_m=0.0,
            source=SOURCE_ASSUMPTION,
            basis="test",
        )
        assert drop.drop_m == 0.0

    def test_zero_spare_is_valid(self) -> None:
        params = _default()
        new_params = params.with_overrides(spare_fraction=0.0)
        assert new_params.spare_fraction == 0.0
