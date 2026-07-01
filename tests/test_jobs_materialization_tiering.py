"""Unit tests for the entity-reduction materialization-tier predicate (#831 PR-2).

Pure logic — no DB. Covers the connectivity precompute and each conjunction arm of
:func:`classify_materialization_tier`, plus its conservative-by-default guarantees.
"""

from __future__ import annotations

from app.jobs.materialization_tiering import (
    CONSUMED_LAYER_TOKENS,
    FILL_COMPONENT_MIN,
    SLIVER_LEN_M,
    TIER_FILL_NOISE,
    TIER_PRIMARY,
    classify_materialization_tier,
    compute_line_component_sizes,
    is_consumed_layer,
)


def _line(x1: float, y1: float, x2: float, y2: float) -> dict[str, object]:
    return {"start": {"x": x1, "y": y1}, "end": {"x": x2, "y": y2}}


class TestComputeLineComponentSizes:
    def test_disjoint_segments_form_singleton_components(self) -> None:
        entities = [
            ("a", _line(0, 0, 1, 0)),
            ("b", _line(10, 10, 11, 10)),
        ]
        sizes = compute_line_component_sizes(entities)
        assert sizes == {"a": 1, "b": 1}

    def test_chained_shared_endpoints_form_one_component(self) -> None:
        # a: (0,0)-(1,0); b: (1,0)-(2,0); c: (2,0)-(3,0) — a chain of 3 touching lines.
        entities = [
            ("a", _line(0, 0, 1, 0)),
            ("b", _line(1, 0, 2, 0)),
            ("c", _line(2, 0, 3, 0)),
        ]
        sizes = compute_line_component_sizes(entities)
        assert sizes == {"a": 3, "b": 3, "c": 3}

    def test_two_separate_clusters_get_distinct_sizes(self) -> None:
        entities = [
            ("a", _line(0, 0, 1, 0)),
            ("b", _line(1, 0, 2, 0)),
            ("c", _line(100, 100, 101, 100)),
        ]
        sizes = compute_line_component_sizes(entities)
        assert sizes["a"] == 2
        assert sizes["b"] == 2
        assert sizes["c"] == 1

    def test_endpoints_within_snap_tolerance_unify(self) -> None:
        # b's start is a tiny epsilon away from a's end — should still snap-union.
        entities = [
            ("a", _line(0, 0, 1, 0)),
            ("b", _line(1 + 1e-9, 0, 2, 0)),
        ]
        sizes = compute_line_component_sizes(entities, snap_tolerance=1e-6)
        assert sizes == {"a": 2, "b": 2}

    def test_unreadable_geometry_is_isolated_singleton(self) -> None:
        entities: list[tuple[str, dict[str, object] | None]] = [
            ("a", _line(0, 0, 1, 0)),
            ("bad", None),
            ("bad2", {"start": {"x": 1, "y": 0}}),  # missing end
        ]
        sizes = compute_line_component_sizes(entities)
        assert sizes["bad"] == 1
        assert sizes["bad2"] == 1
        # 'a' stays its own singleton since nothing else shares its endpoints.
        assert sizes["a"] == 1

    def test_large_dense_component_scales(self) -> None:
        # A chain of 60 touching unit segments — mirrors dense hatch-fill sliver mass.
        entities = [(f"e{i}", _line(float(i), 0.0, float(i + 1), 0.0)) for i in range(60)]
        sizes = compute_line_component_sizes(entities)
        assert all(size == 60 for size in sizes.values())


class TestIsConsumedLayer:
    def test_none_or_empty_never_matches(self) -> None:
        assert is_consumed_layer(None) is False
        assert is_consumed_layer("") is False

    def test_matches_known_token_case_insensitively(self) -> None:
        assert is_consumed_layer("A-WALL") is True
        assert is_consumed_layer("E610G_Cable Tray") is True
        assert is_consumed_layer("Z000") is False  # unnamed arch layer, no token match

    def test_unrelated_layer_does_not_match(self) -> None:
        assert is_consumed_layer("A700") is False

    def test_default_token_set_covers_documented_consumers(self) -> None:
        # Sanity: the constant isn't accidentally emptied.
        assert "wall" in CONSUMED_LAYER_TOKENS
        assert "legend" in CONSUMED_LAYER_TOKENS
        assert "centerline" in CONSUMED_LAYER_TOKENS


class TestClassifyMaterializationTier:
    """Each test isolates one conjunction arm; predicate must be conservative-by-default."""

    def _dense_sliver_kwargs(self, *, layer_ref: str | None = "Z000") -> dict[str, object]:
        return {
            "entity_type": "line",
            "geometry_json": _line(0, 0, SLIVER_LEN_M / 2, 0),
            "layer_ref": layer_ref,
            "component_size": FILL_COMPONENT_MIN,
        }

    def test_all_four_arms_satisfied_yields_fill_noise(self) -> None:
        result = classify_materialization_tier(**self._dense_sliver_kwargs())  # type: ignore[arg-type]
        assert result == TIER_FILL_NOISE

    def test_non_line_entity_type_stays_primary(self) -> None:
        kwargs = self._dense_sliver_kwargs()
        kwargs["entity_type"] = "polyline"
        result = classify_materialization_tier(**kwargs)  # type: ignore[arg-type]
        assert result == TIER_PRIMARY

    def test_arc_hatch_insert_text_never_tier(self) -> None:
        for entity_type in ("arc", "hatch", "insert", "text", "mtext", "spline"):
            kwargs = self._dense_sliver_kwargs()
            kwargs["entity_type"] = entity_type
            assert classify_materialization_tier(**kwargs) == TIER_PRIMARY  # type: ignore[arg-type]

    def test_long_segment_stays_primary(self) -> None:
        kwargs = self._dense_sliver_kwargs()
        kwargs["geometry_json"] = _line(0, 0, SLIVER_LEN_M * 10, 0)
        result = classify_materialization_tier(**kwargs)  # type: ignore[arg-type]
        assert result == TIER_PRIMARY

    def test_segment_length_exactly_at_threshold_stays_primary(self) -> None:
        # Predicate requires strictly < sliver_len_m — boundary is conservative (primary).
        kwargs = self._dense_sliver_kwargs()
        kwargs["geometry_json"] = _line(0, 0, SLIVER_LEN_M, 0)
        result = classify_materialization_tier(**kwargs)  # type: ignore[arg-type]
        assert result == TIER_PRIMARY

    def test_unreadable_geometry_stays_primary(self) -> None:
        kwargs = self._dense_sliver_kwargs()
        kwargs["geometry_json"] = None
        result = classify_materialization_tier(**kwargs)  # type: ignore[arg-type]
        assert result == TIER_PRIMARY

    def test_small_component_stays_primary(self) -> None:
        kwargs = self._dense_sliver_kwargs()
        kwargs["component_size"] = FILL_COMPONENT_MIN - 1
        result = classify_materialization_tier(**kwargs)  # type: ignore[arg-type]
        assert result == TIER_PRIMARY

    def test_component_size_exactly_at_threshold_tiers(self) -> None:
        kwargs = self._dense_sliver_kwargs()
        kwargs["component_size"] = FILL_COMPONENT_MIN
        result = classify_materialization_tier(**kwargs)  # type: ignore[arg-type]
        assert result == TIER_FILL_NOISE

    def test_consumed_layer_stays_primary_even_with_dense_sliver_component(self) -> None:
        """The load-bearing layer gate: a named consumer layer (e.g. wall) with a dense
        sub-cm component (#831 scout: 12% component-test score alone) must NOT tier."""
        kwargs = self._dense_sliver_kwargs(layer_ref="A-WALL")
        result = classify_materialization_tier(**kwargs)  # type: ignore[arg-type]
        assert result == TIER_PRIMARY

    def test_unnamed_layer_with_dense_sliver_component_tiers(self) -> None:
        # Z000 has no consumed-layer token match — this is the #831 target case.
        kwargs = self._dense_sliver_kwargs(layer_ref="Z000")
        result = classify_materialization_tier(**kwargs)  # type: ignore[arg-type]
        assert result == TIER_FILL_NOISE

    def test_none_layer_ref_is_not_a_consumed_layer_but_other_arms_still_gate(self) -> None:
        kwargs = self._dense_sliver_kwargs(layer_ref=None)
        result = classify_materialization_tier(**kwargs)  # type: ignore[arg-type]
        assert result == TIER_FILL_NOISE
