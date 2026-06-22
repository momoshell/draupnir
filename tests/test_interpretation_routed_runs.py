"""Tests for routed_runs coordinator (issue #606, Phase 1 / be-02).

All tests are pure: no DB, no adapters, fakes only.
"""

from __future__ import annotations

from app.interpretation.routed_runs import (
    BASIS_LEGEND_COLOUR,
    BASIS_UNRESOLVED,
    ROUTED_ENTITY_TYPES,
    STATUS_RESOLVED,
    STATUS_UNKNOWN,
    RoutedEntity,
    RoutedRunResult,
    identify_routed_runs,
)
from app.interpretation.service_legend import (
    ServiceLegend,
    SwatchInput,
    from_colour_swatches,
    fuse,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HYDRAULIC_COLOR = {"index": 150, "rgb": "c22d71", "by_layer": False, "by_block": False}
_UNKNOWN_COLOR = {"index": 7, "rgb": None, "by_layer": False, "by_block": False}


def _hydraulic_legend() -> ServiceLegend:
    """Legend with one swatch: index 150 / rgb c22d71 -> HYDRAULIC EQUIPMENT."""
    swatches = [SwatchInput(color=_HYDRAULIC_COLOR, text="HYDRAULIC EQUIPMENT")]
    return fuse(from_colour_swatches(swatches))


# ---------------------------------------------------------------------------
# M-540003 load-bearing fixture
# ---------------------------------------------------------------------------


def test_m540003_single_group_resolved() -> None:
    """N entities, all layer='Pipes', same colour -> EXACTLY ONE RunGroup, RESOLVED."""
    n = 5
    entities = [
        RoutedEntity(
            entity_id=f"e{i}",
            entity_type="line",
            layer_ref="Pipes",
            color=_HYDRAULIC_COLOR,
            geometry=None,
        )
        for i in range(n)
    ]
    legend = _hydraulic_legend()
    result = identify_routed_runs(entities, legend)

    assert isinstance(result, RoutedRunResult)
    assert len(result.groups) == 1

    group = result.groups[0]
    assert group.status == STATUS_RESOLVED
    assert group.discipline == "HYDRAULIC EQUIPMENT"
    assert group.basis == BASIS_LEGEND_COLOUR
    assert group.source_layers == ("Pipes",)
    assert group.entity_ids == tuple(f"e{i}" for i in range(n))


# ---------------------------------------------------------------------------
# Honest-unknown: unmatched layer/colour
# ---------------------------------------------------------------------------


def test_honest_unknown_unmatched_colour() -> None:
    """Entity with a colour not in the legend -> UNKNOWN, raw values preserved."""
    entity = RoutedEntity(
        entity_id="u1",
        entity_type="polyline",
        layer_ref="SomePipe",
        color=_UNKNOWN_COLOR,
        geometry=None,
    )
    legend = _hydraulic_legend()
    result = identify_routed_runs([entity], legend)

    assert len(result.groups) == 1
    group = result.groups[0]
    assert group.status == STATUS_UNKNOWN
    assert group.discipline is None
    assert group.basis == BASIS_UNRESOLVED
    assert group.layer_ref == "SomePipe"
    assert group.colour_index == 7
    assert group.colour_rgb is None
    assert group.source_layers == ("SomePipe",)
    assert group.entity_ids == ("u1",)


# ---------------------------------------------------------------------------
# BYLAYER parity: entities with same resolved colour_key group together
# ---------------------------------------------------------------------------


def test_bylayer_parity_same_colour_key_single_group() -> None:
    """Two entities with the same resolved colour_key (idx:150) group into one RunGroup."""
    # First entity has explicit index 150
    e1 = RoutedEntity(
        entity_id="e1",
        entity_type="line",
        layer_ref="Pipes",
        color={"index": 150, "rgb": "c22d71", "by_layer": False, "by_block": False},
        geometry=None,
    )
    # Second entity also resolves to the same rgb (same colour_key)
    e2 = RoutedEntity(
        entity_id="e2",
        entity_type="arc",
        layer_ref="Pipes",
        color={"index": 150, "rgb": "c22d71", "by_layer": True, "by_block": False},
        geometry=None,
    )
    legend = _hydraulic_legend()
    result = identify_routed_runs([e1, e2], legend)

    # Both share colour_key "c22d71" and layer "Pipes" -> one group
    assert len(result.groups) == 1
    group = result.groups[0]
    assert group.status == STATUS_RESOLVED
    assert group.entity_ids == ("e1", "e2")


# ---------------------------------------------------------------------------
# Conflicted legend: ServiceEntry with competing_disciplines
# ---------------------------------------------------------------------------


def test_conflicted_legend_competing_disciplines_surfaced() -> None:
    """Legend with two disciplines for one colour -> group surfaces competing_disciplines."""
    swatches = [
        SwatchInput(color=_HYDRAULIC_COLOR, text="HYDRAULIC EQUIPMENT"),
        SwatchInput(color=_HYDRAULIC_COLOR, text="FIRE PROTECTION"),
    ]
    legend = fuse(from_colour_swatches(swatches))

    entity = RoutedEntity(
        entity_id="c1",
        entity_type="line",
        layer_ref="Pipes",
        color=_HYDRAULIC_COLOR,
        geometry=None,
    )
    result = identify_routed_runs([entity], legend)

    assert len(result.groups) == 1
    group = result.groups[0]
    assert group.status == STATUS_RESOLVED
    assert group.confidence is None
    assert len(group.competing_disciplines) >= 1


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_entities_returns_empty() -> None:
    """No entities -> empty RoutedRunResult."""
    legend = _hydraulic_legend()
    result = identify_routed_runs([], legend)
    assert result.groups == ()


def test_empty_legend_all_groups_unknown() -> None:
    """Empty legend -> every group is UNKNOWN (legitimate, not an error)."""
    entity = RoutedEntity(
        entity_id="x1",
        entity_type="line",
        layer_ref="Pipes",
        color=_HYDRAULIC_COLOR,
        geometry=None,
    )
    empty_legend = fuse([])
    result = identify_routed_runs([entity], empty_legend)

    assert len(result.groups) == 1
    assert result.groups[0].status == STATUS_UNKNOWN


def test_non_routed_types_ignored() -> None:
    """text and insert entities are filtered out; only ROUTED_ENTITY_TYPES survive."""
    non_routed = [
        RoutedEntity(
            entity_id=f"t{i}",
            entity_type=t,
            layer_ref="Pipes",
            color=_HYDRAULIC_COLOR,
            geometry=None,
        )
        for i, t in enumerate(["text", "insert", "mtext", "hatch"])
    ]
    routed = RoutedEntity(
        entity_id="r1",
        entity_type="line",
        layer_ref="Pipes",
        color=_HYDRAULIC_COLOR,
        geometry=None,
    )
    legend = _hydraulic_legend()
    result = identify_routed_runs([*non_routed, routed], legend)

    assert len(result.groups) == 1
    assert result.groups[0].entity_ids == ("r1",)


def test_permutation_invariant() -> None:
    """Shuffled input produces the same groups and entity order within each group."""
    entities = [
        RoutedEntity(
            entity_id="a",
            entity_type="line",
            layer_ref="Pipes",
            color=_HYDRAULIC_COLOR,
            geometry=None,
        ),
        RoutedEntity(
            entity_id="b",
            entity_type="line",
            layer_ref="Pipes",
            color=_HYDRAULIC_COLOR,
            geometry=None,
        ),
        RoutedEntity(
            entity_id="c",
            entity_type="polyline",
            layer_ref="Gas",
            color=_UNKNOWN_COLOR,
            geometry=None,
        ),
    ]
    legend = _hydraulic_legend()

    result_fwd = identify_routed_runs(entities, legend)
    result_rev = identify_routed_runs(list(reversed(entities)), legend)

    # Same number of groups, same statuses after sort
    assert len(result_fwd.groups) == len(result_rev.groups)
    fwd_keys = [(g.layer_ref, g.colour_key, g.status) for g in result_fwd.groups]
    rev_keys = [(g.layer_ref, g.colour_key, g.status) for g in result_rev.groups]
    assert fwd_keys == rev_keys

    # entity_ids within each group reflect input order (not sorted by id)
    # Forward: a,b together; reversed: b,a together — order within group is input-order.
    pipes_fwd = next(g for g in result_fwd.groups if g.layer_ref == "Pipes")
    pipes_rev = next(g for g in result_rev.groups if g.layer_ref == "Pipes")
    assert set(pipes_fwd.entity_ids) == set(pipes_rev.entity_ids)


def test_none_layer_ref_and_none_colour_key_grouped_honestly() -> None:
    """Entities with None layer_ref and None color group together (key (None, None))."""
    entities = [
        RoutedEntity(
            entity_id=f"n{i}",
            entity_type="arc",
            layer_ref=None,
            color=None,
            geometry=None,
        )
        for i in range(3)
    ]
    legend = _hydraulic_legend()
    result = identify_routed_runs(entities, legend)

    assert len(result.groups) == 1
    group = result.groups[0]
    assert group.layer_ref is None
    assert group.colour_key is None
    assert group.source_layers == ()
    assert group.status == STATUS_UNKNOWN
    assert group.entity_ids == ("n0", "n1", "n2")


def test_source_layers_empty_when_layer_ref_none() -> None:
    """source_layers is () when layer_ref is None."""
    entity = RoutedEntity(
        entity_id="s1",
        entity_type="line",
        layer_ref=None,
        color=_HYDRAULIC_COLOR,
        geometry=None,
    )
    legend = _hydraulic_legend()
    result = identify_routed_runs([entity], legend)

    assert result.groups[0].source_layers == ()


def test_routed_entity_types_constant() -> None:
    """ROUTED_ENTITY_TYPES contains the three expected types."""
    assert set(ROUTED_ENTITY_TYPES) == {"line", "polyline", "arc"}
