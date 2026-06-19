"""Unit tests for the pure revision-diff logic (#524)."""

from uuid import uuid4

from app.api.v1.revision_routes.diff import _EntityKey, compute_revision_diff
from app.schemas.revision_diff import RevisionDiffRead

E = _EntityKey


def _diff(
    base: list[_EntityKey],
    target: list[_EntityKey],
    *,
    base_layers: list[str] | None = None,
    target_layers: list[str] | None = None,
    base_ratio: float | None = None,
    target_ratio: float | None = None,
    include: frozenset[str] = frozenset(),
) -> RevisionDiffRead:
    return compute_revision_diff(
        base_revision_id=uuid4(),
        target_revision_id=uuid4(),
        base_entities=base,
        target_entities=target,
        base_layers=base_layers or [],
        target_layers=target_layers or [],
        base_mapped_ratio=base_ratio,
        target_mapped_ratio=target_ratio,
        include=include,
    )


def test_identical_revisions_report_no_changes() -> None:
    entities = [E("s1", "h1", "line"), E("s2", "h2", "circle")]
    diff = _diff(entities, list(entities))
    assert diff.entities.added == 0
    assert diff.entities.removed == 0
    assert diff.entities.changed == 0
    assert diff.entities.unchanged == 2


def test_additive_reingest_flags_added() -> None:
    base = [E("s1", "h1", "line")]
    target = [E("s1", "h1", "line"), E("s2", "h2", "line")]
    diff = _diff(base, target)
    assert diff.entities.added == 1
    assert diff.entities.removed == 0
    assert diff.entities.unchanged == 1


def test_lossy_reingest_flags_removed() -> None:
    base = [E("s1", "h1", "line"), E("s2", "h2", "line")]
    target = [E("s1", "h1", "line")]
    diff = _diff(base, target)
    assert diff.entities.removed == 1
    assert diff.entities.added == 0
    assert diff.entities.unchanged == 1


def test_content_change_flags_changed_via_hash() -> None:
    base = [E("s1", "h1", "line")]
    target = [E("s1", "h2", "line")]  # same identity, new hash
    diff = _diff(base, target)
    assert diff.entities.changed == 1
    assert diff.entities.unchanged == 0


def test_unkeyed_entities_are_counted_separately() -> None:
    base = [E(None, None, "line"), E("s1", "h1", "line")]
    target = [E(None, None, "line"), E(None, None, "circle"), E("s1", "h1", "line")]
    diff = _diff(base, target)
    assert diff.entities.unkeyed_base == 1
    assert diff.entities.unkeyed_target == 2
    # Unkeyed rows never count as added/removed/changed (no identity to match on).
    assert diff.entities.added == 0
    assert diff.entities.removed == 0


def test_layer_and_type_and_coverage_deltas() -> None:
    base = [E("s1", "h1", "line"), E("s2", "h2", "line")]
    target = [E("s1", "h1", "line"), E("s3", "h3", "circle")]
    diff = _diff(
        base,
        target,
        base_layers=["A-WALL", "A-DOOR"],
        target_layers=["A-WALL", "M-DUCT"],
        base_ratio=0.80,
        target_ratio=0.95,
    )
    assert diff.layers.added == ["M-DUCT"]
    assert diff.layers.removed == ["A-DOOR"]
    assert diff.counts_by_type["line"].base == 2
    assert diff.counts_by_type["line"].target == 1
    assert diff.counts_by_type["line"].delta == -1
    assert diff.counts_by_type["circle"].delta == 1
    assert diff.coverage.mapped_ratio_delta is not None
    assert round(diff.coverage.mapped_ratio_delta, 2) == 0.15


def test_coverage_delta_none_when_either_side_missing() -> None:
    diff = _diff([E("s1", "h1", "line")], [E("s1", "h1", "line")], base_ratio=0.9)
    assert diff.coverage.mapped_ratio_delta is None
    assert diff.coverage.target_mapped_ratio is None


def test_fields_opt_in_includes_id_lists() -> None:
    base = [E("s1", "h1", "line"), E("s2", "h2", "line")]
    target = [E("s2", "hX", "line"), E("s3", "h3", "line")]
    diff = _diff(base, target, include=frozenset({"added", "removed", "changed"}))
    assert diff.entities.added_ids == ["s3"]
    assert diff.entities.removed_ids == ["s1"]
    assert diff.entities.changed_ids == ["s2"]


def test_ids_omitted_by_default() -> None:
    diff = _diff([E("s1", "h1", "line")], [E("s2", "h2", "line")])
    assert diff.entities.added_ids is None
    assert diff.entities.removed_ids is None
    assert diff.entities.changed_ids is None
