"""Unit tests for round-trip reconciliation (#523).

Golden faithful canonical → zero deltas (pass); deliberately perturbed canonical
→ flagged drift (warning). Pure over canonical_json; no DB.
"""

from typing import Any

from app.ingestion.validation.reconciliation import (
    build_reconciliation,
    build_reconciliation_check,
)


def _faithful() -> dict[str, Any]:
    return {
        "entity_counts": {"layouts": 1, "layers": 1, "blocks": 1, "entities": 2},
        "layouts": [{"name": "Model"}],
        "layers": [{"layer_ref": "A-WALL", "name": "A-WALL"}],
        "blocks": [{"block_ref": "DOOR-1", "name": "DOOR-1"}],
        "units": {"normalized": "meter"},
        "entities": [
            {
                "entity_id": "e1",
                "entity_type": "insert",
                "layer_ref": "A-WALL",
                "block_ref": "DOOR-1",
                "geometry": {"position": {"x": 0.0, "y": 0.0}},
            },
            {
                "entity_id": "e2",
                "entity_type": "line",
                "layer_ref": "A-WALL",
                "parent_entity_ref": "e1",
                "geometry": {"start": {"x": 0.0, "y": 0.0}, "end": {"x": 10.0, "y": 5.0}},
            },
        ],
    }


def _invariant(report: dict[str, Any], key: str) -> dict[str, Any]:
    return next(inv for inv in report["invariants"] if inv["key"] == key)


def test_faithful_canonical_reports_no_drift() -> None:
    report = build_reconciliation(_faithful())
    assert report["status"] == "match"
    assert report["drifted_invariants"] == []
    assert build_reconciliation_check(report)["status"] == "pass"


def test_faithful_references_have_no_orphans() -> None:
    report = build_reconciliation(_faithful())
    refs = _invariant(report, "references")
    assert refs["status"] == "match"
    assert refs["orphan_layer_refs"] == []
    assert refs["orphan_block_refs"] == []
    assert refs["dangling_parent_refs"] == []


def test_orphan_references_gate_as_warning() -> None:
    canonical = _faithful()
    canonical["entities"] = [
        {"entity_id": "e1", "entity_type": "insert", "layer_ref": "GHOST", "block_ref": "MISSING"},
        {"entity_id": "e2", "entity_type": "line", "layer_ref": "A-WALL", "parent_entity_ref": "x"},
    ]
    report = build_reconciliation(canonical)
    refs = _invariant(report, "references")
    assert refs["orphan_layer_refs"] == ["GHOST"]
    assert refs["orphan_block_refs"] == ["MISSING"]
    assert refs["dangling_parent_refs"] == ["x"]
    # #539: genuine orphans now gate as drift -> warning (valid_with_warnings, never invalid).
    assert refs["status"] == "drift"
    assert refs["gating"] is True
    assert report["status"] == "drift"
    assert "references" in report["drifted_invariants"]
    assert build_reconciliation_check(report)["status"] == "warning"


def test_references_match_via_set_membership_any_identifier() -> None:
    # #539 core case: a layer carries BOTH "ref" and "name"; an entity may reference EITHER.
    # Under the old single-precedence rule the layer resolved to its name ("A-WALL"), so an entity
    # referencing it by "L" was a false orphan. Set-membership matches any declared identifier.
    canonical = _faithful()
    canonical["layers"] = [{"ref": "L", "name": "A-WALL"}]
    canonical["blocks"] = []
    for entity_ref in ("L", "A-WALL"):
        canonical["entities"] = [
            {"entity_id": "e1", "entity_type": "line", "layer_ref": entity_ref}
        ]
        refs = _invariant(build_reconciliation(canonical), "references")
        assert refs["orphan_layer_refs"] == [], f"layer_ref={entity_ref!r} should not be orphan"
        assert refs["status"] == "match"


def test_faithful_structure_and_extents() -> None:
    report = build_reconciliation(_faithful())
    structure = _invariant(report, "structure")
    assert structure["block_definitions"] == 1
    assert structure["block_instances"] == 1
    assert structure["nested_entities"] == 1
    assert structure["distinct_layer_refs"] == 1

    extents = _invariant(report, "extents")
    assert extents["bbox"] == [0.0, 0.0, 10.0, 5.0]
    assert extents["status"] == "match"
    assert _invariant(report, "units")["normalized"] == "meter"


def test_declared_count_mismatch_is_flagged() -> None:
    canonical = _faithful()
    canonical["entity_counts"]["entities"] = 99  # adapter under/over-reported
    report = build_reconciliation(canonical)

    assert report["status"] == "drift"
    assert "declared_counts" in report["drifted_invariants"]
    counts = _invariant(report, "declared_counts")
    assert counts["deltas"]["entities"] == {"declared": 99, "actual": 2}

    check = build_reconciliation_check(report)
    assert check["status"] == "warning"
    assert "declared_counts" in check["details"]["drifted_invariants"]


def test_missing_entity_counts_is_not_applicable_not_drift() -> None:
    canonical = _faithful()
    del canonical["entity_counts"]
    report = build_reconciliation(canonical)
    # No declared reference → cannot diff, but that is not drift (no false warning).
    assert report["status"] == "match"
    assert _invariant(report, "declared_counts")["status"] == "not_applicable"
    assert build_reconciliation_check(report)["status"] == "pass"


def test_unknown_units_and_degenerate_extents_are_informational_only() -> None:
    canonical = _faithful()
    canonical["units"] = {"normalized": "unknown"}
    # Collapse geometry so the aggregate extent is a single point (degenerate).
    for entity in canonical["entities"]:
        entity["geometry"] = {"position": {"x": 1.0, "y": 1.0}}
    report = build_reconciliation(canonical)

    assert _invariant(report, "units")["status"] == "unresolved"
    assert _invariant(report, "extents")["status"] == "degenerate"
    # Informational invariants never gate the verdict.
    assert report["status"] == "match"
    assert build_reconciliation_check(report)["status"] == "pass"


def test_census_absent_is_not_applicable() -> None:
    report = build_reconciliation(_faithful())
    assert _invariant(report, "census")["status"] == "not_applicable"
    assert report["status"] == "match"


def test_census_complete_when_nothing_dropped() -> None:
    canonical = _faithful()
    canonical["census"] = {
        "raw_object_total": 5,
        "drawable_candidates": 2,
        "materialized": 2,
        "dropped": {"total": 0},
        "unsupported_classes": [],
    }
    census = _invariant(build_reconciliation(canonical), "census")
    assert census["status"] == "complete"
    assert census["materialized"] == 2
    assert census["dropped"] == 0


def test_census_incomplete_is_reported_but_does_not_gate() -> None:
    canonical = _faithful()
    canonical["census"] = {
        "raw_object_total": 5,
        "drawable_candidates": 3,
        "materialized": 2,
        "dropped": {"total": 1},
        "unsupported_classes": [{"dxfname": "ACAD_PROXY_ENTITY", "is_zombie": True}],
    }
    report = build_reconciliation(canonical)
    census = _invariant(report, "census")
    assert census["status"] == "incomplete"
    assert census["dropped"] == 1
    assert census["unsupported_classes"] == 1
    # Descriptive only — extraction loss never flips the verdict in v1.
    assert census["gating"] is False
    assert report["status"] == "match"
    assert build_reconciliation_check(report)["status"] == "pass"
