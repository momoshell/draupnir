from __future__ import annotations

from app.estimating.quantities import RevisionEntityInput, RevisionGateMetadata, compute_quantities


def test_allowed_line_length_is_trusted_and_summarizes_bbox() -> None:
    result = compute_quantities(
        RevisionGateMetadata(status="allowed"),
        [
            RevisionEntityInput(
                entity_id="line-1",
                entity_type="line",
                sequence_index=1,
                geometry_json={
                    "type": "line",
                    "start": {"x": 0, "y": 0},
                    "end": {"x": 10, "y": 0},
                    "units": {"length": "ft", "context": "model"},
                },
                properties_json={},
                provenance_json={"source_ref": "layer/1"},
                canonical_entity_json={"shape": "line"},
                source_identity="layer/1",
                source_hash="a" * 64,
            )
        ],
    )

    assert result.trusted_totals is True
    totals = {
        (aggregate.quantity_type, aggregate.context): aggregate.total
        for aggregate in result.aggregates
    }
    assert totals == {
        ("count", "entity_count"): 1.0,
        ("count", "line_count"): 1.0,
        ("length", "model"): 10.0,
    }
    contributor = next(
        contributor
        for contributor in result.contributors
        if contributor.quantity_type == "length"
    )
    assert contributor.bbox is not None
    assert contributor.bbox.min_x == 0.0
    assert contributor.bbox.max_x == 10.0
    assert result.exclusions == ()
    assert result.conflicts == ()


def test_closed_polyline_yields_perimeter_and_area() -> None:
    result = compute_quantities(
        RevisionGateMetadata(status="allowed"),
        [
            RevisionEntityInput(
                entity_id="poly-1",
                entity_type="polyline",
                sequence_index=1,
                geometry_json={
                    "type": "polyline",
                    "closed": True,
                    "points": [
                        {"x": 0, "y": 0},
                        {"x": 2, "y": 0},
                        {"x": 2, "y": 2},
                        {"x": 0, "y": 2},
                    ],
                    "units": {"length": "ft", "area": "sqft"},
                },
                properties_json={},
                provenance_json={},
                canonical_entity_json={"shape": "rectangle"},
                source_hash="b" * 64,
            )
        ],
    )

    totals = {
        (aggregate.quantity_type, aggregate.context): aggregate.total
        for aggregate in result.aggregates
    }
    assert totals == {
        ("area", None): 4.0,
        ("count", "entity_count"): 1.0,
        ("count", "polyline_count"): 1.0,
        ("perimeter", None): 8.0,
    }


def test_allowed_provisional_totals_are_not_trusted() -> None:
    result = compute_quantities(
        RevisionGateMetadata(status="allowed_provisional", reason="needs_review"),
        [
            RevisionEntityInput(
                entity_id="line-1",
                entity_type="line",
                sequence_index=1,
                geometry_json={
                    "type": "line",
                    "start": {"x": 0, "y": 0},
                    "end": {"x": 5, "y": 0},
                    "units": "ft",
                },
                properties_json={},
                provenance_json={},
                canonical_entity_json={},
                source_hash="c" * 64,
            )
        ],
    )

    assert result.trusted_totals is False
    totals = {
        (aggregate.quantity_type, aggregate.context): aggregate
        for aggregate in result.aggregates
    }
    assert totals[("length", None)].total == 5.0
    assert totals[("length", None)].trusted is False
    assert totals[("count", "entity_count")].trusted is False
    assert totals[("count", "line_count")].total == 1.0


def test_review_gated_suppresses_aggregates() -> None:
    result = compute_quantities(
        RevisionGateMetadata(status="review_gated", reason="scale_unknown"),
        [
            RevisionEntityInput(
                entity_id="line-1",
                entity_type="line",
                sequence_index=1,
                geometry_json={
                    "type": "line",
                    "start": {"x": 0, "y": 0},
                    "end": {"x": 7, "y": 0},
                    "units": "ft",
                },
                properties_json={},
                provenance_json={},
                canonical_entity_json={},
                source_hash="d" * 64,
            )
        ],
    )

    assert result.aggregates == ()
    assert len(result.contributors) == 3


def test_source_hash_dedups_identical_contributors_and_conflicts_on_mismatch() -> None:
    duplicate_hash = "e" * 64
    duplicate = RevisionEntityInput(
        entity_id="line-1",
        entity_type="line",
        sequence_index=1,
        geometry_json={
            "type": "line",
            "start": {"x": 0, "y": 0},
            "end": {"x": 10, "y": 0},
            "units": "ft",
        },
        properties_json={},
        provenance_json={},
        canonical_entity_json={"shape": "line"},
        source_hash=duplicate_hash,
    )
    duplicate_copy = RevisionEntityInput(
        entity_id="line-2",
        entity_type="line",
        sequence_index=2,
        geometry_json={
            "type": "line",
            "start": {"x": 0, "y": 0},
            "end": {"x": 10, "y": 0},
            "units": "ft",
        },
        properties_json={},
        provenance_json={},
        canonical_entity_json={"shape": "line"},
        source_hash=duplicate_hash,
    )
    conflict = RevisionEntityInput(
        entity_id="line-3",
        entity_type="line",
        sequence_index=3,
        geometry_json={
            "type": "line",
            "start": {"x": 0, "y": 0},
            "end": {"x": 11, "y": 0},
            "units": "ft",
        },
        properties_json={},
        provenance_json={},
        canonical_entity_json={"shape": "line"},
        source_hash=duplicate_hash,
    )

    deduped = compute_quantities(
        RevisionGateMetadata(status="allowed"),
        [duplicate, duplicate_copy],
    )
    assert len(deduped.contributors) == 3
    length_contributor = next(
        contributor
        for contributor in deduped.contributors
        if contributor.quantity_type == "length"
    )
    assert length_contributor.duplicate_entity_ids == ("line-2",)
    totals = {
        (aggregate.quantity_type, aggregate.context): aggregate.total
        for aggregate in deduped.aggregates
    }
    assert totals[("length", None)] == 10.0
    assert totals[("count", "entity_count")] == 1.0

    conflicted = compute_quantities(RevisionGateMetadata(status="allowed"), [duplicate, conflict])
    assert conflicted.contributors == ()
    assert len(conflicted.conflicts) == 3


def test_missing_source_hash_uses_fingerprint_scoped_lower_trust_fallback() -> None:
    entity = RevisionEntityInput(
        entity_id="line-1",
        entity_type="line",
        sequence_index=1,
        geometry_json={
            "type": "line",
            "start": {"x": 0, "y": 0},
            "end": {"x": 3, "y": 4},
            "units": {"length": "ft", "context": "model"},
        },
        properties_json={},
        provenance_json={"source_ref": "ref-1"},
        canonical_entity_json={"shape": "line"},
        source_identity="fallback-1",
    )
    same_fingerprint = RevisionEntityInput(
        entity_id="line-1",
        entity_type="line",
        sequence_index=2,
        geometry_json={
            "type": "line",
            "start": {"x": 0, "y": 0},
            "end": {"x": 3, "y": 4},
            "units": {"length": "ft", "context": "model"},
        },
        properties_json={},
        provenance_json={"source_ref": "ref-1"},
        canonical_entity_json={"shape": "line"},
        source_identity="fallback-1",
    )
    changed_fingerprint = RevisionEntityInput(
        entity_id="line-1",
        entity_type="line",
        sequence_index=3,
        geometry_json={
            "type": "line",
            "start": {"x": 0, "y": 0},
            "end": {"x": 6, "y": 8},
            "units": {"length": "ft", "context": "model"},
        },
        properties_json={},
        provenance_json={"source_ref": "ref-1"},
        canonical_entity_json={"shape": "line"},
        source_identity="fallback-1",
    )

    result = compute_quantities(
        RevisionGateMetadata(status="allowed"),
        [entity, same_fingerprint, changed_fingerprint],
    )

    length_contributors = [
        contributor
        for contributor in result.contributors
        if contributor.quantity_type == "length"
    ]
    assert len(length_contributors) == 2
    assert {contributor.value for contributor in length_contributors} == {5.0, 10.0}
    assert {contributor.trust for contributor in result.contributors} == {"lower_trust"}


def test_strict_hint_fallback_never_double_counts_geometry() -> None:
    geometry_entity = RevisionEntityInput(
        entity_id="line-1",
        entity_type="line",
        sequence_index=1,
        geometry_json={
            "type": "line",
            "start": {"x": 0, "y": 0},
            "end": {"x": 10, "y": 0},
            "units": "ft",
        },
        properties_json={
            "quantity_hints": [
                {
                    "quantity_type": "length",
                    "value": 10.0,
                    "unit": "ft",
                    "strict": True,
                    "provenance": {"origin": "adapter"},
                }
            ]
        },
        provenance_json={},
        canonical_entity_json={},
        source_hash="f" * 64,
    )
    hint_only_entity = RevisionEntityInput(
        entity_id="text-1",
        entity_type="text",
        sequence_index=2,
        geometry_json={"type": "text"},
        properties_json={
            "quantity_hints": [
                {
                    "quantity_type": "length",
                    "value": 9.0,
                    "unit": "ft",
                    "strict": True,
                    "provenance": {"origin": "adapter", "source_ref": "hint-1"},
                }
            ]
        },
        provenance_json={"source_ref": "hint-parent"},
        canonical_entity_json={},
        source_hash="1" * 64,
    )

    result = compute_quantities(
        RevisionGateMetadata(status="allowed"),
        [geometry_entity, hint_only_entity],
    )

    quantity_methods = {contributor.method for contributor in result.contributors}
    assert quantity_methods == {"entity_count", "geometry", "hint_fallback"}
    measured_total = sum(
        contributor.value
        for contributor in result.contributors
        if contributor.quantity_type == "length"
    )
    assert measured_total == 19.0
    assert {
        aggregate.context for aggregate in result.aggregates if aggregate.quantity_type == "count"
    } == {"entity_count", "line_count", "text_count"}


def test_count_output_and_adapter_shape_inference_without_top_level_kind() -> None:
    result = compute_quantities(
        RevisionGateMetadata(status="allowed"),
        [
            RevisionEntityInput(
                entity_id="line-1",
                entity_type="line",
                sequence_index=1,
                geometry_json={
                    "start": {"x": 0, "y": 0},
                    "end": {"x": 10, "y": 0},
                    "units": "ft",
                    "geometry_summary": {"kind": "line"},
                },
                properties_json={},
                provenance_json={},
                canonical_entity_json={},
                source_hash="2" * 64,
            ),
            RevisionEntityInput(
                entity_id="poly-1",
                entity_type="polyline",
                sequence_index=2,
                geometry_json={
                    "vertices": [
                        {"x": 0, "y": 0},
                        {"x": 4, "y": 0},
                        {"x": 4, "y": 3},
                    ],
                    "units": "ft",
                    "geometry_summary": {"kind": "polyline"},
                },
                properties_json={},
                provenance_json={},
                canonical_entity_json={},
                source_hash="3" * 64,
            ),
        ],
    )

    totals = {
        (aggregate.quantity_type, aggregate.context): aggregate.total
        for aggregate in result.aggregates
    }
    assert totals[("length", None)] == 17.0
    assert totals[("count", "entity_count")] == 2.0
    assert totals[("count", "line_count")] == 1.0
    assert totals[("count", "polyline_count")] == 1.0


def test_unknown_unit_geometry_is_excluded_from_aggregates() -> None:
    result = compute_quantities(
        RevisionGateMetadata(status="allowed"),
        [
            RevisionEntityInput(
                entity_id="line-1",
                entity_type="line",
                sequence_index=1,
                geometry_json={
                    "type": "line",
                    "start": {"x": 0, "y": 0},
                    "end": {"x": 10, "y": 0},
                    "units": "point",
                },
                properties_json={},
                provenance_json={},
                canonical_entity_json={},
                source_hash="4" * 64,
            )
        ],
    )

    totals = {
        (aggregate.quantity_type, aggregate.context): aggregate.total
        for aggregate in result.aggregates
    }
    assert totals == {
        ("count", "entity_count"): 1.0,
        ("count", "line_count"): 1.0,
    }
    assert [exclusion.reason for exclusion in result.exclusions] == ["ineligible_unit"]
    assert all(contributor.quantity_type == "count" for contributor in result.contributors)


def test_adapter_style_normalized_units_aggregate_length() -> None:
    result = compute_quantities(
        RevisionGateMetadata(status="allowed"),
        [
            RevisionEntityInput(
                entity_id="line-1",
                entity_type="line",
                sequence_index=1,
                geometry_json={
                    "type": "line",
                    "start": {"x": 0, "y": 0},
                    "end": {"x": 3, "y": 4},
                    "units": {"normalized": "meter", "source": "$INSUNITS"},
                },
                properties_json={},
                provenance_json={},
                canonical_entity_json={},
                source_hash="5" * 64,
            )
        ],
    )

    totals = {
        (aggregate.quantity_type, aggregate.context): aggregate.total
        for aggregate in result.aggregates
    }
    assert totals == {
        ("count", "entity_count"): 1.0,
        ("count", "line_count"): 1.0,
        ("length", None): 5.0,
    }
    length_contributor = next(
        contributor
        for contributor in result.contributors
        if contributor.quantity_type == "length"
    )
    assert length_contributor.unit == "meter"


def test_ineligible_and_count_hint_fallbacks_are_suppressed() -> None:
    result = compute_quantities(
        RevisionGateMetadata(status="allowed"),
        [
            RevisionEntityInput(
                entity_id="text-1",
                entity_type="text",
                sequence_index=1,
                geometry_json={"type": "text"},
                properties_json={
                    "quantity_hints": [
                        {
                            "quantity_type": "count",
                            "value": 7.0,
                            "unit": "each",
                            "strict": True,
                            "provenance": {"origin": "adapter"},
                        },
                        {
                            "quantity_type": "length",
                            "value": 12.0,
                            "unit": "point",
                            "strict": True,
                            "provenance": {"origin": "adapter"},
                        },
                    ]
                },
                provenance_json={},
                canonical_entity_json={},
                source_hash="6" * 64,
            )
        ],
    )

    assert all(contributor.quantity_type == "count" for contributor in result.contributors)
    assert {
        (aggregate.quantity_type, aggregate.context): aggregate.total
        for aggregate in result.aggregates
    } == {
        ("count", "entity_count"): 1.0,
        ("count", "text_count"): 1.0,
    }


def test_malformed_hint_strict_and_value_inputs_do_not_create_fallbacks() -> None:
    result = compute_quantities(
        RevisionGateMetadata(status="allowed"),
        [
            RevisionEntityInput(
                entity_id="text-1",
                entity_type="text",
                sequence_index=1,
                geometry_json={"type": "text"},
                properties_json={
                    "quantity_hints": [
                        {
                            "quantity_type": "length",
                            "value": 8.0,
                            "unit": "ft",
                            "strict": "true",
                            "provenance": {"origin": "adapter"},
                        }
                    ]
                },
                provenance_json={},
                canonical_entity_json={},
                source_hash="7" * 64,
            ),
            RevisionEntityInput(
                entity_id="text-2",
                entity_type="text",
                sequence_index=2,
                geometry_json={"type": "text"},
                properties_json={
                    "quantity_hints": [
                        {
                            "quantity_type": "length",
                            "value": True,
                            "unit": "ft",
                            "strict": True,
                            "provenance": {"origin": "adapter"},
                        }
                    ]
                },
                provenance_json={},
                canonical_entity_json={},
                source_hash="8" * 64,
            ),
            RevisionEntityInput(
                entity_id="text-3",
                entity_type="text",
                sequence_index=3,
                geometry_json={"type": "text"},
                properties_json={
                    "quantity_hints": [
                        {
                            "quantity_type": "length",
                            "value": -3.0,
                            "unit": "ft",
                            "strict": True,
                            "provenance": {"origin": "adapter"},
                        }
                    ]
                },
                provenance_json={},
                canonical_entity_json={},
                source_hash="9" * 64,
            ),
        ],
    )

    assert all(contributor.quantity_type == "count" for contributor in result.contributors)
    assert {
        (aggregate.quantity_type, aggregate.context): aggregate.total
        for aggregate in result.aggregates
    } == {
        ("count", "entity_count"): 3.0,
        ("count", "text_count"): 3.0,
    }


def test_nonfinite_geometry_is_excluded() -> None:
    result = compute_quantities(
        RevisionGateMetadata(status="allowed"),
        [
            RevisionEntityInput(
                entity_id="bad-line",
                entity_type="line",
                sequence_index=1,
                geometry_json={
                    "type": "line",
                    "start": {"x": 0, "y": 0},
                    "end": {"x": float("inf"), "y": 0},
                    "units": "ft",
                },
                properties_json={},
                provenance_json={},
                canonical_entity_json={},
            )
        ],
    )

    assert all(contributor.quantity_type == "count" for contributor in result.contributors)
    assert {
        (aggregate.quantity_type, aggregate.context): aggregate.total
        for aggregate in result.aggregates
    } == {
        ("count", "entity_count"): 1.0,
        ("count", "line_count"): 1.0,
    }
    assert len(result.exclusions) == 1
    assert result.exclusions[0].reason == "unsupported_geometry"
