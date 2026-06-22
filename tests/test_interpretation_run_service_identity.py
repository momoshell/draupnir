"""Tests for app/interpretation/run_service_identity.py (pure, fakes, no DB).

M-540003 fixture: ONE RunGroup (layer "Pipes", idx150, discipline "HYDRAULIC EQUIPMENT")
with geometry for its members and a tag stack placed within radius -- verifies the full
RESOLVED identity.
"""

from __future__ import annotations

from app.interpretation.routed_runs import RunGroup
from app.interpretation.run_service_identity import (
    BASIS_LEGEND_AND_TAG,
    IDENTITY_PARTIAL,
    IDENTITY_RESOLVED,
    IDENTITY_UNKNOWN,
    TagPlacement,
    fuse_run_service_identities,
)
from app.interpretation.run_tags import BASIS_TAG_TEXT

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_run(
    *,
    layer_ref: str | None = "Pipes",
    colour_key: str | None = "idx150",
    discipline: str | None = "HYDRAULIC EQUIPMENT",
    entity_ids: tuple[str, ...] = ("e1", "e2"),
    source_layers: tuple[str, ...] = ("Pipes",),
    competing_disciplines: tuple[str, ...] = (),
    confidence: float | None = None,
    basis: str = "legend_colour",
    status: str = "resolved",
) -> RunGroup:
    return RunGroup(
        layer_ref=layer_ref,
        colour_key=colour_key,
        colour_index=150,
        colour_rgb=None,
        status=status,
        discipline=discipline,
        basis=basis,
        source_layers=source_layers,
        confidence=confidence,
        competing_disciplines=competing_disciplines,
        entity_ids=entity_ids,
    )


def _line_geom(x1: float, y1: float, x2: float, y2: float) -> dict[str, list[float]]:
    return {"start": [x1, y1, 0.0], "end": [x2, y2, 0.0]}


def _polyline_geom(pts: list[tuple[float, float]]) -> dict[str, list[list[float]]]:
    return {"vertices": [[x, y] for x, y in pts]}


# ---------------------------------------------------------------------------
# M-540003 load-bearing fixture
# ---------------------------------------------------------------------------


def test_m540003_resolved_tag_stack() -> None:
    """ONE run + tag stack [VAC@54, MA@42, AGSS@42, OXY@42] within radius -> RESOLVED."""
    run = _make_run(entity_ids=("e1", "e2"))
    # Both entities centred on (1000, 1000) => run anchor = (1000, 1000)
    geometry = {
        "e1": _line_geom(500.0, 1000.0, 1500.0, 1000.0),
        "e2": _line_geom(500.0, 1000.0, 1500.0, 1000.0),
    }
    # Tag stack close to (1000, 1000)
    tags = [
        TagPlacement(text="O54 mm VAC", point=(1010.0, 1000.0)),
        TagPlacement(text="42 mm MA", point=(1010.0, 980.0)),
        TagPlacement(text="42 mm AGSS", point=(1010.0, 960.0)),
        TagPlacement(text="42 mm OXY", point=(1010.0, 940.0)),
    ]
    result = fuse_run_service_identities([run], geometry, tags, radius=2000.0)

    assert len(result.identities) == 1
    identity = result.identities[0]

    assert identity.discipline == "HYDRAULIC EQUIPMENT"
    assert identity.status == IDENTITY_RESOLVED
    assert identity.basis == BASIS_LEGEND_AND_TAG

    # Collect (service, diameter) for easy assertion
    service_sizes = {(ss.service, ss.size.diameter) for ss in identity.services}
    assert ("VAC", 54) in service_sizes
    assert ("MA", 42) in service_sizes
    assert ("AGSS", 42) in service_sizes
    assert ("OXY", 42) in service_sizes
    assert len(identity.services) == 4

    # All basis values on ServiceSize are BASIS_TAG_TEXT
    assert all(ss.basis == BASIS_TAG_TEXT for ss in identity.services)

    assert result.unassigned_tags == ()
    assert result.ambiguous_tags == ()


# ---------------------------------------------------------------------------
# No-tag run -> PARTIAL
# ---------------------------------------------------------------------------


def test_no_tag_run_is_partial() -> None:
    run = _make_run()
    geometry = {"e1": _line_geom(0.0, 0.0, 100.0, 0.0), "e2": _line_geom(100.0, 0.0, 200.0, 0.0)}
    result = fuse_run_service_identities([run], geometry, [], radius=2000.0)

    assert len(result.identities) == 1
    identity = result.identities[0]
    assert identity.services == ()
    assert identity.status == IDENTITY_PARTIAL
    assert identity.discipline == "HYDRAULIC EQUIPMENT"
    assert identity.basis == "legend_colour"


# ---------------------------------------------------------------------------
# Garbled Ø prefix still parses
# ---------------------------------------------------------------------------


def test_garbled_o_prefix_still_attaches() -> None:
    """b'\\xef\\xbf\\xbd' prefix (U+FFFD) should still parse OXY@42 and attach."""
    run = _make_run(entity_ids=("e1",))
    geometry = {"e1": _line_geom(0.0, 0.0, 100.0, 0.0)}
    garbled = b"\xef\xbf\xbd".decode() + "42 mm OXY"
    tags = [TagPlacement(text=garbled, point=(50.0, 0.0))]
    result = fuse_run_service_identities([run], geometry, tags, radius=500.0)

    assert len(result.identities) == 1
    identity = result.identities[0]
    assert len(identity.services) == 1
    assert identity.services[0].service == "OXY"
    assert identity.services[0].size.diameter == 42
    assert identity.status == IDENTITY_RESOLVED


# ---------------------------------------------------------------------------
# Non-parseable tag line skipped; no crash, no fabricated service
# ---------------------------------------------------------------------------


def test_non_parseable_tag_skipped() -> None:
    run = _make_run(entity_ids=("e1",))
    geometry = {"e1": _line_geom(0.0, 0.0, 100.0, 0.0)}
    tags = [
        TagPlacement(text="NOT A PIPE TAG", point=(50.0, 0.0)),
        TagPlacement(text="", point=(50.0, 0.0)),
        TagPlacement(text="EA", point=(50.0, 0.0)),  # size-less service-only
    ]
    result = fuse_run_service_identities([run], geometry, tags, radius=500.0)

    identity = result.identities[0]
    assert identity.services == ()
    assert identity.status == IDENTITY_PARTIAL
    # Non-parseable tags do NOT appear in unassigned_tags
    assert result.unassigned_tags == ()


# ---------------------------------------------------------------------------
# Tag beyond radius -> unassigned_tags
# ---------------------------------------------------------------------------


def test_tag_beyond_radius_is_unassigned() -> None:
    run = _make_run(entity_ids=("e1",))
    geometry = {"e1": _line_geom(0.0, 0.0, 100.0, 0.0)}
    # Anchor at (50, 0); tag at (10050, 0) -- clearly beyond 2000
    tags = [TagPlacement(text="42 mm VAC", point=(10050.0, 0.0))]
    result = fuse_run_service_identities([run], geometry, tags, radius=2000.0)

    assert len(result.unassigned_tags) == 1
    assert result.unassigned_tags[0].service == "VAC"
    assert result.identities[0].services == ()


# ---------------------------------------------------------------------------
# Two equidistant runs + one tag -> ambiguous_tags flagged
# ---------------------------------------------------------------------------


def test_equidistant_tag_flagged_as_ambiguous() -> None:
    run_a = _make_run(
        layer_ref="Pipes",
        colour_key="idx150",
        entity_ids=("a1",),
        source_layers=("Pipes",),
    )
    run_b = _make_run(
        layer_ref="Pipes",
        colour_key="idx200",
        entity_ids=("b1",),
        source_layers=("Pipes",),
    )
    geometry = {
        "a1": _line_geom(-100.0, 0.0, 0.0, 0.0),  # anchor at (-50, 0)
        "b1": _line_geom(0.0, 0.0, 100.0, 0.0),  # anchor at (50, 0)
    }
    # Tag equidistant at (0, 0): dist to a=50, dist to b=50
    tags = [TagPlacement(text="54 mm VAC", point=(0.0, 0.0))]
    result = fuse_run_service_identities([run_a, run_b], geometry, tags, radius=500.0)

    assert len(result.ambiguous_tags) == 1
    assert result.ambiguous_tags[0].service == "VAC"
    # Deterministically attached to lower sort key (idx150 < idx200) and does NOT crash
    assert result.unassigned_tags == ()


# ---------------------------------------------------------------------------
# Run with no usable member geometry -> no anchor; tag unassigned
# ---------------------------------------------------------------------------


def test_run_with_no_geometry_has_no_anchor() -> None:
    run = _make_run(entity_ids=("ghost1",))
    # No geometry for ghost1
    geometry: dict[str, dict[str, object]] = {}
    tags = [TagPlacement(text="42 mm VAC", point=(0.0, 0.0))]
    result = fuse_run_service_identities([run], geometry, tags, radius=500.0)

    # Tag cannot attach to a run with no anchor -> unassigned
    assert len(result.unassigned_tags) == 1
    assert result.identities[0].services == ()


# ---------------------------------------------------------------------------
# Empty runs/tags -> empty result
# ---------------------------------------------------------------------------


def test_empty_runs_empty_result() -> None:
    result = fuse_run_service_identities([], {}, [])
    assert result.identities == ()
    assert result.unassigned_tags == ()
    assert result.ambiguous_tags == ()


def test_empty_tags_no_services() -> None:
    run = _make_run(entity_ids=("e1",))
    geometry = {"e1": _line_geom(0.0, 0.0, 100.0, 0.0)}
    result = fuse_run_service_identities([run], geometry, [])
    assert result.identities[0].services == ()
    assert result.unassigned_tags == ()


# ---------------------------------------------------------------------------
# Permutation invariance
# ---------------------------------------------------------------------------


def test_tag_permutation_gives_identical_result() -> None:
    run = _make_run(entity_ids=("e1",))
    geometry = {"e1": _line_geom(0.0, 0.0, 100.0, 0.0)}
    tags = [
        TagPlacement(text="42 mm OXY", point=(50.0, 0.0)),
        TagPlacement(text="54 mm VAC", point=(50.0, 0.0)),
        TagPlacement(text="42 mm AGSS", point=(50.0, 0.0)),
    ]
    tags_reversed = list(reversed(tags))

    result_a = fuse_run_service_identities([run], geometry, tags, radius=500.0)
    result_b = fuse_run_service_identities([run], geometry, tags_reversed, radius=500.0)

    assert result_a.identities[0].services == result_b.identities[0].services


# ---------------------------------------------------------------------------
# Polyline geometry support
# ---------------------------------------------------------------------------


def test_polyline_geometry_anchor() -> None:
    run = _make_run(entity_ids=("p1",))
    # Polyline with vertices making centroid at (200, 200)
    pts = [(100.0, 100.0), (300.0, 100.0), (300.0, 300.0), (100.0, 300.0)]
    geometry = {"p1": _polyline_geom(pts)}
    tags = [TagPlacement(text="76 mm VAC", point=(205.0, 205.0))]
    result = fuse_run_service_identities([run], geometry, tags, radius=500.0)

    assert result.identities[0].status == IDENTITY_RESOLVED
    assert result.identities[0].services[0].service == "VAC"


# ---------------------------------------------------------------------------
# UNKNOWN status when no discipline and no tags
# ---------------------------------------------------------------------------


def test_unknown_status_no_discipline_no_tags() -> None:
    run = _make_run(discipline=None, status="unknown", basis="unresolved", entity_ids=("e1",))
    geometry = {"e1": _line_geom(0.0, 0.0, 100.0, 0.0)}
    result = fuse_run_service_identities([run], geometry, [])
    assert result.identities[0].status == IDENTITY_UNKNOWN
    assert result.identities[0].services == ()


# ---------------------------------------------------------------------------
# source_layers union includes tag layer when provided
# ---------------------------------------------------------------------------


def test_source_layers_union_includes_tag_layer() -> None:
    run = _make_run(entity_ids=("e1",), source_layers=("Pipes",))
    geometry = {"e1": _line_geom(0.0, 0.0, 100.0, 0.0)}
    tags = [TagPlacement(text="42 mm VAC", point=(50.0, 0.0), layer_ref="Pipe Tags")]
    result = fuse_run_service_identities([run], geometry, tags, radius=500.0)

    assert "Pipe Tags" in result.identities[0].source_layers
    assert "Pipes" in result.identities[0].source_layers
