"""Tests for app/interpretation/run_service_identity.py (pure, fakes, no DB).

M-540003 fixture: ONE RunGroup (layer "Pipes", idx150, discipline "HYDRAULIC EQUIPMENT")
with geometry for its members and a tag stack placed within radius -- verifies the full
RESOLVED identity.

All fixture coordinates are in metres (adapters pre-scale to metres per #661).
Radii are explicit on every call so no test depends on the _DEFAULT_RADIUS constant.
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
    """ONE run + tag stack [VAC@54, MA@42, AGSS@42, OXY@42] within radius -> RESOLVED.

    Coords are metre-scale (0.5-1.5 m); tag is ~0.01 m from the run segment.
    No cross-service over-fusion: all four tags land on the single run.
    """
    run = _make_run(entity_ids=("e1", "e2"))
    # Both entities span x=[0.5, 1.5] at y=1.0 — segment min-dist to tag ~0.01 m
    geometry = {
        "e1": _line_geom(0.5, 1.0, 1.5, 1.0),
        "e2": _line_geom(0.5, 1.0, 1.5, 1.0),
    }
    # Tag stack placed just right of the segment midpoint
    tags = [
        TagPlacement(text="O54 mm VAC", point=(1.01, 1.0)),
        TagPlacement(text="42 mm MA", point=(1.01, 0.98)),
        TagPlacement(text="42 mm AGSS", point=(1.01, 0.96)),
        TagPlacement(text="42 mm OXY", point=(1.01, 0.94)),
    ]
    result = fuse_run_service_identities([run], geometry, tags, radius=3.0)

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
    geometry = {"e1": _line_geom(0.0, 0.0, 0.1, 0.0), "e2": _line_geom(0.1, 0.0, 0.2, 0.0)}
    result = fuse_run_service_identities([run], geometry, [], radius=3.0)

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
    # Line spans 0.0-0.1 m; tag at 0.05 m — within 0.5 m radius
    geometry = {"e1": _line_geom(0.0, 0.0, 0.1, 0.0)}
    garbled = b"\xef\xbf\xbd".decode() + "42 mm OXY"
    tags = [TagPlacement(text=garbled, point=(0.05, 0.0))]
    result = fuse_run_service_identities([run], geometry, tags, radius=0.5)

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
    geometry = {"e1": _line_geom(0.0, 0.0, 0.1, 0.0)}
    tags = [
        TagPlacement(text="NOT A PIPE TAG", point=(0.05, 0.0)),
        TagPlacement(text="", point=(0.05, 0.0)),
        TagPlacement(text="EA", point=(0.05, 0.0)),  # size-less service-only
    ]
    result = fuse_run_service_identities([run], geometry, tags, radius=0.5)

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
    # Segment 0.0-0.1 m; tag at 10.0 m — clearly beyond 3.0 m radius
    geometry = {"e1": _line_geom(0.0, 0.0, 0.1, 0.0)}
    tags = [TagPlacement(text="42 mm VAC", point=(10.0, 0.0))]
    result = fuse_run_service_identities([run], geometry, tags, radius=3.0)

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
    # Tag at (0, 0); run_a segment ends at (0, 0), run_b segment starts at (0, 0) —
    # both segments have min-distance 0.0 to the tag point.
    geometry = {
        "a1": _line_geom(-0.1, 0.0, 0.0, 0.0),
        "b1": _line_geom(0.0, 0.0, 0.1, 0.0),
    }
    tags = [TagPlacement(text="54 mm VAC", point=(0.0, 0.0))]
    result = fuse_run_service_identities([run_a, run_b], geometry, tags, radius=0.5)

    assert len(result.ambiguous_tags) == 1
    assert result.ambiguous_tags[0].service == "VAC"
    # Deterministically attached to lower sort key (idx150 < idx200) and does NOT crash
    assert result.unassigned_tags == ()


# ---------------------------------------------------------------------------
# Run with no usable member geometry -> no min-distance; tag unassigned
# ---------------------------------------------------------------------------


def test_run_with_no_geometry_has_no_anchor() -> None:
    run = _make_run(entity_ids=("ghost1",))
    # No geometry for ghost1
    geometry: dict[str, dict[str, object]] = {}
    tags = [TagPlacement(text="42 mm VAC", point=(0.0, 0.0))]
    result = fuse_run_service_identities([run], geometry, tags, radius=0.5)

    # Tag cannot attach to a run with no usable geometry -> unassigned
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
    geometry = {"e1": _line_geom(0.0, 0.0, 0.1, 0.0)}
    result = fuse_run_service_identities([run], geometry, [], radius=3.0)
    assert result.identities[0].services == ()
    assert result.unassigned_tags == ()


# ---------------------------------------------------------------------------
# Permutation invariance
# ---------------------------------------------------------------------------


def test_tag_permutation_gives_identical_result() -> None:
    run = _make_run(entity_ids=("e1",))
    geometry = {"e1": _line_geom(0.0, 0.0, 0.1, 0.0)}
    tags = [
        TagPlacement(text="42 mm OXY", point=(0.05, 0.0)),
        TagPlacement(text="54 mm VAC", point=(0.05, 0.0)),
        TagPlacement(text="42 mm AGSS", point=(0.05, 0.0)),
    ]
    tags_reversed = list(reversed(tags))

    result_a = fuse_run_service_identities([run], geometry, tags, radius=0.5)
    result_b = fuse_run_service_identities([run], geometry, tags_reversed, radius=0.5)

    assert result_a.identities[0].services == result_b.identities[0].services


# ---------------------------------------------------------------------------
# Polyline geometry support
# ---------------------------------------------------------------------------


def test_polyline_geometry_anchor() -> None:
    run = _make_run(entity_ids=("p1",))
    # Polyline with metre-scale vertices; tag near the segment boundary
    pts = [(0.1, 0.1), (0.3, 0.1), (0.3, 0.3), (0.1, 0.3)]
    geometry = {"p1": _polyline_geom(pts)}
    # Tag at (0.2, 0.09) — ~0.01 m from segment (0.1,0.1)-(0.3,0.1)
    tags = [TagPlacement(text="76 mm VAC", point=(0.2, 0.09))]
    result = fuse_run_service_identities([run], geometry, tags, radius=0.5)

    assert result.identities[0].status == IDENTITY_RESOLVED
    assert result.identities[0].services[0].service == "VAC"


# ---------------------------------------------------------------------------
# UNKNOWN status when no discipline and no tags
# ---------------------------------------------------------------------------


def test_unknown_status_no_discipline_no_tags() -> None:
    run = _make_run(discipline=None, status="unknown", basis="unresolved", entity_ids=("e1",))
    geometry = {"e1": _line_geom(0.0, 0.0, 0.1, 0.0)}
    result = fuse_run_service_identities([run], geometry, [], radius=3.0)
    assert result.identities[0].status == IDENTITY_UNKNOWN
    assert result.identities[0].services == ()


# ---------------------------------------------------------------------------
# source_layers union includes tag layer when provided
# ---------------------------------------------------------------------------


def test_source_layers_union_includes_tag_layer() -> None:
    run = _make_run(entity_ids=("e1",), source_layers=("Pipes",))
    geometry = {"e1": _line_geom(0.0, 0.0, 0.1, 0.0)}
    tags = [TagPlacement(text="42 mm VAC", point=(0.05, 0.0), layer_ref="Pipe Tags")]
    result = fuse_run_service_identities([run], geometry, tags, radius=0.5)

    assert "Pipe Tags" in result.identities[0].source_layers
    assert "Pipes" in result.identities[0].source_layers


# ---------------------------------------------------------------------------
# Regression guard: centroid-far but segment-near L-shaped run resolves
# ---------------------------------------------------------------------------


def test_lshaped_run_resolves_via_nearest_segment() -> None:
    """An L-shaped run whose CENTROID is far from the tag resolves via segment proximity.

    The run has two members forming an L:
      - horizontal leg: (0, 0) to (10, 0)   — centroid at (5, 0)
      - vertical leg:   (0, 0) to (0, 10)   — centroid at (0, 5)

    Run centroid (average of both representative points): (2.5, 2.5).
    Tag at (-0.1, 5.0): dist to run centroid ≈ sqrt(2.6^2 + 2.5^2) ≈ 3.6 m.
    Min segment distance: tag is ~0.1 m from segment (0,0)-(0,10) (the vertical leg).

    Under the old centroid metric this tag would be UNASSIGNED (3.6 m > 3.0 m at old radius).
    Under nearest-segment it RESOLVES (~0.1 m < 5.0 m default).
    Radius=5.0 used explicitly so this test is insulated from future default changes.
    """
    run = _make_run(entity_ids=("h1", "v1"))
    geometry = {
        "h1": _line_geom(0.0, 0.0, 10.0, 0.0),  # horizontal leg
        "v1": _line_geom(0.0, 0.0, 0.0, 10.0),  # vertical leg
    }
    # Tag at (-0.1, 5.0) — very close to the vertical leg, far from centroid
    tags = [TagPlacement(text="42 mm VAC", point=(-0.1, 5.0))]
    result = fuse_run_service_identities([run], geometry, tags, radius=5.0)

    identity = result.identities[0]
    assert identity.status == IDENTITY_RESOLVED, (
        "L-shaped run should RESOLVE via nearest-segment even when centroid is > radius"
    )
    assert len(identity.services) == 1
    assert identity.services[0].service == "VAC"
    assert result.unassigned_tags == ()


# ---------------------------------------------------------------------------
# Callout-distance calibration: M-540003 OXY at 3.85 m fuses at 5.0 m default
# ---------------------------------------------------------------------------


def test_callout_tag_at_3_85m_fuses_at_5m_default() -> None:
    """A "FROM LEFT TO RIGHT" callout tag ~3.85 m from the centerline fuses at 5.0 m.

    Mirrors the M-540003 OXY regression: the size-tag is a callout placed 3.85 m from
    the pipe centerline segment.  At the old 3.0 m default it was excluded; at 5.0 m it
    resolves.  A tag at 16 m (title-block/junk distance) remains unassigned.
    """
    run = _make_run(entity_ids=("pipe1",))
    # Horizontal pipe segment along y=0
    geometry = {"pipe1": _line_geom(0.0, 0.0, 10.0, 0.0)}

    # Tag 3.85 m above the segment midpoint — min-distance to segment = 3.85 m
    tag_callout = TagPlacement(text="42 mm OXY", point=(5.0, 3.85))
    # Tag 16 m above — should remain unassigned
    tag_junk = TagPlacement(text="54 mm VAC", point=(5.0, 16.0))

    result = fuse_run_service_identities([run], geometry, [tag_callout, tag_junk], radius=5.0)

    identity = result.identities[0]
    # Callout tag fuses (3.85 m < 5.0 m)
    assert identity.status == IDENTITY_RESOLVED
    services = {ss.service for ss in identity.services}
    assert "OXY" in services

    # Junk tag at 16 m stays unassigned
    assert len(result.unassigned_tags) == 1
    assert result.unassigned_tags[0].service == "VAC"
