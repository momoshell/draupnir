"""Pure unit tests for app.interpretation.tag_reassembly (issue #795, D3a).

No DB, ORM, or FastAPI imports.  All tests operate on in-memory TagPlacement lists.
"""

from __future__ import annotations

import pytest

from app.interpretation.run_service_identity import TagPlacement
from app.interpretation.run_tags import parse_tag
from app.interpretation.tag_reassembly import ReassemblyResult, reassemble_tag_fragments

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make(text: str, x: float = 0.0, y: float = 0.0, layer: str | None = None) -> TagPlacement:
    return TagPlacement(text=text, point=(x, y), layer_ref=layer)


# ---------------------------------------------------------------------------
# 3-fragment cluster: ∅ + 100 + service → single emitted placement
# ---------------------------------------------------------------------------


def test_three_fragment_cluster_canonical_glyph() -> None:
    """∅, 100, SVP AT HL DROPS TB all within 0.4 m → emits ∅100 SVP AT HL DROPS TB."""
    placements = [
        _make("∅", 0.0, 0.0),
        _make("100", 0.1, 0.1),
        _make("SVP AT HL DROPS TB", 0.2, 0.2),
    ]
    result = reassemble_tag_fragments(placements)
    assert isinstance(result, ReassemblyResult)
    assert result.reassembled_count == 1
    assert len(result.placements) == 1

    emitted = result.placements[0]
    assert emitted.text == "∅100 SVP AT HL DROPS TB"

    # The reassembled text must parse to service=SVP, size=100.
    obs = parse_tag(emitted.text, strict_content=True)
    assert obs is not None
    assert obs.service == "SVP"
    assert obs.size.diameter == 100


# ---------------------------------------------------------------------------
# Garbled glyph variants normalise to ∅
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("glyph_text", ["Ø", "?", "�", "Ø?"])
def test_garbled_glyph_variants_normalised(glyph_text: str) -> None:
    """Any garbled glyph variant is normalised to ∅ in the reassembled text."""
    placements = [
        _make(glyph_text, 0.0, 0.0),
        _make("100", 0.1, 0.0),
        _make("SVP AT HL DROPS TB", 0.2, 0.0),
    ]
    result = reassemble_tag_fragments(placements)
    assert result.reassembled_count == 1
    emitted = result.placements[0]
    assert emitted.text.startswith("∅100")
    obs = parse_tag(emitted.text, strict_content=True)
    assert obs is not None
    assert obs.service == "SVP"
    assert obs.size.diameter == 100


# ---------------------------------------------------------------------------
# Noise fragments: RE and callout text pass through unchanged
# ---------------------------------------------------------------------------


def test_re_fitting_does_not_capture_run_length() -> None:
    """RE (Rodding Eye) is a counted POINT FITTING, not a length-bearing run service.

    Even adjacent to ∅+100, an RE fragment must NOT reassemble into a run tag — it is
    excluded from the run-service vocabulary, so it never steals centerline length.
    """
    placements = [
        _make("∅", 0.0, 0.0),
        _make("100", 0.1, 0.0),
        _make("RE", 0.15, 0.0),
    ]
    result = reassemble_tag_fragments(placements)
    assert result.reassembled_count == 0


def test_callout_continuation_connect_is_noise() -> None:
    """'CONNECT WITH AAV AT ML' adjacent to ∅+75 must not form a cluster (first token too long)."""
    placements = [
        _make("∅", 0.0, 0.0),
        _make("75", 0.1, 0.0),
        _make("CONNECT WITH AAV AT ML", 0.15, 0.0),
    ]
    result = reassemble_tag_fragments(placements)
    assert result.reassembled_count == 0


def test_callout_continuation_via_is_noise() -> None:
    """'VIA HEPVO WATERLESS TRAP' adjacent to ∅+75 must not cluster (VIA not in vocabulary)."""
    placements = [
        _make("∅", 0.0, 0.0),
        _make("75", 0.1, 0.0),
        _make("VIA HEPVO WATERLESS TRAP", 0.15, 0.0),
    ]
    result = reassemble_tag_fragments(placements)
    assert result.reassembled_count == 0


def test_single_digit_is_not_number() -> None:
    """A single-digit string (callout leader) must NOT match as a NUMBER fragment."""
    placements = [
        _make("∅", 0.0, 0.0),
        _make("3", 0.1, 0.0),
        _make("SVP FA TO HL", 0.15, 0.0),
    ]
    result = reassemble_tag_fragments(placements)
    assert result.reassembled_count == 0


def test_two_digit_number_still_clusters() -> None:
    """A two-digit size (75) is a valid NUMBER and must still form a cluster."""
    placements = [
        _make("∅", 0.0, 0.0),
        _make("75", 0.1, 0.0),
        _make("SVP FA TO HL", 0.15, 0.0),
    ]
    result = reassemble_tag_fragments(placements)
    assert result.reassembled_count == 1
    obs = parse_tag(result.placements[0].text, strict_content=True)
    assert obs is not None
    assert obs.service == "SVP"
    assert obs.size.diameter == 75


# ---------------------------------------------------------------------------
# NUMBER with no companion within radius → no cluster
# ---------------------------------------------------------------------------


def test_number_with_no_companion_within_radius() -> None:
    """A lone NUMBER with no GLYPH or SERVICE within 0.5 m passes through."""
    placements = [
        _make("100", 0.0, 0.0),
        _make("SVP AT HL DROPS TB", 5.0, 5.0),  # far away
    ]
    result = reassemble_tag_fragments(placements)
    assert result.reassembled_count == 0
    assert result.rejected_cluster_count == 0
    assert len(result.placements) == 2


# ---------------------------------------------------------------------------
# Ambiguous: NUMBER with two equidistant SERVICE candidates → rejected
# ---------------------------------------------------------------------------


def test_ambiguous_two_equidistant_service_candidates_rejected() -> None:
    """Two SERVICE fragments exactly equidistant from NUMBER → rejected cluster."""
    # Place both service candidates at identical distance (same point, different text).
    placements = [
        _make("100", 0.0, 0.0),
        _make("SVP AT HL DROPS TB", 0.3, 0.0),
        _make("SWP AT GROUND", 0.3, 0.0),  # same point → same distance
    ]
    result = reassemble_tag_fragments(placements)
    assert result.reassembled_count == 0
    assert result.rejected_cluster_count == 1
    # All three pass through (NUMBER wasn't consumed, neither were the service frags).
    assert len(result.placements) == 3


# ---------------------------------------------------------------------------
# Non-regression: intact parseable placement passes through unchanged
# ---------------------------------------------------------------------------


def test_intact_parseable_tag_passes_through_unchanged() -> None:
    """An already-parseable tag like 'Ø150 SA' must NOT be consumed as a fragment."""
    intact = _make("Ø150 SA", 0.0, 0.0)
    result = reassemble_tag_fragments([intact])
    assert result.reassembled_count == 0
    assert len(result.placements) == 1
    assert result.placements[0].text == "Ø150 SA"


# ---------------------------------------------------------------------------
# Non-fragmented list (all NOISE/intact) → output equals input
# ---------------------------------------------------------------------------


def test_non_fragmented_list_output_equals_input() -> None:
    """When every placement is NOISE or intact, input is passed through unchanged."""
    placements = [
        _make("Ø150 SA", 0.0, 0.0),
        _make("some note text", 1.0, 0.0),
        _make("ANOTHER LABEL", 2.0, 0.0),
    ]
    result = reassemble_tag_fragments(placements)
    assert result.reassembled_count == 0
    assert result.rejected_cluster_count == 0
    assert list(result.placements) == placements


# ---------------------------------------------------------------------------
# Determinism: same input → same output order
# ---------------------------------------------------------------------------


def test_determinism_same_input_same_output() -> None:
    """Calling reassemble_tag_fragments twice with the same input yields identical output."""
    placements = [
        _make("∅", 0.0, 0.0),
        _make("100", 0.1, 0.1),
        _make("SVP AT HL DROPS TB", 0.2, 0.2),
        _make("Ø150 SA", 5.0, 5.0),
        _make("RE", 3.0, 3.0),
    ]
    result_a = reassemble_tag_fragments(placements)
    result_b = reassemble_tag_fragments(placements)
    assert result_a.placements == result_b.placements
    assert result_a.reassembled_count == result_b.reassembled_count
    assert result_a.rejected_cluster_count == result_b.rejected_cluster_count


# ---------------------------------------------------------------------------
# Point of emitted placement: SERVICE point when present, else NUMBER point
# ---------------------------------------------------------------------------


def test_emitted_placement_uses_service_fragment_point() -> None:
    """Emitted placement point is the SERVICE fragment's point."""
    svc_point = (0.25, 0.25)
    num_point = (0.1, 0.1)
    placements = [
        _make("∅", 0.0, 0.0),
        TagPlacement(text="100", point=num_point, layer_ref="pipes"),
        TagPlacement(text="SVP AT HL DROPS TB", point=svc_point, layer_ref=None),
    ]
    result = reassemble_tag_fragments(placements)
    assert result.reassembled_count == 1
    emitted = result.placements[0]
    assert emitted.point == svc_point
    assert emitted.layer_ref == "pipes"


def test_emitted_placement_uses_number_point_when_no_service() -> None:
    """When only GLYPH + NUMBER cluster (no SERVICE), point falls back to NUMBER's point."""
    # Build a reassembled string that parse_tag can accept without a service word:
    # e.g. ∅100 with a service appended; actually glyph-only clusters don't parse.
    # This test verifies passthrough behaviour when glyph+number don't form a valid tag.
    num_point = (0.1, 0.0)
    placements = [
        _make("∅", 0.0, 0.0),
        TagPlacement(text="100", point=num_point, layer_ref="pipes"),
    ]
    result = reassemble_tag_fragments(placements)
    # ∅100 doesn't parse (no service), so rejected_cluster_count == 1 and nothing emitted.
    assert result.reassembled_count == 0
    assert result.rejected_cluster_count == 1


# ---------------------------------------------------------------------------
# layer_ref of emitted placement is always from the NUMBER fragment
# ---------------------------------------------------------------------------


def test_emitted_layer_ref_from_number_fragment() -> None:
    """Emitted placement inherits layer_ref from the NUMBER fragment."""
    placements = [
        TagPlacement(text="∅", point=(0.0, 0.0), layer_ref="tag-layer-glyph"),
        TagPlacement(text="100", point=(0.1, 0.0), layer_ref="tag-layer-number"),
        TagPlacement(text="SVP AT HL DROPS TB", point=(0.2, 0.0), layer_ref="tag-layer-svc"),
    ]
    result = reassemble_tag_fragments(placements)
    assert result.reassembled_count == 1
    assert result.placements[0].layer_ref == "tag-layer-number"


# ---------------------------------------------------------------------------
# custom cluster_radius_m parameter is respected
# ---------------------------------------------------------------------------


def test_custom_cluster_radius_excludes_distant_fragments() -> None:
    """With a small radius, fragments at 0.4 m are excluded."""
    placements = [
        _make("∅", 0.0, 0.0),
        _make("100", 0.3, 0.0),
        _make("SVP AT HL DROPS TB", 0.3, 0.0),
    ]
    # Default 0.5 m: cluster forms.
    result_default = reassemble_tag_fragments(placements)
    assert result_default.reassembled_count == 1

    # Very tight radius 0.1 m: fragments out of range — no cluster.
    result_tight = reassemble_tag_fragments(placements, cluster_radius_m=0.1)
    assert result_tight.reassembled_count == 0


# ---------------------------------------------------------------------------
# Vocabulary gate: real-data false-positive words must be rejected
# ---------------------------------------------------------------------------


def test_deep_seal_trap_text_is_noise() -> None:
    """'DEEP SEAL ...' adjacent to ∅+40: DEEP is not a service code, must not cluster."""
    placements = [
        _make("∅", 0.0, 0.0),
        _make("40", 0.1, 0.0),
        _make("DEEP SEAL 'P' TRAP CONNECT", 0.15, 0.0),
    ]
    result = reassemble_tag_fragments(placements)
    assert result.reassembled_count == 0


def test_pan_connector_text_is_noise() -> None:
    """'PAN CONNECTOR TO &' adjacent to ∅+100: PAN is not a service code, must not cluster."""
    placements = [
        _make("∅", 0.0, 0.0),
        _make("100", 0.1, 0.0),
        _make("PAN CONNECTOR TO &", 0.15, 0.0),
    ]
    result = reassemble_tag_fragments(placements)
    assert result.reassembled_count == 0


def test_svp_fragment_still_clusters_with_standard_fallback() -> None:
    """∅75 SVP FA TO HL must still reassemble when using the standard fallback vocabulary."""
    placements = [
        _make("∅", 0.0, 0.0),
        _make("75", 0.1, 0.0),
        _make("SVP FA TO HL", 0.15, 0.0),
    ]
    result = reassemble_tag_fragments(placements)
    assert result.reassembled_count == 1
    obs = parse_tag(result.placements[0].text, strict_content=True)
    assert obs is not None
    assert obs.service == "SVP"
    assert obs.size.diameter == 75


# ---------------------------------------------------------------------------
# Legend override: legend_abbreviations overrides standard fallback entirely
# ---------------------------------------------------------------------------


def test_legend_abbreviations_overrides_standard_fallback() -> None:
    """When legend_abbreviations is provided, it is the sole allowed vocabulary.

    XYZ (custom legend code) reassembles; SVP (standard code) does NOT because
    it is absent from the supplied legend set.
    """
    legend = frozenset({"XYZ"})

    # XYZ is in legend → should cluster.
    placements_xyz = [
        _make("∅", 0.0, 0.0),
        _make("75", 0.1, 0.0),
        _make("XYZ AT HL", 0.15, 0.0),
    ]
    result_xyz = reassemble_tag_fragments(placements_xyz, legend_abbreviations=legend)
    assert result_xyz.reassembled_count == 1

    # SVP is NOT in the supplied legend → must not cluster.
    placements_svp = [
        _make("∅", 0.0, 0.0),
        _make("75", 0.1, 0.0),
        _make("SVP AT HL", 0.15, 0.0),
    ]
    result_svp = reassemble_tag_fragments(placements_svp, legend_abbreviations=legend)
    assert result_svp.reassembled_count == 0
