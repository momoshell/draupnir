"""Pure unit tests for app.interpretation.run_tags (issue #785).

Covers the content gate (_is_annotation_prose), the bare _ROUND_RE tightening,
and the legend_abbreviations rescue path.  No DB, ORM, or FastAPI imports.
"""

from __future__ import annotations

import pytest

from app.interpretation.run_tags import (
    parse_tag,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _matches_round(result: object, service: str, diameter: int) -> bool:
    """Return True when *result* is a round TagObservation with the given service+diameter."""
    from app.interpretation.run_tags import TagObservation

    if not isinstance(result, TagObservation):
        return False
    return (
        result.service == service
        and result.size.kind == "round"
        and result.size.diameter == diameter
    )


def _matches_rect(result: object, service: str, width: int, height: int) -> bool:
    """Return True when *result* is a rect TagObservation with the given service+dims."""
    from app.interpretation.run_tags import TagObservation

    if not isinstance(result, TagObservation):
        return False
    return (
        result.service == service
        and result.size.kind == "rect"
        and result.size.width == width
        and result.size.height == height
    )


# ---------------------------------------------------------------------------
# Content gate — contact / title-block prose → None (strict_content=True only)
# ---------------------------------------------------------------------------


class TestContentGate:
    def test_email_address_returns_none(self) -> None:
        """Full contact line with email must be rejected under strict_content."""
        assert parse_tag("tel 020 7631 5291 london@ramboll.co.uk", strict_content=True) is None

    def test_bare_email_returns_none(self) -> None:
        """Email address with no digit+service pattern returns None regardless of gate."""
        assert parse_tag("info@example.co.uk") is None

    def test_tel_keyword_returns_none(self) -> None:
        """TEL-only line with no trailing alpha service is None regardless of gate."""
        assert parse_tag("tel 020 7631 5291") is None

    def test_fax_keyword_returns_none(self) -> None:
        assert parse_tag("fax 020 7631 9999") is None

    def test_www_keyword_returns_none(self) -> None:
        assert parse_tag("www.ramboll.co.uk") is None

    def test_domain_co_uk_returns_none(self) -> None:
        assert parse_tag("visit us at ramboll.co.uk for more info") is None

    def test_domain_com_returns_none(self) -> None:
        assert parse_tag("see acme.com for details") is None

    def test_long_digit_run_returns_none_strict(self) -> None:
        """6+ consecutive digits (phone number) must be rejected under strict_content."""
        assert parse_tag("123456 SERVICE", strict_content=True) is None

    def test_no_number_returns_none(self) -> None:
        """Text with no digit run returns None (unchanged behaviour)."""
        assert parse_tag("SA") is None

    def test_whitespace_only_returns_none(self) -> None:
        assert parse_tag("   ") is None


# ---------------------------------------------------------------------------
# Bare _ROUND_RE path tightened — strict_content=True only
# ---------------------------------------------------------------------------


class TestBareRoundRePath:
    def test_junk_number_word_returns_none_strict(self) -> None:
        """'30 no.' must return None under strict_content — no mm context, no legend."""
        assert parse_tag("30 no.", strict_content=True) is None

    def test_bare_service_no_context_returns_none_strict(self) -> None:
        """Bare number + abbreviation without mm/∅ or legend → None under strict_content."""
        assert parse_tag("150 SA", strict_content=True) is None

    def test_bare_service_with_mm_context_parses(self) -> None:
        """Bare number with mm suffix in text provides context → parses."""
        result = parse_tag("100mm SA")
        assert _matches_round(result, "SA", 100)

    def test_bare_service_with_diameter_glyph_no_space_parses(self) -> None:
        """∅ glyph directly adjacent to digits and service token parses via glyph path.

        D3c-A: '100∅SVP AT HL DROPS TB' was previously None; now parses as SVP/100.
        """
        result = parse_tag("100∅SVP AT HL DROPS TB", strict_content=True)
        assert _matches_round(result, "SVP", 100)

    def test_bare_service_with_unicode_o_diameter_parses(self) -> None:
        """Ø (U+00D8) before a space then digits provides context."""
        result = parse_tag("Ø150 SA")
        assert _matches_round(result, "SA", 150)


# ---------------------------------------------------------------------------
# Real tags must still parse (must not regress)
# ---------------------------------------------------------------------------


class TestRealTagsUnchanged:
    def test_rect_da_parses(self) -> None:
        """700x300 DA is a real rect tag — must keep."""
        result = parse_tag("700x300 DA")
        assert _matches_rect(result, "DA", 700, 300)

    def test_round_mm_sa_parses(self) -> None:
        """100mm SA is a real round+mm tag — must keep."""
        result = parse_tag("100mm SA")
        assert _matches_round(result, "SA", 100)

    def test_round_mm_vac_parses(self) -> None:
        result = parse_tag("Ø76 mm VAC")
        assert _matches_round(result, "VAC", 76)

    def test_round_mm_rect_with_mm_units_parses(self) -> None:
        """Rect form with mm units (M-560103 style)."""
        result = parse_tag("100mmx50mm SERVICE")
        assert _matches_rect(result, "SERVICE", 100, 50)


# ---------------------------------------------------------------------------
# legend_abbreviations rescue path (strict_content=True)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "legend_abbrevs, expected_none",
    [
        (frozenset(), True),  # empty legend → junk remains None
        (frozenset({"LONDON"}), False),  # legend confirms LONDON → parses
    ],
    ids=["empty-legend", "legend-present"],
)
def test_legend_rescue_parametrized(
    legend_abbrevs: frozenset[str],
    expected_none: bool,
) -> None:
    """'5291 LONDON' is None under strict_content without legend; parses when legend confirms.

    Note: 5291 is only 4 digits (not 6+), so it passes the long-digit-run gate and reaches
    the bare path, where the legend rescue applies.
    """
    result = parse_tag("5291 LONDON", legend_abbreviations=legend_abbrevs, strict_content=True)
    if expected_none:
        assert result is None
    else:
        assert result is not None
        assert result.service == "LONDON"
        assert result.size.diameter == 5291


def test_legend_rescue_does_not_affect_prose_gate() -> None:
    """A legend cannot rescue true contact prose (email contains @)."""
    result = parse_tag(
        "tel 020 7631 5291 london@ramboll.co.uk",
        legend_abbreviations=frozenset({"LONDON"}),
        strict_content=True,
    )
    assert result is None


def test_legend_rescue_does_not_affect_long_digit_run() -> None:
    """A 6-digit run is rejected by the content gate before legend rescue."""
    result = parse_tag("123456 SA", legend_abbreviations=frozenset({"SA"}), strict_content=True)
    assert result is None


def test_legend_none_default_preserves_existing_behaviour() -> None:
    """Calling parse_tag without legend_abbreviations is byte-identical to legend=None."""
    assert parse_tag("100mm SA") == parse_tag("100mm SA", legend_abbreviations=None)
    assert parse_tag("700x300 DA") == parse_tag("700x300 DA", legend_abbreviations=None)


# ---------------------------------------------------------------------------
# Regression tests — Round 2 (issue #785 D2)
# ---------------------------------------------------------------------------


class TestFFFDDiameterMarker:
    """U+FFFD replacement glyph is the garbled form of Ø from non-UTF-8 DWGs.

    It must be recognised as a diameter-context token so that bare tags like
    '⁤150 SA' (where ⁤ = U+FFFD) are accepted under strict_content.
    Regression guard for the BLOCKER identified in D2 deep review.
    """

    def test_fffd_bare_tag_default_parses(self) -> None:
        """parse_tag('�150 SA') default (strict_content=False) → parses."""
        result = parse_tag("�150 SA")
        assert _matches_round(result, "SA", 150)

    def test_fffd_bare_tag_strict_parses(self) -> None:
        """parse_tag('�150 SA', strict_content=True) → parses (FFFD is diameter context)."""
        result = parse_tag("�150 SA", strict_content=True)
        assert _matches_round(result, "SA", 150)


class TestStrictContentScoping:
    """Prove that default (strict_content=False) is byte-identical to pre-D1 behaviour.

    The prose gate and bare-path tightening must NOT activate without strict_content=True.
    Any caller NOT explicitly listed in the takeoff sites must remain on the default path.
    """

    def test_five_digit_word_default_parses(self) -> None:
        """'5291 LONDON' default → parses (proves default path not tightened)."""
        result = parse_tag("5291 LONDON")
        assert result is not None
        assert result.service == "LONDON"
        assert result.size.diameter == 5291

    def test_five_digit_word_strict_none(self) -> None:
        """'5291 LONDON' strict_content=True → None (no diameter context, no legend)."""
        assert parse_tag("5291 LONDON", strict_content=True) is None

    def test_email_default_not_gated(self) -> None:
        """Contact prose with email is NOT gated when strict_content=False.

        Without the gate the _ROUND_RE still matches '5291 london' (before the @),
        so the result is a TagObservation (not None).
        """
        result = parse_tag("tel 020 7631 5291 london@ramboll.co.uk")
        # Without strict_content the prose gate is off; _ROUND_RE matches 5291 + london.
        assert result is not None

    def test_email_strict_none(self) -> None:
        """Contact prose with email → None under strict_content=True."""
        assert parse_tag("tel 020 7631 5291 london@ramboll.co.uk", strict_content=True) is None


# ---------------------------------------------------------------------------
# D3c Part A — glyph-adjacency path (_ROUND_GLYPH_RE)
# ---------------------------------------------------------------------------


class TestGlyphAdjacency:
    """Single-entity tags where a diameter glyph sits directly between the size digit
    and the service token (no space required).  D3a's reassembly only fixed *fragmented*
    tags; these are single-entity strings that previously returned None."""

    def test_fffd_glyph_adjacent_cdp(self) -> None:
        """'15�CDP AT HL (TYP.)' → service CDP, diameter 15."""
        result = parse_tag("15�CDP AT HL (TYP.)", strict_content=True)
        assert _matches_round(result, "CDP", 15)

    def test_unicode_o_glyph_svp(self) -> None:
        """'100∅SVP AT HL DROPS TB' → service SVP, diameter 100."""
        result = parse_tag("100∅SVP AT HL DROPS TB", strict_content=True)
        assert _matches_round(result, "SVP", 100)

    def test_fffd_glyph_with_space_cdp(self) -> None:
        """'20� CDP AT HL CONNECTS TO SVP' → service CDP, diameter 20 (first token after glyph)."""
        result = parse_tag("20� CDP AT HL CONNECTS TO SVP", strict_content=True)
        assert _matches_round(result, "CDP", 20)

    def test_noise_service_with_legend_returns_none(self) -> None:
        """Glyph-adjacent tag whose service is not in the supplied legend → None.

        'DEEP' is not a known service; with a non-empty legend gate it should be rejected.
        """
        result = parse_tag(
            "15∅ DEEP SEAL 'P' TRAP",
            legend_abbreviations=frozenset({"CDP", "SVP", "SA"}),
            strict_content=True,
        )
        assert result is None

    def test_noise_service_empty_legend_returns_none(self) -> None:
        """D3c Part B: glyph-adjacent tag rejected when legend is empty AND token not in vocab.

        Drainage drawings have an empty legend; '40∅ DEEP SEAL' must return None because
        'DEEP' is absent from _STANDARD_SERVICE_CODES.  Previously the empty-legend branch
        accepted any token that passed _valid_service (the live hole this fix closes).
        """
        result = parse_tag(
            "40∅ DEEP SEAL 'P' TRAP",
            legend_abbreviations=frozenset(),  # empty legend (drainage)
            strict_content=True,
        )
        assert result is None

    def test_noise_service_none_legend_returns_none(self) -> None:
        """Same as above but with legend_abbreviations=None (no legend at all)."""
        result = parse_tag(
            "40∅ DEEP SEAL 'P' TRAP",
            legend_abbreviations=None,
            strict_content=True,
        )
        assert result is None

    def test_vocab_service_empty_legend_parses(self) -> None:
        """D3c Part B: standard-vocab service accepted when legend is empty.

        '15∅CDP AT HL' → CDP ∈ _STANDARD_SERVICE_CODES → parses as CDP/15.
        """
        result = parse_tag(
            "15�CDP AT HL",
            legend_abbreviations=frozenset(),
            strict_content=True,
        )
        assert _matches_round(result, "CDP", 15)

    def test_vocab_svp_empty_legend_parses(self) -> None:
        """'100∅SVP' with empty legend → SVP ∈ vocab → parses."""
        result = parse_tag(
            "100∅SVP",
            legend_abbreviations=frozenset(),
            strict_content=True,
        )
        assert _matches_round(result, "SVP", 100)

    def test_noise_service_without_legend_returns_none_via_valid_service(self) -> None:
        """Without a legend, _valid_service still rejects unit/function-word noise tokens.

        'AT' is in _NON_SERVICE_WORDS → None even without a legend.
        """
        result = parse_tag("15∅AT HL", strict_content=True)
        assert result is None

    # --- regression: existing match paths must be unaffected ---

    def test_rect_tag_unaffected(self) -> None:
        """700x300 DA must still parse as rect (glyph path must not shadow rect path)."""
        result = parse_tag("700x300 DA")
        assert _matches_rect(result, "DA", 700, 300)

    def test_round_mm_tag_unaffected(self) -> None:
        """100mm SA must still parse via _ROUND_MM_RE (glyph path must not shadow mm path)."""
        result = parse_tag("100mm SA")
        assert _matches_round(result, "SA", 100)

    def test_o_mm_vac_unaffected(self) -> None:
        """Ø76 mm VAC must still parse via _ROUND_MM_RE."""
        result = parse_tag("Ø76 mm VAC")
        assert _matches_round(result, "VAC", 76)

    def test_5291_london_strict_none_unaffected(self) -> None:
        """'5291 LONDON' strict without legend must remain None (no glyph, no context)."""
        assert parse_tag("5291 LONDON", strict_content=True) is None
