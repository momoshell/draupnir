"""Tests for app/interpretation/run_tags.py (pure, no DB)."""

from __future__ import annotations

import pytest

from app.interpretation.run_tags import (
    BASIS_TAG_TEXT,
    TagObservation,
    parse_tag,
)

# ---------------------------------------------------------------------------
# Round + mm
# ---------------------------------------------------------------------------


def test_round_mm_vac() -> None:
    result = parse_tag("Ø54 mm VAC")
    assert result is not None
    assert result.service == "VAC"
    assert result.size.kind == "round"
    assert result.size.diameter == 54
    assert result.size.width is None
    assert result.size.height is None
    assert result.basis == BASIS_TAG_TEXT


def test_round_mm_agss() -> None:
    result = parse_tag("Ø42 mm AGSS")
    assert result is not None
    assert result.service == "AGSS"
    assert result.size.diameter == 42


# ---------------------------------------------------------------------------
# Ø-independence (parametrized)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "Ø42 mm OXY",
        "�42 mm OXY",  # U+FFFD replacement character
        b"\xef\xbf\xbd".decode() + "42 mm OXY",  # raw bytes form of U+FFFD
        "42 mm OXY",  # no prefix at all
    ],
)
def test_round_mm_oxy_independence(text: str) -> None:
    result = parse_tag(text)
    assert result is not None, f"Expected observation for {text!r}"
    assert result.size.diameter == 42
    assert result.service == "OXY"


# ---------------------------------------------------------------------------
# Rectangular duct
# ---------------------------------------------------------------------------


def test_rect_da() -> None:
    result = parse_tag("700x300 DA")
    assert result is not None
    assert result.size.kind == "rect"
    assert result.size.width == 700
    assert result.size.height == 300
    assert result.service == "DA"
    assert result.size.diameter is None


# ---------------------------------------------------------------------------
# Round, no mm
# ---------------------------------------------------------------------------


def test_round_no_mm_sa() -> None:
    result = parse_tag("Ø150 SA")
    assert result is not None
    assert result.size.kind == "round"
    assert result.size.diameter == 150
    assert result.service == "SA"


# ---------------------------------------------------------------------------
# Junk / None cases
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "NOTE: SEE DETAIL",
        "",
        "   ",
        "Ø mm",
        "EA",
    ],
)
def test_junk_returns_none(text: str) -> None:
    assert parse_tag(text) is None, f"Expected None for {text!r}"


# ---------------------------------------------------------------------------
# Field-type and normalization assertions
# ---------------------------------------------------------------------------


def test_service_upper_stripped() -> None:
    result = parse_tag("Ø76 mm vac")
    assert result is not None
    assert result.service == "VAC"


def test_diameter_is_int() -> None:
    result = parse_tag("Ø54 mm VAC")
    assert result is not None
    assert isinstance(result.size.diameter, int)


def test_rect_widths_are_int() -> None:
    result = parse_tag("700x300 DA")
    assert result is not None
    assert isinstance(result.size.width, int)
    assert isinstance(result.size.height, int)


def test_raw_text_preserved() -> None:
    original = "  Ø54 mm VAC  "
    result = parse_tag(original)
    assert result is not None
    assert result.raw_text == original


def test_raw_size_round() -> None:
    result = parse_tag("Ø54 mm VAC")
    assert result is not None
    assert result.size.raw == "54"


def test_raw_size_rect() -> None:
    result = parse_tag("700x300 DA")
    assert result is not None
    assert result.size.raw == "700x300"


def test_basis_is_tag_text() -> None:
    result = parse_tag("Ø54 mm VAC")
    assert result is not None
    assert result.basis == BASIS_TAG_TEXT


def test_never_raises_on_garbage() -> None:
    # parse_tag must not raise for arbitrary strings
    for junk in ["\x00\xff", "123", "x" * 1000, None]:
        try:
            parse_tag(junk)  # type: ignore[arg-type]
        except Exception as exc:
            pytest.fail(f"parse_tag raised {exc!r} on {junk!r}")


def test_returns_tag_observation_type() -> None:
    result = parse_tag("Ø54 mm VAC")
    assert isinstance(result, TagObservation)


@pytest.mark.parametrize(
    "text",
    [
        "Ø42 mm",  # size, no service -> must NOT fabricate service "MM"
        "42 mm",  # same, garbled-O stripped
        "MINIMUM OF 200 MM",  # a note, not a tag
        "100 X 50",  # rect dims, no service -> must NOT fabricate "X"
        "200 CM",  # unit token as service
    ],
)
def test_size_without_real_service_returns_none(text: str) -> None:
    # legend-is-ground-truth: a unit/separator token (MM/CM/M/X) is never a service.
    assert parse_tag(text) is None


@pytest.mark.parametrize(
    "text",
    [
        "100MM ABOVE THE DESK HEIGHT.",  # note fragment -> must NOT fabricate "ABOVE"
        "10MM AND NOT EXCEED",  # note fragment -> must NOT fabricate "AND"
        "200 x 100 IN PLANT ROOM",  # WxH in prose -> must NOT fabricate "IN"
        "50 mm FROM WALL",  # round+mm in prose -> must NOT fabricate "FROM"
    ],
)
def test_connective_word_is_not_a_service(text: str) -> None:
    # A real service code is never an English function word; note prose carrying a size must
    # not fabricate a service (never-guess). See _NON_SERVICE_WORDS.
    assert parse_tag(text) is None


@pytest.mark.parametrize(
    ("text", "service"),
    [
        ("200 x 100 LV DIST", "LV DIST"),
        ("150 x 75 SP & LTG", "SP & LTG"),
        ("650x350 EA", "EA"),
        ("Ø42 mm AGSS", "AGSS"),
        ("100 x 100 ESSENTIAL", "ESSENTIAL"),
    ],
)
def test_real_services_still_parse(text: str, service: str) -> None:
    # Guard: the connective stopwords must not reject genuine service codes.
    result = parse_tag(text)
    assert result is not None
    assert result.service == service


# ---------------------------------------------------------------------------
# Multi-word rect service labels (#680)
# ---------------------------------------------------------------------------


def test_rect_lv_dist() -> None:
    result = parse_tag("300 x 125 LV DIST")
    assert result is not None
    assert result.size.kind == "rect"
    assert result.size.width == 300
    assert result.size.height == 125
    assert result.service == "LV DIST"


def test_rect_fa_dectn_alm() -> None:
    result = parse_tag("150 x 105 FA DECTN ALM")
    assert result is not None
    assert result.service == "FA DECTN ALM"
    assert result.size.width == 150
    assert result.size.height == 105


def test_rect_data_and_comms() -> None:
    result = parse_tag("200 x 100 DATA & COMMS")
    assert result is not None
    assert result.service == "DATA & COMMS"


def test_rect_sp_single() -> None:
    result = parse_tag("150 x 75 SP")
    assert result is not None
    assert result.service == "SP"


def test_rect_da_single_regression() -> None:
    result = parse_tag("700x300 DA")
    assert result is not None
    assert result.size.kind == "rect"
    assert result.size.width == 700
    assert result.size.height == 300
    assert result.service == "DA"


def test_rect_prose_stop_at_from() -> None:
    # Locational note: TYPE is extracted only up to the first function word (FROM).
    result = parse_tag("650x350 LV DIST FROM PIT TO HL")
    assert result is not None
    assert result.service == "LV DIST"


def test_rect_prose_rejection_above() -> None:
    # First token after WxH is a function word — not a real tag.
    result = parse_tag("200 x 100 ABOVE THE DESK")
    assert result is None


def test_round_vac_regression() -> None:
    result = parse_tag("Ø54 mm VAC")
    assert result is not None
    assert result.size.kind == "round"
    assert result.size.diameter == 54
    assert result.service == "VAC"


def test_round_sa_regression() -> None:
    result = parse_tag("Ø150 SA")
    assert result is not None
    assert result.size.kind == "round"
    assert result.size.diameter == 150
    assert result.service == "SA"


def test_round_mm_bare_returns_none() -> None:
    assert parse_tag("42 mm") is None


# ---------------------------------------------------------------------------
# Adversarial & connector edge cases (#680 code-review bugs)
# ---------------------------------------------------------------------------


def test_rect_trailing_ampersand_stripped() -> None:
    # "LV &" → dangling & stripped → service is "LV", not "LV &"
    result = parse_tag("300 x 125 LV &")
    assert result is not None
    assert result.service == "LV"


def test_rect_leading_ampersand_returns_none() -> None:
    # "& COMMS" — no valid leading service token before & → reject
    result = parse_tag("300 x 125 & COMMS")
    assert result is None


def test_rect_double_ampersand_stops_run() -> None:
    # "SP & & COMMS" — second & is a leading & after the first consumed it → stop at second &
    result = parse_tag("300 x 125 SP & & COMMS")
    assert result is not None
    assert result.service == "SP"


# ---------------------------------------------------------------------------
# M-560103 real-label regression tests (#769)
# ---------------------------------------------------------------------------


def test_rect_mm_interspersed_refrigerant_tray() -> None:
    # "100 mmx50 mm REFRIGERANT TRAY" — old _RECT_RE missed interspersed mm units;
    # new regex accepts optional mm after each dimension.
    result = parse_tag("100 mmx50 mm REFRIGERANT TRAY")
    assert result is not None
    assert result.size.kind == "rect"
    assert result.size.width == 100
    assert result.size.height == 50
    assert result.service == "REFRIGERANT TRAY"


def test_rect_mm_interspersed_reversed_dims() -> None:
    # "50 mmx100 mm REFRIGERANT TRAY" — dims reversed variant.
    result = parse_tag("50 mmx100 mm REFRIGERANT TRAY")
    assert result is not None
    assert result.size.kind == "rect"
    assert result.size.width == 50
    assert result.size.height == 100
    assert result.service == "REFRIGERANT TRAY"


def test_bare_mm_dims_no_service_returns_none() -> None:
    # "100 mmx50 mm" — no trailing service; the MMX token is rejected as a unit.
    assert parse_tag("100 mmx50 mm") is None


def test_header_vessel_low_less_returns_none() -> None:
    # "Ø250 LOW LESS HEADER" — equipment vessel label; must not produce a service.
    assert parse_tag("Ø250 LOW LESS HEADER") is None


def test_header_vessel_low_loss_returns_none() -> None:
    # "Ø250 mm LOW LOSS HEADER" — with mm unit; still a vessel, not a pipe run.
    assert parse_tag("Ø250 mm LOW LOSS HEADER") is None


def test_round_mm_lthw_vt_f_regression() -> None:
    # "Ø150 mm LTHW_VT_F" — real pipe run; service extracted up to underscore char class boundary.
    # _ROUND_MM_RE char class is [A-Za-z/] so captures "LTHW" (stops at _); that is acceptable.
    result = parse_tag("Ø150 mm LTHW_VT_F")
    assert result is not None
    assert result.size.kind == "round"
    assert result.size.diameter == 150
    assert result.service == "LTHW"


def test_round_mm_chw_f_regression() -> None:
    # "Ø150 mm ChW-F" — real pipe run; hyphen stops [A-Za-z/] capture → "CHW" is correct.
    result = parse_tag("Ø150 mm ChW-F")
    assert result is not None
    assert result.size.kind == "round"
    assert result.size.diameter == 150
    assert result.service == "CHW"


def test_med_gas_vac_76mm_unchanged() -> None:
    # Regression guard: med-gas "Ø76 mm VAC" must be unaffected by #769 changes.
    result = parse_tag("Ø76 mm VAC")
    assert result is not None
    assert result.size.kind == "round"
    assert result.size.diameter == 76
    assert result.service == "VAC"


def test_containment_lv_dist_unchanged() -> None:
    # Regression guard: containment "300x125 LV DIST" must be unaffected.
    result = parse_tag("300x125 LV DIST")
    assert result is not None
    assert result.size.kind == "rect"
    assert result.size.width == 300
    assert result.size.height == 125
    assert result.service == "LV DIST"


def test_containment_700x300_da_unchanged() -> None:
    # Regression guard: "700x300 DA" must be unaffected.
    result = parse_tag("700x300 DA")
    assert result is not None
    assert result.size.kind == "rect"
    assert result.size.width == 700
    assert result.size.height == 300
    assert result.service == "DA"
