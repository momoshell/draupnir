"""Unit tests for semantic layer-role classification (pure, no DB)."""

from __future__ import annotations

import pytest

from app.interpretation.layer_roles import (
    ROLE_BACKGROUND,
    ROLE_FOREGROUND,
    ROLE_SERVICES,
    ROLE_UNKNOWN,
    classify_layer_role,
)


def test_classify_light_pen_is_background() -> None:
    assert classify_layer_role("pen-eaeaea-w0.51").role == ROLE_BACKGROUND
    assert classify_layer_role("pen-aaaaaa-w0.99").role == ROLE_BACKGROUND


def test_classify_dark_pen_is_foreground() -> None:
    assert classify_layer_role("pen-000000-w0.71").role == ROLE_FOREGROUND


def test_classify_saturated_pen_is_services() -> None:
    assert classify_layer_role("pen-00ff00-w1.42").role == ROLE_SERVICES  # green
    assert classify_layer_role("pen-aaffff-w1.42").role == ROLE_SERVICES  # cyan
    assert classify_layer_role("pen-009dff-w0").role == ROLE_SERVICES  # blue


def test_classify_non_pen_layer_is_unknown() -> None:
    assert classify_layer_role("default").role == ROLE_UNKNOWN
    assert classify_layer_role("A-WALL").role == ROLE_UNKNOWN
    assert classify_layer_role(None).role == ROLE_UNKNOWN


def test_classify_reports_deterministic_basis() -> None:
    role = classify_layer_role("pen-000000-w0.71")
    assert "lightness" in role.basis
    assert "rule v" in role.basis


# ---------------------------------------------------------------------------
# DWG semantic-name keyword rule tests (rule v2)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "layer_name",
    ["Pipes", "Drop", "Rise", "Pipe Fittings"],
)
def test_dwg_services_names(layer_name: str) -> None:
    assert classify_layer_role(layer_name).role == ROLE_SERVICES


@pytest.mark.parametrize(
    "layer_name",
    ["Furniture", "Space Tags", "Data Devices", "Color Fill"],
)
def test_dwg_background_names(layer_name: str) -> None:
    assert classify_layer_role(layer_name).role == ROLE_BACKGROUND


@pytest.mark.parametrize(
    "layer_name",
    ["Pipe Tags", "Center Line"],
)
def test_dwg_foreground_names(layer_name: str) -> None:
    assert classify_layer_role(layer_name).role == ROLE_FOREGROUND


def test_space_tags_is_background_not_foreground() -> None:
    """Collision guard: "Space Tags" must land background, not foreground via "tag"."""
    result = classify_layer_role("Space Tags")
    assert result.role == ROLE_BACKGROUND, (
        f"Space Tags should be background (space rule wins over tag rule), got {result.role!r}"
    )


def test_pipe_tags_is_foreground_not_services() -> None:
    """Collision guard: "Pipe Tags" must land foreground, not services via "pipe"."""
    result = classify_layer_role("Pipe Tags")
    assert result.role == ROLE_FOREGROUND, (
        f"Pipe Tags should be foreground (tag rule wins over pipe rule), got {result.role!r}"
    )


@pytest.mark.parametrize(
    "layer_name",
    ["A060G5", "A060T", "A700", "Z000", "Z010T", "Z030G", "Z2020T"],
)
def test_opaque_codes_are_unknown(layer_name: str) -> None:
    assert classify_layer_role(layer_name).role == ROLE_UNKNOWN


def test_dwg_name_basis_contains_rule_v2() -> None:
    result = classify_layer_role("Pipes")
    assert "rule v2" in result.basis, f"Expected 'rule v2' in basis, got: {result.basis!r}"


def test_pen_path_basis_now_reads_rule_v2() -> None:
    """Pen-path basis must reflect the bumped RULE_VERSION."""
    result = classify_layer_role("pen-000000-w0.71")
    assert "rule v2" in result.basis, f"Expected 'rule v2' in pen basis, got: {result.basis!r}"


@pytest.mark.parametrize(
    "layer_name",
    ["enterprise", "A-WALL", "prevent access", "Inventory"],
)
def test_false_positive_guard(layer_name: str) -> None:
    """Ambiguous substrings that are NOT the real token must not match.

    "prevent"/"Inventory" contain "vent" but are not a vent layer — word-boundary
    matching keeps them unknown.
    """
    assert classify_layer_role(layer_name).role == ROLE_UNKNOWN


@pytest.mark.parametrize("layer_name", ["Vent Pipe", "Soil Vent", "SVP Vent"])
def test_vent_layers_are_services(layer_name: str) -> None:
    """A genuine vent layer (whole-token "vent") classifies as services."""
    assert classify_layer_role(layer_name).role == ROLE_SERVICES


# ---------------------------------------------------------------------------
# Full 17-name mapping table
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "layer_name,expected_role",
    [
        # services
        ("Pipes", ROLE_SERVICES),
        ("Drop", ROLE_SERVICES),
        ("Rise", ROLE_SERVICES),
        ("Pipe Fittings", ROLE_SERVICES),
        # foreground
        ("Pipe Tags", ROLE_FOREGROUND),
        ("Center Line", ROLE_FOREGROUND),
        # background
        ("Furniture", ROLE_BACKGROUND),
        ("Space Tags", ROLE_BACKGROUND),
        ("Data Devices", ROLE_BACKGROUND),
        ("Color Fill", ROLE_BACKGROUND),
        # unknown (opaque codes)
        ("A060G5", ROLE_UNKNOWN),
        ("A060T", ROLE_UNKNOWN),
        ("A700", ROLE_UNKNOWN),
        ("Z000", ROLE_UNKNOWN),
        ("Z010T", ROLE_UNKNOWN),
        ("Z030G", ROLE_UNKNOWN),
        ("Z2020T", ROLE_UNKNOWN),
    ],
)
def test_full_17_name_mapping(layer_name: str, expected_role: str) -> None:
    assert classify_layer_role(layer_name).role == expected_role
