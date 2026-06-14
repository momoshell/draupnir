"""Unit tests for semantic layer-role classification (pure, no DB)."""

from __future__ import annotations

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
