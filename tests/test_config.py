"""Tests for application settings validation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.core.config import Settings, settings


@pytest.mark.parametrize(
    "field",
    [
        "pymupdf_max_drawings_per_page",
        "pymupdf_max_total_drawings",
        "pymupdf_max_entities",
    ],
)
def test_pymupdf_complexity_caps_must_be_positive(field: str) -> None:
    with pytest.raises(ValidationError, match=f"{field} must be positive"):
        Settings.model_validate({field: 0})


def test_pymupdf_complexity_caps_have_real_drawing_sized_defaults() -> None:
    s = Settings()

    # Defaults must comfortably clear a dense real A0 sheet (~16.5k paths/page).
    assert s.pymupdf_max_drawings_per_page >= 16_500
    assert s.pymupdf_max_total_drawings >= s.pymupdf_max_drawings_per_page
    assert s.pymupdf_max_entities >= 16_500


# --- adapter_timeout_seconds ---


def test_adapter_timeout_seconds_default() -> None:
    s = Settings()
    assert s.adapter_timeout_seconds == 300.0


@pytest.mark.parametrize("bad", [0, -1, -0.001])
def test_adapter_timeout_seconds_rejects_non_positive(bad: float) -> None:
    with pytest.raises(ValidationError, match="adapter_timeout_seconds must be positive"):
        Settings.model_validate({"adapter_timeout_seconds": bad})


def test_adapter_timeout_seconds_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADAPTER_TIMEOUT_SECONDS", "600")
    s = Settings()
    assert s.adapter_timeout_seconds == 600.0


def test_adapter_timeout_seconds_wired_into_live_settings() -> None:
    # The module-level singleton must carry the correct default so the worker
    # picks it up without any env override.
    assert settings.adapter_timeout_seconds == 300.0
