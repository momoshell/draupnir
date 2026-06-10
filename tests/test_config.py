"""Tests for application settings validation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.core.config import Settings


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
    settings = Settings()

    # Defaults must comfortably clear a dense real A0 sheet (~16.5k paths/page).
    assert settings.pymupdf_max_drawings_per_page >= 16_500
    assert settings.pymupdf_max_total_drawings >= settings.pymupdf_max_drawings_per_page
    assert settings.pymupdf_max_entities >= 16_500
