"""Pydantic schemas for immutable extraction profile payloads."""

import math
import uuid
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

AllowedUnitsOverride = Literal["metric", "imperial", "mm", "cm", "m", "inch", "foot", "feet"]
LayoutMode = Literal["auto", "model_space", "paper_space"]
XrefHandling = Literal["preserve", "inline", "detach"]
BlockHandling = Literal["expand", "preserve"]

class RasterCalibration(BaseModel):
    """Strict raster calibration payload."""

    model_config = ConfigDict(extra="forbid")

    scale: int | float = Field(
        ...,
        description="Positive calibration scale factor",
    )
    unit: AllowedUnitsOverride = Field(
        ...,
        description="Calibration unit",
    )

    @field_validator("scale", mode="before")
    @classmethod
    def validate_scale(cls, value: Any) -> Any:
        """Require a positive numeric scale without coercing strings/bools."""
        if (
            isinstance(value, bool)
            or not isinstance(value, int | float)
            or not math.isfinite(value)
            or value <= 0
        ):
            raise ValueError("raster_calibration.scale must be a positive number.")
        return value


class ExtractionProfileCreate(BaseModel):
    """Request payload for creating a new immutable extraction profile."""

    model_config = ConfigDict(extra="forbid")

    profile_version: Literal["v0.1"] = Field(
        default="v0.1",
        description="Extraction profile schema version",
    )
    units_override: AllowedUnitsOverride | None = Field(
        default=None,
        description="Optional extraction units override",
    )
    layout_mode: LayoutMode = Field(
        default="auto",
        description="Layout extraction mode",
    )
    xref_handling: XrefHandling = Field(
        default="preserve",
        description="External reference handling mode",
    )
    block_handling: BlockHandling = Field(
        default="expand",
        description="Block extraction handling mode",
    )
    text_extraction: bool = Field(
        default=True,
        description="Whether text extraction is enabled",
    )
    dimension_extraction: bool = Field(
        default=True,
        description="Whether dimension extraction is enabled",
    )
    pdf_page_range: str | None = Field(
        default=None,
        max_length=255,
        description="Optional PDF page range selector",
    )
    raster_calibration: RasterCalibration | None = Field(
        default=None,
        description="Optional raster calibration payload",
    )
    confidence_threshold: float = Field(
        default=0.6,
        ge=0.0,
        le=1.0,
        description="Minimum confidence threshold for extracted results",
    )

    @field_validator("pdf_page_range")
    @classmethod
    def validate_pdf_page_range(cls, value: str | None) -> str | None:
        """Reject blank or malformed PDF page range selectors."""
        if value is None:
            return None

        normalized = value.strip()
        if not normalized:
            raise ValueError("pdf_page_range must not be blank.")

        normalized_tokens: list[str] = []
        for raw_token in normalized.split(","):
            token = raw_token.strip()
            if not token:
                raise ValueError(
                    "pdf_page_range must use comma-separated positive pages or page ranges."
                )

            if "-" not in token:
                if not token.isdigit() or int(token) <= 0:
                    raise ValueError(
                        "pdf_page_range must use comma-separated positive pages or page ranges."
                    )
                normalized_tokens.append(token)
                continue

            if token.count("-") != 1:
                raise ValueError(
                    "pdf_page_range must use comma-separated positive pages or page ranges."
                )

            start_raw, end_raw = token.split("-", maxsplit=1)
            if not start_raw.isdigit() or not end_raw.isdigit():
                raise ValueError(
                    "pdf_page_range must use comma-separated positive pages or page ranges."
                )

            start = int(start_raw)
            end = int(end_raw)
            if start <= 0 or end <= 0 or start > end:
                raise ValueError(
                    "pdf_page_range must use comma-separated positive pages or page ranges."
                )

            normalized_tokens.append(f"{start}-{end}")

        return ", ".join(normalized_tokens)


class FileReprocessRequest(BaseModel):
    """Request payload for reprocessing an existing file."""

    model_config = ConfigDict(extra="forbid")

    extraction_profile_id: uuid.UUID | None = Field(
        default=None,
        description="Existing immutable extraction profile identifier",
    )
    extraction_profile: ExtractionProfileCreate | None = Field(
        default=None,
        description="New immutable extraction profile payload",
    )

    @model_validator(mode="after")
    def validate_profile_selection(self) -> "FileReprocessRequest":
        """Require exactly one extraction profile selector."""
        provided_values = (
            int(self.extraction_profile_id is not None)
            + int(self.extraction_profile is not None)
        )
        if provided_values != 1:
            raise ValueError(
                "Provide exactly one of extraction_profile_id or extraction_profile."
            )
        return self
