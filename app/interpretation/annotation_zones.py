"""Annotation-zone exclusion for PDF room labels (#828 PR-2).

PDF has no sheet-scoping mechanism (``tag_sheet_membership`` is a DWG-only no-op on
PDF), so every room-name-ish text token on the sheet — including legend rows, a
drainage/service key block, grid-bubble labels near the border, and note headers —
becomes a candidate "room" (P-520001 PDF: 209 rooms vs ~59 real). This module masks
those non-room annotation regions so their text never reaches label-derived room
identification.

Pure, no DB. Mirrors the anchor -> padded-region -> point-in-region primitive already
used for PDF legend discovery in ``service_takeoff_loaders`` (do NOT refactor that
module — #752 covers the full extraction). Conservative by design: only specific
anchors (LEGEND / KEY / NOTES / title-block-ish headers) and a thin outer border band
are excluded; when uncertain, a label is kept. A zone must never swallow a genuine
room label.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

from app.interpretation.rooms import RoomLabel

# ---------------------------------------------------------------------------
# Zone reasons
# ---------------------------------------------------------------------------

ZONE_REASON_LEGEND = "legend"
ZONE_REASON_KEY = "key"
ZONE_REASON_NOTES = "notes"
ZONE_REASON_TITLE_BLOCK = "title_block"
ZONE_REASON_GRID_MARGIN = "grid_margin"

# ---------------------------------------------------------------------------
# Anchor patterns (specific — a genuine room is never titled these).
# ---------------------------------------------------------------------------

_LEGEND_ANCHOR_RE: re.Pattern[str] = re.compile(r"(?i)\blegend\b")
_KEY_ANCHOR_RE: re.Pattern[str] = re.compile(r"(?i)\b(?:(?:drainage|service)\s+)?key\b")
_NOTES_ANCHOR_RE: re.Pattern[str] = re.compile(r"(?i)\bnotes?\b")
_TITLE_BLOCK_ANCHOR_RE: re.Pattern[str] = re.compile(
    r"(?i)\b(?:drawing\s+title|drawn\s+by|checked\s+by|scale\s*:|revision|project\s+no)\b"
)
# A generic anchor exclusion so "key plan" (a callout, not the drainage/service key block)
# does not get swallowed as a KEY zone anchor.
_KEY_ANCHOR_EXCLUDE_RE: re.Pattern[str] = re.compile(r"(?i)\bkey\s*plan\b")

# ---------------------------------------------------------------------------
# Region pads — provisional, calibrated to P-520001 (single building); revisit if a
# second PDF building shows different legend/key block proportions.
# ---------------------------------------------------------------------------

_LEGEND_PAD_LEFT: float = 15.0
_LEGEND_PAD_BELOW: float = 5.0
_LEGEND_PAD_RIGHT: float = 320.0
_LEGEND_PAD_ABOVE: float = 420.0

_KEY_PAD_LEFT: float = 15.0
_KEY_PAD_BELOW: float = 5.0
_KEY_PAD_RIGHT: float = 320.0
_KEY_PAD_ABOVE: float = 420.0

_NOTES_PAD_LEFT: float = 15.0
_NOTES_PAD_BELOW: float = 5.0
_NOTES_PAD_RIGHT: float = 260.0
_NOTES_PAD_ABOVE: float = 260.0

_TITLE_BLOCK_PAD_LEFT: float = 10.0
_TITLE_BLOCK_PAD_BELOW: float = 5.0
_TITLE_BLOCK_PAD_RIGHT: float = 200.0
_TITLE_BLOCK_PAD_ABOVE: float = 80.0

# Thin outer ring of the sheet extent where grid/column bubbles live — a fraction of
# the sheet width/height on each side. Kept small and conservative: a genuine room
# label just inside the interior must never fall in this band.
GRID_MARGIN_FRACTION: float = 0.05


@dataclass(frozen=True, slots=True)
class Zone:
    """An axis-aligned exclusion region and why it was discovered."""

    bbox: tuple[float, float, float, float]  # (min_x, min_y, max_x, max_y)
    reason: str


def _pad_anchor(
    point: tuple[float, float],
    *,
    pad_left: float,
    pad_below: float,
    pad_right: float,
    pad_above: float,
) -> tuple[float, float, float, float]:
    x, y = point
    return (x - pad_left, y - pad_below, x + pad_right, y + pad_above)


def discover_zones(
    labels: Sequence[RoomLabel],
    sheet_extent: tuple[float, float, float, float] | None,
) -> list[Zone]:
    """Discover non-room annotation zones from anchor labels + the sheet border.

    ``sheet_extent`` is ``(min_x, min_y, max_x, max_y)`` in drawing units — pass ``None``
    to skip the grid-margin band (e.g. when the extent cannot be derived). Anchor zones are
    padded regions around specific anchor text (LEGEND / KEY / NOTES / title-block headers);
    the grid-margin zone is a thin outer ring of the sheet extent.
    """
    zones: list[Zone] = []
    for label in labels:
        text = label.text.strip()
        if not text:
            continue
        if _KEY_ANCHOR_EXCLUDE_RE.search(text):
            continue
        if _LEGEND_ANCHOR_RE.search(text):
            zones.append(
                Zone(
                    bbox=_pad_anchor(
                        label.point,
                        pad_left=_LEGEND_PAD_LEFT,
                        pad_below=_LEGEND_PAD_BELOW,
                        pad_right=_LEGEND_PAD_RIGHT,
                        pad_above=_LEGEND_PAD_ABOVE,
                    ),
                    reason=ZONE_REASON_LEGEND,
                )
            )
            continue
        if _KEY_ANCHOR_RE.search(text):
            zones.append(
                Zone(
                    bbox=_pad_anchor(
                        label.point,
                        pad_left=_KEY_PAD_LEFT,
                        pad_below=_KEY_PAD_BELOW,
                        pad_right=_KEY_PAD_RIGHT,
                        pad_above=_KEY_PAD_ABOVE,
                    ),
                    reason=ZONE_REASON_KEY,
                )
            )
            continue
        if _NOTES_ANCHOR_RE.search(text):
            zones.append(
                Zone(
                    bbox=_pad_anchor(
                        label.point,
                        pad_left=_NOTES_PAD_LEFT,
                        pad_below=_NOTES_PAD_BELOW,
                        pad_right=_NOTES_PAD_RIGHT,
                        pad_above=_NOTES_PAD_ABOVE,
                    ),
                    reason=ZONE_REASON_NOTES,
                )
            )
            continue
        if _TITLE_BLOCK_ANCHOR_RE.search(text):
            zones.append(
                Zone(
                    bbox=_pad_anchor(
                        label.point,
                        pad_left=_TITLE_BLOCK_PAD_LEFT,
                        pad_below=_TITLE_BLOCK_PAD_BELOW,
                        pad_right=_TITLE_BLOCK_PAD_RIGHT,
                        pad_above=_TITLE_BLOCK_PAD_ABOVE,
                    ),
                    reason=ZONE_REASON_TITLE_BLOCK,
                )
            )
            continue

    if sheet_extent is not None:
        zones.extend(_grid_margin_zones(sheet_extent))

    return zones


def _grid_margin_zones(sheet_extent: tuple[float, float, float, float]) -> list[Zone]:
    """Thin outer-ring band(s) of the sheet extent where grid/column bubbles live."""
    min_x, min_y, max_x, max_y = sheet_extent
    width = max_x - min_x
    height = max_y - min_y
    if width <= 0 or height <= 0:
        return []
    margin_x = width * GRID_MARGIN_FRACTION
    margin_y = height * GRID_MARGIN_FRACTION
    return [
        # left strip
        Zone((min_x, min_y, min_x + margin_x, max_y), ZONE_REASON_GRID_MARGIN),
        # right strip
        Zone((max_x - margin_x, min_y, max_x, max_y), ZONE_REASON_GRID_MARGIN),
        # bottom strip
        Zone((min_x, min_y, max_x, min_y + margin_y), ZONE_REASON_GRID_MARGIN),
        # top strip
        Zone((min_x, max_y - margin_y, max_x, max_y), ZONE_REASON_GRID_MARGIN),
    ]


@dataclass(frozen=True, slots=True)
class ExcludedLabel:
    """A label excluded by zone filtering, with the reason it was excluded (#828)."""

    label: RoomLabel
    reason: str


def _in_zone(point: tuple[float, float], zone: Zone) -> bool:
    x, y = point
    min_x, min_y, max_x, max_y = zone.bbox
    return min_x <= x <= max_x and min_y <= y <= max_y


def filter_labels_by_zones(
    labels: Sequence[RoomLabel], zones: Sequence[Zone]
) -> tuple[list[RoomLabel], list[ExcludedLabel]]:
    """Split ``labels`` into those kept (outside all zones) and those excluded (in a zone).

    Excluded labels are returned (with the zone reason), not dropped — callers may surface
    them for diagnostics/overlay. The first zone (in discovery order) whose bbox contains the
    label's point determines the reported reason.
    """
    kept: list[RoomLabel] = []
    excluded: list[ExcludedLabel] = []
    for label in labels:
        matched_zone = next((zone for zone in zones if _in_zone(label.point, zone)), None)
        if matched_zone is None:
            kept.append(label)
        else:
            excluded.append(ExcludedLabel(label=label, reason=matched_zone.reason))
    return kept, excluded
