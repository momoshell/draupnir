"""Legend detection for vector PDFs.

Tier-3 semantic interpretation derived from (never mutating) the canonical model. A drawing's
legend is its own key: a titled column of rows, each pairing a symbol/abbreviation with a
description. This module locates that column and segments it into rows.

The hard part is that neighbouring columns (NOTES / KEY NOTES) interleave with the legend in
vertical order, so naive top-to-bottom grouping mixes them. We anchor on the legend header,
isolate the legend's x-column via an x-gap to the next column, then cluster rows by y. This is
step one of the legend-anchored device schedule; pairing label/abbreviation/icon (the symbol
dictionary) and resolving body tags are separate follow-ups.

Pure functions over text-block mappings ({"text", "bbox": {x_min,y_min,x_max,y_max}, ...}).
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from itertools import pairwise
from typing import Any

# Header tokens that introduce a legend column.
_LEGEND_HEADER_RE = re.compile(r"(?i)^\s*(legend|key|symbols?)\s*$")
# The legend column is isolated from the next column (e.g. NOTES) at the largest x-gap between
# sorted left-edges, but only when that gap is a clear outlier versus the second-largest gap.
# This keeps the legend's two sub-columns (abbreviation + description, a moderate gap apart) while
# cutting the much larger gap to the notes column; a single-/double-column legend with no such
# outlier is kept whole.
_COLUMN_GAP_OUTLIER_RATIO = 3.0
_MIN_COLUMN_GAP = 1.0
# Two blocks belong to the same row when their vertical centres are within this multiple of the
# median block height.
_ROW_GAP_RATIO = 0.6


@dataclass(frozen=True, slots=True)
class LegendRow:
    """One segmented legend row: bounding box, text spans, and their source block indices."""

    texts: tuple[str, ...]
    bbox: tuple[float, float, float, float]  # (x_min, y_min, x_max, y_max)
    block_indices: tuple[int, ...]


def _bbox_of(block: Mapping[str, Any]) -> tuple[float, float, float, float] | None:
    raw = block.get("bbox")
    if not isinstance(raw, Mapping):
        return None
    try:
        return (
            float(raw["x_min"]),
            float(raw["y_min"]),
            float(raw["x_max"]),
            float(raw["y_max"]),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _text_of(block: Mapping[str, Any]) -> str:
    value = block.get("text")
    return value.strip() if isinstance(value, str) else ""


def find_legend_anchor(
    blocks: Sequence[Mapping[str, Any]],
) -> tuple[int, tuple[float, float, float, float]] | None:
    """Return the (index, bbox) of the first legend header block, or None."""

    for index, block in enumerate(blocks):
        if _LEGEND_HEADER_RE.match(_text_of(block)):
            bbox = _bbox_of(block)
            if bbox is not None:
                return index, bbox
    return None


def segment_legend_rows(blocks: Sequence[Mapping[str, Any]]) -> list[LegendRow]:
    """Locate the legend column and segment it into rows.

    Returns an empty list when no legend header is present. Blocks in adjacent columns (e.g.
    NOTES) are excluded by detecting the x-gap to the next column.
    """

    anchor = find_legend_anchor(blocks)
    if anchor is None:
        return []
    _anchor_index, (anchor_x_min, _ay0, anchor_x_max, anchor_y_max) = anchor
    header_width = max(anchor_x_max - anchor_x_min, _MIN_COLUMN_GAP)

    # Candidate row members: blocks at/below the header, not left of the legend column.
    candidates: list[tuple[int, str, tuple[float, float, float, float]]] = []
    for index, block in enumerate(blocks):
        bbox = _bbox_of(block)
        text = _text_of(block)
        if bbox is None or not text:
            continue
        if bbox[1] < anchor_y_max:  # above the header row
            continue
        if bbox[0] < anchor_x_min - _MIN_COLUMN_GAP:  # left of the legend column
            continue
        candidates.append((index, text, bbox))

    if not candidates:
        return []

    return _cluster_rows(_isolate_column(candidates, header_width=header_width))


def _isolate_column(
    candidates: list[tuple[int, str, tuple[float, float, float, float]]],
    *,
    header_width: float,
) -> list[tuple[int, str, tuple[float, float, float, float]]]:
    """Drop the next column (e.g. NOTES) by cutting at an outlier gap in sorted left-edges.

    A genuine inter-column gap is both an outlier versus the second-largest gap (so the legend's
    own abbreviation/description sub-columns are kept) and wider than a header word (so a lone
    standalone legend with a sub-column split is not falsely cut). The header width is a
    convenient, scale-free reference for "wider than a word".
    """

    ordered = sorted(candidates, key=lambda item: item[2][0])
    x_mins = [item[2][0] for item in ordered]
    gaps = [(current - previous, previous) for previous, current in pairwise(x_mins)]
    if len(gaps) < 2:
        return ordered

    largest, second_largest = sorted((gap for gap, _ in gaps), reverse=True)[:2]
    if largest < header_width or largest <= second_largest * _COLUMN_GAP_OUTLIER_RATIO:
        return ordered  # no clear column break

    right_boundary = max(left for gap, left in gaps if gap == largest)
    return [item for item in ordered if item[2][0] <= right_boundary + _MIN_COLUMN_GAP]


def _cluster_rows(
    column: list[tuple[int, str, tuple[float, float, float, float]]],
) -> list[LegendRow]:
    """Group column blocks into rows by vertical proximity."""

    if not column:
        return []

    heights = [item[2][3] - item[2][1] for item in column if item[2][3] > item[2][1]]
    median_height = sorted(heights)[len(heights) // 2] if heights else 1.0
    row_gap = max(median_height * _ROW_GAP_RATIO, 0.1)

    by_y = sorted(column, key=lambda item: ((item[2][1] + item[2][3]) / 2, item[2][0]))
    rows: list[list[tuple[int, str, tuple[float, float, float, float]]]] = []
    current: list[tuple[int, str, tuple[float, float, float, float]]] = []
    current_center: float | None = None
    for item in by_y:
        center = (item[2][1] + item[2][3]) / 2
        if current_center is None or abs(center - current_center) <= row_gap:
            current.append(item)
            members = [(m[2][1] + m[2][3]) / 2 for m in current]
            current_center = sum(members) / len(members)
        else:
            rows.append(current)
            current = [item]
            current_center = center
    if current:
        rows.append(current)

    return [_build_row(row) for row in rows]


def _build_row(
    row: list[tuple[int, str, tuple[float, float, float, float]]],
) -> LegendRow:
    ordered = sorted(row, key=lambda item: item[2][0])
    return LegendRow(
        texts=tuple(item[1] for item in ordered),
        bbox=(
            min(item[2][0] for item in ordered),
            min(item[2][1] for item in ordered),
            max(item[2][2] for item in ordered),
            max(item[2][3] for item in ordered),
        ),
        block_indices=tuple(item[0] for item in ordered),
    )
