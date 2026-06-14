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

# A legend abbreviation is a short, all-caps token: 1-4 chars with an optional "-X" suffix
# (e.g. DC, P, S, F, L, I, SEC, ACP, NCP, INT, NC, NC-H, EB, R). Descriptions are longer or
# multi-word, so they never match.
_ABBREVIATION_RE = re.compile(r"^[A-Z][A-Z0-9]{0,3}(?:-[A-Z0-9]{1,3})?$")
# Legend-area annotations that are not device entries.
_NON_ENTRY_PREFIX_RE = re.compile(r"(?i)^\s*note\b")
# Maximum vertical gap (as a multiple of a row's height) for pairing a stand-alone abbreviation
# row with a stand-alone description row.
_PAIR_ADJACENCY_ROWS = 2.5


@dataclass(frozen=True, slots=True)
class LegendRow:
    """One segmented legend row: bounding box, text spans, and their source block indices."""

    texts: tuple[str, ...]
    bbox: tuple[float, float, float, float]  # (x_min, y_min, x_max, y_max)
    block_indices: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class SymbolEntry:
    """A legend symbol: its abbreviation, the device type it denotes, and its row bounding box."""

    abbreviation: str
    type_name: str
    bbox: tuple[float, float, float, float]


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


def build_symbol_dictionary(rows: Sequence[LegendRow]) -> dict[str, SymbolEntry]:
    """Pair each legend row's abbreviation with its device description.

    The abbreviation appears in several shapes: a separate span, the first line of a combined
    block, the last line of a block, or its own row adjacent to a description-only row. This
    resolves all of those into ``{abbreviation -> SymbolEntry}``. Rows with no abbreviation (icon
    only) and NOTE annotations are skipped. First occurrence wins on duplicate abbreviations.

    Icon-template capture (for matching untagged body symbols) is intentionally out of scope here.
    """

    parsed = [_parse_row(row) for row in rows]
    paired = _pair_split_rows(parsed)

    dictionary: dict[str, SymbolEntry] = {}
    for abbreviation, type_name, bbox in paired:
        if abbreviation is None or not type_name:
            continue
        if abbreviation not in dictionary:  # first occurrence wins (e.g. a reused abbreviation)
            dictionary[abbreviation] = SymbolEntry(
                abbreviation=abbreviation,
                type_name=type_name,
                bbox=bbox,
            )
    return dictionary


def _parse_row(
    row: LegendRow,
) -> tuple[str | None, str, tuple[float, float, float, float]]:
    """Return (abbreviation|None, description, bbox) for a row."""

    lines = [
        line.strip()
        for text in row.texts
        for line in text.split("\n")
        if line.strip() and not _NON_ENTRY_PREFIX_RE.match(line)
    ]
    abbreviation: str | None = None
    description_lines: list[str] = []
    for line in lines:
        if abbreviation is None and _ABBREVIATION_RE.match(line):
            abbreviation = line
        else:
            description_lines.append(line)

    description = re.sub(r"\s+", " ", " ".join(description_lines)).strip()
    return abbreviation, description, row.bbox


def _pair_split_rows(
    parsed: Sequence[tuple[str | None, str, tuple[float, float, float, float]]],
) -> list[tuple[str | None, str, tuple[float, float, float, float]]]:
    """Pair a stand-alone abbreviation row with an adjacent stand-alone description row."""

    result: list[tuple[str | None, str, tuple[float, float, float, float]]] = []
    pending: tuple[str, tuple[float, float, float, float]] | None = None
    for abbreviation, description, bbox in parsed:
        if abbreviation is not None and not description:
            pending = (abbreviation, bbox)  # abbreviation awaiting its description
            continue
        if abbreviation is None and description and pending is not None:
            pending_abbr, pending_bbox = pending
            if _rows_adjacent(pending_bbox, bbox):
                merged = _union_bbox(pending_bbox, bbox)
                result.append((pending_abbr, description, merged))
                pending = None
                continue
            result.append((pending_abbr, "", pending_bbox))  # orphan abbreviation
            pending = None
        elif pending is not None:
            result.append((pending[0], "", pending[1]))  # orphan abbreviation
            pending = None
        result.append((abbreviation, description, bbox))

    if pending is not None:
        result.append((pending[0], "", pending[1]))
    return result


def _rows_adjacent(
    first: tuple[float, float, float, float],
    second: tuple[float, float, float, float],
) -> bool:
    height = max(first[3] - first[1], second[3] - second[1], 1.0)
    first_center = (first[1] + first[3]) / 2
    second_center = (second[1] + second[3]) / 2
    return abs(second_center - first_center) <= height * _PAIR_ADJACENCY_ROWS


def _union_bbox(
    first: tuple[float, float, float, float],
    second: tuple[float, float, float, float],
) -> tuple[float, float, float, float]:
    return (
        min(first[0], second[0]),
        min(first[1], second[1]),
        max(first[2], second[2]),
        max(first[3], second[3]),
    )
