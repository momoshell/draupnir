"""Unit tests for legend region detection / row segmentation (pure, no DB)."""

from __future__ import annotations

from typing import Any

from app.interpretation.legend import find_legend_anchor, segment_legend_rows


def _block(text: str, x_min: float, y_min: float, *, width: float = 30.0) -> dict[str, Any]:
    return {
        "text": text,
        "bbox": {"x_min": x_min, "y_min": y_min, "x_max": x_min + width, "y_max": y_min + 12.0},
    }


def _drawing_like_legend() -> list[dict[str, Any]]:
    # Legend column at x~100-140 (abbreviation + description sub-columns); a NOTES column far to
    # the right at x~400 that must be excluded. Mirrors the real 680003 title-block layout.
    return [
        _block("LEGEND", 100.0, 10.0, width=44.0),
        _block("DC", 105.0, 40.0, width=14.0),
        _block("DOOR CONTACT", 140.0, 40.0, width=120.0),
        _block("P", 105.0, 70.0, width=10.0),
        _block("PANIC BUTTON", 140.0, 70.0, width=120.0),
        _block("S", 105.0, 100.0, width=10.0),
        _block("STATIC DOME CAMERA", 140.0, 100.0, width=160.0),
        # NOTES column (separate, far right) — must not leak into the legend.
        _block("NOTES", 400.0, 10.0, width=40.0),
        _block("DO NOT SCALE FROM THIS DRAWING.", 400.0, 40.0, width=200.0),
        _block("ALL DIMENSIONS ARE IN MILLIMETRES", 400.0, 70.0, width=200.0),
    ]


def test_find_legend_anchor_matches_header_keywords() -> None:
    assert find_legend_anchor([_block("LEGEND", 0.0, 0.0)]) is not None
    assert find_legend_anchor([_block("Key", 0.0, 0.0)]) is not None
    assert find_legend_anchor([_block("Symbols", 0.0, 0.0)]) is not None
    # "KEY NOTES" is not a bare legend header.
    assert find_legend_anchor([_block("KEY NOTES", 0.0, 0.0)]) is None
    assert find_legend_anchor([_block("random text", 0.0, 0.0)]) is None


def test_segment_legend_rows_groups_pairs_and_excludes_notes_column() -> None:
    rows = segment_legend_rows(_drawing_like_legend())

    joined = [" ".join(row.texts) for row in rows]
    assert "DC DOOR CONTACT" in joined
    assert "P PANIC BUTTON" in joined
    assert "S STATIC DOME CAMERA" in joined
    # Nothing from the NOTES column leaked in.
    assert not any("DO NOT SCALE" in text or "DIMENSIONS" in text for text in joined)
    # Each device row pairs the abbreviation (left) with its description (right), ordered by x.
    dc_row = next(row for row in rows if row.texts[0] == "DC")
    assert dc_row.texts == ("DC", "DOOR CONTACT")


def test_segment_legend_rows_without_notes_keeps_whole_legend() -> None:
    blocks = [
        _block("LEGEND", 100.0, 10.0, width=44.0),
        _block("DC", 105.0, 40.0, width=14.0),
        _block("DOOR CONTACT", 140.0, 40.0, width=120.0),
        _block("P", 105.0, 70.0, width=10.0),
        _block("PANIC BUTTON", 140.0, 70.0, width=120.0),
    ]
    rows = segment_legend_rows(blocks)
    joined = [" ".join(row.texts) for row in rows]
    assert "DC DOOR CONTACT" in joined
    assert "P PANIC BUTTON" in joined


def test_segment_legend_rows_returns_empty_without_header() -> None:
    assert segment_legend_rows([_block("DC", 105.0, 40.0)]) == []
