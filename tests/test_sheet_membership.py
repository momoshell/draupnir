"""Unit tests for printed-sheet membership tagging (#568)."""

from typing import Any

from app.ingestion.sheet_membership import tag_sheet_membership


def _canonical(entities: list[dict[str, Any]], windows: list[dict[str, float]]) -> dict[str, Any]:
    return {
        "viewports": {
            "schema_version": "0.1",
            "items": [{"model_window": w} for w in windows],
        },
        "entities": entities,
        "metadata": {},
    }


def _line(x0: float, y0: float, x1: float, y1: float) -> dict[str, Any]:
    return {
        "entity_type": "line",
        "geometry": {"start": {"x": x0, "y": y0}, "end": {"x": x1, "y": y1}},
    }


_WINDOW = {"min_x": 10.0, "min_y": 10.0, "max_x": 50.0, "max_y": 30.0}


def test_entity_inside_window_tagged_on_sheet() -> None:
    canonical = _canonical([_line(20, 15, 30, 25)], [_WINDOW])
    tagged = tag_sheet_membership(canonical)
    m = tagged["entities"][0]["properties"]["sheet_membership"]
    assert m["on_sheet"] is True
    assert m["viewport_indices"] == [0]
    assert tagged["metadata"]["sheet_membership"]["on_sheet"] == 1
    assert tagged["metadata"]["sheet_membership"]["off_sheet"] == 0


def test_entity_outside_all_windows_tagged_off_sheet() -> None:
    # a title-block / key-plan line far to the left of the window
    canonical = _canonical([_line(-48, 5, -44, 6)], [_WINDOW])
    tagged = tag_sheet_membership(canonical)
    m = tagged["entities"][0]["properties"]["sheet_membership"]
    assert m["on_sheet"] is False
    assert m["viewport_indices"] == []
    assert tagged["metadata"]["sheet_membership"]["off_sheet"] == 1


def test_entity_straddling_window_edge_is_on_sheet() -> None:
    # a wall crossing the viewport boundary counts as on-sheet (don't hide partial content)
    canonical = _canonical([_line(40, 20, 80, 20)], [_WINDOW])
    m = tag_sheet_membership(canonical)["entities"][0]["properties"]["sheet_membership"]
    assert m["on_sheet"] is True


def test_insert_uses_insertion_point_fallback() -> None:
    insert = {
        "entity_type": "insert",
        "geometry": {"transform": {"insertion_point": {"x": 25.0, "y": 20.0}}},
    }
    m = tag_sheet_membership(_canonical([insert], [_WINDOW]))["entities"][0]
    assert m["properties"]["sheet_membership"]["on_sheet"] is True


def test_entity_without_extent_is_undetermined() -> None:
    blank = {"entity_type": "text", "geometry": {"text": "S"}}
    tagged = tag_sheet_membership(_canonical([blank], [_WINDOW]))
    m = tagged["entities"][0]["properties"]["sheet_membership"]
    assert m["on_sheet"] is None
    assert m["reason"] == "no_extent"
    assert tagged["metadata"]["sheet_membership"]["undetermined"] == 1


def test_multiple_windows_report_all_hits() -> None:
    windows = [_WINDOW, {"min_x": 60.0, "min_y": 0.0, "max_x": 70.0, "max_y": 10.0}]
    # spans into the second window only
    canonical = _canonical([_line(62, 2, 68, 8)], windows)
    m = tag_sheet_membership(canonical)["entities"][0]["properties"]["sheet_membership"]
    assert m["viewport_indices"] == [1]


def test_no_viewports_is_noop() -> None:
    # PDF or DWG with no viewports: membership is undeterminable → no tag fabricated
    canonical = {"viewports": {"items": []}, "entities": [_line(0, 0, 1, 1)], "metadata": {}}
    tagged = tag_sheet_membership(canonical)
    assert "properties" not in tagged["entities"][0] or (
        "sheet_membership" not in tagged["entities"][0].get("properties", {})
    )
    assert "sheet_membership" not in tagged["metadata"]


def test_degenerate_window_skipped() -> None:
    # the paperspace overview vp (zero-area window) must not match anything
    degenerate = {"min_x": 0.0, "min_y": 0.0, "max_x": 0.0, "max_y": 0.0}
    canonical = _canonical([_line(0, 0, 1, 1)], [degenerate])
    tagged = tag_sheet_membership(canonical)
    # no real windows → no-op
    assert "sheet_membership" not in tagged["metadata"]


def test_existing_properties_preserved() -> None:
    line = _line(20, 15, 30, 25)
    line["properties"] = {"color": "red"}
    m = tag_sheet_membership(_canonical([line], [_WINDOW]))["entities"][0]["properties"]
    assert m["color"] == "red"
    assert m["sheet_membership"]["on_sheet"] is True
