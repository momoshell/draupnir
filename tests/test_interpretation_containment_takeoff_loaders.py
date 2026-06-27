"""Pure unit tests (fake-session / fake-row stubs) for containment loaders (issue #755, 752b-1).

No real database, no harness registration.  All DB calls are replaced by AsyncMock stubs
that return minimal fake RevisionEntity / RevisionRoutedLength objects.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from app.interpretation.containment_legend import ContainmentLegend
from app.interpretation.service_takeoff_loaders import (
    build_containment_legend_db,
    load_containment_bands,
    load_containment_centerline_segments,
)

# ---------------------------------------------------------------------------
# Helpers — fake row builders
# ---------------------------------------------------------------------------

_REV_ID = UUID("aaaa0000-0000-0000-0000-000000000001")

# A square ring with 4 distinct points (≥3 required).
_RING_RAW = [
    {"x": -0.5, "y": -0.5},
    {"x": 0.5, "y": -0.5},
    {"x": 0.5, "y": 0.5},
    {"x": -0.5, "y": 0.5},
]


def _fake_hatch_row(
    colour_index: int,
    layer_ref: str = "Cable Tray",
    pattern_name: str = "SOLID",
    ring: list[dict[str, float]] | None = None,
) -> SimpleNamespace:
    """Build a minimal fake RevisionEntity hatch row."""
    ring_pts = ring if ring is not None else _RING_RAW
    return SimpleNamespace(
        id="entity-abc",
        entity_type="hatch",
        layer_ref=layer_ref,
        sequence_index=0,
        on_sheet=True,
        style={"color": {"index": colour_index, "rgb": None, "by_layer": False, "by_block": False}},
        geometry_json={
            "boundary_loops": [ring_pts],
            "geometry_summary": {"pattern_name": pattern_name},
        },
        properties_json={"layer": layer_ref},
    )


def _fake_rl_row(
    polyline: list[list[float]],
) -> SimpleNamespace:
    """Build a minimal fake RevisionRoutedLength row with geometry_json."""
    return SimpleNamespace(
        drawing_revision_id=_REV_ID,
        algo_version="c5-singleline-1",
        layer_ref="Cable Tray",
        colour_key="idx:1",
        skeleton_length_du=1.0,
        geometry_json={"polylines": [polyline]},
    )


def _make_db_stub(rows: list[Any]) -> AsyncMock:
    """Return an AsyncMock db whose execute().scalars().all() yields ``rows``."""
    scalars_mock = MagicMock()
    scalars_mock.all.return_value = rows
    execute_result = MagicMock()
    execute_result.scalars.return_value = scalars_mock
    db = AsyncMock()
    db.execute = AsyncMock(return_value=execute_result)
    return db


# ---------------------------------------------------------------------------
# load_containment_bands
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_containment_bands_captures_pattern_name() -> None:
    """load_containment_bands reads pattern_name from geometry_summary."""
    row = _fake_hatch_row(colour_index=1, layer_ref="Cable Tray", pattern_name="FP_1")
    db = _make_db_stub([row])

    bands = await load_containment_bands(db, _REV_ID)

    assert len(bands) == 1
    assert bands[0].pattern_name == "FP_1"
    assert bands[0].colour_key is not None


@pytest.mark.asyncio
async def test_load_containment_bands_distinct_pattern_names_same_colour() -> None:
    """Two bands with the same colour but different pattern_names are both returned."""
    row1 = _fake_hatch_row(colour_index=5, layer_ref="Cable Tray", pattern_name="SOLID")
    row2 = _fake_hatch_row(colour_index=5, layer_ref="Cable Tray", pattern_name="FP_2")
    db = _make_db_stub([row1, row2])

    bands = await load_containment_bands(db, _REV_ID)

    assert len(bands) == 2
    pns = {b.pattern_name for b in bands}
    assert "SOLID" in pns
    assert "FP_2" in pns


@pytest.mark.asyncio
async def test_load_containment_bands_missing_pattern_name_defaults_to_empty_string() -> None:
    """When geometry_summary is absent, pattern_name defaults to empty string."""
    row = SimpleNamespace(
        id="x",
        entity_type="hatch",
        layer_ref="Conduit",
        sequence_index=0,
        on_sheet=True,
        style={"color": {"index": 3, "rgb": None, "by_layer": False, "by_block": False}},
        geometry_json={"boundary_loops": [_RING_RAW]},  # no geometry_summary
        properties_json={},
    )
    db = _make_db_stub([row])

    bands = await load_containment_bands(db, _REV_ID)

    assert len(bands) == 1
    assert bands[0].pattern_name == ""


@pytest.mark.asyncio
async def test_load_containment_bands_empty_on_no_data() -> None:
    """Empty row list → empty result, no raise."""
    db = _make_db_stub([])

    bands = await load_containment_bands(db, _REV_ID)

    assert bands == []


@pytest.mark.asyncio
async def test_load_containment_bands_empty_on_exception() -> None:
    """DB exception → tolerant empty return."""
    db = AsyncMock()
    db.execute = AsyncMock(side_effect=RuntimeError("DB down"))

    bands = await load_containment_bands(db, _REV_ID)

    assert bands == []


# ---------------------------------------------------------------------------
# load_containment_centerline_segments
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_containment_centerline_segments_3vertex_polyline_gives_2_segments() -> None:
    """A 3-vertex polyline → 2 vertex-pair segments."""
    # polyline: (0,0) → (1,0) → (2,0)  → 2 segments
    row = _fake_rl_row([[0.0, 0.0], [1.0, 0.0], [2.0, 0.0]])
    db = _make_db_stub([row])

    segs = await load_containment_centerline_segments(db, _REV_ID)

    assert len(segs) == 2
    assert segs[0] == ((0.0, 0.0), (1.0, 0.0))
    assert segs[1] == ((1.0, 0.0), (2.0, 0.0))


@pytest.mark.asyncio
async def test_load_containment_centerline_segments_empty_on_no_data() -> None:
    """No rows → empty list, no raise."""
    db = _make_db_stub([])

    segs = await load_containment_centerline_segments(db, _REV_ID)

    assert segs == []


@pytest.mark.asyncio
async def test_load_containment_centerline_segments_empty_on_exception() -> None:
    """DB exception → tolerant empty return."""
    db = AsyncMock()
    db.execute = AsyncMock(side_effect=RuntimeError("gone"))

    segs = await load_containment_centerline_segments(db, _REV_ID)

    assert segs == []


# ---------------------------------------------------------------------------
# build_containment_legend_db
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_build_containment_legend_db_empty_on_pdf_family() -> None:
    """PDF revisions have no DWG HATCH legend → empty ContainmentLegend."""
    db = AsyncMock()

    legend = await build_containment_legend_db(db, _REV_ID, input_family="pdf_vector")

    assert isinstance(legend, ContainmentLegend)
    assert legend.entries == ()


@pytest.mark.asyncio
async def test_build_containment_legend_db_empty_on_exception() -> None:
    """Any DB error → tolerant empty legend."""
    db = AsyncMock()
    db.execute = AsyncMock(side_effect=RuntimeError("broken"))

    # Pass input_family explicitly to avoid triggering _resolve_input_family (which needs a
    # real adapter_run_output row — not available with a fake db stub).
    legend = await build_containment_legend_db(db, _REV_ID, input_family="dwg")

    assert isinstance(legend, ContainmentLegend)
    assert legend.entries == ()


@pytest.mark.asyncio
async def test_build_containment_legend_db_pairs_swatch_and_label_for_lookup() -> None:
    """DWG path: an anchor + nearby HATCH swatch + label text → lookup resolves the type."""
    from types import SimpleNamespace

    # Anchor text entity at (0, 3) whose text matches "CONTAINMENTS LEGEND".
    anchor_row = SimpleNamespace(
        id="anchor",
        entity_type="text",
        layer_ref="Legend",
        sequence_index=0,
        style={},
        geometry_json={
            "text": "CONTAINMENTS LEGEND",
            "transform": {"insertion_point": {"x": 0.0, "y": 3.0}},
        },
        properties_json={},
    )

    # HATCH swatch at (0.2, 2.5) — within the anchor region (pad_left=1.0, pad_below=2.0,
    # pad_right=3.5, pad_above=0.5 from anchor).
    # Region: x ∈ [-1, 3.5], y ∈ [1.0, 3.5]  (anchor at 0,3; below=2 → y_lo=1.0).
    hatch_row = SimpleNamespace(
        id="swatch-1",
        entity_type="hatch",
        layer_ref="Legend",
        sequence_index=1,
        on_sheet=True,
        style={"color": {"index": 7, "rgb": "00ff00", "by_layer": False, "by_block": False}},
        geometry_json={
            "vertices": [
                {"x": 0.1, "y": 2.4},
                {"x": 0.3, "y": 2.4},
                {"x": 0.3, "y": 2.6},
                {"x": 0.1, "y": 2.6},
            ],
            "geometry_summary": {"pattern_name": "SOLID"},
        },
        properties_json={},
    )

    # Label text entity at (0.5, 2.5) — right of swatch, within pair radius 1.5 du.
    label_row = SimpleNamespace(
        id="label-1",
        entity_type="text",
        layer_ref="Legend",
        sequence_index=2,
        style={},
        geometry_json={
            "text": "Cable Tray",
            "transform": {"insertion_point": {"x": 0.5, "y": 2.5}},
        },
        properties_json={},
    )

    call_count = 0

    async def _execute_side_effect(stmt: Any) -> Any:
        nonlocal call_count
        call_count += 1
        scalars_mock = MagicMock()
        if call_count == 1:
            # First call: text/mtext query
            scalars_mock.all.return_value = [anchor_row, label_row]
        else:
            # Second call: all entities query
            scalars_mock.all.return_value = [anchor_row, hatch_row, label_row]
        result = MagicMock()
        result.scalars.return_value = scalars_mock
        return result

    db = AsyncMock()
    db.execute = AsyncMock(side_effect=_execute_side_effect)

    # Pass input_family explicitly to avoid triggering _resolve_input_family.
    legend = await build_containment_legend_db(db, _REV_ID, input_family="dwg")

    # The swatch (colour_key for index=7,rgb="00ff00", pattern="SOLID") should resolve
    # to "Cable Tray" via lookup.
    from app.interpretation.service_legend import colour_key as _ck

    ck = _ck({"index": 7, "rgb": "00ff00", "by_layer": False, "by_block": False})
    entry = legend.lookup(ck, "SOLID")
    assert entry is not None, "Expected a legend entry for the swatch"
    assert entry.containment_type == "Cable Tray"


@pytest.mark.asyncio
async def test_build_containment_legend_db_blank_label_yields_no_entry() -> None:
    """A swatch with a blank/whitespace label produces NO legend mapping.

    The blank text is filtered out before pairing, so the swatch is never emitted as a
    ContainmentSwatchInput → the legend has no entry for its (colour, pattern) key. Downstream
    this is the honest-absent path: body hatches of that key resolve to None at classify time
    (they can't be named without a label).
    """
    anchor_row = SimpleNamespace(
        id="anchor",
        entity_type="text",
        layer_ref="Legend",
        sequence_index=0,
        style={},
        geometry_json={
            "text": "CONTAINMENTS LEGEND",
            "transform": {"insertion_point": {"x": 0.0, "y": 3.0}},
        },
        properties_json={},
    )

    hatch_row = SimpleNamespace(
        id="swatch-blank",
        entity_type="hatch",
        layer_ref="Legend",
        sequence_index=1,
        on_sheet=True,
        style={"color": {"index": 5, "rgb": "0000ff", "by_layer": False, "by_block": False}},
        geometry_json={
            "vertices": [
                {"x": 0.1, "y": 2.4},
                {"x": 0.3, "y": 2.4},
                {"x": 0.3, "y": 2.6},
                {"x": 0.1, "y": 2.6},
            ],
            "geometry_summary": {"pattern_name": "SOLID"},
        },
        properties_json={},
    )

    # Label text is whitespace only → blank label → honest-absent
    blank_label_row = SimpleNamespace(
        id="label-blank",
        entity_type="text",
        layer_ref="Legend",
        sequence_index=2,
        style={},
        geometry_json={
            "text": "   ",
            "transform": {"insertion_point": {"x": 0.5, "y": 2.5}},
        },
        properties_json={},
    )

    call_count = 0

    async def _execute_side_effect(stmt: Any) -> Any:
        nonlocal call_count
        call_count += 1
        scalars_mock = MagicMock()
        if call_count == 1:
            scalars_mock.all.return_value = [anchor_row, blank_label_row]
        else:
            scalars_mock.all.return_value = [anchor_row, hatch_row, blank_label_row]
        result = MagicMock()
        result.scalars.return_value = scalars_mock
        return result

    db = AsyncMock()
    db.execute = AsyncMock(side_effect=_execute_side_effect)

    # blank label means the text_entry is filtered out (text.strip() is empty → skipped).
    # So no ContainmentSwatchInput is emitted → legend has no entries.
    # Pass input_family explicitly to avoid triggering _resolve_input_family.
    legend = await build_containment_legend_db(db, _REV_ID, input_family="dwg")

    # The blank-label swatch produced no mapping: its (colour, pattern) key is unmapped.
    assert isinstance(legend, ContainmentLegend)
    assert legend.lookup("0000ff", "SOLID") is None
    assert legend.by_key() == {}
