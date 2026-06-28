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


# ---------------------------------------------------------------------------
# build_containment_legend_db — containment-specific wide region (#760)
# ---------------------------------------------------------------------------


def _make_hatch_entity(
    entity_id: str,
    centroid_x: float,
    centroid_y: float,
    rgb: str,
    pattern: str = "SOLID",
) -> SimpleNamespace:
    """Minimal hatch entity whose centroid is (centroid_x, centroid_y)."""
    half = 0.1
    return SimpleNamespace(
        id=entity_id,
        entity_type="hatch",
        layer_ref="Legend",
        sequence_index=0,
        on_sheet=True,
        style={"color": {"index": None, "rgb": rgb, "by_layer": False, "by_block": False}},
        geometry_json={
            "vertices": [
                {"x": centroid_x - half, "y": centroid_y - half},
                {"x": centroid_x + half, "y": centroid_y - half},
                {"x": centroid_x + half, "y": centroid_y + half},
                {"x": centroid_x - half, "y": centroid_y + half},
            ],
            "geometry_summary": {"pattern_name": pattern},
        },
        properties_json={},
    )


def _make_text_entity(entity_id: str, text: str, x: float, y: float) -> SimpleNamespace:
    return SimpleNamespace(
        id=entity_id,
        entity_type="text",
        layer_ref="Legend",
        sequence_index=0,
        style={},
        geometry_json={
            "text": text,
            "transform": {"insertion_point": {"x": x, "y": y}},
        },
        properties_json={},
    )


@pytest.mark.asyncio
async def test_build_containment_legend_db_wide_region_captures_deep_rows() -> None:
    """Widened containment pads capture rows 1, 2, and 3 (which lie below the old 2.0-unit floor).

    Anchor at (ax=0, ay=3).  Under the OLD pads (below=2.0) the region floor was y=1.0,
    so row-2 (y=0.0) and row-3 (y=-3.5) would have been missed.  Under the NEW pads
    (below=7.2) the floor is y=-4.2, so all three rows are captured.

    Grounded offsets from E-610003:
      swatch column at ax+0.73, label column at ax+1.65
      row 1: dy=-0.6, row 2: dy=-3.0, row 3: dy=-6.5
    """
    ax, ay = 0.0, 3.0
    anchor = _make_text_entity("anchor", "CONTAINMENTS LEGEND", ax, ay)

    # Three chromatic swatch+label pairs at grounded relative positions.
    swatch1 = _make_hatch_entity("sw1", ax + 0.73, ay - 0.6, "ff0000")
    label1 = _make_text_entity("lb1", "Cable Tray", ax + 1.65, ay - 0.6)

    swatch2 = _make_hatch_entity("sw2", ax + 0.73, ay - 3.0, "00ff00")
    label2 = _make_text_entity("lb2", "Cable Ladder", ax + 1.65, ay - 3.0)

    swatch3 = _make_hatch_entity("sw3", ax + 0.73, ay - 6.5, "0000ff")
    label3 = _make_text_entity("lb3", "Dado Trunking", ax + 1.65, ay - 6.5)

    all_entities = [anchor, swatch1, label1, swatch2, label2, swatch3, label3]
    text_entities = [anchor, label1, label2, label3]

    call_count = 0

    async def _side_effect(stmt: Any) -> Any:
        nonlocal call_count
        call_count += 1
        scalars_mock = MagicMock()
        scalars_mock.all.return_value = text_entities if call_count == 1 else all_entities
        result = MagicMock()
        result.scalars.return_value = scalars_mock
        return result

    db = AsyncMock()
    db.execute = AsyncMock(side_effect=_side_effect)

    legend = await build_containment_legend_db(db, _REV_ID, input_family="dwg")

    entry1 = legend.lookup("ff0000", "SOLID")
    assert entry1 is not None, "row-1 swatch must be in legend"
    # rows 2 and 3 were below the old 2.0-unit floor; widened pads must now capture them
    entry2 = legend.lookup("00ff00", "SOLID")
    assert entry2 is not None, "row-2 swatch must be in legend"
    entry3 = legend.lookup("0000ff", "SOLID")
    assert entry3 is not None, "row-3 swatch must be in legend"
    assert entry1.containment_type == "Cable Tray"
    assert entry2.containment_type == "Cable Ladder"
    assert entry3.containment_type == "Dado Trunking"


@pytest.mark.asyncio
async def test_build_containment_legend_db_excludes_notes_left_and_equipment_below() -> None:
    """Negative controls: items outside the containment region must NOT appear in the legend.

    - NOTES-like text at (ax-0.83, ay-3.0): left of the swatch column (x < ax-0.3) → excluded.
    - Equipment-symbol swatch+label at (ax+0.73, ay-7.6): below the table (y < ay-7.2) → excluded.
    """
    ax, ay = 0.0, 3.0
    anchor = _make_text_entity("anchor", "CONTAINMENTS LEGEND", ax, ay)

    # A legitimate in-region swatch so the legend is not empty and we get a meaningful result.
    in_swatch = _make_hatch_entity("sw-in", ax + 0.73, ay - 0.6, "aabbcc")
    in_label = _make_text_entity("lb-in", "Conduit", ax + 1.65, ay - 0.6)

    # NOTES-like text left of swatches (x < ax - 0.3 → outside left bound).
    notes_text = _make_text_entity("notes", "LV NOTES TEXT", ax - 0.83, ay - 3.0)

    # Equipment-symbol swatch below the table (y < ay - 7.2 → outside bottom bound).
    equip_swatch = _make_hatch_entity("sw-eq", ax + 0.73, ay - 7.6, "112233")
    equip_label = _make_text_entity("lb-eq", "ATS Panel", ax + 1.65, ay - 7.6)

    all_entities = [anchor, in_swatch, in_label, notes_text, equip_swatch, equip_label]
    text_entities = [anchor, in_label, notes_text, equip_label]

    call_count = 0

    async def _side_effect(stmt: Any) -> Any:
        nonlocal call_count
        call_count += 1
        scalars_mock = MagicMock()
        scalars_mock.all.return_value = text_entities if call_count == 1 else all_entities
        result = MagicMock()
        result.scalars.return_value = scalars_mock
        return result

    db = AsyncMock()
    db.execute = AsyncMock(side_effect=_side_effect)

    legend = await build_containment_legend_db(db, _REV_ID, input_family="dwg")

    # The in-region conduit swatch must be present.
    conduit_entry = legend.lookup("aabbcc", "SOLID")
    assert conduit_entry is not None, "in-region swatch must be present"
    assert conduit_entry.containment_type == "Conduit"

    # Notes text is outside the left boundary → no pairing with any swatch.
    # Verify: if it HAD been paired it would have overwritten the in-region label; since we
    # only have one swatch in-region it can only be the Conduit entry.
    assert conduit_entry.containment_type != "LV NOTES TEXT"

    # Equipment swatch is below the bottom boundary → not in the legend.
    assert legend.lookup("112233", "SOLID") is None, "below-table swatch must be excluded"


# ---------------------------------------------------------------------------
# build_mech_service_legend_db (issue #775)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_build_mech_service_legend_db_empty_on_pdf_family() -> None:
    """PDF revisions have no DWG HATCH/LINE mechanical swatches → empty MechanicalLegend."""
    from app.interpretation.mechanical_legend import MechanicalLegend
    from app.interpretation.service_takeoff_loaders import build_mech_service_legend_db

    db = AsyncMock()
    legend = await build_mech_service_legend_db(db, _REV_ID, input_family="pdf_vector")

    assert isinstance(legend, MechanicalLegend)
    assert legend.entries == ()


@pytest.mark.asyncio
async def test_build_mech_service_legend_db_empty_on_exception() -> None:
    """Any DB error → tolerant empty MechanicalLegend."""
    from app.interpretation.mechanical_legend import MechanicalLegend
    from app.interpretation.service_takeoff_loaders import build_mech_service_legend_db

    db = AsyncMock()
    db.execute = AsyncMock(side_effect=RuntimeError("DB down"))

    legend = await build_mech_service_legend_db(db, _REV_ID, input_family="dwg")

    assert isinstance(legend, MechanicalLegend)
    assert legend.entries == ()


@pytest.mark.asyncio
async def test_build_mech_service_legend_db_pairs_hatch_swatch_with_label() -> None:
    """DWG path: anchor + chromatic HATCH swatch + label text → lookup resolves service.

    Calibrated to the _DWG_MECH_LEGEND_* constants:
    - Anchor regex: r'(?i)\\blegend\\b' (matches 'LEGEND')
    - Region pads: LEFT=0.2, RIGHT=0.8, ABOVE=0.6, BELOW=3.5
    - Pair radius: 0.8
    """
    from app.interpretation.mechanical_legend import MechanicalLegend
    from app.interpretation.service_legend import colour_key as _ck
    from app.interpretation.service_takeoff_loaders import build_mech_service_legend_db

    # Anchor text at (10.0, 5.0) — matches 'LEGEND'.
    anchor_row = SimpleNamespace(
        id="anchor",
        entity_type="text",
        layer_ref="Legend",
        sequence_index=0,
        style={},
        geometry_json={
            "text": "LEGEND",
            "insertion": {"x": 10.0, "y": 5.0},
        },
        properties_json={},
    )

    # HATCH swatch centroid at (10.1, 4.5):
    # Region x: [10.0-0.2, 10.0+0.8] = [9.8, 10.8]; y: [5.0-3.5, 5.0+0.6] = [1.5, 5.6]
    # Centroid (10.1, 4.5) is inside the region.
    hatch_row = SimpleNamespace(
        id="swatch-mech",
        entity_type="hatch",
        layer_ref="Legend",
        sequence_index=1,
        on_sheet=True,
        style={"color": {"index": 247, "rgb": "c2873f51", "by_layer": False, "by_block": False}},
        geometry_json={
            "vertices": [
                {"x": 10.05, "y": 4.45},
                {"x": 10.15, "y": 4.45},
                {"x": 10.15, "y": 4.55},
                {"x": 10.05, "y": 4.55},
            ],
        },
        properties_json={},
    )

    # Label text at (10.3, 4.5) — right of swatch, within pair radius 0.8.
    # Distance: sqrt((10.3-10.1)^2 + (4.5-4.5)^2) = 0.2 < 0.8 → paired.
    label_row = SimpleNamespace(
        id="label-mech",
        entity_type="text",
        layer_ref="Legend",
        sequence_index=2,
        style={},
        geometry_json={
            "text": "LTHW-VT-F",
            "insertion": {"x": 10.3, "y": 4.5},
        },
        properties_json={},
    )

    call_count = 0

    async def _execute_side_effect(stmt: Any) -> Any:
        nonlocal call_count
        call_count += 1
        scalars_mock = MagicMock()
        if call_count == 1:
            # First call: text/mtext query for anchor detection
            scalars_mock.all.return_value = [anchor_row, label_row]
        else:
            # Second call: all entities query
            scalars_mock.all.return_value = [anchor_row, hatch_row, label_row]
        result = MagicMock()
        result.scalars.return_value = scalars_mock
        return result

    db = AsyncMock()
    db.execute = AsyncMock(side_effect=_execute_side_effect)

    legend = await build_mech_service_legend_db(db, _REV_ID, input_family="dwg")

    assert isinstance(legend, MechanicalLegend)
    ck = _ck({"index": 247, "rgb": "c2873f51", "by_layer": False, "by_block": False})
    entry = legend.lookup(ck)
    assert entry is not None, "Expected a legend entry for the hatch swatch"
    assert entry.service == "LTHW-VT-F"


@pytest.mark.asyncio
async def test_build_mech_service_legend_db_line_swatch_accepted_when_short() -> None:
    """Short LINE entity (< _DWG_MECH_SWATCH_LINE_LEN_MAX) is accepted as a swatch.

    M-560103 happens to use HATCH swatches, but the loader must also accept short line
    samples for DWG drawings that encode the legend with line swatches.
    """
    from app.interpretation.mechanical_legend import MechanicalLegend
    from app.interpretation.service_legend import colour_key as _ck
    from app.interpretation.service_takeoff_loaders import build_mech_service_legend_db

    anchor_row = SimpleNamespace(
        id="anchor",
        entity_type="text",
        layer_ref="Legend",
        sequence_index=0,
        style={},
        geometry_json={
            "text": "LEGEND",
            "insertion": {"x": 10.0, "y": 5.0},
        },
        properties_json={},
    )

    # Short line swatch at (10.1, 4.5): length = 0.2 < 0.6 threshold → accepted.
    line_row = SimpleNamespace(
        id="swatch-line",
        entity_type="line",
        layer_ref="Legend",
        sequence_index=1,
        on_sheet=True,
        style={"color": {"index": 243, "rgb": "c2bc7083", "by_layer": False, "by_block": False}},
        geometry_json={
            "start": {"x": 10.0, "y": 4.5},
            "end": {"x": 10.2, "y": 4.5},
        },
        properties_json={},
    )

    label_row = SimpleNamespace(
        id="label-line",
        entity_type="text",
        layer_ref="Legend",
        sequence_index=2,
        style={},
        geometry_json={
            "text": "LTHW-VT-R",
            "insertion": {"x": 10.3, "y": 4.5},
        },
        properties_json={},
    )

    call_count = 0

    async def _execute_side_effect(stmt: Any) -> Any:
        nonlocal call_count
        call_count += 1
        scalars_mock = MagicMock()
        if call_count == 1:
            scalars_mock.all.return_value = [anchor_row, label_row]
        else:
            scalars_mock.all.return_value = [anchor_row, line_row, label_row]
        result = MagicMock()
        result.scalars.return_value = scalars_mock
        return result

    db = AsyncMock()
    db.execute = AsyncMock(side_effect=_execute_side_effect)

    legend = await build_mech_service_legend_db(db, _REV_ID, input_family="dwg")

    assert isinstance(legend, MechanicalLegend)
    ck = _ck({"index": 243, "rgb": "c2bc7083", "by_layer": False, "by_block": False})
    entry = legend.lookup(ck)
    assert entry is not None, "Short line swatch should produce an entry"
    assert entry.service == "LTHW-VT-R"


@pytest.mark.asyncio
async def test_build_mech_service_legend_db_long_line_excluded() -> None:
    """Long LINE entity (> _DWG_MECH_SWATCH_LINE_LEN_MAX) is rejected (floor pipework gate)."""
    from app.interpretation.mechanical_legend import MechanicalLegend
    from app.interpretation.service_legend import colour_key as _ck
    from app.interpretation.service_takeoff_loaders import build_mech_service_legend_db

    anchor_row = SimpleNamespace(
        id="anchor",
        entity_type="text",
        layer_ref="Legend",
        sequence_index=0,
        style={},
        geometry_json={
            "text": "LEGEND",
            "insertion": {"x": 10.0, "y": 5.0},
        },
        properties_json={},
    )

    # Long line: length = 2.0 > 0.6 threshold → rejected by length gate.
    long_line_row = SimpleNamespace(
        id="swatch-long",
        entity_type="line",
        layer_ref="Legend",
        sequence_index=1,
        on_sheet=True,
        style={"color": {"index": 247, "rgb": "c2873f51", "by_layer": False, "by_block": False}},
        geometry_json={
            "start": {"x": 10.0, "y": 4.5},
            "end": {"x": 12.0, "y": 4.5},
        },
        properties_json={},
    )

    label_row = SimpleNamespace(
        id="label-long",
        entity_type="text",
        layer_ref="Legend",
        sequence_index=2,
        style={},
        geometry_json={
            "text": "LTHW-VT-F",
            "insertion": {"x": 10.3, "y": 4.5},
        },
        properties_json={},
    )

    call_count = 0

    async def _execute_side_effect(stmt: Any) -> Any:
        nonlocal call_count
        call_count += 1
        scalars_mock = MagicMock()
        if call_count == 1:
            scalars_mock.all.return_value = [anchor_row, label_row]
        else:
            scalars_mock.all.return_value = [anchor_row, long_line_row, label_row]
        result = MagicMock()
        result.scalars.return_value = scalars_mock
        return result

    db = AsyncMock()
    db.execute = AsyncMock(side_effect=_execute_side_effect)

    legend = await build_mech_service_legend_db(db, _REV_ID, input_family="dwg")

    assert isinstance(legend, MechanicalLegend)
    # Long line was rejected → no swatch candidates → empty legend
    ck = _ck({"index": 247, "rgb": "c2873f51", "by_layer": False, "by_block": False})
    assert legend.lookup(ck) is None, "Long line should be excluded by length gate"
