"""Unit tests for device identity resolution (Phase 2, epic #545).

Pure — no DB, no ORM, no fixtures beyond local fakes. Covers:
- Classification precedence (legend_exemplar / architecture / device / annotation / unknown).
- Resolution priority 1 (tag→abbreviation), 2 (block_ref→family), 3 (unresolved tag), 4 (none).
- Landmine guard: legend_exemplar is keyed on layer_ref ONLY, not block_ref.
- Conflict carry-through: competing_type_names surfaced from resolved LegendEntry.
- Determinism: same result regardless of input permutation order.
- Nothing is dropped: every instance emits a DeviceIdentity.
- Architecture basis string records matched pattern.
"""

from __future__ import annotations

import itertools

from app.interpretation.device_identity import (
    ARCHITECTURE_FAMILY_PATTERNS,
    BASIS_NONE,
    BASIS_SYMBOL_FAMILY,
    BASIS_TAG_ABBREVIATION,
    BASIS_UNRESOLVED_TAG,
    KIND_ANNOTATION,
    KIND_ARCHITECTURE,
    KIND_DEVICE,
    KIND_LEGEND_EXEMPLAR,
    KIND_UNKNOWN,
    RULE_VERSION,
    STATUS_RESOLVED,
    STATUS_UNKNOWN,
    STATUS_UNRESOLVED,
    DeviceIdentity,
    classify_instance_kind,
    resolve_device_identities,
)
from app.interpretation.devices import Device, DeviceTag
from app.interpretation.legend_dictionary import LegendDictionary, LegendEntry, fuse

# ---------------------------------------------------------------------------
# Helpers / fakes
# ---------------------------------------------------------------------------


def _tag(text: str, layer_ref: str | None = "E-FIRE-TAG") -> DeviceTag:
    return DeviceTag(entity_id="tag-1", text=text, layer_ref=layer_ref, distance=0.5)


def _device(
    entity_id: str = "d-1",
    block_ref: str | None = None,
    layer_ref: str | None = "E-FIRE",
    tag: DeviceTag | None = None,
) -> Device:
    return Device(
        entity_id=entity_id,
        sequence_index=0,
        depth=0,
        block_ref=block_ref,
        layer_ref=layer_ref,
        position={"x": 0.0, "y": 0.0},
        tag=tag,
    )


def _entry(
    abbreviation: str | None = None,
    symbol_family: str | None = None,
    type_name: str | None = None,
    description: str | None = None,
    competing_type_names: tuple[str, ...] = (),
) -> LegendEntry:
    return LegendEntry(
        abbreviation=abbreviation,
        symbol_family=symbol_family,
        type_name=type_name,
        description=description,
        sources=("block_family",),
        confidence=None,
        competing_type_names=competing_type_names,
    )


def _legend(*entries: LegendEntry) -> LegendDictionary:
    return fuse(list(entries))


_EMPTY_LEGEND = LegendDictionary(entries=())


# ---------------------------------------------------------------------------
# RULE_VERSION is accessible and a non-empty string
# ---------------------------------------------------------------------------


def test_rule_version_accessible() -> None:
    assert isinstance(RULE_VERSION, str)
    assert RULE_VERSION


# ---------------------------------------------------------------------------
# classify_instance_kind — legend_exemplar (Priority 1 / landmine guard)
# ---------------------------------------------------------------------------


def test_classify_legend_exemplar_by_layer() -> None:
    """Layer containing 'LEGEND' → legend_exemplar regardless of block_ref."""
    device = _device(
        block_ref="Smoke Detector with Beaconrfa -Legend Stage 4",
        layer_ref="LEGEND-SYMBOLS",
    )
    assert classify_instance_kind(device, _EMPTY_LEGEND) == KIND_LEGEND_EXEMPLAR


def test_classify_legend_exemplar_case_insensitive() -> None:
    device = _device(layer_ref="legend_items")
    assert classify_instance_kind(device, _EMPTY_LEGEND) == KIND_LEGEND_EXEMPLAR


def test_landmine_placed_device_not_exemplar() -> None:
    """The critical landmine: block_ref contains 'Legend Stage 4' but layer is Z000 → device."""
    legend = _legend(_entry(abbreviation="SD", type_name="Smoke Detector"))
    device = _device(
        block_ref="Smoke Detector with Beaconrfa -Legend Stage 4",
        layer_ref="Z000",
        tag=_tag("SD"),
    )
    kind = classify_instance_kind(device, legend)
    assert kind == KIND_DEVICE, (
        "Block_ref containing 'Legend Stage 4' must NOT trigger legend_exemplar — "
        "only layer_ref matters for that classification."
    )


def test_landmine_full_resolve_not_exemplar() -> None:
    """Full resolve path confirms the same landmine result end-to-end."""
    legend = _legend(_entry(abbreviation="SD", type_name="Smoke Detector"))
    device = _device(
        entity_id="placed",
        block_ref="Smoke Detector with Beaconrfa -Legend Stage 4",
        layer_ref="Z000",
        tag=_tag("SD"),
    )
    exemplar_device = _device(
        entity_id="exemplar",
        block_ref="Smoke Detector with Beaconrfa -Legend Stage 4",
        layer_ref="LEGEND-LAYER",
        tag=None,
    )
    results = resolve_device_identities([device, exemplar_device], legend)
    by_id = {r.entity_id: r for r in results}
    assert by_id["placed"].kind == KIND_DEVICE
    assert by_id["exemplar"].kind == KIND_LEGEND_EXEMPLAR


def test_classify_legend_exemplar_custom_match() -> None:
    device = _device(layer_ref="LKEY-SYMBOLS")
    kind = classify_instance_kind(device, _EMPTY_LEGEND, legend_layer_match="LKEY")
    assert kind == KIND_LEGEND_EXEMPLAR


# ---------------------------------------------------------------------------
# classify_instance_kind — architecture (Priority 2)
# ---------------------------------------------------------------------------


def test_classify_architecture_door() -> None:
    device = _device(block_ref="Door - Single Flush", layer_ref="A-DOOR")
    assert classify_instance_kind(device, _EMPTY_LEGEND) == KIND_ARCHITECTURE


def test_classify_architecture_grid() -> None:
    device = _device(block_ref="Grid Bubble A1", layer_ref="A-GRID")
    assert classify_instance_kind(device, _EMPTY_LEGEND) == KIND_ARCHITECTURE


def test_classify_architecture_case_insensitive() -> None:
    device = _device(block_ref="mullion_type_a", layer_ref="A-GLAZ")
    assert classify_instance_kind(device, _EMPTY_LEGEND) == KIND_ARCHITECTURE


def test_classify_architecture_patterns_tuple() -> None:
    assert isinstance(ARCHITECTURE_FAMILY_PATTERNS, tuple)
    assert len(ARCHITECTURE_FAMILY_PATTERNS) > 0


# ---------------------------------------------------------------------------
# classify_instance_kind — device (Priority 3)
# ---------------------------------------------------------------------------


def test_classify_device_by_tag_abbreviation() -> None:
    legend = _legend(_entry(abbreviation="FAP", type_name="Fire Alarm Panel"))
    device = _device(block_ref="FAPR-Block", tag=_tag("FAP"))
    assert classify_instance_kind(device, legend) == KIND_DEVICE


def test_classify_device_by_symbol_family() -> None:
    legend = _legend(_entry(symbol_family="Smoke Detector", type_name="Smoke Detector"))
    device = _device(block_ref="Smoke Detector", tag=None)
    assert classify_instance_kind(device, legend) == KIND_DEVICE


def test_classify_device_any_tag_present() -> None:
    """Even an unmatched tag → kind=device."""
    device = _device(block_ref="UnknownBlock", tag=_tag("XYZ"))
    assert classify_instance_kind(device, _EMPTY_LEGEND) == KIND_DEVICE


# ---------------------------------------------------------------------------
# classify_instance_kind — annotation (Priority 4)
# ---------------------------------------------------------------------------


def test_classify_annotation_north_arrow() -> None:
    device = _device(block_ref="North Arrow", layer_ref="A-ANNO", tag=None)
    assert classify_instance_kind(device, _EMPTY_LEGEND) == KIND_ANNOTATION


def test_classify_annotation_title_block() -> None:
    device = _device(block_ref="Title Block Type 1", layer_ref="A-ANNO", tag=None)
    assert classify_instance_kind(device, _EMPTY_LEGEND) == KIND_ANNOTATION


# ---------------------------------------------------------------------------
# classify_instance_kind — unknown (Priority 5)
# ---------------------------------------------------------------------------


def test_classify_unknown_no_tag_no_family() -> None:
    device = _device(block_ref="GENERIC-BLOCK", layer_ref="E-MISC", tag=None)
    assert classify_instance_kind(device, _EMPTY_LEGEND) == KIND_UNKNOWN


def test_classify_unknown_none_block_ref() -> None:
    device = _device(block_ref=None, layer_ref=None, tag=None)
    assert classify_instance_kind(device, _EMPTY_LEGEND) == KIND_UNKNOWN


# ---------------------------------------------------------------------------
# resolve_device_identities — Priority 1: tag → abbreviation
# ---------------------------------------------------------------------------


def test_resolve_tag_abbreviation_resolved() -> None:
    legend = _legend(
        _entry(
            abbreviation="DC",
            symbol_family=None,
            type_name="Door Contact",
            description="Magnetic contact",
        )
    )
    device = _device(entity_id="d1", block_ref="DC-Block", tag=_tag("DC"))
    results = resolve_device_identities([device], legend)
    assert len(results) == 1
    r = results[0]
    assert r.entity_id == "d1"
    assert r.kind == KIND_DEVICE
    assert r.status == STATUS_RESOLVED
    assert r.type_name == "Door Contact"
    assert r.abbreviation == "DC"
    assert r.description == "Magnetic contact"
    assert r.basis == BASIS_TAG_ABBREVIATION
    assert r.confidence is None
    assert r.competing_type_names == ()


def test_resolve_tag_abbreviation_source_layers_includes_tag_layer() -> None:
    legend = _legend(_entry(abbreviation="FAP", type_name="Fire Alarm Panel"))
    device = _device(
        layer_ref="E-FIRE",
        tag=DeviceTag(entity_id="t1", text="FAP", layer_ref="E-FIRE-TAG", distance=0.3),
    )
    results = resolve_device_identities([device], legend)
    r = results[0]
    assert "E-FIRE" in r.source_layers
    assert "E-FIRE-TAG" in r.source_layers


def test_resolve_tag_abbreviation_dedupes_same_layer() -> None:
    """When tag.layer_ref == device.layer_ref, source_layers has no duplicate."""
    legend = _legend(_entry(abbreviation="FAP", type_name="Fire Alarm Panel"))
    device = _device(
        layer_ref="E-FIRE",
        tag=DeviceTag(entity_id="t1", text="FAP", layer_ref="E-FIRE", distance=0.3),
    )
    results = resolve_device_identities([device], legend)
    assert results[0].source_layers.count("E-FIRE") == 1


# ---------------------------------------------------------------------------
# resolve_device_identities — Priority 2: block_ref → symbol_family
# ---------------------------------------------------------------------------


def test_resolve_symbol_family_resolved() -> None:
    legend = _legend(_entry(symbol_family="Heat Detector", type_name="Heat Detector"))
    device = _device(entity_id="d2", block_ref="Heat Detector", tag=None)
    results = resolve_device_identities([device], legend)
    r = results[0]
    assert r.status == STATUS_RESOLVED
    assert r.type_name == "Heat Detector"
    assert r.basis == BASIS_SYMBOL_FAMILY
    assert r.confidence is None


def test_resolve_symbol_family_no_tag_layer_in_source_layers() -> None:
    legend = _legend(_entry(symbol_family="Sprinkler Head", type_name="Sprinkler Head"))
    device = _device(layer_ref="P-FIRE", block_ref="Sprinkler Head", tag=None)
    results = resolve_device_identities([device], legend)
    assert results[0].source_layers == ("P-FIRE",)


# ---------------------------------------------------------------------------
# resolve_device_identities — Priority 3: unresolved tag
# ---------------------------------------------------------------------------


def test_resolve_unresolved_tag() -> None:
    device = _device(entity_id="d3", block_ref="UNKNOWN-BLOCK", tag=_tag("XYZ"))
    results = resolve_device_identities([device], _EMPTY_LEGEND)
    r = results[0]
    assert r.kind == KIND_DEVICE
    assert r.status == STATUS_UNRESOLVED
    assert r.abbreviation == "XYZ"
    assert r.type_name is None  # NEVER fabricate
    assert r.basis == BASIS_UNRESOLVED_TAG
    assert r.confidence is None


def test_resolve_unresolved_tag_no_fabricated_description() -> None:
    device = _device(tag=_tag("MYSTERY"))
    results = resolve_device_identities([device], _EMPTY_LEGEND)
    assert results[0].description is None


# ---------------------------------------------------------------------------
# resolve_device_identities — Priority 4: no tag, no family
# ---------------------------------------------------------------------------


def test_resolve_no_signal() -> None:
    device = _device(entity_id="d4", block_ref="BARE-BLOCK", tag=None)
    results = resolve_device_identities([device], _EMPTY_LEGEND)
    r = results[0]
    assert r.kind == KIND_UNKNOWN
    assert r.status == STATUS_UNKNOWN
    assert r.type_name is None
    assert r.basis == BASIS_NONE


# ---------------------------------------------------------------------------
# Conflict carry-through
# ---------------------------------------------------------------------------


def test_conflict_carry_through() -> None:
    """competing_type_names from LegendEntry must pass through; confidence stays None."""
    entry = LegendEntry(
        abbreviation="SD",
        symbol_family=None,
        type_name="Smoke Detector Ceiling",
        description=None,
        sources=("block_family",),
        confidence=None,
        competing_type_names=("Smoke Detector Duct",),
    )
    legend = LegendDictionary(entries=(entry,))
    device = _device(tag=_tag("SD"))
    results = resolve_device_identities([device], legend)
    r = results[0]
    assert r.status == STATUS_RESOLVED
    assert r.type_name == "Smoke Detector Ceiling"
    assert r.competing_type_names == ("Smoke Detector Duct",)
    assert r.confidence is None  # P2 does not pick a winner


# ---------------------------------------------------------------------------
# Nothing is dropped
# ---------------------------------------------------------------------------


def test_all_instances_emitted() -> None:
    legend = _legend(_entry(abbreviation="FAP", type_name="Fire Alarm Panel"))
    devices = [
        _device(entity_id="d1", block_ref="FAP-Blk", tag=_tag("FAP")),
        _device(entity_id="d2", block_ref="Door - Single", layer_ref="A-DOOR", tag=None),
        _device(entity_id="d3", block_ref=None, layer_ref="LEGEND-BASE", tag=None),
        _device(entity_id="d4", block_ref="BARE", layer_ref="E-MISC", tag=None),
    ]
    results = resolve_device_identities(devices, legend)
    assert len(results) == 4
    emitted_ids = {r.entity_id for r in results}
    assert emitted_ids == {"d1", "d2", "d3", "d4"}


# ---------------------------------------------------------------------------
# Architecture basis string
# ---------------------------------------------------------------------------


def test_architecture_basis_records_pattern() -> None:
    device = _device(block_ref="Window - Double Hung", layer_ref="A-GLAZ", tag=None)
    results = resolve_device_identities([device], _EMPTY_LEGEND)
    r = results[0]
    assert r.kind == KIND_ARCHITECTURE
    assert r.basis.startswith("architecture:")
    assert "Window" in r.basis


def test_architecture_type_name_none() -> None:
    device = _device(block_ref="Curtain Wall Panel", layer_ref="A-GLAZ", tag=None)
    results = resolve_device_identities([device], _EMPTY_LEGEND)
    r = results[0]
    assert r.type_name is None


# ---------------------------------------------------------------------------
# Legend exemplar emitted but not resolved
# ---------------------------------------------------------------------------


def test_legend_exemplar_emitted_with_no_type() -> None:
    device = _device(entity_id="ex1", block_ref="FAP-Blk", layer_ref="LEGEND-PANEL", tag=None)
    results = resolve_device_identities([device], _EMPTY_LEGEND)
    r = results[0]
    assert r.kind == KIND_LEGEND_EXEMPLAR
    assert r.type_name is None
    assert r.basis == BASIS_NONE


# ---------------------------------------------------------------------------
# Determinism — permutation invariance
# ---------------------------------------------------------------------------


def test_determinism_permutation_invariance() -> None:
    """Resolving the same inputs in any permutation order → identical sorted outputs."""
    legend = _legend(
        _entry(abbreviation="FAP", type_name="Fire Alarm Panel"),
        _entry(symbol_family="Heat Detector", type_name="Heat Detector"),
    )
    devices = [
        _device(entity_id="a", block_ref="FAP-Blk", layer_ref="E-FIRE", tag=_tag("FAP")),
        _device(entity_id="b", block_ref="Heat Detector", layer_ref="P-FIRE", tag=None),
        _device(entity_id="c", block_ref="BARE", layer_ref="E-MISC", tag=None),
        _device(entity_id="d", block_ref="XYZ-Blk", layer_ref="E-FIRE", tag=_tag("XYZ")),
    ]

    def _sorted(identities: list[DeviceIdentity]) -> list[DeviceIdentity]:
        return sorted(identities, key=lambda i: i.entity_id)

    baseline = _sorted(resolve_device_identities(devices, legend))

    for perm in itertools.islice(itertools.permutations(devices), 12):
        result = _sorted(resolve_device_identities(list(perm), legend))
        assert result == baseline, (
            f"Non-deterministic result for permutation {[d.entity_id for d in perm]}"
        )


# ---------------------------------------------------------------------------
# source_layers determinism and deduplication
# ---------------------------------------------------------------------------


def test_source_layers_no_duplicates() -> None:
    legend = _legend(_entry(abbreviation="DC", type_name="Door Contact"))
    layer = "E-FIRE"
    device = _device(
        layer_ref=layer,
        tag=DeviceTag(entity_id="t", text="DC", layer_ref=layer, distance=0.1),
    )
    results = resolve_device_identities([device], legend)
    layers = results[0].source_layers
    assert len(layers) == len(set(layers))


def test_source_layers_none_layer_ref() -> None:
    """Devices with no layer_ref emit empty source_layers tuple."""
    device = _device(layer_ref=None, tag=None, block_ref=None)
    results = resolve_device_identities([device], _EMPTY_LEGEND)
    assert results[0].source_layers == ()


# ---------------------------------------------------------------------------
# Priority 1 beats priority 2 when both could match
# ---------------------------------------------------------------------------


def test_tag_abbreviation_beats_symbol_family() -> None:
    """When both tag-abbreviation and symbol-family match, tag-abbreviation wins."""
    legend = _legend(
        _entry(abbreviation="FAP", symbol_family="FAP-Block", type_name="Fire Alarm Panel via Tag"),
    )
    device = _device(block_ref="FAP-Block", tag=_tag("FAP"))
    results = resolve_device_identities([device], legend)
    r = results[0]
    assert r.basis == BASIS_TAG_ABBREVIATION
    assert r.status == STATUS_RESOLVED


# ---------------------------------------------------------------------------
# Landmine — both directions on classify_instance_kind directly
# ---------------------------------------------------------------------------


def test_landmine_classify_both_directions() -> None:
    """The same block_ref, different layer_ref: one is exemplar, one is device."""
    legend = _legend(_entry(abbreviation="SD", type_name="Smoke Detector"))
    shared_block_ref = "Smoke Detector with Beaconrfa -Legend Stage 4"

    exemplar = _device(block_ref=shared_block_ref, layer_ref="%LEGEND%")
    placed = _device(block_ref=shared_block_ref, layer_ref="Z000", tag=_tag("SD"))

    assert classify_instance_kind(exemplar, legend) == KIND_LEGEND_EXEMPLAR, (
        "Layer containing LEGEND → exemplar regardless of block_ref"
    )
    assert classify_instance_kind(placed, legend) == KIND_DEVICE, (
        "Non-legend layer with matching tag → device, even if block_ref looks like a legend block"
    )


def test_legend_exemplar_none_layer_ref_not_classified_as_exemplar() -> None:
    """layer_ref=None cannot satisfy the legend-layer check; block_ref alone never triggers it."""
    device = _device(block_ref="Legend Stage 4 - Detector", layer_ref=None, tag=None)
    kind = classify_instance_kind(device, _EMPTY_LEGEND)
    assert kind != KIND_LEGEND_EXEMPLAR, (
        "layer_ref=None must not produce legend_exemplar — only layer_ref drives that check"
    )


# ---------------------------------------------------------------------------
# Architecture — all seven patterns tested in isolation
# ---------------------------------------------------------------------------


def test_classify_architecture_room_tag() -> None:
    device = _device(block_ref="Room Tag - Standard", layer_ref="A-ROOM")
    assert classify_instance_kind(device, _EMPTY_LEGEND) == KIND_ARCHITECTURE


def test_classify_architecture_gate() -> None:
    device = _device(block_ref="Gate - Sliding", layer_ref="A-DOOR")
    assert classify_instance_kind(device, _EMPTY_LEGEND) == KIND_ARCHITECTURE


def test_classify_architecture_window() -> None:
    device = _device(block_ref="Window - Fixed", layer_ref="A-GLAZ")
    assert classify_instance_kind(device, _EMPTY_LEGEND) == KIND_ARCHITECTURE


def test_classify_architecture_curtain() -> None:
    device = _device(block_ref="Curtain Wall Type B", layer_ref="A-GLAZ")
    assert classify_instance_kind(device, _EMPTY_LEGEND) == KIND_ARCHITECTURE


def test_architecture_resolve_type_name_is_none_for_all_patterns() -> None:
    """Every architecture pattern emits type_name=None."""
    for pattern in ARCHITECTURE_FAMILY_PATTERNS:
        device = _device(block_ref=f"{pattern} some-variant", layer_ref="A-ARCH")
        results = resolve_device_identities([device], _EMPTY_LEGEND)
        r = results[0]
        assert r.kind == KIND_ARCHITECTURE, f"pattern {pattern!r} should yield kind=architecture"
        assert r.type_name is None, f"pattern {pattern!r} should yield type_name=None"


# ---------------------------------------------------------------------------
# Conflict carry-through via symbol_family path
# ---------------------------------------------------------------------------


def test_conflict_carry_through_symbol_family_path() -> None:
    """competing_type_names are carried through the symbol_family (priority 2) path."""
    entry = LegendEntry(
        abbreviation=None,
        symbol_family="Duct Detector",
        type_name="Duct Smoke Detector A",
        description=None,
        sources=("block_family",),
        confidence=None,
        competing_type_names=("Duct Smoke Detector B",),
    )
    legend = LegendDictionary(entries=(entry,))
    device = _device(block_ref="Duct Detector", tag=None)
    results = resolve_device_identities([device], legend)
    r = results[0]
    assert r.basis == BASIS_SYMBOL_FAMILY
    assert r.status == STATUS_RESOLVED
    assert r.competing_type_names == ("Duct Smoke Detector B",)
    assert r.confidence is None


# ---------------------------------------------------------------------------
# block_ref=None with tag present → device (not crash, not unknown)
# ---------------------------------------------------------------------------


def test_block_ref_none_with_tag_classifies_as_device() -> None:
    """block_ref=None + tag present → kind=device (unresolved path)."""
    device = _device(block_ref=None, layer_ref="E-FIRE", tag=_tag("XYZ"))
    kind = classify_instance_kind(device, _EMPTY_LEGEND)
    assert kind == KIND_DEVICE


def test_block_ref_none_with_tag_resolves_as_unresolved() -> None:
    device = _device(block_ref=None, layer_ref="E-FIRE", tag=_tag("NOPE"))
    results = resolve_device_identities([device], _EMPTY_LEGEND)
    r = results[0]
    assert r.status == STATUS_UNRESOLVED
    assert r.abbreviation == "NOPE"
    assert r.type_name is None
    assert r.basis == BASIS_UNRESOLVED_TAG


# ---------------------------------------------------------------------------
# tag.layer_ref=None excluded from source_layers
# ---------------------------------------------------------------------------


def test_tag_layer_ref_none_excluded_from_source_layers() -> None:
    """When tag.layer_ref is None, no None should appear in source_layers."""
    legend = _legend(_entry(abbreviation="SD", type_name="Smoke Detector"))
    device = _device(
        layer_ref="E-FIRE",
        tag=DeviceTag(entity_id="t1", text="SD", layer_ref=None, distance=0.5),
    )
    results = resolve_device_identities([device], legend)
    r = results[0]
    assert None not in r.source_layers, "None must never appear in source_layers"
    assert r.source_layers == ("E-FIRE",)


# ---------------------------------------------------------------------------
# entity_id correctly threaded through all four resolution paths
# ---------------------------------------------------------------------------


def test_entity_id_threaded_symbol_family_path() -> None:
    legend = _legend(_entry(symbol_family="Heat Detector", type_name="Heat Detector"))
    device = _device(entity_id="eid-sym", block_ref="Heat Detector", tag=None)
    results = resolve_device_identities([device], legend)
    assert results[0].entity_id == "eid-sym"


def test_entity_id_threaded_unresolved_tag_path() -> None:
    device = _device(entity_id="eid-unresolved", block_ref="BLK", tag=_tag("UNKN"))
    results = resolve_device_identities([device], _EMPTY_LEGEND)
    assert results[0].entity_id == "eid-unresolved"


def test_entity_id_threaded_no_signal_path() -> None:
    device = _device(entity_id="eid-none", block_ref="BLK", tag=None)
    results = resolve_device_identities([device], _EMPTY_LEGEND)
    assert results[0].entity_id == "eid-none"


def test_entity_id_threaded_architecture_path() -> None:
    device = _device(entity_id="eid-arch", block_ref="Door - Single", layer_ref="A-DOOR")
    results = resolve_device_identities([device], _EMPTY_LEGEND)
    assert results[0].entity_id == "eid-arch"


def test_entity_id_threaded_legend_exemplar_path() -> None:
    device = _device(entity_id="eid-exemplar", block_ref="FAP-Blk", layer_ref="LEGEND-BASE")
    results = resolve_device_identities([device], _EMPTY_LEGEND)
    assert results[0].entity_id == "eid-exemplar"


# ---------------------------------------------------------------------------
# Empty LegendDictionary — all paths that touch legend lookups remain safe
# ---------------------------------------------------------------------------


def test_empty_legend_does_not_crash_for_any_device_shape() -> None:
    """A variety of device shapes against an empty legend should all emit exactly one identity."""
    devices = [
        _device(entity_id="e1", block_ref="FAP-Blk", layer_ref="E-FIRE", tag=_tag("FAP")),
        _device(entity_id="e2", block_ref="FAP-Blk", layer_ref="E-FIRE", tag=None),
        _device(entity_id="e3", block_ref=None, layer_ref=None, tag=None),
        _device(entity_id="e4", block_ref=None, layer_ref="LEGEND-BASE", tag=None),
        _device(entity_id="e5", block_ref="Door - Single", layer_ref="A-DOOR", tag=None),
        _device(entity_id="e6", block_ref="North Arrow", layer_ref="A-ANNO", tag=None),
    ]
    results = resolve_device_identities(devices, _EMPTY_LEGEND)
    assert len(results) == len(devices)
    emitted = {r.entity_id for r in results}
    assert emitted == {d.entity_id for d in devices}


# ---------------------------------------------------------------------------
# Count match — large heterogeneous batch, nothing dropped
# ---------------------------------------------------------------------------


def test_count_matches_input_length_large_batch() -> None:
    """Every input device in a heterogeneous batch of 20 produces exactly one identity."""
    legend = _legend(
        _entry(abbreviation="SD", type_name="Smoke Detector"),
        _entry(symbol_family="Heat Detector", type_name="Heat Detector"),
    )
    devices = []
    for i in range(5):
        devices.append(_device(entity_id=f"tag-{i}", block_ref="SD-Blk", tag=_tag("SD")))
    for i in range(5):
        devices.append(_device(entity_id=f"fam-{i}", block_ref="Heat Detector", tag=None))
    for i in range(5):
        devices.append(_device(entity_id=f"unres-{i}", block_ref="UNK", tag=_tag(f"T{i}")))
    for i in range(5):
        devices.append(_device(entity_id=f"bare-{i}", block_ref=f"BARE-{i}", tag=None))

    results = resolve_device_identities(devices, legend)
    assert len(results) == 20


# ---------------------------------------------------------------------------
# Fix 1 — case/whitespace-insensitive tag-abbreviation lookup
# ---------------------------------------------------------------------------


def test_lowercase_tag_resolves_via_uppercase_abbreviation_key() -> None:
    """A lowercase tag should hit the uppercase legend key and resolve correctly."""
    legend = _legend(_entry(abbreviation="SD", type_name="Smoke Detector"))
    device = _device(entity_id="d-lower", tag=_tag("sd"))
    results = resolve_device_identities([device], legend)
    r = results[0]
    assert r.kind == KIND_DEVICE
    assert r.status == STATUS_RESOLVED
    assert r.basis == BASIS_TAG_ABBREVIATION
    assert r.type_name == "Smoke Detector"
    # abbreviation on resolved path reflects the legend entry
    assert r.abbreviation == "SD"


def test_padded_tag_resolves_via_abbreviation_key() -> None:
    """A whitespace-padded tag should resolve the same as the trimmed uppercase key."""
    legend = _legend(_entry(abbreviation="FAP", type_name="Fire Alarm Panel"))
    device = _device(entity_id="d-padded", tag=_tag(" FAP "))
    results = resolve_device_identities([device], legend)
    r = results[0]
    assert r.status == STATUS_RESOLVED
    assert r.basis == BASIS_TAG_ABBREVIATION
    assert r.type_name == "Fire Alarm Panel"


def test_mixed_case_tag_resolves_via_abbreviation_key() -> None:
    """Mixed-case tag (e.g. 'Sd') should resolve against the uppercase legend key."""
    legend = _legend(_entry(abbreviation="SD", type_name="Smoke Detector"))
    device = _device(entity_id="d-mixed", tag=_tag("Sd"))
    results = resolve_device_identities([device], legend)
    r = results[0]
    assert r.status == STATUS_RESOLVED
    assert r.type_name == "Smoke Detector"


def test_unresolved_raw_tag_preserves_original_text() -> None:
    """On the unresolved path, abbreviation preserves the original (non-normalized) tag text."""
    device = _device(entity_id="d-raw", tag=_tag(" xyz "))
    results = resolve_device_identities([device], _EMPTY_LEGEND)
    r = results[0]
    assert r.status == STATUS_UNRESOLVED
    assert r.basis == BASIS_UNRESOLVED_TAG
    # The raw tag text must be preserved, NOT the upper-cased form
    assert r.abbreviation == " xyz "


def test_lowercase_tag_classify_as_device_via_abbreviation() -> None:
    """classify_instance_kind recognizes a lowercase tag against an uppercase legend key."""
    legend = _legend(_entry(abbreviation="DC", type_name="Door Contact"))
    device = _device(block_ref="DC-Block", tag=_tag("dc"))
    assert classify_instance_kind(device, legend) == KIND_DEVICE


# ---------------------------------------------------------------------------
# Precedence (corrected from real-data #545 finding): a legend-ICON family
# (Source A, scoped to legend-marked families) beats architecture; a stray
# legend-KEY *tag* does NOT — mullions/columns carry spurious nearby tags.
# ---------------------------------------------------------------------------


def test_legend_icon_family_beats_architectural_block_ref() -> None:
    """A block whose family IS a legend device family → kind=device, even if the family
    name contains an architecture substring (the legend icon is the strong signal)."""
    legend = _legend(_entry(symbol_family="Magnetic Door Holder", type_name="Magnetic Door Holder"))
    device = _device(
        entity_id="d-icon",
        block_ref="Magnetic Door Holder",
        layer_ref="E-FIRE",
        tag=None,
    )
    kind = classify_instance_kind(device, legend)
    assert kind == KIND_DEVICE, "A legend-icon family must override an architecture substring"


def test_legend_icon_family_beats_architectural_block_ref_full_resolve() -> None:
    """Full resolve: a legend-icon family is a resolved device (basis=symbol_family)."""
    legend = _legend(_entry(symbol_family="Magnetic Door Holder", type_name="Magnetic Door Holder"))
    device = _device(
        entity_id="d-icon-full",
        block_ref="Magnetic Door Holder",
        layer_ref="E-FIRE",
        tag=None,
    )
    results = resolve_device_identities([device], legend)
    r = results[0]
    assert r.kind == KIND_DEVICE
    assert r.status == STATUS_RESOLVED
    assert r.type_name == "Magnetic Door Holder"
    assert r.basis == BASIS_SYMBOL_FAMILY


def test_architecture_beats_legend_key_tag() -> None:
    """A stray legend-KEY tag on an architectural family does NOT promote it to a device —
    a mullion/column carrying a nearby 'H'/'MDH' tag is still architecture (#545)."""
    legend = _legend(_entry(abbreviation="MDH", type_name="Magnetic Door Holder"))
    device = _device(
        entity_id="d-arch-tag",
        block_ref="Rectangular Door Mullion",
        layer_ref="A-GLAZ",
        tag=_tag("MDH"),
    )
    kind = classify_instance_kind(device, legend)
    assert kind == KIND_ARCHITECTURE, (
        "An architecture family must not be promoted by a stray legend-key tag"
    )


def test_architectural_block_ref_without_legend_hit_stays_architecture() -> None:
    """block_ref with 'Door' and NO tag/legend hit → architecture (unchanged behavior)."""
    device = _device(
        entity_id="d-arch-only",
        block_ref="Door - Single Flush",
        layer_ref="A-DOOR",
        tag=None,
    )
    kind = classify_instance_kind(device, _EMPTY_LEGEND)
    assert kind == KIND_ARCHITECTURE


def test_legend_exemplar_layer_beats_legend_resolvable_tag() -> None:
    """legend_exemplar (layer match) wins even when the tag is legend-resolvable."""
    legend = _legend(_entry(abbreviation="SD", type_name="Smoke Detector"))
    device = _device(
        entity_id="d-exemplar-wins",
        block_ref="SD-Block",
        layer_ref="LEGEND-ITEMS",
        tag=_tag("SD"),
    )
    kind = classify_instance_kind(device, legend)
    assert kind == KIND_LEGEND_EXEMPLAR, (
        "legend_exemplar (keyed on layer) must still beat legend-resolvable tag"
    )
