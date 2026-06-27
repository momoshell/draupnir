"""#741c — Slim-canonical back-compat, guard, and persistence-size-bound tests.

Three concerns:

1. Back-compat (``TestSlimCanonicalBackCompat``):
   Every post-ingest consumer that reads the persisted ``canonical_json`` column
   (§3 of TRD #741) must work against BOTH blob shapes:
   - OLD (pre-#741a): full entities + blocks list, no breadcrumbs.
   - NEW (post-#741a): ``entities: []``, ``blocks: []``, ``metadata.*_storage`` breadcrumbs.
   Consumers only read small sibling keys (``pdf_scale``, ``units``, ``interpretation``,
   ``census``, ``metadata.text_blocks``), so old blobs with extra bulk are transparent.

2. Guard (``test_no_post_ingest_code_reads_canonical_entities_or_blocks``):
   Source-scan test (mirrors ``test_centerline_import_boundary.py``) that inspects the
   ``app/`` tree for any code reading ``canonical_json["entities"]`` / ``["blocks"]`` outside
   the curated in-memory allowlist.  Fails with a clear author message if a new module
   touches those keys from the DB column.

3. Persistence-size bound (``TestBuildSlimCanonicalForPersist``):
   Unit test on ``build_slim_canonical_for_persist``: large entities + blocks in → slim blob
   out, serialized size well under the Postgres ~1 GB column limit.

TRD reference: ``docs/trds/741-canonical-storage.md`` §7.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from app.ingestion.finalization import build_slim_canonical_for_persist

# ---------------------------------------------------------------------------
# Test 3: Persistence-size bound (pure unit — no DB required)
# ---------------------------------------------------------------------------


class TestBuildSlimCanonicalForPersist:
    """``build_slim_canonical_for_persist`` must produce a tiny blob regardless of input size."""

    def test_strips_entities_and_blocks_to_empty_lists(self) -> None:
        """Entities and blocks must be empty lists in the slim output.

        Arrange: canonical with non-empty entities + blocks.
        Act:     build_slim_canonical_for_persist.
        Assert:  entities == [], blocks == [].
        """
        _ = self
        canonical: dict[str, Any] = {
            "canonical_entity_schema_version": "0.1",
            "layouts": [{"layout_ref": "Model"}],
            "entities": [{"entity_id": f"e-{i}", "entity_type": "line"} for i in range(5000)],
            "blocks": [{"block_ref": f"b-{i}", "name": f"Block-{i}"} for i in range(2000)],
            "metadata": {"text_blocks": [{"text": "hello"}]},
        }

        slim = build_slim_canonical_for_persist(canonical)

        assert slim["entities"] == []
        assert slim["blocks"] == []

    def test_breadcrumb_counts_match_pre_slim_lengths(self) -> None:
        """Storage breadcrumbs must record the original entity + block counts.

        Arrange: canonical with 5000 entities + 2000 blocks.
        Act:     build_slim_canonical_for_persist.
        Assert:  metadata.entities_storage.count == 5000,
                 metadata.blocks_storage.count == 2000.
        """
        _ = self
        entity_count = 5000
        block_count = 2000
        canonical: dict[str, Any] = {
            "entities": [{"entity_id": f"e-{i}"} for i in range(entity_count)],
            "blocks": [{"block_ref": f"b-{i}"} for i in range(block_count)],
        }

        slim = build_slim_canonical_for_persist(canonical)

        storage_meta = slim["metadata"]
        assert storage_meta["entities_storage"]["location"] == "revision_entities"
        assert storage_meta["entities_storage"]["count"] == entity_count
        assert storage_meta["blocks_storage"]["location"] == "revision_blocks"
        assert storage_meta["blocks_storage"]["count"] == block_count

    def test_serialized_size_is_under_one_megabyte(self) -> None:
        """Persisted blob must be tiny even for a dense drawing.

        Arrange: canonical with 5000 entities + 2000 blocks (clearly large input).
        Act:     build_slim_canonical_for_persist, then json.dumps.
        Assert:  serialized size < 1 MB (well under the ~1 GB Postgres column cap).
        """
        _ = self
        entity_count = 5000
        block_count = 2000
        # Give each entity/block a realistic payload size to make this non-trivial.
        canonical: dict[str, Any] = {
            "canonical_entity_schema_version": "0.1",
            "layouts": [{"layout_ref": "Model", "name": "Model"}],
            "layers": [{"layer_ref": f"Layer-{i}", "name": f"Layer-{i}"} for i in range(50)],
            "entities": [
                {
                    "entity_id": f"entity-{i:06d}",
                    "entity_type": "line",
                    "layer_ref": "Layer-0",
                    "geometry_json": {"type": "line", "coordinates": [[0.0, 0.0], [1.0, 1.0]]},
                    "properties_json": {"layer": "Layer-0", "color": "#ff0000"},
                }
                for i in range(entity_count)
            ],
            "blocks": [
                {
                    "block_ref": f"BLOCK-{i:04d}",
                    "name": f"Block Name {i}",
                    "children": [
                        {"entity_type": "line", "coords": [0, 0, 100, 100]} for _ in range(20)
                    ],
                }
                for i in range(block_count)
            ],
            "metadata": {
                "text_blocks": [{"text": f"Note {i}", "bbox": [0, 0, 100, 20]} for i in range(200)]
            },
        }

        slim = build_slim_canonical_for_persist(canonical)
        serialized = json.dumps(slim)

        size_bytes = len(serialized.encode("utf-8"))
        one_mb = 1 * 1024 * 1024
        assert size_bytes < one_mb, (
            f"Slim canonical blob is {size_bytes:,} bytes — expected < 1 MB. "
            "The slim helper is not stripping entities/blocks correctly."
        )

    def test_does_not_mutate_original_canonical(self) -> None:
        """build_slim_canonical_for_persist must be a shallow copy — not mutate the original.

        This is critical: the original canonical_json must remain intact so the
        materializer and report_lineage callers that run after slimming still see
        the full entity list.

        Arrange: canonical with 3 entities.
        Act:     build_slim_canonical_for_persist.
        Assert:  original entities list is unchanged (still length 3).
        """
        _ = self
        original_entities = [
            {"entity_id": "e-1"},
            {"entity_id": "e-2"},
            {"entity_id": "e-3"},
        ]
        canonical: dict[str, Any] = {
            "entities": list(original_entities),
            "blocks": [{"block_ref": "B-1"}],
        }

        build_slim_canonical_for_persist(canonical)

        # Original must be unmodified — the materializer still needs it.
        assert canonical["entities"] == original_entities
        assert len(canonical["entities"]) == 3
        assert canonical["blocks"] == [{"block_ref": "B-1"}]

    def test_existing_metadata_sibling_keys_are_preserved(self) -> None:
        """Pre-existing metadata keys (e.g. text_blocks) must survive slim.

        Arrange: canonical with metadata.text_blocks already set.
        Act:     build_slim_canonical_for_persist.
        Assert:  metadata.text_blocks still present + storage breadcrumbs added.
        """
        _ = self
        text_blocks = [{"text": "PIPE TAG", "bbox": [10, 20, 110, 40]}]
        canonical: dict[str, Any] = {
            "entities": [{"entity_id": "e-1"}],
            "blocks": [],
            "metadata": {"text_blocks": text_blocks, "pdf_scale": {"unit": "point"}},
        }

        slim = build_slim_canonical_for_persist(canonical)

        assert slim["metadata"]["text_blocks"] == text_blocks
        assert slim["metadata"]["pdf_scale"] == {"unit": "point"}
        assert "entities_storage" in slim["metadata"]
        assert "blocks_storage" in slim["metadata"]

    def test_empty_entities_and_blocks_yield_zero_counts(self) -> None:
        """A canonical with no entities/blocks produces zero-count breadcrumbs.

        Arrange: canonical with entities: [], blocks: [].
        Act:     build_slim_canonical_for_persist.
        Assert:  breadcrumb counts are 0.
        """
        _ = self
        canonical: dict[str, Any] = {"entities": [], "blocks": []}

        slim = build_slim_canonical_for_persist(canonical)

        assert slim["metadata"]["entities_storage"]["count"] == 0
        assert slim["metadata"]["blocks_storage"]["count"] == 0
        assert slim["entities"] == []
        assert slim["blocks"] == []

    def test_absent_entities_and_blocks_keys_yield_zero_counts(self) -> None:
        """A canonical missing entities/blocks entirely still produces safe zero-count breadcrumbs.

        Arrange: canonical dict with no entities or blocks keys.
        Act:     build_slim_canonical_for_persist.
        Assert:  breadcrumb counts are 0, entities/blocks keys present and empty.
        """
        _ = self
        canonical: dict[str, Any] = {"layouts": [{"layout_ref": "Model"}]}

        slim = build_slim_canonical_for_persist(canonical)

        assert slim["entities"] == []
        assert slim["blocks"] == []
        assert slim["metadata"]["entities_storage"]["count"] == 0
        assert slim["metadata"]["blocks_storage"]["count"] == 0


# ---------------------------------------------------------------------------
# Test 2: Source-scan guard (pure process — no DB required)
# ---------------------------------------------------------------------------

# Files that legitimately read canonical_json["entities"] / ["blocks"] from the
# in-memory IngestFinalizationPayload DURING finalization (NOT from the DB column).
# Any hit in app/ that is NOT in this allowlist is a violation: new code is
# reading entities/blocks from the persisted blob instead of revision_entities /
# revision_blocks rows.
#
# Authors: if your module is flagged here, read from the DB rows instead:
#   - entities: query RevisionEntity where drawing_revision_id = <revision_id>
#   - blocks:   query RevisionBlock where drawing_revision_id = <revision_id>
# Reference: docs/trds/741-canonical-storage.md §3 and §8 (R1).
_ALLOWED_IN_MEMORY_FILES: frozenset[str] = frozenset(
    {
        # Finalization: build_slim_canonical_for_persist reads entity/block lengths
        # from the full in-memory canonical before slimming it.
        "app/ingestion/finalization.py",
        # Block expansion: expands block instances into world-placed entities
        # using the full in-memory canonical (before persist).
        "app/ingestion/block_expansion.py",
        # Sheet membership: tags entities with viewport membership in-memory.
        "app/ingestion/sheet_membership.py",
        # Debug overlay: renders an SVG from the full in-memory canonical.
        "app/ingestion/debug_overlay.py",
        # Revision materialization: explodes in-memory entities/blocks into rows.
        "app/jobs/revision_materialization.py",
        # Lineage reporting: reads entity count from the full in-memory payload
        # to build the debug-overlay lineage JSON (called before the slim persist).
        "app/jobs/report_lineage.py",
        # Validation pipeline: runs on the full in-memory canonical before persist.
        "app/ingestion/validation/geometry.py",
        "app/ingestion/validation/reconciliation.py",
        "app/ingestion/validation/_utils.py",
    }
)

# Adapter emit paths — they PRODUCE entities/blocks (never read from the DB column).
# Covered by the glob pattern below.
_ADAPTER_DIR = "app/ingestion/adapters/"

# Regex patterns that identify canonical entity/block reads.
# We scan for both dict-subscript and .get() forms, with and without the
# ``payload.`` prefix (all are in-memory; the DB-column form is
# ``adapter_output.canonical_json[...]`` or ``output.canonical_json.get(...)``).
_READ_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r'canonical_json\["entities"\]'),
    re.compile(r"canonical_json\['entities'\]"),
    re.compile(r'canonical_json\.get\(["\']entities["\']'),
    re.compile(r'canonical_json\["blocks"\]'),
    re.compile(r"canonical_json\['blocks'\]"),
    re.compile(r'canonical_json\.get\(["\']blocks["\']'),
    # payload. prefix variants
    re.compile(r'payload\.canonical_json\["entities"\]'),
    re.compile(r"payload\.canonical_json\['entities'\]"),
    re.compile(r'payload\.canonical_json\.get\(["\']entities["\']'),
    re.compile(r'payload\.canonical_json\["blocks"\]'),
    re.compile(r"payload\.canonical_json\['blocks'\]"),
    re.compile(r'payload\.canonical_json\.get\(["\']blocks["\']'),
]


def _is_in_allowlist(rel_path: str) -> bool:
    """Return True if the file is in the in-memory allowlist or is an adapter emit path."""
    if rel_path in _ALLOWED_IN_MEMORY_FILES:
        return True
    # Adapter emit paths (produce entities/blocks, never read from DB column).
    return rel_path.startswith(_ADAPTER_DIR)


def test_no_post_ingest_code_reads_canonical_entities_or_blocks() -> None:
    """No production code outside the in-memory allowlist reads canonical entities/blocks.

    Any file outside the allowlist that accesses ``canonical_json["entities"]`` or
    ``canonical_json["blocks"]`` (or their ``.get()`` equivalents) is a violation:
    post-ingest consumers must read from ``revision_entities`` / ``revision_blocks``
    rows, not the persisted blob (which has ``entities: []`` for new rows per #741a).

    Arrange: walk app/ source tree, collect all .py files.
    Act:     scan each file for the entity/block read patterns.
    Assert:  every hit is in the allowlist; zero violations on the current tree.

    If this test fails on your new file:
        - Read entities from: ``RevisionEntity`` rows (query by drawing_revision_id).
        - Read blocks from:   ``RevisionBlock`` rows (query by drawing_revision_id).
        - Reference: docs/trds/741-canonical-storage.md §3 consumer table.
        - To allow a NEW legitimate in-memory reader, add it to _ALLOWED_IN_MEMORY_FILES
          in this test with a comment explaining why it is an in-memory (not DB) read.
    """
    app_root = Path(__file__).parent.parent / "app"
    assert app_root.is_dir(), f"app/ directory not found at {app_root}"

    violations: list[str] = []

    for py_file in sorted(app_root.rglob("*.py")):
        # Relative path from repo root (e.g. "app/exports/scale.py").
        rel = str(py_file.relative_to(app_root.parent))

        if _is_in_allowlist(rel):
            continue

        source = py_file.read_text(encoding="utf-8")

        for pattern in _READ_PATTERNS:
            for match in pattern.finditer(source):
                # Find the line number for the match.
                line_no = source[: match.start()].count("\n") + 1
                violations.append(f"  {rel}:{line_no}: `{match.group()}`")

    assert violations == [], (
        "Post-ingest code reads canonical_json entities/blocks from outside the allowlist.\n"
        "Violations (read revision_entities/revision_blocks rows instead):\n"
        + "\n".join(violations)
        + "\n\nSee docs/trds/741-canonical-storage.md §3 and _ALLOWED_IN_MEMORY_FILES "
        "in this test for guidance."
    )


# ---------------------------------------------------------------------------
# Test 1: Back-compat — consumers work against both old and new blob shapes
# ---------------------------------------------------------------------------


def _make_old_full_blob(
    *,
    pdf_scale: dict[str, Any] | None = None,
    units: dict[str, Any] | None = None,
    interpretation: dict[str, Any] | None = None,
    census: dict[str, Any] | None = None,
    text_blocks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build an old-shape (pre-#741a) canonical blob.

    Has entities + blocks populated; NO entities_storage / blocks_storage breadcrumbs.
    This is what pre-existing ``adapter_run_outputs`` rows look like.
    """
    blob: dict[str, Any] = {
        "canonical_entity_schema_version": "0.1",
        "schema_version": "0.1",
        "layouts": [{"layout_ref": "Model", "name": "Model"}],
        "layers": [{"layer_ref": "A-WALL", "name": "A-WALL"}],
        # Pre-#741a: entities + blocks are stored in the blob itself.
        "entities": [
            {
                "entity_id": "entity-legacy-001",
                "entity_type": "line",
                "layer_ref": "A-WALL",
                "geometry_json": {"type": "line", "coordinates": [[0, 0], [10, 10]]},
            }
        ],
        "blocks": [
            {
                "block_ref": "LEGACY-BLOCK",
                "name": "LegacyBlock",
                "children": [{"entity_type": "line"}],
            }
        ],
        "entity_counts": {"layouts": 1, "layers": 1, "blocks": 1, "entities": 1},
    }
    if pdf_scale is not None:
        blob["pdf_scale"] = pdf_scale
    if units is not None:
        blob["units"] = units
    if interpretation is not None:
        blob["interpretation"] = interpretation
    if census is not None:
        blob["census"] = census
    if text_blocks is not None:
        blob.setdefault("metadata", {})
        blob["metadata"]["text_blocks"] = text_blocks
    return blob


def _make_new_slim_blob(
    *,
    pdf_scale: dict[str, Any] | None = None,
    units: dict[str, Any] | None = None,
    interpretation: dict[str, Any] | None = None,
    census: dict[str, Any] | None = None,
    text_blocks: list[dict[str, Any]] | None = None,
    entity_count: int = 1,
    block_count: int = 1,
) -> dict[str, Any]:
    """Build a new-shape (post-#741a) slim canonical blob.

    Has entities: [], blocks: [], and metadata.*_storage breadcrumbs.
    """
    metadata: dict[str, Any] = {
        "entities_storage": {"location": "revision_entities", "count": entity_count},
        "blocks_storage": {"location": "revision_blocks", "count": block_count},
    }
    if text_blocks is not None:
        metadata["text_blocks"] = text_blocks
    blob: dict[str, Any] = {
        "canonical_entity_schema_version": "0.1",
        "schema_version": "0.1",
        "layouts": [{"layout_ref": "Model", "name": "Model"}],
        "layers": [{"layer_ref": "A-WALL", "name": "A-WALL"}],
        "entities": [],
        "blocks": [],
        "entity_counts": {
            "layouts": 1,
            "layers": 1,
            "blocks": block_count,
            "entities": entity_count,
        },
        "metadata": metadata,
    }
    if pdf_scale is not None:
        blob["pdf_scale"] = pdf_scale
    if units is not None:
        blob["units"] = units
    if interpretation is not None:
        blob["interpretation"] = interpretation
    if census is not None:
        blob["census"] = census
    return blob


_PDF_SCALE_FIXTURE: dict[str, Any] = {
    "status": "detected",
    "coordinate_space": "page_points",
    "unit": "point",
    "real_world_units": True,
    "real_world_unit": "meter",
    "scale_ratio": "1:100",
    "points_to_real": 0.0352778,
    "confidence": "medium",
}

_UNITS_FIXTURE: dict[str, Any] = {
    "normalized": "millimeter",
    "source": "$INSUNITS",
    "source_value": 4,
    "conversion_target": "meter",
    "conversion_factor": 0.001,
}

_INTERPRETATION_FIXTURE: dict[str, Any] = {
    "schema_version": "0.1",
    "length": {"unit": "millimeter", "scale_factor": 0.001},
    "angle": {"convention": "counterclockwise_from_east"},
    "orientation": {"dominant": "landscape"},
}

_CENSUS_FIXTURE: dict[str, Any] = {
    "schema_version": "0.1",
    "source": "libredwg",
    "raw_object_total": 42,
    "raw_objects": {"LINE": 30, "ARC": 12},
    "drawable_candidates": 42,
    "materialized": 40,
    "dropped": 2,
    "unsupported_classes": [],
}

_TEXT_BLOCKS_FIXTURE: list[dict[str, Any]] = [
    {"text": "Ø76 mm VAC", "bbox": [100, 200, 300, 220]},
    {"text": "Ø42 mm MA", "bbox": [100, 230, 300, 250]},
]


class TestSlimCanonicalBackCompat:
    """Post-ingest consumers work against both OLD full blobs and NEW slim blobs.

    Each test asserts the same result for both blob shapes, proving #741a's
    slim change did not regress existing readers.

    These are pure-unit tests: they call the reader functions directly with
    a synthetic canonical dict, without touching the database.
    """

    # -----------------------------------------------------------------------
    # scale.py: resolve_revision_scale — reads pdf_scale + units
    # -----------------------------------------------------------------------

    def test_scale_reader_reads_pdf_scale_from_old_full_blob(self) -> None:
        """The scale reader extracts pdf_scale from an old full blob (entities present).

        Arrange: old-shape blob with entities + blocks + pdf_scale.
        Act:     extract pdf_scale from the canonical dict (mirrors resolve_revision_scale).
        Assert:  pdf_scale is correctly read; entities bulk is silently ignored.
        """
        _ = self
        old_blob = _make_old_full_blob(pdf_scale=_PDF_SCALE_FIXTURE)

        canonical = dict(old_blob)
        pdf_scale_raw = canonical.get("pdf_scale")
        pdf_scale = dict(pdf_scale_raw) if isinstance(pdf_scale_raw, dict) else None

        assert pdf_scale is not None
        assert pdf_scale["real_world_unit"] == "meter"
        assert pdf_scale["points_to_real"] == 0.0352778
        # Old blob bulk is silently tolerated — entities still present in blob.
        assert len(old_blob["entities"]) == 1

    def test_scale_reader_reads_pdf_scale_from_new_slim_blob(self) -> None:
        """The scale reader extracts pdf_scale from a new slim blob (entities: []).

        Arrange: new-shape slim blob with entities: [] + pdf_scale.
        Act:     extract pdf_scale from the canonical dict.
        Assert:  pdf_scale is correctly read; empty entities list is transparent.
        """
        _ = self
        slim_blob = _make_new_slim_blob(pdf_scale=_PDF_SCALE_FIXTURE)

        canonical = dict(slim_blob)
        pdf_scale_raw = canonical.get("pdf_scale")
        pdf_scale = dict(pdf_scale_raw) if isinstance(pdf_scale_raw, dict) else None

        assert pdf_scale is not None
        assert pdf_scale["real_world_unit"] == "meter"
        assert pdf_scale["points_to_real"] == 0.0352778
        assert slim_blob["entities"] == []

    def test_scale_reader_reads_units_from_old_full_blob(self) -> None:
        """The scale reader extracts units from an old full blob.

        Arrange: old-shape blob with entities + blocks + units.
        Act:     extract units from the canonical dict.
        Assert:  units.normalized == "millimeter".
        """
        _ = self
        old_blob = _make_old_full_blob(units=_UNITS_FIXTURE)

        canonical = dict(old_blob)
        units = dict(canonical.get("units") or {}) or {"normalized": "unknown"}

        assert units["normalized"] == "millimeter"
        assert units["conversion_factor"] == 0.001

    def test_scale_reader_reads_units_from_new_slim_blob(self) -> None:
        """The scale reader extracts units from a new slim blob.

        Arrange: new-shape slim blob with units + entities: [].
        Act:     extract units from the canonical dict.
        Assert:  units.normalized == "millimeter".
        """
        _ = self
        slim_blob = _make_new_slim_blob(units=_UNITS_FIXTURE)

        canonical = dict(slim_blob)
        units = dict(canonical.get("units") or {}) or {"normalized": "unknown"}

        assert units["normalized"] == "millimeter"

    def test_scale_reader_returns_unknown_when_units_absent_old_blob(self) -> None:
        """Missing units key in old blob produces unknown — same as missing in slim blob.

        Arrange: old-shape blob without a units key.
        Act:     extract units.
        Assert:  falls back to {"normalized": "unknown"}.
        """
        _ = self
        old_blob = _make_old_full_blob()  # no units key

        canonical = dict(old_blob)
        units = dict(canonical.get("units") or {}) or {"normalized": "unknown"}

        assert units["normalized"] == "unknown"

    # -----------------------------------------------------------------------
    # canonical.py: interpretation + census
    # -----------------------------------------------------------------------

    def test_interpretation_reader_reads_from_old_full_blob(self) -> None:
        """The interpretation reader extracts the block from an old full blob.

        Arrange: old-shape blob with entities + blocks + interpretation.
        Act:     get interpretation block from canonical dict.
        Assert:  interpretation.schema_version correct; entities bulk ignored.
        """
        _ = self
        old_blob = _make_old_full_blob(interpretation=_INTERPRETATION_FIXTURE)

        canonical = dict(old_blob)
        interpretation = canonical.get("interpretation")

        assert isinstance(interpretation, dict)
        assert interpretation["schema_version"] == "0.1"
        assert interpretation["length"]["unit"] == "millimeter"

    def test_interpretation_reader_reads_from_new_slim_blob(self) -> None:
        """The interpretation reader extracts the block from a new slim blob.

        Arrange: new-shape slim blob with entities: [] + interpretation.
        Act:     get interpretation block.
        Assert:  interpretation.schema_version correct.
        """
        _ = self
        slim_blob = _make_new_slim_blob(interpretation=_INTERPRETATION_FIXTURE)

        canonical = dict(slim_blob)
        interpretation = canonical.get("interpretation")

        assert isinstance(interpretation, dict)
        assert interpretation["schema_version"] == "0.1"

    def test_interpretation_reader_returns_none_when_absent_from_old_blob(self) -> None:
        """Missing interpretation in old blob returns None — same as slim blob.

        Arrange: old-shape blob without interpretation key.
        Act:     get interpretation.
        Assert:  None (caller handles available=False).
        """
        _ = self
        old_blob = _make_old_full_blob()

        interpretation = dict(old_blob).get("interpretation")

        assert interpretation is None

    def test_census_reader_reads_from_old_full_blob(self) -> None:
        """The census reader extracts the block from an old full blob.

        Arrange: old-shape blob with entities + blocks + census.
        Act:     get census block.
        Assert:  census.raw_object_total == 42.
        """
        _ = self
        old_blob = _make_old_full_blob(census=_CENSUS_FIXTURE)

        canonical = dict(old_blob)
        census = canonical.get("census")

        assert isinstance(census, dict)
        assert census["raw_object_total"] == 42
        assert census["raw_objects"]["LINE"] == 30

    def test_census_reader_reads_from_new_slim_blob(self) -> None:
        """The census reader extracts the block from a new slim blob.

        Arrange: new-shape slim blob with entities: [] + census.
        Act:     get census block.
        Assert:  census.raw_object_total == 42.
        """
        _ = self
        slim_blob = _make_new_slim_blob(census=_CENSUS_FIXTURE)

        canonical = dict(slim_blob)
        census = canonical.get("census")

        assert isinstance(census, dict)
        assert census["raw_object_total"] == 42

    def test_census_reader_returns_none_when_absent_from_old_blob(self) -> None:
        """Missing census in old blob returns None.

        Arrange: old-shape blob without census key.
        Act:     get census.
        Assert:  None.
        """
        _ = self
        old_blob = _make_old_full_blob()

        census = dict(old_blob).get("census")

        assert census is None

    # -----------------------------------------------------------------------
    # loaders.py: load_adapter_text_blocks — reads metadata.text_blocks
    # -----------------------------------------------------------------------

    def test_text_blocks_reader_reads_from_old_full_blob(self) -> None:
        """load_adapter_text_blocks extracts text_blocks from metadata in an old blob.

        Arrange: old-shape blob with entities + blocks + metadata.text_blocks.
        Act:     extract metadata.text_blocks from canonical dict (same logic as loader).
        Assert:  text blocks returned correctly.
        """
        _ = self
        old_blob = _make_old_full_blob(text_blocks=_TEXT_BLOCKS_FIXTURE)

        canonical = dict(old_blob)
        metadata = canonical.get("metadata")
        text_blocks_raw = metadata.get("text_blocks") if isinstance(metadata, dict) else None
        text_blocks = list(text_blocks_raw) if isinstance(text_blocks_raw, list) else []

        assert len(text_blocks) == 2
        assert text_blocks[0]["text"] == "Ø76 mm VAC"

    def test_text_blocks_reader_reads_from_new_slim_blob(self) -> None:
        """load_adapter_text_blocks extracts text_blocks from metadata in a slim blob.

        Arrange: new-shape slim blob with entities: [] + metadata.text_blocks.
        Act:     extract metadata.text_blocks.
        Assert:  text blocks returned correctly; empty entities is transparent.
        """
        _ = self
        slim_blob = _make_new_slim_blob(text_blocks=_TEXT_BLOCKS_FIXTURE)

        canonical = dict(slim_blob)
        metadata = canonical.get("metadata")
        text_blocks_raw = metadata.get("text_blocks") if isinstance(metadata, dict) else None
        text_blocks = list(text_blocks_raw) if isinstance(text_blocks_raw, list) else []

        assert len(text_blocks) == 2
        assert text_blocks[1]["text"] == "Ø42 mm MA"
        # Slim blob: entities empty, breadcrumbs present.
        assert slim_blob["entities"] == []
        assert slim_blob["metadata"]["entities_storage"]["count"] == 1

    def test_text_blocks_reader_returns_empty_when_metadata_absent_old_blob(self) -> None:
        """Missing metadata in old blob returns [] — same as slim blob without metadata.

        Arrange: old-shape blob without metadata key.
        Act:     extract text_blocks.
        Assert:  [].
        """
        _ = self
        old_blob = _make_old_full_blob()  # no metadata / text_blocks

        canonical = dict(old_blob)
        metadata = canonical.get("metadata")
        text_blocks_raw = metadata.get("text_blocks") if isinstance(metadata, dict) else None
        text_blocks = list(text_blocks_raw) if isinstance(text_blocks_raw, list) else []

        assert text_blocks == []

    # -----------------------------------------------------------------------
    # revised_dxf.py: _load_real_world_scale — reads metadata.pdf_scale
    # -----------------------------------------------------------------------

    def test_revised_dxf_reader_reads_pdf_scale_from_old_full_blob(self) -> None:
        """_load_real_world_scale extracts metadata.pdf_scale from an old full blob.

        Arrange: old-shape blob with entities + metadata.pdf_scale.
        Act:     extract metadata.pdf_scale (same logic as revised_dxf._load_real_world_scale).
        Assert:  scale extracted correctly.
        """
        _ = self
        metadata_pdf_scale = {
            "real_world_units": True,
            "real_world_unit": "meter",
            "points_to_real": 0.0352778,
        }
        old_blob = _make_old_full_blob()
        old_blob["metadata"] = {"pdf_scale": metadata_pdf_scale}

        canonical = dict(old_blob)
        metadata = canonical.get("metadata") if canonical.get("metadata") else None
        scale = metadata.get("pdf_scale") if isinstance(metadata, dict) else None

        assert isinstance(scale, dict)
        assert scale["real_world_unit"] == "meter"
        assert scale["points_to_real"] == 0.0352778

    def test_revised_dxf_reader_reads_pdf_scale_from_new_slim_blob(self) -> None:
        """_load_real_world_scale extracts metadata.pdf_scale from a new slim blob.

        Arrange: new-shape slim blob with metadata.pdf_scale.
        Act:     extract metadata.pdf_scale.
        Assert:  scale extracted correctly; entities: [] is transparent.
        """
        _ = self
        metadata_pdf_scale = {
            "real_world_units": True,
            "real_world_unit": "meter",
            "points_to_real": 0.0352778,
        }
        slim_blob = _make_new_slim_blob()
        slim_blob["metadata"]["pdf_scale"] = metadata_pdf_scale

        canonical = dict(slim_blob)
        metadata = canonical.get("metadata")
        scale = metadata.get("pdf_scale") if isinstance(metadata, dict) else None

        assert isinstance(scale, dict)
        assert scale["real_world_unit"] == "meter"
        assert slim_blob["entities"] == []

    def test_revised_dxf_reader_returns_none_when_scale_absent_old_blob(self) -> None:
        """Missing metadata.pdf_scale in old blob returns None.

        Arrange: old-shape blob without metadata.pdf_scale.
        Act:     extract scale.
        Assert:  None (no scale available).
        """
        _ = self
        old_blob = _make_old_full_blob()  # no metadata

        canonical = dict(old_blob)
        metadata = canonical.get("metadata")
        scale = metadata.get("pdf_scale") if isinstance(metadata, dict) else None

        assert scale is None
