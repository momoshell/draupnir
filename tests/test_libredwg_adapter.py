"""Contract tests for the thin LibreDWG DWG adapter."""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from app.core.config import settings
from app.ingestion.adapters import libredwg as adapter_module
from app.ingestion.contracts import (
    AdapterExecutionOptions,
    AdapterSource,
    AdapterStatus,
    AdapterTimeout,
    AdapterUnavailableError,
    AvailabilityReason,
    InputFamily,
    UploadFormat,
)
from app.ingestion.registry import get_registry
from app.ingestion.validation import build_validation_outcome
from tests.ingestion_contract_harness import (
    ContractFinalizationExpectation,
    build_contract_source,
    exercise_adapter_contract,
)

_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "dwg" / "libredwg-wrapper-smoke.txt"


class _FakeProcess:
    def __init__(
        self,
        *,
        complete_with: int | None = None,
        complete_when: asyncio.Event | None = None,
    ) -> None:
        self.returncode: int | None = None
        self.terminate_calls = 0
        self.kill_calls = 0
        self._complete_with = complete_with
        self._complete_when = complete_when
        self._killed = asyncio.Event()

    async def wait(self) -> int:
        if self.returncode is not None:
            return self.returncode
        if self._complete_with is not None and self._complete_when is None:
            self.returncode = self._complete_with
            return self.returncode

        wait_tasks = [asyncio.create_task(self._killed.wait())]
        complete_task: asyncio.Task[bool] | None = None
        if self._complete_when is not None:
            complete_task = asyncio.create_task(self._complete_when.wait())
            wait_tasks.append(complete_task)

        done, pending = await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)
        for pending_task in pending:
            pending_task.cancel()

        if (
            complete_task is not None
            and complete_task in done
            and self.returncode is None
            and self._complete_with is not None
        ):
            self.returncode = self._complete_with

        return cast(int, self.returncode)

    def terminate(self) -> None:
        self.terminate_calls += 1

    def kill(self) -> None:
        self.kill_calls += 1
        self.returncode = -9
        self._killed.set()


class _DeferredCancellation:
    def __init__(self, *, cancel_after_calls: int) -> None:
        self.calls = 0
        self._cancel_after_calls = cancel_after_calls

    def is_cancelled(self) -> bool:
        self.calls += 1
        return self.calls >= self._cancel_after_calls


def _build_source() -> AdapterSource:
    return build_contract_source(
        file_path=_FIXTURE_PATH,
        upload_format=UploadFormat.DWG,
        input_family=InputFamily.DWG,
        media_type="image/vnd.dwg",
        original_name="fixtures/libredwg-wrapper-smoke.dwg",
    )


def _assert_score_within_libredwg_descriptor_range(score: float | None) -> None:
    confidence_range = get_registry()[InputFamily.DWG].confidence_range

    assert confidence_range is not None
    assert score is not None
    minimum_confidence, maximum_confidence = confidence_range

    assert minimum_confidence <= score <= maximum_confidence


def _with_hatch_counts(counts: Mapping[str, int]) -> dict[str, int]:
    merged_counts = dict(counts)
    merged_counts.setdefault("unsupported_hatches", 0)
    merged_counts.setdefault("malformed_hatches", 0)
    merged_counts.setdefault("block_references", 0)
    merged_counts.setdefault("unsupported_block_transforms", 0)
    merged_counts.setdefault("malformed_inserts", 0)
    merged_counts.setdefault("skipped_non_drawable", 0)
    return merged_counts


_HATCH_SQUARE_POINTS = (
    {"x": 0.0, "y": 0.0, "z": 0.0},
    {"x": 4.0, "y": 0.0, "z": 0.0},
    {"x": 4.0, "y": 3.0, "z": 0.0},
    {"x": 0.0, "y": 3.0, "z": 0.0},
)


def _hatch_line_edges(count: int) -> list[dict[str, object]]:
    return [
        {
            "type": "LINE",
            "start": {"x": float(index), "y": 0.0, "z": 0.0},
            "end": {"x": float(index + 1), "y": 0.0, "z": 0.0},
        }
        for index in range(count)
    ]


def _install_fake_subprocess(
    monkeypatch: pytest.MonkeyPatch,
    *,
    process: _FakeProcess,
    output_text: str = '{"drawing": {"version": "R2018"}}',
    stdout_text: str = "",
    stderr_text: str = "SUCCESS\n",
    write_output: bool = True,
    format_output: bool = False,
    on_spawn: Callable[[Path, Path, Path], None] | None = None,
) -> dict[str, tuple[str, ...]]:
    seen: dict[str, tuple[str, ...]] = {}

    async def _fake_create_subprocess_exec(*command: str, **kwargs: Any) -> _FakeProcess:
        output_flag_index = command.index("-o") + 1
        output_path = Path(command[output_flag_index])
        stdout_path = Path(cast(str, kwargs["stdout"].name))
        stderr_path = Path(cast(str, kwargs["stderr"].name))
        tempdir = str(output_path.parent)
        source_path = command[output_flag_index + 1]
        if write_output:
            rendered_output = (
                output_text.format(source=source_path, tempdir=tempdir, output=output_path)
                if format_output
                else output_text
            )
            output_path.write_text(rendered_output, encoding="utf-8")

        stdout_handle = kwargs["stdout"]
        stderr_handle = kwargs["stderr"]
        stdout_payload = stdout_text.format(
            source=source_path,
            tempdir=tempdir,
            output=output_path,
        ).encode("utf-8")
        stderr_payload = stderr_text.format(
            source=source_path,
            tempdir=tempdir,
            output=output_path,
        ).encode("utf-8")
        stdout_handle.write(stdout_payload)
        stdout_handle.flush()
        stderr_handle.write(stderr_payload)
        stderr_handle.flush()

        if on_spawn is not None:
            on_spawn(output_path, stdout_path, stderr_path)

        seen["command"] = command
        return process

    monkeypatch.setattr(
        "app.ingestion.adapters.libredwg.asyncio.create_subprocess_exec",
        _fake_create_subprocess_exec,
    )
    return seen


def _schedule_live_overflow_write(
    *,
    overflow_path_kind: str,
    payload_text: str,
) -> Callable[[Path, Path, Path], None]:
    background_tasks: list[asyncio.Task[None]] = []

    def _on_spawn(output_path: Path, stdout_path: Path, stderr_path: Path) -> None:
        target_path = {
            "output": output_path,
            "stdout": stdout_path,
            "stderr": stderr_path,
        }[overflow_path_kind]

        async def _writer() -> None:
            await asyncio.sleep(adapter_module._PROCESS_POLL_INTERVAL_SECONDS)
            target_path.write_text(payload_text, encoding="utf-8")

        background_tasks.append(asyncio.create_task(_writer()))

    return _on_spawn


def _contains_non_finite_numbers(value: Any) -> bool:
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, Mapping):
        return any(_contains_non_finite_numbers(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_non_finite_numbers(item) for item in value)
    return False


def _assert_line_wrapper_entity(entity: Mapping[str, Any]) -> None:
    assert entity["entity_type"] == "line"
    assert entity["kind"] == "line"
    assert entity["start"] == {"x": 1.0, "y": 2.0, "z": 0.0}
    assert entity["end"] == {"x": 1.0, "y": 5.0, "z": 0.0}
    assert entity["length"] == 3.0
    assert entity["geometry"]["geometry_summary"] == {
        "kind": "line_segment",
        "length": 3.0,
        "vertex_count": 2,
    }


def _assert_circle_wrapper_entity(entity: Mapping[str, Any]) -> None:
    assert entity["entity_type"] == "circle"
    assert entity["kind"] == "circle"
    assert entity["center"] == {"x": 5.0, "y": 7.0, "z": 0.0}
    assert entity["radius"] == 2.5
    assert entity["diameter"] == 5.0
    assert entity["geometry"]["geometry_summary"] == {
        "kind": "circle",
        "radius": 2.5,
        "diameter": 5.0,
        "circumference": pytest.approx(5.0 * math.pi),
    }


def test_extract_angle_degrees_radians_default_and_degrees_hint() -> None:
    def extract(record: Mapping[str, Any]) -> float:
        value = adapter_module._extract_angle_degrees(record, "start_angle")
        assert value is not None
        return value

    # dwgread default: radians → converted to degrees
    assert round(extract({"start_angle": math.pi / 2}), 6) == 90.0
    assert round(extract({"start_angle": math.radians(135)}), 6) == 135.0
    # radians wrap normalizes into [0, 360)
    assert round(extract({"start_angle": 2 * math.pi}), 6) == 0.0
    # explicit degrees hint → used as-is (no conversion)
    assert extract({"start_angle": 90, "angle_units": "degrees"}) == 90.0
    assert extract({"start_angle": 45, "angle_units": "deg"}) == 45.0


def _assert_arc_wrapper_entity(entity: Mapping[str, Any]) -> None:
    assert entity["entity_type"] == "arc"
    assert entity["kind"] == "arc"
    assert entity["center"] == {"x": 10.0, "y": 20.0, "z": 0.0}
    assert entity["radius"] == 5.0
    assert entity["start_angle_degrees"] == 45.0
    assert entity["end_angle_degrees"] == 135.0
    assert entity["geometry"]["geometry_summary"] == {
        "kind": "arc",
        "radius": 5.0,
        "start_angle_degrees": 45.0,
        "end_angle_degrees": 135.0,
        "sweep_degrees": 90.0,
        "length": pytest.approx(2.5 * math.pi),
    }


def _assert_lwpolyline_wrapper_entity(entity: Mapping[str, Any]) -> None:
    assert entity["entity_type"] == "polyline"
    assert entity["kind"] == "polyline"
    assert entity["vertices"] == (
        {"x": 0.0, "y": 0.0, "z": 0.0},
        {"x": 3.0, "y": 4.0, "z": 0.0},
        {"x": 3.0, "y": 0.0, "z": 0.0},
    )
    assert entity["closed"] is True
    assert entity["length"] == 12.0
    assert entity["geometry"]["geometry_summary"] == {
        "kind": "polyline",
        "length": 12.0,
        "vertex_count": 3,
        "closed": True,
    }


def _assert_mtext_wrapper_entity(entity: Mapping[str, Any]) -> None:
    assert entity["entity_type"] == "text"
    assert entity["kind"] == "text"
    assert entity["text"] == "Zone B-01"
    assert entity["text_length"] == 9
    assert entity["geometry"] == {
        "text": "Zone B-01",
        "units": {"normalized": "unknown"},
        "geometry_summary": {
            "kind": "text",
            "source_type": "MTEXT",
            "text_length": 9,
        },
        "insertion": {"x": 10.0, "y": 20.0, "z": 0.0},
    }


def test_probe_reports_missing_binary_with_license_review_posture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: None)

    availability = adapter_module.create_adapter().probe()

    assert availability.status is AdapterStatus.UNAVAILABLE
    assert availability.availability_reason is AvailabilityReason.MISSING_BINARY
    assert [(item.kind.value, item.name, item.status.value) for item in availability.observed] == [
        ("binary", "dwgread", "missing"),
        ("license", "libredwg-distribution-review", "available"),
    ]


@pytest.mark.asyncio
async def test_ingest_preflights_missing_binary_as_shared_unavailable_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: None)

    with pytest.raises(AdapterUnavailableError) as exc_info:
        await adapter_module.create_adapter().ingest(
            _build_source(),
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.1)),
        )

    assert exc_info.value.availability_reason is AvailabilityReason.MISSING_BINARY


@pytest.mark.asyncio
async def test_libredwg_adapter_maps_line_entities_into_canonical_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _build_source()
    process = _FakeProcess(complete_with=0)
    seen = _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "LINE",
                        "handle": "1A",
                        "layout": "Sheet-A1",
                        "layer": "Walls",
                        "block": "Door-Block",
                        "start": {"x": 1, "y": 2, "z": 0},
                        "end": {"x": 4, "y": 6, "z": 0},
                    }
                ]
            }
        ),
        stdout_text="reading {source}\n",
        stderr_text="wrote {source} to {output} from {tempdir}\n",
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    adapter = adapter_module.create_adapter()
    payload = await exercise_adapter_contract(
        adapter,
        source=source,
        input_family=InputFamily.DWG,
        adapter_key="libredwg",
        expectation=ContractFinalizationExpectation(
            validation_status="needs_review",
            warning_codes=("libredwg.units_unconfirmed",),
            diagnostic_codes=("libredwg.extract",),
        ),
    )

    command = seen["command"]
    output_path = Path(command[4])
    assert command[:4] == ("/opt/homebrew/bin/dwgread", "-O", "JSON", "-o")
    assert output_path.parent != _FIXTURE_PATH.parent
    assert payload.canonical_json["metadata"]["adapter_mode"] == "dwgread_json_v0_1"
    assert "empty_entities_reason" not in payload.canonical_json["metadata"]
    assert payload.canonical_json["layouts"] == [{"name": "Model"}, {"name": "Sheet-A1"}]
    assert payload.canonical_json["layers"] == [{"name": "Walls"}]
    # Door-Block is referenced by the line's owner field but has no local definition, so it
    # surfaces as a geometry-less (referenced-only) block payload.
    assert payload.canonical_json["blocks"] == [
        {
            "name": "Door-Block",
            "block_ref": "Door-Block",
            "block_handle": None,
            "base_point": {"x": 0.0, "y": 0.0, "z": 0.0},
            "entities": [],
        }
    ]

    entities = cast(list[dict[str, Any]], payload.canonical_json["entities"])
    assert len(entities) == 1
    entity = entities[0]
    assert entity["entity_id"] == "libredwg-line-1a"
    assert entity["entity_type"] == "line"
    assert entity["entity_schema_version"] == "0.1"
    assert entity["source_entity_handle"] == "1A"
    assert entity["layout_name"] == "Sheet-A1"
    assert entity["layer_name"] == "Walls"
    assert entity["block_name"] == "Door-Block"
    assert entity["parent_entity_id"] is None
    assert entity["drawing_revision_id"] is None
    assert entity["source_file_id"] is None
    assert entity["layout_ref"] == "Sheet-A1"
    assert entity["layer_ref"] == "Walls"
    assert entity["block_ref"] == "Door-Block"
    assert entity["parent_entity_ref"] is None
    assert entity["bbox"] == {
        "min": {"x": 1.0, "y": 2.0, "z": 0.0},
        "max": {"x": 4.0, "y": 6.0, "z": 0.0},
    }
    assert entity["geometry"] == {
        "start": {"x": 1.0, "y": 2.0, "z": 0.0},
        "end": {"x": 4.0, "y": 6.0, "z": 0.0},
        "bbox": {
            "min": {"x": 1.0, "y": 2.0, "z": 0.0},
            "max": {"x": 4.0, "y": 6.0, "z": 0.0},
        },
        "units": {"normalized": "unknown"},
        "geometry_summary": {
            "kind": "line_segment",
            "length": 5.0,
            "vertex_count": 2,
        },
    }
    assert entity["properties"] == {
        "source_type": "LINE",
        "source_handle": "1A",
        "quantity_hints": {"length": 5.0, "count": 1.0},
        "adapter_native": {
            "libredwg": {
                "section": "OBJECTS",
                "record_type": "LINE",
                "handle": "1A",
            }
        },
    }
    assert entity["kind"] == "line"
    assert entity["start"] == {"x": 1.0, "y": 2.0, "z": 0.0}
    assert entity["end"] == {"x": 4.0, "y": 6.0, "z": 0.0}
    assert entity["length"] == 5.0
    assert "quantity_hints" not in entity
    assert "adapter_native" not in entity
    assert entity["provenance"]["origin"] == "adapter_normalized"
    assert entity["provenance"]["adapter"] == {"key": "libredwg"}
    assert entity["provenance"]["adapter_key"] == "libredwg"
    assert entity["provenance"]["source_ref"] == "OBJECTS/LINE/1A"
    assert entity["provenance"]["source_identity"] == "1A"
    assert entity["provenance"]["source_hash"] == adapter_module._canonical_hash_json_value(
        {
            "record_type": "LINE",
            "handle": "1A",
            "layer_name": "Walls",
            "layout_name": "Sheet-A1",
            "block_name": "Door-Block",
            "geometry": {
                "start": {"x": 1.0, "y": 2.0, "z": 0.0},
                "end": {"x": 4.0, "y": 6.0, "z": 0.0},
            },
        }
    )
    assert entity["provenance"]["normalized_source_hash"] == entity["provenance"]["source_hash"]
    assert entity["provenance"]["extraction_path"] == ["OBJECTS", "LINE"]
    assert entity["provenance"]["notes"] == ["units_unconfirmed"]
    assert entity["provenance"]["source_locator"] == "OBJECTS/LINE/1A"
    assert entity["provenance"]["extra"] == {
        "native": {
            "libredwg": {
                "section": "OBJECTS",
                "record_type": "LINE",
                "handle": "1A",
            }
        },
        "legacy_aliases": {
            "adapter_key": "libredwg",
            "source": "OBJECTS/LINE/1A",
            "source_section": "OBJECTS",
            "source_entity_ref": "OBJECTS/LINE/1A",
            "source_locator": "OBJECTS/LINE/1A",
            "entity_ref": "OBJECTS/LINE/1A",
            "normalized_source_hash": entity["provenance"]["source_hash"],
            "native_handle": "1A",
            "source_entity_handle": "1A",
            "record_hash": f"sha256:{entity['provenance']['source_hash']}",
        },
    }
    assert len(entity["provenance"]["source_hash"]) == 64
    assert all(character in "0123456789abcdef" for character in entity["provenance"]["source_hash"])
    assert entity["provenance"]["record_hash"] == f"sha256:{entity['provenance']['source_hash']}"
    assert entity["confidence"] == {
        "score": adapter_module._LINE_ENTITY_CONFIDENCE_SCORE,
        "review_required": True,
        "basis": "libredwg_line_mapping_units_unconfirmed",
    }
    _assert_score_within_libredwg_descriptor_range(entity["confidence"]["score"])

    result = await adapter.ingest(
        source,
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )
    second_output_path = Path(seen["command"][4])
    diagnostic_details = cast(Mapping[str, object], result.diagnostics[0].details)
    stdout_excerpt = cast(str, diagnostic_details["stdout_excerpt"])
    stderr_excerpt = cast(str, diagnostic_details["stderr_excerpt"])
    assert str(source.file_path) not in stdout_excerpt
    assert str(source.file_path) not in stderr_excerpt
    assert str(second_output_path.parent) not in stderr_excerpt
    assert "<source>" in stdout_excerpt
    assert "<tempdir>" in stderr_excerpt
    assert result.confidence is not None
    _assert_score_within_libredwg_descriptor_range(result.confidence.score)

    validation = build_validation_outcome(
        input_family=InputFamily.DWG,
        canonical_json=result.canonical,
        canonical_entity_schema_version=cast(
            str,
            result.canonical["canonical_entity_schema_version"],
        ),
        result=result,
        generated_at=datetime.now(UTC),
    )
    findings = cast(list[Mapping[str, object]], validation.report_json["findings"])
    assert not any(finding.get("check_key") == "placeholder_semantics" for finding in findings)


@pytest.mark.asyncio
async def test_libredwg_adapter_emits_deterministic_top_level_layouts_and_blocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "LINE",
                        "handle": "1A",
                        "layout": "Sheet-B1",
                        "layer": "Walls",
                        "block": "Door-Block",
                        "start": {"x": 1, "y": 2, "z": 0},
                        "end": {"x": 4, "y": 6, "z": 0},
                    },
                    {
                        "type": "HATCH",
                        "handle": "20",
                        "layout": "Sheet-A1",
                        "layer": "Unsupported",
                        "block": "Anchor-Block",
                    },
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    assert result.canonical["layouts"] == (
        {"name": "Model"},
        {"name": "Sheet-A1"},
        {"name": "Sheet-B1"},
    )
    # Both blocks are referenced by owner fields without a local definition, so they surface as
    # geometry-less referenced-only payloads, ordered deterministically by name.
    assert result.canonical["blocks"] == (
        {
            "name": "Anchor-Block",
            "block_ref": "Anchor-Block",
            "block_handle": None,
            "base_point": {"x": 0.0, "y": 0.0, "z": 0.0},
            "entities": (),
        },
        {
            "name": "Door-Block",
            "block_ref": "Door-Block",
            "block_handle": None,
            "base_point": {"x": 0.0, "y": 0.0, "z": 0.0},
            "entities": (),
        },
    )


@pytest.mark.asyncio
async def test_libredwg_adapter_diagnostics_include_mapping_counts_and_confidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "LINE",
                        "handle": "1A",
                        "layer": "Walls",
                        "start": {"x": 1, "y": 2},
                        "end": {"x": 4, "y": 6},
                    },
                    {"type": "HATCH", "handle": "20", "layer": "Unsupported"},
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    diagnostic_details = cast(Mapping[str, object], result.diagnostics[0].details)
    assert diagnostic_details["entity_counts"] == _with_hatch_counts(
        {
            "drawable_candidates": 2,
            "supported_geometry": 1,
            "supported_lines": 1,
            "supported_text": 0,
            "unsupported_drawables": 0,
            "malformed_drawables": 0,
            "unsupported_hatches": 1,
            "malformed_hatches": 0,
        }
    )
    assert diagnostic_details["mapping_confidence"] == {
        "score": adapter_module._MIXED_ENTITY_CONFIDENCE_SCORE,
        "review_required": True,
        "basis": "libredwg_dwgread_json_mixed_entity_mapping",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("output_payload", "expected_handle", "expected_layer", "expected_length"),
    [
        (
            {
                "OBJECTS": [
                    {
                        "type": "DWG_TYPE_LINE",
                        "handle": "AB",
                        "layer": "A-WALL",
                        "start": {"x": "0", "y": "1"},
                        "end": {"x": 3, "y": 5, "z": 0},
                    }
                ]
            },
            "AB",
            "A-WALL",
            5.0,
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "LAYER",
                        "entity": "LINE",
                        "handle": "GH",
                        "layer": "D-PIPE",
                        "start": {"x": 1, "y": 1},
                        "end": {"x": 4, "y": 1},
                    }
                ]
            },
            "GH",
            "D-PIPE",
            3.0,
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "DICTIONARY",
                        "object": "LINE",
                        "handle": "IJ",
                        "layer": "E-DUCT",
                        "start": {"x": 2, "y": 2},
                        "end": {"x": 2, "y": 7},
                    }
                ]
            },
            "IJ",
            "E-DUCT",
            5.0,
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "DWG_TYPE_UNKNOWN",
                        "dxfname": "LINE",
                        "handle": "KL",
                        "layer": "F-CABLE",
                        "start": {"x": 0, "y": 0},
                        "end": {"x": 0, "y": 8},
                    }
                ]
            },
            "KL",
            "F-CABLE",
            8.0,
        ),
        (
            {
                "OBJECTS": {
                    "first": {
                        "name": "AcDbLine",
                        "id": "CD",
                        "layer_name": "B-DOOR",
                        "entity": {
                            "start_point": [1, 2, 0],
                            "end_point": [1, 5, 0],
                        },
                    }
                }
            },
            "CD",
            "B-DOOR",
            3.0,
        ),
        (
            {
                "OBJECTS": {
                    "wrapped": {
                        "fixed_type": "LINE",
                        "object_handle": "EF",
                        "owner_layer": "C-GRID",
                        "data": {
                            "start_x": 2,
                            "start_y": 2,
                            "end_x": 2,
                            "end_y": 6,
                        },
                    }
                }
            },
            "EF",
            "C-GRID",
            4.0,
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "LINE",
                        "handle": "MN",
                        "layer": "Core-Lines",
                        "first_endpoint": [1, 3, 0],
                        "second_endpoint": [7, 3, 0],
                    }
                ]
            },
            "MN",
            "Core-Lines",
            6.0,
        ),
    ],
)
async def test_libredwg_adapter_handles_objects_variants_and_type_markers(
    monkeypatch: pytest.MonkeyPatch,
    output_payload: dict[str, Any],
    expected_handle: str,
    expected_layer: str,
    expected_length: float,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(monkeypatch, process=process, output_text=json.dumps(output_payload))
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]
    assert entity["entity_type"] == "line"
    assert entity["source_entity_handle"] == expected_handle
    assert entity["layer_name"] == expected_layer
    assert entity["kind"] == "line"
    assert entity["length"] == expected_length
    assert entity["properties"]["source_type"] == "LINE"
    assert entity["properties"]["source_handle"] == expected_handle
    assert entity["properties"]["quantity_hints"] == {
        "length": expected_length,
        "count": 1.0,
    }
    assert entity["geometry"]["geometry_summary"] == {
        "kind": "line_segment",
        "length": expected_length,
        "vertex_count": 2,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "output_payload",
        "expected_handle",
        "expected_layer",
        "expected_source_type",
        "expected_counts",
        "assert_entity",
    ),
    [
        (
            {
                "OBJECTS": [
                    {
                        "id": "W1",
                        "layer_name": "A-WALL",
                        "record": {
                            "entity": "LINE",
                            "data": {
                                "start_point": [1, 2, 0],
                                "end_point": [1, 5, 0],
                            },
                        },
                    }
                ]
            },
            "W1",
            "A-WALL",
            "LINE",
            {
                "drawable_candidates": 1,
                "supported_geometry": 1,
                "supported_lines": 1,
                "supported_text": 0,
                "unsupported_drawables": 0,
                "malformed_drawables": 0,
            },
            _assert_line_wrapper_entity,
        ),
        (
            {
                "OBJECTS": [
                    {
                        "object_handle": "C1",
                        "owner_layer": "Curves",
                        "payload": {
                            "object": "AcDbCircle",
                            "value": {
                                "centerpoint": {"x": "5", "y": "7", "z": 0},
                                "radius": "2.5",
                            },
                        },
                    }
                ]
            },
            "C1",
            "Curves",
            "CIRCLE",
            {
                "drawable_candidates": 1,
                "supported_geometry": 1,
                "supported_lines": 0,
                "supported_text": 0,
                "unsupported_drawables": 0,
                "malformed_drawables": 0,
            },
            _assert_circle_wrapper_entity,
        ),
        (
            {
                "OBJECTS": [
                    {
                        "entity_handle": "A1",
                        "layer": "Curves",
                        "entity": {
                            "dxf_name": "ARC",
                            "record": {
                                "center_point": [10, 20, 0],
                                "radius": "5",
                                # dwgread emits radians, no angle_units; 45°/135° arc.
                                "startangle": str(math.radians(45)),
                                "endangle": str(math.radians(135)),
                                "orientation": "counterclockwise",
                            },
                        },
                    }
                ]
            },
            "A1",
            "Curves",
            "ARC",
            {
                "drawable_candidates": 1,
                "supported_geometry": 1,
                "supported_lines": 0,
                "supported_text": 0,
                "unsupported_drawables": 0,
                "malformed_drawables": 0,
            },
            _assert_arc_wrapper_entity,
        ),
        (
            {
                "OBJECTS": [
                    {
                        "object_id": "P1",
                        "owner_layer": "Perimeter",
                        "payload": {
                            "type": "DWG_TYPE_LWPOLYLINE",
                            "data": {
                                "closed_flag": "1",
                                "vertexes": [[0, 0], [3, 4], [3, 0]],
                            },
                        },
                    }
                ]
            },
            "P1",
            "Perimeter",
            "LWPOLYLINE",
            {
                "drawable_candidates": 1,
                "supported_geometry": 1,
                "supported_lines": 0,
                "supported_text": 0,
                "unsupported_drawables": 0,
                "malformed_drawables": 0,
            },
            _assert_lwpolyline_wrapper_entity,
        ),
        (
            {
                "OBJECTS": [
                    {
                        "id": "T1",
                        "owner_layer": "Labels",
                        "record": {
                            "dxf_name": "MTEXT",
                            "payload": {
                                "text_position": [10, 20, 0],
                                "textstring": "Zone B-01",
                            },
                        },
                    }
                ]
            },
            "T1",
            "Labels",
            "MTEXT",
            {
                "drawable_candidates": 1,
                "supported_geometry": 0,
                "supported_lines": 0,
                "supported_text": 1,
                "unsupported_drawables": 0,
                "malformed_drawables": 0,
            },
            _assert_mtext_wrapper_entity,
        ),
    ],
)
async def test_libredwg_adapter_recovers_supported_drawable_wrapper_records(
    monkeypatch: pytest.MonkeyPatch,
    output_payload: dict[str, Any],
    expected_handle: str,
    expected_layer: str,
    expected_source_type: str,
    expected_counts: dict[str, int],
    assert_entity: Callable[[Mapping[str, Any]], None],
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(monkeypatch, process=process, output_text=json.dumps(output_payload))
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]
    assert entity["source_entity_handle"] == expected_handle
    assert entity["layer_name"] == expected_layer
    assert entity["layout_name"] == "Model"
    assert entity["properties"]["source_type"] == expected_source_type
    assert_entity(entity)
    assert [warning.code for warning in result.warnings] == ["libredwg.units_unconfirmed"]

    diagnostic_details = cast(Mapping[str, object], result.diagnostics[0].details)
    assert diagnostic_details["entity_counts"] == _with_hatch_counts(expected_counts)


@pytest.mark.asyncio
async def test_libredwg_adapter_keeps_wrapper_record_and_sibling_drawable_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": {
                    "data": {
                        "id": "W1",
                        "layer_name": "A-WALL",
                        "record": {
                            "entity": "LINE",
                            "data": {
                                "start_point": [1, 2, 0],
                                "end_point": [1, 5, 0],
                            },
                        },
                    },
                    "sibling": {
                        "type": "LINE",
                        "handle": "W2",
                        "layer": "B-WALL",
                        "start": {"x": 2, "y": 1, "z": 0},
                        "end": {"x": 6, "y": 1, "z": 0},
                    },
                }
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    assert len(entities) == 2
    assert [entity["source_entity_handle"] for entity in entities] == ["W1", "W2"]
    assert [entity["layer_name"] for entity in entities] == ["A-WALL", "B-WALL"]

    diagnostic_details = cast(Mapping[str, object], result.diagnostics[0].details)
    assert diagnostic_details["entity_counts"] == _with_hatch_counts(
        {
            "drawable_candidates": 2,
            "supported_geometry": 2,
            "supported_lines": 2,
            "supported_text": 0,
            "unsupported_drawables": 0,
            "malformed_drawables": 0,
        }
    )


@pytest.mark.asyncio
async def test_libredwg_adapter_keeps_list_wrapper_record_and_sibling_drawable_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "data": {
                            "id": "LW1",
                            "layer_name": "A-WALL",
                            "record": {
                                "entity": "LINE",
                                "data": {
                                    "start_point": [1, 2, 0],
                                    "end_point": [1, 5, 0],
                                },
                            },
                        },
                        "sibling": {
                            "type": "LINE",
                            "handle": "LW2",
                            "layer": "B-WALL",
                            "start": {"x": 2, "y": 1, "z": 0},
                            "end": {"x": 6, "y": 1, "z": 0},
                        },
                    }
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    assert len(entities) == 2
    assert [entity["source_entity_handle"] for entity in entities] == ["LW1", "LW2"]
    assert [entity["layer_name"] for entity in entities] == ["A-WALL", "B-WALL"]

    diagnostic_details = cast(Mapping[str, object], result.diagnostics[0].details)
    assert diagnostic_details["entity_counts"] == _with_hatch_counts(
        {
            "drawable_candidates": 2,
            "supported_geometry": 2,
            "supported_lines": 2,
            "supported_text": 0,
            "unsupported_drawables": 0,
            "malformed_drawables": 0,
        }
    )


@pytest.mark.asyncio
async def test_libredwg_adapter_maps_legacy_type_records_with_fixed_type_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "DICTIONARY",
                        "fixed_type": "LINE",
                        "handle": "F1",
                        "layer": "Legacy-Lines",
                        "start": {"x": 10, "y": 5, "z": 0},
                        "end": {"x": 13, "y": 9, "z": 0},
                    }
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]
    assert entity["entity_id"] == "libredwg-line-f1"
    assert entity["entity_type"] == "line"
    assert entity["kind"] == "line"
    assert entity["source_entity_handle"] == "F1"
    assert entity["geometry"] == {
        "start": {"x": 10.0, "y": 5.0, "z": 0.0},
        "end": {"x": 13.0, "y": 9.0, "z": 0.0},
        "bbox": {
            "min": {"x": 10.0, "y": 5.0, "z": 0.0},
            "max": {"x": 13.0, "y": 9.0, "z": 0.0},
        },
        "units": {"normalized": "unknown"},
        "geometry_summary": {
            "kind": "line_segment",
            "length": 5.0,
            "vertex_count": 2,
        },
    }
    assert entity["bbox"] == {
        "min": {"x": 10.0, "y": 5.0, "z": 0.0},
        "max": {"x": 13.0, "y": 9.0, "z": 0.0},
    }
    assert entity["length"] == 5.0
    assert entity["start"] == {"x": 10.0, "y": 5.0, "z": 0.0}
    assert entity["end"] == {"x": 13.0, "y": 9.0, "z": 0.0}
    assert entity["provenance"]["source_ref"] == "OBJECTS/LINE/F1"
    assert entity["provenance"]["source_locator"] == "OBJECTS/LINE/F1"
    assert entity["provenance"]["extraction_path"] == ("OBJECTS", "LINE")
    assert entity["confidence"]["score"] == adapter_module._LINE_ENTITY_CONFIDENCE_SCORE
    assert entity["confidence"]["review_required"] is True
    assert entity["confidence"]["basis"] == "libredwg_line_mapping_units_unconfirmed"


@pytest.mark.asyncio
async def test_libredwg_adapter_maps_circle_entities_into_canonical_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "CIRCLE",
                        "handle": "2A",
                        "layout": "Detail-01",
                        "layer": "Curves",
                        "block": "Valve-Block",
                        "center": {"x": 5, "y": 7, "z": 0},
                        "radius": 2.5,
                    }
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]

    assert entity["entity_id"] == "libredwg-circle-2a"
    assert entity["entity_type"] == "circle"
    assert entity["layout_ref"] == "Detail-01"
    assert entity["layer_ref"] == "Curves"
    assert entity["block_ref"] == "Valve-Block"
    assert entity["bbox"] == {
        "min": {"x": 2.5, "y": 4.5, "z": 0.0},
        "max": {"x": 7.5, "y": 9.5, "z": 0.0},
    }
    assert entity["geometry"] == {
        "center": {"x": 5.0, "y": 7.0, "z": 0.0},
        "radius": 2.5,
        "bbox": {
            "min": {"x": 2.5, "y": 4.5, "z": 0.0},
            "max": {"x": 7.5, "y": 9.5, "z": 0.0},
        },
        "units": {"normalized": "unknown"},
        "geometry_summary": {
            "kind": "circle",
            "radius": 2.5,
            "diameter": 5.0,
            "circumference": pytest.approx(5.0 * math.pi),
        },
    }
    assert entity["properties"] == {
        "source_type": "CIRCLE",
        "source_handle": "2A",
        "quantity_hints": {
            "length": pytest.approx(5.0 * math.pi),
            "area": pytest.approx(6.25 * math.pi),
            "count": 1.0,
        },
        "adapter_native": {
            "libredwg": {
                "section": "OBJECTS",
                "record_type": "CIRCLE",
                "handle": "2A",
            }
        },
    }
    assert entity["kind"] == "circle"
    assert entity["center"] == {"x": 5.0, "y": 7.0, "z": 0.0}
    assert entity["radius"] == 2.5
    assert entity["diameter"] == 5.0
    assert entity["provenance"]["source_ref"] == "OBJECTS/CIRCLE/2A"
    assert entity["provenance"]["source_hash"] == adapter_module._canonical_hash_json_value(
        {
            "record_type": "CIRCLE",
            "handle": "2A",
            "layer_name": "Curves",
            "layout_name": "Detail-01",
            "block_name": "Valve-Block",
            "geometry": {
                "center": {"x": 5.0, "y": 7.0, "z": 0.0},
                "radius": 2.5,
            },
        }
    )
    assert entity["confidence"] == {
        "score": adapter_module._LINE_ENTITY_CONFIDENCE_SCORE,
        "review_required": True,
        "basis": "libredwg_circle_mapping_units_unconfirmed",
    }
    assert not _contains_non_finite_numbers(entity)
    assert result.confidence is not None
    assert result.confidence.score == adapter_module._LINE_ENTITY_CONFIDENCE_SCORE
    assert result.confidence.basis == "libredwg_dwgread_json_geometry_mapping"
    assert [warning.code for warning in result.warnings] == ["libredwg.units_unconfirmed"]


@pytest.mark.asyncio
async def test_libredwg_adapter_maps_arc_entities_into_canonical_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "ARC",
                        "handle": "2B",
                        "layer": "Curves",
                        "center": {"x": 10, "y": 20, "z": 0},
                        "radius": 5,
                        # dwgread emits arc angles in radians (no angle_units); 45°/135° arc.
                        "start_angle": math.radians(45),
                        "end_angle": math.radians(135),
                    }
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]

    assert entity["entity_id"] == "libredwg-arc-2b"
    assert entity["entity_type"] == "arc"
    assert entity["layout_ref"] == "Model"
    assert entity["layer_ref"] == "Curves"
    assert entity["block_ref"] is None
    assert entity["center"] == {"x": 10.0, "y": 20.0, "z": 0.0}
    assert entity["radius"] == 5.0
    assert entity["start_angle_degrees"] == 45.0
    assert entity["end_angle_degrees"] == 135.0
    assert entity["start"] == pytest.approx({"x": 13.5355339059, "y": 23.5355339059, "z": 0.0})
    assert entity["end"] == pytest.approx({"x": 6.4644660941, "y": 23.5355339059, "z": 0.0})
    assert entity["bbox"]["min"]["x"] == pytest.approx(6.4644660941)
    assert entity["bbox"]["min"]["y"] == pytest.approx(23.5355339059)
    assert entity["bbox"]["min"]["z"] == pytest.approx(0.0)
    assert entity["bbox"]["max"]["x"] == pytest.approx(13.5355339059)
    assert entity["bbox"]["max"]["y"] == pytest.approx(25.0)
    assert entity["bbox"]["max"]["z"] == pytest.approx(0.0)
    assert entity["geometry"]["geometry_summary"] == {
        "kind": "arc",
        "radius": 5.0,
        "start_angle_degrees": 45.0,
        "end_angle_degrees": 135.0,
        "sweep_degrees": 90.0,
        "length": pytest.approx(2.5 * math.pi),
    }
    assert entity["properties"]["quantity_hints"] == {
        "length": pytest.approx(2.5 * math.pi),
        "count": 1.0,
    }
    assert entity["confidence"] == {
        "score": adapter_module._LINE_ENTITY_CONFIDENCE_SCORE,
        "review_required": True,
        "basis": "libredwg_arc_mapping_units_unconfirmed",
    }
    assert not _contains_non_finite_numbers(entity)
    assert result.confidence is not None
    assert result.confidence.basis == "libredwg_dwgread_json_geometry_mapping"
    assert [warning.code for warning in result.warnings] == ["libredwg.units_unconfirmed"]


@pytest.mark.asyncio
async def test_libredwg_adapter_maps_lwpolyline_entities_into_canonical_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "LWPOLYLINE",
                        "handle": "2C",
                        "layer": "Perimeter",
                        "flags": 1,
                        "vertices": [
                            {"x": 0, "y": 0, "bulge": 0},
                            {"x": 3, "y": 4},
                            {"x": 3, "y": 0},
                        ],
                    }
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]

    assert entity["entity_id"] == "libredwg-polyline-2c"
    assert entity["entity_type"] == "polyline"
    assert entity["layout_ref"] == "Model"
    assert entity["layer_ref"] == "Perimeter"
    assert entity["block_ref"] is None
    assert entity["bbox"] == {
        "min": {"x": 0.0, "y": 0.0, "z": 0.0},
        "max": {"x": 3.0, "y": 4.0, "z": 0.0},
    }
    assert entity["geometry"] == {
        "vertices": (
            {"x": 0.0, "y": 0.0, "z": 0.0},
            {"x": 3.0, "y": 4.0, "z": 0.0},
            {"x": 3.0, "y": 0.0, "z": 0.0},
        ),
        "closed": True,
        "bbox": {
            "min": {"x": 0.0, "y": 0.0, "z": 0.0},
            "max": {"x": 3.0, "y": 4.0, "z": 0.0},
        },
        "units": {"normalized": "unknown"},
        "geometry_summary": {
            "kind": "polyline",
            "length": 12.0,
            "vertex_count": 3,
            "closed": True,
        },
    }
    assert entity["properties"] == {
        "source_type": "LWPOLYLINE",
        "source_handle": "2C",
        "quantity_hints": {
            "length": 12.0,
            "count": 1.0,
        },
        "adapter_native": {
            "libredwg": {
                "section": "OBJECTS",
                "record_type": "LWPOLYLINE",
                "handle": "2C",
            }
        },
    }
    assert entity["kind"] == "polyline"
    assert entity["vertices"] == (
        {"x": 0.0, "y": 0.0, "z": 0.0},
        {"x": 3.0, "y": 4.0, "z": 0.0},
        {"x": 3.0, "y": 0.0, "z": 0.0},
    )
    assert entity["closed"] is True
    assert entity["length"] == 12.0
    assert entity["confidence"] == {
        "score": adapter_module._LINE_ENTITY_CONFIDENCE_SCORE,
        "review_required": True,
        "basis": "libredwg_lwpolyline_mapping_units_unconfirmed",
    }
    assert not _contains_non_finite_numbers(entity)
    assert result.confidence is not None
    assert result.confidence.basis == "libredwg_dwgread_json_geometry_mapping"
    assert [warning.code for warning in result.warnings] == ["libredwg.units_unconfirmed"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("flag", "expected_closed"),
    [
        (0x200, True),  # LibreDWG closed bit
        (0x300, True),  # LibreDWG closed (0x200) + a benign bit (0x100)
        (0x100, False),  # benign bit only -> open, not rejected
        (128, False),  # plinegen (benign) -> open, not rejected
    ],
)
async def test_libredwg_adapter_maps_lwpolyline_flag_bits(
    monkeypatch: pytest.MonkeyPatch,
    flag: int,
    expected_closed: bool,
) -> None:
    result = await _ingest_output_payload(
        monkeypatch,
        {
            "OBJECTS": [
                {
                    "type": "LWPOLYLINE",
                    "handle": "1A",
                    "layer": "0",
                    "flag": flag,
                    "points": [[0, 0], [1, 0], [1, 1]],
                }
            ]
        },
    )

    entity = _single_entity(result)
    assert entity["entity_type"] == "polyline"
    assert entity["geometry"]["closed"] is expected_closed


@pytest.mark.asyncio
@pytest.mark.parametrize("closed_value", ["unknown", 2])
async def test_libredwg_adapter_rejects_invalid_lwpolyline_closed_values(
    monkeypatch: pytest.MonkeyPatch,
    closed_value: object,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "LWPOLYLINE",
                        "handle": "2D",
                        "layer": "Broken",
                        "closed": closed_value,
                        "vertices": [
                            {"x": 0, "y": 0},
                            {"x": 3, "y": 4},
                        ],
                    }
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    assert len(entities) == 1
    assert entities[0]["entity_type"] == "unknown"
    assert entities[0]["geometry"]["status"] == "absent"
    assert entities[0]["geometry"]["reason"] == "malformed_lwpolyline_geometry"
    assert [warning.code for warning in result.warnings] == [
        "libredwg.units_unconfirmed",
        "libredwg.malformed_drawable_record",
    ]


@pytest.mark.asyncio
async def test_libredwg_adapter_maps_mtext_entities_into_canonical_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "MTEXT",
                        "handle": "7A",
                        "layer": "Labels",
                        "insertion": {"x": 10, "y": 20, "z": 0},
                        "text": "Zone A-01",
                    }
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]

    assert entity["entity_id"] == "libredwg-text-7a"
    assert entity["entity_type"] == "text"
    assert entity["entity_schema_version"] == "0.1"
    assert entity["source_entity_handle"] == "7A"
    assert entity["layout_name"] == "Model"
    assert entity["layer_name"] == "Labels"
    assert entity["block_name"] is None
    assert entity["parent_entity_id"] is None
    assert entity["drawing_revision_id"] is None
    assert entity["source_file_id"] is None
    assert entity["layout_ref"] == "Model"
    assert entity["layer_ref"] == "Labels"
    assert entity["block_ref"] is None
    assert entity["parent_entity_ref"] is None
    assert entity["bbox"] == {
        "min": {"x": 10.0, "y": 20.0, "z": 0.0},
        "max": {"x": 10.0, "y": 20.0, "z": 0.0},
    }
    assert entity["geometry"] == {
        "text": "Zone A-01",
        "units": {"normalized": "unknown"},
        "geometry_summary": {
            "kind": "text",
            "source_type": "MTEXT",
            "text_length": 9,
        },
        "insertion": {"x": 10.0, "y": 20.0, "z": 0.0},
    }
    assert entity["properties"] == {
        "source_type": "MTEXT",
        "source_handle": "7A",
        "text": "Zone A-01",
        "text_length": 9,
        "adapter_native": {
            "libredwg": {
                "section": "OBJECTS",
                "record_type": "MTEXT",
                "handle": "7A",
            }
        },
    }
    assert entity["kind"] == "text"
    assert entity["text"] == "Zone A-01"
    assert entity["text_length"] == 9
    assert entity["provenance"]["origin"] == "adapter_normalized"
    assert entity["provenance"]["adapter"] == {"key": "libredwg"}
    assert entity["provenance"]["source_ref"] == "OBJECTS/MTEXT/7A"
    assert entity["provenance"]["source_identity"] == "7A"
    assert entity["provenance"]["source_hash"] == adapter_module._canonical_hash_json_value(
        {
            "record_type": "MTEXT",
            "handle": "7A",
            "layer_name": "Labels",
            "layout_name": "Model",
            "block_name": None,
            "geometry": {"text": "Zone A-01", "insertion": {"x": 10.0, "y": 20.0, "z": 0.0}},
        }
    )
    assert entity["provenance"]["normalized_source_hash"] == entity["provenance"]["source_hash"]
    assert entity["provenance"]["extraction_path"] == ("OBJECTS", "MTEXT")
    assert entity["provenance"]["notes"] == ("units_unconfirmed",)
    assert entity["provenance"]["source_locator"] == "OBJECTS/MTEXT/7A"
    assert entity["provenance"]["extra"] == {
        "native": {
            "libredwg": {
                "section": "OBJECTS",
                "record_type": "MTEXT",
                "handle": "7A",
            }
        },
        "legacy_aliases": {
            "adapter_key": "libredwg",
            "source": "OBJECTS/MTEXT/7A",
            "source_section": "OBJECTS",
            "source_entity_ref": "OBJECTS/MTEXT/7A",
            "source_locator": "OBJECTS/MTEXT/7A",
            "entity_ref": "OBJECTS/MTEXT/7A",
            "normalized_source_hash": entity["provenance"]["source_hash"],
            "native_handle": "7A",
            "source_entity_handle": "7A",
            "record_hash": entity["provenance"]["record_hash"],
        },
    }
    assert len(entity["provenance"]["source_hash"]) == 64
    assert all(character in "0123456789abcdef" for character in entity["provenance"]["source_hash"])
    assert entity["provenance"]["record_hash"] == f"sha256:{entity['provenance']['source_hash']}"
    assert entity["confidence"] == {
        "score": adapter_module._TEXT_ENTITY_CONFIDENCE_SCORE,
        "review_required": True,
        "basis": "libredwg_mtext_mapping_units_unconfirmed",
    }
    _assert_score_within_libredwg_descriptor_range(entity["confidence"]["score"])

    assert result.confidence is not None
    assert result.confidence.score == adapter_module._TEXT_ENTITY_CONFIDENCE_SCORE
    assert result.confidence.review_required is True
    assert result.confidence.basis == "libredwg_dwgread_json_text_mapping"
    assert [warning.code for warning in result.warnings] == ["libredwg.units_unconfirmed"]


@pytest.mark.asyncio
async def test_libredwg_adapter_maps_mtext_ins_pt_list_into_canonical_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "MTEXT",
                        "handle": "7E",
                        "layer": "Labels",
                        "ins_pt": [12, 24, 0],
                        "text": "Zone A-02",
                    }
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]

    assert entity["entity_id"] == "libredwg-text-7e"
    assert entity["entity_type"] == "text"
    assert entity["kind"] == "text"
    assert entity["geometry"] == {
        "text": "Zone A-02",
        "units": {"normalized": "unknown"},
        "geometry_summary": {
            "kind": "text",
            "source_type": "MTEXT",
            "text_length": 9,
        },
        "insertion": {"x": 12.0, "y": 24.0, "z": 0.0},
    }
    assert entity["bbox"] == {
        "min": {"x": 12.0, "y": 24.0, "z": 0.0},
        "max": {"x": 12.0, "y": 24.0, "z": 0.0},
    }
    assert entity["properties"]["text"] == "Zone A-02"
    assert entity["properties"]["source_type"] == "MTEXT"
    assert entity["text"] == "Zone A-02"
    assert entity["text_length"] == 9
    assert entity["confidence"]["score"] == adapter_module._TEXT_ENTITY_CONFIDENCE_SCORE
    assert entity["confidence"]["review_required"] is True


@pytest.mark.asyncio
async def test_libredwg_adapter_maps_mtext_without_insertion_as_partial_text_entity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "MTEXT",
                        "handle": "7B",
                        "layer": "Labels",
                        "text": "No insertion",
                    }
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]

    assert entity["entity_type"] == "text"
    assert entity["layout_ref"] == "Model"
    assert entity["layer_ref"] == "Labels"
    assert entity["block_ref"] is None
    assert entity["bbox"] is None
    assert entity["geometry"] == {
        "text": "No insertion",
        "units": {"normalized": "unknown"},
        "status": "absent",
        "reason": "missing_text_placement",
        "geometry_summary": {
            "kind": "text",
            "source_type": "MTEXT",
            "text_length": 12,
            "reason": "missing_text_placement",
        },
        "bbox": None,
    }
    assert entity["confidence"]["score"] == adapter_module._TEXT_ENTITY_CONFIDENCE_SCORE
    assert entity["confidence"]["basis"] == "libredwg_mtext_mapping_units_unconfirmed"
    assert result.confidence is not None
    assert result.confidence.basis == "libredwg_dwgread_json_text_mapping"
    assert [warning.code for warning in result.warnings] == ["libredwg.units_unconfirmed"]


@pytest.mark.asyncio
async def test_libredwg_adapter_maps_mtext_text_value_with_control_characters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "MTEXT",
                        "handle": "7C",
                        "layer": "Notes",
                        "insertion": {"x": 0, "y": 0, "z": 0},
                        "text": "Safe\nLabel\u0000Path:/tmp/file.txt",
                    }
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]

    assert entity["entity_id"] == "libredwg-text-7c"
    assert entity["text"] == "Safe\nLabel?Path:/tmp/file.txt"
    assert entity["geometry"]["text"] == "Safe\nLabel?Path:/tmp/file.txt"
    assert entity["properties"]["text"] == "Safe\nLabel?Path:/tmp/file.txt"


@pytest.mark.asyncio
async def test_libredwg_adapter_marks_nonfinite_text_placement_as_malformed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "MTEXT",
                        "handle": "7D",
                        "layer": "Labels",
                        "insertion": {"x": "NaN", "y": 1, "z": 0},
                        "text": "Bad placement",
                    }
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]

    assert entity["geometry"]["status"] == "absent"
    assert entity["geometry"]["reason"] == "malformed_text_placement"
    assert entity["geometry"]["geometry_summary"]["reason"] == "malformed_text_placement"
    assert entity["bbox"] is None
    assert "insertion" not in entity["geometry"]
    assert not _contains_non_finite_numbers(entity["geometry"])
    assert [warning.code for warning in result.warnings] == ["libredwg.units_unconfirmed"]


@pytest.mark.asyncio
async def test_libredwg_adapter_maps_supported_hatch_entities_into_canonical_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {"type": "DICTIONARY", "handle": "10"},
                    {"type": "LAYER", "name": "Model"},
                    {
                        "type": "HATCH",
                        "handle": "20",
                        "layout": "Sheet-H1",
                        "layer": "Filled",
                        "block": "Hatch-Block",
                        "boundary_loops": [
                            {
                                "flags": 1,
                                "edges": [
                                    {
                                        "type": "LINE",
                                        "start": {"x": 4, "y": 0, "z": 0},
                                        "end": {"x": 4, "y": 3, "z": 0},
                                    },
                                    {
                                        "type": "LINE",
                                        "start": {"x": 0, "y": 0, "z": 0},
                                        "end": {"x": 4, "y": 0, "z": 0},
                                    },
                                    {
                                        "type": "LINE",
                                        "start": {"x": 0, "y": 3, "z": 0},
                                        "end": {"x": 0, "y": 0, "z": 0},
                                    },
                                    {
                                        "type": "LINE",
                                        "start": {"x": 4, "y": 3, "z": 0},
                                        "end": {"x": 0, "y": 3, "z": 0},
                                    },
                                ],
                            }
                        ],
                    },
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(tuple[dict[str, Any], ...], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]
    assert entity["entity_id"] == "libredwg-hatch-20"
    assert entity["entity_type"] == "hatch"
    assert entity["entity_schema_version"] == "0.1"
    assert entity["source_entity_handle"] == "20"
    assert entity["layout_name"] == "Sheet-H1"
    assert entity["layer_name"] == "Filled"
    assert entity["block_name"] == "Hatch-Block"
    assert entity["layout_ref"] == "Sheet-H1"
    assert entity["layer_ref"] == "Filled"
    assert entity["block_ref"] == "Hatch-Block"
    assert entity["bbox"] == {
        "min": {"x": 0.0, "y": 0.0, "z": 0.0},
        "max": {"x": 4.0, "y": 3.0, "z": 0.0},
    }
    _hatch_square_loop = (
        {"x": 4.0, "y": 0.0, "z": 0.0},
        {"x": 4.0, "y": 3.0, "z": 0.0},
        {"x": 0.0, "y": 3.0, "z": 0.0},
        {"x": 0.0, "y": 0.0, "z": 0.0},
    )
    assert entity["geometry"] == {
        "vertices": _hatch_square_loop,
        "boundary_loops": (_hatch_square_loop,),
        "closed": True,
        "bbox": {
            "min": {"x": 0.0, "y": 0.0, "z": 0.0},
            "max": {"x": 4.0, "y": 3.0, "z": 0.0},
        },
        "units": {"normalized": "unknown"},
        "geometry_summary": {
            "kind": "hatch",
            "vertex_count": 4,
            "loop_count": 1,
            "fill_type": "solid",
            "closed": True,
            "area": 12.0,
            "perimeter": 14.0,
        },
    }
    assert entity["properties"] == {
        "source_type": "HATCH",
        "source_handle": "20",
        "fill_type": "solid",
        "quantity_hints": {"length": 14.0, "area": 12.0, "count": 1.0},
        "adapter_native": {
            "libredwg": {
                "section": "OBJECTS",
                "record_type": "HATCH",
                "handle": "20",
            }
        },
    }
    assert entity["kind"] == "hatch"
    assert entity["vertices"] == (
        {"x": 4.0, "y": 0.0, "z": 0.0},
        {"x": 4.0, "y": 3.0, "z": 0.0},
        {"x": 0.0, "y": 3.0, "z": 0.0},
        {"x": 0.0, "y": 0.0, "z": 0.0},
    )
    assert entity["closed"] is True
    assert entity["area"] == 12.0
    assert entity["perimeter"] == 14.0
    assert entity["provenance"]["origin"] == "adapter_normalized"
    assert entity["provenance"]["adapter"] == {"key": "libredwg"}
    assert entity["provenance"]["source_ref"] == "OBJECTS/HATCH/20"
    assert entity["provenance"]["source_identity"] == "20"
    assert entity["provenance"]["source_hash"] == adapter_module._canonical_hash_json_value(
        {
            "record_type": "HATCH",
            "handle": "20",
            "layer_name": "Filled",
            "layout_name": "Sheet-H1",
            "block_name": "Hatch-Block",
            "geometry": {
                "vertices": (
                    {"x": 4.0, "y": 0.0, "z": 0.0},
                    {"x": 4.0, "y": 3.0, "z": 0.0},
                    {"x": 0.0, "y": 3.0, "z": 0.0},
                    {"x": 0.0, "y": 0.0, "z": 0.0},
                ),
                "closed": True,
            },
        }
    )
    assert entity["provenance"]["normalized_source_hash"] == entity["provenance"]["source_hash"]
    assert entity["provenance"]["extraction_path"] == ("OBJECTS", "HATCH")
    assert entity["provenance"]["notes"] == ("units_unconfirmed",)
    assert entity["provenance"]["extra"] == {
        "native": {
            "libredwg": {
                "section": "OBJECTS",
                "record_type": "HATCH",
                "handle": "20",
            }
        },
        "legacy_aliases": {
            "adapter_key": "libredwg",
            "source": "OBJECTS/HATCH/20",
            "source_section": "OBJECTS",
            "source_entity_ref": "OBJECTS/HATCH/20",
            "source_locator": "OBJECTS/HATCH/20",
            "entity_ref": "OBJECTS/HATCH/20",
            "normalized_source_hash": entity["provenance"]["source_hash"],
            "native_handle": "20",
            "source_entity_handle": "20",
            "record_hash": f"sha256:{entity['provenance']['source_hash']}",
        },
    }
    assert len(entity["provenance"]["source_hash"]) == 64
    assert all(character in "0123456789abcdef" for character in entity["provenance"]["source_hash"])
    assert entity["provenance"]["record_hash"] == f"sha256:{entity['provenance']['source_hash']}"
    assert entity["confidence"] == {
        "score": adapter_module._LINE_ENTITY_CONFIDENCE_SCORE,
        "review_required": True,
        "basis": "libredwg_hatch_mapping_units_unconfirmed",
    }
    assert result.canonical["layouts"] == ({"name": "Model"}, {"name": "Sheet-H1"})
    assert result.canonical["blocks"] == (
        {
            "name": "Hatch-Block",
            "block_ref": "Hatch-Block",
            "block_handle": None,
            "base_point": {"x": 0.0, "y": 0.0, "z": 0.0},
            "entities": (),
        },
    )
    assert result.confidence is not None
    assert result.confidence.score == adapter_module._LINE_ENTITY_CONFIDENCE_SCORE
    assert result.confidence.basis == "libredwg_dwgread_json_geometry_mapping"
    assert not _contains_non_finite_numbers(entity)
    _assert_score_within_libredwg_descriptor_range(entity["confidence"]["score"])
    assert [warning.code for warning in result.warnings] == ["libredwg.units_unconfirmed"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("output_payload", "expected_handle"),
    [
        (
            {
                "OBJECTS": [
                    {
                        "type": "HATCH",
                        "handle": "30",
                        "layout": "Sheet-H2",
                        "layer": "Filled",
                        "points": _HATCH_SQUARE_POINTS,
                    }
                ]
            },
            "30",
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "HATCH",
                        "handle": "31",
                        "layout": "Sheet-H2",
                        "layer": "Filled",
                        "vertices": _HATCH_SQUARE_POINTS,
                    }
                ]
            },
            "31",
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "HATCH",
                        "handle": "32",
                        "layout": "Sheet-H2",
                        "layer": "Filled",
                        "boundary_loops": [{"points": _HATCH_SQUARE_POINTS}],
                    }
                ]
            },
            "32",
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "HATCH",
                        "handle": "33",
                        "layout": "Sheet-H2",
                        "layer": "Filled",
                        "boundary_loops": [{"vertices": _HATCH_SQUARE_POINTS}],
                    }
                ]
            },
            "33",
        ),
    ],
)
async def test_libredwg_adapter_maps_point_and_vertex_hatch_variants_into_canonical_payload(
    monkeypatch: pytest.MonkeyPatch,
    output_payload: dict[str, Any],
    expected_handle: str,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(monkeypatch, process=process, output_text=json.dumps(output_payload))
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(tuple[dict[str, Any], ...], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]
    assert entity["entity_id"] == f"libredwg-hatch-{expected_handle}"
    assert entity["entity_type"] == "hatch"
    assert entity["kind"] == "hatch"
    assert entity["geometry"]["geometry_summary"] == {
        "kind": "hatch",
        "vertex_count": 4,
        "loop_count": 1,
        "fill_type": "solid",
        "closed": True,
        "area": 12.0,
        "perimeter": 14.0,
    }
    assert entity["geometry"]["vertices"] == _HATCH_SQUARE_POINTS
    assert entity["geometry"]["boundary_loops"] == (_HATCH_SQUARE_POINTS,)
    assert entity["vertices"] == _HATCH_SQUARE_POINTS
    assert entity["closed"] is True
    assert entity["area"] == 12.0
    assert entity["perimeter"] == 14.0
    assert [warning.code for warning in result.warnings] == ["libredwg.units_unconfirmed"]

    diagnostic_details = cast(Mapping[str, object], result.diagnostics[0].details)
    assert diagnostic_details["entity_counts"] == _with_hatch_counts(
        {
            "drawable_candidates": 1,
            "supported_geometry": 1,
            "supported_lines": 0,
            "supported_text": 0,
            "unsupported_drawables": 0,
            "malformed_drawables": 0,
        }
    )


@pytest.mark.asyncio
async def test_libredwg_adapter_dedupes_consecutive_hatch_boundary_vertices(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    duplicated_points = (
        _HATCH_SQUARE_POINTS[0],
        _HATCH_SQUARE_POINTS[1],
        _HATCH_SQUARE_POINTS[1],
        _HATCH_SQUARE_POINTS[2],
        _HATCH_SQUARE_POINTS[3],
    )
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "HATCH",
                        "handle": "34",
                        "layout": "Sheet-H2",
                        "layer": "Filled",
                        "boundary_loops": [{"points": list(duplicated_points)}],
                    }
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(tuple[dict[str, Any], ...], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]
    assert entity["entity_type"] == "hatch"
    assert entity["geometry"]["geometry_summary"]["vertex_count"] == 4
    assert entity["geometry"]["vertices"] == _HATCH_SQUARE_POINTS
    assert entity["vertices"] == _HATCH_SQUARE_POINTS
    assert entity["area"] == 12.0
    assert [warning.code for warning in result.warnings] == ["libredwg.units_unconfirmed"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("output_payload", "expected_handle"),
    [
        (
            {
                "OBJECTS": [
                    {
                        "type": "HATCH",
                        "handle": "40",
                        "layout": "Sheet-H3",
                        "layer": "Curved",
                        "boundary_loops": [
                            {
                                "edges": [
                                    {
                                        "type": "ARC",
                                        "center": {"x": 2, "y": 1, "z": 0},
                                        "radius": 1,
                                        "start_angle": 0,
                                        "end_angle": 90,
                                    }
                                ]
                            }
                        ],
                    }
                ]
            },
            "40",
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "HATCH",
                        "handle": "41",
                        "layout": "Sheet-H3",
                        "layer": "Bulged",
                        "boundary_loops": [
                            {
                                "edges": [
                                    {
                                        "type": "LINE",
                                        "start": {"x": 0, "y": 0, "z": 0},
                                        "end": {"x": 2, "y": 0, "z": 0},
                                        "bulge": 0.25,
                                    }
                                ]
                            }
                        ],
                    }
                ]
            },
            "41",
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "HATCH",
                        "handle": "43",
                        "layout": "Sheet-H3",
                        "layer": "VertexBulge",
                        "boundary_loops": [
                            {
                                "vertices": [
                                    {"x": 0, "y": 0, "z": 0, "bulge": 0.25},
                                    {"x": 4, "y": 0, "z": 0},
                                    {"x": 4, "y": 3, "z": 0},
                                    {"x": 0, "y": 3, "z": 0},
                                ]
                            }
                        ],
                    }
                ]
            },
            "43",
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "HATCH",
                        "handle": "44",
                        "layout": "Sheet-H3",
                        "layer": "MixedBoundary",
                        "boundary_loops": [
                            {
                                "points": _HATCH_SQUARE_POINTS,
                                "edges": [
                                    {
                                        "type": "ARC",
                                        "center": {"x": 2, "y": 1.5, "z": 0},
                                        "radius": 1,
                                        "start_angle": 0,
                                        "end_angle": 90,
                                    }
                                ],
                            }
                        ],
                    }
                ]
            },
            "44",
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "HATCH",
                        "handle": "45",
                        "layout": "Sheet-H3",
                        "layer": "OwnerBulgeLoopPoints",
                        "bulge": 0.25,
                        "boundary_loops": [{"points": _HATCH_SQUARE_POINTS}],
                    }
                ]
            },
            "45",
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "HATCH",
                        "handle": "46",
                        "layout": "Sheet-H3",
                        "layer": "OwnerBulge",
                        "points": _HATCH_SQUARE_POINTS,
                        "bulge": 0.25,
                    }
                ]
            },
            "46",
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "HATCH",
                        "handle": "47",
                        "layout": "Sheet-H3",
                        "layer": "LoopOwnerBulge",
                        "boundary_loops": [
                            {
                                "points": _HATCH_SQUARE_POINTS,
                                "bulge": 0.25,
                            }
                        ],
                    }
                ]
            },
            "47",
        ),
    ],
)
async def test_libredwg_adapter_degrades_curved_bulged_or_ambiguous_hatch_geometry_to_unknown(
    monkeypatch: pytest.MonkeyPatch,
    output_payload: dict[str, Any],
    expected_handle: str,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(monkeypatch, process=process, output_text=json.dumps(output_payload))
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(tuple[dict[str, Any], ...], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]
    assert entity["entity_id"] == f"libredwg-unknown-{expected_handle}"
    assert entity["entity_type"] == "unknown"
    assert entity["geometry"]["status"] == "absent"
    assert entity["geometry"]["reason"] == "unsupported_hatch_geometry"
    assert entity["geometry"]["geometry_summary"] == {
        "kind": "unknown",
        "source_type": "HATCH",
        "reason": "unsupported_hatch_geometry",
    }
    assert entity["geometry_reason"] == "unsupported_hatch_geometry"
    assert entity["unknown_reason"] == "unsupported_hatch_geometry"
    assert [warning.code for warning in result.warnings] == [
        "libredwg.units_unconfirmed",
        "libredwg.unsupported_hatch_geometry",
    ]

    diagnostic_details = cast(Mapping[str, object], result.diagnostics[0].details)
    assert diagnostic_details["entity_counts"] == _with_hatch_counts(
        {
            "drawable_candidates": 1,
            "supported_geometry": 0,
            "supported_lines": 0,
            "supported_text": 0,
            "unsupported_drawables": 0,
            "malformed_drawables": 0,
            "unsupported_hatches": 1,
            "malformed_hatches": 0,
        }
    )


@pytest.mark.asyncio
async def test_libredwg_adapter_degrades_oversized_hatch_geometry_to_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "HATCH",
                        "handle": "45",
                        "layout": "Sheet-H4",
                        "layer": "Oversized",
                        "boundary_loops": [
                            {
                                "edges": _hatch_line_edges(
                                    adapter_module._MAX_HATCH_BOUNDARY_COMPONENTS + 1
                                )
                            }
                        ],
                    }
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(tuple[dict[str, Any], ...], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]
    assert entity["entity_id"] == "libredwg-unknown-45"
    assert entity["entity_type"] == "unknown"
    assert entity["geometry"]["status"] == "absent"
    assert entity["geometry"]["reason"] == "unsupported_hatch_geometry"
    assert entity["geometry_reason"] == "unsupported_hatch_geometry"
    assert entity["unknown_reason"] == "unsupported_hatch_geometry"
    assert [warning.code for warning in result.warnings] == [
        "libredwg.units_unconfirmed",
        "libredwg.unsupported_hatch_geometry",
    ]

    diagnostic_details = cast(Mapping[str, object], result.diagnostics[0].details)
    assert diagnostic_details["entity_counts"] == _with_hatch_counts(
        {
            "drawable_candidates": 1,
            "supported_geometry": 0,
            "supported_lines": 0,
            "supported_text": 0,
            "unsupported_drawables": 0,
            "malformed_drawables": 0,
            "unsupported_hatches": 1,
            "malformed_hatches": 0,
        }
    )


@pytest.mark.asyncio
async def test_libredwg_adapter_maps_multi_loop_hatch_with_holes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hatch with an outer boundary and an inner hole maps; area is outer minus hole."""
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "HATCH",
                        "handle": "21",
                        "layout": "Sheet-H1",
                        "layer": "Filled",
                        "block": "Hatch-Block",
                        "boundary_loops": [
                            {
                                "flags": 1,
                                "points": [{"x": 0, "y": 0}, {"x": 2, "y": 0}, {"x": 0, "y": 2}],
                            },
                            {
                                "flags": 16,
                                "points": [
                                    {"x": 0.5, "y": 0.5},
                                    {"x": 1, "y": 0.5},
                                    {"x": 0.5, "y": 1},
                                ],
                            },
                        ],
                    },
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(tuple[dict[str, Any], ...], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]
    assert entity["entity_id"] == "libredwg-hatch-21"
    assert entity["entity_type"] == "hatch"
    assert entity["geometry"]["geometry_summary"]["loop_count"] == 2
    assert entity["geometry"]["geometry_summary"]["fill_type"] == "solid"
    assert len(entity["geometry"]["boundary_loops"]) == 2
    # Outer triangle area 2.0 minus inner-hole triangle area 0.125.
    assert entity["area"] == pytest.approx(1.875)
    assert entity["properties"]["quantity_hints"]["area"] == pytest.approx(1.875)
    assert [warning.code for warning in result.warnings] == ["libredwg.units_unconfirmed"]

    diagnostic_details = cast(Mapping[str, object], result.diagnostics[0].details)
    counts = cast(Mapping[str, int], diagnostic_details["entity_counts"])
    assert counts["supported_geometry"] == 1
    assert counts["unsupported_hatches"] == 0


@pytest.mark.asyncio
async def test_libredwg_adapter_maps_non_solid_pattern_hatch_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pattern (non-solid) fills map via their boundary geometry, tagged fill_type (#385)."""
    result = await _ingest_output_payload(
        monkeypatch,
        {
            "HEADER": {"INSUNITS": 4},
            "OBJECTS": [
                {
                    "type": "HATCH",
                    "handle": "2E",
                    "layer": "Patterned",
                    "solid": False,
                    "boundary_loops": [{"points": list(_HATCH_SQUARE_POINTS)}],
                }
            ],
        },
    )

    entities = cast(tuple[dict[str, Any], ...], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]
    assert entity["entity_type"] == "hatch"
    assert entity["properties"]["fill_type"] == "pattern"
    assert entity["geometry"]["geometry_summary"]["fill_type"] == "pattern"
    assert "libredwg.unsupported_hatch_geometry" not in [w.code for w in result.warnings]
    counts = cast(Mapping[str, int], result.canonical["metadata"]["entity_counts"])
    assert counts["supported_geometry"] == 1
    assert counts["unsupported_hatches"] == 0


@pytest.mark.asyncio
async def test_libredwg_adapter_degrades_malformed_hatch_geometry_to_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "HATCH",
                        "handle": "22",
                        "layer": "Broken",
                        "points": [
                            {"x": 0, "y": 0, "z": 0},
                            {"x": 2, "y": 0, "z": 1},
                            {"x": 0, "y": 2, "z": 0},
                        ],
                    }
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    assert len(entities) == 1
    entity = entities[0]

    assert entity["entity_id"] == "libredwg-unknown-22"
    assert entity["entity_type"] == "unknown"
    assert entity["geometry"]["status"] == "absent"
    assert entity["geometry"]["reason"] == "malformed_hatch_geometry"
    assert entity["geometry"]["geometry_summary"] == {
        "kind": "unknown",
        "source_type": "HATCH",
        "reason": "malformed_hatch_geometry",
    }
    assert entity["geometry_reason"] == "malformed_hatch_geometry"
    assert entity["unknown_reason"] == "malformed_hatch_geometry"
    assert [warning.code for warning in result.warnings] == [
        "libredwg.units_unconfirmed",
        "libredwg.malformed_hatch_geometry",
    ]


@pytest.mark.asyncio
async def test_libredwg_adapter_degrades_malformed_line_geometry_to_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "LINE",
                        "handle": "99",
                        "layer": "Broken",
                        "start": {"x": "NaN", "y": 0},
                        "end": {"x": 1, "y": 1},
                    }
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    assert len(entities) == 1
    assert entities[0]["entity_id"] == "libredwg-unknown-99"
    assert entities[0]["entity_type"] == "unknown"
    assert entities[0]["layout_ref"] == "Model"
    assert entities[0]["layer_ref"] == "Broken"
    assert entities[0]["block_ref"] is None
    assert entities[0]["geometry"] == {
        "bbox": None,
        "units": {"normalized": "unknown"},
        "status": "absent",
        "reason": "malformed_line_geometry",
        "geometry_summary": {
            "kind": "unknown",
            "source_type": "LINE",
            "reason": "malformed_line_geometry",
        },
    }
    assert entities[0]["geometry_reason"] == "malformed_line_geometry"
    assert entities[0]["unknown_reason"] == "malformed_line_geometry"
    assert entities[0]["layer_name"] == "Broken"
    assert [warning.code for warning in result.warnings] == [
        "libredwg.units_unconfirmed",
        "libredwg.malformed_drawable_record",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "output_payload",
        "expected_reason",
        "expected_source_type",
        "expected_warning_codes",
    ),
    [
        (
            {"OBJECTS": [{"type": "HATCH", "handle": "20", "layer": "Unsupported"}]},
            "unsupported_hatch_geometry",
            "HATCH",
            ("libredwg.units_unconfirmed", "libredwg.unsupported_hatch_geometry"),
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "HATCH",
                        "handle": "20B",
                        "layer": "Broken",
                        "points": [
                            {"x": 0, "y": 0, "z": 0},
                            {"x": 1, "y": 0, "z": 1},
                            {"x": 0, "y": 1, "z": 0},
                        ],
                    }
                ]
            },
            "malformed_hatch_geometry",
            "HATCH",
            ("libredwg.units_unconfirmed", "libredwg.malformed_hatch_geometry"),
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "HATCH",
                        "handle": "20C",
                        "layer": "Broken",
                        "boundary_loops": [{"points": 1}],
                    }
                ]
            },
            "malformed_hatch_geometry",
            "HATCH",
            ("libredwg.units_unconfirmed", "libredwg.malformed_hatch_geometry"),
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "HATCH",
                        "handle": "20D",
                        "layer": "Broken",
                        "boundary_loops": [{"points": None}],
                    }
                ]
            },
            "malformed_hatch_geometry",
            "HATCH",
            ("libredwg.units_unconfirmed", "libredwg.malformed_hatch_geometry"),
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "LINE",
                        "handle": "99",
                        "layer": "Broken",
                        "start": {"x": "NaN", "y": 0},
                        "end": {"x": 1, "y": 1},
                    }
                ]
            },
            "malformed_line_geometry",
            "LINE",
            ("libredwg.units_unconfirmed", "libredwg.malformed_drawable_record"),
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "CIRCLE",
                        "handle": "21",
                        "layer": "Broken",
                        "center": {"x": 0, "y": 0, "z": 0},
                    }
                ]
            },
            "malformed_circle_geometry",
            "CIRCLE",
            ("libredwg.units_unconfirmed", "libredwg.malformed_drawable_record"),
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "CIRCLE",
                        "handle": "21B",
                        "layer": "Broken",
                        "center": {"x": 1e308, "y": 1e308, "z": 0},
                        "radius": 1e308,
                    }
                ]
            },
            "malformed_circle_geometry",
            "CIRCLE",
            ("libredwg.units_unconfirmed", "libredwg.malformed_drawable_record"),
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "ARC",
                        "handle": "22",
                        "layer": "Broken",
                        "center": {"x": 0, "y": 0, "z": 0},
                        "radius": 2,
                        "start_angle": 0,
                    }
                ]
            },
            "malformed_arc_geometry",
            "ARC",
            ("libredwg.units_unconfirmed", "libredwg.malformed_drawable_record"),
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "ARC",
                        "handle": "22B",
                        "layer": "Broken",
                        "center": {"x": 1e308, "y": 1e308, "z": 0},
                        "radius": 1e308,
                        "start_angle": 0,
                        "end_angle": 90,
                    }
                ]
            },
            "malformed_arc_geometry",
            "ARC",
            ("libredwg.units_unconfirmed", "libredwg.malformed_drawable_record"),
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "ARC",
                        "handle": "22C",
                        "layer": "Broken",
                        "center": {"x": 0, "y": 0, "z": 0},
                        "radius": 2,
                        "start_angle": 0,
                        "end_angle": 90,
                        "angle_unit": "radians",
                    }
                ]
            },
            "malformed_arc_geometry",
            "ARC",
            ("libredwg.units_unconfirmed", "libredwg.malformed_drawable_record"),
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "ARC",
                        "handle": "22D",
                        "layer": "Broken",
                        "center": {"x": 0, "y": 0, "z": 0},
                        "radius": 2,
                        "start_angle": 0,
                        "end_angle": 90,
                        "direction": "clockwise",
                    }
                ]
            },
            "malformed_arc_geometry",
            "ARC",
            ("libredwg.units_unconfirmed", "libredwg.malformed_drawable_record"),
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "LWPOLYLINE",
                        "handle": "23",
                        "layer": "Broken",
                        "vertices": [
                            {"x": 0, "y": 0, "bulge": 0.25},
                            {"x": 1, "y": 1},
                        ],
                    }
                ]
            },
            "malformed_lwpolyline_geometry",
            "LWPOLYLINE",
            ("libredwg.units_unconfirmed", "libredwg.malformed_drawable_record"),
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "LWPOLYLINE",
                        "handle": "23B",
                        "layer": "Broken",
                        "vertices": [[0, 0, 0], [1, 1]],
                    }
                ]
            },
            "malformed_lwpolyline_geometry",
            "LWPOLYLINE",
            ("libredwg.units_unconfirmed", "libredwg.malformed_drawable_record"),
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "LWPOLYLINE",
                        "handle": "24",
                        "layer": "Broken",
                        "const_width": 0.5,
                        "vertices": [
                            {"x": 0, "y": 0},
                            {"x": 1, "y": 1},
                        ],
                    }
                ]
            },
            "malformed_lwpolyline_geometry",
            "LWPOLYLINE",
            ("libredwg.units_unconfirmed", "libredwg.malformed_drawable_record"),
        ),
        (
            {
                "OBJECTS": [
                    {
                        "type": "LWPOLYLINE",
                        "handle": "24C",
                        "layer": "Broken",
                        "vertices": [
                            {"x": -1e308, "y": 0},
                            {"x": 1e308, "y": 0},
                        ],
                    }
                ]
            },
            "malformed_lwpolyline_geometry",
            "LWPOLYLINE",
            ("libredwg.units_unconfirmed", "libredwg.malformed_drawable_record"),
        ),
    ],
)
async def test_libredwg_adapter_contract_accepts_unknown_geometry_entities(
    monkeypatch: pytest.MonkeyPatch,
    output_payload: dict[str, Any],
    expected_reason: str,
    expected_source_type: str,
    expected_warning_codes: tuple[str, ...],
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(monkeypatch, process=process, output_text=json.dumps(output_payload))
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    payload = await exercise_adapter_contract(
        adapter_module.create_adapter(),
        source=_build_source(),
        input_family=InputFamily.DWG,
        adapter_key="libredwg",
        expectation=ContractFinalizationExpectation(
            validation_status="needs_review",
            warning_codes=expected_warning_codes,
            diagnostic_codes=("libredwg.extract",),
        ),
    )

    entities = cast(list[dict[str, Any]], payload.canonical_json["entities"])
    assert len(entities) == 1
    assert entities[0]["geometry"]["status"] == "absent"
    assert entities[0]["geometry"]["reason"] == expected_reason
    assert entities[0]["geometry"]["geometry_summary"] == {
        "kind": "unknown",
        "source_type": expected_source_type,
        "reason": expected_reason,
    }


@pytest.mark.asyncio
async def test_libredwg_adapter_lowers_confidence_for_mixed_entity_outcomes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "LINE",
                        "handle": "1A",
                        "layer": "Walls",
                        "start": {"x": 1, "y": 2},
                        "end": {"x": 4, "y": 6},
                    },
                    {"type": "HATCH", "handle": "20", "layer": "Unsupported"},
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    assert result.confidence is not None
    assert result.confidence.score == adapter_module._MIXED_ENTITY_CONFIDENCE_SCORE
    assert result.confidence.review_required is True
    assert result.confidence.basis == "libredwg_dwgread_json_mixed_entity_mapping"
    _assert_score_within_libredwg_descriptor_range(result.confidence.score)


@pytest.mark.asyncio
async def test_libredwg_adapter_uses_geometry_confidence_for_non_line_supported_geometry_mix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": [
                    {
                        "type": "LINE",
                        "handle": "1A",
                        "layer": "Walls",
                        "start": {"x": 1, "y": 2},
                        "end": {"x": 4, "y": 6},
                    },
                    {
                        "type": "CIRCLE",
                        "handle": "2A",
                        "layer": "Curves",
                        "center": {"x": 5, "y": 7, "z": 0},
                        "radius": 2.5,
                    },
                ]
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    metadata = cast(dict[str, Any], result.canonical["metadata"])
    assert metadata["entity_counts"] == _with_hatch_counts(
        {
            "drawable_candidates": 2,
            "supported_geometry": 2,
            "supported_lines": 1,
            "supported_text": 0,
            "unsupported_drawables": 0,
            "malformed_drawables": 0,
        }
    )
    assert result.confidence is not None
    assert result.confidence.score == adapter_module._LINE_ENTITY_CONFIDENCE_SCORE
    assert result.confidence.basis == "libredwg_dwgread_json_geometry_mapping"


@pytest.mark.asyncio
async def test_libredwg_adapter_sets_non_placeholder_empty_reason_without_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "OBJECTS": {
                    "metadata": {
                        "type": "DICTIONARY",
                        "handle": "meta",
                        "data": {
                            "entity": "LINE",
                            "start": {"x": 0, "y": 0},
                            "end": {"x": 1, "y": 1},
                        },
                    },
                    "layers": [{"type": "LAYER", "name": "Default"}],
                }
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    assert result.canonical["entities"] == ()
    metadata = cast(dict[str, Any], result.canonical["metadata"])
    assert metadata["empty_entities_reason"] == "no_drawable_candidates_detected"
    assert metadata["entity_counts"] == _with_hatch_counts(
        {
            "drawable_candidates": 0,
            "supported_geometry": 0,
            "supported_lines": 0,
            "supported_text": 0,
            "unsupported_drawables": 0,
            "malformed_drawables": 0,
            # The DICTIONARY container and LAYER record are non-drawable and skipped.
            "skipped_non_drawable": 2,
        }
    )
    assert [warning.code for warning in result.warnings] == ["libredwg.units_unconfirmed"]
    assert result.confidence is not None
    _assert_score_within_libredwg_descriptor_range(result.confidence.score)


@pytest.mark.asyncio
async def test_libredwg_adapter_rejects_oversized_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oversized_output = json.dumps({"x": "y" * adapter_module._MAX_OUTPUT_BYTES})
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(monkeypatch, process=process, output_text=oversized_output)
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    with pytest.raises(adapter_module._OutputLimitExceededError, match="output limit"):
        await adapter_module.create_adapter().ingest(
            _build_source(),
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
        )


@pytest.mark.asyncio
async def test_libredwg_adapter_rejects_oversized_output_when_cap_is_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "libredwg_max_output_mb", 1)
    oversized_output = json.dumps({"x": "y" * (1024 * 1024)})
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(monkeypatch, process=process, output_text=oversized_output)
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    with pytest.raises(adapter_module._OutputLimitExceededError, match="JSON output exceeded"):
        await adapter_module.create_adapter().ingest(
            _build_source(),
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
        )


@pytest.mark.asyncio
async def test_libredwg_adapter_rejects_nonzero_exit_with_json_output_cap_saturation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "libredwg_max_output_mb", 1)
    oversized_output = json.dumps({"x": "y" * (1024 * 1024)})
    process = _FakeProcess(complete_with=3)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=oversized_output,
        stdout_text="failed for {source} in {tempdir}\n",
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    with pytest.raises(adapter_module._OutputLimitExceededError) as exc_info:
        await adapter_module.create_adapter().ingest(
            _build_source(),
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
        )

    assert exc_info.value.output_kind == "json"
    assert exc_info.value.max_output_bytes == 1 * 1024 * 1024
    assert exc_info.value.output_size_bytes >= exc_info.value.max_output_bytes


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overflow_path_kind", "payload_text", "expected_message"),
    [
        ("stdout", "A" * (adapter_module._MAX_STDOUT_BYTES + 1), "stdout exceeded"),
        ("stderr", "B" * (adapter_module._MAX_STDERR_BYTES + 1), "stderr exceeded"),
        (
            "output",
            json.dumps({"x": "y" * adapter_module._MAX_OUTPUT_BYTES}),
            "JSON output exceeded",
        ),
    ],
)
async def test_libredwg_adapter_kills_process_on_live_output_overflow(
    monkeypatch: pytest.MonkeyPatch,
    overflow_path_kind: str,
    payload_text: str,
    expected_message: str,
) -> None:
    process = _FakeProcess(complete_with=0, complete_when=asyncio.Event())
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        write_output=overflow_path_kind != "output",
        on_spawn=_schedule_live_overflow_write(
            overflow_path_kind=overflow_path_kind,
            payload_text=payload_text,
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")
    monkeypatch.setattr(adapter_module, "_PROCESS_POLL_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(adapter_module, "_PROCESS_TERMINATE_GRACE_SECONDS", 0.01)
    monkeypatch.setattr(adapter_module, "_PROCESS_KILL_GRACE_SECONDS", 0.01)

    with pytest.raises(RuntimeError, match=expected_message):
        await adapter_module.create_adapter().ingest(
            _build_source(),
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
        )

    assert process.terminate_calls == 1
    assert process.kill_calls == 1


@pytest.mark.asyncio
async def test_libredwg_adapter_terminates_and_kills_process_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess()
    _install_fake_subprocess(monkeypatch, process=process)
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")
    monkeypatch.setattr(adapter_module, "_PROCESS_POLL_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(adapter_module, "_PROCESS_TERMINATE_GRACE_SECONDS", 0.01)
    monkeypatch.setattr(adapter_module, "_PROCESS_KILL_GRACE_SECONDS", 0.01)

    with pytest.raises(TimeoutError):
        await adapter_module.create_adapter().ingest(
            _build_source(),
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.02)),
        )

    assert process.terminate_calls == 1
    assert process.kill_calls == 1


@pytest.mark.asyncio
async def test_libredwg_adapter_handles_task_cancellation_while_process_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess()
    seen = _install_fake_subprocess(monkeypatch, process=process)
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")
    monkeypatch.setattr(adapter_module, "_PROCESS_POLL_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(adapter_module, "_PROCESS_TERMINATE_GRACE_SECONDS", 0.01)
    monkeypatch.setattr(adapter_module, "_PROCESS_KILL_GRACE_SECONDS", 0.01)

    task = asyncio.create_task(
        adapter_module.create_adapter().ingest(
            _build_source(),
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
        )
    )
    while "command" not in seen:
        await asyncio.sleep(0)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert process.terminate_calls == 1
    assert process.kill_calls == 1


@pytest.mark.asyncio
async def test_libredwg_adapter_reports_sanitized_stdio_excerpts_within_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess(complete_with=0)
    stdout_template = "{source} {tempdir}\n" + ("A" * 1024)
    stderr_template = "{source} {tempdir}\n" + ("B" * 1024)
    seen = _install_fake_subprocess(
        monkeypatch,
        process=process,
        stdout_text=stdout_template,
        stderr_text=stderr_template,
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    source = _build_source()
    result = await adapter_module.create_adapter().ingest(
        source,
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    command = seen["command"]
    output_path = Path(command[4])
    source_path = str(source.file_path)
    tempdir = str(output_path.parent)
    expected_stdout_bytes = len(
        stdout_template.format(source=source_path, tempdir=tempdir, output=output_path).encode(
            "utf-8"
        )
    )
    expected_stderr_bytes = len(
        stderr_template.format(source=source_path, tempdir=tempdir, output=output_path).encode(
            "utf-8"
        )
    )

    diagnostic_details = cast(Mapping[str, object], result.diagnostics[0].details)
    stdout_excerpt = cast(str, diagnostic_details["stdout_excerpt"])
    stderr_excerpt = cast(str, diagnostic_details["stderr_excerpt"])

    assert diagnostic_details["stdout_bytes"] == expected_stdout_bytes
    assert diagnostic_details["stderr_bytes"] == expected_stderr_bytes
    assert diagnostic_details["stdout_truncated"] is False
    assert diagnostic_details["stderr_truncated"] is False
    assert "<source>" in stdout_excerpt
    assert "<tempdir>" in stdout_excerpt
    assert "<source>" in stderr_excerpt
    assert "<tempdir>" in stderr_excerpt
    assert source_path not in stdout_excerpt
    assert source_path not in stderr_excerpt
    assert tempdir not in stdout_excerpt
    assert tempdir not in stderr_excerpt


@pytest.mark.asyncio
async def test_libredwg_adapter_nonzero_exit_failure_is_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _build_source()
    process = _FakeProcess(complete_with=3)
    seen = _install_fake_subprocess(
        monkeypatch,
        process=process,
        stdout_text="failed for {source} in {tempdir}\n",
        stderr_text="error while writing {output}\n",
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    with pytest.raises(RuntimeError, match="execution failed") as exc_info:
        await adapter_module.create_adapter().ingest(
            source,
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
        )

    output_path = Path(seen["command"][4])
    tempdir = str(output_path.parent)
    message = str(exc_info.value)
    assert str(source.file_path) not in message
    assert tempdir not in message


@pytest.mark.asyncio
async def test_libredwg_adapter_missing_output_failure_is_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _build_source()
    process = _FakeProcess(complete_with=0)
    seen = _install_fake_subprocess(
        monkeypatch,
        process=process,
        write_output=False,
        stdout_text="source={source} tempdir={tempdir}\n",
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    with pytest.raises(RuntimeError, match="did not produce JSON output") as exc_info:
        await adapter_module.create_adapter().ingest(
            source,
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
        )

    tempdir = str(Path(seen["command"][4]).parent)
    message = str(exc_info.value)
    assert str(source.file_path) not in message
    assert tempdir not in message


@pytest.mark.asyncio
async def test_libredwg_adapter_invalid_json_failure_sanitizes_decode_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _build_source()
    process = _FakeProcess(complete_with=0)
    seen = _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text='{{"source":"{source}","tempdir":"{tempdir}"',
        format_output=True,
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    with pytest.raises(RuntimeError, match="output was invalid") as exc_info:
        await adapter_module.create_adapter().ingest(
            source,
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
        )

    tempdir = str(Path(seen["command"][4]).parent)
    message = str(exc_info.value)
    assert str(source.file_path) not in message
    assert tempdir not in message

    cause = exc_info.value.__cause__
    assert isinstance(cause, json.JSONDecodeError)
    assert str(source.file_path) not in cause.doc
    assert tempdir not in cause.doc
    assert "<source>" in cause.doc
    assert "<tempdir>" in cause.doc


@pytest.mark.asyncio
async def test_libredwg_adapter_terminates_and_kills_process_on_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess()
    cancellation = _DeferredCancellation(cancel_after_calls=4)
    _install_fake_subprocess(monkeypatch, process=process)
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")
    monkeypatch.setattr(adapter_module, "_PROCESS_POLL_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(adapter_module, "_PROCESS_TERMINATE_GRACE_SECONDS", 0.01)
    monkeypatch.setattr(adapter_module, "_PROCESS_KILL_GRACE_SECONDS", 0.01)

    with pytest.raises(asyncio.CancelledError):
        await adapter_module.create_adapter().ingest(
            _build_source(),
            AdapterExecutionOptions(
                timeout=AdapterTimeout(seconds=0.5),
                cancellation=cancellation,
            ),
        )

    assert process.terminate_calls == 1
    assert process.kill_calls == 1


def test_libredwg_hash_helpers_preserve_raw_and_prefixed_forms() -> None:
    value = {
        "record_type": "LINE",
        "handle": "1A",
        "layer_name": "Walls",
        "layout_name": "Model",
        "block_name": None,
        "geometry": {
            "start": {"x": 1.0, "y": 2.0, "z": 0.0},
            "end": {"x": 4.0, "y": 6.0, "z": 0.0},
        },
    }
    canonical_payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    expected_raw_hash = adapter_module._hash_text(canonical_payload)

    assert adapter_module._canonical_hash_json_value(value) == expected_raw_hash
    assert adapter_module._hash_json_value(value) == f"sha256:{expected_raw_hash}"
    assert adapter_module._hash_json_value(value) != expected_raw_hash


async def _ingest_output_payload(
    monkeypatch: pytest.MonkeyPatch,
    output_payload: dict[str, Any],
) -> Any:
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(monkeypatch, process=process, output_text=json.dumps(output_payload))
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")
    return await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )


def _units_check(result: Any) -> Mapping[str, Any]:
    validation = build_validation_outcome(
        input_family=InputFamily.DWG,
        canonical_json=result.canonical,
        canonical_entity_schema_version=cast(
            str,
            result.canonical["canonical_entity_schema_version"],
        ),
        result=result,
        generated_at=datetime.now(UTC),
    )
    checks = cast(list[Mapping[str, Any]], validation.report_json["checks"])
    return {check["check_key"]: check for check in checks}["units_presence_normalization"]


@pytest.mark.asyncio
async def test_libredwg_adapter_confirms_and_scales_explicit_supported_units(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = await _ingest_output_payload(
        monkeypatch,
        {
            "HEADER": {"INSUNITS": 4},
            "OBJECTS": [
                {
                    "type": "LINE",
                    "handle": "1A",
                    "layer": "Walls",
                    "start": {"x": 0, "y": 0, "z": 0},
                    "end": {"x": 5, "y": 0, "z": 0},
                }
            ],
        },
    )

    assert result.canonical["units"] == {
        "normalized": "meter",
        "source": "INSUNITS",
        "source_value": 4,
        "conversion_target": "meter",
        "conversion_factor": 0.001,
    }
    assert "libredwg.units_unconfirmed" not in [warning.code for warning in result.warnings]

    metadata = cast(dict[str, Any], result.canonical["metadata"])
    assert metadata["units"] == {
        "normalized": "meter",
        "source": "INSUNITS",
        "source_value": 4,
        "confirmed": True,
    }
    diagnostic_details = cast(Mapping[str, Any], result.diagnostics[0].details)
    assert diagnostic_details["units"] == metadata["units"]

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    entity = entities[0]
    assert entity["start"] == {"x": 0.0, "y": 0.0, "z": 0.0}
    assert entity["end"] == {"x": pytest.approx(0.005), "y": 0.0, "z": 0.0}
    assert entity["length"] == pytest.approx(0.005)
    assert entity["geometry"]["units"] == {"normalized": "meter"}
    assert entity["geometry"]["geometry_summary"]["length"] == pytest.approx(0.005)
    assert "units_unconfirmed" not in entity["provenance"]["notes"]
    assert entity["confidence"]["basis"] == "libredwg_line_mapping_units_meter"

    units_check = _units_check(result)
    assert units_check["status"] == "pass"


@pytest.mark.asyncio
async def test_libredwg_adapter_keeps_units_unconfirmed_when_header_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = await _ingest_output_payload(
        monkeypatch,
        {
            "OBJECTS": [
                {
                    "type": "LINE",
                    "handle": "1A",
                    "start": {"x": 0, "y": 0, "z": 0},
                    "end": {"x": 5, "y": 0, "z": 0},
                }
            ],
        },
    )

    assert result.canonical["units"] == {"normalized": "unknown"}
    assert "libredwg.units_unconfirmed" in [warning.code for warning in result.warnings]

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    entity = entities[0]
    assert entity["length"] == 5.0
    assert entity["end"] == {"x": 5.0, "y": 0.0, "z": 0.0}
    assert entity["geometry"]["units"] == {"normalized": "unknown"}
    assert "units_unconfirmed" in entity["provenance"]["notes"]
    assert entity["confidence"]["basis"] == "libredwg_line_mapping_units_unconfirmed"

    units_check = _units_check(result)
    assert units_check["status"] == "review_required"


@pytest.mark.asyncio
@pytest.mark.parametrize("insunits_value", [0, "abc"])
async def test_libredwg_adapter_degrades_ambiguous_units_to_unconfirmed(
    monkeypatch: pytest.MonkeyPatch,
    insunits_value: Any,
) -> None:
    result = await _ingest_output_payload(
        monkeypatch,
        {
            "HEADER": {"INSUNITS": insunits_value},
            "OBJECTS": [
                {
                    "type": "LINE",
                    "handle": "1A",
                    "start": {"x": 0, "y": 0, "z": 0},
                    "end": {"x": 5, "y": 0, "z": 0},
                }
            ],
        },
    )

    assert result.canonical["units"] == {"normalized": "unknown"}
    assert "libredwg.units_unconfirmed" in [warning.code for warning in result.warnings]

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    entity = entities[0]
    assert entity["length"] == 5.0
    assert entity["geometry"]["units"] == {"normalized": "unknown"}

    units_check = _units_check(result)
    assert units_check["status"] == "review_required"


def _validation_checks(result: Any) -> Mapping[str, Mapping[str, Any]]:
    validation = build_validation_outcome(
        input_family=InputFamily.DWG,
        canonical_json=result.canonical,
        canonical_entity_schema_version=cast(
            str,
            result.canonical["canonical_entity_schema_version"],
        ),
        result=result,
        generated_at=datetime.now(UTC),
    )
    checks = cast(list[Mapping[str, Any]], validation.report_json["checks"])
    return {check["check_key"]: check for check in checks}


def _single_entity(result: Any) -> Mapping[str, Any]:
    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    assert len(entities) == 1
    return entities[0]


@pytest.mark.asyncio
async def test_libredwg_adapter_captures_supported_insert_as_block_reference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = await _ingest_output_payload(
        monkeypatch,
        {
            "HEADER": {"INSUNITS": 4},
            "OBJECTS": [
                {"type": "BLOCK_HEADER", "handle": "B1", "name": "Door-Block"},
                {
                    "type": "INSERT",
                    "handle": "1A",
                    "layer": "A-DOOR",
                    "block_header": {"handle": "B1"},
                    "ins_pt": {"x": 1000, "y": 2000, "z": 0},
                    "scale": {"x": 2, "y": 2, "z": 2},
                    "rotation": 0.0,
                },
            ],
        },
    )

    entity = _single_entity(result)
    assert entity["entity_type"] == "insert"
    assert entity["kind"] == "insert"
    assert entity["entity_id"] == "libredwg-insert-1a"
    assert entity["block_name"] == "Door-Block"
    assert entity["block_ref"] == "Door-Block"
    # Insertion point scaled from millimetres to metres; scale factors stay unitless.
    assert entity["insert"] == {"x": 1.0, "y": 2.0, "z": 0.0}
    assert entity["transform"]["insertion_point"] == {"x": 1.0, "y": 2.0, "z": 0.0}
    assert entity["transform"]["scale"] == {"x": 2.0, "y": 2.0, "z": 2.0}
    assert entity["transform"]["rotation_degrees"] == 0.0
    assert entity["transform"]["supported"] is True
    assert entity["transform"]["unsupported_reasons"] == ()
    assert entity["transform_supported"] is True
    assert entity["geometry"]["status"] == "reference"
    assert entity["confidence"]["basis"] == "libredwg_dwgread_json_block_reference_units_meter"
    assert "block_reference_unmaterialized" in entity["provenance"]["notes"]

    assert result.canonical["metadata"]["block_transform_validity"] is True
    # The block is defined (BLOCK_HEADER present) but carries no child geometry in this fixture.
    assert result.canonical["blocks"] == (
        {
            "name": "Door-Block",
            "block_ref": "Door-Block",
            "block_handle": "b1",
            "base_point": {"x": 0.0, "y": 0.0, "z": 0.0},
            "entities": (),
        },
    )
    assert result.confidence is not None
    assert result.confidence.score == adapter_module._BLOCK_REFERENCE_CONFIDENCE_SCORE
    assert result.confidence.basis == "libredwg_dwgread_json_block_reference_mapping"
    _assert_score_within_libredwg_descriptor_range(result.confidence.score)
    assert [warning.code for warning in result.warnings] == ["libredwg.block_reference_captured"]

    diagnostic_details = cast(Mapping[str, object], result.diagnostics[0].details)
    assert diagnostic_details["entity_counts"] == _with_hatch_counts(
        {
            "drawable_candidates": 1,
            "supported_geometry": 0,
            "supported_lines": 0,
            "supported_text": 0,
            "unsupported_drawables": 0,
            "malformed_drawables": 0,
            "block_references": 1,
            # The BLOCK_HEADER table record is non-drawable and skipped.
            "skipped_non_drawable": 1,
        }
    )

    checks = _validation_checks(result)
    assert checks["block_transform_validity"]["status"] == "pass"
    assert checks["geometry_validity"]["status"] == "pass"


@pytest.mark.asyncio
async def test_libredwg_adapter_resolves_insert_block_name_by_direct_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = await _ingest_output_payload(
        monkeypatch,
        {
            "HEADER": {"INSUNITS": 6},
            "OBJECTS": [
                {
                    "type": "INSERT",
                    "handle": "2B",
                    "block_header_name": "Window-Block",
                    "ins_pt": {"x": 3.0, "y": 4.0, "z": 0.0},
                }
            ],
        },
    )

    entity = _single_entity(result)
    assert entity["entity_type"] == "insert"
    assert entity["block_name"] == "Window-Block"
    # Absent scale/rotation default to identity and remain within the supported subset.
    assert entity["transform"]["scale"] == {"x": 1.0, "y": 1.0, "z": 1.0}
    assert entity["transform"]["supported"] is True
    assert result.canonical["metadata"]["block_transform_validity"] is True


@pytest.mark.asyncio
async def test_libredwg_adapter_review_gates_non_uniform_insert_scale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = await _ingest_output_payload(
        monkeypatch,
        {
            "HEADER": {"INSUNITS": 6},
            "OBJECTS": [
                {"type": "BLOCK_HEADER", "handle": "B1", "name": "Skewed-Block"},
                {
                    "type": "INSERT",
                    "handle": "3C",
                    "block_header": {"handle": "B1"},
                    "ins_pt": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "scale": {"x": 2.0, "y": 3.0, "z": 1.0},
                },
            ],
        },
    )

    entity = _single_entity(result)
    assert entity["entity_type"] == "insert"
    assert entity["transform_supported"] is False
    assert entity["transform"]["unsupported_reasons"] == ("unsupported_nonuniform_scale",)
    # No confirmation hint is emitted, so the downstream check stays review-gated.
    assert "block_transform_validity" not in result.canonical["metadata"]

    warning_codes = sorted(warning.code for warning in result.warnings)
    assert warning_codes == [
        "libredwg.block_reference_captured",
        "libredwg.unsupported_block_transform",
    ]
    transform_warning = next(
        warning
        for warning in result.warnings
        if warning.code == "libredwg.unsupported_block_transform"
    )
    assert transform_warning.details["reasons"] == ("unsupported_nonuniform_scale",)

    checks = _validation_checks(result)
    assert checks["block_transform_validity"]["status"] == "review_required"


@pytest.mark.asyncio
async def test_libredwg_adapter_review_gates_insert_block_array(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = await _ingest_output_payload(
        monkeypatch,
        {
            "HEADER": {"INSUNITS": 4},
            "OBJECTS": [
                {
                    "type": "INSERT",
                    "handle": "4D",
                    "block_header_name": "Grid-Block",
                    "ins_pt": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "num_cols": 3,
                    "num_rows": 2,
                    "col_spacing": 1000,
                    "row_spacing": 500,
                }
            ],
        },
    )

    entity = _single_entity(result)
    assert entity["transform_supported"] is False
    assert entity["transform"]["unsupported_reasons"] == ("unsupported_block_array",)
    # Array spacing is scaled to metres alongside the insertion point.
    assert entity["transform"]["array"] == {
        "columns": 3,
        "rows": 2,
        "column_spacing": 1.0,
        "row_spacing": 0.5,
    }
    assert "block_transform_validity" not in result.canonical["metadata"]


@pytest.mark.asyncio
async def test_libredwg_adapter_review_gates_unresolved_insert_block_reference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = await _ingest_output_payload(
        monkeypatch,
        {
            "HEADER": {"INSUNITS": 6},
            "OBJECTS": [
                {
                    "type": "INSERT",
                    "handle": "5E",
                    "block_header": {"handle": "missing"},
                    "ins_pt": {"x": 1.0, "y": 1.0, "z": 0.0},
                }
            ],
        },
    )

    entity = _single_entity(result)
    assert entity["entity_type"] == "insert"
    assert entity["block_name"] is None
    assert entity["transform_supported"] is False
    assert entity["transform"]["unsupported_reasons"] == ("unresolved_block_reference",)


@pytest.mark.asyncio
async def test_libredwg_adapter_falls_back_to_unknown_for_malformed_insert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = await _ingest_output_payload(
        monkeypatch,
        {
            "HEADER": {"INSUNITS": 6},
            "OBJECTS": [
                {
                    "type": "INSERT",
                    "handle": "6F",
                    "block_header_name": "No-Point-Block",
                }
            ],
        },
    )

    entity = _single_entity(result)
    assert entity["entity_type"] == "unknown"
    assert entity["geometry"]["reason"] == "malformed_insert_record"
    assert [warning.code for warning in result.warnings] == ["libredwg.malformed_insert_record"]

    diagnostic_details = cast(Mapping[str, object], result.diagnostics[0].details)
    counts = cast(Mapping[str, int], diagnostic_details["entity_counts"])
    assert counts["malformed_inserts"] == 1
    assert counts["block_references"] == 0


@pytest.mark.asyncio
async def test_libredwg_adapter_skips_block_definition_delimiters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = await _ingest_output_payload(
        monkeypatch,
        {
            "HEADER": {"INSUNITS": 6},
            "OBJECTS": [
                {"type": "BLOCK", "handle": "X1", "name": "Door-Block"},
                {"type": "ENDBLK", "handle": "X2"},
                {
                    "type": "LINE",
                    "handle": "7G",
                    "layer": "Walls",
                    "start": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "end": {"x": 5.0, "y": 0.0, "z": 0.0},
                },
            ],
        },
    )

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    assert [entity["entity_type"] for entity in entities] == ["line"]

    diagnostic_details = cast(Mapping[str, object], result.diagnostics[0].details)
    counts = cast(Mapping[str, int], diagnostic_details["entity_counts"])
    assert counts["drawable_candidates"] == 1
    assert counts["unsupported_drawables"] == 0


@pytest.mark.asyncio
async def test_libredwg_adapter_block_reference_passes_shared_contract_harness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _build_source()
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "HEADER": {"INSUNITS": 6},
                "OBJECTS": [
                    {"type": "BLOCK_HEADER", "handle": "B1", "name": "Door-Block"},
                    {
                        "type": "INSERT",
                        "handle": "1A",
                        "layer": "A-DOOR",
                        "block_header": {"handle": "B1"},
                        "ins_pt": {"x": 1.0, "y": 2.0, "z": 0.0},
                        "scale": {"x": 1.0, "y": 1.0, "z": 1.0},
                    },
                ],
            }
        ),
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    payload = await exercise_adapter_contract(
        adapter_module.create_adapter(),
        source=source,
        input_family=InputFamily.DWG,
        adapter_key="libredwg",
        expectation=ContractFinalizationExpectation(
            validation_status="needs_review",
            warning_codes=("libredwg.block_reference_captured",),
            diagnostic_codes=("libredwg.extract",),
        ),
    )

    entities = cast(list[dict[str, Any]], payload.canonical_json["entities"])
    assert [entity["entity_type"] for entity in entities] == ["insert"]
    assert payload.canonical_json["metadata"]["block_transform_validity"] is True


@pytest.mark.asyncio
async def test_libredwg_adapter_unsupported_transform_insert_stays_reference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Regression of #361: a non-uniform scale INSERT stays a review-gated reference, and the
    # block-definition child lives in the block payload — never leaked as a top-level entity.
    result = await _ingest_output_payload(
        monkeypatch,
        {
            "HEADER": {"INSUNITS": 6},
            "OBJECTS": [
                {"type": "BLOCK_HEADER", "handle": "B1", "name": "Skewed-Block"},
                {
                    "type": "LINE",
                    "handle": "L1",
                    "owner": "B1",
                    "start": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "end": {"x": 1.0, "y": 0.0, "z": 0.0},
                },
                {
                    "type": "INSERT",
                    "handle": "3C",
                    "block_header": {"handle": "B1"},
                    "ins_pt": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "scale": {"x": 2.0, "y": 3.0, "z": 1.0},
                },
            ],
        },
    )

    entity = _single_entity(result)
    assert entity["entity_type"] == "insert"
    assert entity["transform_supported"] is False
    # The block-definition child stays inside the block payload, not at the top level.
    blocks = cast(list[dict[str, Any]], result.canonical["blocks"])
    block = next(b for b in blocks if b["name"] == "Skewed-Block")
    assert [child["entity_id"] for child in block["entities"]] == ["libredwg-line-l1"]


@pytest.mark.asyncio
async def test_libredwg_adapter_does_not_double_emit_block_definition_geometry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = await _ingest_output_payload(
        monkeypatch,
        {
            "HEADER": {"INSUNITS": 6},
            "OBJECTS": [
                {"type": "BLOCK_HEADER", "handle": "B1", "name": "Blk"},
                {
                    "type": "LINE",
                    "handle": "BL",
                    "owner": "B1",
                    "start": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "end": {"x": 1.0, "y": 0.0, "z": 0.0},
                },
                {
                    "type": "LINE",
                    "handle": "ML",
                    "start": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "end": {"x": 2.0, "y": 0.0, "z": 0.0},
                },
                {
                    "type": "INSERT",
                    "handle": "IN",
                    "block_header": {"handle": "B1"},
                    "ins_pt": {"x": 0.0, "y": 0.0, "z": 0.0},
                },
            ],
        },
    )

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    entity_ids = [entity["entity_id"] for entity in entities]
    # Structured model: top level is the model line plus the INSERT reference. The block's own
    # line is emitted once inside the block definition's payload — never as a standalone top-level
    # entity and never flattened per placement.
    assert entity_ids == ["libredwg-line-ml", "libredwg-insert-in"]
    assert "libredwg-line-bl" not in entity_ids

    blocks = cast(list[dict[str, Any]], result.canonical["blocks"])
    blk = next(block for block in blocks if block["name"] == "Blk")
    assert [child["entity_id"] for child in blk["entities"]] == ["libredwg-line-bl"]


@pytest.mark.asyncio
async def test_libredwg_adapter_skips_non_drawable_objects_section_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-drawable OBJECTS records must not leak in as unknown entities (#386)."""
    result = await _ingest_output_payload(
        monkeypatch,
        {
            "HEADER": {"INSUNITS": 4},
            "OBJECTS": [
                {"type": "VIEWPORT", "handle": "V1"},
                {"type": "SCALE", "handle": "S1"},
                {"type": "SUN", "handle": "SU"},
                {"type": "RASTERVARIABLES", "handle": "R1"},
                {"type": "LAYER_CONTROL", "handle": "LC"},
                {"type": "STYLE_CONTROL", "handle": "SC"},
                {
                    "type": "LINE",
                    "handle": "1A",
                    "layer": "Walls",
                    "start": {"x": 0, "y": 0, "z": 0},
                    "end": {"x": 5, "y": 0, "z": 0},
                },
            ],
        },
    )

    entities = cast(list[dict[str, Any]], result.canonical["entities"])
    # Only the LINE survives; none of the non-drawable records become unknown entities.
    assert [entity["entity_type"] for entity in entities] == ["line"]
    counts = cast(Mapping[str, int], result.canonical["metadata"]["entity_counts"])
    assert counts["skipped_non_drawable"] == 6
    assert counts["drawable_candidates"] == 1
    assert counts["unsupported_drawables"] == 0
    # No unsupported-drawable penalty/warning is raised for the skipped records.
    assert "libredwg.unsupported_drawable_record" not in [w.code for w in result.warnings]


@pytest.mark.asyncio
async def test_libredwg_adapter_surfaces_dwgread_parse_errors_as_counted_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """dwgread stderr parse errors should surface as a counted warning (#386)."""
    stderr_text = (
        "ERROR: Invalid MTEXT.class_version 256\n"
        "ERROR: Invalid MTEXT.class_version 256\n"
        "ERROR: Invalid MTEXT.class_version 257\n"
        "ERROR: Unknown class FOO\n"
        "Some unrelated informational line\n"
    )
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(
        monkeypatch,
        process=process,
        output_text=json.dumps(
            {
                "HEADER": {"INSUNITS": 4},
                "OBJECTS": [
                    {
                        "type": "LINE",
                        "handle": "1A",
                        "layer": "Walls",
                        "start": {"x": 0, "y": 0, "z": 0},
                        "end": {"x": 5, "y": 0, "z": 0},
                    }
                ],
            }
        ),
        stderr_text=stderr_text,
    )
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    result = await adapter_module.create_adapter().ingest(
        _build_source(),
        AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
    )

    parse_warnings = [w for w in result.warnings if w.code == "libredwg.source_parse_errors"]
    assert len(parse_warnings) == 1
    details = cast(Mapping[str, Any], parse_warnings[0].details)
    # Digit runs collapse so 256/257 fold into one signature; the info line is ignored.
    assert details["count"] == 4
    assert details["types"] == {
        "Invalid MTEXT.class_version #": 3,
        "Unknown class FOO": 1,
    }
    assert details["stderr_truncated"] is False
