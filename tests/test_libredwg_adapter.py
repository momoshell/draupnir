"""Contract tests for the thin LibreDWG DWG adapter."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

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
from app.ingestion.validation import build_validation_outcome
from tests.ingestion_contract_harness import (
    ContractFinalizationExpectation,
    build_contract_source,
    exercise_adapter_contract,
)

_FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "dwg" / "libredwg-wrapper-smoke.txt"
)


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
        stdout_handle.write(
            stdout_payload
        )
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
                        "layer": "Walls",
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
            review_state="review_required",
            quantity_gate="review_gated",
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
    assert payload.canonical_json["layers"] == [{"name": "Walls"}]

    entities = cast(list[dict[str, Any]], payload.canonical_json["entities"])
    assert len(entities) == 1
    entity = entities[0]
    assert entity["entity_id"] == "libredwg-line-1a"
    assert entity["entity_type"] == "line"
    assert entity["entity_schema_version"] == "0.1"
    assert entity["source_entity_handle"] == "1A"
    assert entity["layout_name"] == "Model"
    assert entity["layer_name"] == "Walls"
    assert entity["block_name"] is None
    assert entity["parent_entity_id"] is None
    assert entity["drawing_revision_id"] is None
    assert entity["source_file_id"] is None
    assert entity["layout_ref"] is None
    assert entity["layer_ref"] is None
    assert entity["block_ref"] is None
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
    assert entity["provenance"]["source_locator"] == "OBJECTS/LINE/1A"
    assert entity["provenance"]["record_hash"].startswith("sha256:")
    assert entity["confidence"] == {
        "score": adapter_module._LINE_ENTITY_CONFIDENCE_SCORE,
        "review_required": True,
        "basis": "libredwg_line_mapping_units_unconfirmed",
    }

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
    assert validation.review_state == "review_required"
    assert validation.quantity_gate == "review_gated"
    assert not any(finding.get("check_key") == "placeholder_semantics" for finding in findings)


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
async def test_libredwg_adapter_ignores_non_entities_and_degrades_unsupported_drawables(
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
                    {"type": "CIRCLE", "handle": "20", "layer": "Unsupported"},
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
    assert entities[0]["entity_id"] == "libredwg-unknown-20"
    assert entities[0]["entity_type"] == "unknown"
    assert entities[0]["entity_schema_version"] == "0.1"
    assert entities[0]["source_entity_handle"] == "20"
    assert entities[0]["layout_name"] == "Model"
    assert entities[0]["layer_name"] == "Unsupported"
    assert entities[0]["block_name"] is None
    assert entities[0]["parent_entity_id"] is None
    assert entities[0]["drawing_revision_id"] is None
    assert entities[0]["source_file_id"] is None
    assert entities[0]["layout_ref"] is None
    assert entities[0]["layer_ref"] is None
    assert entities[0]["block_ref"] is None
    assert entities[0]["parent_entity_ref"] is None
    assert entities[0]["bbox"] is None
    assert entities[0]["geometry"] == {
        "bbox": None,
        "units": {"normalized": "unknown"},
        "status": "absent",
        "reason": "unsupported_drawable_record",
        "geometry_summary": {
            "kind": "unknown",
            "source_type": "CIRCLE",
            "reason": "unsupported_drawable_record",
        },
    }
    assert entities[0]["geometry_reason"] == "unsupported_drawable_record"
    assert entities[0]["quantity_hints"] == {}
    assert entities[0]["adapter_native"] == {
        "section": "OBJECTS",
        "record_type": "CIRCLE",
        "handle": "20",
    }
    assert entities[0]["provenance"]["record_hash"].startswith("sha256:")
    assert entities[0]["confidence"] == {
        "score": adapter_module._UNKNOWN_ENTITY_CONFIDENCE_SCORE,
        "review_required": True,
        "basis": "unsupported_drawable_record",
    }
    assert entities[0]["unknown_reason"] == "unsupported_drawable_record"
    assert entities[0]["provenance"]["record_hash"].startswith("sha256:")
    assert [warning.code for warning in result.warnings] == [
        "libredwg.units_unconfirmed",
        "libredwg.unsupported_drawable_record",
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
            {
                "OBJECTS": [
                    {"type": "CIRCLE", "handle": "20", "layer": "Unsupported"},
                ]
            },
            "unsupported_drawable_record",
            "CIRCLE",
            ("libredwg.units_unconfirmed", "libredwg.unsupported_drawable_record"),
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
            review_state="review_required",
            quantity_gate="review_gated",
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
                    {"type": "CIRCLE", "handle": "20", "layer": "Unsupported"},
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
                    "metadata": {"type": "DICTIONARY", "handle": "meta"},
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
    assert metadata["entity_counts"] == {
        "drawable_candidates": 0,
        "supported_lines": 0,
        "unsupported_drawables": 0,
        "malformed_drawables": 0,
    }
    assert [warning.code for warning in result.warnings] == ["libredwg.units_unconfirmed"]


@pytest.mark.asyncio
async def test_libredwg_adapter_rejects_oversized_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oversized_output = json.dumps({"x": "y" * adapter_module._MAX_OUTPUT_BYTES})
    process = _FakeProcess(complete_with=0)
    _install_fake_subprocess(monkeypatch, process=process, output_text=oversized_output)
    monkeypatch.setattr(adapter_module, "_binary_path", lambda: "/opt/homebrew/bin/dwgread")

    with pytest.raises(RuntimeError, match="output limit"):
        await adapter_module.create_adapter().ingest(
            _build_source(),
            AdapterExecutionOptions(timeout=AdapterTimeout(seconds=0.5)),
        )


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
