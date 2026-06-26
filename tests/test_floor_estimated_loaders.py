"""Mocked (non-DB) tests for floor_estimated_loaders (issue #720, Phase R-F).

All tests mock cable seam loaders so no real database is needed.  The test DB
isolation lane (conftest) is NOT used here — these run in the standard pytest process.

Coverage targets:
- Happy-path mocked run: load_estimated_fusion returns FusedEstimatedResult.
- containment_revision_id=None path → no_containment_home_run_result used.
- Isolation-import test: importing floor_estimated_loaders must not pull any app.api module
  at module level.
"""

from __future__ import annotations

import subprocess
import sys
from typing import Any
from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest

from app.interpretation.cable_circuits import CableCircuitSet
from app.interpretation.cable_estimate import CableEstimateResult
from app.interpretation.cable_home_run import HomeRunResult, no_containment_home_run_result
from app.interpretation.cable_topology import CableGraph, DeviceFootprint, SplineInput
from app.interpretation.floor_counted import CountedFusionResult
from app.interpretation.floor_estimated import FusedEstimatedResult
from app.interpretation.floor_registration import FloorRegistration
from app.interpretation.grid_registration import GridTransform
from app.interpretation.room_fusion import RoomRegistry, build_room_registry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LIT_REV = UUID("aaaa0000-0000-0000-0000-000000000001")
_CONT_REV = UUID("bbbb0000-0000-0000-0000-000000000001")

_IDENTITY = GridTransform(
    dx=0.0,
    dy=0.0,
    rotation_rad=0.0,
    scale=1.0,
    matched_labels=(),
    matched_count=0,
    max_residual_m=0.0,
    median_residual_m=0.0,
    quality="good",
)


def _empty_registration() -> FloorRegistration:
    return FloorRegistration(
        reference_revision_id=_LIT_REV,
        reference_role="lighting",
        members_ok=(),
        members_failed=(),
    )


def _empty_registry() -> RoomRegistry:
    return build_room_registry([])


def _empty_splines() -> list[SplineInput]:
    return []


def _empty_footprints() -> list[DeviceFootprint]:
    return []


def _empty_graph() -> CableGraph:
    return CableGraph(
        nodes=(),
        edges=(),
        terminals=(),
        spline_count=0,
        node_count=0,
        edge_count=0,
        terminal_count=0,
        interior_junction_count=0,
        associated_terminal_count=0,
        dropped=0,
    )


def _empty_circuit_set() -> CableCircuitSet:
    return CableCircuitSet(
        circuits=(), circuit_count=0, edge_count=0, assigned_edge_count=0, device_count=0
    )


def _empty_in_plan() -> CableEstimateResult:
    from app.interpretation.cable_estimate_params import default_estimate_params

    params = default_estimate_params()
    return CableEstimateResult(
        circuits=(),
        total_device_drop_m=0.0,
        total_in_plan_length_m=0.0,
        total_uncategorized_devices=0,
        unattributed_device_count=0,
        unattributed_device_drop_m=0.0,
        spare_fraction=params.spare_fraction,
        quantity_kind="estimated",
        params_stamp=params.as_stamp(),
        reliability={
            "device_drop_m": "reliable",
            "in_plan_length_m": "schematic_provisional",
            "note": "",
        },
    )


def _empty_home_run() -> HomeRunResult:
    return no_containment_home_run_result([])


def _empty_counted() -> CountedFusionResult:
    return CountedFusionResult(assignments=(), counts_by_room={}, excluded_kinds={})


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_happy_path_no_containment() -> None:
    """load_estimated_fusion returns FusedEstimatedResult (no containment)."""
    with (
        patch(
            "app.interpretation.floor_estimated_loaders.load_spline_inputs",
            new=AsyncMock(return_value=_empty_splines()),
        ),
        patch(
            "app.interpretation.floor_estimated_loaders.load_device_footprints",
            new=AsyncMock(return_value=_empty_footprints()),
        ),
        patch(
            "app.interpretation.floor_estimated_loaders.build_cable_graph",
            return_value=_empty_graph(),
        ),
        patch(
            "app.interpretation.floor_estimated_loaders.partition_circuits",
            return_value=_empty_circuit_set(),
        ),
        patch(
            "app.interpretation.floor_estimated_loaders.estimate_cable_in_plan",
            return_value=_empty_in_plan(),
        ),
        patch(
            "app.interpretation.floor_estimated_loaders.assemble_cable_estimate",
        ) as mock_assemble,
        patch(
            "app.interpretation.floor_estimated_loaders.load_counted_fusion",
            new=AsyncMock(return_value=_empty_counted()),
        ),
    ):
        from app.interpretation.cable_estimate_assembly import CableAssembly
        from app.interpretation.cable_estimate_params import default_estimate_params

        params = default_estimate_params()
        mock_assembly = CableAssembly(
            per_circuit=(),
            total_device_drop_m=0.0,
            total_in_plan_length_m=0.0,
            total_home_run_m=0.0,
            unattributed_device_drop_m=0.0,
            grand_base_m=0.0,
            grand_with_spare_m=0.0,
            spare_fraction=params.spare_fraction,
            quantity_kind="estimated",
            reliability={
                "device_drop_m": "reliable",
                "in_plan_length_m": "schematic_provisional",
                "home_run_m": "estimated_routed",
                "combined": "mixed_reliability",
            },
            params_stamp=params.as_stamp(),
            home_run_suppressed_counts={
                "no_anchor": 0,
                "unreachable_tray": 0,
                "disconnected_tray": 0,
                "bad_registration": 0,
                "no_containment_sheet": 0,
            },
        )
        mock_assemble.return_value = mock_assembly

        from app.interpretation.floor_estimated_loaders import load_estimated_fusion

        result = await load_estimated_fusion(
            None,  # type: ignore[arg-type]
            _empty_registration(),
            _empty_registry(),
            lighting_revision_id=_LIT_REV,
            containment_revision_id=None,
        )

    assert isinstance(result, FusedEstimatedResult)
    assert result.quantity_kind == "estimated"
    assert result.source["lighting_revision_id"] == str(_LIT_REV)
    assert result.source["containment_revision_id"] is None


@pytest.mark.asyncio
async def test_no_containment_uses_no_containment_home_run_result() -> None:
    """containment_revision_id=None → no_containment_home_run_result used (not the loader)."""
    calls: list[str] = []

    async def _fake_load_hr(*args: Any, **kwargs: Any) -> tuple[HomeRunResult, dict[str, object]]:
        calls.append("load_and_compute_home_runs")
        return _empty_home_run(), {}

    with (
        patch(
            "app.interpretation.floor_estimated_loaders.load_spline_inputs",
            new=AsyncMock(return_value=_empty_splines()),
        ),
        patch(
            "app.interpretation.floor_estimated_loaders.load_device_footprints",
            new=AsyncMock(return_value=_empty_footprints()),
        ),
        patch(
            "app.interpretation.floor_estimated_loaders.build_cable_graph",
            return_value=_empty_graph(),
        ),
        patch(
            "app.interpretation.floor_estimated_loaders.partition_circuits",
            return_value=_empty_circuit_set(),
        ),
        patch(
            "app.interpretation.floor_estimated_loaders.estimate_cable_in_plan",
            return_value=_empty_in_plan(),
        ),
        patch(
            "app.interpretation.floor_estimated_loaders.assemble_cable_estimate",
        ) as mock_assemble,
        patch(
            "app.interpretation.floor_estimated_loaders.load_counted_fusion",
            new=AsyncMock(return_value=_empty_counted()),
        ),
    ):
        from app.interpretation.cable_estimate_params import default_estimate_params

        params = default_estimate_params()
        from app.interpretation.cable_estimate_assembly import CableAssembly

        mock_assembly = CableAssembly(
            per_circuit=(),
            total_device_drop_m=0.0,
            total_in_plan_length_m=0.0,
            total_home_run_m=0.0,
            unattributed_device_drop_m=0.0,
            grand_base_m=0.0,
            grand_with_spare_m=0.0,
            spare_fraction=params.spare_fraction,
            quantity_kind="estimated",
            reliability={
                "device_drop_m": "reliable",
                "in_plan_length_m": "schematic_provisional",
                "home_run_m": "estimated_routed",
                "combined": "mixed_reliability",
            },
            params_stamp=params.as_stamp(),
            home_run_suppressed_counts={
                "no_anchor": 0,
                "unreachable_tray": 0,
                "disconnected_tray": 0,
                "bad_registration": 0,
                "no_containment_sheet": 0,
            },
        )
        mock_assemble.return_value = mock_assembly

        # Patch load_and_compute_home_runs inside the module's lazy import.
        with patch(
            "app.interpretation.cable_home_run_loaders.load_and_compute_home_runs",
            side_effect=_fake_load_hr,
        ):
            from app.interpretation.floor_estimated_loaders import load_estimated_fusion

            result = await load_estimated_fusion(
                None,  # type: ignore[arg-type]
                _empty_registration(),
                _empty_registry(),
                lighting_revision_id=_LIT_REV,
                containment_revision_id=None,
            )

    # load_and_compute_home_runs must NOT have been called.
    assert "load_and_compute_home_runs" not in calls
    assert isinstance(result, FusedEstimatedResult)


# ---------------------------------------------------------------------------
# Isolation-import test: floor_estimated_loaders must not import app.api at module level
# ---------------------------------------------------------------------------


def test_floor_estimated_loaders_no_module_level_app_api_import() -> None:
    """Importing floor_estimated_loaders in isolation must not pull any app.api module.

    The lazy-import pattern (#705) must be honoured: any app.api use must be inside
    function bodies, not at module scope.

    Arrange: fresh subprocess imports only floor_estimated_loaders.
    Act:     run in fresh subprocess.
    Assert:  exit 0 and 'app.api' not in sys.modules output.
    """
    script = (
        "import sys\n"
        "import app.interpretation.floor_estimated_loaders\n"
        "api_mods = [k for k in sys.modules if k.startswith('app.api')]\n"
        "if api_mods:\n"
        "    print('FORBIDDEN:', api_mods, file=sys.stderr)\n"
        "    sys.exit(1)\n"
        "print('ok')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (
        "floor_estimated_loaders imported app.api at module level.\n"
        f"stderr: {result.stderr}\nstdout: {result.stdout}"
    )
