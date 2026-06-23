"""Spatial-completeness validation checks (#542)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .._constants import _SOURCE_DOCUMENT_REF  # noqa: TID252
from .._utils import geometry_placement  # noqa: TID252
from ._common import _check, _not_applicable_check, _pass_check


def _build_block_expansion_check(
    *,
    canonical_json: Mapping[str, Any],
    add_finding: Any,
) -> tuple[dict[str, Any], bool]:
    """Flag block-definition geometry that never reached the spatial model (#542).

    Entity-mapping ``coverage.mapped_ratio`` measures *placed* entities and can read 100% while a
    large fraction of the drawing's geometry sits unplaced inside block definitions (the 670003
    blind spot: 38% of geometry locked in blocks, yet reported clean). This check reads the
    geometry-placement signal and degrades to ``review_required`` when any block carrying leaf
    geometry never produced a world placement — so ``validation_status`` reflects the gap instead
    of silently reading ``valid``. Honest + non-gating (Path B #485): a status signal, never a
    hard quantity block.
    """

    check_key = "block_expansion_completeness"
    placement = geometry_placement(canonical_json)

    if placement["blocks_with_geometry"] == 0:
        return (
            _not_applicable_check(
                check_key,
                "No block definitions carry geometry; spatial completeness is not applicable.",
            ),
            False,
        )

    if placement["in_block_unexpanded"] == 0:
        return (
            _pass_check(
                check_key,
                "All block-definition geometry was placed into the spatial model.",
            ),
            False,
        )

    finding_ref = add_finding(
        check_key=check_key,
        severity="warning",
        message=(
            f"{placement['in_block_unexpanded']} entities across "
            f"{placement['unexpanded_blocks']} block definition(s) remain unexpanded and are "
            "absent from the spatial model; geometric completeness is below 100%."
        ),
        target_type="revision",
        target_ref=_SOURCE_DOCUMENT_REF,
        quantity_effect="warning_only",
        source="validator",
        details={"geometry_placement": placement},
    )
    return (
        _check(
            check_key=check_key,
            status="review_required",
            summary_message="Some block-definition geometry was not placed into the spatial model.",
            finding_refs=[finding_ref],
            details={"applicable": True, "geometry_placement": placement},
        ),
        True,
    )
