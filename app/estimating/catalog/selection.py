from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from typing import Any
from uuid import UUID

from .contracts import (
    CatalogFormulaAutoSelectRequest,
    CatalogFormulaMatch,
    CatalogFormulaRef,
    CatalogMaterialAutoSelectRequest,
    CatalogMaterialMatch,
    CatalogMaterialRef,
    CatalogRateAutoSelectRequest,
    CatalogRateMatch,
    CatalogRateRef,
)

ChecksumRef = CatalogRateRef | CatalogMaterialRef
TimedMatch = CatalogRateMatch | CatalogMaterialMatch
ProjectScopedMatch = CatalogRateMatch | CatalogMaterialMatch | CatalogFormulaMatch


@dataclass(frozen=True, slots=True)
class SelectedFormula:
    definition_id: UUID
    scope_type: str
    project_id: UUID | None
    formula_id: str
    version: int
    name: str
    dsl_version: str
    output_key: str
    output_contract: dict[str, Any]
    declared_inputs: list[dict[str, Any]]
    checksum_sha256: str
    expression: dict[str, Any]
    rounding: dict[str, Any] | None


class CatalogSelectionError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        conflicting_candidate_ids: tuple[UUID, ...] = (),
    ) -> None:
        self.conflicting_candidate_ids = conflicting_candidate_ids
        detail = (
            f" conflicting_candidate_ids={conflicting_candidate_ids}"
            if conflicting_candidate_ids
            else ""
        )
        super().__init__(f"{message}{detail}")


def select_rate(
    candidates: Iterable[CatalogRateMatch],
    *,
    auto: CatalogRateAutoSelectRequest | None = None,
    ref: CatalogRateRef | None = None,
) -> CatalogRateMatch:
    return _select_timed(candidates, auto=auto, ref=ref)


def select_material(
    candidates: Iterable[CatalogMaterialMatch],
    *,
    auto: CatalogMaterialAutoSelectRequest | None = None,
    ref: CatalogMaterialRef | None = None,
) -> CatalogMaterialMatch:
    return _select_timed(candidates, auto=auto, ref=ref)


def select_formula(
    candidates: Iterable[CatalogFormulaMatch],
    *,
    auto: CatalogFormulaAutoSelectRequest | None = None,
    ref: CatalogFormulaRef | None = None,
) -> SelectedFormula:
    if (auto is None) == (ref is None):
        raise CatalogSelectionError("select_formula requires exactly one selector")

    filtered = _non_stale(candidates)
    if ref is not None:
        match = _select_by_formula_ref(filtered, ref)
    else:
        assert auto is not None
        match = _select_formula_auto(filtered, auto)

    return SelectedFormula(
        definition_id=match.id,
        scope_type=match.scope_type,
        project_id=match.project_id,
        formula_id=match.formula_id,
        version=match.version,
        name=match.name,
        dsl_version=match.dsl_version,
        output_key=match.output_key,
        output_contract=match.output_contract,
        declared_inputs=match.declared_inputs,
        checksum_sha256=match.checksum_sha256,
        expression=match.expression,
        rounding=match.rounding,
    )


def _select_timed[
    TMatch: TimedMatch,
    TAuto: CatalogRateAutoSelectRequest | CatalogMaterialAutoSelectRequest,
    TRef: ChecksumRef,
](
    candidates: Iterable[TMatch],
    *,
    auto: TAuto | None,
    ref: TRef | None,
) -> TMatch:
    if (auto is None) == (ref is None):
        raise CatalogSelectionError("selector requires exactly one of auto or ref")

    filtered = _non_stale(candidates)
    if ref is not None:
        return _select_by_checksum_ref(filtered, ref)

    assert auto is not None
    return _select_timed_auto(filtered, auto)


def _non_stale[TMatch: ProjectScopedMatch](candidates: Iterable[TMatch]) -> list[TMatch]:
    return [candidate for candidate in candidates if candidate.superseded_by_id is None]


def _select_by_checksum_ref[TMatch: TimedMatch, TRef: ChecksumRef](
    candidates: Iterable[TMatch],
    ref: TRef,
) -> TMatch:
    matched = [candidate for candidate in candidates if candidate.id == ref.id]
    if not matched:
        raise CatalogSelectionError("explicit reference is stale or missing")

    if len(matched) > 1:
        raise CatalogSelectionError("explicit reference matched multiple catalog rows")

    candidate = matched[0]
    if candidate.checksum_sha256 != ref.checksum_sha256:
        raise CatalogSelectionError("explicit reference checksum mismatch")
    return candidate


def _select_by_formula_ref(
    candidates: Iterable[CatalogFormulaMatch],
    ref: CatalogFormulaRef,
) -> CatalogFormulaMatch:
    matched = [candidate for candidate in candidates if candidate.id == ref.id]
    if not matched:
        raise CatalogSelectionError("explicit formula reference is stale or missing")
    if len(matched) > 1:
        raise CatalogSelectionError(
            "explicit formula reference matched multiple catalog rows",
            conflicting_candidate_ids=_sorted_candidate_ids(matched),
        )

    candidate = matched[0]
    if candidate.checksum_sha256 != ref.checksum_sha256:
        raise CatalogSelectionError("explicit formula reference checksum mismatch")
    return candidate


def _select_timed_auto[TMatch: TimedMatch](
    candidates: Iterable[TMatch],
    auto: CatalogRateAutoSelectRequest | CatalogMaterialAutoSelectRequest,
) -> TMatch:
    active = [
        candidate
        for candidate in candidates
        if _matches_timed_request(candidate, auto) and _is_effective(candidate, auto.as_of)
    ]
    return _resolve_project_scope(active, auto.project_id)


def _select_formula_auto(
    candidates: Iterable[CatalogFormulaMatch],
    auto: CatalogFormulaAutoSelectRequest,
) -> CatalogFormulaMatch:
    matched = [candidate for candidate in candidates if candidate.formula_id == auto.formula_id]
    if auto.version is not None:
        matched = [candidate for candidate in matched if candidate.version == auto.version]
    return _resolve_project_scope(matched, auto.project_id)


def _resolve_project_scope[TMatch: ProjectScopedMatch](
    candidates: Iterable[TMatch],
    project_id: UUID | None,
) -> TMatch:
    candidate_list = list(candidates)
    if project_id is not None:
        project_matches = [
            candidate for candidate in candidate_list if candidate.project_id == project_id
        ]
        if project_matches:
            return _require_single(project_matches, "project")

    global_matches = [candidate for candidate in candidate_list if candidate.project_id is None]
    if global_matches:
        return _require_single(global_matches, "global")

    raise CatalogSelectionError("no non-stale catalog match found")


def _require_single[TMatch: ProjectScopedMatch](candidates: list[TMatch], scope: str) -> TMatch:
    if len(candidates) == 1:
        return candidates[0]
    raise CatalogSelectionError(
        f"multiple non-stale {scope} catalog matches found",
        conflicting_candidate_ids=_sorted_candidate_ids(candidates),
    )


def _sorted_candidate_ids[TMatch: ProjectScopedMatch](
    candidates: Iterable[TMatch],
) -> tuple[UUID, ...]:
    return tuple(
        sorted(
            (candidate.id for candidate in candidates),
            key=lambda candidate_id: candidate_id.hex,
        )
    )


def _is_effective(candidate: TimedMatch, as_of: date) -> bool:
    if candidate.effective_start > as_of:
        return False
    if candidate.effective_end is None:
        return True
    return as_of < candidate.effective_end


def _matches_timed_request(
    candidate: TimedMatch,
    auto: CatalogRateAutoSelectRequest | CatalogMaterialAutoSelectRequest,
) -> bool:
    if isinstance(candidate, CatalogRateMatch) and isinstance(auto, CatalogRateAutoSelectRequest):
        return (
            candidate.rate_key == auto.rate_key
            and candidate.item_type == auto.item_type
            and candidate.unit == auto.unit
            and candidate.currency == auto.currency
        )

    if isinstance(candidate, CatalogMaterialMatch) and isinstance(
        auto, CatalogMaterialAutoSelectRequest
    ):
        return (
            candidate.material_key == auto.material_key
            and candidate.unit == auto.unit
            and candidate.currency == auto.currency
        )

    return False
