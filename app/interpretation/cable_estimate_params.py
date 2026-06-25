"""Cable estimate parameter model (issue #695).

These are SURFACED and EDITABLE assumptions.  Every cable-length estimate produced
by the system attaches a stamp of these parameters so the output is fully
parameter-stamped and its provenance is auditable.

Canonical values:
- Luminaire vertical drop = 2.0 m.  This is the ONLY spec-note-backed value: drawing
  E-630003 states "LUMINAIRES SHALL BE VIA PLUG IN CEILING ROSE ... NO MORE THAN 2M
  FOR THE LUMINAIRE CABLE ACCESS".  The 2.0 m is taken as the maximum/design value.
- Switch drop = 1.2 m.  Standard-practice wall-switch mounting height assumption;
  not stated in the drawing notes.  Confirm per project.
- Socket drop = 1.2 m.  Standard-practice socket mounting/drop assumption; not stated
  in the drawing notes.  Confirm per project.
- Distribution board / panel drop = 0.0 m.  The circuit source; no run is measured
  from the panel to itself.  Assumption; confirm per project.
- Spare fraction = 0.10 (10%).  Typical spare/waste allowance.  Assumption; confirm
  per project.

All drops are in METRES.  spare_fraction is a dimensionless fraction (0.10 = 10%).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------
# Category constants
# ---------------------------------------------------------------------------

CATEGORY_LUMINAIRE = "luminaire"
CATEGORY_SWITCH = "switch"
CATEGORY_SOCKET = "socket"
CATEGORY_DISTRIBUTION_BOARD = "distribution_board"

# ---------------------------------------------------------------------------
# Source constants
# ---------------------------------------------------------------------------

SOURCE_SPEC_NOTE = "spec_note"
SOURCE_ASSUMPTION = "assumption"
SOURCE_USER_OVERRIDE = "user_override"


# ---------------------------------------------------------------------------
# DropParam
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DropParam:
    """Vertical-drop allowance for a single device category.

    Attributes:
        category: Device category name (use CATEGORY_* constants).
        drop_m: Vertical drop length in metres (>= 0).
        source: Provenance type (SOURCE_SPEC_NOTE | SOURCE_ASSUMPTION |
            SOURCE_USER_OVERRIDE).
        basis: Human-readable provenance description.
    """

    category: str
    drop_m: float
    source: str
    basis: str

    def __post_init__(self) -> None:
        if self.drop_m < 0:
            raise ValueError(
                f"drop_m must be >= 0, got {self.drop_m!r} for category {self.category!r}"
            )


# ---------------------------------------------------------------------------
# CableEstimateParams
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CableEstimateParams:
    """Full parameter set for a cable-length estimate.

    Attributes:
        vertical_drops: One :class:`DropParam` per category, sorted by category
            name for determinism.
        spare_fraction: Fraction added for spare/waste (e.g. 0.10 = 10%).
        spare_source: Provenance type for the spare fraction.
        spare_basis: Human-readable provenance for the spare fraction.
    """

    vertical_drops: tuple[DropParam, ...]
    spare_fraction: float
    spare_source: str
    spare_basis: str

    def __post_init__(self) -> None:
        if self.spare_fraction < 0:
            raise ValueError(f"spare_fraction must be >= 0, got {self.spare_fraction!r}")

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def drop_for(self, category: str) -> DropParam | None:
        """Return the :class:`DropParam` for *category*, or ``None`` if absent."""
        for drop in self.vertical_drops:
            if drop.category == category:
                return drop
        return None

    # ------------------------------------------------------------------
    # Non-mutating override
    # ------------------------------------------------------------------

    def with_overrides(
        self,
        *,
        drops: Mapping[str, float] | None = None,
        spare_fraction: float | None = None,
    ) -> CableEstimateParams:
        """Return a NEW :class:`CableEstimateParams` with selected values replaced.

        Overriding a drop keeps the category but sets source to
        ``SOURCE_USER_OVERRIDE`` and basis to ``"user override"``.
        Unknown override categories are added as new :class:`DropParam` entries.

        Args:
            drops: Mapping of category → new drop_m values.
            spare_fraction: New spare fraction (dimensionless).

        Returns:
            A new frozen :class:`CableEstimateParams`; the original is unchanged.

        Raises:
            ValueError: If any override drop_m or spare_fraction is negative.
        """
        if drops is None and spare_fraction is None:
            return self

        # Build a mutable dict of existing drops keyed by category.
        drop_map: dict[str, DropParam] = {d.category: d for d in self.vertical_drops}

        if drops is not None:
            for cat, value in drops.items():
                # Validation happens inside DropParam.__post_init__
                drop_map[cat] = DropParam(
                    category=cat,
                    drop_m=value,
                    source=SOURCE_USER_OVERRIDE,
                    basis="user override",
                )

        sorted_drops = tuple(sorted(drop_map.values(), key=lambda d: d.category))

        new_spare = spare_fraction if spare_fraction is not None else self.spare_fraction
        if new_spare < 0:
            raise ValueError(f"spare_fraction must be >= 0, got {new_spare!r}")

        new_spare_source = SOURCE_USER_OVERRIDE if spare_fraction is not None else self.spare_source
        new_spare_basis = "user override" if spare_fraction is not None else self.spare_basis

        return CableEstimateParams(
            vertical_drops=sorted_drops,
            spare_fraction=new_spare,
            spare_source=new_spare_source,
            spare_basis=new_spare_basis,
        )

    # ------------------------------------------------------------------
    # Stamp
    # ------------------------------------------------------------------

    def as_stamp(self) -> dict[str, Any]:
        """Return a JSON-serialisable, deterministic representation of these parameters.

        The stamp is attached to every estimate's assumptions band so that the
        estimate is fully parameter-stamped.  Key order is stable.

        Returns:
            A ``dict`` with keys ``"vertical_drops"`` (list of per-category dicts,
            sorted by category) and ``"spare"`` (dict with fraction/source/basis).
        """
        drops_list = [
            {
                "category": d.category,
                "drop_m": d.drop_m,
                "source": d.source,
                "basis": d.basis,
            }
            for d in self.vertical_drops  # already sorted
        ]
        return {
            "vertical_drops": drops_list,
            "spare": {
                "fraction": self.spare_fraction,
                "source": self.spare_source,
                "basis": self.spare_basis,
            },
        }


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def default_estimate_params() -> CableEstimateParams:
    """Return the documented default :class:`CableEstimateParams`.

    Sources:
    - Luminaire 2.0 m — spec note (E-630003).
    - Switch/socket 1.2 m, panel 0.0 m, spare 10% — standard-practice assumptions.
    """
    drops = tuple(
        sorted(
            [
                DropParam(
                    category=CATEGORY_LUMINAIRE,
                    drop_m=2.0,
                    source=SOURCE_SPEC_NOTE,
                    basis=(
                        "E-630003 spec note: 'LUMINAIRES SHALL BE VIA PLUG IN CEILING"
                        " ROSE ... NO MORE THAN 2M FOR THE LUMINAIRE CABLE ACCESS'"
                    ),
                ),
                DropParam(
                    category=CATEGORY_SWITCH,
                    drop_m=1.2,
                    source=SOURCE_ASSUMPTION,
                    basis="standard wall-switch mounting height; not stated in drawing notes",
                ),
                DropParam(
                    category=CATEGORY_SOCKET,
                    drop_m=1.2,
                    source=SOURCE_ASSUMPTION,
                    basis="standard socket mounting/drop; not stated in drawing notes",
                ),
                DropParam(
                    category=CATEGORY_DISTRIBUTION_BOARD,
                    drop_m=0.0,
                    source=SOURCE_ASSUMPTION,
                    basis="circuit source — no drop measured from the panel to itself",
                ),
            ],
            key=lambda d: d.category,
        )
    )
    return CableEstimateParams(
        vertical_drops=drops,
        spare_fraction=0.10,
        spare_source=SOURCE_ASSUMPTION,
        spare_basis="typical 10% spare/waste allowance; confirm per project",
    )
