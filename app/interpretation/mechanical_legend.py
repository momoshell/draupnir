"""Mechanical colour→service legend reducer (issue #775).

Builds a :class:`MechanicalLegend` by reducing coloured line/hatch swatch inputs keyed on
``colour_key`` alone — the load-bearing distinction from ``ContainmentLegend`` which keys on
``(colour_key, pattern_name)``.  Mechanical pipe services are identified purely by fill colour
(the service colour IS the pipe colour; no pattern signal is needed).

Each input carries a resolved colour mapping and the nearest legend label text.  The output
maps every distinct ``colour_key`` to one :class:`MechanicalLegendEntry`.

Design notes
------------
- Blank label (text normalises to empty) → entry emitted with ``service=None``
  (honest-absent signal; a present-but-unlabelled swatch is meaningful).
- ``colour_key`` is ``None`` → input silently skipped.
- Two distinct service names on the same colour_key → ONE entry; primary = alphabetically
  first; the rest populate ``competing_services`` (sorted, deduped).  This mirrors the
  ``ContainmentLegend`` competing-types pattern and is load-bearing for c29632ff on M-560103
  which maps to three services (VRF CASSETTE, OVER DOOR HEATER, RADIANT HEATING PANELS).
- Per-input ``try/except`` mirrors the containment legend tolerance contract.
- Deterministic / permutation-invariant: entries sorted by ``(colour_key or "", service or "")``.

Pure module — NO DB, ORM, FastAPI, or SQLAlchemy imports. Unit-testable with fakes.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from app.interpretation.service_legend import _normalize_text, colour_key

# ---------------------------------------------------------------------------
# Source constant
# ---------------------------------------------------------------------------

MECH_SOURCE_SWATCH: str = "mech_legend_swatch"

# ---------------------------------------------------------------------------
# Input dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MechSwatchInput:
    """One coloured line/hatch swatch from the mechanical service legend.

    ``color`` — resolved style.color mapping, or None.
    ``text``  — nearest legend label text.
    """

    color: Mapping[str, object] | None
    text: str


# ---------------------------------------------------------------------------
# Output dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MechanicalLegendEntry:
    """One entry in the per-revision mechanical colour→service legend.

    Key: ``colour_key`` — the swatch colour identity axis.

    ``service=None`` signals honest-absent (label was blank or not parseable).
    ``competing_services`` is non-empty when two or more distinct service names collided on
    the same colour_key; the primary ``service`` is alphabetically first among them.
    """

    colour_key: str | None
    colour_index: int | None
    colour_rgb: str | None
    service: str | None  # None = honest-absent
    sources: tuple[str, ...]
    competing_services: tuple[
        str, ...
    ]  # other names colliding on same colour_key (sorted, deduped)


@dataclass(frozen=True, slots=True)
class MechanicalLegend:
    """Immutable mechanical colour→service legend for a revision.

    Entries are in deterministic order (sorted by colour_key, service).
    """

    entries: tuple[MechanicalLegendEntry, ...]
    # Built once from ``entries`` (frozen); the lookup hot path reads this instead of
    # rebuilding a dict per call.  Excluded from init/compare/repr.
    _index: dict[str, MechanicalLegendEntry] = field(
        default_factory=dict, init=False, compare=False, repr=False
    )

    def __post_init__(self) -> None:
        index = {e.colour_key: e for e in self.entries if e.colour_key is not None}
        object.__setattr__(self, "_index", index)

    def by_key(self) -> dict[str, MechanicalLegendEntry]:
        """Dict keyed by ``colour_key``; ``colour_key=None`` entries omitted."""
        return dict(self._index)

    def lookup(self, colour_key_value: str | None) -> MechanicalLegendEntry | None:
        """Return the entry for ``colour_key_value``, or ``None`` if unmapped.

        Honest-absent entries (``service=None``) are still returned when found;
        ``None`` is only returned for keys that do not exist in ``by_key()``.
        """
        if colour_key_value is None:
            return None
        return self._index.get(colour_key_value)


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------


def from_mech_swatches(
    inputs: Sequence[MechSwatchInput],
) -> list[MechanicalLegendEntry]:
    """Build one :class:`MechanicalLegendEntry` per distinct ``colour_key``.

    Rules
    -----
    - ``colour_key(color) is None`` → skip silently.
    - Blank label (text normalises to ``""``) → emit entry with ``service=None``.
    - Two distinct service names on the same colour_key → ONE entry; primary = alphabetically
      first; the rest → ``competing_services``.
    - Per-input ``try/except: continue`` — never raises to caller.
    - Deterministic: output sorted by ``(colour_key or "", service or "")``.
    """
    # colour_key → distinct non-blank service name candidates.
    # Honest-absent (every label for the key was blank) is represented by an EMPTY list.
    key_services: dict[str, list[str]] = {}
    key_meta: dict[str, MechSwatchInput] = {}

    for inp in inputs:
        try:
            ck = colour_key(inp.color)
            if ck is None:
                continue

            svc: str | None = _normalize_text(inp.text) or None  # blank → None (honest-absent)

            if ck not in key_services:
                key_services[ck] = []
                key_meta[ck] = inp

            if svc is not None and svc not in key_services[ck]:
                key_services[ck].append(svc)
        except Exception:  # tolerant by contract
            continue

    entries: list[MechanicalLegendEntry] = []
    for ck, services in key_services.items():
        meta = key_meta[ck]

        index_val = meta.color.get("index") if meta.color is not None else None
        rgb_val = meta.color.get("rgb") if meta.color is not None else None
        colour_index = int(str(index_val)) if index_val is not None else None
        colour_rgb = str(rgb_val).lower() if rgb_val is not None else None

        if services:
            sorted_svcs = sorted(services)
            primary = sorted_svcs[0]
            competing = tuple(sorted_svcs[1:])
        else:
            # All labels for this key were blank → honest-absent
            primary = None
            competing = ()

        entries.append(
            MechanicalLegendEntry(
                colour_key=ck,
                colour_index=colour_index,
                colour_rgb=colour_rgb,
                service=primary,
                sources=(MECH_SOURCE_SWATCH,),
                competing_services=competing,
            )
        )

    # Deterministic sort: (colour_key or "", service or "")
    entries.sort(key=lambda e: (e.colour_key or "", e.service or ""))
    return entries


def build_mechanical_legend(
    inputs: Sequence[MechSwatchInput],
) -> MechanicalLegend:
    """Build a :class:`MechanicalLegend` from mechanical swatch inputs.

    ``build_mechanical_legend([])`` returns a :class:`MechanicalLegend` with empty entries.
    """
    entries = from_mech_swatches(inputs)
    return MechanicalLegend(entries=tuple(entries))
