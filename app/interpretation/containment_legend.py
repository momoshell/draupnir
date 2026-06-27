"""Containment legend reducer (issue #754 / Phase 752a).

Builds a :class:`ContainmentLegend` by reducing containment-swatch inputs keyed on the
COMBINED axis ``(colour_key, pattern_name)`` — the load-bearing distinction from
``service_legend.from_colour_swatches`` which keys on colour alone.

Each input carries a resolved colour mapping, a hatch pattern name (e.g. ``"SOLID"``,
``"FP_1"``; ``""`` when absent — never None), and the nearest legend label text. The output
maps every distinct ``(colour_key, pattern_name)`` pair to one :class:`ContainmentLegendEntry`.

Design notes
------------
- Blank label (text normalises to empty) → entry emitted with ``containment_type=None``
  (honest-absent signal; deliberately diverges from ``from_colour_swatches`` which skips blank
  text — here a present-but-unlabelled swatch is meaningful).
- ``colour_key`` is ``None`` → input silently skipped; absent from ``by_key()``.
- Two distinct type names on the same key → ONE entry; primary = alphabetically first;
  the rest populate ``competing_types`` (sorted, deduped).
- ``""`` as ``pattern_name`` is a valid key component — never dropped.
- Per-input ``try/except`` mirrors ``from_colour_swatches`` tolerance contract.
- Deterministic / permutation-invariant: entries sorted by
  ``(colour_key or "", pattern_name, containment_type or "")``.

Pure module — NO DB, ORM, FastAPI, or SQLAlchemy imports. Unit-testable with fakes.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from app.interpretation.service_legend import _normalize_text, colour_key

# ---------------------------------------------------------------------------
# Source constant
# ---------------------------------------------------------------------------

CONTAINMENT_SOURCE_SWATCH: str = "containment_legend_swatch"

# ---------------------------------------------------------------------------
# Input dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ContainmentSwatchInput:
    """One hatch swatch from the containment legend.

    ``color``        — resolved style.color mapping, or None.
    ``pattern_name`` — hatch pattern identifier (e.g. "SOLID", "FP_1"); ``""`` when absent.
    ``text``         — nearest legend label text.
    """

    color: Mapping[str, object] | None
    pattern_name: str  # "" when absent; NEVER None
    text: str


# ---------------------------------------------------------------------------
# Output dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ContainmentLegendEntry:
    """One entry in the per-revision containment legend.

    Key: ``(colour_key, pattern_name)`` — the combined swatch identity axis.

    ``containment_type=None`` signals honest-absent (label was blank or not parseable).
    ``competing_types`` is non-empty when two or more distinct type names collided on the
    same key; the primary ``containment_type`` is alphabetically first among them.
    """

    colour_key: str | None
    colour_index: int | None
    colour_rgb: str | None
    pattern_name: str  # normalised; "" preserved
    containment_type: str | None  # None = honest-absent
    sources: tuple[str, ...]
    competing_types: tuple[str, ...]  # other names colliding on the same key (sorted, deduped)


@dataclass(frozen=True, slots=True)
class ContainmentLegend:
    """Immutable containment legend for a revision.

    Entries are in deterministic order (sorted by colour_key, pattern_name, containment_type).
    """

    entries: tuple[ContainmentLegendEntry, ...]
    # Built once from ``entries`` (frozen); the lookup hot path (one call per body hatch in
    # 752b) reads this instead of rebuilding a dict per call. Excluded from init/compare/repr.
    _index: dict[tuple[str, str], ContainmentLegendEntry] = field(
        default_factory=dict, init=False, compare=False, repr=False
    )

    def __post_init__(self) -> None:
        index = {
            (e.colour_key, e.pattern_name): e for e in self.entries if e.colour_key is not None
        }
        object.__setattr__(self, "_index", index)

    def by_key(self) -> dict[tuple[str, str], ContainmentLegendEntry]:
        """Dict keyed by ``(colour_key, pattern_name)``; ``colour_key=None`` entries omitted."""
        return dict(self._index)

    def lookup(
        self, colour_key_value: str | None, pattern_name: str
    ) -> ContainmentLegendEntry | None:
        """Return the entry for ``(colour_key_value, pattern_name)``, or ``None`` if unmapped.

        Honest-absent entries (``containment_type=None``) are still returned when found;
        ``None`` is only returned for keys that do not exist in ``by_key()``.
        """
        if colour_key_value is None:
            return None
        return self._index.get((colour_key_value, pattern_name))


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------


def from_containment_swatches(
    inputs: Sequence[ContainmentSwatchInput],
) -> list[ContainmentLegendEntry]:
    """Build one :class:`ContainmentLegendEntry` per distinct ``(colour_key, pattern_name)`` pair.

    Rules
    -----
    - ``colour_key(color) is None`` → skip silently.
    - Blank label (text normalises to ``""``) → emit entry with ``containment_type=None``.
    - Two distinct type names on the same key → ONE entry; primary = alphabetically first;
      the rest → ``competing_types``.
    - Per-input ``try/except: continue`` — never raises to caller.
    - Deterministic: output sorted by ``(colour_key or "", pattern_name, containment_type or "")``.
    """
    # (colour_key, pattern_name) -> distinct non-blank containment_type candidates.
    # Honest-absent (every label for the key was blank) is represented by an EMPTY list,
    # not a None element — those keys still emit an entry with containment_type=None below.
    key_types: dict[tuple[str, str], list[str]] = {}
    key_meta: dict[tuple[str, str], ContainmentSwatchInput] = {}

    for inp in inputs:
        try:
            ck = colour_key(inp.color)
            if ck is None:
                continue
            # Whitespace-only pattern_name collapses to "" (same key as a true-empty pattern);
            # upstream adapters emit "" not "   ", so this is benign normalisation.
            norm_pattern = _normalize_text(inp.pattern_name)
            composite_key = (ck, norm_pattern)

            ct: str | None = _normalize_text(inp.text) or None  # blank → None (honest-absent)

            if composite_key not in key_types:
                key_types[composite_key] = []
                key_meta[composite_key] = inp

            # Accumulate distinct non-None type names; honest-absent is stored separately
            # via absence from key_types (we track it via None sentinel below).
            if ct is not None and ct not in key_types[composite_key]:
                key_types[composite_key].append(ct)
        except Exception:  # tolerant by contract
            continue

    # Emit one entry per (colour_key, pattern_name) key. Keys whose type list is empty
    # (only blank labels were seen) emit an honest-absent entry (containment_type=None).
    entries: list[ContainmentLegendEntry] = []
    for composite_key, types in key_types.items():
        ck, norm_pattern = composite_key
        meta = key_meta[composite_key]

        index_val = meta.color.get("index") if meta.color is not None else None
        rgb_val = meta.color.get("rgb") if meta.color is not None else None
        colour_index = int(str(index_val)) if index_val is not None else None
        colour_rgb = str(rgb_val).lower() if rgb_val is not None else None

        if types:
            sorted_types = sorted(types)
            primary = sorted_types[0]
            competing = tuple(sorted_types[1:])
        else:
            # All labels for this key were blank → honest-absent
            primary = None
            competing = ()

        entries.append(
            ContainmentLegendEntry(
                colour_key=ck,
                colour_index=colour_index,
                colour_rgb=colour_rgb,
                pattern_name=norm_pattern,
                containment_type=primary,
                sources=(CONTAINMENT_SOURCE_SWATCH,),
                competing_types=competing,
            )
        )

    # Deterministic sort: (colour_key or "", pattern_name, containment_type or "")
    entries.sort(key=lambda e: (e.colour_key or "", e.pattern_name, e.containment_type or ""))
    return entries


def build_containment_legend(
    inputs: Sequence[ContainmentSwatchInput],
) -> ContainmentLegend:
    """Build a :class:`ContainmentLegend` from containment swatch inputs.

    ``build_containment_legend([])`` returns a :class:`ContainmentLegend` with empty entries.
    """
    entries = from_containment_swatches(inputs)
    return ContainmentLegend(entries=tuple(entries))
