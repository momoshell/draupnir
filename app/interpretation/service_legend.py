"""Per-revision service legend reducer (issue #606, Phase 1 / be-01).

Builds a :class:`ServiceLegend` by reducing two typed extractors keyed on a COLOUR axis:

- Source A — ``from_colour_swatches``: colour-swatch + entry-text pairs from the drawing
  legend; the text names the discipline/service (highest trust for colour identity).
- Source B — ``from_abbreviation_prose``: free-prose abbreviation rows (e.g.
  "GFR DENOTES GAS FIRED RADIANT HEATERS"); supplies abbreviation→description with NO colour
  and NO invented discipline.

``fuse`` merges both and produces a :class:`ServiceLegend` keyed by resolved colour key and
by abbreviation.

Mirrors ``app/interpretation/legend_dictionary.py``'s reducer shape exactly; new key axis is
the resolved colour (rgb-preferred hex, else index as ``"idx:<n>"``).

Pure module — NO DB, ORM, FastAPI, or SQLAlchemy imports. Unit-testable with fakes.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Source constants
# ---------------------------------------------------------------------------

SERVICE_SOURCE_SWATCH: str = "legend_swatch"
SERVICE_SOURCE_PROSE: str = "legend_prose"

# ---------------------------------------------------------------------------
# Local patterns — reuse legend_dictionary shapes (kept independent; not imported).
# Short all-caps token: 1-4 chars + optional "-X" suffix.
# ---------------------------------------------------------------------------

_ABBREVIATION_RE = re.compile(r"^[A-Z][A-Z0-9]{0,3}(?:-[A-Z0-9]{1,3})?$")

# "XX DENOTES <meaning>"
_PROSE_DENOTES_RE = re.compile(
    r"^([A-Z][A-Z0-9]{0,3}(?:-[A-Z0-9]{1,3})?)\s+DENOTES\s+(.+)$",
    re.IGNORECASE,
)
# "XX = <meaning>"
_PROSE_EQUALS_RE = re.compile(
    r"^([A-Z][A-Z0-9]{0,3}(?:-[A-Z0-9]{1,3})?)\s*=\s*(.+)$",
)

# Strip leading \L / trailing \l underline directives and %%u/%%U toggle pairs that
# some DXF/DWG text fields emit (e.g. "\LLEGEND\l", "%%uLEGEND%%u", "%%ULEGEND%%U").
_UNDERLINE_DIRECTIVE_RE = re.compile(r"^\\[Ll]")
_UNDERLINE_TRAILING_RE = re.compile(r"\\[Ll]$")
_UNDERLINE_PERCENT_TOGGLE_RE = re.compile(r"%%[uU]")

# ---------------------------------------------------------------------------
# Input dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SwatchInput:
    """Source-A input: a colour swatch paired with its legend entry text."""

    color: Mapping[str, object]
    # Legend entry text beside the swatch, e.g. "HYDRAULIC EQUIPMENT".
    text: str


@dataclass(frozen=True, slots=True)
class ProseInput:
    """Source-B input: a single free-prose abbreviation row from the legend."""

    text: str


# ---------------------------------------------------------------------------
# Output types — frozen P1 → P2 interface contract
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ServiceEntry:
    """One entry in the per-revision service legend.

    Key space 1: ``colour_key`` (canonical colour identifier; None for abbreviation-only entries).
    Key space 2: ``abbreviation`` (None for swatch-only entries).

    ``confidence=None`` means "not scored / face value" (rooms convention).
    ``confidence=None`` + non-empty ``competing_disciplines`` additionally signals a conflict
    where two distinct disciplines share the same colour.
    """

    colour_key: str | None
    colour_index: int | None
    colour_rgb: str | None
    discipline: str | None
    abbreviation: str | None
    description: str | None
    sources: tuple[str, ...]  # sorted, deduped provenance
    confidence: float | None  # None = not-scored / conflict marker
    competing_disciplines: tuple[str, ...]  # competitors when same colour → distinct disciplines


@dataclass(frozen=True, slots=True)
class ServiceLegend:
    """Immutable service legend for a revision.

    Entries are in deterministic order (sorted by colour_key, discipline, abbreviation).
    """

    entries: tuple[ServiceEntry, ...]

    def by_colour(self) -> dict[str, ServiceEntry]:
        """Dict keyed by colour_key; entries without one are omitted."""
        return {e.colour_key: e for e in self.entries if e.colour_key is not None}

    def by_abbreviation(self) -> dict[str, ServiceEntry]:
        """Dict keyed by abbreviation; entries without one are omitted."""
        return {e.abbreviation: e for e in self.entries if e.abbreviation is not None}


# ---------------------------------------------------------------------------
# Colour key helper
# ---------------------------------------------------------------------------


def colour_key(color: Mapping[str, object] | None) -> str | None:
    """Return a canonical colour key from a resolved style.color mapping.

    Preference: lowercased rgb hex string when present, else ``"idx:<n>"`` when an integer
    index is present, else None.  The adapter pre-resolves BYLAYER/BYBLOCK — callers read
    the final resolved fields; no re-resolution happens here.
    """
    if color is None:
        return None
    rgb = color.get("rgb")
    if rgb is not None:
        return str(rgb).lower()
    index = color.get("index")
    if index is not None:
        return f"idx:{index}"
    return None


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------


def _strip_underline(text: str) -> str:
    """Strip DXF/DWG underline formatting codes from text.

    Handles:
    - Leading ``\\L`` / ``\\l`` (underline-on directives)
    - Trailing ``\\l`` / ``\\L`` (underline-off directives)
    - ``%%u`` / ``%%U`` toggle pairs (alternate underline encoding)
    """
    return _UNDERLINE_PERCENT_TOGGLE_RE.sub(
        "", _UNDERLINE_TRAILING_RE.sub("", _UNDERLINE_DIRECTIVE_RE.sub("", text))
    )


def _normalize_text(raw: str) -> str:
    """Strip underline directives and collapse internal whitespace."""
    return " ".join(_strip_underline(raw).split())


# ---------------------------------------------------------------------------
# Source A — colour swatches
# ---------------------------------------------------------------------------


def from_colour_swatches(inputs: Sequence[SwatchInput]) -> list[ServiceEntry]:
    """Source A: one ServiceEntry per distinct colour_key + discipline pair.

    Two distinct disciplines for the same colour_key are folded into ONE entry with
    ``confidence=None`` and ``competing_disciplines`` populated (sorted, deduped).
    Blank text or colour-less inputs are skipped silently; never raises.
    """
    # colour_key -> list[discipline] (order of first encounter for stable sort later)
    colour_disciplines: dict[str, list[str]] = {}
    # colour_key -> first SwatchInput carrying that key (for rgb/index extraction)
    colour_meta: dict[str, SwatchInput] = {}

    for inp in inputs:
        try:
            ck = colour_key(inp.color)
            if ck is None:
                continue
            disc = _normalize_text(inp.text)
            if not disc:
                continue
            if ck not in colour_disciplines:
                colour_disciplines[ck] = []
                colour_meta[ck] = inp
            if disc not in colour_disciplines[ck]:
                colour_disciplines[ck].append(disc)
        except Exception:  # tolerant by contract
            continue

    entries: list[ServiceEntry] = []
    for ck, disciplines in colour_disciplines.items():
        meta = colour_meta[ck]
        # Deterministic: sort disciplines so the primary and competing are stable.
        sorted_discs = sorted(disciplines)
        primary = sorted_discs[0]
        competing = tuple(sorted_discs[1:])

        index_val = meta.color.get("index")
        rgb_val = meta.color.get("rgb")
        colour_index = int(str(index_val)) if index_val is not None else None
        colour_rgb = str(rgb_val).lower() if rgb_val is not None else None

        entries.append(
            ServiceEntry(
                colour_key=ck,
                colour_index=colour_index,
                colour_rgb=colour_rgb,
                discipline=primary,
                abbreviation=None,
                description=None,
                sources=(SERVICE_SOURCE_SWATCH,),
                confidence=None,
                competing_disciplines=competing,
            )
        )
    return entries


# ---------------------------------------------------------------------------
# Source B — abbreviation prose
# ---------------------------------------------------------------------------


def from_abbreviation_prose(inputs: Sequence[ProseInput]) -> list[ServiceEntry]:
    """Source B: tolerant parse of free-prose abbreviation rows.

    Recognised patterns:
    - ``"GFR DENOTES GAS FIRED RADIANT HEATERS"``  → abbr=GFR, description= ...
    - ``"GP = GAS PROVING PANEL"``                  → abbr=GP,  description= ...
    - Garbage / unparseable / empty                 → skipped silently; NEVER raises.

    B never produces a colour or discipline — NO colour invented, NO discipline invented.
    """
    entries: list[ServiceEntry] = []
    for inp in inputs:
        try:
            entry = _parse_prose_line(inp.text)
        except Exception:  # tolerant by contract
            continue
        if entry is not None:
            entries.append(entry)
    return entries


def _parse_prose_line(raw: str) -> ServiceEntry | None:
    """Parse one prose line into a ServiceEntry, or return None for unparseable/empty."""
    line = _normalize_text(raw)
    if not line:
        return None

    # "XX DENOTES <meaning>"
    m = _PROSE_DENOTES_RE.match(line)
    if m:
        abbr = m.group(1).upper()
        meaning = m.group(2).strip()
        if _ABBREVIATION_RE.match(abbr) and meaning:
            return ServiceEntry(
                colour_key=None,
                colour_index=None,
                colour_rgb=None,
                discipline=None,
                abbreviation=abbr,
                description=meaning,
                sources=(SERVICE_SOURCE_PROSE,),
                confidence=None,
                competing_disciplines=(),
            )

    # "XX = <meaning>"
    m = _PROSE_EQUALS_RE.match(line)
    if m:
        abbr = m.group(1).upper()
        meaning = m.group(2).strip()
        if _ABBREVIATION_RE.match(abbr) and meaning:
            return ServiceEntry(
                colour_key=None,
                colour_index=None,
                colour_rgb=None,
                discipline=None,
                abbreviation=abbr,
                description=meaning,
                sources=(SERVICE_SOURCE_PROSE,),
                confidence=None,
                competing_disciplines=(),
            )

    # Bare all-caps token matching the abbreviation pattern has no meaning — skip.
    if _ABBREVIATION_RE.match(line):
        return None

    # Multi-word or mixed-case phrase → skip (no colour, no discipline to emit).
    return None


# ---------------------------------------------------------------------------
# Fuse
# ---------------------------------------------------------------------------


def fuse(entries: Sequence[ServiceEntry]) -> ServiceLegend:
    """Fuse extractor outputs into a :class:`ServiceLegend`.

    Merging rules:
    - Colour-keyed entries (from swatch) group by ``colour_key``; disciplines are already
      resolved inside ``from_colour_swatches`` — each colour_key arrives as a single entry.
    - Abbreviation-only rows (from prose) are kept distinct; they carry no colour so there
      is no colour↔abbreviation bridge (equality-only — never fabricate a link).
    - ``sources``: sorted, deduped union of all contributors.
    - ``confidence``: always ``None`` (not yet scored); also signals conflict when
      ``competing_disciplines`` is non-empty.

    Deterministic / permutation-invariant: entries sorted by (colour_key, discipline,
    abbreviation) as a stable multi-field key.

    ``fuse([])`` returns an empty :class:`ServiceLegend`.
    """
    if not entries:
        return ServiceLegend(entries=())

    merged = _merge_all(list(entries))

    def _sort_key(e: ServiceEntry) -> tuple[str, str, str]:
        return (e.colour_key or "", e.discipline or "", e.abbreviation or "")

    sorted_entries = sorted(merged, key=_sort_key)
    return ServiceLegend(entries=tuple(sorted_entries))


# ---------------------------------------------------------------------------
# Internal merge machinery
# ---------------------------------------------------------------------------


@dataclass
class _Acc:
    """Mutable accumulator for entries that share a colour_key."""

    colour_key: str | None = None
    colour_index: int | None = None
    colour_rgb: str | None = None
    disciplines: list[str] = field(default_factory=list)
    abbreviation: str | None = None
    descriptions: list[str] = field(default_factory=list)
    sources: set[str] = field(default_factory=set)


def _acc_add(acc: _Acc, entry: ServiceEntry) -> None:
    """Fold one entry's contribution into an accumulator."""
    for src in entry.sources:
        acc.sources.add(src)

    if entry.discipline is not None and entry.discipline not in acc.disciplines:
        acc.disciplines.append(entry.discipline)
    # Merge competing disciplines from a pre-merged swatch entry.
    for cd in entry.competing_disciplines:
        if cd not in acc.disciplines:
            acc.disciplines.append(cd)

    if entry.description is not None and entry.description not in acc.descriptions:
        acc.descriptions.append(entry.description)

    if acc.colour_index is None and entry.colour_index is not None:
        acc.colour_index = entry.colour_index
    if acc.colour_rgb is None and entry.colour_rgb is not None:
        acc.colour_rgb = entry.colour_rgb

    if acc.abbreviation is None and entry.abbreviation is not None:
        acc.abbreviation = entry.abbreviation


def _acc_finalise(acc: _Acc) -> ServiceEntry:
    """Convert accumulator → frozen ServiceEntry with deterministic discipline ordering."""
    description = "; ".join(acc.descriptions) if acc.descriptions else None

    # Deterministic: sort disciplines; primary = first alphabetically.
    sorted_discs = sorted(acc.disciplines)
    primary_disc = sorted_discs[0] if sorted_discs else None
    competing = tuple(sorted_discs[1:])

    return ServiceEntry(
        colour_key=acc.colour_key,
        colour_index=acc.colour_index,
        colour_rgb=acc.colour_rgb,
        discipline=primary_disc,
        abbreviation=acc.abbreviation,
        description=description,
        sources=tuple(sorted(acc.sources)),
        confidence=None,
        competing_disciplines=competing,
    )


def _merge_all(entries: list[ServiceEntry]) -> list[ServiceEntry]:
    """Group entries by colour_key; abbreviation-only rows stay distinct.

    Algorithm:
    Pass 1 — colour-keyed entries: each distinct colour_key becomes its own group.
    Pass 2 — colour-less (abbreviation-only) entries: each is a singleton.

    Colour↔abbreviation bridging is intentionally NOT performed — equality-only per
    the legend-is-ground-truth contract.
    """
    if not entries:
        return []

    # Pass 1: group by colour_key.
    colour_groups: dict[str, _Acc] = {}
    deferred: list[ServiceEntry] = []

    for entry in entries:
        if entry.colour_key is not None:
            ck = entry.colour_key
            if ck not in colour_groups:
                acc = _Acc(colour_key=ck)
                colour_groups[ck] = acc
            _acc_add(colour_groups[ck], entry)
        else:
            deferred.append(entry)

    # Pass 2: colour-less entries (prose abbreviation rows) — each is its own singleton.
    # Group by abbreviation so duplicate prose rows collapse, but no cross-key bridging.
    abbr_groups: dict[str, _Acc] = {}
    bare_singletons: list[_Acc] = []

    for entry in deferred:
        if entry.abbreviation is not None:
            abbr = entry.abbreviation
            if abbr not in abbr_groups:
                abbr_groups[abbr] = _Acc(abbreviation=abbr)
            _acc_add(abbr_groups[abbr], entry)
        else:
            acc = _Acc()
            _acc_add(acc, entry)
            bare_singletons.append(acc)

    result: list[ServiceEntry] = []
    for acc in colour_groups.values():
        result.append(_acc_finalise(acc))
    for acc in abbr_groups.values():
        result.append(_acc_finalise(acc))
    for acc in bare_singletons:
        result.append(_acc_finalise(acc))

    return result
