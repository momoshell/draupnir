"""Per-revision legend dictionary builder (issue #550, Phase 1).

Builds a :class:`LegendDictionary` by reducing three typed extractors in descending authority:

- Source A — ``from_block_families``: device-family names as canonical type_name (highest trust).
- Source B — ``from_prose_schedule``: free-prose schedule strings; enriches description and
  resolves abbreviation→meaning, but never invents a type_name.
- Source C — ``from_tag_layers``: distinct placed tag tokens → vocabulary entries (same authority
  tier as A for type_name; A wins over C on conflict by convention — documented below).

``fuse`` merges all three and produces a :class:`LegendDictionary` keyed by abbreviation and
by symbol_family.

NOTE: ``legend.py`` will later emit :class:`LegendEntry` directly once ``SymbolEntry`` is
migrated. PDF ``SymbolEntry`` (abbreviation, type_name, bbox) maps onto :class:`LegendEntry`
cleanly. That refactor is deferred.

Pure module — NO DB, ORM, FastAPI, or SQLAlchemy imports. Unit-testable with fakes.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Source constants
# ---------------------------------------------------------------------------

LEGEND_SOURCE_FAMILY: str = "block_family"  # Source A
LEGEND_SOURCE_PROSE: str = "prose_schedule"  # Source B
LEGEND_SOURCE_TAG: str = "tag_layer"  # Source C

# ---------------------------------------------------------------------------
# Local abbreviation pattern (NOT imported from legend.py — kept independent).
# Short all-caps token: 1-4 chars + optional "-X" suffix.
# Examples: DC, FAP, FAPR, NC-H, ACS
# ---------------------------------------------------------------------------

_ABBREVIATION_RE = re.compile(r"^[A-Z][A-Z0-9]{0,3}(?:-[A-Z0-9]{1,3})?$")

# Source-B prose patterns.
# "XX DENOTES <meaning>"
_PROSE_DENOTES_RE = re.compile(
    r"^([A-Z][A-Z0-9]{0,3}(?:-[A-Z0-9]{1,3})?)\s+DENOTES\s+(.+)$",
    re.IGNORECASE,
)
# "XX = <meaning>"
_PROSE_EQUALS_RE = re.compile(
    r"^([A-Z][A-Z0-9]{0,3}(?:-[A-Z0-9]{1,3})?)\s*=\s*(.+)$",
)

# ---------------------------------------------------------------------------
# Local input dataclasses (mirrors _TagCandidate shape in devices.py)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FamilyInput:
    """Source-A input: a single block-family name from the revision."""

    family_name: str


@dataclass(frozen=True, slots=True)
class ProseInput:
    """Source-B input: a single free-prose string from a schedule or legend block."""

    text: str


@dataclass(frozen=True, slots=True)
class TagInput:
    """Source-C input: a single placed tag token from a tag layer."""

    token: str


# ---------------------------------------------------------------------------
# Output types — frozen P1 → P2 interface contract
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LegendEntry:
    """One entry in the per-revision legend dictionary.

    Key space 1: ``abbreviation`` (tag/prose token; None for family-only entries).
    Key space 2: ``symbol_family`` (block-family name from Source A; None for tag/prose-only).

    Authority for ``type_name``:
    - A (block_family) and C (tag_layer) are authoritative — they win over B.
    - On conflict between A and C: **A wins** (block geometry is more reliable than a placed
      text token); the C value is placed in ``competing_type_names``.
    - B (prose_schedule) only supplies ``description``; its type_name is always None.

    ``confidence=None`` means "not scored / face value" (rooms convention).
    ``confidence=None`` + non-empty ``competing_type_names`` additionally signals a conflict.
    """

    abbreviation: str | None
    symbol_family: str | None
    type_name: str | None
    description: str | None
    sources: tuple[str, ...]  # sorted, deduped provenance
    confidence: float | None  # None = not-scored / conflict marker
    competing_type_names: tuple[str, ...]  # competitors when A ≠ C; () when none


@dataclass(frozen=True, slots=True)
class LegendDictionary:
    """Immutable legend dictionary for a revision.

    Entries are in deterministic order (sorted by abbreviation then symbol_family).
    """

    entries: tuple[LegendEntry, ...]

    def by_abbreviation(self) -> dict[str, LegendEntry]:
        """Dict keyed by abbreviation; entries without one are omitted."""
        return {e.abbreviation: e for e in self.entries if e.abbreviation is not None}

    def by_symbol_family(self) -> dict[str, LegendEntry]:
        """Dict keyed by symbol_family; entries without one are omitted."""
        return {e.symbol_family: e for e in self.entries if e.symbol_family is not None}


# ---------------------------------------------------------------------------
# Source A — block families
# ---------------------------------------------------------------------------


def from_block_families(inputs: Sequence[FamilyInput]) -> list[LegendEntry]:
    """Source A: one entry per distinct family name; type_name IS the family name.

    Block-family names are the highest-authority type identifier available from geometry.
    Duplicates (same family_name appearing multiple times) are deduped.
    """
    seen: set[str] = set()
    entries: list[LegendEntry] = []
    for inp in inputs:
        name = inp.family_name.strip()
        if not name or name in seen:
            continue
        seen.add(name)
        entries.append(
            LegendEntry(
                abbreviation=None,
                symbol_family=name,
                type_name=name,
                description=None,
                sources=(LEGEND_SOURCE_FAMILY,),
                confidence=None,
                competing_type_names=(),
            )
        )
    return entries


# ---------------------------------------------------------------------------
# Source B — prose schedule
# ---------------------------------------------------------------------------


def from_prose_schedule(inputs: Sequence[ProseInput]) -> list[LegendEntry]:
    """Source B: tolerant parse of free-prose strings into abbreviation definitions.

    Recognised patterns:
    - ``'CV' DENOTES CONTROL VALVE``  → abbr=CV, desc="CONTROL VALVE" (no type_name)
    - ``XX = SOME MEANING``           → abbr=XX, desc="SOME MEANING"  (no type_name)
    - ``SMOKE DETECTOR WITH BEACON``  → description-only (no abbreviation, no type invented)
    - Garbage / unparseable / empty   → skipped silently; NEVER raises.

    B never produces a ``type_name`` — authority for type belongs exclusively to A and C.
    """
    entries: list[LegendEntry] = []
    for inp in inputs:
        try:
            entry = _parse_prose_line(inp.text)
        except Exception:  # tolerant by contract — B must never raise
            continue
        if entry is not None:
            entries.append(entry)
    return entries


def _parse_prose_line(raw: str) -> LegendEntry | None:
    """Parse one prose line; return None for unparseable or empty input."""
    line = raw.strip()
    if not line:
        return None

    # "XX DENOTES <meaning>"
    m = _PROSE_DENOTES_RE.match(line)
    if m:
        abbr = m.group(1).upper()
        meaning = m.group(2).strip()
        if _ABBREVIATION_RE.match(abbr) and meaning:
            return LegendEntry(
                abbreviation=abbr,
                symbol_family=None,
                type_name=None,
                description=meaning,
                sources=(LEGEND_SOURCE_PROSE,),
                confidence=None,
                competing_type_names=(),
            )

    # "XX = <meaning>"
    m = _PROSE_EQUALS_RE.match(line)
    if m:
        abbr = m.group(1).upper()
        meaning = m.group(2).strip()
        if _ABBREVIATION_RE.match(abbr) and meaning:
            return LegendEntry(
                abbreviation=abbr,
                symbol_family=None,
                type_name=None,
                description=meaning,
                sources=(LEGEND_SOURCE_PROSE,),
                confidence=None,
                competing_type_names=(),
            )

    # A bare all-caps token matching the abbreviation pattern has no meaning — skip.
    if _ABBREVIATION_RE.match(line):
        return None

    # Multi-word or mixed-case phrase → description-only, no type_name invented.
    return LegendEntry(
        abbreviation=None,
        symbol_family=None,
        type_name=None,
        description=line,
        sources=(LEGEND_SOURCE_PROSE,),
        confidence=None,
        competing_type_names=(),
    )


# ---------------------------------------------------------------------------
# Source C — tag layers
# ---------------------------------------------------------------------------


def from_tag_layers(inputs: Sequence[TagInput]) -> list[LegendEntry]:
    """Source C: distinct placed tag tokens → vocabulary entries.

    Tokens not matching the abbreviation pattern (non-device noise) are skipped.
    Duplicate tokens from multiple placements yield exactly one entry.
    """
    seen: set[str] = set()
    entries: list[LegendEntry] = []
    for inp in inputs:
        token = inp.token.strip()
        if not token or not _ABBREVIATION_RE.match(token) or token in seen:
            continue
        seen.add(token)
        entries.append(
            LegendEntry(
                abbreviation=token,
                symbol_family=None,
                # The token itself is the best type identity we have from a placed tag.
                type_name=token,
                description=None,
                sources=(LEGEND_SOURCE_TAG,),
                confidence=None,
                competing_type_names=(),
            )
        )
    return entries


# ---------------------------------------------------------------------------
# Fuse
# ---------------------------------------------------------------------------


def fuse(entries: Sequence[LegendEntry]) -> LegendDictionary:
    """Fuse extractor outputs into a :class:`LegendDictionary`.

    Merging rules:
    - Entries with the same ``abbreviation`` OR the same ``symbol_family`` are merged,
      provided doing so does not create a conflict (two distinct values in the same key
      slot).  Conflicting entries live in separate groups.
    - ``type_name``: A (block_family) > C (tag_layer) > B (always None); on A vs C conflict,
      A wins primary ``type_name`` and C appears in ``competing_type_names``.  Within a
      same-authority tier, the primary is chosen by sorted order (deterministic).
    - ``description``: B enriches; A/C descriptions used when B contributes none.
    - ``sources``: sorted, deduped union of all contributors.
    - ``confidence``: always ``None`` (not yet scored); also signals conflict when
      ``competing_type_names`` is non-empty.

    ``fuse([])`` and all-empty inputs return an empty :class:`LegendDictionary` — a legitimate
    degraded outcome.
    """
    if not entries:
        return LegendDictionary(entries=())

    merged = _merge_all(list(entries))

    def _sort_key(e: LegendEntry) -> tuple[str, str, str, str]:
        return (e.abbreviation or "", e.symbol_family or "", e.description or "", e.type_name or "")

    sorted_entries = sorted(merged, key=_sort_key)
    return LegendDictionary(entries=tuple(sorted_entries))


# ---------------------------------------------------------------------------
# Internal merge machinery
# ---------------------------------------------------------------------------


@dataclass
class _Acc:
    """Mutable accumulator for entries that share a key."""

    abbreviation: str | None = None
    symbol_family: str | None = None
    # type_name candidates separated by source tier
    family_types: list[str] = field(default_factory=list)
    tag_types: list[str] = field(default_factory=list)
    descriptions: list[str] = field(default_factory=list)
    sources: set[str] = field(default_factory=set)


def _acc_add(acc: _Acc, entry: LegendEntry) -> None:
    """Fold one entry's contribution into an accumulator."""
    for src in entry.sources:
        acc.sources.add(src)

    if (
        LEGEND_SOURCE_FAMILY in entry.sources
        and entry.type_name is not None
        and entry.type_name not in acc.family_types
    ):
        acc.family_types.append(entry.type_name)

    if (
        LEGEND_SOURCE_TAG in entry.sources
        and entry.type_name is not None
        and entry.type_name not in acc.tag_types
    ):
        acc.tag_types.append(entry.type_name)

    if entry.description is not None and entry.description not in acc.descriptions:
        acc.descriptions.append(entry.description)


def _acc_merge(keep: _Acc, drop: _Acc) -> None:
    """Merge ``drop`` into ``keep`` in-place."""
    keep.family_types.extend(t for t in drop.family_types if t not in keep.family_types)
    keep.tag_types.extend(t for t in drop.tag_types if t not in keep.tag_types)
    keep.descriptions.extend(d for d in drop.descriptions if d not in keep.descriptions)
    keep.sources.update(drop.sources)
    if keep.abbreviation is None and drop.abbreviation is not None:
        keep.abbreviation = drop.abbreviation
    if keep.symbol_family is None and drop.symbol_family is not None:
        keep.symbol_family = drop.symbol_family


def _acc_finalise(acc: _Acc) -> LegendEntry:
    """Convert accumulator → frozen LegendEntry applying A > C authority rules.

    Within the same authority tier, the primary type_name is chosen by sorted order
    so that the result is deterministic regardless of input permutation order.
    """
    description = "; ".join(acc.descriptions) if acc.descriptions else None

    # Sort within each tier for determinism, then A-tier leads (A wins over C).
    sorted_family = sorted(acc.family_types)
    sorted_tag = sorted(acc.tag_types)

    # Build unique authority type list: A-tier first (sorted), then C-tier (sorted).
    seen_types: set[str] = set()
    authority_types: list[str] = []
    for t in sorted_family:
        if t not in seen_types:
            seen_types.add(t)
            authority_types.append(t)
    for t in sorted_tag:
        if t not in seen_types:
            seen_types.add(t)
            authority_types.append(t)

    if not authority_types:
        # B-only or bare-description entry — honest unknown type.
        return LegendEntry(
            abbreviation=acc.abbreviation,
            symbol_family=acc.symbol_family,
            type_name=None,
            description=description,
            sources=tuple(sorted(acc.sources)),
            confidence=None,
            competing_type_names=(),
        )

    primary = authority_types[0]
    competitors = tuple(authority_types[1:])

    return LegendEntry(
        abbreviation=acc.abbreviation,
        symbol_family=acc.symbol_family,
        type_name=primary,
        description=description,
        sources=tuple(sorted(acc.sources)),
        confidence=None,  # not yet scored; also serves as conflict marker when competitors exist
        competing_type_names=competitors,
    )


def _can_merge(group_abbr: str | None, group_family: str | None, entry: LegendEntry) -> bool:
    """Return True iff merging ``entry`` into a group with the given keys is conflict-free.

    Two entries may share a key (abbreviation or symbol_family) but must not force two
    *distinct* values into the same key slot.  A None value is always compatible with any
    other value (it simply gets filled in).
    """
    # Check abbreviation compatibility: if both have non-None abbreviations they must match.
    if (
        group_abbr is not None
        and entry.abbreviation is not None
        and group_abbr != entry.abbreviation
    ):
        return False
    # Check symbol_family compatibility: if both have non-None families they must match.
    return not (
        group_family is not None
        and entry.symbol_family is not None
        and group_family != entry.symbol_family
    )


def _merge_all(entries: list[LegendEntry]) -> list[LegendEntry]:
    """Conflict-aware grouping: entries sharing a key collapse into one group ONLY when
    doing so would not place two distinct values in the same key slot.

    Algorithm (two-pass, fully deterministic):

    Pass 1 — family-keyed groups.
      Each distinct symbol_family becomes its own group.  Entries without a symbol_family
      are deferred to pass 2.  Among entries that share the same symbol_family, all have
      the same group_family by definition, so no family-key conflict can arise.

    Pass 2 — abbreviation-keyed attachment.
      For each deferred (family-less) entry with an abbreviation, look for existing
      family groups whose abbreviation slot already equals this abbreviation (equality
      only — no open-slot heuristic, which would fabricate drawing links that don't
      exist).  If exactly one such group exists, attach the entry.  If zero or multiple
      matching groups exist, the entry starts/joins its own abbreviation-keyed group.

    Pass 3 — bare-description (no abbreviation, no family) entries.
      Each forms its own singleton group (no key to join on).

    This is order-independent because grouping decisions are made on key-value equality,
    not on insertion order, and the attachment logic checks for unambiguous matches only.
    """
    if not entries:
        return []

    # --- Pass 1: group by symbol_family ---
    # family → list of accumulators (one per distinct abbreviation seen for that family).
    # Most families will have a single accumulator; when two entries share a family but carry
    # distinct non-None abbreviations they are conflict-incompatible and get separate accs.
    # deferred holds entries without a symbol_family.
    family_groups: dict[str, list[_Acc]] = {}
    deferred: list[LegendEntry] = []

    for entry in entries:
        if entry.symbol_family is not None:
            fam = entry.symbol_family
            if fam not in family_groups:
                acc = _Acc(symbol_family=fam)
                family_groups[fam] = [acc]
                _acc_add(acc, entry)
                if entry.abbreviation is not None:
                    acc.abbreviation = entry.abbreviation
            else:
                # Find the first accumulator in this family that is compatible with entry.
                placed = False
                for acc in family_groups[fam]:
                    if _can_merge(acc.abbreviation, acc.symbol_family, entry):
                        _acc_add(acc, entry)
                        if acc.abbreviation is None and entry.abbreviation is not None:
                            acc.abbreviation = entry.abbreviation
                        placed = True
                        break
                if not placed:
                    # Abbreviation conflict within the same family → new accumulator.
                    new_acc = _Acc(symbol_family=fam)
                    if entry.abbreviation is not None:
                        new_acc.abbreviation = entry.abbreviation
                    _acc_add(new_acc, entry)
                    family_groups[fam].append(new_acc)
        else:
            deferred.append(entry)

    # --- Pass 2: attach deferred entries that have an abbreviation ---
    # Build a fast lookup: abbreviation → list of (family_name, acc_index) tuples.
    # Each family can have multiple accs (one per distinct abbreviation).
    abbr_to_family_accs: dict[str, list[tuple[str, int]]] = {}
    for fam, accs in family_groups.items():
        for idx, acc in enumerate(accs):
            if acc.abbreviation is not None:
                abbr_to_family_accs.setdefault(acc.abbreviation, []).append((fam, idx))

    # abbreviation-only groups (no symbol_family found): abbr → accumulator.
    abbr_groups: dict[str, _Acc] = {}
    bare_description_entries: list[LegendEntry] = []

    for entry in deferred:
        if entry.abbreviation is not None:
            abbr = entry.abbreviation
            # Find family-group accumulators that already claim this abbreviation.
            matching = abbr_to_family_accs.get(abbr, [])
            if len(matching) == 1:
                # Exactly one family-acc already claims this abbreviation → attach.
                fam, idx = matching[0]
                _acc_add(family_groups[fam][idx], entry)
            elif len(matching) == 0:
                # No family acc carries this abbreviation — form/extend own abbreviation group.
                # Bridging onto a family-acc that merely has an open (None) abbreviation slot
                # would fabricate a family↔abbreviation link not present in the drawing.
                if abbr not in abbr_groups:
                    abbr_groups[abbr] = _Acc(abbreviation=abbr)
                _acc_add(abbr_groups[abbr], entry)
            else:
                # Multiple family accs claim the same abbreviation — shared across distinct
                # families (e.g. "SD" for "Smoke Detector Ceiling" and "Smoke Detector Duct").
                # The entry cannot join any single acc without ambiguity → own group.
                if abbr not in abbr_groups:
                    abbr_groups[abbr] = _Acc(abbreviation=abbr)
                _acc_add(abbr_groups[abbr], entry)
        else:
            bare_description_entries.append(entry)

    # --- Pass 3: bare-description entries (no abbreviation, no family) ---
    # Each is a singleton; they share no key so they can never merge.
    singleton_accs: list[_Acc] = []
    for entry in bare_description_entries:
        acc = _Acc()
        _acc_add(acc, entry)
        singleton_accs.append(acc)

    # --- Finalise all groups ---
    result: list[LegendEntry] = []
    for accs in family_groups.values():
        for acc in accs:
            result.append(_acc_finalise(acc))
    for acc in abbr_groups.values():
        result.append(_acc_finalise(acc))
    for acc in singleton_accs:
        result.append(_acc_finalise(acc))

    return result
