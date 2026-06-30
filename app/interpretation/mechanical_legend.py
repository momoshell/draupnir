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

import colorsys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from app.interpretation.service_legend import _normalize_text, colour_key

# ---------------------------------------------------------------------------
# Hue-matching helpers (private)
# ---------------------------------------------------------------------------

# Minimum saturation (0-1) for a colour to be considered chromatic.
# DWG fill tints are desaturated but still clearly chromatic (e.g. ffd480 has s>=0.50).
# A threshold of 0.15 excludes true greys/blacks/whites while keeping all real pipe fills.
_MIN_SATURATION: float = 0.15

# Minimum value (brightness, 0-1) for a colour to be considered chromatic.
# Excludes near-black colours (e.g. c3000007 -> value ~0.027).
_MIN_VALUE: float = 0.08


def _parse_rgb_from_key(colour_key_value: str) -> tuple[int, int, int] | None:
    """Parse (R, G, B) ints 0-255 from a DWG/DXF colour_key hex string.

    Colour keys are 8-char lowercase hex (e.g. ``c2ffb120``) where the last 6 chars are
    RRGGBB.  Also handles 6-char ``rrggbb`` (no prefix) and leading-hash ``#rrggbb`` forms.
    Returns None if the string cannot be parsed.
    """
    ck = colour_key_value.lstrip("#").lower()
    if len(ck) == 8:
        # 2-char prefix (DWG TrueColor encoding) + 6-char RRGGBB
        hex6 = ck[2:]
    elif len(ck) == 6:
        hex6 = ck
    else:
        return None
    try:
        r = int(hex6[0:2], 16)
        g = int(hex6[2:4], 16)
        b = int(hex6[4:6], 16)
    except ValueError:
        return None
    return r, g, b


def _rgb_to_hsv(r: int, g: int, b: int) -> tuple[float, float, float]:
    """Convert (R, G, B) 0-255 to (hue_deg 0-360, saturation 0-1, value 0-1)."""
    h, s, v = colorsys.rgb_to_hsv(r / 255.0, g / 255.0, b / 255.0)
    return h * 360.0, s, v


def _hue_distance(a: float, b: float) -> float:
    """Shortest angular distance (0-180 degrees) between two hue values in degrees."""
    diff = abs(a - b) % 360.0
    return diff if diff <= 180.0 else 360.0 - diff


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

    def lookup_tolerant(
        self,
        colour_key_value: str | None,
        *,
        hue_tol_deg: float = 22.0,
        margin_deg: float = 8.0,
    ) -> MechanicalLegendEntry | None:
        """Hue-based tolerant lookup for fill colours that are tints of legend swatches.

        DWG pipe fill colours are desaturated/lightened TINTS of the legend swatch: same hue,
        large RGB distance.  RGB-euclidean matching mislabels these (the hue-trap); matching
        on hue resolves them unambiguously.

        Algorithm
        ---------
        1. Parse ``colour_key_value`` → RGB → HSV.
        2. **Achromatic gate**: if saturation < ``_MIN_SATURATION`` or value < ``_MIN_VALUE``,
           return None — only chromatic fills match chromatic legend entries.
        3. Among legend entries whose own colour is chromatic AND whose ``service`` is not None,
           find the entry with the **nearest circular hue distance** within ``hue_tol_deg``.
        4. **Margin guard**: if the nearest and second-nearest hue distances are within
           ``margin_deg`` of each other (ambiguous near-tie), return None — honest abstention
           rather than a guess.  This guards the green-collision (F1/F3) and similar near-misses.
        5. Return the matched entry, or None.

        ``hue_tol_deg=22`` and ``margin_deg=8`` are calibrated on P-520001:
        - SVP fill 40° → SVP swatch 39° (dist=1°); VP swatch 0° (dist=40°) — clear.
        - VP fill 11° → VP swatch 0° (dist=11°); SVP swatch 39° (dist=28°) — clear.
        - RWP fill 120° → RWP swatch 120° (dist=0°) — exact on hue.
        - Cross-family distances are all ≥28°, well outside the 8° margin from nearest.

        Does NOT call ``lookup``; caller is responsible for trying exact lookup first.
        Never raises (returns None on any parse error).
        """
        if colour_key_value is None:
            return None

        # Parse query colour
        query_rgb = _parse_rgb_from_key(colour_key_value)
        if query_rgb is None:
            return None

        query_h, query_s, query_v = _rgb_to_hsv(*query_rgb)

        # Achromatic gate: BYLAYER/mono fills have no hue signal
        if query_s < _MIN_SATURATION or query_v < _MIN_VALUE:
            return None

        # Collect chromatic legend candidates (only those with a real service)
        candidates: list[tuple[float, MechanicalLegendEntry]] = []
        for entry in self.entries:
            if entry.service is None:
                continue
            if entry.colour_rgb is None:
                continue
            entry_rgb = _parse_rgb_from_key(entry.colour_rgb)
            if entry_rgb is None:
                continue
            entry_h, entry_s, entry_v = _rgb_to_hsv(*entry_rgb)
            # Skip achromatic legend entries — they have no reliable hue
            if entry_s < _MIN_SATURATION or entry_v < _MIN_VALUE:
                continue
            dist = _hue_distance(query_h, entry_h)
            candidates.append((dist, entry))

        if not candidates:
            return None

        # Sort by hue distance (ascending); stable sort preserves service-name determinism
        candidates.sort(key=lambda t: (t[0], t[1].service or ""))

        best_dist, best_entry = candidates[0]

        # Reject if outside tolerance
        if best_dist > hue_tol_deg:
            return None

        # Margin guard: reject ambiguous near-ties
        if len(candidates) >= 2:
            second_dist = candidates[1][0]
            if (second_dist - best_dist) < margin_deg:
                return None

        return best_entry


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
