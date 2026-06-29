# ADR-0011 — Fragmented pipe-tag identity via a reassembly pre-pass

- **Status:** accepted
- **Date:** 2026-06-29
- **Context issue:** #787 (D3); composes #785 (D1 `strict_content` gate), #786 (D2 layer roles).

## Context

Drainage DWGs (e.g. Welbeck P-520001, rev `60b900cc`) store a single pipe-tag callout
as **multiple separate MTEXT entities**. The callout `100∅SVP AT HL DROPS TB` arrives on
layer `Z010T` as three fragments: a bare diameter number (`100`/`75`), a bare diameter
glyph (`∅`, also garbled as `�`/`?` from non-UTF-8 sources), and a service+level string
(`SVP AT HL DROPS TB`). `run_tags.parse_tag` is a single-string parser requiring size and
service together, so no fragment alone yields a `TagObservation`. Result: post-D1/D2,
the drainage service-takeoff resolved only 105.8 m of 184.0 m, leaving **78.4 m (43%)
honest-`unknown`** — runs whose nearest usable tag is an unparseable fragment.

The legend on this sheet carries no abbreviations (`by_abbreviation() == []`), so service
identity must come from the tag text itself, not a legend allowlist.

## Decision

Recover fragmented tags with a **pure spatial reassembly pre-pass**
(`app/interpretation/tag_reassembly.py`) that clusters spatially-adjacent fragments,
concatenates them into one canonical tag string, validates the result by round-tripping
through `parse_tag`, and emits **synthetic `TagPlacement`s**. The route runs this pre-pass
between the loader and the two tag consumers (`fuse_run_service_identities` and the
segment-label builder); `parse_tag` and the identity coordinators are unchanged.

Load-bearing specifics:

1. **Concatenation order is `glyph → number → space → service`** (`"∅100 SVP AT HL DROPS TB"`),
   NOT number-first. `parse_tag`'s round patterns require `digits␣service`; a glyph
   *between* number and service (`"100∅SVP"`) does not match and returns `None`. The
   leading `∅` supplies the `strict_content` diameter context so the bare-round path is
   accepted without a legend. (Verified empirically.)
2. **Glyph variants `∅`/`Ø`/`�`(U+FFFD)/`?` all normalize to a single canonical `∅`.**
3. **Cluster radius ≈ 0.5 m**, calibrated from real geometry (per callout, NUM→GLYPH
   ≤0.27 m, NUM→SVC ≤0.40 m) — not a looser legend-row radius, to avoid adjacent-callout
   mis-pairing.
4. **`parse_tag` round-trip validation gates fabrication**: a cluster that does not parse,
   or is ambiguous (>1 equidistant candidate), is rejected and stays honest-`unknown`.
5. **The classifier must never consume a placement `parse_tag` already accepts** — this
   keeps the already-resolved length monotonically non-decreasing (enforced by test).

## Alternatives considered

- **Inline reassembly inside `fuse_run_service_identities`** — rejected: pollutes the
  frozen identity coordinator, does not help the segment-label consumer, harder to test,
  risks regressing the byte-identical colour-keyed path.
- **Change `parse_tag` to span fragments** — rejected: `parse_tag` is a pure single-string
  parser by contract; fragment assembly is a distinct spatial concern.

## Consequences

- **Conservation is structural**: reassembly rewrites tag *text* only; length is computed
  from geometry independently of tags, so the 184.0 m total cannot change — reassembly
  only re-buckets `unknown` → named services.
- **No regression on colour-keyed (electrical) or single-entity (med-gas) sheets**: their
  tags do not fragment, so the pre-pass is a no-op pass-through there.
- Adds one pure module + a single route pre-pass call; no schema, route-shape, or migration
  change. Split for delivery into D3a (#795, the module) and D3b (#796, the wiring + gate).
