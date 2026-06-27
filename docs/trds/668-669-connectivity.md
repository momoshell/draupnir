---
issue: 668, 669
epic: 606
status: draft-pending-approval
author: architecture-lead
date: 2026-06-27
builds_on: 667 (merged)
---

# TRD: Routed-Run Connectivity — Fitting Bridging (#668) + Mid-Span Tee Split (#669)

P4 connectivity of EPIC #606; reopens the connectivity scope of (closed) #618. Builds on
the **merged #667** framework (`app/interpretation/routed_connectivity.py`).

## 0. Review outcome (trd-reviewer, approve-with-changes — folded in)
- **D6 (KEYSTONE for #669): split sub-segments INHERIT the parent's verdict verbatim — NEVER
  re-run `_segment_verdicts` on sub-segments.** Re-classifying a sub-segment can flip its
  colour (overlap-ratio over its own shorter geometry differs from the parent's), silently
  changing per_colour/shared. The length-invariant `_assert_invariant` would NOT catch this
  (it's an attribution, not a length, regression). Splits are pure geometry subdivisions for
  connectivity; the verdict travels with the parent.
- **Invariant is a tautology in `_build_result`** (Σ of the same seg_len list partitioned one
  bucket each), NOT a consequence of collinearity. So: `_build_result` AND `total_length`
  must both be sourced from the SAME post-split `(seg_len, verdict)` list; add a unit test
  asserting Σ(sub-segment lengths) == parent length (length conservation at the split itself).
- **#669 is an index-model RESTRUCTURE of `refine_shared_by_connectivity`, not a clean
  prepend:** every `len(verdicts)`-indexed structure (`node_to_segs`, `seg_neighbours`,
  `frozen_colours`, re-tally at ~line 178) must be rebuilt from the post-split set. #669 owns
  this; when #669 lands, re-validate #668's fitting pass against the post-split index set.
- **#669 determinism edge cases to test explicitly:** (1) one node projecting onto TWO edges
  (split both; both split points must snap to the SAME grid node id, else propagation sees
  them disconnected); (2) a split point within `snap_tol_m` of a THIRD unrelated node → snap
  to it, don't create a new node.
- **OD-2 confirmed:** `centerline_segment_count = len(centerline_segments)` (input count) —
  post-split it intentionally diverges from the tally cardinality; add a one-line note so a
  future reader doesn't "fix" it.
- **Real-data gate pre-work (REQUIRED):** before any code, CAPTURE + RECORD M-540003's current
  `fill_attribution.{shared_length_m, per_colour, total_length_m}` as the frozen #667-only
  baseline — else "plateau = honest pass" is unfalsifiable.
- **#668 specifics:** (a) verify `load_service_fitting_bands` truly mirrors — confirm fitting
  HATCH geometry parses via the same `boundary_loops`/`vertices` path on real M-540003 (idx
  30/17/131) before assuming the mirror holds; (b) "exactly ONE fitting colour" is over
  point-in-(grown-)union membership — a point in two overlapping grown unions → ≥2 → stays
  shared (safe); two endpoints in different colours → stays shared; (c) do NOT reintroduce a
  pre-`refine` `compute_fill_attributed_lengths` call (the route's dead-double-compute,
  service_takeoff.py:294-296) when wiring fitting bands.

## 1. Goal
Reduce the honest `__shared__`/manifold residual (centerline length that fill-colour
proximity can't attribute to a service) via run-graph connectivity — **without guessing**.
- **#668** — a shared segment whose endpoint is inside a SINGLE-colour fitting hatch
  (elbow/tee on `Pipe Fittings`/`Pipe Accessories`) inherits that colour; equidistant
  between two different colours stays shared.
- **#669** — a node on the INTERIOR of another edge (mid-span tee) splits that edge at the
  projection, then #667 propagation re-runs over the enriched graph.
Load-bearing non-negotiable: the honesty invariant **Σ(per_colour) + shared_length_m ==
total_length_m within ±0.1 m**, and **what stays ambiguous stays `__shared__`**.

## 2. Ground truth (code-verified)
- #667 `refine_shared_by_connectivity(...)` owns the verdict pass (`_segment_verdicts`),
  builds a run-graph by snapping endpoints to a `snap_tol_m=0.030` integer grid, does a
  SINGLE-pass dominant-single-neighbour propagation over a frozen verdict snapshot, and
  enforces the invariant in `_build_result`. `total_length_m`/`centerline_segment_count`
  mirror the verdict pass (immune to refinement). Zero-length segments excluded.
- **DRIFT FLAG:** "Step 5c" is in the API route `app/api/v1/revision_routes/service_takeoff.py`
  (~lines 269-314), NOT interpretation `service_takeoff.py`. The route already calls
  `load_service_fill_bands` then `refine_shared_by_connectivity` (DWG-only). **Both PRs edit
  the route file.**
- `load_service_fill_bands` (`service_takeoff_loaders.py:1281`) is the exact mirror for
  #668's loader; `_SERVICE_FILL_LAYER_TOKENS` (~:167) is the token-constant pattern;
  `_colour_key` (~:57) resolves service colour.
- Real-data oracle M-540003 (project `49140cff`, rev `1cc72d6e-...`) ingested; authored
  Center Line total **244.0 m**; baseline shared ≈ 7.1 m / ~3%, ~100% two-colour-equidistant.
  `Pipe Fittings` service hatches present (idx 30/17/131).
- Constraints: pure module (shapely-only, dict-coord discipline via `_xy`/`_xy_list`, metres,
  deterministic + permutation-stable); impure loader for #668 only.

## 3. Architecture
**Composition pipeline (single invariant chokepoint):**
verdict pass → **#669 mid-span split** (length-conserving) → **#667 endpoint propagation**
(frozen) → **#668 fitting bridge** (frozen) → `_build_result` (invariant enforced).

**#668** (impure loader + pure pass): new `_SERVICE_FITTING_LAYER_TOKENS =
("pipe fitting","pipe accessor")`; `load_service_fitting_bands` (verbatim mirror of the fill
loader, service-coloured HATCH only — `colour_key is None` skipped). Pure: optional
`fitting_bands` param; after #667 propagation, a still-shared segment whose endpoint is
inside exactly ONE fitting-colour union inherits it; ≥2 colours / disagreeing endpoints →
stays shared. Route loads + passes fitting bands (DWG-only).

**#669** (pure-only): knob `tee_tol_m=0.030`. Pre-split pass: for each node not terminating
an edge, perpendicular-project onto the edge; if strictly interior (`0<t<1`, excluding
within `snap_tol_m` of endpoints) and distance ≤ `tee_tol_m` → mark split. Splits sorted
`(t,x,y)`, deduped within `snap_tol_m`, sub-segments < `snap_tol_m` dropped; **collinear →
length-conserving**. Re-run #667 propagation over the split graph. Ambiguous stays shared.

## 4. Sequencing: #668 first, then #669 (confirmed)
#668 is lower-risk + self-contained (doesn't alter graph topology). #669 changes the graph
#668 operates on, so the composition order (split → propagate → fitting-bridge) is fixed
regardless of merge order; #668-first means #669 inserts its pre-split upstream of an
existing fitting pass. Technically independent (each works against bare #667); sequential is
cleaner. Parallel is possible but the second-to-merge must reconcile pass ordering.

## 5. Resolved decisions (D1–D5)
- **D1** `tee_tol_m` = 30 mm, named constant + overridable kwarg (single-building → tunable).
- **D2** #668 single-colour-fitting bridging only; equidistant/disagreeing stays shared.
- **D3** #669 split determinism: `(t,x,y)` sort, dedupe/​drop < `snap_tol_m`, frozen propagation.
- **D4** Honesty invariant: single `_build_result` chokepoint; #669 splits length-conserving;
  #668 only re-labels; `total_length_m`/count mirror the verdict pass. Tested every case.
- **D5** `load_service_fitting_bands` in `service_takeoff_loaders.py` (impure); new token constant.

## Open decisions (for user/orchestrator) — lead's recommendations
- **Sequencing** → #668 then #669.
- **OD-1** tee projection metric → perpendicular distance only, NO angle gate in v1 (defer
  until a real false-split appears).
- **OD-2** `centerline_segment_count` reports the INPUT count, not post-split (splits are an
  internal graph detail, not new measured segments).
- **OD-3** reuse `band_buffer_m=0.011` for fitting unions (flag for tuning if M-540003 over-bridges).

## 6. Real-data gate (M-540003 — runnable now)
Both PRs gate on rev `1cc72d6e-...`: hit `GET /v1/revisions/{id}/service-takeoff`, read
`fill_attribution.{shared_length_m, per_colour, total_length_m}` vs the #667-only baseline.
- **#668:** `shared_length_m` reduces (or honest plateau); two-colour-equidistant core stays
  shared (no honesty regression); total stays 244.0; invariant holds.
- **#669:** `shared_length_m` falls OR plateaus honestly (mid-span tees may be rare on this
  sheet — a plateau is a PASS); byte-identical across runs + input permutation; invariant holds.
- **Caveat:** M-540003 is the ONLY oracle → both knobs are PROVISIONAL until a 2nd services
  sheet with fittings/mid-span tees is ingested.

## 7. Scope fence vs #645 / #618 gap (b)
#618 had two gaps: **(a) connectivity** = this TRD (#668/#669), and **(b) double-line-pair /
no-centerline detection** = handled separately (#640/#641, centerline synthesis #681) —
OUT of scope here. **#645 (closed)** is the single-vs-double unpaired-FACTOR derive fix —
neither gap; unrelated; no coordination. #668/#669 touch only connectivity attribution of
the already-measured centerline, never how length is derived.

## 8. Risks
- Single-building calibration (M-540003 only) → keep tunable; label PROVISIONAL; reopen on a
  2nd sheet. · Conservative bridging under-reduces shared → acceptable (honest > confidently
  wrong; plateau = pass). · #669 split non-determinism → explicit sort + snap grid + frozen +
  permutation test. · Near-zero sub-segments → strict interior + drop < `snap_tol_m`. ·
  Honesty-invariant regression → single chokepoint + `_assert_invariant` everywhere. · Perf
  O(nodes×edges) → small sheets; spatial-index deferred.

## 9. Test strategy + decomposition
Each issue ships as ONE PR (loader + pass are co-dependent for #668; split + re-propagate are
cohesive for #669). Synthetic cases in `tests/test_interpretation_routed_connectivity.py`
(per the issues' acceptance: single-colour-fitting inherit, two-colour stays shared, empty
bands == #667 regression; T-junction connect, permutation-stability, split-on-endpoint guard,
length conservation, no-tees == #667). Plus the M-540003 real-data gate per PR. Full
`mypy app tests`. No new route/table → no 4-inventory ripple, no append-only checklist.
