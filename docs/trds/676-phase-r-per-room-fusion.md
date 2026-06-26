---
tier: 3
epic: 676
phase: R
status: approved-with-changes
author: architecture-lead
reviewer: dev-team:trd-reviewer (independent) + dev-team:architect (D1 + cable-attribution)
date: 2026-06-26
relates: ADR-010 (parent), ADR-005/006/008/009, ADR cross-revision-coordinator
supersedes: none
---

> **Review outcome (2026-06-26):** independent trd-reviewer + architect both returned
> approve-with-changes. The three decisions (D1 hybrid, D3 compute-on-read, D4 confirmed)
> hold. Required changes folded into this revision: (a) the floor route is mounted as a
> **prefix-less sub-router inside `revisions_router`** so the router-contract test covers
> it (§5/§6); (b) cable `in_plan_length_m` is reported at **circuit/floor level, NOT
> split per-room** (§4.4) — only `device_drop` and `home_run` localize to rooms; (c)
> Voronoi is a **per-room opt-in fallback, never a global re-tessellation**, with a hard
> on/off byte-identity invariant for polygon rooms (§4.1); (d) explicit ESTIMATED
> per-circuit conservation assertion (§4.4/§9); (e) R-extract symbol list + byte-stability
> gating; (f) new R-0 data-prep phase + schema folded into R-C (§10).

# Phase R — Per-Room Multi-Discipline Quantity Fusion

## 1. Problem

EPIC #676 (ADR-010) defines a legend-driven, multi-discipline, per-room quantity
engine. Three of its branches are shipped and live:

- **MEASURED** (routed length) — `GET /v1/revisions/{id}/service-takeoff`, already
  per-room (LP2 clip: `_resolve_rooms` + `_partition_polylines_by_room`).
- **COUNTED** (device symbols) — `GET /v1/revisions/{id}/devices`, with a per-type
  schedule and per-device room assignment via the rooms pipeline.
- **ESTIMATED** (cable) — `GET /v1/revisions/{id}/cable-estimate?containment_revision_id=...`,
  compute-on-read, fail-loud, validated on the Welbeck plantroom (342.2 m). This is
  per-circuit, NOT per-room.

Each of these is **single-revision** (one sheet). A real floor is split across N
discipline sheets at different model origins (E-610003 containment, E-620003 power,
E-630003 lighting, M-540003 med-gas — all Level-0 plantroom). Phase R is **fusion**:
take all discipline revisions of one floor, register them on the structural grid
(`Z030G`), bucket every quantity into rooms, and emit a single **per-room
multi-discipline quantity report** that preserves the three quantity-kinds distinctly.

The grid-registration primitive (`app/interpretation/grid_registration.py`) and the
two-revision-coordinator pattern (`cable_home_run_loaders.py`) are the foundation;
Phase R generalizes the coordinator from a 2-revision pair to a floor→N-revision group
and adds a room-bucketing + cross-discipline assembly layer on top.

The blocking design decisions D1 (room boundaries), D3 (fusion state), D4 (keep
`tag_stack_service` pipe-specific) are resolved here.

## 2. Goals / Non-Goals

### Goals
- A **floor coordinator** that takes a reference revision + a list of
  `(revision_id, role)` registered into it (the N-revision generalization of the
  cable coordinator), fail-closed on registration quality / units / project mismatch.
- A **room registry** for the floor: rooms derived once on the reference (room-bearing)
  sheet, with a recommended boundary strategy (D1) — every quantity from every
  discipline buckets into this single shared room set.
- A **fused per-room report**: for each room, the MEASURED / COUNTED / ESTIMATED
  quantities contributed by each discipline revision, each kind kept distinct and
  provenance-stamped, never summed across kinds.
- A new compute-on-read API endpoint exposing the fused report (D3 = compute-on-read,
  no persisted fused table in v1).
- Honest degradation: rooms that don't register, circuits/runs that don't bucket, and
  un-registerable sheets all surface as flagged absences, never fabricated.

### Non-Goals
- **No persisted fused table** in v1 (D3 rationale below). The append-only-table
  4-place checklist is therefore NOT triggered in v1; called out as the deferred path.
- **No auto-resolution of "which revisions are this floor"** — the caller supplies the
  revision list (mirrors the explicit `containment_revision_id` decision). A
  floor-registry / discipline-auto-discovery is deferred.
- **No rotation/scale in registration** — `GridTransform.apply()` stays
  translation-only; rotated sheets fail-closed (unchanged from cable home-run).
- **No change to the three single-revision producers' math.** Phase R consumes their
  outputs; pipe/cable/device byte-stability on the single-revision routes is preserved.
- **No new quantity kind, no cost-estimate feed.** ESTIMATED stays flagged and is
  NEVER folded into MEASURED nor fed into the frozen QuantityTakeoff/cost contract
  (ADR-005/004 confident-wrong honesty).
- **No second-building generalization.** Single-building evidence; see §8.

## 3. Ground-truth / Constraints (verified against current code)

- **No floor/level field exists.** `app/models/drawing_revision.py` carries only
  `project_id`, `source_file_id`, `revision_sequence` — no `floor`/`level`/`discipline`.
  Floor identity is therefore *the matched-fiducial set + residual*, exactly as the
  cable coordinator already treats it (`cable_home_run_loaders.py` comment:
  "no same-floor field exists … the matched-fiducial set + residual IS the
  floor-identity check"). Phase R cannot query "give me the floor's sheets"; the
  caller passes them.
- **Grid registration is a pure, reusable, translation-only rigid transform**
  (`grid_registration.py`): median per-pair delta, residual-gated good/degraded/failed,
  ≥3 fiducials for "good". `apply()` ignores rotation+scale by design. Validated
  exact on E-610003↔E-630003 (dx 30.2 m, residual 0, quality good).
- **The cross-revision coordinator pattern is established and ADR-endorsed**
  (`cable_home_run_loaders.py`): assert same `project_id`, register the secondary INTO
  the reference frame, fail-closed on quality≠good / rotation>tol / units mismatch,
  echo a `registration_audit`. Phase R copies this; v1 of the cable route uses exactly
  ONE registered revision — Phase R is the first true N>1 use.
- **Room derivation is mature and reused.** `room_pipeline.interpret_rooms` is a pure
  descending-authority coordinator: IFC space → explicit room/space layer → wall
  polygonize (`wall_rooms.py`, shapely.polygonize with gap-close
  `DEFAULT_WALL_SNAP_TOLERANCE=0.1`, sliver-drop `DEFAULT_WALL_MIN_AREA=1.0`,
  auto wall-layer selection by polygonize yield), enriched/supplemented by label rooms
  (room number `0.9.0x`). `_resolve_rooms` (in `rooms.py`) is the DB seam that
  service-takeoff already reuses. Label-only rooms (number + point, no polygon) are a
  first-class output.
- **MEASURED is already per-room.** `service_takeoff.py` calls `_resolve_rooms` then
  `_partition_polylines_by_room` (LP2: smallest-room-first clip, subtract-as-you-go,
  outside→unassigned, centroid fallback). The per-room MEASURED contribution exists
  today on the single sheet.
- **COUNTED is already per-room-capable.** `rooms.py` `_room_labels` + device
  placements yield `DeviceRoomAssignment`; the devices route emits a per-type schedule.
- **ESTIMATED is per-circuit, NOT per-room.** `cable_estimate.py` returns
  `per_circuit`; circuits span rooms by design (the network is cross-room). Bucketing
  cable to rooms is genuinely new work and is the hardest fusion step (§4.4).
- **Adding any route ripples into FOUR pinned inventories** (verified):
  1. `tests/test_revision_router_contract.py` — `(METHOD, path, handler, operation, response_model, …)` tuple baseline.
  2. `app/main.py` `_OPENAPI_TAGS` — documented tag (e.g. a new `"Floor Takeoff"` tag).
  3. `tests/test_openapi_contract.py` — `test_operation_ids_are_clean_and_unique` + the expected operation-id set.
  4. `tests/test_mcp_read_surface.py` — `_EXPECTED_TOOLS` curated read surface (and `_EXPECTED_RESOURCE_TEMPLATES`).
  PR CI now runs the full matrix (#691) and catches all four.
- **Three quantity-kinds stay distinct.** ESTIMATED is flagged + parameter-stamped
  (`quantity_kind="estimated"`, `reliability` map, `params_stamp` in
  `cable_estimate.py`). The fused report MUST keep MEASURED / COUNTED / ESTIMATED in
  separate sub-structures per room; never a single fused total.
- **Compute-on-read is the established takeoff/estimate pattern** (ADR-005); all three
  producers are compute-on-read.
- **Single building.** Welbeck Oxford only. All thresholds are provisional.

## 4. Architecture

### 4.0 Shape: a floor coordinator + a room registry + a fusion assembler

```
        GET /v1/floors/takeoff?reference_revision_id=R&member=R1:lighting&member=R2:containment&...
                                   │
                 ┌─────────────────┴───────────────────┐
                 │   floor_takeoff route (compute-on-read, fail-loud)   │
                 └─────────────────┬───────────────────┘
                                   │
          ┌────────────────────────┼─────────────────────────┐
          ▼                        ▼                          ▼
  floor_registration       room_registry                per-discipline producers
  (N-rev coordinator)      (rooms on reference)          (existing single-rev seams)
  grid_registration ×N  →  interpret_rooms once       →  service-takeoff / devices /
  GridTransform per member room set + GridTransform        cable-estimate, each loaded
  registered into reference   each member registered IN     in its own frame, then
  frame; fail-closed gates    the reference frame            geometry transformed into
                                                             the reference frame
                                   │
                                   ▼
                          room_fusion assembler (pure)
              bucket each discipline's MEASURED length / COUNTED device /
              ESTIMATED cable into the shared room set; keep 3 kinds distinct;
              stamp per-quantity provenance (member revision + discipline + kind)
                                   │
                                   ▼
                        FloorTakeoffResponse (per-room blocks)
```

Two new pure modules + two new DB-seam loaders + one route, mirroring the cable split:

- `app/interpretation/floor_registration.py` (pure) — generalize the
  GridTransform-per-member machinery; **N members**, each with a `role`/discipline.
- `app/interpretation/floor_registration_loaders.py` (DB seam) — load fiducials for
  the reference + each member, compute a `GridTransform` per member, apply the
  fail-closed gates (project match, quality, rotation, units), return a
  `FloorRegistration` (reference frame + per-member transform + per-member audit).
- `app/interpretation/room_fusion.py` (pure) — given the shared room set (in the
  reference frame) + each member's per-room quantity contributions (already transformed
  into the reference frame), emit the fused per-room report. No DB, no shapely beyond
  the existing `point_in_polygon` / clip primitives reused from the takeoff path.
- `app/interpretation/room_fusion_loaders.py` (DB seam) — orchestrate: resolve rooms on
  the reference, call each member's existing loader, transform member geometry/points
  into the reference frame via the member's `GridTransform`, hand pure inputs to
  `room_fusion`.
- `app/api/v1/revision_routes/floor_takeoff.py` (route) — compute-on-read, fail-loud.

**Reference frame = the room-bearing sheet.** Per the cross-revision ADR refinement
("for per-room: reference = the room-bearing sheet"). Rooms are derived ONCE on the
reference; every member's geometry is translated INTO the reference frame before
bucketing, so there is exactly one room set and one coordinate frame. This minimizes
transform surface (translation-only `apply()`).

### 4.1 D1 — Room boundaries: wall-inference vs Voronoi vs hybrid → RECOMMEND HYBRID

The sheets have NO room/space polygons — only architectural wall LINES and room-tag
POINTS (number `0.9.0x`). We must turn those into the per-room aggregation buckets.

**Option A — Wall-inference (shapely.polygonize from wall linework).**
Already implemented and battle-tested: `wall_rooms.py` polygonizes wall linework with
gap-closing (snap 0.1 m) and sliver-dropping (1.0 m²), auto-selects the wall/architecture
layer by polygonize yield (handles CAD-standard layer names, not just `*wall*`),
excludes dashed grid/match lines, and tags each face with a confidence caveat
(0.8 clean / 0.6 gap-closed). `room_pipeline` already enriches faces with room
number/name from the labels and flags cross-wall duplicate numbers for review.
- *Optimizes:* accuracy — boundaries follow the real walls, so a length/device near a
  wall lands in the correct room. Highest authority output; already integrated with the
  label/number join.
- *Sacrifices:* coverage. Real plantroom linework often fails to close (door openings,
  equipment plinths, hatch artefacts). On M-540003 rooms came back all-unassigned (#662):
  detection coverage was the blocker, not the clip. A pure-walls strategy can leave a
  floor with too few buckets, dumping most quantity into `unassigned`.

**Option B — Tag-proximity Voronoi partition.**
Tessellate the plane around the room-tag POINTS (each point owns its nearest region).
- *Optimizes:* coverage/robustness — ALWAYS produces a partition covering the whole
  sheet, one cell per tagged room, regardless of wall quality. Every quantity gets a
  home; no all-unassigned failure mode. Cheap, deterministic.
- *Sacrifices:* accuracy at walls — a Voronoi edge is the perpendicular bisector
  between two tags, which rarely coincides with the real wall. Quantities near a shared
  wall get mis-attributed; thin/long rooms and rooms with off-centre tags are skewed.
  No new building-model fidelity — it's a reporting convenience, not a true boundary.

**Option C — HYBRID (RECOMMENDED): wall-inference as primary authority; Voronoi as a
fallback partition for the residual, both keyed on the room-NUMBER join.**

The room NUMBER (`0.9.0x`, the same numbers tagged on every discipline sheet) is the
real cross-sheet join key — not the geometry. So:
1. Derive rooms on the reference via the existing `interpret_rooms` (`strategy="auto"`):
   IFC → explicit → wall-polygonize, already enriched by labels → polygon rooms carry
   number+name; un-polygonized labels become label-only rooms (number + point, no
   boundary).
2. **Bucket geometry/points by the polygon rooms first** (the accurate path — the
   existing `_partition_polylines_by_room` smallest-first clip for MEASURED;
   point-in-smallest-polygon for COUNTED/ESTIMATED anchors).
3. **For label-only rooms (no polygon) and for any geometry that fell outside all
   polygons, run a Voronoi partition over the room-tag points** to assign the residual
   to its nearest numbered room — flagged `boundary_basis="voronoi"` /
   `confidence` lowered, never silently merged with wall-accurate buckets.
4. Anything still unattributable (outside the floor extent, no nearby tag) stays an
   honest `unassigned` bucket.

*Why hybrid:* it keeps wall-inference's accuracy where the walls close (free — already
shipped) and gains Voronoi's total-coverage guarantee where they don't, WITHOUT letting
the imprecise method silently overwrite the precise one. The room NUMBER join means a
label-only Voronoi room on one sheet still fuses with a wall-polygon room of the same
number on another sheet — the report is keyed on number, the geometry is per-sheet
evidence. Each per-room quantity records `boundary_basis ∈ {polygon, voronoi}` +
`confidence` so consumers can see which rooms are wall-accurate. This is the only option
that is simultaneously accurate AND never all-unassigned, and it reuses 100% of the
shipped room machinery (the Voronoi fallback is the one genuinely new primitive, and it
is small + pure + deterministic).

*Build-now vs defer:* build wall-primary + label-only-room handling now (mostly wiring
existing code). The Voronoi fallback is a small new pure primitive — build it, but
gate the report so that with Voronoi disabled the engine still works (wall-only) for a
clean A/B during validation. Geodesic/wall-aware boundary refinement is deferred.

**Hard invariants on the Voronoi fallback (required by architect review — Voronoi's
failure mode is global+silent, unlike wall-inference's local+loud).** A missing/mis-OCR'd
room tag re-draws the bisectors of every neighbouring cell, bleeding quantity into the
wrong *named* room with plausible numbers — a confident-wrong risk this codebase fights
(ADR-005). Therefore:
1. **Per-room opt-in, never global re-tessellation.** Voronoi assigns ONLY label-only
   rooms (no polygon) and geometry that fell outside all polygons. A polygon room's
   bucket MUST be byte-identical whether Voronoi is on or off — enforced as a
   `room_fusion` unit test, not just A/B validation.
2. **Bounded partition.** Clip Voronoi cells to the floor extent (convex hull of the
   tag set, with a margin); far-field residual stays `unassigned` rather than being
   captured by the nearest tag from across the building.
3. **Residual-into-confidence.** A Voronoi assignment compounds two error sources
   (bisector-vs-wall AND the member's registration residual). Fold the member's
   `registration_audit` residual into the per-room `confidence` for Voronoi-assigned
   quantities so the report is honest about the lower trust.
4. **Nested-room caveat.** Voronoi has no nesting concept; a small room inside a hall
   (e.g. a control room inside a plantroom) is clipped correctly by the polygon path but
   split arbitrarily by Voronoi. Polygon-vs-Voronoi rooms are not merely "lower
   confidence" — flag nested label-only rooms for review rather than trusting the split.

### 4.2 D3 — Fusion state: compute-on-read vs persisted fused table → COMPUTE-ON-READ

**Recommendation: compute-on-read over a caller-supplied floor→revisions grouping. No
persisted fused table in v1.** (Matches the epic recommendation and ADR-005.)

Data-model implication, spelled out:
- Today's read seam is single-revision (`/revisions/{id}/...`). Phase R needs a
  **floor-grouping concept**, but there is **no floor field** in the schema (verified
  §3). The cable coordinator already solved the 2-rev version with *explicit pairing +
  matched-fiducials-as-floor-identity*. Phase R generalizes that to **explicit
  N-membership**: the caller passes a reference revision + a list of member
  `(revision_id, role)`. The "floor" is defined operationally by "these revisions
  mutually register on `Z030G` with good quality" — exactly the cable model, scaled up.
- This means the new route is **floor-scoped, not revision-scoped** in its semantics
  even though every input is a revision id. Recommended path shape:
  `GET /v1/floors/takeoff?reference_revision_id=...&member=<id>:<role>&member=...`
  (the reference is itself a member with role = its discipline).
- **No new persisted table** → the append-only project-scoped table checklist
  (conftest registration + append-only coverage test + pre_push_check) is NOT triggered.
  The fused report is recomputed each read from the already-persisted single-revision
  artifacts (entities, `revision_routed_lengths`, etc.). This is consistent with all
  three producers being compute-on-read and with ADR-005 (don't persist
  not-yet-estimate-grade fused quantities into the frozen contract).

*Why not the persisted fused table:*
- It would require inventing a `floor` entity (or a fusion-membership table) — net-new
  schema for a single-building, still-provisional capability. Premature.
- It triggers the full append-only 4-place CI checklist + migration + lineage FKs for
  data we recompute cheaply.
- Fusion inputs change whenever any member revision is reprocessed or rooms are
  re-derived; a persisted fusion would need cache-invalidation keyed on N member
  algo-versions (the ADR-008 two-version-surface problem, multiplied by N). Compute-on-read
  sidesteps all of it.

*When to revisit (deferred):* persist a fused snapshot only once (a) the report is
estimate-grade, (b) a 2nd building proves the model generalizes, and (c) there's a
consumer needing a stable historical fused artifact. At that point introduce the
append-only fused table + the floor-registry (auto-discovery of a floor's revisions),
and follow the append-only checklist.

### 4.3 D4 — Keep `tag_stack_service` pipe-specific → CONFIRMED YES

`tag_stack_service.py` (the ordered "FROM TOP TO BOTTOM" stack → parallel-pipe map) is
single-sample calibrated on M-540003 med-gas and is a pipe-specific identity strategy.
**Confirm the epic's recommendation: keep it pipe-specific.** In Phase R it is consumed
only indirectly — the MEASURED contribution comes from the already-shipped
`service-takeoff` output, which already incorporates tag-stack attribution for pipes.
The fusion layer treats each discipline's identity resolution as a black box (one
pluggable strategy per ADR-010's CategoryCatalog framing); Phase R does NOT generalize
tag-stack to other disciplines. Containment uses nearest-label (`segment_label_takeoff`),
not tag-stack; that distinction is preserved.

### 4.4 Per-discipline contributions into the fused report

The fusion assembler buckets three kinds, keeping each distinct:

- **MEASURED** (per member that produces routed length, e.g. containment/pipes):
  reuse the shipped per-room machinery. The member's centerline polylines
  (`revision_routed_lengths` via `polylines_from_geometry_json`) are **transformed into
  the reference frame** via the member's `GridTransform`, then clipped against the
  shared room polygons with the existing smallest-room-first
  `_partition_polylines_by_room` logic (factored to be callable on transformed
  geometry + an external room set). Per-room output: length per (service, size, room),
  `boundary_basis`, conservation invariant Σ(per-room) + unassigned == total (DU,
  tolerance), per ADR-008/LP2. Scale-gated (ADR-004): drawing-units fallback when scale
  unconfirmed.

- **COUNTED** (per member with devices, e.g. lighting/power): reuse `enumerate_devices`
  + `classify_instance_kind` (keep `KIND_DEVICE`). Each device's footprint anchor is
  transformed into the reference frame, then assigned to the smallest containing room
  polygon (or nearest label-room via the Voronoi fallback). Per-room output: device
  count per type (the existing per-type schedule), per room. Architecture/annotation
  kinds excluded from the count denominator (ADR-002).

- **ESTIMATED** (the cable member, lighting + its containment): this is the genuinely
  new bucketing. The cable estimate is per-circuit and circuits span rooms by design.
  **Only the two physically-localizable terms go per-room; the cross-room term stays at
  circuit/floor level (architect review).** The signal that these are the localizable
  terms: they are exactly the two that attach to a physical point (a device, a panel).
  - `device_drop_m` (reliable) → attribute each device's drop to the room containing
    that device (the COUNTED anchor already gives us the room). Clean, accurate, and
    **double-count-safe by construction**: `cable_estimate.py` counts each distinct
    device's drop exactly once (first-claim, conservation-asserted), and device→room is
    a function (one smallest-containing room), so Σ(per-room device_drop) + unassigned
    ≡ `total_device_drop_m`.
  - `home_run_m` (estimated_routed, along containment) → attribute to the room
    containing the circuit's anchor panel (the home run originates at the panel).
    Double-count-safe: one `home_run_m` per circuit → one anchor → one room. When
    no anchor (the **common** `no_anchor` / `unreachable_tray` / `disconnected_tray`
    fragment cases — first-class statuses in `cable_home_run.py`, not edge cases), it
    stays in an `unassigned`/`no_anchor` cable bucket — honest, not fabricated. §9
    sets an acceptable `no_anchor` fraction so "most cable unattributed" is a pass/fail
    bar, not a surprise relabelled as honesty.
  - `in_plan_length_m` (schematic/provisional) → **NOT split per-room.** This term is
    `schematic_provisional` precisely because device positions are notional and there is
    no through-wiring (E-630003), so the spline length is not real installed cable and
    has **no per-room meaning**. Splitting it per-room (clip-by-extent OR proportional
    split) would give room-level precision to a sheet-level guess — the highest
    false-precision risk in the design. Instead, report it as a **circuit-level,
    floor-scoped** line item in a sibling `estimated_circuits` block (§5), listing the
    room numbers the circuit touches as context. Per-room clip-by-extent is an explicit
    **deferred refinement**, gated on the in-plan term becoming physically real
    (through-wiring present); the proportional-split-per-room fallback is **dropped**.
  - Every ESTIMATED number carries `quantity_kind="estimated"` + `reliability` per term
    + the `params_stamp` (copied through from the cable assembly). **NEVER summed with
    MEASURED**; the per-room `estimated` block holds drop+home-run, the floor-level
    `estimated_circuits` block holds in-plan.
  - **ESTIMATED conservation (required assertion, §9):** for each circuit,
    Σ(per-room device_drop) + Σ(per-room home_run) + cable-unassigned, per term, ==
    the circuit's single-revision `cable-estimate` total per term (reusing the
    conservation pattern already in `cable_estimate.py`). Without this the
    "no fabrication" claim is unverifiable for ESTIMATED.
  - The registration_audit for the lighting↔containment pair flows through; a non-good
    pair → cable home-run suppressed per the existing fail-loud rule.

  *Build-now vs defer:* device-drop→room and home-run→anchor-room are clean and
  double-count-safe — build now. In-plan stays at circuit/floor level now; per-room
  clip-by-extent deferred until the term is physically real.

### 4.5 Identity-signal precedence (carried from ADR-010, unchanged)

colour → linetype → label (`WxH TYPE` / `Ø…mm SERVICE`) → hatch pattern-name. Phase R
does not re-resolve identity; it consumes each member producer's already-resolved
identities. This precedence is the producers' contract, restated here so the fused
report's per-quantity `basis`/`source` fields stay consistent across disciplines.

## 5. Data model + read-seam changes

- **No DB schema change in v1** (D3). No new table, no migration, no append-only
  checklist.
- **New read seam: floor-scoped, served at `/v1/floors/takeoff`.** The path is
  floor-scoped (not `/revisions/{id}/...`) to make the floor-grouping concept explicit.
  **Mounting (required by trd-reviewer): a prefix-less `floor_takeoff_router` (paths =
  `/floors/takeoff`) `include_router`'d INTO the existing `revisions_router`** (which is
  itself a bare prefix-less `APIRouter()` — the `/revisions/...` segment lives in each
  sub-router's own paths). This serves the route at `/v1/floors/takeoff` AND keeps it
  inside the router that `tests/test_revision_router_contract.py` iterates, so inventory
  #1 covers it for free. A new **top-level** `floors_router` registered separately in
  `main.py` is **rejected** — it would silently escape the contract baseline.
- **Refactor (additive, byte-stable) — R-extract scope is a cross-module MOVE, not a
  parameterization.** `_partition_polylines_by_room` (`service_takeoff.py:272`) already
  accepts a `rooms` arg; R-extract moves it (and the device-room assignment) to a shared
  module and generalizes it to accept **pre-transformed** geometry + an external room
  set. Constraints: (a) the producer-local unassigned-bucket id
  (`ROOM_UNASSIGNED_ID = "service-takeoff-unassigned"`) stays producer-local — the fused
  path uses its own unassigned key, the shared function must not bake in either; (b) the
  single-revision `service-takeoff`/`devices` route outputs stay **byte-stable**, gated
  by a regression test that is R-extract's own acceptance (not a later phase).
- **New schemas:** `FloorTakeoffResponse` with per-room blocks, each carrying
  `room_number`, `room_name`, `boundary_basis`, `confidence`, and three nested kind
  blocks (`measured`, `counted`, `estimated`), plus a floor-level `registration_audit`
  (per-member transform diagnostics) and a `members` echo.

### Per-room fused report shape (illustrative)

```
FloorTakeoffResponse:
  reference_revision_id: UUID
  members: [ {revision_id, role, registration: {dx,dy,rotation_rad,scale,
              matched_count, median_residual_m, quality}} , ... ]
  rooms:
    - room_number: "0.9.01"
      room_name: "PH Plantroom"
      boundary_basis: "polygon" | "voronoi"
      confidence: 0.8
      measured:                       # per (service,size); MEASURED kind, scale-gated
        - { discipline, service, size_raw, size_kind, real_length_m, drawing_length,
            run_count, basis, source_revision_id, length_provisional }
      counted:                        # COUNTED kind
        - { discipline, type_name, count, source_revision_id }
      estimated:                      # ESTIMATED kind (localizable terms only)
        quantity_kind: "estimated"
        device_drop_m: ...            # attributed to this device's room
        home_run_m: ... | null        # attributed to anchor-panel room
        reliability: { device_drop_m: "reliable", home_run_m: "estimated_routed" }
        params_stamp: { ... }
        source: { lighting_revision_id, containment_revision_id }
  estimated_circuits:                 # FLOOR-level cross-room term (in-plan), NOT per-room
    - { circuit_id, in_plan_length_m, reliability: "schematic_provisional",
        rooms_touched: ["0.9.01","0.9.03"], params_stamp: {...} }
  unassigned:                         # honest residual, same per-room structure
    measured: [...]
    counted: [...]
    estimated: { device_drop_m, home_run_m }   # e.g. no_anchor home-runs
  summary:
    rooms, named_rooms, members_registered, members_failed,
    voronoi_fallback_rooms, no_anchor_fraction, total_measured_m_by_discipline, ...
```

Key invariants: (1) the three kinds are NEVER summed into one number; (2) every quantity
carries `source_revision_id` + `discipline`; (3) MEASURED length stays scale-gated
(`length_provisional` / drawing-units fallback); (4) per-discipline conservation —
Σ(per-room MEASURED) + unassigned == single-revision total for that member.

## 6. API surface (with the 4-inventory ripple per endpoint)

**Endpoint 1 (v1, the deliverable):**
`GET /v1/floors/takeoff` — query: `reference_revision_id`, repeated `member=<id>:<role>`,
optional `scope`, `strategy`, `snap_tolerance`, `min_area`, `voronoi_fallback` (bool).
Response: `FloorTakeoffResponse`. Compute-on-read, fail-loud:
- 404 if any revision id is missing/not-active.
- 422 if members span >1 project, or any member registers with quality≠good /
  rotation>tol / units mismatch (copy the cable route's 422 messaging), or fewer than
  the reference + 1 member supplied.
- 200 with honest absences for rooms/quantities that don't bucket.

**4-inventory ripple for Endpoint 1 (all REQUIRED, PR CI enforces):**
1. `tests/test_revision_router_contract.py` — add the `(GET, "/floors/takeoff",
   get_floor_takeoff, get_floor_takeoff, "FloorTakeoffResponse", None)` tuple. Because
   the route is mounted as a prefix-less sub-router INTO `revisions_router` (§5), the
   existing test (which iterates `revisions_router.routes`) covers it; **R-G must add a
   test asserting the floor route appears in that iteration** so the mounting can't
   silently regress to a top-level router.
2. `app/main.py` `_OPENAPI_TAGS` — add `{"name": "Floor Takeoff", "description": "Fused
   per-room multi-discipline quantity takeoff across a floor's discipline sheets."}` and
   tag the sub-router with `tags=["Floor Takeoff"]`.
3. `tests/test_openapi_contract.py` — add the **actually-generated** operation-id (do
   NOT assume `get_floor_takeoff`; `generate_unique_id_function` builds it from route
   name + tag/path, and this is the first non-`/revisions/` route — R-G must verify the
   emitted id and use that exact string in the expected set).
4. `tests/test_mcp_read_surface.py` — add the floor read tool to `_EXPECTED_TOOLS`.
   **R-G must first verify the MCP generator classifies a GET with required query params
   as a tool (not a resource template)** for a `/floors/` path — every existing tool is
   `get_revision_*`/`list_revision_*`, so neither the name nor the classification can be
   assumed.

**No Endpoint 2 in v1.** A per-room detail/entities sub-route or a CSV/JSON export of
the fused report is a possible follow-on; if added later, it incurs the same 4-inventory
ripple. Keep v1 to the single fused-report endpoint.

## 7. Test strategy

- **Pure-module unit tests (no DB):**
  - `floor_registration.py` — N-member transform composition; fail-closed gates
    (project mismatch, quality≠good, rotation>tol, <2 members); deterministic over
    shuffled member order. Synthetic fiducial sets.
  - `room_fusion.py` — bucketing of synthetic MEASURED/COUNTED/ESTIMATED into a fixture
    room set; the three kinds stay distinct (assert no cross-kind sum); conservation
    invariant per discipline (Σ per-room + unassigned == total); `boundary_basis`
    stamping; Voronoi fallback assigns label-only-room residual to nearest tag and never
    overwrites a polygon bucket.
  - Voronoi primitive — deterministic partition, tie-break by room number; total coverage.
- **DB-seam tests:** `floor_registration_loaders` / `room_fusion_loaders` against the
  test DB with multi-revision fixtures in ONE project (the coordinator asserts same
  project_id — fixtures must co-locate members in one project, the recurring gate
  lesson).
- **Route contract tests:** the four pinned inventories above; 404/422 paths;
  honest-200 on degenerate input (no rooms → all `unassigned`).
- **Byte-stability regression:** the single-revision `service-takeoff` / `devices` /
  `cable-estimate` outputs are unchanged after the per-room-clip extraction refactor.
- **Real-data gate (mandatory, not optional — synthetic has repeatedly masked bugs):**
  the full Welbeck Level-0 floor — E-610003 (containment), E-620003 (power),
  E-630003 (lighting), M-540003 (med-gas) — **all re-ingested into ONE project** at the
  current algo versions (c4-rail-1). Validate: members register on `Z030G` (expect
  clean translations, residual ~0); rooms derive with sensible coverage; MEASURED
  per-room sums match each single-sheet total; cable device-drops land in device rooms;
  the report keeps kinds distinct. Watch for the known M-540003 all-unassigned room
  failure (#662) — it is the Voronoi-fallback's reason to exist; validate the fallback
  rescues it. Capture the dict-coord accessor (`_xy`) usage — the recurring crash class.
- **Import-boundary test:** the fusion modules stay cv2/skimage-free (ADR-008 read path
  purity); pure modules import no DB/ORM.

## 8. Risks

- **Single-building over-fit (HIGH).** Every threshold — grid label radius (0.5 m),
  registration residual bands, wall snap/sliver (0.1 m / 1.0 m²), Voronoi tie-breaks,
  tag-association radii — is calibrated on Welbeck only. The TRD mandates: surface all
  thresholds as parameters, mark every output's calibration `provisional`, and name the
  specific sheet in every real-data gate. Do NOT tune to make Welbeck perfect; a 2nd
  building is the real generalization test.
- **Room coverage on plantroom sheets (MEDIUM-HIGH).** #662 showed all-unassigned on
  M-540003. Wall-inference can under-cover; the Voronoi fallback mitigates but at lower
  accuracy. Mitigation: hybrid D1 + `boundary_basis` transparency so consumers see which
  rooms are wall-accurate; honest `unassigned` bucket; A/B with Voronoi off.
- **Cable→room attribution noise (MEDIUM).** In-plan splines are schematic ("positions
  notional"). Clipping them by room extent may add noise. Mitigation: device-drop→room
  (clean) is primary; in-plan defaults to proportional split with a
  `schematic_provisional` flag; clip-by-extent is the deferrable refinement.
- **Registration scaling to N members (MEDIUM).** Each member registers independently
  into the reference; a single bad member must NOT poison the others. Mitigation:
  per-member fail-closed gate; a failed member is dropped with a flagged
  `members_failed` entry, the rest still fuse. Mirrors the cable per-circuit honesty.
- **Rotated/mis-scaled future sheets (LOW now, LATENT).** `apply()` is translation-only;
  a rotated sheet would silently corrupt geometry if the rotation gate were removed.
  Mitigation: keep the explicit rotation + scale gates (copied from the cable
  coordinator); fail-closed.
- **Route placement churn (LOW).** Introducing the first `/floors/...` route touches
  router wiring + the 4 inventories. Mitigation: feasibility-consult backend lead before
  committing the path shape; keep it to one endpoint in v1.
- **Refactor regression (LOW).** Extracting the per-room clip risks changing
  single-revision output. Mitigation: byte-stability regression tests gate the extraction.

## 9. Acceptance criteria

1. `GET /v1/floors/takeoff` returns a `FloorTakeoffResponse` for the Welbeck Level-0
   floor (E-610003 + E-620003 + E-630003 [+ M-540003]) co-registered in ONE project,
   compute-on-read, no persistence.
2. The report is **per-room**, keyed on room number, with each room carrying distinct
   `measured` / `counted` / `estimated` blocks — the three kinds are NEVER summed.
3. Every member registers via `Z030G` fiducials with a per-member `registration_audit`;
   a member that fails to register is dropped + flagged (`members_failed`), the rest
   still fuse; the route 422s only when the *reference pairing* (lighting↔containment for
   cable) can't register.
4. D1 hybrid: rooms come from wall-inference (`interpret_rooms`), with a Voronoi
   fallback for label-only/residual rooms; each room reports `boundary_basis` +
   `confidence`; no all-unassigned outcome when room tags exist. **Voronoi on/off
   invariant: every polygon room's bucket is byte-identical with the Voronoi fallback
   enabled vs disabled** (unit-tested); Voronoi cells are clipped to the floor extent.
5. Per-discipline MEASURED conservation: Σ(per-room MEASURED) + unassigned == the
   member's single-revision `service-takeoff` total (DU, within tolerance).
   Single-revision route outputs are byte-stable (R-extract regression test).
6. ESTIMATED per-circuit conservation: for each circuit, Σ(per-room device_drop) +
   Σ(per-room home_run) + cable-unassigned, per term, == the circuit's single-revision
   `cable-estimate` total per term. Per-room cable carries `quantity_kind="estimated"` +
   `reliability` + `params_stamp`; device-drops land in their device's room; in-plan is
   reported at circuit/floor level only; never folded into MEASURED nor fed to the cost
   contract.
7. `no_anchor` home-run fraction is within an agreed bar (set during R-H on Welbeck
   Level-0) — "most cable unattributed" fails the gate rather than passing as "honest."
8. The 4 pinned inventories are updated (with the generated op-id + MCP classification
   verified, not assumed) and PR CI (full matrix) is green; the fusion modules pass the
   cv2/skimage + DB import-boundary tests.
9. All outputs documented as provisional / single-building-calibrated.

## 10. Phasing (small, decoupled PRs in dependency order)

R-0 (data prep) is the first phase — R-A's DB-seam test and R-H both need the four
Welbeck sheets co-ingested. R-A (floor registration), R-B (Voronoi primitive), and
R-extract (per-room clip extraction) are independent and parallelizable. R-C (which now
also defines the `FloorTakeoffResponse` skeleton) depends on R-B + R-extract; R-D/R-E/R-F
populate the schema sub-blocks independently; R-G adds the route + 4-inventory ripple;
R-H is the real-data gate.

- **R-0** — Data prep: co-ingest E-610003/E-620003/E-630003/M-540003 (Level-0) into ONE
  project at c4-rail-1; confirm each carries matched `Z030G` fiducials (registration was
  only validated on E-610003↔E-630003 — power/med-gas registration quality is assumed).
- **R-A** — `floor_registration` N-member coordinator (pure + DB seam); per-member
  fail-closed gate so one bad member is dropped+flagged, the rest still fuse.
- **R-B** — Voronoi room-partition primitive (pure): bounded to floor extent,
  deterministic, tie-break by room number.
- **R-extract** — Move per-room clip + device-room assignment to a shared module
  (pre-transformed geometry + external room set; producer-local unassigned id);
  byte-stability regression test is this PR's gating acceptance.
- **R-C** — `room_fusion` shared room registry (D1 hybrid) + `FloorTakeoffResponse`
  schema skeleton; Voronoi on/off byte-identity invariant test.
- **R-D** — MEASURED contribution into the fused report (+ MEASURED conservation).
- **R-E** — COUNTED contribution into the fused report.
- **R-F** — ESTIMATED per-room bucketing: device_drop→room, home_run→anchor-room,
  in-plan→floor-level `estimated_circuits`; ESTIMATED per-circuit conservation assertion.
- **R-G** — `/v1/floors/takeoff` route (prefix-less sub-router in `revisions_router`) +
  4-inventory ripple (verify generated op-id + MCP classification, add the
  route-mounting-coverage test).
- **R-H** — Real-data fusion gate on the Welbeck Level-0 floor; set the `no_anchor`
  fraction bar; validate the #662 Voronoi rescue.

## 11. Assumptions to verify before/during execution

- **Reference frame = which sheet?** §4.0 sets reference = the room-bearing sheet; §4.4
  cable uses the lighting↔containment registration. If the room-bearing sheet is neither
  lighting nor containment, there are two registration frames to reconcile. Confirm
  which Level-0 sheet bears the `0.9.0x` room tags and document it (R-0/R-A).
- **All four sheets carry `Z030G` fiducials** — only E-610003↔E-630003 is proven (R-0).
- **MCP generator** classifies a `/floors/` GET-with-query as a tool, and the emitted
  operation-id (R-G, inventory #3/#4).
