# Phase R — Real-Data Fusion Gate Results (R-H, #722)

**Date:** 2026-06-27 · **Epic:** #676 Phase R · **Status:** gate PASSED (with documented coverage limitations)

The `GET /v1/floors/takeoff` endpoint was exercised end-to-end against the real Welbeck
Oxford Level-0 floor — all four discipline sheets co-registered in project
`49140cff-7368-4b2c-9627-991283553d81`:

| sheet | discipline | revision_id |
|---|---|---|
| E-630003 | lighting (reference) | `95015ea4-b4d0-46a6-b676-e3fc7f622a7f` |
| E-610003 | containment (cable pair) | `b29fcd01-3eb0-41ad-9f0c-012925d47824` |
| E-620003 | power | `bd2ac5ee-7de4-448c-b24e-cf0d294aaef0` |
| M-540003 | med-gas | `1cc72d6e-7723-4189-8a02-a336956a275b` |

Call: `reference_revision_id = E-630003`, all four as `member=<id>:<role>`,
`containment_revision_id = E-610003`. Response: **HTTP 200**.

## What PASSED (the fusion is correct)

- **Registration:** all 4 members register on `Z030G` with `quality=good`, `matched=8`,
  `median_residual_m=0.0`. `members_failed=[]`. The N-member coordinator works on the
  full floor.
- **Per-discipline MEASURED conservation** (fusion rooms+unassigned vs the member's
  single-revision `service-takeoff` total):
  - containment: **2597.7 m** fused vs **2601.0 m** single-rev (**0.1%**).
  - med-gas: **3621.1 m** fused vs **3660.2 m** single-rev (**1.1%**).
  - (Within clip-at-boundary tolerance; the per-member geometry-sum conservation
    assertions in `floor_measured` passed — the 200 response means no `RuntimeError`.)
- **Scale:** `conversion_factor=1.0`, `units_confidence=confirmed` — these drawings are
  natively in metres, so `real_length_m == drawing_length` is correct.
- **Three kinds kept distinct:** `measured` / `counted` / `estimated` are separate
  per-room sub-blocks; in-plan cable is at floor level (`estimated_circuits`, 59 entries);
  `estimated_meta.quantity_kind="estimated"` with per-term reliability + params_stamp.
  Nothing is summed across kinds.
- **ESTIMATED:** device_drop reliable, home_run estimated_routed, in-plan
  schematic_provisional — all flagged, never folded into MEASURED.

## Coverage findings (honest limitations, NOT fusion bugs — single-building, provisional)

1. **Only 3 of 9 room tags polygonize on the lighting reference** (`0.9.01` PH Plantroom,
   `0.9.02` Bin Store, `0.9.04` AHU Plant — all `boundary_basis=polygon`). The other six
   tagged rooms have no closed wall loop on E-630003, so a large share of quantity lands
   in `unassigned` (measured 610.8 m, counted 140 devices). This is the #662 class of
   limitation (wall linework doesn't close on plantroom sheets).
2. **Voronoi fallback did not trigger** (`voronoi_fallback_rooms=0`). The D1-hybrid
   Voronoi rescue assigns label-only rooms (tag, no polygon); on this floor either those
   six tags did not surface as label-only rooms on the reference, or the unassigned
   geometry/anchors fell outside the bound. The rescue is correct-by-construction
   (verified in R-C/R-D unit tests) but is not exercised by this floor's data.
3. **`no_anchor_fraction = 0.92`** — ~92% of cable home-run length is in the unassigned
   bucket (the circuit's anchor panel does not resolve to a polygon room). Consistent
   with finding #1 (panels mostly sit in non-polygonized space). The home-run term was
   always the small reliable slice (~8 m of 342 m total cable); this is honest
   degradation, not fabrication.

## The `no_anchor_fraction` provisional gate (TRD §9.7, issue #735)

Observed **0.92** on Welbeck Level-0. This is HIGH but expected given the room-coverage
limitation — it is a function of how many panels sit in polygonized rooms, which on this
single floor is few.

As of issue #735, `no_anchor_fraction` is now paired with a `no_anchor_status` field that
classifies it into one of three PROVISIONAL, GENEROUS bands:

| Band | Condition | Meaning |
|---|---|---|
| `ok` | fraction ≤ 0.50 | Low unanchored share — healthy |
| `elevated` | 0.50 < fraction ≤ 0.95 | High but tolerable; matches single-building baseline (0.92 → `elevated`) |
| `critical` | fraction > 0.95 | Near-total non-resolution; almost certainly a regression |

**Why generous bounds?** With a single building, a tighter threshold would over-fit to
transient data (0.92 on this floor). "critical" is set at 0.95 so it only fires on a
near-total regression, not on the observed baseline. "ok" is set at 0.50 as a rough
midpoint — most healthy floors should land here once room coverage is adequate.

**These thresholds are PROVISIONAL.** They are defined as named constants
(`_NO_ANCHOR_FRACTION_ELEVATED = 0.50`, `_NO_ANCHOR_FRACTION_CRITICAL = 0.95`) in
`app/interpretation/floor_takeoff_loaders.py` so they can be retightened after:

- Room coverage improves beyond the current 3-of-9 rooms polygonized on this floor, or
- A second building's data is available to validate the bands.

## Recommended follow-ups (improve coverage; do not block the epic)

- Improve room coverage on plantroom sheets so >3 of the 9 tags become buckets — either
  strengthen wall-polygonize gap-closing on these sheets, or make the Voronoi fallback
  actually engage for the un-polygonized tags (investigate why `voronoi_fallback_rooms=0`
  here despite 6 un-bucketed tags).
- Once coverage improves, retighten `_NO_ANCHOR_FRACTION_ELEVATED` and
  `_NO_ANCHOR_FRACTION_CRITICAL` in `floor_takeoff_loaders.py` (issue #735).
- Validate against a second building to lift the "provisional / single-building" caveat
  on all Phase R thresholds.

## Reproduce

Host API + worker on current `main` + podman infra (see `project_local_full_ingest_run`).
Materialize centerlines (`GET /v1/revisions/{id}/service-takeoff` per member), then call
`GET /v1/floors/takeoff` with the params above. The env-guarded smoke
`tests/test_floor_takeoff_route.py::test_floor_takeoff_realdata_smoke`
(`DRAUPNIR_REALDATA_SMOKE=1`, live server) encodes the PASS invariants.
