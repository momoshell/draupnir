---
tier: 2
issue: 741
status: implemented (#741a) — real-data validated
author: architecture-lead
reviewer: dev-team:trd-reviewer (independent)
date: 2026-06-27
---

# TRD #741 — Canonical storage that survives full-floor sheets

> **Implementation corrections (real-data, post-review):**
> 1. **`blocks`, not `entities`, is the bulk.** Per-key profiling of P-520001: `blocks` =
>    **1112 MB** (1688 structured block definitions w/ child geometry), `entities` = 49 MB,
>    all else ~0. So Option A strips **both** bulk collections (`entities` AND `blocks`) —
>    both are materialized into rows (`revision_entities` / `revision_blocks.payload_json`)
>    and read post-ingest only from rows (exports use the rows, not the blob).
> 2. **No schema-version bump.** `canonical_entity_schema_version` is adapter-controlled (a
>    self-stamping adapter like libredwg keeps "0.1"), so it's an unreliable slim-vs-full
>    signal. The authoritative discriminator is the `metadata.entities_storage` /
>    `blocks_storage` breadcrumb. (§6's 0.2 bump was dropped.)
> 3. **Validated:** P-520001 (447 MB JSON / ~1.2 GB canonical) now ingests — persisted
>    `canonical_json` = **7,694 bytes** (was ~1.2 GB), with 18,403 entity rows + 1,688 block
>    rows materialized. The mutation-safety guarantee (shallow copy; `payload.canonical_json`
>    untouched) is proven by the materialized row counts (in-place mutation would yield 0).

## 1. Goal

Make full-floor-sheet ingestion persist successfully. The dense drainage sheet P-520001
(~200k entities, ~1.2 GB canonical payload) fails the `AdapterRunOutput` INSERT — asyncpg
"connection was closed in the middle of operation" is the server severing the protocol when
a single `JSON` column value exceeds Postgres' ~1 GB text limit. Fixing this unblocks the
drainage/water grounding chain (#645-drainage / #621 / #622 / #560 / #736).

## 2. Ground truth (verified in code)

- `adapter_run_outputs.canonical_json` is a Postgres `JSON` column
  (`app/models/adapter_run_output.py:111-115`) — text-backed, ~1 GB hard cap per value.
- The row is written in the finalization transaction (`app/jobs/finalizers.py:568-586`),
  then `session.flush()` (`:648`) is followed by `_persist_revision_materialization(...)`
  (`:649-656`) in the **same** transaction — blob INSERT and `revision_entities`
  materialization commit atomically.
- The bulk is `canonical_json["entities"]` (~200k × ~2 KB). Sibling keys (`layouts`,
  `layers`, `blocks`, `viewports`, `units`, `metadata`, `entity_counts`, `interpretation`,
  `census`, `pdf_scale`, schema-version fields) are all small.
- In-pipeline stages need entities **in memory** (this stays; we only change what is
  *persisted*): `finalization.py:98-102` runs `expand_block_instances` (reads
  `entities` `block_expansion.py:93`, writes back larger `:115`) then `tag_sheet_membership`
  (`sheet_membership.py:34-58`), then `build_validation_outcome` (`:106`).
  Materialization (`revision_materialization.py:508-598`) explodes the in-memory
  `payload.canonical_json["entities"]` into `revision_entities` rows and computes
  `counts_json` from row counts — independent of any persisted blob.
- `adapter_run_outputs` has **no** append-only / truncate guards → low-friction migration;
  does not trip the append-only test-harness machinery.
- `result_checksum_sha256` is computed over the full result envelope incl. `canonical_json`
  (`finalization.py:76-83`, called `:172`). It is **write-only provenance** — no
  `WHERE result_checksum_sha256 = …` dedup query exists; idempotency is enforced by
  `uq_adapter_run_outputs_source_job_id`. Changing the checksum's input has no functional
  blast radius.

## 3. Blast radius — who reads the stored blob's `entities`? (decisive)

Every reader of the `canonical_json` **column** (DB-loaded, post-ingest), classified:

| Reader | Keys read from the stored column | Reads `entities`? |
|---|---|---|
| `api/v1/revision_routes/scale.py:128-131` | `pdf_scale`, `units` | No |
| `api/v1/revision_routes/canonical.py:125,157` | `interpretation`, `census` | No |
| `interpretation/loaders.py:100-106` | `metadata.text_blocks` | No |
| `interpretation/service_takeoff_loaders.py:425-434` | `metadata.text_blocks` | No |
| `exports/revised_dxf.py:433` | `metadata.pdf_scale` | No |
| `exports/revision_json.py` | reads `RevisionEntity`/`RevisionLayer` rows, not blob | No |

Readers of `entities` that run on the **in-memory** payload during finalization (NOT a DB
reload) — unaffected by slimming the persisted blob: `build_validation_outcome` and its
checks (`validation/*`, invoked only from `finalization.py:106`), `report_lineage`
(`:150-154`, only `len(entities)` + `entity_counts`), `block_expansion`, `sheet_membership`,
`debug_overlay`, adapter emit paths.

**Conclusion: no post-ingest consumer reads `entities` from the stored column.** The
materialized `revision_entities` rows are already the sole post-ingest source. Persisting
`entities` in the blob is pure duplication.

## 4. Decision: Option A — slim the persisted blob (drop `entities`)

| | A. Slim | B. Compress (gzip→BYTEA) | C. Externalize (object store) |
|---|---|---|---|
| Fixes column limit | Yes (blob → few MB) | Yes (~80 MB) | Yes |
| Fixes ~3 GB-per-read RAM | **Yes** | No (read still inflates) | No |
| Post-ingest consumer churn | **Zero** (§3) | Zero | Low–moderate |
| New infra/lifecycle | None | None | Yes (object GC/orphans) |
| Eliminates duplication w/ rows | **Yes** | No | No |

A is the only option that *also* removes the latent ~3 GB-per-read RAM wall, has ~zero
consumer churn (§3), and removes dead duplication. B/C move the symptom without fixing the
structure and C adds storage surface we don't need.

Open decisions within A:
- **O1 — drop vs `[]`:** persist `"entities": []` (stable contract shape for naive access)
  plus `metadata.entities_storage = {"location": "revision_entities", "count": N}`
  breadcrumb. (Recommended over key-absent.)
- **O2 — checksum:** keep hashing the **full pre-slim** envelope (value unchanged for new
  rows; full canonical is in memory at hash time → free). (Recommended.)

## 5. Migration & back-compat

- No DDL change strictly required (same `JSON` column, write less). Optional data-less
  Alembic migration updates only the column comment for discoverability. Existing rows
  untouched + readable.
- **No backfill** — old full blobs stay; their (small) sibling keys still serve §3 readers;
  their entities already duplicated into `revision_entities`.
- Read back-compat: every §3 reader `.get()`s the small key it needs and tolerates absence.
  Convention + guard test (§7) keep future code off blob `entities`.

## 6. Canonical schema-version bump

Bump `_CANONICAL_ENTITY_SCHEMA_VERSION` (`finalization.py:22`) `"0.1"` → **`"0.2"`**; new
slim blobs stamp `0.2`, old full blobs keep `0.1` (the discriminator: 0.2 ⇒ entities in
`revision_entities`, not the blob). Per-domain validation/sheet-membership sub-schema
versions are unaffected (they describe in-memory computation).

## 7. Test strategy

- Unit: persisted `canonical_json` has no entities (`[]` per O1) while carrying
  `layouts/layers/blocks/metadata/...`; version stamped `0.2`; in-memory entities still flow
  to materialization (assert `revision_entities` count unchanged).
- Materialization invariance: entity rows + `counts_json` byte-identical before/after
  (materializer reads the in-memory payload).
- Consumer non-regression: scale / interpretation / census / text_blocks / revised_dxf /
  revision_json export all resolve from slim blob + rows.
- Back-compat: a synthetic old full blob (entities present, `0.1`) still reads via every §3
  path.
- Guard test (anti-regression): no post-ingest code reads
  `AdapterRunOutput.canonical_json["entities"]`.
- Persistence-size bound: serialized `canonical_json` for a 200k-entity fixture < 50 MB.
- **Real-data gate (acceptance):** re-ingest P-520001 with `LIBREDWG_MAX_OUTPUT_MB=700
  ADAPTER_TIMEOUT_SECONDS=600` worker `--concurrency=1`: (1) `AdapterRunOutput` persists,
  (2) `revision_entities` materializes with expected count, (3) validation report + overlay
  produced. Manual/integration; run by the orchestrator.

## 8. Risks

- R1 (low): future code reads blob `entities` → gets `[]`. Mitigation: O1 breadcrumb + guard
  test + convention.
- R2 (low): checksum semantics across 0.1/0.2. Mitigation: O2(a) keeps full-envelope hash.
- R3 (low): fix addresses the *column* limit; `expand_block_instances` still inflates
  entities in memory and the worker holds ~3 GB during finalization. A removes the
  *persistence* + *post-ingest-read* RAM cost, NOT the in-pipeline peak. Real-data gate runs
  `--concurrency=1` + raised limits to fit that peak. Streaming/chunked materialization to
  cut the peak is **deferred** (#741e).
- R4 (very low): mypy/contract ripple. Payload field types unchanged (`dict[str, Any]`),
  only contents shrink. Run full `mypy app tests`.

## 9. Acceptance criteria

1. P-520001 ingests end-to-end (blob persists, rows materialize, validation+overlay).
2. No post-ingest consumer regresses.
3. Stored `canonical_json` for a full-floor sheet orders of magnitude under 1 GB.
4. Old full blobs remain readable.
5. Schema version bumped to `0.2`; convention + guard test landed.
6. Full `mypy app tests` + CI integration matrix green.

## 10. Decomposition (small, decoupled PRs)

- **#741a — Slim the persisted blob (core).** In `finalization.py`: after expand+tag+validate
  on the full in-memory canonical, build a *persisted* canonical that strips `entities` (→
  `[]`) + adds `metadata.entities_storage`. Bump schema version → `0.2`. Keep
  `compute_adapter_result_checksum` hashing the full pre-slim envelope. **Ordering hazard:**
  materialize from the full in-memory payload, persist a slimmed copy — confirm in
  `finalizers.py:649` the materializer still sees full entities (materialize from pre-strip
  dict OR strip on a copy for persistence). Unit tests: shape, checksum continuity,
  materialization invariance.
- **#741b — Column comment migration + schema-version doc.** Data-less Alembic migration
  (column comment only); document 0.1(full)/0.2(slim).
- **#741c — Back-compat + guard tests.** Old-full-blob read test across §3 consumers;
  anti-regression guard forbidding post-ingest `canonical_json["entities"]` reads;
  persistence-size bound test.
- **#741d — Real-data validation gate (manual/integration).** Re-ingest P-520001 per §7;
  attach evidence. Closes #741.
- **#741e (deferred — separate, NOT now):** cut the in-pipeline finalization RAM peak
  (streaming/chunked block-expansion + materialization) so dense sheets don't need raised
  limits / `--concurrency=1`. Flagged by R3.
