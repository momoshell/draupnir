# Integration lane profiling notes

## Local usage

- Use a local PostgreSQL test database only: `DATABASE_URL="postgresql+asyncpg://postgres:postgres@localhost:5432/draupnir_test"`.
- Keep the existing `_test` guardrails intact when profiling locally: localhost-only Postgres, database name ending in `_test`, and no shared dev/prod database targets.
- Optional xdist mode is local-only and opt-in: set `PROFILE_INTEGRATION_XDIST_WORKERS=<count>` to pass `-n <count> --dist loadfile` through the profiling runner.
- Repro commands (local snapshots, not permanent benchmarks):
  - `DATABASE_URL="postgresql+asyncpg://postgres:postgres@localhost:5432/draupnir_test" make profile-integration PROFILE_INTEGRATION_MARKER="integration and db_api and not compose_smoke"`
  - `DATABASE_URL="postgresql+asyncpg://postgres:postgres@localhost:5432/draupnir_test" make profile-integration PROFILE_INTEGRATION_MARKER="integration and db_worker and not compose_smoke"`
- Repro commands with xdist enabled (local snapshots, not permanent benchmarks):
  - `DATABASE_URL="postgresql+asyncpg://postgres:postgres@localhost:5432/draupnir_test" make profile-integration PROFILE_INTEGRATION_MARKER="integration and db_api and not compose_smoke" PROFILE_INTEGRATION_XDIST_WORKERS=4`
  - `DATABASE_URL="postgresql+asyncpg://postgres:postgres@localhost:5432/draupnir_test" make profile-integration PROFILE_INTEGRATION_MARKER="integration and db_worker and not compose_smoke" PROFILE_INTEGRATION_XDIST_WORKERS=4`
- The profiler owns `uv run alembic upgrade head`, the collect-only preflight, and the full pytest pass; do not pre-run Alembic before `make profile-integration`.
- Per-worker DB safety constraints: keep the base `DATABASE_URL` local and `_test`-suffixed; worker databases are derived from that base URL and created/migrated per worker, so never point profiling at shared or non-test databases.
- Trade-off framing: xdist can reduce wall clock when tests parallelize cleanly, but can increase setup/load and contention. Treat any speedup as evidence-to-collect per lane (serial vs xdist snapshots on the same machine), not a guaranteed benchmark claim.

## CI behavior

- CI integration matrix lanes run against a fresh `postgres:18-alpine` service per lane.
- The profiled lane invokes `scripts/profile_integration_lane.py` directly with `DATABASE_URL` set on that step.
- CI does not run a separate pre-profile `uv run alembic upgrade head`; the profiler performs the single migration timing pass so local and CI behavior stay aligned and profiled lanes do not double-run migrations.

## Timing evidence

### Issue #297 baseline (`db_api`)

- Repro command (local snapshot, not a permanent benchmark): `DATABASE_URL="postgresql+asyncpg://postgres:postgres@localhost:5432/draupnir_test" make profile-integration PROFILE_INTEGRATION_MARKER="integration and db_api and not compose_smoke" PROFILE_INTEGRATION_DURATIONS=50 PROFILE_INTEGRATION_DURATIONS_MIN=0.2`
- Timing snapshot: collect-only preflight `1.718s`, full pytest run (includes normal collection) `73.203s`, combined wall clock `74.921s`; results `161 passed, 814 deselected, 8 warnings`.
- Top runtime contributors in the `db_api` lane: six export-create API cases at `6.10s`-`6.14s` each — idempotency different-payload rejection, `estimate_csv` persistence, `estimate_pdf` persistence, replay idempotent request, `revision_json` persistence, and `quantity_csv` persistence; file upload size/format setup phases at roughly `0.36s`-`0.42s`; one revision quantity→estimate flow call at `0.39s`; one revision materialization filter call at `0.24s`; one project update setup at `0.23s`.
- Follow-up recommendations: profile the export-create tests together first (likely shared fixture/setup or repeated DB/app startup cost), then trim file-upload fixture overhead, and finally review the quantity→estimate/materialization paths for avoidable setup duplication or unnecessary DB work.

### Issue #298 Phase 2 snapshots

- `db_api` timing snapshot: migration `0.322s`, collect-only `1.186s`, full pytest run `70.025s`, combined wall `71.533s`; result `161 passed, 815 deselected, 8 warnings`.
- `db_api` runtime concentration: full pytest runtime dominates over migration plus collect-only; slowest calls were six export-create API cases at `6.09s`-`6.12s` each, while the only visible setup at/above the reporting threshold was one project update setup at `0.20s`.
- `db_worker` timing snapshot: migration `0.316s`, collect-only `1.151s`, full pytest run `186.241s`, combined wall `187.708s`; result `178 passed, 798 deselected, 7 warnings`.
- `db_worker` runtime concentration: full pytest runtime dominates over migration plus collect-only; slowest calls were worker export storage/readback cases at `12.22s`-`12.26s` and multiple export worker calls at `6.14s`-`6.28s`, while the only visible setup at/above the reporting threshold was one quantity recovery setup at `0.24s`.

## Decision

- No DB lifecycle optimization was adopted.
- Why: Phase 2 showed migration plus collect-only staying under roughly `1.5s` in both `db_api` and `db_worker`, while the meaningful cost remains inside the full pytest run itself.
- Result: keep the current fresh-database flow for both local profiling and CI, avoid template DB or other lifecycle complexity, and use the retained hotspot evidence for later test-specific optimization work.
