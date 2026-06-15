---
tier: 2
round: 1
status: draft
---

## Proposed Approach

**Issue**: none — user-requested audit/plan, not currently tied to a GitHub issue.

**What**: Fix a small set of confirmed health/audit findings: one API error-envelope gap when the database is unconfigured, plus stale onboarding/status documentation that now contradicts the implemented backend. Keep this as a standard Tier 2 maintenance pass: no new dependencies, no architecture changes, no remote mutations.

**Files**:
- Source fix: `app/db/session.py`, likely `tests/test_db.py` or a focused API/dependency test covering the unconfigured DB path.
- Docs/config-comment fix: `README.md`, `AGENTS.md`, `Makefile`, `pyproject.toml`, `docs/ARCHITECTURE.md`, `docs/TRD.md`, `docs/local-dwg-pdf-review-workflow.md`, `services/pdf-intake/README.md`.

**Delegation**:
- `backend-specialist`: make DB-unconfigured dependency failure return the shared API envelope instead of a bare `RuntimeError`; add/adjust focused tests. Suggested low-scope approach is to raise a sanitized `HTTPException` from `get_db()` using `create_error_response(...)` and an appropriate 5xx status, unless the specialist finds a cleaner existing project pattern.
- `doc-writer`: align user/developer docs with current implementation: generated artifact metadata/listing vs explicit download route, current scaffolded repo layout/dev commands, RabbitMQ/async SQLAlchemy comments, current Phase 8/export/revised-DXF implementation status, vector PDF extra guidance, and optional PDF intake standalone-vs-repo workflow wording.

**Scouts**: 0 — direct discovery and read-only audits produced concrete file/line findings; no bounded ambiguity remains for planning.

**Validation**:
- `uv run ruff check app tests`
- `uv run mypy app tests`
- `uv run pytest -m "smoke and not integration and not compose_smoke"`
- Focused tests for the backend fix, likely `uv run pytest -q tests/test_db.py` plus any new/nearby API error-envelope test.
- Documentation re-read/checklist validation for the docs-only edits; no markdown linter is configured.

Approval only needed if scope expands beyond the files above, if new dependencies are proposed, if the error-code taxonomy changes require broader docs/tests, or if the docs update changes product scope rather than correcting stale status.
