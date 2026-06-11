# =============================================================================
# Draupnir — Development Makefile
# =============================================================================
# Usage:
#   make sync        — Sync uv environment with local extras
#   make lint        — Run ruff linter on app/ and tests/
#   make typecheck   — Run mypy on app/ and tests/
#
#   make test        — Run full pytest suite
#   make smoke       — Run fast smoke pytest lane
#   make integration — Run full DB-backed integration pytest lane
#   make profile-integration — Profile DB-backed integration lane timing
#   make ci-db-pr-fast — Run CI-equivalent PR DB fast gate (db_api only)
#   make ci-db-extended — Run CI-equivalent extended DB coverage (all DB lanes)
#   make integration-db-api — Run DB API integration pytest lane
#   make integration-db-worker — Run DB worker integration pytest lane
#   make integration-db-estimation-export — Run DB estimation/export integration pytest lane
#   make integration-db-lineage — Run DB lineage integration pytest lane
#   make integration-db-migration — Run DB migration integration pytest lane
#   make compose-smoke — Run opt-in Docker Compose smoke pytest lane
#   make format      — Run ruff formatter on app/ and tests/
#   make check       — Run lint + typecheck + test
#   make hooks       — Install pre-commit + adaptive pre-push hooks
#
# profile-integration overrides:
#   PROFILE_INTEGRATION_MARKER      — pytest -m expression (default: integration lane)
#   PROFILE_INTEGRATION_DURATIONS   — slow test count for --durations output
#   PROFILE_INTEGRATION_DURATIONS_MIN — minimum seconds for --durations-min
#   PROFILE_INTEGRATION_XDIST_WORKERS — optional local xdist worker count (default: serial)
#   PROFILE_INTEGRATION_PYTEST_ARGS — extra pytest args passed through to the runner
#   make up          — Start Docker Compose stack
#   make down        — Stop Docker Compose stack
#   make logs        — Follow Docker Compose logs
#   make ps          — Show Docker Compose service status
#   make shell-api   — Open shell in api container
#   make shell-worker— Open shell in worker container
#   make migrate     — Run Alembic migrations
# =============================================================================

UV_SYNC_ARGS = --extra db --extra jobs --extra ingestion --extra dev --extra test
PRE_PUSH_HOOK = .git/hooks/pre-push
PRE_PUSH_HOOK_VERSION = v1
PROFILE_INTEGRATION_MARKER ?= $(PYTEST_INTEGRATION_EXPRESSION)
PROFILE_INTEGRATION_DURATIONS ?= 50
PROFILE_INTEGRATION_DURATIONS_MIN ?= 0.2
PROFILE_INTEGRATION_XDIST_WORKERS ?=
PROFILE_INTEGRATION_PYTEST_ARGS ?=
PROFILE_INTEGRATION_XDIST_ARGS = $(if $(strip $(PROFILE_INTEGRATION_XDIST_WORKERS)),--xdist-workers $(PROFILE_INTEGRATION_XDIST_WORKERS),)
PYTEST_SMOKE_EXPRESSION = smoke and not integration and not compose_smoke
PYTEST_INTEGRATION_EXPRESSION = integration and not compose_smoke
PYTEST_DB_API_EXPRESSION = integration and db_api and not compose_smoke
PYTEST_DB_WORKER_EXPRESSION = integration and db_worker and not compose_smoke
PYTEST_DB_ESTIMATION_EXPORT_EXPRESSION = integration and db_estimation_export and not compose_smoke
PYTEST_DB_LINEAGE_EXPRESSION = integration and db_lineage and not compose_smoke
PYTEST_DB_MIGRATION_EXPRESSION = integration and db_migration and not compose_smoke
PYTEST_COMPOSE_SMOKE_EXPRESSION = compose_smoke
SMOKE_BASE_URL ?= http://localhost:8000

.PHONY: sync lint typecheck test smoke integration profile-integration ci-db-pr-fast ci-db-extended integration-db-api integration-db-worker integration-db-estimation-export integration-db-lineage integration-db-migration compose-smoke format check hooks up down logs ps shell-api shell-worker migrate

sync:
	uv sync $(UV_SYNC_ARGS)

lint:
	uv run ruff check app tests

typecheck:
	uv run mypy app tests

test:
	uv run pytest

smoke:
	uv run pytest -m "$(PYTEST_SMOKE_EXPRESSION)"

integration:
	uv run alembic upgrade head
	uv run pytest -m "$(PYTEST_INTEGRATION_EXPRESSION)"

profile-integration:
	uv run python scripts/profile_integration_lane.py --marker "$(PROFILE_INTEGRATION_MARKER)" --durations $(PROFILE_INTEGRATION_DURATIONS) --durations-min $(PROFILE_INTEGRATION_DURATIONS_MIN) $(PROFILE_INTEGRATION_XDIST_ARGS) $(PROFILE_INTEGRATION_PYTEST_ARGS)

ci-db-pr-fast:
	$(MAKE) profile-integration PROFILE_INTEGRATION_MARKER="$(PYTEST_DB_API_EXPRESSION)"

ci-db-extended:
	@status=0; \
	$(MAKE) profile-integration PROFILE_INTEGRATION_MARKER="$(PYTEST_DB_API_EXPRESSION)" || status=$$?; \
	$(MAKE) profile-integration PROFILE_INTEGRATION_MARKER="$(PYTEST_DB_WORKER_EXPRESSION)" || status=$$?; \
	$(MAKE) profile-integration PROFILE_INTEGRATION_MARKER="$(PYTEST_DB_ESTIMATION_EXPORT_EXPRESSION)" || status=$$?; \
	$(MAKE) profile-integration PROFILE_INTEGRATION_MARKER="$(PYTEST_DB_LINEAGE_EXPRESSION)" || status=$$?; \
	$(MAKE) profile-integration PROFILE_INTEGRATION_MARKER="$(PYTEST_DB_MIGRATION_EXPRESSION)" || status=$$?; \
	exit $$status

compose-smoke:
	COMPOSE_SMOKE=1 SMOKE_BASE_URL="$(SMOKE_BASE_URL)" uv run pytest -m "$(PYTEST_COMPOSE_SMOKE_EXPRESSION)"

format:
	uv run ruff format app tests

check: lint typecheck test

hooks:
	uv run pre-commit install
	@mkdir -p "$(dir $(PRE_PUSH_HOOK))"
	@printf '%s\n' '#!/bin/sh' '# draupnir pre-push ci-parity $(PRE_PUSH_HOOK_VERSION)' 'exec uv run python scripts/pre_push_check.py "$$@"' > "$(PRE_PUSH_HOOK)"
	@chmod +x "$(PRE_PUSH_HOOK)"

# ---------------------------------------------------------------------------
# Docker Compose targets
# ---------------------------------------------------------------------------
up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

shell-api:
	docker compose exec api bash

shell-worker:
	docker compose exec worker bash

migrate:
	docker compose exec api alembic upgrade head

integration-db-api:
	uv run alembic upgrade head
	uv run pytest -m "$(PYTEST_DB_API_EXPRESSION)"

integration-db-worker:
	uv run alembic upgrade head
	uv run pytest -m "$(PYTEST_DB_WORKER_EXPRESSION)"

integration-db-estimation-export:
	uv run alembic upgrade head
	uv run pytest -m "$(PYTEST_DB_ESTIMATION_EXPORT_EXPRESSION)"

integration-db-lineage:
	uv run alembic upgrade head
	uv run pytest -m "$(PYTEST_DB_LINEAGE_EXPRESSION)"

integration-db-migration:
	uv run alembic upgrade head
	uv run pytest -m "$(PYTEST_DB_MIGRATION_EXPRESSION)"
