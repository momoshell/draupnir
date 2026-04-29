# =============================================================================
# Draupnir — Development Makefile
# =============================================================================
# Usage:
#   make lint        — Run ruff linter on app/ and tests/
#   make typecheck   — Run mypy on app/ and tests/
#   make test        — Run pytest
#   make format      — Run ruff formatter on app/ and tests/
#   make check       — Run lint + typecheck + test
#   make hooks       — Install pre-commit hooks
# =============================================================================

.PHONY: lint typecheck test format check hooks

lint:
	ruff check app tests

typecheck:
	mypy app tests

test:
	pytest

format:
	ruff format app tests

check: lint typecheck test

hooks:
	pre-commit install
