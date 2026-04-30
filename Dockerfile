# =============================================================================
# Draupnir — Dockerfile
# =============================================================================
# Multi-stage build for lean production image with development mode support.
# =============================================================================

FROM python:3.12-slim

# ---------------------------------------------------------------------------
# System dependencies
# ---------------------------------------------------------------------------
# Install curl for healthchecks and build tools for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Working directory
# ---------------------------------------------------------------------------
WORKDIR /app

# ---------------------------------------------------------------------------
# Install Python dependencies
# ---------------------------------------------------------------------------
# Copy dependency files first for better layer caching
COPY pyproject.toml uv.lock* README.md ./

# Install the package with db and jobs extras
# Using pip with --no-cache-dir to keep image lean
RUN pip install --no-cache-dir -e ".[db,jobs]"

# ---------------------------------------------------------------------------
# Copy application code
# ---------------------------------------------------------------------------
COPY app/ ./app/

# ---------------------------------------------------------------------------
# Create non-root user for security
# ---------------------------------------------------------------------------
RUN useradd --create-home --shell /bin/bash appuser && \
    chown -R appuser:appuser /app

USER appuser

# ---------------------------------------------------------------------------
# Default command (overridden in docker-compose.yml)
# ---------------------------------------------------------------------------
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
