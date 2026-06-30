# syntax=docker/dockerfile:1

# ===========================================================================
# Stage 1 — Build LibreDWG (provides the `dwgread` binary for the DWG adapter,
# ADR-0006). LibreDWG is GPL-3.0+ and is NOT packaged in Debian, so we compile
# from source for version parity with local dev. We track master 0.14 (#802):
# 0.14 parses R2004 files that 0.13.3 cannot (the 0xd40 error) and emits
# per-entity ACI colours that 0.13.3 collapsed to BYLAYER. There is no 0.14
# release tarball on the GNU FTP mirror, so we build from a pinned git commit
# (autotools-generated configure via autogen.sh).
# Built static (--disable-shared) so the runtime stage needs only dwgread plus
# its libc/libpcre2 deps, not the full LibreDWG shared library.
# ===========================================================================
FROM python:3.12-slim AS libredwg-builder

# Pinned to libredwg master commit ce90f755 == dwgread 0.14.8384 (host parity).
ARG LIBREDWG_GIT_URL=https://github.com/LibreDWG/libredwg.git
ARG LIBREDWG_GIT_REF=ce90f755

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    build-essential \
    pkg-config \
    libpcre2-dev \
    perl \
    autoconf \
    automake \
    libtool \
    texinfo \
    gperf \
    gettext \
    autopoint \
    && rm -rf /var/lib/apt/lists/*

RUN git clone "${LIBREDWG_GIT_URL}" /tmp/libredwg \
    && cd /tmp/libredwg \
    && git checkout "${LIBREDWG_GIT_REF}" \
    && git submodule update --init --recursive \
    && sh autogen.sh \
    && ./configure --disable-shared --enable-static --disable-bindings --disable-werror \
    && make -j2 \
    && make install \
    && dwgread --version

# ===========================================================================
# Stage 2 — Application runtime
# ===========================================================================
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.11.8 /uv /uvx /bin/

ENV PATH="/app/.venv/bin:$PATH"

# System dependencies.
#
# Base build/runtime deps plus the system binaries/libraries that the ingestion
# adapters depend on and which cannot be provided by pip extras (local-dev
# stack):
#   - tesseract-ocr: OCR binary wrapped by pytesseract in the raster-PDF
#     adapter (ADR-0008).
#   - libgl1 / libglib2.0-0: OpenCV (opencv-python) runtime shared libraries
#     pulled in by the raster-PDF / centerline pipeline.
#   - libpcre2-8-0: runtime dependency of the dwgread binary copied below.
# `dwgread` (LibreDWG) is compiled in the libredwg-builder stage and copied in.
# The vtracer raster tracer is a pip wheel (the `ingestion` extra), not a
# system binary, so it needs no apt package.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    libpq-dev \
    tesseract-ocr \
    libgl1 \
    libglib2.0-0 \
    libpcre2-8-0 \
    && rm -rf /var/lib/apt/lists/*

# DWG adapter binary (ADR-0006), compiled in stage 1.
COPY --from=libredwg-builder /usr/local/bin/dwgread /usr/local/bin/dwgread

# Set working directory
WORKDIR /app

# Copy dependency manifest first (cache-friendly layer)
COPY pyproject.toml uv.lock README.md ./

# Default to a lean DXF-only image. The full all-formats stack (DWG + vector/
# raster PDF + IFC) is selected by overriding this at build time, e.g. via the
# DRAUPNIR_UV_EXTRAS value in .env:
#   --extra db --extra jobs --extra dxf --extra pdf-vector --extra ingestion
ARG DRAUPNIR_UV_EXTRAS="--extra db --extra jobs --extra dxf"

# Sync runtime dependencies before project source is available
RUN uv sync --locked --no-install-project --no-dev ${DRAUPNIR_UV_EXTRAS}

# Copy application source
COPY app/ ./app/

# Install project and runtime extras into the project environment
RUN uv sync --locked --no-dev ${DRAUPNIR_UV_EXTRAS}

# Create non-root user and shared upload root
RUN useradd -m -u 1000 appuser \
    && mkdir -p /app/var/uploads \
    && chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8000

# Default command
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
