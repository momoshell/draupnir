#!/usr/bin/env bash
#
# dev_stack.sh — bring up the full LOCAL ingestion stack on the host.
#
# This is the host-based recipe (podman infra + host uvicorn + host celery),
# which is what we use on dev machines that have no Docker daemon and need the
# real ingestion adapters (LibreDWG/dwgread, ezdxf, pymupdf) against live code.
# `make up` runs the *containerised* stack instead; use this for adapter work
# and end-to-end ingest gates where you want host code + host binaries.
#
# It encodes the settings that otherwise have to be remembered every session:
#   * BOTH license probes (pymupdf + libredwg) — without the libredwg one the
#     DWG adapter reports `down` (missing_license) and DWG ingest fails.
#   * Big-sheet limits (LIBREDWG_MAX_OUTPUT_MB / ADAPTER_TIMEOUT_SECONDS) so a
#     full-floor DWG (hundreds of MB of dwgread JSON) doesn't hit the 32 MB cap
#     or the 300 s timeout.
#
# Usage:
#   scripts/dev_stack.sh up       # infra + migrate + launch API & worker (default)
#   scripts/dev_stack.sh down     # stop host API & worker (leaves infra running)
#   scripts/dev_stack.sh status   # health + process summary
#   scripts/dev_stack.sh logs     # tail API + worker logs
#   scripts/dev_stack.sh restart  # down + up (use after a code change)
#
# Every value below is overridable from the environment, e.g.
#   LIBREDWG_MAX_OUTPUT_MB=128 scripts/dev_stack.sh up
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# --- configuration (override via env) ----------------------------------------
: "${COMPOSE:=podman compose}"
: "${APP_DB:=draupnir}"
: "${PG_CONTAINER:=draupnir-postgres-1}"
: "${API_HOST:=127.0.0.1}"
: "${API_PORT:=8000}"
: "${WORKER_CONCURRENCY:=1}"

export DATABASE_URL="${DATABASE_URL:-postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/${APP_DB}}"
export BROKER_URL="${BROKER_URL:-amqp://guest:guest@127.0.0.1:5672//}"
export STORAGE_LOCAL_ROOT="${STORAGE_LOCAL_ROOT:-var/uploads}"
# Big-sheet headroom: full-floor DWGs blow past the 32 MB / 300 s defaults.
export LIBREDWG_MAX_OUTPUT_MB="${LIBREDWG_MAX_OUTPUT_MB:-700}"
export ADAPTER_TIMEOUT_SECONDS="${ADAPTER_TIMEOUT_SECONDS:-600}"
# Both probes required: pymupdf (vector PDF) AND libredwg (DWG). Local-dev
# attestations that the GPL/AGPL review has been done for this machine.
export DRAUPNIR_APPROVED_LICENSE_PROBES="${DRAUPNIR_APPROVED_LICENSE_PROBES:-pymupdf-deployment-review,libredwg-distribution-review}"

# LibreDWG 0.14 (master) toolchain (#802). 0.14 reads R2004 files 0.13.3 cannot
# (e.g. P-520002, the 0xd40 parse error) and emits per-entity ACI colours that
# 0.13.3 collapses to BYLAYER. Prepend the pinned 0.14 install so the worker
# resolves it deterministically regardless of login-shell PATH order (a login
# shell puts /opt/homebrew/bin — system 0.13.3 — first). Override via env.
: "${LIBREDWG_DWGREAD:=$HOME/.local/libredwg-0.14/bin/dwgread}"
if [ -x "$LIBREDWG_DWGREAD" ]; then
  export PATH="$(dirname "$LIBREDWG_DWGREAD"):$PATH"
fi

RUN_DIR="$REPO_ROOT/var/run"
LOG_DIR="$REPO_ROOT/var/log"
mkdir -p "$RUN_DIR" "$LOG_DIR" "$STORAGE_LOCAL_ROOT"
UVICORN_PID="$RUN_DIR/uvicorn.pid"
CELERY_PID="$RUN_DIR/celery.pid"
UVICORN_LOG="$LOG_DIR/uvicorn.log"
CELERY_LOG="$LOG_DIR/celery.log"

log() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m!! \033[0m %s\n' "$*"; }

report_dwgread() {
  local bin ver
  bin="$(command -v dwgread 2>/dev/null)"
  if [ -n "$bin" ]; then
    ver="$(dwgread --version 2>/dev/null | head -1)"
    log "dwgread: $ver  ($bin)"
  else
    warn "dwgread not found on PATH"
  fi
}

# --- helpers -----------------------------------------------------------------
proc_count() { pgrep -f "$1" 2>/dev/null | wc -l | tr -d ' '; }

stop_proc() {
  local pattern="$1" name="$2"
  if [ "$(proc_count "$pattern")" -gt 0 ]; then
    pkill -f "$pattern" 2>/dev/null || true
    for _ in $(seq 1 10); do
      [ "$(proc_count "$pattern")" -eq 0 ] && break
      sleep 1
    done
    if [ "$(proc_count "$pattern")" -gt 0 ]; then
      warn "$name still has $(proc_count "$pattern") process(es); force-killing"
      pkill -9 -f "$pattern" 2>/dev/null || true
    fi
  fi
  log "$name stopped"
}

wait_pg() {
  for _ in $(seq 1 30); do
    if podman exec "$PG_CONTAINER" pg_isready -U postgres >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  warn "postgres did not become ready in 30s"; return 1
}

ensure_db() {
  if ! podman exec "$PG_CONTAINER" psql -U postgres -tAc \
      "SELECT 1 FROM pg_database WHERE datname='${APP_DB}'" | grep -q 1; then
    log "creating app DB '${APP_DB}'"
    podman exec "$PG_CONTAINER" psql -U postgres -c "CREATE DATABASE ${APP_DB}" >/dev/null
  fi
}

health_summary() {
  curl -s "http://${API_HOST}:${API_PORT}/v1/system/health" 2>/dev/null | uv run python -c '
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    print("  (API not responding)"); sys.exit()
print("  overall: " + d["status"])
for a in d.get("checks", {}).get("adapters", []):
    print("    {:<18} {}".format(a["adapter_key"], a["status"]))
' || echo "  (API not responding)"
}

# --- commands ----------------------------------------------------------------
cmd_up() {
  log "starting infra (postgres + rabbitmq) via: $COMPOSE"
  $COMPOSE up -d postgres rabbitmq >/dev/null 2>&1 || $COMPOSE up -d postgres rabbitmq
  wait_pg
  ensure_db
  log "running migrations -> head"
  uv run alembic upgrade head >/dev/null 2>&1 && log "migrations applied" || { warn "alembic upgrade failed"; uv run alembic upgrade head; }

  if [ "$(proc_count 'uvicorn app.main')" -gt 0 ]; then
    warn "uvicorn already running; leaving it. Use 'restart' to pick up code changes."
  else
    log "launching uvicorn on ${API_HOST}:${API_PORT}"
    nohup uv run uvicorn app.main:app --host "$API_HOST" --port "$API_PORT" \
      >"$UVICORN_LOG" 2>&1 &
    echo $! >"$UVICORN_PID"
  fi

  if [ "$(proc_count 'celery -A app.jobs.worker')" -gt 0 ]; then
    warn "celery worker already running; leaving it. Use 'restart' to pick up code changes."
  else
    log "launching celery worker (concurrency=${WORKER_CONCURRENCY})"
    nohup uv run celery -A app.jobs.worker worker --concurrency="$WORKER_CONCURRENCY" \
      >"$CELERY_LOG" 2>&1 &
    echo $! >"$CELERY_PID"
  fi

  log "waiting for API health..."
  for _ in $(seq 1 30); do
    curl -sf "http://${API_HOST}:${API_PORT}/v1/system/health" >/dev/null 2>&1 && break
    sleep 1
  done
  echo
  report_dwgread
  log "adapter health:"
  health_summary
  echo
  log "ready. logs: $LOG_DIR  (scripts/dev_stack.sh logs)"
}

cmd_down() {
  stop_proc 'uvicorn app.main' 'uvicorn'
  stop_proc 'celery -A app.jobs.worker' 'celery worker'
  rm -f "$UVICORN_PID" "$CELERY_PID"
  log "host processes down (infra left running; '$COMPOSE down' to stop it)"
}

cmd_status() {
  log "processes:"
  echo "    uvicorn: $(proc_count 'uvicorn app.main') | celery: $(proc_count 'celery -A app.jobs.worker')"
  log "infra containers:"
  podman ps --filter "name=draupnir-postgres" --filter "name=draupnir-rabbitmq" \
    --format "    {{.Names}}  {{.Status}}" 2>/dev/null || true
  report_dwgread
  log "adapter health:"
  health_summary
}

cmd_logs() {
  log "tailing $UVICORN_LOG and $CELERY_LOG (Ctrl-C to stop)"
  tail -n 40 -f "$UVICORN_LOG" "$CELERY_LOG"
}

case "${1:-up}" in
  up)      cmd_up ;;
  down)    cmd_down ;;
  restart) cmd_down; cmd_up ;;
  status)  cmd_status ;;
  logs)    cmd_logs ;;
  *) echo "usage: $0 {up|down|restart|status|logs}" >&2; exit 2 ;;
esac
