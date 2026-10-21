#!/usr/bin/env bash
# Start the host-ops box worker (Rust binary).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ -z "${AGENT_TOKEN:-}" ]]; then
  if [[ -f .env ]]; then
    # shellcheck disable=SC1091
    set -a
    source .env
    set +a
  fi
fi
if [[ -z "${AGENT_TOKEN:-}" && -f data/bootstrap-tokens.txt ]]; then
  if grep -q '^AGENT_TOKEN=' data/bootstrap-tokens.txt 2>/dev/null; then
    AGENT_TOKEN="$(grep '^AGENT_TOKEN=' data/bootstrap-tokens.txt | head -1 | cut -d= -f2-)"
  else
    AGENT_TOKEN="$(awk '/AGENT_TOKEN/{print $2; exit}' data/bootstrap-tokens.txt)"
  fi
  export AGENT_TOKEN
fi

if [[ -z "${AGENT_TOKEN:-}" ]]; then
  echo "AGENT_TOKEN required (env, .env, or data/bootstrap-tokens.txt)" >&2
  exit 1
fi

export HOST_OPS_URL="${HOST_OPS_URL:-http://127.0.0.1:8787}"
export AGENT_ID="${AGENT_ID:-box-pi}"
export PI_SVC="${PI_SVC:-/workspace/pi-runs/pi_svc}"
export PI_SESSION="${PI_SESSION:-host-ops}"
export WORK_DIR="${WORK_DIR:-$ROOT/workspace}"
export POLL_WAIT_SECS="${POLL_WAIT_SECS:-20}"

mkdir -p "$WORK_DIR"

if [[ -x "$ROOT/target/release/worker" ]]; then
  exec "$ROOT/target/release/worker"
fi
exec cargo run --release --bin worker
