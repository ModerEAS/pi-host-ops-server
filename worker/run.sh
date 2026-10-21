#!/usr/bin/env bash
# Start host-ops worker (embedded WebUI). Prefer Release/install binaries; cargo is DEV ONLY.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

pick_bin() {
  local c
  for c in \
    "${INSTALL_DIR:-}/worker" \
    "${HOME}/.local/pi-host-ops/worker/worker" \
    "$ROOT/target/release/worker" \
    "$ROOT/target/debug/worker"
  do
    if [[ -n "$c" && -x "$c" ]]; then
      echo "$c"
      return 0
    fi
  done
  return 1
}

if BIN="$(pick_bin)"; then
  echo "using worker binary: $BIN" >&2
  echo "WebUI default http://127.0.0.1:8788/  config: \${XDG_CONFIG_HOME:-\$HOME/.config}/pi-host-ops/config.json" >&2
  exec "$BIN"
fi

if command -v cargo >/dev/null 2>&1; then
  echo "WARNING: no prebuilt worker; cargo run is DEV ONLY." >&2
  echo "Production: scripts/install-worker.sh (GitHub Releases)" >&2
  exec cargo run --release --bin worker
fi

echo "ERROR: worker binary not found and cargo unavailable." >&2
echo "Install: ./scripts/install-worker.sh --run" >&2
exit 1
