#!/usr/bin/env bash
# Download worker from GitHub Releases into user install dir (no cargo on target).
# Config/WebUI state: ~/.config/pi-host-ops/ (never project tree).
set -euo pipefail

REPO="${REPO:-ModerRAS/pi-host-ops-server}"
VERSION="${VERSION:-latest}"
INSTALL_DIR="${INSTALL_DIR:-${HOME}/.local/pi-host-ops/worker}"
DISPATCHER_TOKEN="${DISPATCHER_TOKEN:-}"
HOST_OPS_URL="${HOST_OPS_URL:-}"
WORKER_LABEL="${WORKER_LABEL:-}"
WORKER_ID="${WORKER_ID:-}"
DO_RUN=0

usage() {
  cat <<USAGE
Usage: $0 [--repo OWNER/NAME] [--version latest|v0.1.0] [--install-dir DIR]
         [--dispatcher-token TOK] [--host-ops-url URL] [--worker-label L] [--worker-id ID] [--run]

Downloads worker-* from GitHub Releases. Open http://127.0.0.1:8788/ after start.
Set dispatcher token + directories in the WebUI (config under ~/.config/pi-host-ops/).
With --dispatcher-token (+ --host-ops-url) the script also enrolls multiplexed
mode: creates the worker server-side, saves worker_token to the user config,
and binds already-registered directory agents. Re-run after adding directories
to bind the new ones. Without a token it stays binary-only (legacy
per-directory heartbeat, flaps offline past a few dirs).
Token/URL/label/id also accepted via env: DISPATCHER_TOKEN, HOST_OPS_URL,
WORKER_LABEL, WORKER_ID.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) REPO="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    --install-dir) INSTALL_DIR="$2"; shift 2 ;;
    --dispatcher-token) DISPATCHER_TOKEN="$2"; shift 2 ;;
    --host-ops-url) HOST_OPS_URL="$2"; shift 2 ;;
    --worker-label) WORKER_LABEL="$2"; shift 2 ;;
    --worker-id) WORKER_ID="$2"; shift 2 ;;
    --run) DO_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown: $1" >&2; usage; exit 2 ;;
  esac
done

uname_s="$(uname -s)"; uname_m="$(uname -m)"
case "$uname_s-$uname_m" in
  Linux-x86_64|Linux-amd64) ASSET="worker-linux-x64" ;;
  Linux-aarch64|Linux-arm64) ASSET="worker-linux-arm64" ;;
  Darwin-x86_64) ASSET="worker-darwin-x64" ;;
  Darwin-arm64) ASSET="worker-darwin-arm64" ;;
  *) echo "unsupported $uname_s $uname_m" >&2; exit 1 ;;
esac

API="https://api.github.com/repos/${REPO}"
if [[ "$VERSION" == "latest" ]]; then REL_URL="${API}/releases/latest"
else TAG="$VERSION"; [[ "$TAG" == v* ]] || TAG="v${TAG}"; REL_URL="${API}/releases/tags/${TAG}"; fi

echo "Resolving $REL_URL ..."
JSON="$(curl -fsSL -H 'Accept: application/vnd.github+json' -H 'User-Agent: pi-host-ops-install' "$REL_URL")" \
  || { echo "ERROR: no release yet — tag v* after merge or run Release workflow" >&2; exit 1; }
TAG_NAME="$(printf '%s' "$JSON" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)"
DOWNLOAD_URL="$(printf '%s' "$JSON" | tr '{' '\n' | grep -F "\"name\": \"$ASSET\"" -A20 | sed -n 's/.*"browser_download_url"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)"
[[ -n "$DOWNLOAD_URL" ]] || { echo "ERROR: asset $ASSET missing in $TAG_NAME" >&2; exit 1; }

mkdir -p "$INSTALL_DIR"
DEST="${INSTALL_DIR}/worker"
curl -fsSL -o "${DEST}.download" "$DOWNLOAD_URL"
chmod +x "${DEST}.download"
mv -f "${DEST}.download" "$DEST"

cat > "${INSTALL_DIR}/run.sh" <<'RUN'
#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
exec "$HERE/worker"
RUN
chmod +x "${INSTALL_DIR}/run.sh"

echo "Installed $TAG_NAME -> $DEST"
echo "Config: \${XDG_CONFIG_HOME:-\$HOME/.config}/pi-host-ops/config.json"

# --- multiplexed-mode enroll (needs dispatcher token; tokens never printed) ---
sanitize_id() {
  out="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | sed -e 's/[^a-z0-9][^a-z0-9]*/-/g' -e 's/^-//' -e 's/-$//')"
  [[ -z "$out" ]] && out="worker-$(date +%s)"
  printf '%s' "$out"
}
json_field() { # $1=json $2=key -> first string value (top-level responses only)
  printf '%s' "$1" | sed -n "s/.*\"$2\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" | head -1
}

if [[ -z "$DISPATCHER_TOKEN" ]]; then
  echo "No dispatcher token: binary only. Re-run with --dispatcher-token (+ --host-ops-url) to auto-enroll multiplexed mode."
else
  [[ -n "$HOST_OPS_URL" ]] || { echo "ERROR: enroll needs --host-ops-url (or HOST_OPS_URL env)" >&2; exit 1; }
  HOST_OPS_URL="${HOST_OPS_URL%/}"
  CFG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/pi-host-ops"
  mkdir -p "$CFG_DIR"
  CFG="$CFG_DIR/config.json"
  SAVED_TOKEN=""; SAVED_ID=""
  if [[ -f "$CFG" ]]; then
    SAVED_TOKEN="$(json_field "$(cat "$CFG")" worker_token)"
    SAVED_ID="$(json_field "$(cat "$CFG")" worker_id)"
  fi
  if [[ -z "$SAVED_TOKEN" ]]; then
    LABEL="${WORKER_LABEL:-$(hostname 2>/dev/null || echo worker)}"
    [[ -n "$LABEL" ]] || LABEL="worker"
    if [[ -n "$WORKER_ID" ]]; then WID="$(sanitize_id "$WORKER_ID")"; else WID="$(sanitize_id "$LABEL")"; fi
    RESP="$(curl -sS -w '\nHTTP_CODE:%{http_code}' -X POST -H "Authorization: Bearer $DISPATCHER_TOKEN" \
      -H 'Content-Type: application/json' -d "{\"id\":\"$WID\",\"label\":\"$LABEL\"}" "$HOST_OPS_URL/v1/workers")" \
      || { echo "ERROR: create worker failed (network)" >&2; exit 1; }
    CODE="${RESP##*HTTP_CODE:}"
    BODY="${RESP%$'\n'HTTP_CODE:*}"
    if [[ "$CODE" == "409" ]]; then
      echo "ERROR: worker id '$WID' already exists server-side. Pass a fresh --worker-id." >&2; exit 1
    fi
    [[ "$CODE" == "200" || "$CODE" == "201" ]] || { echo "ERROR: create worker failed: HTTP $CODE ${BODY:0:200}" >&2; exit 1; }
    WID="$(json_field "$BODY" id)"
    SAVED_TOKEN="$(json_field "$BODY" token)"
    [[ -n "$SAVED_TOKEN" ]] || { echo "ERROR: server did not return a worker token" >&2; exit 1; }
    echo "Worker enrolled: id=$WID (token saved to config, not shown)"
  else
    WID="$SAVED_ID"
    if [[ -z "$WID" ]]; then
      [[ -n "$WORKER_ID" ]] || { echo "ERROR: config has worker_token but no worker_id. Re-run with --worker-id <id>." >&2; exit 1; }
      WID="$(sanitize_id "$WORKER_ID")"
    fi
    echo "Reusing saved worker_token for worker id=$WID"
  fi
  command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 required to merge $CFG" >&2; exit 1; }
  python3 - "$CFG" "$HOST_OPS_URL" "$DISPATCHER_TOKEN" "$WID" "$SAVED_TOKEN" <<'PYEOF'
import json, sys
path, host, disp, wid, wtok = sys.argv[1:6]
try:
    with open(path) as f: cfg = json.load(f)
except Exception: cfg = {}
cfg['host_ops_url'] = host
cfg['dispatcher_token'] = disp
cfg['worker_token'] = wtok
cfg['worker_id'] = wid
cfg.setdefault('directories', [])
with open(path, 'w') as f: json.dump(cfg, f, indent=2)
PYEOF
  echo "Config updated: $CFG"
  if DIRS="$(curl -fsSL --max-time 5 http://127.0.0.1:8788/api/directories 2>/dev/null)"; then
    ok=0; skip=0
    while IFS= read -r aid; do
      [[ -n "$aid" ]] || { skip=$((skip+1)); continue; }
      BRESP="$(curl -sS -X POST -H "Authorization: Bearer $DISPATCHER_TOKEN" \
        -H 'Content-Type: application/json' -d "{\"agent_id\":\"$aid\"}" "$HOST_OPS_URL/v1/workers/$WID/agents" 2>/dev/null)"
      if printf '%s' "$BRESP" | grep -q '"bound":true\|already bound'; then ok=$((ok+1));
      else echo "  bind failed for $aid: ${BRESP:0:160}"; fi
    done < <(printf '%s' "$DIRS" | grep -o '"agent_id"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"\([^"]*\)"$/\1/')
    echo "Agents bound: $ok (not yet registered: $skip). Dirs added later via WebUI need one re-run to bind."
  else
    echo "Worker WebUI not reachable: start it (--run), add directories, then re-run this script to bind."
  fi
fi

echo "Start: ${INSTALL_DIR}/run.sh   then open http://127.0.0.1:8788/"
[[ "$DO_RUN" -eq 1 ]] && exec "${INSTALL_DIR}/run.sh"
