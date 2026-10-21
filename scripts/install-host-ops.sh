#!/usr/bin/env bash
# Download host-ops dispatcher CLI from GitHub Releases (no cargo on target).
# Config: ~/.config/pi-host-ops/dispatcher.json (never project tree).
set -euo pipefail

REPO="${REPO:-ModerRAS/pi-host-ops-server}"
VERSION="${VERSION:-latest}"
INSTALL_DIR="${INSTALL_DIR:-${HOME}/.local/pi-host-ops/bin}"

usage() {
  cat <<USAGE
Usage: $0 [--repo OWNER/NAME] [--version latest|v0.1.0] [--install-dir DIR]

Downloads host-ops-* from GitHub Releases into INSTALL_DIR and prints PATH hint.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) REPO="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    --install-dir) INSTALL_DIR="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown: $1" >&2; usage; exit 2 ;;
  esac
done

uname_s="$(uname -s)"; uname_m="$(uname -m)"
case "$uname_s-$uname_m" in
  Linux-x86_64|Linux-amd64) ASSET="host-ops-linux-x64" ;;
  Linux-aarch64|Linux-arm64) ASSET="host-ops-linux-arm64" ;;
  Darwin-x86_64) ASSET="host-ops-darwin-x64" ;;
  Darwin-arm64) ASSET="host-ops-darwin-arm64" ;;
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
DEST="${INSTALL_DIR}/host-ops"
curl -fsSL -o "${DEST}.download" "$DOWNLOAD_URL"
chmod +x "${DEST}.download"
mv -f "${DEST}.download" "$DEST"

echo "Installed $TAG_NAME -> $DEST"
echo "Config: \${XDG_CONFIG_HOME:-\$HOME/.config}/pi-host-ops/dispatcher.json"
echo "Add to PATH: export PATH=\"$INSTALL_DIR:\$PATH\""
echo "Then: host-ops config set --url http://127.0.0.1:8787 --token \"\$DISPATCHER_TOKEN\""
