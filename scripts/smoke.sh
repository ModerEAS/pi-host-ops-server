#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

TMP="$ROOT/data/smoke-$$"
mkdir -p "$TMP"
export BIND="127.0.0.1:18787"
export DATABASE_URL="sqlite:${TMP}/host-ops.db"
export DISPATCHER_TOKEN="smoke-dispatcher"
export AGENT_TOKEN="smoke-agent"
export RUST_LOG="error"

BIN="$ROOT/target/debug/pi-host-ops-server"
if [[ ! -x "$BIN" ]]; then
  cargo build -q
fi

"$BIN" &
PID=$!
cleanup() { kill "$PID" 2>/dev/null || true; rm -rf "$TMP"; }
trap cleanup EXIT

for i in $(seq 1 50); do
  if curl -sf "http://$BIND/health" >/dev/null; then break; fi
  sleep 0.1
done
curl -sf "http://$BIND/health" | grep -q '"ok":true'

# default agent seeded from AGENT_TOKEN
CREATE=$(curl -sf -X POST "http://$BIND/v1/jobs" \
  -H "Authorization: Bearer $DISPATCHER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"type":"shell","payload":{"command":"echo hi"},"target":{"agent_id":"default"}}')
ID=$(echo "$CREATE" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "$CREATE" | python3 -c "import sys,json; assert json.load(sys.stdin)['delivery_mode']=='steer'"
echo "created $ID delivery_mode=steer"

POLL=$(curl -sf -X POST "http://$BIND/v1/agent/poll" \
  -H "Authorization: Bearer $AGENT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"wait_secs":0}')
echo "$POLL" | grep -q "$ID"
echo "$POLL" | python3 -c "import sys,json; j=json.load(sys.stdin)['job']; assert j['delivery_mode']=='steer'"

curl -sf -X POST "http://$BIND/v1/agent/jobs/$ID/events" \
  -H "Authorization: Bearer $AGENT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"type":"text","message":"smoke progress"}' | grep -q '"seq":1'

curl -sf -X POST "http://$BIND/v1/agent/jobs/$ID/result" \
  -H "Authorization: Bearer $AGENT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"ok":true,"result":{"stdout":"hi"}}' | grep -q succeeded

STATUS=$(curl -sf "http://$BIND/v1/jobs/$ID" \
  -H "Authorization: Bearer $DISPATCHER_TOKEN")
echo "$STATUS" | grep -q succeeded
echo "$STATUS" | grep -q smoke

# empty poll -> 204
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://$BIND/v1/agent/poll" \
  -H "Authorization: Bearer $AGENT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"wait_secs":0}')
[[ "$CODE" == "204" ]]

# unauth -> 401
CODE=$(curl -s -o /dev/null -w '%{http_code}' "http://$BIND/v1/agents")
[[ "$CODE" == "401" ]]

echo "smoke OK"
