# host-ops worker（box 侧）

Bridges `pi-host-ops-server` to local **`pi_svc`**. Per-agent Bearer token. Streams progress via `/events`.

## Behavior

1. `POST /v1/agent/heartbeat` on an interval
2. `POST /v1/agent/poll` long-poll
3. On job:
   - **`shell`** + `payload.command` → local subprocess
   - else → `pi_svc` with **`delivery_mode`**:
     - **`steer`** (server default) → `pi_svc send --steer`
     - **`queue`** + idle → `send`; + busy → `send --follow-up`
4. While waiting: POST `text` / `tool` / `status` events; renew lease
5. `POST /v1/agent/jobs/:id/result`
6. Session default **`host-ops`**; never stop other sessions; model **muse-spark** (no Fast)

## Env

| Variable | Default | Meaning |
|----------|---------|---------|
| `HOST_OPS_URL` | `http://127.0.0.1:8787` | Server |
| `AGENT_TOKEN` | *(required)* | This agent's token |
| `AGENT_ID` | `box-pi` | Log label only (auth is token) |
| `PI_SVC` | `/workspace/pi-runs/pi_svc` | CLI |
| `PI_SESSION` | `host-ops` | Session name |
| `WORK_DIR` | `…/workspace` | cwd |
| `POLL_WAIT_SECS` | `20` | Long-poll |
| `HEARTBEAT_SECS` | `20` | Heartbeat interval |
| `EVENT_INTERVAL_SECS` | `5` | Progress event cadence |
| `PI_PROVIDER` / `PI_MODEL` | `opencode-go` / `muse-spark-1.3-contributor` | No Fast |

## Run

```bash
# Create agent via dispatcher, then:
export AGENT_TOKEN='<token from POST /v1/agents>'
./worker/run.sh
```

Or use seeded `default` token from `data/bootstrap-tokens.txt` / `AGENT_TOKEN` env.
