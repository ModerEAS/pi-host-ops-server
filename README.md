# pi-host-ops-server

远程调度端与多台 **Pi agent** 之间的 HTTP 任务队列（Axum + Tokio + SQLite）。

Multi-agent job queue: dispatcher enqueues targeted jobs; each Pi worker authenticates with its **own** token, claims matching work, streams progress events, and posts results.

```
Dispatcher  --Bearer DISPATCHER-->  Server (Rust)
                                      |  jobs + agents + job_events (SQLite)
Pi workers  --Bearer per-agent---->  claim / heartbeat / events / result
                                      |
                                   pi_svc (box) or host ops
```

## Auth

| Endpoint | Auth |
|----------|------|
| `GET /health` | **public** |
| `POST/GET /v1/jobs*`, `GET /v1/agents*`, `DELETE /v1/agents/:id` | **Dispatcher** `Authorization: Bearer $DISPATCHER_TOKEN` |
| `POST /v1/agent/*` | **Agent** Bearer = that agent's token (sha256 stored; plaintext shown once at create) |

Wrong role → **403**. Missing/invalid → **401**.

### Bootstrap

1. Set `DISPATCHER_TOKEN` (and optional `AGENT_TOKEN`) in env, **or** let the server write `data/bootstrap-tokens.txt` on first run.
2. If `AGENT_TOKEN` is present, agent `id=default` is seeded (backward compatible).
3. Create more agents with `POST /v1/agents` (dispatcher). Save the returned `token` once.

## Run

```bash
cd /workspace/pi-host-ops-server
cp .env.example .env   # optional
cargo run --release
# BIND default 0.0.0.0:8787
```

## Create an agent + targeted jobs

```bash
# Create agent (token shown once)
curl -sS -X POST http://127.0.0.1:8787/v1/agents \
  -H "Authorization: Bearer $DISPATCHER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id":"duolinzao-scale","label":"duolinzao","project":"duolinzao","capabilities":["shell"],"cwd":"/workspace/duolinzao"}'
# → {"id":"...","token":"<save-me>",...}

# Job with delivery_mode=steer (DEFAULT if omitted)
curl -sS -X POST http://127.0.0.1:8787/v1/jobs \
  -H "Authorization: Bearer $DISPATCHER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "type":"observe",
    "payload":{"focus":"cwd"},
    "target":{"agent_id":"duolinzao-scale"},
    "thread_id":"feature-x",
    "timeout_secs":180
  }'
# delivery_mode defaults to "steer"

# Explicit queue (wait / follow-up when busy)
curl -sS -X POST http://127.0.0.1:8787/v1/jobs \
  -H "Authorization: Bearer $DISPATCHER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "type":"shell",
    "payload":{"command":"uname -a"},
    "target":{"project":"duolinzao","capabilities":["shell"]},
    "delivery_mode":"queue"
  }'

# List agents (online/offline from heartbeat TTL)
curl -sS http://127.0.0.1:8787/v1/agents \
  -H "Authorization: Bearer $DISPATCHER_TOKEN"
```

### `delivery_mode`

| Value | Worker behavior (`pi_svc`) |
|-------|----------------------------|
| **`steer`** (default) | `pi_svc send --steer` — interrupt/redirect current turn |
| **`queue`** | idle → normal `send`; busy → `send --follow-up` |

## Feedback events

While a job is `running`, the claiming agent may POST incremental events:

```bash
# Agent
curl -sS -X POST http://127.0.0.1:8787/v1/agent/jobs/$ID/events \
  -H "Authorization: Bearer $AGENT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"type":"text","message":"…progress…","data":{}}'
# types: text | tool | status | log

# Dispatcher poll
curl -sS "http://127.0.0.1:8787/v1/jobs/$ID/events?after=0" \
  -H "Authorization: Bearer $DISPATCHER_TOKEN"

# GET job includes last events in `events`
curl -sS "http://127.0.0.1:8787/v1/jobs/$ID" \
  -H "Authorization: Bearer $DISPATCHER_TOKEN"
```

SSE can come later; v1 is poll-based.

## Agent worker API

```bash
curl -sS -X POST http://127.0.0.1:8787/v1/agent/heartbeat \
  -H "Authorization: Bearer $AGENT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"status":"online"}'

curl -sS -D- -X POST http://127.0.0.1:8787/v1/agent/poll \
  -H "Authorization: Bearer $AGENT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"wait_secs":20}'
# 200 + job (includes delivery_mode) or 204

curl -sS -X POST http://127.0.0.1:8787/v1/agent/jobs/$ID/lease \
  -H "Authorization: Bearer $AGENT_TOKEN" -H "Content-Type: application/json" -d '{}'

curl -sS -X POST http://127.0.0.1:8787/v1/agent/jobs/$ID/result \
  -H "Authorization: Bearer $AGENT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"ok":true,"result":{"stdout":"..."}}'
```

Claim is atomic and **matched**: `target.agent_id`, or filter (`project` / `capabilities` / `machine`), plus `thread_id` sticky routing. Respects `max_concurrency`.

Statuses: `queued` | `running` | `succeeded` | `failed` | `timed_out` | `no_agent`

## Env

| Variable | Default | Meaning |
|----------|---------|---------|
| `BIND` | `0.0.0.0:8787` | Listen |
| `DATABASE_URL` | `sqlite:data/host-ops.db` | SQLite |
| `DISPATCHER_TOKEN` | (generated) | Dispatcher Bearer |
| `AGENT_TOKEN` | (generated) | Seed for `agents.id=default` |
| `HEARTBEAT_TTL_SECS` | `45` | Offline if `last_seen` older |
| `LEASE_SECS` | `60` | Claim / renew lease |
| `MAX_RESULT_BYTES` | `524288` | Result JSON cap |

## Migrations

- `001_init` — jobs table
- `002_agents_routing` — `agents` + job target / `delivery_mode` (default **steer**) / lease / timeout
- `003_job_events` — incremental feedback

## Worker

See **[worker/README.md](worker/README.md)**. Uses `AGENT_TOKEN` for the registered agent (often `default` or a token from `POST /v1/agents`).

```bash
cargo run --release          # terminal A
./worker/run.sh              # terminal B (reads bootstrap / .env)
```

## Tests / smoke

```bash
cargo test
./scripts/smoke.sh
```

## Notes

- Do **not** use Cursor CloudAgent / Fast models from the worker.
- Box is shared: unique `pi_svc` session names; never `stop` others' sessions.
