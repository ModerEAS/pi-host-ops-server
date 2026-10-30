# pi-host-ops-server

远程调度端与多台 **Pi agent** 之间的 HTTP 任务队列（Axum + Tokio + SQLite）。

Multi-agent job queue: dispatcher enqueues targeted jobs; each Pi worker authenticates with its **own** token, claims matching work, streams progress events, and posts results.

```
Dispatcher  --Bearer DISPATCHER-->  Server (Rust)
                                      |  jobs + agents + job_events (SQLite)
Pi workers  --Bearer per-agent---->  claim / heartbeat / events / result
                                      |
                                   pi --mode rpc (box) or host ops
```

## Auth

| Endpoint | Auth |
|----------|------|
| `GET /health` | **public** |
| `POST/GET /v1/jobs*`, `GET /v1/agents*`, `DELETE /v1/agents/:id` | **Dispatcher** `Authorization: Bearer $DISPATCHER_TOKEN` |
| `POST/GET/DELETE /v1/workers*` (create/list/revoke workers, bind/unbind agents) | **Dispatcher** |
| `POST /v1/agent/*` | **Agent** Bearer = that agent's token (sha256 stored; plaintext shown once at create) |
| `POST /v1/worker/heartbeat`, `POST /v1/worker/poll` | **Worker** Bearer = worker token (one batch heartbeat + one multiplexed poll for all bound agents) |

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

## Docker / Swarm

Only the **server** (dispatcher HTTP + SQLite) is containerized — image
`ghcr.io/moderras/pi-host-ops-server` (`:latest` + `:sha-<commit>` on master).
The worker stays host-installed (needs host `pi` binary / project dirs).

```bash
docker compose up --build   # local: server on :8787, SQLite in a volume
```

Production Swarm deploy (GHCR pull + `docker stack deploy`, never build on the
server): see [docs/deploy.md](docs/deploy.md).

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

| Value | Worker behavior (`pi --mode rpc`) |
|-------|--------------------------------------|
| **`steer`** (default) | prompt with `streamingBehavior: steer` — interrupt/redirect current turn |
| **`queue`** | idle → plain prompt; busy → prompt with `streamingBehavior: followUp` |

### Pi idle wait (activity-based)

After the RPC prompt is accepted, the worker waits for the Pi session to go
idle with **two budgets** (see `src/bin/worker/runtime.rs`):

- **Absolute cap** — the job's `timeout_secs` if set, else **3600s (1h)**.
  A long coding/`act` run never fails just for being slow.
- **Inactivity budget** — **600s (10 min)** with no observable progress
  (`running` state, or `text_tail` / `last_tool` / `settled_count` changing).
  While the agent works, the deadline keeps refreshing.

A timeout while the agent is still `running` fails the job but says so
plainly (`absolute Pi wait timeout after …: agent still running`), and the
result keeps `wait_timeout`, `timeout_kind` (`absolute`/`inactivity`), and
tails. A send that never starts the agent fails as an inactivity timeout
with a distinct message. Pass `--timeout` (`host-ops job create` / `steer`)
only when a job needs more than the 1h default.

### Pi child liveness (no pi_svc)

The worker owns one long-lived `pi --mode rpc` child per enabled directory
(no `pi_svc` / control socket anywhere). Liveness is the OS process itself:
if the child is gone, the directory shows an error in the WebUI ("pi process
exited — restarts on next job"), the wait ends as a failure (never as fake
idle success), and the next `act` job spawns a fresh child. Server-side
agent online/offline still follows the worker heartbeat (see § Worker
multiplexing), so dispatch routing keeps working.

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

## Terminal webhook (server → your bot)

When a job enters a terminal status (`succeeded` / `failed` / `timed_out` /
`no_agent`), the **server** POSTs once to `HOST_OPS_WEBHOOK_URL` — right after
the status is persisted. Workers never send webhooks. Default off: no URL =
no POST. Failures retry 3 attempts with backoff and only log; the stored job
state is never affected.

```bash
HOST_OPS_WEBHOOK_URL=https://bot.example.com/host-ops/job-terminal
HOST_OPS_WEBHOOK_AUTH="Bearer <grok-bot-secret>"   # or _FILE for Swarm secrets
# HOST_OPS_WEBHOOK_HEADER=X-Webhook-Token          # custom header if your bot needs it
```

Body (JSON):

```jsonc
{
  "job_id": "<uuid>",
  "status": "succeeded",          // succeeded | failed | timed_out | no_agent
  "agent_id": "duolinzao-scale",  // null if never claimed (e.g. no_agent)
  "project": "duolinzao",         // claiming/target agent project, if known
  "thread_id": "feature-x",       // + alias "thread" with the same value
  "job_type": "shell",
  "summary": "…truncated to 2000 chars…",  // + alias "text"; from result stdout/text/output, else error
  "finished_at": "2026-10-30T…Z",
  "error": "…"                   // only when present
}
```

Grok Bot 例程对接：把你的接收路由（如 `POST /host-ops/job-terminal`）配成
`HOST_OPS_WEBHOOK_URL`，校验 `Authorization` 头等于
`HOST_OPS_WEBHOOK_AUTH` 的值（推荐 `Bearer <随机长串>`），按 `status` /
`agent_id` / `project` / `thread` 分发即可；需要自定义头就把
`HOST_OPS_WEBHOOK_HEADER` 配成同一个名字。

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
| `HOST_OPS_WEBHOOK_URL` | (empty = off) | Terminal-job webhook target; only when set does the server POST |
| `HOST_OPS_WEBHOOK_AUTH` | (empty) | Secret sent as auth header value (e.g. `Bearer <token>`); or `HOST_OPS_WEBHOOK_AUTH_FILE` for Swarm secrets. Never logged |
| `HOST_OPS_WEBHOOK_HEADER` | `Authorization` | Auth header name; set e.g. `X-Webhook-Token` for custom-header bots |

## Migrations

- `001_init` — jobs table
- `002_agents_routing` — `agents` + job target / `delivery_mode` (default **steer**) / lease / timeout
- `003_job_events` — incremental feedback
- `004_agent_model` — per-agent `pi_provider` / `pi_model`
- `005_workers` — `workers` + `worker_agents` bindings (one agent belongs to at most one worker)


## Dispatcher CLI (`host-ops`)

调度端不要裸 `curl`：同一仓库提供 **`host-ops`** CLI（Bearer + JSON 封装）。

配置（**不写进项目目录**）：

- Linux/mac: `~/.config/pi-host-ops/dispatcher.json`
- Windows: `%APPDATA%\pi-host-ops\dispatcher.json`
- 字段: `url`（默认 `http://127.0.0.1:8787`）、`token`（`DISPATCHER_TOKEN`）
- 环境变量覆盖（优先）: `HOST_OPS_URL`、`DISPATCHER_TOKEN`

```bash
cargo run --release --bin host-ops -- config set --url http://127.0.0.1:8787 --token "$DISPATCHER_TOKEN"
# 或安装脚本（Release 后）:
# ./scripts/install-host-ops.sh

host-ops agents list
host-ops agents create --id duolinzao-scale --label duolinzao --project duolinzao --cap shell

# 创建任务（默认 delivery=steer）
host-ops job create --type observe --payload '{"focus":"cwd"}' --agent duolinzao-scale --thread feature-x
host-ops job watch <job-id>

# 纠正 / 打断进行中的 Pi 会话：payload.prompt 几乎原样进 pi --mode rpc prompt（streamingBehavior: steer）
host-ops steer --agent duolinzao-scale --thread feature-x --prompt '纠正：不要改 README，只修编译错误'
# 或从已有 job 推断 agent/thread：
host-ops job steer <job-uuid> --prompt-file ./correction.txt
# job create 也支持 --prompt 简写 → {"prompt":"..."}
host-ops job create --type act --prompt '继续完成剩余测试' --agent duolinzao-scale --delivery steer
```

Worker 侧：若 payload 含字符串字段 **`prompt`**（或 `message` / `text`），则发给 Pi 的正文就是该字符串（仅加一行 `[host-ops job <id>]` 前缀），不再包一整段 JSON boilerplate。

## Worker (embedded WebUI)

One local **worker** process manages many project directories via a small WebUI.

- Open **http://127.0.0.1:8788/** after starting the worker
- Add directories (each = one agent / pi rpc session) and set **provider/model** per dir
- Config: `~/.config/pi-host-ops/config.json` or `%APPDATA%\pi-host-ops\config.json` — **never** project litter
- Default model: `muse-spark-1.3-contributor` / `opencode-go` — **Fast is blocked** (UI + API)
- Change model: edit the directory row in the WebUI (or `provider`/`model` on `POST /v1/agents` / `PATCH /v1/agent/me`)

See **[worker/README.md](worker/README.md)**.

### Production — download from GitHub Releases (no cargo on the machine)

```bash
# Linux/mac
./scripts/install-worker.sh --run

# Windows
powershell -ExecutionPolicy Bypass -File scripts\install-worker.ps1 -Run
```

Assets are auto-published after each successful production deploy
(`v<next-patch>` scheme, marked latest): `worker-*`, `host-ops-*`,
`pi-host-ops-server-*`, `SHA256SUMS`. To jump minor/major, bump
`Cargo.toml` `[package] version` — see [docs/deploy.md](docs/deploy.md) §3.

### Dev only

```bash
cargo run --release                   # terminal A — server
cargo run --release --bin worker      # terminal B — WebUI + pollers
# or: ./worker/run.sh
```

### Agent self APIs

- `GET /v1/agent/me` — agent reads its registration (cwd/project/provider/model)
- `PATCH /v1/agent/me` — agent updates cwd / provider / model (Fast rejected)

## Worker multiplexing (batch heartbeat + single poll)

With ~18 enabled directories, per-directory heartbeat + long-poll (`8s × N`)
exceeds `HEARTBEAT_TTL` (~45s) and agents falsely flap offline. A **worker**
credential fixes this: one batch heartbeat every ~15s + one multiplexed poll
covering all agents bound to that worker. Execution stays **serial** (one job
at a time); results/events/leases still use each agent's own token.

```bash
# 1. Dispatcher creates a worker (token shown once)
curl -sS -X POST http://127.0.0.1:8787/v1/workers \
  -H "Authorization: Bearer $DISPATCHER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id":"box-main","label":"box main worker"}'
# → {"id":"box-main","token":"<save-me>","label":"..."}

# 2. Bind each directory agent to the worker (one agent ≤ one worker)
curl -sS -X POST http://127.0.0.1:8787/v1/workers/box-main/agents \
  -H "Authorization: Bearer $DISPATCHER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"agent_id":"myproj-a1b2c3d4"}'

# 3. Paste the worker token into the worker WebUI Settings
#    (or `worker_token` in `~/.config/pi-host-ops/config.json`).
#    When set, the worker sends ~1 heartbeat/15s + 1 poll for all dirs.
#    When empty, it keeps the legacy per-directory heartbeat+poll.
```

Worker API:

```bash
# Batch heartbeat — only bound agents; unbound ids → 403, nothing updated
curl -sS -X POST http://127.0.0.1:8787/v1/worker/heartbeat \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"agents":[{"agent_id":"a","status":"online"},{"agent_id":"b","status":"busy"}]}'

# Multiplexed poll — claims a job for any bound *online* agent
curl -sS -D- -X POST http://127.0.0.1:8787/v1/worker/poll \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"wait_secs":20}'
# 200 {"job": {...}, "agent_id": "..."} or 204
```

Notes:

- `POST /v1/worker/poll` only considers bound agents seen within
  `HEARTBEAT_TTL` — a dir removed/disabled on the worker ages out instead of
  claiming jobs it can no longer run (unbind it to remove immediately).
- Old single-agent `POST /v1/agent/heartbeat` and `POST /v1/agent/poll` keep
  working unchanged.
- Revoking a worker (`DELETE /v1/workers/:id`) invalidates its token and
  releases its bindings.

## Tests / smoke

```bash
cargo test
./scripts/smoke.sh
```

## Notes

- Do **not** use Cursor CloudAgent / Fast models from the worker.
- Box is shared: unique `pi` session names; never `stop` others' sessions.
