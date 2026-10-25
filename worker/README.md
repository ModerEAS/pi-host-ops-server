# host-ops worker

One local process with an embedded **WebUI** (`http://127.0.0.1:8788/`).

- Add **directories** (each = one agent / one `pi_svc` session)
- Per directory: label, path, enabled, **provider + model** (default `opencode-go` / `muse-spark-1.3-contributor`; **Fast blocked**)
- Settings: host-ops URL, dispatcher token (bootstrap), optional `PI_SVC` path
- Config only under user config dir — **never** inside project trees:
  - Linux/mac: `~/.config/pi-host-ops/config.json` (or `$XDG_CONFIG_HOME/pi-host-ops/`)
  - Windows: `%APPDATA%\pi-host-ops\config.json`

## Production (no cargo)

Download from GitHub Releases:

```bash
./scripts/install-worker.sh --run
# Windows:
powershell -ExecutionPolicy Bypass -File scripts\install-worker.ps1 -Run
```

Then open the WebUI, paste dispatcher token, add project directories.

## Dev only

```bash
cargo run --release --bin worker
# or
./worker/run.sh   # uses target/release if present; else warns and uses cargo
```

## Behaviour

1. WebUI edits save to user config dir
2. For each enabled directory: register agent (dispatcher token) once, store token in config, heartbeat + poll
3. Jobs run with that directory as cwd; `pi_svc start --provider --model` uses the directory's model
4. Without `pi_svc`, only `shell` jobs succeed (typical early Windows setup)

## Multiplexed mode (many directories, one worker)

Per-directory heartbeat + long-poll serially (`8s × N`) exceeds the server
`HEARTBEAT_TTL` (~45s) once you have more than a few dirs, so agents falsely
flap offline. Set a **worker token** (WebUI Settings → Worker token) to
switch this worker to multiplexed mode:

1. On the server (dispatcher): `POST /v1/workers` → save the token once.
2. Bind each directory agent: `POST /v1/workers/{id}/agents`
   `{"agent_id":"..."}` (see server README § Worker multiplexing).
3. Paste the token into WebUI Settings (saved to the user config dir only).

Then the worker sends **one batch heartbeat every ~15s** (background task, so
it keeps flowing while a job runs) plus **one long-poll** for all bound
agents, and still executes only **one job at a time**. Empty worker token =
legacy per-directory mode (unchanged).

## Pi idle wait

After `pi_svc send`, the worker waits for the session to go idle with two
budgets: an **absolute cap** (the job's `timeout_secs`, else **1h**) and an
**inactivity budget** (**10 min** with no `running` state / output change).
Long coding runs keep their lease renewed and stay alive while they work;
a timeout while still `running` is reported as a wait timeout, not a task
failure. Needs more than 1h? Pass `--timeout` on `host-ops job create` / `steer`.

Never uses Cursor CloudAgent / Fast.
