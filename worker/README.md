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
3. Jobs run with that directory as cwd; `pi_svc start --provider --model` uses the directory’s model
4. Without `pi_svc`, only `shell` jobs succeed (typical early Windows setup)

Never uses Cursor CloudAgent / Fast.
