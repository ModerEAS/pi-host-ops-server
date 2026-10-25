# pi-host-ops production deploy (Docker Swarm)

Target: one node runs the **server** (dispatcher HTTP + SQLite); entry is
Traefik on an external network (default `traefik-public`). Release flow: CI
builds the image on green master and pushes to GHCR, then the deploy job SSHes
to the manager and runs `docker stack deploy` (start-first, auto-rollback).
**Never build on the server — pull GHCR only.**

Only the server is containerized. The **worker stays host-installed** (it needs
host `pi` binary / project dirs) via Release scripts — see §6.

This repo is public: domain, entrypoint, certresolver, network name, and deploy
path are all variables (see `deploy/stack.yml` header). No private hostnames
appear here.

## 0. Relation to code

- SQLite migrations run automatically at **server startup** (`sqlx::migrate!`);
  no manual `sqlx migrate run`. Rolling back the image does NOT roll back
  migrations (forward-only — schema only adds, never alters, so old code stays
  compatible).
- Health probe `GET /health` returns `{"ok":true}` (public, no auth, no DB
  check). Traefik/CI only look at it.
- `DISPATCHER_TOKEN` (and optional `AGENT_TOKEN` seed) can come from
  `DISPATCHER_TOKEN_FILE` / `AGENT_TOKEN_FILE` (e.g. `/run/secrets/...`);
  file contents (trimmed) win over the plain env var (see `src/config.rs`).

## 1. First-time setup (on manager, once)

### 1.1 Node label

The stack places the server with `node.labels.hostops.role==app`:

```bash
docker node ls
docker node update --label-add hostops.role=app <server-node-name>
docker node inspect <node> --format '{{.Spec.Labels}}'
```

### 1.2 Data dir (bind mount)

SQLite data lives directly on the host. On the node that will run the server,
create the `DEPLOY_PATH` dir once (root-owned, `755` is enough, e.g.
`sudo mkdir -p -m 755 /opt/host-ops`). The container runs as nobody (65534) and
writes `/data/host-ops.db` there (the image pre-creates `/data` with the right
owner so fresh volumes inherit it).

The dir must be traversable by the container user (at least `755`), otherwise
SQLite fails to open. Back up by tarring the folder (stop the service or accept
crash-consistent copies of `host-ops.db`).

### 1.3 Network

- External net (default `traefik-public`, rename via `TRAEFIK_NETWORK`):
  already exists, Traefik uses it; the stack only references it.

### 1.4 Docker secret (1, external — created by CI on first deploy if absent)

Normally you do nothing: the deploy job creates `host_ops_dispatcher_token`
from the GitHub `production` environment secret `DISPATCHER_TOKEN` when it is
missing, and leaves an existing one untouched. Manual creation is only needed
if you deploy by hand (outside CI):

```bash
printf '%s' '<dispatcher-token>' | docker secret create host_ops_dispatcher_token -
docker secret ls
```

The stack injects it as `DISPATCHER_TOKEN_FILE=/run/secrets/host_ops_dispatcher_token`.
Rotating: `docker secret create` a new name is NOT supported by the stack file —
remove the stack, `docker secret rm`, re-create, re-deploy.

### 1.5 No public network?

If the box has no public domain / Traefik, drop the Traefik part: remove the
`deploy.labels` block and the `traefik-public` network from a local copy of the
stack, and add `ports: ["8787:8787"]` to the service. Everything else
(secret, bind mount, healthcheck) stays the same.

### 1.6 GHCR pull

- Easiest: keep the image **public** (GHCR package → public); the manager needs
  no login and `GHCR_PULL_TOKEN` stays empty.
- If private: create a token with only `read:packages`, store it in the repo
  secret `GHCR_PULL_TOKEN`; deploy runs `docker login ghcr.io` first (token via
  stdin, never in the remote command line).

## 2. GitHub-side config

### 2.1 Variables (Settings → Secrets and variables → Actions → Variables)

| Name | Default (stack fallback) | Meaning |
|---|---|---|
| `HOST_OPS_DOMAIN` | `example.com` | Domain; Traefik Host rule |
| `TRAEFIK_ENTRYPOINT` | `web` | Traefik entrypoint name |
| `TRAEFIK_CERTRESOLVER` | `letsencrypt` | Cert resolver name |
| `TRAEFIK_NETWORK` | `traefik-public` | External network real name (stack reference + `traefik.docker.network` use the same value) |

### 2.2 Secrets (same place, Secrets tab)

| Name | Meaning |
|---|---|
| `DEPLOY_HOST` | Manager IP/hostname |
| `DEPLOY_PORT` | SSH port (optional, default 22) |
| `DEPLOY_USER` | SSH user (can run docker, i.e. in docker group) |
| `DEPLOY_SSH_KEY` | Private key (dedicated deploy key recommended) |
| `DEPLOY_KNOWN_HOSTS` | `ssh-keyscan -p PORT HOST` output; SSH verifies strictly |
| `DEPLOY_PATH` | Dir on manager holding the stack, e.g. `/opt/host-ops` |
| `DISPATCHER_TOKEN` | Dispatcher Bearer; CI creates Swarm secret `host_ops_dispatcher_token` on first deploy if absent (never recreated) |
| `GHCR_PULL_TOKEN` | Optional; see 1.6 |

### 2.3 Environments

Repo Settings → Environments → create `production`, optionally with
**Required reviewers** (at least one Approve before release). The deploy job
declares `environment: production`; without approval it never touches the server.

### 2.4 Trigger rules

Deploy condition: main repo (`ModerRAS/pi-host-ops-server`) + `refs/heads/master`
+ `push` event. Fork master pushes and any PR never trigger (plus the
`environment` second gate). Every master push runs tests → image push
(`sha-<commit>` + `latest`) → serial deploy (`concurrency: deploy-production`).

## 3. Release flow (automatic)

1. `test` green → `image` builds and pushes
   `ghcr.io/moderras/pi-host-ops-server:sha-<commit>` (+ `latest` +
   the upcoming release tag `v<next>`), **in parallel with** `binaries`
   (all release binaries compiled + uploaded as workflow artifacts).
2. `deploy` (needs both, so production approval only blocks here; a failed
   build never reaches deploy): scp `stack.yml` to `DEPLOY_PATH`; inject
   `IMAGE_TAG`/`HOST_OPS_DOMAIN`/Traefik vars; then
   `docker stack deploy -c stack.yml --with-registry-auth host-ops`.
3. Poll `host-ops_server`: spec or running tasks carry the new tag prefix
   (Swarm resolves to `tag@sha256:...`, so prefix-match), a Running task
   exists, `UpdateStatus` is not rollback/paused (10 min timeout fails;
   `failure_action: rollback` already auto-rolled-back by Swarm).
4. Runner `curl https://domain/health` with retries (5 min).
5. `docker image prune -f` cleans old images.
6. `release` (publish phase, after deploy success): creates tag `v<next>`
   at the deployed commit and publishes the GitHub Release (marked latest)
   from the already-built artifacts — **no rebuild**, no manual tag pushing.

### Tag scheme (`v<next>`)

Resolved by the `version` job on every CI run (preview value on PRs,
authoritative on master):

- Latest semver tag `vX.Y.Z` → next is patch+1 (`vX.Y.(Z+1)`).
- If `Cargo.toml` `[package] version` is **greater** than the latest tag,
  it wins as-is — bump Cargo version to jump minor/major.
- No tags yet → Cargo version.
- The release build stamps the crate version to the tag (binaries'
  `--version` matches); the stamp is never committed.

## 4. Rollback

- Automatic: failed updates roll back per `update_config.failure_action: rollback`
  (`rollback_config` configured).
- Manual: `docker service rollback host-ops_server`.
- Pinned rollback: same `docker stack deploy` with an older
  `IMAGE_TAG=sha-<old-commit>` (DEPLOY_PATH still required — the bind path is
  built from it), or re-run the workflow on the old commit (tags are immutable,
  old images remain).
- Note: **migrations don't roll back.** Confirm old code works with the newer
  schema first (current schema only adds tables/columns — safe).

## 5. Ops cheatsheet

```bash
docker stack services host-ops
docker service ps host-ops_server --no-trunc
docker service logs -f host-ops_server
curl -fsS https://<domain>/health
```

DB file on host: `DEPLOY_PATH/host-ops.db`. Backup:
`tar czf /backup/host-ops-$(date +%F).tgz -C <DEPLOY_PATH> host-ops.db`
(stop service or accept crash-consistent). Restore: stop service, extract back.

## 6. Worker stays on the host

The worker (WebUI + owned `pi --mode rpc` children, one process managing many project dirs)
is intentionally NOT in the container stack. Install it from GitHub Releases
(no cargo on the machine):

```bash
# Linux
./scripts/install-worker.sh --run
# Windows
powershell -ExecutionPolicy Bypass -File scripts\install-worker.ps1 -Run
```

Assets are auto-published after each successful production deploy
(`v<next-patch>` scheme, marked latest): `worker-*`, `host-ops-*`,
`pi-host-ops-server-*`, `SHA256SUMS`. To jump minor/major, bump
`Cargo.toml` `[package] version` — it wins over the latest tag.
Point the worker at the deployed server
URL with the agent token. See [worker/README.md](../worker/README.md) and the
top-level README "Run" section.
