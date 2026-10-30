use anyhow::{bail, Context, Result};
use rand::RngCore;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct Config {
    pub bind: SocketAddr,
    pub database_url: String,
    pub dispatcher_token: String,
    /// Optional seed token for agent id=`default` (backward compat / bootstrap).
    pub agent_token_seed: Option<String>,
    pub max_result_bytes: usize,
    pub data_dir: PathBuf,
    /// Agent considered offline if last_seen older than this (default 45).
    pub heartbeat_ttl_secs: i64,
    /// Lease duration on claim / renew (default 60).
    pub lease_secs: i64,
    /// Terminal-job webhook target. None/empty = disabled (default off).
    pub webhook_url: Option<String>,
    /// Header name for webhook auth (default `Authorization`).
    pub webhook_auth_header: String,
    /// Secret value sent as webhook auth header. Never log.
    pub webhook_auth: Option<String>,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("bind", &self.bind)
            .field("database_url", &self.database_url)
            .field("dispatcher_token", &"[redacted]")
            .field("agent_token_seed", &self.agent_token_seed.as_ref().map(|_| "[redacted]"))
            .field("max_result_bytes", &self.max_result_bytes)
            .field("data_dir", &self.data_dir)
            .field("heartbeat_ttl_secs", &self.heartbeat_ttl_secs)
            .field("lease_secs", &self.lease_secs)
            .field("webhook_url", &self.webhook_url)
            .field("webhook_auth_header", &self.webhook_auth_header)
            // Never print the secret itself.
            .field("webhook_auth", &self.webhook_auth.as_ref().map(|_| "[redacted]"))
            .finish()
    }
}

impl Config {
    /// Global switch: webhook fires only when a URL is configured.
    pub fn webhook_enabled(&self) -> bool {
        self.webhook_url.as_ref().map(|u| !u.trim().is_empty()).unwrap_or(false)
    }
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let _ = dotenvy::dotenv();

        let data_dir = PathBuf::from("data");
        fs::create_dir_all(&data_dir).context("create data dir")?;

        let bind: SocketAddr = std::env::var("BIND")
            .unwrap_or_else(|_| "0.0.0.0:8787".into())
            .parse()
            .context("parse BIND")?;

        let database_url =
            std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:data/host-ops.db".into());

        ensure_sqlite_parent(&database_url)?;

        let max_result_bytes: usize = std::env::var("MAX_RESULT_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(512 * 1024);

        let heartbeat_ttl_secs: i64 = std::env::var("HEARTBEAT_TTL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(45);

        let lease_secs: i64 = std::env::var("LEASE_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(60);

        let (dispatcher_token, agent_token_seed) = load_or_bootstrap_tokens(&data_dir)?;

        let webhook_url = std::env::var("HOST_OPS_WEBHOOK_URL")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let webhook_auth_header = std::env::var("HOST_OPS_WEBHOOK_HEADER")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "Authorization".to_string());
        // Secret via file wins (Swarm secret), plain env fallback. Never logged.
        let webhook_auth = read_secret_or_env("HOST_OPS_WEBHOOK_AUTH_FILE", "HOST_OPS_WEBHOOK_AUTH")?
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        Ok(Self {
            bind,
            database_url,
            dispatcher_token,
            agent_token_seed,
            max_result_bytes,
            data_dir,
            heartbeat_ttl_secs,
            lease_secs,
            webhook_url,
            webhook_auth_header,
            webhook_auth,
        })
    }
}

fn ensure_sqlite_parent(database_url: &str) -> Result<()> {
    let path = database_url
        .strip_prefix("sqlite:")
        .or_else(|| database_url.strip_prefix("sqlite://"))
        .unwrap_or(database_url);
    // Keep absolute paths absolute (/data/... in containers); only strip a
    // leading "//" left over from the sqlite:// form.
    let path = path.strip_prefix("//").unwrap_or(path);
    let p = Path::new(path);
    if let Some(parent) = p.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| format!("create db parent {parent:?}"))?;
        }
    }
    Ok(())
}

/// Returns (dispatcher_token, optional AGENT_TOKEN seed for default agent).
///
/// Docker Swarm secret support: `DISPATCHER_TOKEN_FILE` / `AGENT_TOKEN_FILE`
/// point at a file (e.g. `/run/secrets/host_ops_dispatcher_token`); the file
/// contents (trimmed) win over the plain env var so secrets never appear in
/// `docker service inspect` output.
fn load_or_bootstrap_tokens(data_dir: &Path) -> Result<(String, Option<String>)> {
    let env_dispatcher = read_secret_or_env("DISPATCHER_TOKEN_FILE", "DISPATCHER_TOKEN")?;
    let env_agent = read_secret_or_env("AGENT_TOKEN_FILE", "AGENT_TOKEN")?;

    if let Some(d) = env_dispatcher.clone() {
        // Env dispatcher wins; agent seed optional
        return Ok((d, env_agent));
    }

    let bootstrap = data_dir.join("bootstrap-tokens.txt");
    if bootstrap.exists() {
        let (d, a) = read_bootstrap_file(&bootstrap)?;
        let dispatcher = env_dispatcher.unwrap_or(d);
        let agent = env_agent.or(a);
        return Ok((dispatcher, agent));
    }

    let dispatcher = env_dispatcher.unwrap_or_else(random_token);
    let agent = env_agent.unwrap_or_else(random_token);

    let contents = format!(
        "# Generated once by pi-host-ops-server. Keep secret.\n\
         # DISPATCHER_TOKEN: create/list jobs + manage agents.\n\
         # AGENT_TOKEN: seeded into agents.id=default (sha256 stored). Prefer POST /v1/agents for new Pis.\n\
         DISPATCHER_TOKEN={dispatcher}\n\
         AGENT_TOKEN={agent}\n"
    );
    fs::write(&bootstrap, &contents).context("write bootstrap-tokens.txt")?;

    eprintln!("============================================================");
    eprintln!("No DISPATCHER_TOKEN in env.");
    eprintln!("Generated tokens and wrote them to {}:", bootstrap.display());
    eprintln!("  DISPATCHER_TOKEN={dispatcher}");
    eprintln!("  AGENT_TOKEN={agent}  (seed for agent id=default)");
    eprintln!("Create more agents with POST /v1/agents (dispatcher auth).");
    eprintln!("============================================================");

    Ok((dispatcher, Some(agent)))
}

/// Read a secret from `<NAME>_FILE` (file path in env) or fall back to `<NAME>`.
/// File contents are trimmed (trailing newline from `printf`/`docker secret`).
fn read_secret_or_env(file_env: &str, plain_env: &str) -> Result<Option<String>> {
    if let Ok(path) = std::env::var(file_env) {
        if !path.is_empty() {
            let raw = fs::read_to_string(&path)
                .with_context(|| format!("read secret file {file_env}={path}"))?;
            let trimmed = raw.trim().to_string();
            if !trimmed.is_empty() {
                return Ok(Some(trimmed));
            }
        }
    }
    Ok(std::env::var(plain_env).ok().filter(|s| !s.is_empty()))
}

fn read_bootstrap_file(path: &Path) -> Result<(String, Option<String>)> {
    let text = fs::read_to_string(path).context("read bootstrap-tokens.txt")?;
    let mut dispatcher = None;
    let mut agent = None;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(v) = line.strip_prefix("DISPATCHER_TOKEN=") {
            dispatcher = Some(v.trim().to_string());
        } else if let Some(v) = line.strip_prefix("AGENT_TOKEN=") {
            agent = Some(v.trim().to_string());
        }
    }
    match dispatcher {
        Some(d) if !d.is_empty() => Ok((d, agent.filter(|s| !s.is_empty()))),
        _ => bail!("bootstrap-tokens.txt missing DISPATCHER_TOKEN"),
    }
}

pub fn random_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

pub fn hash_token(token: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}
