//! User-config-dir persistence (never project litter).
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const DEFAULT_PROVIDER: &str = "opencode-go";
pub const DEFAULT_MODEL: &str = "muse-spark-1.3-contributor";
pub const DEFAULT_WEBUI_BIND: &str = "127.0.0.1:8788";
pub const DEFAULT_HOST_OPS_URL: &str = "http://127.0.0.1:8787";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    #[serde(default = "default_host")]
    pub host_ops_url: String,
    #[serde(default)]
    pub dispatcher_token: String,
    #[serde(default = "default_webui")]
    pub webui_bind: String,
    /// Optional path to pi_svc; if missing, non-shell jobs fail clearly.
    #[serde(default)]
    pub pi_svc: Option<String>,
    /// Worker credential for multiplexed heartbeat + poll (one batch + one
    /// poll covering all enabled/registered dirs). When empty, the worker
    /// falls back to legacy per-directory heartbeat + poll.
    #[serde(default)]
    pub worker_token: String,
    /// Informational worker id (shown in WebUI); auth uses worker_token.
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub directories: Vec<DirEntry>,
}

fn default_host() -> String {
    DEFAULT_HOST_OPS_URL.into()
}
fn default_webui() -> String {
    DEFAULT_WEBUI_BIND.into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirEntry {
    pub id: String,
    pub label: String,
    pub path: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_provider")]
    pub provider: String,
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub agent_token: Option<String>,
    #[serde(default)]
    pub pi_session: Option<String>,
}

fn default_true() -> bool {
    true
}
fn default_provider() -> String {
    DEFAULT_PROVIDER.into()
}
fn default_model() -> String {
    DEFAULT_MODEL.into()
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            host_ops_url: DEFAULT_HOST_OPS_URL.into(),
            dispatcher_token: String::new(),
            webui_bind: DEFAULT_WEBUI_BIND.into(),
            pi_svc: None,
            worker_token: String::new(),
            worker_id: None,
            directories: Vec::new(),
        }
    }
}

pub fn reject_fast(provider: &str, model: &str) -> Result<()> {
    if provider.to_lowercase().contains("fast") || model.to_lowercase().contains("fast") {
        bail!("provider/model must not be Fast (cost rule)");
    }
    Ok(())
}

pub fn config_dir() -> Result<PathBuf> {
    if let Ok(p) = std::env::var("PI_HOST_OPS_CONFIG_DIR") {
        let pb = PathBuf::from(p);
        std::fs::create_dir_all(&pb)?;
        return Ok(pb);
    }
    #[cfg(windows)]
    {
        let base = std::env::var("APPDATA").unwrap_or_else(|_| ".".into());
        let pb = PathBuf::from(base).join("pi-host-ops");
        std::fs::create_dir_all(&pb)?;
        Ok(pb)
    }
    #[cfg(not(windows))]
    {
        let base = std::env::var("XDG_CONFIG_HOME").unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
            format!("{home}/.config")
        });
        let pb = PathBuf::from(base).join("pi-host-ops");
        std::fs::create_dir_all(&pb)?;
        Ok(pb)
    }
}

pub fn config_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("config.json"))
}

pub fn load() -> Result<(WorkerConfig, PathBuf)> {
    let path = config_path()?;
    if !path.is_file() {
        let cfg = WorkerConfig::default();
        save(&cfg)?;
        return Ok((cfg, path));
    }
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("read {}", path.display()))?;
    let mut cfg: WorkerConfig = serde_json::from_str(&raw)
        .with_context(|| format!("parse {}", path.display()))?;
    cfg.host_ops_url = cfg.host_ops_url.trim_end_matches('/').to_string();
    for d in &mut cfg.directories {
        reject_fast(&d.provider, &d.model)?;
        if d.provider.trim().is_empty() {
            d.provider = DEFAULT_PROVIDER.into();
        }
        if d.model.trim().is_empty() {
            d.model = DEFAULT_MODEL.into();
        }
    }
    Ok((cfg, path))
}

pub fn save(cfg: &WorkerConfig) -> Result<PathBuf> {
    let path = config_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let raw = serde_json::to_string_pretty(cfg)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, raw)?;
    std::fs::rename(&tmp, &path)?;
    Ok(path)
}

pub fn canonicalize_path(p: &str) -> Result<String> {
    let pb = PathBuf::from(p);
    if pb.as_os_str().is_empty() {
        bail!("path is required");
    }
    let abs = if pb.is_absolute() {
        pb
    } else {
        std::env::current_dir()?.join(pb)
    };
    // Prefer canonicalize when path exists; otherwise keep absolute cleaned path.
    let out = abs.canonicalize().unwrap_or(abs);
    Ok(out.display().to_string())
}

pub fn new_dir_id() -> String {
    format!("dir-{}", &Uuid::new_v4().to_string()[..8])
}

pub fn default_session_for(label: &str, id: &str) -> String {
    let slug: String = label
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '-' })
        .collect();
    let slug = slug.trim_matches('-');
    let slug = if slug.is_empty() { id } else { slug };
    format!("host-ops-{slug}")
}

pub fn public_config(cfg: &WorkerConfig, path: &Path) -> serde_json::Value {
    serde_json::json!({
        "host_ops_url": cfg.host_ops_url,
        "dispatcher_token": cfg.dispatcher_token,
        "worker_token": cfg.worker_token,
        "worker_id": cfg.worker_id,
        "webui_bind": cfg.webui_bind,
        "pi_svc": cfg.pi_svc,
        "directories": cfg.directories,
        "config_path": path.display().to_string(),
        "defaults": {
            "provider": DEFAULT_PROVIDER,
            "model": DEFAULT_MODEL,
        }
    })
}

#[derive(Debug, Default, Clone)]
pub struct RuntimeStatus {
    pub directories: HashMap<String, DirStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DirStatus {
    pub state: String,
    pub last_error: Option<String>,
    pub last_job_id: Option<String>,
    pub agent_id: Option<String>,
}
