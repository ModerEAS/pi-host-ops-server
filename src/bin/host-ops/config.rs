//! Dispatcher CLI config in user config dir (never project litter).
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

pub const DEFAULT_URL: &str = "http://127.0.0.1:8787";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DispatcherConfig {
    #[serde(default = "default_url")]
    pub url: String,
    #[serde(default)]
    pub token: String,
}

fn default_url() -> String {
    DEFAULT_URL.into()
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            url: DEFAULT_URL.into(),
            token: String::new(),
        }
    }
}

pub fn reject_fast(provider: Option<&str>, model: Option<&str>) -> Result<()> {
    for v in [provider, model].into_iter().flatten() {
        if v.to_lowercase().contains("fast") {
            bail!("provider/model must not be Fast (cost rule)");
        }
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
    Ok(config_dir()?.join("dispatcher.json"))
}

/// Load file (if any), then apply env overrides. Env wins: `HOST_OPS_URL`, `DISPATCHER_TOKEN`.
pub fn load() -> Result<(DispatcherConfig, PathBuf)> {
    let path = config_path()?;
    let mut cfg = if path.is_file() {
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("read {}", path.display()))?;
        serde_json::from_str::<DispatcherConfig>(&raw)
            .with_context(|| format!("parse {}", path.display()))?
    } else {
        DispatcherConfig::default()
    };

    if let Ok(url) = std::env::var("HOST_OPS_URL") {
        let t = url.trim();
        if !t.is_empty() {
            cfg.url = t.to_string();
        }
    }
    if let Ok(token) = std::env::var("DISPATCHER_TOKEN") {
        if !token.is_empty() {
            cfg.token = token;
        }
    }

    cfg.url = cfg.url.trim_end_matches('/').to_string();
    if cfg.url.is_empty() {
        cfg.url = DEFAULT_URL.into();
    }
    Ok((cfg, path))
}

pub fn save(cfg: &DispatcherConfig) -> Result<PathBuf> {
    let path = config_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut to_write = cfg.clone();
    to_write.url = to_write.url.trim_end_matches('/').to_string();
    let raw = serde_json::to_string_pretty(&to_write)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, raw)?;
    std::fs::rename(&tmp, &path)?;
    Ok(path)
}

pub fn mask_token(token: &str) -> String {
    if token.is_empty() {
        return "(empty)".into();
    }
    if token.len() <= 8 {
        return "****".into();
    }
    format!("{}…{}", &token[..4], &token[token.len() - 4..])
}
