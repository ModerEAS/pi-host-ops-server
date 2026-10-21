//! host-ops local worker: embedded WebUI + multi-directory agents.
//!
//! Config: ~/.config/pi-host-ops/config.json (Linux/mac) or %APPDATA%\pi-host-ops\config.json (Windows).
//! Never writes into project trees.
//!
//! Default model: muse-spark-1.3-contributor. Fast is blocked.

mod config;
mod runtime;
mod webui;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use config::{
    default_session_for, load, new_dir_id, reject_fast, save, DirEntry, DirStatus, WorkerConfig,
    DEFAULT_MODEL, DEFAULT_PROVIDER,
};
use runtime::{ApiClient, DirRuntime};
use webui::{set_dir_status, AppState};

const POLL_WAIT: u64 = 8;
const HEARTBEAT_SECS: f64 = 20.0;
const LOOP_TICK_MS: u64 = 500;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let (cfg, cfg_path) = load().context("load worker config")?;
    info!(
        config = %cfg_path.display(),
        webui = %cfg.webui_bind,
        dirs = cfg.directories.len(),
        "host-ops worker starting"
    );

    let state = Arc::new(AppState {
        cfg: tokio::sync::Mutex::new(cfg.clone()),
        cfg_path: cfg_path.clone(),
        status: tokio::sync::Mutex::new(Default::default()),
    });

    let stop = Arc::new(AtomicBool::new(false));
    let stop_sig = stop.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = sigterm.recv() => {},
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        info!("signal received, shutting down after current jobs");
        stop_sig.store(true, Ordering::Relaxed);
    });

    let bind: SocketAddr = cfg
        .webui_bind
        .parse()
        .with_context(|| format!("parse webui_bind {}", cfg.webui_bind))?;
    let app = webui::router(state.clone());
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("bind WebUI {bind}"))?;
    info!(%bind, "WebUI listening — open http://{bind}/");
    let webui_stop = stop.clone();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                while !webui_stop.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            })
            .await
            .ok();
    });

    let mut last_hb: std::collections::HashMap<String, Instant> = std::collections::HashMap::new();

    while !stop.load(Ordering::Relaxed) {
        // Snapshot config each tick so WebUI edits apply without restart.
        let snapshot = state.cfg.lock().await.clone();
        for dir in snapshot.directories.iter().filter(|d| d.enabled) {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            if let Err(e) = reject_fast(&dir.provider, &dir.model) {
                set_dir_status(
                    &state,
                    &dir.id,
                    DirStatus {
                        state: "error".into(),
                        last_error: Some(e.to_string()),
                        ..Default::default()
                    },
                )
                .await;
                continue;
            }

            match ensure_registered(&state, &snapshot, dir).await {
                Ok((agent_id, agent_token, entry)) => {
                    set_dir_status(
                        &state,
                        &dir.id,
                        DirStatus {
                            state: "online".into(),
                            agent_id: Some(agent_id.clone()),
                            last_error: None,
                            ..Default::default()
                        },
                    )
                    .await;

                    let api = match ApiClient::new(&snapshot.host_ops_url, &agent_token, &agent_id) {
                        Ok(a) => a,
                        Err(e) => {
                            warn!(error = %e, "api client");
                            continue;
                        }
                    };

                    let hb_due = last_hb
                        .get(&dir.id)
                        .map(|t| t.elapsed().as_secs_f64() >= HEARTBEAT_SECS)
                        .unwrap_or(true);
                    if hb_due {
                        api.heartbeat().await;
                        last_hb.insert(dir.id.clone(), Instant::now());
                    }

                    let pi_svc = snapshot
                        .pi_svc
                        .as_ref()
                        .map(PathBuf::from)
                        .filter(|p| !p.as_os_str().is_empty());
                    let session = entry
                        .pi_session
                        .clone()
                        .unwrap_or_else(|| default_session_for(&entry.label, &entry.id));
                    let rt = DirRuntime {
                        host_ops_url: snapshot.host_ops_url.clone(),
                        agent_token: agent_token.clone(),
                        agent_id: agent_id.clone(),
                        work_dir: PathBuf::from(&entry.path),
                        pi_svc,
                        pi_session: session,
                        provider: entry.provider.clone(),
                        model: entry.model.clone(),
                        event_interval_secs: 5.0,
                    };

                    match api.poll_once(POLL_WAIT).await {
                        Ok(None) => {}
                        Ok(Some(job)) => {
                            set_dir_status(
                                &state,
                                &dir.id,
                                DirStatus {
                                    state: "busy".into(),
                                    last_job_id: Some(job.id.to_string()),
                                    ..Default::default()
                                },
                            )
                            .await;
                            let (ok, result, error) =
                                runtime::execute_job(&api, &rt, &job, &stop).await;
                            if let Err(e) = api
                                .submit_result(&job.id.to_string(), ok, result, error.clone())
                                .await
                            {
                                error!(error = %e, "submit_result");
                            }
                            set_dir_status(
                                &state,
                                &dir.id,
                                DirStatus {
                                    state: if ok { "online".into() } else { "error".into() },
                                    last_error: error,
                                    ..Default::default()
                                },
                            )
                            .await;
                        }
                        Err(e) => {
                            warn!(dir = %dir.label, error = %e, "poll failed");
                            set_dir_status(
                                &state,
                                &dir.id,
                                DirStatus {
                                    state: "error".into(),
                                    last_error: Some(e.to_string()),
                                    ..Default::default()
                                },
                            )
                            .await;
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        }
                    }
                }
                Err(e) => {
                    set_dir_status(
                        &state,
                        &dir.id,
                        DirStatus {
                            state: "needs_bootstrap".into(),
                            last_error: Some(e.to_string()),
                            ..Default::default()
                        },
                    )
                    .await;
                }
            }
        }

        // If no enabled dirs, don't spin hard.
        if snapshot.directories.iter().filter(|d| d.enabled).count() == 0 {
            tokio::time::sleep(Duration::from_millis(800)).await;
        } else {
            tokio::time::sleep(Duration::from_millis(LOOP_TICK_MS)).await;
        }
    }

    info!("worker stopped");
    Ok(())
}

async fn ensure_registered(
    state: &AppState,
    snapshot: &WorkerConfig,
    dir: &DirEntry,
) -> Result<(String, String, DirEntry)> {
    // Already have token in config.
    if let (Some(aid), Some(tok)) = (dir.agent_id.as_ref(), dir.agent_token.as_ref()) {
        if !aid.is_empty() && !tok.is_empty() {
            let api = ApiClient::new(&snapshot.host_ops_url, tok, aid)?;
            // Best-effort sync cwd/model to server.
            if let Err(e) = api
                .patch_me(&dir.path, &dir.provider, &dir.model)
                .await
            {
                warn!(agent = %aid, error = %e, "PATCH /v1/agent/me failed (continuing)");
            }
            return Ok((aid.clone(), tok.clone(), dir.clone()));
        }
    }

    if snapshot.dispatcher_token.trim().is_empty() {
        bail!(
            "directory '{}' has no agent_token; set Dispatcher token in WebUI settings to auto-register",
            dir.label
        );
    }

    let disp = ApiClient::with_dispatcher(&snapshot.host_ops_url, &snapshot.dispatcher_token)?;
    let mut agent_id = dir
        .agent_id
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("cwd-{}", &new_dir_id()[4..]));
    // Prefer stable-ish id from path hash
    if dir.agent_id.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
        let h = short_hash(&dir.path);
        let base = std::path::Path::new(&dir.path)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("proj");
        let slug: String = base
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect();
        agent_id = format!("{slug}-{h}");
        agent_id = agent_id.chars().take(48).collect();
    }

    let provider = if dir.provider.trim().is_empty() {
        DEFAULT_PROVIDER
    } else {
        dir.provider.as_str()
    };
    let model = if dir.model.trim().is_empty() {
        DEFAULT_MODEL
    } else {
        dir.model.as_str()
    };
    reject_fast(provider, model)?;

    let (id, token) = match disp
        .create_agent(
            &agent_id,
            &dir.label,
            &dir.label,
            &dir.path,
            provider,
            model,
        )
        .await
    {
        Ok(v) => v,
        Err(e) if e.to_string().contains("already exists") => {
            // Collision: new id
            let alt = format!("{agent_id}-{}", &new_dir_id()[4..]);
            disp.create_agent(&alt, &dir.label, &dir.label, &dir.path, provider, model)
                .await?
        }
        Err(e) => return Err(e),
    };

    // Persist into user config (not project tree).
    {
        let mut cfg = state.cfg.lock().await;
        if let Some(d) = cfg.directories.iter_mut().find(|d| d.id == dir.id) {
            d.agent_id = Some(id.clone());
            d.agent_token = Some(token.clone());
            if d.pi_session.is_none() {
                d.pi_session = Some(default_session_for(&d.label, &d.id));
            }
            if d.provider.trim().is_empty() {
                d.provider = provider.to_string();
            }
            if d.model.trim().is_empty() {
                d.model = model.to_string();
            }
        }
        save(&cfg)?;
    }

    let mut entry = dir.clone();
    entry.agent_id = Some(id.clone());
    entry.agent_token = Some(token.clone());
    info!(agent_id = %id, path = %dir.path, "registered directory agent");
    Ok((id, token, entry))
}

fn short_hash(s: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    let bytes = h.finalize();
    hex::encode(&bytes[..4])
}
