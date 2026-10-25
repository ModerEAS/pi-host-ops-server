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
/// Batch heartbeat cadence in multiplexed mode (must stay well under the
/// server HEARTBEAT_TTL, default 45s).
const WORKER_HEARTBEAT_SECS: f64 = 15.0;
/// Single multiplexed long-poll wait (server caps at 25).
const WORKER_POLL_WAIT: u64 = 20;

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
    // Background batch-heartbeat task while multiplexed mode is active.
    let mut hb_task: Option<tokio::task::JoinHandle<()>> = None;

    while !stop.load(Ordering::Relaxed) {
        // Snapshot config each tick so WebUI edits apply without restart.
        let snapshot = state.cfg.lock().await.clone();
        // Worker multiplexed mode: one batch heartbeat (background task) + one
        // poll for all bound agents. Serial execution holds because
        // execute_job blocks this loop — no second poll starts mid-job.
        let mux = !snapshot.worker_token.trim().is_empty()
            && snapshot.directories.iter().any(|d| d.enabled);
        if mux {
            if hb_task.is_none() {
                info!("worker multiplexed mode: batch heartbeat + single poll");
                hb_task = Some(spawn_worker_heartbeat_task(state.clone()));
            }
            if let Err(e) = run_worker_mux_step(&state, &snapshot, &stop).await {
                warn!(error = %e, "worker mux step failed");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            tokio::time::sleep(Duration::from_millis(LOOP_TICK_MS)).await;
            continue;
        }
        if let Some(h) = hb_task.take() {
            h.abort();
            info!("worker token cleared: back to per-directory poll mode");
        }
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

/// Background batch heartbeat for multiplexed mode: every ~15s send one
/// request covering all enabled + registered dirs, so siblings stay online
/// (age << HEARTBEAT_TTL) even while one dir runs a long job.
fn spawn_worker_heartbeat_task(state: Arc<AppState>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs_f64(WORKER_HEARTBEAT_SECS));
        loop {
            tick.tick().await;
            let snapshot = state.cfg.lock().await.clone();
            let token = snapshot.worker_token.clone();
            if token.trim().is_empty() {
                continue;
            }
            let statuses = state.status.lock().await.directories.clone();
            let mut agents: Vec<(String, String)> = Vec::new();
            for dir in snapshot.directories.iter().filter(|d| d.enabled) {
                let (Some(aid), Some(_)) =
                    (dir.agent_id.clone(), dir.agent_token.clone())
                else {
                    continue;
                };
                if aid.trim().is_empty() {
                    continue;
                }
                let st = statuses
                    .get(&dir.id)
                    .map(|s| s.state.as_str())
                    .unwrap_or("online");
                agents.push((
                    aid,
                    if st == "busy" { "busy".into() } else { "online".into() },
                ));
            }
            if agents.is_empty() {
                continue;
            }
            match ApiClient::with_worker(&snapshot.host_ops_url, &token) {
                Ok(api) => api.worker_heartbeat(&agents).await,
                Err(e) => warn!(error = %e, "worker heartbeat client"),
            }
        }
    })
}

/// One multiplexed step: register dirs (as today), then a single poll for
/// any bound agent; dispatch a claimed job to its directory and run it with
/// that agent's token (results/events/lease stay per-agent).
async fn run_worker_mux_step(
    state: &Arc<AppState>,
    snapshot: &WorkerConfig,
    stop: &AtomicBool,
) -> Result<()> {
    let mut registered: Vec<(DirEntry, String, String)> = Vec::new();
    for dir in snapshot.directories.iter().filter(|d| d.enabled) {
        if let Err(e) = reject_fast(&dir.provider, &dir.model) {
            set_dir_status(
                state,
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
        match ensure_registered(state, snapshot, dir).await {
            Ok((agent_id, agent_token, entry)) => {
                set_dir_status(
                    state,
                    &dir.id,
                    DirStatus {
                        state: "online".into(),
                        agent_id: Some(agent_id.clone()),
                        last_error: None,
                        ..Default::default()
                    },
                )
                .await;
                registered.push((entry, agent_id, agent_token));
            }
            Err(e) => {
                set_dir_status(
                    state,
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
    if registered.is_empty() {
        return Ok(());
    }

    let worker_api = ApiClient::with_worker(&snapshot.host_ops_url, &snapshot.worker_token)?;
    match worker_api.worker_poll(WORKER_POLL_WAIT).await {
        Ok(None) => {}
        Ok(Some((job, agent_id))) => {
            let Some((entry, _, agent_token)) =
                registered.iter().find(|(_, aid, _)| aid == &agent_id)
            else {
                // Bound on the server but no local dir (removed/disabled after
                // bind): can't run it here; the lease will reap it. Unbind the
                // agent or re-enable the dir to fix.
                warn!(%agent_id, job = %job.id, "claimed job for unknown local dir; leaving for lease expiry");
                return Ok(());
            };
            // Find dir id for status updates.
            let dir_id = snapshot
                .directories
                .iter()
                .find(|d| d.agent_id.as_deref() == Some(agent_id.as_str()))
                .map(|d| d.id.clone())
                .unwrap_or_default();
            set_dir_status(
                state,
                &dir_id,
                DirStatus {
                    state: "busy".into(),
                    last_job_id: Some(job.id.to_string()),
                    ..Default::default()
                },
            )
            .await;
            let api = ApiClient::new(&snapshot.host_ops_url, agent_token, &agent_id)?;
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
            let (ok, result, error) = runtime::execute_job(&api, &rt, &job, stop).await;
            if let Err(e) = api
                .submit_result(&job.id.to_string(), ok, result, error.clone())
                .await
            {
                error!(error = %e, "submit_result");
            }
            set_dir_status(
                state,
                &dir_id,
                DirStatus {
                    state: if ok { "online".into() } else { "error".into() },
                    last_error: error,
                    ..Default::default()
                },
            )
            .await;
        }
        Err(e) => {
            warn!(error = %e, "worker poll failed");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
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
