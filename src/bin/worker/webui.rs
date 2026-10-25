//! Local WebUI for managing directories / models (served by the worker binary).
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;

use crate::config::{
    canonicalize_path, default_session_for, new_dir_id, public_config, reject_fast, save,
    DirEntry, DirStatus, RuntimeStatus, WorkerConfig, DEFAULT_MODEL, DEFAULT_PROVIDER,
};

pub struct AppState {
    pub cfg: Mutex<WorkerConfig>,
    pub cfg_path: std::path::PathBuf,
    pub status: Mutex<RuntimeStatus>,
}

const INDEX_HTML: &str = include_str!("../../../worker/webui/index.html");

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/config", get(get_config))
        .route("/api/status", get(get_status))
        .route("/api/settings", axum::routing::put(put_settings))
        .route("/api/directories", get(list_dirs).post(add_dir))
        .route(
            "/api/directories/{id}",
            axum::routing::patch(patch_dir).delete(delete_dir),
        )
        .route("/api/reload", post(reload_noop))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn get_config(State(st): State<Arc<AppState>>) -> Response {
    let cfg = st.cfg.lock().await;
    Json(public_config(&cfg, &st.cfg_path)).into_response()
}

async fn get_status(State(st): State<Arc<AppState>>) -> Response {
    let status = st.status.lock().await;
    let dirs: Vec<_> = status
        .directories
        .iter()
        .map(|(id, d)| {
            json!({
                "id": id,
                "state": d.state,
                "last_error": d.last_error,
                "last_job_id": d.last_job_id,
                "agent_id": d.agent_id,
            })
        })
        .collect();
    Json(json!({ "directories": dirs })).into_response()
}

#[derive(Debug, Deserialize)]
struct SettingsBody {
    host_ops_url: Option<String>,
    webui_bind: Option<String>,
    dispatcher_token: Option<String>,
    worker_token: Option<String>,
    worker_id: Option<Option<String>>,
    pi_svc: Option<Option<String>>,
}

async fn put_settings(State(st): State<Arc<AppState>>, Json(body): Json<SettingsBody>) -> Response {
    let mut cfg = st.cfg.lock().await;
    if let Some(u) = body.host_ops_url {
        cfg.host_ops_url = u.trim().trim_end_matches('/').to_string();
    }
    if let Some(b) = body.webui_bind {
        let b = b.trim().to_string();
        if !b.is_empty() {
            cfg.webui_bind = b;
        }
    }
    if let Some(t) = body.dispatcher_token {
        cfg.dispatcher_token = t;
    }
    if let Some(t) = body.worker_token {
        cfg.worker_token = t;
    }
    if let Some(w) = body.worker_id {
        cfg.worker_id = w.filter(|s| !s.trim().is_empty());
    }
    if let Some(p) = body.pi_svc {
        cfg.pi_svc = p.and_then(|s| {
            let s = s.trim().to_string();
            if s.is_empty() {
                None
            } else {
                Some(s)
            }
        });
    }
    match save(&cfg) {
        Ok(path) => Json(json!({"ok": true, "config_path": path.display().to_string()})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn list_dirs(State(st): State<Arc<AppState>>) -> Response {
    let cfg = st.cfg.lock().await;
    Json(json!(cfg.directories)).into_response()
}

#[derive(Debug, Deserialize)]
struct AddDirBody {
    label: Option<String>,
    path: String,
    provider: Option<String>,
    model: Option<String>,
    enabled: Option<bool>,
}

async fn add_dir(State(st): State<Arc<AppState>>, Json(body): Json<AddDirBody>) -> Response {
    let path = match canonicalize_path(&body.path) {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    let provider = body
        .provider
        .unwrap_or_else(|| DEFAULT_PROVIDER.to_string());
    let model = body.model.unwrap_or_else(|| DEFAULT_MODEL.to_string());
    if let Err(e) = reject_fast(&provider, &model) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response();
    }
    let id = new_dir_id();
    let label = body
        .label
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| {
            std::path::Path::new(&path)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or(&id)
                .to_string()
        });
    let entry = DirEntry {
        id: id.clone(),
        label: label.clone(),
        path,
        enabled: body.enabled.unwrap_or(true),
        provider,
        model,
        agent_id: None,
        agent_token: None,
        pi_session: Some(default_session_for(&label, &id)),
    };
    let mut cfg = st.cfg.lock().await;
    if cfg.directories.iter().any(|d| d.path == entry.path) {
        return (
            StatusCode::CONFLICT,
            Json(json!({"error": "directory already configured"})),
        )
            .into_response();
    }
    cfg.directories.push(entry.clone());
    if let Err(e) = save(&cfg) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response();
    }
    (StatusCode::CREATED, Json(entry)).into_response()
}

#[derive(Debug, Deserialize)]
struct PatchDirBody {
    label: Option<String>,
    path: Option<String>,
    enabled: Option<bool>,
    provider: Option<String>,
    model: Option<String>,
    pi_session: Option<String>,
}

async fn patch_dir(
    State(st): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<PatchDirBody>,
) -> Response {
    let mut cfg = st.cfg.lock().await;
    let Some(dir) = cfg.directories.iter_mut().find(|d| d.id == id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "directory not found"})),
        )
            .into_response();
    };
    if let Some(l) = body.label {
        if !l.trim().is_empty() {
            dir.label = l;
        }
    }
    if let Some(p) = body.path {
        match canonicalize_path(&p) {
            Ok(p) => dir.path = p,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        }
    }
    if let Some(e) = body.enabled {
        dir.enabled = e;
    }
    if let Some(p) = body.provider {
        if let Err(e) = reject_fast(&p, &dir.model) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
        dir.provider = p;
    }
    if let Some(m) = body.model {
        if let Err(e) = reject_fast(&dir.provider, &m) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
        dir.model = m;
    }
    if let Some(s) = body.pi_session {
        if !s.trim().is_empty() {
            dir.pi_session = Some(s);
        }
    }
    let out = dir.clone();
    if let Err(e) = save(&cfg) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response();
    }
    Json(out).into_response()
}

async fn delete_dir(State(st): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    let mut cfg = st.cfg.lock().await;
    let before = cfg.directories.len();
    cfg.directories.retain(|d| d.id != id);
    if cfg.directories.len() == before {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "directory not found"})),
        )
            .into_response();
    }
    if let Err(e) = save(&cfg) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response();
    }
    let mut status = st.status.lock().await;
    status.directories.remove(&id);
    Json(json!({"ok": true, "id": id})).into_response()
}

async fn reload_noop() -> Response {
    Json(json!({"ok": true})).into_response()
}

pub async fn set_dir_status(st: &AppState, id: &str, patch: DirStatus) {
    let mut status = st.status.lock().await;
    let e = status.directories.entry(id.to_string()).or_default();
    if !patch.state.is_empty() {
        e.state = patch.state;
    }
    if patch.last_error.is_some() {
        e.last_error = patch.last_error;
    }
    if patch.last_job_id.is_some() {
        e.last_job_id = patch.last_job_id;
    }
    if patch.agent_id.is_some() {
        e.agent_id = patch.agent_id;
    }
}
