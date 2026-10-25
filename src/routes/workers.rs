use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use uuid::Uuid;

use crate::auth::{require_dispatcher, AuthUser};
use crate::config::random_token;
use crate::db::BindOutcome;
use crate::models::{BindAgentRequest, CreateWorkerRequest, CreateWorkerResponse, WorkerInfo};
use crate::AppState;

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

pub async fn create_worker(
    State(state): State<AppState>,
    user: AuthUser,
    Json(body): Json<CreateWorkerRequest>,
) -> Response {
    if let Err(e) = require_dispatcher(&user) {
        return e.into_response();
    }

    let id = body
        .id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("worker-{}", &Uuid::new_v4().to_string()[..8]));

    if !valid_id(&id) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "id must be alphanumeric/dash/underscore" })),
        )
            .into_response();
    }

    if state.db.get_worker(&id).await.map(|w| w.is_some()).unwrap_or(false) {
        return (
            StatusCode::CONFLICT,
            Json(json!({ "error": "worker id already exists" })),
        )
            .into_response();
    }

    let label = body
        .label
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(&id)
        .to_string();
    let token = random_token();

    match state.db.create_worker(&id, &token, &label).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(CreateWorkerResponse { id, token, label }),
        )
            .into_response(),
        Err(err) => {
            tracing::error!(?err, "create worker failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to create worker" })),
            )
                .into_response()
        }
    }
}

pub async fn list_workers(State(state): State<AppState>, user: AuthUser) -> Response {
    if let Err(e) = require_dispatcher(&user) {
        return e.into_response();
    }

    match state.db.list_workers().await {
        Ok(rows) => {
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                let agents = state.db.worker_agents(&row.id).await.unwrap_or_default();
                out.push(WorkerInfo {
                    id: row.id,
                    label: row.label,
                    created_at: row.created_at,
                    revoked_at: row.revoked_at,
                    last_seen_at: row.last_seen_at,
                    agents,
                });
            }
            Json(out).into_response()
        }
        Err(err) => {
            tracing::error!(?err, "list workers failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to list workers" })),
            )
                .into_response()
        }
    }
}

pub async fn revoke_worker(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<String>,
) -> Response {
    if let Err(e) = require_dispatcher(&user) {
        return e.into_response();
    }

    match state.db.revoke_worker(&id).await {
        Ok(true) => Json(json!({ "id": id, "revoked": true })).into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "worker not found or already revoked" })),
        )
            .into_response(),
        Err(err) => {
            tracing::error!(?err, "revoke worker failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to revoke worker" })),
            )
                .into_response()
        }
    }
}

pub async fn bind_agent(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<String>,
    Json(body): Json<BindAgentRequest>,
) -> Response {
    if let Err(e) = require_dispatcher(&user) {
        return e.into_response();
    }

    let agent_id = body.agent_id.trim();
    if agent_id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "agent_id is required" })),
        )
            .into_response();
    }

    match state.db.bind_agent_to_worker(&id, agent_id).await {
        Ok(BindOutcome::Ok) => Json(json!({ "worker_id": id, "agent_id": agent_id, "bound": true }))
            .into_response(),
        Ok(BindOutcome::WorkerNotFound) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "worker not found or revoked" })),
        )
            .into_response(),
        Ok(BindOutcome::AgentNotFound) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "agent not found or revoked" })),
        )
            .into_response(),
        Ok(BindOutcome::AlreadyBound(other)) => (
            StatusCode::CONFLICT,
            Json(json!({ "error": format!("agent already bound to worker {other}") })),
        )
            .into_response(),
        Err(err) => {
            tracing::error!(?err, "bind agent failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to bind agent" })),
            )
                .into_response()
        }
    }
}

pub async fn unbind_agent(
    State(state): State<AppState>,
    user: AuthUser,
    Path((id, agent_id)): Path<(String, String)>,
) -> Response {
    if let Err(e) = require_dispatcher(&user) {
        return e.into_response();
    }

    match state.db.unbind_agent_from_worker(&id, &agent_id).await {
        Ok(true) => Json(json!({ "worker_id": id, "agent_id": agent_id, "bound": false }))
            .into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "binding not found" })),
        )
            .into_response(),
        Err(err) => {
            tracing::error!(?err, "unbind agent failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to unbind agent" })),
            )
                .into_response()
        }
    }
}
