use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use uuid::Uuid;

use crate::auth::{require_dispatcher, AuthUser};
use crate::config::random_token;
use crate::models::{CreateAgentRequest, CreateAgentResponse};
use crate::AppState;

pub async fn create_agent(
    State(state): State<AppState>,
    user: AuthUser,
    Json(body): Json<CreateAgentRequest>,
) -> Response {
    if let Err(e) = require_dispatcher(&user) {
        return e.into_response();
    }

    let label = body.label.trim();
    if label.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "label is required" })),
        )
            .into_response();
    }

    let id = body
        .id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("agent-{}", &Uuid::new_v4().to_string()[..8]));

    if !id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "id must be alphanumeric/dash/underscore" })),
        )
            .into_response();
    }

    if let Ok(Some(_)) = state.db.get_agent(&id).await {
        return (
            StatusCode::CONFLICT,
            Json(json!({ "error": "agent id already exists" })),
        )
            .into_response();
    }

    let caps = body.capabilities.unwrap_or_default();
    let max_c = body.max_concurrency.unwrap_or(1).max(1);
    let token = random_token();

    match state
        .db
        .create_agent(
            &id,
            &token,
            label,
            body.project.as_deref().filter(|s| !s.is_empty()),
            body.cwd.as_deref().filter(|s| !s.is_empty()),
            body.machine.as_deref().filter(|s| !s.is_empty()),
            &caps,
            max_c,
        )
        .await
    {
        Ok(()) => (
            StatusCode::CREATED,
            Json(CreateAgentResponse {
                id: id.clone(),
                token,
                label: label.to_string(),
                project: body.project,
                cwd: body.cwd,
                machine: body.machine,
                capabilities: caps,
                max_concurrency: max_c,
            }),
        )
            .into_response(),
        Err(err) => {
            tracing::error!(?err, "create agent failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to create agent" })),
            )
                .into_response()
        }
    }
}

pub async fn list_agents(State(state): State<AppState>, user: AuthUser) -> Response {
    if let Err(e) = require_dispatcher(&user) {
        return e.into_response();
    }

    match state.db.list_agents().await {
        Ok(rows) => {
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                let running = state
                    .db
                    .count_running_for_agent(&row.id)
                    .await
                    .ok();
                out.push(row.to_public(state.config.heartbeat_ttl_secs, running));
            }
            Json(out).into_response()
        }
        Err(err) => {
            tracing::error!(?err, "list agents failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to list agents" })),
            )
                .into_response()
        }
    }
}

pub async fn get_agent(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<String>,
) -> Response {
    if let Err(e) = require_dispatcher(&user) {
        return e.into_response();
    }

    match state.db.get_agent(&id).await {
        Ok(Some(row)) => {
            let running = state.db.count_running_for_agent(&row.id).await.ok();
            Json(row.to_public(state.config.heartbeat_ttl_secs, running)).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "agent not found" })),
        )
            .into_response(),
        Err(err) => {
            tracing::error!(?err, "get agent failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to get agent" })),
            )
                .into_response()
        }
    }
}

pub async fn revoke_agent(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<String>,
) -> Response {
    if let Err(e) = require_dispatcher(&user) {
        return e.into_response();
    }

    match state.db.revoke_agent(&id).await {
        Ok(true) => Json(json!({ "id": id, "revoked": true })).into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "agent not found or already revoked" })),
        )
            .into_response(),
        Err(err) => {
            tracing::error!(?err, "revoke agent failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to revoke agent" })),
            )
                .into_response()
        }
    }
}
