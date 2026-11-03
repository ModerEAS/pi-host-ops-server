use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

use crate::auth::{require_agent, AuthUser};
use crate::db::{AppendEventOutcome, LeaseOutcome, SubmitOutcome};
use crate::models::{
    AppendEventRequest, HeartbeatRequest, HeartbeatResponse, LeaseResponse, PatchAgentMeRequest,
    PollRequest, PollResponse, SubmitResultRequest,
};
use crate::AppState;

/// Authenticated agent fetches its own registration (cwd/project/capabilities).
/// Workers call this on startup so install scripts only need AGENT_TOKEN + HOST_OPS_URL.
pub async fn me(State(state): State<AppState>, user: AuthUser) -> Response {
    let agent_id = match require_agent(&user) {
        Ok(id) => id.to_string(),
        Err(e) => return e.into_response(),
    };

    match state.db.get_agent(&agent_id).await {
        Ok(Some(row)) => {
            if row.revoked_at.is_some() {
                return (
                    StatusCode::FORBIDDEN,
                    Json(json!({ "error": "agent revoked" })),
                )
                    .into_response();
            }
            let running = state.db.count_running_for_agent(&row.id).await.ok();
            Json(row.to_public(state.config.heartbeat_ttl_secs, running)).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "agent not found" })),
        )
            .into_response(),
        Err(err) => {
            tracing::error!(?err, "agent me failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to load agent" })),
            )
                .into_response()
        }
    }
}

pub async fn patch_me(
    State(state): State<AppState>,
    user: AuthUser,
    Json(body): Json<PatchAgentMeRequest>,
) -> Response {
    let agent_id = match require_agent(&user) {
        Ok(id) => id.to_string(),
        Err(e) => return e.into_response(),
    };

    for v in [&body.provider, &body.model] {
        if let Some(s) = v {
            if s.to_lowercase().contains("fast") {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": "provider/model must not be Fast (cost rule)" })),
                )
                    .into_response();
            }
        }
    }

    match state
        .db
        .update_agent_profile(
            &agent_id,
            body.cwd.as_deref().map(str::trim).filter(|s| !s.is_empty()),
            body.project.as_deref().map(str::trim).filter(|s| !s.is_empty()),
            body.label.as_deref().map(str::trim).filter(|s| !s.is_empty()),
            body.provider.as_deref().map(str::trim).filter(|s| !s.is_empty()),
            body.model.as_deref().map(str::trim).filter(|s| !s.is_empty()),
        )
        .await
    {
        Ok(Some(row)) => {
            let running = state.db.count_running_for_agent(&row.id).await.ok();
            Json(row.to_public(state.config.heartbeat_ttl_secs, running)).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "agent not found or revoked" })),
        )
            .into_response(),
        Err(err) => {
            tracing::error!(?err, "patch agent me failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to update agent" })),
            )
                .into_response()
        }
    }
}

pub async fn heartbeat(
    State(state): State<AppState>,
    user: AuthUser,
    body: Option<Json<HeartbeatRequest>>,
) -> Response {
    let agent_id = match require_agent(&user) {
        Ok(id) => id.to_string(),
        Err(e) => return e.into_response(),
    };

    let status = body
        .as_ref()
        .and_then(|b| b.status.as_deref())
        .filter(|s| !s.is_empty());

    match state.db.heartbeat(&agent_id, status).await {
        Ok(last_seen) => Json(HeartbeatResponse {
            agent_id,
            status: status.unwrap_or("online").to_string(),
            last_seen_at: last_seen,
            server_time: chrono::Utc::now(),
        })
        .into_response(),
        Err(err) => {
            tracing::error!(?err, "heartbeat failed");
            (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "agent not found or revoked" })),
            )
                .into_response()
        }
    }
}

pub async fn poll(
    State(state): State<AppState>,
    user: AuthUser,
    Json(body): Json<PollRequest>,
) -> Response {
    let agent_id = match require_agent(&user) {
        Ok(id) => id.to_string(),
        Err(e) => return e.into_response(),
    };

    // Touch heartbeat on poll
    let _ = state.db.heartbeat(&agent_id, Some("online")).await;

    let wait_secs = body.wait_secs.unwrap_or(0).min(25);
    let lease_secs = state.config.lease_secs;

    match try_claim(&state, &agent_id, lease_secs).await {
        Ok(Some(job)) => {
            return (StatusCode::OK, Json(PollResponse { job })).into_response();
        }
        Ok(None) if wait_secs == 0 => {
            return StatusCode::NO_CONTENT.into_response();
        }
        Ok(None) => {}
        Err(err) => {
            tracing::error!(?err, "claim failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to claim job" })),
            )
                .into_response();
        }
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(wait_secs);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        tokio::time::sleep(remaining.min(Duration::from_millis(500))).await;

        match try_claim(&state, &agent_id, lease_secs).await {
            Ok(Some(job)) => {
                return (StatusCode::OK, Json(PollResponse { job })).into_response();
            }
            Ok(None) => continue,
            Err(err) => {
                tracing::error!(?err, "claim failed during poll");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "failed to claim job" })),
                )
                    .into_response();
            }
        }
    }

    StatusCode::NO_CONTENT.into_response()
}

async fn try_claim(
    state: &AppState,
    agent_id: &str,
    lease_secs: i64,
) -> anyhow::Result<Option<crate::models::Job>> {
    state.db.claim_next_job_for_agent(agent_id, lease_secs).await
}

pub async fn submit_result(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
    Json(body): Json<SubmitResultRequest>,
) -> Response {
    let agent_id = match require_agent(&user) {
        Ok(id) => id.to_string(),
        Err(e) => return e.into_response(),
    };

    if let Some(ref result) = body.result {
        match serde_json::to_vec(result) {
            Ok(bytes) if bytes.len() > state.config.max_result_bytes => {
                return (
                    StatusCode::PAYLOAD_TOO_LARGE,
                    Json(json!({
                        "error": format!(
                            "result exceeds MAX_RESULT_BYTES ({})",
                            state.config.max_result_bytes
                        )
                    })),
                )
                    .into_response();
            }
            Ok(_) => {}
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": format!("invalid result json: {err}") })),
                )
                    .into_response();
            }
        }
    }

    if !body.ok && body.error.is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "error message required when ok=false" })),
        )
            .into_response();
    }

    match state
        .db
        .submit_result(
            id,
            &agent_id,
            body.ok,
            body.result.as_ref(),
            body.error.as_deref(),
        )
        .await
    {
        Ok(SubmitOutcome::Ok(job)) => {
            // Terminal (succeeded/failed): server-side webhook, async, never blocks persist.
            crate::webhook::spawn_webhook(&state, job.clone());
            Json(job).into_response()
        }
        Ok(SubmitOutcome::NotFound) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "job not found" })),
        )
            .into_response(),
        Ok(SubmitOutcome::Forbidden) => (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "job not claimed by this agent" })),
        )
            .into_response(),
        Ok(SubmitOutcome::WrongStatus(status)) => (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "job is not running",
                "status": status.as_str()
            })),
        )
            .into_response(),
        Err(err) => {
            tracing::error!(?err, "submit result failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to submit result" })),
            )
                .into_response()
        }
    }
}

pub async fn renew_lease(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Response {
    let agent_id = match require_agent(&user) {
        Ok(id) => id.to_string(),
        Err(e) => return e.into_response(),
    };

    match state
        .db
        .renew_lease(id, &agent_id, state.config.lease_secs)
        .await
    {
        Ok(LeaseOutcome::Ok(exp)) => Json(LeaseResponse {
            id,
            lease_expires_at: exp,
        })
        .into_response(),
        Ok(LeaseOutcome::NotFound) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "job not found" })),
        )
            .into_response(),
        Ok(LeaseOutcome::Forbidden) => (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "job not claimed by this agent" })),
        )
            .into_response(),
        Ok(LeaseOutcome::WrongStatus(status)) => (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "job is not running",
                "status": status.as_str()
            })),
        )
            .into_response(),
        Err(err) => {
            tracing::error!(?err, "renew lease failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to renew lease" })),
            )
                .into_response()
        }
    }
}

pub async fn append_event(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
    Json(body): Json<AppendEventRequest>,
) -> Response {
    let agent_id = match require_agent(&user) {
        Ok(id) => id.to_string(),
        Err(e) => return e.into_response(),
    };

    let event_type = body.event_type.trim();
    if event_type.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "type is required" })),
        )
            .into_response();
    }
    let allowed = ["text", "tool", "status", "log"];
    if !allowed.contains(&event_type) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "type must be one of: text, tool, status, log"
            })),
        )
            .into_response();
    }

    match state
        .db
        .append_event(
            id,
            &agent_id,
            event_type,
            body.message.as_deref(),
            body.data.as_ref(),
        )
        .await
    {
        Ok(AppendEventOutcome::Ok(ev)) => (StatusCode::CREATED, Json(ev)).into_response(),
        Ok(AppendEventOutcome::NotFound) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "job not found" })),
        )
            .into_response(),
        Ok(AppendEventOutcome::Forbidden) => (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "job not claimed by this agent" })),
        )
            .into_response(),
        Ok(AppendEventOutcome::WrongStatus(status)) => (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "job is not running",
                "status": status.as_str()
            })),
        )
            .into_response(),
        Err(err) => {
            tracing::error!(?err, "append event failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to append event" })),
            )
                .into_response()
        }
    }
}
