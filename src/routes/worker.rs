use std::collections::HashSet;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

use crate::auth::{require_worker, AuthUser};
use crate::models::{WorkerHeartbeatRequest, WorkerHeartbeatResponse, WorkerPollRequest};
use crate::AppState;

/// Batch heartbeat for all agents bound to this worker.
/// Rejects the whole batch if any agent_id is not bound to this worker.
pub async fn heartbeat(
    State(state): State<AppState>,
    user: AuthUser,
    Json(body): Json<WorkerHeartbeatRequest>,
) -> Response {
    let worker_id = match require_worker(&user) {
        Ok(id) => id.to_string(),
        Err(e) => return e.into_response(),
    };

    if body.agents.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "agents must not be empty" })),
        )
            .into_response();
    }

    let bound: HashSet<String> = state
        .db
        .worker_agents(&worker_id)
        .await
        .unwrap_or_default()
        .into_iter()
        .collect();

    let mut unbound = Vec::new();
    let mut entries = Vec::with_capacity(body.agents.len());
    for a in &body.agents {
        let id = a.agent_id.trim();
        if id.is_empty() || !bound.contains(id) {
            unbound.push(a.agent_id.clone());
            continue;
        }
        let status = a
            .status
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("online");
        entries.push((id.to_string(), status.to_string()));
    }
    if !unbound.is_empty() {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "agents not bound to this worker",
                "unbound": unbound,
            })),
        )
            .into_response();
    }

    match state.db.heartbeat_many(&worker_id, &entries).await {
        Ok(last_seen) => Json(WorkerHeartbeatResponse {
            worker_id,
            updated: entries.into_iter().map(|(id, _)| id).collect(),
            last_seen_at: last_seen,
            server_time: chrono::Utc::now(),
        })
        .into_response(),
        Err(err) => {
            tracing::error!(?err, "worker heartbeat failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to heartbeat" })),
            )
                .into_response()
        }
    }
}

/// One long-poll claiming a matching job for any **online** agent bound to
/// this worker. Response includes `agent_id` so the worker can dispatch to
/// the right directory. Serial execution stays the worker's job: it simply
/// does not poll again while a job is running.
pub async fn poll(
    State(state): State<AppState>,
    user: AuthUser,
    Json(body): Json<WorkerPollRequest>,
) -> Response {
    let worker_id = match require_worker(&user) {
        Ok(id) => id.to_string(),
        Err(e) => return e.into_response(),
    };

    let bound = state.db.worker_agents(&worker_id).await.unwrap_or_default();
    if bound.is_empty() {
        return StatusCode::NO_CONTENT.into_response();
    }

    // Only agents recently heartbeated (batch or legacy per-agent) are
    // claimable — a dir removed/disabled on the worker ages out within the
    // TTL instead of claiming jobs it can no longer run.
    let mut online = Vec::new();
    for agent_id in bound {
        match state.db.get_agent(&agent_id).await {
            Ok(Some(row)) if row.revoked_at.is_none() => {
                let fresh = row.last_seen_at.map(|seen| {
                    chrono::Utc::now().signed_duration_since(seen).num_seconds()
                        <= state.config.heartbeat_ttl_secs
                });
                // No heartbeat yet (never polled) still counts as a fresh
                // registration — the touch below marks it online immediately.
                if fresh.unwrap_or(true) {
                    online.push(agent_id);
                }
            }
            _ => {}
        }
    }
    if online.is_empty() {
        return StatusCode::NO_CONTENT.into_response();
    }

    // Touch heartbeat on poll (mirrors legacy per-agent poll semantics), so a
    // polling worker alone keeps its agents online even between batch beats.
    let entries: Vec<(String, String)> = online
        .iter()
        .map(|id| (id.clone(), "online".to_string()))
        .collect();
    let _ = state.db.heartbeat_many(&worker_id, &entries).await;

    let wait_secs = body.wait_secs.unwrap_or(0).min(25);
    let lease_secs = state.config.lease_secs;

    match state
        .db
        .claim_next_job_for_agents(&online, lease_secs)
        .await
    {
        Ok(Some((job, agent_id))) => {
            return (
                StatusCode::OK,
                Json(json!({ "job": job, "agent_id": agent_id })),
            )
                .into_response();
        }
        Ok(None) if wait_secs == 0 => return StatusCode::NO_CONTENT.into_response(),
        Ok(None) => {}
        Err(err) => {
            tracing::error!(?err, "worker claim failed");
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

        match state
            .db
            .claim_next_job_for_agents(&online, lease_secs)
            .await
        {
            Ok(Some((job, agent_id))) => {
                return (
                    StatusCode::OK,
                    Json(json!({ "job": job, "agent_id": agent_id })),
                )
                    .into_response();
            }
            Ok(None) => continue,
            Err(err) => {
                tracing::error!(?err, "worker claim failed during poll");
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
