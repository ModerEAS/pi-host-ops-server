use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use uuid::Uuid;

use crate::auth::{require_dispatcher, AuthUser};
use crate::db::resolve_target;
use crate::models::{
    CreateJobRequest, CreateJobResponse, GetJobQuery, ListEventsQuery, ListJobsQuery,
};
use crate::AppState;

pub async fn create_job(
    State(state): State<AppState>,
    user: AuthUser,
    Json(body): Json<CreateJobRequest>,
) -> Response {
    if let Err(e) = require_dispatcher(&user) {
        return e.into_response();
    }

    let payload = if body.payload.is_null() {
        json!({})
    } else if body.payload.is_object() {
        body.payload
    } else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "payload must be a JSON object" })),
        )
            .into_response();
    };

    let (target_agent_id, target_filter) = resolve_target(body.target.as_ref());
    let thread_id = body
        .thread_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let id = Uuid::new_v4();
    match state
        .db
        .insert_job(
            id,
            body.job_type,
            &payload,
            body.timeout_secs,
            target_agent_id.as_deref(),
            target_filter.as_ref(),
            thread_id,
            body.delivery_mode,
        )
        .await
    {
        Ok(job) => (
            StatusCode::CREATED,
            Json(CreateJobResponse {
                id: job.id,
                status: job.status,
                created_at: job.created_at,
                delivery_mode: job.delivery_mode,
                target_agent_id: job.target_agent_id,
                thread_id: job.thread_id,
            }),
        )
            .into_response(),
        Err(err) => {
            tracing::error!(?err, "create job failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to create job" })),
            )
                .into_response()
        }
    }
}

pub async fn get_job(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
    Query(q): Query<GetJobQuery>,
) -> Response {
    if let Err(e) = require_dispatcher(&user) {
        return e.into_response();
    }

    let include = q
        .include_events
        .as_deref()
        .map(|s| s == "1" || s.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    match state.db.get_job(id).await {
        Ok(Some(job)) => {
            if include {
                let events = state
                    .db
                    .list_events_tail(id, 20)
                    .await
                    .unwrap_or_default();
                let mut map = serde_json::to_value(&job).unwrap_or(json!({}));
                if let Some(obj) = map.as_object_mut() {
                    obj.insert("events".into(), serde_json::to_value(events).unwrap_or(json!([])));
                }
                Json(map).into_response()
            } else {
                // Always include last few events as a light summary (last 5)
                let events = state
                    .db
                    .list_events_tail(id, 5)
                    .await
                    .unwrap_or_default();
                let mut map = serde_json::to_value(&job).unwrap_or(json!({}));
                if let Some(obj) = map.as_object_mut() {
                    obj.insert("events".into(), serde_json::to_value(events).unwrap_or(json!([])));
                }
                Json(map).into_response()
            }
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "job not found" })),
        )
            .into_response(),
        Err(err) => {
            tracing::error!(?err, "get job failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to get job" })),
            )
                .into_response()
        }
    }
}

pub async fn list_jobs(
    State(state): State<AppState>,
    user: AuthUser,
    Query(q): Query<ListJobsQuery>,
) -> Response {
    if let Err(e) = require_dispatcher(&user) {
        return e.into_response();
    }

    if let Some(ref s) = q.status {
        if crate::models::JobStatus::parse(s).is_none() {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "invalid status filter" })),
            )
                .into_response();
        }
    }

    let limit = q.limit.unwrap_or(20);
    match state.db.list_jobs(q.status.as_deref(), limit).await {
        Ok(jobs) => Json(jobs).into_response(),
        Err(err) => {
            tracing::error!(?err, "list jobs failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to list jobs" })),
            )
                .into_response()
        }
    }
}

pub async fn list_events(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
    Query(q): Query<ListEventsQuery>,
) -> Response {
    if let Err(e) = require_dispatcher(&user) {
        return e.into_response();
    }

    match state.db.get_job(id).await {
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "job not found" })),
            )
                .into_response();
        }
        Ok(Some(_)) => {}
        Err(err) => {
            tracing::error!(?err, "get job for events failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to get job" })),
            )
                .into_response();
        }
    }

    let after = q.after.unwrap_or(0);
    let limit = q.limit.unwrap_or(50);
    match state.db.list_events(id, after, limit).await {
        Ok(events) => Json(json!({ "job_id": id, "events": events })).into_response(),
        Err(err) => {
            tracing::error!(?err, "list events failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "failed to list events" })),
            )
                .into_response()
        }
    }
}
