mod agent;
mod agents;
mod health;
mod jobs;

use axum::routing::{get, post};
use axum::Router;

use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/health", get(health::health))
        // Dispatcher: jobs
        .route("/v1/jobs", post(jobs::create_job).get(jobs::list_jobs))
        .route("/v1/jobs/{id}", get(jobs::get_job))
        .route("/v1/jobs/{id}/events", get(jobs::list_events))
        // Dispatcher: agents
        .route("/v1/agents", post(agents::create_agent).get(agents::list_agents))
        .route("/v1/agents/{id}", get(agents::get_agent).delete(agents::revoke_agent))
        // Agent worker
        .route("/v1/agent/heartbeat", post(agent::heartbeat))
        .route("/v1/agent/poll", post(agent::poll))
        .route("/v1/agent/jobs/{id}/result", post(agent::submit_result))
        .route("/v1/agent/jobs/{id}/lease", post(agent::renew_lease))
        .route("/v1/agent/jobs/{id}/events", post(agent::append_event))
}
