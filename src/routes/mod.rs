mod agent;
mod agents;
mod health;
mod jobs;
mod worker;
mod workers;

use axum::routing::{delete, get, post};
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
        // Dispatcher: workers (multiplexed heartbeat + poll)
        .route("/v1/workers", post(workers::create_worker).get(workers::list_workers))
        .route("/v1/workers/{id}", delete(workers::revoke_worker))
        .route("/v1/workers/{id}/agents", post(workers::bind_agent))
        .route("/v1/workers/{id}/agents/{agent_id}", delete(workers::unbind_agent))
        // Worker (Bearer worker token)
        .route("/v1/worker/heartbeat", post(worker::heartbeat))
        .route("/v1/worker/poll", post(worker::poll))
        // Agent worker
        .route("/v1/agent/me", get(agent::me).patch(agent::patch_me))
        .route("/v1/agent/heartbeat", post(agent::heartbeat))
        .route("/v1/agent/poll", post(agent::poll))
        .route("/v1/agent/jobs/{id}/result", post(agent::submit_result))
        .route("/v1/agent/jobs/{id}/lease", post(agent::renew_lease))
        .route("/v1/agent/jobs/{id}/events", post(agent::append_event))
}
