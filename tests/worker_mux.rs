//! Worker multiplexing: batch heartbeat + single poll over bound agents.
//! Legacy per-agent heartbeat/poll keep working unchanged.
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;
use uuid::Uuid;

use pi_host_ops_server::config::Config;
use pi_host_ops_server::db::Db;
use pi_host_ops_server::{app, AppState};

async fn test_state(dispatcher: &str) -> AppState {
    let dir = std::env::temp_dir().join(format!("pi-host-ops-worker-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("t.db");
    let database_url = format!("sqlite:{}", db_path.display());

    let config = Arc::new(Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        database_url: database_url.clone(),
        dispatcher_token: dispatcher.to_string(),
        agent_token_seed: None,
        max_result_bytes: 512 * 1024,
        data_dir: dir,
        heartbeat_ttl_secs: 45,
        lease_secs: 60,
    });

    let db = Db::connect(&database_url).await.unwrap();
    AppState { db, config }
}

async fn json_req(
    state: AppState,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(t) = token {
        builder = builder.header("Authorization", format!("Bearer {t}"));
    }
    if body.is_some() {
        builder = builder.header("Content-Type", "application/json");
    }
    let req = if let Some(b) = body {
        builder.body(Body::from(serde_json::to_vec(&b).unwrap())).unwrap()
    } else {
        builder.body(Body::empty()).unwrap()
    };

    let response = app(state).oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let val = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::String(
            String::from_utf8_lossy(&bytes).to_string(),
        ))
    };
    (status, val)
}

async fn make_agent(state: &AppState, id: &str) -> String {
    let (s, body) = json_req(
        state.clone(),
        "POST",
        "/v1/agents",
        Some("disp"),
        Some(json!({"id": id, "label": id, "capabilities": ["shell"]})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    body["token"].as_str().unwrap().to_string()
}

async fn make_worker(state: &AppState, id: &str) -> String {
    let (s, body) = json_req(
        state.clone(),
        "POST",
        "/v1/workers",
        Some("disp"),
        Some(json!({"id": id, "label": id})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    body["token"].as_str().unwrap().to_string()
}

async fn bind(state: &AppState, worker: &str, agent: &str) -> (StatusCode, Value) {
    json_req(
        state.clone(),
        "POST",
        &format!("/v1/workers/{worker}/agents"),
        Some("disp"),
        Some(json!({"agent_id": agent})),
    )
    .await
}

#[tokio::test]
async fn worker_crud_and_bind_rules() {
    let state = test_state("disp").await;
    let _tok_a = make_agent(&state, "w-agent-a").await;
    let _tok_b = make_agent(&state, "w-agent-b").await;
    let _tok_w = make_worker(&state, "w-1").await;
    let _tok_w2 = make_worker(&state, "w-2").await;

    // Duplicate worker id → 409
    let (s, _) = json_req(
        state.clone(),
        "POST",
        "/v1/workers",
        Some("disp"),
        Some(json!({"id": "w-1"})),
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT);

    // Bind unknown agent → 404; bind to unknown worker → 404
    let (s, _) = bind(&state, "w-1", "nope").await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, _) = bind(&state, "nope", "w-agent-a").await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    // Bind A to w-1 → ok, idempotent rebind ok
    let (s, body) = bind(&state, "w-1", "w-agent-a").await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let (s, _) = bind(&state, "w-1", "w-agent-a").await;
    assert_eq!(s, StatusCode::OK);

    // Same agent to another worker → 409
    let (s, body) = bind(&state, "w-2", "w-agent-a").await;
    assert_eq!(s, StatusCode::CONFLICT, "{body}");

    // List shows bindings
    let (s, list) = json_req(state.clone(), "GET", "/v1/workers", Some("disp"), None).await;
    assert_eq!(s, StatusCode::OK);
    let w1 = list
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["id"] == "w-1")
        .unwrap();
    assert_eq!(w1["agents"], json!(["w-agent-a"]));

    // Unbind missing → 404; unbind present → ok
    let (s, _) = json_req(
        state.clone(),
        "DELETE",
        "/v1/workers/w-1/agents/w-agent-b",
        Some("disp"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, _) = json_req(
        state.clone(),
        "DELETE",
        "/v1/workers/w-1/agents/w-agent-a",
        Some("disp"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    // Revoke → ok, then 404; revoked worker token is 401 on worker routes
    let (s, _) = json_req(
        state.clone(),
        "DELETE",
        "/v1/workers/w-1",
        Some("disp"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let (s, _) = json_req(
        state.clone(),
        "DELETE",
        "/v1/workers/w-1",
        Some("disp"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, _) = json_req(
        state.clone(),
        "POST",
        "/v1/worker/poll",
        Some(&_tok_w),
        Some(json!({"wait_secs": 0})),
    )
    .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn worker_heartbeat_batch_and_rejects_unbound() {
    let state = test_state("disp").await;
    let _tok_a = make_agent(&state, "h-agent-a").await;
    let _tok_b = make_agent(&state, "h-agent-b").await;
    let tok_w = make_worker(&state, "h-w").await;
    bind(&state, "h-w", "h-agent-a").await;

    // Batch with an unbound agent → 403, nothing updated
    let (s, body) = json_req(
        state.clone(),
        "POST",
        "/v1/worker/heartbeat",
        Some(&tok_w),
        Some(json!({"agents": [
            {"agent_id": "h-agent-a", "status": "online"},
            {"agent_id": "h-agent-b", "status": "online"},
        ]})),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["unbound"], json!(["h-agent-b"]));

    // h-agent-a must still be offline (never heartbeated)
    let (s, agents) = json_req(state.clone(), "GET", "/v1/agents", Some("disp"), None).await;
    assert_eq!(s, StatusCode::OK);
    let a = agents
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["id"] == "h-agent-a")
        .unwrap();
    assert_eq!(a["status"], "offline");

    // Bound-only batch → 200, agents come online in one shot
    let (s, body) = json_req(
        state.clone(),
        "POST",
        "/v1/worker/heartbeat",
        Some(&tok_w),
        Some(json!({"agents": [
            {"agent_id": "h-agent-a", "status": "busy"},
        ]})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["worker_id"], "h-w");
    assert_eq!(body["updated"], json!(["h-agent-a"]));

    let (s, agents) = json_req(state.clone(), "GET", "/v1/agents", Some("disp"), None).await;
    assert_eq!(s, StatusCode::OK);
    let a = agents
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["id"] == "h-agent-a")
        .unwrap();
    assert_eq!(a["status"], "online");

    // Empty batch → 400
    let (s, _) = json_req(
        state.clone(),
        "POST",
        "/v1/worker/heartbeat",
        Some(&tok_w),
        Some(json!({"agents": []})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);

    // Wrong roles on worker routes → 403; missing token → 401
    let (s, _) = json_req(
        state.clone(),
        "POST",
        "/v1/worker/heartbeat",
        Some("disp"),
        Some(json!({"agents": [{"agent_id": "h-agent-a"}]})),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    let (s, _) = json_req(
        state.clone(),
        "POST",
        "/v1/worker/poll",
        Some("disp"),
        Some(json!({"wait_secs": 0})),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    let (s, _) = json_req(
        state.clone(),
        "POST",
        "/v1/worker/heartbeat",
        None,
        Some(json!({"agents": [{"agent_id": "h-agent-a"}]})),
    )
    .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn worker_poll_claims_bound_agent_and_legacy_still_works() {
    let state = test_state("disp").await;
    let tok_a = make_agent(&state, "p-agent-a").await;
    let tok_b = make_agent(&state, "p-agent-b").await;
    let tok_w = make_worker(&state, "p-w").await;
    bind(&state, "p-w", "p-agent-a").await;
    // p-agent-b deliberately left unbound.

    // Job targeted to the unbound agent: worker poll must NOT claim it.
    let (s, job_b) = json_req(
        state.clone(),
        "POST",
        "/v1/jobs",
        Some("disp"),
        Some(json!({
            "type": "shell",
            "payload": {"command": "echo b"},
            "target": {"agent_id": "p-agent-b"},
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{job_b}");
    let job_b_id = job_b["id"].as_str().unwrap().to_string();

    let (s, _) = json_req(
        state.clone(),
        "POST",
        "/v1/worker/poll",
        Some(&tok_w),
        Some(json!({"wait_secs": 0})),
    )
    .await;
    assert_eq!(s, StatusCode::NO_CONTENT);

    // Legacy per-agent poll still claims it for B.
    let (s, claimed) = json_req(
        state.clone(),
        "POST",
        "/v1/agent/poll",
        Some(&tok_b),
        Some(json!({"wait_secs": 0})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{claimed}");
    assert_eq!(claimed["job"]["id"], job_b_id);

    // Job targeted to the bound agent: single worker poll claims it with agent_id.
    let (s, job_a) = json_req(
        state.clone(),
        "POST",
        "/v1/jobs",
        Some("disp"),
        Some(json!({
            "type": "shell",
            "payload": {"command": "echo a"},
            "target": {"agent_id": "p-agent-a"},
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{job_a}");
    let job_a_id = job_a["id"].as_str().unwrap().to_string();

    let (s, claimed) = json_req(
        state.clone(),
        "POST",
        "/v1/worker/poll",
        Some(&tok_w),
        Some(json!({"wait_secs": 0})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{claimed}");
    assert_eq!(claimed["job"]["id"], job_a_id);
    assert_eq!(claimed["agent_id"], "p-agent-a");
    assert_eq!(claimed["job"]["agent_id"], "p-agent-a");

    // Worker poll alone kept the bound agent online (poll touches heartbeat).
    let (s, agents) = json_req(state.clone(), "GET", "/v1/agents", Some("disp"), None).await;
    assert_eq!(s, StatusCode::OK);
    let a = agents
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["id"] == "p-agent-a")
        .unwrap();
    assert_eq!(a["status"], "online");

    // Legacy per-agent heartbeat/poll for the bound agent still works too.
    let (s, _) = json_req(
        state.clone(),
        "POST",
        "/v1/agent/heartbeat",
        Some(&tok_a),
        Some(json!({"status": "online"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
}
