//! HTTP integration: multi-agent auth, routing, delivery_mode default=steer, events.
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;
use uuid::Uuid;

use pi_host_ops_server::config::{hash_token, Config};
use pi_host_ops_server::db::Db;
use pi_host_ops_server::{app, AppState};

async fn test_state(dispatcher: &str, seed_agent: Option<&str>) -> AppState {
    let dir = std::env::temp_dir().join(format!("pi-host-ops-http-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("t.db");
    let database_url = format!("sqlite:{}", db_path.display());

    let config = Arc::new(Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        database_url: database_url.clone(),
        dispatcher_token: dispatcher.to_string(),
        agent_token_seed: seed_agent.map(|s| s.to_string()),
        max_result_bytes: 512 * 1024,
        data_dir: dir,
        heartbeat_ttl_secs: 45,
        lease_secs: 60,
        webhook_url: None,
        webhook_auth_header: "Authorization".to_string(),
        webhook_auth: None,
    });

    let db = Db::connect(&database_url).await.unwrap();
    if let Some(seed) = seed_agent {
        db.upsert_seed_agent("default", seed, "default (bootstrap)")
            .await
            .unwrap();
    }
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

#[tokio::test]
async fn health_is_public() {
    let state = test_state("disp", Some("agent-seed")).await;
    let (status, body) = json_req(state, "GET", "/health", None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], true);
}

#[tokio::test]
async fn unauthenticated_jobs_and_agents_are_401() {
    let state = test_state("disp", Some("agent-seed")).await;
    let (s1, _) = json_req(state.clone(), "GET", "/v1/jobs", None, None).await;
    assert_eq!(s1, StatusCode::UNAUTHORIZED);
    let (s2, _) = json_req(state.clone(), "GET", "/v1/agents", None, None).await;
    assert_eq!(s2, StatusCode::UNAUTHORIZED);
    let (s3, _) = json_req(
        state,
        "POST",
        "/v1/jobs",
        None,
        Some(json!({"type":"shell","payload":{}})),
    )
    .await;
    assert_eq!(s3, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn dispatcher_cannot_poll_agent_cannot_create_jobs() {
    let state = test_state("disp-token", Some("agent-seed")).await;

    // Dispatcher calling agent poll → 403
    let (s, body) = json_req(
        state.clone(),
        "POST",
        "/v1/agent/poll",
        Some("disp-token"),
        Some(json!({"wait_secs": 0})),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{body}");

    // Agent calling create job → 403
    let (s, body) = json_req(
        state.clone(),
        "POST",
        "/v1/jobs",
        Some("agent-seed"),
        Some(json!({"type":"shell","payload":{"command":"echo x"}})),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{body}");

    // Agent cannot list agents
    let (s, _) = json_req(state, "GET", "/v1/agents", Some("agent-seed"), None).await;
    assert_eq!(s, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn delivery_mode_defaults_to_steer() {
    let state = test_state("disp", Some("agent-seed")).await;
    let (s, body) = json_req(
        state.clone(),
        "POST",
        "/v1/jobs",
        Some("disp"),
        Some(json!({"type":"shell","payload":{"command":"true"}})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_eq!(body["delivery_mode"], "steer");

    let id = body["id"].as_str().unwrap();
    let (s, job) = json_req(
        state,
        "GET",
        &format!("/v1/jobs/{id}"),
        Some("disp"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(job["delivery_mode"], "steer");
}

#[tokio::test]
async fn two_agents_targeted_claim() {
    let state = test_state("disp", None).await;

    // Create agent A and B
    let (s, a) = json_req(
        state.clone(),
        "POST",
        "/v1/agents",
        Some("disp"),
        Some(json!({
            "id": "agent-a",
            "label": "A",
            "project": "proj-a",
            "capabilities": ["shell"]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{a}");
    let token_a = a["token"].as_str().unwrap().to_string();

    let (s, b) = json_req(
        state.clone(),
        "POST",
        "/v1/agents",
        Some("disp"),
        Some(json!({
            "id": "agent-b",
            "label": "B",
            "project": "proj-b",
            "capabilities": ["shell"]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let token_b = b["token"].as_str().unwrap().to_string();

    // Job targeted to A
    let (s, job) = json_req(
        state.clone(),
        "POST",
        "/v1/jobs",
        Some("disp"),
        Some(json!({
            "type": "shell",
            "payload": {"command": "echo hi"},
            "target": {"agent_id": "agent-a"},
            "delivery_mode": "queue"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{job}");
    assert_eq!(job["delivery_mode"], "queue");
    assert_eq!(job["target_agent_id"], "agent-a");
    let job_id = job["id"].as_str().unwrap().to_string();

    // B polls → 204
    let (s, _) = json_req(
        state.clone(),
        "POST",
        "/v1/agent/poll",
        Some(&token_b),
        Some(json!({"wait_secs": 0})),
    )
    .await;
    assert_eq!(s, StatusCode::NO_CONTENT);

    // A polls → gets job
    let (s, claimed) = json_req(
        state.clone(),
        "POST",
        "/v1/agent/poll",
        Some(&token_a),
        Some(json!({"wait_secs": 0})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{claimed}");
    assert_eq!(claimed["job"]["id"], job_id);
    assert_eq!(claimed["job"]["delivery_mode"], "queue");
    assert_eq!(claimed["job"]["agent_id"], "agent-a");

    // List agents (dispatcher)
    let (s, agents) = json_req(state.clone(), "GET", "/v1/agents", Some("disp"), None).await;
    assert_eq!(s, StatusCode::OK);
    assert!(agents.as_array().unwrap().len() >= 2);

    // Events: A posts, dispatcher reads; B cannot post
    let (s, ev) = json_req(
        state.clone(),
        "POST",
        &format!("/v1/agent/jobs/{job_id}/events"),
        Some(&token_a),
        Some(json!({"type": "text", "message": "progress…"})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{ev}");
    assert_eq!(ev["seq"], 1);
    assert_eq!(ev["type"], "text");

    let (s, _) = json_req(
        state.clone(),
        "POST",
        &format!("/v1/agent/jobs/{job_id}/events"),
        Some(&token_b),
        Some(json!({"type": "log", "message": "nope"})),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);

    let (s, listed) = json_req(
        state.clone(),
        "GET",
        &format!("/v1/jobs/{job_id}/events?after=0"),
        Some("disp"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{listed}");
    assert_eq!(listed["events"].as_array().unwrap().len(), 1);

    // Agent cannot read events
    let (s, _) = json_req(
        state.clone(),
        "GET",
        &format!("/v1/jobs/{job_id}/events"),
        Some(&token_a),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);

    // Result only by claiming agent
    let (s, _) = json_req(
        state.clone(),
        "POST",
        &format!("/v1/agent/jobs/{job_id}/result"),
        Some(&token_b),
        Some(json!({"ok": true, "result": {"x": 1}})),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN);

    let (s, done) = json_req(
        state,
        "POST",
        &format!("/v1/agent/jobs/{job_id}/result"),
        Some(&token_a),
        Some(json!({"ok": true, "result": {"stdout": "hi"}})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{done}");
    assert_eq!(done["status"], "succeeded");
}

#[tokio::test]
async fn hash_token_stable() {
    assert_eq!(hash_token("abc").len(), 64);
    assert_eq!(hash_token("abc"), hash_token("abc"));
    assert_ne!(hash_token("abc"), hash_token("abd"));
}

#[tokio::test]
async fn agent_me_returns_registration_and_rejects_dispatcher() {
    let state = test_state("disp", None).await;
    let (s, created) = json_req(
        state.clone(),
        "POST",
        "/v1/agents",
        Some("disp"),
        Some(json!({
            "id": "proj-a",
            "label": "Project A",
            "project": "alpha",
            "cwd": "/tmp/alpha-workdir",
            "capabilities": ["shell"]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let token = created["token"].as_str().unwrap();

    let (s, me) = json_req(state.clone(), "GET", "/v1/agent/me", Some(token), None).await;
    assert_eq!(s, StatusCode::OK, "{me}");
    assert_eq!(me["id"], "proj-a");
    assert_eq!(me["project"], "alpha");
    assert_eq!(me["cwd"], "/tmp/alpha-workdir");

    // Dispatcher token must not use agent /me
    let (s, _) = json_req(state, "GET", "/v1/agent/me", Some("disp"), None).await;
    assert_eq!(s, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn agent_patch_me_updates_cwd_and_model() {
    let state = test_state("disp", None).await;
    let (s, created) = json_req(
        state.clone(),
        "POST",
        "/v1/agents",
        Some("disp"),
        Some(json!({
            "id": "proj-b",
            "label": "B",
            "project": "beta",
            "cwd": "/tmp/old",
            "provider": "opencode-go",
            "model": "muse-spark-1.3-contributor",
            "capabilities": ["shell"]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let token = created["token"].as_str().unwrap();

    let (s, me) = json_req(
        state.clone(),
        "PATCH",
        "/v1/agent/me",
        Some(token),
        Some(json!({
            "cwd": "/tmp/new-workdir",
            "model": "muse-spark-1.3-contributor"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{me}");
    assert_eq!(me["cwd"], "/tmp/new-workdir");
    assert_eq!(me["model"], "muse-spark-1.3-contributor");

    let (s, _) = json_req(
        state,
        "PATCH",
        "/v1/agent/me",
        Some(token),
        Some(json!({"model": "something-fast"})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
}
