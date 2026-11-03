//! Terminal webhook: server POSTs once after job reaches terminal state.
//! Mock-HTTP style (stdlib TcpListener, no extra deps).
use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

use pi_host_ops_server::config::Config;
use pi_host_ops_server::db::Db;
use pi_host_ops_server::{app, AppState};

#[derive(Debug, Clone, Default)]
struct Captured {
    headers: HashMap<String, String>,
    body: Value,
}

async fn start_mock_server(
    status: u16,
) -> (
    String,
    Arc<Mutex<Vec<Captured>>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let captured: Arc<Mutex<Vec<Captured>>> = Arc::new(Mutex::new(Vec::new()));
    let cap = captured.clone();
    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let cap = cap.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = Vec::new();
                let mut tmp = [0u8; 4096];
                // Read until end of headers.
                loop {
                    match socket.read(&mut tmp).await {
                        Ok(0) => break,
                        Ok(n) => {
                            buf.extend_from_slice(&tmp[..n]);
                            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                            if buf.len() > 64 * 1024 {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                let header_end = buf
                    .windows(4)
                    .position(|w| w == b"\r\n\r\n")
                    .map(|p| p + 4)
                    .unwrap_or(buf.len());
                let header_text = String::from_utf8_lossy(&buf[..header_end]).to_string();
                let mut headers = HashMap::new();
                let mut content_len = 0usize;
                for line in header_text.lines().skip(1) {
                    if let Some((k, v)) = line.split_once(':') {
                        let key = k.trim().to_lowercase();
                        let val = v.trim().to_string();
                        if key == "content-length" {
                            content_len = val.parse().unwrap_or(0);
                        }
                        headers.insert(key, val);
                    }
                }
                let mut body_bytes = buf[header_end..].to_vec();
                while body_bytes.len() < content_len {
                    match socket.read(&mut tmp).await {
                        Ok(0) => break,
                        Ok(n) => body_bytes.extend_from_slice(&tmp[..n]),
                        Err(_) => break,
                    }
                }
                let body = serde_json::from_slice::<Value>(&body_bytes).unwrap_or(Value::Null);
                cap.lock().await.push(Captured { headers, body });
                let reason = if (200..300).contains(&status) {
                    "OK"
                } else {
                    "Error"
                };
                let resp = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
                );
                let _ = socket.write_all(resp.as_bytes()).await;
            });
        }
    });
    (format!("http://{addr}/hook"), captured, handle)
}

async fn test_state_with_webhook(
    dispatcher: &str,
    url: Option<String>,
    auth: Option<String>,
    header: Option<String>,
) -> AppState {
    let dir = std::env::temp_dir().join(format!("pi-host-ops-webhook-{}", Uuid::new_v4()));
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
        webhook_url: url,
        webhook_auth_header: header.unwrap_or_else(|| "Authorization".to_string()),
        webhook_auth: auth,
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
        builder
            .body(Body::from(serde_json::to_vec(&b).unwrap()))
            .unwrap()
    } else {
        builder.body(Body::empty()).unwrap()
    };
    let response = app(state).oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let val = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or(Value::String(String::from_utf8_lossy(&bytes).to_string()))
    };
    (status, val)
}

async fn wait_for_capture(cap: &Arc<Mutex<Vec<Captured>>>, n: usize) -> Vec<Captured> {
    for _ in 0..100 {
        if cap.lock().await.len() >= n {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    cap.lock().await.clone()
}

#[tokio::test]
async fn terminal_submit_posts_webhook_once_with_auth() {
    let (url, captured, _srv) = start_mock_server(200).await;
    let state = test_state_with_webhook(
        "disp",
        Some(url),
        Some("Bearer grok-secret".to_string()),
        None,
    )
    .await;

    let (s, a) = json_req(
        state.clone(),
        "POST",
        "/v1/agents",
        Some("disp"),
        Some(
            json!({"id": "agent-a", "label": "A", "project": "proj-x", "capabilities": ["shell"]}),
        ),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{a}");
    let token_a = a["token"].as_str().unwrap().to_string();

    let (s, job) = json_req(
        state.clone(),
        "POST",
        "/v1/jobs",
        Some("disp"),
        Some(json!({
            "type": "shell",
            "payload": {"command": "echo hi"},
            "target": {"agent_id": "agent-a"},
            "thread_id": "thread-1",
            "delivery_mode": "queue"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{job}");
    let job_id = job["id"].as_str().unwrap().to_string();

    let (s, _) = json_req(
        state.clone(),
        "POST",
        "/v1/agent/poll",
        Some(&token_a),
        Some(json!({"wait_secs": 0})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, done) = json_req(
        state.clone(),
        "POST",
        &format!("/v1/agent/jobs/{job_id}/result"),
        Some(&token_a),
        Some(json!({"ok": true, "result": {"stdout": "hi there"}})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{done}");
    assert_eq!(done["status"], "succeeded");

    let got = wait_for_capture(&captured, 1).await;
    assert_eq!(got.len(), 1, "webhook must POST exactly once");
    let c = &got[0];
    assert_eq!(
        c.headers.get("authorization").map(|s| s.as_str()),
        Some("Bearer grok-secret")
    );
    assert_eq!(c.body["job_id"], job_id);
    assert_eq!(c.body["status"], "succeeded");
    assert_eq!(c.body["agent_id"], "agent-a");
    assert_eq!(c.body["project"], "proj-x");
    assert_eq!(c.body["thread_id"], "thread-1");
    assert_eq!(c.body["thread"], "thread-1");
    assert_eq!(c.body["job_type"], "shell");
    assert_eq!(c.body["summary"], "hi there");
    assert_eq!(c.body["text"], "hi there");
    assert!(c.body["finished_at"].is_string());
}

#[tokio::test]
async fn webhook_custom_header_name() {
    let (url, captured, _srv) = start_mock_server(200).await;
    let state = test_state_with_webhook(
        "disp",
        Some(url),
        Some("s3cr3t".to_string()),
        Some("X-Webhook-Token".to_string()),
    )
    .await;

    let (s, a) = json_req(
        state.clone(),
        "POST",
        "/v1/agents",
        Some("disp"),
        Some(json!({"id": "a1", "label": "A"})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{a}");
    let token_a = a["token"].as_str().unwrap().to_string();

    let (s, job) = json_req(
        state.clone(),
        "POST",
        "/v1/jobs",
        Some("disp"),
        Some(json!({"type": "shell", "payload": {}, "target": {"agent_id": "a1"}})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{job}");
    let job_id = job["id"].as_str().unwrap().to_string();

    let (s, _) = json_req(
        state.clone(),
        "POST",
        "/v1/agent/poll",
        Some(&token_a),
        Some(json!({"wait_secs": 0})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, done) = json_req(
        state.clone(),
        "POST",
        &format!("/v1/agent/jobs/{job_id}/result"),
        Some(&token_a),
        Some(json!({"ok": false, "error": "boom"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{done}");
    assert_eq!(done["status"], "failed");

    let got = wait_for_capture(&captured, 1).await;
    assert_eq!(got.len(), 1);
    assert_eq!(
        got[0].headers.get("x-webhook-token").map(|s| s.as_str()),
        Some("s3cr3t")
    );
    assert_eq!(got[0].body["status"], "failed");
    // error falls back into summary
    assert_eq!(got[0].body["summary"], "boom");
}

#[tokio::test]
async fn webhook_failure_does_not_break_job_persist() {
    // Closed port: POST fails, but job must still land as succeeded.
    let state = test_state_with_webhook(
        "disp",
        Some("http://127.0.0.1:1/hook".to_string()),
        None,
        None,
    )
    .await;

    let (s, a) = json_req(
        state.clone(),
        "POST",
        "/v1/agents",
        Some("disp"),
        Some(json!({"id": "a9", "label": "A"})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{a}");
    let token_a = a["token"].as_str().unwrap().to_string();

    let (s, job) = json_req(
        state.clone(),
        "POST",
        "/v1/jobs",
        Some("disp"),
        Some(json!({"type": "shell", "payload": {}, "target": {"agent_id": "a9"}})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{job}");
    let job_id = job["id"].as_str().unwrap().to_string();

    let (s, _) = json_req(
        state.clone(),
        "POST",
        "/v1/agent/poll",
        Some(&token_a),
        Some(json!({"wait_secs": 0})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, done) = json_req(
        state.clone(),
        "POST",
        &format!("/v1/agent/jobs/{job_id}/result"),
        Some(&token_a),
        Some(json!({"ok": true, "result": {}})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{done}");
    assert_eq!(done["status"], "succeeded");

    // Persisted despite webhook down.
    let (s, fetched) = json_req(
        state.clone(),
        "GET",
        &format!("/v1/jobs/{job_id}"),
        Some("disp"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(fetched["status"], "succeeded");
}
