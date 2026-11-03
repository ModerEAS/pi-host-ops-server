//! Terminal-job webhook: server-side, async POST after a job reaches a
//! terminal status (`succeeded` / `failed` / `timed_out` / `no_agent`).
//!
//! - Global switch, default off: fires only when `HOST_OPS_WEBHOOK_URL` is set.
//! - Auth: `HOST_OPS_WEBHOOK_AUTH` (or `HOST_OPS_WEBHOOK_AUTH_FILE`) is sent
//!   verbatim as `HOST_OPS_WEBHOOK_HEADER` (default `Authorization`).
//!   The secret is never logged.
//! - Delivery never blocks job persistence: callers `spawn_webhook` (fire and
//!   forget). Failures are logged only, with up to 3 attempts + backoff.

use serde::Serialize;

use crate::config::Config;
use crate::db::Db;
use crate::models::Job;
use crate::AppState;

/// Truncation budget for the human-readable summary (chars, not bytes).
pub const SUMMARY_TRUNC_CHARS: usize = 2000;

/// Outgoing body. `thread` is an alias of `thread_id`, `text` an alias of
/// `summary`, so Grok-style bots can read either key.
#[derive(Debug, Clone, Serialize)]
pub struct WebhookPayload {
    pub job_id: String,
    pub status: String,
    pub agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread: Option<String>,
    pub job_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    pub finished_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub fn build_payload(job: &Job, project: Option<String>) -> WebhookPayload {
    let summary = extract_summary(job);
    WebhookPayload {
        job_id: job.id.to_string(),
        status: job.status.as_str().to_string(),
        agent_id: job.agent_id.clone(),
        project,
        thread_id: job.thread_id.clone(),
        thread: job.thread_id.clone(),
        job_type: job.job_type.as_str().to_string(),
        summary: summary.clone(),
        text: summary,
        finished_at: job.finished_at.map(|t| t.to_rfc3339()),
        error: job.error.clone(),
    }
}

/// Pick a short human-readable summary from `result`, falling back to `error`.
/// Never returns the full result blob — only a truncated excerpt.
pub fn extract_summary(job: &Job) -> Option<String> {
    if let Some(ref v) = job.result {
        if let Some(s) = pick_text(v) {
            let s = s.trim();
            if !s.is_empty() {
                return Some(truncate_chars(s, SUMMARY_TRUNC_CHARS));
            }
        }
    }
    job.error
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| truncate_chars(s, SUMMARY_TRUNC_CHARS))
}

fn pick_text(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Object(map) => {
            for key in [
                "summary",
                "text",
                "output",
                "stdout",
                "text_tail",
                "text_log_tail",
                "stderr",
                "status",
            ] {
                if let Some(s) = map.get(key).and_then(|x| x.as_str()) {
                    if !s.trim().is_empty() {
                        return Some(s.to_string());
                    }
                }
            }
            None
        }
        _ => None,
    }
}

fn truncate_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max).collect();
    out.push_str("…[truncated]");
    out
}

async fn resolve_project(db: &Db, job: &Job) -> Option<String> {
    // Prefer the claiming agent's current project.
    if let Some(ref aid) = job.agent_id {
        if let Ok(Some(row)) = db.get_agent(aid).await {
            if let Some(p) = row.project.filter(|s| !s.is_empty()) {
                return Some(p);
            }
        }
    }
    // Fall back to the targeted agent's project, then the target filter.
    if let Some(ref aid) = job.target_agent_id {
        if let Ok(Some(row)) = db.get_agent(aid).await {
            if let Some(p) = row.project.filter(|s| !s.is_empty()) {
                return Some(p);
            }
        }
    }
    job.target_filter
        .as_ref()
        .and_then(|f| f.get("project"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

/// Fire-and-forget: resolve project, then POST with retries.
/// Never propagates errors; failures are only logged (no secret in logs).
pub fn spawn_webhook(state: &AppState, job: Job) {
    if !state.config.webhook_enabled() {
        return;
    }
    let state = state.clone();
    tokio::spawn(async move {
        let project = resolve_project(&state.db, &job).await;
        let payload = build_payload(&job, project);
        post_with_retry(&state.config, &payload).await;
    });
}

/// POST once (+ up to 2 retries, exponential backoff). Logs failures only.
pub(crate) async fn post_with_retry(config: &Config, payload: &WebhookPayload) {
    let Some(url) = config.webhook_url.clone().filter(|s| !s.trim().is_empty()) else {
        return;
    };
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
    // 3 attempts total: initial + 2 retries.
    let backoffs = [
        std::time::Duration::from_millis(0),
        std::time::Duration::from_millis(500),
        std::time::Duration::from_secs(1),
    ];
    for (attempt, wait) in backoffs.iter().enumerate() {
        if attempt > 0 {
            tokio::time::sleep(*wait).await;
        }
        let mut req = client.post(url.clone()).json(payload);
        if let Some(ref secret) = config.webhook_auth {
            // Custom header name supported; default `Authorization`.
            match reqwest::header::HeaderName::from_bytes(config.webhook_auth_header.as_bytes()) {
                Ok(name) => match reqwest::header::HeaderValue::from_str(secret) {
                    Ok(val) => {
                        req = req.header(name, val);
                    }
                    Err(e) => {
                        tracing::error!(job_id = %payload.job_id, ?e, "webhook auth value invalid; sending without auth");
                    }
                },
                Err(e) => {
                    tracing::error!(job_id = %payload.job_id, ?e, "webhook auth header name invalid; sending without auth");
                }
            }
        }
        match req.send().await {
            Ok(resp) if resp.status().is_success() => {
                return;
            }
            Ok(resp) => {
                // 4xx (auth/config/receiver bug) won't heal on retry — log once.
                tracing::warn!(
                    job_id = %payload.job_id,
                    status = %payload.status,
                    http_status = %resp.status(),
                    attempt = attempt + 1,
                    "terminal webhook POST failed"
                );
                if resp.status().is_client_error() {
                    return;
                }
            }
            Err(e) => {
                tracing::warn!(
                    job_id = %payload.job_id,
                    status = %payload.status,
                    attempt = attempt + 1,
                    error = %e,
                    "terminal webhook POST error"
                );
            }
        }
    }
    tracing::error!(
        job_id = %payload.job_id,
        status = %payload.status,
        "terminal webhook gave up after 3 attempts (job state unaffected)"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    fn test_job() -> Job {
        Job {
            id: Uuid::new_v4(),
            job_type: crate::models::JobType::Shell,
            status: crate::models::JobStatus::Succeeded,
            payload: json!({}),
            result: Some(json!({"stdout": "hello"})),
            error: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            claimed_at: None,
            finished_at: Some(chrono::Utc::now()),
            agent_id: Some("agent-a".into()),
            timeout_secs: None,
            target_agent_id: None,
            target_filter: None,
            thread_id: Some("t1".into()),
            delivery_mode: crate::models::DeliveryMode::Steer,
            lease_expires_at: None,
        }
    }

    #[test]
    fn summary_prefers_stdout_and_truncates() {
        let mut job = test_job();
        job.result = Some(json!({"stdout": "abc", "stderr": "e"}));
        assert_eq!(extract_summary(&job).as_deref(), Some("abc"));
        let big = "x".repeat(5000);
        job.result = Some(json!({"stdout": big}));
        let s = extract_summary(&job).unwrap();
        assert!(s.ends_with("…[truncated]"));
        assert!(s.chars().count() <= SUMMARY_TRUNC_CHARS + 20);
    }

    #[test]
    fn summary_falls_back_to_error() {
        let mut job = test_job();
        job.result = None;
        job.error = Some("boom".into());
        assert_eq!(extract_summary(&job).as_deref(), Some("boom"));
    }

    #[test]
    fn payload_aliases_thread_and_text() {
        let job = test_job();
        let p = build_payload(&job, Some("proj".into()));
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["job_id"], job.id.to_string());
        assert_eq!(v["status"], "succeeded");
        assert_eq!(v["thread"], "t1");
        assert_eq!(v["thread_id"], "t1");
        assert_eq!(v["text"], v["summary"]);
        assert!(v["finished_at"].is_string());
    }
}
