//! Job execution: local shell + optional pi_svc.
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::process::Command;
use tokio::time::{sleep, timeout};
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::{reject_fast, DEFAULT_MODEL, DEFAULT_PROVIDER};

const MAX_RESULT_BYTES: usize = 512 * 1024;
const DEFAULT_SHELL_TIMEOUT: u64 = 60;
const DEFAULT_PI_TIMEOUT: u64 = 300;
const POLL_STATUS_INTERVAL: f64 = 2.0;

const PI_JOB_TYPES: &[&str] = &["observe", "act", "open", "clipboard", "notify"];

#[derive(Clone, Debug)]
pub struct DirRuntime {
    #[allow(dead_code)]
    pub host_ops_url: String,
    #[allow(dead_code)]
    pub agent_token: String,
    pub agent_id: String,
    pub work_dir: PathBuf,
    pub pi_svc: Option<PathBuf>,
    pub pi_session: String,
    pub provider: String,
    pub model: String,
    pub event_interval_secs: f64,
}

impl DirRuntime {
    pub fn validate_model(&self) -> Result<()> {
        reject_fast(&self.provider, &self.model)
    }
}

pub fn clip_chars(s: &str, max: usize) -> String {
    let mut out: String = s.chars().take(max).collect();
    if s.chars().count() > max {
        out.push('…');
    }
    out
}

pub fn tail_chars(s: &str, max: usize) -> String {
    let count = s.chars().count();
    if count <= max {
        return s.to_string();
    }
    s.chars().skip(count - max).collect()
}

pub fn truncate_result(obj: Value) -> Value {
    let raw = match serde_json::to_string(&obj) {
        Ok(s) => s,
        Err(_) => return json!({"truncated": true, "note": "serialize failed"}),
    };
    if raw.len() <= MAX_RESULT_BYTES {
        return obj;
    }
    if let Value::Object(map) = &obj {
        let mut out = map.clone();
        for key in ["text_tail", "stdout", "stderr", "status", "output", "text_log_tail"] {
            if let Some(Value::String(text)) = out.get(key) {
                let keep = (MAX_RESULT_BYTES / 4).max(1024);
                if text.len() > keep {
                    let mut trimmed = clip_chars(text, keep);
                    trimmed.push_str("\n…[truncated for MAX_RESULT_BYTES]");
                    out.insert(key.to_string(), Value::String(trimmed));
                    out.insert("truncated".into(), Value::Bool(true));
                }
            }
        }
        let out_val = Value::Object(out);
        if serde_json::to_string(&out_val).map(|s| s.len() <= MAX_RESULT_BYTES).unwrap_or(false) {
            return out_val;
        }
        return json!({"truncated": true, "note": "result exceeded 512KB", "keys": map.keys().cloned().collect::<Vec<_>>()});
    }
    json!({"truncated": true, "note": "result too large"})
}

pub struct ApiClient {
    http: reqwest::Client,
    host_ops_url: String,
    agent_token: String,
    agent_id: String,
}

impl ApiClient {
    pub fn new(host_ops_url: &str, agent_token: &str, agent_id: &str) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .context("build reqwest")?;
        Ok(Self {
            http,
            host_ops_url: host_ops_url.trim_end_matches('/').to_string(),
            agent_token: agent_token.to_string(),
            agent_id: agent_id.to_string(),
        })
    }

    pub fn with_dispatcher(host_ops_url: &str, dispatcher_token: &str) -> Result<Self> {
        Self::new(host_ops_url, dispatcher_token, "dispatcher")
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.host_ops_url, path)
    }

    async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
        timeout_secs: u64,
    ) -> Result<(u16, Option<Value>)> {
        let mut req = self
            .http
            .request(method, self.url(path))
            .bearer_auth(&self.agent_token)
            .header("Accept", "application/json")
            .header("X-Agent-Id", &self.agent_id)
            .timeout(Duration::from_secs(timeout_secs));
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req.send().await.context("http request")?;
        let status = resp.status().as_u16();
        let bytes = resp.bytes().await.unwrap_or_default();
        if bytes.is_empty() {
            return Ok((status, None));
        }
        match serde_json::from_slice::<Value>(&bytes) {
            Ok(v) => Ok((status, Some(v))),
            Err(_) => Ok((status, Some(Value::String(String::from_utf8_lossy(&bytes).into_owned())))),
        }
    }

    pub async fn create_agent(
        &self,
        id: &str,
        label: &str,
        project: &str,
        cwd: &str,
        provider: &str,
        model: &str,
    ) -> Result<(String, String)> {
        reject_fast(provider, model)?;
        let (status, body) = self
            .request(
                reqwest::Method::POST,
                "/v1/agents",
                Some(json!({
                    "id": id,
                    "label": label,
                    "project": project,
                    "cwd": cwd,
                    "capabilities": ["shell", "observe", "act"],
                    "max_concurrency": 1,
                    "provider": provider,
                    "model": model,
                })),
                30,
            )
            .await?;
        if status == 409 {
            // id exists — try with new id
            bail!("agent id already exists: {id}");
        }
        if !(200..300).contains(&status) {
            bail!("POST /v1/agents failed HTTP {status}: {body:?}");
        }
        let body = body.context("empty create agent body")?;
        let token = body.get("token").and_then(|v| v.as_str()).context("missing token")?.to_string();
        let id = body.get("id").and_then(|v| v.as_str()).unwrap_or(id).to_string();
        Ok((id, token))
    }

    pub async fn patch_me(&self, cwd: &str, provider: &str, model: &str) -> Result<()> {
        reject_fast(provider, model)?;
        let (status, body) = self
            .request(
                reqwest::Method::PATCH,
                "/v1/agent/me",
                Some(json!({"cwd": cwd, "provider": provider, "model": model})),
                15,
            )
            .await?;
        if !(200..300).contains(&status) {
            bail!("PATCH /v1/agent/me failed HTTP {status}: {body:?}");
        }
        Ok(())
    }

    pub async fn heartbeat(&self) {
        let _ = self
            .request(
                reqwest::Method::POST,
                "/v1/agent/heartbeat",
                Some(json!({"status": "online"})),
                15,
            )
            .await;
    }

    pub async fn post_event(&self, job_id: &str, event_type: &str, message: Option<&str>, data: Option<Value>) {
        if job_id.is_empty() {
            return;
        }
        let mut body = json!({"type": event_type});
        if let Some(m) = message {
            body["message"] = Value::String(clip_chars(m, 8000));
        }
        if let Some(d) = data {
            body["data"] = d;
        }
        let _ = self
            .request(
                reqwest::Method::POST,
                &format!("/v1/agent/jobs/{job_id}/events"),
                Some(body),
                15,
            )
            .await;
    }

    pub async fn renew_lease(&self, job_id: &str) {
        let _ = self
            .request(
                reqwest::Method::POST,
                &format!("/v1/agent/jobs/{job_id}/lease"),
                Some(json!({})),
                15,
            )
            .await;
    }

    pub async fn poll_once(&self, wait_secs: u64) -> Result<Option<Job>> {
        let (status, body) = self
            .request(
                reqwest::Method::POST,
                "/v1/agent/poll",
                Some(json!({"wait_secs": wait_secs})),
                wait_secs + 15,
            )
            .await?;
        if status == 204 || body.is_none() {
            return Ok(None);
        }
        if !(200..300).contains(&status) {
            bail!("poll HTTP {status}: {body:?}");
        }
        let body = body.unwrap();
        let job_v = body.get("job").cloned().unwrap_or(body);
        let job: Job = serde_json::from_value(job_v).context("decode job")?;
        Ok(Some(job))
    }

    pub async fn submit_result(
        &self,
        job_id: &str,
        ok: bool,
        result: Option<Value>,
        error: Option<String>,
    ) -> Result<()> {
        let (status, body) = self
            .request(
                reqwest::Method::POST,
                &format!("/v1/agent/jobs/{job_id}/result"),
                Some(json!({"ok": ok, "result": result, "error": error})),
                30,
            )
            .await?;
        if !(200..300).contains(&status) {
            bail!("submit_result HTTP {status}: {body:?}");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Job {
    pub id: Uuid,
    #[serde(rename = "type")]
    pub job_type: String,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub timeout_secs: Option<i64>,
    #[serde(default)]
    pub delivery_mode: Option<String>,
}

struct CmdOut {
    code: i32,
    stdout: String,
    stderr: String,
}

async fn run_cmd(argv: &[String], timeout_secs: Option<f64>, cwd: Option<&Path>) -> CmdOut {
    if argv.is_empty() {
        return CmdOut { code: 1, stdout: String::new(), stderr: "empty argv".into() };
    }
    let mut cmd = Command::new(&argv[0]);
    if argv.len() > 1 {
        cmd.args(&argv[1..]);
    }
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
    if let Some(c) = cwd {
        cmd.current_dir(c);
    }
    let fut = async {
        match cmd.output().await {
            Ok(o) => CmdOut {
                code: o.status.code().unwrap_or(1),
                stdout: String::from_utf8_lossy(&o.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&o.stderr).into_owned(),
            },
            Err(e) => CmdOut { code: 1, stdout: String::new(), stderr: e.to_string() },
        }
    };
    match timeout_secs {
        Some(t) => match timeout(Duration::from_secs_f64(t), fut).await {
            Ok(o) => o,
            Err(_) => CmdOut { code: 124, stdout: String::new(), stderr: format!("timed out after {t}s") },
        },
        None => fut.await,
    }
}

async fn pi_svc(rt: &DirRuntime, args: &[&str], timeout_secs: f64) -> CmdOut {
    let Some(ref bin) = rt.pi_svc else {
        return CmdOut { code: 127, stdout: String::new(), stderr: "PI_SVC not configured".into() };
    };
    let mut argv = vec![bin.display().to_string()];
    argv.extend(args.iter().map(|s| (*s).to_string()));
    run_cmd(&argv, Some(timeout_secs), None).await
}

async fn session_running(rt: &DirRuntime) -> bool {
    let out = pi_svc(rt, &["list"], 30.0).await;
    if out.code != 0 {
        return false;
    }
    let Ok(sessions) = serde_json::from_str::<Value>(&out.stdout) else { return false };
    let Some(arr) = sessions.as_array() else { return false };
    for s in arr {
        if s.get("name").and_then(|v| v.as_str()) == Some(rt.pi_session.as_str()) {
            return s.get("running").and_then(|v| v.as_bool()).unwrap_or(false);
        }
    }
    false
}

fn status_inner(st: &Value) -> &Value {
    st.get("status").filter(|v| v.is_object()).unwrap_or(st)
}

async fn get_status(rt: &DirRuntime) -> Option<Value> {
    let out = pi_svc(rt, &["status", &rt.pi_session], 30.0).await;
    if out.code != 0 {
        return None;
    }
    match serde_json::from_str::<Value>(&out.stdout) {
        Ok(v) => Some(v),
        Err(_) => Some(json!({"raw": out.stdout})),
    }
}

async fn ensure_session(rt: &DirRuntime) -> Result<()> {
    rt.validate_model()?;
    if session_running(rt).await {
        info!(session = %rt.pi_session, "reusing pi_svc session");
        return Ok(());
    }
    let cwd = rt.work_dir.display().to_string();
    let out = pi_svc(
        rt,
        &[
            "start", &rt.pi_session, "--cwd", &cwd,
            "--provider", &rt.provider, "--model", &rt.model,
        ],
        120.0,
    ).await;
    if out.code != 0 {
        let msg = if out.stderr.is_empty() { out.stdout } else { out.stderr };
        bail!("pi_svc start failed ({}): {msg}", out.code);
    }
    for _ in 0..30 {
        if let Some(st) = get_status(rt).await {
            let state = status_inner(&st).get("state").and_then(|v| v.as_str()).unwrap_or("");
            if matches!(state, "idle" | "running") {
                return Ok(());
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
    warn!(session = %rt.pi_session, "pi_svc session start slow; continuing");
    Ok(())
}

fn build_pi_prompt(job: &Job) -> String {
    let payload_json = serde_json::to_string_pretty(&job.payload).unwrap_or_else(|_| "{}".into());
    format!(
        "你是 host-ops worker 驱动的 Pi 会话。完成下面远程任务并用中英简短总结。不要 Fast / CloudAgent。\n\n\
job_id: {}\ntype: {}\ntimeout_secs: {:?}\npayload:\n{}\n",
        job.id, job.job_type, job.timeout_secs, payload_json
    )
}

async fn wait_until_idle_with_events(
    api: &ApiClient,
    rt: &DirRuntime,
    job_id: &str,
    timeout_secs: f64,
    settled_before: Option<i64>,
    stop: &AtomicBool,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs_f64(timeout_secs);
    let mut last = json!({});
    let mut saw_running = false;
    let mut last_event_at = Instant::now() - Duration::from_secs(3600);
    let mut last_tail = String::new();
    let mut last_tool: Option<String> = None;

    while Instant::now() < deadline {
        if stop.load(Ordering::Relaxed) { break; }
        let st = get_status(rt).await.unwrap_or_else(|| json!({}));
        last = st.clone();
        let inner = status_inner(&st);
        let state = inner.get("state").and_then(|v| v.as_str());
        let settled = inner.get("settled_count").and_then(|v| v.as_i64());
        let tool = inner.get("last_tool").and_then(|v| v.as_str()).map(|s| s.to_string());
        let text_tail = inner.get("text_tail").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if state == Some("running") { saw_running = true; }
        let now = Instant::now();
        if now.duration_since(last_event_at).as_secs_f64() >= rt.event_interval_secs {
            if let Some(ref t) = tool {
                if last_tool.as_ref() != Some(t) {
                    api.post_event(job_id, "tool", Some(t), Some(json!({"last_tool": t}))).await;
                    last_tool = Some(t.clone());
                }
            }
            if !text_tail.is_empty() && text_tail != last_tail {
                api.post_event(job_id, "text", Some(&tail_chars(&text_tail, 1500)), None).await;
                last_tail = text_tail;
            }
            api.post_event(job_id, "status", Some(&format!("state={}", state.unwrap_or(""))), Some(json!({"state": state, "settled_count": settled}))).await;
            api.renew_lease(job_id).await;
            last_event_at = now;
        }
        if matches!(state, Some("error" | "dead" | "stopped")) { return last; }
        if state == Some("idle") {
            let bumped = matches!((settled, settled_before), (Some(s), Some(b)) if s > b);
            if bumped || saw_running { return last; }
        }
        sleep(Duration::from_secs_f64(POLL_STATUS_INTERVAL)).await;
    }
    if let Value::Object(ref mut m) = last {
        m.insert("_wait_timeout".into(), Value::Bool(true));
    } else {
        last = json!({"_wait_timeout": true});
    }
    last
}

async fn run_via_pi(api: &ApiClient, rt: &DirRuntime, job: &Job, stop: &AtomicBool) -> (bool, Option<Value>, Option<String>) {
    if rt.pi_svc.is_none() {
        return (false, None, Some("PI_SVC not configured; this directory can only run shell jobs until pi_svc is set in WebUI settings".into()));
    }
    if let Err(e) = ensure_session(rt).await {
        return (false, None, Some(e.to_string()));
    }
    let before = get_status(rt).await.unwrap_or_else(|| json!({}));
    let settled_before = status_inner(&before).get("settled_count").and_then(|v| v.as_i64());
    let prompt = build_pi_prompt(job);
    let timeout_secs = job.timeout_secs.filter(|&t| t > 0).map(|t| t as f64).unwrap_or(DEFAULT_PI_TIMEOUT as f64);
    // Prompt file lives in the OS temp dir — never litter the project tree.
    let prompt_path = std::env::temp_dir().join(format!("host-ops-{}-prompt.txt", job.id));
    if let Err(e) = tokio::fs::write(&prompt_path, prompt.as_bytes()).await {
        return (false, None, Some(format!("write prompt: {e}")));
    }

    let delivery = job.delivery_mode.as_deref().unwrap_or("steer").trim().to_lowercase();
    let job_id = job.id.to_string();
    let st_now = get_status(rt).await.unwrap_or_else(|| json!({}));
    let state = status_inner(&st_now).get("state").and_then(|v| v.as_str()).unwrap_or("");
    let prompt_path_s = prompt_path.display().to_string();
    let session = rt.pi_session.as_str();
    let (label, send_args): (&str, Vec<&str>) = if delivery == "steer" {
        ("steer", vec!["send", session, "--steer", "--file", &prompt_path_s])
    } else if state == "running" {
        ("queue+busy", vec!["send", session, "--follow-up", "--file", &prompt_path_s])
    } else {
        ("queue+idle", vec!["send", session, "--file", &prompt_path_s])
    };
    info!(delivery = %delivery, label, "pi_svc send");
    api.post_event(&job_id, "status", Some(&format!("pi_svc send ({delivery})")), Some(json!({"delivery_mode": delivery}))).await;
    let out = pi_svc(rt, &send_args, (timeout_secs + 30.0).min(120.0)).await;
    if out.code != 0 {
        let _ = tokio::fs::remove_file(&prompt_path).await;
        let err = if out.stderr.is_empty() { out.stdout } else { out.stderr };
        return (false, None, Some(format!("pi_svc send failed ({}): {err}", out.code)));
    }
    let final_st = wait_until_idle_with_events(api, rt, &job_id, timeout_secs, settled_before, stop).await;
    let inner_f = status_inner(&final_st);
    let text_tail = inner_f.get("text_tail").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let tout = pi_svc(rt, &["tail", session, "--text", "-n", "40"], 30.0).await;
    let text_log = if tout.code == 0 { tout.stdout } else { String::new() };
    let wait_timeout = final_st.get("_wait_timeout").and_then(|v| v.as_bool()).unwrap_or(false);
    let state_f = inner_f.get("state").and_then(|v| v.as_str());
    let result = truncate_result(json!({
        "via": "pi_svc",
        "session": rt.pi_session,
        "provider": rt.provider,
        "model": rt.model,
        "delivery_mode": delivery,
        "state": state_f,
        "settled_count": inner_f.get("settled_count"),
        "last_tool": inner_f.get("last_tool"),
        "text_tail": text_tail,
        "text_log_tail": tail_chars(&text_log, 8000),
        "wait_timeout": wait_timeout,
        "send_stdout": clip_chars(&out.stdout, 2000),
    }));
    let mut ok = true;
    let mut err_msg = None;
    if matches!(state_f, Some("error" | "dead")) {
        ok = false;
        err_msg = Some(inner_f.get("error").and_then(|v| v.as_str()).map(|s| s.to_string()).unwrap_or_else(|| format!("session state={state_f:?}")));
    } else if wait_timeout {
        ok = false;
        err_msg = Some(format!("timed out waiting for idle after {timeout_secs}s"));
    }
    let _ = tokio::fs::remove_file(&prompt_path).await;
    (ok, Some(result), err_msg)
}

async fn run_shell_local(rt: &DirRuntime, job: &Job) -> (bool, Option<Value>, Option<String>) {
    let command = job.payload.get("command").and_then(|v| v.as_str()).map(|s| s.to_string());
    let Some(command) = command.filter(|c| !c.trim().is_empty()) else {
        return (false, None, Some("shell job missing payload.command string".into()));
    };
    let timeout_secs = job.timeout_secs
        .or_else(|| job.payload.get("timeout_secs").and_then(|v| v.as_i64()))
        .filter(|&t| t > 0)
        .map(|t| t as f64)
        .unwrap_or(DEFAULT_SHELL_TIMEOUT as f64);
    let cwd = job.payload.get("cwd").and_then(|v| v.as_str()).filter(|s| !s.is_empty()).map(PathBuf::from)
        .unwrap_or_else(|| rt.work_dir.clone());
    info!(command = %clip_chars(&command, 200), cwd = %cwd.display(), "local shell");
    let mut cmd = {
        #[cfg(windows)]
        { let mut c = Command::new("cmd"); c.arg("/C").arg(&command); c }
        #[cfg(not(windows))]
        { let mut c = Command::new("bash"); c.arg("-c").arg(&command); c }
    };
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true).current_dir(&cwd);
    match timeout(Duration::from_secs_f64(timeout_secs), cmd.output()).await {
        Ok(Ok(output)) => {
            let code = output.status.code().unwrap_or(1);
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            let result = truncate_result(json!({
                "via": "local_shell",
                "command": command,
                "cwd": cwd.display().to_string(),
                "exit_code": code,
                "stdout": tail_chars(&stdout, 200_000),
                "stderr": tail_chars(&stderr, 50_000),
            }));
            (code == 0, Some(result), if code == 0 { None } else { Some(format!("exit_code={code}")) })
        }
        Ok(Err(e)) => (false, None, Some(format!("shell spawn failed: {e}"))),
        Err(_) => (false, Some(truncate_result(json!({"via":"local_shell","command":command,"timeout":true}))), Some(format!("shell timed out after {timeout_secs}s"))),
    }
}

pub async fn execute_job(api: &ApiClient, rt: &DirRuntime, job: &Job, stop: &AtomicBool) -> (bool, Option<Value>, Option<String>) {
    info!(id = %job.id, job_type = %job.job_type, agent = %rt.agent_id, "execute job");
    if job.job_type == "shell" {
        let has_cmd = job.payload.get("command").and_then(|v| v.as_str()).map(|s| !s.trim().is_empty()).unwrap_or(false);
        if has_cmd {
            return run_shell_local(rt, job).await;
        }
        return run_via_pi(api, rt, job, stop).await;
    }
    if PI_JOB_TYPES.contains(&job.job_type.as_str()) {
        return run_via_pi(api, rt, job, stop).await;
    }
    (false, None, Some(format!("unknown job type: {}", job.job_type)))
}

#[allow(dead_code)]
pub fn defaults() -> (&'static str, &'static str) {
    (DEFAULT_PROVIDER, DEFAULT_MODEL)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn truncate_small_unchanged() {
        let v = json!({"stdout": "hi"});
        assert_eq!(truncate_result(v.clone()), v);
    }
    #[test]
    fn truncate_large_stdout() {
        let big = "x".repeat(600_000);
        let out = truncate_result(json!({"via":"local_shell","stdout": big, "stderr":"e"}));
        let s = serde_json::to_string(&out).unwrap();
        assert!(s.len() <= MAX_RESULT_BYTES);
        assert_eq!(out.get("truncated"), Some(&Value::Bool(true)));
    }
    #[test]
    fn clip_and_tail() {
        assert_eq!(clip_chars("abcdef", 3), "abc…");
        assert_eq!(tail_chars("abcdef", 3), "def");
    }
    #[test]
    fn reject_fast_model() {
        assert!(reject_fast("opencode-go", "muse-spark").is_ok());
        assert!(reject_fast("x", "foo-fast").is_err());
    }
}
