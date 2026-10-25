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
/// Absolute cap on the Pi idle wait when the job sets no `timeout_secs` (1h).
/// A long coding/`act` run must never fail just for being slow.
const DEFAULT_PI_ABS_TIMEOUT: u64 = 3600;
/// Give up only after this long with no observable Pi activity (10 min).
/// While the agent is `running` or its output keeps changing, we keep waiting.
const DEFAULT_PI_INACTIVITY_SECS: f64 = 600.0;
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

    pub fn with_worker(host_ops_url: &str, worker_token: &str) -> Result<Self> {
        Self::new(host_ops_url, worker_token, "worker")
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

    /// Batch heartbeat for all agents bound to this worker (worker token).
    /// `agents`: (agent_id, status) pairs.
    pub async fn worker_heartbeat(&self, agents: &[(String, String)]) {
        let list: Vec<Value> = agents
            .iter()
            .map(|(id, status)| json!({"agent_id": id, "status": status}))
            .collect();
        let _ = self
            .request(
                reqwest::Method::POST,
                "/v1/worker/heartbeat",
                Some(json!({"agents": list})),
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

    /// Multiplexed poll (worker token): claims a job for any bound online
    /// agent. Returns the job plus the agent_id to dispatch it to.
    pub async fn worker_poll(&self, wait_secs: u64) -> Result<Option<(Job, String)>> {
        let (status, body) = self
            .request(
                reqwest::Method::POST,
                "/v1/worker/poll",
                Some(json!({"wait_secs": wait_secs})),
                wait_secs + 15,
            )
            .await?;
        if status == 204 || body.is_none() {
            return Ok(None);
        }
        if !(200..300).contains(&status) {
            bail!("worker poll HTTP {status}: {body:?}");
        }
        let body = body.unwrap();
        let agent_id = body
            .get("agent_id")
            .and_then(|v| v.as_str())
            .context("worker poll missing agent_id")?
            .to_string();
        let job_v = body.get("job").cloned().unwrap_or(body);
        let job: Job = serde_json::from_value(job_v).context("decode job")?;
        Ok(Some((job, agent_id)))
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

fn payload_prompt_text(payload: &Value) -> Option<String> {
    for key in ["prompt", "message", "text"] {
        if let Some(s) = payload.get(key).and_then(|v| v.as_str()) {
            let t = s.trim();
            if !t.is_empty() {
                return Some(t.to_string());
            }
        }
    }
    None
}

/// Build text sent to `pi_svc send [--steer] --file`.
/// When payload has `prompt` / `message` / `text`, send that string nearly as-is
/// (one-line job_id prefix only) — used by `host-ops steer` corrections.
/// Otherwise wrap the full job JSON in the legacy boilerplate template.
fn build_pi_prompt(job: &Job) -> String {
    if let Some(text) = payload_prompt_text(&job.payload) {
        return format!("[host-ops job {}]\n{}", job.id, text);
    }
    let payload_json = serde_json::to_string_pretty(&job.payload).unwrap_or_else(|_| "{}".into());
    format!(
        "你是 host-ops worker 驱动的 Pi 会话。完成下面远程任务并用中英简短总结。不要 Fast / CloudAgent。\n\n\
job_id: {}\ntype: {}\ntimeout_secs: {:?}\npayload:\n{}\n",
        job.id, job.job_type, job.timeout_secs, payload_json
    )
}

/// Observable Pi progress snapshot driving the idle-wait policy.
#[derive(Debug, Clone, Default, PartialEq)]
struct PiProgress {
    running: bool,
    settled_count: Option<i64>,
    last_tool: Option<String>,
    text_tail: String,
}

impl PiProgress {
    fn from_status(st: &Value) -> Self {
        let inner = status_inner(st);
        Self {
            running: inner.get("state").and_then(|v| v.as_str()) == Some("running"),
            settled_count: inner.get("settled_count").and_then(|v| v.as_i64()),
            last_tool: inner
                .get("last_tool")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            text_tail: inner
                .get("text_tail")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        }
    }

    /// The agent is observably doing work (or produced new output) since prev.
    /// `running` alone counts — a coding agent thinking without emitting
    /// tools/text for a while is still working, not stuck.
    fn advanced_since(&self, prev: &Self) -> bool {
        self.running
            || self.settled_count != prev.settled_count
            || self.last_tool != prev.last_tool
            || self.text_tail != prev.text_tail
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitEnd {
    Continue,
    IdleDone,
    Terminal,
    Stopped,
    TimeoutAbs,
    TimeoutInactivity,
}

/// Pure idle-wait policy (fake-clock friendly: callers pass elapsed times).
/// Terminal/idle wins over timeouts; the absolute cap wins over inactivity.
fn decide_wait(
    terminal: bool,
    idle_done: bool,
    abs_elapsed_secs: f64,
    idle_elapsed_secs: f64,
    abs_timeout_secs: f64,
    inactivity_secs: f64,
) -> WaitEnd {
    if terminal {
        return WaitEnd::Terminal;
    }
    if idle_done {
        return WaitEnd::IdleDone;
    }
    if abs_elapsed_secs >= abs_timeout_secs {
        return WaitEnd::TimeoutAbs;
    }
    if idle_elapsed_secs >= inactivity_secs {
        return WaitEnd::TimeoutInactivity;
    }
    WaitEnd::Continue
}

struct WaitOutcome {
    status: Value,
    end: WaitEnd,
    saw_running: bool,
}

/// Activity-based wait for the Pi session to go idle after send.
///
/// Two budgets: an absolute cap (`abs_timeout_secs`, default 1h or the job's
/// `timeout_secs`) and an inactivity budget (`inactivity_secs`, default 10min
/// with no observable progress). While the agent is `running` or its
/// `text_tail` / `last_tool` / `settled_count` keeps changing, the inactivity
/// deadline keeps refreshing — a long coding run is never failed just for
/// being slow. Lease renewals + progress events continue as today.
async fn wait_until_idle_with_events(
    api: &ApiClient,
    rt: &DirRuntime,
    job_id: &str,
    abs_timeout_secs: f64,
    inactivity_secs: f64,
    settled_before: Option<i64>,
    stop: &AtomicBool,
) -> WaitOutcome {
    let start = Instant::now();
    let mut last_progress_at = start;
    let mut prev = PiProgress::default();
    let mut last = json!({});
    let mut saw_running = false;
    let mut last_event_at = Instant::now() - Duration::from_secs(3600);
    let mut last_tail = String::new();
    let mut last_tool: Option<String> = None;

    loop {
        if stop.load(Ordering::Relaxed) {
            return WaitOutcome { status: last, end: WaitEnd::Stopped, saw_running };
        }
        let st = get_status(rt).await.unwrap_or_else(|| json!({}));
        last = st.clone();
        let inner = status_inner(&st);
        let state = inner.get("state").and_then(|v| v.as_str());
        let settled = inner.get("settled_count").and_then(|v| v.as_i64());
        let tool = inner.get("last_tool").and_then(|v| v.as_str()).map(|s| s.to_string());
        let text_tail = inner.get("text_tail").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let cur = PiProgress::from_status(&st);
        if cur.running {
            saw_running = true;
        }
        let now = Instant::now();
        if cur.advanced_since(&prev) {
            last_progress_at = now;
        }
        prev = cur;
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
        let terminal = matches!(state, Some("error" | "dead" | "stopped"));
        let bumped = matches!((settled, settled_before), (Some(s), Some(b)) if s > b);
        let idle_done = state == Some("idle") && (bumped || saw_running);
        let end = decide_wait(
            terminal,
            idle_done,
            start.elapsed().as_secs_f64(),
            last_progress_at.elapsed().as_secs_f64(),
            abs_timeout_secs,
            inactivity_secs,
        );
        match end {
            WaitEnd::Continue => {}
            _ => return WaitOutcome { status: last, end, saw_running },
        }
        sleep(Duration::from_secs_f64(POLL_STATUS_INTERVAL)).await;
    }
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
    // Absolute wait cap: explicit job timeout wins, else the 1h default.
    // The inactivity budget (no observable progress) applies on top.
    let abs_timeout_secs = job.timeout_secs.filter(|&t| t > 0).map(|t| t as f64).unwrap_or(DEFAULT_PI_ABS_TIMEOUT as f64);
    let inactivity_secs = DEFAULT_PI_INACTIVITY_SECS;
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
    let out = pi_svc(rt, &send_args, (abs_timeout_secs + 30.0).min(120.0)).await;
    if out.code != 0 {
        let _ = tokio::fs::remove_file(&prompt_path).await;
        let err = if out.stderr.is_empty() { out.stdout } else { out.stderr };
        return (false, None, Some(format!("pi_svc send failed ({}): {err}", out.code)));
    }
    let outcome = wait_until_idle_with_events(api, rt, &job_id, abs_timeout_secs, inactivity_secs, settled_before, stop).await;
    let final_st = outcome.status;
    let inner_f = status_inner(&final_st);
    let text_tail = inner_f.get("text_tail").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let tout = pi_svc(rt, &["tail", session, "--text", "-n", "40"], 30.0).await;
    let text_log = if tout.code == 0 { tout.stdout } else { String::new() };
    let wait_timeout = matches!(outcome.end, WaitEnd::TimeoutAbs | WaitEnd::TimeoutInactivity);
    let timeout_kind = match outcome.end {
        WaitEnd::TimeoutAbs => Some("absolute"),
        WaitEnd::TimeoutInactivity => Some("inactivity"),
        _ => None,
    };
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
        "timeout_kind": timeout_kind,
        "send_stdout": clip_chars(&out.stdout, 2000),
    }));
    let mut ok = true;
    let mut err_msg = None;
    if matches!(state_f, Some("error" | "dead")) {
        ok = false;
        err_msg = Some(inner_f.get("error").and_then(|v| v.as_str()).map(|s| s.to_string()).unwrap_or_else(|| format!("session state={state_f:?}")));
    } else if outcome.end == WaitEnd::TimeoutAbs {
        // Never claim success on timeout — but say plainly the agent was
        // still working, so this reads as "needs longer", not "task failed".
        ok = false;
        err_msg = Some(format!(
            "absolute Pi wait timeout after {}s: agent still {}",
            abs_timeout_secs as u64,
            state_f.unwrap_or("unknown"),
        ));
    } else if outcome.end == WaitEnd::TimeoutInactivity {
        ok = false;
        err_msg = Some(if !outcome.saw_running && state_f != Some("idle") {
            format!(
                "Pi inactivity timeout after {}s: send accepted but agent never started (state={})",
                inactivity_secs as u64,
                state_f.unwrap_or("unknown"),
            )
        } else {
            format!(
                "Pi inactivity timeout after {}s with no progress (state={}; absolute cap {}s)",
                inactivity_secs as u64,
                state_f.unwrap_or("unknown"),
                abs_timeout_secs as u64,
            )
        });
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

    fn progress(state: &str, tail: &str, tool: Option<&str>, settled: Option<i64>) -> PiProgress {
        let mut inner = serde_json::Map::new();
        inner.insert("state".into(), Value::String(state.into()));
        inner.insert("text_tail".into(), Value::String(tail.into()));
        if let Some(t) = tool {
            inner.insert("last_tool".into(), Value::String(t.into()));
        }
        if let Some(s) = settled {
            inner.insert("settled_count".into(), Value::Number(s.into()));
        }
        PiProgress::from_status(&Value::Object({
            let mut o = serde_json::Map::new();
            o.insert("status".into(), Value::Object(inner));
            o
        }))
    }

    #[test]
    fn progress_advances_on_running_or_changed_output() {
        let empty = PiProgress::default();
        // Idle with no output change is not progress.
        let idle = progress("idle", "", None, None);
        assert!(!idle.advanced_since(&empty));
        assert!(!idle.advanced_since(&idle));
        // Running alone counts — a thinking agent emitting nothing is working.
        assert!(progress("running", "", None, None).advanced_since(&idle));
        // New text / tool / settled count counts even when not running.
        assert!(progress("idle", "ab", None, None).advanced_since(&idle));
        assert!(progress("idle", "", Some("wiki_observe"), None).advanced_since(&idle));
        assert!(progress("idle", "", None, Some(1)).advanced_since(&progress("idle", "", None, Some(0))));
        // Identical snapshot twice is not progress.
        let same = progress("idle", "ab", Some("wiki_observe"), Some(1));
        assert!(!same.advanced_since(&same));
    }

    #[test]
    fn wait_policy_branches() {
        let abs = 3600.0;
        let inact = 600.0;
        // Long run, still working near the absolute cap → keep waiting.
        // (The old flat-300s wall clock failed exactly this case.)
        assert_eq!(
            decide_wait(false, false, 3500.0, 5.0, abs, inact),
            WaitEnd::Continue
        );
        // Silence past the inactivity budget → inactivity timeout.
        assert_eq!(
            decide_wait(false, false, 100.0, 600.0, abs, inact),
            WaitEnd::TimeoutInactivity
        );
        // Absolute cap exceeded while still running → absolute timeout.
        assert_eq!(
            decide_wait(false, false, 3600.0, 5.0, abs, inact),
            WaitEnd::TimeoutAbs
        );
        // Idle after running → success, even at the deadline.
        assert_eq!(
            decide_wait(false, true, 3600.0, 600.0, abs, inact),
            WaitEnd::IdleDone
        );
        // Terminal state beats timeouts.
        assert_eq!(
            decide_wait(true, false, 3600.0, 600.0, abs, inact),
            WaitEnd::Terminal
        );
    }

    /// Fold a status timeline through the same progress/idle accounting the
    /// wait loop uses (fake clock): replicates the b76169c6 repro — a slow
    /// coding run with occasional output must not time out at old 300s.
    #[test]
    fn wait_policy_survives_slow_coding_run() {
        let abs = 3600.0;
        let inact = 600.0;
        // (state, tail, tool, settled, dt_secs)
        let timeline: Vec<(&str, &str, Option<&str>, Option<i64>, f64)> = vec![
            ("idle", "", None, None, 2.0), // send just dispatched
            ("running", "step 1", None, None, 120.0),
            ("running", "step 1", Some("wiki_observe"), None, 300.0),
            ("running", "step 2: editing", Some("wiki_observe"), None, 590.0),
            // Long quiet stretch, still running → progress, timer refreshes.
            ("running", "step 2: editing", Some("wiki_observe"), None, 590.0),
            ("idle", "done", Some("wiki_observe"), Some(1), 5.0),
        ];
        let mut prev = PiProgress::default();
        let mut saw_running = false;
        let mut abs_e = 0.0;
        let mut idle_e = 0.0;
        let mut end = WaitEnd::Continue;
        for (state, tail, tool, settled, dt) in &timeline {
            let cur = progress(state, tail, *tool, *settled);
            if cur.running {
                saw_running = true;
            }
            if cur.advanced_since(&prev) {
                idle_e = 0.0;
            } else {
                idle_e += dt;
            }
            prev = cur;
            abs_e += dt;
            let idle_done = *state == "idle" && saw_running;
            end = decide_wait(false, idle_done, abs_e, idle_e, abs, inact);
            if end != WaitEnd::Continue {
                break;
            }
        }
        assert!(abs_e > 300.0, "timeline must outlast the old flat killer");
        assert_eq!(end, WaitEnd::IdleDone);
    }

    #[test]
    fn wait_policy_stuck_send_times_out_inactivity() {
        // Steer accepted but the agent never starts: idle + no output.
        let mut prev = PiProgress::default();
        let mut abs_e = 0.0;
        let mut idle_e = 0.0;
        let mut end = WaitEnd::Continue;
        for _ in 0..7 {
            let cur = progress("idle", "", None, None);
            if cur.advanced_since(&prev) {
                idle_e = 0.0;
            } else {
                idle_e += 100.0;
            }
            prev = cur;
            abs_e += 100.0;
            end = decide_wait(false, false, abs_e, idle_e, 3600.0, 600.0);
            if end != WaitEnd::Continue {
                break;
            }
        }
        assert_eq!(end, WaitEnd::TimeoutInactivity);
    }
}
