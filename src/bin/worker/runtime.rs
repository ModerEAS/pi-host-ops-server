//! Job execution: local shell + owned `pi --mode rpc` children (no pi_svc).
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout};
use tokio::sync::{oneshot, Mutex};
use tokio::process::Command;
use tokio::time::{sleep, timeout};
use tracing::info;
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
    /// Optional pi binary path (default: `pi` from PATH).
    pub pi_bin: Option<PathBuf>,
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

// ── Direct `pi --mode rpc` sessions (no pi_svc) ──────────────────────────
// One long-lived child per directory, owned by the worker. Liveness is the
// OS process itself (`try_wait`); no status/list round-trips. Protocol
// mirrors /workspace/pi-runs/svc/pi_svcd.py (reference only, not vendored).

/// In-memory session state, fed by the stdout drain (replaces status.json).
/// Counters mirror pi_svcd.py's status.json for future debugging.
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct PiState {
    pub state: String, // starting|idle|running|error|dead|stopped
    pub settled_count: i64,
    pub agent_starts: i64,
    pub tool_count: i64,
    pub last_tool: Option<String>,
    pub last_event: Option<String>,
    text_buf: String, // recent assistant text, capped
    pub error: Option<String>,
    pub ui_dialogs: u64,
}

impl PiState {
    /// Last 4000 chars of assistant text.
    pub fn text_tail(&self) -> String {
        tail_chars(&self.text_buf, 4000)
    }
    /// Capped text history; result `text_log_tail` slices this further.
    pub fn text_log(&self) -> &str {
        &self.text_buf
    }
    fn push_text(&mut self, delta: &str) {
        if delta.is_empty() {
            return;
        }
        self.text_buf.push_str(delta);
        const CAP: usize = 32768;
        if self.text_buf.len() > CAP {
            let mut idx = self.text_buf.len() - CAP;
            while idx < self.text_buf.len() && !self.text_buf.is_char_boundary(idx) {
                idx += 1;
            }
            self.text_buf.drain(..idx);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SpawnParams {
    bin: String,
    work_dir: PathBuf,
    session: String,
    provider: String,
    model: String,
}

fn spawn_params(rt: &DirRuntime) -> SpawnParams {
    SpawnParams {
        bin: rt
            .pi_bin
            .clone()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "pi".into()),
        work_dir: rt.work_dir.clone(),
        session: rt.pi_session.clone(),
        provider: rt.provider.clone(),
        model: rt.model.clone(),
    }
}

/// Liveness of the owned child. `try_wait` is the source of truth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildAlive {
    Running,
    Exited(Option<i32>),
}

fn classify_wait(res: std::io::Result<Option<std::process::ExitStatus>>) -> ChildAlive {
    match res {
        Ok(None) => ChildAlive::Running,
        Ok(Some(status)) => ChildAlive::Exited(status.code()),
        Err(_) => ChildAlive::Exited(None),
    }
}

#[derive(Debug, Clone, Default)]
struct PromptAccept {
    success: bool,
    disposition: Option<String>,
    error: Option<String>,
}

impl PromptAccept {
    /// The drain routes only records carrying this prompt's id here.
    fn from_response(rec: &Value) -> Self {
        Self {
            success: rec.get("success").and_then(|v| v.as_bool()).unwrap_or(false),
            disposition: rec
                .get("data")
                .and_then(|d| d.get("disposition"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            error: rec
                .get("error")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        }
    }
}

/// Fold one RPC stdout record into session state (pure; unit-tested).
/// Appends unattended `extension_ui_response` replies to `outbox`.
fn apply_record(st: &mut PiState, rec: &Value, outbox: &mut Vec<Value>) {
    let t = rec.get("type").and_then(|v| v.as_str()).unwrap_or("");
    st.last_event = if t.is_empty() { None } else { Some(t.to_string()) };
    match t {
        "response" if rec.get("command").and_then(|v| v.as_str()) == Some("prompt") => {
            let ok = rec.get("success").and_then(|v| v.as_bool()).unwrap_or(false);
            let disp = rec
                .get("data")
                .and_then(|d| d.get("disposition"))
                .and_then(|v| v.as_str());
            if ok && disp == Some("handled") {
                // Synchronously completed: counts as settled, already idle.
                st.settled_count += 1;
                st.state = "idle".into();
                st.error = None;
            } else if ok {
                st.state = "running".into();
                st.error = None;
            } else {
                st.error = Some(
                    rec.get("error")
                        .and_then(|v| v.as_str())
                        .unwrap_or("prompt rejected")
                        .to_string(),
                );
            }
        }
        "agent_start" => {
            st.agent_starts += 1;
            st.state = "running".into();
        }
        "agent_settled" => {
            st.settled_count += 1;
            st.state = "idle".into();
        }
        "message_update" => {
            // Only text deltas extend the tail (mirror pi_svcd).
            let ev = rec.get("assistantMessageEvent");
            if ev.and_then(|e| e.get("type")).and_then(|v| v.as_str()) == Some("text_delta") {
                st.push_text(ev.and_then(|e| e.get("delta")).and_then(|v| v.as_str()).unwrap_or(""));
            }
        }
        "tool_execution_start" => {
            st.tool_count += 1;
            if let Some(name) = rec.get("toolName").and_then(|v| v.as_str()) {
                st.last_tool = Some(name.to_string());
            }
            st.state = "running".into();
        }
        "extension_ui_request" => {
            if let Some(reply) = ui_auto_reply(rec) {
                st.ui_dialogs += 1;
                outbox.push(reply);
            }
        }
        "error" => {
            st.error = Some(rec.to_string().chars().take(1000).collect());
            st.state = "error".into();
        }
        _ => {}
    }
}

/// Unattended answers for Pi UI dialogs (mirror pi_svcd defaults).
/// Fire-and-forget methods (setStatus/notify/...) need no reply → None.
fn ui_auto_reply(rec: &Value) -> Option<Value> {
    let req_id = rec.get("id").and_then(|v| v.as_str())?;
    match rec.get("method").and_then(|v| v.as_str())? {
        "confirm" => Some(json!({"type": "extension_ui_response", "id": req_id, "confirmed": true})),
        "select" => {
            let first = rec
                .get("options")
                .and_then(|v| v.as_array())
                .and_then(|a| a.first())
                .cloned()
                .unwrap_or(Value::Null);
            Some(json!({"type": "extension_ui_response", "id": req_id, "value": first}))
        }
        "input" => Some(json!({"type": "extension_ui_response", "id": req_id, "value": ""})),
        "editor" => Some(json!({"type": "extension_ui_response", "id": req_id, "cancelled": true})),
        _ => None,
    }
}

/// One owned `pi --mode rpc` child + its stdout/stderr drains.
/// Never held across awaits by callers: snapshot/clone out, or lock briefly.
pub struct PiSession {
    params: SpawnParams,
    state: Mutex<PiState>,
    child: Mutex<Child>,
    stdin: Mutex<Option<ChildStdin>>,
    pending: Mutex<HashMap<String, oneshot::Sender<Value>>>,
    stderr_tail: Mutex<String>,
}

impl PiSession {
    async fn spawn(rt: &DirRuntime) -> Result<Arc<Self>> {
        rt.validate_model()?;
        let params = spawn_params(rt);
        let mut cmd = Command::new(&params.bin);
        cmd.arg("--mode")
            .arg("rpc")
            .arg("--provider")
            .arg(&params.provider)
            .arg("--model")
            .arg(&params.model)
            .arg("--name")
            .arg(&params.session)
            .arg("--approve")
            .current_dir(&params.work_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = cmd.spawn().with_context(|| format!("spawn {}", params.bin))?;
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().context("pi stdout")?;
        let stderr = child.stderr.take().context("pi stderr")?;
        let sess = Arc::new(Self {
            params,
            state: Mutex::new(PiState { state: "starting".into(), ..Default::default() }),
            child: Mutex::new(child),
            stdin: Mutex::new(stdin),
            pending: Mutex::new(HashMap::new()),
            stderr_tail: Mutex::new(String::new()),
        });
        let s = sess.clone();
        tokio::spawn(async move { s.drain_stdout(stdout).await });
        let s = sess.clone();
        tokio::spawn(async move { s.drain_stderr(stderr).await });
        // Brief settle so the session loads; fail fast if it exits at once.
        sleep(Duration::from_millis(800)).await;
        if let ChildAlive::Exited(code) = sess.check().await {
            let tail = sess.stderr_tail.lock().await.clone();
            bail!("pi exited immediately code={code:?}: {}", tail_chars(&tail, 1000));
        }
        Ok(sess)
    }

    async fn drain_stdout(self: Arc<Self>, stdout: ChildStdout) {
        let mut lines = BufReader::new(stdout).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if line.trim().is_empty() {
                        continue;
                    }
                    self.on_line(&line).await;
                }
                _ => break, // EOF or read error: child gone
            }
        }
        self.mark_dead("pi stdout closed").await;
    }

    async fn drain_stderr(self: Arc<Self>, mut stderr: ChildStderr) {
        let mut buf = [0u8; 4096];
        loop {
            match stderr.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let s = String::from_utf8_lossy(&buf[..n]).into_owned();
                    let mut tail = self.stderr_tail.lock().await;
                    tail.push_str(&s);
                    const CAP: usize = 8192;
                    if tail.len() > CAP {
                        let mut idx = tail.len() - CAP;
                        while idx < tail.len() && !tail.is_char_boundary(idx) {
                            idx += 1;
                        }
                        tail.drain(..idx);
                    }
                }
                Err(_) => break,
            }
        }
    }

    async fn on_line(&self, line: &str) {
        let rec: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => {
                tracing::debug!(preview = %clip_chars(line, 200), "pi rpc: skip non-JSON line");
                return;
            }
        };
        // Fold into state first so prompt waiters observe post-accept state.
        let mut outbox = Vec::new();
        {
            let mut st = self.state.lock().await;
            apply_record(&mut st, &rec, &mut outbox);
        }
        for reply in outbox {
            if self.write_json(&reply).await.is_err() {
                let mut st = self.state.lock().await;
                st.error = Some("ui_response write failed: pi stdin closed".into());
                break;
            }
        }
        if let Some(rid) = rec.get("id").and_then(|v| v.as_str()) {
            if let Some(tx) = self.pending.lock().await.remove(rid) {
                let _ = tx.send(rec);
            }
        }
    }

    async fn mark_dead(&self, why: &str) {
        // Never overwrite a clean stop.
        {
            let mut st = self.state.lock().await;
            if st.state != "stopped" && st.state != "dead" {
                st.state = "dead".into();
                if st.error.is_none() {
                    st.error = Some(format!("{why} (pi child gone)"));
                }
            }
        }
        // Wake prompt waiters so sends fail instead of hanging.
        let mut pending = self.pending.lock().await;
        for (_, tx) in pending.drain() {
            let _ = tx.send(json!({"type": "response", "success": false, "error": "pi child exited"}));
        }
    }

    async fn write_json(&self, v: &Value) -> Result<()> {
        let raw = serde_json::to_vec(v)?;
        let mut g = self.stdin.lock().await;
        let stdin = g.as_mut().context("pi stdin closed")?;
        stdin.write_all(&raw).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await?;
        Ok(())
    }

    async fn check(&self) -> ChildAlive {
        classify_wait(self.child.lock().await.try_wait())
    }

    /// Liveness source of truth: the OS process itself.
    pub async fn is_alive(&self) -> bool {
        matches!(self.check().await, ChildAlive::Running)
    }

    pub async fn snapshot(&self) -> PiState {
        self.state.lock().await.clone()
    }

    fn matches(&self, rt: &DirRuntime) -> bool {
        self.params == spawn_params(rt)
    }

    /// Send one prompt; resolves when pi accepts (not when the agent settles).
    /// `behavior`: `steer` interrupts the current turn, `followUp` queues.
    async fn send_prompt(
        &self,
        id: &str,
        message: &str,
        behavior: Option<&str>,
    ) -> Result<PromptAccept> {
        if !self.is_alive().await {
            bail!("pi child not running");
        }
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id.to_string(), tx);
        let mut obj = json!({"id": id, "type": "prompt", "message": message});
        if let Some(b) = behavior {
            obj["streamingBehavior"] = json!(b);
        }
        if let Err(e) = self.write_json(&obj).await {
            self.pending.lock().await.remove(id);
            bail!("write prompt to pi: {e:#}");
        }
        match timeout(Duration::from_secs(60), rx).await {
            Ok(Ok(rec)) => Ok(PromptAccept::from_response(&rec)),
            Ok(Err(_)) => bail!("pi child exited while waiting for prompt accept"),
            Err(_) => {
                self.pending.lock().await.remove(id);
                bail!("timeout waiting for pi prompt accept");
            }
        }
    }

    /// Clean shutdown: ask politely, close stdin, wait briefly, then kill.
    /// Idempotent (second call reaps instantly).
    pub async fn shutdown(&self) {
        let _ = self.write_json(&json!({"type": "shutdown"})).await;
        self.stdin.lock().await.take(); // EOF for a deaf child
        let mut child = self.child.lock().await;
        if tokio::time::timeout(Duration::from_secs(10), child.wait()).await.is_err() {
            let _ = child.kill().await;
        }
        self.state.lock().await.state = "stopped".into();
    }
}

/// Owned long-lived `pi --mode rpc` children, keyed by session name.
#[derive(Default)]
pub struct PiSessions {
    inner: Mutex<HashMap<String, Arc<PiSession>>>,
}

impl PiSessions {
    /// Live child for this dir: reuse when params match and the process runs,
    /// else shut down the stale one and spawn fresh. Never fakes liveness.
    pub async fn ensure(&self, rt: &DirRuntime) -> Result<Arc<PiSession>> {
        // Bind first: the map guard must not be held across the awaits below
        // (tokio Mutex is not reentrant).
        let cached = { self.inner.lock().await.get(&rt.pi_session).cloned() };
        if let Some(s) = cached {
            if s.matches(rt) && s.is_alive().await {
                return Ok(s);
            }
            s.shutdown().await;
            self.inner.lock().await.remove(&rt.pi_session);
        }
        let s = PiSession::spawn(rt).await?;
        self.inner.lock().await.insert(rt.pi_session.clone(), s.clone());
        Ok(s)
    }

    /// None = never started; Some(true) = process running; Some(false) = exited.
    pub async fn probe(&self, name: &str) -> Option<bool> {
        let cached = { self.inner.lock().await.get(name).cloned() };
        match cached {
            Some(s) => Some(s.is_alive().await),
            None => None,
        }
    }

    /// Drop sessions for removed/disabled/renamed dirs (clean shutdown).
    pub async fn retain_active(&self, names: &std::collections::HashSet<String>) {
        let stale: Vec<Arc<PiSession>> = {
            let inner = self.inner.lock().await;
            inner
                .iter()
                .filter(|(k, _)| !names.contains(*k))
                .map(|(_, s)| s.clone())
                .collect()
        };
        if stale.is_empty() {
            return;
        }
        let mut handles = Vec::with_capacity(stale.len());
        for s in &stale {
            let c = s.clone();
            handles.push(tokio::spawn(async move { c.shutdown().await }));
        }
        for h in handles {
            let _ = h.await;
        }
        self.inner.lock().await.retain(|k, _| names.contains(k));
    }

    /// Terminate all children cleanly (worker shutdown path).
    pub async fn shutdown_all(&self) {
        let all: Vec<Arc<PiSession>> = self.inner.lock().await.values().cloned().collect();
        let mut handles = Vec::with_capacity(all.len());
        for s in &all {
            let c = s.clone();
            handles.push(tokio::spawn(async move { c.shutdown().await }));
        }
        for h in handles {
            let _ = h.await;
        }
        self.inner.lock().await.clear();
    }
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

/// Build text sent as the RPC prompt message.
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
    fn from_state(s: &PiState) -> Self {
        Self {
            running: s.state == "running",
            settled_count: Some(s.settled_count),
            last_tool: s.last_tool.clone(),
            text_tail: s.text_tail(),
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
    status: PiState,
    end: WaitEnd,
    saw_running: bool,
}

/// Activity-based wait for the Pi session to go idle after the prompt is
/// accepted. Same two budgets as before (absolute cap + inactivity budget
/// refreshed by observable progress). Progress now comes from the owned
/// child's in-memory state, and a dead child ends the wait as a failure —
/// never as idle success. Lease renewals + progress events continue as today.
async fn wait_until_idle_with_events(
    api: &ApiClient,
    sess: &PiSession,
    job_id: &str,
    abs_timeout_secs: f64,
    inactivity_secs: f64,
    event_interval_secs: f64,
    settled_before: Option<i64>,
    stop: &AtomicBool,
) -> WaitOutcome {
    let start = Instant::now();
    let mut last_progress_at = start;
    let mut prev = PiProgress::default();
    let mut last = PiState { state: "starting".into(), ..Default::default() };
    let mut saw_running = false;
    let mut last_event_at = Instant::now() - Duration::from_secs(3600);
    let mut last_tail = String::new();
    let mut last_tool: Option<String> = None;

    loop {
        if stop.load(Ordering::Relaxed) {
            return WaitOutcome { status: last, end: WaitEnd::Stopped, saw_running };
        }
        // The OS process is the source of truth: a dead child ends the wait
        // as a failure even if the last snapshot looked healthy.
        let alive = sess.check().await;
        let mut cur_state = sess.snapshot().await;
        if let ChildAlive::Exited(code) = alive {
            if cur_state.state != "stopped" {
                cur_state.state = "dead".into();
            }
            if cur_state.error.is_none() {
                cur_state.error = Some(format!("pi child exited code={code:?}"));
            }
        }
        let cur = PiProgress::from_state(&cur_state);
        if cur.running {
            saw_running = true;
        }
        let now = Instant::now();
        if cur.advanced_since(&prev) {
            last_progress_at = now;
        }
        prev = cur;
        last = cur_state;
        let tool = last.last_tool.clone();
        let text_tail = last.text_tail();
        if now.duration_since(last_event_at).as_secs_f64() >= event_interval_secs {
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
            api.post_event(job_id, "status", Some(&format!("state={}", last.state)), Some(json!({"state": last.state, "settled_count": last.settled_count}))).await;
            api.renew_lease(job_id).await;
            last_event_at = now;
        }
        let terminal = matches!(last.state.as_str(), "error" | "dead" | "stopped");
        let bumped = matches!(settled_before, Some(b) if last.settled_count > b);
        let idle_done = last.state == "idle" && (bumped || saw_running);
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

async fn run_via_pi(
    api: &ApiClient,
    sessions: &PiSessions,
    rt: &DirRuntime,
    job: &Job,
    stop: &AtomicBool,
) -> (bool, Option<Value>, Option<String>) {
    // Spawn (or reuse) the owned `pi --mode rpc` child. A dead child is
    // respawned here; liveness is never faked from stale state.
    let sess = match sessions.ensure(rt).await {
        Ok(s) => s,
        Err(e) => return (false, None, Some(format!("pi spawn failed: {e:#}"))),
    };
    let settled_before = Some(sess.snapshot().await.settled_count);
    let prompt = build_pi_prompt(job);
    // Absolute wait cap: explicit job timeout wins, else the 1h default.
    // The inactivity budget (no observable progress) applies on top.
    let abs_timeout_secs = job.timeout_secs.filter(|&t| t > 0).map(|t| t as f64).unwrap_or(DEFAULT_PI_ABS_TIMEOUT as f64);
    let inactivity_secs = DEFAULT_PI_INACTIVITY_SECS;

    let delivery = job.delivery_mode.as_deref().unwrap_or("steer").trim().to_lowercase();
    let job_id = job.id.to_string();
    let busy = sess.snapshot().await.state == "running";
    let (label, behavior): (&str, Option<&str>) = if delivery == "steer" {
        ("steer", Some("steer"))
    } else if busy {
        ("queue+busy", Some("followUp"))
    } else {
        ("queue+idle", None)
    };
    info!(delivery = %delivery, label, session = %rt.pi_session, "pi rpc send");
    api.post_event(&job_id, "status", Some(&format!("pi rpc send ({delivery})")), Some(json!({"delivery_mode": delivery}))).await;
    // The prompt rides inline in the RPC record — no temp prompt file.
    let req_id = format!("p{}", Uuid::new_v4());
    let accept = match sess.send_prompt(&req_id, &prompt, behavior).await {
        Ok(a) => a,
        Err(e) => return (false, None, Some(format!("pi rpc send failed: {e:#}"))),
    };
    if !accept.success {
        return (false, None, Some(format!("pi rejected prompt: {}", accept.error.unwrap_or_else(|| "unknown".into()))));
    }
    info!(session = %rt.pi_session, disposition = ?accept.disposition, "pi rpc prompt accepted");
    let outcome = wait_until_idle_with_events(api, &sess, &job_id, abs_timeout_secs, inactivity_secs, rt.event_interval_secs, settled_before, stop).await;
    let st = outcome.status;
    let text_tail = st.text_tail();
    let wait_timeout = matches!(outcome.end, WaitEnd::TimeoutAbs | WaitEnd::TimeoutInactivity);
    let timeout_kind = match outcome.end {
        WaitEnd::TimeoutAbs => Some("absolute"),
        WaitEnd::TimeoutInactivity => Some("inactivity"),
        _ => None,
    };
    let result = truncate_result(json!({
        "via": "pi_rpc",
        "session": rt.pi_session,
        "provider": rt.provider,
        "model": rt.model,
        "delivery_mode": delivery,
        "state": st.state,
        "settled_count": st.settled_count,
        "last_tool": st.last_tool,
        "text_tail": text_tail,
        "text_log_tail": tail_chars(st.text_log(), 8000),
        "wait_timeout": wait_timeout,
        "timeout_kind": timeout_kind,
    }));
    let mut ok = true;
    let mut err_msg = None;
    if st.state == "dead" {
        // The OS process is gone — never stale status. Next act respawns.
        ok = false;
        err_msg = Some(format!(
            "pi child process exited{}; session restarts on next job",
            st.error.as_deref().map(|e| format!(" ({e})")).unwrap_or_default(),
        ));
    } else if matches!(st.state.as_str(), "error" | "stopped") {
        ok = false;
        err_msg = Some(st.error.clone().unwrap_or_else(|| format!("session state={}", st.state)));
    } else if outcome.end == WaitEnd::TimeoutAbs {
        // Never claim success on timeout — but say plainly the agent was
        // still working, so this reads as "needs longer", not "task failed".
        ok = false;
        err_msg = Some(format!(
            "absolute Pi wait timeout after {}s: agent still {}",
            abs_timeout_secs as u64,
            st.state,
        ));
    } else if outcome.end == WaitEnd::TimeoutInactivity {
        ok = false;
        err_msg = Some(if !outcome.saw_running && st.state != "idle" {
            format!(
                "Pi inactivity timeout after {}s: send accepted but agent never started (state={})",
                inactivity_secs as u64,
                st.state,
            )
        } else {
            format!(
                "Pi inactivity timeout after {}s with no progress (state={}; absolute cap {}s)",
                inactivity_secs as u64,
                st.state,
                abs_timeout_secs as u64,
            )
        });
    }
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

pub async fn execute_job(api: &ApiClient, sessions: &PiSessions, rt: &DirRuntime, job: &Job, stop: &AtomicBool) -> (bool, Option<Value>, Option<String>) {
    info!(id = %job.id, job_type = %job.job_type, agent = %rt.agent_id, "execute job");
    if job.job_type == "shell" {
        let has_cmd = job.payload.get("command").and_then(|v| v.as_str()).map(|s| !s.trim().is_empty()).unwrap_or(false);
        if has_cmd {
            return run_shell_local(rt, job).await;
        }
        return run_via_pi(api, sessions, rt, job, stop).await;
    }
    if PI_JOB_TYPES.contains(&job.job_type.as_str()) {
        return run_via_pi(api, sessions, rt, job, stop).await;
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

    fn progress(state: &str, tail: &str, tool: Option<&str>, settled: i64) -> PiProgress {
        let mut st = PiState::default();
        st.state = state.into();
        st.push_text(tail);
        st.last_tool = tool.map(|s| s.to_string());
        st.settled_count = settled;
        PiProgress::from_state(&st)
    }

    #[test]
    fn progress_advances_on_running_or_changed_output() {
        // Idle with no output change is not progress.
        let base = progress("idle", "", None, 0);
        assert!(!base.advanced_since(&progress("idle", "", None, 0)));
        // Running alone counts — a thinking agent emitting nothing is working.
        assert!(progress("running", "", None, 0).advanced_since(&base));
        // New text / tool / settled count counts even when not running.
        assert!(progress("idle", "ab", None, 0).advanced_since(&base));
        assert!(progress("idle", "", Some("wiki_observe"), 0).advanced_since(&base));
        assert!(progress("idle", "", None, 1).advanced_since(&base));
        // Identical snapshot twice is not progress.
        let same = progress("idle", "ab", Some("wiki_observe"), 1);
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
        let timeline: Vec<(&str, &str, Option<&str>, i64, f64)> = vec![
            ("idle", "", None, 0, 2.0), // send just dispatched
            ("running", "step 1", None, 0, 120.0),
            ("running", "step 1", Some("wiki_observe"), 0, 300.0),
            ("running", "step 2: editing", Some("wiki_observe"), 0, 590.0),
            // Long quiet stretch, still running → progress, timer refreshes.
            ("running", "step 2: editing", Some("wiki_observe"), 0, 590.0),
            ("idle", "done", Some("wiki_observe"), 1, 5.0),
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
            let cur = progress("idle", "", None, 0);
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

    fn record(t: &str) -> Value {
        json!({"type": t})
    }

    /// RPC stdout → session state mapping (mirrors pi_svcd.py semantics).
    #[test]
    fn rpc_record_mapping() {
        let mut st = PiState::default();
        let mut outbox = Vec::new();
        // Prompt accepted + started → running.
        apply_record(&mut st, &json!({"id":"p1","type":"response","command":"prompt","success":true,"data":{"disposition":"started"}}), &mut outbox);
        assert_eq!(st.state, "running");
        assert!(outbox.is_empty());
        // Agent lifecycle with text + tool output.
        apply_record(&mut st, &record("agent_start"), &mut outbox);
        assert_eq!(st.agent_starts, 1);
        apply_record(&mut st, &json!({"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"hello "}}), &mut outbox);
        apply_record(&mut st, &json!({"type":"tool_execution_start","toolName":"bash"}), &mut outbox);
        assert_eq!(st.last_tool.as_deref(), Some("bash"));
        assert_eq!(st.tool_count, 1);
        apply_record(&mut st, &record("agent_settled"), &mut outbox);
        assert_eq!(st.state, "idle");
        assert_eq!(st.settled_count, 1);
        assert_eq!(st.text_tail(), "hello ");
        // Non-text deltas don't extend the tail.
        apply_record(&mut st, &json!({"type":"message_update","assistantMessageEvent":{"type":"thinking_delta","delta":"hmm"}}), &mut outbox);
        assert_eq!(st.text_tail(), "hello ");
        // Synchronously-handled prompt settles immediately.
        apply_record(&mut st, &json!({"id":"p2","type":"response","command":"prompt","success":true,"data":{"disposition":"handled"}}), &mut outbox);
        assert_eq!(st.settled_count, 2);
        assert_eq!(st.state, "idle");
        // Rejected prompt records the error, settles nothing.
        apply_record(&mut st, &json!({"id":"p3","type":"response","command":"prompt","success":false,"error":"nope"}), &mut outbox);
        assert_eq!(st.settled_count, 2);
        assert_eq!(st.error.as_deref(), Some("nope"));
        // Dialogs get unattended replies; fire-and-forget gets none.
        outbox.clear();
        apply_record(&mut st, &json!({"id":"d1","type":"extension_ui_request","method":"confirm"}), &mut outbox);
        assert_eq!(outbox.len(), 1);
        assert_eq!(outbox[0]["confirmed"], json!(true));
        outbox.clear();
        apply_record(&mut st, &json!({"id":"d2","type":"extension_ui_request","method":"select","options":["a","b"]}), &mut outbox);
        assert_eq!(outbox[0]["value"], json!("a"));
        outbox.clear();
        apply_record(&mut st, &json!({"id":"d3","type":"extension_ui_request","method":"input"}), &mut outbox);
        assert_eq!(outbox[0]["value"], json!(""));
        outbox.clear();
        apply_record(&mut st, &json!({"id":"d4","type":"extension_ui_request","method":"editor"}), &mut outbox);
        assert_eq!(outbox[0]["cancelled"], json!(true));
        outbox.clear();
        apply_record(&mut st, &json!({"id":"d5","type":"extension_ui_request","method":"setStatus"}), &mut outbox);
        apply_record(&mut st, &json!({"id":"d6","type":"extension_ui_request","method":"notify"}), &mut outbox);
        assert!(outbox.is_empty());
        assert_eq!(st.ui_dialogs, 4);
        // Error record marks the session.
        apply_record(&mut st, &json!({"type":"error","message":"boom"}), &mut outbox);
        assert_eq!(st.state, "error");
        assert!(st.error.is_some());
        // Prompt-accept parsing for the send path.
        let a = PromptAccept::from_response(&json!({"id":"p1","type":"response","command":"prompt","success":true,"data":{"disposition":"started"}}));
        assert!(a.success);
        assert_eq!(a.disposition.as_deref(), Some("started"));
        let a = PromptAccept::from_response(&json!({"type":"response","success":false,"error":"pi child exited"}));
        assert!(!a.success);
        assert_eq!(a.error.as_deref(), Some("pi child exited"));
    }

    #[test]
    fn child_wait_classification() {
        // Still running → alive.
        assert_eq!(classify_wait(Ok(None)), ChildAlive::Running);
        // Lookup errors are treated as dead, never as alive.
        assert_eq!(
            classify_wait(Err(std::io::Error::new(std::io::ErrorKind::Other, "x"))),
            ChildAlive::Exited(None)
        );
    }

    /// A real exited OS process classifies as not-alive (no pi binary needed).
    #[cfg(unix)]
    #[test]
    fn exited_process_is_not_alive() {
        let status = std::process::Command::new("true").status().unwrap();
        assert_eq!(classify_wait(Ok(Some(status))), ChildAlive::Exited(Some(0)));
    }

    #[cfg(windows)]
    #[test]
    fn exited_process_is_not_alive() {
        let status = std::process::Command::new("cmd").args(["/C", "exit 3"]).status().unwrap();
        assert_eq!(classify_wait(Ok(Some(status))), ChildAlive::Exited(Some(3)));
    }
}
