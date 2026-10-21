//! host-ops box worker: heartbeat + long-poll → local shell / pi_svc → events + result.
//!
//! Never stops other pi_svc sessions. Never uses Fast / CloudAgent.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::process::Command;
use tokio::signal;
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const MAX_RESULT_BYTES: usize = 512 * 1024;
const DEFAULT_SHELL_TIMEOUT: u64 = 60;
const DEFAULT_PI_TIMEOUT: u64 = 300;
const POLL_STATUS_INTERVAL: f64 = 2.0;
const BACKOFF_START: f64 = 1.0;
const BACKOFF_MAX: f64 = 30.0;

const PI_JOB_TYPES: &[&str] = &["observe", "act", "open", "clipboard", "notify"];

#[derive(Clone, Debug)]
struct Config {
    host_ops_url: String,
    agent_token: String,
    agent_id: String,
    pi_svc: PathBuf,
    pi_session: String,
    work_dir: PathBuf,
    poll_wait_secs: u64,
    heartbeat_secs: f64,
    event_interval_secs: f64,
    provider: String,
    model: String,
}

impl Config {
    fn from_env() -> Result<Self> {
        let _ = dotenvy::dotenv();

        let model = env_or("PI_MODEL", "muse-spark-1.3-contributor");
        let provider = env_or("PI_PROVIDER", "opencode-go");
        if model.to_lowercase().contains("fast") || provider.to_lowercase().contains("fast") {
            bail!("PI_MODEL/PI_PROVIDER must not be Fast (cost rule)");
        }

        let agent_token = std::env::var("AGENT_TOKEN")
            .unwrap_or_default()
            .trim()
            .to_string();
        if agent_token.is_empty() {
            bail!("AGENT_TOKEN is required");
        }

        let pi_svc = PathBuf::from(env_or("PI_SVC", "/workspace/pi-runs/pi_svc"));
        if !pi_svc.is_file() {
            bail!("PI_SVC not found: {}", pi_svc.display());
        }

        let work_dir = PathBuf::from(env_or(
            "WORK_DIR",
            "/workspace/pi-host-ops-server/workspace",
        ));
        std::fs::create_dir_all(&work_dir)
            .with_context(|| format!("mkdir WORK_DIR {}", work_dir.display()))?;

        let agent_id = env_or("AGENT_ID", "box-pi");
        let agent_id = if agent_id.trim().is_empty() {
            "box-pi".into()
        } else {
            agent_id
        };

        let pi_session = env_or("PI_SESSION", "host-ops");
        let pi_session = if pi_session.trim().is_empty() {
            "host-ops".into()
        } else {
            pi_session
        };

        Ok(Self {
            host_ops_url: env_or("HOST_OPS_URL", "http://127.0.0.1:8787")
                .trim_end_matches('/')
                .to_string(),
            agent_token,
            agent_id,
            pi_svc,
            pi_session,
            work_dir,
            poll_wait_secs: env_parse("POLL_WAIT_SECS", 20),
            heartbeat_secs: env_parse("HEARTBEAT_SECS", 20.0),
            event_interval_secs: env_parse("EVENT_INTERVAL_SECS", 5.0),
            provider,
            model,
        })
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn clip_chars(s: &str, max: usize) -> String {
    let mut out: String = s.chars().take(max).collect();
    if s.chars().count() > max {
        out.push('…');
    }
    out
}

fn tail_chars(s: &str, max: usize) -> String {
    let count = s.chars().count();
    if count <= max {
        return s.to_string();
    }
    s.chars().skip(count - max).collect()
}

/// Ensure JSON-serialized result stays under MAX_RESULT_BYTES.
fn truncate_result(obj: Value) -> Value {
    let raw = match serde_json::to_string(&obj) {
        Ok(s) => s,
        Err(_) => return json!({"truncated": true, "note": "serialize failed"}),
    };
    if raw.len() <= MAX_RESULT_BYTES {
        return obj;
    }

    if let Value::Object(map) = &obj {
        let mut out = map.clone();
        for key in [
            "text_tail",
            "stdout",
            "stderr",
            "status",
            "output",
            "text_log_tail",
        ] {
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
        if serde_json::to_string(&out_val)
            .map(|s| s.len() <= MAX_RESULT_BYTES)
            .unwrap_or(false)
        {
            return out_val;
        }
        return json!({
            "truncated": true,
            "note": "result exceeded 512KB; see pi_svc logs",
            "keys": map.keys().cloned().collect::<Vec<_>>(),
        });
    }
    json!({"truncated": true, "note": "result too large"})
}

struct ApiClient {
    http: reqwest::Client,
    cfg: Config,
}

impl ApiClient {
    fn new(cfg: Config) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .context("build reqwest client")?;
        Ok(Self { http, cfg })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.cfg.host_ops_url, path)
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
            .bearer_auth(&self.cfg.agent_token)
            .header("Accept", "application/json")
            .header("X-Agent-Id", &self.cfg.agent_id)
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
            Err(_) => Ok((
                status,
                Some(Value::String(String::from_utf8_lossy(&bytes).into_owned())),
            )),
        }
    }

    async fn heartbeat(&self) {
        match self
            .request(
                reqwest::Method::POST,
                "/v1/agent/heartbeat",
                Some(json!({"status": "online"})),
                15,
            )
            .await
        {
            Ok((status, resp)) if status >= 400 => {
                warn!(status, ?resp, "heartbeat HTTP error");
            }
            Ok(_) => {}
            Err(e) => warn!(error = %e, "heartbeat failed"),
        }
    }

    async fn post_event(
        &self,
        job_id: &str,
        event_type: &str,
        message: Option<&str>,
        data: Option<Value>,
    ) {
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
        let path = format!("/v1/agent/jobs/{job_id}/events");
        match self
            .request(reqwest::Method::POST, &path, Some(body), 15)
            .await
        {
            Ok((status, resp)) if status >= 400 => {
                debug!(status, ?resp, "post_event HTTP error");
            }
            Err(e) => debug!(error = %e, "post_event failed"),
            _ => {}
        }
    }

    async fn renew_lease(&self, job_id: &str) {
        let path = format!("/v1/agent/jobs/{job_id}/lease");
        let _ = self
            .request(reqwest::Method::POST, &path, Some(json!({})), 15)
            .await;
    }

    async fn poll_once(&self) -> Result<Option<Job>> {
        let client_timeout = self.cfg.poll_wait_secs + 15;
        let (status, resp) = self
            .request(
                reqwest::Method::POST,
                "/v1/agent/poll",
                Some(json!({"wait_secs": self.cfg.poll_wait_secs})),
                client_timeout,
            )
            .await?;
        if status == 204 {
            return Ok(None);
        }
        if status == 200 {
            if let Some(Value::Object(map)) = &resp {
                if let Some(job_v) = map.get("job") {
                    let job: Job =
                        serde_json::from_value(job_v.clone()).context("parse poll job")?;
                    return Ok(Some(job));
                }
                if map.contains_key("id") {
                    let job: Job = serde_json::from_value(resp.clone().unwrap())
                        .context("parse bare job")?;
                    return Ok(Some(job));
                }
            }
            return Ok(None);
        }
        if status >= 400 {
            bail!("poll HTTP {status}: {resp:?}");
        }
        Ok(None)
    }

    async fn submit_result(
        &self,
        job_id: &str,
        ok: bool,
        result: Option<Value>,
        error: Option<String>,
    ) -> Result<()> {
        let mut body = json!({"ok": ok});
        if let Some(r) = result {
            body["result"] = truncate_result(r);
        }
        if !ok {
            body["error"] = Value::String(error.unwrap_or_else(|| "failed".into()));
        } else if let Some(e) = error {
            body["error"] = Value::String(e);
        }
        let path = format!("/v1/agent/jobs/{job_id}/result");
        let (status, resp) = self
            .request(reqwest::Method::POST, &path, Some(body), 60)
            .await?;
        if status >= 400 {
            error!(status, ?resp, job_id, "submit_result HTTP error");
        } else {
            info!(job_id, ok, status, "submitted result");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
struct Job {
    id: Uuid,
    #[serde(rename = "type")]
    job_type: String,
    #[serde(default)]
    payload: Value,
    #[serde(default)]
    timeout_secs: Option<i64>,
    #[serde(default)]
    delivery_mode: Option<String>,
}

struct CmdOut {
    code: i32,
    stdout: String,
    stderr: String,
}

async fn run_cmd(argv: &[String], timeout_secs: Option<f64>, cwd: Option<&Path>) -> CmdOut {
    let mut cmd = Command::new(&argv[0]);
    if argv.len() > 1 {
        cmd.args(&argv[1..]);
    }
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(c) = cwd {
        cmd.current_dir(c);
    }

    let fut = async {
        let output = cmd.output().await.context("spawn command")?;
        Ok::<CmdOut, anyhow::Error>(CmdOut {
            code: output.status.code().unwrap_or(1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    };

    match timeout_secs {
        Some(t) if t > 0.0 => match timeout(Duration::from_secs_f64(t), fut).await {
            Ok(Ok(o)) => o,
            Ok(Err(e)) => CmdOut {
                code: 1,
                stdout: String::new(),
                stderr: e.to_string(),
            },
            Err(_) => CmdOut {
                code: 124,
                stdout: String::new(),
                stderr: format!("timeout after {t}s"),
            },
        },
        _ => match fut.await {
            Ok(o) => o,
            Err(e) => CmdOut {
                code: 1,
                stdout: String::new(),
                stderr: e.to_string(),
            },
        },
    }
}

async fn pi_svc(cfg: &Config, args: &[&str], timeout_secs: f64) -> CmdOut {
    let mut argv: Vec<String> = vec![cfg.pi_svc.display().to_string()];
    argv.extend(args.iter().map(|s| (*s).to_string()));
    run_cmd(&argv, Some(timeout_secs), None).await
}

async fn session_running(cfg: &Config) -> bool {
    let out = pi_svc(cfg, &["list"], 30.0).await;
    if out.code != 0 {
        return false;
    }
    let Ok(sessions) = serde_json::from_str::<Value>(&out.stdout) else {
        return false;
    };
    let Some(arr) = sessions.as_array() else {
        return false;
    };
    for s in arr {
        if s.get("name").and_then(|v| v.as_str()) == Some(cfg.pi_session.as_str()) {
            return s.get("running").and_then(|v| v.as_bool()).unwrap_or(false);
        }
    }
    false
}

fn status_inner(st: &Value) -> &Value {
    st.get("status").filter(|v| v.is_object()).unwrap_or(st)
}

async fn get_status(cfg: &Config) -> Option<Value> {
    let out = pi_svc(cfg, &["status", &cfg.pi_session], 30.0).await;
    if out.code != 0 {
        warn!(stderr = %out.stderr, stdout = %out.stdout, "status failed");
        return None;
    }
    match serde_json::from_str::<Value>(&out.stdout) {
        Ok(v) => Some(v),
        Err(_) => Some(json!({"raw": out.stdout})),
    }
}

async fn ensure_session(cfg: &Config) -> Result<()> {
    if session_running(cfg).await {
        info!(session = %cfg.pi_session, "reusing pi_svc session");
        return Ok(());
    }
    info!(
        session = %cfg.pi_session,
        cwd = %cfg.work_dir.display(),
        provider = %cfg.provider,
        model = %cfg.model,
        "starting pi_svc session"
    );
    let cwd = cfg.work_dir.display().to_string();
    let out = pi_svc(
        cfg,
        &[
            "start",
            &cfg.pi_session,
            "--cwd",
            &cwd,
            "--provider",
            &cfg.provider,
            "--model",
            &cfg.model,
        ],
        120.0,
    )
    .await;
    if out.code != 0 {
        let msg = if out.stderr.is_empty() {
            out.stdout
        } else {
            out.stderr
        };
        bail!("pi_svc start failed ({}): {msg}", out.code);
    }
    for _ in 0..30 {
        if let Some(st) = get_status(cfg).await {
            let state = status_inner(&st)
                .get("state")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if matches!(state, "idle" | "running") {
                return Ok(());
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
    warn!(
        session = %cfg.pi_session,
        "pi_svc session start did not reach idle quickly; continuing"
    );
    Ok(())
}

fn build_pi_prompt(job: &Job) -> String {
    let job_type = &job.job_type;
    let job_id = job.id;
    let payload_json = serde_json::to_string_pretty(&job.payload).unwrap_or_else(|_| "{}".into());
    let timeout = job.timeout_secs;
    format!(
        r#"你是 host-ops box worker 驱动的 Pi 会话。请完成下面这一条远程任务，然后用简短中文+英文总结结果（不要开新 session，不要 Fast）。

You are driven by the host-ops worker. Complete this single remote job, then summarize briefly in Chinese + English. Do not start other sessions. Do not use Fast.

job_id: {job_id}
type: {job_type}
timeout_secs: {timeout:?}

payload:
```json
{payload_json}
```

任务类型说明 / type hints:
- observe: 观察当前环境（cwd、关键文件、进程摘要等），回报可读摘要。 Observe environment; return a readable summary.
- act: 按 payload 执行具体操作（编辑/运行等）。 Perform the action described in payload.
- open: 打开/定位 payload 中的路径或 URL（在本机 box 文件系统语义下）。 Open/locate path or URL from payload.
- clipboard: 读写剪贴板相关请求（若本机无 GUI 剪贴板则说明不可用）。 Clipboard get/set if available.
- notify: 记录/发送一条通知类消息到日志或合理通道。 Emit a notification/log of the message.

完成后停在 idle；把关键输出写在最终回复里。 When done, settle idle; put key output in your final reply.
"#
    )
}

async fn wait_until_idle_with_events(
    api: &ApiClient,
    cfg: &Config,
    job_id: &str,
    timeout_secs: f64,
    settled_before: Option<i64>,
    stop: &AtomicBool,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs_f64(timeout_secs.max(1.0));
    let mut last = json!({});
    let mut saw_running = false;
    let mut last_event_at = Instant::now() - Duration::from_secs(3600);
    let mut last_tail = String::new();
    let mut last_tool: Option<String> = None;

    while Instant::now() < deadline {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let st = get_status(cfg).await.unwrap_or_else(|| json!({}));
        last = st.clone();
        let inner = status_inner(&st);
        let state = inner.get("state").and_then(|v| v.as_str());
        let settled = inner.get("settled_count").and_then(|v| v.as_i64());
        let tool = inner
            .get("last_tool")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let text_tail = inner
            .get("text_tail")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if state == Some("running") {
            saw_running = true;
        }

        let now = Instant::now();
        if now.duration_since(last_event_at).as_secs_f64() >= cfg.event_interval_secs {
            if let Some(ref t) = tool {
                if last_tool.as_ref() != Some(t) {
                    api.post_event(job_id, "tool", Some(t), Some(json!({"last_tool": t})))
                        .await;
                    last_tool = Some(t.clone());
                }
            }
            if !text_tail.is_empty() && text_tail != last_tail {
                let tip = tail_chars(&text_tail, 1500);
                api.post_event(job_id, "text", Some(&tip), None).await;
                last_tail = text_tail;
            }
            api.post_event(
                job_id,
                "status",
                Some(&format!("state={}", state.unwrap_or(""))),
                Some(json!({"state": state, "settled_count": settled})),
            )
            .await;
            api.renew_lease(job_id).await;
            last_event_at = now;
        }

        if matches!(state, Some("error" | "dead" | "stopped")) {
            return last;
        }
        if state == Some("idle") {
            let bumped = matches!((settled, settled_before), (Some(s), Some(b)) if s > b);
            if bumped || saw_running {
                return last;
            }
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

async fn run_via_pi(
    api: &ApiClient,
    cfg: &Config,
    job: &Job,
    stop: &AtomicBool,
) -> (bool, Option<Value>, Option<String>) {
    if let Err(e) = ensure_session(cfg).await {
        return (false, None, Some(e.to_string()));
    }

    let before = get_status(cfg).await.unwrap_or_else(|| json!({}));
    let settled_before = status_inner(&before)
        .get("settled_count")
        .and_then(|v| v.as_i64());

    let prompt = build_pi_prompt(job);
    let timeout_secs = job
        .timeout_secs
        .filter(|&t| t > 0)
        .map(|t| t as f64)
        .unwrap_or(DEFAULT_PI_TIMEOUT as f64);

    let prompt_path = cfg.work_dir.join(format!("host-ops-{}-prompt.txt", job.id));
    if let Err(e) = tokio::fs::write(&prompt_path, prompt.as_bytes()).await {
        return (false, None, Some(format!("write prompt: {e}")));
    }

    let delivery = job
        .delivery_mode
        .as_deref()
        .unwrap_or("steer")
        .trim()
        .to_lowercase();
    let job_id = job.id.to_string();

    let cleanup = |path: PathBuf| async move {
        let _ = tokio::fs::remove_file(&path).await;
    };

    let st_now = get_status(cfg).await.unwrap_or_else(|| json!({}));
    let state = status_inner(&st_now)
        .get("state")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let prompt_path_s = prompt_path.display().to_string();
    let session = cfg.pi_session.as_str();

    let (label, send_args): (&str, Vec<&str>) = if delivery == "steer" {
        (
            "steer",
            vec!["send", session, "--steer", "--file", &prompt_path_s],
        )
    } else if state == "running" {
        (
            "queue+busy",
            vec!["send", session, "--follow-up", "--file", &prompt_path_s],
        )
    } else {
        ("queue+idle", vec!["send", session, "--file", &prompt_path_s])
    };
    info!(delivery = %delivery, label, "pi_svc send");

    api.post_event(
        &job_id,
        "status",
        Some(&format!("pi_svc send ({delivery})")),
        Some(json!({"delivery_mode": delivery})),
    )
    .await;

    let send_timeout = (timeout_secs + 30.0).min(120.0);
    let out = pi_svc(cfg, &send_args, send_timeout).await;
    if out.code != 0 {
        cleanup(prompt_path).await;
        let err = if out.stderr.is_empty() {
            out.stdout
        } else {
            out.stderr
        };
        return (
            false,
            None,
            Some(format!("pi_svc send failed ({}): {err}", out.code)),
        );
    }

    let final_st =
        wait_until_idle_with_events(api, cfg, &job_id, timeout_secs, settled_before, stop).await;
    let inner_f = status_inner(&final_st);
    let text_tail = inner_f
        .get("text_tail")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let n40 = "40";
    let tout = pi_svc(cfg, &["tail", session, "--text", "-n", n40], 30.0).await;
    let text_log = if tout.code == 0 {
        tout.stdout
    } else {
        String::new()
    };

    let wait_timeout = final_st
        .get("_wait_timeout")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let state_f = inner_f.get("state").and_then(|v| v.as_str());

    let result = truncate_result(json!({
        "via": "pi_svc",
        "session": cfg.pi_session,
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
        err_msg = Some(
            inner_f
                .get("error")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("session state={state_f:?}")),
        );
    } else if wait_timeout {
        ok = false;
        err_msg = Some(format!(
            "timed out waiting for idle after {timeout_secs}s"
        ));
    }

    cleanup(prompt_path).await;
    (ok, Some(result), err_msg)
}

async fn run_shell_local(job: &Job) -> (bool, Option<Value>, Option<String>) {
    let command = job
        .payload
        .get("command")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let Some(command) = command.filter(|c| !c.trim().is_empty()) else {
        return (
            false,
            None,
            Some("shell job missing payload.command string".into()),
        );
    };

    let timeout_secs = job
        .timeout_secs
        .or_else(|| job.payload.get("timeout_secs").and_then(|v| v.as_i64()))
        .filter(|&t| t > 0)
        .map(|t| t as f64)
        .unwrap_or(DEFAULT_SHELL_TIMEOUT as f64);

    let cwd = job
        .payload
        .get("cwd")
        .and_then(|v| v.as_str())
        .map(PathBuf::from);

    info!(
        command = %clip_chars(&command, 200),
        timeout = timeout_secs,
        "local shell"
    );

    let mut cmd = Command::new("bash");
    cmd.arg("-c").arg(&command);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(ref c) = cwd {
        cmd.current_dir(c);
    }

    match timeout(Duration::from_secs_f64(timeout_secs), cmd.output()).await {
        Ok(Ok(output)) => {
            let code = output.status.code().unwrap_or(1);
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            let result = truncate_result(json!({
                "via": "local_shell",
                "command": command,
                "cwd": cwd.as_ref().map(|p| p.display().to_string()),
                "exit_code": code,
                "stdout": tail_chars(&stdout, 200_000),
                "stderr": tail_chars(&stderr, 50_000),
            }));
            let ok = code == 0;
            let err = if ok {
                None
            } else {
                Some(format!("exit_code={code}"))
            };
            (ok, Some(result), err)
        }
        Ok(Err(e)) => (false, None, Some(format!("shell spawn failed: {e}"))),
        Err(_) => (
            false,
            Some(truncate_result(json!({
                "via": "local_shell",
                "command": command,
                "timeout": true,
                "stdout": "",
                "stderr": "",
            }))),
            Some(format!("shell timed out after {timeout_secs}s")),
        ),
    }
}

async fn execute_job(
    api: &ApiClient,
    cfg: &Config,
    job: &Job,
    stop: &AtomicBool,
) -> (bool, Option<Value>, Option<String>) {
    info!(id = %job.id, job_type = %job.job_type, "execute job");
    if job.job_type == "shell" {
        let has_cmd = job
            .payload
            .get("command")
            .and_then(|v| v.as_str())
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false);
        if has_cmd {
            return run_shell_local(job).await;
        }
        return run_via_pi(api, cfg, job, stop).await;
    }
    if PI_JOB_TYPES.contains(&job.job_type.as_str()) {
        return run_via_pi(api, cfg, job, stop).await;
    }
    (
        false,
        None,
        Some(format!("unknown job type: {}", job.job_type)),
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cfg = Config::from_env().context("load worker config")?;
    let api = ApiClient::new(cfg.clone())?;
    let stop = Arc::new(AtomicBool::new(false));
    let stop_sig = stop.clone();

    tokio::spawn(async move {
        let mut sigterm =
            signal::unix::signal(signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = signal::ctrl_c() => {},
            _ = sigterm.recv() => {},
        }
        info!("signal received, will exit after current job");
        stop_sig.store(true, Ordering::Relaxed);
    });

    info!(
        url = %cfg.host_ops_url,
        agent_id = %cfg.agent_id,
        session = %cfg.pi_session,
        work_dir = %cfg.work_dir.display(),
        "host_ops_worker starting"
    );

    let mut backoff = BACKOFF_START;
    let mut last_hb = Instant::now() - Duration::from_secs(3600);

    while !stop.load(Ordering::Relaxed) {
        if last_hb.elapsed().as_secs_f64() >= cfg.heartbeat_secs {
            api.heartbeat().await;
            last_hb = Instant::now();
        }

        match api.poll_once().await {
            Ok(None) => {
                backoff = BACKOFF_START;
            }
            Ok(Some(job)) => {
                backoff = BACKOFF_START;
                let job_id = job.id.to_string();
                let (ok, result, error) = execute_job(&api, &cfg, &job, &stop).await;
                if let Err(e) = api.submit_result(&job_id, ok, result, error).await {
                    error!(error = %e, "submit_result failed");
                }
            }
            Err(e) => {
                warn!(error = %e, backoff, "connection/loop error");
                sleep(Duration::from_secs_f64(backoff)).await;
                backoff = (backoff * 2.0).min(BACKOFF_MAX);
            }
        }
    }

    info!("worker stopped");
    Ok(())
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
        let v = json!({"via": "local_shell", "stdout": big, "stderr": "e"});
        let out = truncate_result(v);
        let s = serde_json::to_string(&out).unwrap();
        assert!(s.len() <= MAX_RESULT_BYTES, "len={}", s.len());
        assert_eq!(out.get("truncated"), Some(&Value::Bool(true)));
    }

    #[test]
    fn clip_and_tail() {
        assert_eq!(clip_chars("abcdef", 3), "abc…");
        assert_eq!(tail_chars("abcdef", 3), "def");
        assert_eq!(tail_chars("ab", 5), "ab");
    }
}
