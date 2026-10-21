//! Dispatcher CLI — wraps host-ops HTTP APIs (no raw curl).
mod config;

use std::io::{self, Read as _, Write as _};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde_json::{json, Value};

use config::{mask_token, reject_fast, DispatcherConfig};

#[derive(Debug, Parser)]
#[command(
    name = "host-ops",
    about = "Dispatcher CLI for pi-host-ops-server (jobs + agents)",
    version
)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Read / write ~/.config/pi-host-ops/dispatcher.json
    Config {
        #[command(subcommand)]
        cmd: ConfigCmd,
    },
    /// Manage agents (dispatcher auth)
    Agents {
        #[command(subcommand)]
        cmd: AgentsCmd,
    },
    /// Create / list / watch jobs
    Job {
        #[command(subcommand)]
        cmd: JobCmd,
    },
    /// Steer a correction prompt into a running agent (delivery_mode=steer, payload.prompt)
    Steer(SteerArgs),
}

#[derive(Debug, Subcommand)]
enum ConfigCmd {
    /// Write dispatcher.json (url + token). Env HOST_OPS_URL / DISPATCHER_TOKEN still override at runtime.
    Set {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
    },
    /// Print url and masked token + config path
    Show,
}

#[derive(Debug, Subcommand)]
enum AgentsCmd {
    List,
    Create {
        #[arg(long)]
        id: String,
        #[arg(long)]
        label: String,
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        cwd: Option<String>,
        #[arg(long)]
        machine: Option<String>,
        #[arg(long = "cap", action = clap::ArgAction::Append)]
        caps: Vec<String>,
        #[arg(long)]
        provider: Option<String>,
        #[arg(long)]
        model: Option<String>,
        #[arg(long)]
        max_concurrency: Option<i64>,
    },
    Delete {
        id: String,
    },
}

#[derive(Debug, Clone, ValueEnum)]
enum Delivery {
    Steer,
    Queue,
}

#[derive(Debug, Subcommand)]
enum JobCmd {
    Create {
        #[arg(long = "type")]
        job_type: String,
        /// JSON object string
        #[arg(long, conflicts_with_all = ["payload_file", "prompt", "prompt_file"])]
        payload: Option<String>,
        #[arg(long, conflicts_with_all = ["payload", "prompt", "prompt_file"])]
        payload_file: Option<PathBuf>,
        /// Shorthand: sets payload to {"prompt": "..."} (for Pi steer / act)
        #[arg(long, conflicts_with_all = ["payload", "payload_file", "prompt_file"])]
        prompt: Option<String>,
        #[arg(long, conflicts_with_all = ["payload", "payload_file", "prompt"])]
        prompt_file: Option<PathBuf>,
        #[arg(long, conflicts_with = "project")]
        agent: Option<String>,
        #[arg(long, conflicts_with = "agent")]
        project: Option<String>,
        #[arg(long)]
        thread: Option<String>,
        #[arg(long, value_enum, default_value_t = Delivery::Steer)]
        delivery: Delivery,
        #[arg(long)]
        timeout: Option<u64>,
    },
    Get {
        id: String,
        #[arg(long)]
        events: bool,
    },
    Events {
        id: String,
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long)]
        follow: bool,
        #[arg(long, default_value_t = 500)]
        interval_ms: u64,
    },
    Watch {
        id: String,
        #[arg(long, default_value_t = 500)]
        interval_ms: u64,
    },
    List {
        #[arg(long)]
        status: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: i64,
    },
    /// Alias: steer correction at agent/thread inferred from an existing job, or pass --agent
    Steer(JobSteerArgs),
}

#[derive(Debug, Args)]
struct SteerArgs {
    #[arg(long)]
    agent: String,
    #[arg(long)]
    thread: Option<String>,
    #[arg(long, conflicts_with = "prompt_file")]
    prompt: Option<String>,
    #[arg(long, conflicts_with = "prompt")]
    prompt_file: Option<PathBuf>,
    #[arg(long)]
    timeout: Option<u64>,
    #[arg(long = "type", default_value = "act")]
    job_type: String,
}

#[derive(Debug, Args)]
struct JobSteerArgs {
    /// Existing job UUID (preferred) or thread_id (then --agent required)
    target: String,
    #[arg(long)]
    agent: Option<String>,
    #[arg(long)]
    thread: Option<String>,
    #[arg(long, conflicts_with = "prompt_file")]
    prompt: Option<String>,
    #[arg(long, conflicts_with = "prompt")]
    prompt_file: Option<PathBuf>,
    #[arg(long)]
    timeout: Option<u64>,
    #[arg(long = "type", default_value = "act")]
    job_type: String,
}

struct Client {
    http: reqwest::Client,
    base: String,
    token: String,
}

impl Client {
    fn from_cfg(cfg: &DispatcherConfig) -> Result<Self> {
        if cfg.token.trim().is_empty() {
            bail!(
                "dispatcher token missing — run `host-ops config set --token …` or set DISPATCHER_TOKEN"
            );
        }
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .context("build http client")?;
        Ok(Self {
            http,
            base: cfg.url.trim_end_matches('/').to_string(),
            token: cfg.token.clone(),
        })
    }

    async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<Value>,
    ) -> Result<(u16, Value)> {
        let url = format!("{}{}", self.base, path);
        let mut req = self
            .http
            .request(method, &url)
            .header(AUTHORIZATION, format!("Bearer {}", self.token));
        for (k, v) in query {
            req = req.query(&[(k, v)]);
        }
        if let Some(b) = body {
            req = req.header(CONTENT_TYPE, "application/json").json(&b);
        }
        let resp = req.send().await.with_context(|| format!("HTTP {url}"))?;
        let status = resp.status().as_u16();
        let bytes = resp.bytes().await.context("read body")?;
        let val = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or_else(|_| {
                Value::String(String::from_utf8_lossy(&bytes).into_owned())
            })
        };
        if !(200..300).contains(&status) {
            bail!("HTTP {status}: {}", pretty(&val));
        }
        Ok((status, val))
    }

    async fn get(&self, path: &str, query: &[(&str, String)]) -> Result<Value> {
        Ok(self.request(reqwest::Method::GET, path, query, None).await?.1)
    }

    async fn post(&self, path: &str, body: Value) -> Result<Value> {
        Ok(self
            .request(reqwest::Method::POST, path, &[], Some(body))
            .await?
            .1)
    }

    async fn delete(&self, path: &str) -> Result<Value> {
        Ok(self
            .request(reqwest::Method::DELETE, path, &[], None)
            .await?
            .1)
    }
}

fn pretty(v: &Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
}

fn print_json(v: &Value) {
    println!("{}", pretty(v));
}

fn read_prompt(prompt: Option<String>, prompt_file: Option<PathBuf>) -> Result<String> {
    if let Some(p) = prompt {
        if p.trim().is_empty() {
            bail!("prompt is empty");
        }
        return Ok(p);
    }
    if let Some(path) = prompt_file {
        let s = std::fs::read_to_string(&path)
            .with_context(|| format!("read {}", path.display()))?;
        if s.trim().is_empty() {
            bail!("prompt file is empty");
        }
        return Ok(s);
    }
    // stdin if not a tty
    if !atty_stdin() {
        let mut buf = String::new();
        io::stdin().read_to_string(&mut buf)?;
        if buf.trim().is_empty() {
            bail!("provide --prompt, --prompt-file, or pipe prompt on stdin");
        }
        return Ok(buf);
    }
    bail!("provide --prompt or --prompt-file");
}

fn atty_stdin() -> bool {
    use std::io::IsTerminal;
    io::stdin().is_terminal()
}

fn looks_like_uuid(s: &str) -> bool {
    uuid::Uuid::parse_str(s).is_ok()
}

const TERMINAL: &[&str] = &["succeeded", "failed", "timed_out", "no_agent"];

fn is_terminal_status(job: &Value) -> bool {
    job.get("status")
        .and_then(|v| v.as_str())
        .map(|s| TERMINAL.contains(&s))
        .unwrap_or(false)
}

fn event_seq(ev: &Value) -> i64 {
    ev.get("seq").and_then(|v| v.as_i64()).unwrap_or(0)
}

async fn create_steer_job(
    client: &Client,
    job_type: &str,
    agent_id: &str,
    thread_id: Option<&str>,
    prompt: &str,
    timeout: Option<u64>,
) -> Result<Value> {
    let mut body = json!({
        "type": job_type,
        "payload": { "prompt": prompt },
        "delivery_mode": "steer",
        "target": { "agent_id": agent_id },
    });
    if let Some(t) = thread_id.filter(|s| !s.is_empty()) {
        body["thread_id"] = json!(t);
    }
    if let Some(secs) = timeout {
        body["timeout_secs"] = json!(secs);
    }
    client.post("/v1/jobs", body).await
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Commands::Config { cmd } => run_config(cmd).await?,
        Commands::Agents { cmd } => {
            let (cfg, _) = config::load()?;
            let client = Client::from_cfg(&cfg)?;
            run_agents(&client, cmd).await?;
        }
        Commands::Job { cmd } => {
            let (cfg, _) = config::load()?;
            let client = Client::from_cfg(&cfg)?;
            run_job(&client, cmd).await?;
        }
        Commands::Steer(args) => {
            let (cfg, _) = config::load()?;
            let client = Client::from_cfg(&cfg)?;
            let prompt = read_prompt(args.prompt, args.prompt_file)?;
            let resp = create_steer_job(
                &client,
                &args.job_type,
                &args.agent,
                args.thread.as_deref(),
                &prompt,
                args.timeout,
            )
            .await?;
            print_json(&resp);
        }
    }
    Ok(())
}

async fn run_config(cmd: ConfigCmd) -> Result<()> {
    match cmd {
        ConfigCmd::Set { url, token } => {
            let path = config::config_path()?;
            let mut cfg = if path.is_file() {
                let raw = std::fs::read_to_string(&path)?;
                serde_json::from_str::<DispatcherConfig>(&raw).unwrap_or_default()
            } else {
                DispatcherConfig::default()
            };
            if let Some(u) = url {
                cfg.url = u.trim_end_matches('/').to_string();
            }
            if let Some(t) = token {
                cfg.token = t;
            }
            if cfg.url.is_empty() {
                cfg.url = config::DEFAULT_URL.into();
            }
            let saved = config::save(&cfg)?;
            println!("wrote {}", saved.display());
            println!("url={}", cfg.url);
            println!("token={}", mask_token(&cfg.token));
        }
        ConfigCmd::Show => {
            let (cfg, path) = config::load()?;
            println!("config_path={}", path.display());
            println!("url={}", cfg.url);
            println!("token={}", mask_token(&cfg.token));
            if std::env::var_os("HOST_OPS_URL").is_some() {
                println!("note=HOST_OPS_URL env overrides url");
            }
            if std::env::var_os("DISPATCHER_TOKEN").is_some() {
                println!("note=DISPATCHER_TOKEN env overrides token");
            }
        }
    }
    Ok(())
}

async fn run_agents(client: &Client, cmd: AgentsCmd) -> Result<()> {
    match cmd {
        AgentsCmd::List => {
            let v = client.get("/v1/agents", &[]).await?;
            print_json(&v);
        }
        AgentsCmd::Create {
            id,
            label,
            project,
            cwd,
            machine,
            caps,
            provider,
            model,
            max_concurrency,
        } => {
            reject_fast(provider.as_deref(), model.as_deref())?;
            let mut body = json!({
                "id": id,
                "label": label,
                "capabilities": caps,
            });
            if let Some(p) = project {
                body["project"] = json!(p);
            }
            if let Some(c) = cwd {
                body["cwd"] = json!(c);
            }
            if let Some(m) = machine {
                body["machine"] = json!(m);
            }
            if let Some(p) = provider {
                body["provider"] = json!(p);
            }
            if let Some(m) = model {
                body["model"] = json!(m);
            }
            if let Some(n) = max_concurrency {
                body["max_concurrency"] = json!(n);
            }
            let v = client.post("/v1/agents", body).await?;
            print_json(&v);
            eprintln!("note: agent token is shown once — save it now");
        }
        AgentsCmd::Delete { id } => {
            let v = client.delete(&format!("/v1/agents/{id}")).await?;
            print_json(&v);
        }
    }
    Ok(())
}

async fn run_job(client: &Client, cmd: JobCmd) -> Result<()> {
    match cmd {
        JobCmd::Create {
            job_type,
            payload,
            payload_file,
            prompt,
            prompt_file,
            agent,
            project,
            thread,
            delivery,
            timeout,
        } => {
            let payload_val = resolve_payload(payload, payload_file, prompt, prompt_file)?;
            let mut body = json!({
                "type": job_type,
                "payload": payload_val,
                "delivery_mode": match delivery {
                    Delivery::Steer => "steer",
                    Delivery::Queue => "queue",
                },
            });
            if let Some(a) = agent {
                body["target"] = json!({ "agent_id": a });
            } else if let Some(p) = project {
                body["target"] = json!({ "project": p });
            }
            if let Some(t) = thread {
                body["thread_id"] = json!(t);
            }
            if let Some(secs) = timeout {
                body["timeout_secs"] = json!(secs);
            }
            let v = client.post("/v1/jobs", body).await?;
            print_json(&v);
        }
        JobCmd::Get { id, events } => {
            let owned: Vec<(String, String)> = if events {
                vec![("include_events".into(), "1".into())]
            } else {
                vec![]
            };
            let q: Vec<(&str, String)> = owned
                .iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect();
            let v = client.get(&format!("/v1/jobs/{id}"), &q).await?;
            print_json(&v);
        }
        JobCmd::Events {
            id,
            after,
            follow,
            interval_ms,
        } => {
            let mut after = after;
            loop {
                let q = vec![
                    ("after", after.to_string()),
                    ("limit", "50".into()),
                ];
                let v = client
                    .get(&format!("/v1/jobs/{id}/events"), &q)
                    .await?;
                if let Some(arr) = v.get("events").and_then(|e| e.as_array()) {
                    for ev in arr {
                        print_json(ev);
                        let _ = io::stdout().flush();
                        after = after.max(event_seq(ev));
                    }
                }
                if !follow {
                    break;
                }
                let job = client.get(&format!("/v1/jobs/{id}"), &[]).await?;
                if is_terminal_status(&job) {
                    eprintln!("status={}", job.get("status").and_then(|s| s.as_str()).unwrap_or("?"));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(interval_ms.max(100))).await;
            }
        }
        JobCmd::Watch { id, interval_ms } => {
            let mut after: i64 = 0;
            loop {
                let q = vec![
                    ("after", after.to_string()),
                    ("limit", "50".into()),
                ];
                if let Ok(v) = client.get(&format!("/v1/jobs/{id}/events"), &q).await {
                    if let Some(arr) = v.get("events").and_then(|e| e.as_array()) {
                        for ev in arr {
                            print_json(ev);
                            let _ = io::stdout().flush();
                            after = after.max(event_seq(ev));
                        }
                    }
                }
                let job = client.get(&format!("/v1/jobs/{id}"), &[]).await?;
                if is_terminal_status(&job) {
                    print_json(&job);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(interval_ms.max(100))).await;
            }
        }
        JobCmd::List { status, limit } => {
            list_jobs(client, status, limit).await?;
        }
        JobCmd::Steer(args) => {
            let (agent_id, thread_id) = resolve_steer_target(client, &args).await?;
            let prompt = read_prompt(args.prompt, args.prompt_file)?;
            let resp = create_steer_job(
                client,
                &args.job_type,
                &agent_id,
                thread_id.as_deref(),
                &prompt,
                args.timeout,
            )
            .await?;
            print_json(&resp);
        }
    }
    Ok(())
}

async fn list_jobs(client: &Client, status: Option<String>, limit: i64) -> Result<()> {
    let mut owned: Vec<(String, String)> = vec![("limit".into(), limit.to_string())];
    if let Some(s) = status {
        owned.push(("status".into(), s));
    }
    let q: Vec<(&str, String)> = owned
        .iter()
        .map(|(k, v)| (k.as_str(), v.clone()))
        .collect();
    let v = client.get("/v1/jobs", &q).await?;
    print_json(&v);
    Ok(())
}

fn resolve_payload(
    payload: Option<String>,
    payload_file: Option<PathBuf>,
    prompt: Option<String>,
    prompt_file: Option<PathBuf>,
) -> Result<Value> {
    if let Some(p) = prompt {
        return Ok(json!({ "prompt": p }));
    }
    if let Some(path) = prompt_file {
        let s = std::fs::read_to_string(&path)
            .with_context(|| format!("read {}", path.display()))?;
        return Ok(json!({ "prompt": s }));
    }
    if let Some(path) = payload_file {
        let s = std::fs::read_to_string(&path)
            .with_context(|| format!("read {}", path.display()))?;
        let v: Value = serde_json::from_str(&s).context("payload-file JSON")?;
        if !v.is_object() {
            bail!("payload must be a JSON object");
        }
        return Ok(v);
    }
    if let Some(s) = payload {
        let v: Value = serde_json::from_str(&s).context("payload JSON")?;
        if !v.is_object() {
            bail!("payload must be a JSON object");
        }
        return Ok(v);
    }
    Ok(json!({}))
}

async fn resolve_steer_target(
    client: &Client,
    args: &JobSteerArgs,
) -> Result<(String, Option<String>)> {
    if looks_like_uuid(&args.target) {
        let job = client
            .get(&format!("/v1/jobs/{}", args.target), &[])
            .await?;
        let agent = args
            .agent
            .clone()
            .or_else(|| {
                job.get("target_agent_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .or_else(|| {
                job.get("agent_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .context("job has no target_agent_id/agent_id — pass --agent")?;
        let thread = args.thread.clone().or_else(|| {
            job.get("thread_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        });
        return Ok((agent, thread));
    }
    // treat target as thread_id
    let agent = args
        .agent
        .clone()
        .context("when target is a thread_id, --agent is required")?;
    let thread = Some(
        args.thread
            .clone()
            .unwrap_or_else(|| args.target.clone()),
    );
    Ok((agent, thread))
}
