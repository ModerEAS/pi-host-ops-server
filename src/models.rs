use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    TimedOut,
    NoAgent,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::NoAgent => "no_agent",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "queued" => Some(Self::Queued),
            "running" => Some(Self::Running),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "timed_out" => Some(Self::TimedOut),
            "no_agent" => Some(Self::NoAgent),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobType {
    Observe,
    Act,
    Shell,
    Open,
    Clipboard,
    Notify,
}

impl JobType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Observe => "observe",
            Self::Act => "act",
            Self::Shell => "shell",
            Self::Open => "open",
            Self::Clipboard => "clipboard",
            Self::Notify => "notify",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "observe" => Some(Self::Observe),
            "act" => Some(Self::Act),
            "shell" => Some(Self::Shell),
            "open" => Some(Self::Open),
            "clipboard" => Some(Self::Clipboard),
            "notify" => Some(Self::Notify),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMode {
    Queue,
    Steer,
}

impl DeliveryMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queue => "queue",
            Self::Steer => "steer",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "queue" => Some(Self::Queue),
            "steer" => Some(Self::Steer),
            _ => None,
        }
    }
}

impl Default for DeliveryMode {
    fn default() -> Self {
        Self::Steer
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobTarget {
    /// Exact agent id (takes precedence over filter fields when both present).
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub capabilities: Option<Vec<String>>,
    #[serde(default)]
    pub machine: Option<String>,
}

impl JobTarget {
    pub fn is_empty(&self) -> bool {
        self.agent_id.as_ref().map(|s| s.is_empty()).unwrap_or(true)
            && self.project.as_ref().map(|s| s.is_empty()).unwrap_or(true)
            && self.capabilities.as_ref().map(|c| c.is_empty()).unwrap_or(true)
            && self.machine.as_ref().map(|s| s.is_empty()).unwrap_or(true)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: Uuid,
    #[serde(rename = "type")]
    pub job_type: JobType,
    pub status: JobStatus,
    pub payload: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claimed_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_filter: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    pub delivery_mode: DeliveryMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
pub struct CreateJobRequest {
    #[serde(rename = "type")]
    pub job_type: JobType,
    pub payload: Value,
    pub timeout_secs: Option<u64>,
    pub target: Option<JobTarget>,
    pub thread_id: Option<String>,
    /// `steer` (default) or `queue`. Explicitly sendable either way.
    #[serde(default)]
    pub delivery_mode: DeliveryMode,
}

#[derive(Debug, Serialize)]
pub struct CreateJobResponse {
    pub id: Uuid,
    pub status: JobStatus,
    pub created_at: DateTime<Utc>,
    pub delivery_mode: DeliveryMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ListJobsQuery {
    pub status: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct PollRequest {
    /// Long-poll wait in seconds (0–25). Default 0 = return immediately.
    pub wait_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct PollResponse {
    pub job: Job,
}

#[derive(Debug, Deserialize)]
pub struct SubmitResultRequest {
    pub ok: bool,
    pub result: Option<Value>,
    pub error: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct HeartbeatRequest {
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct HeartbeatResponse {
    pub agent_id: String,
    pub status: String,
    pub last_seen_at: DateTime<Utc>,
    pub server_time: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreateAgentRequest {
    pub id: Option<String>,
    pub label: String,
    pub project: Option<String>,
    pub cwd: Option<String>,
    pub machine: Option<String>,
    pub capabilities: Option<Vec<String>>,
    pub max_concurrency: Option<i64>,
    /// Pi provider (e.g. opencode-go). Never Fast.
    pub provider: Option<String>,
    /// Pi model (e.g. muse-spark-1.3-contributor). Never Fast.
    pub model: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CreateAgentResponse {
    pub id: String,
    /// Plaintext token — shown once. Store hash only on server.
    pub token: String,
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub machine: Option<String>,
    pub capabilities: Vec<String>,
    pub max_concurrency: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agent {
    pub id: String,
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub machine: Option<String>,
    pub capabilities: Vec<String>,
    pub max_concurrency: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen_at: Option<DateTime<Utc>>,
    /// Computed: online if last_seen within HEARTBEAT_TTL and not revoked.
    pub status: String,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<DateTime<Utc>>,
    /// Running jobs currently claimed by this agent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub running_jobs: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PatchAgentMeRequest {
    pub cwd: Option<String>,
    pub project: Option<String>,
    pub label: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub ok: bool,
}

#[derive(Debug, Serialize)]
pub struct LeaseResponse {
    pub id: Uuid,
    pub lease_expires_at: DateTime<Utc>,
}


#[derive(Debug, Deserialize)]
pub struct AppendEventRequest {
    #[serde(rename = "type")]
    pub event_type: String,
    pub message: Option<String>,
    pub data: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct ListEventsQuery {
    /// Return events with seq > after (default 0).
    pub after: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct GetJobQuery {
    /// If 1/true, include last N events on the job response.
    pub include_events: Option<String>,
}

