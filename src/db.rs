use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow};
use sqlx::{Row, SqlitePool};
use std::str::FromStr;
use std::time::Duration;
use uuid::Uuid;

use crate::config::hash_token;
use crate::models::{
    Agent, DeliveryMode, Job, JobStatus, JobTarget, JobType,
};

const MAX_EVENT_MESSAGE_BYTES: usize = 16 * 1024;
const MAX_EVENT_DATA_BYTES: usize = 64 * 1024;

#[derive(Clone)]
pub struct Db {
    pool: SqlitePool,
}

impl Db {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let options = SqliteConnectOptions::from_str(database_url)
            .context("parse DATABASE_URL")?
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .context("connect sqlite")?;

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .context("run migrations")?;

        Ok(Self { pool })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    // ── Agents ──────────────────────────────────────────────────────────

    pub async fn upsert_seed_agent(
        &self,
        id: &str,
        token: &str,
        label: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let token_hash = hash_token(token);
        // Only insert if missing; do not overwrite an existing agent's token.
        let exists: Option<String> =
            sqlx::query_scalar("SELECT id FROM agents WHERE id = ?")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
        if exists.is_some() {
            return Ok(());
        }
        sqlx::query(
            r#"
            INSERT INTO agents (id, token_hash, label, project, cwd, machine, capabilities,
                                max_concurrency, last_seen_at, status, created_at, revoked_at)
            VALUES (?, ?, ?, NULL, NULL, NULL, '[]', 1, NULL, 'offline', ?, NULL)
            "#,
        )
        .bind(id)
        .bind(&token_hash)
        .bind(label)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn create_agent(
        &self,
        id: &str,
        token: &str,
        label: &str,
        project: Option<&str>,
        cwd: Option<&str>,
        machine: Option<&str>,
        capabilities: &[String],
        max_concurrency: i64,
        provider: Option<&str>,
        model: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let token_hash = hash_token(token);
        let caps = serde_json::to_string(capabilities)?;
        sqlx::query(
            r#"
            INSERT INTO agents (id, token_hash, label, project, cwd, machine, capabilities,
                                max_concurrency, last_seen_at, status, created_at, revoked_at,
                                pi_provider, pi_model)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, 'offline', ?, NULL, ?, ?)
            "#,
        )
        .bind(id)
        .bind(&token_hash)
        .bind(label)
        .bind(project)
        .bind(cwd)
        .bind(machine)
        .bind(&caps)
        .bind(max_concurrency.max(1))
        .bind(&now)
        .bind(provider)
        .bind(model)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Agent self-update: cwd / project / label / provider / model.
    pub async fn update_agent_profile(
        &self,
        id: &str,
        cwd: Option<&str>,
        project: Option<&str>,
        label: Option<&str>,
        provider: Option<&str>,
        model: Option<&str>,
    ) -> Result<Option<AgentRow>> {
        let existing = self.get_agent(id).await?;
        let Some(row) = existing else {
            return Ok(None);
        };
        if row.revoked_at.is_some() {
            return Ok(None);
        }
        let cwd = cwd.filter(|s| !s.is_empty()).or(row.cwd.as_deref());
        let project = project.filter(|s| !s.is_empty()).or(row.project.as_deref());
        let label = label.filter(|s| !s.is_empty()).unwrap_or(row.label.as_str());
        let provider = provider.filter(|s| !s.is_empty()).or(row.provider.as_deref());
        let model = model.filter(|s| !s.is_empty()).or(row.model.as_deref());
        sqlx::query(
            r#"
            UPDATE agents
            SET cwd = ?, project = ?, label = ?, pi_provider = ?, pi_model = ?
            WHERE id = ? AND revoked_at IS NULL
            "#,
        )
        .bind(cwd)
        .bind(project)
        .bind(label)
        .bind(provider)
        .bind(model)
        .bind(id)
        .execute(&self.pool)
        .await?;
        self.get_agent(id).await
    }

    pub async fn find_agent_by_token_hash(&self, token_hash: &str) -> Result<Option<AgentRow>> {
        let row = sqlx::query(
            r#"
            SELECT id, token_hash, label, project, cwd, machine, capabilities,
                   max_concurrency, last_seen_at, status, created_at, revoked_at,
                   pi_provider, pi_model
            FROM agents WHERE token_hash = ?
            "#,
        )
        .bind(token_hash)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(row_to_agent_row).transpose()?)
    }

    pub async fn get_agent(&self, id: &str) -> Result<Option<AgentRow>> {
        let row = sqlx::query(
            r#"
            SELECT id, token_hash, label, project, cwd, machine, capabilities,
                   max_concurrency, last_seen_at, status, created_at, revoked_at,
                   pi_provider, pi_model
            FROM agents WHERE id = ?
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(row_to_agent_row).transpose()?)
    }

    pub async fn list_agents(&self) -> Result<Vec<AgentRow>> {
        let rows = sqlx::query(
            r#"
            SELECT id, token_hash, label, project, cwd, machine, capabilities,
                   max_concurrency, last_seen_at, status, created_at, revoked_at,
                   pi_provider, pi_model
            FROM agents
            ORDER BY created_at ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(row_to_agent_row).collect()
    }

    pub async fn revoke_agent(&self, id: &str) -> Result<bool> {
        let now = Utc::now().to_rfc3339();
        let res = sqlx::query(
            "UPDATE agents SET revoked_at = ?, status = 'offline', token_hash = ? WHERE id = ? AND revoked_at IS NULL",
        )
        .bind(&now)
        // Invalidate token by replacing hash with an unusable value
        .bind(format!("revoked:{}", Uuid::new_v4()))
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    pub async fn heartbeat(&self, agent_id: &str, status: Option<&str>) -> Result<DateTime<Utc>> {
        let now = Utc::now();
        let status = status.unwrap_or("online");
        let res = sqlx::query(
            "UPDATE agents SET last_seen_at = ?, status = ? WHERE id = ? AND revoked_at IS NULL",
        )
        .bind(now.to_rfc3339())
        .bind(status)
        .bind(agent_id)
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 0 {
            anyhow::bail!("agent not found or revoked");
        }
        Ok(now)
    }

    pub async fn count_running_for_agent(&self, agent_id: &str) -> Result<i64> {
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM jobs WHERE agent_id = ? AND status = 'running'",
        )
        .bind(agent_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(n)
    }

    // ── Workers ─────────────────────────────────────────────────────────
    // One worker process multiplexes many agents: one batch heartbeat +
    // one poll covering all bound agents (serial execution by default).

    pub async fn create_worker(&self, id: &str, token: &str, label: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let token_hash = hash_token(token);
        sqlx::query(
            r#"
            INSERT INTO workers (id, token_hash, label, created_at, revoked_at, last_seen_at)
            VALUES (?, ?, ?, ?, NULL, NULL)
            "#,
        )
        .bind(id)
        .bind(&token_hash)
        .bind(label)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn find_worker_by_token_hash(
        &self,
        token_hash: &str,
    ) -> Result<Option<WorkerRow>> {
        let row = sqlx::query(
            r#"
            SELECT id, label, created_at, revoked_at, last_seen_at
            FROM workers WHERE token_hash = ?
            "#,
        )
        .bind(token_hash)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(row_to_worker_row).transpose()?)
    }

    pub async fn get_worker(&self, id: &str) -> Result<Option<WorkerRow>> {
        let row = sqlx::query(
            r#"
            SELECT id, label, created_at, revoked_at, last_seen_at
            FROM workers WHERE id = ?
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(row_to_worker_row).transpose()?)
    }

    pub async fn list_workers(&self) -> Result<Vec<WorkerRow>> {
        let rows = sqlx::query(
            r#"
            SELECT id, label, created_at, revoked_at, last_seen_at
            FROM workers
            ORDER BY created_at ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(row_to_worker_row).collect()
    }

    pub async fn revoke_worker(&self, id: &str) -> Result<bool> {
        let now = Utc::now().to_rfc3339();
        let res = sqlx::query(
            "UPDATE workers SET revoked_at = ?, token_hash = ? WHERE id = ? AND revoked_at IS NULL",
        )
        .bind(&now)
        // Invalidate token by replacing hash with an unusable value
        .bind(format!("revoked:{}", Uuid::new_v4()))
        .bind(id)
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 0 {
            return Ok(false);
        }
        // Release bindings so agents can be re-bound to another worker.
        sqlx::query("DELETE FROM worker_agents WHERE worker_id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(true)
    }

    /// Agent ids bound to this worker, sorted for deterministic claim order.
    pub async fn worker_agents(&self, worker_id: &str) -> Result<Vec<String>> {
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT agent_id FROM worker_agents WHERE worker_id = ? ORDER BY agent_id ASC",
        )
        .bind(worker_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn worker_id_for_agent(&self, agent_id: &str) -> Result<Option<String>> {
        let row: Option<String> = sqlx::query_scalar(
            "SELECT worker_id FROM worker_agents WHERE agent_id = ?",
        )
        .bind(agent_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn bind_agent_to_worker(
        &self,
        worker_id: &str,
        agent_id: &str,
    ) -> Result<BindOutcome> {
        match self.get_worker(worker_id).await? {
            Some(w) if w.revoked_at.is_none() => {}
            _ => return Ok(BindOutcome::WorkerNotFound),
        }
        match self.get_agent(agent_id).await? {
            Some(a) if a.revoked_at.is_none() => {}
            _ => return Ok(BindOutcome::AgentNotFound),
        }
        if let Some(other) = self.worker_id_for_agent(agent_id).await? {
            if other == worker_id {
                return Ok(BindOutcome::Ok);
            }
            return Ok(BindOutcome::AlreadyBound(other));
        }
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO worker_agents (worker_id, agent_id, created_at) VALUES (?, ?, ?)",
        )
        .bind(worker_id)
        .bind(agent_id)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(BindOutcome::Ok)
    }

    pub async fn unbind_agent_from_worker(
        &self,
        worker_id: &str,
        agent_id: &str,
    ) -> Result<bool> {
        let res = sqlx::query(
            "DELETE FROM worker_agents WHERE worker_id = ? AND agent_id = ?",
        )
        .bind(worker_id)
        .bind(agent_id)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    /// Batch-refresh `last_seen_at` / `status` for this worker's bound agents
    /// in one shot. Callers must ensure every id is bound to `worker_id`.
    pub async fn heartbeat_many(
        &self,
        worker_id: &str,
        entries: &[(String, String)],
    ) -> Result<DateTime<Utc>> {
        let now = Utc::now();
        let now_s = now.to_rfc3339();
        // Group by status so mixed online/busy batches need at most a few queries.
        let mut by_status: std::collections::HashMap<&str, Vec<&str>> =
            std::collections::HashMap::new();
        for (id, status) in entries {
            by_status.entry(status.as_str()).or_default().push(id.as_str());
        }
        for (status, ids) in by_status {
            let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let sql = format!(
                "UPDATE agents SET last_seen_at = ?, status = ? \
                 WHERE id IN ({placeholders}) AND revoked_at IS NULL"
            );
            let mut q = sqlx::query(&sql).bind(&now_s).bind(status);
            for id in ids {
                q = q.bind(id);
            }
            q.execute(&self.pool).await?;
        }
        sqlx::query("UPDATE workers SET last_seen_at = ? WHERE id = ? AND revoked_at IS NULL")
            .bind(&now_s)
            .bind(worker_id)
            .execute(&self.pool)
            .await?;
        Ok(now)
    }

    /// Claim the next matching job for any of these agents (in order),
    /// reusing the single-agent claim/lease/concurrency rules.
    pub async fn claim_next_job_for_agents(
        &self,
        agent_ids: &[String],
        lease_secs: i64,
    ) -> Result<Option<(Job, String)>> {
        for agent_id in agent_ids {
            if let Some(job) = self.claim_next_job_for_agent(agent_id, lease_secs).await? {
                return Ok(Some((job, agent_id.clone())));
            }
        }
        Ok(None)
    }

    // ── Jobs ────────────────────────────────────────────────────────────

    pub async fn insert_job(
        &self,
        id: Uuid,
        job_type: JobType,
        payload: &Value,
        timeout_secs: Option<u64>,
        target_agent_id: Option<&str>,
        target_filter: Option<&Value>,
        thread_id: Option<&str>,
        delivery_mode: DeliveryMode,
    ) -> Result<Job> {
        let now = Utc::now();
        let payload_str = serde_json::to_string(payload)?;
        let filter_str = match target_filter {
            Some(v) => Some(serde_json::to_string(v)?),
            None => None,
        };
        let timeout_i = timeout_secs.map(|t| t as i64);

        sqlx::query(
            r#"
            INSERT INTO jobs (
                id, job_type, status, payload, created_at, updated_at,
                target_agent_id, target_filter, thread_id, delivery_mode, timeout_secs
            ) VALUES (?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(id.to_string())
        .bind(job_type.as_str())
        .bind(&payload_str)
        .bind(now.to_rfc3339())
        .bind(now.to_rfc3339())
        .bind(target_agent_id)
        .bind(filter_str)
        .bind(thread_id)
        .bind(delivery_mode.as_str())
        .bind(timeout_i)
        .execute(&self.pool)
        .await?;

        self.get_job(id)
            .await?
            .context("job missing after insert")
    }

    pub async fn get_job(&self, id: Uuid) -> Result<Option<Job>> {
        let row = sqlx::query(
            r#"
            SELECT id, job_type, status, payload, result, error,
                   created_at, updated_at, claimed_at, finished_at, agent_id,
                   target_agent_id, target_filter, thread_id, delivery_mode,
                   lease_expires_at, timeout_secs
            FROM jobs WHERE id = ?
            "#,
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(row_to_job).transpose()?)
    }

    pub async fn list_jobs(&self, status: Option<&str>, limit: i64) -> Result<Vec<Job>> {
        let limit = limit.clamp(1, 100);
        let rows = if let Some(status) = status {
            sqlx::query(
                r#"
                SELECT id, job_type, status, payload, result, error,
                       created_at, updated_at, claimed_at, finished_at, agent_id,
                       target_agent_id, target_filter, thread_id, delivery_mode,
                       lease_expires_at, timeout_secs
                FROM jobs WHERE status = ?
                ORDER BY created_at DESC
                LIMIT ?
                "#,
            )
            .bind(status)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                r#"
                SELECT id, job_type, status, payload, result, error,
                       created_at, updated_at, claimed_at, finished_at, agent_id,
                       target_agent_id, target_filter, thread_id, delivery_mode,
                       lease_expires_at, timeout_secs
                FROM jobs
                ORDER BY created_at DESC
                LIMIT ?
                "#,
            )
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };

        rows.into_iter().map(row_to_job).collect()
    }

    /// Sticky agent for a thread: last agent that claimed a running/succeeded job on this thread.
    async fn sticky_agent_for_thread(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        thread_id: &str,
    ) -> Result<Option<String>> {
        let row = sqlx::query(
            r#"
            SELECT agent_id FROM jobs
            WHERE thread_id = ? AND agent_id IS NOT NULL
              AND status IN ('running', 'succeeded')
            ORDER BY COALESCE(finished_at, claimed_at, created_at) DESC
            LIMIT 1
            "#,
        )
        .bind(thread_id)
        .fetch_optional(&mut **tx)
        .await?;
        Ok(row
            .map(|r| r.try_get::<String, _>("agent_id"))
            .transpose()?)
    }

    fn filter_matches(agent: &AgentRow, filter: &Value) -> bool {
        let Some(obj) = filter.as_object() else {
            return true;
        };
        if let Some(p) = obj.get("project").and_then(|v| v.as_str()) {
            if agent.project.as_deref() != Some(p) {
                return false;
            }
        }
        if let Some(m) = obj.get("machine").and_then(|v| v.as_str()) {
            if agent.machine.as_deref() != Some(m) {
                return false;
            }
        }
        if let Some(caps) = obj.get("capabilities").and_then(|v| v.as_array()) {
            for c in caps {
                let Some(need) = c.as_str() else { continue };
                if !agent.capabilities.iter().any(|h| h == need) {
                    return false;
                }
            }
        }
        true
    }

    /// Atomically claim the oldest queued job matching this agent.
    pub async fn claim_next_job_for_agent(
        &self,
        agent_id: &str,
        lease_secs: i64,
    ) -> Result<Option<Job>> {
        let agent = match self.get_agent(agent_id).await? {
            Some(a) if a.revoked_at.is_none() => a,
            _ => return Ok(None),
        };

        let running = self.count_running_for_agent(agent_id).await?;
        if running >= agent.max_concurrency {
            return Ok(None);
        }

        // IMMEDIATE: take the write lock up front so concurrent claims serialize
        // instead of deadlocking on deferred SHARED→RESERVED upgrades (SQLITE_BUSY).
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        let candidates = sqlx::query(
            r#"
            SELECT id, target_agent_id, target_filter, thread_id
            FROM jobs
            WHERE status = 'queued'
            ORDER BY created_at ASC
            LIMIT 50
            "#,
        )
        .fetch_all(&mut *tx)
        .await?;

        let mut chosen: Option<String> = None;
        for row in candidates {
            let id: String = row.try_get("id")?;
            let target_agent_id: Option<String> = row.try_get("target_agent_id")?;
            let target_filter: Option<String> = row.try_get("target_filter")?;
            let thread_id: Option<String> = row.try_get("thread_id")?;

            // Sticky thread overrides filter
            if let Some(ref tid) = thread_id {
                if let Some(sticky) = self.sticky_agent_for_thread(&mut tx, tid).await? {
                    if sticky != agent_id {
                        continue;
                    }
                    // sticky == this agent → claim
                    chosen = Some(id);
                    break;
                }
            }

            if let Some(ref tid) = target_agent_id {
                if tid != agent_id {
                    continue;
                }
                chosen = Some(id);
                break;
            }

            if let Some(ref filter_str) = target_filter {
                let filter: Value = serde_json::from_str(filter_str).unwrap_or(Value::Null);
                if !Self::filter_matches(&agent, &filter) {
                    continue;
                }
                chosen = Some(id);
                break;
            }

            // Untargeted: any agent may claim
            chosen = Some(id);
            break;
        }

        let Some(id) = chosen else {
            tx.commit().await?;
            return Ok(None);
        };

        let now = Utc::now();
        let lease_exp = now + chrono::Duration::seconds(lease_secs.max(5));

        let updated = sqlx::query(
            r#"
            UPDATE jobs
            SET status = 'running', claimed_at = ?, updated_at = ?, agent_id = ?,
                lease_expires_at = ?
            WHERE id = ? AND status = 'queued'
            "#,
        )
        .bind(now.to_rfc3339())
        .bind(now.to_rfc3339())
        .bind(agent_id)
        .bind(lease_exp.to_rfc3339())
        .bind(&id)
        .execute(&mut *tx)
        .await?;

        if updated.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(None);
        }

        let job_row = sqlx::query(
            r#"
            SELECT id, job_type, status, payload, result, error,
                   created_at, updated_at, claimed_at, finished_at, agent_id,
                   target_agent_id, target_filter, thread_id, delivery_mode,
                   lease_expires_at, timeout_secs
            FROM jobs WHERE id = ?
            "#,
        )
        .bind(&id)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(Some(row_to_job(job_row)?))
    }

    pub async fn renew_lease(
        &self,
        id: Uuid,
        agent_id: &str,
        lease_secs: i64,
    ) -> Result<LeaseOutcome> {
        let now = Utc::now();
        let lease_exp = now + chrono::Duration::seconds(lease_secs.max(5));
        let updated = sqlx::query(
            r#"
            UPDATE jobs
            SET lease_expires_at = ?, updated_at = ?
            WHERE id = ? AND status = 'running' AND agent_id = ?
            "#,
        )
        .bind(lease_exp.to_rfc3339())
        .bind(now.to_rfc3339())
        .bind(id.to_string())
        .bind(agent_id)
        .execute(&self.pool)
        .await?;

        if updated.rows_affected() == 0 {
            return match self.get_job(id).await? {
                None => Ok(LeaseOutcome::NotFound),
                Some(j) if j.agent_id.as_deref() != Some(agent_id) => {
                    Ok(LeaseOutcome::Forbidden)
                }
                Some(j) => Ok(LeaseOutcome::WrongStatus(j.status)),
            };
        }
        Ok(LeaseOutcome::Ok(lease_exp))
    }

    pub async fn submit_result(
        &self,
        id: Uuid,
        agent_id: &str,
        ok: bool,
        result: Option<&Value>,
        error: Option<&str>,
    ) -> Result<SubmitOutcome> {
        // Ownership check
        match self.get_job(id).await? {
            None => return Ok(SubmitOutcome::NotFound),
            Some(j) if j.agent_id.as_deref() != Some(agent_id) => {
                return Ok(SubmitOutcome::Forbidden);
            }
            Some(j) if j.status != JobStatus::Running => {
                return Ok(SubmitOutcome::WrongStatus(j.status));
            }
            Some(_) => {}
        }

        let now = Utc::now().to_rfc3339();
        let status = if ok {
            JobStatus::Succeeded.as_str()
        } else {
            JobStatus::Failed.as_str()
        };
        let result_str = match result {
            Some(v) => Some(serde_json::to_string(v)?),
            None => None,
        };

        let updated = sqlx::query(
            r#"
            UPDATE jobs
            SET status = ?, result = ?, error = ?, finished_at = ?, updated_at = ?,
                lease_expires_at = NULL
            WHERE id = ? AND status = 'running' AND agent_id = ?
            "#,
        )
        .bind(status)
        .bind(result_str)
        .bind(error)
        .bind(&now)
        .bind(&now)
        .bind(id.to_string())
        .bind(agent_id)
        .execute(&self.pool)
        .await?;

        if updated.rows_affected() == 0 {
            return match self.get_job(id).await? {
                None => Ok(SubmitOutcome::NotFound),
                Some(j) => Ok(SubmitOutcome::WrongStatus(j.status)),
            };
        }

        Ok(SubmitOutcome::Ok(
            self.get_job(id).await?.context("missing after result")?,
        ))
    }

    // ── Events ──────────────────────────────────────────────────────────

    pub async fn append_event(
        &self,
        job_id: Uuid,
        agent_id: &str,
        event_type: &str,
        message: Option<&str>,
        data: Option<&Value>,
    ) -> Result<AppendEventOutcome> {
        match self.get_job(job_id).await? {
            None => return Ok(AppendEventOutcome::NotFound),
            Some(j) if j.agent_id.as_deref() != Some(agent_id) => {
                return Ok(AppendEventOutcome::Forbidden);
            }
            Some(j) if j.status != JobStatus::Running => {
                return Ok(AppendEventOutcome::WrongStatus(j.status));
            }
            Some(_) => {}
        }

        let message = message.map(|m| truncate_str(m, MAX_EVENT_MESSAGE_BYTES));
        let data_str = match data {
            Some(v) => {
                let s = serde_json::to_string(v)?;
                Some(truncate_str(&s, MAX_EVENT_DATA_BYTES))
            }
            None => None,
        };

        let mut tx = self.pool.begin().await?;
        let next_seq: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM job_events WHERE job_id = ?",
        )
        .bind(job_id.to_string())
        .fetch_one(&mut *tx)
        .await?;

        let eid = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO job_events (id, job_id, seq, event_type, message, data, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&eid)
        .bind(job_id.to_string())
        .bind(next_seq)
        .bind(event_type)
        .bind(&message)
        .bind(&data_str)
        .bind(&now)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(AppendEventOutcome::Ok(JobEvent {
            id: eid,
            job_id: job_id.to_string(),
            seq: next_seq,
            event_type: event_type.to_string(),
            message,
            data: data.cloned(),
            created_at: parse_dt(&now)?,
        }))
    }

    pub async fn list_events(
        &self,
        job_id: Uuid,
        after_seq: i64,
        limit: i64,
    ) -> Result<Vec<JobEvent>> {
        let limit = limit.clamp(1, 200);
        let rows = sqlx::query(
            r#"
            SELECT id, job_id, seq, event_type, message, data, created_at
            FROM job_events
            WHERE job_id = ? AND seq > ?
            ORDER BY seq ASC
            LIMIT ?
            "#,
        )
        .bind(job_id.to_string())
        .bind(after_seq)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(row_to_event).collect()
    }

    pub async fn list_events_tail(&self, job_id: Uuid, limit: i64) -> Result<Vec<JobEvent>> {
        let limit = limit.clamp(1, 50);
        let rows = sqlx::query(
            r#"
            SELECT id, job_id, seq, event_type, message, data, created_at FROM (
                SELECT id, job_id, seq, event_type, message, data, created_at
                FROM job_events
                WHERE job_id = ?
                ORDER BY seq DESC
                LIMIT ?
            ) ORDER BY seq ASC
            "#,
        )
        .bind(job_id.to_string())
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(row_to_event).collect()
    }

    // ── Reapers ─────────────────────────────────────────────────────────

    /// Mark timed-out running jobs; returns transitioned jobs for terminal webhooks.
    pub async fn mark_timed_out(&self, older_than: DateTime<Utc>) -> Result<Vec<Job>> {
        let now = Utc::now().to_rfc3339();
        let cutoff = older_than.to_rfc3339();
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM jobs WHERE status = 'running' AND claimed_at IS NOT NULL AND claimed_at < ?",
        )
        .bind(&cutoff)
        .fetch_all(&self.pool)
        .await?;
        if ids.is_empty() {
            return Ok(vec![]);
        }
        sqlx::query(
            r#"
            UPDATE jobs
            SET status = 'timed_out', finished_at = ?, updated_at = ?,
                error = COALESCE(error, 'job timed out while running'),
                lease_expires_at = NULL
            WHERE status = 'running' AND claimed_at IS NOT NULL AND claimed_at < ?
            "#,
        )
        .bind(&now)
        .bind(&now)
        .bind(&cutoff)
        .execute(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Ok(j) = Uuid::parse_str(&id) {
                if let Ok(Some(job)) = self.get_job(j).await {
                    if job.status == JobStatus::TimedOut {
                        out.push(job);
                    }
                }
            }
        }
        Ok(out)
    }

    /// Mark lease-expired running jobs; returns transitioned jobs for webhooks.
    pub async fn mark_lease_expired(&self) -> Result<Vec<Job>> {
        let now = Utc::now().to_rfc3339();
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM jobs WHERE status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?",
        )
        .bind(&now)
        .fetch_all(&self.pool)
        .await?;
        if ids.is_empty() {
            return Ok(vec![]);
        }
        sqlx::query(
            r#"
            UPDATE jobs
            SET status = 'timed_out', finished_at = ?, updated_at = ?,
                error = COALESCE(error, 'lease expired'),
                lease_expires_at = NULL
            WHERE status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?
            "#,
        )
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Ok(j) = Uuid::parse_str(&id) {
                if let Ok(Some(job)) = self.get_job(j).await {
                    if job.status == JobStatus::TimedOut {
                        out.push(job);
                    }
                }
            }
        }
        Ok(out)
    }

    /// Queued jobs past their timeout with an offline/missing target agent → no_agent.
    /// Returns transitioned jobs for webhooks.
    pub async fn mark_no_agent(&self, heartbeat_ttl_secs: i64) -> Result<Vec<Job>> {
        let now = Utc::now();
        let now_s = now.to_rfc3339();
        // Jobs with explicit target_agent_id that is offline or missing, and created past timeout
        let rows = sqlx::query(
            r#"
            SELECT j.id, j.timeout_secs, j.created_at, j.target_agent_id
            FROM jobs j
            WHERE j.status = 'queued' AND j.target_agent_id IS NOT NULL
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::new();
        for row in rows {
            let id: String = row.try_get("id")?;
            let timeout_secs: Option<i64> = row.try_get("timeout_secs")?;
            let created_at: String = row.try_get("created_at")?;
            let target: String = row.try_get("target_agent_id")?;
            let created = parse_dt(&created_at)?;
            let timeout = timeout_secs.unwrap_or(600);
            if now.signed_duration_since(created).num_seconds() < timeout {
                continue;
            }
            let agent = self.get_agent(&target).await?;
            let offline = match agent {
                None => true,
                Some(a) if a.revoked_at.is_some() => true,
                Some(a) => match a.last_seen_at {
                    None => true,
                    Some(seen) => {
                        now.signed_duration_since(seen).num_seconds() > heartbeat_ttl_secs
                    }
                },
            };
            if !offline {
                continue;
            }
            let res = sqlx::query(
                r#"
                UPDATE jobs
                SET status = 'no_agent', finished_at = ?, updated_at = ?,
                    error = COALESCE(error, 'no matching online agent')
                WHERE id = ? AND status = 'queued'
                "#,
            )
            .bind(&now_s)
            .bind(&now_s)
            .bind(&id)
            .execute(&self.pool)
            .await?;
            if res.rows_affected() > 0 {
                if let Ok(j) = Uuid::parse_str(&id) {
                    if let Ok(Some(job)) = self.get_job(j).await {
                        out.push(job);
                    }
                }
            }
        }
        Ok(out)
    }
}

#[derive(Debug, Clone)]
pub struct WorkerRow {
    pub id: String,
    pub label: String,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub last_seen_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub enum BindOutcome {
    Ok,
    WorkerNotFound,
    AgentNotFound,
    AlreadyBound(String),
}

#[derive(Debug, Clone)]
pub struct AgentRow {
    pub id: String,
    pub label: String,
    pub project: Option<String>,
    pub cwd: Option<String>,
    pub machine: Option<String>,
    pub capabilities: Vec<String>,
    pub max_concurrency: i64,
    pub last_seen_at: Option<DateTime<Utc>>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub provider: Option<String>,
    pub model: Option<String>,
}

impl AgentRow {
    pub fn to_public(&self, heartbeat_ttl_secs: i64, running_jobs: Option<i64>) -> Agent {
        let now = Utc::now();
        let online = self.revoked_at.is_none()
            && self
                .last_seen_at
                .map(|s| now.signed_duration_since(s).num_seconds() <= heartbeat_ttl_secs)
                .unwrap_or(false);
        Agent {
            id: self.id.clone(),
            label: self.label.clone(),
            project: self.project.clone(),
            cwd: self.cwd.clone(),
            machine: self.machine.clone(),
            capabilities: self.capabilities.clone(),
            max_concurrency: self.max_concurrency,
            last_seen_at: self.last_seen_at,
            status: if self.revoked_at.is_some() {
                "revoked".into()
            } else if online {
                "online".into()
            } else {
                "offline".into()
            },
            created_at: self.created_at,
            revoked_at: self.revoked_at,
            running_jobs,
            provider: self.provider.clone(),
            model: self.model.clone(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct JobEvent {
    pub id: String,
    pub job_id: String,
    pub seq: i64,
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug)]
pub enum SubmitOutcome {
    Ok(Job),
    NotFound,
    WrongStatus(JobStatus),
    Forbidden,
}

#[derive(Debug)]
pub enum LeaseOutcome {
    Ok(DateTime<Utc>),
    NotFound,
    WrongStatus(JobStatus),
    Forbidden,
}

#[derive(Debug)]
pub enum AppendEventOutcome {
    Ok(JobEvent),
    NotFound,
    WrongStatus(JobStatus),
    Forbidden,
}

fn row_to_worker_row(row: SqliteRow) -> Result<WorkerRow> {
    let revoked: Option<String> = row.try_get("revoked_at")?;
    let seen: Option<String> = row.try_get("last_seen_at")?;
    let created: String = row.try_get("created_at")?;
    Ok(WorkerRow {
        id: row.try_get("id")?,
        label: row.try_get("label")?,
        created_at: parse_dt(&created)?,
        revoked_at: revoked.map(|s| parse_dt(&s)).transpose()?,
        last_seen_at: seen.map(|s| parse_dt(&s)).transpose()?,
    })
}

fn row_to_agent_row(row: SqliteRow) -> Result<AgentRow> {
    let caps_str: String = row.try_get("capabilities")?;
    let capabilities: Vec<String> = serde_json::from_str(&caps_str).unwrap_or_default();
    let last_seen: Option<String> = row.try_get("last_seen_at")?;
    let revoked: Option<String> = row.try_get("revoked_at")?;
    let created: String = row.try_get("created_at")?;
    Ok(AgentRow {
        id: row.try_get("id")?,
        label: row.try_get("label")?,
        project: row.try_get("project")?,
        cwd: row.try_get("cwd")?,
        machine: row.try_get("machine")?,
        capabilities,
        max_concurrency: row.try_get("max_concurrency")?,
        last_seen_at: last_seen.map(|s| parse_dt(&s)).transpose()?,
        status: row.try_get("status")?,
        created_at: parse_dt(&created)?,
        revoked_at: revoked.map(|s| parse_dt(&s)).transpose()?,
        provider: row.try_get("pi_provider").ok().flatten(),
        model: row.try_get("pi_model").ok().flatten(),
    })
}

fn row_to_job(row: SqliteRow) -> Result<Job> {
    let id: String = row.try_get("id")?;
    let job_type: String = row.try_get("job_type")?;
    let status: String = row.try_get("status")?;
    let payload: String = row.try_get("payload")?;
    let result: Option<String> = row.try_get("result")?;
    let error: Option<String> = row.try_get("error")?;
    let created_at: String = row.try_get("created_at")?;
    let updated_at: String = row.try_get("updated_at")?;
    let claimed_at: Option<String> = row.try_get("claimed_at")?;
    let finished_at: Option<String> = row.try_get("finished_at")?;
    let agent_id: Option<String> = row.try_get("agent_id")?;
    let target_agent_id: Option<String> = row.try_get("target_agent_id")?;
    let target_filter: Option<String> = row.try_get("target_filter")?;
    let thread_id: Option<String> = row.try_get("thread_id")?;
    let delivery_mode: String = row
        .try_get("delivery_mode")
        .unwrap_or_else(|_| "steer".into());
    let lease_expires_at: Option<String> = row.try_get("lease_expires_at").unwrap_or(None);
    let timeout_secs: Option<i64> = row.try_get("timeout_secs").unwrap_or(None);

    Ok(Job {
        id: Uuid::parse_str(&id)?,
        job_type: JobType::parse(&job_type).context("bad job_type")?,
        status: JobStatus::parse(&status).context("bad status")?,
        payload: serde_json::from_str(&payload)?,
        result: result.map(|s| serde_json::from_str(&s)).transpose()?,
        error,
        created_at: parse_dt(&created_at)?,
        updated_at: parse_dt(&updated_at)?,
        claimed_at: claimed_at.map(|s| parse_dt(&s)).transpose()?,
        finished_at: finished_at.map(|s| parse_dt(&s)).transpose()?,
        agent_id,
        timeout_secs,
        target_agent_id,
        target_filter: target_filter
            .map(|s| serde_json::from_str(&s))
            .transpose()?,
        thread_id,
        delivery_mode: DeliveryMode::parse(&delivery_mode).unwrap_or(DeliveryMode::Steer),
        lease_expires_at: lease_expires_at.map(|s| parse_dt(&s)).transpose()?,
    })
}

fn row_to_event(row: SqliteRow) -> Result<JobEvent> {
    let data: Option<String> = row.try_get("data")?;
    let created: String = row.try_get("created_at")?;
    Ok(JobEvent {
        id: row.try_get("id")?,
        job_id: row.try_get("job_id")?,
        seq: row.try_get("seq")?,
        event_type: row.try_get("event_type")?,
        message: row.try_get("message")?,
        data: data.map(|s| serde_json::from_str(&s)).transpose()?,
        created_at: parse_dt(&created)?,
    })
}

fn parse_dt(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| {
            s.parse::<DateTime<Utc>>()
                .map_err(|e| anyhow::anyhow!("parse datetime {s}: {e}"))
        })
}

fn truncate_str(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…[truncated]", &s[..end])
}

/// Helper used by routes when building target from CreateJobRequest.
pub fn resolve_target(
    target: Option<&JobTarget>,
) -> (Option<String>, Option<Value>) {
    let Some(t) = target else {
        return (None, None);
    };
    if t.is_empty() {
        return (None, None);
    }
    if let Some(ref aid) = t.agent_id {
        if !aid.is_empty() {
            return (Some(aid.clone()), None);
        }
    }
    // Filter form
    let mut obj = serde_json::Map::new();
    if let Some(ref p) = t.project {
        if !p.is_empty() {
            obj.insert("project".into(), Value::String(p.clone()));
        }
    }
    if let Some(ref m) = t.machine {
        if !m.is_empty() {
            obj.insert("machine".into(), Value::String(m.clone()));
        }
    }
    if let Some(ref caps) = t.capabilities {
        if !caps.is_empty() {
            obj.insert(
                "capabilities".into(),
                Value::Array(caps.iter().cloned().map(Value::String).collect()),
            );
        }
    }
    if obj.is_empty() {
        (None, None)
    } else {
        (None, Some(Value::Object(obj)))
    }
}
