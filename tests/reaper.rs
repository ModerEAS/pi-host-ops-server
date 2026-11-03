//! Reaper: a long-running job lives while its lease is renewed; no fixed age cap.
use chrono::{Duration, Utc};
use pi_host_ops_server::db::Db;
use pi_host_ops_server::models::JobStatus;
use sqlx::Row;
use uuid::Uuid;

async fn setup() -> (Db, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("pi-host-ops-reaper-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = Db::connect(dir.join("t.db").to_str().unwrap()).await.unwrap();
    (db, dir)
}

async fn insert_running(db: &Db, claimed_ago: Duration, lease_in: Duration) -> Uuid {
    let id = Uuid::new_v4();
    let claimed = (Utc::now() - claimed_ago).to_rfc3339();
    let lease = (Utc::now() + lease_in).to_rfc3339();
    sqlx::query(
        "INSERT INTO jobs (id, job_type, status, payload, created_at, updated_at, delivery_mode, claimed_at, lease_expires_at)
         VALUES (?, 'act', 'running', '{}', ?, ?, 'steer', ?, ?)",
    )
    .bind(id.to_string())
    .bind(&claimed)
    .bind(&claimed)
    .bind(&claimed)
    .bind(&lease)
    .execute(db.pool())
    .await
    .unwrap();
    id
}

/// The old reaper killed any job whose `claimed_at` was >10 min old. A job that
/// has been running for hours with a live lease must survive.
#[tokio::test]
async fn old_running_job_with_live_lease_survives() {
    let (db, dir) = setup().await;
    let id = insert_running(&db, Duration::hours(6), Duration::minutes(1)).await;

    let reaped = db.mark_lease_expired().await.unwrap();

    assert!(reaped.is_empty(), "healthy long job must not be reaped");
    let job = db.get_job(id).await.unwrap().expect("job exists");
    assert_eq!(job.status, JobStatus::Running);

    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A worker that stops renewing is still recovered — by lease expiry only.
#[tokio::test]
async fn dead_worker_lease_expiry_times_out_job() {
    let (db, dir) = setup().await;
    let id = insert_running(&db, Duration::hours(6), Duration::seconds(-1)).await;

    let reaped = db.mark_lease_expired().await.unwrap();

    assert_eq!(reaped.len(), 1, "expired lease must be reaped");
    let row = sqlx::query("SELECT status, error FROM jobs WHERE id = ?")
        .bind(id.to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
    let status: String = row.try_get("status").unwrap();
    let error: Option<String> = row.try_get("error").unwrap();
    assert_eq!(status, "timed_out");
    assert_eq!(error.as_deref(), Some("lease expired"));

    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}
