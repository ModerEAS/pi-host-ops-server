//! Concurrent claim atomicity with targeted jobs.
use chrono::Utc;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::Row;
use std::str::FromStr;
use uuid::Uuid;

async fn setup_pool(path: &str) -> sqlx::SqlitePool {
    let options = SqliteConnectOptions::from_str(&format!("sqlite:{path}"))
        .unwrap()
        .create_if_missing(true)
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(10)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    pool
}

async fn insert_queued(pool: &sqlx::SqlitePool, id: Uuid) {
    let now = Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO jobs (id, job_type, status, payload, created_at, updated_at, delivery_mode) VALUES (?, 'shell', 'queued', '{}', ?, ?, 'steer')",
    )
    .bind(id.to_string())
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await
    .unwrap();
}

async fn claim_one(pool: &sqlx::SqlitePool, agent: &str) -> Option<String> {
    let mut tx = pool.begin().await.unwrap();
    let row = sqlx::query(
        "SELECT id FROM jobs WHERE status = 'queued' ORDER BY created_at ASC LIMIT 1",
    )
    .fetch_optional(&mut *tx)
    .await
    .unwrap();
    let Some(row) = row else {
        tx.commit().await.unwrap();
        return None;
    };
    let id: String = row.try_get("id").unwrap();
    let now = Utc::now().to_rfc3339();
    let updated = sqlx::query(
        "UPDATE jobs SET status = 'running', claimed_at = ?, updated_at = ?, agent_id = ? WHERE id = ? AND status = 'queued'",
    )
    .bind(&now)
    .bind(&now)
    .bind(agent)
    .bind(&id)
    .execute(&mut *tx)
    .await
    .unwrap();
    if updated.rows_affected() != 1 {
        tx.rollback().await.unwrap();
        return None;
    }
    tx.commit().await.unwrap();
    Some(id)
}

#[tokio::test]
async fn concurrent_claims_only_one_wins() {
    let dir = std::env::temp_dir().join(format!("pi-host-ops-claim-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("t.db");
    let pool = setup_pool(db_path.to_str().unwrap()).await;
    let job_id = Uuid::new_v4();
    insert_queued(&pool, job_id).await;

    let p1 = pool.clone();
    let p2 = pool.clone();
    let (a, b) = tokio::join!(
        async move { claim_one(&p1, "a").await },
        async move { claim_one(&p2, "b").await },
    );

    let wins = [a, b].into_iter().flatten().collect::<Vec<_>>();
    assert_eq!(wins.len(), 1, "exactly one claim should succeed: {wins:?}");
    assert_eq!(wins[0], job_id.to_string());
}
