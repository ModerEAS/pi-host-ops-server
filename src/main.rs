use std::sync::Arc;

use anyhow::Context;
use tracing_subscriber::EnvFilter;

use pi_host_ops_server::config::Config;
use pi_host_ops_server::{app, bootstrap};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("pi_host_ops_server=info,tower_http=info")),
        )
        .init();

    let config = Arc::new(Config::from_env().context("load config")?);
    let state = bootstrap(config.clone()).await.context("bootstrap")?;

    let db = state.db.clone();
    let heartbeat_ttl = config.heartbeat_ttl_secs;
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;
            let cutoff = chrono::Utc::now() - chrono::Duration::minutes(10);
            let _ = db.mark_timed_out(cutoff).await;
            let _ = db.mark_lease_expired().await;
            let _ = db.mark_no_agent(heartbeat_ttl).await;
        }
    });

    let listener = tokio::net::TcpListener::bind(config.bind)
        .await
        .with_context(|| format!("bind {}", config.bind))?;

    tracing::info!(%config.bind, "pi-host-ops-server listening");
    tracing::info!(
        heartbeat_ttl_secs = config.heartbeat_ttl_secs,
        lease_secs = config.lease_secs,
        "multi-agent auth; GET /health public; all /v1/* require Bearer"
    );

    axum::serve(listener, app(state)).await.context("serve")?;
    Ok(())
}
