pub mod auth;
pub mod config;
pub mod db;
pub mod models;
pub mod routes;
pub mod webhook;

use std::sync::Arc;

use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use config::Config;
use db::Db;

#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub config: Arc<Config>,
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .merge(routes::router())
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}

pub async fn bootstrap(config: Arc<Config>) -> anyhow::Result<AppState> {
    let db = Db::connect(&config.database_url).await?;
    if let Some(ref seed) = config.agent_token_seed {
        let _ = db
            .upsert_seed_agent("default", seed, "default (bootstrap)")
            .await;
    }
    Ok(AppState { db, config })
}
