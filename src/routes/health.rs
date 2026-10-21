use axum::Json;

use crate::models::HealthResponse;

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { ok: true })
}
