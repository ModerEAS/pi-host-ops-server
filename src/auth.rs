use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

use crate::config::hash_token;
use crate::AppState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Dispatcher,
    Agent,
    Worker,
}

pub struct AuthUser {
    pub role: Role,
    /// Set when role == Agent (looked up from token_hash).
    pub agent_id: Option<String>,
    /// Set when role == Worker (looked up from token_hash).
    pub worker_id: Option<String>,
}

#[derive(Debug)]
pub struct AuthError(pub StatusCode, pub String);

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

fn extract_bearer(parts: &Parts) -> Result<&str, AuthError> {
    let header = parts
        .headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            AuthError(
                StatusCode::UNAUTHORIZED,
                "missing Authorization header".into(),
            )
        })?;

    header
        .strip_prefix("Bearer ")
        .or_else(|| header.strip_prefix("bearer "))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AuthError(StatusCode::UNAUTHORIZED, "expected Bearer token".into()))
}

impl FromRequestParts<AppState> for AuthUser {
    type Rejection = AuthError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let token = extract_bearer(parts)?;

        if token == state.config.dispatcher_token {
            return Ok(AuthUser {
                role: Role::Dispatcher,
                agent_id: None,
                worker_id: None,
            });
        }

        let token_hash = hash_token(token);

        // Worker credential (multiplexed heartbeat + poll over bound agents).
        match state.db.find_worker_by_token_hash(&token_hash).await {
            Ok(Some(worker)) => {
                if worker.revoked_at.is_some() {
                    return Err(AuthError(
                        StatusCode::UNAUTHORIZED,
                        "worker token revoked".into(),
                    ));
                }
                return Ok(AuthUser {
                    role: Role::Worker,
                    agent_id: None,
                    worker_id: Some(worker.id),
                });
            }
            Ok(None) => {}
            Err(err) => {
                tracing::error!(?err, "worker token lookup failed");
                return Err(AuthError(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "auth lookup failed".into(),
                ));
            }
        }

        match state.db.find_agent_by_token_hash(&token_hash).await {
            Ok(Some(agent)) => {
                if agent.revoked_at.is_some() {
                    return Err(AuthError(
                        StatusCode::UNAUTHORIZED,
                        "agent token revoked".into(),
                    ));
                }
                Ok(AuthUser {
                    role: Role::Agent,
                    agent_id: Some(agent.id),
                    worker_id: None,
                })
            }
            Ok(None) => Err(AuthError(StatusCode::UNAUTHORIZED, "invalid token".into())),
            Err(err) => {
                tracing::error!(?err, "agent token lookup failed");
                Err(AuthError(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "auth lookup failed".into(),
                ))
            }
        }
    }
}

pub fn require_dispatcher(user: &AuthUser) -> Result<(), AuthError> {
    if user.role == Role::Dispatcher {
        Ok(())
    } else {
        Err(AuthError(
            StatusCode::FORBIDDEN,
            "dispatcher token required".into(),
        ))
    }
}

pub fn require_agent(user: &AuthUser) -> Result<&str, AuthError> {
    if user.role == Role::Agent {
        user.agent_id
            .as_deref()
            .ok_or_else(|| AuthError(StatusCode::UNAUTHORIZED, "agent id missing".into()))
    } else {
        Err(AuthError(
            StatusCode::FORBIDDEN,
            "agent token required".into(),
        ))
    }
}

pub fn require_worker(user: &AuthUser) -> Result<&str, AuthError> {
    if user.role == Role::Worker {
        user.worker_id
            .as_deref()
            .ok_or_else(|| AuthError(StatusCode::UNAUTHORIZED, "worker id missing".into()))
    } else {
        Err(AuthError(
            StatusCode::FORBIDDEN,
            "worker token required".into(),
        ))
    }
}
