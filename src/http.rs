use crate::backend::fan_out;
use crate::error::Error;
use crate::k8s::BackendState;
use crate::webfinger::{merge_jrd, to_json_bytes};
use axum::{
    body::Body,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use std::sync::Arc;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::{debug, info, warn, Level};

/// Cap on concurrent backend requests per incoming query.
const MAX_IN_FLIGHT: usize = 10;

#[derive(Clone)]
pub struct AppState {
    pub backends: Arc<BackendState>,
    pub client: reqwest::Client,
}

pub fn app(state: AppState) -> Router {
    let webfinger = Router::new()
        .route("/.well-known/webfinger", get(handle_webfinger))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        );

    // Health routes stay outside the trace layer so probes do not flood the
    // access log every few seconds.
    Router::new()
        .merge(webfinger)
        .route("/healthz", get(handle_healthz))
        .route("/readyz", get(handle_readyz))
        .route("/health", get(handle_healthz))
        .with_state(state)
}

async fn handle_webfinger(
    State(state): State<AppState>,
    Query(params): Query<Vec<(String, String)>>,
) -> Result<Response, Error> {
    let resource = params
        .iter()
        .find(|(k, _)| k == "resource")
        .map(|(_, v)| v.clone())
        .ok_or_else(|| Error::InvalidResource("missing resource parameter".to_string()))?;

    if !resource.starts_with("acct:") {
        return Err(Error::InvalidResource(resource));
    }

    let backends = state.backends.get_all().await;
    if backends.is_empty() {
        warn!(resource = %resource, "webfinger request but no backends registered");
        return Err(Error::NoBackends);
    }

    debug!(resource = %resource, backends = backends.len(), "fanning out");

    let results = fan_out(&state.client, &backends, &resource, MAX_IN_FLIGHT).await;

    info!(
        resource = %resource,
        ok = results.len(),
        "webfinger fan-out complete"
    );

    if results.is_empty() {
        return Err(Error::AllBackendsFailed);
    }

    let merged = merge_jrd(results);
    let body = to_json_bytes(&merged)?;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/jrd+json")
        .body(Body::from(body))
        .expect("static response must build"))
}

async fn handle_healthz(State(state): State<AppState>) -> Response {
    let backends = state.backends.get_all().await.len();
    let last_sync_seconds = state.backends.last_sync_age().await.map(|d| d.as_secs());
    Json(serde_json::json!({
        "status": "ok",
        "backends": backends,
        "last_sync_seconds": last_sync_seconds,
    }))
    .into_response()
}

async fn handle_readyz(State(state): State<AppState>) -> Response {
    if state.backends.synced().await {
        let backends = state.backends.get_all().await.len();
        Json(serde_json::json!({"status": "ready", "backends": backends})).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "status": "unready",
                "reason": "awaiting first HTTPRoute sync",
            })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Request;
    use tower::ServiceExt;

    async fn get_response(state: AppState, uri: &str) -> Response {
        app(state)
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    async fn body_json(resp: Response) -> serde_json::Value {
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn test_readiness_gates_on_first_sync() {
        let st = Arc::new(BackendState::new());
        let state = AppState {
            backends: st.clone(),
            client: reqwest::Client::new(),
        };

        let resp = get_response(state.clone(), "/readyz").await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

        // An empty-but-completed sync still counts as ready.
        st.update(vec![]).await;
        let resp = get_response(state, "/readyz").await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_healthz_always_ok() {
        let st = Arc::new(BackendState::new());
        let state = AppState {
            backends: st,
            client: reqwest::Client::new(),
        };
        let resp = get_response(state, "/healthz").await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["status"], "ok");
        assert_eq!(body["last_sync_seconds"], serde_json::Value::Null);
    }
}
