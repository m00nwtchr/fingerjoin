use crate::backend::fan_out;
use crate::error::Error;
use crate::k8s::BackendState;
use crate::webfinger::{filter_rels, merge_jrd, to_json_bytes};
use axum::{
    Json, Router,
    body::Body,
    extract::{Query, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::{Level, debug, info, warn};

/// Cap on concurrent backend requests per incoming query.
const MAX_IN_FLIGHT: usize = 10;

#[derive(Clone)]
pub struct AppState {
    pub backends: Arc<BackendState>,
    pub client: reqwest::Client,
}

pub fn app(state: AppState) -> Router {
    // RFC 7033 §5: WebFinger must be queryable cross-origin from browsers.
    // The layer also answers CORS preflights.
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET]);

    let webfinger = Router::new()
        .route("/.well-known/webfinger", get(handle_webfinger))
        .layer(cors)
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

    // RFC 7033 allows any URI as the resource: acct:, https:, mailto:, ...
    // Mastodon and friends answer https: lookups, so only reject values that
    // are not absolute URIs at all.
    if url::Url::parse(&resource).is_err() {
        return Err(Error::InvalidResource(resource));
    }

    let rels: Vec<String> = params
        .iter()
        .filter(|(k, _)| k == "rel")
        .map(|(_, v)| v.clone())
        .collect();

    let backends = state.backends.get_all().await;
    if backends.is_empty() {
        warn!(resource = %resource, "webfinger request but no backends registered");
        return Err(Error::NoBackends);
    }

    debug!(resource = %resource, backends = backends.len(), "fanning out");

    let out = fan_out(&state.client, &backends, &resource, &rels, MAX_IN_FLIGHT).await;

    info!(
        resource = %resource,
        ok = out.successes.len(),
        not_found = out.not_found,
        gone = out.gone,
        declined = out.declined,
        failed = out.failed,
        "webfinger fan-out complete"
    );

    if out.successes.is_empty() {
        // Order matters for federation semantics: a transport failure means
        // the answer is unknown, so return a retryable 502 rather than a
        // definitive 404 that remote servers would cache as "no such user".
        return Err(if out.failed > 0 {
            warn!(resource = %resource, failed = out.failed, "no backend answered successfully");
            Error::AllBackendsFailed
        } else if out.gone > 0 {
            Error::ResourceGone
        } else {
            Error::ResourceNotFound
        });
    }

    let mut merged = merge_jrd(out.successes);
    filter_rels(&mut merged, &rels);
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
    use crate::backend::Backend;
    use crate::testutil::{mock_client, spawn_mock_backend};
    use axum::body::to_bytes;
    use axum::http::Request;
    use tower::ServiceExt;

    async fn state_with(backends: Vec<Backend>) -> AppState {
        let st = Arc::new(BackendState::new());
        st.update(backends).await;
        AppState {
            backends: st,
            client: mock_client(),
        }
    }

    fn backend(url: &url::Url, priority: u16) -> Backend {
        Backend {
            name: format!("mock-{priority}"),
            url: url.clone(),
            priority,
        }
    }

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
    async fn test_missing_resource_is_400_with_cors() {
        let url = spawn_mock_backend("a").await;
        let resp = get_response(
            state_with(vec![backend(&url, 50)]).await,
            "/.well-known/webfinger",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            resp.headers()
                .get("access-control-allow-origin")
                .map(|v| v.to_str().unwrap()),
            Some("*")
        );
    }

    #[tokio::test]
    async fn test_non_uri_resource_is_400() {
        let url = spawn_mock_backend("a").await;
        let resp = get_response(
            state_with(vec![backend(&url, 50)]).await,
            "/.well-known/webfinger?resource=alice%40example.com",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_no_backends_is_503_with_retry_after() {
        let resp = get_response(
            state_with(vec![]).await,
            "/.well-known/webfinger?resource=acct:alice@example.com",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok()),
            Some("10")
        );
    }

    #[tokio::test]
    async fn test_success_merges_and_sets_jrd_content_type() {
        let a = spawn_mock_backend("a").await;
        let b = spawn_mock_backend("b").await;
        let resp = get_response(
            state_with(vec![backend(&a, 10), backend(&b, 90)]).await,
            "/.well-known/webfinger?resource=acct:alice@example.com",
        )
        .await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("content-type")
                .map(|v| v.to_str().unwrap()),
            Some("application/jrd+json")
        );
        assert_eq!(
            resp.headers()
                .get("access-control-allow-origin")
                .map(|v| v.to_str().unwrap()),
            Some("*")
        );

        let body = body_json(resp).await;
        assert_eq!(body["subject"], "acct:alice@example.com");
        // One self link per variant (different hrefs) survives the merge...
        let links = body["links"].as_array().unwrap();
        assert_eq!(links.iter().filter(|l| l["rel"] == "self").count(), 2);
        // ...while the shared profile-page link deduplicates to one.
        assert_eq!(
            links
                .iter()
                .filter(|l| l["rel"] == "http://webfinger.net/rel/profile-page")
                .count(),
            1
        );
        // Higher priority backend wins the contested property.
        assert_eq!(body["properties"]["http://example.com/ns/variant"], "b");
        // Nonstandard `template` member passes through.
        assert!(
            links
                .iter()
                .any(|l| l["rel"] == "http://ostatus.org/schema/1.0/subscribe"
                    && l["template"].is_string())
        );
    }

    #[tokio::test]
    async fn test_rel_filter_applied_to_merged_result() {
        let a = spawn_mock_backend("a").await;
        let resp = get_response(
            state_with(vec![backend(&a, 50)]).await,
            "/.well-known/webfinger?resource=acct:alice@example.com&rel=self",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let links = body["links"].as_array().unwrap();
        assert!(!links.is_empty());
        assert!(links.iter().all(|l| l["rel"] == "self"));
    }

    #[tokio::test]
    async fn test_unknown_resource_is_404() {
        let a = spawn_mock_backend("a").await;
        let resp = get_response(
            state_with(vec![backend(&a, 50)]).await,
            "/.well-known/webfinger?resource=acct:missing@example.com",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_gone_resource_is_410() {
        let a = spawn_mock_backend("a").await;
        let resp = get_response(
            state_with(vec![backend(&a, 50)]).await,
            "/.well-known/webfinger?resource=acct:gone@example.com",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::GONE);
    }

    #[tokio::test]
    async fn test_declined_resource_is_404() {
        // The only backend rejects the resource form with 400: definitive
        // negative, so the client gets 404, never a retryable 502.
        let a = spawn_mock_backend("a").await;
        let resp = get_response(
            state_with(vec![backend(&a, 50)]).await,
            "/.well-known/webfinger?resource=acct:decline@example.com",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_decline_does_not_poison_merge() {
        // One backend declines with 400 while another answers: the answer
        // must merge through as a 200.
        let a = spawn_mock_backend("a").await;
        let b = spawn_mock_backend("b").await;
        let resp = get_response(
            state_with(vec![backend(&a, 50), backend(&b, 60)]).await,
            "/.well-known/webfinger?resource=acct:decline@example.com",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["subject"], "acct:decline@example.com");
        assert_eq!(body["properties"]["http://example.com/ns/variant"], "b");
    }

    #[tokio::test]
    async fn test_backend_error_is_502() {
        let a = spawn_mock_backend("a").await;
        let resp = get_response(
            state_with(vec![backend(&a, 50)]).await,
            "/.well-known/webfinger?resource=acct:boom@example.com",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn test_mixed_failure_and_not_found_is_502() {
        // One backend says 404, another is down: the answer is unknown, so
        // the retryable status must win over the definitive 404.
        let a = spawn_mock_backend("a").await;
        let dead = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = l.local_addr().unwrap();
            drop(l);
            url::Url::parse(&format!("http://{addr}")).unwrap()
        };
        let resp = get_response(
            state_with(vec![backend(&a, 50), backend(&dead, 60)]).await,
            "/.well-known/webfinger?resource=acct:missing@example.com",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn test_readiness_gates_on_first_sync() {
        let st = Arc::new(BackendState::new());
        let state = AppState {
            backends: st.clone(),
            client: mock_client(),
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
            client: mock_client(),
        };
        let resp = get_response(state, "/healthz").await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["status"], "ok");
        assert_eq!(body["last_sync_seconds"], serde_json::Value::Null);
    }
}
