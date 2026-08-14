use crate::backend::Backend;
use kube::{
    api::{Api, ListParams, NotUsed, Object},
    discovery::Discovery,
    Client,
};
use serde::Deserialize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, error, info, warn};

const WEBFINGER_KEY: &str = "fingerjoin.naktis.eu/webfinger";
const PRIORITY_KEY: &str = "fingerjoin.naktis.eu/priority";
const HTTPS_KEY: &str = "fingerjoin.naktis.eu/https";
const BACKEND_KEY: &str = "fingerjoin.naktis.eu/backend";

pub const SYNC_INTERVAL: Duration = Duration::from_secs(30);
const GATEWAY_API_GROUP: &str = "gateway.networking.k8s.io";

pub struct BackendState {
    backends: RwLock<Vec<Backend>>,
    last_sync: RwLock<Option<Instant>>,
}

impl BackendState {
    pub fn new() -> Self {
        Self {
            backends: RwLock::new(Vec::new()),
            last_sync: RwLock::new(None),
        }
    }

    pub async fn update(&self, new_backends: Vec<Backend>) {
        {
            let mut backends = self.backends.write().await;
            if *backends != new_backends {
                info!(
                    count = new_backends.len(),
                    backends = ?new_backends.iter().map(|b| b.url.as_str()).collect::<Vec<_>>(),
                    "backend set changed"
                );
                *backends = new_backends;
            }
        }
        *self.last_sync.write().await = Some(Instant::now());
    }

    pub async fn get_all(&self) -> Vec<Backend> {
        self.backends.read().await.clone()
    }

    /// True once at least one HTTPRoute list has completed. Readiness gates
    /// on this so pods do not receive traffic before the first sync.
    pub async fn synced(&self) -> bool {
        self.last_sync.read().await.is_some()
    }

    pub async fn last_sync_age(&self) -> Option<Duration> {
        self.last_sync.read().await.map(|t| t.elapsed())
    }
}

impl Default for BackendState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Deserialize)]
struct HTTPRouteSpec {
    #[serde(default)]
    rules: Vec<RouteRule>,
}

#[derive(Debug, Clone, Deserialize)]
struct RouteRule {
    #[serde(rename = "backendRefs", default)]
    backend_refs: Vec<BackendRef>,
}

#[derive(Debug, Clone, Deserialize)]
struct BackendRef {
    #[serde(default)]
    group: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    namespace: Option<String>,
    #[serde(default)]
    port: Option<u16>,
}

type HTTPRoute = Object<HTTPRouteSpec, NotUsed>;

pub async fn start_reconciler(client: Client, state: Arc<BackendState>, cluster_domain: String) {
    let api = discover_httproute_api(&client).await;

    let mut ticker = interval(SYNC_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        match api.list(&ListParams::default()).await {
            Ok(routes) => {
                let backends = backends_from_routes(routes, &cluster_domain);
                state.update(backends).await;
            }
            Err(e) => {
                // Keep serving the previous backend set; readiness stays
                // green because a stale list beats no service at all.
                error!(error = %e, "failed to list HTTPRoutes, keeping previous backends");
            }
        }
    }
}

/// Resolve the HTTPRoute API, retrying with backoff until the Gateway API is
/// available. This must never panic: with `panic = "abort"` a failed startup
/// discovery would kill the whole process, and a dead reconciler task would
/// otherwise leave the proxy serving zero backends forever.
async fn discover_httproute_api(client: &Client) -> Api<HTTPRoute> {
    let mut delay = Duration::from_secs(5);
    loop {
        match try_discover(client).await {
            Ok(api) => {
                info!("discovered HTTPRoute API");
                return api;
            }
            Err(e) => {
                error!(error = %e, retry_in_seconds = delay.as_secs(), "Gateway API discovery failed");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(60));
            }
        }
    }
}

async fn try_discover(client: &Client) -> Result<Api<HTTPRoute>, String> {
    let discovery = Discovery::new(client.clone())
        .filter(&[GATEWAY_API_GROUP])
        .run()
        .await
        .map_err(|e| format!("API discovery failed: {e}"))?;

    let group = discovery
        .groups()
        .find(|g| g.name() == GATEWAY_API_GROUP)
        .ok_or_else(|| {
            format!("{GATEWAY_API_GROUP} API group not found (is the Gateway API installed?)")
        })?;

    let (ar, _caps) = group
        .recommended_resources()
        .iter()
        .find(|(ar, _)| ar.kind == "HTTPRoute")
        .ok_or_else(|| format!("HTTPRoute kind not found in {GATEWAY_API_GROUP}"))?
        .clone();

    Ok(Api::all_with(client.clone(), &ar))
}

/// Map annotated HTTPRoutes to backends, deduplicating by URL (highest
/// priority wins) and sorting for stable change detection.
fn backends_from_routes(
    routes: impl IntoIterator<Item = HTTPRoute>,
    cluster_domain: &str,
) -> Vec<Backend> {
    let mut backends: Vec<Backend> = Vec::new();
    for route in routes {
        let Some(b) = backend_from_route(&route, cluster_domain) else {
            continue;
        };
        if let Some(existing) = backends.iter_mut().find(|e| e.url == b.url) {
            if b.priority > existing.priority {
                *existing = b;
            }
        } else {
            backends.push(b);
        }
    }
    backends.sort_by(|a, b| a.url.as_str().cmp(b.url.as_str()));
    backends
}

fn backend_from_route(route: &HTTPRoute, cluster_domain: &str) -> Option<Backend> {
    let annotations = route.metadata.annotations.as_ref()?;

    let is_webfinger = annotations
        .get(WEBFINGER_KEY)
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    if !is_webfinger {
        return None;
    }

    let route_namespace = route.metadata.namespace.as_deref().unwrap_or("default");
    let route_id = format!(
        "{route_namespace}/{}",
        route.metadata.name.as_deref().unwrap_or("<unnamed>")
    );

    let priority: u16 = parse_annotation(annotations, PRIORITY_KEY, 50, &route_id);
    let backend_index: usize = parse_annotation(annotations, BACKEND_KEY, 0, &route_id);
    let https = annotations
        .get(HTTPS_KEY)
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));

    let Some(rule) = route.spec.rules.get(backend_index) else {
        warn!(
            route = %route_id,
            index = backend_index,
            rules = route.spec.rules.len(),
            "backend rule index out of range, skipping route"
        );
        return None;
    };

    let Some(backend_ref) = rule.backend_refs.first() else {
        warn!(route = %route_id, "rule has no backendRefs, skipping route");
        return None;
    };

    // Only core/v1 Services resolve to a stable cluster DNS name.
    let group_ok = backend_ref.group.as_deref().is_none_or(str::is_empty);
    let kind_ok = backend_ref.kind.as_deref().is_none_or(|k| k == "Service");
    if !group_ok || !kind_ok {
        warn!(
            route = %route_id,
            group = backend_ref.group.as_deref().unwrap_or(""),
            kind = backend_ref.kind.as_deref().unwrap_or("Service"),
            "backendRef is not a core Service, skipping route"
        );
        return None;
    }

    let Some(backend_name) = backend_ref.name.as_deref() else {
        warn!(route = %route_id, "backendRef has no name, skipping route");
        return None;
    };

    // An explicit backendRef namespace (cross-namespace routing via
    // ReferenceGrant) takes precedence over the route's own namespace.
    let namespace = backend_ref.namespace.as_deref().unwrap_or(route_namespace);
    let scheme = if https { "https" } else { "http" };
    let port = backend_ref.port.unwrap_or(if https { 443 } else { 80 });
    let url = format!("{scheme}://{backend_name}.{namespace}.svc.{cluster_domain}:{port}");

    let url = match url::Url::parse(&url) {
        Ok(u) => u,
        Err(e) => {
            warn!(route = %route_id, url = %url, error = %e, "backend URL invalid, skipping route");
            return None;
        }
    };

    debug!(route = %route_id, url = %url, priority, "registered webfinger backend");

    Some(Backend {
        name: backend_name.to_string(),
        url,
        priority,
    })
}

fn parse_annotation<T: FromStr + Copy>(
    annotations: &std::collections::BTreeMap<String, String>,
    key: &str,
    default: T,
    route_id: &str,
) -> T {
    match annotations.get(key) {
        None => default,
        Some(raw) => raw.parse().unwrap_or_else(|_| {
            warn!(route = %route_id, annotation = key, value = %raw, "invalid annotation value, using default");
            default
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn route(annotations: serde_json::Value, spec: serde_json::Value) -> HTTPRoute {
        serde_json::from_value(json!({
            "apiVersion": "gateway.networking.k8s.io/v1",
            "kind": "HTTPRoute",
            "metadata": {
                "name": "test-route",
                "namespace": "apps",
                "annotations": annotations,
            },
            "spec": spec,
        }))
        .expect("test route should deserialize")
    }

    fn simple_spec() -> serde_json::Value {
        json!({"rules": [{"backendRefs": [{"name": "mastodon-web", "port": 3000}]}]})
    }

    #[test]
    fn test_unannotated_route_ignored() {
        let r = route(json!({}), simple_spec());
        assert!(backend_from_route(&r, "cluster.local").is_none());
    }

    #[test]
    fn test_annotated_route_resolves_fqdn() {
        let r = route(json!({WEBFINGER_KEY: "true"}), simple_spec());
        let b = backend_from_route(&r, "cluster.local").expect("should resolve");
        assert_eq!(
            b.url.as_str(),
            "http://mastodon-web.apps.svc.cluster.local:3000/"
        );
        assert_eq!(b.priority, 50);
    }

    #[test]
    fn test_https_default_port() {
        let r = route(
            json!({WEBFINGER_KEY: "true", HTTPS_KEY: "true"}),
            json!({"rules": [{"backendRefs": [{"name": "keyserver"}]}]}),
        );
        let b = backend_from_route(&r, "cluster.local").expect("should resolve");
        assert_eq!(b.url.scheme(), "https");
        assert_eq!(b.url.port_or_known_default(), Some(443));
    }

    #[test]
    fn test_cross_namespace_backend_ref() {
        let r = route(
            json!({WEBFINGER_KEY: "true"}),
            json!({"rules": [{"backendRefs": [{"name": "svc", "namespace": "other", "port": 80}]}]}),
        );
        let b = backend_from_route(&r, "cluster.local").expect("should resolve");
        assert_eq!(b.url.host_str(), Some("svc.other.svc.cluster.local"));
    }

    #[test]
    fn test_non_service_backend_ref_skipped() {
        let r = route(
            json!({WEBFINGER_KEY: "true"}),
            json!({"rules": [{"backendRefs": [{"name": "fn", "kind": "ServiceImport", "group": "multicluster.x-k8s.io"}]}]}),
        );
        assert!(backend_from_route(&r, "cluster.local").is_none());
    }

    #[test]
    fn test_backend_index_out_of_range_skipped() {
        let r = route(
            json!({WEBFINGER_KEY: "true", BACKEND_KEY: "3"}),
            simple_spec(),
        );
        assert!(backend_from_route(&r, "cluster.local").is_none());
    }

    #[test]
    fn test_invalid_priority_uses_default() {
        let r = route(
            json!({WEBFINGER_KEY: "true", PRIORITY_KEY: "not-a-number"}),
            simple_spec(),
        );
        let b = backend_from_route(&r, "cluster.local").expect("should resolve");
        assert_eq!(b.priority, 50);
    }

    #[test]
    fn test_custom_priority_and_backend_index() {
        let r = route(
            json!({WEBFINGER_KEY: "true", PRIORITY_KEY: "100", BACKEND_KEY: "1"}),
            json!({"rules": [
                {"backendRefs": [{"name": "first", "port": 80}]},
                {"backendRefs": [{"name": "second", "port": 8080}]}
            ]}),
        );
        let b = backend_from_route(&r, "cluster.local").expect("should resolve");
        assert_eq!(b.priority, 100);
        assert_eq!(b.url.host_str(), Some("second.apps.svc.cluster.local"));
    }

    #[test]
    fn test_dedup_keeps_highest_priority() {
        let a = route(
            json!({WEBFINGER_KEY: "true", PRIORITY_KEY: "10"}),
            simple_spec(),
        );
        let b = route(
            json!({WEBFINGER_KEY: "true", PRIORITY_KEY: "90"}),
            simple_spec(),
        );
        let backends = backends_from_routes(vec![a, b], "cluster.local");
        assert_eq!(backends.len(), 1);
        assert_eq!(backends[0].priority, 90);
    }
}
