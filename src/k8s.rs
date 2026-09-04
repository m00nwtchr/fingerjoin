use crate::backend::Backend;
use k8s_openapi::api::core::v1::Service;
use kube::{
    Client,
    api::{Api, ListParams},
};
use std::time::{Duration, Instant};
use std::{str::FromStr, sync::Arc};
use tokio::sync::RwLock;
use tokio::time::{Instant as TokioInstant, MissedTickBehavior, interval_at};
use tracing::{debug, error, info, warn};

const WEBFINGER_KEY: &str = "fingerjoin.naktis.eu/webfinger";
const PRIORITY_KEY: &str = "fingerjoin.naktis.eu/priority";
const HTTPS_KEY: &str = "fingerjoin.naktis.eu/https";
const PORT_KEY: &str = "fingerjoin.naktis.eu/port";

pub const SYNC_INTERVAL: Duration = Duration::from_secs(30);

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

    /// True once at least one Service list has completed. Readiness gates
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

pub async fn start_reconciler(client: Client, state: Arc<BackendState>, cluster_domain: String) {
    let api: Api<Service> = Api::all(client);

    let mut ticker = interval_at(TokioInstant::now() + SYNC_INTERVAL, SYNC_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        match api.list(&ListParams::default()).await {
            Ok(services) => {
                let backends = backends_from_services(services, &cluster_domain);
                state.update(backends).await;
            }
            Err(e) => {
                // Keep serving the previous backend set; readiness stays
                // green because a stale list beats no service at all.
                error!(error = %e, "failed to list Services, keeping previous backends");
            }
        }
    }
}

/// Map annotated Services to backends, deduplicating by URL (highest
/// priority wins) and sorting for stable change detection.
fn backends_from_services(
    services: impl IntoIterator<Item = Service>,
    cluster_domain: &str,
) -> Vec<Backend> {
    let mut backends: Vec<Backend> = Vec::new();
    for service in services {
        let Some(b) = backend_from_service(&service, cluster_domain) else {
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

fn backend_from_service(service: &Service, cluster_domain: &str) -> Option<Backend> {
    let annotations = service.metadata.annotations.as_ref()?;

    let is_webfinger = annotations
        .get(WEBFINGER_KEY)
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    if !is_webfinger {
        return None;
    }

    let namespace = service.metadata.namespace.as_deref().unwrap_or("default");
    let service_id = format!(
        "{namespace}/{}",
        service.metadata.name.as_deref().unwrap_or("<unnamed>")
    );

    let priority: u16 = parse_annotation(annotations, PRIORITY_KEY, 50, &service_id);
    let https = annotations
        .get(HTTPS_KEY)
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));

    let ports = service.spec.as_ref().and_then(|spec| spec.ports.as_ref());
    let usable_ports = ports
        .into_iter()
        .flatten()
        .filter(|port| port.port > 0)
        .collect::<Vec<_>>();
    if usable_ports.is_empty() {
        warn!(service = %service_id, "Service has no usable positive ports, skipping");
        return None;
    }

    let (selected, auto_selected) = match annotations.get(PORT_KEY) {
        Some(name) => {
            let Some(port) = usable_ports
                .iter()
                .find(|port| port.name.as_deref() == Some(name))
            else {
                warn!(service = %service_id, port = %name, "explicit Service port name not found, skipping");
                return None;
            };
            (port, false)
        }
        None => {
            let preferred_name = if https { "https" } else { "http" };
            let secondary_name = if https { "http" } else { "https" };
            let port = usable_ports
                .iter()
                .find(|port| port.name.as_deref() == Some(preferred_name))
                .or_else(|| {
                    usable_ports
                        .iter()
                        .find(|port| port.name.as_deref() == Some(secondary_name))
                })
                .unwrap_or(&usable_ports[0]);
            (port, true)
        }
    };

    let scheme = if https || (auto_selected && selected.name.as_deref() == Some("https")) {
        "https"
    } else {
        "http"
    };
    let backend_name = service.metadata.name.as_deref().unwrap_or("<unnamed>");
    let url = format!(
        "{scheme}://{backend_name}.{namespace}.svc.{cluster_domain}:{}",
        selected.port
    );

    let url = match url::Url::parse(&url) {
        Ok(u) => u,
        Err(e) => {
            warn!(service = %service_id, url = %url, error = %e, "backend URL invalid, skipping Service");
            return None;
        }
    };

    debug!(service = %service_id, url = %url, priority, "registered webfinger backend");

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
    service_id: &str,
) -> T {
    match annotations.get(key) {
        None => default,
        Some(raw) => raw.parse().unwrap_or_else(|_| {
            warn!(service = %service_id, annotation = key, value = %raw, "invalid annotation value, using default");
            default
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::Service;
    use serde_json::json;

    fn service(annotations: serde_json::Value, ports: serde_json::Value) -> Service {
        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "test-service",
                "namespace": "apps",
                "annotations": annotations,
            },
            "spec": {"ports": ports},
        }))
        .expect("test service should deserialize")
    }

    fn ports() -> serde_json::Value {
        json!([
            {"name": "http", "port": 8080},
            {"name": "https", "port": 8443}
        ])
    }

    #[test]
    fn unannotated_service_is_ignored() {
        let service = service(json!({}), ports());
        assert!(backend_from_service(&service, "cluster.local").is_none());
    }

    #[test]
    fn webfinger_annotation_is_case_insensitive() {
        let service = service(json!({WEBFINGER_KEY: "TrUe"}), ports());
        let b = backend_from_service(&service, "cluster.local").expect("should resolve");
        assert_eq!(
            b.url.as_str(),
            "http://test-service.apps.svc.cluster.local:8080/"
        );
        assert_eq!(b.priority, 50);
    }

    #[test]
    fn priority_defaults_and_malformed_values_fall_back() {
        let defaulted = service(json!({WEBFINGER_KEY: "true"}), ports());
        let malformed = service(
            json!({WEBFINGER_KEY: "true", PRIORITY_KEY: "not-a-number"}),
            ports(),
        );
        assert_eq!(
            backend_from_service(&defaulted, "cluster.local")
                .unwrap()
                .priority,
            50
        );
        assert_eq!(
            backend_from_service(&malformed, "cluster.local")
                .unwrap()
                .priority,
            50
        );
    }

    #[test]
    fn custom_priority_is_used() {
        let service = service(json!({WEBFINGER_KEY: "true", PRIORITY_KEY: "100"}), ports());
        assert_eq!(
            backend_from_service(&service, "cluster.local")
                .unwrap()
                .priority,
            100
        );
    }

    #[test]
    fn http_annotation_prefers_http_then_https() {
        let service = service(json!({WEBFINGER_KEY: "true"}), ports());
        let b = backend_from_service(&service, "cluster.local").unwrap();
        assert_eq!(b.url.port(), Some(8080));
        assert_eq!(b.url.scheme(), "http");
    }

    #[test]
    fn https_annotation_reverses_named_port_preference() {
        let service = service(json!({WEBFINGER_KEY: "true", HTTPS_KEY: "true"}), ports());
        let b = backend_from_service(&service, "cluster.local").unwrap();
        assert_eq!(b.url.port(), Some(8443));
        assert_eq!(b.url.scheme(), "https");
    }

    #[test]
    fn unnamed_ports_fall_back_to_first_usable_port() {
        let service = service(
            json!({WEBFINGER_KEY: "true"}),
            json!([
                {"name": "metrics", "port": 9090},
                {"name": "invalid", "port": 0}
            ]),
        );
        let b = backend_from_service(&service, "cluster.local").unwrap();
        assert_eq!(b.url.port(), Some(9090));
    }

    #[test]
    fn explicit_port_name_overrides_automatic_selection() {
        let service = service(
            json!({WEBFINGER_KEY: "true", PORT_KEY: "metrics"}),
            json!([
                {"name": "http", "port": 8080},
                {"name": "metrics", "port": 9090}
            ]),
        );
        let b = backend_from_service(&service, "cluster.local").unwrap();
        assert_eq!(b.url.port(), Some(9090));
        assert_eq!(b.url.scheme(), "http");
    }

    #[test]
    fn missing_explicit_port_name_skips_service() {
        let service = service(json!({WEBFINGER_KEY: "true", PORT_KEY: "missing"}), ports());
        assert!(backend_from_service(&service, "cluster.local").is_none());
    }

    #[test]
    fn https_named_port_uses_actual_non_443_port() {
        let service = service(
            json!({WEBFINGER_KEY: "true"}),
            json!([
                {"name": "https", "port": 8443}
            ]),
        );
        let b = backend_from_service(&service, "cluster.local").unwrap();
        assert_eq!(
            b.url.as_str(),
            "https://test-service.apps.svc.cluster.local:8443/"
        );
    }

    #[test]
    fn explicit_custom_port_https_depends_on_annotation() {
        let http = service(
            json!({WEBFINGER_KEY: "true", PORT_KEY: "custom"}),
            json!([
                {"name": "custom", "port": 9443}
            ]),
        );
        let https = service(
            json!({WEBFINGER_KEY: "true", PORT_KEY: "custom", HTTPS_KEY: "TRUE"}),
            json!([{ "name": "custom", "port": 9443 }]),
        );
        assert_eq!(
            backend_from_service(&http, "cluster.local")
                .unwrap()
                .url
                .scheme(),
            "http"
        );
        assert_eq!(
            backend_from_service(&https, "cluster.local")
                .unwrap()
                .url
                .scheme(),
            "https"
        );
    }

    #[test]
    fn namespace_falls_back_to_default_for_fqdn() {
        let mut service = service(json!({WEBFINGER_KEY: "true"}), ports());
        service.metadata.namespace = None;
        let b = backend_from_service(&service, "cluster.local").unwrap();
        assert_eq!(
            b.url.host_str(),
            Some("test-service.default.svc.cluster.local")
        );
    }

    #[test]
    fn service_without_usable_ports_is_skipped() {
        let service = service(
            json!({WEBFINGER_KEY: "true"}),
            json!([
                {"name": "http", "port": 0},
                {"name": "https", "port": -1}
            ]),
        );
        assert!(backend_from_service(&service, "cluster.local").is_none());
    }

    #[test]
    fn duplicate_urls_keep_highest_priority_and_sort() {
        let low = service(json!({WEBFINGER_KEY: "true", PRIORITY_KEY: "10"}), ports());
        let high = service(json!({WEBFINGER_KEY: "true", PRIORITY_KEY: "90"}), ports());
        let other = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "another-service", "namespace": "apps", "annotations": {WEBFINGER_KEY: "true"}},
            "spec": {"ports": [{"name": "http", "port": 8080}]}
        }))
        .expect("test service should deserialize");

        let backends = backends_from_services(vec![low, other, high], "cluster.local");
        assert_eq!(backends.len(), 2);
        assert_eq!(backends[0].name, "another-service");
        assert_eq!(backends[1].priority, 90);
    }
}
