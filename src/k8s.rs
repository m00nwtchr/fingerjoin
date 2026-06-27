use crate::backend::Backend;
use kube::{
    Client,
    api::{Api, ListParams, NotUsed, Object},
    discovery::Discovery,
};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, interval};
use tracing::{error, info};

const WEBFINGER_KEY: &str = "fingerjoin.naktis.eu/webfinger";
const PRIORITY_KEY: &str = "fingerjoin.naktis.eu/priority";
const HTTPS_KEY: &str = "fingerjoin.naktis.eu/https";
const BACKEND_KEY: &str = "fingerjoin.naktis.eu/backend";

pub struct BackendState {
    backends: RwLock<Vec<Backend>>,
}

impl BackendState {
    pub fn new() -> Self {
        Self {
            backends: RwLock::new(Vec::new()),
        }
    }

    pub async fn update(&self, new_backends: Vec<Backend>) {
        let mut backends = self.backends.write().await;
        *backends = new_backends.clone();
        info!(count = new_backends.len(), "backends updated");
    }

    pub async fn get_all(&self) -> Vec<Backend> {
        self.backends.read().await.clone()
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
    name: Option<String>,
    #[serde(default)]
    port: Option<u16>,
}

type HTTPRoute = Object<HTTPRouteSpec, NotUsed>;

pub async fn start_reconciler(client: Client, state: Arc<BackendState>, cluster_domain: String) {
    let mut ticker = interval(Duration::from_secs(30));

    let discovery = Discovery::new(client.clone())
        .run()
        .await
        .expect("failed to discover apis");
    let apigroup = discovery
        .groups()
        .find(|g| g.name() == "gateway.networking.k8s.io")
        .expect("gateway.networking.k8s.io not found");
    let (ar, _caps) = apigroup
        .recommended_resources()
        .iter()
        .find(|(ar, _)| ar.kind == "HTTPRoute")
        .expect("HttpRoute not found")
        .clone();
    let api: Api<HTTPRoute> = Api::all_with(client.clone(), &ar);

    loop {
        ticker.tick().await;

        let routes = match api.list(&ListParams::default()).await {
            Ok(r) => r,
            Err(e) => {
                error!(err = %e, "failed to list httproutes");
                continue;
            }
        };

        let mut all_backends = Vec::new();
        for route in routes {
            let annotations = match route.metadata.annotations.as_ref() {
                Some(a) => a,
                None => continue,
            };

            let is_webfinger = annotations
                .get(WEBFINGER_KEY)
                .map(|v| v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);
            if !is_webfinger {
                continue;
            }

            let priority = annotations
                .get(PRIORITY_KEY)
                .and_then(|v| v.parse().ok())
                .unwrap_or(50);

            let https = annotations
                .get(HTTPS_KEY)
                .map(|v| v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);

            let backend_index: usize = annotations
                .get(BACKEND_KEY)
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);

            let rule = match route.spec.rules.get(backend_index) {
                Some(r) => r,
                None => continue,
            };

            let backend_entry = match rule.backend_refs.first() {
                Some(b) => b,
                None => continue,
            };

            let backend_name = match backend_entry.name.as_deref() {
                Some(n) => n,
                None => continue,
            };

            let namespace = route.metadata.namespace.as_deref().unwrap_or("default");
            let port = backend_entry.port.unwrap_or(if https { 443 } else { 80 });
            let scheme = if https { "https" } else { "http" };
            let host = format!("{}.{}.svc.{}", backend_name, namespace, cluster_domain);
            let url = format!("{}://{}:{}", scheme, host, port);
            all_backends.push(Backend {
                name: backend_name.to_string(),
                url: url::Url::parse(&url).unwrap(),
                priority,
            });
        }
        state.update(all_backends).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn make_annotations(
        webfinger: Option<&str>,
        priority: Option<&str>,
        https: Option<&str>,
        backend: Option<&str>,
    ) -> BTreeMap<String, String> {
        let mut m = BTreeMap::new();
        if let Some(v) = webfinger {
            m.insert(WEBFINGER_KEY.to_string(), v.to_string());
        }
        if let Some(v) = priority {
            m.insert(PRIORITY_KEY.to_string(), v.to_string());
        }
        if let Some(v) = https {
            m.insert(HTTPS_KEY.to_string(), v.to_string());
        }
        if let Some(v) = backend {
            m.insert(BACKEND_KEY.to_string(), v.to_string());
        }
        m
    }

    #[test]
    fn test_webfinger_annotation_exists() {
        let a = make_annotations(Some("true"), None, None, None);
        assert!(a.get(WEBFINGER_KEY).map(|v| v == "true").unwrap_or(false));
    }

    #[test]
    fn test_priority_default() {
        let a = make_annotations(Some("true"), None, None, None);
        let priority = a
            .get(PRIORITY_KEY)
            .and_then(|v| v.parse().ok())
            .unwrap_or(50);
        assert_eq!(priority, 50);
    }

    #[test]
    fn test_priority_custom() {
        let a = make_annotations(Some("true"), Some("100"), None, None);
        let priority = a
            .get(PRIORITY_KEY)
            .and_then(|v| v.parse().ok())
            .unwrap_or(50);
        assert_eq!(priority, 100);
    }

    #[test]
    fn test_https_false() {
        let a = make_annotations(Some("true"), None, Some("false"), None);
        let https = a
            .get(HTTPS_KEY)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        assert!(!https);
    }

    #[test]
    fn test_https_true() {
        let a = make_annotations(Some("true"), None, Some("true"), None);
        let https = a
            .get(HTTPS_KEY)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        assert!(https);
    }

    #[test]
    fn test_backend_index_default() {
        let a = make_annotations(Some("true"), None, None, None);
        let idx: usize = a.get(BACKEND_KEY).and_then(|v| v.parse().ok()).unwrap_or(0);
        assert_eq!(idx, 0);
    }

    #[test]
    fn test_backend_index_custom() {
        let a = make_annotations(Some("true"), None, None, Some("2"));
        let idx: usize = a.get(BACKEND_KEY).and_then(|v| v.parse().ok()).unwrap_or(0);
        assert_eq!(idx, 2);
    }
}
