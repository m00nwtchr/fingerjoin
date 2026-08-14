use crate::webfinger::{parse_jrd, JrdResource};
use reqwest::StatusCode;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{debug, warn};
use url::Url;

/// Upper bound on a backend's response body. JRD documents are small;
/// anything larger is misbehaving and must not be buffered into memory.
const MAX_BODY_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub struct Backend {
    pub name: String,
    pub url: Url,
    pub priority: u16,
}

/// Per-backend result of a WebFinger query, kept distinct so the handler can
/// tell "nobody knows this resource" (404) apart from "backends are broken"
/// (502).
#[derive(Debug)]
pub enum FetchOutcome {
    Success(JrdResource),
    /// Backend answered 404: it does not know the resource.
    NotFound,
    /// Backend answered 410: the resource existed and is permanently gone.
    Gone,
    /// Backend answered another 4xx: it declines to serve this resource
    /// form (e.g. GoToSocial rejects `acct:@domain` with 400). Definitive
    /// like a 404, so it must not poison the aggregate into a 502.
    Declined,
    /// Transport error, 5xx, oversized or unparseable body.
    Failed,
}

/// Aggregated fan-out result across all backends.
#[derive(Debug, Default)]
pub struct FanOutResult {
    pub successes: Vec<(u16, JrdResource)>,
    pub not_found: usize,
    pub gone: usize,
    pub declined: usize,
    pub failed: usize,
}

pub async fn fetch_jrd(
    client: &reqwest::Client,
    backend: &Backend,
    resource: &str,
    rels: &[String],
) -> FetchOutcome {
    let url = match backend.url.join("/.well-known/webfinger") {
        Ok(u) => u,
        Err(e) => {
            warn!(backend = %backend.name, error = %e, "invalid backend URL");
            return FetchOutcome::Failed;
        }
    };

    // reqwest percent-encodes query values, so resources containing
    // `&`, `#`, `%` or spaces survive the trip intact.
    let mut query: Vec<(&str, &str)> = vec![("resource", resource)];
    for rel in rels {
        query.push(("rel", rel));
    }

    debug!(backend = %backend.name, url = %url, "querying backend");

    let resp = match client
        .get(url.clone())
        .query(&query)
        .header("Accept", "application/jrd+json")
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!(backend = %backend.name, url = %url, error = %e, "backend request failed");
            return FetchOutcome::Failed;
        }
    };

    let status = resp.status();
    match status {
        s if s.is_success() => {}
        StatusCode::NOT_FOUND => {
            debug!(backend = %backend.name, "backend does not know the resource");
            return FetchOutcome::NotFound;
        }
        StatusCode::GONE => {
            debug!(backend = %backend.name, "backend reports the resource gone");
            return FetchOutcome::Gone;
        }
        s if s.is_client_error() => {
            debug!(backend = %backend.name, status = %s, "backend declined the resource");
            return FetchOutcome::Declined;
        }
        s => {
            // 5xx and redirects land here: the client follows no redirects,
            // deliberately, so a registered backend cannot bounce the proxy
            // to an arbitrary URL.
            warn!(backend = %backend.name, status = %s, "backend returned unexpected status");
            return FetchOutcome::Failed;
        }
    }

    if let Some(len) = resp.content_length() {
        if len > MAX_BODY_BYTES as u64 {
            warn!(backend = %backend.name, content_length = len, "backend response too large");
            return FetchOutcome::Failed;
        }
    }

    let mut resp = resp;
    let mut body: Vec<u8> = Vec::new();
    loop {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                if body.len() + chunk.len() > MAX_BODY_BYTES {
                    warn!(backend = %backend.name, "backend response too large");
                    return FetchOutcome::Failed;
                }
                body.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(e) => {
                warn!(backend = %backend.name, error = %e, "failed to read backend response");
                return FetchOutcome::Failed;
            }
        }
    }

    debug!(backend = %backend.name, body = %String::from_utf8_lossy(&body), "response body");

    match parse_jrd(&body) {
        Ok(jrd) => FetchOutcome::Success(jrd),
        Err(e) => {
            warn!(backend = %backend.name, error = %e, "failed to parse JRD");
            FetchOutcome::Failed
        }
    }
}

pub async fn fan_out(
    client: &reqwest::Client,
    backends: &[Backend],
    resource: &str,
    rels: &[String],
    max_in_flight: usize,
) -> FanOutResult {
    let semaphore = Arc::new(Semaphore::new(max_in_flight));

    let futures = backends.iter().map(|backend| {
        let sem = semaphore.clone();
        async move {
            let _guard = sem.acquire().await.expect("semaphore never closed");
            (
                backend.priority,
                fetch_jrd(client, backend, resource, rels).await,
            )
        }
    });

    let mut result = FanOutResult::default();
    for (priority, outcome) in futures::future::join_all(futures).await {
        match outcome {
            FetchOutcome::Success(jrd) => result.successes.push((priority, jrd)),
            FetchOutcome::NotFound => result.not_found += 1,
            FetchOutcome::Gone => result.gone += 1,
            FetchOutcome::Declined => result.declined += 1,
            FetchOutcome::Failed => result.failed += 1,
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{mock_client, spawn_mock_backend};

    fn backend(url: &Url, priority: u16) -> Backend {
        Backend {
            name: format!("mock-{priority}"),
            url: url.clone(),
            priority,
        }
    }

    #[tokio::test]
    async fn test_fetch_success_echoes_resource() {
        let url = spawn_mock_backend("a").await;
        let b = backend(&url, 50);

        // Resource with characters that must be percent-encoded on the wire;
        // the mock echoes the decoded resource back as the subject.
        let resource = "acct:we ird&user@example.com";
        match fetch_jrd(&mock_client(), &b, resource, &[]).await {
            FetchOutcome::Success(jrd) => assert_eq!(jrd.subject.as_deref(), Some(resource)),
            other => panic!("expected success, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_fetch_not_found_and_gone() {
        let url = spawn_mock_backend("a").await;
        let b = backend(&url, 50);
        let client = mock_client();

        assert!(matches!(
            fetch_jrd(&client, &b, "acct:missing@example.com", &[]).await,
            FetchOutcome::NotFound
        ));
        assert!(matches!(
            fetch_jrd(&client, &b, "acct:gone@example.com", &[]).await,
            FetchOutcome::Gone
        ));
        assert!(matches!(
            fetch_jrd(&client, &b, "acct:boom@example.com", &[]).await,
            FetchOutcome::Failed
        ));
        // A backend 400 is a definitive decline, not a failure.
        assert!(matches!(
            fetch_jrd(&client, &b, "acct:decline@example.com", &[]).await,
            FetchOutcome::Declined
        ));
    }

    #[tokio::test]
    async fn test_fetch_rejects_oversized_body() {
        let url = spawn_mock_backend("a").await;
        let b = backend(&url, 50);
        assert!(matches!(
            fetch_jrd(&mock_client(), &b, "acct:huge@example.com", &[]).await,
            FetchOutcome::Failed
        ));
    }

    #[tokio::test]
    async fn test_fan_out_mixed_backends() {
        let url = spawn_mock_backend("a").await;
        // A port that was bound and released: connection refused.
        let dead = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = l.local_addr().unwrap();
            drop(l);
            Url::parse(&format!("http://{addr}")).unwrap()
        };

        let backends = vec![backend(&url, 50), backend(&dead, 60)];
        let out = fan_out(&mock_client(), &backends, "acct:alice@example.com", &[], 10).await;

        assert_eq!(out.successes.len(), 1);
        assert_eq!(out.failed, 1);
        assert_eq!(out.not_found, 0);
    }
}
