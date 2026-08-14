use crate::error::Error;
use crate::webfinger::{parse_jrd, JrdResource};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{debug, warn};
use url::Url;

#[derive(Debug, Clone, PartialEq)]
pub struct Backend {
    pub name: String,
    pub url: Url,
    pub priority: u16,
}

pub async fn fetch_jrd(
    client: &reqwest::Client,
    backend: &Backend,
    resource: &str,
) -> Result<JrdResource, Error> {
    let url = backend
        .url
        .join(".well-known/webfinger")
        .map_err(Error::Url)?;
    let url = url
        .join(&format!("?resource={resource}"))
        .map_err(Error::Url)?;

    debug!(backend = %backend.name, url = %url, "fetching webfinger");

    let resp = client
        .get(url.clone())
        .header("Accept", "application/jrd+json")
        .send()
        .await
        .map_err(Error::Request)?;

    let status = resp.status();
    debug!(backend = %backend.name, url = %url, status = %status, "received response");

    if !status.is_success() {
        return Err(Error::AllBackendsFailed);
    }

    let bytes = resp.bytes().await.map_err(Error::Request)?;
    debug!(backend = %backend.name, body = %String::from_utf8_lossy(&bytes), "response body");

    let jrd = parse_jrd(&bytes).map_err(|e| {
        warn!(backend = %backend.name, error = %e, "failed to parse JRD");
        Error::Webfinger(e)
    })?;

    Ok(jrd)
}

pub async fn fan_out(
    client: &reqwest::Client,
    backends: &[Backend],
    resource: &str,
    max_in_flight: usize,
) -> Vec<(u16, JrdResource)> {
    let semaphore = Arc::new(Semaphore::new(max_in_flight));

    let futures = backends.iter().map(|backend| {
        let sem = semaphore.clone();
        async move {
            let _guard = sem.acquire().await.expect("semaphore never closed");
            fetch_jrd(client, backend, resource)
                .await
                .ok()
                .map(|jrd| (backend.priority, jrd))
        }
    });

    futures::future::join_all(futures)
        .await
        .into_iter()
        .flatten()
        .collect()
}
