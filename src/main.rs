mod backend;
mod error;
mod http;
mod k8s;
mod webfinger;

use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

fn init_tracing() {
    // Default to `info` when RUST_LOG is unset: an EnvFilter built from an
    // empty environment enables nothing at all.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let json = std::env::var("LOG_FORMAT").is_ok_and(|v| v.eq_ignore_ascii_case("json"));
    if json {
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().json().with_ansi(false))
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().with_ansi(false))
            .init();
    }
}

/// One shared client for all backend requests: connection pooling, uniform
/// timeouts, and no redirect following (a registered backend must not be able
/// to bounce the proxy to an arbitrary URL).
fn build_backend_client() -> Result<reqwest::Client, Box<dyn std::error::Error>> {
    let mut builder = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .connect_timeout(Duration::from_secs(2))
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(concat!("fingerjoin/", env!("CARGO_PKG_VERSION")));

    // In-cluster HTTPS backends usually present certificates from a private
    // CA that the bundled webpki roots do not trust.
    if let Ok(path) = std::env::var("EXTRA_CA_CERTS") {
        let pem = std::fs::read(&path)
            .map_err(|e| format!("failed to read EXTRA_CA_CERTS {path}: {e}"))?;
        let certs = reqwest::Certificate::from_pem_bundle(&pem)
            .map_err(|e| format!("failed to parse EXTRA_CA_CERTS {path}: {e}"))?;
        let count = certs.len();
        for cert in certs {
            builder = builder.add_root_certificate(cert);
        }
        info!(path = %path, count, "loaded extra CA certificates");
    }

    Ok(builder.build()?)
}

async fn shutdown_signal() {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = sigterm.recv() => {},
    }
    info!("shutdown signal received, draining connections");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let cluster_domain =
        std::env::var("CLUSTER_DOMAIN").unwrap_or_else(|_| "cluster.local".to_string());
    let port = match std::env::var("PORT") {
        Err(_) => 8080,
        Ok(v) => v.parse::<u16>().unwrap_or_else(|_| {
            warn!(value = %v, "invalid PORT, using 8080");
            8080
        }),
    };

    info!(
        version = env!("CARGO_PKG_VERSION"),
        cluster_domain = %cluster_domain,
        port,
        "starting fingerjoin"
    );

    let kube_client = kube::Client::try_default().await?;
    let state = Arc::new(k8s::BackendState::new());

    {
        let state = state.clone();
        tokio::spawn(async move {
            k8s::start_reconciler(kube_client, state, cluster_domain).await;
        });
    }

    let app = http::app(http::AppState {
        backends: state,
        client: build_backend_client()?,
    });

    let listener = tokio::net::TcpListener::bind(format!("[::]:{port}")).await?;
    info!(addr = %listener.local_addr()?, "listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("shut down cleanly");
    Ok(())
}
