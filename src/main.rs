#[warn(clippy::pedantic)]
mod backend;
mod error;
mod http;
mod k8s;
mod webfinger;

use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_ansi(false))
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cluster_domain = std::env::var("CLUSTER_DOMAIN")
        .unwrap_or_else(|_| "cluster.local".to_string());
    let port = std::env::var("PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(8080);

    info!("starting fingerjoin");

    let kube_client = kube::Client::try_default().await?;
    let state = Arc::new(k8s::BackendState::new());

    {
        let state = state.clone();
        tokio::spawn(async move {
            k8s::start_reconciler(kube_client, state, cluster_domain).await;
        });
    }

    let app = http::app(state);
    let listener = tokio::net::TcpListener::bind(format!("[::]:{port}")).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
