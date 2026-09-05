use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("webfinger serialization error: {0}")]
    Webfinger(#[from] super::webfinger::Error),

    #[error("invalid resource format: {0}")]
    InvalidResource(String),

    #[error("no backends available")]
    NoBackends,

    #[error("resource not found")]
    ResourceNotFound,

    #[error("resource gone")]
    ResourceGone,

    #[error("all backends failed")]
    AllBackendsFailed,
}

impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        use axum::http::StatusCode;

        let (status, msg) = match &self {
            Error::InvalidResource(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            Error::NoBackends => (
                StatusCode::SERVICE_UNAVAILABLE,
                "no backends configured".to_string(),
            ),
            Error::ResourceNotFound => (StatusCode::NOT_FOUND, "resource not found".to_string()),
            Error::ResourceGone => (StatusCode::GONE, "resource gone".to_string()),
            Error::AllBackendsFailed => {
                (StatusCode::BAD_GATEWAY, "all backends failed".to_string())
            }
            Error::Webfinger(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_string(),
            ),
        };

        let body = serde_json::json!({
            "error": msg
        });

        let mut builder = axum::response::Response::builder()
            .status(status)
            .header("Content-Type", "application/json");

        // 503s are transient while the watcher recovers.
        if matches!(self, Error::NoBackends) {
            builder = builder.header("Retry-After", crate::k8s::RETRY_AFTER.as_secs().to_string());
        }

        builder
            .body(axum::body::Body::from(body.to_string()))
            .expect("static error response must build")
    }
}
