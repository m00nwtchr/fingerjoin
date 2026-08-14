//! In-process mock WebFinger backend for tests.
//!
//! Behavior keys off the local part of the queried resource:
//! `missing` → 404, `gone` → 410, `boom` → 500, `huge` → an oversized body,
//! `decline` → 400 from variant "a" only (mimicking GoToSocial rejecting
//! `acct:@domain` while other backends still answer), anything else → a JRD
//! echoing the resource as subject, with links and aliases parameterized by
//! the backend's `variant` so merges are observable.

use axum::{
    extract::Query,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use url::Url;

pub fn mock_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

pub async fn spawn_mock_backend(variant: &'static str) -> Url {
    let app = Router::new().route(
        "/.well-known/webfinger",
        get(move |q| mock_handler(variant, q)),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Url::parse(&format!("http://{addr}")).unwrap()
}

async fn mock_handler(
    variant: &'static str,
    Query(params): Query<Vec<(String, String)>>,
) -> Response {
    let Some(resource) = params
        .iter()
        .find(|(k, _)| k == "resource")
        .map(|(_, v)| v.clone())
    else {
        return StatusCode::BAD_REQUEST.into_response();
    };

    if resource.contains("missing") {
        return StatusCode::NOT_FOUND.into_response();
    }
    if resource.contains("gone") {
        return StatusCode::GONE.into_response();
    }
    if resource.contains("boom") {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    if resource.contains("decline") && variant == "a" {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if resource.contains("huge") {
        return (
            StatusCode::OK,
            [("content-type", "application/jrd+json")],
            "x".repeat(512 * 1024),
        )
            .into_response();
    }

    let body = serde_json::json!({
        "subject": resource,
        "aliases": [
            format!("https://{variant}.example.com/@user"),
            "https://shared.example.com/@user",
        ],
        "properties": {
            "http://example.com/ns/variant": variant,
            "http://example.com/ns/null": null,
        },
        "links": [
            {
                "rel": "self",
                "type": "application/activity+json",
                "href": format!("https://{variant}.example.com/users/user"),
            },
            {
                "rel": "http://webfinger.net/rel/profile-page",
                "type": "text/html",
                "href": "https://shared.example.com/@user",
            },
            {
                "rel": "http://ostatus.org/schema/1.0/subscribe",
                "template": format!("https://{variant}.example.com/authorize_interaction?uri={{uri}}"),
            },
        ],
    });

    (
        StatusCode::OK,
        [("content-type", "application/jrd+json")],
        body.to_string(),
    )
        .into_response()
}
