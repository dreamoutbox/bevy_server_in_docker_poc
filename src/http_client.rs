use std::{
    sync::OnceLock,
    time::{Duration, Instant},
};

/// Default HTTPS endpoint used when `HTTP_EXAMPLE_URL` is not set.
pub const DEFAULT_HTTP_EXAMPLE_URL: &str = "https://httpbin.org/get";

/// Environment variable overriding the default HTTP example URL.
pub const HTTP_EXAMPLE_URL_ENV: &str = "HTTP_EXAMPLE_URL";

/// Request timeout applied to all example `reqwest` HTTP calls.
pub const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum number of characters retained in [`HttpFetchOutcome::body_preview`].
pub const BODY_PREVIEW_MAX_CHARS: usize = 72;

static HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

/// Returns the shared pooled [`reqwest::Client`] configured with [`HTTP_REQUEST_TIMEOUT`].
fn shared_http_client() -> &'static reqwest::Client {
    HTTP_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(HTTP_REQUEST_TIMEOUT)
            .build()
            .expect("failed to build reqwest HTTP client")
    })
}

/// Result of an async `reqwest` HTTP GET request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpFetchOutcome {
    pub url: String,
    pub status_code: Option<u16>,
    pub elapsed_ms: u64,
    pub body_preview: String,
    pub error: Option<String>,
}

/// Resolves the HTTP example target URL from `HTTP_EXAMPLE_URL`, defaulting to [`DEFAULT_HTTP_EXAMPLE_URL`].
///
/// # Example
/// ```
/// use bevy_server_in_docker_poc::http_client::configured_example_url;
///
/// assert!(!configured_example_url().is_empty());
/// ```
pub fn configured_example_url() -> String {
    std::env::var(HTTP_EXAMPLE_URL_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_HTTP_EXAMPLE_URL.to_string())
}

/// Performs an async HTTP GET to `url` using [`reqwest`] and returns a structured [`HttpFetchOutcome`].
///
/// Must be executed inside a Tokio runtime (for example via [`crate::tokio_task::TokioTask::spawn`]).
///
/// # Arguments
/// * `url` - Full HTTP or HTTPS URL to fetch.
///
/// # Example
/// ```no_run
/// use bevy_server_in_docker_poc::{http_client::fetch_url, tokio_task::TokioTask};
///
/// let _task = TokioTask::spawn(async { fetch_url("https://httpbin.org/get").await });
/// ```
pub async fn fetch_url(url: &str) -> HttpFetchOutcome {
    let started = Instant::now();
    let response = match shared_http_client().get(url).send().await {
        Ok(resp) => resp,
        Err(err) => {
            return HttpFetchOutcome {
                url: url.to_string(),
                status_code: None,
                elapsed_ms: started.elapsed().as_millis() as u64,
                body_preview: String::new(),
                error: Some(err.to_string()),
            };
        }
    };

    let status_code = response.status().as_u16();
    match response.text().await {
        Ok(body) => HttpFetchOutcome {
            url: url.to_string(),
            status_code: Some(status_code),
            elapsed_ms: started.elapsed().as_millis() as u64,
            body_preview: compact_preview(&body, BODY_PREVIEW_MAX_CHARS),
            error: None,
        },
        Err(err) => HttpFetchOutcome {
            url: url.to_string(),
            status_code: Some(status_code),
            elapsed_ms: started.elapsed().as_millis() as u64,
            body_preview: String::new(),
            error: Some(err.to_string()),
        },
    }
}

/// Collapses whitespace into single spaces and truncates `text` to `max_chars` characters for UI/logs.
///
/// # Arguments
/// * `text` - Raw response body text.
/// * `max_chars` - Maximum character count before appending `"..."`.
fn compact_preview(text: &str, max_chars: usize) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = collapsed.chars();
    let preview: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{preview}...")
    } else {
        preview
    }
}

#[cfg(test)]
#[path = "http_client_test.rs"]
mod tests;
