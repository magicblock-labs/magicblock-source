use std::time::{Duration, Instant};

use reqwest::Client;
use serde_json::json;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::errors::{GeykagError, GeykagResult};
use crate::kafka::KafkaAccountUpdateStream;

const PROBE_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const PROBE_MAX_BACKOFF: Duration = Duration::from_secs(2);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// Run startup preflight against all required dependencies. Returns
/// `Ok(())` once every probe has succeeded at least once. Returns a
/// `PreflightTimeout` error tagged with the last failing probe if the
/// total elapsed time exceeds `total_timeout`.
#[allow(dead_code)]
pub async fn wait_for_dependencies(
    config: &Config,
    total_timeout: Duration,
) -> GeykagResult<()> {
    let started = Instant::now();
    let deadline = started + total_timeout;
    let http = build_http_client()?;

    info!(
        plugin = config.validator.accounts_filter_url,
        rpc = config.validator.rpc_url,
        kafka = config.kafka.bootstrap_servers,
        "startup preflight: probing dependencies"
    );

    run_probe_with_retry("validator-plugin-admin", deadline, || async {
        probe_validator_plugin_admin(
            &http,
            &config.validator.accounts_filter_url,
        )
        .await
    })
    .await?;

    run_probe_with_retry("validator-rpc", deadline, || async {
        probe_validator_rpc_health(&http, &config.validator.rpc_url).await
    })
    .await?;

    run_probe_with_retry("kafka-metadata", deadline, || async {
        KafkaAccountUpdateStream::probe_config(&config.kafka)
    })
    .await?;

    let elapsed_ms = started.elapsed().as_millis();
    info!(elapsed_ms, "startup preflight: all dependencies ready");
    Ok(())
}

#[allow(dead_code)]
async fn run_probe_with_retry<F, Fut>(
    probe: &'static str,
    deadline: Instant,
    mut attempt: F,
) -> GeykagResult<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = GeykagResult<()>>,
{
    let started = Instant::now();
    let mut backoff = PROBE_INITIAL_BACKOFF;
    loop {
        debug!(probe, "startup preflight: attempting probe");
        match attempt().await {
            Ok(()) => {
                let elapsed_ms = started.elapsed().as_millis();
                info!(probe, elapsed_ms, "startup preflight: probe ok");
                return Ok(());
            }
            Err(error) => {
                if Instant::now() >= deadline {
                    let elapsed_ms = started.elapsed().as_millis();
                    warn!(
                        probe,
                        elapsed_ms,
                        error = %error,
                        "startup preflight: probe deadline exceeded"
                    );
                    return Err(GeykagError::PreflightTimeout {
                        probe,
                        elapsed_ms,
                    });
                }
                debug!(
                    probe,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %error,
                    "startup preflight: probe failed; will retry"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(PROBE_MAX_BACKOFF);
            }
        }
    }
}

#[allow(dead_code)]
fn build_http_client() -> GeykagResult<Client> {
    Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|source| GeykagError::PreflightClientBuild { source })
}

#[allow(dead_code)]
async fn probe_validator_plugin_admin(
    http: &Client,
    url: &str,
) -> GeykagResult<()> {
    let body = r#"{"pubkeys":[]}"#;
    let response = http
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .map_err(|source| GeykagError::PreflightValidatorPluginRequest {
            url: url.to_owned(),
            source,
        })?;

    response.error_for_status().map(|_| ()).map_err(|source| {
        GeykagError::PreflightValidatorPluginStatus {
            url: url.to_owned(),
            source,
        }
    })
}

#[allow(dead_code)]
async fn probe_validator_rpc_health(
    http: &Client,
    url: &str,
) -> GeykagResult<()> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getHealth",
    });
    let response = http
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|source| GeykagError::PreflightValidatorRpcRequest {
            url: url.to_owned(),
            source,
        })?;

    response.error_for_status().map(|_| ()).map_err(|source| {
        GeykagError::PreflightValidatorRpcStatus {
            url: url.to_owned(),
            source,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, http::StatusCode, routing::post};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    async fn spawn_test_server(router: Router) -> String {
        let listener =
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn test_probe_validator_plugin_admin_succeeds_on_2xx() {
        let app = Router::new()
            .route("/filters/accounts", post(|| async { StatusCode::OK }));
        let base = spawn_test_server(app).await;
        let url = format!("{base}/filters/accounts");
        let http = build_http_client().unwrap();

        probe_validator_plugin_admin(&http, &url).await.unwrap();
    }

    #[tokio::test]
    async fn test_probe_validator_plugin_admin_fails_on_5xx() {
        let app = Router::new().route(
            "/filters/accounts",
            post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        );
        let base = spawn_test_server(app).await;
        let url = format!("{base}/filters/accounts");
        let http = build_http_client().unwrap();

        let err = probe_validator_plugin_admin(&http, &url).await.unwrap_err();
        assert!(matches!(
            err,
            GeykagError::PreflightValidatorPluginStatus { .. }
        ));
    }

    #[tokio::test]
    async fn test_probe_validator_rpc_health_succeeds_on_2xx() {
        let app = Router::new().route(
            "/",
            post(|| async {
                (
                    StatusCode::OK,
                    [("content-type", "application/json")],
                    r#"{"jsonrpc":"2.0","result":"ok","id":1}"#,
                )
            }),
        );
        let base = spawn_test_server(app).await;
        let http = build_http_client().unwrap();

        probe_validator_rpc_health(&http, &base).await.unwrap();
    }

    #[tokio::test]
    async fn test_run_probe_with_retry_eventually_succeeds() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_inner = calls.clone();
        let deadline = Instant::now() + Duration::from_secs(5);

        run_probe_with_retry("test-probe", deadline, move || {
            let calls_inner = calls_inner.clone();
            async move {
                let n = calls_inner.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err(GeykagError::PreflightTimeout {
                        probe: "test",
                        elapsed_ms: 0,
                    })
                } else {
                    Ok(())
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_run_probe_with_retry_returns_timeout() {
        let deadline = Instant::now() + Duration::from_millis(50);
        let err = run_probe_with_retry("test-probe", deadline, || async {
            Err(GeykagError::PreflightTimeout {
                probe: "noop",
                elapsed_ms: 0,
            })
        })
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            GeykagError::PreflightTimeout {
                probe: "test-probe",
                ..
            }
        ));
    }
}
