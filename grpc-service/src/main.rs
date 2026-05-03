mod app;
mod config;
mod domain;
mod errors;
mod grpc_service;
mod kafka;
mod ksql;
mod output;
mod preflight;
mod traits;

use anyhow::Result;
use tokio_util::sync::CancellationToken;

use crate::app::App;
use crate::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let config = Config::load()?;
    let shutdown = CancellationToken::new();
    let (app, grpc_handle) =
        App::new_grpc_with_console(config, shutdown.clone())?;
    let mut app_task = tokio::spawn(async move { app.run().await });

    let app_result = tokio::select! {
        result = &mut app_task => result?,
        _ = shutdown_signal() => {
            tracing::info!("shutdown requested");
            shutdown.cancel();
            app_task.await?
        }
    };

    grpc_handle.shutdown().await?;
    Ok(app_result?)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kafka2grpc=info,rdkafka=warn".into()),
        )
        .with_target(false)
        .init();
}
