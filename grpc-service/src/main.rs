mod app;
mod config;
mod domain;
mod errors;
mod grpc_service;
mod kafka;
mod ksql;
mod output;
mod traits;

use anyhow::Result;

use crate::app::App;
use crate::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let config = Config::load()?;
    let (app, grpc_handle) = App::new_grpc_with_console(config)?;
    let result = app.run().await;
    grpc_handle.shutdown().await?;
    Ok(result?)
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
