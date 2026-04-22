mod cli;
mod config;
mod scenario;

use tracing::info;

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ix_tests=info".into()),
        )
        .with_target(false)
        .init();
}

fn main() -> anyhow::Result<()> {
    init_tracing();

    let cli = cli::Cli::parse()?;
    let config = config::SuiteConfig::load(&cli.config_path)?;
    let scenario = scenario::ScenarioName::parse(&cli.scenario)?;

    info!(
        config_path = %cli.config_path.display(),
        scenario = scenario.as_str(),
        service_binary = %config.service_binary.display(),
        validator_rpc_url = %config.validator_rpc_url,
        failure_artifact_root = %config.failure_artifact_root.display(),
        service_start_timeout_ms = config.service_start_timeout_ms,
        checkpoint_timeout_ms = config.checkpoint_timeout_ms,
        transaction_timeout_ms = config.transaction_timeout_ms,
        "loaded integration test suite config"
    );

    Ok(())
}
