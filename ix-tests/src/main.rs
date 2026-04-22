#[allow(dead_code)]
mod accounts;
mod artifacts;
mod cli;
mod config;
mod layout;
mod runner;
mod scenario;
mod service;

use tracing::info;

use crate::layout::ServiceInstance;
use crate::scenario::ScenarioName;
use crate::service::{ServiceController, ServiceSpec};

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ix_tests=info".into()),
        )
        .with_target(false)
        .init();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cli = cli::Cli::parse()?;
    let config = config::SuiteConfig::load(&cli.config_path)?;
    let requested = scenario::ScenarioName::parse(&cli.scenario)?;

    info!(
        config_path = %cli.config_path.display(),
        scenario = requested.as_str(),
        service_binary = %config.service_binary.display(),
        validator_rpc_url = %config.validator_rpc_url,
        failure_artifact_root = %config.failure_artifact_root.display(),
        service_start_timeout_ms = config.service_start_timeout_ms,
        checkpoint_timeout_ms = config.checkpoint_timeout_ms,
        transaction_timeout_ms = config.transaction_timeout_ms,
        "loaded integration test suite config"
    );

    let scenarios = runner::ordered_scenarios(requested);
    let names: Vec<&str> = scenarios.iter().map(|s| s.as_str()).collect();
    info!(scenarios = ?names, "resolved scenario execution order");

    let controller = ServiceController::new(&config);

    for scenario in &scenarios {
        info!(scenario = scenario.as_str(), "running scenario");
        let artifacts = artifacts::RunArtifacts::new(&config, *scenario)?;

        if *scenario == ScenarioName::SingleBasic {
            let spec = ServiceSpec::for_instance(ServiceInstance::One);
            let svc = controller.start(&spec, &artifacts).await?;
            controller.shutdown(svc).await?;
        }

        artifacts.cleanup_success()?;
        info!(scenario = scenario.as_str(), "scenario passed");
    }

    Ok(())
}
