mod accounts;
mod artifacts;
mod cli;
#[allow(dead_code)]
mod client;
mod config;
mod context;
mod expectation;
mod layout;
#[allow(dead_code)]
mod observation;
mod runner;
mod scenario;
mod scenarios;
mod service;
#[allow(dead_code)]
mod validator;

use tracing::{info, warn};

use crate::accounts::ScenarioAccounts;
use crate::context::ScenarioContext;
use crate::expectation::CheckpointRunner;
use crate::service::ServiceController;
use crate::validator::ValidatorDriver;

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ix_tests=info".into()),
        )
        .without_time()
        .with_file(true)
        .with_line_number(true)
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

    for scenario in &scenarios {
        info!(scenario = scenario.as_str(), "running scenario");
        let artifacts = artifacts::RunArtifacts::new(&config, *scenario)?;
        let ctx = ScenarioContext {
            suite_config: config.clone(),
            artifacts,
            service_controller: ServiceController::new(&config),
            validator: ValidatorDriver::new(&config),
            checkpoint_runner: CheckpointRunner::new(&config),
            accounts: ScenarioAccounts::new(),
        };

        match scenarios::run_scenario(*scenario, &ctx).await {
            Ok(()) => {
                ctx.artifacts.cleanup_success()?;
                info!(scenario = scenario.as_str(), "scenario passed");
            }
            Err(failure) => {
                let scenarios::ScenarioFailure {
                    error: original_error,
                    clients,
                } = failure;

                if !clients.is_empty()
                    && let Err(error) =
                        ctx.artifacts.write_client_updates(*scenario, &clients)
                {
                    warn!(?error, "failed to write client updates artifact");
                }
                for service in
                    [layout::ServiceInstance::One, layout::ServiceInstance::Two]
                {
                    if let Err(error) = ctx.artifacts.dump_service_logs(service)
                    {
                        warn!(?error, ?service, "failed to dump service logs");
                    }
                }
                if let Err(error) = ctx.artifacts.persist_failure() {
                    warn!(?error, "failed to persist failure artifacts");
                }
                return Err(original_error);
            }
        }
    }

    Ok(())
}
