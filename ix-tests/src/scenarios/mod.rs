mod dual_concurrent;
mod dual_restart;
mod single_basic;
mod single_load;
mod single_triage;

use anyhow::anyhow;

use crate::client::TestGrpcClient;
use crate::context::ScenarioContext;
use crate::scenario::ScenarioName;

pub struct ScenarioFailure {
    pub error: anyhow::Error,
    pub clients: Vec<TestGrpcClient>,
}

pub async fn run_scenario(
    name: ScenarioName,
    ctx: &ScenarioContext,
) -> Result<(), ScenarioFailure> {
    match name {
        ScenarioName::SingleTriage => single_triage::run(ctx).await,
        ScenarioName::SingleBasic => single_basic::run(ctx).await,
        ScenarioName::SingleLoad => single_load::run(ctx).await,
        ScenarioName::DualConcurrent => dual_concurrent::run(ctx).await,
        ScenarioName::DualRestart => dual_restart::run(ctx).await,
        ScenarioName::All => Err(ScenarioFailure {
            error: anyhow!("scenario dispatch does not accept 'all'"),
            clients: Vec::new(),
        }),
    }
}
