mod dual_concurrent;
mod dual_restart;
mod single_basic;
mod single_load;

use anyhow::bail;

use crate::context::ScenarioContext;
use crate::scenario::ScenarioName;

pub async fn run_scenario(
    name: ScenarioName,
    ctx: &ScenarioContext,
) -> anyhow::Result<()> {
    match name {
        ScenarioName::SingleBasic => single_basic::run(ctx).await,
        ScenarioName::SingleLoad => single_load::run(ctx).await,
        ScenarioName::DualConcurrent => dual_concurrent::run(ctx).await,
        ScenarioName::DualRestart => dual_restart::run(ctx).await,
        ScenarioName::All => bail!("scenario dispatch does not accept 'all'"),
    }
}
