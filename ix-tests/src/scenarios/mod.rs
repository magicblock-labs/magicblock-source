mod single_basic;

use anyhow::bail;

use crate::context::ScenarioContext;
use crate::scenario::ScenarioName;

pub async fn run_scenario(
    name: ScenarioName,
    ctx: &ScenarioContext,
) -> anyhow::Result<()> {
    match name {
        ScenarioName::SingleBasic => single_basic::run(ctx).await,
        ScenarioName::SingleLoad
        | ScenarioName::DualConcurrent
        | ScenarioName::DualRestart => {
            bail!("scenario not implemented: {}", name.as_str())
        }
        ScenarioName::All => bail!("scenario dispatch does not accept 'all'"),
    }
}
