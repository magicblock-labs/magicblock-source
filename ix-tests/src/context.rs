use crate::accounts::ScenarioAccounts;
use crate::artifacts::RunArtifacts;
use crate::config::SuiteConfig;
use crate::expectation::CheckpointRunner;
use crate::service::ServiceController;
use crate::validator::ValidatorDriver;

#[allow(dead_code)]
pub struct ScenarioContext {
    pub suite_config: SuiteConfig,
    pub artifacts: RunArtifacts,
    pub service_controller: ServiceController,
    pub validator: ValidatorDriver,
    pub checkpoint_runner: CheckpointRunner,
    pub accounts: ScenarioAccounts,
}
