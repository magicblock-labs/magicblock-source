use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use serde::Serialize;

use crate::client::TestGrpcClient;
use crate::config::SuiteConfig;
use crate::layout::ServiceInstance;
use crate::scenario::ScenarioName;

#[allow(dead_code)]
pub struct ServiceLogPaths {
    pub stdout: PathBuf,
    pub stderr: PathBuf,
}

#[allow(dead_code)]
pub struct RunArtifacts {
    run_dir: PathBuf,
    run_id: String,
    failure_root: PathBuf,
    persist_on_failure: bool,
}

impl RunArtifacts {
    pub fn new(
        config: &SuiteConfig,
        scenario: ScenarioName,
    ) -> anyhow::Result<Self> {
        let pid = std::process::id();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let run_dir = PathBuf::from(format!(
            "target/ix-tests/tmp/{}-{}",
            scenario.as_str(),
            pid
        ));
        let run_id = format!("{}-{}", pid, timestamp);
        std::fs::create_dir_all(&run_dir).with_context(|| {
            format!("failed to create run dir: {}", run_dir.display())
        })?;

        Ok(Self {
            run_dir,
            run_id,
            failure_root: config.failure_artifact_root.clone(),
            persist_on_failure: true,
        })
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn generated_service_config_path(
        &self,
        instance: ServiceInstance,
    ) -> PathBuf {
        self.run_dir
            .join(format!("{}.generated.toml", Self::service_label(instance)))
    }

    pub fn service_logs(&self, instance: ServiceInstance) -> ServiceLogPaths {
        let label = Self::service_label(instance);
        ServiceLogPaths {
            stdout: self.run_dir.join(format!("{label}.stdout.log")),
            stderr: self.run_dir.join(format!("{label}.stderr.log")),
        }
    }

    fn service_label(instance: ServiceInstance) -> &'static str {
        match instance {
            ServiceInstance::One => "service-1",
            ServiceInstance::Two => "service-2",
        }
    }

    pub fn dump_service_logs_at(paths: &ServiceLogPaths) -> anyhow::Result<()> {
        for path in [&paths.stdout, &paths.stderr] {
            if path.exists() {
                let content =
                    std::fs::read_to_string(path).with_context(|| {
                        format!(
                            "failed to read service log: {}",
                            path.display()
                        )
                    })?;
                println!("--- {} ---\n{}", path.display(), content);
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn dump_service_logs(
        &self,
        instance: ServiceInstance,
    ) -> anyhow::Result<()> {
        let paths = self.service_logs(instance);
        Self::dump_service_logs_at(&paths)
    }

    #[allow(dead_code)]
    pub fn client_updates_path(&self, scenario: ScenarioName) -> PathBuf {
        self.run_dir
            .join(format!("{}-client-updates.json", scenario.as_str()))
    }

    pub fn write_client_updates(
        &self,
        scenario: ScenarioName,
        clients: &[TestGrpcClient],
    ) -> anyhow::Result<()> {
        #[derive(Serialize)]
        struct ClientUpdates {
            client_id: usize,
            service: ServiceInstance,
            endpoint: String,
            updates: Vec<crate::observation::ObservedUpdate>,
        }

        let payload = clients
            .iter()
            .map(|client| ClientUpdates {
                client_id: client.id,
                service: client.service,
                endpoint: client.endpoint.clone(),
                updates: client.log().snapshot(),
            })
            .collect::<Vec<_>>();
        let path = self.client_updates_path(scenario);
        let json = serde_json::to_vec_pretty(&payload)
            .context("failed to serialize client updates")?;
        std::fs::write(&path, json).with_context(|| {
            format!("failed to write client updates to {}", path.display())
        })
    }

    #[allow(dead_code)]
    pub fn persist_failure(&self) -> anyhow::Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let scenario_name = self
            .run_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");
        let dest = self
            .failure_root
            .join(format!("{scenario_name}-{timestamp}"));
        std::fs::create_dir_all(&self.failure_root).with_context(|| {
            format!(
                "failed to create failure root: {}",
                self.failure_root.display()
            )
        })?;
        std::fs::rename(&self.run_dir, &dest).with_context(|| {
            format!(
                "failed to move run dir {} to {}",
                self.run_dir.display(),
                dest.display()
            )
        })?;
        Ok(())
    }

    pub fn cleanup_success(&self) -> anyhow::Result<()> {
        if self.run_dir.exists() {
            std::fs::remove_dir_all(&self.run_dir).with_context(|| {
                format!("failed to remove run dir: {}", self.run_dir.display())
            })?;
        }
        Ok(())
    }
}
