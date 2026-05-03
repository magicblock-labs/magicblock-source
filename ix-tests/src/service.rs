use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, bail};
use helius_laserstream::grpc::PingRequest;
use helius_laserstream::grpc::geyser_client::GeyserClient;
use tokio::process::Command;
use tracing::{debug, info, warn};

use crate::artifacts::RunArtifacts;
use crate::config::SuiteConfig;
use crate::layout::ServiceInstance;

#[allow(dead_code)]
pub enum ServiceOwnership {
    Owned(tokio::process::Child),
    External,
}

#[allow(dead_code)]
pub struct ServiceHandle {
    pub instance: ServiceInstance,
    pub endpoint: String,
    pub ownership: ServiceOwnership,
}

#[allow(dead_code)]
impl ServiceHandle {
    pub fn is_owned(&self) -> bool {
        matches!(self.ownership, ServiceOwnership::Owned(_))
    }

    pub fn is_external(&self) -> bool {
        matches!(self.ownership, ServiceOwnership::External)
    }
}

pub struct ServiceController {
    service_binary: PathBuf,
    service_start_timeout: Duration,
}

pub struct ServiceSpec {
    pub instance: ServiceInstance,
    pub config_path: PathBuf,
    pub endpoint: String,
}

impl ServiceSpec {
    pub fn for_instance(instance: ServiceInstance) -> Self {
        match instance {
            ServiceInstance::One => Self {
                instance,
                config_path: PathBuf::from(
                    "ix-tests/configs/grpc-service/service-1.toml",
                ),
                endpoint: "http://127.0.0.1:51051".to_owned(),
            },
            ServiceInstance::Two => Self {
                instance,
                config_path: PathBuf::from(
                    "ix-tests/configs/grpc-service/service-2.toml",
                ),
                endpoint: "http://127.0.0.1:51052".to_owned(),
            },
        }
    }
}

impl ServiceController {
    pub fn new(config: &SuiteConfig) -> Self {
        Self {
            service_binary: config.service_binary.clone(),
            service_start_timeout: Duration::from_millis(
                config.service_start_timeout_ms,
            ),
        }
    }

    fn write_generated_config(
        &self,
        spec: &ServiceSpec,
        artifacts: &RunArtifacts,
    ) -> anyhow::Result<PathBuf> {
        let base_group_id = Self::base_group_id(spec.instance);
        let run_scoped_group_id =
            Self::run_scoped_group_id(spec.instance, artifacts);
        let base_group_id_line = format!("group_id = \"{base_group_id}\"");
        let generated_group_id_line =
            format!("group_id = \"{run_scoped_group_id}\"");
        let config_text = std::fs::read_to_string(&spec.config_path)
            .with_context(|| {
                format!(
                    "failed to read service config template: {}",
                    spec.config_path.display()
                )
            })?;
        if config_text.matches(&base_group_id_line).count() != 1 {
            bail!(
                "expected exactly one `{}` entry in {}",
                base_group_id_line,
                spec.config_path.display()
            );
        }
        let generated_config_text =
            config_text.replace(&base_group_id_line, &generated_group_id_line);
        let generated_config_path =
            artifacts.generated_service_config_path(spec.instance);
        std::fs::write(&generated_config_path, generated_config_text)
            .with_context(|| {
                format!(
                    "failed to write generated service config: {}",
                    generated_config_path.display()
                )
            })?;

        Ok(generated_config_path)
    }

    fn base_group_id(instance: ServiceInstance) -> &'static str {
        match instance {
            ServiceInstance::One => "ix-tests-service-1",
            ServiceInstance::Two => "ix-tests-service-2",
        }
    }

    fn run_scoped_group_id(
        instance: ServiceInstance,
        artifacts: &RunArtifacts,
    ) -> String {
        format!("{}-{}", Self::base_group_id(instance), artifacts.run_id())
    }

    pub async fn start(
        &self,
        spec: &ServiceSpec,
        artifacts: &RunArtifacts,
    ) -> anyhow::Result<ServiceHandle> {
        if self.probe_ready(&spec.endpoint).await {
            info!(
                endpoint = %spec.endpoint,
                "harness is reusing an already-running external grpc-service"
            );
            return Ok(ServiceHandle {
                instance: spec.instance,
                endpoint: spec.endpoint.clone(),
                ownership: ServiceOwnership::External,
            });
        }

        let generated_config_path =
            self.write_generated_config(spec, artifacts)?;
        let log_paths = artifacts.service_logs(spec.instance);

        let stdout_file = std::fs::File::create(&log_paths.stdout)
            .with_context(|| {
                format!(
                    "failed to create stdout log: {}",
                    log_paths.stdout.display()
                )
            })?;
        let stderr_file = std::fs::File::create(&log_paths.stderr)
            .with_context(|| {
                format!(
                    "failed to create stderr log: {}",
                    log_paths.stderr.display()
                )
            })?;

        let run_scoped_group_id =
            Self::run_scoped_group_id(spec.instance, artifacts);

        info!(
            binary = %self.service_binary.display(),
            config = %generated_config_path.display(),
            endpoint = %spec.endpoint,
            group_id = %run_scoped_group_id,
            "starting grpc-service"
        );

        let child = Command::new(&self.service_binary)
            .arg("--config")
            .arg(&generated_config_path)
            .stdout(stdout_file)
            .stderr(stderr_file)
            .kill_on_drop(true)
            .spawn()
            .with_context(|| {
                format!(
                    "failed to spawn service binary: {}",
                    self.service_binary.display()
                )
            })?;

        let handle = ServiceHandle {
            instance: spec.instance,
            endpoint: spec.endpoint.clone(),
            ownership: ServiceOwnership::Owned(child),
        };

        self.wait_until_ready(&spec.endpoint, &log_paths).await?;

        Ok(handle)
    }

    pub async fn shutdown(&self, service: ServiceHandle) -> anyhow::Result<()> {
        match service.ownership {
            ServiceOwnership::Owned(mut child) => {
                info!(
                    endpoint = %service.endpoint,
                    "shutting down grpc-service"
                );
                child.start_kill().context("failed to send kill")?;
                let status =
                    child.wait().await.context("failed to wait for child")?;
                debug!(
                    endpoint = %service.endpoint,
                    status = %status,
                    "grpc-service exited"
                );
            }
            ServiceOwnership::External => {
                info!(
                    endpoint = %service.endpoint,
                    "external grpc-service was left running intentionally"
                );
            }
        }
        Ok(())
    }

    async fn probe_ready(&self, endpoint: &str) -> bool {
        let Ok(mut client) = GeyserClient::connect(endpoint.to_owned()).await
        else {
            return false;
        };

        client.ping(PingRequest { count: 1 }).await.is_ok()
    }

    async fn wait_until_ready(
        &self,
        endpoint: &str,
        log_paths: &crate::artifacts::ServiceLogPaths,
    ) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + self.service_start_timeout;
        let mut announced_waiting = false;

        loop {
            match GeyserClient::connect(endpoint.to_owned()).await {
                Ok(mut client) => {
                    match client.ping(PingRequest { count: 1 }).await {
                        Ok(_) => {
                            info!(endpoint, "grpc-service is ready");
                            return Ok(());
                        }
                        Err(err) if err.code() == tonic::Code::Unavailable => {
                            if !announced_waiting {
                                info!(
                                    endpoint,
                                    message = %err.message(),
                                    "grpc-service listening but not yet ready; waiting for startup preflight"
                                );
                                announced_waiting = true;
                            } else {
                                debug!(
                                    endpoint,
                                    message = %err.message(),
                                    "grpc-service still preflight-pending"
                                );
                            }
                        }
                        Err(err) => {
                            warn!(
                                endpoint,
                                "ping returned non-readiness error: {err}"
                            );
                        }
                    }
                }
                Err(err) => {
                    debug!(
                        endpoint,
                        "grpc-service not yet accepting connections: {err}"
                    );
                }
            }

            if tokio::time::Instant::now() >= deadline {
                RunArtifacts::dump_service_logs_at(log_paths)
                    .context("failed to dump service logs")?;
                bail!(
                    "grpc-service at {} did not become ready within {:?}\n\
                     stdout: {}\n\
                     stderr: {}",
                    endpoint,
                    self.service_start_timeout,
                    log_paths.stdout.display(),
                    log_paths.stderr.display(),
                );
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}
