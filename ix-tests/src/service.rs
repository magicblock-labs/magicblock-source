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
pub struct ManagedService {
    pub instance: ServiceInstance,
    pub endpoint: String,
    child: tokio::process::Child,
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

    pub async fn start(
        &self,
        spec: &ServiceSpec,
        artifacts: &RunArtifacts,
    ) -> anyhow::Result<ManagedService> {
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

        info!(
            binary = %self.service_binary.display(),
            config = %spec.config_path.display(),
            endpoint = %spec.endpoint,
            "starting grpc-service"
        );

        let child = Command::new(&self.service_binary)
            .arg("--config")
            .arg(&spec.config_path)
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

        let managed = ManagedService {
            instance: spec.instance,
            endpoint: spec.endpoint.clone(),
            child,
        };

        self.wait_until_ready(&spec.endpoint, &log_paths).await?;

        Ok(managed)
    }

    pub async fn shutdown(
        &self,
        mut service: ManagedService,
    ) -> anyhow::Result<()> {
        info!(
            endpoint = %service.endpoint,
            "shutting down grpc-service"
        );
        service.child.start_kill().context("failed to send kill")?;
        let status = service
            .child
            .wait()
            .await
            .context("failed to wait for child")?;
        debug!(
            endpoint = %service.endpoint,
            status = %status,
            "grpc-service exited"
        );
        Ok(())
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
