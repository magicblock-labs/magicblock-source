use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use std::time::Duration;

use anyhow::{Context, bail};
use helius_laserstream::grpc::PingRequest;
use helius_laserstream::grpc::geyser_client::GeyserClient;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use tokio::process::Command;
use tracing::{debug, info, warn};

use crate::artifacts::RunArtifacts;
use crate::config::SuiteConfig;
use crate::layout::ServiceInstance;

const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_SERVICE_GRPC_PORT: u16 = 50051;

/// Describes whether a service was started by this harness or supplied externally.
#[allow(dead_code)]
pub enum ServiceOwnership {
    /// Child process spawned by [`ServiceController::start`].
    ///
    /// The process is spawned with `.kill_on_drop(true)` and stored in
    /// [`ServiceHandle`] as `ServiceOwnership::Owned(child)`. Dropping the
    /// handle without passing it to [`ServiceController::shutdown`] therefore
    /// kills the child immediately instead of performing the graceful SIGTERM
    /// shutdown path.
    Owned(tokio::process::Child),
    External,
}

/// Handle that keeps a service process alive for the lifetime of a test.
///
/// Callers must retain this handle until they are ready to terminate the
/// service. For owned services, call [`ServiceController::shutdown`] with the
/// handle to request graceful termination; otherwise the contained child was
/// created with `.kill_on_drop(true)` and will be killed when the handle (and
/// its `ServiceOwnership::Owned(child)`) is dropped.
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

#[derive(Debug, Deserialize)]
struct FileServiceConfig {
    #[serde(default)]
    grpc: Option<FileServiceGrpcConfig>,
}

#[derive(Debug, Deserialize)]
struct FileServiceGrpcConfig {
    #[serde(default)]
    port: Option<u16>,
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
                endpoint: "http://127.0.0.1:50051".to_owned(),
            },
            ServiceInstance::Two => Self {
                instance,
                config_path: PathBuf::from(
                    "ix-tests/configs/grpc-service/service-2.toml",
                ),
                endpoint: "http://127.0.0.1:50052".to_owned(),
            },
        }
    }
}

impl ServiceController {
    fn endpoint_port(endpoint: &str) -> anyhow::Result<u16> {
        let socket_addr = endpoint
            .strip_prefix("http://")
            .ok_or_else(|| {
                anyhow::anyhow!("unsupported endpoint format: {endpoint}")
            })?
            .parse::<SocketAddr>()
            .with_context(|| {
                format!("failed to parse endpoint as host:port: {endpoint}")
            })?;
        Ok(socket_addr.port())
    }

    fn configured_grpc_port(config_path: &Path) -> anyhow::Result<u16> {
        let config_text =
            std::fs::read_to_string(config_path).with_context(|| {
                format!(
                    "failed to read service config: {}",
                    config_path.display()
                )
            })?;
        let file: FileServiceConfig = toml::from_str(&config_text)
            .with_context(|| {
                format!(
                    "failed to parse service config: {}",
                    config_path.display()
                )
            })?;
        Ok(file
            .grpc
            .and_then(|grpc| grpc.port)
            .unwrap_or(DEFAULT_SERVICE_GRPC_PORT))
    }

    fn validate_spec_matches_config(
        &self,
        spec: &ServiceSpec,
    ) -> anyhow::Result<()> {
        let endpoint_port = Self::endpoint_port(&spec.endpoint)?;
        let config_port = Self::configured_grpc_port(&spec.config_path)?;
        if endpoint_port != config_port {
            bail!(
                "refusing to continue for {:?}: probe endpoint {} uses port {}, but template {} binds port {}; probe/reuse and spawned-service bind port would diverge",
                spec.instance,
                spec.endpoint,
                endpoint_port,
                spec.config_path.display(),
                config_port,
            );
        }
        Ok(())
    }

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
        self.validate_spec_matches_config(spec)?;

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
                let pid = child.id();

                if let Some(pid) = pid {
                    match signal::kill(
                        Pid::from_raw(pid as i32),
                        Signal::SIGTERM,
                    ) {
                        Ok(()) => {
                            info!(
                                endpoint = %service.endpoint,
                                pid,
                                "graceful shutdown requested for grpc-service"
                            );

                            match tokio::time::timeout(
                                GRACEFUL_SHUTDOWN_TIMEOUT,
                                child.wait(),
                            )
                            .await
                            {
                                Ok(wait_result) => {
                                    let status = wait_result.context(
                                        "failed to wait for child after SIGTERM",
                                    )?;
                                    info!(
                                        endpoint = %service.endpoint,
                                        pid,
                                        status = %status,
                                        "grpc-service shut down gracefully"
                                    );
                                    return Ok(());
                                }
                                Err(_) => {
                                    warn!(
                                        endpoint = %service.endpoint,
                                        pid,
                                        timeout = ?GRACEFUL_SHUTDOWN_TIMEOUT,
                                        "graceful shutdown timed out; forcing grpc-service kill"
                                    );
                                }
                            }
                        }
                        Err(err) => {
                            warn!(
                                endpoint = %service.endpoint,
                                pid,
                                error = %err,
                                "failed to request graceful shutdown; forcing grpc-service kill"
                            );
                        }
                    }
                }

                child
                    .start_kill()
                    .context("failed to send forced kill to grpc-service")?;
                let status = child
                    .wait()
                    .await
                    .context("failed to wait for child after forced kill")?;
                warn!(
                    endpoint = %service.endpoint,
                    pid = pid.unwrap_or_default(),
                    status = %status,
                    "grpc-service shutdown was forced"
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

        enum ReadinessProbeResult {
            Ready,
            ConnectError(tonic::transport::Error),
            PingError(tonic::Status),
        }

        loop {
            let attempt_timeout =
                deadline.saturating_duration_since(tokio::time::Instant::now());
            let probe = async {
                match GeyserClient::connect(endpoint.to_owned()).await {
                    Ok(mut client) => {
                        match client.ping(PingRequest { count: 1 }).await {
                            Ok(_) => ReadinessProbeResult::Ready,
                            Err(err) => ReadinessProbeResult::PingError(err),
                        }
                    }
                    Err(err) => ReadinessProbeResult::ConnectError(err),
                }
            };

            match tokio::time::timeout(attempt_timeout, probe).await {
                Ok(ReadinessProbeResult::Ready) => {
                    info!(endpoint, "grpc-service is ready");
                    return Ok(());
                }
                Ok(ReadinessProbeResult::PingError(err))
                    if err.code() == tonic::Code::Unavailable =>
                {
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
                Ok(ReadinessProbeResult::PingError(err)) => {
                    warn!(endpoint, "ping returned non-readiness error: {err}");
                }
                Ok(ReadinessProbeResult::ConnectError(err)) => {
                    debug!(
                        endpoint,
                        "grpc-service not yet accepting connections: {err}"
                    );
                }
                Err(_) => {
                    if !announced_waiting {
                        info!(
                            endpoint,
                            timeout = ?attempt_timeout,
                            "grpc-service readiness probe timed out; waiting for startup"
                        );
                        announced_waiting = true;
                    } else {
                        debug!(
                            endpoint,
                            timeout = ?attempt_timeout,
                            "grpc-service readiness probe still timing out"
                        );
                    }
                }
            }

            if tokio::time::Instant::now() >= deadline {
                if let Err(err) = RunArtifacts::dump_service_logs_at(log_paths)
                {
                    warn!(
                        endpoint,
                        error = %err,
                        "failed to dump service logs after grpc-service readiness timeout"
                    );
                }
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
