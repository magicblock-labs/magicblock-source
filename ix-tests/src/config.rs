use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Clone, Debug)]
pub struct SuiteConfig {
    pub service_binary: PathBuf,
    pub validator_rpc_url: String,
    pub failure_artifact_root: PathBuf,
    pub service_start_timeout_ms: u64,
    pub checkpoint_timeout_ms: u64,
    pub transaction_timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
struct FileSuiteConfig {
    #[serde(default)]
    service_binary: Option<PathBuf>,
    #[serde(default)]
    validator_rpc_url: Option<String>,
    #[serde(default)]
    failure_artifact_root: Option<PathBuf>,
    #[serde(default)]
    service_start_timeout_ms: Option<u64>,
    #[serde(default)]
    checkpoint_timeout_ms: Option<u64>,
    #[serde(default)]
    transaction_timeout_ms: Option<u64>,
}

impl SuiteConfig {
    pub fn load(path: &Path) -> anyhow::Result<SuiteConfig> {
        let contents = std::fs::read_to_string(path)?;
        let file: FileSuiteConfig = toml::from_str(&contents)?;

        Ok(SuiteConfig {
            service_binary: file.service_binary.unwrap_or_else(|| {
                PathBuf::from("target/debug/magigblock-grpc-service")
            }),
            validator_rpc_url: file
                .validator_rpc_url
                .unwrap_or_else(|| "http://127.0.0.1:8899".to_owned()),
            failure_artifact_root: file
                .failure_artifact_root
                .unwrap_or_else(|| PathBuf::from("target/ix-tests/failures")),
            service_start_timeout_ms: file
                .service_start_timeout_ms
                .unwrap_or(10_000),
            checkpoint_timeout_ms: file.checkpoint_timeout_ms.unwrap_or(20_000),
            transaction_timeout_ms: file
                .transaction_timeout_ms
                .unwrap_or(20_000),
        })
    }
}
