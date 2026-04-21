use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::domain::PubkeyFilter;
use crate::errors::{GeykagError, GeykagResult};

const DEFAULT_CONFIG_PATH: &str = "configs/config.toml";
const DEFAULT_BOOTSTRAP_SERVERS: &str = "localhost:9092";
const DEFAULT_TOPIC: &str = "solana.testnet.account_updates";
const DEFAULT_GROUP_ID: &str = "kafka2grpc-dev";
const DEFAULT_KSQL_URL: &str = "http://localhost:8088";
const DEFAULT_KSQL_TABLE: &str = "ACCOUNTS";
const DEFAULT_VALIDATOR_ACCOUNTS_FILTER_URL: &str =
    "http://localhost:3000/filters/accounts";
const DEFAULT_AUTO_OFFSET_RESET: &str = "latest";
const DEFAULT_GRPC_BIND_HOST: &str = "0.0.0.0";
const DEFAULT_GRPC_PORT: u16 = 50051;
const DEFAULT_GRPC_DISPATCHER_CAPACITY: usize = 1024;

#[derive(Clone, Debug)]
pub struct Config {
    pub kafka: KafkaConfig,
    pub ksql: KsqlConfig,
    pub validator: ValidatorConfig,
    pub grpc: GrpcConfig,
    pub pubkey_filter: Option<PubkeyFilter>,
}

#[derive(Clone, Debug)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    pub group_id: String,
    pub auto_offset_reset: String,
    pub client: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct KsqlConfig {
    pub url: String,
    pub table: String,
}

#[derive(Clone, Debug)]
pub struct ValidatorConfig {
    pub accounts_filter_url: String,
}

#[derive(Clone, Debug)]
pub struct GrpcConfig {
    pub bind_host: String,
    pub port: u16,
    pub dispatcher_capacity: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    #[serde(default)]
    kafka: Option<FileKafkaConfig>,
    #[serde(default)]
    ksql: Option<FileKsqlConfig>,
    #[serde(default)]
    validator: Option<FileValidatorConfig>,
    #[serde(default)]
    grpc: Option<FileGrpcConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileKafkaConfig {
    #[serde(default)]
    bootstrap_servers: Option<String>,
    #[serde(default)]
    topic: Option<String>,
    #[serde(default)]
    group_id: Option<String>,
    #[serde(default)]
    auto_offset_reset: Option<String>,
    #[serde(default)]
    client: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileKsqlConfig {
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    table: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileValidatorConfig {
    #[serde(default)]
    accounts_filter_url: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileGrpcConfig {
    #[serde(default)]
    bind_host: Option<String>,
    #[serde(default)]
    port: Option<u16>,
    #[serde(default)]
    dispatcher_capacity: Option<usize>,
}

impl Config {
    pub fn load() -> GeykagResult<Self> {
        let (path, pubkey_filter) = Self::parse_args()?;
        let file = Self::load_file(&path)?;

        let kafka = file.kafka.unwrap_or(FileKafkaConfig {
            bootstrap_servers: None,
            topic: None,
            group_id: None,
            auto_offset_reset: None,
            client: BTreeMap::new(),
        });
        let ksql = file.ksql.unwrap_or(FileKsqlConfig {
            url: None,
            table: None,
        });
        let validator = file.validator.unwrap_or(FileValidatorConfig {
            accounts_filter_url: None,
        });
        let grpc = file.grpc.unwrap_or(FileGrpcConfig {
            bind_host: None,
            port: None,
            dispatcher_capacity: None,
        });

        Ok(Self {
            kafka: KafkaConfig {
                bootstrap_servers: kafka
                    .bootstrap_servers
                    .unwrap_or_else(|| DEFAULT_BOOTSTRAP_SERVERS.to_owned()),
                topic: kafka.topic.unwrap_or_else(|| DEFAULT_TOPIC.to_owned()),
                group_id: kafka
                    .group_id
                    .unwrap_or_else(|| DEFAULT_GROUP_ID.to_owned()),
                auto_offset_reset: kafka
                    .auto_offset_reset
                    .unwrap_or_else(|| DEFAULT_AUTO_OFFSET_RESET.to_owned()),
                client: kafka.client,
            },
            ksql: KsqlConfig {
                url: ksql.url.unwrap_or_else(|| DEFAULT_KSQL_URL.to_owned()),
                table: ksql
                    .table
                    .unwrap_or_else(|| DEFAULT_KSQL_TABLE.to_owned()),
            },
            validator: ValidatorConfig {
                accounts_filter_url: validator
                    .accounts_filter_url
                    .unwrap_or_else(|| {
                        DEFAULT_VALIDATOR_ACCOUNTS_FILTER_URL.to_owned()
                    }),
            },
            grpc: GrpcConfig {
                bind_host: grpc
                    .bind_host
                    .unwrap_or_else(|| DEFAULT_GRPC_BIND_HOST.to_owned()),
                port: grpc.port.unwrap_or(DEFAULT_GRPC_PORT),
                dispatcher_capacity: grpc
                    .dispatcher_capacity
                    .unwrap_or(DEFAULT_GRPC_DISPATCHER_CAPACITY),
            },
            pubkey_filter,
        })
    }

    fn parse_args() -> GeykagResult<(PathBuf, Option<PubkeyFilter>)> {
        let args = std::env::args().skip(1).collect::<Vec<_>>();

        match args.as_slice() {
            [] => Ok((PathBuf::from(DEFAULT_CONFIG_PATH), None)),
            [arg] if arg == "--config" => Err(GeykagError::InvalidCliUsage),
            [pubkey] => Ok((
                PathBuf::from(DEFAULT_CONFIG_PATH),
                Some(PubkeyFilter::parse(pubkey)?),
            )),
            [flag, path] if flag == "--config" => {
                Ok((PathBuf::from(path), None))
            }
            [flag, path, pubkey] if flag == "--config" => {
                Ok((PathBuf::from(path), Some(PubkeyFilter::parse(pubkey)?)))
            }
            _ => Err(GeykagError::InvalidCliUsage),
        }
    }

    fn load_file(path: &Path) -> GeykagResult<FileConfig> {
        let contents = fs::read_to_string(path).map_err(|source| {
            GeykagError::ConfigFileRead {
                path: path.to_path_buf(),
                source,
            }
        })?;

        toml::from_str(&contents).map_err(|source| {
            GeykagError::ConfigTomlParse {
                path: path.to_path_buf(),
                source,
            }
        })
    }
}
