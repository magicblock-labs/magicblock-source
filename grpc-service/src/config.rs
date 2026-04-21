use std::env;

use crate::domain::PubkeyFilter;
use crate::errors::{GeykagError, GeykagResult};

const DEFAULT_BROKER: &str = "localhost:9092";
const DEFAULT_TOPIC: &str = "solana.testnet.account_updates";
const DEFAULT_GROUP_ID: &str = "kafka2grpc-dev";
const DEFAULT_KSQL_SERVER_URL: &str = "http://localhost:8088";
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
    pub broker: String,
    pub topic: String,
    pub group_id: String,
    pub auto_offset_reset: String,
}

#[derive(Clone, Debug)]
pub struct KsqlConfig {
    pub server_url: String,
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

impl Config {
    pub fn load() -> GeykagResult<Self> {
        let kafka = KafkaConfig {
            broker: env::var("KAFKA_BROKER")
                .unwrap_or_else(|_| DEFAULT_BROKER.to_owned()),
            topic: env::var("KAFKA_TOPIC")
                .unwrap_or_else(|_| DEFAULT_TOPIC.to_owned()),
            group_id: env::var("KAFKA_GROUP_ID")
                .unwrap_or_else(|_| DEFAULT_GROUP_ID.to_owned()),
            auto_offset_reset: env::var("KAFKA_AUTO_OFFSET_RESET")
                .unwrap_or_else(|_| DEFAULT_AUTO_OFFSET_RESET.to_owned()),
        };

        let ksql = KsqlConfig {
            server_url: env::var("KSQL_SERVER_URL")
                .unwrap_or_else(|_| DEFAULT_KSQL_SERVER_URL.to_owned()),
            table: env::var("KSQL_TABLE")
                .unwrap_or_else(|_| DEFAULT_KSQL_TABLE.to_owned()),
        };

        let validator = ValidatorConfig {
            accounts_filter_url: env::var("VALIDATOR_ACCOUNTS_FILTER_URL")
                .unwrap_or_else(|_| {
                    DEFAULT_VALIDATOR_ACCOUNTS_FILTER_URL.to_owned()
                }),
        };

        let grpc = GrpcConfig {
            bind_host: env::var("GRPC_BIND_HOST")
                .unwrap_or_else(|_| DEFAULT_GRPC_BIND_HOST.to_owned()),
            port: parse_env_or_default("GRPC_PORT", DEFAULT_GRPC_PORT)?,
            dispatcher_capacity: parse_env_or_default(
                "GRPC_DISPATCHER_CAPACITY",
                DEFAULT_GRPC_DISPATCHER_CAPACITY,
            )?,
        };

        let args = env::args().skip(1).collect::<Vec<_>>();
        let pubkey_filter = match args.as_slice() {
            [] => None,
            [pubkey] => Some(PubkeyFilter::parse(pubkey)?),
            _ => return Err(GeykagError::InvalidCliUsage),
        };

        Ok(Self {
            kafka,
            ksql,
            validator,
            grpc,
            pubkey_filter,
        })
    }
}

fn parse_env_or_default<T>(key: &'static str, default: T) -> GeykagResult<T>
where
    T: std::str::FromStr + Copy,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match env::var(key) {
        Ok(value) => {
            value
                .parse()
                .map_err(|source| GeykagError::InvalidEnvValue {
                    key,
                    value,
                    source: Box::new(source),
                })
        }
        Err(env::VarError::NotPresent) => Ok(default),
        Err(source) => Err(GeykagError::InvalidEnvRead { key, source }),
    }
}
