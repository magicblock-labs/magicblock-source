// Copyright 2022 Blockdaemon Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError, Result as PluginResult,
    },
    reqwest::Url,
    serde::Deserialize,
    std::{collections::BTreeMap, fs::File, io::Read, net::SocketAddr, path::Path},
};

/// Plugin config.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[allow(dead_code)]
    libpath: String,

    /// Kafka config.
    pub kafka: KafkaConfig,

    /// Optional ksqlDB startup restore config.
    #[serde(default)]
    pub ksql: KsqlConfig,

    /// Plugin-specific runtime config.
    pub plugin: PluginConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    #[serde(default)]
    pub client: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KsqlConfig {
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default = "default_ksql_table")]
    pub table: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginConfig {
    #[serde(default = "default_shutdown_timeout_ms")]
    pub shutdown_timeout_ms: u64,
    pub local_rpc_url: String,
    pub admin: SocketAddr,
    #[serde(default)]
    pub metrics: bool,
}

fn default_shutdown_timeout_ms() -> u64 {
    30_000
}

fn default_ksql_table() -> String {
    "accounts".to_owned()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            libpath: "".to_owned(),
            kafka: KafkaConfig {
                bootstrap_servers: String::new(),
                topic: String::new(),
                client: BTreeMap::new(),
            },
            ksql: KsqlConfig {
                url: None,
                table: default_ksql_table(),
            },
            plugin: PluginConfig {
                shutdown_timeout_ms: default_shutdown_timeout_ms(),
                local_rpc_url: String::new(),
                admin: SocketAddr::from(([127, 0, 0, 1], 0)),
                metrics: false,
            },
        }
    }
}

impl Default for KsqlConfig {
    fn default() -> Self {
        Self {
            url: None,
            table: default_ksql_table(),
        }
    }
}

impl Config {
    /// Read plugin from TOML file.
    pub fn read_from<P: AsRef<Path>>(config_path: P) -> PluginResult<Self> {
        let mut file = File::open(config_path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let mut this: Self = toml::from_str(&contents)
            .map_err(|e| GeyserPluginError::ConfigFileReadError { msg: e.to_string() })?;
        this.fill_defaults();
        this.validate()?;
        Ok(this)
    }

    fn set_default(&mut self, k: &'static str, v: &'static str) {
        if !self.kafka.client.contains_key(k) {
            self.kafka.client.insert(k.to_owned(), v.to_owned());
        }
    }

    fn fill_defaults(&mut self) {
        self.set_default("request.required.acks", "1");
        self.set_default("message.timeout.ms", "30000");
        self.set_default("compression.type", "lz4");
        self.set_default("partitioner", "murmur2_random");
    }

    fn validate(&self) -> PluginResult<()> {
        if self.kafka.bootstrap_servers.trim().is_empty() {
            return Err(GeyserPluginError::ConfigFileReadError {
                msg: "missing required config field `kafka.bootstrap_servers`".to_owned(),
            });
        }

        if self.kafka.topic.trim().is_empty() {
            return Err(GeyserPluginError::ConfigFileReadError {
                msg: "missing required config field `kafka.topic`".to_owned(),
            });
        }

        if self.plugin.local_rpc_url.trim().is_empty() {
            return Err(GeyserPluginError::ConfigFileReadError {
                msg: "missing required config field `plugin.local_rpc_url`".to_owned(),
            });
        }

        if self.plugin.admin.port() == 0 {
            return Err(GeyserPluginError::ConfigFileReadError {
                msg: "invalid admin address: port 0 is not allowed".to_owned(),
            });
        }

        if let Some(url) = &self.ksql.url {
            let trimmed = url.trim();
            if trimmed.is_empty() {
                return Err(GeyserPluginError::ConfigFileReadError {
                    msg: "invalid config field `ksql.url`: URL must not be empty".to_owned(),
                });
            }

            let parsed =
                Url::parse(trimmed).map_err(|error| GeyserPluginError::ConfigFileReadError {
                    msg: format!("invalid config field `ksql.url`: {error}"),
                })?;

            match parsed.scheme() {
                "http" | "https" => {}
                scheme => {
                    return Err(GeyserPluginError::ConfigFileReadError {
                        msg: format!(
                            "invalid config field `ksql.url`: unsupported scheme `{scheme}`"
                        ),
                    });
                }
            }

            if !parsed.has_host() {
                return Err(GeyserPluginError::ConfigFileReadError {
                    msg: "invalid config field `ksql.url`: host is required".to_owned(),
                });
            }

            if self.ksql.table.trim().is_empty() {
                return Err(GeyserPluginError::ConfigFileReadError {
                    msg: "invalid config field `ksql.table`: table must not be empty".to_owned(),
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Config;

    fn parse_config(toml: &str) -> Result<Config, String> {
        let mut config: Config = toml::from_str(toml).map_err(|error| error.to_string())?;
        config.fill_defaults();
        config.validate().map_err(|error| format!("{error:?}"))?;
        Ok(config)
    }

    #[test]
    fn test_parses_valid_minimal_config() {
        let config = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap();

        assert_eq!(config.kafka.topic, "solana.testnet.account_updates");
        assert_eq!(config.plugin.local_rpc_url, "http://127.0.0.1:8899");
    }

    #[test]
    fn test_rejects_missing_admin() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
"#,
        )
        .unwrap_err();

        assert!(error.contains("missing field `admin`"));
    }

    #[test]
    fn test_rejects_missing_kafka_topic() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap_err();

        assert!(error.contains("missing field `topic`"));
    }

    #[test]
    fn test_rejects_missing_bootstrap_servers() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "   "
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap_err();

        assert!(error.contains("missing required config field `kafka.bootstrap_servers`"));
    }

    #[test]
    fn test_rejects_legacy_filter_fields() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"
filters = [{ update_account_topic = "legacy" }]

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap_err();

        assert!(error.contains("unknown field `filters`"));
    }

    #[test]
    fn test_parses_config_with_metrics_enabled() {
        let config = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
metrics = true
"#,
        )
        .unwrap();

        assert!(config.plugin.metrics);
    }

    #[test]
    fn test_metrics_defaults_to_false() {
        let config = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap();

        assert!(!config.plugin.metrics);
    }

    #[test]
    fn test_parses_config_without_ksql_startup_restore_url() {
        let config = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap();

        assert_eq!(config.ksql.url, None);
    }

    #[test]
    fn test_parses_config_with_valid_ksql_startup_restore_url() {
        let config = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[ksql]
url = "https://127.0.0.1:8088"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap();

        assert_eq!(config.ksql.url.as_deref(), Some("https://127.0.0.1:8088"));
    }

    #[test]
    fn test_rejects_empty_ksql_startup_restore_url() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[ksql]
url = "   "

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap_err();

        assert!(error.contains("URL must not be empty"));
    }

    #[test]
    fn test_rejects_ksql_startup_restore_url_without_scheme() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[ksql]
url = "127.0.0.1:8088"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap_err();

        assert!(error.contains("relative URL without a base"));
    }

    #[test]
    fn test_rejects_ksql_startup_restore_url_without_host() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[ksql]
url = "http://:8088"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap_err();

        assert!(error.contains("empty host") || error.contains("host is required"));
    }

    #[test]
    fn test_passes_through_kafka_client_overrides() {
        let config = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[kafka.client]
"linger.ms" = "5"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap();

        assert_eq!(config.kafka.client.get("linger.ms"), Some(&"5".to_owned()));
    }
}
